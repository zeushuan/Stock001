"""即時 / 盤中資料載入

來源優先順序：
  1. Alpaca (若設了 ALPACA_API_KEY) — 真即時 US 股票
  2. yfinance 1m bars — 免費，15-20 分鐘延遲，7 天歷史限制

對外接口（兩個都用 Streamlit cache 30 秒）：
  - load_daily(ticker, days)
  - load_intraday(ticker, interval='5m', days=2)
"""
import os
import io
import contextlib
import logging
from typing import Optional
import pandas as pd
import numpy as np


def _yf_silent_download(*args, **kwargs):
    """silent yfinance download"""
    import yfinance as yf
    logging.getLogger('yfinance').setLevel(logging.CRITICAL)
    with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
        df = yf.download(*args, progress=False, auto_adjust=True, **kwargs)
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def load_daily(ticker: str, days: int = 100) -> Optional[pd.DataFrame]:
    """日線（用 yfinance 14mo period 然後 tail）"""
    try:
        df = _yf_silent_download(ticker, period='14mo', interval='1d', timeout=30)
        df = _normalize_columns(df)
        if df is None or len(df) == 0: return None
        df = df.dropna().tail(days)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        return df
    except Exception:
        return None


def load_intraday(ticker: str, interval: str = '5m',
                    days: int = 2) -> Optional[pd.DataFrame]:
    """盤中 K（yfinance 1m / 5m / 15m / 1h）

    period 限制：
      - 1m: 最多 7 天
      - 5m / 15m: 最多 60 天
      - 1h: 最多 730 天
    """
    period_map = {
        '1m': f'{min(days, 7)}d',
        '5m': f'{min(days, 60)}d',
        '15m': f'{min(days, 60)}d',
        '1h': f'{min(days, 730)}d',
    }
    period = period_map.get(interval, f'{days}d')
    try:
        df = _yf_silent_download(ticker, period=period, interval=interval, timeout=30)
        df = _normalize_columns(df)
        if df is None or len(df) == 0: return None
        df = df.dropna()
        # 保留 tz 給 chart 顯示，盤後顯示 last-bar 時間正確
        return df
    except Exception:
        return None


def get_realtime_price(ticker: str) -> Optional[dict]:
    """即時 quote — 用 yfinance Ticker.info 或最後 1m bar"""
    try:
        df = load_intraday(ticker, '1m', days=1)
        if df is None or len(df) == 0:
            return None
        last = df.iloc[-1]
        return {
            'price': float(last['Close']),
            'volume': float(last['Volume']),
            'time': df.index[-1],
            'open_today': float(df.iloc[0]['Open']) if len(df) > 0 else None,
            'high_today': float(df['High'].max()),
            'low_today': float(df['Low'].min()),
            'cumulative_volume': float(df['Volume'].sum()),
        }
    except Exception:
        return None


def compute_vwap(df_intraday: pd.DataFrame) -> Optional[pd.Series]:
    """從 intraday df 算 VWAP（累計平均）"""
    if df_intraday is None or len(df_intraday) == 0: return None
    typical = (df_intraday['High'] + df_intraday['Low'] + df_intraday['Close']) / 3
    cum_vp = (typical * df_intraday['Volume']).cumsum()
    cum_v = df_intraday['Volume'].cumsum()
    vwap = cum_vp / cum_v
    return vwap


# ────────────────────────────────────────────────────────────────
# Alpaca hook（將來啟用）
# ────────────────────────────────────────────────────────────────

def _alpaca_available() -> bool:
    return bool(os.environ.get('ALPACA_API_KEY') and
                 os.environ.get('ALPACA_SECRET_KEY'))


def load_intraday_alpaca(ticker: str, interval: str = '5Min',
                          days: int = 2) -> Optional[pd.DataFrame]:
    """Alpaca real-time data fetch（待啟用）

    需 pip install alpaca-py，並設環境變數：
        ALPACA_API_KEY=...
        ALPACA_SECRET_KEY=...
    """
    if not _alpaca_available():
        return None
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        from datetime import datetime, timedelta

        client = StockHistoricalDataClient(
            api_key=os.environ['ALPACA_API_KEY'],
            secret_key=os.environ['ALPACA_SECRET_KEY'],
        )
        tf_map = {
            '1Min': TimeFrame.Minute,
            '5Min': TimeFrame(5, TimeFrameUnit.Minute),
            '15Min': TimeFrame(15, TimeFrameUnit.Minute),
            '1Hour': TimeFrame.Hour,
        }
        tf = tf_map.get(interval, TimeFrame(5, TimeFrameUnit.Minute))
        end = datetime.utcnow()
        start = end - timedelta(days=days)
        req = StockBarsRequest(symbol_or_symbols=ticker, timeframe=tf,
                                start=start, end=end)
        bars = client.get_stock_bars(req).df
        if bars.empty: return None
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.xs(ticker, level=0)
        bars = bars.rename(columns={
            'open': 'Open', 'high': 'High', 'low': 'Low',
            'close': 'Close', 'volume': 'Volume',
        })
        return bars
    except Exception:
        return None
