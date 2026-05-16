"""Intraday Data Layer — Stock001 v9.29
========================================

統一資料取得介面。TW 走 fugle_connector，US 走 yfinance。
共用 intraday_cache/{ticker}_{freq}.parquet 快取。

對外 API：
    get_intraday(ticker, tf='5m', market='auto', refresh=False) -> DataFrame
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import pandas as pd
import numpy as np

from intraday.config import TIMEFRAMES, get_tf_config

CACHE_DIR = Path(__file__).parent.parent / 'intraday_cache'
CACHE_DIR.mkdir(exist_ok=True)


def _detect_market(ticker: str) -> str:
    """簡單判斷 ticker 屬於哪個市場"""
    t = ticker.upper()
    # TW: 4 位數 (2330) 或 4 位數+L/R (00631L) 或 .TW
    if '.TW' in t:
        return 'tw'
    pure = t.replace('.TW', '')
    if pure.isdigit() and 4 <= len(pure) <= 6:
        return 'tw'
    if pure[:4].isdigit() and len(pure) <= 6:  # ETF 含 L/R/K/U 字尾
        return 'tw'
    # 其它視為美股
    return 'us'


def _cache_path(ticker: str, tf: str) -> Path:
    pure = ticker.replace('.TW', '').upper()
    return CACHE_DIR / f"{pure}_{tf}.parquet"


def _is_cache_fresh(path: Path, ttl_seconds: int) -> bool:
    if not path.exists():
        return False
    age = (datetime.now().timestamp() - path.stat().st_mtime)
    return age < ttl_seconds


def _save_cache(path: Path, df: pd.DataFrame):
    try:
        df.to_parquet(path, compression='snappy')
    except Exception as e:
        print(f"  [intraday cache] {path.name} save fail: {e}")


def _load_cache(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _fetch_yfinance(ticker: str, tf: str, market: str) -> Optional[pd.DataFrame]:
    """yfinance 抓取 — US 用裸 ticker、TW 加 .TW"""
    try:
        import yfinance as yf
    except ImportError:
        return None
    cfg = get_tf_config(tf)
    sym = ticker if market == 'us' else f"{ticker.replace('.TW','')}.TW"
    try:
        df = yf.download(sym, period=cfg.yf_max_period,
                         interval=cfg.yf_interval,
                         progress=False, auto_adjust=False, threads=False)
        if df is None or df.empty:
            return None
        # 攤平 multi-index
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        # 移除 tz info（統一）
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        # 只保留 OHLCV
        keep = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in df.columns]
        if not keep:
            return None
        df = df[keep].dropna(how='all')
        if len(df) == 0:
            return None
        return df
    except Exception as e:
        print(f"  [intraday] yfinance fail {sym} {tf}: {type(e).__name__}: {str(e)[:60]}")
        return None


def _fetch_fugle(ticker: str, tf: str) -> Optional[pd.DataFrame]:
    """走 fugle_connector（TW 專用、有完整歷史）"""
    try:
        from fugle_connector import get_minute_candles, _has_fugle
        if not _has_fugle():
            return None
        cfg = get_tf_config(tf)
        # fugle_connector 的 freq 跟 yfinance 一致（1m/5m/15m/30m/60m/1d）
        df = get_minute_candles(ticker.replace('.TW', ''),
                                  freq=cfg.fugle_freq, use_cache=False)
        return df if df is not None and len(df) > 0 else None
    except Exception as e:
        print(f"  [intraday] fugle fail {ticker} {tf}: {type(e).__name__}: {str(e)[:60]}")
        return None


def get_intraday(ticker: str, tf: str = '5m',
                  market: str = 'auto',
                  refresh: bool = False) -> Optional[pd.DataFrame]:
    """取得 intraday 資料

    Args:
        ticker: 股票代號（TW: '2330' 或 'AAPL'）
        tf: '1m' / '5m' / '15m' / '30m' / '1h' / '1d'
        market: 'tw' / 'us' / 'auto'（自動判斷）
        refresh: 強制重抓（跳過快取）

    Returns:
        DataFrame [Open, High, Low, Close, Volume]，index=DatetimeIndex
        失敗回 None
    """
    if tf not in TIMEFRAMES:
        raise ValueError(f"未支援 tf '{tf}'，可用 {list(TIMEFRAMES.keys())}")

    if market == 'auto':
        market = _detect_market(ticker)

    cfg = get_tf_config(tf)
    cpath = _cache_path(ticker, tf)

    # 1. 快取（如果新鮮）
    if not refresh and _is_cache_fresh(cpath, cfg.cache_ttl_seconds):
        df = _load_cache(cpath)
        if df is not None and len(df) > 0:
            return df

    # 2. TW 優先試 Fugle（如果有 API key）
    df = None
    if market == 'tw':
        df = _fetch_fugle(ticker, tf)

    # 3. yfinance fallback
    if df is None or len(df) == 0:
        df = _fetch_yfinance(ticker, tf, market)

    # 4. 仍然失敗 → 試讀過期快取（總比沒有好）
    if df is None or len(df) == 0:
        df = _load_cache(cpath)
        if df is not None and len(df) > 0:
            print(f"  [intraday] {ticker} {tf} 用過期快取（fresh fetch 失敗）")
        return df

    # 5. 成功 → 寫快取
    _save_cache(cpath, df)
    return df


def get_session_bars_today(ticker: str, tf: str = '5m',
                            market: str = 'auto') -> Optional[pd.DataFrame]:
    """只取「今日 session」的 bars（用於 intraday-only 分析）"""
    df = get_intraday(ticker, tf, market)
    if df is None or len(df) == 0:
        return None
    last_date = df.index[-1].date()
    today_df = df[df.index.date == last_date]
    return today_df if len(today_df) > 0 else None


def market_info(ticker: str) -> dict:
    """回傳 ticker 的市場 metadata"""
    market = _detect_market(ticker)
    return {
        'ticker': ticker.replace('.TW', '').upper(),
        'market': market,
        'yf_symbol': ticker if market == 'us' else f"{ticker.replace('.TW','')}.TW",
        'session_hours': '09:00-13:30 (TW)' if market == 'tw' else '09:30-16:00 ET (US)',
    }
