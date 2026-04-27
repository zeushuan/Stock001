"""
Fugle (玉山證券富果) API connector
====================================
統一 minute bar 介面：
  - 有 FUGLE_API_KEY env：使用 Fugle 完整歷史 + 即時
  - 無 key：fallback 到 yfinance 7 日 1-min（限制大）

API 文件：https://developer.fugle.tw/

用法：
  from fugle_connector import get_minute_candles
  df = get_minute_candles('2330', start='2024-01-01', end='2024-12-31', freq='1m')

支援頻率：
  '1m' / '5m' / '15m' / '30m' / '60m' / '1d'

快取：
  intraday_cache/{ticker}_{freq}.parquet
"""
import os
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 讀 .env (簡易實作)
def _load_env():
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        for line in env_file.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#'): continue
            if '=' in line:
                k, _, v = line.partition('=')
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_env()

CACHE_DIR = Path(__file__).parent / 'intraday_cache'
CACHE_DIR.mkdir(exist_ok=True)

# Fugle API key 從環境變數讀
FUGLE_API_KEY = os.environ.get('FUGLE_API_KEY')


def _has_fugle() -> bool:
    """檢查 Fugle credentials 是否設定"""
    return bool(FUGLE_API_KEY) and FUGLE_API_KEY != 'YOUR_KEY_HERE'


# ─── Fugle 客戶端（lazy init）─────────────────────────────────
_fugle_client = None

def _get_fugle():
    global _fugle_client
    if _fugle_client is None and _has_fugle():
        try:
            from fugle_marketdata import RestClient
            _fugle_client = RestClient(api_key=FUGLE_API_KEY)
        except Exception as e:
            print(f"Fugle init 失敗：{e}")
            _fugle_client = None
    return _fugle_client


# ─── 統一介面 ──────────────────────────────────────────────────
def get_minute_candles(ticker: str, start: str = None, end: str = None,
                       freq: str = '1m', use_cache: bool = True) -> pd.DataFrame:
    """取得 K 棒資料。
    優先順序：cache → Fugle → yfinance fallback

    Args:
        ticker: 股票代號（不含 .TW）
        start/end: 'YYYY-MM-DD'
        freq: '1m'/'5m'/'15m'/'30m'/'60m'/'1d'

    Returns:
        DataFrame (index=Timestamp, cols=Open/High/Low/Close/Volume)
        失敗回 None
    """
    cache_path = CACHE_DIR / f"{ticker}_{freq}.parquet"

    # 1. 嘗試快取
    if use_cache and cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            # 過濾時間範圍
            if start: df = df[df.index >= start]
            if end:   df = df[df.index <= end]
            if len(df) > 0:
                return df
        except Exception:
            pass

    # 2. 嘗試 Fugle
    if _has_fugle():
        df = _fetch_fugle(ticker, start, end, freq)
        if df is not None and len(df) > 0:
            if use_cache:
                try: df.to_parquet(cache_path)
                except: pass
            return df

    # 3. yfinance fallback（僅 7 日歷史 1m）
    df = _fetch_yfinance(ticker, freq)
    if df is not None and len(df) > 0:
        if start: df = df[df.index >= start]
        if end:   df = df[df.index <= end]
    return df


def _fetch_fugle(ticker: str, start: str, end: str, freq: str) -> pd.DataFrame:
    """從 Fugle 取資料（自動 chunk 跨年區間）"""
    client = _get_fugle()
    if not client: return None
    try:
        tf_map = {
            '1m': '1', '5m': '5', '15m': '15', '30m': '30',
            '60m': '60', '1d': 'D'
        }
        timeframe = tf_map.get(freq, '1')

        # Fugle 限制每次請求 < 1 年；切成多段
        if start and end:
            s = datetime.strptime(start, '%Y-%m-%d')
            e = datetime.strptime(end, '%Y-%m-%d')
        else:
            # 預設取近期
            s = datetime.now() - timedelta(days=30)
            e = datetime.now()

        chunks = []
        cur_s = s
        while cur_s < e:
            cur_e = min(cur_s + timedelta(days=360), e)
            try:
                resp = client.stock.historical.candles(
                    symbol=ticker, timeframe=timeframe,
                    **{'from': cur_s.strftime('%Y-%m-%d'),
                       'to': cur_e.strftime('%Y-%m-%d')}
                )
                data = resp.get('data', [])
                if data: chunks.extend(data)
            except Exception as e_inner:
                print(f"  chunk {cur_s.date()}~{cur_e.date()} err: {str(e_inner)[:60]}")
            cur_s = cur_e + timedelta(days=1)

        if not chunks: return None
        df = pd.DataFrame(chunks)
        df['date'] = pd.to_datetime(df['date'])
        df = df.drop_duplicates(subset=['date']).set_index('date').sort_index()
        df = df.rename(columns={
            'open': 'Open', 'high': 'High', 'low': 'Low',
            'close': 'Close', 'volume': 'Volume',
        })
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]
    except Exception as e:
        print(f"Fugle 抓取失敗 ({ticker}): {e}")
        return None


def _fetch_yfinance(ticker: str, freq: str) -> pd.DataFrame:
    """yfinance fallback"""
    try:
        import yfinance as yf
        # yfinance interval map
        intv_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '60m': '60m', '1d': '1d',
        }
        intv = intv_map.get(freq, '1m')

        # yfinance 1m 只給 7 日，5-60m 給 60 日
        period_map = {
            '1m': '7d', '5m': '60d', '15m': '60d', '30m': '60d',
            '60m': '60d', '1d': '6y',
        }
        period = period_map.get(freq, '7d')

        sym = f"{ticker}.TW"
        df = yf.Ticker(sym).history(period=period, interval=intv,
                                     auto_adjust=True)
        if df is None or df.empty: return None
        df.index = pd.to_datetime(df.index)
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]
    except Exception as e:
        print(f"yfinance 抓取失敗 ({ticker}): {e}")
        return None


# ─── 工具：取最新即時 quote（盤中策略用）──────────────────────
def get_realtime_quote(ticker: str) -> dict:
    """取得最新即時報價（需 Fugle）"""
    client = _get_fugle()
    if not client:
        return {'error': 'Fugle credentials not set'}
    try:
        return client.stock.intraday.quote(symbol=ticker)
    except Exception as e:
        return {'error': str(e)}


# ─── 自我測試 ──────────────────────────────────────────────────
def status():
    """檢查 connector 狀態"""
    print("=" * 60)
    print("Fugle Connector Status")
    print("=" * 60)
    print(f"  FUGLE_API_KEY: {'✅ 已設定' if _has_fugle() else '❌ 未設定（將用 yfinance fallback）'}")
    print(f"  Cache dir: {CACHE_DIR} ({sum(1 for _ in CACHE_DIR.glob('*.parquet'))} 檔)")
    print(f"  yfinance: {'✅ 可用' if _try_yf_import() else '❌ 缺套件'}")
    if _has_fugle():
        try:
            client = _get_fugle()
            print(f"  Fugle client: ✅ 初始化成功")
        except Exception as e:
            print(f"  Fugle client: ❌ {e}")


def _try_yf_import() -> bool:
    try: import yfinance; return True
    except: return False


def smoke_test():
    """測試抓取（不用 Fugle key 也能跑）"""
    print("\n[Smoke test] 抓 2330 1-min 資料（最近 5 天）")
    df = get_minute_candles('2330', freq='1m')
    if df is None or df.empty:
        print("  ❌ 失敗（可能是市場休市/網路問題）")
        return
    print(f"  ✅ {len(df)} 筆資料")
    print(f"     範圍: {df.index.min()} ~ {df.index.max()}")
    print(f"     最後 3 筆:")
    print(df.tail(3).to_string())


if __name__ == '__main__':
    status()
    smoke_test()
