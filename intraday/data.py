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


def _fetch_yfinance(ticker: str, tf: str, market: str,
                      prepost: bool = True) -> Optional[pd.DataFrame]:
    """yfinance 抓取 — US 用裸 ticker、TW 加 .TW
    🆕 v9.32：prepost=True 預設開啟（含夜盤 pre/post-market）
    🆕 v9.32.1：period fallback 鏈 — 新上市股 730d 會 fail，自動降到 60d
    """
    try:
        import yfinance as yf
    except ImportError:
        return None
    cfg = get_tf_config(tf)
    sym = ticker if market == 'us' else f"{ticker.replace('.TW','')}.TW"
    # TW 股票沒有夜盤（期貨才有），prepost 對 .TW stock 等於 noop
    use_prepost = prepost and market == 'us' and tf != '1d'

    # period fallback 鏈：依 TF 不同設定多個嘗試 period
    # 新上市股票如 NVD（GraniteShares 2x Short NVDA）歷史 < 730d 會直接 fail
    period_chain_by_tf = {
        '1m':  ['7d'],                                  # yf 硬限制 7d
        '5m':  ['60d', '30d', '14d'],
        '15m': ['60d', '30d', '14d'],
        '30m': ['60d', '30d', '14d'],
        '1h':  ['730d', '365d', '180d', '60d'],         # 1h 最易 fail
        '1d':  ['10y', '5y', '2y', '1y', '6mo', '3mo'],
    }
    periods_to_try = period_chain_by_tf.get(tf, [cfg.yf_max_period])

    last_err = None
    for period in periods_to_try:
        try:
            df = yf.download(sym, period=period,
                             interval=cfg.yf_interval,
                             progress=False, auto_adjust=False, threads=False,
                             prepost=use_prepost)
            if df is None or df.empty:
                continue
            # 攤平 multi-index
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            # 移除 tz info（統一）
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            # 只保留 OHLCV
            keep = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in df.columns]
            if not keep:
                continue
            df = df[keep].dropna(how='all')
            if len(df) == 0:
                continue
            # 成功
            if period != periods_to_try[0]:
                print(f"  [intraday] {sym} {tf}: 用 fallback period={period} 抓到 {len(df)} bars")
            return df
        except Exception as e:
            last_err = e
            continue

    if last_err:
        print(f"  [intraday] yfinance 全部 fallback 都失敗 {sym} {tf}: {type(last_err).__name__}: {str(last_err)[:80]}")
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
                  refresh: bool = False,
                  prepost: bool = True) -> Optional[pd.DataFrame]:
    """取得 intraday 資料

    Args:
        ticker: 股票代號（TW: '2330' 或 'AAPL'）
        tf: '1m' / '5m' / '15m' / '30m' / '1h' / '1d'
        market: 'tw' / 'us' / 'auto'（自動判斷）
        refresh: 強制重抓（跳過快取）
        prepost: 是否含夜盤（pre/post-market）— 預設 True（v9.32）

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

    # 2. TW 優先試 Fugle（如果有 API key）— TW 股票沒夜盤，不需 prepost
    df = None
    if market == 'tw':
        df = _fetch_fugle(ticker, tf)
        # 🆕 v9.33：Fugle 預設只抓近 30 天，1d 通常給 20 bars 不夠用 → fallback
        # 對其他 TF：若給的少於該 TF 合理量也要 fallback
        _min_bars = {
            '1m': 100, '5m': 200, '15m': 100,
            '30m': 100, '1h': 100, '1d': 200,
        }
        if df is not None and len(df) < _min_bars.get(tf, 100):
            print(f"  [intraday] {ticker} {tf}: Fugle 只給 {len(df)} bars "
                  f"(< {_min_bars.get(tf, 100)}) → fallback yfinance")
            df = None

    # 3. yfinance fallback（US 用 prepost=True 抓含夜盤）
    if df is None or len(df) == 0:
        df = _fetch_yfinance(ticker, tf, market, prepost=prepost)

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
