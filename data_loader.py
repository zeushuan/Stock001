"""
資料快取層（v8 優化基礎建設）

功能：
  1. 個股 OHLC + 技術指標（calc_ind 後）儲存為 parquet
  2. 首次：批次 yf.download → calc_ind → 寫 parquet
  3. 後續：直接讀 parquet（~10ms/檔，1263 檔約 10 秒讀完）
  4. 索引指標一次計算，所有變體共用 → 不重複算 calc_ind

效能：
  原本：每次完整跑 ~3.4 分鐘（含下載 + 算指標）
  快取後：首次同樣 3.4 分鐘；之後僅需 ~30 秒（純策略邏輯）
"""
import os
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import numpy as np

import backtest_all as bt

CACHE_DIR = Path(__file__).parent / "data_cache"
CACHE_DIR.mkdir(exist_ok=True)

# 預設快取有效天數（隔日重新下載當日資料）
DEFAULT_MAX_AGE_HOURS = 12


def cache_path(ticker: str) -> Path:
    return CACHE_DIR / f"{ticker}.parquet"


def is_cache_fresh(ticker: str, max_age_hours: float = DEFAULT_MAX_AGE_HOURS) -> bool:
    p = cache_path(ticker)
    if not p.exists():
        return False
    age_h = (datetime.now() -
             datetime.fromtimestamp(p.stat().st_mtime)).total_seconds() / 3600
    return age_h < max_age_hours


def save_to_cache(ticker: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    p = cache_path(ticker)
    try:
        df.to_parquet(p, compression='snappy')
    except Exception as e:
        print(f"  [cache] {ticker} save fail: {e}")


def load_from_cache(ticker: str):
    p = cache_path(ticker)
    if not p.exists():
        return None
    try:
        return pd.read_parquet(p)
    except Exception as e:
        print(f"  [cache] {ticker} load fail: {e}")
        return None


def fetch_one(ticker: str):
    """單檔下載 + calc_ind"""
    df = bt.download(ticker)
    if df is None or df.empty or "Close" not in df.columns:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    try:
        df = bt.calc_ind(df)
        save_to_cache(ticker, df)
        return df
    except Exception:
        return None


def get_data(ticker: str, force_refresh: bool = False):
    """單入口：先讀快取，沒有才下載"""
    if not force_refresh and is_cache_fresh(ticker):
        df = load_from_cache(ticker)
        if df is not None:
            return df
    return fetch_one(ticker)


def batch_get_all(tickers: list, force_refresh: bool = False,
                  download_workers: int = 10, batch_size: int = 80,
                  verbose: bool = True):
    """
    批次取得多支股票資料：
      - 已快取且新鮮 → 直接讀
      - 未快取 → 用 backtest_tw_all 的 batch_download
      回傳 {ticker: df_with_indicators}
    """
    cached = {}
    to_fetch = []

    # 第一步：盤點已快取的
    for tk in tickers:
        if not force_refresh and is_cache_fresh(tk):
            df = load_from_cache(tk)
            if df is not None:
                cached[tk] = df
                continue
        to_fetch.append(tk)

    if verbose:
        print(f"  [快取] 命中 {len(cached)}/{len(tickers)} 檔，需下載 {len(to_fetch)} 檔")

    if not to_fetch:
        return cached

    # 第二步：批次下載未快取的
    from backtest_tw_all import batch_download

    n_batches = (len(to_fetch) + batch_size - 1) // batch_size
    for bi in range(n_batches):
        batch = to_fetch[bi * batch_size:(bi + 1) * batch_size]
        if verbose:
            print(f"  [下載] 批次 {bi+1}/{n_batches}：{len(batch)} 檔...", flush=True)
        try:
            df_map = batch_download(batch)
        except Exception as ex:
            print(f"  [下載失敗] {ex}")
            continue

        # 每支股票算指標 + 存快取
        with ThreadPoolExecutor(max_workers=download_workers) as ex:
            def proc(item):
                tk, raw = item
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        raw.columns = raw.columns.get_level_values(0)
                    df = bt.calc_ind(raw)
                    save_to_cache(tk, df)
                    return tk, df
                except Exception:
                    return tk, None

            for tk, df in ex.map(proc, df_map.items()):
                if df is not None and not df.empty:
                    cached[tk] = df

    return cached


def cache_info():
    """檢查快取狀態"""
    files = list(CACHE_DIR.glob("*.parquet"))
    if not files:
        print("快取為空")
        return
    total_size = sum(f.stat().st_size for f in files)
    ages = [(datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)).total_seconds() / 3600
            for f in files]
    print(f"快取目錄：{CACHE_DIR}")
    print(f"  檔案數：{len(files)}")
    print(f"  總大小：{total_size / 1024 / 1024:.1f} MB")
    print(f"  最新：{min(ages):.1f}h 前   最舊：{max(ages):.1f}h 前")


def purge():
    """清除所有快取"""
    files = list(CACHE_DIR.glob("*.parquet"))
    for f in files:
        f.unlink()
    print(f"已清除 {len(files)} 個快取檔")


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == 'info':
            cache_info()
        elif cmd == 'purge':
            purge()
        else:
            print("用法：python data_loader.py [info|purge]")
    else:
        cache_info()
