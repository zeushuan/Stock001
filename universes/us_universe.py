"""US 股票宇宙管理

提供三個層級的宇宙：
- SP500: S&P 500 全成份股
- RUSSELL1000: 較廣的中大型股池
- LIQUID_3000: 過去 50 日平均日成交額 > $10M 的股票

宇宙快照存到 universes/snapshots/<universe>_<YYYY-MM>.parquet，避免 survivorship bias。
"""
import os
import glob
from datetime import datetime
from typing import List, Optional

import pandas as pd
import numpy as np


SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), 'snapshots')
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# ────────────────────────────────────────────────────────────────
# Hard-coded 核心成份股清單（最近版本，當 snapshot 不存在時用）
# ────────────────────────────────────────────────────────────────

# S&P 500 樣本（精選 50 大流動性）— 完整 500 檔請從 data_cache 動態抓
SP500_CORE = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
    'JPM', 'V', 'JNJ', 'WMT', 'PG', 'MA', 'HD', 'CVX', 'AVGO', 'LLY',
    'ABBV', 'MRK', 'PEP', 'KO', 'BAC', 'COST', 'TMO', 'ORCL', 'DIS',
    'ADBE', 'NFLX', 'WFC', 'CSCO', 'MCD', 'CRM', 'ABT', 'ACN', 'AMD',
    'LIN', 'DHR', 'NEE', 'TXN', 'NKE', 'PM', 'BMY', 'COP', 'HON',
    'UNH', 'IBM', 'QCOM', 'INTC', 'INTU',
]

# 用戶關注主題標的（從指示書 §10 整合）
EDDY_THEMES = {
    'AI_storage': ['MU', 'SNDK', 'STX', 'WDC'],
    'AI_energy':  ['CEG', 'VST', 'GEV', 'PWR', 'OKLO', 'BE', 'CCJ'],
}


def get_universe(name: str = 'LIQUID_3000', as_of: Optional[str] = None,
                  cache_dir: str = 'data_cache') -> List[str]:
    """取得指定宇宙的 ticker 清單

    Args:
        name: 'SP500' / 'RUSSELL1000' / 'LIQUID_3000' / 'ALL_US'
        as_of: 日期 'YYYY-MM-DD'，若指定且有 snapshot 則用 snapshot
        cache_dir: data_cache 路徑

    Returns:
        list of tickers
    """
    # 1. 試讀 snapshot
    if as_of:
        snap = _load_snapshot(name, as_of)
        if snap is not None:
            return snap

    # 2. 動態建構
    if name == 'SP500':
        return list(SP500_CORE)

    if name == 'RUSSELL1000':
        # 沒有真實 R1000 清單時，用 LIQUID_3000 的前 1000 檔當代理
        liquid = _build_liquid_universe(cache_dir, min_dollar_vol_50d=5e6,
                                          max_tickers=1000)
        return liquid

    if name == 'LIQUID_3000':
        return _build_liquid_universe(cache_dir, min_dollar_vol_50d=1e7)

    if name == 'ALL_US':
        # 全部 data_cache 中的美股 ticker
        return _all_us_tickers(cache_dir)

    # Eddy 主題清單
    if name == 'EDDY_AI_STORAGE':
        return EDDY_THEMES['AI_storage']
    if name == 'EDDY_AI_ENERGY':
        return EDDY_THEMES['AI_energy']
    if name == 'EDDY_ALL':
        return EDDY_THEMES['AI_storage'] + EDDY_THEMES['AI_energy']

    raise ValueError(f'未知的 universe: {name}')


def get_theme_for_ticker(ticker: str) -> Optional[str]:
    """回傳該 ticker 對應 Eddy 的主題分類，若不屬於任何主題回傳 None"""
    for theme, ticks in EDDY_THEMES.items():
        if ticker in ticks:
            return theme
    return None


def _all_us_tickers(cache_dir: str) -> List[str]:
    """從 data_cache 抓全部美股 ticker（純大寫字母）"""
    if not os.path.isdir(cache_dir):
        return []
    files = glob.glob(os.path.join(cache_dir, '*.parquet'))
    us = [os.path.basename(f).replace('.parquet', '')
          for f in files
          if os.path.basename(f).replace('.parquet', '').isalpha()
          and os.path.basename(f).replace('.parquet', '').isupper()]
    return sorted(us)


def _build_liquid_universe(cache_dir: str, min_dollar_vol_50d: float = 1e7,
                             max_tickers: int = 3000) -> List[str]:
    """從 data_cache 算流動性，過濾出符合門檻的 ticker"""
    us = _all_us_tickers(cache_dir)
    if not us:
        return list(SP500_CORE)

    qualified = []
    for tk in us:
        fp = os.path.join(cache_dir, f'{tk}.parquet')
        try:
            df = pd.read_parquet(fp)
            if len(df) < 50: continue
            # 取最近 50 日的 Close × Volume 平均
            recent = df.tail(50)
            if 'Close' not in recent.columns or 'Volume' not in recent.columns:
                continue
            dollar_vol = float((recent['Close'] * recent['Volume']).mean())
            if dollar_vol >= min_dollar_vol_50d:
                qualified.append((tk, dollar_vol))
        except Exception:
            continue

    # 按流動性排序，取前 max_tickers
    qualified.sort(key=lambda x: -x[1])
    return [tk for tk, _ in qualified[:max_tickers]]


def _snapshot_path(name: str, as_of: str) -> str:
    """產生 snapshot 檔案路徑"""
    month = as_of[:7]  # YYYY-MM
    return os.path.join(SNAPSHOT_DIR, f'{name}_{month}.parquet')


def _load_snapshot(name: str, as_of: str) -> Optional[List[str]]:
    path = _snapshot_path(name, as_of)
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    return df['ticker'].tolist()


def save_snapshot(name: str, as_of: str, tickers: List[str]) -> str:
    """儲存當月宇宙 snapshot（避免 survivorship bias）"""
    path = _snapshot_path(name, as_of)
    df = pd.DataFrame({'ticker': tickers, 'snapshot_date': as_of})
    df.to_parquet(path)
    return path
