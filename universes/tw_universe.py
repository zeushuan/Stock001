"""TW 股票宇宙管理

提供三個層級：
- TW50: 台灣 50 成份股
- TW0050_0056: 主要 ETF 成份股聯集
- LIQUID_TW: 過去 50 日平均成交額 > $50M TWD 的股票
"""
import os
import glob
from typing import List, Optional

import pandas as pd


SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), 'snapshots')
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# 台灣 50 成份股（精選；建議定期從證交所更新）
TW50_CONSTITUENTS = [
    '2330', '2317', '2308', '2454', '2882', '2891', '2412', '2881', '2382',
    '2884', '2885', '2002', '2886', '2880', '3045', '2207', '1216', '2887',
    '2892', '5880', '2890', '2912', '2357', '2303', '3008', '2379', '2474',
    '2395', '1303', '1301', '6505', '5871', '4938', '1102', '2880', '2883',
    '2105', '2027', '2207', '5347', '3034', '2356', '2618', '2105', '3702',
]

# 0050 + 0056 + 主要主題 ETF 持股聯集（樣本）
TW0050_0056_CORE = list(set(TW50_CONSTITUENTS + [
    '2603', '2609', '2615',  # 航運
    '3231', '6669', '6488',  # 半導體 / IC 設計
    '6182', '5269', '4961',  # AI
    '2492', '3406',          # 其他主流
]))


def get_universe(name: str = 'LIQUID_TW', as_of: Optional[str] = None,
                  cache_dir: str = 'data_cache') -> List[str]:
    """取得 TW universe"""
    if as_of:
        snap = _load_snapshot(name, as_of)
        if snap is not None:
            return snap

    if name == 'TW50':
        return [t + '.TW' for t in TW50_CONSTITUENTS]

    if name == 'TW0050_0056':
        return [t + '.TW' for t in TW0050_0056_CORE]

    if name == 'LIQUID_TW':
        return _build_liquid_universe(cache_dir, min_dollar_vol_50d=5e7,
                                        max_tickers=500)

    if name == 'ALL_TW':
        return _all_tw_tickers(cache_dir)

    raise ValueError(f'未知的 TW universe: {name}')


def _all_tw_tickers(cache_dir: str) -> List[str]:
    """從 data_cache 抓全部 TW ticker"""
    if not os.path.isdir(cache_dir):
        return []
    files = glob.glob(os.path.join(cache_dir, '*.parquet'))
    tw = []
    for f in files:
        n = os.path.basename(f).replace('.parquet', '')
        # TW: 4-7 位數字（含 ETF 00xxxx, 6 位 OTC）
        if n.replace('.TW', '').replace('.TWO', '').isdigit():
            if '.TW' not in n:
                n = n + '.TW'
            tw.append(n)
    return sorted(set(tw))


def _build_liquid_universe(cache_dir: str, min_dollar_vol_50d: float = 5e7,
                             max_tickers: int = 500) -> List[str]:
    """流動性過濾"""
    tw = _all_tw_tickers(cache_dir)
    qualified = []
    for tk in tw:
        # 找對應的 cache 檔（可能不帶 .TW）
        fp = os.path.join(cache_dir, f'{tk}.parquet')
        if not os.path.exists(fp):
            fp = os.path.join(cache_dir, f'{tk.replace(".TW","")}.parquet')
        if not os.path.exists(fp):
            continue
        try:
            df = pd.read_parquet(fp)
            if len(df) < 50: continue
            recent = df.tail(50)
            dollar_vol = float((recent['Close'] * recent['Volume']).mean())
            if dollar_vol >= min_dollar_vol_50d:
                qualified.append((tk, dollar_vol))
        except Exception:
            continue
    qualified.sort(key=lambda x: -x[1])
    return [tk for tk, _ in qualified[:max_tickers]]


def _snapshot_path(name: str, as_of: str) -> str:
    month = as_of[:7]
    return os.path.join(SNAPSHOT_DIR, f'{name}_{month}.parquet')


def _load_snapshot(name: str, as_of: str) -> Optional[List[str]]:
    path = _snapshot_path(name, as_of)
    if not os.path.exists(path): return None
    df = pd.read_parquet(path)
    return df['ticker'].tolist()


def save_snapshot(name: str, as_of: str, tickers: List[str]) -> str:
    path = _snapshot_path(name, as_of)
    df = pd.DataFrame({'ticker': tickers, 'snapshot_date': as_of})
    df.to_parquet(path)
    return path
