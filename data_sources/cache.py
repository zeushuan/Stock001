"""data_sources.cache — 13F / Form 4 結果本地快取
======================================================

13F 規格 45 天延遲（規格 line 136），所以週度更新即可，
不需每次 CLI 都重打 SEC API（每基金 ~3-5 秒 × 7 家 = 20-30 秒）。

設計:
  - Pickle 檔案存 `data_cache/edgar_*.pkl`
  - Key by (fund_ticker, form_type) → value 是 fetch_13f_compare 的 dict
  - TTL 預設 7 天（>= 13F 週度更新節奏）
  - 直接寫進 .gitignore（資料快取不入版控）

公開 API:
  - cache_get(key, ttl_days=7)              → Optional[dict]  快取命中回值，否則 None
  - cache_set(key, value)                    → None           寫入
  - cached_13f_compare(fund, ttl_days=7)     → dict           wrap fetch_13f_compare
"""
from __future__ import annotations

import logging
import pickle
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger("stock001.data_sources.cache")

_CACHE_DIR = Path(__file__).parent.parent / 'data_cache'
_CACHE_DIR.mkdir(exist_ok=True)


def _key_to_path(key: str) -> Path:
    safe = key.replace('/', '_').replace(':', '_').replace(' ', '_')
    return _CACHE_DIR / f'{safe}.pkl'


def cache_get(key: str, ttl_days: float = 7) -> Optional[dict]:
    """命中返回 value；過期 / 不存在 / 解析錯誤回 None。"""
    p = _key_to_path(key)
    if not p.exists():
        return None
    try:
        age_s = time.time() - p.stat().st_mtime
        if age_s > ttl_days * 86400:
            return None
        with p.open('rb') as f:
            return pickle.load(f)
    except Exception as exc:
        log.debug("[cache_get/%s] %s", key, exc)
        return None


def cache_set(key: str, value: dict) -> None:
    """寫入；失敗只 log 不爆。"""
    try:
        with _key_to_path(key).open('wb') as f:
            pickle.dump(value, f)
    except Exception as exc:
        log.debug("[cache_set/%s] %s", key, exc)


def cached_13f_compare(fund: str, ttl_days: float = 7) -> dict:
    """fetch_13f_compare 的快取版本。"""
    key = f'13f_compare__{fund}'
    cached = cache_get(key, ttl_days=ttl_days)
    if cached is not None:
        return cached
    from .edgar_13f import fetch_13f_compare
    result = fetch_13f_compare(fund)
    if 'error' not in result:
        cache_set(key, result)
    return result


def cached_13f_holdings(fund: str, n: int = 1, ttl_days: float = 7) -> list[dict]:
    """fetch_13f_holdings 的快取版本（用於 cost distance 計算）。"""
    key = f'13f_holdings__{fund}__n{n}'
    cached = cache_get(key, ttl_days=ttl_days)
    if cached is not None:
        return cached
    from .edgar_13f import fetch_13f_holdings
    result = fetch_13f_holdings(fund, n=n)
    if result:
        cache_set(key, result)
    return result


def cache_clear(pattern: Optional[str] = None) -> int:
    """清快取。pattern=None 清全部；給字串就只清檔名含此字串者。"""
    n = 0
    for p in _CACHE_DIR.glob('*.pkl'):
        if pattern is None or pattern in p.name:
            try:
                p.unlink(); n += 1
            except Exception:
                pass
    return n
