"""sympathy 模組共用的 data loader

優先順序：
  0. 注入的 df_dict（unified scan 共用，最快）
  1. data_loader.load_from_cache（local data_cache）
  2. yfinance fallback（cache miss 才下載）
"""
import os
import pandas as pd
import numpy as np
from typing import Optional, Dict


_CACHE_DIR = '.cache/sympathy'
os.makedirs(_CACHE_DIR, exist_ok=True)

# 🆕 v9.25.6：可注入外部 df_dict（unified_cron_scan 共用 fetch 結果）
_INJECTED_DF_DICT: Dict[str, pd.DataFrame] = {}


def set_injected_data(df_dict: Dict[str, pd.DataFrame]):
    """注入預抓的 df_dict（unified scan 用）。Key 為 ticker 名稱。"""
    global _INJECTED_DF_DICT
    _INJECTED_DF_DICT = df_dict or {}


def clear_injected_data():
    global _INJECTED_DF_DICT
    _INJECTED_DF_DICT = {}


def _lookup_injected(ticker: str) -> Optional[pd.DataFrame]:
    """在 injected df_dict 中找 ticker（容忍 .TW 後綴差異）"""
    if not _INJECTED_DF_DICT: return None
    # 直接命中
    if ticker in _INJECTED_DF_DICT:
        return _INJECTED_DF_DICT[ticker]
    # 試 strip .TW
    if ticker.endswith('.TW'):
        bare = ticker[:-3]
        if bare in _INJECTED_DF_DICT:
            return _INJECTED_DF_DICT[bare]
    else:
        # 試 加 .TW
        wt = ticker + '.TW'
        if wt in _INJECTED_DF_DICT:
            return _INJECTED_DF_DICT[wt]
    return None


def load_history(ticker: str, lookback_days: int = 90,
                  as_of_date: Optional[pd.Timestamp] = None) -> Optional[pd.DataFrame]:
    """取得 ticker 的 OHLCV 歷史，截至 as_of_date

    回傳 DataFrame[Open/High/Low/Close/Volume]，tz-naive，由舊到新排序
    若資料不足或載入失敗回 None
    """
    df = None
    # Path 0: injected df_dict（unified scan 共用）
    df = _lookup_injected(ticker)
    # Path A: data_cache
    if df is None:
        try:
            import data_loader as _dl
            df = _dl.load_from_cache(ticker)
        except Exception:
            df = None

    # Path B: yfinance fallback（silence stderr，部分 ticker 會 404）
    if df is None or len(df) < lookback_days:
        try:
            import yfinance as yf, io, contextlib, logging
            with contextlib.redirect_stderr(io.StringIO()), \
                 contextlib.redirect_stdout(io.StringIO()):
                logging.getLogger('yfinance').setLevel(logging.CRITICAL)
                df = yf.download(ticker,
                                  period=f'{max(lookback_days + 20, 250)}d',
                                  interval='1d',
                                  progress=False, auto_adjust=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
        except Exception:
            return None

    if df is None or len(df) == 0:
        return None

    # 正規化 index
    df = df.copy()
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df.index = df.index.normalize()
    df = df.sort_index()

    # 切到 as_of_date
    if as_of_date is not None:
        if isinstance(as_of_date, str):
            as_of_date = pd.Timestamp(as_of_date)
        if as_of_date.tz is not None:
            as_of_date = as_of_date.tz_localize(None)
        df = df.loc[:as_of_date]

    if len(df) < 20:
        return None
    return df


def batch_load(tickers, lookback_days=90, as_of_date=None):
    """批次載入；回傳 dict {ticker: df}"""
    out = {}
    for t in tickers:
        df = load_history(t, lookback_days, as_of_date)
        if df is not None:
            out[t] = df
    return out
