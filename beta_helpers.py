"""個股 vs 大盤 Beta 計算（v9.27）

Beta = Cov(stock_returns, market_returns) / Var(market_returns)

慣例：
  - Beta = 1.0 → 與大盤同步
  - Beta > 1.5 → 高 Beta（典型動能 / 飆股）
  - Beta > 2.0 → 超高 Beta（爆炸性）
  - Beta < 0.7 → 低 Beta（防禦性）
  - Beta < 0   → 反向（避險資產）

預設 lookback 60 個交易日（3 個月）— 兼顧敏感度與穩定性。
"""
import numpy as np
import pandas as pd
from typing import Optional, Dict


def compute_beta(stock_close: pd.Series,
                  index_close: pd.Series,
                  lookback: int = 60) -> Optional[float]:
    """計算個股對大盤的 beta

    Args:
        stock_close: 個股收盤價序列
        index_close: 大盤指數收盤價序列
        lookback: 回看天數（預設 60d）

    Returns:
        float beta（None 若資料不足）
    """
    if stock_close is None or index_close is None: return None
    if len(stock_close) < lookback + 1 or len(index_close) < lookback + 1:
        return None

    # 對齊日期
    try:
        s = pd.Series(stock_close).copy()
        i = pd.Series(index_close).copy()
        if hasattr(s.index, 'tz') and s.index.tz is not None:
            s.index = s.index.tz_localize(None)
        if hasattr(i.index, 'tz') and i.index.tz is not None:
            i.index = i.index.tz_localize(None)
        common = s.index.intersection(i.index)
        if len(common) < lookback + 1: return None
        s = s.loc[common].tail(lookback + 1)
        i = i.loc[common].tail(lookback + 1)
    except Exception:
        return None

    # 算日報酬
    s_ret = s.pct_change().dropna().values
    i_ret = i.pct_change().dropna().values

    n = min(len(s_ret), len(i_ret))
    if n < lookback: return None
    s_ret = s_ret[-n:]
    i_ret = i_ret[-n:]

    # 過濾極端值（單日 > 50%，通常是分割/錯誤資料）
    mask = (np.abs(s_ret) < 0.5) & (np.abs(i_ret) < 0.2)
    s_ret = s_ret[mask]
    i_ret = i_ret[mask]
    if len(s_ret) < lookback * 0.8:   # 過濾後剩太少不算
        return None

    var_i = float(np.var(i_ret, ddof=1))
    if var_i <= 0: return None
    cov = float(np.cov(s_ret, i_ret, ddof=1)[0, 1])
    beta = cov / var_i
    # 限制極端值
    if not np.isfinite(beta) or abs(beta) > 10:
        return None
    return round(beta, 2)


def classify_beta(beta: Optional[float]) -> str:
    """把 beta 分類為描述性字串"""
    if beta is None: return 'unknown'
    if beta < 0: return 'inverse'
    if beta < 0.7: return 'defensive'
    if beta < 1.2: return 'normal'
    if beta < 1.5: return 'elevated'
    if beta < 2.0: return 'high'
    return 'very_high'


def compute_beta_with_r2(stock_close: pd.Series,
                          index_close: pd.Series,
                          lookback: int = 60) -> Dict:
    """進階版：beta + R² + alpha

    R² > 0.5 才表示 beta 有意義（高度相關於大盤）
    Alpha 是超額報酬（每日 %）

    Returns:
      {'beta': float, 'r_squared': float, 'alpha_daily': float}
    """
    if stock_close is None or index_close is None: return {}
    try:
        s = pd.Series(stock_close).copy()
        i = pd.Series(index_close).copy()
        if hasattr(s.index, 'tz') and s.index.tz is not None:
            s.index = s.index.tz_localize(None)
        if hasattr(i.index, 'tz') and i.index.tz is not None:
            i.index = i.index.tz_localize(None)
        common = s.index.intersection(i.index)
        if len(common) < lookback + 1: return {}
        s = s.loc[common].tail(lookback + 1)
        i = i.loc[common].tail(lookback + 1)
    except Exception:
        return {}

    s_ret = s.pct_change().dropna().values
    i_ret = i.pct_change().dropna().values
    n = min(len(s_ret), len(i_ret))
    if n < lookback: return {}
    s_ret = s_ret[-n:]; i_ret = i_ret[-n:]

    mask = (np.abs(s_ret) < 0.5) & (np.abs(i_ret) < 0.2)
    s_ret = s_ret[mask]; i_ret = i_ret[mask]
    if len(s_ret) < lookback * 0.8: return {}

    var_i = float(np.var(i_ret, ddof=1))
    if var_i <= 0: return {}
    cov = float(np.cov(s_ret, i_ret, ddof=1)[0, 1])
    beta = cov / var_i
    alpha_daily = float(np.mean(s_ret) - beta * np.mean(i_ret))

    # R² = correlation^2
    if np.std(s_ret) <= 0 or np.std(i_ret) <= 0:
        r2 = 0.0
    else:
        corr = float(np.corrcoef(s_ret, i_ret)[0, 1])
        r2 = corr ** 2

    return {
        'beta': round(beta, 2) if np.isfinite(beta) and abs(beta) <= 10 else None,
        'r_squared': round(r2, 3),
        'alpha_daily': round(alpha_daily * 100, 4),  # %
    }
