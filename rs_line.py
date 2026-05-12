"""TraderLion 風格的 RS Line（補完 v9.23.4）

Stock001 主要 RS 使用 `sepa_vcp.compute_rs_ratings`（IBD/Minervini 風格 percentile）。
本模組額外提供 RS Line（時間序列）API，給 Phase 1 / Phase 2 驗證使用。

RS Line 公式：
    rs_line(t) = stock_close(t) / index_close(t)

選項：
- 21 日 WMA 平滑（TraderLion 預設）
- 紫色點：RS Line 領先股價創新高（Stage 2 起飛訊號）

** 重要 docstring **：此函式只使用截至傳入日期（含）的資料，無 look-ahead bias。
"""
import numpy as np
import pandas as pd


def calculate_rs_line(stock: pd.Series, index: pd.Series,
                       smooth_wma: int = None) -> pd.Series:
    """計算 RS Line（Price / Index 比值）

    Args:
        stock: 股票收盤價序列（DatetimeIndex）
        index: 大盤指數收盤價序列（DatetimeIndex）
        smooth_wma: 可選 WMA 平滑期數（None = 不平滑）

    Returns:
        pd.Series: RS Line，index 為兩個輸入序列日期的交集

    Notes:
        - 自動處理日期對齊（intersection）
        - NaN 資料自動丟棄
        - 無 look-ahead bias（每個 t 只用 t 當日資料）
    """
    if not isinstance(stock, pd.Series) or not isinstance(index, pd.Series):
        raise TypeError('stock 和 index 必須是 pd.Series')
    if not isinstance(stock.index, pd.DatetimeIndex):
        raise TypeError('stock.index 必須是 DatetimeIndex')
    if not isinstance(index.index, pd.DatetimeIndex):
        raise TypeError('index.index 必須是 DatetimeIndex')

    # 對齊兩個序列的日期交集
    common_dates = stock.index.intersection(index.index)
    if len(common_dates) == 0:
        return pd.Series(dtype=float)

    s = stock.loc[common_dates].astype(float)
    i = index.loc[common_dates].astype(float)

    # 丟棄任一方為 NaN 或 0 的日期
    valid = (~s.isna()) & (~i.isna()) & (s > 0) & (i > 0)
    s = s[valid]
    i = i[valid]

    if len(s) == 0:
        return pd.Series(dtype=float)

    rs = s / i

    if smooth_wma is not None and smooth_wma > 1:
        rs = _wma(rs, smooth_wma)

    return rs


def _wma(series: pd.Series, period: int) -> pd.Series:
    """加權移動平均（最新值權重最高）"""
    weights = np.arange(1, period + 1)
    weights = weights / weights.sum()
    return series.rolling(window=period).apply(
        lambda x: np.dot(x, weights), raw=True
    )


def detect_rs_new_high(rs_line: pd.Series, lookback: int = 50) -> pd.Series:
    """偵測 RS Line 領先創新高的點（紫色點）

    Args:
        rs_line: 已計算好的 RS Line
        lookback: 比較區間（預設 50 日）

    Returns:
        pd.Series[bool]: True = 該日 RS Line 創新高（含對應 t-lookback 到 t）
    """
    if rs_line is None or len(rs_line) == 0:
        return pd.Series(dtype=bool)
    rolling_max = rs_line.rolling(window=lookback, min_periods=1).max()
    return rs_line >= rolling_max
