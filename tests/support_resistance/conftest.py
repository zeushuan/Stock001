"""共用 fixtures：合成 OHLCV 餵子系統測試用。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def synth_double_top_bottom() -> pd.DataFrame:
    """合成一段 OHLCV：價格在 100 與 110 之間擺盪數次，
    形成多個 swing high 在 110、swing low 在 100 附近。

    用於驗證 swing_cluster 能找到 ~100 支撐 + ~110 壓力兩區。
    """
    np.random.seed(42)
    n = 120
    # 用正弦波（週期 ~20）做主結構，加微小噪音
    t = np.arange(n)
    base = 105 + 5 * np.sin(2 * np.pi * t / 20)   # 100 ~ 110
    noise = np.random.normal(0, 0.3, n)
    close = base + noise
    high  = close + np.abs(np.random.normal(0.5, 0.2, n))
    low   = close - np.abs(np.random.normal(0.5, 0.2, n))
    open_ = close + np.random.normal(0, 0.2, n)
    vol = np.random.randint(800, 1200, n).astype(float)
    # 在 high 接近 110 附近的 bar 上加大量（測試 volume profile POC）
    vol[(close > 109)] *= 3.0
    idx = pd.date_range('2024-01-01', periods=n, freq='D')
    return pd.DataFrame({
        'Open': open_, 'High': high, 'Low': low, 'Close': close, 'Volume': vol,
    }, index=idx)


@pytest.fixture
def synth_uptrend_with_pullbacks() -> pd.DataFrame:
    """合成上升趨勢但有多次拉回到同一支撐線（~100）。

    用於驗證 'support 預測力較佳' 的偏置。
    """
    np.random.seed(7)
    n = 100
    base = 100 + np.arange(n) * 0.2   # slope
    # 加 4 次拉回到 100（模擬 retest）
    pullback = np.zeros(n)
    for k in [15, 35, 55, 75]:
        pullback[k] = -(base[k] - 100)   # 拉回到 100
    close = base + pullback + np.random.normal(0, 0.5, n)
    high  = close + 0.6
    low   = close - 0.6
    open_ = close + np.random.normal(0, 0.2, n)
    vol = np.random.randint(800, 1200, n).astype(float)
    idx = pd.date_range('2024-01-01', periods=n, freq='D')
    return pd.DataFrame({
        'Open': open_, 'High': high, 'Low': low, 'Close': close, 'Volume': vol,
    }, index=idx)


@pytest.fixture
def synth_volume_spike_at_105() -> pd.DataFrame:
    """合成價格在 100-110 區間均勻分布，但成交量集中在 ~105。

    用於驗證 Volume Profile POC ≈ 105。
    """
    np.random.seed(11)
    n = 200
    close = np.random.uniform(100, 110, n)
    high  = close + 0.2
    low   = close - 0.2
    open_ = close + np.random.normal(0, 0.1, n)
    # volume 用 gaussian 集中在 105
    vol = 1000 * np.exp(-((close - 105) ** 2) / 4) + 100
    idx = pd.date_range('2024-01-01', periods=n, freq='D')
    return pd.DataFrame({
        'Open': open_, 'High': high, 'Low': low, 'Close': close, 'Volume': vol,
    }, index=idx)
