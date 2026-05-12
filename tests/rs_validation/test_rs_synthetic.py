"""Phase 1 — RS Line 合成資料單元測試

四個必須通過的案例：
1. 同步走勢 → RS 為水平直線
2. 雙倍走勢 → RS 穩定指數成長
3. 反向走勢 → RS 單調遞減
4. 段落切換 → RS 在中點轉折
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import pytest

from rs_line import calculate_rs_line


@pytest.fixture
def trading_dates():
    return pd.date_range('2024-01-01', periods=252, freq='B')


def test_identical_movement(trading_dates):
    """同步走勢：股票與指數每日報酬相同 → RS 為水平直線。

    驗收：rs_line.std() / rs_line.mean() < 1e-10
    """
    stock = pd.Series(100 * (1.0005 ** np.arange(252)), index=trading_dates)
    index = pd.Series( 50 * (1.0005 ** np.arange(252)), index=trading_dates)
    rs = calculate_rs_line(stock, index)
    cv = rs.std() / rs.mean()
    assert cv < 1e-10, f'同步走勢 RS 應為水平 (CV={cv:.2e})'


def test_double_beta(trading_dates):
    """雙倍走勢：股票每日報酬為指數的兩倍 → RS 線穩定成長。

    每日報酬期望值 (1 + 2r) / (1 + r) - 1。
    驗收：mean ≈ expected ±1e-6，std < 1e-8
    """
    r = 0.001
    stock = pd.Series(100 * ((1 + 2*r) ** np.arange(252)), index=trading_dates)
    index = pd.Series( 50 * ((1 + r)   ** np.arange(252)), index=trading_dates)
    rs = calculate_rs_line(stock, index)
    daily_ret = rs.pct_change().dropna()
    expected = (1 + 2*r) / (1 + r) - 1
    assert abs(daily_ret.mean() - expected) < 1e-6, \
        f'RS 每日報酬均值偏差: {daily_ret.mean():.6f} vs {expected:.6f}'
    assert daily_ret.std() < 1e-8, \
        f'RS 每日報酬 std 過大: {daily_ret.std():.2e}'


def test_inverse_movement(trading_dates):
    """反向走勢：股票報酬為指數報酬的負值 → RS 線單調遞減。"""
    r = 0.001
    stock = pd.Series(100 * ((1 - r) ** np.arange(252)), index=trading_dates)
    index = pd.Series( 50 * ((1 + r) ** np.arange(252)), index=trading_dates)
    rs = calculate_rs_line(stock, index)
    diffs = rs.diff().dropna()
    assert (diffs < 0).all(), \
        f'反向走勢 RS 必須單調遞減，但有 {(diffs >= 0).sum()} 日違反'


def test_regime_switch(trading_dates):
    """段落切換：前半跑贏、後半跑輸 → RS 在中點 (~126 日) 出現轉折高點。

    驗收：peak_idx 落在 [124, 128] 範圍內
    """
    n = 252
    half = n // 2
    stock_rets = np.concatenate([np.full(half, 0.002), np.full(n - half, -0.002)])
    index_rets = np.full(n, 0.001)
    stock = pd.Series(100 * np.cumprod(1 + stock_rets), index=trading_dates)
    index = pd.Series( 50 * np.cumprod(1 + index_rets), index=trading_dates)
    rs = calculate_rs_line(stock, index)
    peak_idx = int(rs.values.argmax())
    assert 124 <= peak_idx <= 128, \
        f'轉折點應在第 126 日附近，實際第 {peak_idx} 日'


# ────────────────────────────────────────────────────────────────
# 額外：WMA 平滑版測試
# ────────────────────────────────────────────────────────────────

def test_wma_smoothing(trading_dates):
    """WMA 平滑後仍應呈現主趨勢，不應有時間方向偏移。"""
    r = 0.001
    stock = pd.Series(100 * ((1 + 2*r) ** np.arange(252)), index=trading_dates)
    index = pd.Series( 50 * ((1 + r)   ** np.arange(252)), index=trading_dates)
    rs_raw = calculate_rs_line(stock, index)
    rs_smooth = calculate_rs_line(stock, index, smooth_wma=21)
    # 平滑版前 20 日為 NaN
    valid = rs_smooth.dropna()
    assert len(valid) > 100
    # 兩者趨勢相關性必須 > 0.999
    aligned = pd.concat([rs_raw, valid], axis=1, join='inner').dropna()
    corr = aligned.iloc[:, 0].corr(aligned.iloc[:, 1])
    assert corr > 0.999, f'平滑後相關性過低: {corr:.4f}'


# ────────────────────────────────────────────────────────────────
# 額外：日期對齊測試
# ────────────────────────────────────────────────────────────────

def test_date_alignment_intersection():
    """股票與指數有不同日期 → 自動取交集"""
    dates_stock = pd.date_range('2024-01-01', periods=100, freq='B')
    dates_index = pd.date_range('2024-01-15', periods=100, freq='B')  # 後延 2 週
    stock = pd.Series(100.0, index=dates_stock)
    index = pd.Series(50.0, index=dates_index)
    rs = calculate_rs_line(stock, index)
    common = dates_stock.intersection(dates_index)
    assert len(rs) == len(common), \
        f'RS 長度應為交集 ({len(common)})，實際 {len(rs)}'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
