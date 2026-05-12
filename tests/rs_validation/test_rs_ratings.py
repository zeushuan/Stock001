"""Phase 1+ — compute_rs_ratings 單元測試

驗證 IBD/Minervini 風格 percentile RS Rating 的正確性
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import pytest

from sepa_vcp import compute_rs_ratings, compute_returns


# ────────────────────────────────────────────────────────────────
# compute_rs_ratings 測試
# ────────────────────────────────────────────────────────────────

def test_rs_rating_top_is_100():
    """報酬最高的 ticker 應該得到接近 100 的分數"""
    returns = {
        'BEST':  {'13w': 50.0, '26w': 80.0, '39w': 100.0, '52w': 120.0},
        'MED1':  {'13w': 10.0, '26w': 20.0, '39w':  30.0, '52w':  40.0},
        'MED2':  {'13w':  5.0, '26w': 10.0, '39w':  15.0, '52w':  20.0},
        'WORST': {'13w':-10.0, '26w':-20.0, '39w': -30.0, '52w': -40.0},
    }
    rs = compute_rs_ratings(returns)
    assert rs['BEST'] == 100.0, f'最佳應為 100，實際 {rs["BEST"]}'
    assert rs['WORST'] == 25.0, f'最差應為 25 (1/4 * 100)，實際 {rs["WORST"]}'


def test_rs_rating_ordering():
    """排名順序必須一致：報酬越高 RS Rating 越高"""
    returns = {
        'A': {'13w': 100.0, '26w': 50.0, '39w': 30.0, '52w': 20.0},
        'B': {'13w':  80.0, '26w': 50.0, '39w': 30.0, '52w': 20.0},
        'C': {'13w':  60.0, '26w': 50.0, '39w': 30.0, '52w': 20.0},
        'D': {'13w':  40.0, '26w': 50.0, '39w': 30.0, '52w': 20.0},
        'E': {'13w':  20.0, '26w': 50.0, '39w': 30.0, '52w': 20.0},
    }
    rs = compute_rs_ratings(returns)
    # A > B > C > D > E
    assert rs['A'] > rs['B'] > rs['C'] > rs['D'] > rs['E'], \
        f'排名錯誤: {rs}'


def test_rs_rating_weight_13w_double():
    """13w 權重 = 2，其他 = 1。

    給兩支股票：A 在 13w 報酬高 10%，B 在 26w 報酬高 10%。
    依加權公式：A score = 2×10 + 0 + 0 + 0 = 20，B score = 0 + 10 + 0 + 0 = 10。
    A 應該 rank 比 B 高。
    """
    returns = {
        'A': {'13w': 10.0, '26w': 0.0,  '39w': 0.0, '52w': 0.0},
        'B': {'13w': 0.0,  '26w': 10.0, '39w': 0.0, '52w': 0.0},
    }
    rs = compute_rs_ratings(returns)
    assert rs['A'] > rs['B'], \
        f'13w 高的 A 應 rank 高（加權 2x），實際 A={rs["A"]} vs B={rs["B"]}'


def test_rs_rating_empty_input():
    """空輸入應該返回空 dict，不丟例外"""
    assert compute_rs_ratings({}) == {}
    assert compute_rs_ratings({'BAD': {}}) == {}


def test_rs_rating_handles_missing_keys():
    """缺失部分週期的 return 應該視為 0，仍能計算"""
    returns = {
        'A': {'13w': 50.0},  # 缺 26w/39w/52w
        'B': {'13w': 10.0, '26w': 10.0, '39w': 10.0, '52w': 10.0},
    }
    rs = compute_rs_ratings(returns)
    # A score = 2*50 = 100, B = 2*10+10+10+10 = 50, A > B
    assert rs['A'] > rs['B']


def test_rs_rating_custom_weights():
    """自訂權重應該生效"""
    returns = {
        'A': {'13w': 10.0, '26w': 0.0,  '39w': 0.0, '52w': 0.0},
        'B': {'13w': 0.0,  '26w': 10.0, '39w': 0.0, '52w': 0.0},
    }
    # 預設 (2,1,1,1) → A 贏
    rs1 = compute_rs_ratings(returns, weights=(2, 1, 1, 1))
    assert rs1['A'] > rs1['B']
    # 改成 (1, 3, 1, 1) → B 贏
    rs2 = compute_rs_ratings(returns, weights=(1, 3, 1, 1))
    assert rs2['B'] > rs2['A']


# ────────────────────────────────────────────────────────────────
# compute_returns 測試
# ────────────────────────────────────────────────────────────────

def test_compute_returns_basic():
    """簡單序列：Close 從 100 漲到 110，return 應該是 +10%"""
    dates = pd.date_range('2024-01-01', periods=300, freq='B')
    close = pd.Series(np.linspace(100, 110, 300), index=dates)
    df = pd.DataFrame({'Close': close})
    rets = compute_returns(df, periods_days=(65, 130, 195, 252))
    # 52w return ≈ from price[-253] to price[-1]
    assert abs(rets['52w'] - (close.iloc[-1] / close.iloc[-253] - 1) * 100) < 1e-6


def test_compute_returns_short_history():
    """歷史不夠長 → 較長期 return = 0"""
    dates = pd.date_range('2024-01-01', periods=50, freq='B')
    close = pd.Series(np.linspace(100, 110, 50), index=dates)
    df = pd.DataFrame({'Close': close})
    rets = compute_returns(df, periods_days=(65, 130, 195, 252))
    # 50 bars 不足 65d，全部返回 0
    assert rets['13w'] == 0
    assert rets['26w'] == 0


def test_compute_returns_empty_df():
    assert compute_returns(None) == {}
    assert compute_returns(pd.DataFrame()) == {}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
