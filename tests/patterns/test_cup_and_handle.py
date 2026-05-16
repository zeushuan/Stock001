"""Cup and Handle 單元測試"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import numpy as np
import pandas as pd
import pytest

from patterns.cup_and_handle import detect_cup_and_handle, CupAndHandleResult


def _make_synthetic_cup(n_prior: int = 60, cup_bars: int = 60,
                         handle_bars: int = 10,
                         a_price: float = 100.0, depth_pct: float = 0.20,
                         add_breakout: bool = False) -> pd.DataFrame:
    """合成杯柄資料：
       - prior: 上升 30%
       - cup: U 型
       - handle: 輕微下傾
       - 最後 1 根（若 add_breakout）突破 pivot
    """
    n_total = n_prior + cup_bars + handle_bars + (1 if add_breakout else 0)
    idx = pd.date_range("2024-01-01", periods=n_total, freq="B")

    # Prior：從 70 漲到 a_price (=100)
    prior_close = np.linspace(a_price * 0.7, a_price, n_prior)

    # Cup：U 形（二次曲線）
    cup_low = a_price * (1 - depth_pct)
    cup_x = np.linspace(-1, 1, cup_bars)
    cup_close = cup_low + (a_price - cup_low) * cup_x ** 2   # U 形

    # Handle：從 cup 結尾下傾 ~10%
    b_price = cup_close[-1]
    handle_close = np.linspace(b_price, b_price * 0.92, handle_bars)

    close = np.concatenate([prior_close, cup_close, handle_close])

    if add_breakout:
        # 突破 pivot
        breakout_price = a_price * 1.02
        close = np.append(close, breakout_price)

    # OHLC + Volume
    high = close * 1.01
    low = close * 0.99
    open_ = close * 0.995
    vol = np.full(len(close), 1e6)
    # 突破日量爆
    if add_breakout:
        vol[-1] = 2e6

    return pd.DataFrame({
        "Open": open_, "High": high, "Low": low,
        "Close": close, "Volume": vol,
    }, index=idx)


def test_detect_cup_basic():
    """合成完美杯柄 → detected=True，score > 30"""
    df = _make_synthetic_cup()
    r = detect_cup_and_handle(df, market_status="uptrend")
    assert r.detected, f"未偵測到：{r.reasons}"
    assert r.score > 30, f"分數太低: {r.score}"
    assert r.cup_start_idx is not None
    assert r.cup_bottom_idx > r.cup_start_idx
    assert r.cup_end_idx > r.cup_bottom_idx


def test_detect_cup_with_breakout():
    """突破完成（量爆）→ 分數更高"""
    df_no_bo = _make_synthetic_cup(add_breakout=False)
    df_bo = _make_synthetic_cup(add_breakout=True)
    r_no = detect_cup_and_handle(df_no_bo, market_status="uptrend")
    r_bo = detect_cup_and_handle(df_bo, market_status="uptrend")
    assert r_bo.score > r_no.score, "突破應加分"


def test_detect_cup_insufficient_data():
    """資料 < 120 → 回 detected=False"""
    df = _make_synthetic_cup(n_prior=10, cup_bars=20, handle_bars=5)
    r = detect_cup_and_handle(df)
    assert not r.detected


def test_hard_filter_cup_too_deep():
    """杯深 > 50% 觸發 hard filter"""
    df = _make_synthetic_cup(depth_pct=0.55)
    r = detect_cup_and_handle(df)
    # 應該根本找不到符合條件的杯（深度 > 0.50 直接被排）
    # 或被 hard filter 0 分
    assert not r.detected or r.score == 0


def test_market_correction_multiplier():
    """correction 市況 → score × 0.3"""
    df = _make_synthetic_cup(add_breakout=True)
    r_up = detect_cup_and_handle(df, market_status="uptrend")
    r_corr = detect_cup_and_handle(df, market_status="correction")
    assert r_corr.score < r_up.score * 0.5, \
        f"correction 應大幅降分: up={r_up.score}, corr={r_corr.score}"


def test_rs_rating_high():
    """RS ≥ 80 加分"""
    df = _make_synthetic_cup(add_breakout=True)
    r_no_rs = detect_cup_and_handle(df, market_status="uptrend")
    r_high_rs = detect_cup_and_handle(df, market_status="uptrend", rs_rating=90)
    assert r_high_rs.breakdown.get("rs_rating", 0) > r_no_rs.breakdown.get("rs_rating", 0)


def test_breakdown_has_all_keys():
    """breakdown 含所有 WEIGHTS keys"""
    df = _make_synthetic_cup(add_breakout=True)
    r = detect_cup_and_handle(df, market_status="uptrend")
    from patterns.cup_and_handle import WEIGHTS
    for key in WEIGHTS.keys():
        assert key in r.breakdown, f"breakdown 缺 {key}"


def test_variant_classic():
    df = _make_synthetic_cup(add_breakout=True)
    r = detect_cup_and_handle(df, market_status="uptrend")
    if r.detected:
        assert r.pattern_variant in ("classic", "no_handle")


def test_pivot_target_stop_relationship():
    df = _make_synthetic_cup(add_breakout=True)
    r = detect_cup_and_handle(df, market_status="uptrend")
    if r.detected and r.pivot_price and r.target_price and r.stop_loss:
        # target > pivot > stop
        assert r.target_price > r.pivot_price
        assert r.stop_loss < r.pivot_price


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
