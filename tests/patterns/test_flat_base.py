"""Flat Base 單元測試"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import numpy as np
import pandas as pd
import pytest

from patterns.flat_base import detect_flat_base


def _make_synthetic_flat_base(
    prior_bars: int = 250,
    base_bars: int = 30,
    prior_gain: float = 0.30,
    base_depth: float = 0.08,
    add_breakout: bool = False,
    a_price: float = 100.0,
) -> pd.DataFrame:
    """合成 flat base：前期漲 prior_gain，整理 base_depth"""
    n = prior_bars + base_bars + (1 if add_breakout else 0)
    idx = pd.date_range("2024-01-01", periods=n, freq="B")

    # Prior：前面 flat，最後 60 天才上升 prior_gain（符合 spec 的 prior_window=60）
    rise_bars = min(60, prior_bars)
    start_price = a_price / (1 + prior_gain)
    flat_part = np.full(prior_bars - rise_bars, start_price)
    rise_part = np.linspace(start_price, a_price, rise_bars)
    prior_close = np.concatenate([flat_part, rise_part]) if prior_bars > rise_bars else rise_part

    # Base：橫向震盪 ±base_depth/2
    base_close = a_price + np.sin(np.linspace(0, 4*np.pi, base_bars)) * (a_price * base_depth / 2)
    base_close += np.random.RandomState(42).randn(base_bars) * 0.2

    close = np.concatenate([prior_close, base_close])

    if add_breakout:
        # 突破：要 > base 期間 High max（含 base_depth/2 + sin amplitude + 高低點）
        # base swing 至 ±a_price*base_depth/2 = ±4，High = close*1.005，max ≈ 104.5
        # 故突破價設 a_price * (1 + base_depth)
        close = np.append(close, a_price * (1 + base_depth + 0.02))

    high = close * 1.005
    low = close * 0.995
    open_ = close * 0.998
    vol = np.full(n, 1e6)
    # Base 期間量縮
    vol[prior_bars:prior_bars+base_bars] *= 0.65
    if add_breakout:
        vol[-1] = 1.8e6   # 突破爆量

    return pd.DataFrame({
        "Open": open_, "High": high, "Low": low,
        "Close": close, "Volume": vol,
    }, index=idx)


def test_detect_basic():
    df = _make_synthetic_flat_base()
    r = detect_flat_base(df)
    assert r.detected, f"未偵測到：{r.notes}"
    assert r.base_depth <= 0.15
    assert r.score > 0


def test_depth_too_large_fails():
    df = _make_synthetic_flat_base(base_depth=0.30)
    r = detect_flat_base(df)
    assert not r.detected
    assert any("深度" in n for n in r.notes)


def test_breakout_scores_higher():
    df_no = _make_synthetic_flat_base(add_breakout=False)
    df_bo = _make_synthetic_flat_base(add_breakout=True)
    r_no = detect_flat_base(df_no)
    r_bo = detect_flat_base(df_bo)
    assert r_bo.score > r_no.score
    assert r_bo.breakout
    assert r_bo.breakout_volume_ratio >= 1.4


def test_insufficient_data():
    df = pd.DataFrame({
        "Open": [100]*30, "High": [101]*30, "Low": [99]*30,
        "Close": [100]*30, "Volume": [1e6]*30,
    }, index=pd.date_range("2024-01-01", periods=30, freq="B"))
    r = detect_flat_base(df)
    assert not r.detected


def test_prior_gain_low_score():
    df_high = _make_synthetic_flat_base(prior_gain=0.30)
    df_low = _make_synthetic_flat_base(prior_gain=0.05)
    r_h = detect_flat_base(df_high)
    r_l = detect_flat_base(df_low)
    if r_h.detected and r_l.detected:
        assert r_h.breakdown["prior_gain"] > r_l.breakdown["prior_gain"]


def test_market_correction_reduces_score():
    df = _make_synthetic_flat_base(add_breakout=True)
    r_up = detect_flat_base(df, market_status="uptrend")
    r_corr = detect_flat_base(df, market_status="correction")
    assert r_corr.score < r_up.score


def test_rs_rating_boost():
    df = _make_synthetic_flat_base(add_breakout=True)
    r_no = detect_flat_base(df)
    r_high = detect_flat_base(df, rs_rating=92)
    assert r_high.breakdown["rs_rating"] >= r_no.breakdown["rs_rating"]


def test_breakdown_keys():
    df = _make_synthetic_flat_base(add_breakout=True)
    r = detect_flat_base(df)
    for key in ["depth", "duration", "prior_gain", "ma_align",
                "volume_dryup", "tight_closes", "base_count",
                "rs_rating", "breakout"]:
        assert key in r.breakdown


def test_pivot_above_window_high():
    df = _make_synthetic_flat_base()
    r = detect_flat_base(df)
    if r.detected:
        window_high = float(df["High"].iloc[-30:].max())
        assert r.pivot_point > window_high


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
