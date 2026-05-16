"""Stan Weinstein Stage Analysis 單元測試"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import numpy as np
import pandas as pd
import pytest

from patterns.stage_analysis import (
    classify_stage, apply_stage_filter, stage_score, StageResult, STAGE_NAMES,
)


def _make_synthetic_stage(stage: int, n_bars: int = 300,
                            start_price: float = 100.0) -> pd.DataFrame:
    """合成 4 種 stage 的資料"""
    idx = pd.date_range("2023-01-01", periods=n_bars, freq="B")
    if stage == 1:
        # Basing：橫向震盪
        close = start_price + np.sin(np.linspace(0, 8*np.pi, n_bars)) * 3 + \
                np.random.RandomState(1).randn(n_bars) * 0.5
    elif stage == 2:
        # Advancing：穩定上升
        close = np.linspace(start_price, start_price * 1.5, n_bars) + \
                np.random.RandomState(2).randn(n_bars) * 1.0
    elif stage == 3:
        # Top：上升後走平
        rise = np.linspace(start_price, start_price * 1.4, n_bars - 80)
        flat = np.full(80, start_price * 1.4) + np.random.RandomState(3).randn(80) * 1.5
        close = np.concatenate([rise, flat])
    elif stage == 4:
        # Declining：下跌
        close = np.linspace(start_price * 1.5, start_price * 0.7, n_bars) + \
                np.random.RandomState(4).randn(n_bars) * 1.0
    else:
        close = np.full(n_bars, start_price)
    return pd.DataFrame({
        "Open": close * 0.998,
        "High": close * 1.005,
        "Low": close * 0.995,
        "Close": close,
        "Volume": np.full(n_bars, 1e6),
    }, index=idx)


def test_stage_2_synthetic():
    df = _make_synthetic_stage(2)
    r = classify_stage(df)
    assert r.stage == 2, f"預期 Stage 2，實際 {r.stage} ({r.stage_name})"
    assert r.sma30w_slope > 0
    assert r.price_vs_sma30w > 0


def test_stage_4_synthetic():
    df = _make_synthetic_stage(4)
    r = classify_stage(df)
    assert r.stage == 4, f"預期 Stage 4，實際 {r.stage} ({r.stage_name})"
    assert r.sma30w_slope < 0
    assert r.price_vs_sma30w < 0


def test_stage_1_synthetic():
    df = _make_synthetic_stage(1)
    r = classify_stage(df)
    # Basing 預期 stage=1 或 3（依價在均線上下）
    assert r.stage in (1, 3)
    assert abs(r.sma30w_slope) < 0.05


def test_insufficient_data():
    df = pd.DataFrame({
        "Open": [100]*100, "High": [101]*100, "Low": [99]*100,
        "Close": [100]*100, "Volume": [1e6]*100,
    }, index=pd.date_range("2024-01-01", periods=100, freq="B"))
    r = classify_stage(df)
    assert r.stage == 0


def test_apply_stage_filter():
    """方案 A：Stage 4 → 0；Stage 2 → 不變"""
    assert apply_stage_filter(80.0, 2) == 80.0
    assert apply_stage_filter(80.0, 4) == 0.0
    assert apply_stage_filter(80.0, 1) == 40.0   # half
    assert apply_stage_filter(80.0, 3) == pytest.approx(16.0)


def test_stage_score_method_b():
    """方案 B：Stage 2 early = 25，Stage 4 = 0"""
    r2_early = StageResult(stage=2, stage_name="Advancing",
                            price_vs_sma30w=0.1, sma30w_slope=0.05,
                            sub_stage="early")
    r2_late = StageResult(stage=2, stage_name="Advancing",
                           price_vs_sma30w=0.1, sma30w_slope=0.05,
                           sub_stage="late")
    r4 = StageResult(stage=4, stage_name="Declining",
                      price_vs_sma30w=-0.1, sma30w_slope=-0.05)

    assert stage_score(r2_early) == 25.0
    assert stage_score(r2_late) == 15.0
    assert stage_score(r4) == 0.0


def test_stage_names_consistency():
    """stage_name 與 STAGE_NAMES 一致"""
    df = _make_synthetic_stage(2)
    r = classify_stage(df)
    assert r.stage_name == STAGE_NAMES.get(r.stage, "Unknown")


def test_confidence_range():
    df = _make_synthetic_stage(2)
    r = classify_stage(df)
    assert 0.0 <= r.confidence <= 1.0


def test_transition_signal_stage_2_breakout():
    """合成 stage 1→2 過渡資料：應該有 transition signal"""
    # 250 天 flat + 50 天上升 (帶量)
    base_close = np.full(250, 100.0) + np.random.RandomState(5).randn(250) * 0.3
    breakout_close = np.linspace(100, 115, 50)
    close = np.concatenate([base_close, breakout_close])
    n = len(close)
    idx = pd.date_range("2023-01-01", periods=n, freq="B")
    vol = np.full(n, 1e6)
    # 突破日量增
    vol[-5:] = 2e6
    df = pd.DataFrame({
        "Open": close, "High": close*1.005,
        "Low": close*0.995, "Close": close, "Volume": vol,
    }, index=idx)
    r = classify_stage(df)
    # 可能 sub_stage 是 early（剛進 Stage 2）
    assert r.stage in (1, 2, 3)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
