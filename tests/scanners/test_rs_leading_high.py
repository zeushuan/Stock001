"""RS Leading High Scanner — 單元與整合測試"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import pytest

from scanners.rs_leading_high import (
    detect_rs_leading_high, apply_quality_filters,
    score_signal, signal_to_dict, scan_universe,
    RSLeadingHighSignal,
    _days_since_recent_high, _linear_slope, _wma,
    _score_purple_dot_frequency, _score_distance_from_high,
    _score_volume,
)


# ────────────────────────────────────────────────────────────────
# Helper: 建造合成資料
# ────────────────────────────────────────────────────────────────

@pytest.fixture
def trading_dates():
    return pd.date_range('2024-01-01', periods=300, freq='B')


def _build_leading_high_scenario(n=300):
    """合成資料設計（最終可成立 leading high）：
    - 0-230 (230d): 股票/指數同步緩漲（基準）
    - 230-260 (30d): 股票 +0.5%/day → 創高 → 之後是 63d 高點
    - 260-290 (30d): 股票 -0.3%/day → 回檔 ~9%
    - 290-298 (8d): 股票 0、指數 -0.5%/day → RS 逐步升
    - 298-300 (2d): 股票 0、指數 -3%/day → RS 大跳，創 63d 新高
    """
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    stock_rets = np.zeros(n)
    index_rets = np.zeros(n)
    stock_rets[:230] = 0.001
    index_rets[:230] = 0.001
    stock_rets[230:260] = 0.005
    index_rets[230:260] = 0.0
    stock_rets[260:290] = -0.003
    index_rets[260:290] = 0.0
    stock_rets[290:298] = 0.0
    index_rets[290:298] = -0.005
    stock_rets[298:] = 0.0
    index_rets[298:] = -0.03

    stock_prices = pd.Series(100 * np.cumprod(1 + stock_rets), index=dates)
    index_prices = pd.Series( 50 * np.cumprod(1 + index_rets), index=dates)
    stock_volumes = pd.Series(1e6 + np.random.RandomState(42).rand(n) * 1e5,
                                 index=dates)
    return stock_prices, index_prices, stock_volumes


# ────────────────────────────────────────────────────────────────
# Phase 1: detect_rs_leading_high
# ────────────────────────────────────────────────────────────────

def test_detect_returns_signal_for_qualifying_data():
    stock, index, vol = _build_leading_high_scenario()
    as_of = stock.index[-1]
    sig = detect_rs_leading_high(stock, index, vol, 'TEST', as_of)
    assert sig is not None, '合成的合格資料應該偵測到訊號'
    assert sig.ticker == 'TEST'
    assert sig.days_since_rs_high <= 5
    assert sig.stock_distance_from_high_pct >= 0.03


def test_detect_returns_none_when_price_at_high():
    """股票在 N 日高點 → 不算 leading high"""
    dates = pd.date_range('2024-01-01', periods=300, freq='B')
    stock = pd.Series(100 * (1.001 ** np.arange(300)), index=dates)  # 一路漲
    index = pd.Series( 50 * (1.0005 ** np.arange(300)), index=dates)
    vol = pd.Series(1e6, index=dates)
    sig = detect_rs_leading_high(stock, index, vol, 'TEST', dates[-1])
    assert sig is None, '股票在新高，不應有訊號'


def test_detect_returns_none_when_rs_high_too_old():
    """RS 在 30 日前創高（>5 天） → 不算"""
    dates = pd.date_range('2024-01-01', periods=300, freq='B')
    # 前 200 日 RS 上升，後 100 日 RS 下降
    s_rets = np.concatenate([np.full(200, 0.002), np.full(100, -0.001)])
    i_rets = np.full(300, 0.0005)
    stock = pd.Series(100 * np.cumprod(1 + s_rets), index=dates)
    index = pd.Series( 50 * np.cumprod(1 + i_rets), index=dates)
    vol = pd.Series(1e6, index=dates)
    sig = detect_rs_leading_high(stock, index, vol, 'TEST', dates[-1])
    assert sig is None, 'RS 新高已過期，不應有訊號'


def test_detect_handles_short_history():
    """資料不足 63 日 → 返回 None，不丟例外"""
    dates = pd.date_range('2024-01-01', periods=30, freq='B')
    stock = pd.Series(100.0, index=dates)
    index = pd.Series(50.0, index=dates)
    vol = pd.Series(1e6, index=dates)
    sig = detect_rs_leading_high(stock, index, vol, 'TEST', dates[-1])
    assert sig is None


def test_detect_handles_tz_aware_index():
    """tz-aware 與 tz-naive 混用應該自動處理"""
    dates_tz = pd.date_range('2024-01-01', periods=300, freq='B', tz='America/New_York')
    dates_naive = pd.date_range('2024-01-01', periods=300, freq='B')
    stock, index, vol = _build_leading_high_scenario()
    stock_tz = pd.Series(stock.values, index=dates_tz)
    index_naive = pd.Series(index.values, index=dates_naive)
    vol_tz = pd.Series(vol.values, index=dates_tz)

    # 不應丟例外
    sig = detect_rs_leading_high(stock_tz, index_naive, vol_tz, 'TZ_TEST', dates_naive[-1])
    assert sig is not None


# ────────────────────────────────────────────────────────────────
# Phase 2: apply_quality_filters
# ────────────────────────────────────────────────────────────────

def _make_signal(ticker='T', **overrides):
    base = dict(
        ticker=ticker, signal_date=pd.Timestamp('2024-12-01'),
        rs_value=1.0, rs_lookback_high=1.0, days_since_rs_high=0,
        purple_dot_count_recent=3,
        stock_price=100.0, stock_distance_from_high_pct=0.05,
        rs_above_wma21=True, rs_long_term_trend_up=True,
        volume_ratio=1.5,
        above_sma200=True, dollar_volume_50d=1e8, rs_slope_50d=0.001,
    )
    base.update(overrides)
    return RSLeadingHighSignal(**base)


def test_filter_rs_trend_must_be_up():
    sig = _make_signal(rs_long_term_trend_up=False, rs_slope_50d=-0.001)
    stock = pd.Series([100] * 300, index=pd.date_range('2024-01-01', periods=300, freq='B'))
    vol = pd.Series([1e6] * 300, index=stock.index)
    passed = apply_quality_filters(sig, stock, vol, market='US')
    assert not passed
    assert any('F1' in r for r in sig.filter_failed_reasons)


def test_filter_rs_above_wma21():
    sig = _make_signal(rs_above_wma21=False)
    stock = pd.Series([100] * 300, index=pd.date_range('2024-01-01', periods=300, freq='B'))
    vol = pd.Series([1e6] * 300, index=stock.index)
    passed = apply_quality_filters(sig, stock, vol, market='US')
    assert not passed
    assert any('F2' in r for r in sig.filter_failed_reasons)


def test_filter_above_sma200():
    sig = _make_signal(above_sma200=False)
    stock = pd.Series([100] * 300, index=pd.date_range('2024-01-01', periods=300, freq='B'))
    vol = pd.Series([1e6] * 300, index=stock.index)
    passed = apply_quality_filters(sig, stock, vol, market='US')
    assert not passed
    assert any('F3' in r for r in sig.filter_failed_reasons)


def test_filter_liquidity_us():
    sig = _make_signal(dollar_volume_50d=1e6)  # $1M 太低
    stock = pd.Series([100] * 300, index=pd.date_range('2024-01-01', periods=300, freq='B'))
    vol = pd.Series([1e6] * 300, index=stock.index)
    passed = apply_quality_filters(sig, stock, vol, market='US')
    assert not passed
    assert any('F4' in r for r in sig.filter_failed_reasons)


def test_filter_short_history():
    sig = _make_signal()
    stock = pd.Series([100] * 100, index=pd.date_range('2024-01-01', periods=100, freq='B'))
    vol = pd.Series([1e6] * 100, index=stock.index)
    passed = apply_quality_filters(sig, stock, vol, market='US')
    assert not passed
    assert any('F5' in r for r in sig.filter_failed_reasons)


def test_filter_all_passed():
    sig = _make_signal()
    stock = pd.Series([100] * 300, index=pd.date_range('2024-01-01', periods=300, freq='B'))
    vol = pd.Series([1e6] * 300, index=stock.index)
    passed = apply_quality_filters(sig, stock, vol, market='US')
    assert passed


# ────────────────────────────────────────────────────────────────
# Phase 3: scoring
# ────────────────────────────────────────────────────────────────

def test_score_purple_dot_frequency():
    assert _score_purple_dot_frequency(0) == 0
    assert _score_purple_dot_frequency(1) == 4
    assert _score_purple_dot_frequency(5) == 20
    assert _score_purple_dot_frequency(10) == 20  # cap


def test_score_distance_inverted_u():
    """5%-15% 滿分，<3% 或 >25% 為 0"""
    assert _score_distance_from_high(0.02) == 0  # too close
    assert _score_distance_from_high(0.30) == 0  # too far
    assert _score_distance_from_high(0.05) == 20  # boundary
    assert _score_distance_from_high(0.10) == 20  # 中段
    assert _score_distance_from_high(0.15) == 20  # boundary
    # 0.04 應該 < 20
    assert _score_distance_from_high(0.04) < 20
    assert _score_distance_from_high(0.04) > 0


def test_score_volume():
    assert _score_volume(_make_signal(volume_ratio=2.0)) == 20
    assert _score_volume(_make_signal(volume_ratio=0.5)) == 0


def test_score_signal_total():
    sig = _make_signal(purple_dot_count_recent=5,
                         stock_distance_from_high_pct=0.10,
                         volume_ratio=2.0,
                         rs_slope_50d=0.001)
    ctx = {'rs_slopes': [0.0001, 0.0005, 0.001, 0.0008]}
    total = score_signal(sig, ctx)
    assert 0 <= total <= 100
    assert sig.quality_score == total
    assert 'purple_dots' in sig.score_breakdown


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def test_days_since_recent_high():
    s = pd.Series([1, 2, 3, 5, 4, 3, 2])  # 5 在 index 3，距末端 7-1-3=3
    assert _days_since_recent_high(s) == 3


def test_linear_slope():
    # y = 2x → slope = 2
    s = pd.Series(2 * np.arange(10).astype(float))
    assert abs(_linear_slope(s) - 2.0) < 1e-9


def test_wma_basic():
    s = pd.Series([1, 2, 3, 4, 5], dtype=float)
    w = _wma(s, 3)
    # 最後一個 = (1×1 + 2×2 + 3×3) / 6 = (1+4+9)/6
    # WMA period=3, weights [1/6, 2/6, 3/6]
    # 最後 = 3*1/6 + 4*2/6 + 5*3/6 = (3+8+15)/6 = 26/6 ≈ 4.333
    assert abs(w.iloc[-1] - 26/6) < 1e-6


# ────────────────────────────────────────────────────────────────
# Integration: scan_universe + JSON export
# ────────────────────────────────────────────────────────────────

def test_signal_to_dict_serializable():
    sig = _make_signal()
    d = signal_to_dict(sig)
    import json
    s = json.dumps(d, default=str)
    assert 'ticker' in s
    assert 'signal_date' in s


def test_t3_export_schema():
    """匯出 T3 JSON 並通過 schema 驗證"""
    from integrations.t3_export import export_signals_to_t3, validate_t3_schema
    import tempfile, json
    sig = _make_signal(quality_score=80.0, rank=1)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        path = f.name
    export_signals_to_t3([sig], path)
    with open(path, 'r') as f:
        payload = json.load(f)
    errors = validate_t3_schema(payload)
    assert not errors, f'T3 schema 錯誤: {errors}'
    os.unlink(path)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
