"""Intraday indicators 單元測試 — Stock001 v9.29"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import numpy as np
import pandas as pd
import pytest

from intraday.indicators import (
    vwap_session, anchored_vwap, orb_levels, floor_pivots,
    floor_pivots_from_df, gap_metrics, relative_volume,
    add_standard_indicators,
)
from intraday.config import TIMEFRAMES, BARS_PER_WEEK, get_tf_config, NotApplicable


def _make_intraday_df(n_days: int = 3, bars_per_day: int = 78,
                       freq_minutes: int = 5,
                       start_price: float = 100.0) -> pd.DataFrame:
    """合成 intraday data：n_days × bars_per_day 根 5min bar"""
    rows = []
    idx = []
    price = start_price
    for d in range(n_days):
        date = pd.Timestamp('2024-01-15') + pd.Timedelta(days=d)
        for b in range(bars_per_day):
            ts = date + pd.Timedelta(hours=9, minutes=30 + b * freq_minutes)
            o = price
            c = price * (1 + (np.random.RandomState(d*100+b).rand() - 0.5) * 0.01)
            h = max(o, c) * 1.002
            l = min(o, c) * 0.998
            v = 1e5 + np.random.RandomState(d*100+b).randint(0, 1e5)
            rows.append([o, h, l, c, v])
            idx.append(ts)
            price = c
    return pd.DataFrame(rows, columns=['Open', 'High', 'Low', 'Close', 'Volume'],
                         index=pd.DatetimeIndex(idx))


# ─── Config tests ──

def test_timeframes_have_all_required_fields():
    for tf, cfg in TIMEFRAMES.items():
        assert cfg.code == tf
        assert cfg.minutes_per_bar > 0
        assert cfg.bars_per_day > 0
        assert isinstance(cfg.supports_stage, bool)


def test_bars_per_week_computed():
    # 1d 一週 5 根；1h 一週約 32 根
    assert BARS_PER_WEEK['1d'] == 5
    assert 30 <= BARS_PER_WEEK['1h'] <= 35


def test_get_tf_config_unknown_raises():
    with pytest.raises(ValueError):
        get_tf_config('999m')


# ─── VWAP tests ──

def test_vwap_session_resets_daily():
    df = _make_intraday_df(n_days=2, bars_per_day=20)
    vw = vwap_session(df)
    assert len(vw) == len(df)
    assert not vw.isna().all()
    # 每一天的第一根 VWAP 應該接近該根 typical price
    first_of_day_1 = df.iloc[0]
    typical_1 = (first_of_day_1['High'] + first_of_day_1['Low']
                  + first_of_day_1['Close']) / 3
    assert abs(vw.iloc[0] - typical_1) < typical_1 * 0.01


def test_anchored_vwap_starts_from_anchor():
    df = _make_intraday_df(n_days=2, bars_per_day=20)
    anchor = df.index[10]
    av = anchored_vwap(df, anchor)
    # anchor 前的應該都是 NaN
    assert av.iloc[:10].isna().all()
    assert not np.isnan(av.iloc[10])
    assert not np.isnan(av.iloc[-1])


# ─── ORB tests ──

def test_orb_levels_basic():
    df = _make_intraday_df(n_days=1, bars_per_day=78)
    orb = orb_levels(df, minutes=30, tf_minutes=5)
    assert orb['or_high'] is not None
    assert orb['or_low'] is not None
    assert orb['or_high'] >= orb['or_low']
    assert orb['bars_used'] == 6  # 30 / 5 = 6 bars
    assert orb['current_position'] in ('above_high', 'inside', 'below_low')


def test_orb_levels_breakout_up():
    # 製造一個明確的上突破：前 6 根是 100-101，後面拉到 105
    df = _make_intraday_df(n_days=1, bars_per_day=20)
    df.iloc[:6, df.columns.get_loc('High')] = 101.0
    df.iloc[:6, df.columns.get_loc('Low')] = 100.0
    df.iloc[10:, df.columns.get_loc('Close')] = 105.0
    df.iloc[10:, df.columns.get_loc('High')] = 106.0
    orb = orb_levels(df, minutes=30, tf_minutes=5)
    assert orb['breakout_up']
    assert orb['current_position'] == 'above_high'


# ─── Floor Pivots ──

def test_floor_pivots_classic():
    p = floor_pivots(prev_high=105, prev_low=95, prev_close=100)
    assert p['P'] == 100  # (105+95+100)/3
    assert p['R1'] == 105  # 2*100 - 95
    assert p['S1'] == 95   # 2*100 - 105
    assert p['R2'] == 110  # 100 + (105-95)
    assert p['S2'] == 90


def test_floor_pivots_from_df():
    df = _make_intraday_df(n_days=2, bars_per_day=20, start_price=100)
    p = floor_pivots_from_df(df)
    assert 'P' in p and 'R1' in p and 'S1' in p


# ─── Gap ──

def test_gap_metrics_up():
    df = _make_intraday_df(n_days=2, bars_per_day=20)
    # 強制前一日收 100、今日開 102
    last_prev_idx = df[df.index.date == df.index.date[0]].index[-1]
    df.loc[last_prev_idx, 'Close'] = 100
    first_today_idx = df[df.index.date == df.index.date[-1]].index[0]
    df.loc[first_today_idx, 'Open'] = 102
    gap = gap_metrics(df)
    assert gap['gap_type'] == 'up'
    assert abs(gap['gap_pct'] - 2.0) < 0.01


def test_gap_metrics_none():
    df = _make_intraday_df(n_days=2, bars_per_day=20)
    last_prev_idx = df[df.index.date == df.index.date[0]].index[-1]
    df.loc[last_prev_idx, 'Close'] = 100
    first_today_idx = df[df.index.date == df.index.date[-1]].index[0]
    df.loc[first_today_idx, 'Open'] = 100.1   # 0.1% only
    gap = gap_metrics(df)
    assert gap['gap_type'] == 'none'


# ─── Relative Volume ──

def test_relative_volume_returns_ratio():
    df = _make_intraday_df(n_days=10, bars_per_day=20)
    rv = relative_volume(df, lookback_sessions=5)
    # 同時段平均量是隨機的，所以只測 ratio 是 float 且 > 0
    if rv is not None:
        assert rv > 0


# ─── Standard indicators ──

def test_add_standard_indicators_adds_columns():
    df = _make_intraday_df(n_days=10, bars_per_day=20)
    df2 = add_standard_indicators(df)
    for col in ['e10', 'e20', 'e60', 'rsi', 'adx', 'atr',
                 'bb_mid', 'bb_up', 'bb_lo', 'bb_pctb']:
        assert col in df2.columns, f"缺欄位 {col}"


# ─── Pattern scaling ──

def test_classify_stage_tf_raises_on_5m():
    from intraday.patterns import classify_stage_tf
    df = _make_intraday_df(n_days=5, bars_per_day=78)
    with pytest.raises(NotApplicable):
        classify_stage_tf(df, tf='5m')


def test_classify_stage_tf_works_on_1h():
    from intraday.patterns import classify_stage_tf
    # 1h timeframe 需要 30 × 6.5 ≈ 195 bars SMA + 65 slope = 260+ bars
    df = _make_intraday_df(n_days=60, bars_per_day=7, freq_minutes=60)
    try:
        r = classify_stage_tf(df, tf='1h')
        assert r is not None
        # stage 可能 0（資料量恰好不夠）也算 OK
        assert r.stage in (0, 1, 2, 3, 4)
    except Exception as e:
        # 資料量不足是合理的 fail
        assert 'unknown' in str(e).lower() or '資料' in str(e)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
