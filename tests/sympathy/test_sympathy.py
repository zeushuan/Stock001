"""Sympathy module — Phase 1 單元測試"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
import pytest


def test_peer_mapping_load():
    from sympathy.peer_mapping import get_default_mapping
    pm = get_default_mapping()
    assert len(pm.list_groups()) > 0
    # AI_Storage_US 必含 MU
    assert 'MU' in pm.get_members('AI_Storage_US')


def test_peer_mapping_get_group_for_ticker():
    from sympathy.peer_mapping import get_default_mapping
    pm = get_default_mapping()
    groups = pm.get_group('MU')
    assert 'AI_Storage_US' in groups


def test_peer_mapping_leader_candidates_subset():
    """leader_candidates 必須是 members 子集（yaml 驗證已在 _validate 做）"""
    from sympathy.peer_mapping import get_default_mapping
    pm = get_default_mapping()
    for g in pm.list_groups():
        members = set(pm.get_members(g))
        for lc in pm.get_leader_candidates(g):
            assert lc in members, f'group {g} leader {lc} not in members'


def test_peer_mapping_settings():
    from sympathy.peer_mapping import get_default_mapping
    pm = get_default_mapping()
    assert pm.get_setting('min_correlation') is not None
    assert pm.get_setting('default_lookback_days') == 60


# ────────────────────────────────────────────────────────────────
# leader_detector
# ────────────────────────────────────────────────────────────────

def test_leader_detector_synthetic():
    """合成 df：一根大紅 K + 量增 → 應該識別為 leader"""
    from sympathy.leader_detector import _is_leader
    dates = pd.date_range('2024-01-01', periods=30, freq='B')
    o = pd.Series([100.0] * 30, index=dates)
    h = pd.Series([102.0] * 30, index=dates)
    l = pd.Series([98.0] * 30, index=dates)
    c = pd.Series([100.0] * 30, index=dates)
    v = pd.Series([1e6] * 30, index=dates)
    # 最後一日大漲 + 量增
    c.iloc[-1] = 108.0   # +8%
    h.iloc[-1] = 109.0
    l.iloc[-1] = 99.0
    v.iloc[-1] = 3e6     # 量增 3x

    df = pd.DataFrame({'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': v})
    res = _is_leader(df, dates[-1])
    assert res is not None
    assert res['return_pct'] > 0.05
    assert res['volume_ratio'] > 1.5


def test_leader_detector_rejects_no_volume():
    """大漲但量縮 → 不是 leader"""
    from sympathy.leader_detector import _is_leader
    dates = pd.date_range('2024-01-01', periods=30, freq='B')
    o = pd.Series([100.0] * 30, index=dates)
    h = pd.Series([102.0] * 30, index=dates)
    l = pd.Series([98.0] * 30, index=dates)
    c = pd.Series([100.0] * 30, index=dates)
    v = pd.Series([1e6] * 30, index=dates)
    c.iloc[-1] = 108.0
    h.iloc[-1] = 109.0
    l.iloc[-1] = 99.0
    v.iloc[-1] = 5e5    # 量縮

    df = pd.DataFrame({'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': v})
    assert _is_leader(df, dates[-1]) is None


def test_leader_detector_rejects_upper_pullback():
    """大漲但衝高拉回（收盤在下半部）→ 不是 leader"""
    from sympathy.leader_detector import _is_leader
    dates = pd.date_range('2024-01-01', periods=30, freq='B')
    o = pd.Series([100.0] * 30, index=dates)
    h = pd.Series([102.0] * 30, index=dates)
    l = pd.Series([98.0] * 30, index=dates)
    c = pd.Series([100.0] * 30, index=dates)
    v = pd.Series([1e6] * 30, index=dates)
    c.iloc[-1] = 106.0
    h.iloc[-1] = 115.0
    l.iloc[-1] = 100.0
    v.iloc[-1] = 3e6
    # close 106 vs (high+low)/2 = 107.5 → 在下半部

    df = pd.DataFrame({'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': v})
    assert _is_leader(df, dates[-1]) is None


# ────────────────────────────────────────────────────────────────
# laggard_scorer
# ────────────────────────────────────────────────────────────────

def test_score_laggards_synthetic(monkeypatch):
    """合成資料：leader 大漲，peer 強相關但今日落後 → 應該入列"""
    from sympathy import _data
    from sympathy.laggard_scorer import score_laggards

    dates = pd.date_range('2024-01-01', periods=80, freq='B')
    # 高度相關（同步起伏）
    leader_close = pd.Series(100 + np.cumsum(np.random.RandomState(42).randn(80)*0.5),
                                index=dates)
    peer_close = (leader_close * 0.5 +
                   pd.Series(np.random.RandomState(43).randn(80)*0.2,
                              index=dates))
    # 最後一日 leader 大漲，peer 落後
    leader_close.iloc[-1] *= 1.08    # +8%
    peer_close.iloc[-1] *= 1.01      # +1%
    # 確保 peer > 20MA
    peer_close.iloc[-1] = peer_close.iloc[-21:-1].mean() * 1.05

    leader_df = pd.DataFrame({
        'Open': leader_close, 'High': leader_close*1.01, 'Low': leader_close*0.99,
        'Close': leader_close, 'Volume': [1e6]*80,
    })
    peer_df = pd.DataFrame({
        'Open': peer_close, 'High': peer_close*1.01, 'Low': peer_close*0.99,
        'Close': peer_close, 'Volume': [1e6]*80,
    })

    fake = {'LEADER': leader_df, 'PEER': peer_df}
    monkeypatch.setattr(_data, 'load_history',
                          lambda t, **kw: fake.get(t))
    # 同時 patch in laggard_scorer module space
    import sympathy.laggard_scorer as ls_mod
    monkeypatch.setattr(ls_mod, 'load_history',
                          lambda t, n=90, a=None: fake.get(t))

    candidates = score_laggards('LEADER', ['LEADER', 'PEER'],
                                   as_of_date=dates[-1])
    # PEER 該入列
    assert any(c['ticker'] == 'PEER' for c in candidates), \
        f'PEER not in candidates: {candidates}'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
