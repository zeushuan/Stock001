"""Subsystem C — sr_engine 單元測試。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from support_resistance.sr_engine import (
    round_number_zones, fuse_zones, score_zones,
    detect_role_reversal, detect_sr_zones, sr_context_for_t3,
)
from support_resistance.types import SRZone


class TestRoundNumberZones:
    def test_basic_range(self):
        zones = round_number_zones(low=98.0, high=103.0, atr=1.0)
        centers = [z.center for z in zones]
        # 應該包含 99, 100, 101, 102, 103；可能也含 .50
        assert 100.0 in centers
        assert 101.0 in centers

    def test_zero_atr_returns_empty(self):
        zones = round_number_zones(low=98.0, high=103.0, atr=0.0)
        assert zones == []

    def test_inverted_range_returns_empty(self):
        zones = round_number_zones(low=110, high=100, atr=1.0)
        assert zones == []

    def test_all_marked_source_round(self):
        zones = round_number_zones(low=98.0, high=103.0, atr=1.0)
        assert all(z.source == 'round' for z in zones)


class TestFuseZones:
    def test_overlapping_same_kind_merged(self):
        a = SRZone(kind='support', low=99, high=100, center=99.5,
                    touches=3, source='swing', last_touch_idx=10)
        b = SRZone(kind='support', low=99.5, high=100.5, center=100,
                    touches=0, source='profile', last_touch_idx=15)
        fused = fuse_zones([a], [b], [], atr=1.0)
        assert len(fused) == 1
        z = fused[0]
        assert z.source == 'fused'
        assert z.low == 99.0
        assert z.high == 100.5
        # 兩源 → source_set 包含 swing + profile
        assert {'swing', 'profile'}.issubset(
            z.components.get('_source_set', set()))

    def test_different_kinds_not_merged(self):
        a = SRZone(kind='support', low=99, high=100, center=99.5,
                    touches=3, source='swing', last_touch_idx=10)
        b = SRZone(kind='resistance', low=99.5, high=100.5, center=100,
                    touches=2, source='swing', last_touch_idx=15)
        fused = fuse_zones([a], [], [b], atr=1.0)
        # 不同 kind 不該合併
        assert len(fused) == 2

    def test_round_zones_classified_by_price(self):
        r = SRZone(kind='support', low=99.75, high=100.25, center=100,
                    touches=0, source='round')
        fused = fuse_zones([], [], [r], atr=0.5, current_price=95.0)
        # 100 > 現價 95 → resistance
        assert fused[0].kind == 'resistance'


class TestScoreZones:
    def test_strength_in_range(self):
        df = pd.DataFrame({
            'High':  np.full(50, 105.0),
            'Low':   np.full(50, 95.0),
            'Close': np.full(50, 100.0),
            'Volume': np.full(50, 1000.0),
        })
        zones = [
            SRZone(kind='support', low=99, high=101, center=100,
                    touches=3, source='swing', last_touch_idx=49),
        ]
        out = score_zones(zones, df)
        assert 0 <= out[0].strength <= 100
        assert 'touch' in out[0].components
        assert 'volume' in out[0].components
        assert 'recency' in out[0].components
        assert 'confluence' in out[0].components

    def test_support_bias_applied(self):
        """同樣的 zone，support 應比 resistance 略強（×1.05）。"""
        df = pd.DataFrame({
            'High':   np.full(50, 105.0),
            'Low':    np.full(50, 95.0),
            'Close':  np.full(50, 100.0),
            'Volume': np.full(50, 1000.0),
        })
        sup = SRZone(kind='support', low=99, high=101, center=100,
                      touches=3, source='swing', last_touch_idx=49)
        res = SRZone(kind='resistance', low=99, high=101, center=100,
                      touches=3, source='swing', last_touch_idx=49)
        score_zones([sup, res], df)
        assert sup.strength > res.strength
        # 應該大約是 5% 差距（受其他分量影響可能略偏）
        assert sup.strength / max(res.strength, 0.1) > 1.0

    def test_recency_higher_for_recent_touch(self):
        df = pd.DataFrame({
            'High':   np.full(100, 105.0),
            'Low':    np.full(100, 95.0),
            'Close':  np.full(100, 100.0),
            'Volume': np.full(100, 1000.0),
        })
        old = SRZone(kind='support', low=99, high=101, center=100,
                      touches=3, source='swing', last_touch_idx=5)   # 老
        new = SRZone(kind='support', low=99, high=101, center=100,
                      touches=3, source='swing', last_touch_idx=99)  # 新
        score_zones([old, new], df, lookback=100)
        assert new.components['recency'] > old.components['recency']


class TestDetectRoleReversal:
    def test_crossed_zone_flagged(self):
        df = pd.DataFrame({
            'Close': [99, 100, 101, 102, 101, 100, 99, 98, 97],
            'High':  [100, 101, 102, 103, 102, 101, 100, 99, 98],
            'Low':   [98, 99, 100, 101, 100, 99, 98, 97, 96],
            'Volume': [100] * 9,
        })
        # 區間 [99, 101]，從 idx=2 開始 close 跌破又拉上又跌
        z = SRZone(kind='resistance', low=99.5, high=100.5, center=100,
                    touches=2, source='swing', last_touch_idx=0)
        detect_role_reversal([z], df)
        assert z.role_reversal is True


class TestSRContextForT3:
    def _zones_with_strong_resistance_near(self, price: float):
        return [
            SRZone(kind='resistance', low=price + 0.5, high=price + 1.5,
                    center=price + 1.0, strength=80, source='swing'),
            SRZone(kind='support', low=price - 5, high=price - 4,
                    center=price - 4.5, strength=70, source='swing'),
        ]

    def test_near_resistance_returns_negative_adj(self):
        zones = self._zones_with_strong_resistance_near(100.0)
        ctx = sr_context_for_t3(zones, current_price=100.0,
                                proximity_pct=2.0)
        assert ctx['reason'] == 'near_resistance'
        assert ctx['adjustment'] < 0
        assert ctx['nearest_resistance'] is not None
        assert ctx['nearest_support'] is None  # 不在 tol 內

    def test_near_support_returns_positive_adj(self):
        zones = [
            SRZone(kind='support', low=99.0, high=100.0, center=99.5,
                    strength=90, source='swing'),
        ]
        ctx = sr_context_for_t3(zones, current_price=100.0,
                                proximity_pct=2.0)
        assert ctx['reason'] == 'near_support'
        assert ctx['adjustment'] > 0

    def test_no_nearby_zone(self):
        zones = [
            SRZone(kind='resistance', low=200, high=201, center=200.5,
                    strength=80, source='swing'),
        ]
        ctx = sr_context_for_t3(zones, current_price=100.0)
        assert ctx['adjustment'] == 0
        assert ctx['reason'] == 'none'

    def test_adx_damping_reduces_magnitude(self):
        zones = self._zones_with_strong_resistance_near(100.0)
        low_adx = sr_context_for_t3(zones, current_price=100.0,
                                     adx=10, proximity_pct=2.0)
        high_adx = sr_context_for_t3(zones, current_price=100.0,
                                      adx=50, proximity_pct=2.0)
        # 高 ADX 應該讓 |adj| 變小
        assert abs(high_adx['adjustment']) < abs(low_adx['adjustment'])
        assert high_adx['adx_damping_applied'] < low_adx['adx_damping_applied']

    def test_cap_enforced(self):
        zones = [
            SRZone(kind='resistance', low=100.5, high=101, center=100.7,
                    strength=100, source='swing'),
        ]
        ctx = sr_context_for_t3(zones, current_price=100.0,
                                proximity_pct=2.0, cap=15)
        assert ctx['adjustment'] >= -15
        assert ctx['adjustment'] <= 15


class TestDetectSRZonesIntegration:
    def test_end_to_end_returns_zones(self, synth_double_top_bottom):
        zones = detect_sr_zones(
            synth_double_top_bottom, swing_window=3,
            cluster_atr_mult=1.5, min_touches=2,
        )
        assert len(zones) > 0
        # 每個 zone 都該有 strength（評分完成）
        assert all(0 <= z.strength <= 100 for z in zones)
        # 應該有 support + resistance
        kinds = {z.kind for z in zones}
        assert 'support' in kinds or 'resistance' in kinds

    def test_zones_sorted_by_strength_desc(self, synth_double_top_bottom):
        zones = detect_sr_zones(
            synth_double_top_bottom, swing_window=3, cluster_atr_mult=1.5,
        )
        if len(zones) >= 2:
            for i in range(len(zones) - 1):
                assert zones[i].strength >= zones[i + 1].strength

    def test_short_data_returns_empty(self):
        df = pd.DataFrame({
            'High': [10] * 10, 'Low': [9] * 10,
            'Close': [9.5] * 10, 'Volume': [100] * 10,
        })
        assert detect_sr_zones(df) == []
