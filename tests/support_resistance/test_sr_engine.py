"""Subsystem C — sr_engine 單元測試。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from support_resistance.sr_engine import (
    round_number_zones, fuse_zones, score_zones,
    detect_role_reversal, detect_sr_zones, sr_context_for_t3,
    latest_pivot_levels,
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

    def test_different_kinds_not_merged_when_far(self):
        """不同 kind 且中心遠 > atr×0.5 不該被合併。

        （中心近 ≤ atr×0.5 的情境見 test_cross_kind_merge_role_reversal,
         會被 v9.47.8 cross-kind merge 合成 role_reversal zone）
        """
        a = SRZone(kind='support', low=99, high=100, center=99.5,
                    touches=3, source='swing', last_touch_idx=10,
                    components={'_origins': [], '_source_set': {'swing'}})
        # 中心差 10.5 >> atr*0.5=0.5，不會 cross-kind 合併
        b = SRZone(kind='resistance', low=109.5, high=110.5, center=110,
                    touches=2, source='swing', last_touch_idx=15,
                    components={'_origins': [], '_source_set': {'swing'}})
        fused = fuse_zones([a, b], [], [], atr=1.0)
        assert len(fused) == 2
        kinds = {z.kind for z in fused}
        assert kinds == {'support', 'resistance'}

    def test_round_zones_classified_by_price(self):
        r = SRZone(kind='support', low=99.75, high=100.25, center=100,
                    touches=0, source='round')
        fused = fuse_zones([], [], [r], atr=0.5, current_price=95.0)
        # 100 > 現價 95 → resistance
        assert fused[0].kind == 'resistance'

    def test_round_far_from_center_dropped(self):
        """🆕 v9.47.7：round origin 距 zone center > atr×0.5 應被過濾。

        場景：寬 swing zone 涵蓋多個整數，只有近中心的才該算共振。
        swing [85, 105] center=95，atr=10 → merge_tol=10, filter_tol=5。
        - round 87  距中心 8  → 丟
        - round 95  距中心 0  → 保留
        - round 96  距中心 1  → 保留
        - round 103 距中心 8  → 丟
        """
        swing = SRZone(
            kind='resistance', low=85, high=105, center=95,
            touches=3, source='swing', last_touch_idx=10,
            components={'_origins': [{'kind': 'swing', 'touches': 3,
                                       'center': 95}]},
        )
        rounds = [
            SRZone(kind='resistance', low=86.6, high=87.4, center=87.0,
                    touches=0, source='round', last_touch_idx=-1,
                    components={'_origins': [{'kind': 'round', 'level': 87.0}]}),
            SRZone(kind='resistance', low=94.6, high=95.4, center=95.0,
                    touches=0, source='round', last_touch_idx=-1,
                    components={'_origins': [{'kind': 'round', 'level': 95.0}]}),
            SRZone(kind='resistance', low=95.6, high=96.4, center=96.0,
                    touches=0, source='round', last_touch_idx=-1,
                    components={'_origins': [{'kind': 'round', 'level': 96.0}]}),
            SRZone(kind='resistance', low=102.6, high=103.4, center=103.0,
                    touches=0, source='round', last_touch_idx=-1,
                    components={'_origins': [{'kind': 'round', 'level': 103.0}]}),
        ]
        # atr=10: tol_merge = 10 (allows all to merge), filter_tol = 5
        fused = fuse_zones([swing], [], rounds, atr=10.0)
        assert len(fused) == 1, f'expected 1 fused zone, got {len(fused)}'
        z = fused[0]
        origins = z.components.get('_origins', [])
        round_origins = [o for o in origins if o.get('kind') == 'round']
        # 應該只剩 R95, R96（距 center=95 ≤ 5）
        levels = sorted(o['level'] for o in round_origins)
        assert levels == [95.0, 96.0], f'expected [95.0, 96.0], got {levels}'

    def test_round_only_zone_not_filtered(self):
        """純 round zone 不該被過濾（自己的 center 就是 level）。"""
        rounds = [
            SRZone(kind='resistance', low=99.6, high=100.4, center=100.0,
                    touches=0, source='round', last_touch_idx=-1,
                    components={'_origins': [{'kind': 'round', 'level': 100.0}]}),
        ]
        fused = fuse_zones([], [], rounds, atr=2.0)
        assert len(fused) == 1
        origins = fused[0].components.get('_origins', [])
        assert len(origins) == 1
        assert origins[0].get('level') == 100.0

    def test_cross_kind_merge_role_reversal(self):
        """🆕 v9.47.8：S 和 R 中心距 ≤ atr×0.5 應合併成 role_reversal zone。

        場景：歷史上 75 既是壓力又是支撐（價格往下測過幾次又往上反彈幾次）
        - R 81 swing center=76.52
        - S 58 profile+round center=75.02
        - 中心差 1.50，atr=10 → cross_tol=5 → 合併
        - current_price=85 → 合併後 center 落在現價之下 → kind=support
        - role_reversal=True
        """
        r = SRZone(
            kind='resistance', low=73, high=77, center=76.52,
            touches=3, source='swing', last_touch_idx=10,
            components={'_origins': [{'kind': 'swing', 'touches': 3,
                                       'center': 76.52}],
                         '_source_set': {'swing'}},
        )
        s = SRZone(
            kind='support', low=72, high=76, center=75.02,
            touches=1, source='fused', last_touch_idx=15,
            components={'_origins': [
                {'kind': 'profile', 'hvn_low': 72, 'hvn_high': 76},
                {'kind': 'round', 'level': 75.0},
            ], '_source_set': {'profile', 'round'}},
        )
        fused = fuse_zones([s], [], [], atr=10.0, current_price=85.0)
        # 加 r 進 swing list (因為 r 來自 swing 來源)
        fused = fuse_zones([s], [], [], atr=10.0, current_price=85.0)
        # 用正確的呼叫：把 r 當 swing_zones，s 當 profile_zones
        fused = fuse_zones([r], [s], [], atr=10.0, current_price=85.0)
        # cross-kind 合併後應只剩 1 個 zone
        assert len(fused) == 1, f'expected 1 zone after cross-kind merge, got {len(fused)}'
        z = fused[0]
        # current_price=85, merged center ~75-76 < 85 → support
        assert z.kind == 'support', f'expected support, got {z.kind}'
        # role_reversal 旗標
        assert z.role_reversal is True
        # bounds 是 union
        assert z.low == 72
        assert z.high == 77
        # source_set 含兩源
        assert {'swing', 'profile', 'round'}.issubset(
            z.components.get('_source_set', set()))

    def test_cross_kind_no_merge_when_far(self):
        """中心距 > atr×0.5 不該合併。"""
        r = SRZone(
            kind='resistance', low=98, high=102, center=100,
            touches=2, source='swing', last_touch_idx=10,
            components={'_origins': [{'kind': 'swing', 'touches': 2,
                                       'center': 100}],
                         '_source_set': {'swing'}},
        )
        s = SRZone(
            kind='support', low=48, high=52, center=50,
            touches=2, source='swing', last_touch_idx=15,
            components={'_origins': [{'kind': 'swing', 'touches': 2,
                                       'center': 50}],
                         '_source_set': {'swing'}},
        )
        fused = fuse_zones([r, s], [], [], atr=10.0, current_price=75.0)
        # 兩個保留（差 50 >> tol=5）
        assert len(fused) == 2
        kinds = {z.kind for z in fused}
        assert kinds == {'support', 'resistance'}

    def test_source_label_reverts_when_round_filtered(self):
        """過濾後只剩單一 source，zone.source 應從 'fused' 回到單一名稱。

        合併後 center = (47×1 + 55×4) / 5 = 53.4，
        round 47 距中心 6.4 > filter_tol=5 → 丟。
        過濾後只剩 swing，source 從 'fused' 回到 'swing'。
        """
        swing = SRZone(
            kind='support', low=40, high=70, center=55,
            touches=4, source='swing', last_touch_idx=20,
            components={'_origins': [{'kind': 'swing', 'touches': 4,
                                       'center': 55}]},
        )
        round_far = SRZone(
            kind='support', low=46.6, high=47.4, center=47.0,
            touches=0, source='round', last_touch_idx=-1,
            components={'_origins': [{'kind': 'round', 'level': 47.0}]},
        )
        fused = fuse_zones([swing], [], [round_far], atr=10.0)
        z = next((x for x in fused if x.touches > 0), fused[0])
        assert z.source == 'swing', f'expected swing, got {z.source}'
        assert z.components['_source_set'] == {'swing'}

    def test_runaway_chain_prevented(self):
        """連續 5 個 swing zone 各寬 1，間隔 2，atr=2
        → 沒寬度上限會合併成 1 個寬 13 的巨大 zone；
        有 MAX_ZONE_WIDTH_ATR_MULT=3 → 寬度上限 = 6，應保留多個 zone。"""
        from support_resistance.params import MAX_ZONE_WIDTH_ATR_MULT
        zones = []
        for i, ctr in enumerate([100, 102, 104, 106, 108]):
            zones.append(SRZone(
                kind='support', low=ctr - 0.5, high=ctr + 0.5, center=ctr,
                touches=2, source='swing', last_touch_idx=i * 10,
            ))
        fused = fuse_zones(zones, [], [], atr=2.0)
        # 每個 zone 寬度上限 = atr × MAX_ZONE_WIDTH_ATR_MULT = 6
        for z in fused:
            assert z.width() <= 2.0 * MAX_ZONE_WIDTH_ATR_MULT + 1e-9, \
                f'zone width {z.width()} exceeds cap'
        # 應保留 ≥ 2 個 zone（不是巨大 1 個）
        assert len(fused) >= 2


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


class TestLatestPivotLevels:
    """LuxAlgo-style 最近 pivot R/S 測試。"""

    def test_double_top_bottom_finds_both(self, synth_double_top_bottom):
        # 資料在 100-110 擺盪
        levels = latest_pivot_levels(
            synth_double_top_bottom, current_price=105.0, swing_window=3)
        # 最近 pivot high above 105 應 ≥ 108
        assert levels['resistance'] is not None
        assert levels['resistance'] >= 108
        # 最近 pivot low below 105 應 ≤ 102
        assert levels['support'] is not None
        assert levels['support'] <= 102

    def test_breakout_no_resistance(self, synth_double_top_bottom):
        # 現價遠高於資料最高 → 無 pivot high above → R=None
        levels = latest_pivot_levels(
            synth_double_top_bottom, current_price=200.0, swing_window=3)
        assert levels['resistance'] is None
        # 但有 support（資料所有 low 都在 200 之下）
        assert levels['support'] is not None

    def test_breakdown_no_support(self, synth_double_top_bottom):
        levels = latest_pivot_levels(
            synth_double_top_bottom, current_price=10.0, swing_window=3)
        assert levels['support'] is None
        assert levels['resistance'] is not None

    def test_returns_latest_pivot_when_multiple(self):
        """有多個 pivot 時應該回最後一個（最近）。"""
        import numpy as np
        import pandas as pd
        n = 80
        # 兩個 pivot high: idx 20 高度 110，idx 60 高度 115
        closes = np.full(n, 100.0)
        highs = np.full(n, 101.0)
        lows = np.full(n, 99.0)
        highs[20] = 110
        highs[60] = 115
        lows[40] = 88
        lows[70] = 90
        df = pd.DataFrame({
            'High': highs, 'Low': lows, 'Close': closes,
            'Volume': np.full(n, 1000.0),
        })
        levels = latest_pivot_levels(df, current_price=105.0, swing_window=5)
        # 最近 pivot high above 105 應是 idx 60 = 115（不是 idx 20 = 110）
        assert levels['resistance'] == 115
        assert levels['resistance_idx'] == 60
        # 最近 pivot low below 105 應是 idx 70 = 90（不是 idx 40 = 88）
        assert levels['support'] == 90
        assert levels['support_idx'] == 70

    def test_short_data_returns_empty(self):
        import pandas as pd
        df = pd.DataFrame({
            'High': [10] * 5, 'Low': [9] * 5,
            'Close': [9.5] * 5, 'Volume': [100] * 5,
        })
        levels = latest_pivot_levels(df, current_price=10.0, swing_window=5)
        assert levels['resistance'] is None
        assert levels['support'] is None


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
