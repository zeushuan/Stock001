"""Subsystem A — swing_cluster 單元測試。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from support_resistance.swing_cluster import (
    compute_atr, find_pivots, cluster_pivots, detect_swing_zones,
)
from support_resistance.types import Pivot


class TestComputeATR:
    def test_returns_zero_when_too_few_bars(self):
        df = pd.DataFrame({
            'High': [10, 11], 'Low': [9, 10], 'Close': [9.5, 10.5],
        })
        assert compute_atr(df, period=14) == 0.0

    def test_constant_range(self):
        """每根 H-L 都 = 2，ATR 應 ≈ 2。"""
        n = 30
        df = pd.DataFrame({
            'High':  [12.0] * n,
            'Low':   [10.0] * n,
            'Close': [11.0] * n,
        })
        atr = compute_atr(df, period=14)
        assert abs(atr - 2.0) < 0.1


class TestFindPivots:
    def test_finds_obvious_high(self):
        """中央一根 high 比左右各 2 根都高 → 該為 pivot。"""
        df = pd.DataFrame({
            'High':   [10, 10, 10, 10, 15, 10, 10, 10, 10],
            'Low':    [9,  9,  9,  9,  14, 9,  9,  9,  9],
            'Volume': [100] * 9,
        })
        highs, lows = find_pivots(df, swing_window=2)
        assert len(highs) == 1
        assert highs[0].idx == 4
        assert highs[0].price == 15
        assert highs[0].kind == 'high'

    def test_finds_obvious_low(self):
        df = pd.DataFrame({
            'High':   [10] * 9,
            'Low':    [9,  9,  9,  9,  5,  9,  9,  9,  9],
            'Volume': [100] * 9,
        })
        highs, lows = find_pivots(df, swing_window=2)
        assert len(lows) == 1
        assert lows[0].idx == 4
        assert lows[0].price == 5

    def test_empty_for_short_data(self):
        df = pd.DataFrame({
            'High': [10, 11], 'Low': [9, 10], 'Volume': [100, 100],
        })
        highs, lows = find_pivots(df, swing_window=5)
        assert highs == [] and lows == []

    def test_volume_attached(self):
        df = pd.DataFrame({
            'High':   [10, 10, 15, 10, 10],
            'Low':    [9,  9,  14, 9,  9],
            'Volume': [100, 200, 999, 200, 100],
        })
        highs, _ = find_pivots(df, swing_window=2)
        assert highs[0].volume == 999


class TestClusterPivots:
    def test_close_pivots_merged(self):
        """價格 100、100.5、101 用 eps=2 應併成 1 個 zone。"""
        pivots = [
            Pivot(idx=1, price=100.0, kind='high', volume=100),
            Pivot(idx=5, price=100.5, kind='high', volume=200),
            Pivot(idx=9, price=101.0, kind='high', volume=300),
        ]
        zones = cluster_pivots(pivots, eps=2.0, kind='high', min_touches=2)
        assert len(zones) == 1
        z = zones[0]
        assert z.kind == 'resistance'
        assert z.touches == 3
        assert z.low == 100.0
        assert z.high == 101.0
        # 量能加權中心 = (100×100 + 100.5×200 + 101×300) / 600
        expected_center = (100 * 100 + 100.5 * 200 + 101 * 300) / 600
        assert abs(z.center - expected_center) < 1e-6

    def test_far_pivots_separate(self):
        pivots = [
            Pivot(idx=1, price=100.0, kind='low', volume=100),
            Pivot(idx=5, price=110.0, kind='low', volume=100),
        ]
        zones = cluster_pivots(pivots, eps=2.0, kind='low', min_touches=1)
        assert len(zones) == 2
        assert all(z.kind == 'support' for z in zones)

    def test_below_min_touches_dropped(self):
        pivots = [Pivot(idx=1, price=100.0, kind='high', volume=100)]
        zones = cluster_pivots(pivots, eps=2.0, kind='high', min_touches=2)
        assert zones == []

    def test_zero_eps_returns_empty(self):
        pivots = [Pivot(idx=1, price=100.0, kind='high', volume=100)]
        zones = cluster_pivots(pivots, eps=0.0, kind='high', min_touches=1)
        assert zones == []


class TestDetectSwingZonesIntegration:
    def test_double_top_bottom_finds_both(self, synth_double_top_bottom):
        zones = detect_swing_zones(
            synth_double_top_bottom, swing_window=3,
            cluster_atr_mult=1.5, min_touches=2,
        )
        kinds = {z.kind for z in zones}
        assert 'resistance' in kinds, f'no resistance found: {zones}'
        assert 'support' in kinds, f'no support found: {zones}'
        # resistance 中心應落在 [108, 112]，支撐在 [98, 102]
        res = [z for z in zones if z.kind == 'resistance']
        sup = [z for z in zones if z.kind == 'support']
        assert any(108 <= z.center <= 112 for z in res), \
            f'no resistance near 110: {[z.center for z in res]}'
        assert any(98 <= z.center <= 102 for z in sup), \
            f'no support near 100: {[z.center for z in sup]}'

    def test_lookback_truncates(self, synth_double_top_bottom):
        # 用 lookback=40 應該只看末 40 根
        zones_short = detect_swing_zones(
            synth_double_top_bottom, swing_window=3, lookback=40,
        )
        zones_full = detect_swing_zones(
            synth_double_top_bottom, swing_window=3, lookback=None,
        )
        # 完整資料應該找到 ≥ 截短後的觸及次數合計
        assert sum(z.touches for z in zones_full) >= sum(
            z.touches for z in zones_short)
