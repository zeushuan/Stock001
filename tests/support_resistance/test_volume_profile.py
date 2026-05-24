"""Subsystem B — volume_profile 單元測試。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from support_resistance.volume_profile import (
    filter_rth, compute_profile, profile_to_zones, _merge_consecutive,
)
from support_resistance.types import VolumeProfile, SRZone


class TestFilterRTH:
    def test_passthrough_when_no_datetime_index(self):
        df = pd.DataFrame({'High': [1], 'Low': [1], 'Volume': [1]})
        out = filter_rth(df)
        assert len(out) == 1

    def test_keeps_rth_only_naive(self):
        idx = pd.DatetimeIndex([
            '2024-01-02 09:00',    # pre-market — drop
            '2024-01-02 10:00',    # RTH — keep
            '2024-01-02 15:59',    # RTH — keep
            '2024-01-02 16:00',    # after-hours — drop（< 16:00 exclusive）
            '2024-01-02 17:00',    # after-hours — drop
        ])
        df = pd.DataFrame({'High': [1]*5, 'Low': [1]*5, 'Volume': [1]*5}, index=idx)
        out = filter_rth(df)
        # 09:30-16:00 預設 [09:30, 16:00) → keep 10:00, 15:59 共 2 根
        assert len(out) == 2


class TestComputeProfile:
    def test_returns_none_for_short_data(self):
        df = pd.DataFrame({
            'High': [10] * 5, 'Low': [9] * 5, 'Volume': [100] * 5,
        })
        assert compute_profile(df, rth_filter=False) is None

    def test_volume_spike_finds_poc(self, synth_volume_spike_at_105):
        prof = compute_profile(
            synth_volume_spike_at_105, n_bins=50, rth_filter=False,
        )
        assert prof is not None
        # POC 應落在 104-106
        assert 104 <= prof.poc <= 106, f'POC={prof.poc}, expected ~105'

    def test_value_area_covers_at_least_70pct(self, synth_volume_spike_at_105):
        prof = compute_profile(
            synth_volume_spike_at_105, n_bins=50,
            value_area_pct=0.70, rth_filter=False,
        )
        assert prof is not None
        # VA 應該包含 POC
        assert prof.val <= prof.poc <= prof.vah
        # VA 應該比 [min, max] 窄
        full_range = max(prof.bin_edges) - min(prof.bin_edges)
        va_range = prof.vah - prof.val
        assert va_range < full_range

    def test_total_volume_consistent(self, synth_volume_spike_at_105):
        prof = compute_profile(
            synth_volume_spike_at_105, n_bins=50, rth_filter=False,
        )
        total_bins = prof.total_volume()
        total_df = float(synth_volume_spike_at_105['Volume'].sum())
        # 因為近似分配可能有小數誤差，允許 1% 容差
        assert abs(total_bins - total_df) / total_df < 0.05


class TestMergeConsecutive:
    def test_simple_run(self):
        edges = np.array([0, 1, 2, 3, 4, 5])
        vols  = np.array([0.1, 0.9, 1.0, 0.2, 0.8])
        zones = _merge_consecutive(vols, edges, lambda v: v >= 0.5)
        # bins 1,2 = high；bin 4 = high → 兩段
        assert len(zones) == 2
        assert zones[0] == (1.0, 3.0)
        assert zones[1] == (4.0, 5.0)


class TestProfileToZones:
    def test_classifies_by_current_price(self, synth_volume_spike_at_105):
        prof = compute_profile(
            synth_volume_spike_at_105, n_bins=50, rth_filter=False,
        )
        zones_below = profile_to_zones(prof, current_price=200.0, bar_count=200)
        # 全部 HVN 都低於現價 200 → 全 support
        assert all(z.kind == 'support' for z in zones_below)

        zones_above = profile_to_zones(prof, current_price=50.0, bar_count=200)
        # 全部 HVN 都高於現價 50 → 全 resistance
        assert all(z.kind == 'resistance' for z in zones_above)

    def test_none_profile_returns_empty(self):
        assert profile_to_zones(None) == []
