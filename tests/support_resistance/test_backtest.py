"""backtest.py 單元測試。

合成 OHLCV 帶有「已知會反轉」的觸及，驗證 backtest_one 能正確辨識。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from support_resistance.backtest import (
    TouchEvent, backtest_one, aggregate, format_report,
)
from support_resistance.types import SRZone


@pytest.fixture
def synth_bounce_at_100() -> pd.DataFrame:
    """價在 90-110 區間擺盪，多次在 ~100 反彈
    → 應該有 support@100 zone，且觸及後 close 上漲。"""
    np.random.seed(42)
    n = 200
    # 反覆 V 形：50 bar 一輪上下
    t = np.arange(n)
    base = 100 + 8 * np.sin(2 * np.pi * t / 30)   # 92 ~ 108
    noise = np.random.normal(0, 0.5, n)
    close = base + noise
    high = close + np.abs(np.random.normal(0.5, 0.2, n))
    low = close - np.abs(np.random.normal(0.5, 0.2, n))
    open_ = close + np.random.normal(0, 0.2, n)
    vol = np.full(n, 1000.0)
    idx = pd.date_range('2024-01-01', periods=n, freq='D')
    return pd.DataFrame({
        'Open': open_, 'High': high, 'Low': low, 'Close': close, 'Volume': vol,
    }, index=idx)


class TestBacktestOne:
    def test_empty_df_returns_empty(self):
        df = pd.DataFrame({
            'High': [10] * 10, 'Low': [9] * 10,
            'Close': [9.5] * 10, 'Volume': [100] * 10,
        })
        evts = backtest_one(df, warmup=60)
        assert evts == []

    def test_bouncing_data_generates_events(self, synth_bounce_at_100):
        """V 形反覆擺盪 → 應該有觸及事件。
        命中率因觸及分布（多數在 mid-cycle 而非 peak/trough）而有限,
        但應該明顯高於 0（≥ 25% 是合理的下限基準）。"""
        evts = backtest_one(
            synth_bounce_at_100, symbol='SYN',
            warmup=60, reaction_window=5,
            reversal_pct=1.0, min_strength=20,
        )
        assert len(evts) > 0, '預期至少有一次觸及'
        hit_rate = sum(1 for e in evts if e.reversed) / len(evts)
        assert hit_rate >= 0.25, f'V 形反彈資料命中率應 ≥ 25%，實際 {hit_rate:.1%}'
        # 平均反轉幅度應 > 0
        mean_rev = sum(e.reversal_pct for e in evts) / len(evts)
        assert mean_rev > 0, f'平均反轉幅度應 > 0, 實際 {mean_rev:.2f}%'

    def test_no_lookahead(self, synth_bounce_at_100):
        """SR 用 [0:j] 算 zone；event 在 bar j；反轉在 [j+1:j+1+W] 看。
        如果有 lookahead bug，會在 warmup 之前就有 event。"""
        evts = backtest_one(synth_bounce_at_100, warmup=60)
        if evts:
            assert min(e.bar_idx for e in evts) >= 60

    def test_event_has_all_fields(self, synth_bounce_at_100):
        evts = backtest_one(synth_bounce_at_100, min_strength=20)
        if evts:
            e = evts[0]
            assert e.symbol is not None
            assert e.zone_kind in ('support', 'resistance')
            assert e.zone_source in ('swing', 'profile', 'round', 'fused')
            assert 0 <= e.zone_strength <= 100
            assert e.zone_source_count >= 1
            assert e.touched_price > 0
            assert isinstance(e.reversed, bool)


class TestAggregate:
    def test_empty_returns_zero(self):
        stats = aggregate([])
        assert stats['n_events'] == 0
        assert stats['overall_hit_rate'] is None

    def test_basic_aggregation(self):
        evts = [
            TouchEvent('A', 50, 'support', 'swing', 60, 1, 100.0, 2.0, True),
            TouchEvent('A', 55, 'support', 'fused', 80, 2, 99.0, 1.5, True),
            TouchEvent('A', 60, 'resistance', 'swing', 50, 1, 110.0, 0.5, False),
        ]
        stats = aggregate(evts)
        assert stats['n_events'] == 3
        assert abs(stats['overall_hit_rate'] - 2/3) < 1e-9

        # by_kind
        assert stats['by_kind']['support']['n'] == 2
        assert stats['by_kind']['support']['hit_rate'] == 1.0
        assert stats['by_kind']['resistance']['n'] == 1
        assert stats['by_kind']['resistance']['hit_rate'] == 0.0

        # by_source_count
        assert stats['by_source_count'][1]['n'] == 2
        assert stats['by_source_count'][2]['n'] == 1

        # by_strength
        assert stats['by_strength']['mid(50-75)']['n'] == 2  # 60, 50
        assert stats['by_strength']['high(75-100)']['n'] == 1  # 80


class TestFormatReport:
    def test_includes_headers(self):
        evts = [
            TouchEvent('A', 50, 'support', 'swing', 60, 1, 100.0, 2.0, True),
        ]
        text = format_report(aggregate(evts), title='Test')
        assert 'Test' in text
        assert 'By kind' in text
        assert 'By source count' in text
        assert 'By strength' in text

    def test_empty_no_crash(self):
        text = format_report(aggregate([]))
        assert 'no events' in text or 'n_events: 0' in text
