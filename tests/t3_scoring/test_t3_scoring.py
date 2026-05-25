"""t3_scoring 單元測試。

涵蓋：
  - compute_t3_confidence: 5 個子項各種命中組合
  - compute_t3_pullback_days: RSI 連續 <50 天數
  - compute_t4_rising_days: RSI <35 連續上升天數
  - technical_gate: Wyckoff phase 判別
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from t3_scoring import (
    compute_t3_confidence,
    compute_t3_pullback_days,
    compute_t4_rising_days,
    technical_gate,
    PHASE_ACCUMULATION, PHASE_NEUTRAL, PHASE_DISTRIBUTION,
)


class TestComputeT3Confidence:
    def test_all_hits_strong_bull(self):
        """五項全中：close>EMA20 + 兩條都升 + EMA5>EMA20 + 雙升 = 5/5"""
        score, hits = compute_t3_confidence(
            close_now=105, ema5_now=104, ema20_now=100,
            ema5_5d_ago=101, ema20_5d_ago=98,
        )
        assert score == 5
        assert 'close>EMA20' in hits
        assert 'EMA20上升' in hits
        assert 'EMA5上升' in hits
        assert 'EMA5>EMA20' in hits
        assert '雙均線都升' in hits

    def test_no_hits_strong_bear(self):
        """全空：close<EMA20、兩條都跌、EMA5<EMA20 = 0/5"""
        score, hits = compute_t3_confidence(
            close_now=95, ema5_now=96, ema20_now=100,
            ema5_5d_ago=98, ema20_5d_ago=102,
        )
        assert score == 0
        assert hits == []

    def test_partial_close_above_only(self):
        """只有 close > EMA20，其他都不滿足"""
        score, hits = compute_t3_confidence(
            close_now=105, ema5_now=98, ema20_now=100,
            ema5_5d_ago=99, ema20_5d_ago=101,
        )
        # close>EMA20 (1) ✓
        # EMA20 5d down ✗
        # EMA5 5d down ✗
        # EMA5 (98) < EMA20 (100) ✗
        # 雙升 ✗
        assert score == 1
        assert hits == ['close>EMA20']

    def test_none_inputs_safe(self):
        """None 輸入不爆炸，回 0 分"""
        score, hits = compute_t3_confidence(None, None, None, None, None)
        assert score == 0
        assert hits == []

    def test_partial_none(self):
        """部分 None 應該跳過該檢查，其他能算的還是算"""
        # close 有，ema20 有 → close>EMA20 應該能判定
        score, hits = compute_t3_confidence(
            close_now=105, ema5_now=None, ema20_now=100,
            ema5_5d_ago=None, ema20_5d_ago=None,
        )
        assert score == 1
        assert hits == ['close>EMA20']


class TestComputeT3PullbackDays:
    def test_continuous_below_50(self):
        """RSI [60, 45, 40, 35, 30] → 最後 4 天連續 <50"""
        rsi = pd.Series([60, 45, 40, 35, 30])
        assert compute_t3_pullback_days(rsi) == 4

    def test_latest_above_50_returns_zero(self):
        """最新 RSI ≥ 50 → 0"""
        rsi = pd.Series([60, 40, 30, 55])
        assert compute_t3_pullback_days(rsi) == 0

    def test_all_below_50(self):
        rsi = pd.Series([40, 35, 30, 25, 20])
        assert compute_t3_pullback_days(rsi) == 5

    def test_empty(self):
        assert compute_t3_pullback_days(pd.Series([], dtype=float)) == 0

    def test_with_nan(self):
        """NaN 應被 dropna 過濾"""
        rsi = pd.Series([60, np.nan, 40, 35, np.nan, 30])
        # dropna 後 [60, 40, 35, 30] → 最後 3 天 <50
        assert compute_t3_pullback_days(rsi) == 3

    def test_array_input(self):
        """支援 array-like 輸入"""
        assert compute_t3_pullback_days([60, 45, 40, 35, 30]) == 4


class TestComputeT4RisingDays:
    def test_below_35_rising(self):
        """RSI [20, 25, 28, 31] 連續上升且最新 < 32 → 4 天
        （函式邏輯：arr[-1] >= 32 直接 0，所以最後一日必須 < 32）"""
        rsi = pd.Series([20, 25, 28, 31])
        assert compute_t4_rising_days(rsi) == 4

    def test_latest_above_32_returns_zero(self):
        """最新 RSI ≥ 32 → 直接 0（不在空頭區）"""
        rsi = pd.Series([20, 25, 30, 35])
        assert compute_t4_rising_days(rsi) == 0

    def test_decreasing_returns_only_latest(self):
        """RSI 不連續上升 → 只算當天"""
        rsi = pd.Series([25, 28, 30, 31, 29])  # 最後一天 29 (< 32, < 35)
        # 29 < 32 ✓，但 29 < 31 不上升，所以只算當天 cnt=1
        # cnt 從 1 開始，找前一日 31>29 不算上升，break
        result = compute_t4_rising_days(rsi)
        assert result == 1

    def test_too_short(self):
        """資料 < 3 → 0"""
        assert compute_t4_rising_days(pd.Series([25, 28])) == 0


class TestTechnicalGate:
    def _make_d(self, **kwargs):
        """合成 d dict，預設值為「中性」"""
        defaults = {
            'close': 100, 'sma20': 99, 'sma50': 98, 'sma200': 95,
            'ema5': 100, 'ema20': 99, 'ema5_5d_ago': 99, 'ema20_5d_ago': 98,
            'rsi': 55, 'adx': 18,
        }
        defaults.update(kwargs)
        return defaults

    def test_distribution_high_dev_sma20(self):
        """乖離 SMA20 > 20% → distribution"""
        d = self._make_d(close=130, sma20=100)   # dev = 30%
        res = technical_gate(d)
        assert res['phase'] == PHASE_DISTRIBUTION
        assert res['multiplier'] == 0.3
        assert any('乖離 SMA20' in r for r in res['reasons'])

    def test_distribution_rsi_overbought(self):
        """RSI > 70 + 乖離 > 10 → distribution"""
        d = self._make_d(close=115, sma20=100, rsi=80)  # dev 15%, rsi 80
        res = technical_gate(d)
        assert res['phase'] == PHASE_DISTRIBUTION

    def test_accumulation_near_sma200(self):
        """價在 SMA200 ±5% 內，且 RSI < 45 → accumulation"""
        d = self._make_d(close=96, sma200=100, rsi=40, sma20=95)
        res = technical_gate(d)
        assert res['phase'] == PHASE_ACCUMULATION
        assert res['multiplier'] == 1.0

    def test_accumulation_strong_t3_with_adx(self):
        """ADX ≥ 25 + T3 ≥ 4 → 啟動初期 accumulation"""
        d = self._make_d(
            close=105, sma200=80,    # 遠離 SMA200
            ema5=104, ema20=100,
            ema5_5d_ago=101, ema20_5d_ago=99,  # 兩條都升
            sma20=99,   # close > SMA20 微幅
            rsi=55, adx=30,
        )
        res = technical_gate(d)
        # T3 score 應該很高（close>EMA20+EMA20升+EMA5升+EMA5>EMA20+雙升）= 5
        # ADX 30 + T3 5 → accumulation
        assert res['t3_score'] >= 4
        assert res['phase'] == PHASE_ACCUMULATION

    def test_default_neutral(self):
        """無顯著訊號 → neutral"""
        d = self._make_d()    # 預設值都中性
        res = technical_gate(d)
        assert res['phase'] == PHASE_NEUTRAL
        assert res['multiplier'] == 0.7

    def test_dev_calculations(self):
        d = self._make_d(close=110, sma20=100, sma50=100)
        res = technical_gate(d)
        assert abs(res['dev_sma20'] - 10.0) < 0.01
        assert abs(res['dev_sma50'] - 10.0) < 0.01

    def test_distribution_veto(self):
        """高乖離派發 → multiplier 直接 0.3（規格 §6.1）"""
        d = self._make_d(close=150, sma20=100, sma50=100, rsi=78)
        res = technical_gate(d)
        assert res['multiplier'] == 0.3

    def test_invalid_input_safe(self):
        res = technical_gate(None)
        # 預設 neutral，不爆炸
        assert res['phase'] == PHASE_NEUTRAL
        res = technical_gate({})
        assert res['phase'] == PHASE_NEUTRAL
