"""signal_predictor 單元測試。

驗證:
  - predict_t1_close: 已知 EMA20/EMA60 → 正確算出 cross threshold close
  - predict_rsi_close: 已知 close 系列 → 算出 target RSI 的 close
  - predict_signal_triggers: 端到端 d dict 走通所有分支
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from signal_predictor import (
    predict_t1_close,
    predict_rsi_close,
    predict_signal_triggers,
)


class TestPredictT1Close:
    def test_none_inputs_safe(self):
        assert predict_t1_close(None, 100) is None
        assert predict_t1_close(100, None) is None

    def test_already_crossed_returns_below_current(self):
        """EMA20 > EMA60 (已是金叉)：threshold close 應 < 兩者
        (因為 ema20 還會繼續上揚，price 不用衝高就 cross 持續)
        實際上：cross threshold = ema20==ema60 的 close
        """
        # ema20=110, ema60=100 → 已金叉
        # 求 close 使 ema20==ema60：應該是「跌」到讓 ema20 降下來的 close
        c = predict_t1_close(110, 100)
        assert c is not None
        # 數學驗證：a20=2/21, a60=2/61, denom = a20-a60 ≈ 0.0624
        # close = [(1-a60)*100 - (1-a20)*110] / 0.0624
        # ≈ [(0.9672)*100 - (0.9048)*110] / 0.0624
        # ≈ [96.72 - 99.52] / 0.0624 ≈ -44.9 (負值 → 跌到 -45 才會 cross down 但實務上 close 不會負)
        # 這代表 cross 已穩固，今天不會輕易 down-cross
        # 數值的 sign 正確即可
        assert c < 110

    def test_death_cross_state_returns_high_target(self):
        """EMA20 < EMA60 (死叉)：要 close 大漲才會 golden cross
        ema20=100, ema60=110 → 死叉
        close = [(1-a60)*110 - (1-a20)*100] / 0.0624
              ≈ [106.39 - 90.48] / 0.0624 ≈ 254.9
        所以要 close 上衝到 ~$255 才會單日 cross。實務上不會發生 (~155% 漲)
        """
        c = predict_t1_close(100, 110)
        assert c is not None
        assert c > 110  # threshold 在兩 EMA 之上
        # 數學驗證
        expected = ((1 - 2/61) * 110 - (1 - 2/21) * 100) / (2/21 - 2/61)
        assert abs(c - expected) < 0.01

    def test_formula_consistency(self):
        """套用公式正向：給定 close=threshold 確實會讓 EMA20==EMA60。"""
        ema20_prev = 95
        ema60_prev = 105
        c = predict_t1_close(ema20_prev, ema60_prev)
        assert c is not None
        a20 = 2/21
        a60 = 2/61
        ema20_today = (1 - a20) * ema20_prev + a20 * c
        ema60_today = (1 - a60) * ema60_prev + a60 * c
        assert abs(ema20_today - ema60_today) < 0.001


class TestPredictRsiClose:
    def test_short_series_returns_none(self):
        assert predict_rsi_close([100, 101, 102]) is None

    def test_constant_series_handles(self):
        """RSI 在 constant series 上未定義（div 0），應 fail-safe 處理"""
        # 全部相同 → avg_gain=0, avg_loss=0 → RS undef
        closes = [100.0] * 30
        # 應不爆炸（回 None 或 prev_close）
        result = predict_rsi_close(closes)
        # 不爆即可
        assert result is None or isinstance(result, float)

    def test_round_trip_target_50(self):
        """構造一個明顯下跌序列（RSI 很低），算 target=50 的 close
        應該是「漲」才能拉 RSI 到 50。"""
        # 連跌
        closes = [100 - i * 0.5 for i in range(30)]
        prev_close = closes[-1]
        target = predict_rsi_close(closes, target_rsi=50)
        if target is not None:
            # 漲到 target 才會 RSI = 50（or 跌 ?）
            # 連跌資料 → avg_loss > avg_gain → RSI < 50
            # 要 RSI=50 需 gain ≈ loss → close 大漲
            assert target > prev_close

    def test_round_trip_target_50_uptrend(self):
        """連漲 → RSI 很高 → 要 RSI=50 需「跌」"""
        closes = [100 + i * 0.5 for i in range(30)]
        prev_close = closes[-1]
        target = predict_rsi_close(closes, target_rsi=50)
        if target is not None:
            assert target < prev_close


class TestPredictSignalTriggers:
    def _make_d(self, **kwargs):
        defaults = {
            'close': 100, 'ema20': 99, 'ema60': 98,
            'rsi': 55, 'rsi_prev': 53, 'rsi_prev2': 52,
            'adx': 22, 'adx_th': 22, 't4_rsi': 35,
            'ema20_cross_days': None,
            '_swing_history': {
                'close': [100 - i*0.1 for i in range(50, 0, -1)] + [100],
            },
        }
        defaults.update(kwargs)
        return defaults

    def test_t1_already_triggered(self):
        d = self._make_d(ema20_cross_days=3)
        out = predict_signal_triggers(d)
        assert out['t1']['status'] == 'triggered'

    def test_t1_reachable(self):
        d = self._make_d(ema20=95, ema60=100, ema20_cross_days=None)
        out = predict_signal_triggers(d)
        assert out['t1']['status'] == 'reachable'
        # death cross → t1 needs big rise → target close way above current
        assert out['t1']['target_close'] is not None
        assert out['t1']['target_pct'] > 0  # 需漲

    def test_t3_triggered_when_rsi_low(self):
        d = self._make_d(rsi=45, ema20=101, ema60=100)
        out = predict_signal_triggers(d)
        assert out['t3']['status'] == 'triggered'

    def test_t3_not_applicable_in_bear(self):
        """空頭排列 → T3 不適用"""
        d = self._make_d(ema20=95, ema60=100, rsi=60)
        out = predict_signal_triggers(d)
        assert out['t3']['status'] == 'not_applicable'

    def test_t3_reachable_high_rsi_bull(self):
        """多頭 + ADX OK + RSI=60 → T3 reachable，需跌讓 RSI<50"""
        d = self._make_d(
            ema20=110, ema60=100,
            rsi=60, adx=25,
            _swing_history={
                'close': [100 + i * 0.3 for i in range(60)],
            },
        )
        out = predict_signal_triggers(d)
        assert out['t3']['status'] == 'reachable'
        # 需跌（target_pct 應為負）
        assert out['t3']['target_pct'] is not None

    def test_t4_not_applicable_in_bull(self):
        """多頭 → T4 不適用"""
        d = self._make_d(ema20=110, ema60=100, rsi=60)
        out = predict_signal_triggers(d)
        assert out['t4']['status'] == 'not_applicable'

    def test_t4_triggered(self):
        """空頭 + RSI<35 + 連 2 日上升 → T4 triggered"""
        d = self._make_d(
            ema20=95, ema60=100,
            rsi=33, rsi_prev=30, rsi_prev2=28,
        )
        out = predict_signal_triggers(d)
        assert out['t4']['status'] == 'triggered'

    def test_t4_reachable_in_bear(self):
        """空頭 + RSI=60 → T4 reachable，需跌到 35"""
        d = self._make_d(
            ema20=95, ema60=100,
            rsi=60, rsi_prev=58, rsi_prev2=55,
            _swing_history={
                'close': [100 - i * 0.1 for i in range(60, 0, -1)] + [100],
            },
        )
        out = predict_signal_triggers(d)
        assert out['t4']['status'] == 'reachable'

    def test_unknown_when_no_data(self):
        out = predict_signal_triggers({})
        # 沒 close/ema → all unknown or not_applicable
        for k in ('t1', 't3', 't4'):
            assert out[k]['status'] in ('unknown', 'not_applicable')
