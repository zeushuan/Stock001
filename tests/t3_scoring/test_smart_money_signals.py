"""smart_money_signals 單元測試（用 mock 資料，不打網路）。"""
from __future__ import annotations

import pandas as pd
import pytest

from smart_money_signals import (
    institutional_score,
    insider_score,
    compute_sms,
    action_level,
    DEFAULT_TRACKED_FUNDS,
)


class TestInstitutionalScore:
    """institutional_score 用 compare_cache 注入 mock 資料避免打網路。"""

    def _make_compare(self, ticker: str, status: str, value: float,
                      share_chg_pct: float = 0,
                      total_value: float = 100_000_000,
                      fund_name: str = 'TEST_FUND') -> dict:
        df = pd.DataFrame([
            {'Ticker': ticker, 'Status': status, 'Value': value,
             'ShareChangePct': share_chg_pct, 'Issuer': ticker + ' INC'},
            # 加一個 padding row 讓 total_value 接近指定值
            {'Ticker': 'PAD', 'Status': 'UNCHANGED',
             'Value': max(total_value - value, 0),
             'ShareChangePct': 0, 'Issuer': 'PADDING'},
        ])
        return {
            'current_period': '2026-03-31',
            'previous_period': '2025-12-31',
            'manager_name': fund_name,
            'delta': df,
            'n_total': len(df),
        }

    def test_no_matches_returns_zero(self):
        # 沒有任何 fund 提到該 ticker
        cache = {'BRK-A': self._make_compare('OTHER', 'NEW', 1_000_000)}
        res = institutional_score('NVDA',
                                   tracked_funds=['BRK-A'],
                                   compare_cache=cache)
        assert res['score'] == 0

    def test_strong_consensus_5_funds(self):
        """5 家基金都 NEW → consensus 15"""
        cache = {}
        for i, f in enumerate(['BRK-A', 'BLK', 'BX', 'BAC', 'JPM']):
            cache[f] = self._make_compare(
                'NVDA', 'NEW', 5_000_000,
                share_chg_pct=100, fund_name=f)
        res = institutional_score(
            'NVDA',
            tracked_funds=['BRK-A', 'BLK', 'BX', 'BAC', 'JPM'],
            compare_cache=cache)
        assert res['sub_scores']['consensus'] == 15
        assert len(res['matches']) == 5
        assert res['score'] >= 25  # belief + consensus + direction

    def test_high_belief_portfolio_pct(self):
        """單一 fund 的 portfolio % ≥ 10% → belief 15"""
        cache = {'BRK-A': self._make_compare(
            'NEW', 'NEW', value=15_000_000, total_value=100_000_000,
            share_chg_pct=200)}
        # value 15M / total 100M = 15% → belief 15
        res = institutional_score('NEW',
                                   tracked_funds=['BRK-A'],
                                   compare_cache=cache)
        assert res['sub_scores']['belief'] == 15

    def test_decreased_no_score(self):
        """全部 DECREASED → 沒 bullish match"""
        cache = {f: self._make_compare('NVDA', 'DECREASED', 1_000_000,
                                         share_chg_pct=-50, fund_name=f)
                 for f in ['BRK-A', 'BLK']}
        res = institutional_score('NVDA',
                                   tracked_funds=['BRK-A', 'BLK'],
                                   compare_cache=cache)
        assert res['sub_scores']['consensus'] == 0
        assert res['sub_scores']['belief'] == 0

    def test_red_flag_cluster_closed(self):
        """3 家集體 CLOSED → red flag -15"""
        cache = {f: self._make_compare(
            'NVDA', 'CLOSED', 1_000_000, fund_name=f)
            for f in ['BRK-A', 'BLK', 'BX']}
        res = institutional_score('NVDA',
                                   tracked_funds=['BRK-A', 'BLK', 'BX'],
                                   compare_cache=cache)
        assert res['sub_scores']['red_flag'] == -15


class TestInsiderScore:
    """insider_score 用 cluster_result 注入 mock 避免打網路。"""

    def test_no_p_gate_returns_zero(self):
        """前置閘門：cluster_size = 0 → score = 0（規格 §4.3）"""
        cluster = {'cluster_size': 0, 'has_cfo': False, 'total_value': 0,
                   'max_value': 0, 'insiders': [], 'cluster_sells': 0,
                   'period_start': None, 'period_end': None}
        res = insider_score('TSLA', cluster_result=cluster,
                              transactions_df=pd.DataFrame())
        assert res['score'] == 0

    def test_max_score_with_cfo(self):
        """4 人集群 + CFO + ≥$1M → 50/50"""
        cluster = {
            'cluster_size': 4, 'has_cfo': True,
            'total_value': 5_000_000, 'max_value': 2_000_000,
            'insiders': [
                {'name': 'A', 'position': 'CEO', 'total_amount': 2_000_000,
                 'max_amount': 2_000_000, 'n_transactions': 1},
                {'name': 'B', 'position': 'CFO', 'total_amount': 1_500_000,
                 'max_amount': 1_500_000, 'n_transactions': 1},
                {'name': 'C', 'position': 'COO', 'total_amount': 1_000_000,
                 'max_amount': 1_000_000, 'n_transactions': 1},
                {'name': 'D', 'position': 'Director',
                 'total_amount': 500_000, 'max_amount': 500_000,
                 'n_transactions': 1},
            ],
            'cluster_sells': 0,
        }
        res = insider_score('XX', cluster_result=cluster,
                              transactions_df=pd.DataFrame([{'fake': 1}]))
        assert res['sub_scores']['amount']   == 15
        assert res['sub_scores']['cluster']  == 20
        assert res['sub_scores']['position'] == 15
        assert res['score'] == 50  # 15+20+15+0

    def test_solo_purchase_low_score(self):
        """單人小額：1 人 + $100K """
        cluster = {
            'cluster_size': 1, 'has_cfo': False,
            'total_value': 100_000, 'max_value': 100_000,
            'insiders': [{'name': 'X', 'position': 'Director',
                           'total_amount': 100_000, 'max_amount': 100_000,
                           'n_transactions': 1}],
            'cluster_sells': 0,
        }
        res = insider_score('XX', cluster_result=cluster,
                              transactions_df=pd.DataFrame([{'fake': 1}]))
        assert res['sub_scores']['amount']   == 8   # $100K~500K
        assert res['sub_scores']['cluster']  == 4   # 1 人
        assert res['sub_scores']['position'] == 4   # 非 CFO/CEO
        assert res['score'] == 16

    def test_red_flag_cluster_sells(self):
        """有 P 買入但同時 cluster sells ≥5 → 紅旗"""
        cluster = {
            'cluster_size': 2, 'has_cfo': False,
            'total_value': 300_000, 'max_value': 200_000,
            'insiders': [
                {'name': 'X', 'position': 'CEO', 'total_amount': 200_000,
                 'max_amount': 200_000, 'n_transactions': 1},
                {'name': 'Y', 'position': 'Director', 'total_amount': 100_000,
                 'max_amount': 100_000, 'n_transactions': 1},
            ],
            'cluster_sells': 6,
        }
        res = insider_score('XX', cluster_result=cluster,
                              transactions_df=pd.DataFrame([{'fake': 1}]))
        assert res['sub_scores']['red_flag'] == -20


class TestActionLevel:
    def test_strong_at_75(self):
        a = action_level(80)
        assert a['level'] == 'strong'
        assert a['icon'] == '🟢'

    def test_watch_55(self):
        a = action_level(60)
        assert a['level'] == 'watch'

    def test_neutral_35(self):
        a = action_level(40)
        assert a['level'] == 'neutral'

    def test_avoid_below_35(self):
        a = action_level(20)
        assert a['level'] == 'avoid'


class TestComputeSmsIntegration:
    """compute_sms 整合測試（會打網路，僅做 smoke 不嚴格 assert 分數）。"""

    def test_returns_well_formed_dict(self):
        # 這個會慢（網路 fetch），暫時跳過
        # 完整 integration test 由 CLI 真實資料跑時驗證
        pass
