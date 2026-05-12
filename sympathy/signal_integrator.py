"""Sympathy 訊號與 T3 評分整合（Phase 3）

依指示書 §2.5：補漲訊號是事件驅動、時效短，不應覆蓋既有技術面評分，
而是作為**加成項**。

加成規則：
  score ≥ 0.75:  +8 分 (強)
  score ≥ 0.60:  +5 分 (中)
  score ≥ 0.45:  +3 分 (弱)
  有量能確認：bonus × 1.3
  訊號有效期：5 個交易日

生命週期：
  - 過期自動移除
  - 補漲已實現（peer 漲幅 ≥ leader 漲幅 × 0.7）→ 提前移除
  - Leader 反向大跌 ≥ 5% → 立即取消（催化劑破壞）
"""
from typing import Dict, Optional, List
import pandas as pd

SIGNAL_VALIDITY_DAYS = 5
BONUS_STRONG = 8
BONUS_MEDIUM = 5
BONUS_WEAK = 3
VOLUME_CONFIRM_MULT = 1.3


def apply_sympathy_bonus(base_t3_score: float,
                          sympathy_signal: dict,
                          volume_confirmed: bool = False) -> Dict:
    """套用 sympathy 訊號加成到 T3 分數

    Args:
        base_t3_score: 原始 T3 / 綜合決策分數
        sympathy_signal: laggard_scorer 輸出的 candidate dict
        volume_confirmed: 是否已通過隔日量能確認（Phase 3 後續）

    Returns:
        {
          'adjusted_score': float,
          'bonus_applied': float,
          'signal_expires': YYYY-MM-DD,
          'reason': str,
        }
    """
    s = float(sympathy_signal.get('score', 0))
    if s >= 0.75:
        bonus = BONUS_STRONG
    elif s >= 0.60:
        bonus = BONUS_MEDIUM
    elif s >= 0.45:
        bonus = BONUS_WEAK
    else:
        bonus = 0

    if volume_confirmed and bonus > 0:
        bonus *= VOLUME_CONFIRM_MULT

    # 訊號有效期（信號日 + 5 個交易日）
    sig_date = sympathy_signal.get('signal_date', '')
    try:
        exp = (pd.Timestamp(sig_date) +
                pd.tseries.offsets.BDay(SIGNAL_VALIDITY_DAYS)).strftime('%Y-%m-%d')
    except Exception:
        exp = ''

    adjusted = min(base_t3_score + bonus, 100)
    return {
        'adjusted_score': round(adjusted, 2),
        'bonus_applied': round(bonus, 2),
        'signal_expires': exp,
        'reason': (f"Sympathy play after {sympathy_signal.get('leader','?')} rally "
                    f"(score {s:.2f}{', volume confirmed' if volume_confirmed else ''})"),
        'sympathy_score': s,
        'sympathy_leader': sympathy_signal.get('leader'),
        'sympathy_group': sympathy_signal.get('group'),
    }


def is_signal_expired(sympathy_signal: dict,
                       as_of_date: Optional[pd.Timestamp] = None) -> bool:
    """檢查訊號是否過期（信號日 + 5 個交易日後）"""
    sig_date = sympathy_signal.get('signal_date', '')
    if not sig_date: return True
    try:
        sig = pd.Timestamp(sig_date)
    except Exception:
        return True
    if as_of_date is None:
        as_of_date = pd.Timestamp.now()
    if not isinstance(as_of_date, pd.Timestamp):
        as_of_date = pd.Timestamp(as_of_date)
    if as_of_date.tz is not None:
        as_of_date = as_of_date.tz_localize(None)
    expiry = sig + pd.tseries.offsets.BDay(SIGNAL_VALIDITY_DAYS)
    return as_of_date > expiry


def check_signal_validity(sympathy_signal: dict,
                           peer_current_return: float = 0,
                           leader_current_return: float = 0,
                           as_of_date: Optional[pd.Timestamp] = None
                           ) -> Dict:
    """檢查訊號是否仍然有效

    Returns:
      {'valid': bool, 'reason': str, 'action': 'keep'|'remove'}
    """
    # 1. 過期
    if is_signal_expired(sympathy_signal, as_of_date):
        return {'valid': False, 'reason': '訊號已過 5 個交易日', 'action': 'remove'}

    # 2. 補漲已實現（peer 漲幅 ≥ leader_today × 0.7）
    leader_today = float(sympathy_signal.get('leader_today_pct', 0))
    if peer_current_return >= leader_today * 0.7 and leader_today > 0:
        return {'valid': False,
                'reason': f'補漲已實現（peer +{peer_current_return*100:.1f}% '
                          f'達 leader +{leader_today*100:.1f}% 的 70%）',
                'action': 'remove'}

    # 3. Leader 反向大跌 ≥ 5%（催化劑破壞）
    if leader_current_return <= -0.05:
        return {'valid': False,
                'reason': f'Leader 反向大跌 {leader_current_return*100:.1f}% — 催化劑破壞',
                'action': 'remove'}

    return {'valid': True, 'reason': '', 'action': 'keep'}


def integrate_sympathy_into_state(state: dict,
                                    sympathy_signal: Optional[dict],
                                    volume_confirmed: bool = False) -> dict:
    """把 sympathy 訊號嵌入 state dict（tv_app 用）

    Args:
        state: 個股 state dict（含 close、is_bull、score 等）
        sympathy_signal: 該 ticker 對應的 sympathy candidate dict（若無則 None）

    Returns:
        state 加入 sympathy_* 欄位後回傳
    """
    if sympathy_signal is None:
        return state
    base = state.get('decision_score', state.get('score', 0)) or 0
    result = apply_sympathy_bonus(base, sympathy_signal, volume_confirmed)
    state['sympathy_score'] = result['sympathy_score']
    state['sympathy_bonus'] = result['bonus_applied']
    state['sympathy_adjusted_decision'] = result['adjusted_score']
    state['sympathy_leader'] = result['sympathy_leader']
    state['sympathy_group'] = result['sympathy_group']
    state['sympathy_expires'] = result['signal_expires']
    state['sympathy_reason'] = result['reason']
    return state
