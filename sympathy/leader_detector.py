"""領漲股偵測

判斷準則（全部滿足）：
  1. 當日報酬率 ≥ leader_return_pct（預設 5%）
  2. 當日成交量 ≥ 20 日均量 × leader_volume_ratio（預設 1.5）
  3. 收盤價 > 20 日均線（避免反彈陷阱）
  4. 收盤位於當日 K 上半部（排除衝高拉回）
"""
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

from sympathy.peer_mapping import PeerMapping, get_default_mapping
from sympathy._data import load_history


def _is_leader(df: pd.DataFrame, as_of_date: pd.Timestamp,
                min_return: float = 0.05,
                min_vol_ratio: float = 1.5) -> Optional[Dict]:
    """判斷該 df 在 as_of_date 是否為 leader"""
    if df is None or len(df) < 22: return None
    # 取截至 as_of_date 的 row
    sub = df.loc[:as_of_date]
    if len(sub) < 22: return None
    if sub.index[-1].date() != as_of_date.date():
        # 該日無交易（假日 / 停牌）
        return None

    today = sub.iloc[-1]
    prev = sub.iloc[-2]
    close = float(today['Close'])
    prev_close = float(prev['Close'])
    if prev_close <= 0: return None
    ret = (close - prev_close) / prev_close

    high = float(today['High'])
    low = float(today['Low'])
    vol = float(today['Volume'])
    # 20 日均量（不含今日）
    vol_avg20 = float(sub['Volume'].iloc[-21:-1].mean())
    vol_ratio = (vol / vol_avg20) if vol_avg20 > 0 else 0
    ma20 = float(sub['Close'].iloc[-20:].mean())
    upper_half = close > (high + low) / 2

    # 條件全過才算 leader
    if (ret >= min_return and vol_ratio >= min_vol_ratio
        and close > ma20 and upper_half):
        return {
            'return_pct': round(ret, 4),
            'volume_ratio': round(vol_ratio, 2),
            'close': round(close, 2),
            'detected_at': as_of_date.strftime('%Y-%m-%d'),
        }
    return None


def detect_leaders(as_of_date,
                    mapping: Optional[PeerMapping] = None,
                    group_filter: Optional[List[str]] = None
                    ) -> List[Dict]:
    """掃描所有族群，回傳當日 leaders

    Args:
        as_of_date: 'YYYY-MM-DD' 或 pd.Timestamp
        mapping: 自訂 PeerMapping，預設用 yaml
        group_filter: 只掃這些 group

    Returns:
        list of dicts: [{'ticker', 'group', 'return_pct',
                          'volume_ratio', 'close', 'detected_at'}]
    """
    if isinstance(as_of_date, str):
        as_of_date = pd.Timestamp(as_of_date)
    if as_of_date.tz is not None:
        as_of_date = as_of_date.tz_localize(None)

    if mapping is None:
        mapping = get_default_mapping()

    min_return = mapping.get_setting('leader_return_pct', 0.05)
    min_vol_ratio = mapping.get_setting('leader_volume_ratio', 1.5)

    groups_to_scan = group_filter or mapping.list_groups()
    leaders = []
    seen = set()  # 避免同一檔在多 group 中重複 detect 多次

    for group_name in groups_to_scan:
        members = mapping.get_members(group_name)
        for tk in members:
            df = load_history(tk, lookback_days=60, as_of_date=as_of_date)
            res = _is_leader(df, as_of_date,
                              min_return=min_return,
                              min_vol_ratio=min_vol_ratio)
            if res is None: continue
            key = (tk, group_name)
            if key in seen: continue
            seen.add(key)
            leaders.append({
                'ticker': tk,
                'group': group_name,
                **res,
            })

    # 排序：return_pct DESC
    leaders.sort(key=lambda x: -x['return_pct'])
    return leaders
