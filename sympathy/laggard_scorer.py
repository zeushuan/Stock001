"""落後股評分（補漲候選 scoring）

公式：
    score = corr_60d × 0.4 + (1 - spread_pctile) × 0.3 + min(lag_today × 10, 1) × 0.3

過濾（全部過才入列）：
    - corr_60d > 0.6
    - spread_pctile < 0.35
    - lag_today > 0.02
    - peer close > 20d MA  (避免空頭排列接刀)
    - peer 連續下跌 < 5 日
"""
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

from sympathy._data import load_history
from sympathy.peer_mapping import PeerMapping, get_default_mapping


def score_laggards(leader: str, peers: List[str],
                    as_of_date,
                    lookback: int = 60,
                    min_corr: float = 0.6,
                    max_spread_pctile: float = 0.35,
                    min_lag: float = 0.02,
                    group_name: Optional[str] = None
                    ) -> List[Dict]:
    """對 group 中所有 peers 算補漲分數

    Args:
        leader: 領漲 ticker
        peers: 全部 group members（會自動排除 leader 自己）
        as_of_date: 評估日（leader 大漲那天）
        lookback: 計算相關性 / spread 百分位的回看天數

    Returns:
        list of dicts sorted by score DESC，包含過濾後的合格候選
    """
    if isinstance(as_of_date, str):
        as_of_date = pd.Timestamp(as_of_date)
    if as_of_date.tz is not None:
        as_of_date = as_of_date.tz_localize(None)

    # 載入 leader + peers 的歷史
    need_bars = lookback + 20
    leader_df = load_history(leader, need_bars, as_of_date)
    if leader_df is None or len(leader_df) < lookback + 5:
        return []

    leader_close = leader_df['Close'].astype(float)
    leader_ret = leader_close.pct_change().dropna()
    if as_of_date not in leader_ret.index:
        # 找最近一個交易日
        valid = leader_ret.index[leader_ret.index <= as_of_date]
        if len(valid) == 0: return []
        as_of_date = valid[-1]
    leader_today = float(leader_ret.loc[as_of_date])

    candidates = []
    for peer in peers:
        if peer == leader: continue
        try:
            peer_df = load_history(peer, need_bars, as_of_date)
            if peer_df is None or len(peer_df) < lookback + 5: continue
            peer_close = peer_df['Close'].astype(float)
            peer_ret = peer_close.pct_change().dropna()

            # 對齊兩個 ret 序列
            common = leader_ret.index.intersection(peer_ret.index)
            if len(common) < lookback: continue
            common = common[common <= as_of_date]
            if as_of_date not in common: continue

            # 取最後 (lookback+1) 個共同交易日做計算
            recent_common = common[-(lookback + 1):]

            # 1. corr_60d（排除今日）
            past_idx = recent_common[:-1]
            if len(past_idx) < lookback: continue
            corr = float(leader_ret.loc[past_idx].corr(peer_ret.loc[past_idx]))
            if np.isnan(corr) or corr <= min_corr: continue

            # 2. spread_pctile：peer/leader 比值百分位
            common_price = leader_close.index.intersection(peer_close.index)
            common_price = common_price[common_price <= as_of_date]
            if len(common_price) < lookback: continue
            recent_price = common_price[-lookback:]
            ratio = (peer_close.loc[recent_price] /
                     leader_close.loc[recent_price])
            r_min = float(ratio.min())
            r_max = float(ratio.max())
            if r_max <= r_min: continue
            spread_pctile = float((ratio.iloc[-1] - r_min) / (r_max - r_min))
            if spread_pctile >= max_spread_pctile: continue

            # 3. lag_today
            peer_today = float(peer_ret.loc[as_of_date])
            lag = leader_today - peer_today
            if lag <= min_lag: continue

            # 4. peer close > 20d MA
            peer_recent = peer_close.loc[:as_of_date].tail(21)
            if len(peer_recent) < 20: continue
            ma20 = float(peer_recent.iloc[-20:].mean())
            above_ma20 = float(peer_recent.iloc[-1]) > ma20
            if not above_ma20: continue

            # 5. 連續下跌 < 5 日
            last5_ret = peer_ret.loc[:as_of_date].tail(5)
            if (last5_ret < 0).sum() >= 5: continue

            # 算分
            score = (corr * 0.4
                     + (1 - spread_pctile) * 0.3
                     + min(lag * 10, 1) * 0.3)

            candidates.append({
                'ticker': peer,
                'leader': leader,
                'group': group_name,
                'score': round(score, 3),
                'corr_60d': round(corr, 3),
                'spread_pctile': round(spread_pctile, 3),
                'lag_today': round(lag, 4),
                'leader_today_pct': round(leader_today, 4),
                'peer_today_pct': round(peer_today, 4),
                'signal_date': as_of_date.strftime('%Y-%m-%d'),
            })
        except Exception:
            continue

    candidates.sort(key=lambda x: -x['score'])
    return candidates


def scan_all_groups(as_of_date,
                     mapping: Optional[PeerMapping] = None,
                     group_filter: Optional[List[str]] = None
                     ) -> List[Dict]:
    """掃描所有 group：
       1. 找 leader（per group）
       2. 對每個 leader 算 peer 補漲分數
       3. 合併 + 去重（同 peer 出現在多 group 取最高分）
    """
    from sympathy.leader_detector import detect_leaders

    if mapping is None:
        mapping = get_default_mapping()

    leaders = detect_leaders(as_of_date, mapping, group_filter)
    if not leaders:
        return []

    all_candidates = []
    seen_peers = {}  # ticker -> best candidate

    for ld in leaders:
        peers = mapping.get_members(ld['group'])
        cands = score_laggards(
            leader=ld['ticker'],
            peers=peers,
            as_of_date=as_of_date,
            lookback=mapping.get_setting('default_lookback_days', 60),
            min_corr=mapping.get_setting('min_correlation', 0.6),
            max_spread_pctile=mapping.get_setting('max_spread_percentile', 0.35),
            min_lag=mapping.get_setting('min_lag_today', 0.02),
            group_name=ld['group'],
        )
        for c in cands:
            tk = c['ticker']
            if tk not in seen_peers or c['score'] > seen_peers[tk]['score']:
                seen_peers[tk] = c

    return sorted(seen_peers.values(), key=lambda x: -x['score'])
