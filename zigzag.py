"""ZigZag swing filter — 抓「視覺上的轉折」

核心想法：
  人眼挑底，不會挑「前後 5 天最低」這種 local minima 雜訊，
  而是挑「相對前次轉折 X% 以上的擺盪」。
  ZigZag 就在做這件事 — 從上一個 pivot 起，追蹤 running 高/低，
  當價格反向超過門檻時，確認 running 極值為新 pivot。

門檻有兩種模式：
  1. pct  — 固定百分比（簡單）
  2. atr  — N × ATR(14)（跨市場、跨價位通用，推薦）
"""
import numpy as np
import pandas as pd


def compute_atr(df, period=14):
    """Wilder ATR"""
    h = df['High'].values.astype(float)
    l = df['Low'].values.astype(float)
    c = df['Close'].values.astype(float)
    n = len(df)
    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(h[i]-l[i],
                    abs(h[i]-c[i-1]),
                    abs(l[i]-c[i-1]))
    atr = np.full(n, np.nan)
    if n <= period: return atr
    atr[period] = np.mean(tr[1:period+1])
    for i in range(period+1, n):
        atr[i] = (atr[i-1]*(period-1) + tr[i]) / period
    # Forward-fill
    return atr


def zigzag(df, mode='atr', atr_mult=5.0, pct=7.0, atr_period=14,
             price_source='close'):
    """ZigZag pivot detection

    Args:
      mode: 'atr' 或 'pct'
      atr_mult: ATR 倍數（mode='atr' 時用）
      pct: 百分比門檻（mode='pct' 時用）
      price_source: 'close'（預設，v9.32 起改用收盤價偵測 pivot — 抖動較小）
                    或 'hl'（舊行為，用 High/Low）

    Returns:
      pivots: list of dicts [{'idx': int, 'type': 'H'|'L', 'price': float,
                              'date': Timestamp, 'tentative': bool}]

    Note:
      v9.32 改變：預設改用 close 當 pivot 偵測來源。對 intraday 特別重要
      （5min bar 的 H/L 抖動大，close 更能代表市場「結論」）。
      想要回到舊行為傳 price_source='hl'。
    """
    n = len(df)
    if n < 3:
        return []

    c = df['Close'].values.astype(float)
    # 🆕 v9.32：用 close 還是 H/L
    if price_source == 'close':
        h_arr = c   # 「找高點」用 close
        l_arr = c   # 「找低點」也用 close
    else:  # 'hl' = 舊行為
        h_arr = df['High'].values.astype(float)
        l_arr = df['Low'].values.astype(float)

    if mode == 'atr':
        atr = compute_atr(df, atr_period)
        # ATR 還沒成熟前，用 close 的 pct 當 fallback
        fallback = c * (pct/100.0)
        def threshold(i):
            v = atr[i] * atr_mult
            return v if not np.isnan(v) and v > 0 else fallback[i]
    else:
        def threshold(i):
            return c[i] * (pct/100.0)

    pivots = []
    # Running extremes since last pivot
    swing_h_idx, swing_h = 0, h_arr[0]
    swing_l_idx, swing_l = 0, l_arr[0]
    mode_state = None    # None / 'up' (looking for new high) / 'down'

    for i in range(1, n):
        thr = threshold(i)
        if h_arr[i] > swing_h:
            swing_h_idx, swing_h = i, h_arr[i]
        if l_arr[i] < swing_l:
            swing_l_idx, swing_l = i, l_arr[i]

        if mode_state is None:
            if swing_h - swing_l >= thr:
                if swing_l_idx <= swing_h_idx:
                    pivots.append({'idx': swing_l_idx, 'type': 'L',
                                    'price': swing_l, 'tentative': False})
                    mode_state = 'up'
                    swing_l_idx, swing_l = swing_h_idx, l_arr[swing_h_idx]
                else:
                    pivots.append({'idx': swing_h_idx, 'type': 'H',
                                    'price': swing_h, 'tentative': False})
                    mode_state = 'down'
                    swing_h_idx, swing_h = swing_l_idx, h_arr[swing_l_idx]
        elif mode_state == 'up':
            if swing_h - l_arr[i] >= thr:
                pivots.append({'idx': swing_h_idx, 'type': 'H',
                                'price': swing_h, 'tentative': False})
                mode_state = 'down'
                swing_l_idx, swing_l = i, l_arr[i]
                swing_h_idx, swing_h = i, h_arr[i]
        elif mode_state == 'down':
            if h_arr[i] - swing_l >= thr:
                pivots.append({'idx': swing_l_idx, 'type': 'L',
                                'price': swing_l, 'tentative': False})
                mode_state = 'up'
                swing_h_idx, swing_h = i, h_arr[i]
                swing_l_idx, swing_l = i, l_arr[i]

    # 末端 tentative pivot
    if pivots:
        last = pivots[-1]
        if mode_state == 'up' and swing_h > last['price']:
            pivots.append({'idx': swing_h_idx, 'type': 'H',
                            'price': swing_h, 'tentative': True})
        elif mode_state == 'down' and swing_l < last['price']:
            pivots.append({'idx': swing_l_idx, 'type': 'L',
                            'price': swing_l, 'tentative': True})

    # 加上 date 欄位
    for p in pivots:
        p['date'] = df.index[p['idx']]
    return pivots


def find_double_bottoms_from_pivots(pivots, similarity_tol=0.05,
                                       min_rebound_pct=8.0, max_age_days=60,
                                       n=None):
    """從 ZigZag pivots 找雙底配對

    保證性質（因 zigzag 結構）：
    - L1 / L2 之間只有一個 pivot high（=> 自動是 neckline）
    - L1 / L2 之間不會有更深的低點

    Args:
      n: 總 bar 數（用來算 days_since_2nd_bottom）

    Returns: list of result dicts (按品質排序)
    """
    lows = [p for p in pivots if p['type'] == 'L']
    highs = [p for p in pivots if p['type'] == 'H']
    results = []

    # 遍歷所有相鄰 L-H-L 三元組
    for i in range(len(pivots) - 2):
        a, b, c = pivots[i], pivots[i+1], pivots[i+2]
        if a['type'] != 'L' or b['type'] != 'H' or c['type'] != 'L':
            continue

        L1, NK, L2 = a, b, c
        sim = abs(L2['price'] - L1['price']) / min(L1['price'], L2['price'])
        if sim > similarity_tol:
            continue
        rebound_pct = (NK['price'] - L1['price']) / L1['price'] * 100
        if rebound_pct < min_rebound_pct:
            continue

        sep_days = L2['idx'] - L1['idx']
        days_since = (n - 1 - L2['idx']) if n is not None else None

        if days_since is not None and days_since > max_age_days:
            continue

        results.append({
            'L1': L1, 'neckline': NK, 'L2': L2,
            'similarity_pct': round(sim * 100, 2),
            'rebound_pct': round(rebound_pct, 2),
            'separation_days': int(sep_days),
            'days_since_2nd': int(days_since) if days_since is not None else None,
            'L2_tentative': L2.get('tentative', False),
        })
    return results


def find_double_tops_from_pivots(pivots, similarity_tol=0.05,
                                    min_pullback_pct=8.0, max_age_days=60,
                                    n=None):
    """從 ZigZag pivots 找雙頂配對（鏡像）"""
    results = []
    for i in range(len(pivots) - 2):
        a, b, c = pivots[i], pivots[i+1], pivots[i+2]
        if a['type'] != 'H' or b['type'] != 'L' or c['type'] != 'H':
            continue
        H1, NK, H2 = a, b, c
        sim = abs(H2['price'] - H1['price']) / min(H1['price'], H2['price'])
        if sim > similarity_tol: continue
        pullback_pct = (H1['price'] - NK['price']) / H1['price'] * 100
        if pullback_pct < min_pullback_pct: continue

        sep_days = H2['idx'] - H1['idx']
        days_since = (n - 1 - H2['idx']) if n is not None else None
        if days_since is not None and days_since > max_age_days: continue

        results.append({
            'H1': H1, 'neckline': NK, 'H2': H2,
            'similarity_pct': round(sim * 100, 2),
            'pullback_pct': round(pullback_pct, 2),
            'separation_days': int(sep_days),
            'days_since_2nd': int(days_since) if days_since is not None else None,
            'H2_tentative': H2.get('tentative', False),
        })
    return results
