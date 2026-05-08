"""三狀態分類器（v9.16）— 上升 / 盤整 / 下降
==========================================================
依用戶需求設計：
- 三狀態：UPTREND / RANGING / DOWNTREND
- 加 N 天緩衝（避免單日跨越誤判）
- 盤整分三類：偏多盤整 / 偏空盤整 / 中性盤整

核心使用方式
-------------
從 detail card 用：
    from state_classifier import classify_market_state
    info = classify_market_state(df, d)
    # info: {
    #   'state': 'UPTREND' | 'RANGING' | 'DOWNTREND',
    #   'sub_state': str,          # 細分（如 'RANGING_BULL_BIAS'）
    #   'days_in_state': int,      # 已在此狀態幾天
    #   'state_label': str,        # 顯示標籤（如 '📈 上升中'）
    #   'state_color': str,        # 顯示色
    #   'state_desc': str,         # 簡述
    # }
"""
import numpy as np


N_BUFFER = 3  # N 天緩衝（state 必須持續 ≥ N 天才算）


def _raw_state_at(df, i):
    """單日 raw 狀態（不含緩衝）— 回傳 'UPTREND' | 'RANGING' | 'DOWNTREND'"""
    if i < 60: return None
    e20 = df['e20'].values
    e60 = df['e60'].values
    c = df['Close'].values
    adx = df['adx'].values

    if any(np.isnan(x) for x in [e20[i], e60[i], c[i], adx[i]]):
        return None

    # 多頭排列
    is_bull = e20[i] > e60[i] and c[i] > e20[i]
    # 空頭排列
    is_bear = e20[i] < e60[i] and c[i] < e20[i]
    # ADX 強度
    adx_strong = adx[i] >= 22

    # EMA 糾纏（gap < 1%）
    if e60[i] > 0:
        gap_pct = abs(e20[i] - e60[i]) / e60[i] * 100
        ema_tangled = gap_pct < 1.0
    else:
        ema_tangled = True

    # BB Squeeze（用 pctb 邊緣 + bandwidth 簡化判斷；正式版要 BB bandwidth）
    # 這裡簡化：ADX < 22 是主要盤整訊號
    if not adx_strong or ema_tangled:
        return 'RANGING'
    if is_bull:
        return 'UPTREND'
    if is_bear:
        return 'DOWNTREND'
    return 'RANGING'  # fallback


def classify_market_state(df, d=None, n_buffer=N_BUFFER):
    """主入口：判斷當前狀態（含 N 天緩衝）

    df: pandas DataFrame with [Open, High, Low, Close, e10, e20, e60, rsi, adx, atr]
    d:  optional pre-computed indicators dict（detail card 已算）
    n_buffer: 緩衝天數（state 持續 ≥ N 天才確認）

    回傳：
      {
        'state': 'UPTREND' | 'RANGING' | 'DOWNTREND' | 'TRANSITION',
        'sub_state': str,
        'days_in_state': int,
        'state_label': str,
        'state_color': str,
        'state_desc': str,
        'metrics': dict,
      }
    """
    if df is None or len(df) < 80:
        return _empty_state()

    n = len(df)
    last_i = n - 1

    # 算過去 30 天的 raw state（用於判斷持續時長）
    lookback = min(30, last_i)
    raw_states = []
    for j in range(last_i - lookback + 1, last_i + 1):
        s = _raw_state_at(df, j)
        raw_states.append(s)

    if not raw_states or raw_states[-1] is None:
        return _empty_state()

    # 緩衝：今日 + 前 N-1 天必須一致才算「確認」
    today = raw_states[-1]
    if n_buffer > 1 and len(raw_states) >= n_buffer:
        last_n = raw_states[-n_buffer:]
        if not all(s == today for s in last_n):
            # 不一致 → 過渡狀態
            committed = 'TRANSITION'
            days_in_state = 1
        else:
            committed = today
            # 計算持續天數
            days_in_state = 1
            for j in range(len(raw_states) - 2, -1, -1):
                if raw_states[j] == committed:
                    days_in_state += 1
                else:
                    break
    else:
        committed = today
        days_in_state = 1
        for j in range(len(raw_states) - 2, -1, -1):
            if raw_states[j] == committed:
                days_in_state += 1
            else:
                break

    # 細分（盤整時看偏多/偏空/中性）
    sub_state = committed
    if committed == 'RANGING':
        e20 = df['e20'].values; e60 = df['e60'].values
        if not (np.isnan(e20[last_i]) or np.isnan(e60[last_i])):
            gap = (e20[last_i] - e60[last_i]) / e60[last_i] * 100
            if gap > 0.5:
                sub_state = 'RANGING_BULL_BIAS'
            elif gap < -0.5:
                sub_state = 'RANGING_BEAR_BIAS'
            else:
                sub_state = 'RANGING_NEUTRAL'

    # Label / 顏色 / 描述
    label_map = {
        'UPTREND':              ('📈 上升中',     '#3dbb6a'),
        'DOWNTREND':            ('📉 下降中',     '#ff5555'),
        'RANGING':              ('🌫️ 盤整中',     '#888'),
        'RANGING_BULL_BIAS':    ('🌫️↑ 偏多盤整',  '#7abadd'),
        'RANGING_BEAR_BIAS':    ('🌫️↓ 偏空盤整',  '#e8a020'),
        'RANGING_NEUTRAL':      ('🌫️= 中性盤整',  '#888'),
        'TRANSITION':           ('🔄 過渡中',     '#c8b87a'),
    }
    label, color = label_map.get(sub_state, ('❓ 未知', '#666'))

    # 取出 metrics（detail card 顯示用）
    e20_v = float(df['e20'].iloc[-1]) if not np.isnan(df['e20'].iloc[-1]) else None
    e60_v = float(df['e60'].iloc[-1]) if not np.isnan(df['e60'].iloc[-1]) else None
    adx_v = float(df['adx'].iloc[-1]) if not np.isnan(df['adx'].iloc[-1]) else None
    close_v = float(df['Close'].iloc[-1])
    gap_pct = (e20_v - e60_v) / e60_v * 100 if e20_v and e60_v else 0

    # 描述
    if committed == 'UPTREND':
        desc = f'多頭排列 + ADX {adx_v:.0f}（≥22 趨勢確立），持續 {days_in_state} 天'
    elif committed == 'DOWNTREND':
        desc = f'空頭排列 + ADX {adx_v:.0f}（≥22 趨勢確立），持續 {days_in_state} 天'
    elif committed == 'TRANSITION':
        desc = f'狀態變化中（最近 {n_buffer} 天不一致），等明確訊號'
    else:  # RANGING
        if sub_state == 'RANGING_BULL_BIAS':
            desc = f'EMA 偏多但 ADX {adx_v:.0f} 弱，可能突破向上'
        elif sub_state == 'RANGING_BEAR_BIAS':
            desc = f'EMA 偏空但 ADX {adx_v:.0f} 弱，可能突破向下'
        else:
            desc = f'EMA 糾纏 + ADX {adx_v:.0f}，方向不明'
        desc += f'，持續 {days_in_state} 天'

    return {
        'state': committed,
        'sub_state': sub_state,
        'days_in_state': days_in_state,
        'state_label': label,
        'state_color': color,
        'state_desc': desc,
        'metrics': {
            'close': close_v,
            'ema20': e20_v, 'ema60': e60_v,
            'gap_pct': round(gap_pct, 2),
            'adx': adx_v,
        },
    }


def _empty_state():
    return {
        'state': 'UNKNOWN',
        'sub_state': 'UNKNOWN',
        'days_in_state': 0,
        'state_label': '❓ 資料不足',
        'state_color': '#666',
        'state_desc': '無法判斷狀態（資料不足或缺失）',
        'metrics': {},
    }


# ─── Active Exit Recipes 的 live 評估（detail card 用） ────────────

def evaluate_recipes_live(df, entry_i=None):
    """評估當前所有 4 個 recipe 是否觸發 + 細節（給 detail card 用）

    entry_i: 假設進場 index（沒提供就用最後一日的 60 天前，純當作評估）

    回傳：
      [
        {'name': 'A', 'label': '🛡️ 保守快出 (E1 OR E2)',
         'triggered': True/False, 'reason': str, 'detail': str},
        ...
      ]
    """
    if df is None or len(df) < 80:
        return []

    n = len(df)
    last_i = n - 1
    if entry_i is None:
        entry_i = max(0, last_i - 60)

    e10 = df['e10'].values; e20 = df['e20'].values
    o = df['Open'].values; c = df['Close'].values; v = df['Volume'].values
    adx = df['adx'].values; atr = df['atr'].values

    # 計算 running peak
    peak = float(c[entry_i]) if not np.isnan(c[entry_i]) else 0
    for j in range(entry_i, last_i + 1):
        h_j = float(df['High'].iloc[j])
        if not np.isnan(h_j) and h_j > peak:
            peak = h_j

    results = []

    # E1 / Recipe A 部分
    e1_ok = False; e1_detail = ''
    if last_i >= 1 and not (np.isnan(e20[last_i]) or np.isnan(e20[last_i-1])):
        e1_ok = (c[last_i] < e20[last_i]) and (c[last_i-1] < e20[last_i-1])
        e1_detail = (f'close {c[last_i]:.2f} {("<" if c[last_i] < e20[last_i] else "≥")} '
                     f'EMA20 {e20[last_i]:.2f}')

    # E2 (3 black + vol)
    e2_ok = False; e2_detail = ''
    if last_i >= 2:
        blacks = sum(1 for j in [last_i, last_i-1, last_i-2]
                     if not np.isnan(o[j]) and not np.isnan(c[j]) and c[j] < o[j])
        vol_ratio = 1.0
        if last_i >= 20:
            v_avg = np.nanmean(v[last_i-20:last_i])
            if v_avg > 0:
                vol_ratio = v[last_i] / v_avg
        e2_ok = (blacks >= 3 and vol_ratio > 1.3)
        e2_detail = f'{blacks} 連黑K + 量比 {vol_ratio:.2f}x'

    # E3 (ADX 5d down)
    e3_ok = False; e3_detail = ''
    if last_i >= 5 and not (np.isnan(adx[last_i]) or np.isnan(adx[last_i-5])):
        adx_diff = adx[last_i] - adx[last_i-5]
        e3_ok = adx_diff <= -5
        e3_detail = f'ADX {adx[last_i-5]:.0f} → {adx[last_i]:.0f} ({adx_diff:+.1f})'

    # E10 break (close < EMA10 連 2 天)
    e10_ok = False; e10_detail = ''
    if last_i >= 1 and not (np.isnan(e10[last_i]) or np.isnan(e10[last_i-1])):
        e10_ok = (c[last_i] < e10[last_i]) and (c[last_i-1] < e10[last_i-1])
        e10_detail = (f'close {c[last_i]:.2f} {("<" if c[last_i] < e10[last_i] else "≥")} '
                      f'EMA10 {e10[last_i]:.2f}')

    # ATR 2.5 trail
    atr_ok = False; atr_detail = ''
    if not np.isnan(atr[last_i]) and atr[last_i] > 0 and peak > 0:
        threshold = peak - 2.5 * atr[last_i]
        if threshold > 0:
            entry_close = float(c[entry_i]) if not np.isnan(c[entry_i]) else 0
            atr_ok = (peak > entry_close) and (c[last_i] <= threshold)
            atr_detail = (f'peak {peak:.2f} − 2.5×ATR({atr[last_i]:.2f}) = {threshold:.2f}, '
                          f'now {c[last_i]:.2f}')

    # 組成 4 recipes
    results.append({
        'name': 'A',
        'label': '🛡️ A 保守快出',
        'rule': 'E1 OR E2',
        'triggered': e1_ok or e2_ok,
        'reason': ('E1+E2' if (e1_ok and e2_ok) else 'E1' if e1_ok else 'E2' if e2_ok else '-'),
        'detail': f'E1: {e1_detail}; E2: {e2_detail}',
    })
    results.append({
        'name': 'B',
        'label': '⚖️ B 平衡 ⭐',
        'rule': 'E1 AND E3',
        'triggered': e1_ok and e3_ok,
        'reason': ('E1+E3' if (e1_ok and e3_ok) else '需兩項同時'),
        'detail': f'E1: {e1_detail}; E3: {e3_detail}',
    })
    results.append({
        'name': 'C',
        'label': '🚀 C 飆股',
        'rule': 'close < EMA10 連 2 天',
        'triggered': e10_ok,
        'reason': ('E10' if e10_ok else '-'),
        'detail': e10_detail,
    })
    results.append({
        'name': 'D',
        'label': '🎯 D ATR 動態',
        'rule': 'close ≤ peak − 2.5 ATR',
        'triggered': atr_ok,
        'reason': ('ATR' if atr_ok else '-'),
        'detail': atr_detail,
    })

    return results
