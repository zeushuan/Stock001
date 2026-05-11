"""雙底雙頂 + VCP 偵測（v9.23）— ZigZag (ATR×1.5) 統一引擎
============================================================
v9.23 重大改寫：
  - 取代 v9.22 的 _find_local_extremes + 4 道嚴格檢查
  - 改用 ZigZag (ATR×1.5) pivot 偵測（OOS 驗證 win@60d 51%/54%）
  - W 偵測：從 ZigZag pivots 找 L-H-L 三元組
  - 新增 VCP 偵測：從 H-L 對序列檢查收窄
  - 保留 v9.22 職業分析層：
    * 動能衰減 4-metric
    * 反應 K 棒 / 掃流動性
    * 突破有效性
    * 位置 context
    * Quality A/B/C/D
    * A_test / B_breakout / C_retest 三段建倉
"""
import numpy as np
import pandas as pd

from zigzag import zigzag as _zigzag, compute_atr as _compute_atr

DEFAULT_ATR_MULT = 1.5
DEFAULT_ATR_PERIOD = 14


# ────────────────────────────────────────────────────────────────
# v9.22 既有的職業分析層 helpers（保留）
# ────────────────────────────────────────────────────────────────

def _kbar_features(o, h, l, c):
    """單根 K 棒特徵"""
    if any(np.isnan(x) for x in [o, h, l, c]): return None
    rng = h - l
    body = abs(c - o)
    upper_shadow = h - max(c, o)
    lower_shadow = min(c, o) - l
    is_red = c >= o
    return {
        'range': rng, 'body': body,
        'body_pct': body / rng if rng > 0 else 0,
        'upper': upper_shadow, 'lower': lower_shadow,
        'upper_pct': upper_shadow / rng if rng > 0 else 0,
        'lower_pct': lower_shadow / rng if rng > 0 else 0,
        'is_red': is_red,
    }


def _detect_reaction_kbar(df, idx, side='bull', window=3):
    """偵測底部 / 頂部「表態」反應 K 棒"""
    if df is None or idx < 1 or idx >= len(df):
        return {'has_reaction': False}
    n = len(df)
    o = df['Open'].values; h = df['High'].values
    l = df['Low'].values;  c = df['Close'].values

    best = {'has_reaction': False, 'type': None, 'idx': None, 'desc': ''}
    for k in range(max(0, idx-1), min(n, idx + window + 1)):
        kf = _kbar_features(o[k], h[k], l[k], c[k])
        if kf is None: continue

        if side == 'bull':
            if (kf['range'] > 0 and kf['lower'] >= kf['body'] * 2
                and kf['upper_pct'] < 0.2 and kf['body'] > 0):
                best = {'has_reaction': True, 'type': 'hammer', 'idx': k,
                        'desc': '錘子線（下影 ≥ 2× 實體）'}
                break
            if k > 0 and not np.isnan(o[k-1]) and not np.isnan(c[k-1]):
                prev_kf = _kbar_features(o[k-1], h[k-1], l[k-1], c[k-1])
                if (prev_kf and not prev_kf['is_red'] and kf['is_red']
                    and o[k] < c[k-1] and c[k] > o[k-1]):
                    best = {'has_reaction': True, 'type': 'bull_engulfing', 'idx': k,
                            'desc': '多方吞噬（紅 K 完全包前黑 K）'}
                    break
            if kf['is_red'] and kf['body_pct'] >= 0.7:
                best = {'has_reaction': True, 'type': 'big_bullish', 'idx': k,
                        'desc': f'大實體紅 K（{kf["body_pct"]*100:.0f}% range）'}

        elif side == 'bear':
            if (kf['range'] > 0 and kf['upper'] >= kf['body'] * 2
                and kf['lower_pct'] < 0.2 and kf['body'] > 0):
                best = {'has_reaction': True, 'type': 'shooting_star', 'idx': k,
                        'desc': '射擊之星（上影 ≥ 2× 實體）'}
                break
            if k > 0 and not np.isnan(o[k-1]) and not np.isnan(c[k-1]):
                prev_kf = _kbar_features(o[k-1], h[k-1], l[k-1], c[k-1])
                if (prev_kf and prev_kf['is_red'] and not kf['is_red']
                    and o[k] > c[k-1] and c[k] < o[k-1]):
                    best = {'has_reaction': True, 'type': 'bear_engulfing', 'idx': k,
                            'desc': '空方吞噬（黑 K 完全包前紅 K）'}
                    break
            if not kf['is_red'] and kf['body_pct'] >= 0.7:
                best = {'has_reaction': True, 'type': 'big_bearish', 'idx': k,
                        'desc': f'大實體黑 K（{kf["body_pct"]*100:.0f}% range）'}
    return best


def _detect_liquidity_sweep(df, ref_idx, second_idx, side='bull'):
    """掃流動性 / 假突破偵測"""
    if ref_idx >= len(df) or second_idx >= len(df): return False
    o = df['Open'].iloc[second_idx]
    h = df['High'].iloc[second_idx]
    l = df['Low'].iloc[second_idx]
    c = df['Close'].iloc[second_idx]
    ref_l = df['Low'].iloc[ref_idx]
    ref_h = df['High'].iloc[ref_idx]
    if any(np.isnan(x) for x in [o, h, l, c, ref_l, ref_h]): return False
    if side == 'bull':
        return l < ref_l and c > ref_l and c >= o
    else:
        return h > ref_h and c < ref_h and c <= o


def _classify_position(df, lookback_long=200):
    """位置判別：大週期方向"""
    if df is None or len(df) < 60: return 'unknown'
    n = len(df)
    c = df['Close'].values
    if n >= 200:
        sma200 = np.nanmean(c[-200:])
        sma200_30d = np.nanmean(c[-230:-30]) if n >= 230 else None
    else:
        sma200 = None; sma200_30d = None
    sma60 = np.nanmean(c[-60:])
    cur = c[-1]

    if sma200 is None:
        if cur > sma60: return 'uptrend'
        if cur < sma60: return 'downtrend'
        return 'sideways'

    if cur > sma200 and (sma200_30d is None or sma200 > sma200_30d):
        return 'uptrend'
    if cur < sma200 and (sma200_30d is None or sma200 < sma200_30d):
        return 'downtrend'
    return 'sideways'


def _measure_momentum_decay(df, first_idx, second_idx, side='bull', window=3):
    """測量第二次測試時，原趨勢動能是否衰減"""
    if df is None: return {}
    n = len(df)
    if first_idx >= n or second_idx >= n: return {}

    o = df['Open'].values; h = df['High'].values
    l = df['Low'].values;  c = df['Close'].values; v = df['Volume'].values

    f_start = max(0, first_idx - window)
    f_end = min(n, first_idx + window + 1)
    s_start = max(0, second_idx - window)
    s_end = min(n, second_idx + window + 1)
    vol_first = float(np.nanmean(v[f_start:f_end])) if f_end > f_start else 0
    vol_second = float(np.nanmean(v[s_start:s_end])) if s_end > s_start else 0
    vol_ratio = (vol_second / vol_first) if vol_first > 0 else 1.0

    def _rev_body_avg(start, end):
        bodies = []
        for k in range(start, end):
            if any(np.isnan(x) for x in [o[k], c[k]]): continue
            if side == 'bull':
                if c[k] < o[k]: bodies.append(o[k] - c[k])
            else:
                if c[k] > o[k]: bodies.append(c[k] - o[k])
        return float(np.mean(bodies)) if bodies else 0
    body_first = _rev_body_avg(f_start, f_end)
    body_second = _rev_body_avg(s_start, s_end)
    body_ratio = (body_second / body_first) if body_first > 0 else 1.0

    def _rev_shadow_avg(start, end):
        shads = []
        for k in range(start, end):
            kf = _kbar_features(o[k], h[k], l[k], c[k])
            if kf is None: continue
            shads.append(kf['lower'] if side == 'bull' else kf['upper'])
        return float(np.mean(shads)) if shads else 0
    shad_first = _rev_shadow_avg(f_start, f_end)
    shad_second = _rev_shadow_avg(s_start, s_end)
    shadow_ratio = (shad_second / shad_first) if shad_first > 0 else 1.0

    def _rev_count(start, end):
        cnt = 0
        for k in range(start, end):
            if any(np.isnan(x) for x in [o[k], c[k]]): continue
            if side == 'bull':
                if c[k] > o[k]: cnt += 1
            else:
                if c[k] < o[k]: cnt += 1
        return cnt
    cnt_first = _rev_count(f_start, f_end)
    cnt_second = _rev_count(s_start, s_end)
    rev_kbar_diff = cnt_second - cnt_first

    score = 0
    if vol_ratio < 0.8: score += 1
    if body_ratio < 0.7: score += 1
    if shadow_ratio > 1.2: score += 1
    if rev_kbar_diff > 0: score += 1

    return {
        'volume_ratio':     round(vol_ratio, 2),
        'body_ratio':       round(body_ratio, 2),
        'shadow_ratio':     round(shadow_ratio, 2),
        'rev_kbar_count_diff': int(rev_kbar_diff),
        'decay_score':      int(score),
    }


def _check_breakout_validity(df, mid_idx, neckline, side='bull',
                              vol_avg_window=20):
    """檢查頸線突破是否有效"""
    if df is None or mid_idx >= len(df): return {}
    n = len(df)
    o = df['Open'].values; h = df['High'].values
    l = df['Low'].values;  c = df['Close'].values; v = df['Volume'].values

    breakout_idx = None
    for k in range(mid_idx + 1, n):
        if np.isnan(c[k]): continue
        if side == 'bull' and c[k] > neckline:
            breakout_idx = k; break
        if side == 'bear' and c[k] < neckline:
            breakout_idx = k; break

    if breakout_idx is None:
        return {'breakout_idx': None, 'validity_score': 0,
                'volume_surge': False, 'body_breakout': False,
                'no_pull_back': False}

    bi = breakout_idx
    vol_avg = (float(np.nanmean(v[max(0, bi-vol_avg_window):bi]))
                if bi >= 5 else 0)
    bi_vol_ratio = (float(v[bi]) / vol_avg) if vol_avg > 0 else 1.0
    volume_surge = bi_vol_ratio >= 1.5

    if side == 'bull':
        body_breakout = o[bi] > neckline * 0.99 and c[bi] > neckline
    else:
        body_breakout = o[bi] < neckline * 1.01 and c[bi] < neckline

    n_check = min(5, n - bi - 1)
    no_pull_back = True
    for j in range(bi + 1, bi + 1 + n_check):
        if j >= n: break
        if np.isnan(c[j]): continue
        if side == 'bull' and c[j] < neckline:
            no_pull_back = False; break
        if side == 'bear' and c[j] > neckline:
            no_pull_back = False; break

    score = (int(volume_surge) + int(body_breakout) + int(no_pull_back))

    return {
        'breakout_idx': int(bi),
        'volume_surge': bool(volume_surge),
        'body_breakout': bool(body_breakout),
        'no_pull_back': bool(no_pull_back),
        'validity_score': score,
        'breakout_close': round(float(c[bi]), 2),
        'breakout_vol_ratio': round(bi_vol_ratio, 2),
    }


# ────────────────────────────────────────────────────────────────
# v9.23 ZigZag pivot helper
# ────────────────────────────────────────────────────────────────

def _get_zigzag_pivots(df, lookback_days, atr_mult=DEFAULT_ATR_MULT):
    """跑 ZigZag，回傳 (pivots_with_global_idx, df_offset_in_full)

    pivots 內的 'idx' 已經轉成「相對 lookback slice 的 index」。
    我們也存 'global_idx' 為相對 full df 的 index。
    """
    n_full = len(df)
    start = max(0, n_full - lookback_days)
    df_slice = df.iloc[start:].copy()
    pivots = _zigzag(df_slice, mode='atr', atr_mult=atr_mult,
                       atr_period=DEFAULT_ATR_PERIOD)
    # 加 global_idx
    for p in pivots:
        p['global_idx'] = start + p['idx']
    return pivots, start


# ────────────────────────────────────────────────────────────────
# 主入口：detect_double_bottom（W底）— v9.23 ZigZag 版
# ────────────────────────────────────────────────────────────────

def detect_double_bottom(df, lookback_days=180,
                          similarity_tol=0.05, min_separation=15,
                          max_separation=120, peak_window=5,
                          min_rebound_pct=8, max_age_2nd=60,
                          atr_mult=DEFAULT_ATR_MULT):
    """雙底偵測（W底）— ZigZag (ATR×1.5) 版

    從 ZigZag pivots 自動找 L-H-L 三元組，套用：
      - similarity_tol：兩底相似度（默認 5%）
      - min_rebound_pct：中間反彈幅度
      - max_separation / max_age_2nd：時效窗口

    再套用 v9.22 職業分析層計算 Quality A-D。
    """
    if df is None or len(df) < 60:
        return {'is_double_bottom': False, 'status': 'none'}

    n_full = len(df)
    pivots, slice_start = _get_zigzag_pivots(df, lookback_days, atr_mult)
    if len(pivots) < 3:
        return {'is_double_bottom': False, 'status': 'none'}

    # 找最近一個合格 L-H-L 三元組（從新到舊掃，回第一個過關的）
    best = None
    for i in range(len(pivots) - 3, -1, -1):
        a, b, c = pivots[i], pivots[i+1], pivots[i+2]
        if a['type'] != 'L' or b['type'] != 'H' or c['type'] != 'L':
            continue
        # 在這個 L-H-L 三元組之後可能還有 pivots（H/L），但我們選這個三元組做 W

        sep = c['idx'] - a['idx']
        if sep < min_separation: continue
        if sep > max_separation: continue

        L1_p = a['price']; L2_p = c['price']
        if min(L1_p, L2_p) <= 0: continue
        sim = abs(L2_p - L1_p) / min(L1_p, L2_p)
        if sim > similarity_tol: continue

        neckline = b['price']
        rebound_pct = (neckline - L1_p) / L1_p * 100
        if rebound_pct < min_rebound_pct: continue

        # age check
        n_slice = n_full - slice_start
        days_since = n_slice - 1 - c['idx']
        if days_since > max_age_2nd: continue

        best = (a, b, c, sim, rebound_pct)
        break

    if not best:
        return {'is_double_bottom': False, 'status': 'none'}

    L1, NK, L2, sim, rebound_pct = best
    real_left = L1['global_idx']
    real_right = L2['global_idx']
    real_mid = NK['global_idx']
    p_low_left = L1['price']; p_low_right = L2['price']
    neckline = NK['price']
    avg_bottom = (p_low_left + p_low_right) / 2
    height = neckline - avg_bottom
    target = neckline + height
    cur_close = float(df['Close'].iloc[-1])
    df_idx = df.index

    # ─── 五大關鍵分析（v9.22 layer）────
    pos_ctx = _classify_position(df)
    pos_quality = 'high_prob' if pos_ctx in ('uptrend', 'downtrend') else 'neutral'

    decay = _measure_momentum_decay(df, real_left, real_right, side='bull', window=3)
    reaction = _detect_reaction_kbar(df, real_right, side='bull', window=3)
    liq_sweep = _detect_liquidity_sweep(df, real_left, real_right, side='bull')
    valid = _check_breakout_validity(df, real_mid, neckline, side='bull')

    quality_score = 0
    if pos_quality == 'high_prob': quality_score += 1
    if (decay or {}).get('decay_score', 0) >= 2: quality_score += 1
    if reaction.get('has_reaction'): quality_score += 1
    if valid.get('validity_score', 0) >= 2: quality_score += 1
    if rebound_pct >= 12 and sim <= 0.03:
        quality_score += 1

    quality_grade = 'A' if quality_score >= 5 else ('B' if quality_score >= 4
                    else ('C' if quality_score >= 3 else 'D'))

    # ─── 三段建倉狀態 ───
    days_since_2nd = (n_full - slice_start) - 1 - L2['idx']
    n = n_full
    breakout_idx_real = valid.get('breakout_idx')

    if cur_close < min(p_low_left, p_low_right) * 0.97:
        status = 'failed'
        entry_stage = 'wait'
    elif breakout_idx_real and cur_close < neckline and cur_close > p_low_right:
        if reaction.get('has_reaction') and breakout_idx_real < n - 3:
            status = 'C_retest_buy'; entry_stage = 'C_retest'
        else:
            status = 'confirmed'; entry_stage = 'wait'
    elif cur_close > neckline and breakout_idx_real:
        status = 'B_breakout_buy'; entry_stage = 'B_breakout'
    elif reaction.get('has_reaction') and days_since_2nd <= 5:
        status = 'A_test_buy'; entry_stage = 'A_test'
    elif days_since_2nd <= 5:
        status = 'forming'; entry_stage = 'wait'
    else:
        status = 'confirmed'; entry_stage = 'wait'

    # 結構停損
    if entry_stage == 'A_test':
        stop_loss = float(p_low_right) * 0.98
    elif entry_stage == 'B_breakout':
        stop_loss = float(neckline) * 0.97
    elif entry_stage == 'C_retest':
        if reaction.get('idx') is not None:
            stop_loss = float(min(df['Low'].iloc[reaction['idx']],
                                    df['Low'].iloc[real_right]) * 0.98)
        else:
            stop_loss = float(neckline) * 0.97
    else:
        stop_loss = float(min(p_low_left, p_low_right) * 0.97)

    return {
        'is_double_bottom': status not in ('failed', 'none'),
        'status': status,
        'entry_stage': entry_stage,
        'left_bottom':  {'date': df_idx[real_left].strftime('%Y-%m-%d'),
                          'price': round(p_low_left, 2)},
        'right_bottom': {'date': df_idx[real_right].strftime('%Y-%m-%d'),
                          'price': round(p_low_right, 2)},
        'middle_peak':  {'date': df_idx[real_mid].strftime('%Y-%m-%d'),
                          'price': round(neckline, 2)},
        'neckline_price': round(neckline, 2),
        'pattern_height': round(height, 2),
        'breakout_confirmed': cur_close > neckline,
        'target_price': round(target, 2),
        'stop_loss': round(stop_loss, 2),
        'similarity_pct': round(sim * 100, 2),
        'rebound_pct': round(rebound_pct, 2),
        'separation_days': int(L2['idx'] - L1['idx']),
        'days_since_2nd_bottom': int(days_since_2nd),
        'position_context': pos_ctx,
        'position_quality': pos_quality,
        'momentum_decay': decay,
        'reaction_kbar': reaction,
        'liquidity_sweep': bool(liq_sweep),
        'breakout_validity': valid,
        'quality_score': quality_score,
        'quality_grade': quality_grade,
        # 🆕 v9.23
        'detector_version': 'v9.23_zigzag',
        'atr_mult_used': atr_mult,
    }


# ────────────────────────────────────────────────────────────────
# detect_double_top（M頂）— ZigZag 版（鏡像）
# ────────────────────────────────────────────────────────────────

def detect_double_top(df, lookback_days=180,
                       similarity_tol=0.05, min_separation=15,
                       max_separation=120, peak_window=5,
                       min_pullback_pct=8, max_age_2nd=60,
                       atr_mult=DEFAULT_ATR_MULT):
    """雙頂偵測（M頂，看空反轉）— ZigZag 版鏡像"""
    if df is None or len(df) < 60:
        return {'is_double_top': False, 'status': 'none'}

    n_full = len(df)
    pivots, slice_start = _get_zigzag_pivots(df, lookback_days, atr_mult)
    if len(pivots) < 3:
        return {'is_double_top': False, 'status': 'none'}

    # 找 H-L-H 三元組
    best = None
    for i in range(len(pivots) - 3, -1, -1):
        a, b, c = pivots[i], pivots[i+1], pivots[i+2]
        if a['type'] != 'H' or b['type'] != 'L' or c['type'] != 'H':
            continue
        sep = c['idx'] - a['idx']
        if sep < min_separation: continue
        if sep > max_separation: continue

        H1_p = a['price']; H2_p = c['price']
        if min(H1_p, H2_p) <= 0: continue
        sim = abs(H2_p - H1_p) / min(H1_p, H2_p)
        if sim > similarity_tol: continue

        neckline = b['price']
        pullback_pct = (H1_p - neckline) / H1_p * 100
        if pullback_pct < min_pullback_pct: continue

        n_slice = n_full - slice_start
        days_since = n_slice - 1 - c['idx']
        if days_since > max_age_2nd: continue

        best = (a, b, c, sim, pullback_pct)
        break

    if not best:
        return {'is_double_top': False, 'status': 'none'}

    H1, NK, H2, sim, pullback_pct = best
    real_left = H1['global_idx']
    real_right = H2['global_idx']
    real_mid = NK['global_idx']
    p_high_left = H1['price']; p_high_right = H2['price']
    neckline = NK['price']
    avg_top = (p_high_left + p_high_right) / 2
    height = avg_top - neckline
    target = neckline - height
    cur_close = float(df['Close'].iloc[-1])
    df_idx = df.index

    pos_ctx = _classify_position(df)
    pos_quality = 'high_prob' if pos_ctx in ('uptrend', 'downtrend') else 'neutral'

    decay = _measure_momentum_decay(df, real_left, real_right, side='bear', window=3)
    reaction = _detect_reaction_kbar(df, real_right, side='bear', window=3)
    liq_sweep = _detect_liquidity_sweep(df, real_left, real_right, side='bear')
    valid = _check_breakout_validity(df, real_mid, neckline, side='bear')

    quality_score = 0
    if pos_quality == 'high_prob': quality_score += 1
    if (decay or {}).get('decay_score', 0) >= 2: quality_score += 1
    if reaction.get('has_reaction'): quality_score += 1
    if valid.get('validity_score', 0) >= 2: quality_score += 1
    if pullback_pct >= 12 and sim <= 0.03:
        quality_score += 1

    quality_grade = 'A' if quality_score >= 5 else ('B' if quality_score >= 4
                    else ('C' if quality_score >= 3 else 'D'))

    days_since_2nd = (n_full - slice_start) - 1 - H2['idx']
    n = n_full
    breakdown_idx_real = valid.get('breakout_idx')

    if cur_close > max(p_high_left, p_high_right) * 1.03:
        status = 'failed'; entry_stage = 'wait'
    elif breakdown_idx_real and cur_close > neckline and cur_close < p_high_right:
        if reaction.get('has_reaction') and breakdown_idx_real < n - 3:
            status = 'C_retest_short'; entry_stage = 'C_retest'
        else:
            status = 'confirmed'; entry_stage = 'wait'
    elif cur_close < neckline and breakdown_idx_real:
        status = 'B_breakdown_short'; entry_stage = 'B_breakout'
    elif reaction.get('has_reaction') and days_since_2nd <= 5:
        status = 'A_test_short'; entry_stage = 'A_test'
    elif days_since_2nd <= 5:
        status = 'forming'; entry_stage = 'wait'
    else:
        status = 'confirmed'; entry_stage = 'wait'

    if entry_stage == 'A_test':
        stop_loss = float(p_high_right) * 1.02
    elif entry_stage == 'B_breakout':
        stop_loss = float(neckline) * 1.03
    elif entry_stage == 'C_retest':
        if reaction.get('idx') is not None:
            stop_loss = float(max(df['High'].iloc[reaction['idx']],
                                    df['High'].iloc[real_right]) * 1.02)
        else:
            stop_loss = float(neckline) * 1.03
    else:
        stop_loss = float(max(p_high_left, p_high_right) * 1.03)

    return {
        'is_double_top': status not in ('failed', 'none'),
        'status': status,
        'entry_stage': entry_stage,
        'left_top':  {'date': df_idx[real_left].strftime('%Y-%m-%d'),
                       'price': round(p_high_left, 2)},
        'right_top': {'date': df_idx[real_right].strftime('%Y-%m-%d'),
                       'price': round(p_high_right, 2)},
        'middle_trough': {'date': df_idx[real_mid].strftime('%Y-%m-%d'),
                           'price': round(neckline, 2)},
        'neckline_price': round(neckline, 2),
        'pattern_height': round(height, 2),
        'breakdown_confirmed': cur_close < neckline,
        'target_price': round(target, 2),
        'stop_loss': round(stop_loss, 2),
        'similarity_pct': round(sim * 100, 2),
        'pullback_pct': round(pullback_pct, 2),
        'separation_days': int(H2['idx'] - H1['idx']),
        'days_since_2nd_top': int(days_since_2nd),
        'position_context': pos_ctx,
        'position_quality': pos_quality,
        'momentum_decay': decay,
        'reaction_kbar': reaction,
        'liquidity_sweep': bool(liq_sweep),
        'breakout_validity': valid,
        'quality_score': quality_score,
        'quality_grade': quality_grade,
        'detector_version': 'v9.23_zigzag',
        'atr_mult_used': atr_mult,
    }


# ────────────────────────────────────────────────────────────────
# 🆕 v9.23 VCP 偵測（收口序列）
# ────────────────────────────────────────────────────────────────

def detect_vcp_zigzag(df, lookback_days=180,
                       min_contractions=3, max_contractions=8,
                       vol_dryup_threshold=0.75,
                       atr_mult=DEFAULT_ATR_MULT):
    """VCP（Volatility Contraction Pattern）偵測

    從 ZigZag pivots 找連續 H-L 對，檢查：
      - 收口寬度逐次遞減
      - 最後一次收口 ≤ 5-10%
      - 量逐次衰減
      - 目前接近最後 pivot H = 突破前準備位置

    Returns dict, schema 同 vcp_from_pivots.detect_vcp_from_pivots
    額外多 'detector_version', 'atr_mult_used'
    """
    if df is None or len(df) < 60:
        return {'is_vcp': False, 'reason': '資料不足'}

    pivots, slice_start = _get_zigzag_pivots(df, lookback_days, atr_mult)
    if len(pivots) < 4:
        return {'is_vcp': False, 'reason': 'pivots 不足'}

    # 用 vcp_from_pivots 邏輯
    df_slice = df.iloc[slice_start:].copy()
    from vcp_from_pivots import detect_vcp_from_pivots
    result = detect_vcp_from_pivots(df_slice, pivots,
                                       min_contractions=min_contractions,
                                       max_contractions=max_contractions,
                                       vol_dryup_threshold=vol_dryup_threshold)
    result['detector_version'] = 'v9.23_zigzag'
    result['atr_mult_used'] = atr_mult
    return result
