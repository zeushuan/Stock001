"""雙底雙頂進階分析（v9.22）— 職業交易員方法論
============================================================
核心觀念：雙底雙頂的真正核心 不是「形態本身」
        而是「第二次攻擊時，原本趨勢的動能有沒有衰減」
- 雙底：第二次空方有沒有沒力？
- 雙頂：第二次多方有沒有推不動？

五大關鍵過濾網
---------------
1. 位置（context）：大週期方向 + 小週期回測位置
   - 高勝率：大週期上升 + 小週期回測支撐 / 下跌末端
   - 陷阱：剛反轉後的第一波底（= 中繼，非反轉）
2. 動能衰減（momentum decay）— 核心
   - 量縮：第二次量 < 第一次量
   - 黑K實體變短（雙底）/ 紅K實體變短（雙頂）
   - 下影線變多（雙底）/ 上影線變多（雙頂）
   - 反向 K 數量增加
3. 第二次回到關鍵位置的「表態」
   - 反應 K 棒：錘子線 / 多方吞噬（雙底）/ 射擊之星 / 空方吞噬（雙頂）
   - 掃流動性（liquidity sweep）：刺穿前低 / 前高後拉回
4. 後續量價配合：突破要帶量
5. 頸線突破有效性：實體突破 + 帶量 + 後續確認

三段式建倉
-----------
A. 底部附近 1/3 試單（出現反應 K 棒）
B. 頸線整理後帶量突破，補滿到 2/3 ~ 3/3
C. 突破後回踩頸線有效，補剩餘部位

每個 entry 點都有對應的 stop-loss（結構低點）
"""
import numpy as np
import pandas as pd


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def _find_local_extremes(values, window=5, find_min=False):
    n = len(values)
    out = []
    for i in range(window, n - window):
        if np.isnan(values[i]): continue
        win_l = values[i-window:i]
        win_r = values[i+1:i+window+1]
        if len(win_l) == 0 or len(win_r) == 0: continue
        if find_min:
            if values[i] <= np.nanmin(win_l) and values[i] <= np.nanmin(win_r):
                out.append(i)
        else:
            if values[i] >= np.nanmax(win_l) and values[i] >= np.nanmax(win_r):
                out.append(i)
    return out


def _filter_close_extremes(extremes, min_separation=10):
    if not extremes: return []
    out = [extremes[0]]
    for e in extremes[1:]:
        if e - out[-1] >= min_separation:
            out.append(e)
    return out


def _kbar_features(o, h, l, c):
    """單根 K 棒特徵"""
    if any(np.isnan(x) for x in [o, h, l, c]): return None
    rng = h - l
    body = abs(c - o)
    upper_shadow = h - max(c, o)
    lower_shadow = min(c, o) - l
    is_red = c >= o
    return {
        'range': rng,
        'body': body,
        'body_pct': body / rng if rng > 0 else 0,
        'upper': upper_shadow,
        'lower': lower_shadow,
        'upper_pct': upper_shadow / rng if rng > 0 else 0,
        'lower_pct': lower_shadow / rng if rng > 0 else 0,
        'is_red': is_red,
    }


def _detect_reaction_kbar(df, idx, side='bull', window=3):
    """偵測底部 / 頂部「表態」反應 K 棒
    side='bull': 錘子線 / 多方吞噬 / 紅 K 大實體
    side='bear': 射擊之星 / 空方吞噬 / 黑 K 大實體
    回傳 dict {has_reaction, type, idx}"""
    if df is None or idx < 1 or idx >= len(df):
        return {'has_reaction': False}
    n = len(df)
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values

    # 檢查 idx 前後 window 天，找最強反應 K 棒
    best = {'has_reaction': False, 'type': None, 'idx': None, 'desc': ''}
    for k in range(max(0, idx-1), min(n, idx + window + 1)):
        kf = _kbar_features(o[k], h[k], l[k], c[k])
        if kf is None: continue

        if side == 'bull':
            # ① 錘子線：下影線 ≥ 2× 實體 + 小上影
            if (kf['range'] > 0 and kf['lower'] >= kf['body'] * 2
                and kf['upper_pct'] < 0.2 and kf['body'] > 0):
                best = {'has_reaction': True, 'type': 'hammer', 'idx': k,
                        'desc': '錘子線（下影 ≥ 2× 實體）'}
                break
            # ② 多方吞噬：紅 K 實體完全包覆前一根黑 K
            if k > 0 and not np.isnan(o[k-1]) and not np.isnan(c[k-1]):
                prev_kf = _kbar_features(o[k-1], h[k-1], l[k-1], c[k-1])
                if (prev_kf and not prev_kf['is_red'] and kf['is_red']
                    and o[k] < c[k-1] and c[k] > o[k-1]):
                    best = {'has_reaction': True, 'type': 'bull_engulfing', 'idx': k,
                            'desc': '多方吞噬（紅 K 完全包前黑 K）'}
                    break
            # ③ 大紅 K（實體 ≥ 70% range）
            if kf['is_red'] and kf['body_pct'] >= 0.7:
                best = {'has_reaction': True, 'type': 'big_bullish', 'idx': k,
                        'desc': f'大實體紅 K（{kf["body_pct"]*100:.0f}% range）'}

        elif side == 'bear':
            # ① 射擊之星：上影線 ≥ 2× 實體
            if (kf['range'] > 0 and kf['upper'] >= kf['body'] * 2
                and kf['lower_pct'] < 0.2 and kf['body'] > 0):
                best = {'has_reaction': True, 'type': 'shooting_star', 'idx': k,
                        'desc': '射擊之星（上影 ≥ 2× 實體）'}
                break
            # ② 空方吞噬
            if k > 0 and not np.isnan(o[k-1]) and not np.isnan(c[k-1]):
                prev_kf = _kbar_features(o[k-1], h[k-1], l[k-1], c[k-1])
                if (prev_kf and prev_kf['is_red'] and not kf['is_red']
                    and o[k] > c[k-1] and c[k] < o[k-1]):
                    best = {'has_reaction': True, 'type': 'bear_engulfing', 'idx': k,
                            'desc': '空方吞噬（黑 K 完全包前紅 K）'}
                    break
            # ③ 大黑 K
            if not kf['is_red'] and kf['body_pct'] >= 0.7:
                best = {'has_reaction': True, 'type': 'big_bearish', 'idx': k,
                        'desc': f'大實體黑 K（{kf["body_pct"]*100:.0f}% range）'}
    return best


def _detect_liquidity_sweep(df, ref_idx, second_idx, side='bull'):
    """掃流動性 / 假突破偵測
    side='bull'：第 2 底刺穿第 1 底（前低）後拉回 + 紅 K 實體
    side='bear'：第 2 頂刺穿第 1 頂（前高）後拉回 + 黑 K 實體
    """
    if ref_idx >= len(df) or second_idx >= len(df): return False
    o = df['Open'].iloc[second_idx]
    h = df['High'].iloc[second_idx]
    l = df['Low'].iloc[second_idx]
    c = df['Close'].iloc[second_idx]
    ref_l = df['Low'].iloc[ref_idx]
    ref_h = df['High'].iloc[ref_idx]
    if any(np.isnan(x) for x in [o, h, l, c, ref_l, ref_h]): return False
    if side == 'bull':
        return l < ref_l and c > ref_l and c >= o   # 跌破前低後收紅
    else:
        return h > ref_h and c < ref_h and c <= o   # 突破前高後收黑


def _classify_position(df, lookback_long=200):
    """位置判別：大週期方向（簡化）
    回傳 'uptrend' / 'downtrend' / 'sideways'"""
    if df is None or len(df) < 60: return 'unknown'
    n = len(df)
    c = df['Close'].values
    # 用 200/120/60 MA 判斷
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


# ────────────────────────────────────────────────────────────────
# 動能衰減量化（核心 metric）
# ────────────────────────────────────────────────────────────────

def _measure_momentum_decay(df, first_idx, second_idx, side='bull', window=3):
    """測量第二次測試時，原趨勢動能是否衰減

    side='bull': 雙底 — 看空方衰減（量縮 / 黑K縮 / 下影增）
    side='bear': 雙頂 — 看多方衰減（量縮 / 紅K縮 / 上影增）

    回傳 dict {
        'volume_ratio': 第二次量 / 第一次量（< 0.8 = 量縮）
        'body_ratio': 第二次反方向K實體 / 第一次反方向K實體（< 0.7 = 縮短）
        'shadow_ratio': 第二次反向影線 / 第一次反向影線（> 1.2 = 影線變長）
        'rev_kbar_count_diff': 第二次反向K數量 - 第一次（> 0 = 增加）
        'decay_score': 0-4（4 = 全 4 項過關 = 強衰減）
    }"""
    if df is None: return {}
    n = len(df)
    if first_idx >= n or second_idx >= n: return {}

    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    v = df['Volume'].values

    # ─── ① 量比 ───
    f_start = max(0, first_idx - window)
    f_end = min(n, first_idx + window + 1)
    s_start = max(0, second_idx - window)
    s_end = min(n, second_idx + window + 1)
    vol_first = float(np.nanmean(v[f_start:f_end])) if f_end > f_start else 0
    vol_second = float(np.nanmean(v[s_start:s_end])) if s_end > s_start else 0
    vol_ratio = (vol_second / vol_first) if vol_first > 0 else 1.0

    # ─── ② 反向 K 棒實體比 ───
    # 雙底：第二次測試時的「黑 K」實體應變短
    # 雙頂：第二次測試時的「紅 K」實體應變短
    def _rev_body_avg(start, end):
        bodies = []
        for k in range(start, end):
            if any(np.isnan(x) for x in [o[k], c[k]]): continue
            if side == 'bull':  # 看黑 K
                if c[k] < o[k]: bodies.append(o[k] - c[k])
            else:  # 看紅 K
                if c[k] > o[k]: bodies.append(c[k] - o[k])
        return float(np.mean(bodies)) if bodies else 0
    body_first = _rev_body_avg(f_start, f_end)
    body_second = _rev_body_avg(s_start, s_end)
    body_ratio = (body_second / body_first) if body_first > 0 else 1.0

    # ─── ③ 反方向影線（雙底看下影；雙頂看上影）──
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

    # ─── ④ 反向 K 棒數量差（雙底看紅 K 增加；雙頂看黑 K 增加）──
    def _rev_count(start, end):
        cnt = 0
        for k in range(start, end):
            if any(np.isnan(x) for x in [o[k], c[k]]): continue
            if side == 'bull':  # 雙底看紅 K 增加 = 多方力量增
                if c[k] > o[k]: cnt += 1
            else:
                if c[k] < o[k]: cnt += 1
        return cnt
    cnt_first = _rev_count(f_start, f_end)
    cnt_second = _rev_count(s_start, s_end)
    rev_kbar_diff = cnt_second - cnt_first

    # ─── 衰減 score（0-4） ──
    score = 0
    if vol_ratio < 0.8: score += 1   # 量縮
    if body_ratio < 0.7: score += 1   # 反向 K 實體縮短
    if shadow_ratio > 1.2: score += 1  # 反方向影線變長（多/空 反擊增）
    if rev_kbar_diff > 0: score += 1   # 反向 K 棒增加（攻守易位）

    return {
        'volume_ratio':     round(vol_ratio, 2),
        'body_ratio':       round(body_ratio, 2),
        'shadow_ratio':     round(shadow_ratio, 2),
        'rev_kbar_count_diff': int(rev_kbar_diff),
        'decay_score':      int(score),  # 0-4
    }


# ────────────────────────────────────────────────────────────────
# 突破有效性（neckline breakout validity）
# ────────────────────────────────────────────────────────────────

def _check_breakout_validity(df, mid_idx, neckline, side='bull',
                              vol_avg_window=20):
    """檢查頸線突破是否有效
    1. 帶量（突破日量 ≥ 1.5× 20 日均量）
    2. 實體突破（不是上下影刺）
    3. 後續無實體跌回另一側

    回傳 dict {
        'breakout_idx': 哪一天突破,
        'volume_surge': bool,
        'body_breakout': bool,
        'no_pull_back': bool,
        'validity_score': 0-3,
        'breakout_close': 突破日收盤,
        'breakout_vol_ratio': 突破日量比,
    }"""
    if df is None or mid_idx >= len(df): return {}
    n = len(df)
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    v = df['Volume'].values

    # 從 mid_idx 後找第一個收盤站上 / 跌破 neckline 的日子
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
    # 量比
    vol_avg = (float(np.nanmean(v[max(0, bi-vol_avg_window):bi]))
                if bi >= 5 else 0)
    bi_vol_ratio = (float(v[bi]) / vol_avg) if vol_avg > 0 else 1.0
    volume_surge = bi_vol_ratio >= 1.5

    # 實體突破：開盤 + 收盤都站上 / 跌破
    if side == 'bull':
        body_breakout = o[bi] > neckline * 0.99 and c[bi] > neckline
    else:
        body_breakout = o[bi] < neckline * 1.01 and c[bi] < neckline

    # 後續確認：突破後 N 天無實體跌回 / 漲回另一側
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
        'validity_score': score,  # 0-3
        'breakout_close': round(float(c[bi]), 2),
        'breakout_vol_ratio': round(bi_vol_ratio, 2),
    }


# ────────────────────────────────────────────────────────────────
# 主入口：detect_double_bottom（W底）
# ────────────────────────────────────────────────────────────────

def detect_double_bottom(df, lookback_days=180,
                          similarity_tol=0.05, min_separation=15,
                          max_separation=120, peak_window=5,
                          min_rebound_pct=8, max_age_2nd=60):
    """雙底偵測（W底，看多反轉）

    新增 v9.22：
    - 動能衰減量化
    - 位置判別（uptrend / downtrend / sideways）
    - 反應 K 棒偵測
    - 掃流動性偵測
    - 頸線突破有效性
    - 五大關鍵 score（0-5）+ Quality 分級

    Returns:
      {
        'is_double_bottom': bool,
        'status': 'none' | 'forming' | 'A_test_buy' | 'confirmed' |
                  'B_breakout_buy' | 'C_retest_buy' | 'failed',
        'left_bottom', 'right_bottom', 'middle_peak',
        'neckline_price', 'pattern_height', 'target_price',
        # 🆕 v9.22
        'position_context': 'uptrend' / 'downtrend' / 'sideways',
        'position_quality': 'high_prob' / 'trap_risk' / 'neutral',
        'momentum_decay': dict,
        'reaction_kbar': dict,
        'liquidity_sweep': bool,
        'breakout_validity': dict,
        'quality_score': 0-5（5 大關鍵過關數）,
        'quality_grade': 'A' / 'B' / 'C' / 'D',  # 5/4/3/<3
        'entry_stage': 'A_test' / 'B_breakout' / 'C_retest' / 'wait',
        'stop_loss': float,  # 結構低點
      }
    """
    if df is None or len(df) < 60:
        return {'is_double_bottom': False, 'status': 'none'}

    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    n = len(df)
    start = max(0, n - lookback_days)
    h_s = h[start:]; l_s = l[start:]; c_s = c[start:]
    m = len(c_s)
    if m < 30:
        return {'is_double_bottom': False, 'status': 'none'}

    minima = _find_local_extremes(l_s, window=peak_window, find_min=True)
    minima = _filter_close_extremes(minima, min_separation=10)
    if len(minima) < 2:
        return {'is_double_bottom': False, 'status': 'none'}

    # 找最近合格雙底
    best_pair = None
    for i in range(len(minima)-1, 0, -1):
        for j in range(i-1, -1, -1):
            sep = minima[i] - minima[j]
            if sep < min_separation: continue
            if sep > max_separation: break
            p_low_left = float(l_s[minima[j]])
            p_low_right = float(l_s[minima[i]])
            if min(p_low_left, p_low_right) <= 0: continue
            sim = abs(p_low_right - p_low_left) / min(p_low_left, p_low_right)
            if sim <= similarity_tol:
                mid_slice = h_s[minima[j]:minima[i]+1]
                if len(mid_slice) == 0: continue
                mid_idx_local = int(np.nanargmax(mid_slice))
                mid_idx = minima[j] + mid_idx_local
                neckline = float(h_s[mid_idx])
                rebound_pct = (neckline - p_low_left) / p_low_left * 100
                if rebound_pct < min_rebound_pct:
                    continue
                days_since = m - 1 - minima[i]
                if days_since > max_age_2nd:
                    continue
                best_pair = (j, i, mid_idx, sim, rebound_pct)
                break
        if best_pair:
            break

    if not best_pair:
        return {'is_double_bottom': False, 'status': 'none'}

    j_idx, i_idx, mid_idx, sim, rebound_pct = best_pair
    left_idx = minima[j_idx]
    right_idx = minima[i_idx]

    p_low_left = float(l_s[left_idx])
    p_low_right = float(l_s[right_idx])
    avg_bottom = (p_low_left + p_low_right) / 2
    neckline = float(h_s[mid_idx])
    height = neckline - avg_bottom
    target = neckline + height
    cur_close = float(c_s[-1])

    # 轉成原 df idx
    real_left = start + left_idx
    real_right = start + right_idx
    real_mid = start + mid_idx
    df_idx = df.index

    # ─── 五大關鍵分析 ────
    # 1. 位置（context）
    pos_ctx = _classify_position(df)
    # 雙底高勝率位置：downtrend 末端 OR uptrend 中的回測支撐
    if pos_ctx == 'downtrend':
        pos_quality = 'high_prob'  # 下跌末端反轉
    elif pos_ctx == 'uptrend':
        pos_quality = 'high_prob'  # 上升回測（continuation）
    elif pos_ctx == 'sideways':
        pos_quality = 'neutral'
    else:
        pos_quality = 'neutral'

    # 2. 動能衰減
    decay = _measure_momentum_decay(df, real_left, real_right, side='bull', window=3)

    # 3. 反應 K 棒（在第 2 底附近找）
    reaction = _detect_reaction_kbar(df, real_right, side='bull', window=3)

    # 4. 掃流動性（第 2 底是否刺穿第 1 底）
    liq_sweep = _detect_liquidity_sweep(df, real_left, real_right, side='bull')

    # 5. 頸線突破有效性
    valid = _check_breakout_validity(df, real_mid, neckline, side='bull')

    # ─── 五大關鍵 score（0-5） ───
    quality_score = 0
    if pos_quality == 'high_prob': quality_score += 1
    if (decay or {}).get('decay_score', 0) >= 2: quality_score += 1   # 動能衰減 ≥ 2/4
    if reaction.get('has_reaction'): quality_score += 1
    if liq_sweep or reaction.get('type') in ('hammer', 'bull_engulfing'):
        # 掃流動性 OR 強反應 K → 加分（避免重複，這裡用 OR 而非 +1）
        pass
    if valid.get('validity_score', 0) >= 2: quality_score += 1   # 突破 ≥ 2/3
    # 第 5 項：rebound 強度 + similarity tightness
    if rebound_pct >= 12 and sim <= 0.03:
        quality_score += 1

    quality_grade = 'A' if quality_score >= 5 else ('B' if quality_score >= 4
                    else ('C' if quality_score >= 3 else 'D'))

    # ─── 三段建倉狀態 ───
    days_since_2nd = m - 1 - right_idx
    breakout_idx_real = (start + valid['breakout_idx']) if valid.get('breakout_idx') is not None else None

    if cur_close < min(p_low_left, p_low_right) * 0.97:
        status = 'failed'
        entry_stage = 'wait'
    elif breakout_idx_real and cur_close < neckline and cur_close > p_low_right:
        # 已突破過但目前回測 neckline → C 補滿
        if reaction.get('has_reaction') and breakout_idx_real < n - 3:
            status = 'C_retest_buy'
            entry_stage = 'C_retest'
        else:
            status = 'confirmed'
            entry_stage = 'wait'
    elif cur_close > neckline and breakout_idx_real:
        status = 'B_breakout_buy'
        entry_stage = 'B_breakout'
    elif reaction.get('has_reaction') and right_idx >= m - 5:
        # 第 2 底剛形成 + 反應 K → A 試單
        status = 'A_test_buy'
        entry_stage = 'A_test'
    elif right_idx >= m - 5:
        status = 'forming'
        entry_stage = 'wait'
    else:
        status = 'confirmed'
        entry_stage = 'wait'

    # 結構停損
    if entry_stage == 'A_test':
        stop_loss = float(p_low_right) * 0.98
    elif entry_stage == 'B_breakout':
        stop_loss = float(neckline) * 0.97
    elif entry_stage == 'C_retest':
        if reaction.get('idx') is not None:
            stop_loss = float(min(l[reaction['idx']],
                                    l[real_right]) * 0.98)
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
        'separation_days': int(right_idx - left_idx),
        'days_since_2nd_bottom': int(days_since_2nd),
        # 🆕 v9.22 五大關鍵
        'position_context': pos_ctx,
        'position_quality': pos_quality,
        'momentum_decay': decay,
        'reaction_kbar': reaction,
        'liquidity_sweep': bool(liq_sweep),
        'breakout_validity': valid,
        'quality_score': quality_score,  # 0-5
        'quality_grade': quality_grade,  # A/B/C/D
    }


# ────────────────────────────────────────────────────────────────
# detect_double_top（M頂）— 鏡像
# ────────────────────────────────────────────────────────────────

def detect_double_top(df, lookback_days=180,
                       similarity_tol=0.05, min_separation=15,
                       max_separation=120, peak_window=5,
                       min_pullback_pct=8, max_age_2nd=60):
    """雙頂偵測（M頂，看空反轉）— 結構同 detect_double_bottom 鏡像"""
    if df is None or len(df) < 60:
        return {'is_double_top': False, 'status': 'none'}

    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    n = len(df)
    start = max(0, n - lookback_days)
    h_s = h[start:]; l_s = l[start:]; c_s = c[start:]
    m = len(c_s)
    if m < 30:
        return {'is_double_top': False, 'status': 'none'}

    maxima = _find_local_extremes(h_s, window=peak_window, find_min=False)
    maxima = _filter_close_extremes(maxima, min_separation=10)
    if len(maxima) < 2:
        return {'is_double_top': False, 'status': 'none'}

    best_pair = None
    for i in range(len(maxima)-1, 0, -1):
        for j in range(i-1, -1, -1):
            sep = maxima[i] - maxima[j]
            if sep < min_separation: continue
            if sep > max_separation: break
            p_high_left = float(h_s[maxima[j]])
            p_high_right = float(h_s[maxima[i]])
            if min(p_high_left, p_high_right) <= 0: continue
            sim = abs(p_high_right - p_high_left) / min(p_high_left, p_high_right)
            if sim <= similarity_tol:
                mid_slice = l_s[maxima[j]:maxima[i]+1]
                if len(mid_slice) == 0: continue
                mid_idx_local = int(np.nanargmin(mid_slice))
                mid_idx = maxima[j] + mid_idx_local
                neckline = float(l_s[mid_idx])
                pullback_pct = (p_high_left - neckline) / p_high_left * 100
                if pullback_pct < min_pullback_pct:
                    continue
                days_since = m - 1 - maxima[i]
                if days_since > max_age_2nd:
                    continue
                best_pair = (j, i, mid_idx, sim, pullback_pct)
                break
        if best_pair:
            break

    if not best_pair:
        return {'is_double_top': False, 'status': 'none'}

    j_idx, i_idx, mid_idx, sim, pullback_pct = best_pair
    left_idx = maxima[j_idx]
    right_idx = maxima[i_idx]

    p_high_left = float(h_s[left_idx])
    p_high_right = float(h_s[right_idx])
    avg_top = (p_high_left + p_high_right) / 2
    neckline = float(l_s[mid_idx])
    height = avg_top - neckline
    target = neckline - height
    cur_close = float(c_s[-1])

    real_left = start + left_idx
    real_right = start + right_idx
    real_mid = start + mid_idx
    df_idx = df.index

    # 位置：雙頂高勝率位置 = uptrend 末端 OR downtrend 反彈到壓力
    pos_ctx = _classify_position(df)
    if pos_ctx == 'uptrend':
        pos_quality = 'high_prob'
    elif pos_ctx == 'downtrend':
        pos_quality = 'high_prob'
    elif pos_ctx == 'sideways':
        pos_quality = 'neutral'
    else:
        pos_quality = 'neutral'

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

    days_since_2nd = m - 1 - right_idx
    breakdown_idx_real = (start + valid['breakout_idx']) if valid.get('breakout_idx') is not None else None

    if cur_close > max(p_high_left, p_high_right) * 1.03:
        status = 'failed'
        entry_stage = 'wait'
    elif breakdown_idx_real and cur_close > neckline and cur_close < p_high_right:
        if reaction.get('has_reaction') and breakdown_idx_real < n - 3:
            status = 'C_retest_short'
            entry_stage = 'C_retest'
        else:
            status = 'confirmed'
            entry_stage = 'wait'
    elif cur_close < neckline and breakdown_idx_real:
        status = 'B_breakdown_short'
        entry_stage = 'B_breakout'
    elif reaction.get('has_reaction') and right_idx >= m - 5:
        status = 'A_test_short'
        entry_stage = 'A_test'
    elif right_idx >= m - 5:
        status = 'forming'
        entry_stage = 'wait'
    else:
        status = 'confirmed'
        entry_stage = 'wait'

    if entry_stage == 'A_test':
        stop_loss = float(p_high_right) * 1.02
    elif entry_stage == 'B_breakout':
        stop_loss = float(neckline) * 1.03
    elif entry_stage == 'C_retest':
        if reaction.get('idx') is not None:
            stop_loss = float(max(h[reaction['idx']],
                                    h[real_right]) * 1.02)
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
        'separation_days': int(right_idx - left_idx),
        'days_since_2nd_top': int(days_since_2nd),
        # 🆕 v9.22
        'position_context': pos_ctx,
        'position_quality': pos_quality,
        'momentum_decay': decay,
        'reaction_kbar': reaction,
        'liquidity_sweep': bool(liq_sweep),
        'breakout_validity': valid,
        'quality_score': quality_score,
        'quality_grade': quality_grade,
    }
