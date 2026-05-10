"""雙底雙頂形態偵測（v9.21）
==========================================================
Double Bottom (W底, 看多反轉) / Double Top (M頂, 看空反轉)

形態定義（Mark Minervini / William O'Neil 標準）
-----------------------------------------------
雙底 (W 形):
  ① 兩個低點價格相似（容差 ≤ 5%）
  ② 兩個低點間距 15-120 天
  ③ 中間有明顯反彈（≥ 5% from bottom）形成 neckline
  ④ 突破 neckline 確認 → 進場
  ⑤ 目標價 = neckline + (neckline - bottom)

雙頂 (M 形):
  ① 兩個高點價格相似
  ② 中間有明顯回檔（≥ 5%）
  ③ 跌破 neckline 確認 → 出場 / 放空
  ④ 目標價 = neckline - (top - neckline)

成熟度分類
-----------
- forming: 第一個底/頂剛形成，等第二次測試
- confirmed: 兩個底/頂都形成 + 中間有反彈
- breakout: 已突破 neckline（最強訊號）
- failed: 跌破第一個低點（雙底失敗 → 進入下跌）
"""
import numpy as np
import pandas as pd


def _find_local_extremes(values, window=5, find_min=False):
    """找 local maxima 或 minima
    回傳 list of indices"""
    n = len(values)
    extremes = []
    for i in range(window, n - window):
        if np.isnan(values[i]):
            continue
        win_l = values[i-window:i]
        win_r = values[i+1:i+window+1]
        if len(win_l) == 0 or len(win_r) == 0:
            continue
        if find_min:
            if values[i] <= np.nanmin(win_l) and values[i] <= np.nanmin(win_r):
                extremes.append(i)
        else:
            if values[i] >= np.nanmax(win_l) and values[i] >= np.nanmax(win_r):
                extremes.append(i)
    return extremes


def _filter_close_extremes(extremes, min_separation=10):
    """過濾相鄰過近的 extremes（< min_separation 天）"""
    if not extremes: return []
    out = [extremes[0]]
    for e in extremes[1:]:
        if e - out[-1] >= min_separation:
            out.append(e)
    return out


def detect_double_bottom(df, lookback_days=180,
                          similarity_tol=0.03, min_separation=20,
                          max_separation=120, peak_window=5,
                          min_rebound_pct=10, max_age_2nd=60):
    """雙底偵測

    Returns dict:
      {
        'is_double_bottom': bool,
        'status': 'none' | 'forming' | 'confirmed' | 'breakout' | 'failed',
        'left_bottom':  {'date', 'price'} or None,
        'right_bottom': {'date', 'price'} or None,
        'middle_peak':  {'date', 'price'} or None,
        'neckline_price': float,
        'pattern_height': float,  # neckline - 平均底
        'breakout_confirmed': bool,
        'target_price': float,   # neckline + height
        'similarity_pct': float, # 兩底差異 %
        'separation_days': int,
        'days_since_2nd_bottom': int,  # 用於判斷新鮮度
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

    # 找 local minima（用 low）
    minima = _find_local_extremes(l_s, window=peak_window, find_min=True)
    minima = _filter_close_extremes(minima, min_separation=10)
    if len(minima) < 2:
        return {'is_double_bottom': False, 'status': 'none',
                'n_minima': len(minima)}

    # 找最近的「合格」雙底（取最後 2 個價格相近的 minima）
    best_pair = None
    for i in range(len(minima)-1, 0, -1):  # 從最近的開始往回找
        for j in range(i-1, -1, -1):
            sep = minima[i] - minima[j]
            if sep < min_separation: continue
            if sep > max_separation: break
            p_low_left = float(l_s[minima[j]])
            p_low_right = float(l_s[minima[i]])
            if min(p_low_left, p_low_right) <= 0: continue
            sim = abs(p_low_right - p_low_left) / min(p_low_left, p_low_right)
            if sim <= similarity_tol:
                # 找中間最高點
                mid_slice = h_s[minima[j]:minima[i]+1]
                if len(mid_slice) == 0: continue
                mid_idx_local = int(np.nanargmax(mid_slice))
                mid_idx = minima[j] + mid_idx_local
                neckline = float(h_s[mid_idx])
                # 中間反彈幅度（從第一個底到 neckline）≥ min_rebound_pct
                rebound_pct = (neckline - p_low_left) / p_low_left * 100
                if rebound_pct < min_rebound_pct:
                    continue
                # 🆕 v9.21：第 2 底必須在最近 max_age_2nd 天內（避免過期 pattern）
                days_since = m - 1 - minima[i]
                if days_since > max_age_2nd:
                    continue
                best_pair = (j, i, mid_idx, sim, rebound_pct)
                break
        if best_pair:
            break

    if not best_pair:
        return {'is_double_bottom': False, 'status': 'none',
                'n_minima': len(minima)}

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
    days_since_2nd = m - 1 - right_idx

    # 狀態判斷
    if cur_close > neckline:
        status = 'breakout'
    elif cur_close < min(p_low_left, p_low_right) * 0.97:
        # 跌破第一個底 -3% → failed
        status = 'failed'
    elif right_idx >= m - 5:
        # 第二個底剛形成（5 天內）
        status = 'forming'
    else:
        status = 'confirmed'

    df_idx = df.index
    real_left = start + left_idx
    real_right = start + right_idx
    real_mid = start + mid_idx

    return {
        'is_double_bottom': status in ('confirmed', 'breakout', 'forming'),
        'status': status,
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
        'similarity_pct': round(sim * 100, 2),
        'rebound_pct': round(rebound_pct, 2),
        'separation_days': int(right_idx - left_idx),
        'days_since_2nd_bottom': int(days_since_2nd),
    }


def detect_double_top(df, lookback_days=180,
                       similarity_tol=0.03, min_separation=20,
                       max_separation=120, peak_window=5,
                       min_pullback_pct=10, max_age_2nd=60):
    """雙頂偵測（鏡像於雙底）

    Returns dict（結構同 detect_double_bottom，欄位改 top）:
      {
        'is_double_top': bool,
        'status': 'none' | 'forming' | 'confirmed' | 'breakdown' | 'failed',
        'left_top':  {'date', 'price'},
        'right_top': {'date', 'price'},
        'middle_trough': {'date', 'price'},
        'neckline_price': float,
        'pattern_height': float,
        'breakdown_confirmed': bool,
        'target_price': float,  # neckline - height
        'similarity_pct': float,
        'separation_days': int,
        'days_since_2nd_top': int,
      }
    """
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
        return {'is_double_top': False, 'status': 'none',
                'n_maxima': len(maxima)}

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
                # 中間最低點
                mid_slice = l_s[maxima[j]:maxima[i]+1]
                if len(mid_slice) == 0: continue
                mid_idx_local = int(np.nanargmin(mid_slice))
                mid_idx = maxima[j] + mid_idx_local
                neckline = float(l_s[mid_idx])
                # 回檔幅度
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
        return {'is_double_top': False, 'status': 'none',
                'n_maxima': len(maxima)}

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
    days_since_2nd = m - 1 - right_idx

    if cur_close < neckline:
        status = 'breakdown'
    elif cur_close > max(p_high_left, p_high_right) * 1.03:
        status = 'failed'  # 突破第一個頂 = 雙頂失敗 → 繼續上漲
    elif right_idx >= m - 5:
        status = 'forming'
    else:
        status = 'confirmed'

    df_idx = df.index
    real_left = start + left_idx
    real_right = start + right_idx
    real_mid = start + mid_idx

    return {
        'is_double_top': status in ('confirmed', 'breakdown', 'forming'),
        'status': status,
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
        'similarity_pct': round(sim * 100, 2),
        'pullback_pct': round(pullback_pct, 2),
        'separation_days': int(right_idx - left_idx),
        'days_since_2nd_top': int(days_since_2nd),
    }
