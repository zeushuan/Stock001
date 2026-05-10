"""SEPA / VCP / RS Rating 檢測模組（v9.19）
==========================================================
依 Mark Minervini《Trade Like a Stock Market Wizard》策略：

1. SEPA Trend Template — 8 條件多頭體質檢查
2. VCP（Volatility Contraction Pattern）— 形態偵測
3. RS Rating — 相對強度評分（0-100，需 universe-wide 計算）

被 screener_filters._get_state 引用。
"""
import numpy as np
import pandas as pd


# ────────────────────────────────────────────────────────────────
# 1. Trend Template（8 條件）
# ────────────────────────────────────────────────────────────────

def check_sepa_trend_template(c, sma50, sma150, sma200, sma200_30d_ago,
                                 high_52w, low_52w):
    """SEPA Trend Template 8 條件檢查
    回傳：(passed: bool, n_conditions_met: int, details: dict)

    8 條件：
    1. close > SMA150 AND close > SMA200
    2. SMA150 > SMA200
    3. SMA200 已上升 ≥ 1 個月（vs 30d 前）
    4. SMA50 > SMA150 AND SMA50 > SMA200
    5. close > SMA50
    6. close ≥ 52週低點 + 30%
    7. close ≤ 52週高點 - 25%
    8. RS Rating ≥ 70（外部判斷，這裡先給 placeholder）
    """
    if any(x is None for x in [c, sma50, sma150, sma200]):
        return (False, 0, {})
    if sma200 == 0 or sma150 == 0 or sma50 == 0:
        return (False, 0, {})

    cond1 = c > sma150 and c > sma200
    cond2 = sma150 > sma200
    cond3 = (sma200_30d_ago is not None and sma200_30d_ago > 0
             and sma200 > sma200_30d_ago)  # 上升中
    cond4 = sma50 > sma150 and sma50 > sma200
    cond5 = c > sma50
    cond6 = (low_52w is not None and low_52w > 0
             and c >= low_52w * 1.30)
    # 🐛 fix v9.19：cond7 是「距 52w 高 ≤ 25%」(close 在高點附近)
    # 不是「close ≤ high × 0.75」(close 距高點 > 25%)
    cond7 = (high_52w is not None and high_52w > 0
             and c >= high_52w * 0.75)

    conds = [cond1, cond2, cond3, cond4, cond5, cond6, cond7]
    n_met = sum(conds)
    all_passed = all(conds)

    details = {
        'cond1_close_above_150_200':  cond1,
        'cond2_sma150_above_200':     cond2,
        'cond3_sma200_rising_30d':    cond3,
        'cond4_sma50_above_150_200':  cond4,
        'cond5_close_above_50':       cond5,
        'cond6_above_52w_low_30pct':  cond6,
        'cond7_below_52w_high_25pct': cond7,
        'n_met':                       n_met,
        'all_passed':                  all_passed,
        # cond8 RS≥70 由 screener_full_cloud 注入
    }
    return (all_passed, n_met, details)


# ────────────────────────────────────────────────────────────────
# 2. VCP（Volatility Contraction Pattern）
# ────────────────────────────────────────────────────────────────

def detect_vcp(df, lookback_days=180, min_contractions=2,
               max_contractions=5, peak_window=5):
    """VCP 形態偵測

    Args:
        df: pandas DataFrame with High, Low, Close
        lookback_days: 看回幾天
        min_contractions: 最少 contraction 次數（≥ 2）
        max_contractions: 最多（防誤判）
        peak_window: peak 偵測窗口（前後 N 天）

    Returns:
        dict {
          'is_vcp': bool,
          'n_contractions': int,
          'declines_pct': list[float],   # 每次跌幅 %
          'is_contracting': bool,         # 跌幅是否遞減
          'pivot_price': float,           # 最近 peak（pivot point）
          'near_pivot_pct': float,        # 現價距 pivot 的 %（負=低於）
          'near_pivot': bool,             # 距 pivot ≤ 5%（接近進場點）
          'volume_dry_up': bool,          # 整理期間量縮 ≥ 30%
        }
        若資料不足回傳 {'is_vcp': False}
    """
    if df is None or len(df) < 60:
        return {'is_vcp': False}

    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    v = df['Volume'].values

    n = len(df)
    # 取最近 lookback_days 的資料
    start = max(0, n - lookback_days)
    h_s = h[start:]
    l_s = l[start:]
    c_s = c[start:]
    v_s = v[start:]
    m = len(c_s)
    if m < 60:
        return {'is_vcp': False}

    # ① 找 peaks（local maxima）
    peaks = []
    for i in range(peak_window, m - peak_window):
        if np.isnan(h_s[i]):
            continue
        # 前後 peak_window 天都 ≤ 自己
        win_left = h_s[i-peak_window:i]
        win_right = h_s[i+1:i+peak_window+1]
        if (len(win_left) > 0 and len(win_right) > 0
            and h_s[i] >= np.nanmax(win_left)
            and h_s[i] >= np.nanmax(win_right)):
            peaks.append(i)

    # 過濾相鄰過近的 peaks（差 < 10 天的當作同一個）
    if peaks:
        filtered_peaks = [peaks[0]]
        for p in peaks[1:]:
            if p - filtered_peaks[-1] >= 10:
                filtered_peaks.append(p)
        peaks = filtered_peaks

    if len(peaks) < min_contractions:
        return {
            'is_vcp': False,
            'n_contractions': len(peaks),
            'declines_pct': [],
        }

    # 限制最多 max_contractions（取最近的）
    if len(peaks) > max_contractions:
        peaks = peaks[-max_contractions:]

    # ② 計算每次 contraction（peak → 之後最低點）
    contractions = []
    for j, p_idx in enumerate(peaks):
        peak_high = h_s[p_idx]
        # 下一個 peak（或結束）為終點
        next_p = peaks[j+1] if j+1 < len(peaks) else m - 1
        if next_p <= p_idx:
            continue
        trough_low = np.nanmin(l_s[p_idx:next_p+1])
        if peak_high <= 0:
            continue
        decline_pct = (peak_high - trough_low) / peak_high * 100
        contractions.append({
            'peak_idx': p_idx, 'next_idx': next_p,
            'peak_high': float(peak_high),
            'trough_low': float(trough_low),
            'decline_pct': float(decline_pct),
        })

    # ③ 加上最後一段（最後 peak 到現在）
    last_peak = peaks[-1]
    if last_peak < m - 3:
        # 最後 peak 之後到今天的最低點
        trough_after = np.nanmin(l_s[last_peak:])
        last_decline = (h_s[last_peak] - trough_after) / h_s[last_peak] * 100 if h_s[last_peak] > 0 else 0
        # 取代最後一個（如果剛剛已加）
        if contractions and contractions[-1]['next_idx'] >= m - 3:
            contractions[-1]['decline_pct'] = float(last_decline)
        else:
            contractions.append({
                'peak_idx': last_peak, 'next_idx': m-1,
                'peak_high': float(h_s[last_peak]),
                'trough_low': float(trough_after),
                'decline_pct': float(last_decline),
            })

    declines_pct = [round(con['decline_pct'], 2) for con in contractions]

    # ④ 檢查跌幅是否遞減（容許 +2% 緩衝）
    is_contracting = (len(declines_pct) >= 2
                      and all(declines_pct[i] >= declines_pct[i+1] - 2
                              for i in range(len(declines_pct)-1)))

    # ⑤ Pivot point = 最近 peak 的高
    pivot_price = float(h_s[last_peak]) if last_peak >= 0 else 0
    cur_close = float(c_s[-1])
    near_pivot_pct = ((cur_close - pivot_price) / pivot_price * 100
                      if pivot_price > 0 else -99)
    near_pivot = -8 <= near_pivot_pct <= 5

    # ⑥ Volume dry up（整理期間量縮）
    if last_peak < m - 5:
        recent_vol = np.nanmean(v_s[last_peak:])
        prior_vol = np.nanmean(v_s[max(0, last_peak-30):last_peak]) if last_peak >= 30 else 0
        volume_dry_up = (prior_vol > 0 and recent_vol / prior_vol < 0.7)
    else:
        volume_dry_up = False

    is_vcp = (is_contracting and len(contractions) >= min_contractions and near_pivot)

    return {
        'is_vcp':            bool(is_vcp),
        'n_contractions':    int(len(contractions)),
        'declines_pct':      declines_pct,
        'is_contracting':    bool(is_contracting),
        'pivot_price':       round(pivot_price, 2),
        'near_pivot_pct':    round(near_pivot_pct, 2),
        'near_pivot':        bool(near_pivot),
        'volume_dry_up':     bool(volume_dry_up),
    }


# ────────────────────────────────────────────────────────────────
# 3. RS Rating（universe-wide 計算）
# ────────────────────────────────────────────────────────────────

def compute_rs_ratings(returns_dict, weights=(2, 1, 1, 1)):
    """計算 RS Rating（0-100 percentile）

    Mark Minervini 加權：13w * 2 + 26w + 39w + 52w
    回傳：{ticker: rs_score (0-100)}

    Args:
        returns_dict: {ticker: {'13w': pct, '26w': pct, '39w': pct, '52w': pct}}
        weights: (w13, w26, w39, w52)

    Returns:
        dict {ticker: rs_rating} (0-100, percentile rank)
    """
    if not returns_dict:
        return {}

    composites = {}
    for ticker, rets in returns_dict.items():
        if not rets:
            continue
        try:
            r13 = float(rets.get('13w', 0) or 0)
            r26 = float(rets.get('26w', 0) or 0)
            r39 = float(rets.get('39w', 0) or 0)
            r52 = float(rets.get('52w', 0) or 0)
            score = (weights[0] * r13 + weights[1] * r26
                     + weights[2] * r39 + weights[3] * r52)
            composites[ticker] = score
        except Exception:
            continue

    if not composites:
        return {}

    # Percentile rank
    sorted_scores = sorted(composites.items(), key=lambda x: x[1])
    n = len(sorted_scores)
    rs_ratings = {}
    for rank, (ticker, _) in enumerate(sorted_scores):
        rs_ratings[ticker] = round((rank + 1) / n * 100, 1)
    return rs_ratings


def compute_returns(df, periods_days=(65, 130, 195, 252)):
    """計算多期間百分比報酬（給 RS 用）
    65d ≈ 13w, 130d ≈ 26w, 195d ≈ 39w, 252d ≈ 52w
    回傳 dict {'13w': pct, '26w': pct, '39w': pct, '52w': pct}"""
    if df is None or len(df) < 30:
        return {}
    try:
        c = df['Close'].values
        cur = c[-1]
        if cur <= 0 or np.isnan(cur):
            return {}
    except Exception:
        return {}
    out = {}
    labels = ['13w', '26w', '39w', '52w']
    for label, days in zip(labels, periods_days):
        if len(c) > days:
            past = c[-days-1]
            if past > 0 and not np.isnan(past):
                out[label] = (cur - past) / past * 100
            else:
                out[label] = 0
        else:
            out[label] = 0
    return out


# ────────────────────────────────────────────────────────────────
# 4. SMA helper（150 / 200 / 30d ago）
# ────────────────────────────────────────────────────────────────

def compute_sma_helpers(df):
    """算出 SEPA Trend Template 需要的 SMA 值
    回傳 dict {'sma50', 'sma150', 'sma200', 'sma200_30d_ago',
               'high_52w', 'low_52w', 'from_52w_low', 'from_52w_high'}"""
    if df is None or len(df) < 60:
        return {}
    try:
        c = df['Close'].values
        h = df['High'].values
        l = df['Low'].values
        n = len(df)
        if n == 0 or np.isnan(c[-1]):
            return {}

        sma50 = float(np.nanmean(c[-50:])) if n >= 50 else None
        sma150 = float(np.nanmean(c[-150:])) if n >= 150 else None
        sma200 = float(np.nanmean(c[-200:])) if n >= 200 else None
        # 30 天前的 sma200
        sma200_30d_ago = (float(np.nanmean(c[-230:-30]))
                           if n >= 230 else None)
        # 52 週高低（252 個交易日）
        high_52w = float(np.nanmax(h[-252:])) if n >= 60 else None
        low_52w = float(np.nanmin(l[-252:])) if n >= 60 else None
        cur = float(c[-1])
        from_52w_low = ((cur - low_52w) / low_52w * 100
                         if low_52w and low_52w > 0 else 0)
        from_52w_high = ((high_52w - cur) / high_52w * 100
                          if high_52w and high_52w > 0 else 0)

        return {
            'sma50': sma50, 'sma150': sma150, 'sma200': sma200,
            'sma200_30d_ago': sma200_30d_ago,
            'high_52w': high_52w, 'low_52w': low_52w,
            'from_52w_low': round(from_52w_low, 2),
            'from_52w_high': round(from_52w_high, 2),
        }
    except Exception:
        return {}
