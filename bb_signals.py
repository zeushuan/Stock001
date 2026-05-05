"""Bollinger Bands 全套判斷模組（v9.12）
=============================================
依 OANDA BB 文章內容實作 10 種判斷：

1. **BB Squeeze**（頻寬收縮 → 大行情前兆）
2. **BB Expansion**（頻寬突放 → 趨勢啟動）
3. **Walking Up the Band**（連 N 天 > 中軌，強勢延續）
4. **Walking Down the Band**（連 N 天 < 中軌，弱勢延續）
5. **%B Extreme High** (%B > 1.0 過熱反轉)
6. **%B Extreme Low** (%B < 0 過冷反彈)
7. **Mean Reversion High**（close 遠離中軌上方反轉）
8. **Mean Reversion Low**（close 遠離中軌下方反彈）
9. **W Bottom**（雙觸下軌，第二次更淺，反轉訊號）
10. **M Top**（雙觸上軌，第二次更淺，反轉訊號）

每個函數接受 BB 計算後的 series，回傳布林值 / 連續天數。
可直接被 _detect_alerts / detail card 調用。
"""
import numpy as np
import pandas as pd


def compute_bb(close, period=20, std_mult=2):
    """計算 BB 上中下軌、bandwidth、%B"""
    sma = pd.Series(close).rolling(period, min_periods=period//2).mean().values
    std = pd.Series(close).rolling(period, min_periods=period//2).std().values
    bbu = sma + std_mult * std
    bbl = sma - std_mult * std
    bandwidth = np.where(sma > 0, (bbu - bbl) / sma * 100, 0)
    pct_b = np.where(bbu > bbl, (close - bbl) / (bbu - bbl), 0.5)
    return {'sma': sma, 'bbu': bbu, 'bbl': bbl,
            'bandwidth': bandwidth, 'pct_b': pct_b}


# ── 1. BB Squeeze ──
def is_squeeze(bandwidth, t, lookback=120, percentile=20, days=5):
    """連續 N 天 bandwidth 落在過去 lookback 天最低 percentile%"""
    if t < lookback or t < days - 1: return False
    bw_window = bandwidth[t-lookback:t]
    bw_window = bw_window[~np.isnan(bw_window)]
    if len(bw_window) < lookback // 2: return False
    threshold = np.percentile(bw_window, percentile)
    recent = bandwidth[t-days+1:t+1]
    if any(np.isnan(r) for r in recent): return False
    return all(b <= threshold for b in recent)


def squeeze_percentile(bandwidth, t, lookback=120):
    """目前 bandwidth 落在過去 lookback 天的 percentile（0-100）"""
    if t < lookback: return None
    bw_window = bandwidth[t-lookback:t]
    bw_window = bw_window[~np.isnan(bw_window)]
    if len(bw_window) < lookback // 2 or np.isnan(bandwidth[t]): return None
    return float((bw_window <= bandwidth[t]).mean() * 100)


# ── 2. BB Expansion ──
def is_expansion(bandwidth, t, ratio=1.5, lookback=20):
    """bandwidth 比過去 N 天平均放大 X 倍以上"""
    if t < lookback: return False
    avg = np.nanmean(bandwidth[t-lookback:t])
    if np.isnan(avg) or avg <= 0: return False
    return bandwidth[t] >= avg * ratio


# ── 3. Walking Up the Band ──
def is_walking_up(close, sma, bbu, t, days=5):
    """連續 N 天 close > 中軌，且至少 1 次觸碰上軌"""
    if t < days - 1: return False
    above_mid = all(close[t-i] > sma[t-i] for i in range(days)
                     if not np.isnan(sma[t-i]))
    touched_upper = any(close[t-i] >= bbu[t-i] * 0.99 for i in range(days)
                          if not np.isnan(bbu[t-i]))
    return above_mid and touched_upper


# ── 4. Walking Down the Band ──
def is_walking_down(close, sma, bbl, t, days=5):
    """連續 N 天 close < 中軌，且至少 1 次觸碰下軌"""
    if t < days - 1: return False
    below_mid = all(close[t-i] < sma[t-i] for i in range(days)
                     if not np.isnan(sma[t-i]))
    touched_lower = any(close[t-i] <= bbl[t-i] * 1.01 for i in range(days)
                          if not np.isnan(bbl[t-i]))
    return below_mid and touched_lower


# ── 5. %B Extreme High（過熱）──
def pct_b_extreme_high(pct_b, t, threshold=1.0):
    """%B > threshold（預設 1.0 = 收在上軌之外）"""
    if np.isnan(pct_b[t]): return False
    return pct_b[t] > threshold


# ── 6. %B Extreme Low（過冷）──
def pct_b_extreme_low(pct_b, t, threshold=0.0):
    """%B < threshold（預設 0 = 收在下軌之外）"""
    if np.isnan(pct_b[t]): return False
    return pct_b[t] < threshold


# ── 7. Mean Reversion High（從上方回歸）──
def mean_reversion_high(close, sma, bbu, pct_b, t, lookback=3):
    """近期 %B > 0.85 後跌回中軌（均值回歸做空）"""
    if t < lookback: return False
    recent_overheat = any(pct_b[t-i] > 0.85 for i in range(1, lookback+1)
                            if not np.isnan(pct_b[t-i]))
    near_mid = (not np.isnan(sma[t]) and abs(close[t] - sma[t]) / sma[t] < 0.02)
    return recent_overheat and near_mid


# ── 8. Mean Reversion Low（從下方回歸）──
def mean_reversion_low(close, sma, bbl, pct_b, t, lookback=3):
    """近期 %B < 0.15 後反彈回中軌"""
    if t < lookback: return False
    recent_oversold = any(pct_b[t-i] < 0.15 for i in range(1, lookback+1)
                            if not np.isnan(pct_b[t-i]))
    near_mid = (not np.isnan(sma[t]) and abs(close[t] - sma[t]) / sma[t] < 0.02)
    return recent_oversold and near_mid


# ── 9. W Bottom（雙觸下軌反轉）──
def is_w_bottom(low, close, bbl, sma, t, lookback=20):
    """近 N 天兩次觸 BBL，第二次低點 > 第一次低點，且 close > 中軌"""
    if t < lookback: return False
    # 找近 N 天兩次觸碰 bbl 的 index
    touches = []
    for i in range(lookback):
        idx = t - i
        if idx < 0: break
        if not np.isnan(bbl[idx]) and low[idx] <= bbl[idx] * 1.005:
            touches.append((idx, low[idx]))
    # 必須有 ≥ 2 次觸碰，且間隔 ≥ 3 天
    if len(touches) < 2: return False
    # 取最近兩次（時間順序：touches[0] 是最新）
    second_touch_idx, second_low = touches[0]
    first_touch_idx, first_low = None, None
    for tch in touches[1:]:
        if second_touch_idx - tch[0] >= 3:
            first_touch_idx, first_low = tch
            break
    if first_touch_idx is None: return False
    # 第二次低點高於第一次（higher low）
    if second_low <= first_low: return False
    # 當前 close 已突破中軌
    if np.isnan(sma[t]) or close[t] <= sma[t]: return False
    return True


# ── 10. M Top（雙觸上軌反轉）──
def is_m_top(high, close, bbu, sma, t, lookback=20):
    """近 N 天兩次觸 BBU，第二次高點 < 第一次高點，且 close < 中軌"""
    if t < lookback: return False
    touches = []
    for i in range(lookback):
        idx = t - i
        if idx < 0: break
        if not np.isnan(bbu[idx]) and high[idx] >= bbu[idx] * 0.995:
            touches.append((idx, high[idx]))
    if len(touches) < 2: return False
    second_touch_idx, second_high = touches[0]
    first_touch_idx, first_high = None, None
    for tch in touches[1:]:
        if second_touch_idx - tch[0] >= 3:
            first_touch_idx, first_high = tch
            break
    if first_touch_idx is None: return False
    if second_high >= first_high: return False
    if np.isnan(sma[t]) or close[t] >= sma[t]: return False
    return True


# ── 主 detect 函數：對單一 t 偵測所有 BB 訊號 ──
def detect_all_bb_signals(close, high, low, sma, bbu, bbl, bandwidth, pct_b, t):
    """對 index t 偵測所有 BB 訊號，回傳 dict"""
    return {
        'squeeze':                is_squeeze(bandwidth, t),
        'squeeze_percentile':     squeeze_percentile(bandwidth, t),
        'expansion':              is_expansion(bandwidth, t),
        'walking_up':             is_walking_up(close, sma, bbu, t),
        'walking_down':           is_walking_down(close, sma, bbl, t),
        'pct_b_high':             pct_b_extreme_high(pct_b, t),
        'pct_b_low':              pct_b_extreme_low(pct_b, t),
        'mean_rev_high':          mean_reversion_high(close, sma, bbu, pct_b, t),
        'mean_rev_low':           mean_reversion_low(close, sma, bbl, pct_b, t),
        'w_bottom':               is_w_bottom(low, close, bbl, sma, t),
        'm_top':                  is_m_top(high, close, bbu, sma, t),
        # 數值欄位
        'pct_b':                  float(pct_b[t]) if not np.isnan(pct_b[t]) else None,
        'bandwidth':              float(bandwidth[t]) if not np.isnan(bandwidth[t]) else None,
    }
