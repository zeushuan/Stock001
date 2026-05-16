"""杯柄型態（Cup and Handle）— Stock001 patterns 模組

依 cup_and_handle_spec v1.0：
  - O'Neil + IBD CANSLIM 框架
  - 11 個子項加總 0-100 + market & base-stage multiplier
  - Hard filter 直接 return 0

對外 API：
    detect_cup_and_handle(df, market_status, rs_rating, config) -> CupAndHandleResult
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, List
import numpy as np
import pandas as pd


# ───────────────────────────────────────────────────────────────
# 設定常數（spec §4）
# ───────────────────────────────────────────────────────────────

WEIGHTS = {
    "cup_shape":       15,
    "cup_depth":       10,
    "cup_symmetry":     8,
    "handle_depth":    10,
    "handle_slope":    10,
    "handle_position":  7,
    "volume_breakout": 15,
    "volume_dryup":     8,
    "prior_uptrend":    7,
    "base_stage":       5,
    "rs_rating":        5,
}
TOTAL_WEIGHT = sum(WEIGHTS.values())   # 100

MARKET_MULT = {
    "uptrend":    1.0,
    "pressure":   0.7,
    "correction": 0.3,
}

BASE_MULT = {1: 1.0, 2: 0.8, 3: 0.5}   # late stage → 0.2


@dataclass
class CupAndHandleResult:
    """杯柄偵測結果"""
    detected: bool
    score: float                            # 0-100
    cup_start_idx: Optional[int] = None
    cup_bottom_idx: Optional[int] = None
    cup_end_idx: Optional[int] = None
    handle_start_idx: Optional[int] = None
    handle_end_idx: Optional[int] = None
    pivot_price: Optional[float] = None
    stop_loss: Optional[float] = None
    target_price: Optional[float] = None
    breakdown: Dict[str, float] = field(default_factory=dict)
    pattern_variant: str = "classic"
    reasons: List[str] = field(default_factory=list)


# ───────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────

def _find_local_extreme(arr: np.ndarray, window: int = 5,
                          find_min: bool = False) -> List[int]:
    """簡單局部極值（前後 window 內最低 / 最高）"""
    out = []
    n = len(arr)
    for i in range(window, n - window):
        if np.isnan(arr[i]): continue
        win_l = arr[i-window:i]
        win_r = arr[i+1:i+window+1]
        if find_min:
            if arr[i] <= np.nanmin(win_l) and arr[i] <= np.nanmin(win_r):
                out.append(i)
        else:
            if arr[i] >= np.nanmax(win_l) and arr[i] >= np.nanmax(win_r):
                out.append(i)
    return out


def _u_shape_fit(prices: np.ndarray) -> float:
    """二次多項式擬合 R²，越接近 1 越像 U"""
    if len(prices) < 5: return 0.0
    x = np.arange(len(prices))
    try:
        coef = np.polyfit(x, prices, 2)
        if coef[0] <= 0:
            return 0.0   # 開口朝下 = ∩ 形不是 U
        fitted = np.polyval(coef, x)
        ss_res = np.sum((prices - fitted) ** 2)
        ss_tot = np.sum((prices - np.mean(prices)) ** 2)
        if ss_tot == 0: return 0.0
        r2 = 1 - ss_res / ss_tot
        return max(0.0, float(r2))
    except Exception:
        return 0.0


def _linear_slope(arr: np.ndarray) -> float:
    """1d 線性回歸斜率 / 平均值（正規化）"""
    if len(arr) < 2: return 0.0
    x = np.arange(len(arr))
    try:
        m, _ = np.polyfit(x, arr, 1)
        mean = float(np.nanmean(arr))
        return float(m / mean) if mean > 0 else 0.0
    except Exception:
        return 0.0


# ───────────────────────────────────────────────────────────────
# 主入口
# ───────────────────────────────────────────────────────────────

def detect_cup_and_handle(
    df: pd.DataFrame,
    market_status: str = "uptrend",
    rs_rating: Optional[float] = None,
    config: Optional[dict] = None,
) -> CupAndHandleResult:
    """杯柄型態偵測

    Args:
        df: DataFrame with columns Open/High/Low/Close/Volume, DatetimeIndex
        market_status: 'uptrend' / 'pressure' / 'correction'
        rs_rating: 0-100，可選
        config: 覆寫預設參數

    Returns:
        CupAndHandleResult
    """
    cfg = dict({
        "min_bars": 120,
        "min_cup_bars": 35,         # 7 週
        "max_cup_bars": 325,        # 65 週
        "min_cup_depth": 0.12,
        "max_cup_depth": 0.50,
        "max_cup_depth_filter": 0.50,
        "max_symmetry_diff": 0.05,
        "min_u_r2": 0.50,           # hard filter
        "good_u_r2": 0.70,
        "best_u_r2": 0.85,
        "min_handle_bars": 5,
        "max_handle_bars": 25,
        "max_handle_pct_of_cup": 0.33,
        "min_prior_uptrend_pct": 0.30,
        "prior_window": 60,
        "volume_breakout_mult": 1.4,
        "volume_dryup_ratio": 0.7,
        "pivot_buffer_pct": 0.001,
    })
    if config: cfg.update(config)

    if df is None or len(df) < cfg["min_bars"]:
        return CupAndHandleResult(
            detected=False, score=0.0,
            reasons=[f"資料不足（< {cfg['min_bars']} bars）"]
        )

    high = df["High"].values
    low = df["Low"].values
    close = df["Close"].values
    vol = df["Volume"].values
    n = len(df)

    # ─── Step 1: 找潛在杯部 — 從尾巴往前找 ───
    # 最近的 maxima 當杯右緣，再往前找配對左緣
    maxima_idx = _find_local_extreme(high, window=5, find_min=False)
    if len(maxima_idx) < 2:
        return CupAndHandleResult(
            detected=False, score=0.0,
            reasons=["局部高點不足，無法形成杯部"]
        )

    best: Optional[dict] = None

    # 從新到舊掃 B 點（杯右緣）
    for b_idx in reversed(maxima_idx):
        if b_idx > n - cfg["min_handle_bars"] - 2:
            continue   # 右緣到尾巴太短，沒空間長 handle
        b_price = high[b_idx]

        # 配對 A 點（左緣，更早的 maxima）
        for a_idx in reversed(maxima_idx):
            if a_idx >= b_idx: continue
            cup_bars = b_idx - a_idx
            if cup_bars < cfg["min_cup_bars"]: continue
            if cup_bars > cfg["max_cup_bars"]: break
            a_price = high[a_idx]
            # 對稱性
            if min(a_price, b_price) <= 0: continue
            symm_diff = abs(a_price - b_price) / min(a_price, b_price)
            if symm_diff > cfg["max_symmetry_diff"]: continue
            # 杯深
            c_idx = a_idx + int(np.argmin(low[a_idx:b_idx+1]))
            c_price = low[c_idx]
            cup_depth = (max(a_price, b_price) - c_price) / max(a_price, b_price)
            if not (cfg["min_cup_depth"] <= cup_depth <= cfg["max_cup_depth"]):
                continue
            # U 型擬合
            cup_slice = close[a_idx:b_idx+1]
            r2 = _u_shape_fit(cup_slice)
            if r2 < cfg["min_u_r2"]: continue

            best = {
                "a_idx": a_idx, "b_idx": b_idx, "c_idx": c_idx,
                "a_price": a_price, "b_price": b_price, "c_price": c_price,
                "cup_bars": cup_bars,
                "cup_depth": cup_depth,
                "symm_diff": symm_diff,
                "u_r2": r2,
            }
            break
        if best:
            break

    if not best:
        return CupAndHandleResult(
            detected=False, score=0.0,
            reasons=["未找到合格杯部"]
        )

    # ─── Step 2: 杯右緣後找 handle ───
    a, b, c = best["a_idx"], best["b_idx"], best["c_idx"]
    handle_window = high[b+1:min(n, b+1+cfg["max_handle_bars"])]
    handle_low_window = low[b+1:min(n, b+1+cfg["max_handle_bars"])]

    if len(handle_window) < cfg["min_handle_bars"]:
        # 沒有 handle 的話可能是 "Cup without Handle" 變體
        variant = "no_handle"
        h_start = h_end = h_low_idx = None
        handle_drawdown = 0.0
        handle_slope = 0.0
        handle_above_sma50 = True
    else:
        variant = "classic"
        h_start = b + 1
        h_end = h_start + len(handle_window) - 1
        h_low_idx = h_start + int(np.argmin(handle_low_window))
        h_low_price = low[h_low_idx]
        handle_drawdown = (best["b_price"] - h_low_price) / best["b_price"]
        # 柄部斜率 — 取 handle 範圍 close
        h_close = close[h_start:h_end+1]
        handle_slope = _linear_slope(h_close)
        # 柄部位置：是否在杯部上半（low > (a+c)/2）
        handle_above_sma50 = h_low_price > (best["a_price"] + best["c_price"]) / 2

    # Hard filters per spec §4.2
    if best["cup_depth"] > 0.50:
        return CupAndHandleResult(detected=False, score=0.0,
                                    reasons=["杯深 > 50%"])
    if variant == "classic":
        if handle_drawdown > best["cup_depth"] * 0.5:
            return CupAndHandleResult(detected=False, score=0.0,
                                        reasons=["柄部回檔 > 杯深 × 0.5"])
        if handle_slope > 0.05:
            return CupAndHandleResult(detected=False, score=0.0,
                                        reasons=["柄部斜率上揚 > 5°"])

    # ─── Step 3: 量能驗證 ───
    cup_left_slice = vol[a:c+1]
    cup_left_vol_slope = _linear_slope(cup_left_slice) if len(cup_left_slice) >= 3 else 0
    cup_bottom_avg_vol = float(np.nanmean(vol[max(0, c-5):min(n, c+6)]))
    ma50_vol = float(np.nanmean(vol[max(0, n-50):n])) if n >= 50 else float(np.nanmean(vol))
    cup_bottom_dryup = cup_bottom_avg_vol < ma50_vol * cfg["volume_dryup_ratio"]

    handle_dryup = True
    if variant == "classic" and h_start is not None:
        handle_avg_vol = float(np.nanmean(vol[h_start:h_end+1]))
        handle_dryup = handle_avg_vol < ma50_vol * cfg["volume_dryup_ratio"]

    # 突破檢查：當前收盤是否 > pivot
    pivot_price = (low[h_low_idx] if variant == "classic" and h_low_idx is not None
                    else best["b_price"]) * (1 + cfg["pivot_buffer_pct"])
    if variant == "classic":
        pivot_price = best["b_price"] * (1 + cfg["pivot_buffer_pct"])
    current_close = float(close[-1])
    current_vol = float(vol[-1])
    is_breakout = (current_close > pivot_price
                    and current_vol >= ma50_vol * cfg["volume_breakout_mult"])

    # ─── Step 4: 前期漲幅 ───
    prior_start = max(0, a - cfg["prior_window"])
    prior_close = close[prior_start]
    if prior_close > 0:
        prior_uptrend = (best["a_price"] - prior_close) / prior_close
    else:
        prior_uptrend = 0.0

    # ─── Step 5: 11 個子項計分 ───
    bd: Dict[str, float] = {}

    # 1. cup_shape（U 型 R²）
    if best["u_r2"] >= cfg["best_u_r2"]:
        bd["cup_shape"] = WEIGHTS["cup_shape"]
    elif best["u_r2"] >= cfg["good_u_r2"]:
        bd["cup_shape"] = WEIGHTS["cup_shape"] * 0.7
    else:
        bd["cup_shape"] = WEIGHTS["cup_shape"] * 0.4

    # 2. cup_depth
    d = best["cup_depth"]
    if 0.15 <= d <= 0.25:
        bd["cup_depth"] = WEIGHTS["cup_depth"]
    elif 0.12 <= d <= 0.33:
        bd["cup_depth"] = WEIGHTS["cup_depth"] * 0.7
    else:
        bd["cup_depth"] = WEIGHTS["cup_depth"] * 0.4

    # 3. cup_symmetry
    if best["symm_diff"] < 0.02:
        bd["cup_symmetry"] = WEIGHTS["cup_symmetry"]
    else:
        bd["cup_symmetry"] = WEIGHTS["cup_symmetry"] * 0.6

    # 4. handle_depth
    if variant == "classic":
        if 0.08 <= handle_drawdown <= 0.15:
            bd["handle_depth"] = WEIGHTS["handle_depth"]
        elif handle_drawdown <= best["cup_depth"] / 3:
            bd["handle_depth"] = WEIGHTS["handle_depth"] * 0.7
        else:
            bd["handle_depth"] = WEIGHTS["handle_depth"] * 0.3
    else:
        bd["handle_depth"] = WEIGHTS["handle_depth"] * 0.5   # no handle

    # 5. handle_slope
    if variant == "classic" and handle_slope < 0:
        bd["handle_slope"] = WEIGHTS["handle_slope"]
    elif variant == "no_handle":
        bd["handle_slope"] = WEIGHTS["handle_slope"] * 0.5
    else:
        bd["handle_slope"] = 0

    # 6. handle_position
    if variant == "classic":
        bd["handle_position"] = WEIGHTS["handle_position"] if handle_above_sma50 else 0
    else:
        bd["handle_position"] = WEIGHTS["handle_position"] * 0.5

    # 7. volume_breakout
    if is_breakout:
        bd["volume_breakout"] = WEIGHTS["volume_breakout"]
    elif current_close > pivot_price * 0.97:
        bd["volume_breakout"] = WEIGHTS["volume_breakout"] * 0.4   # 接近 pivot
    else:
        bd["volume_breakout"] = 0

    # 8. volume_dryup
    bd["volume_dryup"] = (WEIGHTS["volume_dryup"]
                          if (cup_bottom_dryup and handle_dryup) else
                          (WEIGHTS["volume_dryup"] * 0.5 if cup_bottom_dryup else 0))

    # 9. prior_uptrend
    if prior_uptrend >= cfg["min_prior_uptrend_pct"]:
        bd["prior_uptrend"] = WEIGHTS["prior_uptrend"]
    elif prior_uptrend >= 0.10:
        bd["prior_uptrend"] = WEIGHTS["prior_uptrend"] * 0.5
    else:
        bd["prior_uptrend"] = 0

    # 10. base_stage（簡化：算 base 數 — 未來可接 Stage Analysis）
    base_stage = 1   # 先預設 stage 1，待 stage_analysis 整合
    bd["base_stage"] = WEIGHTS["base_stage"]

    # 11. rs_rating
    if rs_rating is not None:
        if rs_rating >= 80:
            bd["rs_rating"] = WEIGHTS["rs_rating"]
        elif rs_rating >= 70:
            bd["rs_rating"] = WEIGHTS["rs_rating"] * 0.6
        else:
            bd["rs_rating"] = 0
    else:
        bd["rs_rating"] = WEIGHTS["rs_rating"] * 0.5   # 未提供

    raw_score = sum(bd.values())

    # 套 multiplier
    market_mult = MARKET_MULT.get(market_status, 1.0)
    base_mult = BASE_MULT.get(base_stage, 0.2)
    final_score = raw_score * market_mult * base_mult

    # Target = pivot + cup depth equivalent extension
    height = best["a_price"] - best["c_price"]
    target = pivot_price + height
    stop = (low[h_low_idx] if variant == "classic" and h_low_idx is not None
            else best["c_price"]) * 0.97

    reasons = []
    if variant != "classic":
        reasons.append(f"變體：{variant}")
    if is_breakout:
        reasons.append("突破完成（量爆）")
    elif current_close > pivot_price:
        reasons.append("突破但量未跟")

    return CupAndHandleResult(
        detected=True,
        score=round(final_score, 1),
        cup_start_idx=a, cup_bottom_idx=c, cup_end_idx=b,
        handle_start_idx=h_start, handle_end_idx=h_end,
        pivot_price=round(pivot_price, 4),
        stop_loss=round(stop, 4),
        target_price=round(target, 4),
        breakdown={k: round(v, 2) for k, v in bd.items()},
        pattern_variant=variant,
        reasons=reasons,
    )
