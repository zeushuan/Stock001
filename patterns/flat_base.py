"""平台底型態（Flat Base）— Stock001 patterns 模組

依 flat_base_pattern_spec v1.0：
  - O'Neil CAN SLIM + Minervini SEPA
  - 0-100 信心分數
  - 整理深度 ≤ 15%、時間 ≥ 5 週

對外 API：
    detect_flat_base(df, ...) -> FlatBaseResult
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List
import numpy as np
import pandas as pd


@dataclass
class FlatBaseResult:
    detected: bool
    score: float = 0.0
    pivot_point: Optional[float] = None
    base_start_idx: Optional[int] = None
    base_end_idx: Optional[int] = None
    base_depth: float = 0.0
    base_duration_days: int = 0
    base_count: int = 1
    breakout: bool = False
    breakout_volume_ratio: Optional[float] = None
    stop_loss: Optional[float] = None
    target_price: Optional[float] = None
    notes: List[str] = field(default_factory=list)
    breakdown: dict = field(default_factory=dict)


def _detect_base_count(df: pd.DataFrame, lookback_days: int = 504) -> int:
    """簡易 base count：算過去 N 日內 sma50 → sma200 突破次數"""
    if df is None or len(df) < 200: return 1
    n = len(df)
    start = max(0, n - lookback_days)
    sub = df.iloc[start:]
    try:
        sma50 = sub["Close"].rolling(50).mean()
        sma200 = sub["Close"].rolling(200).mean()
    except Exception:
        return 1
    # 算 sma50 上穿 sma200 次數
    crosses = ((sma50.shift(1) <= sma200.shift(1)) & (sma50 > sma200)).sum()
    return max(1, int(crosses))


def _tight_closes(window_df: pd.DataFrame, weeks: int = 3,
                   max_diff_pct: float = 0.015) -> bool:
    """3-Weeks-Tight：連續 N 週週收盤價差 ≤ max_diff_pct"""
    try:
        weekly = window_df["Close"].resample("W").last().dropna()
        if len(weekly) < weeks: return False
        last_n = weekly.iloc[-weeks:]
        if last_n.min() <= 0: return False
        return float(last_n.max() / last_n.min() - 1) <= max_diff_pct
    except Exception:
        return False


def detect_flat_base(
    df: pd.DataFrame,
    window_days: int = 30,
    max_depth: float = 0.15,
    min_duration: int = 25,
    prior_gain_threshold: float = 0.20,
    prior_window: int = 60,
    rs_rating: Optional[float] = None,
    market_status: str = "uptrend",
) -> FlatBaseResult:
    """偵測平台底型態

    Args:
        df: OHLCV DataFrame with DatetimeIndex
        window_days: 整理窗口（預設 30d ≈ 6 週）
        max_depth: 最大整理深度（預設 15%）
        min_duration: 最短整理（預設 25d ≈ 5 週）
        prior_gain_threshold: 前期最小漲幅（預設 20%）
        prior_window: 前期上漲檢測窗口
        rs_rating: 0-100，可選
        market_status: 'uptrend' / 'pressure' / 'correction'

    Returns:
        FlatBaseResult
    """
    if df is None or len(df) < window_days + prior_window + 50:
        return FlatBaseResult(
            detected=False, score=0.0,
            notes=[f"資料不足（需 {window_days + prior_window + 50}+ bars）"]
        )

    notes: List[str] = []

    # ─── Step 1: 切窗口（base = 不含最後一根，最後一根當突破日測） ──
    window = df.iloc[-(window_days + 1):-1]
    if "High" not in window.columns:
        return FlatBaseResult(detected=False, score=0.0,
                              notes=["缺 High 欄位"])

    window_high = float(window["High"].max())
    window_low = float(window["Low"].min())
    if window_high <= 0:
        return FlatBaseResult(detected=False, score=0.0,
                              notes=["window_high 異常"])
    depth = (window_high - window_low) / window_high

    if depth > max_depth:
        return FlatBaseResult(
            detected=False, score=0.0,
            base_depth=depth, base_duration_days=window_days,
            notes=[f"深度 {depth:.1%} > 上限 {max_depth:.0%}"]
        )

    if window_days < min_duration:
        notes.append(f"窗口 {window_days}d < 最短 {min_duration}d")

    # ─── Step 2: 前期漲幅 ──
    prior_close = float(df["Close"].iloc[-(window_days + prior_window)])
    base_start_close = float(df["Close"].iloc[-window_days])
    if prior_close <= 0:
        prior_gain = 0.0
    else:
        prior_gain = (base_start_close / prior_close) - 1

    # ─── Step 3: 均線結構 ──
    try:
        sma50 = df["Close"].rolling(50).mean()
        sma150 = df["Close"].rolling(150).mean()
        sma200 = df["Close"].rolling(200).mean()
        # window 內 95% 收盤 > sma50
        above_sma50_pct = (window["Close"] > sma50.iloc[-window_days:]).mean()
        ma_aligned = (
            above_sma50_pct >= 0.95
            and sma50.iloc[-1] > sma150.iloc[-1] > sma200.iloc[-1]
        )
        # 200MA 斜率向上
        if len(sma200.dropna()) >= 21:
            sma200_slope_up = sma200.iloc[-1] > sma200.iloc[-21]
        else:
            sma200_slope_up = False
    except Exception:
        ma_aligned = False
        sma200_slope_up = False

    # ─── Step 4: 量能 dry-up ──
    try:
        prior_avg_vol = float(df["Volume"].iloc[-(window_days + 60):-window_days].mean())
        base_avg_vol = float(window["Volume"].mean())
        ma50_vol = float(df["Volume"].iloc[-50:].mean()) if len(df) >= 50 else base_avg_vol
        volume_dryup = base_avg_vol < prior_avg_vol * 0.85
        very_dry = base_avg_vol < ma50_vol * 0.7
    except Exception:
        volume_dryup = False
        very_dry = False
        ma50_vol = float(df["Volume"].mean())

    # ─── Step 5: Tight Closes ──
    tight_3w = _tight_closes(window, weeks=3, max_diff_pct=0.015)
    if tight_3w:
        notes.append("3-Weeks-Tight 高勝率訊號")

    # ─── Step 6: 突破偵測 ──
    pivot_point = window_high * 1.001
    latest_close = float(df["Close"].iloc[-1])
    latest_vol = float(df["Volume"].iloc[-1])
    breakout = (
        latest_close > pivot_point
        and latest_vol >= ma50_vol * 1.4
    )
    breakout_vol_ratio = (latest_vol / ma50_vol) if ma50_vol > 0 else None

    # ─── Step 7: Base count ──
    base_count = _detect_base_count(df)

    # ─── Step 8: 評分 (依 spec §5.2 權重) ──
    breakdown = {}

    # 1. 整理深度 (15)
    if depth <= 0.10:
        breakdown["depth"] = 15
    elif depth <= 0.12:
        breakdown["depth"] = 12
    elif depth <= 0.15:
        breakdown["depth"] = 8
    else:
        breakdown["depth"] = 0

    # 2. 整理時間 (10) — 5-7 週 = 25-35 d
    if 25 <= window_days <= 35:
        breakdown["duration"] = 10
    elif window_days >= 20:
        breakdown["duration"] = 6
    else:
        breakdown["duration"] = 3

    # 3. 前期漲勢 (15)
    if prior_gain >= 0.30:
        breakdown["prior_gain"] = 15
    elif prior_gain >= 0.20:
        breakdown["prior_gain"] = 10
    elif prior_gain >= 0.10:
        breakdown["prior_gain"] = 5
    else:
        breakdown["prior_gain"] = 0

    # 4. 均線排列 (15)
    if ma_aligned and sma200_slope_up:
        breakdown["ma_align"] = 15
    elif ma_aligned:
        breakdown["ma_align"] = 10
    else:
        breakdown["ma_align"] = 0

    # 5. 量縮 (10)
    if very_dry:
        breakdown["volume_dryup"] = 10
    elif volume_dryup:
        breakdown["volume_dryup"] = 7
    else:
        breakdown["volume_dryup"] = 0

    # 6. Tight closes (10)
    breakdown["tight_closes"] = 10 if tight_3w else 0

    # 7. Base count (10)
    breakdown["base_count"] = {1: 10, 2: 8, 3: 5, 4: 2}.get(base_count, 1)

    # 8. RS Rating (10)
    if rs_rating is not None:
        if rs_rating >= 90:
            breakdown["rs_rating"] = 10
        elif rs_rating >= 80:
            breakdown["rs_rating"] = 7
        elif rs_rating >= 70:
            breakdown["rs_rating"] = 4
        else:
            breakdown["rs_rating"] = 0
    else:
        breakdown["rs_rating"] = 5   # 未提供

    # 9. 突破確認 (5 + bonus 10)
    if breakout:
        breakdown["breakout"] = 5
        if breakout_vol_ratio and breakout_vol_ratio >= 1.5:
            breakdown["breakout_bonus"] = 10
        else:
            breakdown["breakout_bonus"] = 0
    else:
        breakdown["breakout"] = 0
        breakdown["breakout_bonus"] = 0

    raw_score = sum(breakdown.values())

    # Market multiplier
    market_mult = {"uptrend": 1.0, "pressure": 0.7, "correction": 0.3}.get(market_status, 1.0)
    final_score = min(raw_score * market_mult, 100.0)

    # Stop / Target
    stop_loss = window_low * 0.97   # 跌破 base 低點 3%
    target = pivot_point + (pivot_point - window_low) * 2   # 簡化目標

    n = len(df)
    return FlatBaseResult(
        detected=True,
        score=round(final_score, 1),
        pivot_point=round(pivot_point, 4),
        base_start_idx=n - window_days,
        base_end_idx=n - 1,
        base_depth=round(depth, 4),
        base_duration_days=window_days,
        base_count=base_count,
        breakout=breakout,
        breakout_volume_ratio=round(breakout_vol_ratio, 2) if breakout_vol_ratio else None,
        stop_loss=round(stop_loss, 4),
        target_price=round(target, 4),
        notes=notes,
        breakdown={k: round(v, 2) for k, v in breakdown.items()},
    )
