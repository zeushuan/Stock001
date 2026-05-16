"""Stan Weinstein 階段分析（Stage Analysis）— Stock001 patterns 模組

依 stan_weinstein_stage_analysis_spec v1.0：
  - 30 週 SMA（= 150 日 SMA）斜率為主要判別
  - 四階段：1 Basing / 2 Advancing / 3 Top / 4 Declining
  - 提供 Stage Filter（一票否決）和 Stage Score（0-25）兩種整合方案

對外 API：
    classify_stage(df) -> StageResult
    apply_stage_filter(t3_score, stage) -> float    （方案 A）
    stage_score(stage_result) -> float (0-25)       （方案 B）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List
import numpy as np
import pandas as pd


@dataclass
class StageResult:
    stage: int                            # 1-4
    stage_name: str                       # "Basing" / "Advancing" / "Top" / "Declining"
    price_vs_sma30w: float                # (close - sma30w) / sma30w
    sma30w_slope: float                   # 10 週變化率（正規化）
    rs_rating: Optional[float] = None
    confidence: float = 0.0               # 0-1 對 stage 判斷的自信度
    sub_stage: str = ""                   # "early" / "mid" / "late"
    transition_signals: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


STAGE_NAMES = {1: "Basing", 2: "Advancing", 3: "Top", 4: "Declining"}


def classify_stage(
    df: pd.DataFrame,
    slope_threshold: float = 0.02,
    sma_period_days: int = 150,            # 30 週 ≈ 150 trading days
    slope_window_days: int = 50,           # 10 週 ≈ 50 trading days
    rs_rating: Optional[float] = None,
    confirm_weeks: int = 4,                # 連續 N 週確認以避免跳動
) -> StageResult:
    """Stan Weinstein 四階段分類

    Args:
        df: OHLCV DataFrame with DatetimeIndex
        slope_threshold: 判別均線上揚/走平/下彎的斜率門檻（預設 ±2%）
        sma_period_days: 30 週 SMA 換算的交易日（150）
        slope_window_days: 算斜率回看天數（50 ≈ 10 週）
        rs_rating: 0-100 RS Rating（可選）
        confirm_weeks: 連續 N 週確認（簡化版用 N×5 天）
    """
    if df is None or len(df) < sma_period_days + slope_window_days + 5:
        return StageResult(
            stage=0, stage_name="Unknown",
            price_vs_sma30w=0.0, sma30w_slope=0.0,
            notes=[f"資料不足（需 {sma_period_days + slope_window_days + 5}+ bars）"],
        )

    close = df["Close"]
    sma30w = close.rolling(sma_period_days).mean()

    cur_close = float(close.iloc[-1])
    cur_sma = float(sma30w.iloc[-1])
    sma_back = float(sma30w.iloc[-slope_window_days])

    if cur_sma <= 0 or sma_back <= 0 or np.isnan(cur_sma) or np.isnan(sma_back):
        return StageResult(
            stage=0, stage_name="Unknown",
            price_vs_sma30w=0.0, sma30w_slope=0.0,
            notes=["SMA 計算失敗"],
        )

    slope = (cur_sma - sma_back) / sma_back
    price_dev = (cur_close - cur_sma) / cur_sma
    price_above = cur_close > cur_sma

    # ─── 階段判別 ──
    if slope > slope_threshold:
        stage = 2 if price_above else 1
    elif slope < -slope_threshold:
        stage = 4 if not price_above else 3
    else:
        # 均線走平：價貼近 SMA = basing；明顯偏離 = top（上）/ 仍 basing（下）
        if abs(price_dev) < 0.05:
            stage = 1
        elif price_above:
            stage = 3
        else:
            stage = 1   # 跌至 SMA 之下但 slope 未下彎，仍視為 basing

    # ─── 連續確認（confirm_weeks）──
    confirm_days = confirm_weeks * 5
    if len(df) >= confirm_days:
        recent_closes = close.iloc[-confirm_days:].values
        recent_smas = sma30w.iloc[-confirm_days:].values
        above_ratio = np.nanmean(recent_closes > recent_smas)
        # 更嚴格的閾值（避免 basing 震盪期被誤判為 stage 4）
        if stage in (2, 3) and above_ratio < 0.3:
            stage = 4 if stage == 3 else 1
        elif stage in (1, 4) and above_ratio > 0.7:
            stage = 2 if stage == 1 else 3
    else:
        above_ratio = 1.0 if price_above else 0.0

    # ─── Sub-stage（early / mid / late）──
    sub_stage = "mid"
    # 計算 stage 已持續多久（簡化：找最近 stage 改變的點）
    try:
        # 過去 252 日掃描 stage 變化點
        n_check = min(252, len(df))
        stage_history = []
        for i in range(max(sma_period_days + slope_window_days, len(df) - n_check),
                        len(df)):
            sma_i = sma30w.iloc[i]
            sma_i_back = sma30w.iloc[max(0, i - slope_window_days)]
            if sma_i_back <= 0 or np.isnan(sma_i) or np.isnan(sma_i_back):
                continue
            slope_i = (sma_i - sma_i_back) / sma_i_back
            c_i = close.iloc[i]
            above_i = c_i > sma_i
            if slope_i > slope_threshold:
                s_i = 2 if above_i else 1
            elif slope_i < -slope_threshold:
                s_i = 4 if not above_i else 3
            else:
                s_i = 3 if above_i else 1
            stage_history.append(s_i)
        # 算 stage 連續天數
        cnt = 0
        for s in reversed(stage_history):
            if s == stage:
                cnt += 1
            else:
                break
        if cnt < 30:
            sub_stage = "early"
        elif cnt < 90:
            sub_stage = "mid"
        else:
            sub_stage = "late"
    except Exception:
        sub_stage = "mid"

    # ─── 轉換訊號 ──
    transition_signals = []
    # Stage 1→2 突破：價突破 + 量爆
    if stage == 2 and sub_stage == "early":
        try:
            window_high = float(df["High"].iloc[-60:-5].max())
            if cur_close > window_high * 1.001:
                ma50_vol = float(df["Volume"].iloc[-50:].mean())
                cur_vol = float(df["Volume"].iloc[-1])
                if cur_vol >= ma50_vol * 1.5:
                    transition_signals.append("Stage 1→2 突破（量爆）")
                else:
                    transition_signals.append("Stage 1→2 突破（量未跟）")
        except Exception:
            pass

    # Stage 2→3 警示：slope 趨近 0
    if stage == 2 and abs(slope) < slope_threshold * 1.5 and slope < slope_threshold:
        transition_signals.append("Stage 2→3 警示（斜率轉緩）")

    # Stage 3→4 確認：跌破 sma
    if stage in (3, 4) and not price_above:
        if stage == 3:
            transition_signals.append("Stage 3→4 風險（價跌破 SMA30w）")

    # ─── Confidence ──
    # 斜率越強、price_dev 越大 → 自信度越高
    conf = min(1.0, abs(slope) / (slope_threshold * 3))
    if stage == 2:
        conf = min(1.0, conf * (1 + max(0, price_dev) * 2))
    elif stage == 4:
        conf = min(1.0, conf * (1 + max(0, -price_dev) * 2))

    return StageResult(
        stage=stage,
        stage_name=STAGE_NAMES.get(stage, "Unknown"),
        price_vs_sma30w=round(price_dev, 4),
        sma30w_slope=round(slope, 4),
        rs_rating=rs_rating,
        confidence=round(conf, 3),
        sub_stage=sub_stage,
        transition_signals=transition_signals,
        notes=[],
    )


# ─────────────────────────────────────────────────────────────
# Integration with T3 Score (spec §4)
# ─────────────────────────────────────────────────────────────

def apply_stage_filter(t3_score: float, stage: int) -> float:
    """方案 A：Stage 一票否決

    Stage 2 完整保留，Stage 1 半倉，Stage 3 大幅降，Stage 4 歸零
    """
    multipliers = {1: 0.5, 2: 1.0, 3: 0.2, 4: 0.0}
    return float(t3_score) * multipliers.get(stage, 0.0)


def stage_score(result: StageResult) -> float:
    """方案 B：Stage 子分數 0-25 加入 T3 總分

    Stage 2 early（突破後 4-12 週）= 25 滿分
    Stage 2 mid = 22
    Stage 2 late = 15
    Stage 1 late (即將突破) = 18
    Stage 1 early/mid = 10
    Stage 3 = 5
    Stage 4 = 0
    """
    if result.stage == 2:
        if result.sub_stage == "early":
            return 25.0
        elif result.sub_stage == "mid":
            return 22.0
        else:
            return 15.0
    elif result.stage == 1:
        if result.sub_stage == "late":
            return 18.0
        else:
            return 10.0
    elif result.stage == 3:
        return 5.0
    elif result.stage == 4:
        return 0.0
    return 0.0
