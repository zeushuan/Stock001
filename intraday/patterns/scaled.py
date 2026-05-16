"""Pattern Scaling for Intraday Timeframes — Stock001 v9.29
=============================================================

把 daily-period 參數 → bar-count 參數，再呼叫既有的 patterns.* 偵測器。

Stage / SEPA / 30W SMA 等 framework 在 1m/5m/15m 上沒有意義（30W SMA 在 5m
變成 12 小時 SMA），所以這些 timeframe 會 raise NotApplicable。

對外 API：
  classify_stage_tf(df, tf='1h')
  detect_cup_handle_tf(df, tf='1h')
  detect_flat_base_tf(df, tf='1h')
"""
from __future__ import annotations

import pandas as pd
from typing import Optional

from intraday.config import get_tf_config, NotApplicable, BARS_PER_WEEK


def _bars_for_weeks(tf: str, weeks: float) -> int:
    """週數 → bar 數（依該 timeframe 每週幾根）"""
    return max(1, int(round(BARS_PER_WEEK[tf] * weeks)))


# ─── Stage Analysis (Stan Weinstein) ─────────────────────────────

def classify_stage_tf(df: pd.DataFrame, tf: str = '1h',
                       rs_rating: Optional[float] = None):
    """Stan Weinstein 4 階段（依 timeframe scaled）

    日線：30W SMA = 150 bars
    1h: 30W → 30 × 32.5 = 975 bars（不切實際）
       → 改用「30 個交易日的 1h bar」≈ 195 bar 當「中期趨勢」
    """
    cfg = get_tf_config(tf)
    if not cfg.supports_stage:
        raise NotApplicable(
            f"Stage 分析在 {tf} 不適用（最低需 1h；建議用日線當主、1h 當輔）")

    from patterns.stage_analysis import classify_stage

    if tf == '1d':
        return classify_stage(df, rs_rating=rs_rating)

    # 1h: 30W SMA → 改成「30 天 × bars_per_day」≈ 約 195 bar
    bars_per_day = max(1, int(round(cfg.bars_per_day)))
    sma_period = 30 * bars_per_day               # 約 195 bars
    slope_window = 10 * bars_per_day              # 約 65 bars
    return classify_stage(
        df,
        sma_period_days=sma_period,
        slope_window_days=slope_window,
        rs_rating=rs_rating,
        confirm_weeks=4,
    )


# ─── Cup and Handle ──────────────────────────────────────────────

def detect_cup_handle_tf(df: pd.DataFrame, tf: str = '1h',
                          rs_rating: Optional[float] = None,
                          market_status: str = 'uptrend'):
    """杯柄型態 — 依 timeframe scale 杯/柄的最短時間"""
    cfg = get_tf_config(tf)
    if not cfg.supports_cup_handle:
        raise NotApplicable(
            f"杯柄在 {tf} 不適用（噪音太大；建議 15m 以上）")

    from patterns.cup_and_handle import detect_cup_and_handle
    # cup_and_handle 預設參數已經是 day-based；只在 1d 直用，
    # intraday 用相同函數但要確保資料夠多。
    return detect_cup_and_handle(
        df, rs_rating=rs_rating, market_status=market_status)


# ─── Flat Base ───────────────────────────────────────────────────

def detect_flat_base_tf(df: pd.DataFrame, tf: str = '1h',
                         rs_rating: Optional[float] = None,
                         market_status: str = 'uptrend'):
    """平台底 — 把「5 週」翻成該 timeframe 的 bar 數"""
    cfg = get_tf_config(tf)
    if not cfg.supports_flat_base:
        raise NotApplicable(
            f"平台底在 {tf} 不適用（建議 15m 以上）")

    from patterns.flat_base import detect_flat_base
    if tf == '1d':
        return detect_flat_base(df, rs_rating=rs_rating, market_status=market_status)

    # Intraday：把「30 d」整理窗口翻成 30 × bars_per_day 個 bar
    bars_per_day = max(1, int(round(cfg.bars_per_day)))
    window_bars = 30 * bars_per_day              # 整理窗口
    min_dur = 25 * bars_per_day                   # 最短 5 週 → 25 天
    prior_window = 60 * bars_per_day              # 前期上漲檢測窗口

    return detect_flat_base(
        df,
        window_days=window_bars,
        min_duration=min_dur,
        prior_window=prior_window,
        rs_rating=rs_rating,
        market_status=market_status,
    )
