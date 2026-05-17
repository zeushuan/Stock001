"""Pattern Scaling for Intraday Timeframes — Stock001 v9.32
=============================================================

設計原則（v9.32 修正）：**所有 period 用「bar 數」表達，跨 TF 統一**

例：「30W SMA」= 30 × 5 = 150 個 bar
  - 日線 (1d)：150 bars = 150 個交易日（原本意義）
  - 1h     ：150 bars = 150 個 1h bar ≈ 23 個交易日
  - 5m     ：150 bars = 150 個 5m bar ≈ 12.5 小時
  - 1m     ：150 bars = 150 個 1m bar ≈ 2.5 小時

關鍵概念：**N 個 bar 的意義在每個 TF 都「保留它的相對位置」**。
30W SMA 在日線是「中期趨勢」(150 bars)；在 5m 也是「中期趨勢」(150 5m bars)。
時間長度不同，但統計意義「中期」相同。

對外 API：
  classify_stage_tf(df, tf='1h')
  detect_cup_handle_tf(df, tf='1h')
  detect_flat_base_tf(df, tf='1h')
"""
from __future__ import annotations

import pandas as pd
from typing import Optional


# v9.32：bar 數參數 — 跨 TF 統一（原本 daily 概念直接 copy 用 bar 數）
SMA_30W_BARS = 150         # 30W × 5d = 150 bars
SMA_10W_BARS = 50          # 10W × 5d = 50 bars
HIGH_52W_BARS = 260        # 52W × 5d = 260 bars
SMA_150_BARS = 150         # SEPA 用
SMA_200_BARS = 200         # SEPA 用
FLAT_WINDOW_BARS = 30      # 整理窗口（原 30d）
FLAT_MIN_DUR_BARS = 25     # 最短整理（原 25d = 5 週）
FLAT_PRIOR_WINDOW_BARS = 60  # 前期上漲（原 60d）


# ─── Stage Analysis (Stan Weinstein) ─────────────────────────────

def classify_stage_tf(df: pd.DataFrame, tf: str = '1h',
                       rs_rating: Optional[float] = None):
    """Stan Weinstein 4 階段 — 跨 TF 統一用 150 bar SMA

    意義：150 個該 TF 的 bar 當「中期趨勢」基準。
      - 1d: 150 bars = 150 個交易日（標準 30W SMA）
      - 1h: 150 bars ≈ 23 個交易日（中期趨勢，比 30W 短但比短線長）
      - 5m: 150 bars = 12.5 小時（intraday 中期）
    """
    from patterns.stage_analysis import classify_stage
    return classify_stage(
        df,
        sma_period_days=SMA_30W_BARS,    # 150
        slope_window_days=SMA_10W_BARS,  # 50
        rs_rating=rs_rating,
        confirm_weeks=4,
    )


# ─── Cup and Handle ──────────────────────────────────────────────

def detect_cup_handle_tf(df: pd.DataFrame, tf: str = '1h',
                          rs_rating: Optional[float] = None,
                          market_status: str = 'uptrend'):
    """杯柄型態 — 預設參數直接用（detector 本身 N bar 概念）"""
    from patterns.cup_and_handle import detect_cup_and_handle
    return detect_cup_and_handle(
        df, rs_rating=rs_rating, market_status=market_status)


# ─── Flat Base ───────────────────────────────────────────────────

def detect_flat_base_tf(df: pd.DataFrame, tf: str = '1h',
                         rs_rating: Optional[float] = None,
                         market_status: str = 'uptrend'):
    """平台底 — 統一用 30 bar 整理窗口、60 bar 前期上漲檢測"""
    from patterns.flat_base import detect_flat_base
    return detect_flat_base(
        df,
        window_days=FLAT_WINDOW_BARS,       # 30 bars
        min_duration=FLAT_MIN_DUR_BARS,     # 25 bars
        prior_window=FLAT_PRIOR_WINDOW_BARS, # 60 bars
        rs_rating=rs_rating,
        market_status=market_status,
    )
