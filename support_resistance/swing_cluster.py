"""support_resistance.swing_cluster — Subsystem A
=====================================================

對應規格 §3.1：Swing-Cluster 偵測。

演算法：
  1. 左右各 swing_window 根取局部高（壓力候選）／低（支撐候選）點。
  2. 依價格排序，相鄰價差 ≤ median(ATR) × cluster_atr_mult 則併為同一區
     （一維 gap clustering，等價於 1D DBSCAN）。
  3. 區內以量能加權算中心；觸及數 < min_touches 的區捨棄。

公開 API（給 sr_engine 用）：
  - find_pivots(df, swing_window) -> tuple[list[Pivot], list[Pivot]]
  - compute_atr(df, period=14) -> float (回傳 median ATR)
  - cluster_pivots(pivots, eps, kind) -> list[SRZone]
  - detect_swing_zones(df, **kwargs) -> list[SRZone]     # 端到端 pipeline
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .params import (
    SWING_WINDOW_DAILY, CLUSTER_ATR_MULT, MIN_TOUCHES,
    LOOKBACK_BARS, MIN_BARS_FOR_DETECTION,
)
from .types import Pivot, SRZone


# ── 公開 helper：median ATR ─────────────────────────────────────
def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    """回傳 median(ATR(period))，用作 cluster eps 與容差。

    Args:
        df: 必含 High, Low, Close 三欄
        period: ATR 視窗

    Returns:
        median ATR；資料不足回傳 0.0
    """
    if len(df) < period + 1:
        return 0.0
    h, l, c = df['High'].astype(float), df['Low'].astype(float), df['Close'].astype(float)
    prev_c = c.shift(1)
    tr = pd.concat([
        h - l,
        (h - prev_c).abs(),
        (l - prev_c).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(period, min_periods=period).mean()
    valid = atr.dropna()
    return float(valid.median()) if len(valid) else 0.0


# ── Step 1：找 swing pivots ─────────────────────────────────────
def find_pivots(
    df: pd.DataFrame,
    swing_window: int = SWING_WINDOW_DAILY,
) -> tuple[list[Pivot], list[Pivot]]:
    """找出局部高（resistance 候選）與低（support 候選）點。

    定義：第 i 根 bar 的 High 嚴格大於左右各 swing_window 根的 High
    → i 為局部高點。Low 亦然（反向）。

    Args:
        df: 必含 High, Low, Volume 三欄；index 任意
        swing_window: 左右視窗大小

    Returns:
        (highs, lows)：兩個 Pivot list
    """
    if len(df) < 2 * swing_window + 1:
        return [], []

    highs_arr = df['High'].astype(float).values
    lows_arr  = df['Low'].astype(float).values
    vols_arr  = (df['Volume'].astype(float).values if 'Volume' in df.columns
                 else np.zeros(len(df)))

    highs: list[Pivot] = []
    lows:  list[Pivot] = []
    w = swing_window
    n = len(df)

    for i in range(w, n - w):
        hi = highs_arr[i]
        lo = lows_arr[i]
        # 嚴格大於左右視窗的所有 high（用 max 配 epsilon 避免平台抖動）
        left_h  = highs_arr[i - w:i].max()
        right_h = highs_arr[i + 1:i + w + 1].max()
        if hi > left_h and hi > right_h:
            highs.append(Pivot(idx=i, price=float(hi), kind='high',
                                volume=float(vols_arr[i])))

        left_l  = lows_arr[i - w:i].min()
        right_l = lows_arr[i + 1:i + w + 1].min()
        if lo < left_l and lo < right_l:
            lows.append(Pivot(idx=i, price=float(lo), kind='low',
                                volume=float(vols_arr[i])))

    return highs, lows


# ── Step 2：把 pivots 群聚成 SR Zones ───────────────────────────
def cluster_pivots(
    pivots: list[Pivot],
    eps: float,
    kind: str,
    min_touches: int = MIN_TOUCHES,
) -> list[SRZone]:
    """一維 gap clustering：依價格排序，相鄰價差 ≤ eps 則併入同一群。

    Args:
        pivots: 來自 find_pivots 的同類 pivot（全 high 或全 low）
        eps: 群聚容差（=  median(ATR) × cluster_atr_mult）
        kind: 'high' 或 'low'（決定產出的 SRZone.kind）
        min_touches: 觸及次數低於此值的群捨棄

    Returns:
        list[SRZone]：每群一個區，中心為量能加權平均，
                      low/high 為該群最低/最高 pivot 價格
    """
    if not pivots or eps <= 0:
        return []

    sr_kind = 'resistance' if kind == 'high' else 'support'

    # 按價格排序
    sorted_p = sorted(pivots, key=lambda p: p.price)

    clusters: list[list[Pivot]] = [[sorted_p[0]]]
    for p in sorted_p[1:]:
        if p.price - clusters[-1][-1].price <= eps:
            clusters[-1].append(p)
        else:
            clusters.append([p])

    zones: list[SRZone] = []
    for cl in clusters:
        if len(cl) < min_touches:
            continue
        prices = [p.price for p in cl]
        vols   = [p.volume for p in cl]
        # 量能加權中心；若 volume 全為 0 退化成簡單平均
        vsum = sum(vols)
        if vsum > 0:
            center = sum(p * v for p, v in zip(prices, vols)) / vsum
        else:
            center = sum(prices) / len(prices)
        zones.append(SRZone(
            kind=sr_kind,
            low=min(prices),
            high=max(prices),
            center=float(center),
            touches=len(cl),
            source='swing',
            last_touch_idx=max(p.idx for p in cl),
        ))
    return zones


# ── Step 3：端到端 pipeline ─────────────────────────────────────
def detect_swing_zones(
    df: pd.DataFrame,
    swing_window: int = SWING_WINDOW_DAILY,
    cluster_atr_mult: float = CLUSTER_ATR_MULT,
    min_touches: int = MIN_TOUCHES,
    lookback: Optional[int] = LOOKBACK_BARS,
) -> list[SRZone]:
    """完整 A 源 pipeline：OHLCV → swing pivots → ATR cluster → SR Zones。

    Args:
        df: 必含 High, Low, Close, Volume 四欄
        swing_window: 左右視窗（日線預設 5）
        cluster_atr_mult: 群聚容差倍數
        min_touches: 觸及門檻
        lookback: 只用最後 N 根 bar 計算；None 用全部

    Returns:
        list[SRZone]：含 support 與 resistance；source='swing'；
                      strength 尚未填入（待 sr_engine.score_zones）
    """
    if df is None or len(df) < MIN_BARS_FOR_DETECTION:
        return []
    if lookback is not None and len(df) > lookback:
        df = df.iloc[-lookback:].copy()

    atr = compute_atr(df)
    if atr <= 0:
        return []
    eps = atr * cluster_atr_mult

    highs, lows = find_pivots(df, swing_window=swing_window)
    res_zones = cluster_pivots(highs, eps=eps, kind='high', min_touches=min_touches)
    sup_zones = cluster_pivots(lows,  eps=eps, kind='low',  min_touches=min_touches)
    return res_zones + sup_zones
