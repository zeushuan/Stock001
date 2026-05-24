"""support_resistance.volume_profile — Subsystem B
======================================================

對應規格 §3.2：Volume Profile（量價分布）。

演算法：
  1. 決定價格分箱 bins：bin 寬 = median(ATR) × 0.5（或固定 N_bins=50）
  2. 對每根 bar，把 volume 灌進其價格範圍 [low, high] 涵蓋的 bins
     - 近似法：把 volume 平均分配到 [low, high] 涵蓋的 bins
       （規格 §3.2 — 用 bars_1m 時的標準做法）
  3. 得 volume-at-price 直方圖 V[bin]
  4. POC = argmax(V)
  5. Value Area：自 POC 向兩側擴張，累積到達總量 70% 為止 → VAL, VAH
  6. HVN：V[bin] ≥ mean(V) × hvn_mult 的連續 bins → 高量區
     LVN：V[bin] ≤ mean(V) × lvn_mult 的連續 bins → 真空區

公開 API（給 sr_engine 用）：
  - compute_profile(df, n_bins, ...) -> VolumeProfile
  - profile_to_zones(profile) -> list[SRZone]    # HVN 轉成 SRZone
  - filter_rth(df) -> df                          # 09:30-16:00 ET 過濾
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .params import (
    N_BINS, BIN_WIDTH_ATR_MULT, VALUE_AREA_PCT,
    HVN_MULT, LVN_MULT, RTH_FILTER_DEFAULT, MIN_BARS_FOR_DETECTION,
)
from .types import VolumeProfile, SRZone


# ── RTH 過濾（規格 §2.4）─────────────────────────────────────────
def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    """過濾出 RTH（regular trading hours 09:30-16:00 ET）的 bar。

    - 若 index 是 DatetimeIndex 且帶 tz（或 ET 假設），按時間切
    - 否則回傳原 df（無法判斷時 fallback 用全部）

    Args:
        df: OHLCV，index 為 DatetimeIndex 較佳

    Returns:
        過濾後的 df；若無法判斷時間則回原 df
    """
    if not isinstance(df.index, pd.DatetimeIndex) or len(df) == 0:
        return df
    try:
        # 若沒 tz 就視為 ET（規格 §2.4 美股鎖定 09:30-16:00 ET）
        idx = df.index
        if idx.tz is None:
            # naive timestamp — 直接用時間過濾
            t = idx.time
        else:
            # 已 tz-aware → 轉 ET 後取 time
            t = idx.tz_convert('America/New_York').time
        from datetime import time as _t
        rth_mask = np.array([
            _t(9, 30) <= ts < _t(16, 0) for ts in t
        ])
        return df.iloc[rth_mask]
    except Exception:
        return df


# ── 主函式：計算量價分布 ────────────────────────────────────────
def compute_profile(
    df: pd.DataFrame,
    n_bins: Optional[int] = N_BINS,
    bin_width: Optional[float] = None,
    value_area_pct: float = VALUE_AREA_PCT,
    hvn_mult: float = HVN_MULT,
    lvn_mult: float = LVN_MULT,
    rth_filter: bool = RTH_FILTER_DEFAULT,
) -> Optional[VolumeProfile]:
    """計算 OHLCV 的 Volume Profile（規格 §3.2）。

    Args:
        df: 必含 High, Low, Volume 三欄
        n_bins: 固定 bin 數；若給 bin_width 則此參數忽略
        bin_width: 動態 bin 寬（如 median(ATR)×0.5）；優先於 n_bins
        value_area_pct: Value Area 累積比例（0-1，預設 0.70）
        hvn_mult: 高量區門檻（× mean）
        lvn_mult: 真空區門檻（× mean）
        rth_filter: 是否只用 RTH bar（美股 09:30-16:00 ET）

    Returns:
        VolumeProfile；資料不足或無量回傳 None
    """
    if df is None or len(df) < MIN_BARS_FOR_DETECTION:
        return None

    if rth_filter:
        df_use = filter_rth(df)
        if len(df_use) < MIN_BARS_FOR_DETECTION:
            # RTH 過濾後不足，fallback 用全部（避免回 None）
            df_use = df
    else:
        df_use = df

    high = df_use['High'].astype(float).values
    low  = df_use['Low'].astype(float).values
    vol  = (df_use['Volume'].astype(float).values
            if 'Volume' in df_use.columns else np.zeros(len(df_use)))

    p_min = float(low.min())
    p_max = float(high.max())
    if p_max <= p_min or vol.sum() <= 0:
        return None

    # 決定 bins
    if bin_width is not None and bin_width > 0:
        nb = max(2, int(np.ceil((p_max - p_min) / bin_width)))
    else:
        nb = max(2, int(n_bins or N_BINS))
    edges = np.linspace(p_min, p_max, nb + 1)
    bin_vols = np.zeros(nb)

    # 規格 §3.2 近似法：把每根 bar 的 volume 平均分配到 [low, high] 涵蓋的 bins
    for hi, lo, v in zip(high, low, vol):
        if v <= 0 or hi < lo:
            continue
        # 找出該 bar 涵蓋的 bin 範圍
        lo_idx = int(np.searchsorted(edges, lo, side='right')) - 1
        hi_idx = int(np.searchsorted(edges, hi, side='right')) - 1
        lo_idx = max(0, min(nb - 1, lo_idx))
        hi_idx = max(0, min(nb - 1, hi_idx))
        n_bins_covered = hi_idx - lo_idx + 1
        share = v / n_bins_covered
        bin_vols[lo_idx:hi_idx + 1] += share

    # POC = argmax
    poc_idx = int(np.argmax(bin_vols))
    poc = float((edges[poc_idx] + edges[poc_idx + 1]) / 2)

    # Value Area：從 POC 向兩側擴張，至累積 ≥ value_area_pct
    total = float(bin_vols.sum())
    target = total * value_area_pct
    cum = float(bin_vols[poc_idx])
    lo_i, hi_i = poc_idx, poc_idx
    while cum < target and (lo_i > 0 or hi_i < nb - 1):
        left_v  = bin_vols[lo_i - 1] if lo_i > 0 else -1
        right_v = bin_vols[hi_i + 1] if hi_i < nb - 1 else -1
        if right_v >= left_v:
            hi_i += 1
            cum += float(bin_vols[hi_i])
        else:
            lo_i -= 1
            cum += float(bin_vols[lo_i])
    val = float(edges[lo_i])         # Value Area Low（下緣 = bin 下界）
    vah = float(edges[hi_i + 1])     # Value Area High（上緣 = bin 上界）

    # HVN / LVN：mean 為門檻基準
    mean_v = float(bin_vols.mean())
    hvn_thresh = mean_v * hvn_mult
    lvn_thresh = mean_v * lvn_mult
    hvn_zones = _merge_consecutive(bin_vols, edges, lambda v: v >= hvn_thresh)
    lvn_zones = _merge_consecutive(bin_vols, edges, lambda v: v <= lvn_thresh)

    return VolumeProfile(
        bin_edges=edges.tolist(),
        bin_volumes=bin_vols.tolist(),
        poc=poc, val=val, vah=vah,
        hvn_zones=hvn_zones,
        lvn_zones=lvn_zones,
    )


def _merge_consecutive(
    bin_vols: np.ndarray,
    edges: np.ndarray,
    predicate,
) -> list[tuple[float, float]]:
    """把符合 predicate(v) 的連續 bins 合併成 [low, high] 區段。"""
    out = []
    in_zone = False
    z_lo = 0
    nb = len(bin_vols)
    for i, v in enumerate(bin_vols):
        if predicate(float(v)):
            if not in_zone:
                z_lo = i
                in_zone = True
        else:
            if in_zone:
                out.append((float(edges[z_lo]), float(edges[i])))
                in_zone = False
    if in_zone:
        out.append((float(edges[z_lo]), float(edges[nb])))
    return out


# ── HVN → SRZone 轉換（給 sr_engine 融合用）─────────────────────
def profile_to_zones(
    profile: VolumeProfile,
    current_price: Optional[float] = None,
    bar_count: int = 0,
    max_width: Optional[float] = None,
) -> list[SRZone]:
    """把 Volume Profile 的 HVN 區轉成 SRZone list。

    HVN 區當下若高於現價→ resistance；低於現價 → support。
    若 current_price 為 None，全部標 'support'（中性 fallback;
    sr_engine 會在融合時依現價重新分類）。

    Args:
        profile: compute_profile 的輸出
        current_price: 用來判定 support / resistance
        bar_count: 該 profile 對應的 bar 數（給 last_touch_idx 用最後一根）
        max_width: HVN 區最大寬度上限（>0 才生效）；超寬 HVN 切成多段子區，
                   避免單一巨大 zone 在 chart overlay 上把整片塗一色

    Returns:
        list[SRZone]，source='profile'
    """
    if profile is None:
        return []
    zones: list[SRZone] = []
    last_idx = max(0, bar_count - 1)
    for lo, hi in profile.hvn_zones:
        width = hi - lo
        # 寬度上限 → 切割成多段子區
        if max_width and max_width > 0 and width > max_width:
            n_segments = int(np.ceil(width / max_width))
            seg_width = width / n_segments
            for k in range(n_segments):
                sub_lo = lo + k * seg_width
                sub_hi = sub_lo + seg_width
                sub_ctr = (sub_lo + sub_hi) / 2
                if current_price is None:
                    sub_kind = 'support'
                else:
                    sub_kind = 'resistance' if sub_ctr > current_price else 'support'
                # 🆕 v9.47.6：追溯 origin — 紀錄此區由「原 HVN [lo, hi] 第 k/n 段」分出
                origin = {
                    'kind': 'profile',
                    'hvn_low': float(lo),
                    'hvn_high': float(hi),
                    'segment': f'{k+1}/{n_segments}',
                }
                zones.append(SRZone(
                    kind=sub_kind, low=sub_lo, high=sub_hi, center=sub_ctr,
                    touches=0, source='profile', last_touch_idx=last_idx,
                    components={'_origins': [origin]},
                ))
        else:
            center = (lo + hi) / 2
            if current_price is None:
                kind = 'support'
            else:
                kind = 'resistance' if center > current_price else 'support'
            origin = {
                'kind': 'profile',
                'hvn_low': float(lo),
                'hvn_high': float(hi),
            }
            zones.append(SRZone(
                kind=kind, low=lo, high=hi, center=center,
                touches=0,           # profile 來源不用觸及次數
                source='profile',
                last_touch_idx=last_idx,
                components={'_origins': [origin]},
            ))
    return zones
