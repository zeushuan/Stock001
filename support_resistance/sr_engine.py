"""support_resistance.sr_engine — Subsystem C
=================================================

對應規格 §1.4 / §2.1 / §3.3 / §3.4 / §4：
- 整數價位（規格 §1.4 / §2.1：心理價位）
- 融合 / confluence（§2.1 / §3.3）
- 強度評分 0-100（§3.3，四分量加權 + support_bias）
- Role reversal（§3.3）
- Recency decay（§3.4）
- sr_context_for_t3（§4）

公開 API：
  - round_number_zones(low, high, atr) -> list[SRZone]
  - fuse_zones(swing, profile, round, atr) -> list[SRZone]
  - score_zones(zones, df, weights, support_bias) -> list[SRZone]
  - detect_role_reversal(zones, df) -> list[SRZone]     # mutates in place
  - detect_sr_zones(df, current_price, **kwargs) -> list[SRZone]   # 端到端
  - sr_context_for_t3(zones, current_price, adx, ...) -> dict
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .params import (
    W_TOUCH, W_VOLUME, W_RECENCY, W_CONFLUENCE,
    SUPPORT_BIAS, ROUND_NUMBER_STEPS,
    PROXIMITY_PCT, T3_ADJUSTMENT_CAP,
    ADX_TREND_DAMPING, ADX_DAMPING_REFERENCE,
    MIN_BARS_FOR_DETECTION, CLUSTER_ATR_MULT,
    MAX_ZONE_WIDTH_ATR_MULT,
)
from .types import SRZone
from .swing_cluster import compute_atr, detect_swing_zones, find_pivots
from .volume_profile import compute_profile, profile_to_zones


# ── LuxAlgo-style: 最近 pivot R 與 S ──────────────────────────────
def latest_pivot_levels(
    df: pd.DataFrame,
    current_price: float,
    swing_window: int = 5,
) -> dict:
    """簡化版 LuxAlgo 風格（已被 luxalgo_sr 取代，保留向後相容）。

    回傳最近一個在現價方向的 pivot high/low。

    Args:
        df: OHLCV
        current_price: 現在價
        swing_window: 左右視窗

    Returns:
        {resistance, resistance_idx, support, support_idx}
    """
    out = {'resistance': None, 'resistance_idx': None,
           'support': None, 'support_idx': None}
    if df is None or len(df) < 2 * swing_window + 1 or current_price <= 0:
        return out
    highs, lows = find_pivots(df, swing_window=swing_window)
    for p in reversed(highs):
        if p.price > current_price:
            out['resistance'] = float(p.price)
            out['resistance_idx'] = int(p.idx)
            break
    for p in reversed(lows):
        if p.price < current_price:
            out['support'] = float(p.price)
            out['support_idx'] = int(p.idx)
            break
    return out


def luxalgo_sr(
    df: pd.DataFrame,
    left_bars: int = 15,
    right_bars: int = 15,
    volume_threshold: float = 20.0,
) -> dict:
    """完整版 LuxAlgo「Support and Resistance Levels with Breaks」實作。

    對應 Pine Script 演算法:
        ph = ta.pivothigh(leftBars, rightBars)
        pl = ta.pivotlow(leftBars, rightBars)
        short = ta.ema(volume, 5)
        long  = ta.ema(volume, 10)
        osc = 100 * (short - long) / long
        var float r = na; var float s = na
        if not na(ph): r := ph
        if not na(pl): s := pl
        breakBull = ta.crossover(close, r)   // (+ vol confirm)
        breakBear = ta.crossunder(close, s)

    Args:
        df: OHLCV (High, Low, Close, Volume；index 任意)
        left_bars:        pivot 左視窗（LuxAlgo 預設 15）
        right_bars:       pivot 右視窗（LuxAlgo 預設 15）
        volume_threshold: volume oscillator 突破門檻 %（LuxAlgo 預設 20）

    Returns:
        {
            'pivot_highs':   [(idx, price), ...]   全部偵測到的 pivot high
            'pivot_lows':    [(idx, price), ...]
            'current_r':     float | None          最新 pivot high 價
            'current_r_idx': int   | None          該 pivot 的 bar idx
            'current_s':     float | None
            'current_s_idx': int   | None
            'breaks_up':     [(idx, price), ...]   每次向上突破事件
            'breaks_down':   [(idx, price), ...]
            'left_bars':     int                   參數（給 chart 用）
            'right_bars':    int
        }

    Notes:
        - left/right 可以不同（如 left=10 right=20）。LuxAlgo 預設兩邊都 15。
        - breaks 已含 volume oscillator 確認（osc > volume_threshold）。
        - current_r/s 是「最新」pivot 的價位，會在每個新 pivot 形成時更新。
          線在圖上會「從 pivot idx 延伸到右側」，這是 LuxAlgo 視覺特徵。
    """
    out = {
        'pivot_highs': [], 'pivot_lows': [],
        'current_r': None, 'current_r_idx': None,
        'current_s': None, 'current_s_idx': None,
        'breaks_up': [], 'breaks_down': [],
        'left_bars': left_bars, 'right_bars': right_bars,
    }
    if df is None or len(df) < left_bars + right_bars + 5:
        return out

    n = len(df)
    high = df['High'].astype(float).values
    low  = df['Low'].astype(float).values
    close = df['Close'].astype(float).values
    volume = (df['Volume'].astype(float).values
              if 'Volume' in df.columns else np.zeros(n))

    # ── 1. 非對稱 pivot 偵測 ─────────────────────────────────────
    # LuxAlgo: ta.pivothigh(left, right) 在 i 確認 high 為 pivot 若
    #   i 的 high > 左邊 left 根 high 且 > 右邊 right 根 high
    # 該 pivot 在 bar (i + right) 「確認」（因為要看到右邊 right 根才知道）
    pivot_highs: list[tuple[int, float]] = []
    pivot_lows:  list[tuple[int, float]] = []
    for i in range(left_bars, n - right_bars):
        h_i = high[i]
        l_i = low[i]
        left_h_max = high[i - left_bars:i].max()
        right_h_max = high[i + 1:i + right_bars + 1].max()
        if h_i > left_h_max and h_i > right_h_max:
            pivot_highs.append((i, float(h_i)))

        left_l_min = low[i - left_bars:i].min()
        right_l_min = low[i + 1:i + right_bars + 1].min()
        if l_i < left_l_min and l_i < right_l_min:
            pivot_lows.append((i, float(l_i)))

    out['pivot_highs'] = pivot_highs
    out['pivot_lows']  = pivot_lows

    # ── 2. Volume oscillator（5/10 EMA）─────────────────────────
    # osc = 100 * (EMA5 - EMA10) / EMA10
    def _ema(arr: np.ndarray, span: int) -> np.ndarray:
        alpha = 2.0 / (span + 1)
        out_arr = np.empty(len(arr))
        out_arr[0] = arr[0]
        for k in range(1, len(arr)):
            out_arr[k] = alpha * arr[k] + (1 - alpha) * out_arr[k - 1]
        return out_arr

    if volume.sum() > 0:
        ema5  = _ema(volume, 5)
        ema10 = _ema(volume, 10)
        with np.errstate(divide='ignore', invalid='ignore'):
            osc = 100 * (ema5 - ema10) / np.where(ema10 == 0, 1, ema10)
    else:
        osc = np.zeros(n)

    # ── 3. 持續追蹤 current R / S（var float r/s 模擬）+ break 偵測 ─
    # 走訪每根 bar i，在 i + right_bars 「確認」當下的 pivot
    cur_r: float | None = None
    cur_r_idx: int | None = None
    cur_s: float | None = None
    cur_s_idx: int | None = None
    # 建 idx→pivot 對照（用 confirm 時點 i + right_bars）
    ph_confirm = {idx + right_bars: price for idx, price in pivot_highs}
    pl_confirm = {idx + right_bars: price for idx, price in pivot_lows}

    breaks_up:   list[tuple[int, float]] = []
    breaks_down: list[tuple[int, float]] = []

    for i in range(n):
        # 更新 pivot（在 confirm 時點）
        if i in ph_confirm:
            cur_r = ph_confirm[i]
            # 找回原 pivot bar idx
            cur_r_idx = i - right_bars
        if i in pl_confirm:
            cur_s = pl_confirm[i]
            cur_s_idx = i - right_bars

        # 突破判定（需有前一根 close，且 vol osc > threshold）
        if i >= 1 and cur_r is not None:
            prev_c = close[i - 1]
            curr_c = close[i]
            if prev_c <= cur_r < curr_c and osc[i] > volume_threshold:
                breaks_up.append((i, float(curr_c)))
        if i >= 1 and cur_s is not None:
            prev_c = close[i - 1]
            curr_c = close[i]
            if prev_c >= cur_s > curr_c and osc[i] > volume_threshold:
                breaks_down.append((i, float(curr_c)))

    out['current_r'] = cur_r
    out['current_r_idx'] = cur_r_idx
    out['current_s'] = cur_s
    out['current_s_idx'] = cur_s_idx
    out['breaks_up'] = breaks_up
    out['breaks_down'] = breaks_down
    return out


# ── C-1：整數價位（規格 §1.4 / §2.1） ───────────────────────────
def round_number_zones(
    low: float,
    high: float,
    atr: float,
    steps: tuple = ROUND_NUMBER_STEPS,
) -> list[SRZone]:
    """在 [low, high] 範圍內列出整數關卡（.00 / .50）作為 SRZone 候選。

    每個整數價位以 ATR 寬度給一個窄區（low=p-atr*0.25, high=p+atr*0.25）。
    kind 先標 'support'，融合階段會依現價重新分類。

    Args:
        low / high: 顯示範圍
        atr: 用於決定區寬
        steps: 整數步長，預設 (1.00, 0.50)

    Returns:
        list[SRZone]，source='round'
    """
    if atr <= 0 or high <= low:
        return []
    zones: list[SRZone] = []
    # 規格 §1.4「權重應小，輔助性質」→ round zones 必須窄：
    #   - 上限 atr*0.10（不要比真實波動更寬）
    #   - 同 step 內彼此不重疊：halfw ≤ step*0.4
    # 較大 step 優先（.00 比 .50 重要）
    for step in steps:
        halfw = min(max(atr * 0.10, (high - low) * 0.001), step * 0.4)
        # 從 ceil(low/step)*step 往上跑到 high
        n0 = int(np.ceil(low / step))
        p = n0 * step
        while p <= high:
            # 跳過 .00 同時在 .50 步長時產生的重複（.00 已包在 1.00 步）
            if not any(abs(p - z.center) < z.width() * 0.5 + halfw for z in zones):
                # 🆕 v9.47.6：追溯 origin — 紀錄是哪個 step 的哪個整數價
                origin = {
                    'kind': 'round',
                    'level': float(p),
                    'step': float(step),
                }
                zones.append(SRZone(
                    kind='support',                  # 暫定，融合時重判
                    low=p - halfw, high=p + halfw, center=float(p),
                    touches=0, source='round',
                    last_touch_idx=-1,
                    components={'_origins': [origin]},
                ))
            p = round(p + step, 8)
    return zones


# ── C-2：融合（規格 §2.1 / §3.3）─────────────────────────────────
def fuse_zones(
    swing_zones: list[SRZone],
    profile_zones: list[SRZone],
    round_zones: list[SRZone],
    atr: float,
    current_price: Optional[float] = None,
) -> list[SRZone]:
    """把三源 zones 融合：ATR 容差內重疊者合併成一個 'fused' zone。

    融合規則（規格 §2.1）：
      A 與 B 落在同一 ATR 容差帶內 → 重疊；
      重疊區的 confluence 分量會在 score_zones 計算。

    本函式只做物理合併，不做評分。

    Args:
        swing_zones / profile_zones / round_zones: 三源輸出
        atr: 容差基準（容差帶 = atr * CLUSTER_ATR_MULT）
        current_price: 用來分類 round zones 的 kind（若 None 維持原樣）

    Returns:
        list[SRZone]：融合後的 zones，多源重疊者 source='fused'
                       且 components['_source_set'] 記錄原始來源集
    """
    if current_price is not None:
        for z in round_zones:
            z.kind = 'resistance' if z.center > current_price else 'support'

    tol = atr * CLUSTER_ATR_MULT if atr > 0 else 0.0
    # 防 runaway chaining：合併後寬度上限
    max_width = atr * MAX_ZONE_WIDTH_ATR_MULT if atr > 0 else 0.0
    all_zones = swing_zones + profile_zones + round_zones

    # 為避免合併不同 kind（support 不該併壓力），分組
    fused_by_kind: dict[str, list[SRZone]] = {'support': [], 'resistance': []}
    for kind in ('support', 'resistance'):
        same_kind = [z for z in all_zones if z.kind == kind]
        same_kind.sort(key=lambda z: z.center)
        for z in same_kind:
            should_merge = False
            if fused_by_kind[kind] and fused_by_kind[kind][-1].overlaps(z, tol=tol):
                prev = fused_by_kind[kind][-1]
                merged_lo_test = min(prev.low, z.low)
                merged_hi_test = max(prev.high, z.high)
                # 寬度上限檢查（防多個 swing 經由 HVN 連鎖無限合併）
                if max_width <= 0 or (merged_hi_test - merged_lo_test) <= max_width:
                    should_merge = True
            if should_merge:
                prev = fused_by_kind[kind][-1]
                merged_lo  = min(prev.low, z.low)
                merged_hi  = max(prev.high, z.high)
                # 中心 = 觸及加權（profile/round 觸及視為 1）
                w_prev = max(prev.touches, 1)
                w_z    = max(z.touches, 1)
                ctr = (prev.center * w_prev + z.center * w_z) / (w_prev + w_z)
                src_set = set(prev.components.get('_source_set', {prev.source}))
                src_set.add(z.source)
                # 🆕 v9.47.6：origins 串接 — 保留 prev 與 z 的所有 origin 細節
                prev_origins = (prev.components.get('_origins', [])
                                if isinstance(prev.components, dict) else [])
                z_origins = (z.components.get('_origins', [])
                             if isinstance(z.components, dict) else [])
                merged_origins = list(prev_origins) + list(z_origins)
                merged = SRZone(
                    kind=kind,
                    low=merged_lo, high=merged_hi, center=ctr,
                    touches=prev.touches + z.touches,
                    source='fused' if len(src_set) > 1 else prev.source,
                    last_touch_idx=max(prev.last_touch_idx, z.last_touch_idx),
                    components={
                        '_source_set': src_set,
                        '_origins': merged_origins,
                    },
                )
                fused_by_kind[kind][-1] = merged
            else:
                if isinstance(z.components, dict):
                    z.components.setdefault('_source_set', {z.source})
                    z.components.setdefault('_origins', [])
                fused_by_kind[kind].append(z)

    # 🆕 v9.47.7：round 貢獻過濾
    # 問題：fuse_zones 把任何邊重疊的 round 都當共振，但一個 $9 寬的 swing
    #        zone 內會涵蓋 9 個整數，這些整數其實只是「被涵蓋」而非「真共振」。
    # 解：round origin level 距 zone.center 必須 ≤ atr × 0.5 才保留；
    #      過濾後若只剩單一 source，把 zone.source 從 'fused' 改回該 source。
    round_tol = atr * 0.5 if atr > 0 else 0
    if round_tol > 0:
        for kind_zones in fused_by_kind.values():
            for zone in kind_zones:
                if not isinstance(zone.components, dict):
                    continue
                origins = zone.components.get('_origins', [])
                if not origins:
                    continue
                filtered = []
                dropped_round = 0
                for o in origins:
                    if o.get('kind') == 'round':
                        level = float(o.get('level', 0))
                        if abs(level - zone.center) > round_tol:
                            dropped_round += 1
                            continue  # 整數價偏離中心 → 不算共振
                    filtered.append(o)
                if not filtered or dropped_round == 0:
                    continue  # 沒丟東西，不用更新
                new_sources = {o.get('kind') for o in filtered if o.get('kind')}
                zone.components['_origins'] = filtered
                zone.components['_source_set'] = new_sources or {zone.source}
                # 若過濾後只剩單一 source，從 'fused' 改回該 source
                if len(new_sources) == 1:
                    zone.source = next(iter(new_sources))

    # 🆕 v9.47.8：跨類 S+R 合併 — 同價位歷史上既當壓力又當支撐
    # 問題：swing 看歷史 pivots，可能在同一價位附近同時記到 swing high (R)
    #        和 swing low (S)；profile 看量能也可能跟 swing 鄰近卻 kind 不同。
    #        結果 chart 上同價位被同時標 S 又標 R，視覺衝突。
    # 解：若 S 和 R zone 中心距 ≤ atr × 0.5，併為一個 zone：
    #      kind 依 current_price 判定（上方=R，下方=S，等於不變）
    #      role_reversal=True 標示「同一價位歷史曾雙向」
    cross_tol = atr * 0.5 if atr > 0 else 0
    if cross_tol > 0:
        sup_list = list(fused_by_kind['support'])
        res_list = list(fused_by_kind['resistance'])
        new_sup, new_res = [], []
        used_r = set()
        for s in sup_list:
            matched = None
            for ri, r in enumerate(res_list):
                if ri in used_r:
                    continue
                if abs(s.center - r.center) <= cross_tol:
                    matched = (ri, r)
                    break
            if matched is None:
                new_sup.append(s)
                continue
            ri, r = matched
            used_r.add(ri)
            # 合併 bounds / origins / source_set
            merged_lo = min(s.low, r.low)
            merged_hi = max(s.high, r.high)
            w_s = max(s.touches, 1)
            w_r = max(r.touches, 1)
            merged_ctr = (s.center * w_s + r.center * w_r) / (w_s + w_r)
            s_o = (s.components.get('_origins', [])
                   if isinstance(s.components, dict) else [])
            r_o = (r.components.get('_origins', [])
                   if isinstance(r.components, dict) else [])
            s_ss = (s.components.get('_source_set', {s.source})
                    if isinstance(s.components, dict) else {s.source})
            r_ss = (r.components.get('_source_set', {r.source})
                    if isinstance(r.components, dict) else {r.source})
            merged_ss = set(s_ss) | set(r_ss)
            # kind 依現價：等於現價時保持 support（保守）
            if current_price is not None and merged_ctr > current_price:
                m_kind = 'resistance'
            else:
                m_kind = 'support'
            merged_src = ('fused' if len(merged_ss) > 1
                          else next(iter(merged_ss)))
            merged = SRZone(
                kind=m_kind,
                low=merged_lo, high=merged_hi, center=merged_ctr,
                touches=s.touches + r.touches,
                source=merged_src,
                last_touch_idx=max(s.last_touch_idx, r.last_touch_idx),
                role_reversal=True,  # ⇄ 同價位歷史雙向
                components={
                    '_source_set': merged_ss,
                    '_origins': list(s_o) + list(r_o),
                },
            )
            if m_kind == 'support':
                new_sup.append(merged)
            else:
                new_res.append(merged)
        # 沒被併的 resistance
        for ri, r in enumerate(res_list):
            if ri not in used_r:
                new_res.append(r)
        fused_by_kind['support'] = new_sup
        fused_by_kind['resistance'] = new_res

    # 🆕 v9.47.9：依現價方向統一分類
    # 修正 stale kind — 譬如 swing high 在現價下方仍標 resistance
    # 一律改：center > 現價 → resistance；center < 現價 → support
    # kind 變動 → 自動標 role_reversal
    if current_price is not None:
        all_zones = fused_by_kind['support'] + fused_by_kind['resistance']
        for z in all_zones:
            orig_kind = z.kind
            if z.center > current_price:
                z.kind = 'resistance'
            elif z.center < current_price:
                z.kind = 'support'
            if z.kind != orig_kind:
                z.role_reversal = True
        # 重新分組
        fused_by_kind = {'support': [], 'resistance': []}
        for z in all_zones:
            fused_by_kind.setdefault(z.kind, []).append(z)

    return fused_by_kind.get('support', []) + fused_by_kind.get('resistance', [])


# ── C-3：強度評分（規格 §3.3）────────────────────────────────────
def score_zones(
    zones: list[SRZone],
    df: pd.DataFrame,
    weights: tuple = (W_TOUCH, W_VOLUME, W_RECENCY, W_CONFLUENCE),
    support_bias: float = SUPPORT_BIAS,
    lookback: Optional[int] = None,
) -> list[SRZone]:
    """填入每個 zone 的 strength（0-100）與 components 四分量。

    分量定義（規格 §3.3）：
      touch      = min(touches/5, 1)
      volume     = 區內 bar 的累積成交量相對 df 中位 squash
      recency    = 1 − age/lookback（age = 從 last_touch_idx 到最後 bar）
      confluence = (來源 set 大小) / 3

    Args:
        zones: 融合後的 zone list
        df: 用於計算 volume 與 recency 的 OHLCV
        weights: 四分量權重，預設 (0.30, 0.30, 0.20, 0.20)
        support_bias: 支撐區乘數，預設 1.05
        lookback: recency 用的視窗；None 用 len(df)

    Returns:
        同一個 list（in-place 修改），strength 與 components 已填入
    """
    if not zones or df is None or len(df) == 0:
        return zones
    w_t, w_v, w_r, w_c = weights
    n = len(df)
    lb = lookback if lookback else n
    median_vol = float(pd.Series(df['Volume']).median()) if 'Volume' in df.columns else 0.0

    for z in zones:
        # touch
        touch_s = min(z.touches / 5.0, 1.0)

        # volume：該區內 bars 的累積量 / (median × lookback)
        if 'Volume' in df.columns and median_vol > 0:
            in_zone = ((df['High'] >= z.low) & (df['Low'] <= z.high))
            vol_in = float(df.loc[in_zone, 'Volume'].sum())
            # squash：相對於 (median × bars_count_in_zone) 比 1 大就強
            n_in = int(in_zone.sum())
            if n_in > 0:
                vol_score = vol_in / (median_vol * n_in)
                # 映射到 0-1：用 tanh squash 避免極端值
                volume_s = float(np.tanh(vol_score - 1))
                volume_s = max(0.0, min(1.0, (volume_s + 1) / 2))
            else:
                volume_s = 0.0
        else:
            volume_s = 0.0

        # recency
        if z.last_touch_idx >= 0:
            age = (n - 1) - z.last_touch_idx
            recency_s = max(0.0, 1.0 - age / max(lb, 1))
        else:
            recency_s = 0.0

        # confluence
        src_set = z.components.get('_source_set', {z.source}) if isinstance(z.components, dict) else {z.source}
        conf_s = min(len(src_set) / 3.0, 1.0)

        raw = 100.0 * (
            w_t * touch_s +
            w_v * volume_s +
            w_r * recency_s +
            w_c * conf_s
        )
        if z.kind == 'support':
            raw *= support_bias
        z.strength = float(max(0.0, min(100.0, raw)))
        # 🆕 v9.47.6：保留 _origins（追溯來源細節給 UI 顯示）
        existing_origins = (z.components.get('_origins', [])
                            if isinstance(z.components, dict) else [])
        z.components = {
            'touch': round(touch_s, 3),
            'volume': round(volume_s, 3),
            'recency': round(recency_s, 3),
            'confluence': round(conf_s, 3),
            '_source_set': src_set,
            '_origins': existing_origins,
        }
    return zones


# ── C-4：Role Reversal（規格 §3.3）───────────────────────────────
def detect_role_reversal(
    zones: list[SRZone],
    df: pd.DataFrame,
) -> list[SRZone]:
    """若收盤價曾穿越某區兩側，標記 role_reversal=True。

    判定：在 last_touch_idx 之後，是否有 close 同時越過 z.high 與 z.low。
    """
    if not zones or df is None or len(df) == 0:
        return zones
    closes = df['Close'].astype(float).values
    n = len(closes)
    for z in zones:
        start = max(0, z.last_touch_idx)
        if start >= n - 1:
            continue
        seg = closes[start:]
        if (seg > z.high).any() and (seg < z.low).any():
            z.role_reversal = True
    return zones


# ── C-5：端到端 pipeline ────────────────────────────────────────
def detect_sr_zones(
    df: pd.DataFrame,
    current_price: Optional[float] = None,
    swing_window: Optional[int] = None,
    cluster_atr_mult: float = CLUSTER_ATR_MULT,
    min_touches: Optional[int] = None,
    lookback: Optional[int] = None,
    use_volume_profile: bool = True,
    use_round_numbers: bool = True,
    profile_rth_filter: bool = True,
) -> list[SRZone]:
    """完整 S/R 偵測 pipeline（A + B + C 三源融合 + 評分）。

    Args:
        df: OHLCV (High, Low, Close, Volume；index 任意)
        current_price: 用來給 round_number_zones 分類；None 用 df 最後 Close
        swing_window: 覆寫 swing 視窗（None 用 params 預設）
        cluster_atr_mult: ATR cluster 容差倍數
        min_touches: swing zone 最少觸及次數
        lookback: 回看 bar 數
        use_volume_profile: 是否啟用 B 源
        use_round_numbers: 是否啟用 C-1 整數價位
        profile_rth_filter: B 源是否只用 RTH

    Returns:
        list[SRZone]：strength 已填入；可直接給前端或 sr_context_for_t3
    """
    if df is None or len(df) < MIN_BARS_FOR_DETECTION:
        return []

    # 截尾
    if lookback is not None and len(df) > lookback:
        df = df.iloc[-lookback:].copy()

    atr = compute_atr(df)
    if atr <= 0:
        return []

    if current_price is None:
        current_price = float(df['Close'].iloc[-1])

    # A 源
    kwargs_a = {'cluster_atr_mult': cluster_atr_mult}
    if swing_window is not None:
        kwargs_a['swing_window'] = swing_window
    if min_touches is not None:
        kwargs_a['min_touches'] = min_touches
    swing = detect_swing_zones(df, **kwargs_a)

    # B 源
    if use_volume_profile:
        prof = compute_profile(df, bin_width=atr * 0.5,
                                rth_filter=profile_rth_filter)
        # 🆕 v9.47：HVN 寬度也吃 MAX_ZONE_WIDTH_ATR_MULT
        profile = profile_to_zones(
            prof, current_price=current_price,
            bar_count=len(df),
            max_width=atr * MAX_ZONE_WIDTH_ATR_MULT,
        )
    else:
        profile = []

    # C-1 整數
    rounds = (round_number_zones(low=float(df['Low'].min()),
                                  high=float(df['High'].max()), atr=atr)
              if use_round_numbers else [])

    # 融合 + 評分 + role reversal
    fused = fuse_zones(swing, profile, rounds, atr=atr,
                       current_price=current_price)
    scored = score_zones(fused, df, lookback=lookback or len(df))
    detect_role_reversal(scored, df)
    # 依強度降冪排序，方便上層直接吃前 N 個
    return sorted(scored, key=lambda z: -z.strength)


# ── C-6：T3 接口（規格 §4）───────────────────────────────────────
def sr_context_for_t3(
    zones: list[SRZone],
    current_price: float,
    adx: Optional[float] = None,
    proximity_pct: float = PROXIMITY_PCT,
    cap: int = T3_ADJUSTMENT_CAP,
    adx_damping: float = ADX_TREND_DAMPING,
    adx_ref: float = ADX_DAMPING_REFERENCE,
) -> dict:
    """依現價貼近強支撐／壓力的程度，回傳給 T3 Confidence 的修正值。

    規格 §4：
      - 貼近強壓力（在 proximity_pct 內）→ 多頭信心扣分
      - 貼近強支撐 → 加分
      - 修正幅度 ∝ 區強度，封頂 ±cap（預設 ±15）
      - ADX 強趨勢時降低 S/R 對 T3 的影響：
        adjustment × (1 − min(ADX/adx_ref, 1) × adx_damping)

    Args:
        zones: detect_sr_zones 的輸出（含 strength）
        current_price: 目前價格
        adx: 用於趨勢調節；None 視為不調節
        proximity_pct: 觸發貼近的百分比門檻（預設 1.5%）

    Returns:
        dict {
          'adjustment': int (−cap ~ +cap，多頭視角；負=追高風險),
          'reason': str ('near_resistance' / 'near_support' / 'none'),
          'nearest_resistance': SRZone or None,
          'nearest_support': SRZone or None,
          'adx_damping_applied': float (0-1，1 = 不衰減),
        }
    """
    if not zones or current_price <= 0:
        return {
            'adjustment': 0, 'reason': 'none',
            'nearest_resistance': None, 'nearest_support': None,
            'adx_damping_applied': 1.0,
        }

    tol = current_price * (proximity_pct / 100.0)
    # 找最強且在 tol 範圍內的支撐／壓力
    near_res = None
    near_sup = None
    for z in zones:
        d = z.center - current_price
        if z.kind == 'resistance' and 0 <= d <= tol:
            if near_res is None or z.strength > near_res.strength:
                near_res = z
        elif z.kind == 'support' and 0 <= -d <= tol:
            if near_sup is None or z.strength > near_sup.strength:
                near_sup = z

    raw_adj = 0.0
    reason = 'none'
    # 同時近壓力＋近支撐時，比強度
    if near_res and near_sup:
        if near_res.strength >= near_sup.strength:
            raw_adj = -(near_res.strength / 100.0) * cap
            reason = 'near_resistance'
        else:
            raw_adj = +(near_sup.strength / 100.0) * cap
            reason = 'near_support'
    elif near_res:
        raw_adj = -(near_res.strength / 100.0) * cap
        reason = 'near_resistance'
    elif near_sup:
        raw_adj = +(near_sup.strength / 100.0) * cap
        reason = 'near_support'

    # ADX 趨勢調節（規格 §4 末段）
    damping = 1.0
    if adx is not None and adx > 0:
        damping = 1.0 - min(adx / adx_ref, 1.0) * adx_damping
        damping = max(0.0, min(1.0, damping))
    raw_adj *= damping

    adj = int(round(max(-cap, min(cap, raw_adj))))
    return {
        'adjustment': adj,
        'reason': reason,
        'nearest_resistance': near_res,
        'nearest_support': near_sup,
        'adx_damping_applied': round(damping, 3),
    }
