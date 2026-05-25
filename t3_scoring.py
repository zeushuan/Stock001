"""t3_scoring — T3 信心評分 + Wyckoff 技術閘門 (SMS Phase 2)
==============================================================

把原本內嵌在 tv_app.fetch_indicators 的 T3 計算抽出來成獨立模組,
給後續 smart_money_signals 與其它下游消費者共用。

對應規格《Stock001_SmartMoney_Module_Spec.md》§5：
  - T3 信心分數 = 5 個 EMA-based 子項（每命中 1 分，0-5 分）
  - Wyckoff 階段判別 → technical_gate multiplier (0.3 / 0.7 / 1.0)

公開 API:
  - compute_t3_confidence(...)    → (score 0-5, hits[])
  - compute_t3_pullback_days(rsi) → int (連續 RSI<50 天數)
  - compute_t4_rising_days(rsi)   → int (RSI<35 連續上升天數)
  - technical_gate(d)             → {phase, multiplier, reasons}

設計原則:
  - 純函式（無 state、無 I/O）
  - 與 tv_app 既有行為**完全一致**（同樣輸入 → 同樣輸出）
  - 例外吞掉 → 0 / 空 list（沿用 tv_app 原本 fallback）
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("stock001.t3_scoring")


# ── T3 信心分數（0-5）──────────────────────────────────────────
def compute_t3_confidence(
    close_now:    Optional[float],
    ema5_now:     Optional[float],
    ema20_now:    Optional[float],
    ema5_5d_ago:  Optional[float],
    ema20_5d_ago: Optional[float],
) -> tuple[int, list[str]]:
    """T3 多頭信心分數（5 個子項加總，每命中 1 分）。

    子項:
      C1     : close > EMA20            (價在 EMA20 上)
      C3     : EMA20_now > EMA20_5d_ago (EMA20 5d 斜率為正)
      E5     : EMA5_now > EMA5_5d_ago   (EMA5 5d 斜率為正)
      E5>E20 : EMA5 > EMA20             (多頭排列)
      雙升   : E5 上升 且 E20 上升      (雙均線都升)

    Returns:
        (score, hits): score 0-5；hits 命中描述清單
    """
    score = 0
    hits: list[str] = []
    try:
        if (close_now is not None and ema20_now is not None
                and close_now > ema20_now):
            score += 1; hits.append('close>EMA20')
        c3 = (ema20_now is not None and ema20_5d_ago is not None
              and ema20_now > ema20_5d_ago)
        if c3:
            score += 1; hits.append('EMA20上升')
        e5_up = (ema5_now is not None and ema5_5d_ago is not None
                 and ema5_now > ema5_5d_ago)
        if e5_up:
            score += 1; hits.append('EMA5上升')
        if (ema5_now is not None and ema20_now is not None
                and ema5_now > ema20_now):
            score += 1; hits.append('EMA5>EMA20')
        if e5_up and c3:
            score += 1; hits.append('雙均線都升')
    except Exception as exc:
        log.debug("[compute_t3_confidence] %s", exc)
    return score, hits


# ── T3 拉回天數 ───────────────────────────────────────────────
def compute_t3_pullback_days(rsi_series) -> int:
    """RSI 連續低於 50 的天數（從最後一天倒數）。

    Args:
        rsi_series: pd.Series 或 array-like (RSI 值)

    Returns:
        天數；若最新 RSI ≥ 50 則回 0
    """
    try:
        if isinstance(rsi_series, pd.Series):
            arr = rsi_series.dropna().values
        else:
            arr = np.asarray(rsi_series)
            arr = arr[~np.isnan(arr)]
        if len(arr) == 0 or arr[-1] >= 50:
            return 0
        cnt = 0
        for v in reversed(arr):
            if v < 50:
                cnt += 1
            else:
                break
        return cnt if cnt > 0 else 0
    except Exception as exc:
        log.debug("[compute_t3_pullback_days] %s", exc)
        return 0


# ── T4 反彈天數 ───────────────────────────────────────────────
def compute_t4_rising_days(rsi_series) -> int:
    """RSI < 35 且連續上升的天數（含當天）。

    用於 T4 空頭反彈型態的延續度判定。RSI 必須 < 35 才認定為空頭區。

    Args:
        rsi_series: pd.Series 或 array-like

    Returns:
        連續上升天數；若最新 RSI ≥ 32 直接 0
    """
    try:
        if isinstance(rsi_series, pd.Series):
            arr = rsi_series.dropna().values
        else:
            arr = np.asarray(rsi_series)
            arr = arr[~np.isnan(arr)]
        if len(arr) < 3 or arr[-1] >= 32:
            return 0
        cnt = 1  # 含當天
        for i in range(len(arr) - 1, 0, -1):
            if arr[i] > arr[i - 1] and arr[i] < 35:
                cnt += 1
            else:
                break
        return cnt
    except Exception as exc:
        log.debug("[compute_t4_rising_days] %s", exc)
        return 0


# ── Wyckoff 階段判別 + Technical Gate Multiplier ──────────────
# 對應規格《Smart Money》§5.1 / §5.2 / §6.1
#
# 規格 §6.1 multiplier:
#   啟動/吸籌 (Accumulation) → 1.0  (GO)
#   中性/盤整 (Neutral)      → 0.7  (WATCH)
#   派發 (Distribution)      → 0.3  (AVOID / 可視為 veto)

PHASE_ACCUMULATION = 'accumulation'
PHASE_NEUTRAL      = 'neutral'
PHASE_DISTRIBUTION = 'distribution'

PHASE_MULTIPLIER = {
    PHASE_ACCUMULATION: 1.0,
    PHASE_NEUTRAL:      0.7,
    PHASE_DISTRIBUTION: 0.3,
}

# 規格 §5.2 可量化指標門檻
DEV_SMA20_DISTRIBUTION = 20.0   # 乖離 > 20% → 派發風險
DEV_SMA50_DISTRIBUTION = 25.0   # 乖離 > 25% → 派發
RSI_OVERBOUGHT         = 70.0   # RSI > 70 → 超買
ACCUMULATION_SMA200_PCT = 5.0   # 距 SMA200 ±5% → 吸籌候選
ACCUMULATION_RSI_LOW   = 45.0   # 吸籌期 RSI 通常 < 50


def technical_gate(d: dict) -> dict:
    """Wyckoff 階段判別 + Technical Gate Multiplier。

    用 tv_app fetch_indicators 產出的 d dict 判斷現在是
    吸籌 / 盤整 / 派發 哪個階段，回傳 multiplier 給 SMS 使用。

    Args:
        d: fetch_indicators 的輸出 (含 close/sma20/sma50/sma200/rsi/adx/
           ema5/ema20/ema5_5d_ago/ema20_5d_ago 等欄位)

    Returns:
        {
            'phase':       'accumulation' / 'neutral' / 'distribution',
            'multiplier':  1.0 / 0.7 / 0.3,
            'reasons':     [str, ...]  判定依據
            'dev_sma20':   float | None  乖離 %
            'dev_sma50':   float | None  乖離 %
            't3_score':    int | None    T3 信心分數 (0-5)
        }
    """
    out = {
        'phase':      PHASE_NEUTRAL,
        'multiplier': PHASE_MULTIPLIER[PHASE_NEUTRAL],
        'reasons':    [],
        'dev_sma20':  None,
        'dev_sma50':  None,
        't3_score':   None,
    }
    if not isinstance(d, dict):
        return out

    close  = d.get('close')
    sma20  = d.get('sma20')
    sma50  = d.get('sma50')
    sma200 = d.get('sma200')
    rsi    = d.get('rsi')
    adx    = d.get('adx')

    dev20 = ((close - sma20) / sma20 * 100
             if (close and sma20 and sma20 > 0) else None)
    dev50 = ((close - sma50) / sma50 * 100
             if (close and sma50 and sma50 > 0) else None)
    out['dev_sma20'] = dev20
    out['dev_sma50'] = dev50

    # T3 信心分數（在判定中作為輔助）
    t3, _ = compute_t3_confidence(
        close, d.get('ema5'), d.get('ema20'),
        d.get('ema5_5d_ago'), d.get('ema20_5d_ago'))
    out['t3_score'] = t3

    reasons: list[str] = []

    # ── 1. 派發判定（嚴格優先 — 一旦派發就 veto）──
    is_distribution = False
    if dev20 is not None and dev20 > DEV_SMA20_DISTRIBUTION:
        is_distribution = True
        reasons.append(f'乖離 SMA20 +{dev20:.1f}% > {DEV_SMA20_DISTRIBUTION}%')
    if dev50 is not None and dev50 > DEV_SMA50_DISTRIBUTION:
        is_distribution = True
        reasons.append(f'乖離 SMA50 +{dev50:.1f}% > {DEV_SMA50_DISTRIBUTION}%')
    if rsi is not None and rsi > RSI_OVERBOUGHT and dev20 is not None and dev20 > 10:
        # RSI 超買 + 乖離過大 = 派發風險（規格 §5.2 RSI 頂背離簡化版）
        is_distribution = True
        reasons.append(f'RSI {rsi:.0f} > {RSI_OVERBOUGHT} 且 dev_sma20 大')

    if is_distribution:
        out['phase']      = PHASE_DISTRIBUTION
        out['multiplier'] = PHASE_MULTIPLIER[PHASE_DISTRIBUTION]
        out['reasons']    = reasons
        return out

    # ── 2. 吸籌判定 ──
    is_accumulation = False
    if (close is not None and sma200 is not None and sma200 > 0):
        dev200 = (close - sma200) / sma200 * 100
        if abs(dev200) <= ACCUMULATION_SMA200_PCT:
            # 在 SMA200 附近（±5%）
            # 加碼條件：RSI < 45 或 T3 score 開始上升（≥ 2）→ 吸籌進場期
            if (rsi is not None and rsi < ACCUMULATION_RSI_LOW) or t3 >= 2:
                is_accumulation = True
                reasons.append(f'距 SMA200 {dev200:+.1f}% (±5% 內)')
                if rsi is not None and rsi < ACCUMULATION_RSI_LOW:
                    reasons.append(f'RSI {rsi:.0f} < {ACCUMULATION_RSI_LOW}')
                if t3 >= 2:
                    reasons.append(f'T3 score {t3}/5 (轉強中)')

    # ADX 強趨勢 + T3 高分 → 也視為吸籌完成/啟動初期
    if (not is_accumulation and adx is not None and adx >= 25 and t3 >= 4):
        is_accumulation = True
        reasons.append(f'ADX {adx:.0f} ≥ 25 + T3 {t3}/5 強勢')

    if is_accumulation:
        out['phase']      = PHASE_ACCUMULATION
        out['multiplier'] = PHASE_MULTIPLIER[PHASE_ACCUMULATION]
        out['reasons']    = reasons
        return out

    # ── 3. 預設盤整 ──
    out['reasons'] = reasons or ['無顯著訊號，預設盤整']
    return out
