"""signal_predictor — T1/T3/T4 觸發所需 close 預測
======================================================

對應現有戰法（tv_app.classify_action）的進場條件:
  T1 黃金交叉：EMA20 由下穿越 EMA60
  T3 多頭拉回：EMA20>EMA60 (多頭) + RSI < 50
  T4 空頭反彈：EMA20<EMA60 (空頭) + RSI < 35 + RSI 上升

對每個訊號預測「今天 close 落到多少」會觸發。

公開 API:
  - predict_t1_close(ema20_prev, ema60_prev) -> Optional[float]
        EMA20 = EMA60 的 close threshold（多日死叉 → 單日翻紅的價位）
  - predict_rsi_close(close_series, target_rsi=50, period=14) -> Optional[float]
        今日 close 多少 → RSI 等於 target_rsi（Wilder 14 平滑）
  - predict_signal_triggers(d) -> dict
        端到端：給 fetch_indicators 的 d，回傳 3 個 signal 的 trigger info

每個 trigger info 結構:
  {
    'status': 'triggered' / 'reachable' / 'not_applicable',
    'target_close': float | None,
    'target_pct':   float | None  (今日 close 距 target 的 %; +=要漲，-=要跌),
    'reason': str (人類可讀說明),
  }
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("stock001.signal_predictor")


# EMA 平滑係數
def _alpha(period: int) -> float:
    return 2.0 / (period + 1)


# ── T1 黃金交叉 close 預測 ─────────────────────────────────────
def predict_t1_close(
    ema20_prev: float,
    ema60_prev: float,
) -> Optional[float]:
    """求今日 close 使 EMA20_today == EMA60_today（即「剛好」金叉）。

    EMA recurrence:
        ema_t = (1-α) * ema_{t-1} + α * close_t
    where α = 2/(n+1)

    Set ema20_t = ema60_t:
        (1-α20)*ema20_prev + α20*close = (1-α60)*ema60_prev + α60*close
        close * (α20 - α60) = (1-α60)*ema60_prev - (1-α20)*ema20_prev
        close = [(1-α60)*ema60_prev - (1-α20)*ema20_prev] / (α20 - α60)

    Args:
        ema20_prev: 昨日收盤後 EMA20
        ema60_prev: 昨日收盤後 EMA60

    Returns:
        threshold close；若計算失敗回 None
    """
    if ema20_prev is None or ema60_prev is None:
        return None
    try:
        a20 = _alpha(20)
        a60 = _alpha(60)
        denom = a20 - a60   # ≈ 0.0624
        if abs(denom) < 1e-9:
            return None
        return ((1 - a60) * ema60_prev - (1 - a20) * ema20_prev) / denom
    except Exception as exc:
        log.debug("[predict_t1_close] %s", exc)
        return None


# ── RSI = target 的 close 預測（Wilder 14 平滑）───────────────
def predict_rsi_close(
    close_series,
    target_rsi: float = 50.0,
    period: int = 14,
) -> Optional[float]:
    """求今日 close 使 RSI（Wilder 14 平滑）等於 target_rsi。

    Wilder smoothing:
        avg_gain_t = (avg_gain_{t-1} * (n-1) + gain_t) / n
        avg_loss_t = (avg_loss_{t-1} * (n-1) + loss_t) / n
    RSI = 100 - 100/(1 + avg_gain/avg_loss)

    若 close 漲 X (X≥0): gain=X, loss=0
    若 close 跌 X (X≥0): gain=0, loss=X

    對 target_rs = target_rsi/(100-target_rsi):
        Case 1 (跌): new_gain/new_loss = target_rs
            ((n-1)/n * G) / ((n-1)/n * L + X/n) = target_rs
            X_drop = (n-1) * (G - target_rs * L) / target_rs
        Case 2 (漲): X_rise = (n-1) * (target_rs * L - G)

    Args:
        close_series: 至少 period+1 天的 close (用來算 avg_gain/avg_loss)
        target_rsi: 目標 RSI（預設 50 = T3 拉回門檻）
        period: Wilder 平滑視窗（預設 14）

    Returns:
        該 close；計算失敗回 None
    """
    if close_series is None:
        return None
    try:
        if isinstance(close_series, pd.Series):
            closes = close_series.dropna().values
        else:
            arr = np.asarray(close_series, dtype=float)
            closes = arr[~np.isnan(arr)]
        if len(closes) < period + 1:
            return None
        # 算到「昨天」為止的 Wilder avg_gain / avg_loss
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        # 種子：前 period 個 delta 的簡單平均
        avg_gain = float(gains[:period].mean())
        avg_loss = float(losses[:period].mean())
        # 逐日 Wilder 推到最後一個 delta（= 昨日相對前天）
        for i in range(period, len(gains)):
            avg_gain = ((period - 1) * avg_gain + gains[i]) / period
            avg_loss = ((period - 1) * avg_loss + losses[i]) / period

        # 此時 avg_gain / avg_loss 已是「昨日收盤後」的值
        # 求今日 close → target_rsi
        if target_rsi >= 100 or target_rsi <= 0:
            return None
        target_rs = target_rsi / (100.0 - target_rsi)
        prev_close = float(closes[-1])  # 昨日收盤

        # 哪個方向：current RSI vs target
        # 若 current RS > target_rs → 需要跌; 反之需要漲
        # current_rs = avg_gain / max(avg_loss, 1e-9)
        # 算兩種方向，取 makes sense 的那個（正 X）

        n = period
        # Case 1: drop X (gain=0, loss=X)
        if target_rs > 0:
            x_drop = (n - 1) * (avg_gain - target_rs * avg_loss) / target_rs
        else:
            x_drop = None
        # Case 2: rise X (gain=X, loss=0)
        x_rise = (n - 1) * (target_rs * avg_loss - avg_gain)

        # 選正的那個（負代表反方向）
        if x_drop is not None and x_drop > 0:
            return prev_close - x_drop
        if x_rise > 0:
            return prev_close + x_rise
        # 邊界：已等於 target
        return prev_close
    except Exception as exc:
        log.debug("[predict_rsi_close/%s] %s", target_rsi, exc)
        return None


# ── 端到端：對 d dict 算 3 個訊號的 trigger info ────────────────
def predict_signal_triggers(d: dict) -> dict:
    """從 fetch_indicators 的 d 算出 T1/T3/T4 各自的觸發 close 預測。

    Returns:
        {
            't1': {status, target_close, target_pct, reason},
            't3': {status, target_close, target_pct, reason},
            't4': {status, target_close, target_pct, reason},
        }

    status 值:
      'triggered'      已觸發（今日已滿足）
      'reachable'      未觸發但今天有機會（有 target_close）
      'not_applicable' 不適用（前置條件不滿足，例如 T4 在多頭）
      'unknown'        無資料
    """
    out = {k: {'status': 'unknown', 'target_close': None,
               'target_pct': None, 'reason': ''}
           for k in ('t1', 't3', 't4')}
    if not isinstance(d, dict):
        return out

    close   = d.get('close')
    ema20   = d.get('ema20')
    ema60   = d.get('ema60')
    rsi     = d.get('rsi')
    adx     = d.get('adx')
    adx_th  = d.get('adx_th', 22)
    t3_rsi  = 50  # T3 RSI 上限
    t4_rsi  = d.get('t4_rsi', 35)
    cross_d = d.get('ema20_cross_days')
    rsi_prev  = d.get('rsi_prev')
    rsi_prev2 = d.get('rsi_prev2')
    sh = d.get('_swing_history') or {}
    close_series = sh.get('close') or []

    # ── 反推「昨天」EMA: ema_prev 大致等於 ema 現值的「來自昨日收盤」狀態
    # 我們手邊只有 ema 的當前值（基於今日收盤的話），但 fetch_indicators
    # 的 ema 是基於最新 close 算的（即「今日」收盤後）。在這裡我們假設
    # 今天還在盤中，預測「今日收盤」要多少；輸入的 ema 已是「昨日收盤後」。
    # 這在實務上跟「dataframe 上 latest bar 為昨日」一致。
    is_bull = (ema20 is not None and ema60 is not None and ema20 > ema60)

    # ── T1 黃金交叉 ──
    # 情境:
    #   A. cross_days > 0 (≤10): 最近剛金叉 → triggered（fresh）
    #   B. cross_days > 10: 多頭多時 → past_event（不在 T1 視窗內，但已多頭）
    #   C. ema20 > ema60 但無 cross_days: 算是多頭 → past_event
    #   D. ema20 ≤ ema60 (死叉): reachable，需 close 上漲到 cross threshold
    if cross_d is not None and 0 < cross_d <= 10:
        out['t1'] = {'status': 'triggered', 'target_close': None,
                     'target_pct': None,
                     'reason': f'已黃金交叉 {cross_d} 天 (T1 視窗內)'}
    elif cross_d is not None and cross_d > 10:
        out['t1'] = {'status': 'past_event', 'target_close': None,
                     'target_pct': None,
                     'reason': f'已黃金交叉 {cross_d} 天 (超過 T1 視窗)'}
    elif is_bull:  # ema20 > ema60 但 cross_days 缺
        out['t1'] = {'status': 'past_event', 'target_close': None,
                     'target_pct': None,
                     'reason': 'EMA20>EMA60 (已在多頭排列)'}
    elif ema20 is not None and ema60 is not None and close is not None:
        # 死叉狀態 → 預測金叉所需 close
        tgt = predict_t1_close(ema20, ema60)
        if tgt is not None and close > 0 and tgt > 0:
            pct = (tgt - close) / close * 100
            out['t1'] = {
                'status': 'reachable',
                'target_close': float(tgt),
                'target_pct': float(pct),
                'reason': f'EMA20={ema20:.2f}<EMA60={ema60:.2f}',
            }
        else:
            out['t1'] = {'status': 'unknown', 'target_close': None,
                         'target_pct': None,
                         'reason': '無法計算 EMA 交叉 close'}

    # ── T3 多頭拉回 ──
    # 前置：多頭 + ADX OK；觸發：RSI < t3_rsi
    if not is_bull:
        out['t3'] = {'status': 'not_applicable', 'target_close': None,
                     'target_pct': None,
                     'reason': 'EMA20≤EMA60 (空頭, T3 不適用)'}
    elif adx is not None and adx < adx_th:
        out['t3'] = {'status': 'not_applicable', 'target_close': None,
                     'target_pct': None,
                     'reason': f'ADX {adx:.1f}<{adx_th} (趨勢不足)'}
    elif rsi is not None and rsi < t3_rsi:
        out['t3'] = {'status': 'triggered', 'target_close': None,
                     'target_pct': None,
                     'reason': f'RSI {rsi:.1f}<{t3_rsi}'}
    else:
        # 預測 RSI=t3_rsi 所需 close
        tgt = predict_rsi_close(close_series, target_rsi=t3_rsi)
        if tgt is not None and close and close > 0:
            pct = (tgt - close) / close * 100
            out['t3'] = {
                'status': 'reachable',
                'target_close': float(tgt),
                'target_pct': float(pct),
                'reason': f'RSI {rsi:.1f}→{t3_rsi}',
            }
        else:
            out['t3'] = {'status': 'unknown', 'target_close': None,
                         'target_pct': None,
                         'reason': 'RSI 預測失敗'}

    # ── T4 空頭反彈 ──
    # 前置：空頭；觸發：RSI < t4_rsi AND rising
    if is_bull:
        out['t4'] = {'status': 'not_applicable', 'target_close': None,
                     'target_pct': None,
                     'reason': 'EMA20>EMA60 (多頭, T4 不適用)'}
    elif (rsi is not None and rsi < t4_rsi
          and rsi_prev is not None and rsi > rsi_prev
          and rsi_prev2 is not None and rsi_prev > rsi_prev2):
        out['t4'] = {'status': 'triggered', 'target_close': None,
                     'target_pct': None,
                     'reason': f'RSI {rsi:.1f}<{t4_rsi} 且連 2 日上升'}
    else:
        # 預測 RSI=t4_rsi 所需 close（不考慮上升條件；那需要連 3 日資料）
        tgt = predict_rsi_close(close_series, target_rsi=t4_rsi)
        # 加註上升條件
        rising = (rsi_prev is not None and rsi_prev2 is not None
                  and rsi_prev > rsi_prev2) if rsi is not None else False
        if tgt is not None and close and close > 0:
            pct = (tgt - close) / close * 100
            note = ('RSI 已上升趨勢' if rising
                    else 'RSI 尚未確立上升 (T4 觸發需 ≥2 日連升)')
            out['t4'] = {
                'status': 'reachable',
                'target_close': float(tgt),
                'target_pct': float(pct),
                'reason': f'RSI {rsi:.1f}→{t4_rsi}；{note}',
            }
        else:
            out['t4'] = {'status': 'unknown', 'target_close': None,
                         'target_pct': None,
                         'reason': 'RSI 預測失敗'}

    return out
