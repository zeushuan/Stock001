"""即時波段訊號計算（即時版）

從 daily + intraday 算出：
  - VCP 收口狀態（用日線）
  - W/M 型態（用日線）
  - 進場觸發：突破 pivot + 量增（intraday 即時驗證）
  - 出場訊號：4 主動出場 recipe（peak-2.5ATR / EMA10 連跌 / RSI 過熱 / SMA20 跌破）
  - Key levels: pivot / ATR stop / target / VWAP
"""
from typing import Dict, Optional
import pandas as pd
import numpy as np


def compute_full_signal(daily_df: pd.DataFrame,
                          intraday_df: Optional[pd.DataFrame] = None,
                          current_price: Optional[float] = None,
                          ticker: str = '?') -> Dict:
    """產出完整的盯盤訊號

    Returns:
      {
        'price': float,           # 即時或最後收
        'change_pct_today': float,
        'atr': float,
        'vcp': dict,              # VCP 偵測結果
        'w_bottom': dict,         # W 底偵測
        'm_top': dict,
        # 重點：
        'entry_signal': 'BUY' / 'WAIT' / 'OVERHEAT' / 'NO_SIGNAL',
        'exit_signal': 'HOLD' / 'PARTIAL' / 'EXIT_ALL',
        'pivot_price': float,         # VCP 整理頂 / W 頸線（突破買點）
        'stop_loss': float,           # 結構停損
        'target_price': float,        # 量度目標
        'vwap': float,                # 當日 VWAP
        'reasons': list[str],         # 訊號原因（給 UI 顯示）
      }
    """
    if daily_df is None or len(daily_df) < 30:
        return {'error': 'daily 資料不足'}

    # ─── 1. 用既有引擎算 VCP / W / M
    try:
        from double_pattern import (detect_double_bottom, detect_double_top,
                                       detect_vcp_zigzag)
        vcp_info = detect_vcp_zigzag(daily_df, lookback_days=180)
        db_info = detect_double_bottom(daily_df, lookback_days=180)
        dt_info = detect_double_top(daily_df, lookback_days=180)
    except Exception:
        vcp_info = {}; db_info = {}; dt_info = {}

    # ─── 2. 即時價 + 今日 %
    last_close = float(daily_df['Close'].iloc[-1])
    prev_close = float(daily_df['Close'].iloc[-2]) if len(daily_df) >= 2 else last_close

    if current_price is None and intraday_df is not None and len(intraday_df) > 0:
        current_price = float(intraday_df['Close'].iloc[-1])
    if current_price is None:
        current_price = last_close

    change_pct = ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0

    # ─── 3. ATR
    high = daily_df['High'].values[-15:]
    low = daily_df['Low'].values[-15:]
    close_seq = daily_df['Close'].values[-16:]
    if len(close_seq) >= 15:
        tr = np.maximum.reduce([
            high - low,
            np.abs(high - close_seq[:-1]),
            np.abs(low - close_seq[:-1]),
        ])
        atr = float(np.nanmean(tr))
    else:
        atr = 0

    # ─── 4. VWAP (intraday)
    vwap = None
    if intraday_df is not None and len(intraday_df) > 0:
        try:
            from realtime.intraday_loader import compute_vwap
            vwap_s = compute_vwap(intraday_df)
            if vwap_s is not None and len(vwap_s) > 0:
                vwap = float(vwap_s.iloc[-1])
        except Exception: pass

    # ─── 5. 關鍵價位（pivot / stop / target）
    pivot_price = None
    stop_loss = None
    target_price = None

    # VCP 有 → 用 consolidation_top 當 pivot
    if vcp_info and vcp_info.get('consolidation_top'):
        pivot_price = float(vcp_info['consolidation_top'])
        if vcp_info.get('stop_loss'):
            stop_loss = float(vcp_info['stop_loss'])

    # W 底 → 用 neckline 當 pivot
    if db_info and db_info.get('is_double_bottom'):
        if pivot_price is None:
            pivot_price = float(db_info['neckline_price'])
        if stop_loss is None:
            stop_loss = float(db_info.get('stop_loss', 0)) or None
        if db_info.get('target_price'):
            target_price = float(db_info['target_price'])

    # 預設 ATR stop（若上面沒給）
    if stop_loss is None and atr > 0:
        stop_loss = current_price - 2 * atr

    # ─── 6. 進場/出場 訊號邏輯
    reasons = []
    entry_signal = 'NO_SIGNAL'
    exit_signal = 'HOLD'

    # 進場：價格突破 pivot + 量增（intraday 驗證）
    if pivot_price is not None and current_price > pivot_price * 1.001:
        # 已突破！但要看 intraday 是否帶量
        if intraday_df is not None and len(intraday_df) >= 20:
            recent_v = float(intraday_df['Volume'].iloc[-5:].mean())
            avg_v = float(intraday_df['Volume'].iloc[:-5].mean()) if len(intraday_df) > 5 else recent_v
            vol_surge = recent_v > avg_v * 1.3 if avg_v > 0 else False
            if vol_surge:
                entry_signal = 'BUY'
                reasons.append(f'✅ 突破 pivot ${pivot_price:.2f}，5m 量比 {recent_v/avg_v:.2f}x')
            else:
                entry_signal = 'WAIT'
                reasons.append(f'⚠️ 突破 pivot ${pivot_price:.2f} 但量未跟（{recent_v/avg_v:.2f}x）')
        else:
            entry_signal = 'BUY'
            reasons.append(f'✅ 突破 pivot ${pivot_price:.2f}')
    elif pivot_price is not None and current_price < pivot_price * 0.99:
        entry_signal = 'WAIT'
        reasons.append(f'等待突破 ${pivot_price:.2f}（差 {(pivot_price-current_price)/current_price*100:.2f}%）')
    elif pivot_price is not None:
        entry_signal = 'WAIT'
        reasons.append(f'貼近 pivot ${pivot_price:.2f}（差 {(pivot_price-current_price)/current_price*100:.2f}%）')

    # 過熱檢查
    rsi = None
    if len(daily_df) > 14:
        try:
            from ta.momentum import RSIIndicator
            rsi = float(RSIIndicator(daily_df['Close'], 14).rsi().iloc[-1])
            if rsi >= 80:
                entry_signal = 'OVERHEAT'
                reasons.append(f'🔴 RSI {rsi:.0f} 過熱')
        except Exception:
            pass

    # 出場訊號（簡化版的 v9.17 主動出場 recipe）
    # E1: 跌破 SMA20
    sma20 = float(daily_df['Close'].iloc[-20:].mean()) if len(daily_df) >= 20 else None
    if sma20 is not None and current_price < sma20:
        exit_signal = 'PARTIAL'
        reasons.append(f'⚠️ 跌破 SMA20 (${sma20:.2f})')

    # E2: peak - 2.5 ATR（ATR trailing stop）
    if len(daily_df) >= 60 and atr > 0:
        peak = float(daily_df['High'].iloc[-60:].max())
        atr_stop = peak - 2.5 * atr
        if current_price < atr_stop:
            exit_signal = 'EXIT_ALL'
            reasons.append(f'🛑 跌破 ATR trailing stop (peak ${peak:.2f} - 2.5×ATR = ${atr_stop:.2f})')

    # E3: M 頂出現
    if dt_info and dt_info.get('is_double_top'):
        exit_signal = 'EXIT_ALL' if exit_signal == 'HOLD' else exit_signal
        reasons.append(f'🔴 M 頂出現 ({dt_info.get("quality_grade","")})')

    return {
        'ticker': ticker,
        'price': round(current_price, 2),
        'change_pct_today': round(change_pct, 2),
        'atr': round(atr, 2),
        'rsi': round(rsi, 1) if rsi else None,
        'sma20': round(sma20, 2) if sma20 else None,
        'vwap': round(vwap, 2) if vwap else None,
        'pivot_price': round(pivot_price, 2) if pivot_price else None,
        'stop_loss': round(stop_loss, 2) if stop_loss else None,
        'target_price': round(target_price, 2) if target_price else None,
        'entry_signal': entry_signal,
        'exit_signal': exit_signal,
        'vcp_info': vcp_info,
        'db_info': db_info,
        'dt_info': dt_info,
        'reasons': reasons,
        'last_update': pd.Timestamp.now().strftime('%H:%M:%S'),
    }
