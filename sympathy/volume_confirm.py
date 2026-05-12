"""量能確認模組（Phase 4）

依指示書 §2.4：
- 隔日量比 ≥ 1.3 → 有效訊號（confirmed）
- 隔日量比 < 1.0 且收黑 → 訊號失效（invalidated）
- 介於兩者 → 待觀察（pending）

台股加分項（若資料可取得，目前不接 FinMind）：
- 外資買超 ≥ 1000 張 → +20%
- 投信買超 → +10%
"""
from typing import Dict, Optional
import pandas as pd
import numpy as np

from sympathy._data import load_history


CONFIRM_THRESHOLD = 1.3   # 隔日量比 ≥ 1.3
INVALIDATE_THRESHOLD = 1.0  # 量比 < 1.0 + 收黑


def confirm_volume(ticker: str,
                    signal_date,
                    check_date=None,
                    vol_avg_period: int = 20) -> Dict:
    """檢查 signal_date 隔日（或指定 check_date）的量能

    Args:
        ticker: 補漲候選 ticker
        signal_date: 訊號日（leader 大漲那天）
        check_date: 確認日（預設 signal_date 隔一個交易日）
        vol_avg_period: 計算均量的回看天數

    Returns:
      {
        'status': 'confirmed' | 'invalidated' | 'pending' | 'no_data',
        'vol_ratio': float,  # 確認日量 / 均量
        'close_color': 'red' | 'green' | 'doji',
        'check_date': YYYY-MM-DD,
        'reason': str,
      }
    """
    sig = pd.Timestamp(signal_date)
    if sig.tz is not None: sig = sig.tz_localize(None)

    df = load_history(ticker, lookback_days=vol_avg_period + 10,
                        as_of_date=sig + pd.Timedelta(days=10))
    if df is None or len(df) < vol_avg_period + 2:
        return {'status': 'no_data', 'reason': '資料不足'}

    # 取 sig 之後的 bars
    after = df.loc[sig:]
    if len(after) < 2:
        return {'status': 'no_data', 'reason': 'sig date 後無資料'}

    if check_date is None:
        confirm_bar = after.iloc[1]   # 隔日
        confirm_date = after.index[1]
    else:
        confirm_date = pd.Timestamp(check_date)
        if confirm_date.tz is not None: confirm_date = confirm_date.tz_localize(None)
        if confirm_date not in df.index:
            return {'status': 'no_data',
                     'reason': f'{confirm_date.strftime("%Y-%m-%d")} 無交易'}
        confirm_bar = df.loc[confirm_date]

    # 算 20 日均量（不含 confirm_bar）
    pre = df.loc[:confirm_date].iloc[:-1].tail(vol_avg_period)
    if len(pre) < 5:
        return {'status': 'no_data', 'reason': '均量計算資料不足'}
    avg_vol = float(pre['Volume'].mean())
    cur_vol = float(confirm_bar['Volume'])
    vol_ratio = (cur_vol / avg_vol) if avg_vol > 0 else 1.0

    # K 棒顏色
    o = float(confirm_bar['Open'])
    c = float(confirm_bar['Close'])
    if c > o * 1.001:
        color = 'green'
    elif c < o * 0.999:
        color = 'red'
    else:
        color = 'doji'

    # 判定
    if vol_ratio >= CONFIRM_THRESHOLD:
        status = 'confirmed'
        reason = f'隔日量比 {vol_ratio:.2f} ≥ 1.3 — 量能配合'
    elif vol_ratio < INVALIDATE_THRESHOLD and color == 'red':
        status = 'invalidated'
        reason = f'隔日量比 {vol_ratio:.2f} < 1.0 且收黑 — 訊號失效'
    else:
        status = 'pending'
        reason = f'隔日量比 {vol_ratio:.2f}（介於 1.0-1.3）— 待觀察'

    return {
        'status': status,
        'vol_ratio': round(vol_ratio, 2),
        'close_color': color,
        'check_date': confirm_date.strftime('%Y-%m-%d'),
        'reason': reason,
    }


def apply_to_signals(candidates: list, signal_date_key: str = 'signal_date') -> list:
    """對 sympathy candidates 批次跑量能確認（隔日）

    candidates 中每個 dict 加入 volume_confirm 子 dict
    """
    enriched = []
    for c in candidates:
        if signal_date_key not in c or 'ticker' not in c:
            enriched.append(c); continue
        try:
            v = confirm_volume(c['ticker'], c[signal_date_key])
            c2 = dict(c)
            c2['volume_confirm'] = v
            enriched.append(c2)
        except Exception:
            enriched.append(c)
    return enriched
