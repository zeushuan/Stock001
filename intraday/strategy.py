"""15m 當沖波段戰法 — Stock001 v9.33
================================================

進場進攻 / 出場防守 + EOD 強制出場（買入當天必賣）

對外 API:
  detect_swing_signal(df, market='us') -> dict
      偵測 df 最後一根 bar 的訊號（同時返回 entry + exit 訊號）

  scan_historical_signals(df, market='us', lookback_bars=180) -> list[dict]
      掃過去 N bar，找完整 entry → exit 配對交易（給歷史 marker / 統計用）
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Optional, List, Dict


# ────────────────────────────────────────────────────────────────
# 指標計算
# ────────────────────────────────────────────────────────────────

def _compute_indicators(df: pd.DataFrame) -> dict:
    """計算戰法需要的所有指標"""
    c = df['Close']; h = df['High']; l = df['Low']; v = df['Volume']
    o = df['Open']

    ind = {
        'open': o, 'high': h, 'low': l, 'close': c, 'volume': v,
        'ema5':  c.ewm(span=5, adjust=False).mean(),
        'ema20': c.ewm(span=20, adjust=False).mean(),
        'ema60': c.ewm(span=60, adjust=False).mean(),
    }
    # BB(20, 2σ) — SMA20 + 2σ
    sma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    ind['bb_mid'] = sma20
    ind['bb_upper'] = sma20 + 2 * std20
    ind['bb_lower'] = sma20 - 2 * std20

    try:
        import ta
        ind['atr'] = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
        ind['adx'] = ta.trend.ADXIndicator(h, l, c, 14).adx()
        ind['rsi'] = ta.momentum.RSIIndicator(c, 14).rsi()
    except Exception:
        ind['atr'] = pd.Series(np.nan, index=c.index)
        ind['adx'] = pd.Series(np.nan, index=c.index)
        ind['rsi'] = pd.Series(np.nan, index=c.index)
    # 🆕 v9.33：Volume = 0 (post-market quote bars) 不算進 vol_ma20，
    # 避免 regular hours 量永遠看似「未放大」
    v_nonzero = v.where(v > 0)
    ind['vol_ma20'] = v_nonzero.rolling(20, min_periods=5).mean()
    return ind


# ────────────────────────────────────────────────────────────────
# EOD 時間檢查
# ────────────────────────────────────────────────────────────────

def _eod_status(ts, market: str = 'us') -> dict:
    """檢查當前 bar 距離收盤多遠（US summer DST 預設）

    US: 09:30-16:00 ET = 13:30-20:00 UTC (summer)
    TW: 09:00-13:30 TW time
    """
    h = ts.hour; m = ts.minute
    cur_min = h * 60 + m

    if market == 'us':
        regular_open = 13 * 60 + 30      # 13:30 UTC
        regular_close = 20 * 60          # 20:00 UTC
    else:  # TW
        regular_open = 9 * 60            # 09:00
        regular_close = 13 * 60 + 30     # 13:30

    in_session = regular_open <= cur_min <= regular_close
    minutes_to_close = regular_close - cur_min if in_session else None

    return {
        'in_regular_session': in_session,
        'minutes_to_close': minutes_to_close,
        'no_entry': in_session and minutes_to_close is not None and minutes_to_close <= 60,
        'warning': in_session and minutes_to_close is not None and 15 < minutes_to_close <= 30,
        'force_exit': in_session and minutes_to_close is not None and minutes_to_close <= 15,
    }


# ────────────────────────────────────────────────────────────────
# 進場條件評估
# ────────────────────────────────────────────────────────────────

def _check_trend_filter(ind: dict, i: int) -> tuple:
    """7 個趨勢過濾條件
    Returns: (passed: bool, fails: list[str], details: dict)
    """
    fails = []
    details = {}

    if i < 20:
        return False, ['資料不足（需 ≥ 20 bar）'], {}

    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    e5  = _v('ema5')
    e20 = _v('ema20')
    e60 = _v('ema60')
    if None in (e5, e20, e60):
        return False, ['EMA indicators NaN'], {}
    details['ema5'] = e5; details['ema20'] = e20; details['ema60'] = e60

    # 1. EMA20 > EMA60 持續 ≥ 3 bar
    cnt = 0
    for j in range(i, max(-1, i - 10), -1):
        v20 = _v('ema20', j); v60 = _v('ema60', j)
        if v20 is None or v60 is None: break
        if v20 > v60: cnt += 1
        else: break
    details['ema_cross_bars'] = cnt
    if cnt < 3:
        fails.append(f'EMA20>60 持續 {cnt} bar (需 ≥3)')

    # 2. gap > 0.3%
    gap_pct = (e20 - e60) / e60 * 100 if e60 > 0 else 0
    details['ema_gap_pct'] = gap_pct
    if gap_pct <= 0.3:
        fails.append(f'EMA20/60 gap {gap_pct:+.2f}% ≤ 0.3%')

    # 3. EMA5 > EMA20 > EMA60
    if not (e5 > e20 > e60):
        fails.append(f'EMA 排列錯：E5={e5:.2f} E20={e20:.2f} E60={e60:.2f}')

    # 4. EMA20 5-bar 斜率 > 0
    if i >= 5:
        e20_5b = _v('ema20', i - 5)
        if e20_5b is not None:
            slope20 = (e20 - e20_5b) / e20_5b * 100 if e20_5b > 0 else 0
            details['ema20_slope_5b'] = slope20
            if slope20 <= 0:
                fails.append(f'EMA20 5b 斜率 {slope20:+.2f}% ≤ 0')

    # 5. EMA60 5-bar 斜率 ≥ 0
    if i >= 5:
        e60_5b = _v('ema60', i - 5)
        if e60_5b is not None:
            slope60 = (e60 - e60_5b) / e60_5b * 100 if e60_5b > 0 else 0
            details['ema60_slope_5b'] = slope60
            if slope60 < 0:
                fails.append(f'EMA60 5b 斜率 {slope60:+.2f}% < 0')

    # 6. 過去 10 bar 內 ≥ 7 bar Close > BB Mid
    cnt_above = 0
    for j in range(max(0, i - 9), i + 1):
        c = _v('close', j); m = _v('bb_mid', j)
        if c is not None and m is not None and c > m:
            cnt_above += 1
    details['close_above_mid_10b'] = cnt_above
    if cnt_above < 7:
        fails.append(f'Close>BB Mid {cnt_above}/10b (需 ≥7)')

    # 7. ADX ≥ 20
    adx = _v('adx')
    details['adx'] = adx
    if adx is None or adx < 20:
        _adx_str = f'{adx:.1f}' if adx is not None else 'NaN'
        fails.append(f'ADX {_adx_str} < 20')

    return len(fails) == 0, fails, details


def _check_entry_trigger(ind: dict, i: int) -> tuple:
    """3 種進場觸發模式（A/B/C）
    Returns: (mode: 'A'/'B'/'C'/None, reason: str, details: dict)
    """
    if i < 21: return None, '', {}

    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    c = _v('close'); h = _v('high'); l = _v('low'); o = _v('open')
    bb_mid = _v('bb_mid'); bb_upper = _v('bb_upper'); bb_lower = _v('bb_lower')
    e20 = _v('ema20')
    vol = _v('volume'); vol_ma = _v('vol_ma20')
    l_prev = _v('low', i - 1)
    atr = _v('atr')

    if None in (c, h, l, o, bb_mid, bb_upper, e20):
        return None, '指標 NaN', {}

    # 進場確認共用條件（v9.33 放寬）
    # 量：≥ 80% 均量（不必強硬放大，只要不縮量太多）
    # Volume=0 (post-market) 跳過量檢查（regular hours 才嚴格）
    vol_ok = (vol == 0 or
               (vol is not None and vol_ma is not None and vol >= vol_ma * 0.8))
    # 不創新低：容忍 0.3 × ATR 短暫探底
    low_higher = (l_prev is not None and atr is not None and
                   l >= l_prev - atr * 0.3)
    is_red = c > o

    # 模式 A：Pullback to BB Mid
    # bar Low ≤ BB Mid 且 Close > BB Mid 且 紅 K + vol_ok + low_higher
    if l <= bb_mid and c > bb_mid and is_red and vol_ok and low_higher:
        return 'A', 'BB Mid 拉回反彈', {
            'mode': 'A', 'bb_mid': bb_mid, 'close': c,
            'vol_ratio': vol / vol_ma if vol_ma else None,
        }

    # 模式 B：Pullback to EMA20
    # Close 距 EMA20 ≤ 0.3% + 紅 K 或 hammer + vol_ok + low_higher
    dist_e20_pct = abs(c - e20) / e20 * 100 if e20 > 0 else 99
    body = abs(c - o)
    lower_shadow = min(c, o) - l
    is_hammer = body > 0 and lower_shadow >= body * 2
    if dist_e20_pct <= 0.3 and (is_red or is_hammer) and vol_ok and low_higher:
        return 'B', f'EMA20 拉回（距 {dist_e20_pct:+.2f}%）', {
            'mode': 'B', 'ema20': e20, 'dist_pct': dist_e20_pct,
            'is_hammer': is_hammer, 'close': c,
        }

    # 模式 C：Squeeze Breakout
    # BB Width < 過去 60 bar 中位 + Close 突破 BB Upper + vol > 1.5x
    if i >= 60 and bb_lower is not None:
        bb_width = bb_upper - bb_lower
        # 取過去 60 bar 的 BB width
        widths = []
        for j in range(max(0, i - 60), i):
            bbu = _v('bb_upper', j); bbl = _v('bb_lower', j)
            if bbu is not None and bbl is not None:
                widths.append(bbu - bbl)
        if widths:
            median_w = np.median(widths)
            vol_ratio = vol / vol_ma if vol_ma else 1
            if bb_width < median_w and c > bb_upper and vol_ratio > 1.5:
                return 'C', f'BB 收斂後突破（width={bb_width:.3f} < med={median_w:.3f}）', {
                    'mode': 'C', 'bb_upper': bb_upper, 'close': c,
                    'vol_ratio': vol_ratio,
                }

    # 都沒觸發
    no_match_reason = []
    if not vol_ok: no_match_reason.append('量未放大')
    if not low_higher: no_match_reason.append('創新低')
    if not (l <= bb_mid and c > bb_mid): no_match_reason.append('未拉回 Mid')
    if not (dist_e20_pct <= 0.3): no_match_reason.append(f'EMA20 距 {dist_e20_pct:.2f}%')

    return None, '、'.join(no_match_reason) if no_match_reason else '無拉回', {}


# ────────────────────────────────────────────────────────────────
# 出場條件評估
# ────────────────────────────────────────────────────────────────

def _check_exit_condition(ind: dict, i: int, entry_idx: int, entry_price: float,
                           entry_stop: float, market: str = 'us') -> tuple:
    """檢查 i bar 是否該出場（v9.33: 只防守性出場，不主動停利）

    Returns: (should_exit: bool, reason: str, exit_price: float)

    出場原因（防守性）：
      🚪 EOD 強制（買入當天必賣）
      🛑 停損觸發
      🚨 急停損 -3%
      💀 死叉確認（EMA20<60 ≥ 2 bar）
      💧 Close 連 5 bar < BB Mid（趨勢轉弱）
    """
    if i <= entry_idx:
        return False, '', None

    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    c = _v('close'); l = _v('low'); h = _v('high')
    bb_mid = _v('bb_mid')
    e20 = _v('ema20'); e60 = _v('ema60')

    # 1. EOD 強制出場（最高優先）
    ts = ind['close'].index[i]
    eod = _eod_status(ts, market)
    if eod['force_exit']:
        return True, f'🚪 EOD 強制出場 (距收盤 {eod["minutes_to_close"]}min)', c

    # 2. 硬停損（防守）
    if l is not None and l <= entry_stop:
        return True, f'🛑 停損觸發 (${entry_stop:.2f})', entry_stop

    # 3. 急跌 emergency floor（防守）
    if c is not None and entry_price > 0:
        dd_pct = (c - entry_price) / entry_price * 100
        if dd_pct < -3:
            return True, f'🚨 緊急停損 (-{abs(dd_pct):.1f}%)', c

    # 4. 死亡交叉確認（趨勢反轉，直接出場 — 不等彈點不找最佳賣價）
    if e20 is not None and e60 is not None and e20 < e60:
        e20_prev = _v('ema20', i - 1)
        e60_prev = _v('ema60', i - 1)
        if e20_prev is not None and e60_prev is not None and e20_prev < e60_prev:
            return True, f'💀 死叉確認出場', c

    # 5. Close 連 5 bar < BB Mid（趨勢轉弱）
    if bb_mid is not None and c is not None and c < bb_mid:
        cnt_below = 0
        for j in range(i, max(entry_idx, i - 10), -1):
            cj = _v('close', j); mj = _v('bb_mid', j)
            if cj is not None and mj is not None and cj < mj:
                cnt_below += 1
            else:
                break
        if cnt_below >= 5:
            return True, f'💧 Close<BB Mid 連 {cnt_below}b 出場', c

    return False, '', None


# ────────────────────────────────────────────────────────────────
# 對外主 API：偵測當前 bar 訊號
# ────────────────────────────────────────────────────────────────

def detect_swing_signal(df: pd.DataFrame, market: str = 'us') -> dict:
    """偵測 df 最後一根 bar 的戰法訊號（同時返回 entry + exit）

    Returns: {
      'ts': pd.Timestamp,
      'close': float,
      'entry': {
        'triggered': bool,
        'mode': 'A'/'B'/'C'/None,
        'label': str,        # 顯示用
        'detail': str,
        'entry_price': float,
        'stop_price': float,
        'failed': [str],
      },
      'eod': {...},
      'trend_pass': bool,
    }
    """
    if df is None or len(df) < 30:
        return {'error': '資料不足 (<30 bar)'}

    ind = _compute_indicators(df)
    i = len(df) - 1
    ts = df.index[i]
    close = float(df['Close'].iloc[i])
    atr_v = ind['atr'].iloc[i]
    if pd.isna(atr_v): atr_v = close * 0.02   # fallback 2%
    atr_v = float(atr_v)

    # EOD 檢查
    eod = _eod_status(ts, market)

    # 趨勢過濾
    trend_pass, trend_fails, trend_details = _check_trend_filter(ind, i)

    # 進場觸發
    entry_mode = None
    entry_reason = ''
    entry_details = {}
    if trend_pass and not eod['no_entry']:
        entry_mode, entry_reason, entry_details = _check_entry_trigger(ind, i)

    # 計算進場價 / 停損
    if entry_mode:
        entry_price = close
        ema60 = float(ind['ema60'].iloc[i])
        stop1 = entry_price - atr_v * 1.5
        stop2 = ema60 - atr_v
        stop_price = max(stop1, stop2)
    else:
        entry_price = None
        stop_price = None

    # 出場訊號（假設「if I were holding from entry_price=close-something」— 但實際上沒有部位
    # 所以我們改成：返回「若有部位，現在會觸發什麼出場訊號」
    # 假設假進場價就是當前 close（最壞情境）以判斷出場 mode
    exit_mode = None
    exit_reason = ''
    exit_price = None
    if i >= 30:
        # 用過去 N bar 找一個合理的「if I held from there」基準
        # 簡化：假設 entry 在 10 bar 前，stop 在 -3%
        hypo_entry_idx = max(0, i - 10)
        hypo_entry_price = float(df['Close'].iloc[hypo_entry_idx])
        hypo_stop = hypo_entry_price * 0.97
        should_exit, reason, ep = _check_exit_condition(
            ind, i, hypo_entry_idx, hypo_entry_price, hypo_stop, market)
        if should_exit:
            exit_mode = reason.split()[0] if reason else 'exit'
            exit_reason = reason
            exit_price = ep

    # 組 result
    return {
        'ts': ts,
        'close': close,
        'market': market,
        'eod': eod,
        'trend_pass': trend_pass,
        'trend_fails': trend_fails,
        'trend_details': trend_details,
        'entry': {
            'triggered': entry_mode is not None,
            'mode': entry_mode,
            'label': (f'🟢 進場 {entry_mode} · {entry_reason}' if entry_mode
                      else '⚪ 無進場訊號'),
            'detail': entry_reason or '、'.join(trend_fails[:3]),
            'entry_price': entry_price,
            'stop_price': stop_price,
            'failed': trend_fails,
            'details': entry_details,
        },
        'exit': {
            'triggered': exit_mode is not None,
            'mode': exit_mode,
            'label': exit_reason or '⚪ 持有（無風險訊號）',
            'detail': exit_reason,
            'exit_price': exit_price,
        },
        'eod_status': (
            '🚪 強制出場時段' if eod['force_exit'] else
            '⏰ 警告時段（不建議進場）' if eod['warning'] else
            '❌ T-60min 不進場' if eod['no_entry'] else
            '✅ 可正常操作' if eod['in_regular_session'] else
            '🌙 非交易時段'
        ),
    }


# ────────────────────────────────────────────────────────────────
# 歷史掃描：找出完整 entry → exit 交易配對（給 chart marker 用）
# ────────────────────────────────────────────────────────────────

def scan_historical_signals(df: pd.DataFrame, market: str = 'us',
                              lookback_bars: int = 180) -> List[Dict]:
    """掃過去 N bar，找完整的 entry → exit 交易

    Returns: [{
      'entry_idx', 'entry_time', 'entry_price', 'entry_mode',
      'exit_idx', 'exit_time', 'exit_price', 'exit_reason',
      'pnl_pct', 'pnl_dollar', 'holding_bars',
    }]
    """
    if df is None or len(df) < 30:
        return []

    ind = _compute_indicators(df)
    trades = []

    # 截尾掃描範圍
    n = len(df)
    start = max(20, n - lookback_bars)

    in_position = False
    entry_idx = -1
    entry_price = 0.0
    entry_stop = 0.0
    entry_mode = ''

    for i in range(start, n):
        atr_v = ind['atr'].iloc[i]
        atr_v = float(atr_v) if not pd.isna(atr_v) else float(df['Close'].iloc[i]) * 0.02

        if not in_position:
            # 檢查 EOD 是否允許進場
            ts = df.index[i]
            eod = _eod_status(ts, market)
            if eod['no_entry']:
                continue

            # 檢查進場
            tp, _, _ = _check_trend_filter(ind, i)
            if not tp:
                continue
            mode, reason, _ = _check_entry_trigger(ind, i)
            if mode:
                in_position = True
                entry_idx = i
                entry_price = float(df['Close'].iloc[i])
                ema60_v = float(ind['ema60'].iloc[i])
                entry_stop = max(
                    entry_price - atr_v * 1.5,
                    ema60_v - atr_v,
                )
                entry_mode = mode
        else:
            # 檢查出場
            should_exit, reason, exit_p = _check_exit_condition(
                ind, i, entry_idx, entry_price, entry_stop, market)
            if should_exit:
                exit_price = float(exit_p) if exit_p else float(df['Close'].iloc[i])
                pnl_pct = (exit_price - entry_price) / entry_price * 100
                pnl_dollar = exit_price - entry_price
                trades.append({
                    'entry_idx': entry_idx,
                    'entry_time': df.index[entry_idx],
                    'entry_price': entry_price,
                    'entry_mode': entry_mode,
                    'exit_idx': i,
                    'exit_time': df.index[i],
                    'exit_price': exit_price,
                    'exit_reason': reason,
                    'pnl_pct': round(pnl_pct, 2),
                    'pnl_dollar': round(pnl_dollar, 4),
                    'holding_bars': i - entry_idx,
                })
                in_position = False

    # 若最後仍持倉，標 open（標出來但不計 P/L）
    if in_position and entry_idx >= 0:
        last_price = float(df['Close'].iloc[-1])
        trades.append({
            'entry_idx': entry_idx,
            'entry_time': df.index[entry_idx],
            'entry_price': entry_price,
            'entry_mode': entry_mode,
            'exit_idx': None,
            'exit_time': None,
            'exit_price': None,
            'exit_reason': '🟡 持倉中',
            'pnl_pct': round((last_price - entry_price) / entry_price * 100, 2),
            'pnl_dollar': round(last_price - entry_price, 4),
            'holding_bars': len(df) - 1 - entry_idx,
            'open': True,
        })

    return trades


def summarize_trades(trades: List[Dict]) -> dict:
    """計算交易統計"""
    closed = [t for t in trades if not t.get('open', False)]
    if not closed:
        return {'n': 0, 'open': len(trades)}
    wins = [t for t in closed if t['pnl_pct'] > 0]
    losses = [t for t in closed if t['pnl_pct'] <= 0]
    return {
        'n': len(closed),
        'open': len([t for t in trades if t.get('open', False)]),
        'win_n': len(wins),
        'loss_n': len(losses),
        'win_rate': round(len(wins) / len(closed) * 100, 1) if closed else 0,
        'avg_pnl_pct': round(np.mean([t['pnl_pct'] for t in closed]), 2),
        'best_pnl_pct': max(t['pnl_pct'] for t in closed),
        'worst_pnl_pct': min(t['pnl_pct'] for t in closed),
        'avg_holding_bars': round(np.mean([t['holding_bars'] for t in closed]), 1),
    }
