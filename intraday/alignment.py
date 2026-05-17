"""MTF (Multi-Timeframe) State Engine — Stock001 v9.29
========================================================

一次計算一檔股票在多個 timeframe 上的趨勢狀態，
回傳一個彙總 dict 讓 UI 一目了然。

對外 API：
  compute_mtf_state(ticker, market='auto', timeframes=['5m','15m','1h','1d'])
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Optional, List, Dict

from intraday.config import get_tf_config, NotApplicable
from intraday.data import get_intraday, market_info
from intraday.indicators import (
    add_standard_indicators, vwap_session,
    orb_levels, floor_pivots_from_df, gap_metrics, relative_volume,
)


def _bull_bear_emoji(is_bull: bool, is_bear: bool) -> str:
    if is_bull: return '🟢'
    if is_bear: return '🔴'
    return '⚪'


def _safe(x, default=None):
    """安全取值"""
    if x is None: return default
    if isinstance(x, float) and np.isnan(x): return default
    return x


def _compute_single_tf(ticker: str, tf: str, market: str) -> Dict:
    """單一 timeframe 的狀態 dict"""
    out = {'tf': tf, 'ok': False, 'reason': '', 'bars': 0}
    df = get_intraday(ticker, tf, market)
    if df is None or len(df) < 20:
        out['reason'] = '資料不足' if df is not None else 'fetch 失敗'
        return out

    df = add_standard_indicators(df)
    cfg = get_tf_config(tf)
    last_idx = -1
    close = float(df['Close'].iloc[last_idx])
    o = float(df['Open'].iloc[last_idx])
    h = float(df['High'].iloc[last_idx])
    l = float(df['Low'].iloc[last_idx])
    vol_cur = float(df['Volume'].iloc[last_idx])

    out.update({
        'ok': True,
        'bars': len(df),
        'last_ts': df.index[last_idx],
        'close': round(close, 4),
        'open': round(o, 4), 'high': round(h, 4), 'low': round(l, 4),
        'volume': int(vol_cur),
    })

    # K 線結構（body / shadow / range）
    rng = h - l
    body = abs(close - o)
    upper_shadow = h - max(close, o)
    lower_shadow = min(close, o) - l
    is_green = close >= o
    body_pct = (body / rng * 100) if rng > 0 else 0
    out.update({
        'k_color': 'green' if is_green else 'red',
        'k_body_pct': round(body_pct, 1),    # body 佔整根 K 線比例
        'k_upper_shadow_pct': round(upper_shadow / rng * 100, 1) if rng > 0 else 0,
        'k_lower_shadow_pct': round(lower_shadow / rng * 100, 1) if rng > 0 else 0,
        'k_range': round(rng, 4),
        'k_change_pct': round((close / o - 1) * 100, 2) if o > 0 else 0,
    })

    # EMA / RSI / ADX + 各種 derived 指標
    try:
        e10 = _safe(df.get('e10').iloc[last_idx] if 'e10' in df else None)
        e20 = _safe(df.get('e20').iloc[last_idx] if 'e20' in df else None)
        e60 = _safe(df.get('e60').iloc[last_idx] if 'e60' in df else None)
        rsi = _safe(df.get('rsi').iloc[last_idx] if 'rsi' in df else None)
        adx = _safe(df.get('adx').iloc[last_idx] if 'adx' in df else None)
        atr = _safe(df.get('atr').iloc[last_idx] if 'atr' in df else None)
        bb_mid = _safe(df.get('bb_mid').iloc[last_idx] if 'bb_mid' in df else None)
        bb_up = _safe(df.get('bb_up').iloc[last_idx] if 'bb_up' in df else None)
        bb_lo = _safe(df.get('bb_lo').iloc[last_idx] if 'bb_lo' in df else None)
        bb_pctb = _safe(df.get('bb_pctb').iloc[last_idx] if 'bb_pctb' in df else None)

        # EMA slope（5 bar 前比現在）
        if e20 is not None and len(df) >= 6 and 'e20' in df:
            e20_5b = _safe(df['e20'].iloc[-6])
            e20_slope = ((e20 - e20_5b) / e20_5b * 100) if e20_5b else None
        else:
            e20_slope = None
        if e60 is not None and len(df) >= 6 and 'e60' in df:
            e60_5b = _safe(df['e60'].iloc[-6])
            e60_slope = ((e60 - e60_5b) / e60_5b * 100) if e60_5b else None
        else:
            e60_slope = None

        # ADX 趨勢
        if adx is not None and len(df) >= 6 and 'adx' in df:
            adx_5b = _safe(df['adx'].iloc[-6])
            adx_rising = (adx_5b is not None and adx > adx_5b)
        else:
            adx_rising = None

        is_bull = bool(e20 is not None and e60 is not None and e20 > e60)
        is_bear = bool(e20 is not None and e60 is not None and e20 < e60)

        # EMA 排列
        if e10 and e20 and e60:
            if e10 > e20 > e60: ema_alignment = '多頭排列 (10>20>60)'
            elif e10 < e20 < e60: ema_alignment = '空頭排列 (10<20<60)'
            elif e20 > e60: ema_alignment = '偏多 (20>60 但 10 混亂)'
            else: ema_alignment = '偏空 / 整理'
        else:
            ema_alignment = 'N/A'

        # ATR %
        atr_pct = (atr / close * 100) if (atr and close) else None

        # EMA20 乖離
        ema20_dev = ((close - e20) / e20 * 100) if e20 else None

        out.update({
            'ema10': round(e10, 4) if e10 else None,
            'ema20': round(e20, 4) if e20 else None,
            'ema60': round(e60, 4) if e60 else None,
            'ema20_slope_5b_pct': round(e20_slope, 3) if e20_slope is not None else None,
            'ema60_slope_5b_pct': round(e60_slope, 3) if e60_slope is not None else None,
            'ema_alignment': ema_alignment,
            'ema20_dev_pct': round(ema20_dev, 2) if ema20_dev is not None else None,
            'rsi': round(rsi, 1) if rsi else None,
            'adx': round(adx, 1) if adx else None,
            'adx_rising': adx_rising,
            'atr': round(atr, 4) if atr else None,
            'atr_pct': round(atr_pct, 2) if atr_pct is not None else None,
            'bb_mid': round(bb_mid, 4) if bb_mid else None,
            'bb_up': round(bb_up, 4) if bb_up else None,
            'bb_lo': round(bb_lo, 4) if bb_lo else None,
            'bb_pctb': round(bb_pctb * 100, 1) if bb_pctb is not None else None,
            'bb_width_pct': round((bb_up - bb_lo) / bb_mid * 100, 2)
                            if (bb_up and bb_lo and bb_mid) else None,
            'is_bull': is_bull,
            'is_bear': is_bear,
            'trend_emoji': _bull_bear_emoji(is_bull, is_bear),
        })

        # Price ROC (1 bar / 5 bar / 10 bar / 20 bar)
        for n in [1, 5, 10, 20]:
            if len(df) > n:
                prev = float(df['Close'].iloc[-(n+1)])
                roc = ((close / prev) - 1) * 100 if prev > 0 else 0
                out[f'roc_{n}b_pct'] = round(roc, 2)

        # Volume ratio vs MA20
        if len(df) >= 20:
            vol_ma20 = float(df['Volume'].iloc[-21:-1].mean())
            out['vol_ma20'] = int(vol_ma20)
            out['vol_ratio_ma20'] = round(vol_cur / vol_ma20, 2) if vol_ma20 > 0 else None

        # N-bar high/low distance
        for n in [20, 60]:
            if len(df) >= n:
                nbar_h = float(df['High'].iloc[-n:].max())
                nbar_l = float(df['Low'].iloc[-n:].min())
                out[f'high_{n}b'] = round(nbar_h, 4)
                out[f'low_{n}b'] = round(nbar_l, 4)
                out[f'dist_high_{n}b_pct'] = round((close / nbar_h - 1) * 100, 2) if nbar_h > 0 else None
                out[f'dist_low_{n}b_pct'] = round((close / nbar_l - 1) * 100, 2) if nbar_l > 0 else None

        # MACD (12,26,9)
        try:
            import ta
            macd_ind = ta.trend.MACD(df['Close'], window_slow=26, window_fast=12, window_sign=9)
            macd_line = _safe(macd_ind.macd().iloc[last_idx])
            macd_signal = _safe(macd_ind.macd_signal().iloc[last_idx])
            macd_hist = _safe(macd_ind.macd_diff().iloc[last_idx])
            macd_hist_prev = _safe(macd_ind.macd_diff().iloc[-2]) if len(df) > 1 else None
            out.update({
                'macd_line': round(macd_line, 4) if macd_line is not None else None,
                'macd_signal': round(macd_signal, 4) if macd_signal is not None else None,
                'macd_hist': round(macd_hist, 4) if macd_hist is not None else None,
                'macd_hist_rising': (macd_hist_prev is not None and macd_hist is not None
                                       and macd_hist > macd_hist_prev),
                'macd_above_zero': (macd_line is not None and macd_line > 0),
                'macd_bull_cross': (macd_line is not None and macd_signal is not None
                                       and macd_line > macd_signal),
            })
        except Exception:
            pass

        # Stochastic RSI / Williams %R 等補充
        try:
            import ta
            stoch = ta.momentum.StochasticOscillator(df['High'], df['Low'], df['Close'])
            stoch_k = _safe(stoch.stoch().iloc[last_idx])
            stoch_d = _safe(stoch.stoch_signal().iloc[last_idx])
            if stoch_k is not None:
                out['stoch_k'] = round(stoch_k, 1)
                out['stoch_d'] = round(stoch_d, 1) if stoch_d else None
            wr = ta.momentum.WilliamsRIndicator(df['High'], df['Low'], df['Close'])
            wr_v = _safe(wr.williams_r().iloc[last_idx])
            if wr_v is not None:
                out['williams_r'] = round(wr_v, 1)
        except Exception:
            pass

    except Exception as e:
        out['compute_err'] = str(e)[:80]

    # VWAP（只在 intraday timeframe）
    if cfg.supports_vwap_session:
        try:
            vw = vwap_session(df)
            cur_vw = float(vw.iloc[last_idx]) if not vw.empty else None
            out['vwap'] = round(cur_vw, 4) if cur_vw is not None else None
            if cur_vw is not None:
                out['vs_vwap_pct'] = round((close / cur_vw - 1) * 100, 2)
                out['above_vwap'] = close > cur_vw
        except Exception:
            pass

    # ORB（只在 intraday timeframe）
    if cfg.supports_orb:
        try:
            orb = orb_levels(df, minutes=30, tf_minutes=cfg.minutes_per_bar)
            out['orb'] = orb
        except Exception:
            pass

    # Floor pivots（任何 timeframe 都算前一日 pivot）
    if tf in ('1m', '5m', '15m', '30m', '1h'):
        try:
            out['pivots'] = floor_pivots_from_df(df)
        except Exception:
            pass

    # Gap（只在 intraday）
    if cfg.minutes_per_bar < 390:
        try:
            out['gap'] = gap_metrics(df)
        except Exception:
            pass

    # Relative volume（同時段比較，只在 intraday）
    if cfg.minutes_per_bar < 390:
        try:
            rv = relative_volume(df, lookback_sessions=20)
            out['rel_vol'] = rv
        except Exception:
            pass

    # Stage（只在 1h / 1d）
    if cfg.supports_stage:
        try:
            from intraday.patterns import classify_stage_tf
            sg = classify_stage_tf(df, tf=tf)
            out['stage'] = {
                'stage': sg.stage,
                'name': sg.stage_name,
                'sub': sg.sub_stage,
                'slope': round(sg.sma30w_slope * 100, 2),
                'price_vs_sma': round(sg.price_vs_sma30w * 100, 2),
                'confidence': round(sg.confidence, 2),
                'transitions': sg.transition_signals,
            }
        except (NotApplicable, Exception) as e:
            out['stage_skipped'] = str(e)[:80] if isinstance(e, NotApplicable) else None

    # Cup & Flat 在 15m+ 才有意義
    if cfg.supports_cup_handle and len(df) >= 200:
        try:
            from intraday.patterns import detect_cup_handle_tf
            cup = detect_cup_handle_tf(df, tf=tf)
            if cup and cup.detected:
                out['cup'] = {
                    'detected': True, 'score': cup.score,
                    'pivot': cup.pivot_price, 'target': cup.target_price,
                    'stop': cup.stop_loss, 'variant': cup.pattern_variant,
                }
        except (NotApplicable, Exception):
            pass

    if cfg.supports_flat_base and len(df) >= 200:
        try:
            from intraday.patterns import detect_flat_base_tf
            flat = detect_flat_base_tf(df, tf=tf)
            if flat and flat.detected:
                out['flat'] = {
                    'detected': True, 'score': flat.score,
                    'pivot': flat.pivot_point, 'target': flat.target_price,
                    'stop': flat.stop_loss, 'depth_pct': flat.base_depth * 100,
                    'duration': flat.base_duration_days,
                }
        except (NotApplicable, Exception):
            pass

    return out


def compute_mtf_state(ticker: str, market: str = 'auto',
                       timeframes: Optional[List[str]] = None) -> Dict:
    """多時間框架彙總

    Returns:
        {
          'ticker': str, 'market': str,
          'by_tf': {'5m': {...}, '15m': {...}, '1h': {...}, '1d': {...}},
          'summary': {
            'alignment': 'bull' / 'bear' / 'mixed',
            'mtf_bullish_count': int,    # 多少 timeframe 多頭
            'mtf_bearish_count': int,
            'best_entry_tf': str,        # 建議的進場 timeframe
          }
        }
    """
    if timeframes is None:
        timeframes = ['5m', '15m', '1h', '1d']
    info = market_info(ticker)
    out = {
        'ticker': info['ticker'],
        'market': info['market'],
        'yf_symbol': info['yf_symbol'],
        'session': info['session_hours'],
        'by_tf': {},
    }
    for tf in timeframes:
        try:
            out['by_tf'][tf] = _compute_single_tf(ticker, tf, info['market'])
        except Exception as e:
            out['by_tf'][tf] = {'tf': tf, 'ok': False,
                                 'reason': f'{type(e).__name__}: {str(e)[:60]}'}

    # 彙總
    bull = sum(1 for s in out['by_tf'].values()
                if s.get('ok') and s.get('is_bull'))
    bear = sum(1 for s in out['by_tf'].values()
                if s.get('ok') and s.get('is_bear'))
    total_ok = sum(1 for s in out['by_tf'].values() if s.get('ok'))

    if total_ok == 0:
        alignment = 'no_data'
    elif bull == total_ok:
        alignment = 'bull_aligned'
    elif bear == total_ok:
        alignment = 'bear_aligned'
    elif bull > bear:
        alignment = 'bull_leaning'
    elif bear > bull:
        alignment = 'bear_leaning'
    else:
        alignment = 'mixed'

    # best entry tf：日線多頭 + 較短 timeframe 也多頭時，建議用最短的
    best_entry = None
    if out['by_tf'].get('1d', {}).get('is_bull'):
        for tf in ['5m', '15m', '1h']:
            if out['by_tf'].get(tf, {}).get('is_bull'):
                best_entry = tf
                break

    out['summary'] = {
        'alignment': alignment,
        'mtf_bullish_count': bull,
        'mtf_bearish_count': bear,
        'total_tf': total_ok,
        'best_entry_tf': best_entry,
    }
    return out
