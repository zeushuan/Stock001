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

    out.update({
        'ok': True,
        'bars': len(df),
        'last_ts': df.index[last_idx],
        'close': round(close, 4),
    })

    # EMA / RSI / ADX
    try:
        e10 = df.get('e10').iloc[last_idx] if 'e10' in df else np.nan
        e20 = df.get('e20').iloc[last_idx] if 'e20' in df else np.nan
        e60 = df.get('e60').iloc[last_idx] if 'e60' in df else np.nan
        rsi = df.get('rsi').iloc[last_idx] if 'rsi' in df else np.nan
        adx = df.get('adx').iloc[last_idx] if 'adx' in df else np.nan
        is_bull = bool(not np.isnan(e20) and not np.isnan(e60) and e20 > e60)
        is_bear = bool(not np.isnan(e20) and not np.isnan(e60) and e20 < e60)
        out.update({
            'ema10': round(e10, 4) if not np.isnan(e10) else None,
            'ema20': round(e20, 4) if not np.isnan(e20) else None,
            'ema60': round(e60, 4) if not np.isnan(e60) else None,
            'rsi': round(rsi, 1) if not np.isnan(rsi) else None,
            'adx': round(adx, 1) if not np.isnan(adx) else None,
            'is_bull': is_bull,
            'is_bear': is_bear,
            'trend_emoji': _bull_bear_emoji(is_bull, is_bear),
        })
    except Exception:
        pass

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
