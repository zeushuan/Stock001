"""Intraday Indicators — Stock001 v9.29
========================================

Intraday-specific 指標：
  - vwap_session(df)        : 每日 reset 的 session VWAP
  - anchored_vwap(df, ts)   : 從 anchor 點開始的 anchored VWAP
  - orb_levels(df, minutes) : Opening Range Breakout 上下軌
  - floor_pivots(df)        : 經典 floor pivot points（P/R1/S1/R2/S2/R3/S3）
  - gap_metrics(df)         : 跳空缺口分析（gap%、is_filled）
  - relative_volume(df)     : 同時段相對量（intraday seasonality）

標準指標（EMA/RSI/ADX/BB/ATR）直接呼叫 ta 套件即可，不在此 module。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional, Tuple, List


# ─── VWAP ────────────────────────────────────────────────────────

def vwap_session(df: pd.DataFrame) -> pd.Series:
    """每日 session 都會 reset 的 VWAP

    VWAP = cumulative(Typical_Price * Volume) / cumulative(Volume)
    Typical Price = (H + L + C) / 3

    每個交易日（依 df.index.date）獨立累積。
    """
    if df is None or len(df) == 0:
        return pd.Series(dtype=float)
    typical = (df['High'] + df['Low'] + df['Close']) / 3.0
    pv = typical * df['Volume']
    grp = df.index.date
    cum_pv = pd.Series(pv.values, index=df.index).groupby(grp).cumsum()
    cum_v = pd.Series(df['Volume'].values, index=df.index).groupby(grp).cumsum()
    # 避免除以零
    vwap = cum_pv / cum_v.replace(0, np.nan)
    return vwap


def anchored_vwap(df: pd.DataFrame, anchor: pd.Timestamp) -> pd.Series:
    """從 anchor 時間點開始的 anchored VWAP

    用途：從事件點（earnings、突破日）算起的 VWAP，
    觀察價格相對於這個 anchor 的多空力量。
    """
    if df is None or len(df) == 0:
        return pd.Series(dtype=float)
    mask = df.index >= anchor
    if not mask.any():
        return pd.Series(np.nan, index=df.index)
    sub = df[mask]
    typical = (sub['High'] + sub['Low'] + sub['Close']) / 3.0
    pv = (typical * sub['Volume']).cumsum()
    cv = sub['Volume'].cumsum().replace(0, np.nan)
    out = pd.Series(np.nan, index=df.index)
    out.loc[sub.index] = (pv / cv).values
    return out


# ─── Opening Range Breakout (ORB) ────────────────────────────────

def orb_levels(df: pd.DataFrame, minutes: int = 30,
                tf_minutes: int = 5) -> dict:
    """計算最近一個交易日的 ORB（Opening Range Breakout）上下軌

    Args:
        df: intraday DataFrame
        minutes: opening range 多長（預設 30 分鐘）
        tf_minutes: 每根 bar 多少分鐘（需配合 timeframe）

    Returns:
        {
          'or_high': float, 'or_low': float,
          'or_start': Timestamp, 'or_end': Timestamp,
          'bars_used': int,
          'breakout_up': bool,         # 收盤已突破上軌
          'breakout_down': bool,       # 收盤已跌破下軌
          'current_position': str,     # 'above_high' / 'inside' / 'below_low'
        }
    """
    out = {'or_high': None, 'or_low': None, 'or_start': None, 'or_end': None,
            'bars_used': 0, 'breakout_up': False, 'breakout_down': False,
            'current_position': 'unknown'}
    if df is None or len(df) == 0:
        return out
    last_date = df.index[-1].date()
    today = df[df.index.date == last_date]
    if len(today) == 0:
        return out

    n_bars = max(1, minutes // max(tf_minutes, 1))
    or_window = today.iloc[:n_bars]
    if len(or_window) == 0:
        return out

    or_high = float(or_window['High'].max())
    or_low = float(or_window['Low'].min())
    out.update({
        'or_high': or_high, 'or_low': or_low,
        'or_start': or_window.index[0], 'or_end': or_window.index[-1],
        'bars_used': len(or_window),
    })

    if len(today) > len(or_window):
        last_close = float(today['Close'].iloc[-1])
        out['breakout_up'] = last_close > or_high
        out['breakout_down'] = last_close < or_low
        if last_close > or_high:
            out['current_position'] = 'above_high'
        elif last_close < or_low:
            out['current_position'] = 'below_low'
        else:
            out['current_position'] = 'inside'
    return out


# ─── Floor Pivot Points ──────────────────────────────────────────

def floor_pivots(prev_high: float, prev_low: float,
                  prev_close: float) -> dict:
    """經典 floor pivot points（依前一日 HLC）

    P  = (H + L + C) / 3
    R1 = 2P - L              S1 = 2P - H
    R2 = P + (H - L)         S2 = P - (H - L)
    R3 = H + 2(P - L)        S3 = L - 2(H - P)
    """
    if any(np.isnan([prev_high, prev_low, prev_close])):
        return {}
    p = (prev_high + prev_low + prev_close) / 3.0
    rng = prev_high - prev_low
    return {
        'P': round(p, 4),
        'R1': round(2*p - prev_low, 4),
        'S1': round(2*p - prev_high, 4),
        'R2': round(p + rng, 4),
        'S2': round(p - rng, 4),
        'R3': round(prev_high + 2*(p - prev_low), 4),
        'S3': round(prev_low - 2*(prev_high - p), 4),
    }


def floor_pivots_from_df(df: pd.DataFrame) -> dict:
    """從 intraday DataFrame 自動取前一日 HLC 計算 pivots"""
    if df is None or len(df) == 0:
        return {}
    last_date = df.index[-1].date()
    prev = df[df.index.date < last_date]
    if len(prev) == 0:
        return {}
    last_prev_date = prev.index[-1].date()
    prev_day = prev[prev.index.date == last_prev_date]
    if len(prev_day) == 0:
        return {}
    return floor_pivots(
        prev_high=float(prev_day['High'].max()),
        prev_low=float(prev_day['Low'].min()),
        prev_close=float(prev_day['Close'].iloc[-1]),
    )


# ─── Gap Analysis ────────────────────────────────────────────────

def gap_metrics(df: pd.DataFrame) -> dict:
    """跳空缺口分析（從最近一個交易日的開盤跟前一日收盤對比）

    Returns:
        {
          'gap_pct': float,        # 跳空幅度 (open / prev_close - 1) × 100
          'gap_type': str,         # 'up' / 'down' / 'none'（< 0.3% 視為無）
          'is_filled': bool,       # 缺口是否已回補
          'today_open': float,
          'prev_close': float,
        }
    """
    out = {'gap_pct': 0.0, 'gap_type': 'none', 'is_filled': False,
            'today_open': None, 'prev_close': None}
    if df is None or len(df) == 0:
        return out
    last_date = df.index[-1].date()
    today = df[df.index.date == last_date]
    prev = df[df.index.date < last_date]
    if len(today) == 0 or len(prev) == 0:
        return out
    last_prev_date = prev.index[-1].date()
    prev_day = prev[prev.index.date == last_prev_date]
    if len(prev_day) == 0:
        return out

    today_open = float(today['Open'].iloc[0])
    prev_close = float(prev_day['Close'].iloc[-1])
    if prev_close <= 0:
        return out
    gap_pct = (today_open / prev_close - 1) * 100
    out.update({'gap_pct': round(gap_pct, 3),
                 'today_open': today_open, 'prev_close': prev_close})

    if gap_pct > 0.3:
        out['gap_type'] = 'up'
        # 回補：今日低點 <= 前日收盤
        out['is_filled'] = float(today['Low'].min()) <= prev_close
    elif gap_pct < -0.3:
        out['gap_type'] = 'down'
        out['is_filled'] = float(today['High'].max()) >= prev_close
    return out


# ─── Relative Volume（同時段比較）────────────────────────────────

def relative_volume(df: pd.DataFrame, lookback_sessions: int = 20) -> Optional[float]:
    """同時段相對量（intraday seasonality）

    例：現在是 10:00-10:05 的 bar，比較近 N 個交易日同時段的平均量。
    Returns: ratio（> 1 = 比平均放量）
    """
    if df is None or len(df) == 0:
        return None
    last_ts = df.index[-1]
    last_date = last_ts.date()
    minutes_of_day = last_ts.hour * 60 + last_ts.minute

    # 收集每個 session 同分鐘的 bar volume
    same_time_vols = []
    for d, day_df in df.groupby(df.index.date):
        if d == last_date:
            continue
        mins = day_df.index.hour * 60 + day_df.index.minute
        match = day_df[mins == minutes_of_day]
        if len(match) > 0:
            same_time_vols.append(float(match['Volume'].iloc[0]))
        if len(same_time_vols) >= lookback_sessions:
            break
    if not same_time_vols:
        return None
    avg = float(np.mean(same_time_vols))
    cur_vol = float(df['Volume'].iloc[-1])
    if avg <= 0:
        return None
    return round(cur_vol / avg, 2)


# ─── 標準指標套裝（包 ta 套件）───────────────────────────────────

def add_standard_indicators(df: pd.DataFrame,
                              ema_periods: tuple = (10, 20, 60),
                              rsi_period: int = 14,
                              adx_period: int = 14,
                              bb_period: int = 20,
                              atr_period: int = 14) -> pd.DataFrame:
    """為 intraday df 加上標準技術指標

    Returns df with additional columns:
        e10, e20, e60, rsi, adx, atr, bb_mid, bb_up, bb_lo, bb_pctb
    """
    if df is None or len(df) < max(ema_periods[-1], 60):
        return df
    try:
        import ta
        df = df.copy()
        for p in ema_periods:
            df[f'e{p}'] = ta.trend.ema_indicator(df['Close'], window=p)
        df['rsi'] = ta.momentum.rsi(df['Close'], window=rsi_period)
        df['adx'] = ta.trend.adx(df['High'], df['Low'], df['Close'],
                                   window=adx_period)
        df['atr'] = ta.volatility.average_true_range(
            df['High'], df['Low'], df['Close'], window=atr_period)
        bb = ta.volatility.BollingerBands(df['Close'], window=bb_period)
        df['bb_mid'] = bb.bollinger_mavg()
        df['bb_up'] = bb.bollinger_hband()
        df['bb_lo'] = bb.bollinger_lband()
        rng = (df['bb_up'] - df['bb_lo']).replace(0, np.nan)
        df['bb_pctb'] = (df['Close'] - df['bb_lo']) / rng
    except ImportError:
        print("  [intraday] ta 套件未安裝，跳過標準指標")
    return df
