"""波段戰法 — Stock001 v9.34
================================================

進場：3 條件 setup（黃金交叉 + EMA 全部上揚 + Close 連續站上 BB Mid）
      → 接近 BB Mid（≤ 0.5 ATR）時觸發「買點」

出場：移除 EOD 強制出場
      死叉出場改為 2 條件 setup（死叉 + Close 連續跌破 BB Mid）
      → 接近 BB Mid（≤ 0.5 ATR）時觸發「賣點」
      防守性硬停損保留（hard stop / emergency stop）

對外 API:
  detect_swing_signal(df, market='us', tf='15m') -> dict
  scan_historical_signals(df, market='us', lookback_bars=180, tf='15m') -> list[dict]
  compute_sepa_status(df) -> dict
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Optional, List, Dict


# ────────────────────────────────────────────────────────────────
# SEPA 狀態（Minervini Stage 2 Trend Template）
# ────────────────────────────────────────────────────────────────

def compute_sepa_status(df: pd.DataFrame) -> dict:
    """Minervini SEPA Stage 2 Trend Template — 7 條件評分

    說明：
      原版設計於 1d bars（SMA50/150/200 day），但這裡用 bar 數計算，
      可套用至所有 TF：
        - 1d：100% 等同原版定義（SMA200 = 200 trading days）
        - intraday：bar-based 近似（SMA200 = 200 bars，時間長度依 TF 變化）

    Returns: {
        'score': int (0-7),
        'total': 7,
        'conditions': [{'name', 'pass', 'detail'}, ...],
        'label': str (e.g. "💪 SEPA (5/7)"),
        'available': bool,
    }
    """
    if df is None or len(df) < 50:
        return {
            'score': 0, 'total': 7,
            'conditions': [],
            'label': 'SEPA (資料不足)', 'available': False,
        }

    c = df['Close']
    n = len(df)
    current = float(c.iloc[-1])

    # 三條 SMA（min_periods 兜底，避免 NaN）
    def _last(series):
        v = series.iloc[-1]
        return float(v) if not pd.isna(v) else None

    sma50 = _last(c.rolling(50, min_periods=20).mean())
    sma150 = _last(c.rolling(150, min_periods=50).mean())
    sma200 = _last(c.rolling(200, min_periods=50).mean())

    # SMA200 約 1 month 前（≈20 bars 前）
    sma200_prev = None
    if n >= 21:
        s200 = c.rolling(200, min_periods=50).mean()
        v = s200.iloc[-21]
        sma200_prev = float(v) if not pd.isna(v) else None

    # 52-week (or available range) high / low
    lookback = min(252, n)
    high_52w = float(df['High'].iloc[-lookback:].max())
    low_52w = float(df['Low'].iloc[-lookback:].min())

    conditions = []

    # 1. Price > SMA150 & SMA200
    p1 = (sma150 is not None and sma200 is not None and
          current > sma150 and current > sma200)
    conditions.append({
        'name': '1. Price > SMA150 & SMA200',
        'pass': p1,
        'detail': (f'${current:.2f} vs SMA150 ${sma150:.2f}, SMA200 ${sma200:.2f}'
                   if (sma150 and sma200) else 'N/A'),
    })

    # 2. SMA150 > SMA200
    p2 = (sma150 is not None and sma200 is not None and sma150 > sma200)
    conditions.append({
        'name': '2. SMA150 > SMA200',
        'pass': p2,
        'detail': (f'${sma150:.2f} > ${sma200:.2f}' if (sma150 and sma200) else 'N/A'),
    })

    # 3. SMA200 趨勢向上（vs 20 bar 前）
    p3 = (sma200 is not None and sma200_prev is not None and sma200 > sma200_prev)
    conditions.append({
        'name': '3. SMA200 上升 (vs 20b 前)',
        'pass': p3,
        'detail': (f'${sma200:.2f} vs ${sma200_prev:.2f}'
                   if (sma200 and sma200_prev) else 'N/A'),
    })

    # 4. SMA50 > SMA150 > SMA200
    p4 = (sma50 is not None and sma150 is not None and sma200 is not None and
          sma50 > sma150 > sma200)
    conditions.append({
        'name': '4. SMA50 > SMA150 > SMA200',
        'pass': p4,
        'detail': (f'${sma50:.2f} > ${sma150:.2f} > ${sma200:.2f}'
                   if (sma50 and sma150 and sma200) else 'N/A'),
    })

    # 5. Price > SMA50
    p5 = (sma50 is not None and current > sma50)
    conditions.append({
        'name': '5. Price > SMA50',
        'pass': p5,
        'detail': (f'${current:.2f} > ${sma50:.2f}' if sma50 else 'N/A'),
    })

    # 6. Price ≥ 30% above 52w-low
    if low_52w > 0:
        pct_above = (current - low_52w) / low_52w * 100
        p6 = pct_above >= 30
        conditions.append({
            'name': '6. 距 52w 低 ≥ 30%',
            'pass': p6,
            'detail': f'+{pct_above:.1f}% (低 ${low_52w:.2f})',
        })
    else:
        p6 = False
        conditions.append({'name': '6. 距 52w 低 ≥ 30%', 'pass': False, 'detail': 'N/A'})

    # 7. Price within 25% of 52w-high
    if high_52w > 0:
        pct_below = (high_52w - current) / high_52w * 100
        p7 = pct_below <= 25
        conditions.append({
            'name': '7. 距 52w 高 ≤ 25%',
            'pass': p7,
            'detail': f'-{pct_below:.1f}% (高 ${high_52w:.2f})',
        })
    else:
        p7 = False
        conditions.append({'name': '7. 距 52w 高 ≤ 25%', 'pass': False, 'detail': 'N/A'})

    score = sum(1 for cond in conditions if cond['pass'])

    # 每條件 pass/fail 用 🟢/🔴 表示，串接成 7 字元 dot 字串
    dots = ''.join('🟢' if cond['pass'] else '🔴' for cond in conditions)

    return {
        'score': score,
        'total': 7,
        'conditions': conditions,
        'dots': dots,
        'label': f'SEPA({score}/7){dots}',
        'available': True,
    }


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

def _eod_status(ts, market: str = 'us', tf: str = '15m') -> dict:
    """檢查當前 bar 距離收盤多遠（US summer DST 預設）

    US: 09:30-16:00 ET = 13:30-20:00 UTC (summer)
    TW: 09:00-13:30 TW time

    🆕 v9.33：tf='1d' 時 EOD 概念不適用（每根 bar 已是一個交易日）
              其他 intraday TF 都套 EOD 邏輯
    """
    # 1d 不適用 EOD（每個 bar = 一個交易日，沒有「當天必賣」概念）
    if tf == '1d':
        return {
            'in_regular_session': True,    # 概念上永遠 in session
            'minutes_to_close': None,
            'no_entry': False,
            'warning': False,
            'force_exit': False,
        }

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

    # EOD 警告 / 強制出場閾值依 TF bar 長度動態調整
    # 1m bar：T-1 min 強制（最後一根 bar）
    # 5m bar：T-5 min
    # 15m bar：T-15 min
    # 30m bar：T-30 min
    # 1h bar：T-60 min
    tf_minutes = {'1m': 1, '5m': 5, '15m': 15, '30m': 30,
                   '1h': 60, '4h': 240}.get(tf, 15)
    force_exit_threshold = tf_minutes        # 最後一根 bar
    warning_threshold = tf_minutes * 2       # 倒數第二根 bar
    no_entry_threshold = max(60, tf_minutes * 4)  # 收盤前 1 hour 不新進場

    return {
        'in_regular_session': in_session,
        'minutes_to_close': minutes_to_close,
        'no_entry': (in_session and minutes_to_close is not None and
                     minutes_to_close <= no_entry_threshold),
        'warning': (in_session and minutes_to_close is not None and
                    force_exit_threshold < minutes_to_close <= warning_threshold),
        'force_exit': (in_session and minutes_to_close is not None and
                       minutes_to_close <= force_exit_threshold),
    }


# ────────────────────────────────────────────────────────────────
# 進場條件評估（v9.34 — 雙階段：setup + buypoint）
# ────────────────────────────────────────────────────────────────

def _check_entry_setup(ind: dict, i: int) -> tuple:
    """進場 setup — 3 條件（全 AND）

    1. 明確黃金交叉 — EMA20 > EMA60 連續 ≥ 3 bar
    2. 所有 EMA 上揚 — EMA5 / EMA20 / EMA60 之 5-bar 斜率皆 > 0
    3. BB 保持中軌以上 — 最近 5 bar Close 都 > BB Mid（嚴格連續）

    Returns: (setup_ok: bool, fails: list[str], details: dict)
    """
    fails: list = []
    details: dict = {}

    if i < 20:
        return False, ['資料不足（需 ≥ 20 bar）'], {}

    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    e5 = _v('ema5'); e20 = _v('ema20'); e60 = _v('ema60')
    bb_mid = _v('bb_mid')
    if None in (e5, e20, e60, bb_mid):
        return False, ['EMA / BB 指標 NaN'], {}
    details['ema5'] = e5; details['ema20'] = e20; details['ema60'] = e60

    # 1. 明確黃金交叉 — EMA20 > EMA60 連續 ≥ 3 bar
    gc_bars = 0
    for j in range(i, max(-1, i - 100), -1):
        v20 = _v('ema20', j); v60 = _v('ema60', j)
        if v20 is None or v60 is None or v20 <= v60:
            break
        gc_bars += 1
    details['gc_bars'] = gc_bars
    if gc_bars < 3:
        fails.append(f'金叉僅 {gc_bars}b (需 ≥3)')

    # 2. 所有 EMA 上揚 — 5b 斜率 > 0
    if i >= 5:
        for ema_key, ema_label in (('ema5', 'EMA5'), ('ema20', 'EMA20'), ('ema60', 'EMA60')):
            cur = _v(ema_key); past = _v(ema_key, i - 5)
            if cur is None or past is None or past == 0:
                fails.append(f'{ema_label} 斜率不可算')
                continue
            slope = (cur - past) / past * 100
            details[f'{ema_key}_slope_5b'] = slope
            if slope <= 0:
                fails.append(f'{ema_label} 5b 斜率 {slope:+.2f}% ≤ 0')

    # 3. BB 保持中軌以上 — 最近 5 bar Close 都 > BB Mid（嚴格連續）
    above_streak = 0
    for j in range(i, max(-1, i - 5), -1):
        c = _v('close', j); m = _v('bb_mid', j)
        if c is None or m is None or c <= m:
            break
        above_streak += 1
    details['close_above_mid_streak'] = above_streak
    if above_streak < 5:
        fails.append(f'Close>BB Mid 連 {above_streak}b (需 ≥5)')

    return len(fails) == 0, fails, details


def _check_entry_buypoint(ind: dict, i: int) -> tuple:
    """買點觸發 — 條件：Close > BB Mid 且 |Close - BB Mid| ≤ 0.5 × ATR

    Returns: (is_buypoint: bool, dist_pct: float|None, dist_atr: float|None)
    """
    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    c = _v('close'); bb_mid = _v('bb_mid'); atr = _v('atr')
    if c is None or bb_mid is None or atr is None or atr <= 0 or bb_mid <= 0:
        return False, None, None

    dist = c - bb_mid              # > 0 表示在 Mid 之上（理想拉回方向）
    dist_pct = dist / bb_mid * 100
    dist_atr = abs(dist) / atr

    # 必須仍站在 Mid 之上（沒跌破），且距 Mid ≤ 0.5 ATR
    is_buy = (c > bb_mid and dist_atr <= 0.5)
    return is_buy, dist_pct, dist_atr


def _check_entry_sepa_vcp(df: pd.DataFrame, ind: dict, i: int,
                            min_sepa: int = 5) -> tuple:
    """🆕 v9.34 SEPA + VCP 進場 — Minervini 經典模式（放寬 VCP 認定）

    條件（全 AND）:
      1. SEPA score ≥ min_sepa（強趨勢確認，預設 ≥ 5/7）
      2. VCP-like 形態：
         - 至少 2 個 contractions
         - 最後一次 contraction < 第一次 contraction（明顯收縮趨勢）
         - 或：最後一次 < 過去平均（recent tightening）
      3. Pivot breakout — Close > VCP pivot_price OR within +5% of pivot

    Returns: (is_entry: bool, label: str, details: dict)
    """
    if i < 60:
        return False, '', {}

    df_now = df.iloc[:i + 1]
    if len(df_now) < 60:
        return False, '', {}

    # 1. SEPA score
    sepa = compute_sepa_status(df_now)
    if not sepa.get('available'):
        return False, '', {}
    sepa_score = sepa.get('score', 0)
    if sepa_score < min_sepa:
        return False, '', {'sepa': sepa_score, 'fail': 'sepa<min'}

    # 2. VCP-like — 放寬條件
    try:
        from sepa_vcp import detect_vcp
        vcp = detect_vcp(df_now, lookback_days=180, min_contractions=2)
    except Exception:
        return False, '', {'sepa': sepa_score, 'fail': 'vcp_err'}

    declines = vcp.get('declines_pct', [])
    n_con = vcp.get('n_contractions', 0)
    if n_con < 2 or len(declines) < 2:
        return False, '', {'sepa': sepa_score, 'n_con': n_con}

    # 放寬 1: 最後 < 第一次 (clear shrinking trend over the base)
    cond_last_lt_first = declines[-1] < declines[0]
    # 放寬 2: 最後 < 平均 (recent tightening)
    avg_prior = sum(declines[:-1]) / len(declines[:-1]) if len(declines) > 1 else 999
    cond_last_lt_avg = declines[-1] < avg_prior
    is_vcp_like = cond_last_lt_first or cond_last_lt_avg
    if not is_vcp_like:
        return False, '', {
            'sepa': sepa_score, 'n_con': n_con,
            'declines': declines, 'fail': 'not_contracting',
        }

    # 3. Pivot breakout — Close > pivot OR 接近 pivot 上緣（容忍 +5%）
    c = float(df['Close'].iloc[i])
    pivot = float(vcp.get('pivot_price', 0))
    if pivot <= 0:
        return False, '', {'sepa': sepa_score, 'fail': 'no_pivot'}
    near_pct = (c - pivot) / pivot * 100
    # 必須 Close >= pivot（突破或站在 pivot 上）且離 pivot 不超過 +5%（避免追高）
    if c < pivot or near_pct > 5:
        return False, '', {
            'sepa': sepa_score, 'n_con': n_con, 'pivot': pivot,
            'close': c, 'near_pct': near_pct, 'fail': 'no_breakout',
        }

    return True, (f'SEPA {sepa_score}/7 + VCP {n_con}c + pivot ${pivot:.2f} '
                  f'(near +{near_pct:.1f}%)'), {
        'sepa': sepa_score,
        'vcp_contractions': n_con,
        'declines_pct': declines,
        'pivot': pivot,
        'near_pivot_pct': near_pct,
        'volume_dry_up': vcp.get('volume_dry_up', False),
        'close': c,
    }


def _check_entry_breakout(ind: dict, i: int) -> tuple:
    """🆕 v9.34 突破型進場 — 補抓「拉直股」的初期突破

    條件（全 AND）：
      1. **Fresh GC** — EMA20 > EMA60 連 ≤ 2 bar（避免追老 GC 的高）
      2. **突破近 5b 高** — Close > max(High of i-5 to i-1)
      3. **Close > BB Mid** — 趨勢方向確認
      4. **EMA20 5b 斜率 > 0** — EMA20 已轉揚

    Returns: (is_breakout: bool, label: str, details: dict)
    """
    if i < 20:
        return False, '', {}

    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    c = _v('close'); bb_mid = _v('bb_mid')
    e20 = _v('ema20'); e60 = _v('ema60')
    if None in (c, bb_mid, e20, e60):
        return False, '', {}

    # 1. Fresh GC（≤ 2 bar）
    if e20 <= e60:
        return False, '', {}
    gc_bars = 0
    for j in range(i, max(-1, i - 10), -1):
        v20 = _v('ema20', j); v60 = _v('ema60', j)
        if v20 is None or v60 is None or v20 <= v60:
            break
        gc_bars += 1
    if gc_bars > 2:
        return False, '', {'gc_bars': gc_bars}

    # 2. Close > BB Mid（順勢方向）
    if c <= bb_mid:
        return False, '', {'gc_bars': gc_bars}

    # 3. Close > max(High of last 5 bars excluding today)
    highs = []
    for j in range(max(0, i - 5), i):
        v = _v('high', j)
        if v is not None:
            highs.append(v)
    if not highs:
        return False, '', {}
    max_h5 = max(highs)
    if c <= max_h5:
        return False, '', {'gc_bars': gc_bars, 'max_h5': max_h5}

    # 4. EMA20 5b 斜率 > 0
    e20_5b = _v('ema20', i - 5)
    if e20_5b is None or e20_5b <= 0:
        return False, '', {}
    slope = (e20 - e20_5b) / e20_5b * 100
    if slope <= 0:
        return False, '', {'ema20_slope_5b': slope}

    return True, f'fresh GC 突破 (GC {gc_bars}b, > 5b高 ${max_h5:.2f})', {
        'gc_bars': gc_bars,
        'breakout_high': max_h5,
        'close': c,
        'ema20_slope_5b': slope,
    }


# ────────────────────────────────────────────────────────────────
# 出場條件評估（v9.34 — 雙階段賣點 + 防守性硬停損）
# ────────────────────────────────────────────────────────────────

def _check_exit_sell_setup(ind: dict, i: int) -> tuple:
    """賣出 setup — 2 條件（全 AND）

    1. 明確死亡交叉 — EMA20 < EMA60 連續 ≥ 3 bar
    2. BB 保持中軌以下 — 最近 5 bar Close 都 < BB Mid（嚴格連續）

    Returns: (setup_ok: bool, fails: list[str], details: dict)
    """
    fails: list = []
    details: dict = {}

    if i < 20:
        return False, ['資料不足'], {}

    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    # 1. 死叉連續 ≥ 3 bar
    dc_bars = 0
    for j in range(i, max(-1, i - 100), -1):
        v20 = _v('ema20', j); v60 = _v('ema60', j)
        if v20 is None or v60 is None or v20 >= v60:
            break
        dc_bars += 1
    details['dc_bars'] = dc_bars
    if dc_bars < 3:
        fails.append(f'死叉僅 {dc_bars}b (需 ≥3)')

    # 2. Close 連續 5b < BB Mid
    below_streak = 0
    for j in range(i, max(-1, i - 5), -1):
        c = _v('close', j); m = _v('bb_mid', j)
        if c is None or m is None or c >= m:
            break
        below_streak += 1
    details['close_below_mid_streak'] = below_streak
    if below_streak < 5:
        fails.append(f'Close<BB Mid 連 {below_streak}b (需 ≥5)')

    return len(fails) == 0, fails, details


def _check_exit_sellpoint(ind: dict, i: int) -> tuple:
    """賣點觸發 — 條件：Close < BB Mid 且 |BB Mid - Close| ≤ 0.5 × ATR

    從下方反彈逼近中軌（counter-trend bounce 至阻力位）即觸發。

    Returns: (is_sellpoint: bool, dist_pct: float|None, dist_atr: float|None)
    """
    def _v(key, idx=None):
        idx = i if idx is None else idx
        try:
            x = ind[key].iloc[idx]
            return None if pd.isna(x) else float(x)
        except Exception:
            return None

    c = _v('close'); bb_mid = _v('bb_mid'); atr = _v('atr')
    if c is None or bb_mid is None or atr is None or atr <= 0 or bb_mid <= 0:
        return False, None, None

    dist = bb_mid - c              # > 0 表示在 Mid 之下（反彈方向）
    dist_pct = -dist / bb_mid * 100   # 負值表示在 Mid 下方
    dist_atr = abs(dist) / atr

    # 必須仍在 Mid 之下（沒突破），且距 Mid ≤ 0.5 ATR
    is_sell = (c < bb_mid and dist_atr <= 0.5)
    return is_sell, dist_pct, dist_atr


def _check_defensive_exit(ind: dict, i: int, entry_idx: int,
                            entry_price: float, entry_stop: float,
                            entry_kind: str = 'pullback') -> tuple:
    """防守性硬停損

    entry_kind:
      'pullback' — 拉回 BB Mid 進場，急停損 -3%
      'breakout' — fresh GC 突破進場，急停損 -7%（容忍突破後正常 pullback）

    Returns: (should_exit: bool, reason: str, exit_price: float|None)
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

    c = _v('close'); l = _v('low')

    # 1. 硬停損
    if l is not None and l <= entry_stop:
        return True, f'🛑 停損觸發 (${entry_stop:.2f})', entry_stop

    # 2. 急跌 — 依 entry_kind 調整門檻
    emergency_pct = {
        'breakout': -7.0,
        'sepa_vcp': -5.0,   # Minervini 經典 max loss
        'pullback': -3.0,
    }.get(entry_kind, -3.0)
    if c is not None and entry_price > 0:
        dd_pct = (c - entry_price) / entry_price * 100
        if dd_pct < emergency_pct:
            return True, f'🚨 緊急停損 (-{abs(dd_pct):.1f}%)', c

    return False, '', None


# ────────────────────────────────────────────────────────────────
# 對外主 API：偵測當前 bar 訊號
# ────────────────────────────────────────────────────────────────

def detect_swing_signal(df: pd.DataFrame, market: str = 'us',
                          tf: str = '15m') -> dict:
    """偵測 df 最後一根 bar 的戰法訊號（v9.34 雙階段 setup + trigger）

    進場：3 條件 setup → Close 接近 BB Mid（≤0.5 ATR）觸發買點
    出場：2 條件 sell setup → Close 反彈接近 BB Mid（≤0.5 ATR）觸發賣點

    Returns: {
        ts, close, market, sepa,
        entry: {setup_ok, triggered, state, label, fails, dist_pct, dist_atr, ...}
        exit:  {setup_ok, triggered, state, label, fails, dist_pct, dist_atr, ...}
    }
    """
    if df is None or len(df) < 30:
        return {'error': '資料不足 (<30 bar)'}

    ind = _compute_indicators(df)
    i = len(df) - 1
    ts = df.index[i]
    close = float(df['Close'].iloc[i])
    atr_v = ind['atr'].iloc[i]
    if pd.isna(atr_v): atr_v = close * 0.02
    atr_v = float(atr_v)
    bb_mid_v = ind['bb_mid'].iloc[i]
    bb_mid_v = float(bb_mid_v) if not pd.isna(bb_mid_v) else None

    # 價格格式化 helper（小數位依價位動態：< $10 用 4 位，否則 2 位）
    def _fmt(p):
        if p is None: return '-'
        return f'${p:.4f}' if p < 10 else f'${p:.2f}'

    # ── 進場：setup + buypoint ──
    setup_ok, setup_fails, setup_details = _check_entry_setup(ind, i)
    is_buy, buy_dist_pct, buy_dist_atr = _check_entry_buypoint(ind, i)

    # 建議買入價格範圍：[BB Mid, BB Mid + 0.5×ATR]
    # (中軌是理想下緣 — 不希望買在中軌以下；上緣是 buypoint trigger 的上限)
    buy_target_low = bb_mid_v if bb_mid_v else None
    buy_target_high = (bb_mid_v + atr_v * 0.5) if bb_mid_v else None

    if setup_ok and is_buy:
        entry_state = 'BUY'
        entry_label = (f'🟢 買點 · {_fmt(close)}'
                       f'（建議區 {_fmt(buy_target_low)}–{_fmt(buy_target_high)}）')
    elif setup_ok:
        entry_state = 'WAIT_BUY'
        if buy_dist_pct is not None:
            entry_label = (f'⏳ 待買點 · 目標 {_fmt(buy_target_low)}'
                           f'（距 {buy_dist_pct:+.2f}%）')
        else:
            entry_label = f'⏳ 待買點 · 目標 {_fmt(buy_target_low)}'
    else:
        entry_state = 'NO_SETUP'
        entry_label = None    # page 用 ema cross status fallback

    # 計算建議進場價 / 停損（觸發時提供）
    entry_price = None
    stop_price = None
    if entry_state == 'BUY':
        entry_price = close
        ema60 = float(ind['ema60'].iloc[i])
        stop_price = max(close - atr_v * 1.5, ema60 - atr_v)

    # ── 出場：sell setup + sellpoint ──
    sell_setup_ok, sell_fails, sell_details = _check_exit_sell_setup(ind, i)
    is_sell, sell_dist_pct, sell_dist_atr = _check_exit_sellpoint(ind, i)

    # 建議賣出價格範圍：[BB Mid - 0.5×ATR, BB Mid]
    # (中軌是反彈阻力上緣 — 不希望賣在中軌以上；下緣是 sellpoint trigger 的下限)
    sell_target_low = (bb_mid_v - atr_v * 0.5) if bb_mid_v else None
    sell_target_high = bb_mid_v if bb_mid_v else None

    if sell_setup_ok and is_sell:
        exit_state = 'SELL'
        exit_label = (f'🔴 賣點 · {_fmt(close)}'
                      f'（建議區 {_fmt(sell_target_low)}–{_fmt(sell_target_high)}）')
    elif sell_setup_ok:
        exit_state = 'WAIT_SELL'
        if sell_dist_pct is not None:
            exit_label = (f'⏳ 待賣點 · 目標 {_fmt(sell_target_high)}'
                          f'（距 {sell_dist_pct:+.2f}%）')
        else:
            exit_label = f'⏳ 待賣點 · 目標 {_fmt(sell_target_high)}'
    else:
        exit_state = 'HOLD'
        exit_label = '⚪ 持有（無風險訊號）'

    # SEPA
    sepa = compute_sepa_status(df)

    return {
        'ts': ts,
        'close': close,
        'market': market,
        'bb_mid': bb_mid_v,
        'atr': atr_v,
        'sepa': sepa,
        'entry': {
            'setup_ok': setup_ok,
            'triggered': (entry_state == 'BUY'),
            'state': entry_state,
            'label': entry_label,
            'fails': setup_fails,
            'dist_pct': buy_dist_pct,
            'dist_atr': buy_dist_atr,
            'entry_price': entry_price,
            'stop_price': stop_price,
            'target_low': buy_target_low,    # 建議買入下緣 = BB Mid
            'target_high': buy_target_high,  # 建議買入上緣 = BB Mid + 0.5 ATR
            'details': setup_details,
        },
        'exit': {
            'setup_ok': sell_setup_ok,
            'triggered': (exit_state == 'SELL'),
            'state': exit_state,
            'label': exit_label,
            'fails': sell_fails,
            'dist_pct': sell_dist_pct,
            'dist_atr': sell_dist_atr,
            'target_low': sell_target_low,    # 建議賣出下緣 = BB Mid - 0.5 ATR
            'target_high': sell_target_high,  # 建議賣出上緣 = BB Mid
            'details': sell_details,
        },
    }


# ────────────────────────────────────────────────────────────────
# 歷史掃描：找出完整 entry → exit 交易配對（給 chart marker 用）
# ────────────────────────────────────────────────────────────────

def scan_historical_signals(df: pd.DataFrame, market: str = 'us',
                              lookback_bars: int = 180,
                              tf: str = '15m') -> List[Dict]:
    """掃過去 N bar，找完整 entry→exit 交易（v9.34 雙階段邏輯）

    進場：setup 通過 + 接近中軌（buypoint）→ BUY
    出場（優先序）：
      1. 硬停損 (Low ≤ entry_stop)
      2. 急停損 (-3%)
      3. 賣點 (sell setup + sellpoint)
    """
    if df is None or len(df) < 30:
        return []

    ind = _compute_indicators(df)
    trades = []
    n = len(df)
    start = max(20, n - lookback_bars)

    in_position = False
    entry_idx = -1
    entry_price = 0.0
    entry_stop = 0.0

    for i in range(start, n):
        atr_v = ind['atr'].iloc[i]
        atr_v = float(atr_v) if not pd.isna(atr_v) else float(df['Close'].iloc[i]) * 0.02

        if not in_position:
            # 進場：setup + buypoint 都符合
            setup_ok, _, _ = _check_entry_setup(ind, i)
            if not setup_ok:
                continue
            is_buy, _, _ = _check_entry_buypoint(ind, i)
            if is_buy:
                in_position = True
                entry_idx = i
                entry_price = float(df['Close'].iloc[i])
                ema60_v = float(ind['ema60'].iloc[i])
                entry_stop = max(
                    entry_price - atr_v * 1.5,
                    ema60_v - atr_v,
                )
        else:
            # 出場優先序：硬停損 → 急停損 → 賣點
            should_exit, reason, exit_p = _check_defensive_exit(
                ind, i, entry_idx, entry_price, entry_stop)

            if not should_exit:
                sell_setup_ok, _, _ = _check_exit_sell_setup(ind, i)
                if sell_setup_ok:
                    is_sell, sp_pct, _ = _check_exit_sellpoint(ind, i)
                    if is_sell:
                        should_exit = True
                        reason = f'🔴 賣點 · 接近中軌 (距 {sp_pct:+.2f}%)'
                        exit_p = float(df['Close'].iloc[i])

            if should_exit:
                exit_price = float(exit_p) if exit_p else float(df['Close'].iloc[i])
                pnl_pct = (exit_price - entry_price) / entry_price * 100
                pnl_dollar = exit_price - entry_price
                trades.append({
                    'entry_idx': entry_idx,
                    'entry_time': df.index[entry_idx],
                    'entry_price': entry_price,
                    'entry_mode': 'BUY',
                    'exit_idx': i,
                    'exit_time': df.index[i],
                    'exit_price': exit_price,
                    'exit_reason': reason,
                    'pnl_pct': round(pnl_pct, 2),
                    'pnl_dollar': round(pnl_dollar, 4),
                    'holding_bars': i - entry_idx,
                })
                in_position = False

    # 若最後仍持倉
    if in_position and entry_idx >= 0:
        last_price = float(df['Close'].iloc[-1])
        trades.append({
            'entry_idx': entry_idx,
            'entry_time': df.index[entry_idx],
            'entry_price': entry_price,
            'entry_mode': 'BUY',
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


# ────────────────────────────────────────────────────────────────
# 🆕 v9.34：8 種候選 Exit Rule（real-time，給賣點比較回測用）
# 簽章：(df, ind, i, entry_idx, entry_price, atr_at_entry) -> (should_exit, reason, exit_price)
# ────────────────────────────────────────────────────────────────

def _exit_chandelier_3atr(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """Chandelier: highest_high_since_entry - 3 × ATR(now)"""
    if i <= entry_idx + 2:
        return False, '', None
    high_since = float(df['High'].iloc[entry_idx:i + 1].max())
    atr_v = ind['atr'].iloc[i]
    atr_v = float(atr_v) if not pd.isna(atr_v) else atr_at_entry
    trail = high_since - 3 * atr_v
    lo = float(df['Low'].iloc[i])
    if lo <= trail:
        return True, f'🪜 Chandelier 3ATR (high ${high_since:.2f} - 3×ATR)', trail
    return False, '', None


def _exit_ema20_trail(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """Close 連 2 bar < EMA20"""
    if i <= entry_idx + 1:
        return False, '', None
    e20_now = ind['ema20'].iloc[i]
    e20_prev = ind['ema20'].iloc[i - 1]
    c_now = float(df['Close'].iloc[i])
    c_prev = float(df['Close'].iloc[i - 1])
    if pd.isna(e20_now) or pd.isna(e20_prev):
        return False, '', None
    if c_now < float(e20_now) and c_prev < float(e20_prev):
        return True, f'📉 EMA20 break 連 2b', c_now
    return False, '', None


def _exit_bb_upper_reject(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """High ≥ BB_Upper AND Close < BB_Upper（摸到上軌但無法守住）"""
    if i <= entry_idx + 2:
        return False, '', None
    bb_u = ind['bb_upper'].iloc[i]
    if pd.isna(bb_u): return False, '', None
    h = float(df['High'].iloc[i])
    c = float(df['Close'].iloc[i])
    bb_u = float(bb_u)
    if h >= bb_u and c < bb_u:
        return True, f'🚫 BB Upper Reject (摸 {h:.2f} > U {bb_u:.2f})', c
    return False, '', None


def _exit_climax_reverse(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """量爆 + 寬幅 + 紅變黑：vol > 2×MA20 + range > 1.5×ATR + Close < Open"""
    if i <= entry_idx + 5:
        return False, '', None
    vol = float(df['Volume'].iloc[i] or 0)
    vol_ma = ind['vol_ma20'].iloc[i]
    if pd.isna(vol_ma) or vol_ma <= 0: return False, '', None
    o = float(df['Open'].iloc[i]); c = float(df['Close'].iloc[i])
    h = float(df['High'].iloc[i]); l = float(df['Low'].iloc[i])
    atr_v = ind['atr'].iloc[i]
    if pd.isna(atr_v) or atr_v <= 0: return False, '', None
    rng = h - l
    if vol > float(vol_ma) * 2 and rng > float(atr_v) * 1.5 and c < o:
        vr = vol / float(vol_ma); rr = rng / float(atr_v)
        return True, f'⚡ Climax Reverse (vol {vr:.1f}×, range {rr:.1f}ATR, 紅變黑)', c
    return False, '', None


def _exit_death_cross(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """EMA20 < EMA60 連 2 bar"""
    if i <= entry_idx + 1:
        return False, '', None
    e20_n = ind['ema20'].iloc[i]; e60_n = ind['ema60'].iloc[i]
    e20_p = ind['ema20'].iloc[i - 1]; e60_p = ind['ema60'].iloc[i - 1]
    if pd.isna(e20_n) or pd.isna(e60_n) or pd.isna(e20_p) or pd.isna(e60_p):
        return False, '', None
    if float(e20_n) < float(e60_n) and float(e20_p) < float(e60_p):
        return True, f'💀 死叉確認 (EMA20<60 連 2b)', float(df['Close'].iloc[i])
    return False, '', None


def _exit_sma40_break(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """Close < SMA40 (≈ 8 週 on 1d) — Minervini 經典停損線"""
    if i < 40 or i <= entry_idx + 1: return False, '', None
    sma40 = float(df['Close'].iloc[i - 39:i + 1].mean())
    c = float(df['Close'].iloc[i])
    if c < sma40:
        return True, f'📊 SMA40 break (${c:.2f} < SMA40 ${sma40:.2f})', c
    return False, '', None


def _exit_time_stop_30(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """強制持倉 ≤ 30 bar 後出場"""
    if i - entry_idx >= 30:
        c = float(df['Close'].iloc[i])
        return True, f'⏰ Time stop 30b', c
    return False, '', None


def _exit_hybrid(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """組合：Chandelier OR EMA20 break OR Climax Reverse 任一觸發"""
    for fn, _ in (
        (_exit_chandelier_3atr, 'chandelier'),
        (_exit_ema20_trail, 'ema20'),
        (_exit_climax_reverse, 'climax'),
    ):
        r = fn(df, ind, i, entry_idx, entry_price, atr_at_entry)
        if r[0]:
            return r
    return False, '', None


def _exit_time30_or_sma40(df, ind, i, entry_idx, entry_price, atr_at_entry):
    """🆕 v9.34 推薦組合：
       SMA40 跌破（提前出，保護獲利）OR 30 bar 強制出（時間頂）

    邏輯：
      - 順勢段：陪伴持倉到 30 bar 然後強制出（抓波段尾段）
      - 趨勢翻空：Close < SMA40 提前出（不等回吐太多）
    """
    # 1. SMA40 break (priority — 趨勢翻空優先)
    r = _exit_sma40_break(df, ind, i, entry_idx, entry_price, atr_at_entry)
    if r[0]:
        return r
    # 2. 30 bar time stop
    r = _exit_time_stop_30(df, ind, i, entry_idx, entry_price, atr_at_entry)
    if r[0]:
        return r
    return False, '', None


EXIT_RULES = {
    'chandelier_3atr':  _exit_chandelier_3atr,
    'ema20_trail':      _exit_ema20_trail,
    'bb_upper_reject':  _exit_bb_upper_reject,
    'climax_reverse':   _exit_climax_reverse,
    'death_cross':      _exit_death_cross,
    'sma40_break':      _exit_sma40_break,
    'time_stop_30':     _exit_time_stop_30,
    'hybrid':           _exit_hybrid,
    'time30_or_sma40':  _exit_time30_or_sma40,
}


def scan_with_exit_rule(df: pd.DataFrame, market: str = 'us',
                          lookback_bars: int = 252, tf: str = '1d',
                          exit_rule: str = 'chandelier_3atr',
                          entry_mode: str = 'both') -> List[Dict]:
    """通用回測 — 同 setup+buypoint 進場邏輯，但 exit 規則可選

    entry_mode:
      'pullback'  — 只用拉回 BB Mid 買點
      'breakout'  — 只用 fresh GC 突破買點
      'sepa_vcp'  — SEPA≥5 + VCP + pivot 突破（Minervini 經典）
      'both'      — pullback OR breakout
      'all'       — pullback OR breakout OR sepa_vcp（🆕 v9.34 強烈推薦）

    支援 exit_rule:
      'lookforward_swing'  — look-forward swing high midpoint（作弊上限）
      'chandelier_3atr'    — Chandelier trailing stop
      'ema20_trail'        — Close 連 2b < EMA20
      'bb_upper_reject'    — High ≥ BB Upper 且 Close < BB Upper
      'climax_reverse'     — 量爆 + 寬幅 + 紅變黑
      'death_cross'        — EMA20 < EMA60 連 2b
      'sma40_break'        — Close < SMA40 (8w on 1d)
      'time_stop_30'       — 強制 30 bar 出
      'hybrid'             — chandelier OR ema20 OR climax 任一

    所有規則都先檢查 defensive stop (-1.5×ATR or -3%)，再檢查 exit_rule
    """
    if df is None or len(df) < 30:
        return []

    if exit_rule == 'lookforward_swing':
        return scan_swing_profit_signals(df, market=market,
                                            lookback_bars=lookback_bars, tf=tf,
                                            entry_mode=entry_mode)

    exit_fn = EXIT_RULES.get(exit_rule)
    if exit_fn is None:
        raise ValueError(f'Unknown exit_rule: {exit_rule}')

    ind = _compute_indicators(df)
    trades: list = []
    n = len(df)
    start = max(40, n - lookback_bars)
    last_exit_idx = -1

    for i in range(start, n):
        if i <= last_exit_idx:
            continue

        # 🆕 v9.34：進場 — pullback OR breakout OR sepa_vcp
        is_pullback = False
        is_breakout = False
        is_sepavcp = False
        if entry_mode in ('pullback', 'both', 'all'):
            setup_ok, _, _ = _check_entry_setup(ind, i)
            if setup_ok:
                is_pullback, _, _ = _check_entry_buypoint(ind, i)
        if not is_pullback and entry_mode in ('breakout', 'both', 'all'):
            is_breakout, _, _ = _check_entry_breakout(ind, i)
        if (not is_pullback and not is_breakout and
            entry_mode in ('sepa_vcp', 'all')):
            is_sepavcp, _, _ = _check_entry_sepa_vcp(df, ind, i)
        if not (is_pullback or is_breakout or is_sepavcp):
            continue
        if is_pullback:    entry_kind = 'pullback'
        elif is_breakout:  entry_kind = 'breakout'
        else:              entry_kind = 'sepa_vcp'

        entry_idx = i
        entry_price = float(df['Close'].iloc[i])
        atr_v = ind['atr'].iloc[i]
        atr_v = float(atr_v) if not pd.isna(atr_v) else entry_price * 0.02
        ema60_v = float(ind['ema60'].iloc[i])
        # 🆕 v9.34：依 entry_kind 設不同 stop
        if entry_kind == 'breakout':
            recent_lo = float(df['Low'].iloc[max(0, entry_idx - 5):entry_idx].min())
            entry_stop = max(entry_price - atr_v * 2.0,
                              recent_lo - atr_v * 0.3)
        elif entry_kind == 'sepa_vcp':
            # SEPA+VCP: stop 在 pivot 下方（或近期低點 - 0.5 ATR）
            recent_lo = float(df['Low'].iloc[max(0, entry_idx - 10):entry_idx].min())
            entry_stop = max(entry_price - atr_v * 1.75,
                              recent_lo - atr_v * 0.2)
        else:  # pullback
            entry_stop = max(entry_price - atr_v * 1.5, ema60_v - atr_v)

        # 出場 entry_mode 文字
        kind_tag = {'breakout': 'BO', 'sepa_vcp': 'SV',
                     'pullback': 'PB'}.get(entry_kind, 'PB')

        # 找出場
        exit_found = False
        for j in range(entry_idx + 1, n):
            # 1. defensive stop (依 entry_kind 不同停損閾值)
            should_exit, reason, exit_p = _check_defensive_exit(
                ind, j, entry_idx, entry_price, entry_stop, entry_kind=entry_kind)
            if not should_exit:
                # 2. exit_rule
                should_exit, reason, exit_p = exit_fn(
                    df, ind, j, entry_idx, entry_price, atr_v)

            if should_exit:
                exit_price = float(exit_p) if exit_p is not None else float(df['Close'].iloc[j])
                pnl_pct = (exit_price - entry_price) / entry_price * 100
                trades.append({
                    'entry_idx': entry_idx,
                    'entry_time': df.index[entry_idx],
                    'entry_price': entry_price,
                    'entry_mode': 'BUY-' + kind_tag,
                    'exit_idx': j,
                    'exit_time': df.index[j],
                    'exit_price': exit_price,
                    'exit_reason': reason,
                    'pnl_pct': round(pnl_pct, 2),
                    'pnl_dollar': round(exit_price - entry_price, 4),
                    'holding_bars': j - entry_idx,
                })
                last_exit_idx = j
                exit_found = True
                break

        # 跑到 lookback 結束未觸發 → mark open
        if not exit_found:
            last_price = float(df['Close'].iloc[-1])
            trades.append({
                'entry_idx': entry_idx,
                'entry_time': df.index[entry_idx],
                'entry_price': entry_price,
                'entry_mode': 'BUY',
                'exit_idx': None,
                'exit_time': None,
                'exit_price': None,
                'exit_reason': '🟡 持倉中（lookback 結束）',
                'pnl_pct': round((last_price - entry_price) / entry_price * 100, 2),
                'pnl_dollar': round(last_price - entry_price, 4),
                'holding_bars': n - 1 - entry_idx,
                'open': True,
            })

    return trades


def _exit_at_swing_high(trades: list, df: pd.DataFrame, ind: dict,
                          entry_idx: int, entry_price: float,
                          window_end: int) -> None:
    """[helper] 在 [entry_idx+1, window_end] 範圍找 swing high，
    出場價 = (swing_high + BB Mid at swing) / 2"""
    if window_end <= entry_idx:
        return
    window = df.iloc[entry_idx + 1:window_end + 1]
    if len(window) == 0:
        return
    swing_high = float(window['High'].max())
    swing_high_label = window['High'].idxmax()
    swing_high_pos = df.index.get_loc(swing_high_label)
    bb_mid_at = ind['bb_mid'].iloc[swing_high_pos]
    bb_mid_at = float(bb_mid_at) if not pd.isna(bb_mid_at) else None

    if bb_mid_at is None or swing_high <= entry_price:
        # 無獲利機會 — 用 window 末端 close 平倉
        last_close = float(df['Close'].iloc[window_end])
        trades.append({
            'entry_idx': entry_idx,
            'entry_time': df.index[entry_idx],
            'entry_price': entry_price,
            'entry_mode': 'BUY',
            'exit_idx': window_end,
            'exit_time': df.index[window_end],
            'exit_price': last_close,
            'exit_reason': '⌛ Window 結束（無獲利機會）',
            'pnl_pct': round((last_close - entry_price) / entry_price * 100, 2),
            'pnl_dollar': round(last_close - entry_price, 4),
            'holding_bars': window_end - entry_idx,
        })
        return

    # 出場價 = (high + bb_mid) / 2 — clamp 至 [bb_mid, swing_high]
    hypo_exit = (swing_high + bb_mid_at) / 2
    exit_price = max(bb_mid_at, min(swing_high, hypo_exit))
    trades.append({
        'entry_idx': entry_idx,
        'entry_time': df.index[entry_idx],
        'entry_price': entry_price,
        'entry_mode': 'BUY',
        'exit_idx': swing_high_pos,
        'exit_time': df.index[swing_high_pos],
        'exit_price': exit_price,
        'exit_reason': f'🎯 波段獲利 (高 ${swing_high:.2f} ~ 中 ${bb_mid_at:.2f})',
        'pnl_pct': round((exit_price - entry_price) / entry_price * 100, 2),
        'pnl_dollar': round(exit_price - entry_price, 4),
        'holding_bars': swing_high_pos - entry_idx,
    })


def scan_swing_profit_signals(df: pd.DataFrame, market: str = 'us',
                                lookback_bars: int = 180,
                                tf: str = '15m',
                                entry_mode: str = 'both') -> List[Dict]:
    """波段獲利回測 — look-forward 模擬

    entry_mode:
      'pullback'  — 只用拉回 BB Mid 買點
      'breakout'  — 只用 fresh GC 突破買點
      'both'      — 任一成立即進場（🆕 v9.34 預設，補捉 FCEL 拉直股）

    進場：戰法 buy point trigger
    出場（優先序）：
      1. 防守性停損（hard stop / -3%）
      2. Window 內找 swing high → 出場價 = (swing high + BB Mid) / 2
    """
    if df is None or len(df) < 30:
        return []

    ind = _compute_indicators(df)
    trades: list = []
    n = len(df)
    start = max(20, n - lookback_bars)

    # 持倉 window 上限
    if tf == '1d':
        max_hold_bars = 30        # 約 6 週
    else:
        from intraday.config import get_tf_config
        cfg = get_tf_config(tf)
        max_hold_bars = max(5, int(cfg.bars_per_day))

    last_exit_idx = -1

    for i in range(start, n):
        if i <= last_exit_idx:
            continue

        # 🆕 v9.34：進場 — pullback OR breakout OR sepa_vcp
        is_pullback = False
        is_breakout = False
        is_sepavcp = False
        if entry_mode in ('pullback', 'both', 'all'):
            setup_ok, _, _ = _check_entry_setup(ind, i)
            if setup_ok:
                is_pullback, _, _ = _check_entry_buypoint(ind, i)
        if not is_pullback and entry_mode in ('breakout', 'both', 'all'):
            is_breakout, _, _ = _check_entry_breakout(ind, i)
        if (not is_pullback and not is_breakout and
            entry_mode in ('sepa_vcp', 'all')):
            is_sepavcp, _, _ = _check_entry_sepa_vcp(df, ind, i)
        if not (is_pullback or is_breakout or is_sepavcp):
            continue
        if is_pullback:    entry_kind = 'pullback'
        elif is_breakout:  entry_kind = 'breakout'
        else:              entry_kind = 'sepa_vcp'

        # 入場
        entry_idx = i
        entry_price = float(df['Close'].iloc[i])
        atr_v = ind['atr'].iloc[i]
        atr_v = float(atr_v) if not pd.isna(atr_v) else entry_price * 0.02
        ema60_v = float(ind['ema60'].iloc[i])
        if entry_kind == 'breakout':
            recent_lo = float(df['Low'].iloc[max(0, entry_idx - 5):entry_idx].min())
            entry_stop = max(entry_price - atr_v * 2.0,
                              recent_lo - atr_v * 0.3)
        elif entry_kind == 'sepa_vcp':
            recent_lo = float(df['Low'].iloc[max(0, entry_idx - 10):entry_idx].min())
            entry_stop = max(entry_price - atr_v * 1.75,
                              recent_lo - atr_v * 0.2)
        else:
            entry_stop = max(entry_price - atr_v * 1.5, ema60_v - atr_v)
        entry_day = df.index[i].date() if hasattr(df.index[i], 'date') else None

        # 找出場
        stop_exited = False
        window_end_idx = min(n - 1, entry_idx + max_hold_bars)

        for j in range(entry_idx + 1, window_end_idx + 1):
            # intraday 換日 → 截斷 window
            if tf != '1d' and entry_day is not None:
                cur_day = df.index[j].date() if hasattr(df.index[j], 'date') else None
                if cur_day != entry_day:
                    window_end_idx = j - 1
                    break

            # defensive stop (依 entry_kind 不同停損閾值)
            should_exit, reason, exit_p = _check_defensive_exit(
                ind, j, entry_idx, entry_price, entry_stop, entry_kind=entry_kind)
            if should_exit:
                exit_price = float(exit_p) if exit_p else float(df['Close'].iloc[j])
                pnl_pct = (exit_price - entry_price) / entry_price * 100
                kind_tag = {'breakout': 'BO', 'sepa_vcp': 'SV',
                             'pullback': 'PB'}.get(entry_kind, 'PB')
                trades.append({
                    'entry_idx': entry_idx,
                    'entry_time': df.index[entry_idx],
                    'entry_price': entry_price,
                    'entry_mode': 'BUY-' + kind_tag,
                    'exit_idx': j,
                    'exit_time': df.index[j],
                    'exit_price': exit_price,
                    'exit_reason': reason,
                    'pnl_pct': round(pnl_pct, 2),
                    'pnl_dollar': round(exit_price - entry_price, 4),
                    'holding_bars': j - entry_idx,
                })
                last_exit_idx = j
                stop_exited = True
                break

        if not stop_exited:
            # 沒被 stop 出場 → 在 window 內找 swing high
            _exit_at_swing_high(trades, df, ind, entry_idx, entry_price, window_end_idx)
            last_exit_idx = window_end_idx

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
