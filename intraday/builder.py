"""Intraday d-dict Builder — Stock001 v9.30
=============================================

把任意 timeframe 的 OHLCV df → 跟 tv_app.fetch_indicators() 完全一樣 schema 的
`d` dict，讓既有的 detail_card_render.judge_trend / render_detail 直接吃。

關鍵設計：
  - 不下載資料（呼叫端傳 df 進來）
  - 「週線 resample」改用 BARS_PER_WEEK[tf]（5m 一週 = 390 bar 取一根）
  - 跳過 PER / margin / yfinance.info（intraday 無關）
  - RS Rating 設 None（跨 universe 比較不可即時）

對外 API：
  build_d_from_intraday(df, tf, ticker='', market='us') -> dict
"""
from __future__ import annotations

import pandas as pd
import numpy as np
import ta

from intraday.config import get_tf_config, BARS_PER_WEEK


def _bar_unit_for_tf(tf: str) -> str:
    """timeframe → 顯示用 bar 單位（'天' / '個 5m bar' / '個 1h bar' 等）"""
    units = {
        '1m':  '個 1m bar',
        '5m':  '個 5m bar',
        '15m': '個 15m bar',
        '30m': '個 30m bar',
        '1h':  '個 1h bar',
        '1d':  '天',
    }
    return units.get(tf, '個 bar')


# ────────────────────────────────────────────────────────────────
# Helper functions (從 fetch_indicators 內部複製)
# ────────────────────────────────────────────────────────────────

def _last(s: pd.Series):
    s_clean = s.dropna()
    if s_clean.empty:
        return None
    return float(s_clean.iloc[-1])


def _prev(s: pd.Series, n: int = 1):
    idx = -(n + 1)
    if len(s) >= abs(idx) and pd.notna(s.iloc[idx]):
        return float(s.iloc[idx])
    return None


def _t3_pullback_days(rsi_series: pd.Series) -> int:
    """RSI 連續 < 50 的 bar 數"""
    try:
        arr = rsi_series.dropna().values
        if len(arr) == 0 or arr[-1] >= 50:
            return 0
        cnt = 0
        for v in reversed(arr):
            if v < 50: cnt += 1
            else: break
        return cnt
    except Exception:
        return 0


def _t4_rising_days(rsi_series: pd.Series) -> int:
    """RSI < 32 且連續上升的 bar 數"""
    try:
        arr = rsi_series.dropna().values
        if len(arr) < 3 or arr[-1] >= 32:
            return 0
        cnt = 1
        for i in range(len(arr) - 1, 0, -1):
            if arr[i] > arr[i - 1] and arr[i] < 32:
                cnt += 1
            else:
                break
        return cnt
    except Exception:
        return 0


def _vwma(close: pd.Series, vol: pd.Series, window: int = 20) -> pd.Series:
    """Volume Weighted Moving Average"""
    pv = close * vol
    return pv.rolling(window).sum() / vol.rolling(window).sum().replace(0, np.nan)


def _hull_ma(close: pd.Series, window: int = 9) -> pd.Series:
    """Hull MA = WMA(2*WMA(close, n/2) - WMA(close, n), sqrt(n))"""
    if len(close) < window * 2:
        return pd.Series(np.nan, index=close.index)
    half = max(1, int(window / 2))
    sqrt_n = max(1, int(np.sqrt(window)))
    def wma(s, n):
        weights = np.arange(1, n + 1)
        return s.rolling(n).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)
    raw = 2 * wma(close, half) - wma(close, window)
    return wma(raw, sqrt_n)


def _t3_confidence(close_now, ema5_now, ema20_now, ema5_5b_ago, ema20_5b_ago):
    """5 個指標加總（每命中 1 分）"""
    score = 0
    hits = []
    try:
        if close_now is not None and ema20_now is not None and close_now > ema20_now:
            score += 1; hits.append('close>EMA20')
        if ema20_now is not None and ema20_5b_ago is not None and ema20_now > ema20_5b_ago:
            score += 1; hits.append('EMA20上升')
        e5_up = (ema5_now is not None and ema5_5b_ago is not None
                  and ema5_now > ema5_5b_ago)
        if e5_up:
            score += 1; hits.append('EMA5上升')
        if ema5_now is not None and ema20_now is not None and ema5_now > ema20_now:
            score += 1; hits.append('EMA5>EMA20')
        e20_up = (ema20_now is not None and ema20_5b_ago is not None
                  and ema20_now > ema20_5b_ago)
        if e5_up and e20_up:
            score += 1; hits.append('雙均線都升')
    except Exception:
        pass
    return score, hits


def _detect_kline_patterns(df: pd.DataFrame, lookback: int = 5) -> list:
    """K 線型態偵測 — 共用 kline_patterns module"""
    try:
        from kline_patterns import detect_recent
        try:
            atr_s = ta.volatility.AverageTrueRange(
                df['High'], df['Low'], df['Close'], 14
            ).average_true_range()
        except Exception:
            atr_s = pd.Series(np.nan, index=df.index)
        kdf = df[['Open', 'High', 'Low', 'Close']].copy()
        kdf['atr'] = atr_s
        return detect_recent(kdf, lookback=lookback)
    except Exception:
        return []


# ────────────────────────────────────────────────────────────────
# 主 builder
# ────────────────────────────────────────────────────────────────

def build_d_from_intraday(df: pd.DataFrame, tf: str = '5m',
                            ticker: str = '', market: str = 'us') -> dict:
    """從任意 timeframe 的 OHLCV df 組裝 d dict（跟 tv_app.fetch_indicators 同 schema）

    Args:
        df: OHLCV DataFrame，index = DatetimeIndex
        tf: '1m' / '5m' / '15m' / '30m' / '1h' / '1d'
        ticker: 股票代號（會放入 d.name）
        market: 'tw' / 'us'

    Returns:
        d dict，可直接餵給 detail_card_render.judge_trend / render_detail
        若資料不足則回傳 {'_error': 'rows=N'}
    """
    if df is None or len(df) < 30:
        rows = len(df) if df is not None else 0
        return {'_error': f'rows={rows}'}

    cfg = get_tf_config(tf)
    bars_per_week = BARS_PER_WEEK.get(tf, 5)

    # ── 統一欄位 ──
    try:
        c = df['Close'].astype(float)
        h = df['High'].astype(float)
        l = df['Low'].astype(float)
        o = df['Open'].astype(float)
        v = df['Volume'].astype(float)
    except Exception as e:
        return {'_error': f'columns: {e}'}

    if c.dropna().empty:
        return {'_error': 'close all NaN'}

    # ── 計算所有指標（同 fetch_indicators）──
    bb = ta.volatility.BollingerBands(c, 20, 2)
    ema13 = ta.trend.EMAIndicator(c, 13).ema_indicator()
    ichi = ta.trend.IchimokuIndicator(h, l, 9, 26, 52)
    ema5_s = ta.trend.EMAIndicator(c, 5).ema_indicator()
    ema20_s = ta.trend.EMAIndicator(c, 20).ema_indicator()
    ema60_s = ta.trend.EMAIndicator(c, 60).ema_indicator()
    sma200_s = ta.trend.SMAIndicator(c, 200).sma_indicator()
    adx_obj = ta.trend.ADXIndicator(h, l, c, 14)
    adx_s = adx_obj.adx()
    macd_obj = ta.trend.MACD(c)
    macd_hist_s = macd_obj.macd_diff()
    atr_s = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    rsi_s = ta.momentum.RSIIndicator(c, 14).rsi()
    vol_ma20_s = (ta.trend.SMAIndicator(v, 20).sma_indicator()
                   if not v.dropna().empty else pd.Series(dtype=float))

    stoch_obj = ta.momentum.StochasticOscillator(h, l, c, 14, 3)
    stoch_k_s = stoch_obj.stoch()
    stoch_d_s = stoch_obj.stoch_signal()

    ao_series = ta.momentum.AwesomeOscillatorIndicator(h, l).awesome_oscillator()
    mom_series = c - c.shift(10)

    stochrsi_obj = ta.momentum.StochRSIIndicator(c, 14, 3, 3)
    stochrsi_d_s = stochrsi_obj.stochrsi_d() * 100

    willr_s = ta.momentum.WilliamsRIndicator(h, l, c, 14).williams_r()
    bbpower_s = c - ema13

    # ── 漲跌幅 ──
    close_val = _last(c)
    prev_close_val = _prev(c)
    change_pct = ((close_val - prev_close_val) / prev_close_val * 100
                   if close_val and prev_close_val and prev_close_val != 0 else None)
    change_amt = (close_val - prev_close_val
                   if close_val is not None and prev_close_val is not None else None)

    # ── EMA20/60 黃金/死亡交叉距今 bar 數 ──
    ema20_cross_days = None
    try:
        diff_s = (ema20_s - ema60_s).dropna()
        for _k in range(1, min(len(diff_s) - 1, 120)):
            d1, d0 = diff_s.iloc[-_k], diff_s.iloc[-_k - 1]
            if pd.notna(d1) and pd.notna(d0):
                if d0 < 0 and d1 >= 0:
                    ema20_cross_days = _k; break
                elif d0 > 0 and d1 <= 0:
                    ema20_cross_days = -_k; break
    except Exception:
        pass

    # ── T1 觸發至今的變化 ──
    cross_day_close = None
    cross_change_pct = None
    if ema20_cross_days is not None:
        try:
            _k = abs(ema20_cross_days)
            if _k < len(c):
                cross_day_close = float(c.iloc[-_k - 1])
                if cross_day_close and cross_day_close > 0 and close_val is not None:
                    cross_change_pct = (close_val - cross_day_close) / cross_day_close * 100
        except Exception:
            pass

    # ── 週線結構：BARS_PER_WEEK[tf] 個 bar 取一根當「週收」──
    # 5m: 390 bar/week；1h: 32 bar/week；1d: 5 bar/week
    w_close_v = w_ma10_v = w_ma20_v = None
    try:
        if tf == '1d':
            # 日線版照原本 resample('W')
            c_tz = c.copy()
            if hasattr(c_tz.index, 'tz') and c_tz.index.tz is not None:
                c_tz.index = c_tz.index.tz_localize(None)
            wc = c_tz.resample('W').last().dropna()
        else:
            # Intraday：取每 bars_per_week 個 bar 的最後一根
            if bars_per_week > 1 and len(c) >= bars_per_week:
                # 從尾巴往前每 bars_per_week 取一根（保證最後一根落在 wc[-1]）
                # 用 step + offset 取樣
                n = len(c)
                start = (n - 1) % bars_per_week
                wc = c.iloc[start::bars_per_week].dropna()
            else:
                wc = pd.Series(dtype=float)
        if len(wc) >= 20:
            w_close_v = float(wc.iloc[-1])
            w_ma10_v = _last(ta.trend.SMAIndicator(wc, 10).sma_indicator())
            w_ma20_v = _last(ta.trend.SMAIndicator(wc, 20).sma_indicator())
    except Exception:
        pass

    # ── 組 d dict（schema 跟 tv_app.fetch_indicators 一樣）──
    d = {
        'name': ticker or '',
        'close': close_val,
        'prev_close': prev_close_val,
        'change_pct': change_pct,
        'change_amt': change_amt,
        'rsi': _last(rsi_s),
        'rsi_prev': _prev(rsi_s),
        'rsi_prev2': _prev(rsi_s, 2),
        't3_pullback_days': _t3_pullback_days(rsi_s),
        't4_rising_days': _t4_rising_days(rsi_s),
        'ema5': _last(ema5_s),
        'ema5_5d_ago': _prev(ema5_s, 5),
        'ema20_5d_ago': _prev(ema20_s, 5),
        'atr14': _last(atr_s),
        'stoch_k': _last(stoch_k_s),
        'stoch_d': _last(stoch_d_s),
        'stoch_d_prev': _prev(stoch_d_s),
        'cci': _last(ta.trend.CCIIndicator(h, l, c, 20).cci()),
        'adx': _last(adx_s),
        'adx_prev': _prev(adx_s),
        'adx_pos': _last(adx_obj.adx_pos()),
        'adx_neg': _last(adx_obj.adx_neg()),
        'macd_hist': _last(macd_hist_s),
        'macd_hist_prev': _prev(macd_hist_s),
        'volume': _last(v),
        'vol_ma20': _last(vol_ma20_s) if not vol_ma20_s.empty else None,
        'ao': _last(ao_series),
        'ao_prev': _prev(ao_series),
        'ao_prev2': _prev(ao_series, 2),
        'mom': _last(mom_series),
        'mom_prev': _prev(mom_series),
        'macd': _last(macd_obj.macd()),
        'stochrsi': _last(stochrsi_d_s),
        'stochrsi_prev': _prev(stochrsi_d_s),
        'willr': _last(willr_s),
        'willr_prev': _prev(willr_s),
        'bbpower': _last(bbpower_s),
        'bbpower_prev': _prev(bbpower_s),
        'uo': _last(ta.momentum.UltimateOscillator(h, l, c, 7, 14, 28).ultimate_oscillator()),
        'bbu': _last(bb.bollinger_hband()),
        'bbl': _last(bb.bollinger_lband()),
        'bb_sma': _last(bb.bollinger_mavg()),
        'bb_pct_b': _last(bb.bollinger_pband()),
        'bb_bandwidth': _last(bb.bollinger_wband()),
        'bb_squeeze_pct': (
            lambda bw_s: float(((bw_s.dropna().tail(120) <= bw_s.iloc[-1]).mean() * 100))
                          if pd.notna(bw_s.iloc[-1]) and len(bw_s.dropna()) >= 60 else None
        )(bb.bollinger_wband()),
        'ema10': _last(ta.trend.EMAIndicator(c, 10).ema_indicator()),
        'sma10': _last(ta.trend.SMAIndicator(c, 10).sma_indicator()),
        'ema20': _last(ema20_s),
        'ema20_prev': _prev(ema20_s),
        'sma20': _last(ta.trend.SMAIndicator(c, 20).sma_indicator()),
        'ema30': _last(ta.trend.EMAIndicator(c, 30).ema_indicator()),
        'sma30': _last(ta.trend.SMAIndicator(c, 30).sma_indicator()),
        'ema50': _last(ta.trend.EMAIndicator(c, 50).ema_indicator()),
        'sma50': _last(ta.trend.SMAIndicator(c, 50).sma_indicator()),
        'ema60': _last(ema60_s),
        'ema60_prev': _prev(ema60_s),
        'ema20_cross_days': ema20_cross_days,
        'cross_day_close': cross_day_close,
        'cross_change_pct': cross_change_pct,
        'w_close': w_close_v,
        'w_ma10': w_ma10_v,
        'w_ma20': w_ma20_v,
        'w_dev': ((w_close_v - w_ma10_v) / w_ma10_v * 100
                   if w_close_v and w_ma10_v and w_ma10_v != 0 else None),
        'sma60': _last(ta.trend.SMAIndicator(c, 60).sma_indicator()),
        'ema100': _last(ta.trend.EMAIndicator(c, 100).ema_indicator()),
        'sma100': _last(ta.trend.SMAIndicator(c, 100).sma_indicator()),
        'ema200': _last(ta.trend.EMAIndicator(c, 200).ema_indicator()),
        'sma200': _last(sma200_s),
        'sma200_prev': _prev(sma200_s),
        'ichimoku': _last(ichi.ichimoku_base_line()),
        'vwma': _last(_vwma(c, v, 20)),
        'hma': _last(_hull_ma(c, 9)),
        'high60': float(h.iloc[-60:].max()) if len(h) >= 60 else None,
        'low60': float(l.iloc[-60:].min()) if len(l) >= 60 else None,
        # 🆕 intraday-specific metadata
        '_intraday_tf': tf,
        '_intraday_bars': len(df),
        '_intraday_last_ts': str(df.index[-1]),
        '_intraday_market': market,
        '_bar_unit': _bar_unit_for_tf(tf),    # 🆕 v9.31：「天」/「個 5m bar」等
    }

    # 🆕 v9.31：產生 _swing_history 讓 ZigZag chart 用 intraday df 而非 daily fallback
    try:
        import numpy as _np
        tail = min(252, len(df))
        def _np_tail(s, n=tail):
            try:
                arr = s.values if hasattr(s, 'values') else _np.asarray(s)
                if len(arr) >= n:
                    return arr[-n:].tolist()
                return arr.tolist()
            except Exception:
                return []
        sma50_s_ = ta.trend.SMAIndicator(c, 50).sma_indicator()
        sma150_s_ = ta.trend.SMAIndicator(c, 150).sma_indicator()
        ema10_s_ = ta.trend.EMAIndicator(c, 10).ema_indicator()
        _idx_arr = df.index
        # intraday 用完整 timestamp 字串（含小時分鐘）；daily 用 YYYY-MM-DD
        if cfg.minutes_per_bar < 1440:
            _dates_tail = [str(x) for x in (_idx_arr[-tail:] if len(_idx_arr) >= tail else _idx_arr)]
        else:
            _dates_tail = [str(x)[:10] for x in (_idx_arr[-tail:] if len(_idx_arr) >= tail else _idx_arr)]
        d['_swing_history'] = {
            'dates':  _dates_tail,
            'open':   _np_tail(o),
            'high':   _np_tail(h),
            'low':    _np_tail(l),
            'close':  _np_tail(c),
            'volume': _np_tail(v),
            'ema10':  _np_tail(ema10_s_),
            'ema20':  _np_tail(ema20_s),
            'ema60':  _np_tail(ema60_s),
            'sma50':  _np_tail(sma50_s_),
            'sma150': _np_tail(sma150_s_),
            'sma200': _np_tail(sma200_s),
            'adx':    _np_tail(adx_s),
            'atr':    _np_tail(atr_s),
            'rsi':    _np_tail(rsi_s),
        }
    except Exception:
        d['_swing_history'] = None

    # ── K 線型態 ──
    d['kline_patterns'] = _detect_kline_patterns(df, lookback=5)

    # ── T3 信心度 ──
    try:
        score, hits = _t3_confidence(
            d.get('close'), d.get('ema5'), d.get('ema20'),
            d.get('ema5_5d_ago'), d.get('ema20_5d_ago'))
        d['t3_confidence'] = score
        d['t3_confidence_hits'] = hits
    except Exception:
        d['t3_confidence'] = 0
        d['t3_confidence_hits'] = []

    # ── SEPA / VCP / Stage / Cup / Flat / Double bottom-top（餵 intraday df 進去）──
    # 這些 detector 都接受任意 df，會依據 bar 數計算（不關心是 1d 還是 5m）
    try:
        from sepa_vcp import (compute_sma_helpers, compute_returns,
                                 check_sepa_trend_template, detect_vcp)
        ind_df = pd.DataFrame({'Close': c, 'High': h, 'Low': l})
        _sma_helpers = compute_sma_helpers(ind_df)
        _ret = compute_returns(pd.DataFrame({'Close': c}))
        d['sma150'] = _sma_helpers.get('sma150')
        d['sma200_real'] = _sma_helpers.get('sma200')
        d['high_52w'] = _sma_helpers.get('high_52w')
        d['low_52w'] = _sma_helpers.get('low_52w')
        d['from_52w_low'] = _sma_helpers.get('from_52w_low', 0)
        d['from_52w_high_pct'] = _sma_helpers.get('from_52w_high', 0)
        d['returns_13w'] = _ret.get('13w', 0)
        d['returns_26w'] = _ret.get('26w', 0)
        d['returns_39w'] = _ret.get('39w', 0)
        d['returns_52w'] = _ret.get('52w', 0)

        _sepa_pass, _sepa_n, _sepa_det = check_sepa_trend_template(
            d.get('close'),
            d.get('sma50'),
            d.get('sma150'),
            d.get('sma200_real'),
            _sma_helpers.get('sma200_30d_ago'),
            d.get('high_52w'),
            d.get('low_52w'))
        d['sepa_passed'] = _sepa_pass
        d['sepa_n_met'] = _sepa_n
        d['sepa_details'] = _sepa_det

        d['vcp_info'] = detect_vcp(pd.DataFrame({
            'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': v
        }))
    except Exception:
        d['sepa_passed'] = False
        d['sepa_n_met'] = 0
        d['vcp_info'] = {'is_vcp': False}

    # Double bottom / top
    try:
        from double_pattern import (detect_double_bottom, detect_double_top,
                                       detect_vcp_zigzag)
        _ddf = pd.DataFrame({
            'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': v
        })
        d['double_bottom_info'] = detect_double_bottom(_ddf)
        d['double_top_info'] = detect_double_top(_ddf)
        d['vcp_zigzag_info'] = detect_vcp_zigzag(_ddf)
    except Exception:
        d['double_bottom_info'] = {'is_double_bottom': False, 'status': 'none'}
        d['double_top_info'] = {'is_double_top': False, 'status': 'none'}
        d['vcp_zigzag_info'] = {'is_vcp': False}

    # RS Rating 設 None（跨 universe 不可即時）
    d['rs_rating'] = None

    # 沒有 PER / margin / news（intraday 不適用）
    # 但需要的欄位仍要存在以免 render_detail 取空 fail
    return d
