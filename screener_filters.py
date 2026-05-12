"""全市場篩選器 — 集中所有驗證過的指標組合（v9.13）
==========================================================
依過去研究驗證有 alpha 的訊號，提供下拉式選單篩選。

每個 filter 是一個函數 (df) → bool，回傳是否觸發。
df 必須含計算過的指標：Close, Open, High, Low, Volume, e10, e20, e60, rsi, adx, atr

filter 分組：
  📈 強看多 (alpha 已驗證)
  📉 強看空 (alpha 已驗證)
  📊 BB 系列 (OANDA 文章 10 種)
  🌱 即將觸發 (前期觀察)
  🎯 進階組合
  📋 基礎篩選
"""
import numpy as np
import pandas as pd
from bb_signals import (compute_bb, is_squeeze, is_expansion,
                          is_walking_up, is_walking_down,
                          pct_b_extreme_high, pct_b_extreme_low,
                          mean_reversion_high, mean_reversion_low,
                          is_w_bottom, is_m_top)
# 🆕 v9.19：SEPA / VCP / RS Rating
from sepa_vcp import (check_sepa_trend_template, detect_vcp,
                       compute_sma_helpers, compute_returns)
# 🆕 v9.21：雙底雙頂偵測
from double_pattern import detect_double_bottom, detect_double_top


# ── Helper: 取得最後一日的關鍵指標 ──
def _get_state(df, market='tw'):
    """取得最後一日所有需要的指標值"""
    if df is None or len(df) < 60: return None
    try:
        c = df['Close'].values
        h = df['High'].values
        l = df['Low'].values
        o = df['Open'].values
        v = df['Volume'].values
        e10 = df['e10'].values if 'e10' in df.columns else None
        e20 = df['e20'].values if 'e20' in df.columns else None
        e60 = df['e60'].values if 'e60' in df.columns else None
        rsi = df['rsi'].values if 'rsi' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        atr = df['atr'].values if 'atr' in df.columns else None
        if any(x is None for x in [e20, e60, rsi, adx]):
            return None

        # BB 計算
        bb = compute_bb(c)
        bb_sma = bb['sma']
        bb_bbu = bb['bbu']
        bb_bbl = bb['bbl']
        bb_bw = bb['bandwidth']
        bb_pctb = bb['pct_b']

        i = len(df) - 1
        close = float(c[i])
        if close <= 0 or np.isnan(close): return None
        e20_v = float(e20[i]) if not np.isnan(e20[i]) else None
        e60_v = float(e60[i]) if not np.isnan(e60[i]) else None
        if e20_v is None or e60_v is None: return None

        rsi_v = float(rsi[i]) if not np.isnan(rsi[i]) else None
        adx_v = float(adx[i]) if not np.isnan(adx[i]) else None
        atr_v = float(atr[i]) if atr is not None and not np.isnan(atr[i]) else None

        # 歷史指標
        adx_5d = float(adx[i-5]) if i >= 5 and not np.isnan(adx[i-5]) else adx_v
        adx_rising = adx_v is not None and adx_v > adx_5d if adx_5d else False
        adx_falling = adx_v is not None and adx_v < adx_5d if adx_5d else False

        # drop_30d
        drop_30d = ((close - c[i-30]) / c[i-30] * 100) if i >= 30 and c[i-30] > 0 else 0
        # 60d 高低
        high60 = float(h[max(0, i-60):i+1].max())
        low60 = float(l[max(0, i-60):i+1].min())
        from_high = (high60 - close) / high60 * 100 if high60 > 0 else 0
        from_low = (close - low60) / low60 * 100 if low60 > 0 else 0
        # SMA200
        sma200 = float(np.mean(c[max(0, i-199):i+1])) if i >= 100 else close
        sma200_pct = (close / sma200 - 1) * 100 if sma200 > 0 else 0
        # 量
        vol60_avg = float(np.mean(v[max(0, i-59):i+1])) if i >= 30 else 0
        vol_ratio = v[i] / vol60_avg if vol60_avg > 0 else 1
        # cross_days
        cross_days = None
        try:
            diff_arr = e20 - e60
            for k in range(1, min(i, 200)):
                d1 = diff_arr[i - k + 1]
                d0 = diff_arr[i - k]
                if not np.isnan(d1) and not np.isnan(d0):
                    if d0 < 0 and d1 >= 0:
                        cross_days = k; break
                    elif d0 > 0 and d1 <= 0:
                        cross_days = -k; break
        except Exception: pass

        # K 線型態（最後一日是否觸發）
        try:
            from kline_patterns import detect_recent
            patterns = detect_recent(df, lookback=2)
            recent_patterns = {p['name']: p for p in patterns if p['days_ago'] <= 1}
        except Exception:
            recent_patterns = {}

        # imminent_dc
        imminent_dc = False
        if (cross_days and cross_days > 10 and atr_v and atr_v > 0
                and e20_v > e60_v and (e20_v - e60_v) < atr_v):
            e20_5d_ago = e20[i-5] if i >= 5 and not np.isnan(e20[i-5]) else None
            ema20_falling = e20_5d_ago is not None and e20_v < e20_5d_ago
            if ema20_falling or cross_days > 30:
                imminent_dc = True

        # 🆕 v9.17：主動出場 recipe 需要的指標
        ema10_v = float(e10[i]) if e10 is not None and not np.isnan(e10[i]) else None
        # close < EMA20 連 2 天
        close_below_ema20_2d = (
            ema10_v is not None and i >= 1 and e20 is not None
            and not np.isnan(e20[i-1]) and not np.isnan(c[i-1])
            and c[i] < e20_v and c[i-1] < e20[i-1]
        )
        # close < EMA10 連 2 天
        close_below_ema10_2d = (
            ema10_v is not None and i >= 1
            and not np.isnan(e10[i-1]) and not np.isnan(c[i-1])
            and c[i] < ema10_v and c[i-1] < e10[i-1]
        )
        # 3 連黑K + 量增 > 1.3x
        try:
            blacks = sum(1 for j in [i, i-1, i-2]
                         if j >= 0 and not np.isnan(o[j]) and not np.isnan(c[j]) and c[j] < o[j])
            vol_avg_20 = float(np.nanmean(v[max(0,i-20):i])) if i >= 20 else 0
            three_black_vol = (blacks >= 3 and vol_avg_20 > 0
                                and v[i] / vol_avg_20 > 1.3)
        except Exception:
            three_black_vol = False
        # ADX 5d 下降 ≥ 5
        adx_5d_decay = (adx_v is not None and adx_5d is not None
                         and adx_v - adx_5d <= -5)
        # ATR×2.5 trailing：用過去 30d 高作 peak
        try:
            peak_30d = float(np.nanmax(h[max(0,i-30):i+1])) if i >= 30 else float(h[i])
            atr_trail_triggered = (atr_v and atr_v > 0
                                    and (peak_30d - 2.5 * atr_v) > 0
                                    and c[i] <= (peak_30d - 2.5 * atr_v))
        except Exception:
            atr_trail_triggered = False

        # 🆕 v9.19：SEPA / VCP / RS Rating（必須在 universe-wide 計算 RS 前先算）
        try:
            sma_helpers = compute_sma_helpers(df)
        except Exception:
            sma_helpers = {}
        try:
            returns_dict = compute_returns(df)
        except Exception:
            returns_dict = {}
        try:
            vcp_info = detect_vcp(df)
        except Exception:
            vcp_info = {'is_vcp': False}
        # 🆕 v9.21：雙底雙頂
        try:
            db_info = detect_double_bottom(df)
        except Exception:
            db_info = {'is_double_bottom': False, 'status': 'none'}
        try:
            dt_info = detect_double_top(df)
        except Exception:
            dt_info = {'is_double_top': False, 'status': 'none'}
        # SEPA Trend Template 7 條件（cond8 RS 由外部注入）
        try:
            sepa_passed, sepa_n_met, sepa_details = check_sepa_trend_template(
                close,
                sma_helpers.get('sma50'),
                sma_helpers.get('sma150'),
                sma_helpers.get('sma200'),
                sma_helpers.get('sma200_30d_ago'),
                sma_helpers.get('high_52w'),
                sma_helpers.get('low_52w'))
        except Exception:
            sepa_passed = False; sepa_n_met = 0; sepa_details = {}

        return {
            'close': close, 'open': float(o[i]), 'high': float(h[i]), 'low': float(l[i]),
            'ema10': ema10_v, 'ema20': e20_v, 'ema60': e60_v,
            'rsi': rsi_v, 'adx': adx_v, 'atr': atr_v,
            # 🆕 v9.19 SEPA / VCP / RS
            'sma50': sma_helpers.get('sma50'),
            'sma150': sma_helpers.get('sma150'),
            'sma200_v': sma_helpers.get('sma200'),  # 'sma200' 已被原本佔用
            'sma200_30d_ago': sma_helpers.get('sma200_30d_ago'),
            'high_52w': sma_helpers.get('high_52w'),
            'low_52w': sma_helpers.get('low_52w'),
            'from_52w_low': sma_helpers.get('from_52w_low', 0),
            'from_52w_high_pct': sma_helpers.get('from_52w_high', 0),
            'returns_13w': returns_dict.get('13w', 0),
            'returns_26w': returns_dict.get('26w', 0),
            'returns_39w': returns_dict.get('39w', 0),
            'returns_52w': returns_dict.get('52w', 0),
            'vcp_is_vcp': vcp_info.get('is_vcp', False),
            'vcp_near_pivot': vcp_info.get('near_pivot', False),
            'vcp_near_pivot_pct': vcp_info.get('near_pivot_pct', 0),
            'vcp_pivot_price': vcp_info.get('pivot_price', 0),
            'vcp_n_contractions': vcp_info.get('n_contractions', 0),
            'vcp_declines_pct': vcp_info.get('declines_pct', []),
            'vcp_volume_dry_up': vcp_info.get('volume_dry_up', False),
            'sepa_passed': sepa_passed,
            'sepa_n_met': sepa_n_met,
            'sepa_details': sepa_details,
            # 🆕 v9.21：雙底雙頂
            'double_bottom_status': db_info.get('status', 'none'),
            'double_bottom_breakout': db_info.get('breakout_confirmed', False),
            'double_bottom_neckline': db_info.get('neckline_price'),
            'double_bottom_target': db_info.get('target_price'),
            'double_bottom_quality': db_info.get('quality_grade', 'D'),  # A/B/C/D
            'double_bottom_score': db_info.get('quality_score', 0),
            'double_bottom_stage': db_info.get('entry_stage', 'wait'),
            'double_bottom_info': db_info,
            'double_top_status': dt_info.get('status', 'none'),
            'double_top_breakdown': dt_info.get('breakdown_confirmed', False),
            'double_top_neckline': dt_info.get('neckline_price'),
            'double_top_target': dt_info.get('target_price'),
            'double_top_quality': dt_info.get('quality_grade', 'D'),
            'double_top_score': dt_info.get('quality_score', 0),
            'double_top_stage': dt_info.get('entry_stage', 'wait'),
            'double_top_info': dt_info,
            # 'rs_rating' 由 screener_full_cloud 在 universe-wide 計算後注入
            'adx_5d_prev': adx_5d, 'adx_rising': adx_rising, 'adx_falling': adx_falling,
            'is_bull': e20_v > e60_v, 'is_bear': e20_v < e60_v,
            'drop_30d': drop_30d, 'from_high': from_high, 'from_low': from_low,
            'sma200': sma200, 'sma200_pct': sma200_pct,
            'vol_ratio': vol_ratio, 'cross_days': cross_days,
            'imminent_dc': imminent_dc,
            # 🆕 v9.17 主動出場觸發
            'close_below_ema20_2d': close_below_ema20_2d,
            'close_below_ema10_2d': close_below_ema10_2d,
            'three_black_vol': three_black_vol,
            'adx_5d_decay': adx_5d_decay,
            'atr_trail_triggered': atr_trail_triggered,
            # BB
            'bb_sma': float(bb_sma[i]) if not np.isnan(bb_sma[i]) else None,
            'bb_bbu': float(bb_bbu[i]) if not np.isnan(bb_bbu[i]) else None,
            'bb_bbl': float(bb_bbl[i]) if not np.isnan(bb_bbl[i]) else None,
            'bb_pct_b': float(bb_pctb[i]) if not np.isnan(bb_pctb[i]) else None,
            'bb_bandwidth': float(bb_bw[i]) if not np.isnan(bb_bw[i]) else None,
            'bb_squeeze': is_squeeze(bb_bw, i),
            'bb_expansion': is_expansion(bb_bw, i),
            'bb_walking_up': is_walking_up(c, bb_sma, bb_bbu, i),
            'bb_walking_down': is_walking_down(c, bb_sma, bb_bbl, i),
            'bb_w_bottom': is_w_bottom(l, c, bb_bbl, bb_sma, i),
            'bb_m_top': is_m_top(h, c, bb_bbu, bb_sma, i),
            # K 線
            'kline': recent_patterns,
            # 期間參考
            'date': df.index[i].strftime('%Y-%m-%d') if hasattr(df.index[i], 'strftime') else '',
            # ATR/Price 相對波動
            'atr_pct': (atr_v / close * 100) if (atr_v and close > 0) else 0,
        }
    except Exception:
        return None


# ── Filter 集（每個都是 (state) → bool）──

# 📈 強看多（alpha 已驗證）
def f_inv_hammer_strong(s):
    """★★★★★ 倒鎚 + RSI≤25 + ADX↑ (71.8% 漲, +9.35% 30d, OOS 驗證)"""
    return ('INV_HAMMER' in s.get('kline', {}) and s.get('rsi', 99) <= 25
            and s.get('adx_rising', False))

def f_inv_hammer_extended_down(s):
    """★★★★ 倒鎚 + 距 SMA200 < -25%（跌深）"""
    return ('INV_HAMMER' in s.get('kline', {}) and s.get('sma200_pct', 0) < -25)

def f_doji_oversold(s):
    """★★★ 底部十字星 + RSI≤25 + ADX↑（67.4% 漲, +7.02% 30d）"""
    return ('DOJI' in s.get('kline', {}) and s.get('rsi', 99) <= 25
            and s.get('adx_rising', False) and s.get('from_low', 99) < 10)

def f_t1_imminent_strict(s):
    """★★ T1 即將上穿（V7 嚴格：距 EMA20 ≤ 1% + 連 2 漲 + ADX≥22 + 多頭）"""
    if not s.get('is_bull'): return False
    if s.get('close', 0) >= s.get('ema20', 0): return False
    e20 = s.get('ema20', 0)
    if e20 <= 0: return False
    dist = (e20 - s['close']) / e20 * 100
    return dist <= 1.0 and s.get('adx', 0) >= 22

def f_t1_imminent_loose(s):
    """T1 即將上穿（寬鬆 L3：距 EMA20 ≤ 3% + 多頭）"""
    if not s.get('is_bull'): return False
    if s.get('close', 0) >= s.get('ema20', 0): return False
    e20 = s.get('ema20', 0)
    if e20 <= 0: return False
    dist = (e20 - s['close']) / e20 * 100
    return dist <= 3.0

def f_t1_sweet_spot(s):
    """T1 黃金交叉 sweet spot（5-7 天）— TW 研究最佳"""
    cd = s.get('cross_days')
    return (s.get('is_bull') and s.get('adx', 0) >= 22
            and cd is not None and 5 <= cd <= 7)

def f_t1_fresh(s):
    """T1 黃金交叉 1-10 天（剛 cross 上 EMA20）"""
    cd = s.get('cross_days')
    return (s.get('is_bull') and s.get('adx', 0) >= 22
            and cd is not None and 1 <= cd <= 10)

def f_t1_today(s):
    """🆕 v9.14：T1 黃金交叉 1 天（snapshot 時點剛 cross；注意：JSON 若已過交易日，今日 cd 會 +N）"""
    cd = s.get('cross_days')
    # snapshot 時 cd=1 = JSON 計算當日剛 cross；今日 cd 視 JSON 新鮮度可能 +1~+N
    return (s.get('is_bull') and s.get('adx', 0) >= 22
            and cd is not None and cd == 1)

def f_t1_within3(s):
    """🆕 v9.14：T1 黃金交叉 1-3 天（snapshot；注意：JSON 若已過交易日，今日 cd 會 +N）"""
    cd = s.get('cross_days')
    return (s.get('is_bull') and s.get('adx', 0) >= 22
            and cd is not None and 1 <= cd <= 3)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🌊 波段策略（Swing Trading）— 5-30 天持有
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def f_swing_trend_continuation(s):
    """🌊 波段 A：趨勢延續（最穩，主力波段）
    多頭 + ADX≥25 + RSI 45-65 健康 + T1 cross 5-15 天 + 不在過熱
    → 趨勢已啟動且健康，拉回後再跑一段"""
    cd = s.get('cross_days')
    rsi = s.get('rsi') or 0
    return (s.get('is_bull')
            and (s.get('adx') or 0) >= 25
            and 45 <= rsi <= 65
            and cd is not None and 5 <= cd <= 15
            and s.get('from_high', 99) > 2  # 不要在前高（避免追)
            and not s.get('imminent_dc'))


def f_swing_breakout(s):
    """🚀 波段 B：突破波段（最猛）
    距 60d 高 < 1%（突破中）+ 量增 1.5x + 多頭 + ADX≥22
    → 突破前高 + 大量確認 = 動能爆發"""
    return (s.get('is_bull')
            and (s.get('adx') or 0) >= 22
            and s.get('from_high', 99) < 1
            and s.get('vol_ratio', 0) > 1.5
            and (s.get('rsi') or 0) < 75)  # 還沒過熱


def f_swing_pullback_to_ema20(s):
    """💧 波段 C：拉回 EMA20 進場（中線經典）
    多頭 + ADX≥22 + close 距 EMA20 < 2% + RSI 40-55
    → 健康拉回到 EMA20 支撐，準備下一波"""
    close = s.get('close', 0)
    ema20 = s.get('ema20', 0)
    if not (close and ema20 and ema20 > 0): return False
    dist_ema20 = abs(close - ema20) / ema20 * 100
    rsi = s.get('rsi') or 0
    return (s.get('is_bull')
            and (s.get('adx') or 0) >= 22
            and dist_ema20 < 2
            and 40 <= rsi <= 55
            and not s.get('imminent_dc'))


def f_swing_momentum_acceleration(s):
    """⚡ 波段 D：動能加速（加碼點）
    多頭 + ADX 從中等飆強（升 +5 點以上）+ RSI < 70
    → ADX 突然加速 = 大行情啟動，仍未過熱"""
    adx_now = s.get('adx') or 0
    adx_5d = s.get('adx_5d_prev') or 0
    return (s.get('is_bull')
            and adx_now >= 25
            and (adx_now - adx_5d) >= 5  # 5 日內 ADX 加速 +5
            and (s.get('rsi') or 0) < 70)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 OOS 驗證波段（v9.15）— 經過 walk-forward 驗證的最佳組合
# Walk-forward (TW+US) OOS robust:
#   - B + rsi_80：54% win, +4.0% mean（aggressive）
#   - B + rsi_75：61% win, +2.6% mean（balanced）
#   - B + rsi_70：65% win, +1.3% mean（conservative）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def f_swing_B_entry_validated(s):
    """🌟 OOS 驗證波段 B 入場（突破前高 ≤1% + 量增 1.5x + RSI<70 + ADX≥22）
    跨 TW + US OOS 兩市場通：win 54-65%, mean +1.3-4.0%（依出場規則）"""
    return (s.get('is_bull')
            and (s.get('adx') or 0) >= 22
            and s.get('from_high', 99) < 1
            and s.get('vol_ratio', 0) > 1.5
            and (s.get('rsi') or 0) < 70)  # 比原版更嚴 (<75 → <70)


def f_swing_near_breakout(s):
    """🟢 波段近突破：距 60d 高 1-3% + 量增 1.2x + 多頭 + ADX≥22
    → 候選清單，等突破時進場"""
    fh = s.get('from_high', 99)
    return (s.get('is_bull')
            and (s.get('adx') or 0) >= 22
            and 1 <= fh <= 3
            and s.get('vol_ratio', 0) > 1.2
            and (s.get('rsi') or 0) < 70)


def f_swing_exit_overheat(s):
    """🚪 波段過熱出場警示：多頭 + RSI≥80
    → 若你持有此股，OOS 驗證的最佳出場點。1-2 天內賣"""
    return s.get('is_bull') and (s.get('rsi') or 0) >= 80


def f_swing_exit_warning(s):
    """🟡 波段接近過熱：多頭 + RSI 75-80
    → 提早 watchlist，準備出場。OOS 驗證 rsi_75 = 61% win/+2.6%"""
    rsi = s.get('rsi') or 0
    return s.get('is_bull') and 75 <= rsi < 80


def f_swing_exit_dc_warn(s):
    """⚠️ 波段死叉警告：多頭 + imminent_dc（持倉應重新評估）"""
    return s.get('is_bull') and s.get('imminent_dc', False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🚪 主動波段出場 recipe（v9.17）— 找全市場該賣的股票
# 配合 detail card 的 4 種 recipe 即時評估
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def f_active_exit_A(s):
    """🛡️ 主動出場 A 保守快出（多頭 + E1 OR E2 觸發）
    E1: close < EMA20 連 2 天，OR
    E2: 3 連黑K + 量比 > 1.3x"""
    return s.get('is_bull') and (
        s.get('close_below_ema20_2d', False) or s.get('three_black_vol', False))


def f_active_exit_B(s):
    """⚖️ 主動出場 B 平衡 ⭐（多頭 + E1 AND E3 同時觸發）
    E1: close < EMA20 連 2 天 AND
    E3: ADX 5d 下降 ≥ 5"""
    return s.get('is_bull') and (
        s.get('close_below_ema20_2d', False) and s.get('adx_5d_decay', False))


def f_active_exit_C(s):
    """🚀 主動出場 C 飆股模式（多頭 + close < EMA10 連 2 天）"""
    return s.get('is_bull') and s.get('close_below_ema10_2d', False)


def f_active_exit_D(s):
    """🎯 主動出場 D ATR 動態（多頭 + close ≤ peak − 2.5 ATR）
    OOS 最高效率 mean/d +0.097（US）/ +0.075（TW）"""
    return s.get('is_bull') and s.get('atr_trail_triggered', False)


def f_active_exit_any(s):
    """🚪 任一 recipe 觸發（A OR B OR C OR D 任一）— 持倉警示總覽"""
    return s.get('is_bull') and (
        f_active_exit_A(s) or f_active_exit_C(s) or f_active_exit_D(s))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔮 Squeeze 突破方向預測（v9.17）— 基於 79,605 events 研究
# 最佳組合：BULL + ACCUMULATION → UP 47% / DOWN 29% (bias +18%)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def f_squeeze_breakout_bull(s):
    """🔮 BB Squeeze + 多排（即將向上突破）
    OOS：47% UP / 29% DOWN，平均 +3.7%
    需 BB squeeze + EMA20 > EMA60（明顯多排，非糾纏）"""
    if not s.get('bb_squeeze', False): return False
    e20 = s.get('ema20'); e60 = s.get('ema60')
    if not (e20 and e60 and e60 > 0): return False
    gap_pct = (e20 - e60) / e60 * 100
    return gap_pct > 0.5  # 明顯多排


def f_squeeze_breakout_bear(s):
    """🔮 BB Squeeze + 空排（即將向下突破）
    OOS：BEAR + DISTRIBUTION → DOWN 47% / UP 35%
    需 BB squeeze + EMA20 < EMA60"""
    if not s.get('bb_squeeze', False): return False
    e20 = s.get('ema20'); e60 = s.get('ema60')
    if not (e20 and e60 and e60 > 0): return False
    gap_pct = (e60 - e20) / e60 * 100
    return gap_pct > 0.5


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🏆 SEPA / VCP / RS Rating（v9.19）— Mark Minervini 飆股策略
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def f_sepa_trend_template(s):
    """🏆 SEPA Trend Template（7 條件全過 + RS≥70）
    Mark Minervini 飆股體質 8 條件中前 7 條（RS≥70 由 cond8 另計）
    全市場通常只剩 5-10% 通過"""
    if not s.get('sepa_passed', False): return False
    rs = s.get('rs_rating')
    if rs is None: return s.get('sepa_passed', False)  # RS 未注入時用 7 條件
    return rs >= 70


def f_sepa_trend_template_7of7(s):
    """🏆 SEPA Trend Template 7/7（基礎版，不需 RS≥70）"""
    return s.get('sepa_passed', False)


def f_sepa_partial_6of7(s):
    """🥈 SEPA 6/7（接近過關，可加 watchlist）"""
    return s.get('sepa_n_met', 0) >= 6


def f_vcp_pattern(s):
    """📐 VCP 形態（≥2 次振幅遞減 + 接近 pivot）
    Volatility Contraction Pattern — Minervini 進場形態"""
    return s.get('vcp_is_vcp', False)


def f_vcp_volume_dry_pivot(s):
    """📐 VCP + 量縮 + 接近 pivot（最佳 setup）"""
    return (s.get('vcp_is_vcp', False)
            and s.get('vcp_volume_dry_up', False)
            and s.get('vcp_near_pivot', False))


def f_rs_rating_70(s):
    """💪 RS Rating ≥ 70（強於 70% 同期股票）"""
    rs = s.get('rs_rating')
    return rs is not None and rs >= 70


def f_rs_rating_80(s):
    """💪 RS Rating ≥ 80（強於 80%）"""
    rs = s.get('rs_rating')
    return rs is not None and rs >= 80


def f_rs_rating_90(s):
    """💪 RS Rating ≥ 90（飆股候選 — 強於 90%）"""
    rs = s.get('rs_rating')
    return rs is not None and rs >= 90


def f_sepa_full_setup(s):
    """🏆⭐ 完整 Minervini Setup：SEPA 7/7 + VCP + RS≥70"""
    return (s.get('sepa_passed', False)
            and s.get('vcp_is_vcp', False)
            and (s.get('rs_rating') or 0) >= 70)


def f_pivot_near_breakout(s):
    """🎯 Pivot Point 接近突破（VCP 收口 + 距 pivot ≤ 1%）
    Minervini 進場時機：突破時放量買進"""
    return (s.get('vcp_is_vcp', False)
            and -2 <= (s.get('vcp_near_pivot_pct') or -99) <= 1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🟢🔴 雙底雙頂（v9.21）— Double Bottom / Top reversal
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def f_double_bottom_breakout(s):
    """🟢 雙底突破（任何 grade）"""
    return s.get('double_bottom_status') in ('B_breakout_buy', 'breakout')


def f_double_bottom_confirmed(s):
    """🟢 雙底成形（兩底已現 + 反彈，待突破 neckline）"""
    return s.get('double_bottom_status') == 'confirmed'


def f_double_top_breakdown(s):
    """🔴 雙頂跌破（任何 grade）"""
    return s.get('double_top_status') in ('B_breakdown_short', 'breakdown')


def f_double_top_confirmed(s):
    """🔴 雙頂成形（兩頂已現 + 回檔，待跌破 neckline）"""
    return s.get('double_top_status') == 'confirmed'


# 🆕 v9.22：依五大關鍵 quality 分級
def f_double_bottom_grade_A(s):
    """🥇 雙底 Grade A（5/5 五大關鍵全過 — 最高品質）"""
    return (s.get('double_bottom_status') not in ('none', 'failed')
            and s.get('double_bottom_quality') == 'A')


def f_double_bottom_grade_AB(s):
    """🥈 雙底 Grade A 或 B（≥4/5 — 高品質）"""
    return (s.get('double_bottom_status') not in ('none', 'failed')
            and s.get('double_bottom_quality') in ('A', 'B'))


def f_double_top_grade_A(s):
    """🥇 雙頂 Grade A（5/5）"""
    return (s.get('double_top_status') not in ('none', 'failed')
            and s.get('double_top_quality') == 'A')


def f_double_top_grade_AB(s):
    """🥈 雙頂 Grade A 或 B（≥4/5）"""
    return (s.get('double_top_status') not in ('none', 'failed')
            and s.get('double_top_quality') in ('A', 'B'))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🟣 v9.24：RS Leading High 紫色點訊號（TraderLion 機構累積足跡）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def f_rs_leading_high_signal(s):
    """🟣 RS 領先創新高（任何分數）— RS 創新高但股價未創新高"""
    return s.get('rs_leading_high_passed', False)


def f_rs_leading_high_top(s):
    """🟣 RS 領先創新高 — 高品質（score ≥ 60）"""
    return (s.get('rs_leading_high_passed', False)
            and (s.get('rs_leading_high_score') or 0) >= 60)


def f_rs_leading_high_purple_5plus(s):
    """🟣 紫色點密集（近 20d ≥ 5 次）— 機構買盤明顯"""
    return (s.get('rs_leading_high_passed', False)
            and (s.get('rs_leading_high_purple_dots') or 0) >= 5)


def f_rs_leading_high_eddy_theme(s):
    """🟣 RS 領先創新高 — Eddy 主題（AI 儲存 / AI 能源）"""
    return (s.get('rs_leading_high_passed', False)
            and s.get('rs_leading_high_theme') in ('AI_storage', 'AI_energy'))


# 三段式建倉 stage filters
def f_double_bottom_stage_A(s):
    """🟢 雙底 A 段試單（底部反應K + 第2底剛現）"""
    return s.get('double_bottom_stage') == 'A_test'


def f_double_bottom_stage_C(s):
    """🟢 雙底 C 段補滿（突破後回踩 neckline 有效）"""
    return s.get('double_bottom_stage') == 'C_retest'

def f_t3_pullback(s):
    """T3 多頭拉回（多頭 + ADX≥22 + RSI<50）"""
    return s.get('is_bull') and s.get('adx', 0) >= 22 and (s.get('rsi') or 99) < 50

def f_drop_deep_bull(s):
    """跌深反彈訊號（多頭 + 跌≥30%）"""
    return s.get('is_bull') and s.get('from_high', 0) >= 30

def f_high_volatility_alpha(s):
    """🚀 高波動 alpha（多頭 + ATR/P > 5%）— 飆股訊號"""
    return s.get('is_bull') and s.get('atr_pct', 0) > 5.0

def f_wangzhao_combo(s):
    """🎰 王炸組合：跌深 + T1 + 多頭 + ADX 達標（TW 研究最強）"""
    return (s.get('is_bull') and s.get('adx', 0) >= 22
            and s.get('from_high', 0) >= 15
            and s.get('cross_days') and 0 < s.get('cross_days') <= 10)


# 📉 強看空（alpha 已驗證）
def f_three_crows(s):
    """★★★★ 三隻烏鴉 + 距高<5% + 量縮（71% 跌, -1.26% 30d）"""
    return ('THREE_CROWS' in s.get('kline', {}) and s.get('from_high', 99) < 5
            and s.get('vol_ratio', 1) < 0.7)

def f_bear_engulf_overbought(s):
    """★★★ 空頭吞噬 + RSI≥75 + ADX↓"""
    return ('BEAR_ENGULF' in s.get('kline', {}) and (s.get('rsi') or 0) >= 75
            and s.get('adx_falling', False))

def f_evening_star(s):
    """★★ 黃昏之星 + RSI≥75"""
    return ('EVENING_STAR' in s.get('kline', {}) and (s.get('rsi') or 0) >= 75)

def f_imminent_dc(s):
    """⛔ 即將死叉警告（多頭 + EMA gap < 1 ATR + cross_days>30 OR EMA20 下行）"""
    return s.get('imminent_dc', False)


# 📊 BB 系列（OANDA 文章 10 種）
def f_bb_pctb_overheat(s):
    """%B > 1.0 (BB 上軌之外，過熱)"""
    return s.get('bb_pct_b') is not None and s['bb_pct_b'] > 1.0

def f_bb_pctb_oversold(s):
    """%B < 0 (BB 下軌之外，過冷反彈訊號 ★)"""
    return s.get('bb_pct_b') is not None and s['bb_pct_b'] < 0

def f_bb_squeeze(s):
    """BB Squeeze (頻寬 5 天連窄, 大行情前兆)"""
    return s.get('bb_squeeze', False)

def f_bb_expansion(s):
    """BB Expansion 突放（空頭末段反轉訊號 ★）"""
    return s.get('bb_expansion', False)

def f_bb_walking_up(s):
    """BB Walking Up the Band（多頭強勢延續）"""
    return s.get('bb_walking_up', False) and s.get('is_bull', False)

def f_bb_walking_down(s):
    """BB Walking Down the Band（空頭弱勢延續）"""
    return s.get('bb_walking_down', False) and s.get('is_bear', False)

def f_bb_w_bottom(s):
    """BB W 底（雙觸下軌 + 二次更高 + 突破中軌, 多頭反轉）"""
    return s.get('bb_w_bottom', False)

def f_bb_m_top(s):
    """BB M 頂（雙觸上軌 + 二次更低 + 跌破中軌, 空頭反轉）"""
    return s.get('bb_m_top', False)

def f_bb_squeeze_bull_bias(s):
    """BB Squeeze + 多頭排列（即將向上爆發，多頭中的觀察點）"""
    return s.get('bb_squeeze', False) and s.get('is_bull', False)

def f_bb_squeeze_bear_bias(s):
    """BB Squeeze + 空頭排列（即將向下爆發）"""
    return s.get('bb_squeeze', False) and s.get('is_bear', False)


# 🌱 即將觸發
def f_imm_inv_hammer_rsi(s):
    """即將：倒鎚 + RSI 26-30 + ADX↑（差 RSI 1-5 點即達 ★★★★★）"""
    return ('INV_HAMMER' in s.get('kline', {}) and 25 < (s.get('rsi') or 0) <= 30
            and s.get('adx_rising', False))

def f_imm_three_crows(s):
    """即將：三隻烏鴉 + 距高 5-10% + 量縮"""
    return ('THREE_CROWS' in s.get('kline', {}) and 5 <= s.get('from_high', 99) < 10
            and s.get('vol_ratio', 1) < 0.7)


# 📋 基礎篩選
def f_bull_alignment(s):
    """所有多頭排列（EMA20 > EMA60）"""
    return s.get('is_bull', False)

def f_bear_alignment(s):
    """所有空頭排列（EMA20 < EMA60）"""
    return s.get('is_bear', False)

def f_strong_trend(s):
    """強趨勢（ADX ≥ 30）"""
    return (s.get('adx') or 0) >= 30

def f_fake_bull(s):
    """⚠️ 假多頭（EMA20>EMA60 但 ADX<22，趨勢強度不足，可能震盪）"""
    return s.get('is_bull', False) and (s.get('adx') or 0) < 22

def f_fake_bear(s):
    """⚠️ 假空頭（EMA20<EMA60 但 ADX<22，趨勢強度不足，可能震盪）"""
    return s.get('is_bear', False) and (s.get('adx') or 0) < 22

def f_weak_trend(s):
    """⏸ 弱趨勢 / 震盪（ADX < 22，無論多空）"""
    return (s.get('adx') or 0) < 22

def f_rsi_oversold(s):
    """RSI < 30 極度超賣"""
    return (s.get('rsi') or 99) < 30

def f_rsi_overbought(s):
    """RSI > 70 過熱"""
    return (s.get('rsi') or 0) > 70

def f_at_60d_low(s):
    """接近 60 日低點（距低 < 5%）"""
    return s.get('from_low', 99) < 5

def f_at_60d_high(s):
    """接近 60 日高點（距高 < 5%）"""
    return s.get('from_high', 99) < 5


# ── 集中註冊（顯示用）──
FILTERS = {
    # 📈 強看多
    '🚀 ★★★★★ 倒鎚 + RSI≤25 + ADX↑': f_inv_hammer_strong,
    '🚀 ★★★★ 倒鎚 + 跌深(SMA200<-25%)': f_inv_hammer_extended_down,
    '⚡ ★★★ 底部十字星 + RSI≤25 + ADX↑': f_doji_oversold,
    '🎰 王炸組合：跌深+T1+多頭+ADX': f_wangzhao_combo,
    '📉 跌深反彈（≥30% + 多頭）': f_drop_deep_bull,
    '🚀 高波動 alpha（多頭+ATR/P>5%）': f_high_volatility_alpha,
    '🌟 T1 剛剛黃金交叉（0-1天，最早）': f_t1_today,
    '⚡ T1 黃金交叉 3 天內（早期進場）': f_t1_within3,
    # 🌊 波段策略（Swing Trading 專用，hold 5-30 天）
    '🌊 波段 A：趨勢延續（cross 5-15d + 健康RSI）': f_swing_trend_continuation,
    '🚀 波段 B：突破前高 + 量增 1.5x': f_swing_breakout,
    '💧 波段 C：拉回 EMA20（< 2% + RSI 40-55）': f_swing_pullback_to_ema20,
    '⚡ 波段 D：動能加速（ADX 5d 升 +5）': f_swing_momentum_acceleration,
    # 🎯 OOS 驗證波段（v9.15）— walk-forward 驗證的最佳組合
    '🌟 OOS驗證 波段B 入場（突破1%內+RSI<70）': f_swing_B_entry_validated,
    '🟢 波段近突破（距高1-3%+量增1.2x）': f_swing_near_breakout,
    '🚪 波段過熱該賣（多頭+RSI≥80）': f_swing_exit_overheat,
    '🟡 波段接近過熱（多頭+RSI 75-80）': f_swing_exit_warning,
    '⚠️ 波段死叉警告（多頭+imminent_dc）': f_swing_exit_dc_warn,
    # 🆕 v9.17：4 種主動出場 recipe（OOS 驗證的退場時機）
    '🚪 主動出場 A 保守快出（E1或E2）': f_active_exit_A,
    '🚪 主動出場 B 平衡 ⭐（E1+E3 同時）': f_active_exit_B,
    '🚪 主動出場 C 飆股（EMA10 連跌2天）': f_active_exit_C,
    '🚪 主動出場 D ATR動態（peak-2.5ATR）⭐效率王': f_active_exit_D,
    '🚪 主動出場 任一觸發（持倉警示總覽）': f_active_exit_any,
    # 🆕 v9.17：Squeeze 突破方向預測（OOS 79k events 研究）
    '🔮 Squeeze 偏多突破（BULL+squeeze→UP 47%）': f_squeeze_breakout_bull,
    '🔮 Squeeze 偏空突破（BEAR+squeeze→DOWN 47%）': f_squeeze_breakout_bear,
    # 🆕 v9.19：SEPA / VCP / RS Rating（Mark Minervini 飆股策略）
    '🏆 SEPA Trend Template（7條件+RS≥70）': f_sepa_trend_template,
    '🏆 SEPA Trend Template 7/7（基礎）': f_sepa_trend_template_7of7,
    '🥈 SEPA 6/7（接近過關 watchlist）': f_sepa_partial_6of7,
    '📐 VCP 形態（≥2 次收口+接近 pivot）': f_vcp_pattern,
    '📐 VCP + 量縮 + 接近 pivot（最佳 setup）': f_vcp_volume_dry_pivot,
    '💪 RS Rating ≥ 70（強於前 30%）': f_rs_rating_70,
    '💪 RS Rating ≥ 80（強於前 20%）': f_rs_rating_80,
    '💪 RS Rating ≥ 90（飆股候選 強於前 10%）': f_rs_rating_90,
    '🏆⭐ Minervini 完整 setup（SEPA+VCP+RS≥70）': f_sepa_full_setup,
    '🎯 Pivot 接近突破（VCP+距pivot≤1%）': f_pivot_near_breakout,
    # 🆕 v9.21：雙底雙頂（基本）
    '🟢 雙底突破（W底+突破neckline）': f_double_bottom_breakout,
    '🟢 雙底成形（待突破 watchlist）': f_double_bottom_confirmed,
    '🔴 雙頂跌破（M頂+跌破neckline）': f_double_top_breakdown,
    '🔴 雙頂成形（待跌破警示）': f_double_top_confirmed,
    # 🆕 v9.22：雙底雙頂 Quality 分級（五大關鍵）
    '🥇 雙底 Grade A（五大關鍵全過 5/5）': f_double_bottom_grade_A,
    '🥈 雙底 Grade A+B（≥4/5 高品質）': f_double_bottom_grade_AB,
    '🥇 雙頂 Grade A（五大關鍵全過 5/5）': f_double_top_grade_A,
    '🥈 雙頂 Grade A+B（≥4/5 高品質）': f_double_top_grade_AB,
    # 🆕 v9.22：三段建倉
    '🟢 雙底 A 段試單（底部反應K + 第2底剛現）': f_double_bottom_stage_A,
    '🟢 雙底 C 段補滿（突破後回踩有效）': f_double_bottom_stage_C,
    # 🆕 v9.24：RS Leading High（紫色點訊號 — 機構累積足跡）
    '🟣 RS 領先創新高（任何分數）': f_rs_leading_high_signal,
    '🟣 RS 領先創新高 — 高品質 (score≥60)': f_rs_leading_high_top,
    '🟣 RS 領先創新高 — 紫色點密集 (≥5次)': f_rs_leading_high_purple_5plus,
    '🟣 RS 領先創新高 — Eddy 主題 (AI 儲存/能源)': f_rs_leading_high_eddy_theme,
    '⚡ T1 黃金交叉 sweet spot（5-7天）': f_t1_sweet_spot,
    '🟢 T1 剛黃金交叉（1-10天）': f_t1_fresh,
    '🟢 T3 多頭拉回（RSI<50）': f_t3_pullback,
    '🎯 T1 即將上穿（V7嚴格 距≤1%）': f_t1_imminent_strict,
    '🎯 T1 即將上穿（寬鬆 距≤3%）': f_t1_imminent_loose,

    # 📉 強看空
    '🚨 ★★★★ 三隻烏鴉 + 距高<5% + 量縮': f_three_crows,
    '🚨 ★★★ 空頭吞噬 + RSI≥75 + ADX↓': f_bear_engulf_overbought,
    '⚠️ ★★ 黃昏之星 + RSI≥75': f_evening_star,
    '⛔ 即將死叉警告': f_imminent_dc,

    # 📊 BB 系列（OANDA 10 種）
    '📊 %B > 1.0 (BB 過熱)': f_bb_pctb_overheat,
    '📊 %B < 0 (BB 過冷反彈★)': f_bb_pctb_oversold,
    '📊 BB Squeeze (頻寬連窄)': f_bb_squeeze,
    '📊 BB Squeeze + 多頭（即將向上爆發）': f_bb_squeeze_bull_bias,
    '📊 BB Squeeze + 空頭（即將向下爆發）': f_bb_squeeze_bear_bias,
    '📊 BB Expansion 突放（空頭反轉★）': f_bb_expansion,
    '📊 BB Walking Up（多頭強勢延續）': f_bb_walking_up,
    '📊 BB Walking Down（空頭弱勢延續）': f_bb_walking_down,
    '📊 BB W 底（雙觸下軌反轉★）': f_bb_w_bottom,
    '📊 BB M 頂（雙觸上軌反轉）': f_bb_m_top,

    # 🌱 即將觸發
    '🌱 即將：倒鎚+RSI 26-30': f_imm_inv_hammer_rsi,
    '⚠️ 即將：三隻烏鴉+距高 5-10%': f_imm_three_crows,

    # 📋 基礎篩選
    '✅ 多頭排列（EMA20>EMA60）': f_bull_alignment,
    '❌ 空頭排列（EMA20<EMA60）': f_bear_alignment,
    '🔥 強趨勢（ADX≥30）': f_strong_trend,
    '⚠️ 假多頭（多頭排列但 ADX<22）': f_fake_bull,
    '⚠️ 假空頭（空頭排列但 ADX<22）': f_fake_bear,
    '⏸ 弱趨勢/震盪（ADX<22）': f_weak_trend,
    '📉 RSI < 30 極度超賣': f_rsi_oversold,
    '📈 RSI > 70 過熱': f_rsi_overbought,
    '⬇️ 接近 60 日低點（<5%）': f_at_60d_low,
    '⬆️ 接近 60 日高點（<5%）': f_at_60d_high,
}


def filter_universe(universe, market, filter_names, min_vol=None, min_price=None,
                     logic='AND', exclude_names=None):
    """🆕 v9.13：支援多條件篩選 + 排除條件
    filter_names: 包含條件（list[str]）— 必須符合
    exclude_names: 排除條件（list[str]）— 必須不符合（任一符合就排除）
    logic: include 條件之間用 'AND' / 'OR'
    exclude 條件之間永遠用 OR（任一符合就排除）
    """
    import data_loader as dl

    if isinstance(filter_names, str):
        filter_names = [filter_names]
    if exclude_names is None:
        exclude_names = []
    if isinstance(exclude_names, str):
        exclude_names = [exclude_names]

    fns = []
    for fname in filter_names:
        fn = FILTERS.get(fname)
        if fn is None:
            print(f"  ⚠️ 未知 filter: {fname}")
            continue
        fns.append((fname, fn))
    excl_fns = []
    for fname in exclude_names:
        fn = FILTERS.get(fname)
        if fn is None:
            continue
        excl_fns.append((fname, fn))
    if not fns:
        return []

    if min_vol is None:
        min_vol = 500_000 if market == 'tw' else 1_000_000
    if min_price is None:
        min_price = 5.0

    results = []
    for ticker in universe:
        try:
            df = dl.load_from_cache(ticker)
            if df is None or len(df) < 60: continue
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df = df.copy(); df.index = df.index.tz_localize(None)

            state = _get_state(df, market)
            if state is None: continue

            # 質量過濾
            if state['close'] < min_price: continue
            v_arr = df['Volume'].values
            if len(v_arr) >= 60:
                avg_vol = float(np.mean(v_arr[-60:]))
                if avg_vol < min_vol: continue

            # 多條件邏輯
            if logic == 'AND':
                passed = all(fn(state) for _, fn in fns)
            else:  # OR
                passed = any(fn(state) for _, fn in fns)

            # 🆕 v9.13：排除條件（任一符合就剔除）
            if passed and excl_fns:
                excluded = any(fn(state) for _, fn in excl_fns)
                if excluded:
                    passed = False

            if passed:
                results.append({
                    'ticker': ticker,
                    'market': market,
                    'close': round(state['close'], 2),
                    'rsi': round(state['rsi'], 1) if state.get('rsi') else None,
                    'adx': round(state['adx'], 1) if state.get('adx') else None,
                    'is_bull': state['is_bull'],
                    'cross_days': state.get('cross_days'),
                    'pct_b': round(state['bb_pct_b'], 2) if state.get('bb_pct_b') is not None else None,
                    'from_high': round(state['from_high'], 1),
                    'from_low': round(state['from_low'], 1),
                    'imminent_dc': state.get('imminent_dc', False),
                    'date': state['date'],
                    'matched_filters': [n for n, fn in fns if fn(state)],  # 記錄符合哪些
                })
        except Exception:
            continue

    return results


def intersect_from_json(by_filter_dict, filter_names, logic='AND', exclude_names=None):
    """🆕 v9.13：從預計算 JSON 計算多條件結果
    filter_names: 包含條件（list）
    exclude_names: 排除條件（list，任一符合就剔除）
    """
    if isinstance(filter_names, str):
        filter_names = [filter_names]
    if exclude_names is None:
        exclude_names = []
    if isinstance(exclude_names, str):
        exclude_names = [exclude_names]

    per_filter_map = {}
    for fname in filter_names:
        items = by_filter_dict.get(fname, [])
        per_filter_map[fname] = {r['ticker']: r for r in items}

    if not per_filter_map:
        return []
    if logic == 'AND':
        common = set.intersection(*(set(m.keys()) for m in per_filter_map.values()))
    else:
        common = set.union(*(set(m.keys()) for m in per_filter_map.values()))

    # 🆕 v9.13：套排除條件
    if exclude_names:
        excl_tickers = set()
        for fname in exclude_names:
            items = by_filter_dict.get(fname, [])
            excl_tickers.update(r['ticker'] for r in items)
        common = common - excl_tickers

    out = []
    seen = set()
    for fname in filter_names:
        for ticker, stock in per_filter_map[fname].items():
            if ticker in common and ticker not in seen:
                seen.add(ticker)
                matched = [fn for fn in filter_names if ticker in per_filter_map.get(fn, {})]
                stock_copy = dict(stock)
                stock_copy['matched_filters'] = matched
                out.append(stock_copy)
    return out
