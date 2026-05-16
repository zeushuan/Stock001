"""個股詳細卡渲染 — Stock001 v9.30
=====================================

從 tv_app.py 抽出的純函數（不含 Streamlit 副作用 / 不依賴 module-level UI 程式碼），
讓 tv_app.py 跟 pages/01_intraday.py 共用。

對外 API（與 tv_app 原本完全等價）：
  - 判讀群組：judge_trend / judge_position / judge_momentum / judge_aux
              judge_oscillators / judge_mas
  - 摘要：calc_summary / _calc_aux_summary / compute_momentum_grade
  - Verdict：_rec / apply_cap / badge / get_rec_label
  - 概念股：_load_concept_tags / _get_concepts / _concept_chip_html
  - 渲染：render_detail(ticker, d, groups, group_summs, tsumm, cap, market="",
                         advice_fn=None, concepts_fn=None, news_fn=None,
                         concept_chip_fn=None)
       advice_fn / news_fn 為 optional callbacks（tv_app 傳 get_operation_advice
       / get_news_sentiment 進來；intraday page 可不傳）。

常數：
  GROUP_NAMES / GROUP_COLORS / GROUP_WEIGHTS
  TREND_W / POSITION_W / MOMENTUM_W / AUX_W
  _INVERSE_ETF_TICKERS / _CONCEPT_COLORS
"""
from __future__ import annotations

import streamlit as st


# ════════════════════════════════════════════════════════════════
# 常數
# ════════════════════════════════════════════════════════════════

GROUP_NAMES   = ["趨勢結構", "位置風險", "動能確認", "輔助指標"]
GROUP_WEIGHTS = [40, 30, 20, 10]
GROUP_COLORS  = ["#3b9eff", "#f0a030", "#a060ff", "#7a8899"]

TREND_W    = [5.0, 4.0, 3.0, 3.0, 2.0]   # 合計 17 → 40%
POSITION_W = [4.0, 3.5, 2.5]              # 合計 10 → 30%
MOMENTUM_W = [3.5, 2.5, 2.0, 1.5, 1.5]    # 合計 11 → 20%
AUX_W      = [1.0, 1.0, 1.0, 0.8, 0.7, 0.7,   # 震盪 6 項
              1.2, 1.5, 1.5, 0.5]              # EMA10/EMA60/SMA200/Hull

_INVERSE_ETF_TICKERS = {"00632R", "00633L", "00648U", "00675L", "00676L"}

_CONCEPT_COLORS = [
    "#3b9eff", "#ff6dc8", "#9d6dff", "#10c0c0", "#3dbb6a",
    "#f0a030", "#ff5555", "#7abadd", "#c0a060", "#a060ff",
]


# ════════════════════════════════════════════════════════════════
# 基本判讀 helpers
# ════════════════════════════════════════════════════════════════

def _j(v, lo, hi):
    if v is None: return "中立"
    return "買入" if v < lo else ("賣出" if v > hi else "中立")


def _jz(v):
    if v is None: return "中立"
    return "買入" if v > 0 else ("賣出" if v < 0 else "中立")


def _rec(b, s):
    if   b > s * 2: return "強力買入"
    elif b > s:     return "買入"
    elif s > b * 2: return "強力賣出"
    elif s > b:     return "賣出"
    else:           return "中立"


def fmt(v, d=2):
    return f"{v:.{d}f}" if v is not None else "N/A"


def _jadx(adx, pos, neg):
    """ADX 方向判斷：> 25 且 +DI > -DI = 買入；> 25 且 -DI > +DI = 賣出；其餘中立"""
    if adx is None or pos is None or neg is None:
        return "中立"
    if adx > 25:
        return "買入" if pos > neg else "賣出"
    return "中立"


# ════════════════════════════════════════════════════════════════
# 震盪指標群（informational scoring）
# ════════════════════════════════════════════════════════════════

def judge_oscillators(d: dict) -> list:
    close, bbu, bbl = d["close"], d["bbu"], d["bbl"]

    # 布林 %B
    pct_b = None
    if bbu and bbl and (bbu - bbl) != 0:
        pct_b = (close - bbl) / (bbu - bbl) * 100
    bb_j = ("賣出" if pct_b is not None and pct_b > 100 else
            "買入" if pct_b is not None and pct_b < 0 else "中立")

    # 隨機 %K
    stoch_d  = d.get("stoch_d")
    stoch_d1 = d.get("stoch_d_prev")
    if stoch_d is not None:
        if stoch_d < 20:
            stoch_j = "買入"
        elif stoch_d > 80:
            if stoch_d1 is not None and stoch_d1 <= 80:
                stoch_j = "賣出"
            else:
                stoch_j = "中立"
        else:
            stoch_j = "中立"
    else:
        stoch_j = "中立"

    # AO
    ao, ao1, ao2 = d.get("ao"), d.get("ao_prev"), d.get("ao_prev2")
    if ao is not None and ao1 is not None and ao2 is not None:
        if ao > 0 and ao > ao1 and ao1 < ao2:
            ao_j = "買入"
        elif ao < 0 and ao < ao1 and ao1 > ao2:
            ao_j = "賣出"
        elif ao1 < 0 and ao > 0:
            ao_j = "買入"
        elif ao1 > 0 and ao < 0:
            ao_j = "賣出"
        else:
            ao_j = "中立"
    else:
        ao_j = "中立"

    # 動量
    mom, mom1 = d.get("mom"), d.get("mom_prev")
    if mom is not None and mom1 is not None:
        mom_j = "買入" if mom > mom1 else ("賣出" if mom < mom1 else "中立")
    else:
        mom_j = "中立"

    # StochRSI
    sr  = d.get("stochrsi")
    sr1 = d.get("stochrsi_prev")
    if sr is not None:
        if sr < 20:
            sr_j = "買入"
        elif sr > 80:
            if sr1 is not None and sr1 <= 80:
                sr_j = "賣出"
            else:
                sr_j = "中立"
        else:
            sr_j = "中立"
    else:
        sr_j = "中立"

    # 威廉 %R
    wr  = d.get("willr")
    wr1 = d.get("willr_prev")
    if wr is not None and wr1 is not None:
        if wr1 < -80 and wr >= -80:
            wr_j = "買入"
        elif wr1 > -20 and wr <= -20:
            wr_j = "賣出"
        else:
            wr_j = "中立"
    else:
        wr_j = "中立"

    # 牛熊力度
    bbp, bbp1 = d.get("bbpower"), d.get("bbpower_prev")
    if bbp is not None and bbp1 is not None:
        if bbp1 <= 0 and bbp > 0:
            bbp_j = "買入"
        elif bbp1 >= 0 and bbp < 0:
            bbp_j = "賣出"
        else:
            bbp_j = "中立"
    else:
        bbp_j = "中立"

    # 終極震盪
    uo = d.get("uo")
    uo_j = ("買入" if uo is not None and uo > 70 else
            "賣出" if uo is not None and uo < 30 else "中立")

    return [
        (fmt(d["rsi"]),       _j(d["rsi"],   30,  70)),
        (fmt(d["stoch_k"]),   stoch_j),
        (fmt(d["cci"]),       _j(d["cci"],  -100, 100)),
        (fmt(d["adx"]),       _jadx(d.get("adx"), d.get("adx_pos"), d.get("adx_neg"))),
        (fmt(ao),             ao_j),
        (fmt(mom),            mom_j),
        (fmt(d["macd"]),      _jz(d["macd"])),
        (fmt(sr),             sr_j),
        (fmt(wr),             wr_j),
        (fmt(d["bbpower"]),   bbp_j),
        (fmt(uo),             uo_j),
        (f"{pct_b:.1f}%" if pct_b is not None else "N/A", bb_j),
    ]


def judge_mas(d: dict) -> list:
    close = d["close"]
    keys  = ["ema10","sma10","ema20","sma20","ema30","sma30",
             "ema50","sma50","ema100","sma100","ema200","sma200",
             "ichimoku","vwma","hma"]
    return [(fmt(d[k]),
             "買入" if d[k] is not None and close > d[k] else
             "賣出" if d[k] is not None and close < d[k] else "中立")
            for k in keys]


def calc_summary(items, weights=None):
    """items 可以是 (val, judg) 或 (label, val, judg)"""
    pairs = [(it[-2], it[-1]) if len(it) == 3 else it for it in items]
    if weights is None:
        weights = [1.0] * len(pairs)
    b = sum(w for (_, j), w in zip(pairs, weights) if j == "買入")
    s = sum(w for (_, j), w in zip(pairs, weights) if j == "賣出")
    n = sum(w for (_, j), w in zip(pairs, weights) if j == "中立")
    return round(b, 1), round(s, 1), round(n, 1), _rec(b, s)


# ════════════════════════════════════════════════════════════════
# 四群組判讀
# ════════════════════════════════════════════════════════════════

def judge_trend(d: dict) -> list:
    """趨勢結構 (40%)：趨勢方向/強度/多頭階段/乖離風險/週線結構"""
    close  = d["close"]
    ema20, ema60 = d.get("ema20"), d.get("ema60")
    sma200 = d.get("sma200")
    adx, adx_pos, adx_neg = d.get("adx"), d.get("adx_pos"), d.get("adx_neg")
    adx_prev = d.get("adx_prev")

    ema20_prev = d.get("ema20_prev")
    ema60_prev = d.get("ema60_prev")
    sma200_prev = d.get("sma200_prev")

    # ── 1. 趨勢方向：EMA20/60 交叉 + 斜率 + SMA200 ───────────────
    if ema20 is not None and ema60 is not None:
        e20_rising = (ema20_prev is not None and ema20 > ema20_prev)
        e60_rising = (ema60_prev is not None and ema60 > ema60_prev)
        above200   = (sma200 is None or close > sma200)
        both_up    = e20_rising and e60_rising
        if ema20 > ema60 and both_up and above200:
            dir_val, dir_j = "多頭 (雙線↑)", "買入"
        elif ema20 > ema60 and above200:
            dir_val, dir_j = "多頭", "買入"
        elif ema20 > ema60:
            dir_val, dir_j = "偏多 (MA分歧)", "中立"
        elif ema20 < ema60 and not above200:
            dir_val, dir_j = "空頭", "賣出"
        else:
            dir_val, dir_j = "盤整", "中立"
        s200 = " · 站SMA200" if sma200 and close > sma200 else (" · 跌SMA200" if sma200 else "")
        ema_gap_pct = abs(ema20 - ema60) / ema60 * 100 if ema60 else 999
        if ema20 > ema60 and ema_gap_pct < 2.0:
            cross_warn = " ⚠近死叉"
        elif ema20 < ema60 and ema_gap_pct < 2.0:
            cross_warn = " 📈近黃金交叉"
        else:
            cross_warn = ""
        dir_disp = f"{dir_val}{s200}{cross_warn}"
    else:
        dir_val, dir_j, dir_disp = "N/A", "中立", "N/A"

    # ── 2. 趨勢強度：ADX>25 + +DI/-DI ────────────────────────────
    if adx is not None:
        adx_rising = (adx_prev is not None and adx > adx_prev)
        di_bull = (adx_pos and adx_neg and adx_pos > adx_neg)
        if adx < 20:
            str_val, str_j = f"弱 (ADX {adx:.1f})", "中立"
        elif adx < 25:
            str_val, str_j = f"偏弱 (ADX {adx:.1f}{'↑' if adx_rising else ''})", "中立"
        elif adx < 40:
            str_val = f"中 (ADX {adx:.1f}{'↑' if adx_rising else ''})"
            str_j   = "買入" if di_bull else "賣出"
        elif adx < 55:
            str_val = f"強 (ADX {adx:.1f}{'↑' if adx_rising else ''})"
            str_j   = "買入" if di_bull else "賣出"
        else:
            str_val, str_j = f"過熱 (ADX {adx:.1f})", "中立"
    else:
        str_val, str_j = "N/A", "中立"

    # ── 3. 多頭階段 ─────────────────────────────────────────────
    cross = d.get("ema20_cross_days")
    adx_rising = (adx is not None and adx_prev is not None and adx > adx_prev)
    bbu2, bbl2 = d.get("bbu"), d.get("bbl")
    dev_for_phase = None
    if ema20 and close:
        dev_for_phase = (close - ema20) / ema20 * 100
    is_phase3 = (dev_for_phase is not None and dev_for_phase > 10) or (adx is not None and adx > 50)

    if cross is not None and cross > 0:
        if cross <= 30:
            phase_val = f"Phase1 啟動 (+{cross}日)"
            phase_j   = "買入"
        elif is_phase3:
            phase_val = f"Phase3 加速 (+{cross}日) ⚠禁加碼"
            phase_j   = "中立"
        else:
            phase_val = f"Phase2 主升 (+{cross}日)"
            phase_j   = "買入"
    elif cross is not None and cross < 0:
        phase_val = f"死叉 ({-cross}日前)"
        phase_j   = "賣出"
    else:
        phase_val = "Phase3 加速" if is_phase3 else "無明確交叉"
        phase_j   = "中立"

    # ── 4. 乖離風險 ─────────────────────────────────────────────
    bbu, bbl = d.get("bbu"), d.get("bbl")
    pct_b = ((close - bbl) / (bbu - bbl) * 100
             if bbu and bbl and (bbu - bbl) != 0 else None)
    if ema20:
        dev = (close - ema20) / ema20 * 100
        if dev < 8 and (pct_b is None or pct_b < 65):
            dev_val = f"低 ({dev:+.1f}%)"
            dev_j   = "買入"
        elif dev > 15 or (pct_b is not None and pct_b > 90):
            dev_val = f"過熱 ({dev:+.1f}%) 禁新倉"
            dev_j   = "賣出"
        elif dev > 10 or (pct_b is not None and pct_b > 80):
            dev_val = f"高 ({dev:+.1f}%) 禁加碼"
            dev_j   = "賣出"
        else:
            dev_val = f"中 ({dev:+.1f}%)"
            dev_j   = "中立"
    else:
        dev_val, dev_j = "N/A", "中立"

    # ── 5. 週線結構 ─────────────────────────────────────────────
    wc, wm10, wm20 = d.get("w_close"), d.get("w_ma10"), d.get("w_ma20")
    if wc and wm10 and wm20:
        if wc > wm10 > wm20:
            week_val = "多頭排列 (週MA10>MA20)"
            week_j   = "買入"
        elif wc < wm10 < wm20:
            week_val = "空頭排列 (週MA10<MA20)"
            week_j   = "賣出"
        elif wm10 > wm20:
            week_val = "週MA10>MA20 整理中"
            week_j   = "中立"
        else:
            week_val = "週MA10<MA20 整理中"
            week_j   = "中立"
    else:
        week_val, week_j = "週線資料不足", "中立"

    return [
        ("趨勢方向", dir_disp,  dir_j),
        ("趨勢強度", str_val,   str_j),
        ("多頭階段", phase_val, phase_j),
        ("乖離風險", dev_val,   dev_j),
        ("週線結構", week_val,  week_j),
    ]


def judge_position(d: dict) -> list:
    """位置風險 (30%)：EMA20 乖離 + RSI(14) + 布林 %B"""
    close = d["close"]
    rsi   = d.get("rsi")
    ema20 = d.get("ema20")
    bbu, bbl = d.get("bbu"), d.get("bbl")

    # EMA20 乖離
    dev_pct = ((close - ema20) / ema20 * 100) if ema20 else None
    if dev_pct is not None:
        if dev_pct > 15:
            dev_j, dev_desc = "賣出", f"乖離 {dev_pct:+.1f}% 過熱"
        elif dev_pct > 8:
            dev_j, dev_desc = "賣出", f"乖離 {dev_pct:+.1f}% 高"
        elif dev_pct < -8:
            dev_j, dev_desc = "買入", f"乖離 {dev_pct:+.1f}% 超跌"
        elif abs(dev_pct) < 3:
            dev_j, dev_desc = "買入", f"乖離 {dev_pct:+.1f}% 低"
        else:
            dev_j, dev_desc = "中立", f"乖離 {dev_pct:+.1f}%"
    else:
        dev_j, dev_desc = "中立", "EMA20 乖離"

    # RSI 區間
    if rsi is not None:
        if rsi < 30:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 超賣"
        elif rsi < 40:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 近超賣"
        elif rsi < 55:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 健康"
        elif rsi < 65:
            rsi_j, rsi_desc = "中立", f"RSI {rsi:.1f} 偏高"
        elif rsi < 78:
            rsi_j, rsi_desc = "中立", f"RSI {rsi:.1f} 高位"
        else:
            rsi_j, rsi_desc = "賣出", f"RSI {rsi:.1f} 過熱"
    else:
        rsi_j, rsi_desc = "中立", "RSI"

    # 布林 %B
    pct_b = None
    if bbu and bbl and (bbu - bbl) != 0:
        pct_b = (close - bbl) / (bbu - bbl) * 100
    if pct_b is not None:
        if pct_b < 20:
            bb_j, bb_desc = "買入", f"%B {pct_b:.1f}% 近下軌"
        elif pct_b > 80:
            bb_j, bb_desc = "賣出", f"%B {pct_b:.1f}% 近上軌"
        else:
            bb_j, bb_desc = "中立", f"%B {pct_b:.1f}%"
    else:
        bb_j, bb_desc = "中立", "布林%B"

    return [
        (dev_desc,  f"{dev_pct:+.1f}%" if dev_pct is not None else "N/A", dev_j),
        (rsi_desc,  fmt(rsi) if rsi is not None else "N/A",  rsi_j),
        (bb_desc,   f"{pct_b:.1f}%" if pct_b is not None else "N/A", bb_j),
    ]


def judge_momentum(d: dict) -> list:
    """動能確認 (20%)：MACD零軸+柱體、動量(10)、量能比率、MA10位置"""
    close = d["close"]
    macd       = d.get("macd")
    macd_hist  = d.get("macd_hist")
    macd_hist_prev = d.get("macd_hist_prev")
    mom        = d.get("mom")
    volume     = d.get("volume")
    vol_ma     = d.get("vol_ma20")
    ema10      = d.get("ema10")

    # MACD 零軸
    macd_j = ("買入" if macd and macd > 0 else "賣出" if macd and macd < 0 else "中立")

    # MACD 柱體方向
    if macd_hist is not None and macd_hist_prev is not None:
        if macd_hist > 0 and macd_hist > macd_hist_prev:
            hist_j, hist_disp = "買入", f"{fmt(macd_hist)} ↑放大"
        elif macd_hist > 0:
            hist_j, hist_disp = "中立", f"{fmt(macd_hist)} ↓縮小"
        elif macd_hist < 0:
            hist_j, hist_disp = "賣出", f"{fmt(macd_hist)} 負值"
        else:
            hist_j, hist_disp = "中立", "0附近"
    elif macd_hist is not None:
        hist_j = "買入" if macd_hist > 0 else "賣出" if macd_hist < 0 else "中立"
        hist_disp = fmt(macd_hist)
    else:
        hist_j, hist_disp = "中立", "N/A"

    # 動量(10)
    if mom is not None:
        mom_j = "買入" if mom > 0 else "賣出" if mom < 0 else "中立"
    else:
        mom_j = "中立"

    # 量能比率
    if volume is not None and vol_ma is not None and vol_ma > 0:
        vol_ratio = volume / vol_ma
        if vol_ratio > 1.5:
            vol_j, vol_disp = "買入", f"×{vol_ratio:.1f} 放量"
        elif vol_ratio < 0.7:
            vol_j, vol_disp = "賣出", f"×{vol_ratio:.1f} 縮量"
        else:
            vol_j, vol_disp = "中立", f"×{vol_ratio:.1f}"
    else:
        vol_j, vol_disp = "中立", "N/A"

    # MA10 位置
    ma10_j = ("買入" if ema10 and close > ema10 else
              "賣出" if ema10 and close < ema10 else "中立")

    return [
        ("MACD 零軸",  fmt(macd),   macd_j),
        ("MACD 柱體",  hist_disp,   hist_j),
        ("動量(10)",   fmt(mom),    mom_j),
        ("量能比率",   vol_disp,    vol_j),
        ("MA10 位置",  fmt(ema10),  ma10_j),
    ]


def compute_momentum_grade(d: dict) -> str:
    """直接計算動能評等（A→強力買入 / B→買入 / C→中立 / D→賣出）"""
    close = d["close"]
    macd       = d.get("macd")
    macd_hist  = d.get("macd_hist")
    macd_hist_prev = d.get("macd_hist_prev")
    mom        = d.get("mom")
    volume     = d.get("volume")
    vol_ma     = d.get("vol_ma20")
    ema10      = d.get("ema10")
    ema20      = d.get("ema20")

    macd_pos       = macd is not None and macd > 0
    hist_expanding = (macd_hist is not None and macd_hist_prev is not None
                      and macd_hist > 0 and macd_hist > macd_hist_prev)
    hist_ok        = not (macd_hist is not None and macd_hist_prev is not None
                          and macd_hist > 0 and macd_hist < macd_hist_prev)
    mom_pos    = mom is not None and mom > 0
    vol_strong = (volume is not None and vol_ma is not None and vol_ma > 0
                  and volume > vol_ma * 1.5)
    vol_expand = (volume is not None and vol_ma is not None and vol_ma > 0
                  and volume > vol_ma * 1.2)
    above_ma10 = ema10 is not None and close > ema10
    below_ma20 = ema20 is not None and close < ema20

    if macd_pos and hist_expanding and mom_pos and vol_strong and above_ma10:
        return "強力買入"
    if macd_pos and hist_ok and mom_pos:
        return "買入"
    if (macd is not None and macd < 0 and below_ma20) or \
       (mom is not None and mom < 0 and vol_expand):
        return "賣出"
    return "中立"


def judge_aux(d: dict) -> list:
    """輔助指標 (10%)：震盪指標6項 + 核心均線 EMA10/EMA60/SMA200 + Hull MA"""
    close = d["close"]

    stoch_d, stoch_d1 = d.get("stoch_d"), d.get("stoch_d_prev")
    if stoch_d is not None:
        stoch_j = ("買入" if stoch_d < 20 else
                   "賣出" if stoch_d > 80 and stoch_d1 is not None and stoch_d1 <= 80 else "中立")
    else:
        stoch_j = "中立"

    sr, sr1 = d.get("stochrsi"), d.get("stochrsi_prev")
    sr_j = ("買入" if sr is not None and sr < 20 else
            "賣出" if sr is not None and sr > 80 and sr1 is not None and sr1 <= 80 else "中立")

    wr, wr1 = d.get("willr"), d.get("willr_prev")
    wr_j = ("買入" if wr is not None and wr1 is not None and wr1 < -80 and wr >= -80 else
            "賣出" if wr is not None and wr1 is not None and wr1 > -20 and wr <= -20 else "中立")

    bbp, bbp1 = d.get("bbpower"), d.get("bbpower_prev")
    bbp_j = ("買入" if bbp is not None and bbp1 is not None and bbp1 <= 0 and bbp > 0 else
             "賣出" if bbp is not None and bbp1 is not None and bbp1 >= 0 and bbp < 0 else "中立")

    uo = d.get("uo")
    uo_j = "買入" if uo and uo > 70 else "賣出" if uo and uo < 30 else "中立"

    def mj(k):
        v = d.get(k)
        return ("買入" if v and close > v else "賣出" if v and close < v else "中立")

    hma_j = mj("hma")

    return [
        ("隨機%K",   fmt(d.get("stoch_k")),  stoch_j),
        ("CCI(20)",  fmt(d.get("cci")),       _j(d.get("cci"), -100, 100)),
        ("StochRSI", fmt(d.get("stochrsi")),  sr_j),
        ("威廉%R",   fmt(d.get("willr")),     wr_j),
        ("牛熊力度", fmt(d.get("bbpower")),   bbp_j),
        ("終極震盪", fmt(uo),                 uo_j),
        ("EMA(10)",  fmt(d.get("ema10")),     mj("ema10")),
        ("EMA(60)",  fmt(d.get("ema60")),     mj("ema60")),
        ("SMA(200)", fmt(d.get("sma200")),    mj("sma200")),
        ("Hull MA",  fmt(d.get("hma")),       hma_j),
    ]


def _calc_aux_summary(items, weights):
    """輔助群組：需 ≥4 個同向才計分，否則整群中立"""
    buy_w  = sum(w for (_,_,j), w in zip(items, weights) if j == "買入")
    sell_w = sum(w for (_,_,j), w in zip(items, weights) if j == "賣出")
    neu_w  = sum(w for (_,_,j), w in zip(items, weights) if j == "中立")
    buy_n  = sum(1 for (_,_,j) in items if j == "買入")
    sell_n = sum(1 for (_,_,j) in items if j == "賣出")
    if buy_n >= 4 or sell_n >= 4:
        return round(buy_w,1), round(sell_w,1), round(neu_w,1), _rec(buy_w, sell_w)
    total_w = sum(weights)
    return 0.0, 0.0, round(total_w, 1), "中立"


# ════════════════════════════════════════════════════════════════
# Cap & Verdict
# ════════════════════════════════════════════════════════════════

def apply_cap(verdict: str, d: dict, mom_grade: str = "中立") -> tuple:
    """Hard Limits（寫死，不可被加權覆蓋）"""
    ema20, ema60 = d.get("ema20"), d.get("ema60")
    sma200 = d.get("sma200")
    adx    = d.get("adx")
    close, bbu, bbl = d.get("close"), d.get("bbu"), d.get("bbl")
    rsi    = d.get("rsi")
    w_dev  = d.get("w_dev")
    dev_pct = ((close - ema20) / ema20 * 100) if ema20 and close else None

    # ① 空頭封頂
    if ema20 and ema60 and ema20 < ema60:
        if rsi is not None and rsi < 30:
            return "空頭，不買", (
                f"⚠️ EMA20<EMA60（空頭）｜RSI {rsi:.1f} 極度超賣"
                "，可留意反轉訊號，但未確認前勿進場"
            )
        if rsi is not None and rsi < 32:
            return "空頭，不買", (
                f"⚠️ EMA20<EMA60（空頭）｜RSI {rsi:.1f} 接近進場區(RSI<32)"
                "，觀察 RSI 止跌確認後評估"
            )
        return "空頭，不買", "⚠️ EMA20 < EMA60（空頭趨勢）"

    # ② 過熱禁新倉
    if (dev_pct is not None and dev_pct > 15) or (rsi is not None and rsi > 78):
        note = f"乖離{dev_pct:.1f}%" if dev_pct and dev_pct > 15 else f"RSI{rsi:.1f}"
        return "過熱觀望｜禁止新倉", f"⚠️ {note} 過熱"

    # ③ 週線過熱
    if w_dev is not None and w_dev > 20 and verdict == "強力買入":
        return "買入", f"⚠️ 週線乖離{w_dev:.1f}%（過熱，日線降級）"

    # ④ 動能壓制
    if mom_grade in ("中立", "賣出") and verdict in ("強力買入", "買入"):
        return "中立", f"⚠️ 動能{mom_grade}（壓制整體評等）"

    # ⑤ 乖離 > 10%
    if dev_pct is not None and dev_pct > 10 and verdict == "強力買入":
        return "上限買入｜持有/短線", f"⚠️ EMA20 乖離{dev_pct:.1f}%（Phase3 禁加碼）"

    # ⑥ ADX < 25
    if adx is not None and adx < 25 and verdict == "強力買入":
        return "買入", f"⚠️ ADX {adx:.1f} < 25（趨勢偏弱）"

    return verdict, None


def badge(rec: str) -> str:
    cls_map = {
        "強力買入":        "badge-strong-buy",
        "買入":            "badge-buy",
        "上限買入｜持有/短線": "badge-buy-limit",
        "強力賣出":        "badge-strong-sell",
        "賣出":            "badge-sell",
        "過熱觀望｜禁止新倉": "badge-overheat",
        "空頭，不買":      "badge-bearish",
    }
    cls = cls_map.get(rec, "badge-neutral")
    return f'<span class="badge {cls}">{rec}</span>'


def get_rec_label(d: dict, ticker: str = "") -> tuple:
    """④推薦策略 輕量版：回傳 (rec_name, badge_inline_style)
    與 get_operation_advice() 使用完全相同的決策樹邏輯。"""
    ema20      = d.get("ema20")
    ema60      = d.get("ema60")
    adx        = d.get("adx")
    rsi        = d.get("rsi")
    rsi_prev   = d.get("rsi_prev")
    rsi_prev2  = d.get("rsi_prev2")
    cross_days = d.get("ema20_cross_days")
    atr14      = d.get("atr14")
    close      = d.get("close")

    if ema20 is None or ema60 is None:
        return ("—", "background:#0a1020;color:#7a8899")

    _tk_upper  = ticker.upper().replace(".TW", "").replace(".TWO", "")
    _is_inverse = _tk_upper in _INVERSE_ETF_TICKERS
    _tk_clean = _tk_upper.replace('-USD', '').replace('-', '')
    _is_us = _tk_clean.isalpha() and _tk_clean.isupper() and not _is_inverse
    _is_crypto = _tk_upper.endswith('-USD')
    is_bull    = ema20 > ema60
    _adx_th    = 18 if (_is_us or _is_crypto) else 22
    adx_ok     = (adx is not None and adx >= _adx_th)

    # 即將死叉判斷
    _imminent_dc = False
    if (cross_days is not None and cross_days > 10
            and ema20 is not None and ema60 is not None
            and atr14 is not None and atr14 > 0
            and ema20 > ema60):
        if (ema20 - ema60) < atr14:
            _e20_5d = d.get('ema20_5d_ago')
            ema20_falling = (_e20_5d is not None and ema20 < _e20_5d)
            if ema20_falling:
                _imminent_dc = True
            elif cross_days > 30:
                _imminent_dc = True

    _t1_ok = (cross_days is not None and 0 < cross_days <= 10)
    _t3_ok = (rsi is not None and rsi < 50)
    _entry_blocked_by_dc = (is_bull and adx_ok and _imminent_dc and (_t1_ok or _t3_ok))

    if _entry_blocked_by_dc and not _is_inverse:
        return ("🛑 不進場 即將死叉",
                "background:#2a0a0a;color:#ff7755;border:1px solid #ff775566")

    if _is_inverse:
        if is_bull and adx_ok:
            t1_ok = (cross_days is not None and 0 < cross_days <= 10)
            t3_ok = (rsi is not None and rsi < 50)
            if t1_ok:
                return ("⑦反向ETF T1進場", "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
            elif t3_ok:
                return ("⑦反向ETF T3拉回", "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
            else:
                return ("⑦反向ETF 觀察",   "background:#0a1628;color:#7abadd;border:1px solid #7abadd44")
        elif is_bull and not adx_ok:
            return ("反向ETF 假多頭",       "background:#1a1200;color:#e8a020;border:1px solid #e8a02044")
        else:
            return ("空頭，不買",           "background:#1a0010;color:#ff5555;border:1px solid #ff555544")

    _t4_rising = (rsi is not None and rsi < 32 and
                  rsi_prev is not None and rsi > rsi_prev and
                  rsi_prev2 is not None and rsi_prev > rsi_prev2)

    t4_days = d.get('t4_rising_days', 0) or 0

    if not is_bull:
        if _t4_rising:
            t4_str = f"T4 {t4_days}D 反彈" if t4_days else "T4 反彈"
            return (t4_str,                  "background:#2a1500;color:#ff9944;border:1px solid #ff994455")
        else:
            return ("不操作 — 等待訊號",    "background:#0a1020;color:#7a8899;border:1px solid #7a889944")
    elif not adx_ok:
        return ("不操作 — 假多頭",          "background:#1a1200;color:#e8a020;border:1px solid #e8a02044")
    else:
        _is_strong   = (adx is not None and adx >= 30)
        _is_fresh    = (cross_days is not None and 0 < cross_days <= 10)
        _is_pullback = (rsi is not None and rsi < 50)
        _is_hot      = (rsi is not None and rsi >= 70)

        t1_str = f"T1 {cross_days}D 進場" if cross_days else "T1 進場"

        if _is_strong and _is_fresh:
            return ("🚀 飆股 進場",          "background:#1a1400;color:#f0c030;border:1px solid #f0c03055")
        elif _is_strong and _is_pullback:
            return ("✅ T3 強趨勢拉回",       "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
        elif _is_strong and not _is_pullback and not _is_hot:
            return ("T3 等待拉回",           "background:#0a1628;color:#7abadd;border:1px solid #7abadd44")
        elif not _is_strong and _is_fresh:
            return (t1_str,                 "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
        elif not _is_strong and _is_pullback:
            return ("T3 拉回進場",           "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
        elif _is_hot:
            return ("等待回調 — 不追高",     "background:#1a1805;color:#c8b87a;border:1px solid #c8b87a44")
        else:
            return ("等待 T3 拉回",          "background:#1a1805;color:#c8b87a;border:1px solid #c8b87a44")


# ════════════════════════════════════════════════════════════════
# 概念股 helpers
# ════════════════════════════════════════════════════════════════

@st.cache_data(ttl=86400, show_spinner=False)
def _load_concept_tags() -> dict:
    """主題概念股對照（純 concept_tags.json）"""
    import json as _json
    from pathlib import Path as _P
    p = _P(__file__).parent / 'concept_tags.json'
    if not p.exists(): return {}
    rev = {}
    try:
        data = _json.loads(p.read_text(encoding='utf-8'))
        for market in ('tw', 'us'):
            for concept, tickers in data.get(market, {}).items():
                for t in tickers:
                    rev.setdefault(t, []).append(concept)
        return {t: list(dict.fromkeys(cs)) for t, cs in rev.items()}
    except Exception:
        return {}


def _get_concepts(ticker: str, max_n: int = 5) -> list:
    """取得主題概念股標籤"""
    return _load_concept_tags().get(ticker, [])[:max_n]


def _concept_chip_html(c: str) -> str:
    color = _CONCEPT_COLORS[hash(c) % len(_CONCEPT_COLORS)]
    return (f'<span style="background:{color}22;color:{color};'
            f'border:1px solid {color}66;border-radius:10px;'
            f'padding:1px 7px;margin:1px 3px 1px 0;font-size:.66rem;'
            f'white-space:nowrap;display:inline-block">{c}</span>')


# ════════════════════════════════════════════════════════════════
# 主渲染函數
# ════════════════════════════════════════════════════════════════

def render_detail(ticker, d, groups, group_summs, tsumm, cap,
                   market: str = "",
                   advice_fn=None,
                   news_fn=None,
                   concepts_fn=None,
                   concept_chip_fn=None) -> str:
    """產生個股詳細 HTML

    Args:
        ticker, d, groups, group_summs, tsumm, cap, market: 同原 tv_app 版本
        advice_fn(d, ticker=...) -> HTML string : 操作建議產生器（可不傳）
        news_fn(ticker, market) -> dict : 新聞情感（可不傳，dict 需有 n/avg_score/headlines）
        concepts_fn(ticker, max_n=10) -> list[str] : 概念股取得（可不傳，預設用 _get_concepts）
        concept_chip_fn(c) -> str : 概念股 chip 渲染（可不傳，預設用 _concept_chip_html）
    """
    if concepts_fn is None: concepts_fn = _get_concepts
    if concept_chip_fn is None: concept_chip_fn = _concept_chip_html

    g_trend, g_position, g_momentum, g_aux = groups
    ts_s, ps_s, ms_s, xs_s = group_summs
    tb, ts_, tn_, tr_ = tsumm
    gc = GROUP_COLORS

    def ind(label, val, judg):
        cls = {"買入": "ind-buy", "賣出": "ind-sell"}.get(judg, "ind-neu")
        jcls = {"買入": "color:#3b9eff", "賣出": "color:#ff5555"}.get(judg, "color:#7a8899")
        return (f'<div class="ind-item {cls}">'
                f'<span class="ind-label">{label}</span>'
                f'<div style="display:flex;justify-content:space-between;align-items:baseline;gap:6px">'
                f'<span class="ind-val">{val}</span>'
                f'<span style="font-size:.72rem;font-weight:700;{jcls};flex-shrink:0">{judg}</span>'
                f'</div></div>')

    def group_section(title, color, items, summ):
        b, s, n, r = summ
        header = (
            f'<div style="display:flex;align-items:center;gap:8px;margin:14px 0 8px;'
            f'border-top:1px solid #0f1f33;padding-top:10px;flex-wrap:wrap">'
            f'<span style="color:{color};font-size:.85rem;font-weight:700">{title}</span>'
            f'{badge(r)}'
            f'<span style="color:#8ab0c8;font-size:.75rem">'
            f'買:<b style="color:#3b9eff">{b}</b>&nbsp;'
            f'賣:<b style="color:#ff5555">{s}</b>&nbsp;'
            f'中:<b style="color:#9aaabb">{n}</b></span>'
            f'</div>'
        )
        items_html = "".join(ind(lbl, val, judg) for lbl, val, judg in items)
        return header + f'<div class="ind-grid">{items_html}</div>'

    summary_row = (
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;align-items:center">'
        f'<div style="background:#0a1628;border:1px solid #1a2f48;border-radius:8px;padding:7px 12px">'
        f'<span style="color:#7ab0d0;font-size:.68rem">收盤價 </span>'
        f'<b style="color:#e8f4fd;font-family:\'IBM Plex Mono\',monospace">{fmt(d["close"])}</b></div>'
    )
    for gname, color, pct, summ in zip(GROUP_NAMES[:3], gc[:3], GROUP_WEIGHTS[:3], [ts_s, ps_s, ms_s]):
        b, s, n, r = summ
        summary_row += (
            f'<div style="background:#0a1628;border:1px solid #1a2f48;border-radius:8px;padding:7px 12px">'
            f'<span style="color:{color};font-size:.65rem;font-weight:700">{gname}({pct}%) </span>'
            f'{badge(r)}'
            f'<span style="color:#7a9ab0;font-size:.65rem;margin-left:4px">'
            f'買:<b style="color:#3b9eff">{b}</b> '
            f'賣:<b style="color:#ff5555">{s}</b> '
            f'中:<b style="color:#7a8899">{n}</b></span>'
            f'</div>'
        )
    summary_row += (
        f'<div style="background:#0a1628;border:1px solid #2a3f5f;border-radius:8px;padding:7px 14px">'
        f'<span style="color:#c8dff0;font-size:.68rem;font-weight:700">整體 </span>'
        f'{badge(tr_)}'
        f'<span style="color:#7a9ab0;font-size:.65rem;margin-left:4px">'
        f'買:<b style="color:#3b9eff">{tb}</b> '
        f'賣:<b style="color:#ff5555">{ts_}</b> '
        f'中:<b style="color:#7a8899">{tn_}</b></span>'
        f'</div>'
        f'</div>'
    )

    sections = (
        group_section("趨勢結構", gc[0], g_trend,    ts_s) +
        group_section("位置風險", gc[1], g_position,  ps_s) +
        group_section("動能確認", gc[2], g_momentum,  ms_s)
    )

    # ── 操作建議（optional callback）──
    advice_html = ""
    if advice_fn is not None:
        try:
            advice_html = advice_fn(d, ticker=ticker) or ""
        except Exception:
            advice_html = ""

    # ── 概念股 ──
    concept_html = ""
    try:
        concepts = concepts_fn(ticker, max_n=10) or []
        if concepts:
            chips = "".join(concept_chip_fn(c) for c in concepts)
            concept_html = (
                f'<div style="background:#0a1628;border:1px solid #1a2f48;'
                f'border-radius:8px;padding:8px 12px;margin-bottom:10px">'
                f'<span style="color:#7ab0d0;font-size:.68rem;font-weight:700;'
                f'margin-right:8px">🏷️ 概念股</span>{chips}</div>'
            )
    except Exception:
        pass

    # ── 新聞情感（optional callback）──
    sent_html = ""
    if news_fn is not None and market:
        try:
            sent = news_fn(ticker, market)
            if sent and sent.get('n', 0) > 0:
                avg = sent['avg_score']
                if avg > 0.3:
                    avg_color, avg_label = '#3dbb6a', '🟢 偏正面'
                elif avg > 0.05:
                    avg_color, avg_label = '#88c8a8', '🟢 微正面'
                elif avg < -0.3:
                    avg_color, avg_label = '#ff5555', '🔴 偏負面'
                elif avg < -0.05:
                    avg_color, avg_label = '#e8a020', '🟠 微負面'
                else:
                    avg_color, avg_label = '#7a8899', '⚪ 中性'

                rows_html = ''
                for i, hdr in enumerate(sent.get('headlines', [])):
                    if len(hdr) == 4:
                        title, s, link, pub = hdr
                    else:
                        title, s, link = hdr[:3]
                        pub = ''
                    if s > 0.2:    s_color = '#3dbb6a'
                    elif s > 0.05: s_color = '#88c8a8'
                    elif s < -0.2: s_color = '#ff5555'
                    elif s < -0.05:s_color = '#e8a020'
                    else:          s_color = '#7a8899'
                    pub_str = f'<span style="color:#3a5a7a;font-size:.65rem;margin-left:6px">{pub}</span>' if pub else ''
                    rows_html += (
                        f'<div style="display:flex;align-items:baseline;gap:8px;padding:4px 0;'
                        f'border-bottom:1px solid #0f1f33">'
                        f'<span style="background:{s_color}22;color:{s_color};font-size:.62rem;'
                        f'font-weight:700;padding:1px 5px;border-radius:3px;flex-shrink:0;'
                        f'min-width:46px;text-align:center">{s:+.2f}</span>'
                        f'<a href="{link}" target="_blank" '
                        f'style="color:#c8dff0;font-size:.74rem;text-decoration:none;line-height:1.5;'
                        f'flex:1;overflow:hidden">{title}</a>{pub_str}</div>'
                    )

                sent_html = (
                    f'<div style="background:#0a1628;border:1px solid #1a2f48;'
                    f'border-radius:8px;padding:8px 12px;margin-bottom:10px">'
                    f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">'
                    f'<span style="color:#7abadd;font-size:.78rem;font-weight:700">📰 新聞情感</span>'
                    f'<span style="background:{avg_color}33;color:{avg_color};'
                    f'padding:1px 8px;border-radius:4px;font-size:.7rem;font-weight:700">'
                    f'{avg:+.2f} {avg_label}</span>'
                    f'<span style="color:#5a8ab0;font-size:.65rem">'
                    f'（{sent["n"]} 篇平均）</span>'
                    f'</div>'
                    f'{rows_html}'
                    f'</div>'
                )
        except Exception:
            pass

    return f'<div style="padding:4px 8px">{advice_html}{concept_html}{sent_html}{summary_row}{sections}</div>'
