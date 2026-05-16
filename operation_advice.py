"""操作建議 + 個股詳細卡 banner — Stock001 v9.31
=====================================================

從 tv_app.py 抽出的純函數，內含：
  - 8 個 helper（檔案載入 / 美股盤後 / 接近條件預警 / 反向ETF 建議）
  - get_operation_advice(d, ticker) → 完整操作建議 HTML（含所有 banner）：
    * SEPA / VCP / RS Rating 8 條件診斷
    * 杯柄 / 平台底 / Stan Weinstein Stage v9.28
    * 雙底雙頂進階 + ZigZag chart
    * Sympathy 補漲 / RS 領先創新高
    * 綜合決策得分

讓 tv_app.py 跟 pages/01_intraday.py 共用同一份。

注意：
  - 此 module 用 streamlit 的 cache_data 裝飾器（有 streamlit context 才會生效）
  - 從 detail_card_render import badge / _INVERSE_ETF_TICKERS
"""
from __future__ import annotations

import json
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf

# 從 detail_card_render 取 badge / _INVERSE_ETF_TICKERS
from detail_card_render import badge, _INVERSE_ETF_TICKERS


# ───── _get_us_overnight ─────
@st.cache_data(ttl=1800, show_spinner=False)
def _get_us_overnight() -> dict:
    """抓昨夜美股 SPX/SOX/TSM/VIX 收盤報酬（cache 30 分鐘）
    回傳 dict {symbol: {'close': X, 'change_pct': Y, 'date': 'YYYY-MM-DD'}}"""
    out = {}
    targets = {'^GSPC': 'SPX', '^SOX': 'SOX', 'TSM': 'TSM', '^VIX': 'VIX'}
    for sym, label in targets.items():
        try:
            df = yf.Ticker(sym).history(period='5d', interval='1d', auto_adjust=False)
            if df is None or df.empty or len(df) < 2:
                continue
            close_now = float(df['Close'].iloc[-1])
            close_prev = float(df['Close'].iloc[-2])
            chg = (close_now - close_prev) / close_prev * 100 if close_prev > 0 else 0
            out[label] = {
                'close': round(close_now, 2),
                'change_pct': round(chg, 2),
                'date': df.index[-1].strftime('%Y-%m-%d'),
            }
        except Exception:
            continue
    return out


# ───── _load_us_impact ─────
@st.cache_data(ttl=86400, show_spinner=False)
def _load_us_impact() -> dict:
    """載入 us_impact_on_tw.json — 個股對美股 lag-1 相關"""
    from pathlib import Path as _P
    import json as _json
    p = _P(__file__).parent / 'us_impact_on_tw.json'
    if not p.exists(): return {}
    try:
        return _json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return {}


# ───── _load_per_stock_wf ─────
@st.cache_data(ttl=86400, show_spinner=False)
def _load_per_stock_wf() -> dict:
    """載入 per_stock_walkforward.json — 個股 walk-forward β"""
    from pathlib import Path as _P
    import json as _json
    p = _P(__file__).parent / 'per_stock_walkforward.json'
    if not p.exists(): return {}
    try:
        d = _json.loads(p.read_text(encoding='utf-8'))
        return d.get('all_results', {})
    except Exception:
        return {}


# ───── _load_clusters ─────
@st.cache_data(ttl=86400, show_spinner=False)
def _load_clusters() -> dict:
    """載入 clusters.json — ticker → cluster_id 對應 + cluster 主題"""
    from pathlib import Path as _P
    import json as _json
    p = _P(__file__).parent / 'clusters.json'
    if not p.exists(): return {}
    try:
        d = _json.loads(p.read_text(encoding='utf-8'))
        ticker_to_cid = {}
        cluster_themes = {
            0: ('ABF 載板群', '#ff6dc8'),
            1: ('大型主流群', '#7abadd'),
            2: ('航運四雄群', '#3dbb6a'),
            3: ('PCB 老牌群', '#c8b87a'),
            4: ('記憶體群', '#9d6dff'),
            5: ('AI 概念群', '#ffd700'),
        }
        for cid_str, info in d.get('clusters', {}).items():
            cid = int(cid_str)
            for t in info.get('members', []):
                ticker_to_cid[t] = cid
        return {'ticker_to_cid': ticker_to_cid, 'themes': cluster_themes}
    except Exception:
        return {}


# ───── _get_proximity_alerts ─────
def _get_proximity_alerts(d: dict) -> list:
    """
    產生「接近條件」的預警字串清單（HTML 已格式化）

    預警類型：
      📈 進場預警：
        - T1 即將黃金交叉（EMA20 距 EMA60 < 1.5% 且向上靠近）
        - T3 即將拉回到位（RSI 50~55 下行中）
        - ADX 即將達標（ADX 18~22 上行中）

      📉 出場/停損預警：
        - 接近 EMA 死叉（多頭中，EMA20 距 EMA60 已縮小）
        - 接近 ATR 停損價（價格距近期低點 < 2 ATR）
        - RSI 即將過熱（RSI 70~75 上行中）
        - 黃金交叉天數 > 60 天且 ADX 下降（趨勢可能轉弱）

    回傳：list of (level, html_text)
      level: 'info' / 'warning' / 'danger'
    """
    alerts = []
    e20      = d.get('ema20')
    e60      = d.get('ema60')
    e20_prev = d.get('ema20_prev')
    e60_prev = d.get('ema60_prev')
    rsi      = d.get('rsi')
    rsi_prev = d.get('rsi_prev')
    adx      = d.get('adx')
    adx_prev = d.get('adx_prev')
    close    = d.get('close')
    atr14    = d.get('atr14')

    if not all(v is not None for v in [e20, e60, rsi, close, atr14]):
        return alerts

    is_bull = e20 > e60
    cross_days = d.get('cross_days')

    # ── 進場預警（空頭時觀察）──
    if not is_bull:
        # T1 即將黃金交叉：EMA20 與 EMA60 收斂中
        diff_pct = (e60 - e20) / e60 * 100 if e60 > 0 else 0
        if 0 < diff_pct < 1.5:
            # 進一步確認 EMA20 是否上行
            if e20_prev is not None and e20 > e20_prev:
                alerts.append(('info',
                    f'⏰ <b style="color:#7abadd">T1 黃金交叉預警</b>'
                    f'：EMA20 距 EMA60 僅 <b>{diff_pct:.2f}%</b>'
                    f'（且 EMA20 上行中），可能短期內形成黃金交叉，準備進場條件'))

    # ── T3 拉回預警（多頭中）──
    if is_bull:
        adx_ok = adx is not None and adx >= 22
        if adx_ok and rsi is not None and rsi_prev is not None:
            if 50 < rsi < 55 and rsi < rsi_prev:
                alerts.append(('info',
                    f'⏰ <b style="color:#7abadd">T3 拉回預警</b>'
                    f'：RSI {rsi:.1f} 下行中（前日 {rsi_prev:.1f}），'
                    f'再跌 <b>{rsi - 50:.1f} 點</b>即達 T3 進場條件（RSI&lt;50）'))

    # ── ADX 預警（趨勢即將達標）──
    if adx is not None and adx_prev is not None:
        if 18 <= adx < 22 and adx > adx_prev and is_bull:
            alerts.append(('info',
                f'⏰ <b style="color:#7abadd">ADX 強度預警</b>'
                f'：ADX {adx:.1f} 上行中（前日 {adx_prev:.1f}），'
                f'即將突破 22 門檻啟動策略可進場狀態'))

    # ── 出場預警（多頭中持倉假設）──
    if is_bull:
        # 接近 EMA 死叉
        if e20_prev is not None and e60_prev is not None:
            spread_now  = (e20 - e60) / e60 * 100 if e60 > 0 else 0
            spread_prev = (e20_prev - e60_prev) / e60_prev * 100 if e60_prev > 0 else 0
            if 0 < spread_now < 1.0 and spread_now < spread_prev:
                alerts.append(('warning',
                    f'⚠️ <b style="color:#e8a020">EMA 死叉預警</b>'
                    f'：EMA20/60 差距收斂至 <b>{spread_now:.2f}%</b>（前日 {spread_prev:.2f}%）'
                    f'，若持續收斂可能觸發出場'))

        # 接近 ATR 停損（用近期低點+ATR×2.5 推算）
        # 簡化版：價格距 EMA60 < 1×ATR 視為接近重要支撐
        if e60 > 0 and atr14 > 0:
            dist_e60 = close - e60
            if 0 < dist_e60 < atr14 * 1.0:
                alerts.append(('warning',
                    f'⚠️ <b style="color:#e8a020">支撐預警</b>'
                    f'：收盤距 EMA60 僅 <b>{dist_e60:.1f}</b> 元（&lt; 1×ATR），'
                    f'若跌破恐觸發停損'))

        # RSI 過熱預警（多頭 + 高位）
        if rsi is not None and rsi_prev is not None:
            if 70 <= rsi < 75 and rsi > rsi_prev:
                alerts.append(('warning',
                    f'⚠️ <b style="color:#e8a020">RSI 過熱預警</b>'
                    f'：RSI {rsi:.1f} 上行中接近 75，若 ADX&lt;25 可能觸發出場'))

        # 黃金交叉時間長 + ADX 下降 → 趨勢轉弱
        if cross_days is not None and cross_days > 60:
            if adx is not None and adx_prev is not None and adx < adx_prev:
                alerts.append(('warning',
                    f'⚠️ <b style="color:#e8a020">趨勢轉弱預警</b>'
                    f'：黃金交叉已 {cross_days} 天，ADX 從 {adx_prev:.1f} 降至 {adx:.1f}，'
                    f'趨勢動能減弱，可考慮收緊停損'))

    return alerts


# ───── _get_inverse_etf_advice ─────
def _get_inverse_etf_advice(d, tk, ema20, ema60, adx, rsi, rsi_prev,
                             rsi_prev2, atr14, close, cross_days) -> str:
    """
    反向ETF專屬操作建議卡片。
    邏輯：對此標的自身K線套用 ⑦T1/T3，但：
      - 無T2（已從 v7 移除）
      - 無T4（空頭=大盤多頭，不抓反彈）
      - ATR×1.5（更緊停損）
      - RSI>70 即出場（回測最佳：不限ADX，RSI>70就走）
    EMA黃金交叉在反向ETF上 = 大盤進入空頭，是持有反向ETF的最佳時機。
    """
    is_bull  = ema20 > ema60
    adx_ok   = (adx is not None and adx >= 22)
    rsi_str  = f"{rsi:.1f}" if rsi is not None else "N/A"
    adx_str  = f"{adx:.1f}" if adx is not None else "N/A"

    # 反向ETF 名稱對照
    _inv_names = {
        "00632R": "台灣50反1", "00633L": "台灣50正2",
        "00648U": "標普500正2", "00675L": "中國A50正2", "00676L": "中國A50反1",
    }
    inv_name = _inv_names.get(tk, "反向/槓桿ETF")

    # ── 頂部說明橫幅 ─────────────────────────────────────────────
    inv_banner = (
        f'<div style="background:#0a1a0a;border:1px solid #1a6030;border-radius:6px;'
        f'padding:8px 12px;margin-bottom:10px;font-size:.76rem">'
        f'<span style="color:#40c070;font-weight:700">🔄 反向ETF策略模式｜{tk}（{inv_name}）</span><br>'
        f'<span style="color:#90d0a0">'
        f'此標的與大盤<b>反向</b>連動。當大盤出現死亡交叉，此標的出現<b>黃金交叉</b>，才是進場時機。<br>'
        f'策略調整：<b>無T4空頭反彈</b>（空頭=大盤多頭，不宜持有反向ETF）｜'
        f'<b>ATR×1.5嚴格停損</b>｜<b>RSI&gt;70即出場</b>（回測：此條件最小虧損）'
        f'</span>'
        f'</div>'
    )

    # ── ① 環境判斷 ──────────────────────────────────────────────
    if cross_days is not None and cross_days > 0:
        cross_txt = f"，黃金交叉 {cross_days} 天前（= 大盤死亡交叉 {cross_days} 天前）"
    elif cross_days is not None and cross_days < 0:
        cross_txt = f"，死亡交叉 {abs(cross_days)} 天前（= 大盤黃金交叉，反向ETF趨勢結束）"
    else:
        cross_txt = ""

    if is_bull and adx_ok:
        env_color, env_icon = "#40c070", "✅"
        env_tag  = "反向ETF多頭（大盤空頭期）"
        env_desc = (f"EMA20 &gt; EMA60{cross_txt}｜ADX {adx_str} ≥ 22（趨勢有效）<br>"
                    f'<span style="color:#90d0a0;font-size:.73rem">'
                    f'⚡ 此為操作反向ETF的黃金時機：大盤正在下跌，持有此標的可獲利</span>')
    elif is_bull and not adx_ok:
        env_color, env_icon = "#e8c030", "⚠️"
        env_tag  = "反向ETF多頭但趨勢弱"
        env_desc = (f"EMA20 &gt; EMA60{cross_txt}，但 ADX {adx_str} &lt; 22，趨勢強度不足。"
                    f"可能是大盤短暫回檔而非真正空頭，等待 ADX ≥ 22 確認後再進場")
    else:
        env_color, env_icon = "#ff5555", "🚫"
        env_tag  = "反向ETF空頭（大盤多頭期）"
        env_desc = (f"EMA20 &lt; EMA60{cross_txt}，此時大盤處於多頭，<b>反向ETF持續下跌</b>。"
                    f"⑦策略不進場，等待反向ETF出現黃金交叉（= 大盤空頭確立）")

    # ── ② 進場判斷 ──────────────────────────────────────────────
    entry_rows = []
    t1_ok = t3_ok = False
    if is_bull and adx_ok:
        t1_ok = (cross_days is not None and 0 < cross_days <= 10)
        t1c   = "#40c070" if t1_ok else "#4a6070"
        t1d   = f"{cross_days} 天前" if (cross_days and cross_days > 0) else "尚未發生"
        entry_rows.append(
            f'<div style="display:flex;gap:6px;align-items:baseline">'
            f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
            f'font-size:.65rem;color:#5a9acf;white-space:nowrap">T1 黃金交叉</span>'
            f'<span style="color:{t1c}">{"✅" if t1_ok else "⬜"} {t1d}'
            f'{"　← 積極進場（大盤剛進入空頭）" if t1_ok else ""}</span></div>'
        )
        t3_ok = (rsi is not None and rsi < 50)
        t3c   = "#40c070" if t3_ok else "#4a6070"
        t3_gap = f"（還差 {50 - rsi:.1f} 點）" if (rsi is not None and not t3_ok) else ""
        entry_rows.append(
            f'<div style="display:flex;gap:6px;align-items:baseline">'
            f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
            f'font-size:.65rem;color:#5a9acf;white-space:nowrap">T3 回調進場</span>'
            f'<span style="color:{t3c}">{"✅" if t3_ok else "⬜"} RSI {rsi_str}'
            f' {"< 50 回調到位" if t3_ok else f"≥ 50，等待回調{t3_gap}"}'
            f'{"　← 可進場" if t3_ok else ""}</span></div>'
        )
        entry_rows.append(
            f'<div style="color:#7a8899;font-size:.73rem">🚫 T4空頭反彈：<b>停用</b>'
            f'（反向ETF在EMA空頭期 = 大盤多頭，不宜進場）</div>'
        )
    elif is_bull and not adx_ok:
        entry_rows.append(
            f'<div style="color:#e8c030">ADX {adx_str} &lt; 22，趨勢強度不足，等待 ADX ≥ 22 後進場</div>'
        )
    else:
        entry_rows.append(
            f'<div style="color:#7a8899">反向ETF處於下跌趨勢（= 大盤多頭），不進場。'
            f'等待 EMA 黃金交叉出現（= 大盤開始反轉下跌）</div>'
        )

    # 動作標籤
    if is_bull and adx_ok and (t1_ok or t3_ok):
        action_label, action_bg, action_fg = "進場條件達成 ✅", "#0d2a10", "#40c070"
    elif is_bull and adx_ok:
        action_label, action_bg, action_fg = "等待回調進場",    "#0a1628", "#7a9ab0"
    elif is_bull:
        action_label, action_bg, action_fg = "ADX不足，觀望",   "#1a1a05", "#e8c030"
    else:
        action_label, action_bg, action_fg = "大盤多頭，不操作", "#1a0505", "#ff5555"

    # ── ③ 出場/停損 ─────────────────────────────────────────────
    risk_rows = []
    # 停損：ATR×1.5（比一般更緊）
    if atr14 is not None and close is not None and close > 0:
        stop_dist  = atr14 * 1.5
        stop_price = close - stop_dist
        stop_pct   = stop_dist / close * 100
        risk_rows.append(
            f'<div>🛡️ <b>停損價 <span style="color:#ff7a7a">{stop_price:.2f}</span></b>'
            f'&nbsp;<span style="color:#7a8899">（收盤 {close:.2f} − ATR×1.5 {stop_dist:.2f}'
            f' = -{stop_pct:.1f}%）反向ETF衰減特性，停損比一般更緊</span></div>'
        )
    else:
        risk_rows.append('<div style="color:#7a8899">ATR 資料不足，無法計算動態停損</div>')

    if is_bull:
        ema_gap_pct = (ema20 - ema60) / ema60 * 100 if ema60 else None
        if ema_gap_pct is not None and ema_gap_pct < 1.0:
            risk_rows.append(
                f'<div>⚠️ <span style="color:#ff9944"><b>出場警示</b>：EMA20/60 差距僅'
                f' {ema_gap_pct:.2f}%，接近死亡交叉（= 大盤即將反彈），隨時準備出場！</span></div>'
            )
        else:
            gap_s = f"{ema_gap_pct:.1f}%" if ema_gap_pct is not None else "N/A"
            risk_rows.append(
                f'<div>📌 <span style="color:#7abadd">出場條件①：EMA 死亡交叉時出場（目前差距 {gap_s}）</span></div>'
            )
        # RSI>70 出場（回測結論：不限ADX，RSI>70就出場最佳，-9.49% vs ADX<25→RSI>65的-19.57%）
        rsi_exit_color = "#e8c030" if (rsi is not None and rsi >= 65) else "#7a8899"
        risk_rows.append(
            f'<div>📌 <span style="color:{rsi_exit_color}">出場條件②：RSI &gt; 70 即出場'
            f'（回測驗證最佳，不限ADX；目前 RSI {rsi_str}）</span></div>'
        )
    else:
        risk_rows.append(
            '<div><span style="color:#7abadd">📌 轉多條件：等待 EMA 黃金交叉（此標的）= 大盤開始下跌</span></div>'
        )

    # ── 組合 HTML ────────────────────────────────────────────────
    label_tag = (
        f'<span style="background:{action_bg};color:{action_fg};'
        f'border:1px solid {action_fg}44;border-radius:4px;'
        f'padding:2px 9px;font-size:.72rem;font-weight:700;margin-left:8px">'
        f'{action_label}</span>'
    )
    sec_style = "display:flex;gap:8px;align-items:flex-start;margin-bottom:6px"
    tag_style = ("background:#0a1e30;border-radius:4px;padding:1px 7px;"
                 "font-size:.68rem;font-weight:700;color:#5a9acf;"
                 "white-space:nowrap;margin-top:2px")
    val_style = "font-size:.78rem;line-height:1.8;color:#c8dff0"

    # 反向ETF 預警（用相同函式，但邏輯仍適用：黃金交叉預警 = 大盤死叉預警）
    inv_proximity = _get_proximity_alerts(d)
    inv_alert_html = ""
    if inv_proximity:
        ainv_lines = []
        for level, txt in inv_proximity:
            bg = "#0d1f30" if level == 'info' else "#1a1505"
            border = "#2a4060" if level == 'info' else "#5a4a10"
            ainv_lines.append(
                f'<div style="background:{bg};border:1px solid {border};'
                f'border-radius:4px;padding:4px 10px;margin-top:3px;font-size:.72rem">'
                f'{txt}</div>'
            )
        inv_alert_html = (
            f'<div style="margin-top:10px;border-top:1px solid #1a3055;padding-top:8px">'
            f'<div style="font-size:.7rem;color:#5a8ab0;margin-bottom:4px">'
            f'🔔 接近條件預警</div>'
            f'{"".join(ainv_lines)}'
            f'</div>'
        )

    return (
        f'<div style="background:#050e1a;border:1px solid #1a4030;border-radius:8px;'
        f'padding:10px 14px;margin-bottom:12px">'
        f'{inv_banner}'
        f'<div style="font-size:.82rem;font-weight:700;color:#40c070;margin-bottom:8px">'
        f'🔄 ⑦ 反向ETF專屬策略{label_tag}</div>'
        f'<div style="{sec_style}">'
        f'<span style="{tag_style}">①市場環境</span>'
        f'<div style="{val_style}">'
        f'<span style="color:{env_color};font-weight:700">{env_icon} {env_tag}</span>'
        f'&nbsp;<span style="color:#8ab0c8">{env_desc}</span>'
        f'</div></div>'
        f'<div style="{sec_style}">'
        f'<span style="{tag_style}">②進場判斷</span>'
        f'<div style="{val_style}">{"".join(entry_rows)}</div>'
        f'</div>'
        f'<div style="{sec_style.replace("margin-bottom:6px","")}">'
        f'<span style="{tag_style}">③出場停損</span>'
        f'<div style="{val_style}">{"".join(risk_rows)}</div>'
        f'</div>'
        f'{inv_alert_html}'
        f'</div>'
    )


# ───── _SPECIAL_TICKER_WARN ─────
_SPECIAL_TICKER_WARN = {
    "00633L": ("2倍槓桿ETF（台灣50正2）",
               "2倍正向槓桿，波動劇烈且有衰減成本。適用⑦反向ETF版策略規則：ATR×1.5嚴格停損，RSI>70即出場（回測最佳）。"),
    "00648U": ("原油正2ETF",
               "商品槓桿ETF，受期貨轉倉成本侵蝕，不適合長期持有。建議以短線波段操作為主。"),
}


# ───── render_confidence_dots ─────
def render_confidence_dots(score: int, max_score: int = 5,
                            color_filled: str = '#3dbb6a',
                            color_empty: str = '#3a5a7a',
                            size: str = '.7rem') -> str:
    """T3 信心度視覺化：● + ⚪
    score 0-5（每命中 1 分）
    """
    if score is None: score = 0
    score = max(0, min(score, max_score))
    filled = '●' * score
    empty = '○' * (max_score - score)
    return (f'<span title="T3 信心度 {score}/{max_score}" '
            f'style="font-size:{size};letter-spacing:1px;font-family:monospace">'
            f'<span style="color:{color_filled}">{filled}</span>'
            f'<span style="color:{color_empty}">{empty}</span></span>')


# ───── get_operation_advice ─────
def get_operation_advice(d: dict, ticker: str = "") -> str:
    """
    依 ⑦自適應趨勢 框架輸出 HTML 操作建議卡片。
    回傳空字串表示無資料可顯示。
    ticker 用於特殊標的警告。
    """
    ema20      = d.get("ema20")
    ema60      = d.get("ema60")
    adx        = d.get("adx")
    rsi        = d.get("rsi")
    rsi_prev   = d.get("rsi_prev")
    rsi_prev2  = d.get("rsi_prev2")          # T4：連續2天上升確認用
    atr14      = d.get("atr14")
    close      = d.get("close")
    sma200     = d.get("sma200")
    cross_days = d.get("ema20_cross_days")   # +N=黃金交叉N天前, -N=死亡交叉N天前

    if ema20 is None or ema60 is None:
        return ""

    # ── 風險評估指標（給策略風險匹配檢查用）──────────────────
    _rel_atr_global = (atr14 / close * 100) if (atr14 and close and close > 0) else 0
    _is_high_vol    = _rel_atr_global > 5.0     # 飆股級高波動（ATR/P > 5%）
    _ext_200        = (close / sma200) if (close and sma200 and sma200 > 0) else None
    _is_extended    = _ext_200 is not None and _ext_200 > 1.40   # 距 SMA200 > 40% = 過度延伸
    # 距 EMA60 的 ATR 倍數（< 1.0 表示停損點極近 = 弱支撐）
    _ema60_atr_dist = ((close - ema60) / atr14) if (atr14 and atr14 > 0 and ema60 and close) else None
    _weak_support   = _ema60_atr_dist is not None and 0 < _ema60_atr_dist < 1.0

    # ── 🆕 接刀風險檢查（B 方案：只警告，不修改交易邏輯）─────
    # 觸發：(已死叉<30天 OR 即將死叉) + 從60日高點跌≥15% + %B<0.10
    bbu_x  = d.get("bbu")
    bbl_x  = d.get("bbl")
    high60 = d.get("high60")
    pct_b_now = None
    if bbu_x and bbl_x and (bbu_x - bbl_x) > 0 and close is not None:
        pct_b_now = (close - bbl_x) / (bbu_x - bbl_x)
    # 已死叉（過去 30 天內）
    _just_dead_cross = (cross_days is not None and -30 <= cross_days < 0)
    # 即將死叉：仍多頭但 EMA20 距 EMA60 < 1 ATR（隨時可能交叉）
    # 🆕 v9.10s：排除「剛黃金交叉」的 gap 自然小情境
    #   bug 修復：cross_days=1 黃金交叉初期 gap 必小，不該誤判成即將死叉
    #   真正即將死叉 = (1) gap 小 (2) cross_days > 10 已多頭一陣
    #                  (3) EMA20 不上升或下彎（gap 收窄中而非剛擴張）
    _imminent_dc = False
    if (cross_days is not None and cross_days > 10  # cross_days 從 >0 改成 >10
            and ema20 is not None and ema60 is not None
            and atr14 is not None and atr14 > 0
            and ema20 > ema60):
        if (ema20 - ema60) < atr14:
            # 額外條件：EMA20 5 日下行（gap 真的在收窄）
            _e20_5d = d.get('ema20_5d_ago')
            ema20_falling = (_e20_5d is not None and ema20 < _e20_5d)
            # gap 小 + cross_days>10 + EMA20 下行 → 才是真正即將死叉
            if ema20_falling:
                _imminent_dc = True
            # 退而求其次：cross_days > 30（多頭很久 gap 還是小）
            elif cross_days > 30:
                _imminent_dc = True
    _knife_dc_zone = _just_dead_cross or _imminent_dc
    # 🆕 v9.11：把 _entry_blocked_by_dc 提前算（讓後面所有訊號都可以參考）
    # 注意：此時 t1_ok / t3_ok 還沒算，先用 cross_days/rsi 預估
    _t1_ok_pre = (cross_days is not None and 0 < cross_days <= 10
                  and ema20 is not None and ema60 is not None and ema20 > ema60
                  and adx is not None and adx >= 22)
    _t3_ok_pre = (rsi is not None and rsi < 50
                  and ema20 is not None and ema60 is not None and ema20 > ema60
                  and adx is not None and adx >= 22)
    _is_bull_pre = ema20 is not None and ema60 is not None and ema20 > ema60
    _entry_blocked_by_dc = (_is_bull_pre and _imminent_dc and (_t1_ok_pre or _t3_ok_pre))
    _drawdown_pct = ((high60 - close) / high60 * 100) if (high60 and close and high60 > 0) else None
    _knife_drawdown = (_drawdown_pct is not None and _drawdown_pct >= 15)
    _knife_at_lowband = (pct_b_now is not None and pct_b_now < 0.10)
    _is_falling_knife = _knife_dc_zone and _knife_drawdown and _knife_at_lowband

    # 讀取使用者選擇的策略風格（用於「策略風險匹配」檢查）
    try:
        _active_style = st.session_state.get('active_strategy') or {}
        _active_mode  = _active_style.get('mode', '')
    except Exception:
        _active_mode = ''
    # 風險偏好分類（v9.10n 修正：⭐ 最佳 不視為保守）
    # 「最佳」風格：含 VWAPEXEC（TW 最佳）或 ADX18（US 最佳）的調校版
    # 這兩個風格 RR 高，足以承受飆股，不該被歸保守觸發警告
    _is_best_style = ('VWAPEXEC' in _active_mode) or \
                     ('ADX18' in _active_mode and 'P10' in _active_mode)
    _is_conservative_style = (
        not _is_best_style and
        any(k in _active_mode for k in ('POS+IND+DXY', 'POS+DXY', 'WRSI+WADX'))
    )
    # 主動「進攻」/「平衡」style：只有 POS（無 DXY/IND）或純 P0_T1T3
    _is_aggressive_style = _active_mode in ('P0_T1T3', 'P0_T1T3+POS') or \
                           ('+RL' in _active_mode)

    # ── ⓪ 標的分類（反向ETF 走專屬邏輯）────────────────────────────
    special_banner = ""
    _tk_upper = ticker.upper().replace(".TW", "").replace(".TWO", "")
    _is_inverse = _tk_upper in _INVERSE_ETF_TICKERS

    # 反向ETF → 直接轉入專屬建議，不走一般 ⑦ 邏輯
    if _is_inverse:
        return _get_inverse_etf_advice(d, _tk_upper, ema20, ema60, adx, rsi, rsi_prev,
                                       rsi_prev2, atr14, close, cross_days)

    if _tk_upper in _SPECIAL_TICKER_WARN:
        _kind, _warn_msg = _SPECIAL_TICKER_WARN[_tk_upper]
        special_banner = (
            f'<div style="background:#1a0a00;border:1px solid #c05000;border-radius:6px;'
            f'padding:8px 12px;margin-bottom:10px;font-size:.76rem">'
            f'<span style="color:#ff8040;font-weight:700">⚠️ 特殊標的警告｜{_kind}</span><br>'
            f'<span style="color:#f0c090">{_warn_msg}</span>'
            f'</div>'
        )

    # 🆕 v9.10i：偵測 US vs TW，套用各自最佳閾值
    _tk_upper = ticker.upper().replace(".TW", "").replace(".TWO", "")
    _tk_clean = _tk_upper.replace('-USD', '').replace('-', '')
    _is_us = _tk_clean.isalpha() and _tk_clean.isupper() and \
             _tk_upper not in _INVERSE_ETF_TICKERS
    _is_crypto = _tk_upper.endswith('-USD')
    # 美股 / 加密：ADX 18（依美股研究 P10+POS+ADX18 RR 0.496 局部最佳）
    # 台股：ADX 22（依台股 P5+VWAPEXEC 預設）
    _adx_th = 18 if (_is_us or _is_crypto) else 22
    _market_tag = '🇺🇸 US' if _is_us else ('🪙 Crypto' if _is_crypto else '🇹🇼 TW')

    is_bull  = ema20 > ema60
    adx_ok   = (adx is not None and adx >= _adx_th)
    rsi_str  = f"{rsi:.1f}" if rsi is not None else "N/A"
    adx_str  = f"{adx:.1f}" if adx is not None else "N/A"

    # T4 條件：RSI<35（與回測一致）+ 連續2天上升
    _t4_rsi_oversold = (rsi is not None and rsi < 35)
    _t4_rsi_rising = (rsi is not None and rsi_prev is not None and rsi > rsi_prev
                      and rsi_prev2 is not None and rsi_prev > rsi_prev2)
    _t4_rising = (not is_bull) and _t4_rsi_oversold and _t4_rsi_rising

    # ── 🆕 v9.10x：K 線型態強警報（看空 + 看多）────────────
    bear_alert_html = ""
    bull_alert_html = ""
    _kline_patterns = d.get('kline_patterns', []) or []
    # 找最近 1-2 天內出現的型態
    _recent_patterns = {p.get('name'): p for p in _kline_patterns
                         if p.get('days_ago', 99) <= 1}

    # ─── 🚀 強看多警報（底部反轉 + 過濾條件）───
    # 對所有股票都適用（不需 is_bull）
    if rsi is not None and atr14 is not None:
        adx_prev = d.get('adx_prev')
        adx_rising = (adx is not None and adx_prev is not None
                       and adx > adx_prev)
        # 跌深判斷：距 60d 低 / SMA200
        _from_low_pct = ((close - d.get('low60', close)) /
                         (d.get('low60', close) or 1) * 100
                         if d.get('low60') else 99)
        _below_sma200 = ((close / sma200 - 1) * 100
                         if sma200 and sma200 > 0 else 0)
        _extended_down = _below_sma200 < -25

        # ★★★★★ 極強看多：倒鎚 + RSI≤25 + ADX 上升
        if ('INV_HAMMER' in _recent_patterns and rsi <= 25 and adx_rising):
            bull_alert_html = (
                f'<div style="background:#0a2018;border:2px solid #3dbb6a;'
                f'padding:8px 12px;margin:6px 0;border-radius:6px">'
                f'<div style="color:#3dbb6a;font-weight:700;font-size:.9rem">'
                f'🚀🚀 極強看多警報 ★★★★★ — 三重底部訊號</div>'
                f'<div style="color:#a8d8a8;font-size:.78rem;margin-top:3px">'
                f'• 倒鎚 ({_recent_patterns["INV_HAMMER"].get("days_ago", 0)} 天前)<br/>'
                f'• RSI {rsi:.1f} ≤ 25（極度超賣）<br/>'
                f'• ADX 上升中（趨勢轉強）<br/>'
                f'<b>實證：n=1223 / 漲機率 71.4% / 30d 均報 +9.36%</b>'
                f'</div></div>'
            )
        # ★★★★ 強看多：倒鎚 + 距 SMA200<-25% (跌深)
        elif ('INV_HAMMER' in _recent_patterns and _extended_down):
            bull_alert_html = (
                f'<div style="background:#0a2018;border:2px solid #3dbb6a;'
                f'padding:7px 11px;margin:5px 0;border-radius:5px">'
                f'<div style="color:#3dbb6a;font-weight:700;font-size:.85rem">'
                f'🚀 強看多警報 ★★★★ — 倒鎚 + 跌深</div>'
                f'<div style="color:#a8d8a8;font-size:.75rem;margin-top:2px">'
                f'倒鎚 + 距 SMA200 {_below_sma200:+.1f}%（跌深 >25%）｜'
                f'實證 n=2589 / 漲機率 64.5% / 30d 均報 +7.85%'
                f'</div></div>'
            )
        # ★★★ 中強看多：底部十字星 + RSI≤25 + ADX 上升
        elif ('DOJI' in _recent_patterns and rsi <= 25 and adx_rising):
            bull_alert_html = (
                f'<div style="background:#0d1825;border:1px solid #7abadd;'
                f'padding:6px 10px;margin:5px 0;border-radius:4px">'
                f'<div style="color:#7abadd;font-weight:700;font-size:.85rem">'
                f'⚡ 中強看多警報 ★★★ — 底部十字星 + 超賣</div>'
                f'<div style="color:#a8c8d8;font-size:.75rem;margin-top:2px">'
                f'底部十字星 + RSI {rsi:.1f} ≤ 25 + ADX 上升 ｜'
                f'實證 n=2920 / 漲機率 67.4% / 30d 均報 +7.02%'
                f'</div></div>'
            )
    # 條件：多頭 + 高位（距 60d 高 < 10%）
    _drawdown = d.get('drawdown_pct', 100) or 100
    _at_top = _drawdown < 10
    if is_bull and _at_top:
        # 強空頭警報 1: 空頭吞噬 + RSI≥75 + ADX 下降
        adx_prev = d.get('adx_prev')
        adx_falling = (adx is not None and adx_prev is not None and adx < adx_prev)
        if 'BEAR_ENGULF' in _recent_patterns and rsi is not None and rsi >= 75 and adx_falling:
            bear_alert_html = (
                f'<div style="background:#2a0a0a;border:2px solid #ff5555;'
                f'padding:8px 12px;margin:6px 0;border-radius:6px">'
                f'<div style="color:#ff5555;font-weight:700;font-size:.9rem">'
                f'🚨 強空頭警報 ★★★ — 三重條件達成</div>'
                f'<div style="color:#ff9090;font-size:.78rem;margin-top:3px">'
                f'• 空頭吞噬 ({_recent_patterns["BEAR_ENGULF"].get("days_ago", 0)} 天前)<br/>'
                f'• RSI {rsi:.1f} ≥ 75（極度過熱）<br/>'
                f'• ADX 下降中（趨勢轉弱）<br/>'
                f'<b>實證：n=30 / 跌機率 60% / 30天均報 -0.24%</b>'
                f'</div></div>'
            )
        # 強空頭警報 2: 三隻烏鴉 + 距 60d 高 < 5% + 量縮
        vol = d.get('volume') or 0
        vol_ma20 = d.get('vol_ma20') or 0
        vol_dry = (vol_ma20 > 0 and vol / vol_ma20 < 0.7)
        if ('THREE_CROWS' in _recent_patterns and _drawdown < 5 and vol_dry):
            bear_alert_html = (
                f'<div style="background:#2a0a0a;border:2px solid #ff3333;'
                f'padding:8px 12px;margin:6px 0;border-radius:6px">'
                f'<div style="color:#ff3333;font-weight:700;font-size:.9rem">'
                f'🚨🚨 極強空頭警報 ★★★★ — 三隻烏鴉 + 高位量縮</div>'
                f'<div style="color:#ff9090;font-size:.78rem;margin-top:3px">'
                f'• 三隻烏鴉 ({_recent_patterns["THREE_CROWS"].get("days_ago", 0)} 天前)<br/>'
                f'• 距 60d 高僅 {_drawdown:.1f}%（極高位）<br/>'
                f'• 量縮（vol/MA20 = {vol/vol_ma20:.2f}）<br/>'
                f'<b>實證：n=7 / 跌機率 71% / 30天均報 -1.26%</b>'
                f'</div></div>'
            )
        # 中度警報: 黃昏之星 + RSI≥75
        elif 'EVENING_STAR' in _recent_patterns and rsi is not None and rsi >= 75:
            bear_alert_html = (
                f'<div style="background:#1a1208;border:1px solid #e8a020;'
                f'padding:6px 10px;margin:5px 0;border-radius:4px">'
                f'<div style="color:#e8a020;font-weight:700;font-size:.85rem">'
                f'⚠️ 中度空頭警報 ★★ — 黃昏之星 + 過熱</div>'
                f'<div style="color:#e8c878;font-size:.75rem;margin-top:2px">'
                f'黃昏之星 + RSI {rsi:.1f} ≥ 75 ｜ 實證 n=10 / 跌機率 70%'
                f'</div></div>'
            )

    # ── 🆕 v9.10t：美股盤後預警 + 美股連動度（僅 TW 個股）────────
    # 警報順序：強看多 → 強看空 → 雲端美股盤後 → 連動度
    us_alert_html = bull_alert_html + bear_alert_html
    if not (_is_us or _is_crypto):
        try:
            _us_data = _get_us_overnight()
            _impact = _load_us_impact()
            _wf = _load_per_stock_wf()
            _t_impact = _impact.get('per_ticker', {}).get(ticker, {}) if _impact else {}
            _t_wf = _wf.get(ticker, {}) if _wf else {}

            # 🆕 v9.10u：個股所屬 Cluster 群組標籤
            _clu_data = _load_clusters()
            if _clu_data:
                _cid = _clu_data.get('ticker_to_cid', {}).get(ticker)
                if _cid is not None:
                    _theme = _clu_data.get('themes', {}).get(_cid)
                    if _theme:
                        _theme_name, _theme_color = _theme
                        us_alert_html += (
                            f'<div style="background:#0d1825;border-left:3px solid '
                            f'{_theme_color};padding:5px 10px;margin:4px 0;'
                            f'border-radius:3px;font-size:.78rem">'
                            f'<span style="color:{_theme_color};font-weight:700">'
                            f'🎯 群組標籤：{_theme_name}</span>'
                            f' <span style="color:#7a8899">'
                            f'（K-means cluster {_cid} / 行為相近主題）</span>'
                            f'</div>'
                        )

            if _us_data:
                # 計算預估今日跳空（依 SOX β）
                sox_data = _us_data.get('SOX')
                sox_beta = (_t_wf.get('train_beta') or
                            _t_impact.get('SOX', {}).get('beta'))
                pred_str = ""
                if sox_data and sox_beta:
                    pred = sox_beta * sox_data['change_pct']
                    pred_color = ('#3dbb6a' if pred > 0.3 else
                                  '#ff5555' if pred < -0.3 else '#7a8899')
                    pred_str = (f' → 預估 <b style="color:{pred_color}">'
                                f'{pred:+.2f}%</b>')

                # 整體市場跳空機率（基於 spx 漲跌規則）
                spx_data = _us_data.get('SPX')
                gap_prob_str = ""
                if spx_data:
                    chg = spx_data['change_pct']
                    if chg > 2:
                        gap_prob_str = ' ｜大盤跳空高開機率 <b>92.5%</b>'
                    elif chg > 0.5:
                        gap_prob_str = ' ｜大盤跳空高開機率 <b>87.4%</b>'
                    elif chg < -2:
                        gap_prob_str = ' ｜大盤跳空低開機率 <b>94.8%</b>'
                    elif chg < -0.5:
                        gap_prob_str = ' ｜大盤跳空低開機率 <b>75.5%</b>'

                # 顯示美股盤後快訊
                us_lines = []
                for label in ['SPX', 'SOX', 'TSM', 'VIX']:
                    d_us = _us_data.get(label)
                    if not d_us: continue
                    chg = d_us['change_pct']
                    color = ('#3dbb6a' if chg > 0.5 else
                             '#ff5555' if chg < -0.5 else '#7a8899')
                    icon = '↑' if chg > 0 else '↓' if chg < 0 else '─'
                    us_lines.append(
                        f'<span style="color:{color};font-weight:700">'
                        f'{label} {icon} {chg:+.2f}%</span>'
                    )

                _date_str = sox_data.get('date', '') if sox_data else ''
                us_alert_html = (
                    f'<div style="background:#0a1830;border:1px solid #5a8ab055;'
                    f'border-radius:6px;padding:8px 10px;margin:6px 0;'
                    f'font-size:.78rem">'
                    f'<div style="color:#7abadd;font-weight:700;margin-bottom:3px">'
                    f'🌃 美股盤後快訊（{_date_str} 收盤）{gap_prob_str}</div>'
                    f'<div style="color:#a8cce8">'
                    + ' ｜ '.join(us_lines) +
                    f'{pred_str}</div>'
                    f'</div>'
                )

            # 美股連動度（靜態歷史相關）
            if _t_impact:
                sox_corr = _t_impact.get('SOX', {}).get('corr', 0)
                sox_r2 = _t_impact.get('SOX', {}).get('r2', 0)
                spx_corr = _t_impact.get('SPX', {}).get('corr', 0)
                vix_corr = _t_impact.get('VIX', {}).get('corr', 0)

                if abs(sox_corr) >= 0.05:
                    if sox_r2 > 0.10:
                        impact_label = '⚡ 受美股強影響'
                        impact_color = '#e8a020'
                    elif sox_r2 > 0.05:
                        impact_label = '📊 受美股中度影響'
                        impact_color = '#7abadd'
                    else:
                        impact_label = '🛡️ 受美股影響低'
                        impact_color = '#7a8899'

                    test_r2 = _t_wf.get('test_r2', 0)
                    mae_imp = _t_wf.get('mae_improve_pct', 0)
                    wf_str = ''
                    if test_r2 > 0:
                        wf_str = (f'<br><span style="color:#7a8899;font-size:.7rem">'
                                  f'  Walk-forward (2024-2026): Test R² '
                                  f'<b>{test_r2:.3f}</b>，MAE 改善 '
                                  f'<b>{mae_imp:+.1f}%</b></span>')

                    us_alert_html += (
                        f'<div style="background:#0d1825;border-left:3px solid '
                        f'{impact_color};padding:6px 10px;margin:4px 0;'
                        f'border-radius:3px;font-size:.78rem">'
                        f'<span style="color:{impact_color};font-weight:700">'
                        f'{impact_label}</span> '
                        f'<span style="color:#a8cce8">'
                        f'SOX 相關 <b>{sox_corr:+.3f}</b> (R² {sox_r2:.3f}) ｜ '
                        f'SPX <b>{spx_corr:+.3f}</b> ｜ '
                        f'VIX <b>{vix_corr:+.3f}</b></span>'
                        f'{wf_str}</div>'
                    )
        except Exception:
            pass

    # ── ① 環境判斷 ────────────────────────────────────────────
    if not is_bull:
        # 空頭：細分嚴重程度
        if cross_days is not None and cross_days < 0:
            cross_txt = f"，死亡交叉 {abs(cross_days)} 天前"
        else:
            cross_txt = ""
        if _t4_rising:
            env_color, env_icon = "#ff9944", "🟡"
            env_tag   = "空頭 — 超賣反彈觀察（T4）"
            env_desc  = (f"EMA20 &lt; EMA60{cross_txt}｜RSI {rsi_str} &lt; 35 且<b>連續2天止跌回升</b>"
                         f"（{rsi_prev2:.1f}→{rsi_prev:.1f}→{rsi_str}），T4反彈條件達成（ATR×2.0嚴格停損）")
        elif _t4_rsi_oversold:
            env_color, env_icon = "#ff9944", "🔴"
            env_tag   = "空頭 — 極度超賣"
            env_desc  = (f"EMA20 &lt; EMA60{cross_txt}｜RSI {rsi_str} 極度超賣，"
                         f"等待 RSI 止跌回升確認後再評估")
        else:
            env_color, env_icon = "#ff5555", "🚫"
            env_tag   = "空頭市場"
            env_desc  = (f"EMA20 &lt; EMA60{cross_txt}，"
                         f"⑦策略不進場（回測：空頭持有均值 -26%，觀望保本率最高）")
    elif not adx_ok:
        env_color, env_icon = "#e8a020", "⚠️"
        env_tag   = "假多頭警告"
        env_desc  = (f"EMA20 &gt; EMA60，但 ADX {adx_str} &lt; {_adx_th}，趨勢強度不足"
                     f"（{_market_tag} 門檻 ADX≥{_adx_th}）")
    else:
        # 🆕 v9.10k：依市場顯示 Sweet Spot / 早鳥期 標記
        # （cross_days 研究：TW Day 5-7 sweet spot / US Day 1-5 早鳥期）
        if cross_days is not None and cross_days > 0:
            if _is_us or _is_crypto:
                # 美股 / Crypto：早鳥越早越好
                if 1 <= cross_days <= 5:
                    sweet_tag = " <span style='color:#3dbb6a;font-weight:700'>⚡ 早鳥期</span>"
                elif 6 <= cross_days <= 10:
                    sweet_tag = " <span style='color:#e8a020'>⚠️ 已過早鳥（衰減 -17%）</span>"
                else:
                    sweet_tag = " <span style='color:#7a8899'>（過 T1 窗）</span>"
            else:
                # 台股：Day 5-7 sweet spot
                if 5 <= cross_days <= 7:
                    sweet_tag = " <span style='color:#3dbb6a;font-weight:700'>⭐ Sweet Spot</span>"
                elif 1 <= cross_days <= 4:
                    sweet_tag = " <span style='color:#7abadd'>🌱 偏早（等趨勢確認）</span>"
                elif 8 <= cross_days <= 15:
                    sweet_tag = " <span style='color:#7abadd'>仍可進場</span>"
                else:
                    sweet_tag = " <span style='color:#7a8899'>（過 T1 窗）</span>"
            # 🆕 v9.11：T1 觸發至今的 % 變化
            _cross_pct = d.get('cross_change_pct')
            _perf_inline = ""
            if _cross_pct is not None:
                _pcolor = '#3dbb6a' if _cross_pct >= 0 else '#ff5555'
                _psign = '+' if _cross_pct >= 0 else ''
                _perf_inline = (f" <span style='color:{_pcolor};font-weight:700'>"
                                f"累計 {_psign}{_cross_pct:.2f}%</span>")
            if cross_days <= 10:
                cross_info = (f"<b style='color:#3dbb6a'>黃金交叉 {cross_days} 天前 🔥</b>"
                              f"{sweet_tag}{_perf_inline}｜")
            else:
                cross_info = f"黃金交叉 {cross_days} 天前{sweet_tag}{_perf_inline}｜"
        else:
            cross_info = ""
        env_color, env_icon = "#3b9eff", "✅"
        env_tag   = "多頭市場"
        env_desc  = f"{cross_info}EMA20 &gt; EMA60｜ADX {adx_str} ≥ {_adx_th}（{_market_tag} 趨勢有效）"

    # ── 🆕 估值參考（EPS/PER/PBR/殖利率 + PER 動量） ─────────────
    val_rows = []
    per_v = d.get('per')
    pbr_v = d.get('pbr')
    div_v = d.get('div_yield')
    eps_v = d.get('eps_ttm')
    per_60d_chg = d.get('per_60d_chg_pct')      # PER 60 日變化 %
    per_vs_med90 = d.get('per_vs_med90')         # PER 相對 90 日中位 %

    if per_v is not None or pbr_v is not None:
        # PER 顏色判定
        if per_v is None:
            pe_color = '#7a8899'; pe_label = '—'
        elif per_v <= 0 or per_v > 100:
            pe_color = '#ff5555'; pe_label = '虧損 / 過高'
        elif per_v < 10:
            pe_color = '#3dbb6a'; pe_label = '偏低（價值/警訊）'
        elif per_v <= 20:
            pe_color = '#3dbb6a'; pe_label = '合理偏低'
        elif per_v <= 30:
            pe_color = '#c8b87a'; pe_label = '合理'
        elif per_v <= 50:
            pe_color = '#e8a020'; pe_label = '偏高（成長股）'
        else:
            pe_color = '#ff5555'; pe_label = '過熱'

        per_str = f'{per_v:.1f}' if per_v else '—'
        pbr_str = f'{pbr_v:.2f}' if pbr_v else '—'
        div_str = f'{div_v:.2f}%' if div_v else '—'
        eps_str = f'{eps_v:.2f}' if eps_v else '—'

        val_rows.append(
            f'<div style="background:#0a1a2a;border-left:3px solid {pe_color};'
            f'padding:6px 10px;margin:5px 0;border-radius:3px">'
            f'<span style="color:#8ab8d8;font-size:.78rem;font-weight:700">'
            f'💰 估值參考</span>　'
            f'<span style="color:#c8d8e8;font-size:.82rem">'
            f'EPS(TTM) <b style="color:#fff">{eps_str}</b>　│　'
            f'PER <b style="color:{pe_color}">{per_str}</b> '
            f'<span style="color:#7a8899">({pe_label})</span>　│　'
            f'PBR <b>{pbr_str}</b>　│　'
            f'殖利率 <b>{div_str}</b>'
            f'</span></div>'
        )

        # PER 動量（盈餘上修信號）
        if per_60d_chg is not None:
            if per_60d_chg < -15:
                mom_color = '#3dbb6a'
                mom_label = '🔻 PER 顯著下降 → 盈餘上修中（強多頭信號）'
            elif per_60d_chg < -5:
                mom_color = '#c8b87a'
                mom_label = '↘ PER 緩降 → 盈餘溫和上修'
            elif per_60d_chg > 15:
                mom_color = '#ff5555'
                mom_label = '🔺 PER 擴張 → 盈餘下修風險（小心）'
            elif per_60d_chg > 5:
                mom_color = '#e8a020'
                mom_label = '↗ PER 緩升 → 估值偏熱'
            else:
                mom_color = None
                mom_label = None

            if mom_color:
                rel_str = ''
                if per_vs_med90 is not None:
                    rel_str = (f'　│　vs 90日中位 '
                               f'<b style="color:{"#3dbb6a" if per_vs_med90 < 0 else "#e8a020"}">'
                               f'{per_vs_med90:+.1f}%</b>')
                val_rows.append(
                    f'<div style="background:#0a1a2a;border-left:3px solid {mom_color};'
                    f'padding:6px 10px;margin:5px 0;border-radius:3px">'
                    f'<span style="color:{mom_color};font-size:.82rem">'
                    f'<b>📊 PER 動量</b>　60 日變化 '
                    f'<b>{per_60d_chg:+.1f}%</b>　│　{mom_label}'
                    f'{rel_str}'
                    f'</span></div>'
                )

    # 🆕 券資比 區塊（軋空潛力 / 過熱警告）
    msratio = d.get('msratio')
    ms_60d_chg = d.get('msratio_60d_chg_pct')
    margin_b = d.get('margin_balance')
    short_b = d.get('short_balance')
    if msratio is not None:
        if msratio < 5:
            ms_color, ms_label = '#7a8899', '低（多頭主導）'
        elif msratio < 15:
            ms_color, ms_label = '#3dbb6a', '中（健康）'
        elif msratio < 30:
            ms_color, ms_label = '#c8b87a', '中高（軋空潛力）'
        elif msratio < 50:
            ms_color, ms_label = '#e8a020', '高（強軋空候選）'
        else:
            ms_color, ms_label = '#ff5555', '極高（過熱 / 風險）'

        chg_str = ''
        if ms_60d_chg is not None:
            if ms_60d_chg >= 50:
                chg_color = '#3dbb6a'
                chg_str = f'　│　60d <b style="color:{chg_color}">+{ms_60d_chg:.0f}% 🔥 空方加碼</b>'
            elif ms_60d_chg >= 20:
                chg_str = f'　│　60d <b>+{ms_60d_chg:.0f}%</b>'
            elif ms_60d_chg <= -30:
                chg_str = f'　│　60d <b style="color:#7a8899">{ms_60d_chg:+.0f}% 空方退場</b>'
        bal_str = ''
        if margin_b and short_b:
            bal_str = f'　│　融資 {margin_b:,} / 融券 {short_b:,}'

        val_rows.append(
            f'<div style="background:#0a1a2a;border-left:3px solid {ms_color};'
            f'padding:6px 10px;margin:5px 0;border-radius:3px">'
            f'<span style="color:{ms_color};font-size:.82rem">'
            f'<b>⚖️ 券資比</b>　<b style="font-size:.95rem">{msratio:.2f}%</b>'
            f' <span style="color:#7a8899">({ms_label})</span>{chg_str}{bal_str}'
            f'</span></div>'
        )

    # ── ② 進場判斷（三觸發，僅多頭+ADX≥門檻 有效；TW=22 / US=18）────
    entry_rows  = list(val_rows)  # 估值放在進場判斷頭部
    t1_ok = t3_ok = t2_ok = False

    # 🆕 v9.11：阻擋進場警告 — 顯示在最頂部，後續所有訊號失效
    if _entry_blocked_by_dc:
        entry_rows.append(
            f'<div style="background:#2a0a0a;border:2px solid #ff5555;'
            f'padding:10px 14px;margin:6px 0;border-radius:6px">'
            f'<div style="color:#ff7755;font-weight:700;font-size:.95rem;'
            f'margin-bottom:4px">'
            f'🛑 阻擋進場：即將死叉</div>'
            f'<div style="color:#ffaaaa;font-size:.78rem;line-height:1.5">'
            f'EMA20 距 EMA60 已縮到 1 ATR 以內 + 多頭超過 30 天，'
            f'隨時可能死叉。<b>下方所有「可進場」「進場建議」「立即進場」訊號全部失效，</b>'
            f'此時若進場將陷入「進場 → 立刻死叉 → 出場」的尷尬。<br>'
            f'<b style="color:#ffd070">建議：等死叉發生後再評估，或改觀察 T4 反彈條件</b>'
            f'</div></div>'
        )

    # 🆕 v9.10l：TW 研究發現的「順勢延續」訊號（與 US 相反）
    if is_bull and not (_is_us or _is_crypto) and adx_ok:
        # 過度延伸：距 EMA60 > 3 ATR 反而 RR +0.015（強者恆強）
        if _ema60_atr_dist is not None and _ema60_atr_dist > 3:
            entry_rows.append(
                f'<div style="background:#0a1825;border-left:4px solid #5a9acf;'
                f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                f'<span style="color:#5a9acf;font-weight:700;font-size:.85rem">'
                f'🚀 強勢延續訊號 ⭐</span><br>'
                f'<span style="color:#a8c8d8;font-size:.78rem">'
                f'距 EMA60 <b>{_ema60_atr_dist:.1f} ATR</b>（>3 達標）｜'
                f'🇹🇼 研究：T1+過度延伸 RR 0.052 vs baseline 0.038 = <b>+0.015（37%）</b>，'
                f'16265 樣本平均 +2.50%（強者恆強，非過熱避坑）</span></div>'
            )

    # 🆕 v9.10q：跌深反彈訊號 — 依市場 + 跌幅級距分層
    # （取代 v9.10l 一律 ≥15% 的簡化邏輯）
    # TW 研究：跌得越深越強（30-50% RR 0.266 / >50% RR 0.445）
    # US 研究：30-50% 反而負 RR -0.025（基本面壞）
    if is_bull and _drawdown_pct is not None and _drawdown_pct >= 15:
        if not (_is_us or _is_crypto):
            # 🇹🇼 TW 跌深訊號（級距分層）
            if _drawdown_pct >= 50:
                entry_rows.append(
                    f'<div style="background:#0d1f0d;border-left:4px solid #ffd700;'
                    f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                    f'<span style="color:#ffd700;font-weight:700;font-size:.85rem">'
                    f'💎 重挫鑽石期 ★★★★</span><br>'
                    f'<span style="color:#e8d878;font-size:.78rem">'
                    f'從 60d 高點跌 <b>{_drawdown_pct:.1f}%</b>（≥50% 重挫達標）｜'
                    f'🇹🇼 研究：30 天平均 +23.9% / RR <b>0.445（11.7 倍 baseline）</b>'
                    f'，極端超賣後反彈最強</span></div>'
                )
            elif _drawdown_pct >= 30:
                entry_rows.append(
                    f'<div style="background:#0a2018;border-left:4px solid #3dbb6a;'
                    f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                    f'<span style="color:#3dbb6a;font-weight:700;font-size:.85rem">'
                    f'🔥 深跌黃金期 ★★★</span><br>'
                    f'<span style="color:#a8d8a8;font-size:.78rem">'
                    f'從 60d 高點跌 <b>{_drawdown_pct:.1f}%</b>（≥30% 深跌達標）｜'
                    f'🇹🇼 研究：30 天平均 +12.5% / 勝率 82.4% / RR <b>0.266（7 倍 baseline）</b>'
                    f'，5957 樣本</span></div>'
                )
            elif _drawdown_pct >= 20:
                entry_rows.append(
                    f'<div style="background:#0d1825;border-left:4px solid #7abadd;'
                    f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                    f'<span style="color:#7abadd;font-weight:700;font-size:.85rem">'
                    f'📉 中跌反彈訊號 ★★</span><br>'
                    f'<span style="color:#a8c8d8;font-size:.78rem">'
                    f'從 60d 高點跌 <b>{_drawdown_pct:.1f}%</b>（20-30% 中跌）｜'
                    f'🇹🇼 研究：勝率 67% / 均報 +7% / RR <b>0.141（3.7 倍 baseline）</b>'
                    f'，20637 樣本</span></div>'
                )
            else:
                entry_rows.append(
                    f'<div style="background:#0d1825;border-left:4px solid #5a8ab0;'
                    f'padding:5px 10px;margin:3px 0;border-radius:3px">'
                    f'<span style="color:#7a9ab0;font-size:.78rem">'
                    f'📉 淺跌 {_drawdown_pct:.1f}%（15-20%）｜🇹🇼 RR 0.045 微正</span></div>'
                )
        else:
            # 🇺🇸 US 跌深訊號（注意：30-50% 反向）
            if 15 <= _drawdown_pct < 20:
                entry_rows.append(
                    f'<div style="background:#0a2018;border-left:4px solid #3dbb6a;'
                    f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                    f'<span style="color:#3dbb6a;font-weight:700;font-size:.85rem">'
                    f'📉 淺跌反彈訊號 ★</span><br>'
                    f'<span style="color:#a8d8a8;font-size:.78rem">'
                    f'從 60d 高點跌 <b>{_drawdown_pct:.1f}%</b>（15-20% 淺跌）｜'
                    f'🇺🇸 研究：勝率 56% / RR 0.043（baseline 0.025 → +0.018）'
                    f'，越早進越好（Day 0-1 最佳）</span></div>'
                )
            elif 20 <= _drawdown_pct < 30:
                entry_rows.append(
                    f'<div style="background:#1a1408;border-left:4px solid #e8a020;'
                    f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                    f'<span style="color:#e8a020;font-weight:700;font-size:.85rem">'
                    f'⚠️ 中跌觀望（20-30%）</span><br>'
                    f'<span style="color:#e8c878;font-size:.78rem">'
                    f'從 60d 高點跌 <b>{_drawdown_pct:.1f}%</b>｜'
                    f'🇺🇸 研究：勝率僅 50% / RR 0.033（中性）'
                    f'，需配合 RSI<30(目前{rsi:.1f}) + 多頭(目前{"是" if is_bull else "否"}) 才能加分</span></div>'
                )
            elif 30 <= _drawdown_pct < 50:
                entry_rows.append(
                    f'<div style="background:#2a0a0a;border-left:4px solid #ff5555;'
                    f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                    f'<span style="color:#ff5555;font-weight:700;font-size:.85rem">'
                    f'🚫 深跌警告 (30-50%)</span><br>'
                    f'<span style="color:#ff9090;font-size:.78rem">'
                    f'從 60d 高點跌 <b>{_drawdown_pct:.1f}%</b>｜'
                    f'🇺🇸 研究：勝率 44% / 均報 <b>-1.95%</b> / RR <b>-0.025（負！）</b>'
                    f'，機構市場 30%+ 跌幅多為基本面壞，不是逢低買進</span></div>'
                )
            else:  # >= 50
                entry_rows.append(
                    f'<div style="background:#0a1825;border-left:4px solid #5a9acf;'
                    f'padding:6px 10px;margin:4px 0;border-radius:3px">'
                    f'<span style="color:#5a9acf;font-weight:700;font-size:.85rem">'
                    f'💎 極端反彈（>50%）★</span><br>'
                    f'<span style="color:#a8c8d8;font-size:.78rem">'
                    f'從 60d 高點跌 <b>{_drawdown_pct:.1f}%</b>｜'
                    f'🇺🇸 研究：勝率 38% / 均報 +5.4% / RR 0.065'
                    f'（極端超賣終究反彈，但勝率偏低）</span></div>'
                )

    # 🇺🇸 高波動 alpha（v9.10l 保留）
    if is_bull and (_is_us or _is_crypto) and _is_high_vol:
        entry_rows.append(
            f'<div style="background:#1a1408;border-left:4px solid #e8a020;'
            f'padding:6px 10px;margin:4px 0;border-radius:3px">'
            f'<span style="color:#e8a020;font-weight:700;font-size:.85rem">'
            f'⚡ 高波動 alpha ★★</span><br>'
            f'<span style="color:#e8c878;font-size:.78rem">'
            f'ATR/P <b>{_rel_atr_global:.1f}%</b>（≥5% 達標）｜'
            f'🇺🇸 研究：T1+此條件 RR 0.107 vs baseline 0.025 = <b>+0.082（4.3 倍）</b>，'
            f'4528 樣本平均報酬 +8.35%</span></div>'
        )

    # 🇹🇼 跌深 + T1 王炸組合（最強）
    if (is_bull and adx_ok and not (_is_us or _is_crypto)
            and _drawdown_pct is not None and _drawdown_pct >= 15
            and cross_days is not None and 0 < cross_days <= 10):
        entry_rows.append(
            f'<div style="background:#1a1500;border-left:4px solid #ffd700;'
            f'padding:6px 10px;margin:4px 0;border-radius:3px">'
            f'<span style="color:#ffd700;font-weight:700;font-size:.9rem">'
            f'🎰 王炸組合：跌深 + T1 + 多頭 + ADX 達標 ⭐⭐⭐</span><br>'
            f'<span style="color:#e8d878;font-size:.78rem">'
            f'跌 {_drawdown_pct:.1f}% + 黃金交叉 {cross_days} 天前 + ADX {adx_str} ｜'
            f'🇹🇼 研究：RR <b>0.224</b>（baseline 0.038 → 5.9 倍）'
            f'，1839 樣本平均 +8.04%</span></div>'
        )

    if is_bull and adx_ok:
        # T1：黃金交叉（距今 ≤ 10 天）——新多頭啟動，積極進場
        t1_ok = (cross_days is not None and 0 < cross_days <= 10)
        # 🆕 v9.11：被 dc 阻擋時，t1_ok 顯示為「條件成立但已否決」
        t1c   = ("#7a8899" if _entry_blocked_by_dc else "#3dbb6a") if t1_ok else "#4a6070"
        t1d   = f"{cross_days} 天前" if (cross_days and cross_days > 0) else "尚未發生"
        # 🆕 v9.11：T1 觸發至今 % 變化
        cross_pct = d.get('cross_change_pct')
        cross_close = d.get('cross_day_close')
        t1_perf = ""
        if cross_days and cross_days > 0 and cross_pct is not None:
            _color = "#3dbb6a" if cross_pct >= 0 else "#ff5555"
            _sign = "+" if cross_pct >= 0 else ""
            cross_close_str = f' (從 {cross_close:.2f} → {close:.2f})' if cross_close else ''
            t1_perf = (f' <span style="color:{_color};font-weight:600;font-size:.78rem">'
                        f'累計 {_sign}{cross_pct:.2f}%</span>'
                        f'<span style="color:#5a7a99;font-size:.7rem">{cross_close_str}</span>')
        if t1_ok and _entry_blocked_by_dc:
            t1_action = '　<s style="color:#7a8899">← 積極進場</s> <b style="color:#ff7755">（已被即將死叉否決）</b>'
        elif t1_ok:
            t1_action = "　← 積極進場"
        else:
            t1_action = ""
        entry_rows.append(
            f'<div style="display:flex;gap:6px;align-items:baseline">'
            f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
            f'font-size:.65rem;color:#5a9acf;white-space:nowrap">T1 黃金交叉</span>'
            f'<span style="color:{t1c}">{"✅" if t1_ok else "⬜"} {t1d}{t1_perf}{t1_action}</span></div>'
        )

        # T3：多頭拉回 RSI < 50——停損後再入場 / 回調機會
        t3_ok = (rsi is not None and rsi < 50)
        t3c   = ("#7a8899" if _entry_blocked_by_dc else "#3dbb6a") if t3_ok else "#4a6070"
        if rsi is not None:
            t3_gap = f"（還差 {50 - rsi:.1f} 點）" if not t3_ok else ""
        else:
            t3_gap = ""
        if t3_ok and _entry_blocked_by_dc:
            t3_action = '　<s style="color:#7a8899">← 可進場</s> <b style="color:#ff7755">（已被即將死叉否決）</b>'
        elif t3_ok:
            t3_action = "　← 可進場"
        else:
            t3_action = ""
        entry_rows.append(
            f'<div style="display:flex;gap:6px;align-items:baseline">'
            f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
            f'font-size:.65rem;color:#5a9acf;white-space:nowrap">T3 多頭拉回</span>'
            f'<span style="color:{t3c}">{"✅" if t3_ok else "⬜"} RSI {rsi_str}'
            f' {"< 50 拉回到位" if t3_ok else f"≥ 50，等待拉回{t3_gap}"}{t3_action}</span></div>'
        )

        # 🆕 v9.9t：T3 信心度（5 個指標命中數）— 只在 T3 拉回（RSI<50）時顯示
        # T1（黃金交叉）/ 飆股（強趨勢+新交叉）狀態下，T3 信心度不適用
        _t3_conf = d.get('t3_confidence', 0) or 0
        _t3_hits = d.get('t3_confidence_hits', []) or []
        _t3_relevant = t3_ok or (rsi is not None and 50 <= rsi < 65)  # T3 觸發或等待中
        if _t3_relevant:
            # 5 個檢查項：close>EMA20 / EMA20上升 / EMA5上升 / EMA5>EMA20 / 雙均線都升
            _checks = [
                ('close > EMA20',      'close>EMA20'  in _t3_hits),
                ('EMA20 5 日上升',     'EMA20上升'    in _t3_hits),
                ('EMA5 5 日上升',      'EMA5上升'     in _t3_hits),
                ('EMA5 > EMA20（多頭排列）', 'EMA5>EMA20' in _t3_hits),
                ('EMA5+EMA20 都上升',  '雙均線都升'   in _t3_hits),
            ]
            _check_rows = ''
            for label, hit in _checks:
                ic = '✅' if hit else '⬜'
                col = '#3dbb6a' if hit else '#5a7a99'
                _check_rows += (
                    f'<div style="font-size:.7rem;color:{col};padding:1px 0">'
                    f'{ic} {label}</div>')

            if _t3_conf >= 4:    _conf_color = '#3dbb6a'; _conf_label = '高信心 ✨'
            elif _t3_conf >= 2:  _conf_color = '#c8b87a'; _conf_label = '中信心'
            else:                _conf_color = '#7a8899'; _conf_label = '低信心 ⚠️'

            entry_rows.append(
                f'<div style="display:flex;gap:6px;align-items:baseline;margin-top:3px">'
                f'<span style="background:#0a1628;border-radius:3px;padding:0 5px;'
                f'font-size:.65rem;color:#7a9ab0;white-space:nowrap">📊 T3 信心度</span>'
                f'<span style="color:{_conf_color};font-weight:700">'
                f'{render_confidence_dots(_t3_conf, color_filled=_conf_color, size=".82rem")} '
                f'{_t3_conf}/5　{_conf_label}</span></div>'
                f'<div style="margin:2px 0 4px 12px;line-height:1.5">{_check_rows}</div>'
            )

        # （v7 已移除 T2 強制進場；多頭中段顯示等待 T3 拉回）
        if rsi is not None and 50 <= rsi < 65 and not t1_ok and not t3_ok:
            to50 = f"{rsi - 50:.1f}"
            # 🆕 v9.10r：估計需跌到的價格才能讓 RSI<50
            # Wilder RSI 倒推：d = 13 × ATR × (RSI-50)/50（單日跌幅）
            # 多日緩跌可平均分攤
            target_price_html = ''
            if atr14 is not None and atr14 > 0 and close is not None:
                # 單日急跌目標
                d_1day = 13 * atr14 * (rsi - 50) / 50
                target_1d = close - d_1day
                target_1d_pct = -d_1day / close * 100 if close > 0 else 0
                # 3 日緩跌（每日 1/3 跌幅）
                d_3day = d_1day / 3
                target_3d_today = close - d_3day
                target_3d_pct = -d_3day / close * 100 if close > 0 else 0
                target_price_html = (
                    f'<br><span style="color:#a8c8d8;font-size:.72rem;margin-left:18px">'
                    f'💡 預估今日拉回到 <b style="color:#3dbb6a">≤ {target_3d_today:.2f}</b>'
                    f'（{target_3d_pct:.1f}%，約 0.3 ATR，3 日緩跌情境）｜'
                    f'單日急跌 <b style="color:#e8a020">≤ {target_1d:.2f}</b>'
                    f'（{target_1d_pct:.1f}%）才一日入區'
                    f'</span>'
                )
            entry_rows.append(
                f'<div style="display:flex;gap:6px;align-items:baseline">'
                f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
                f'font-size:.65rem;color:#7a8899;white-space:nowrap">等待 T3</span>'
                f'<span style="color:#c8b87a">📌 RSI {rsi_str}，多頭中段，'
                f'等待 RSI &lt; 50（再距 {to50} 點）確認 T3 拉回再進場'
                f'{target_price_html}</span></div>'
            )
        elif rsi is not None and rsi >= 65 and not t1_ok:
            # 🆕 v9.10r：估計回落目標價
            target_html_overheat = ''
            if atr14 is not None and atr14 > 0 and close is not None:
                d_1day = 13 * atr14 * (rsi - 50) / 50
                target_1d = close - d_1day
                target_1d_pct = -d_1day / close * 100 if close > 0 else 0
                d_5day = d_1day / 5  # 5 日緩跌（過熱通常需更多時間）
                target_5d = close - d_5day
                target_5d_pct = -d_5day / close * 100 if close > 0 else 0
                target_html_overheat = (
                    f'<br><span style="color:#a8c8d8;font-size:.72rem;margin-left:18px">'
                    f'💡 預估今日拉回到 <b style="color:#3dbb6a">≤ {target_5d:.2f}</b>'
                    f'（{target_5d_pct:.1f}%，5 日緩跌）｜'
                    f'單日急跌 <b style="color:#e8a020">≤ {target_1d:.2f}</b>'
                    f'（{target_1d_pct:.1f}%）才一日入區'
                    f'</span>'
                )
            # 🆕 v9.10l：TW 研究發現 RSI≥70 反而 RR +0.014（強勢延續）→ 改提示而非警告
            if _is_us or _is_crypto:
                # 美股維持原警告（US RSI≥70 中性）
                entry_rows.append(
                    f'<div style="color:#7a8899;font-size:.75rem">'
                    f'RSI {rsi_str} ≥ 65，{('過熱，不進場' if rsi >= 75 else "等待回落至 RSI < 50 再進場")}'
                    f'{target_html_overheat}'
                    f'</div>'
                )
            else:
                # 台股：研究顯示 RSI≥70 強勢延續，提示而非警告
                if rsi >= 70:
                    entry_rows.append(
                        f'<div style="color:#3dbb6a;font-size:.75rem">'
                        f'⚡ RSI {rsi_str} ≥ 70 — 🇹🇼 研究顯示「強勢延續」反而 RR +0.014，不該避開'
                        f'</div>'
                    )
                else:
                    entry_rows.append(
                        f'<div style="color:#7a8899;font-size:.75rem">'
                        f'RSI {rsi_str} ≥ 65，多頭偏熱，等待回落至 RSI &lt; 50 再進場'
                        f'{target_html_overheat}'
                        f'</div>'
                    )

        # 🆕 T4 預警：多頭但 EMA20 即將跌破 EMA60（距 < 1 ATR），不亮燈
        if (atr14 is not None and atr14 > 0
                and ema20 is not None and ema60 is not None
                and (ema20 - ema60) < atr14):
            _gap_atr = (ema20 - ema60) / atr14
            entry_rows.append(
                f'<div style="display:flex;gap:6px;align-items:baseline;opacity:0.75">'
                f'<span style="background:#1a1410;border-radius:3px;padding:0 5px;'
                f'font-size:.65rem;color:#7a6050;white-space:nowrap">T4 空頭反彈</span>'
                f'<span style="color:#7a8899">⬜ 即將適用（EMA20 距 EMA60 僅 '
                f'{_gap_atr:.2f} ATR，跌破後切換 T4 通道）</span></div>'
            )

    elif is_bull and not adx_ok:
        entry_rows.append(
            f'<div style="color:#e8a020">ADX {adx_str} &lt; {_adx_th}，趨勢強度不足，'
            f'等待 ADX ≥ {_adx_th} 後進場（{_market_tag}）</div>'
        )
    else:  # 空頭：以 T4 為主要進場通道
        # T4 條件分項顯示（與 T1/T3 一致格式）
        _t4_cond1 = (rsi is not None and rsi < 35)
        _t4_cond2 = _t4_rsi_rising
        if rsi is None:
            _t4_state = "資料不足"
        elif _t4_rising:
            _t4_state = (f"RSI {rsi_str} &lt; 35 且<b>連續2天回升</b>"
                         f"（{rsi_prev2:.1f}→{rsi_prev:.1f}→{rsi_str}）　← <b>可進場</b>")
            _t4_color = "#3dbb6a"
        elif _t4_cond1 and not _t4_cond2:
            _need = ("僅差1日（昨升今續升即達標）"
                     if (rsi_prev is not None and rsi > rsi_prev)
                     else "RSI 尚未止跌")
            _t4_state = f"RSI {rsi_str} &lt; 35 ✅ 但連續2日上升 ⬜（{_need}）"
            _t4_color = "#c8b87a"
        elif not _t4_cond1:
            _t4_state = (f"RSI {rsi_str} ≥ 35（還差 {35 - rsi:.1f} 點到超賣門檻）"
                         if rsi is not None else "")
            _t4_color = "#7a8899"
        else:
            _t4_state = "等待"; _t4_color = "#7a8899"

        _t4_icon = "✅" if _t4_rising else ("🟡" if _t4_cond1 else "⬜")
        entry_rows.append(
            f'<div style="display:flex;gap:6px;align-items:baseline">'
            f'<span style="background:#2a1500;border-radius:3px;padding:0 5px;'
            f'font-size:.65rem;color:#ff9944;white-space:nowrap">T4 空頭反彈</span>'
            f'<span style="color:{_t4_color}">{_t4_icon} {_t4_state}</span></div>'
        )

        # 補充：仍是空頭的提示
        if not _t4_rising:
            entry_rows.append(
                f'<div style="color:#7a8899;font-size:.72rem;margin-left:6px">'
                f'空頭期間僅 T4 適用（ATR×2.0 嚴格停損）；'
                f'其餘等 EMA 黃金交叉後重新評估</div>'
            )

    # 🆕 VWAP 進場側建議（93 檔回測 +VWAPEXEC：進場價 = min(close, VWAP)）
    # 出場側建議在 ④ 出場獲利區塊
    vwap_today = d.get("vwap_today")
    if vwap_today and close:
        vwap_pct = (close - vwap_today) / vwap_today * 100
        if close < vwap_today:
            if _entry_blocked_by_dc:
                # 🆕 v9.11：被即將死叉阻擋 → 改成中性顯示（不建議進場）
                entry_rows.append(
                    f'<div style="background:#1a1208;border-left:3px solid #7a8899;'
                    f'padding:6px 10px;margin:5px 0;border-radius:3px;opacity:.6">'
                    f'<span style="color:#7a8899;font-size:.78rem">'
                    f'<s>📈 VWAP 進場建議</s>（已被即將死叉否決）　'
                    f'收盤 {close:.2f} 低於 VWAP {vwap_today:.2f} '
                    f'(-{abs(vwap_pct):.1f}%)，<b>但此刻不建議進場</b>'
                    f'</span></div>'
                )
            else:
                # 進場有利：綠色強調框
                entry_rows.append(
                    f'<div style="background:#08131f;border-left:3px solid #3dbb6a;'
                    f'padding:6px 10px;margin:5px 0;border-radius:3px">'
                    f'<span style="color:#3dbb6a;font-size:.85rem">'
                    f'<b>📈 VWAP 進場建議</b>　收盤 {close:.2f} 低於 VWAP '
                    f'<b style="font-size:.95rem">{vwap_today:.2f}</b> '
                    f'(<b>-{abs(vwap_pct):.1f}%</b>)，進場成本佳；'
                    f'<b>盤中可在 ≤ {vwap_today:.2f} 掛買單</b>'
                    f'</span></div>'
                )
        else:
            # close ≥ VWAP：警告框（黃色），明顯但非綠/紅
            entry_rows.append(
                f'<div style="background:#1a1605;border-left:3px solid #d4a020;'
                f'padding:6px 10px;margin:5px 0;border-radius:3px">'
                f'<span style="color:#e8b830;font-size:.85rem">'
                f'<b>⚠️ VWAP 進場提醒</b>　收盤 {close:.2f} 已高於 VWAP '
                f'<b style="font-size:.95rem">{vwap_today:.2f}</b> '
                f'(<b>+{vwap_pct:.1f}%</b>)，<b>不建議追高進場</b>；'
                f'盤中等回落至 ≤ {vwap_today:.2f} 再考慮'
                f'</span></div>'
            )

    # 🆕 v9.11：月份效應警告（earnings season research）
    # 計算當前月份（資料最後一日的月份）
    try:
        import datetime as _dt
        _today_month = _dt.datetime.now().month
    except Exception:
        _today_month = 0
    _month_warn = None
    if is_bull and _today_month > 0:
        if _is_us or _is_crypto:
            # 🇺🇸 美股月份效應（T1_V7 research）
            if _today_month == 5:
                _month_warn = ('🇺🇸 5月地雷月', '#ff5555',
                                f'美股研究：5 月 T1_V7 平均 -3.17%（"Sell in May" 應驗）— 建議減少新倉')
            elif _today_month == 9:
                _month_warn = ('🇺🇸 9月偏弱', '#e8a020',
                                f'美股研究：9 月 T1_V7 平均 -0.64%（季節性回調）— 建議謹慎')
            elif _today_month == 10:
                _month_warn = ('🇺🇸 10月強月', '#3dbb6a',
                                f'美股研究：10 月 T1_V7 平均 +5.77%（69.8% 漲）— 黃金月！')
            elif _today_month == 12:
                _month_warn = ('🇺🇸 12月強月', '#3dbb6a',
                                f'美股研究：12 月 T1_V7 平均 +5.37%（75% 漲）— 黃金月！')
        else:
            # 🇹🇼 台股月份效應（倒鎚 research）
            if _today_month == 3:
                _month_warn = ('🇹🇼 3月地雷月', '#ff5555',
                                f'台股研究：3 月倒鎚平均 -7.94%（21% 勝率，年報截止前）— 建議延至 4 月')
            elif _today_month == 4:
                _month_warn = ('🇹🇼 4月黃金月', '#3dbb6a',
                                f'台股研究：4 月倒鎚平均 +15.91%（89.5% 勝率）— 年報後反彈最強！')
            elif _today_month == 5:
                _month_warn = ('🇹🇼 5月強月', '#3dbb6a',
                                f'台股研究：5 月倒鎚平均 +12.01%（80.9% 勝率）— 強勢延續')
            elif _today_month in [6, 9]:
                _month_warn = (f'🇹🇼 {_today_month}月偏弱', '#e8a020',
                                f'台股研究：{_today_month} 月倒鎚 < 0%（建議謹慎）')

    if _month_warn:
        _mtag, _mcolor, _mdesc = _month_warn
        entry_rows.append(
            f'<div style="background:#0a1628;border-left:3px solid {_mcolor};'
            f'padding:6px 10px;margin:5px 0;border-radius:3px">'
            f'<span style="color:{_mcolor};font-weight:700;font-size:.82rem">{_mtag}</span>'
            f'<span style="color:#a8c8d8;font-size:.76rem">　{_mdesc}</span></div>'
        )

    # 進場動作標籤
    # 🆕 v9.10j：即將死叉時否決進場訊號（避免「進場 + 即將出場」矛盾）
    # 🆕 v9.11：加 action_reason 顯示原因（建議進場/不建議進場/等待 + 為什麼）
    _entry_blocked_by_dc = (is_bull and _imminent_dc and (t1_ok or t3_ok))
    if not is_bull:
        if _t4_rising:
            action_label = "🟡 T4 反彈條件達成（空頭中）"
            action_reason = (f"原因：EMA20 < EMA60（空頭排列）但 RSI {rsi_str} < 32 + "
                              f"連續上升 → T4 反彈訊號，可短線觀察")
            action_bg, action_fg = "#2a1500", "#ff9944"
        else:
            action_label = "❌ 不建議進場（空頭趨勢）"
            action_reason = f"原因：EMA20 < EMA60（空頭排列），RSI {rsi_str}，趨勢未確立多頭"
            action_bg, action_fg = "#1a0505", "#ff5555"
    elif not adx_ok:
        adx_th = 18 if (_is_us or _is_crypto) else 22
        action_label = "⚠️ 不建議進場（假多頭）"
        action_reason = (f"原因：EMA20 > EMA60 但 ADX {adx_str} < {adx_th}，"
                          f"趨勢強度不足，可能是震盪市場的假多頭")
        action_bg, action_fg = "#1a1200", "#e8a020"
    elif _entry_blocked_by_dc:
        gap_atr = (ema20 - ema60) / atr14 if atr14 and atr14 > 0 else 0
        action_label = "🛑 不建議進場（即將死叉）"
        action_reason = (f"原因：雖 RSI {rsi_str} < 50 / 黃金交叉 {cross_days} 天前 等進場條件成立，"
                          f"但 EMA20 距 EMA60 僅 {gap_atr:.2f} ATR + 多頭已 {cross_days} 天，"
                          f"隨時可能死叉。等死叉發生後再評估")
        action_bg, action_fg = "#2a0a0a", "#ff7755"
    elif t1_ok or t3_ok:
        # 🆕 v9.18.2：細分標籤，與 classify_action（主表格）一致
        _is_strong_a = (adx is not None and adx >= 30)
        _is_fresh_a  = (cross_days is not None and 0 < cross_days <= 10)
        _is_pullback_a = (rsi is not None and rsi < 50)
        if t1_ok and t3_ok:
            trigger = f"T1 黃金交叉 {cross_days} 天前 + T3 RSI {rsi_str}<50 拉回"
        elif t1_ok:
            trigger = f"T1 黃金交叉 {cross_days} 天前"
        else:
            trigger = f"T3 RSI {rsi_str}<50 拉回到位"

        # 細分四象限（呼應主表格的 飆股 / T3強拉 / T1 進場 / T3拉回進場）
        if _is_strong_a and _is_fresh_a:
            action_label = "🚀 飆股 進場（強趨勢主升段）"
            action_reason = (f"原因：多頭排列 + ADX {adx_str} ≥30（強趨勢）+ 黃金交叉 {cross_days} 天（剛啟動）"
                              f"<br>⚠️ 飆股風險高但獲利空間大；採持到 EMA 死叉的策略，不設 RSI 出場")
            action_bg, action_fg = "#1a1400", "#f0c030"   # 金黃，呼應主表格 chip
        elif _is_strong_a and _is_pullback_a:
            action_label = "✅ T3 強趨勢拉回 進場"
            action_reason = (f"原因：ADX {adx_str} ≥30 強趨勢 + RSI {rsi_str}<50 拉回到位（最佳加碼點）")
            action_bg, action_fg = "#0d2a10", "#3dbb6a"
        elif (not _is_strong_a) and _is_fresh_a:
            action_label = f"✅ T1 {cross_days}D 進場"
            action_reason = (f"原因：多頭排列 + ADX {adx_str}（22-30 穩健）+ 黃金交叉 {cross_days} 天前")
            action_bg, action_fg = "#0d2a10", "#3dbb6a"
        elif (not _is_strong_a) and _is_pullback_a:
            action_label = "✅ T3 拉回進場"
            action_reason = (f"原因：多頭排列 + ADX {adx_str} 達標 + RSI {rsi_str}<50 拉回")
            action_bg, action_fg = "#0d2a10", "#3dbb6a"
        else:
            action_label = "✅ 建議進場"
            action_reason = (f"原因：多頭排列 + ADX {adx_str} 達標 + {trigger}")
            action_bg, action_fg = "#0d2a10", "#3dbb6a"
    elif t2_ok:
        action_label = "🟡 可觀察進場（次要訊號）"
        action_reason = "原因：多頭排列 + ADX 達標但無 T1/T3 主訊號，可觀察等待主訊號確認"
        action_bg, action_fg = "#1a1a05", "#c8b87a"
    elif rsi is not None and rsi >= 70:
        action_label = "⏸ 等待拉回（RSI 過熱）"
        action_reason = (f"原因：多頭健康但 RSI {rsi_str} ≥ 70 偏熱，"
                          f"等回落至 RSI < 50 出現 T3 拉回再進場")
        action_bg, action_fg = "#0a1628", "#7a9ab0"
    else:
        action_label = "⏸ 等待拉回（多頭中段）"
        action_reason = (f"原因：多頭排列 + ADX {adx_str} 達標，但 RSI {rsi_str} 在 50-70 中段，"
                          f"等 T3 拉回（RSI<50）出現再進場")
        action_bg, action_fg = "#0a1628", "#7a9ab0"

    # ── 🆕 v9.23：ZigZag 對照圖（W底 / M頂 / VCP 視覺化）─────────────
    def _build_zigzag_chart_img(d_local, ticker_local=None, max_bars=180):
        """從 d['_swing_history'] 或 data_cache 渲染 ZigZag 對照圖
        回傳 base64 data URI（或 None）"""
        try:
            import io, base64
            import pandas as _pd
            import numpy as _np
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as _plt
            import matplotlib.dates as _mdates
            import matplotlib as _mpl
            _mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei',
                                                   'SimHei', 'Arial Unicode MS']
            _mpl.rcParams['axes.unicode_minus'] = False

            df_plot = None

            # 路徑 A：從 _swing_history 重建（雲端 path，主要來源）
            sh = d_local.get('_swing_history') or {}
            closes = sh.get('close') or []
            if closes and len(closes) >= 30:
                dates_arr = sh.get('dates') or []
                # 🆕 v9.23.2：若無 dates（舊 cron 資料），用 business days 反推
                if not dates_arr or len(dates_arr) != len(closes):
                    _today = _pd.Timestamp.now().normalize()
                    dates_arr = _pd.bdate_range(end=_today, periods=len(closes))
                try:
                    df_plot = _pd.DataFrame({
                        'Open':   sh.get('open') or closes,
                        'High':   sh.get('high') or closes,
                        'Low':    sh.get('low')  or closes,
                        'Close':  closes,
                        'Volume': sh.get('volume') or [0]*len(closes),
                    }, index=_pd.to_datetime(dates_arr))
                except Exception:
                    df_plot = None

            # 路徑 B：fallback 到 data_cache（local dev path）
            if df_plot is None or len(df_plot) < 30:
                if ticker_local:
                    try:
                        import data_loader as _dl
                        df_plot = _dl.load_from_cache(ticker_local)
                    except Exception:
                        df_plot = None

            if df_plot is None or len(df_plot) < 30:
                return None
            df_plot = df_plot.dropna()
            if len(df_plot) > max_bars:
                df_plot = df_plot.tail(max_bars)
            if len(df_plot) < 30:
                return None

            from zigzag import zigzag as _zz
            pivots = _zz(df_plot, mode='atr', atr_mult=1.3, atr_period=14)

            fig, (ax1, ax2) = _plt.subplots(2, 1, figsize=(13, 6.5),
                                              gridspec_kw={'height_ratios': [3, 1]},
                                              sharex=True)

            # 蠟燭線
            for _d, _row in df_plot.iterrows():
                _c = '#26a69a' if _row['Close'] >= _row['Open'] else '#ef5350'
                ax1.plot([_d, _d], [_row['Low'], _row['High']],
                          color=_c, linewidth=0.5, alpha=0.65, zorder=1)
                ax1.add_patch(_plt.Rectangle(
                    (_mdates.date2num(_d) - 0.3, min(_row['Open'], _row['Close'])),
                    0.6, abs(_row['Close'] - _row['Open']),
                    facecolor=_c, edgecolor=_c, alpha=0.65, zorder=2))

            # ZigZag 線
            if pivots:
                xs = [p['date'] for p in pivots]
                ys = [p['price'] for p in pivots]
                ax1.plot(xs, ys, color='#ff6b35', linewidth=2.3, alpha=0.9,
                          marker='o', markersize=8,
                          markerfacecolor='gold', markeredgecolor='#cc4400',
                          markeredgewidth=1.6,
                          label=f'ZigZag ATR×1.3 ({len(pivots)} pivots)', zorder=5)

            # W 底標註
            _dbi = d_local.get('double_bottom_info') or {}
            if _dbi.get('is_double_bottom'):
                try:
                    L1d = _pd.to_datetime(_dbi['left_bottom']['date'])
                    L2d = _pd.to_datetime(_dbi['right_bottom']['date'])
                    NKd = _pd.to_datetime(_dbi['middle_peak']['date'])
                    L1p = _dbi['left_bottom']['price']
                    L2p = _dbi['right_bottom']['price']
                    NKp = _dbi['middle_peak']['price']
                    ax1.scatter([L1d, L2d], [L1p, L2p], s=300,
                                 facecolor='none', edgecolor='red', linewidth=3,
                                 marker='o', zorder=7,
                                 label=f'W底 L1/L2 Grade {_dbi.get("quality_grade","")}')
                    ax1.scatter([NKd], [NKp], s=300, facecolor='none',
                                 edgecolor='blue', linewidth=3, marker='o', zorder=7,
                                 label=f'Neckline ${NKp:.2f}')
                    ax1.axhline(NKp, color='blue', linestyle='--', linewidth=1.2, alpha=0.5)
                except Exception:
                    pass

            # M 頂標註
            _dti = d_local.get('double_top_info') or {}
            if _dti.get('is_double_top'):
                try:
                    H1d = _pd.to_datetime(_dti['left_top']['date'])
                    H2d = _pd.to_datetime(_dti['right_top']['date'])
                    H1p = _dti['left_top']['price']
                    H2p = _dti['right_top']['price']
                    ax1.scatter([H1d, H2d], [H1p, H2p], s=300,
                                 facecolor='none', edgecolor='purple', linewidth=3,
                                 marker='s', zorder=7,
                                 label=f'M頂 H1/H2 Grade {_dti.get("quality_grade","")}')
                except Exception:
                    pass

            # VCP 收口 box
            _vcp = d_local.get('vcp_zigzag_info') or {}
            _ctr = _vcp.get('contractions') or []
            if _ctr:
                vcp_colors = ['#42a5f5', '#1e88e5', '#1565c0', '#0d47a1']
                for _i, _c in enumerate(_ctr):
                    try:
                        cc = vcp_colors[_i % len(vcp_colors)]
                        _ax, _aH = _c['L_date'], _c['H_date']
                        _aLp, _aHp = _c['L_price'], _c['H_price']
                        x_start = min(_ax, _aH); x_end = max(_ax, _aH)
                        import matplotlib.patches as _mp
                        rect = _mp.Rectangle(
                            (_mdates.date2num(x_start), _aLp),
                            _mdates.date2num(x_end) - _mdates.date2num(x_start),
                            _aHp - _aLp,
                            linewidth=1.3, edgecolor=cc, facecolor=cc,
                            alpha=0.10, zorder=3, linestyle='--')
                        ax1.add_patch(rect)
                    except Exception:
                        continue
                _top = _vcp.get('consolidation_top')
                if _top:
                    ax1.axhline(_top, color='purple', linestyle='-',
                                 linewidth=1.4, alpha=0.55,
                                 label=f'VCP 整理頂 ${_top:.2f}')

            # 標題
            _has_db = _dbi.get('is_double_bottom', False)
            _has_dt = _dti.get('is_double_top', False)
            _has_vcp = _vcp.get('is_vcp', False)
            tags = []
            if _has_db: tags.append(f'W底-{_dbi.get("quality_grade","")} ({_dbi.get("status","")})')
            if _has_dt: tags.append(f'M頂-{_dti.get("quality_grade","")} ({_dti.get("status","")})')
            if _has_vcp: tags.append(f'VCP-{_vcp.get("vcp_grade","")} ({_vcp.get("num_contractions",0)}收口 {_vcp.get("breakout_status","")})')
            elif _ctr:
                tags.append(f'VCP候選 ({len(_ctr)}收口，未達標)')
            title = f'ZigZag (ATR×1.3) {len(pivots)} pivots'
            if tags: title += '   |   ' + '  ｜  '.join(tags)
            ax1.set_title(title, fontsize=10, fontweight='bold', pad=6)
            ax1.set_ylabel('Price', fontsize=9)
            ax1.legend(loc='upper left', fontsize=7.5)
            ax1.grid(True, alpha=0.3)

            # Volume
            for _d, _row in df_plot.iterrows():
                _c = '#26a69a' if _row['Close'] >= _row['Open'] else '#ef5350'
                ax2.bar(_d, _row['Volume'], color=_c, alpha=0.55, width=0.7)
            ax2.set_ylabel('Vol', fontsize=8)
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_locator(_mdates.MonthLocator())
            ax2.xaxis.set_major_formatter(_mdates.DateFormatter('%Y-%m'))
            _plt.setp(ax2.xaxis.get_majorticklabels(), rotation=25, fontsize=8)

            _plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            _plt.close(fig)
            buf.seek(0)
            data_b64 = base64.b64encode(buf.read()).decode('ascii')
            return f'data:image/png;base64,{data_b64}'
        except Exception:
            return None

    # ── 🌊 波段診斷（v9.15）— 基於 walk-forward OOS 驗證的 Strategy B 推薦 ──
    # OOS robust（TW + US 雙市場）：
    #   B + rsi_70：65% win, +1.3% mean (保守)
    #   B + rsi_75：61% win, +2.6% mean (平衡)
    #   B + rsi_80：54% win, +4.0% mean (進取)
    swing_state = None
    swing_rows = []
    # 計算指標（不論多空都算）
    _high60_local = float(d.get('high60', close)) if d else close
    _vol_local = d.get('volume') if d else None
    _vol_ma20_local = d.get('vol_ma20') if d else None
    _vol_ratio = (_vol_local / _vol_ma20_local) if (_vol_local and _vol_ma20_local and _vol_ma20_local > 0) else 1
    _from_high_pct = (_high60_local - close) / _high60_local * 100 if _high60_local > 0 else 99
    _adx_v = adx if adx is not None else 0
    _rsi_v = rsi if rsi is not None else 0

    if not is_bull:
        # 空頭：不適合波段
        swing_state = 'bear_skip'
        swing_rows.append(
            f'<div style="background:#1a0808;border-left:4px solid #888;'
            f'padding:8px 12px;border-radius:4px">'
            f'<b style="color:#999;font-size:.92rem">🔴 空頭排列 — 不適合波段操作</b>'
            f'<div style="color:#aaa;font-size:.74rem;margin-top:4px;line-height:1.5">'
            f'EMA20 &lt; EMA60（空頭），波段策略 OOS 驗證僅在多頭中有效'
            f'<br>📌 等 EMA 黃金交叉（轉多頭）後再看波段訊號'
            f'</div></div>'
        )
    elif not atr14:
        swing_state = 'no_data'
        swing_rows.append(
            f'<div style="color:#7a8899;font-size:.78rem">'
            f'波段診斷需要 ATR 資料（目前不足）</div>'
        )
    elif _rsi_v >= 80:
        # ① 過熱出場警示（最高優先）
        swing_state = 'overheat_exit'
        swing_rows.append(
            f'<div style="background:#3a0a0a;border-left:4px solid #ff5555;'
            f'padding:8px 12px;border-radius:4px">'
            f'<b style="color:#ff5555;font-size:.92rem">🚪 波段過熱出場警示</b>'
            f'<div style="color:#ffaaaa;font-size:.74rem;margin-top:4px;line-height:1.5">'
            f'RSI {_rsi_v:.0f} ≥ 80（過熱）+ 多頭排列 →'
            f'<br><b>OOS 驗證：rsi_80 是 Strategy B 最佳出場點</b>'
            f'（54% 勝率、+4.0% 平均報酬，跨 TW+US 兩市場 robust）'
            f'<br>📌 <b>若你持有此股，1-2 天內應獲利了結</b>'
            f'</div></div>'
        )
    elif 75 <= _rsi_v < 80:
        # ② 接近過熱（觀察）
        swing_state = 'warn_exit'
        swing_rows.append(
            f'<div style="background:#1a1408;border-left:4px solid #e8a020;'
            f'padding:8px 12px;border-radius:4px">'
            f'<b style="color:#e8a020;font-size:.92rem">🟡 波段接近過熱</b>'
            f'<div style="color:#ddc080;font-size:.74rem;margin-top:4px;line-height:1.5">'
            f'RSI {_rsi_v:.0f} 處於 75-80（rsi_75 出場區）→'
            f'<br><b>OOS 驗證：rsi_75 = 61% 勝率、+2.6% 報酬</b>（已達不錯水準）'
            f'<br>📌 持倉者可考慮減碼一半，或設緊停損；激進派可等 RSI≥80 再賣'
            f'</div></div>'
        )
    elif (_adx_v >= 22 and _from_high_pct < 1 and _vol_ratio > 1.5 and _rsi_v < 70):
        # ③ Strategy B 入場條件達成（OOS 驗證）
        swing_state = 'B_entry'
        swing_rows.append(
            f'<div style="background:#0a2a14;border-left:4px solid #3dbb6a;'
            f'padding:8px 12px;border-radius:4px">'
            f'<b style="color:#3dbb6a;font-size:.92rem">🌟 OOS驗證 波段 B 入場條件達成</b>'
            f'<div style="color:#a8e0c0;font-size:.74rem;margin-top:4px;line-height:1.5">'
            f'距 60d 高 {_from_high_pct:.2f}% &lt; 1% + 量比 {_vol_ratio:.1f}x &gt; 1.5x + '
            f'RSI {_rsi_v:.0f} &lt; 70 + ADX {_adx_v:.0f} ≥ 22'
            f'<br><b>OOS 驗證：跨 TW+US 兩市場 robust 的唯一策略</b>'
            f'<br>📌 <b>動態出場推薦</b>（依風險偏好擇一）：'
            f'<br>　🛡️ <b>保守 rsi_70</b>：等 RSI≥70 賣 → 65% 勝率 / +1.3% 報酬'
            f'<br>　⚖️ <b>平衡 rsi_75</b>：等 RSI≥75 賣 → 61% 勝率 / +2.6% 報酬'
            f'<br>　🚀 <b>進取 rsi_80</b>：等 RSI≥80 賣 → 54% 勝率 / +4.0% 報酬'
            f'<br>　⏰ 安全網：最長持 90 天'
            f'</div></div>'
        )
    elif (_adx_v >= 22 and 1 <= _from_high_pct <= 3 and _vol_ratio > 1.2 and _rsi_v < 70):
        # ④ 接近突破（候選）
        swing_state = 'B_near'
        swing_rows.append(
            f'<div style="background:#0a1828;border-left:4px solid #7abadd;'
            f'padding:8px 12px;border-radius:4px">'
            f'<b style="color:#7abadd;font-size:.92rem">🟢 波段近突破（候選 watchlist）</b>'
            f'<div style="color:#bce0e8;font-size:.74rem;margin-top:4px;line-height:1.5">'
            f'距 60d 高 {_from_high_pct:.2f}%（1-3% 區）+ 量比 {_vol_ratio:.1f}x + RSI {_rsi_v:.0f} &lt; 70'
            f'<br>📌 等股價突破 60d 高 <b>{_high60_local:.2f}</b>（再漲 {_from_high_pct:.2f}%）+ '
            f'量增 1.5x → 觸發 Strategy B 入場'
            f'</div></div>'
        )
    elif _imminent_dc:
        # ⑤ 波段死叉警告
        swing_state = 'dc_warn'
        swing_rows.append(
            f'<div style="background:#2a1605;border-left:4px solid #ff9944;'
            f'padding:8px 12px;border-radius:4px">'
            f'<b style="color:#ff9944;font-size:.92rem">⚠️ 波段持倉應重評（即將死叉）</b>'
            f'<div style="color:#ffd0a0;font-size:.74rem;margin-top:4px;line-height:1.5">'
            f'多頭排列但 EMA20 距 EMA60 &lt; 1 ATR + 黃金交叉已 {cross_days or "?"} 天 + EMA20 走弱'
            f'<br>📌 <b>波段已成熟，持倉者該設緊停損或主動出場</b>，不要等死叉發生'
            f'</div></div>'
        )
    else:
        # ⑥ 中性：多頭但沒觸發任何訊號 — 顯示目前狀態 + 距各觸發條件多遠
        swing_state = 'neutral'
        # 分析離各條件還差多少
        _gaps = []
        if _adx_v < 22:
            _gaps.append(f'ADX {_adx_v:.0f}（差 {22-_adx_v:.0f} 點達 22 門檻）')
        if _from_high_pct >= 3:
            _gaps.append(f'距高 {_from_high_pct:.1f}%（需 ≤3% 才接近突破）')
        elif _from_high_pct >= 1:
            _gaps.append(f'距高 {_from_high_pct:.1f}%（差 {_from_high_pct-1:.1f}% 才達突破）')
        if _vol_ratio < 1.2:
            _gaps.append(f'量比 {_vol_ratio:.1f}x（需 ≥1.2x，差 {1.2-_vol_ratio:.1f}x）')
        elif _vol_ratio < 1.5:
            _gaps.append(f'量比 {_vol_ratio:.1f}x（達近突破，需 ≥1.5x 才達正式 B）')
        if _rsi_v >= 70:
            _gaps.append(f'RSI {_rsi_v:.0f}（已偏熱，等回到 &lt;70 + 突破才進場）')

        _gap_text = '<br>　• '.join(_gaps) if _gaps else '所有指標均接近觸發點，等待突破或回檔'

        swing_rows.append(
            f'<div style="background:#0a1422;border-left:4px solid #4a6878;'
            f'padding:8px 12px;border-radius:4px">'
            f'<b style="color:#7a9ab0;font-size:.92rem">📊 波段觀察中（目前無明確訊號）</b>'
            f'<div style="color:#a8c0d0;font-size:.74rem;margin-top:4px;line-height:1.5">'
            f'目前指標：ADX {_adx_v:.0f} ｜ RSI {_rsi_v:.0f} ｜ '
            f'距 60d 高 {_from_high_pct:.1f}% ｜ 量比 {_vol_ratio:.1f}x'
            f'<br><b>距觸發 Strategy B 的差距</b>：'
            f'<br>　• {_gap_text}'
            f'<br>📌 持有者：繼續持有觀察；非持有：等突破訊號（B 入場）或回檔（B 拉回）'
            f'</div></div>'
        )

    # ── 🆕 v9.16 / v9.17.1 / v9.17.2：三狀態 + 4 主動出場 recipe 即時評估 ─────
    # 先試用 d['_swing_history']（雲端從 main scan 流程帶入）
    # fallback 到 data_cache 本地（local dev）
    _swing_diag_msg = None  # 診斷訊息（顯示給用戶看為何 banner 沒出來）
    try:
        from state_classifier import classify_market_state, evaluate_recipes_live
        _df_for_state = None
        _data_source = 'unknown'

        # 路徑 A：從 d['_swing_history']（30d tail）重建 mini-DataFrame
        _hist = d.get('_swing_history') if d else None
        if _hist and _hist.get('close') and len(_hist['close']) >= 10:
            try:
                import pandas as _pd_local
                _hist_n = len(_hist['close'])
                _df_for_state = _pd_local.DataFrame({
                    'Open':   _hist.get('open', _hist['close']),
                    'High':   _hist.get('high', _hist['close']),
                    'Low':    _hist.get('low', _hist['close']),
                    'Close':  _hist['close'],
                    'Volume': _hist.get('volume', [0]*_hist_n),
                    'e10':    _hist.get('ema10', _hist['close']),
                    'e20':    _hist.get('ema20', _hist['close']),
                    'e60':    _hist.get('ema60', _hist['close']),
                    'adx':    _hist.get('adx', [0]*_hist_n),
                    'atr':    _hist.get('atr', [0]*_hist_n),
                    'rsi':    _hist.get('rsi', [50]*_hist_n),
                })
                _data_source = f'_swing_history ({_hist_n}d)'
            except Exception as e:
                _swing_diag_msg = f'_swing_history 解析失敗：{type(e).__name__}'
                _df_for_state = None

        # 路徑 B：fallback to local cache（local dev）
        if (_df_for_state is None or len(_df_for_state) < 10) and ticker:
            try:
                import data_loader as _dl_local
                _df_for_state = _dl_local.load_from_cache(ticker)
                if _df_for_state is not None:
                    _data_source = 'data_cache (local)'
            except Exception:
                _df_for_state = None

        if _df_for_state is None or len(_df_for_state) < 10:
            _swing_diag_msg = (f'波段診斷需 ≥10d 資料（_swing_history 缺，cache 也無）'
                                f' [d.has_swing_history={bool(_hist)}, ticker={ticker}]')

        if _df_for_state is not None and len(_df_for_state) >= 10:
            _state_info = classify_market_state(_df_for_state, d)
            _recipes = evaluate_recipes_live(_df_for_state)

            # 三狀態 banner
            swing_rows.append(
                f'<div style="background:#08131f;border-left:4px solid {_state_info["state_color"]};'
                f'padding:8px 12px;border-radius:4px;margin-top:6px">'
                f'<b style="color:{_state_info["state_color"]};font-size:.92rem">'
                f'目前狀態：{_state_info["state_label"]}（持續 {_state_info["days_in_state"]} 天）</b>'
                f'<div style="color:#a8c0d0;font-size:.74rem;margin-top:3px;line-height:1.5">'
                f'{_state_info["state_desc"]}'
                f'</div></div>'
            )

            # 4 recipe 即時評估表格
            _recipe_rows = []
            for r in _recipes:
                if r['triggered']:
                    _icon = '🚪'
                    _bg_c = '#3a0a0a'
                    _text_c = '#ff5555'
                    _msg = f'<b>已觸發出場</b>（{r["reason"]}）'
                else:
                    _icon = '⏳'
                    _bg_c = '#0a1828'
                    _text_c = '#7abadd'
                    _msg = f'未觸發 — {r["detail"]}'
                _recipe_rows.append(
                    f'<div style="background:{_bg_c};padding:5px 10px;margin:3px 0;'
                    f'border-radius:3px;font-size:.72rem;line-height:1.4">'
                    f'{_icon} <b style="color:{_text_c}">{r["label"]}</b> '
                    f'<span style="color:#7a8899;font-size:.68rem">'
                    f'(規則: {r["rule"]})</span><br>'
                    f'<span style="color:#a8c0d0;margin-left:18px">{_msg}</span>'
                    f'</div>'
                )

            # OOS 統計參考表
            _oos_table = (
                '<div style="font-size:.66rem;color:#7a8899;margin-top:4px;'
                'border-top:1px dashed #2a3a4a;padding-top:6px">'
                '<b style="color:#9abacf">📋 OOS（2024+）統計參考</b><br>'
                '🐢 baseline rsi80+90d：TW win 49% / mean +2.8% / hold 62d ｜ US win 58% / +4.5% / 65d<br>'
                '🎯 D ATR 動態 ⭐：TW win 40% / mean +1.9% / hold 25d ｜ US win 44% / +2.5% / 26d<br>'
                '⚖️ B 平衡：TW win 32% / mean +1.5% / hold 26d ｜ US win 40% / +0.5% / 39d<br>'
                '🛡️ A 保守快出：TW win 29% / mean +0.7% / hold 15d ｜ US win 35% / -0.3% / 19d<br>'
                '🚀 C 飆股：TW win 28% / mean +0.1% / hold 10d ｜ US win 32% / -0.6% / 13d'
                '</div>'
            )

            swing_rows.append(
                f'<div style="background:#0a1422;border-left:4px solid #5dccdd;'
                f'padding:8px 12px;border-radius:4px;margin-top:6px">'
                f'<b style="color:#5dccdd;font-size:.92rem">🚪 4 種主動出場規則 — 即時評估</b>'
                f'<div style="color:#7a8899;font-size:.66rem;margin-top:2px;margin-bottom:4px">'
                f'min_hold 3d / max_hold 不設限（180d safety）'
                f'</div>'
                f'{"".join(_recipe_rows)}'
                f'{_oos_table}'
                f'</div>'
            )
    except Exception as _e_state:
        _swing_diag_msg = f'波段診斷異常：{type(_e_state).__name__}: {str(_e_state)[:80]}'

    # 顯示診斷訊息（資料缺失或失敗時讓用戶看到原因）
    if _swing_diag_msg:
        swing_rows.append(
            f'<div style="background:#1a1208;border-left:3px solid #d4a020;'
            f'padding:5px 10px;border-radius:3px;margin-top:6px;font-size:.7rem;color:#c8a050">'
            f'⚠️ {_swing_diag_msg}'
            f'</div>'
        )

    # ── 🆕 v9.20：SEPA / VCP / RS Rating 詳細診斷 + 進出場判斷 ────
    try:
        sepa_passed = d.get('sepa_passed')
        sepa_n_met = d.get('sepa_n_met')
        sepa_details = d.get('sepa_details') or {}
        rs_rating = d.get('rs_rating')
        # 🐛 fix v9.20.7：cached d 沒 rs_rating 時，從 screener_results.json fresh lookup
        if rs_rating is None and ticker:
            try:
                from pathlib import Path as _P_rs
                _sj_rs = _P_rs(__file__).parent / 'screener_results.json'
                if _sj_rs.exists():
                    import json as _json_rs
                    _sd_rs = _json_rs.loads(_sj_rs.read_text(encoding='utf-8'))
                    _tk_pure_rs = ticker.replace('.TW', '')
                    _rs_dict_rs = _sd_rs.get('rs_ratings') or {}
                    if _tk_pure_rs in _rs_dict_rs:
                        rs_rating = _rs_dict_rs[_tk_pure_rs]
                        d['rs_rating'] = rs_rating  # 寫回 d 給綜合決策用
            except Exception:
                pass
        vcp_info = d.get('vcp_info') or {}
        sma150 = d.get('sma150')
        sma200_real = d.get('sma200_real')
        if sepa_n_met is not None and sepa_n_met >= 0:
            # 8 條件詳細
            cond_rows = []
            _sma150_str = f'{sma150:.2f}' if sma150 else '?'
            _sma200_str = f'{sma200_real:.2f}' if sma200_real else '?'
            sma50_v_disp = d.get('sma50') or 0
            _sma50_for_label = f'{sma50_v_disp:.2f}' if sma50_v_disp else '?'
            cond_labels = [
                ('cond1_close_above_150_200', f'1. 收盤 > SMA150 ({_sma150_str}) AND > SMA200 ({_sma200_str})'),
                ('cond2_sma150_above_200', '2. SMA150 > SMA200'),
                ('cond3_sma200_rising_30d', '3. SMA200 上升 ≥ 30 天（趨勢確立）'),
                ('cond4_sma50_above_150_200', f'4. SMA50 ({_sma50_for_label}) > SMA150 AND > SMA200'),
                ('cond5_close_above_50', f'5. 收盤 > SMA50 ({_sma50_for_label})'),
                ('cond6_above_52w_low_30pct', '6. 收盤 ≥ 52週低點 + 30%（已脫底）'),
                ('cond7_below_52w_high_25pct', '7. 收盤 距 52週高 ≤ 25%（接近高點）'),
            ]
            for key, label in cond_labels:
                ok = sepa_details.get(key, False)
                ic = '✅' if ok else '❌'
                col = '#3dbb6a' if ok else '#aa6655'
                cond_rows.append(
                    f'<div style="font-size:.7rem;color:{col};margin:1px 0">'
                    f'{ic} {label}</div>'
                )

            # 8th condition: RS≥70
            rs_ok = rs_rating is not None and rs_rating >= 70
            rs_disp = f'{rs_rating:.0f}' if rs_rating is not None else '—'
            ic = '✅' if rs_ok else ('❌' if rs_rating is not None else '⚪')
            col = '#3dbb6a' if rs_ok else ('#aa6655' if rs_rating is not None else '#7a8899')

            # 🆕 v9.20.10：RS 無資料時用 returns_52w 顯示替代資訊
            if rs_rating is None:
                _ret52 = d.get('returns_52w')
                if _ret52 is not None and _ret52 != 0:
                    _alt = f'（不在 universe — 52 週報酬 {_ret52:+.1f}%）'
                else:
                    _alt = '（不在 universe — ETF / 新上市 / 資料不足）'
            else:
                _alt = ''
            cond_rows.append(
                f'<div style="font-size:.7rem;color:{col};margin:1px 0">'
                f'{ic} 8. RS Rating {rs_disp} ≥ 70（強於 70% 同期）{_alt}</div>'
            )

            # 整體 SEPA 結論
            full_pass = sepa_passed and rs_ok
            conds_total = sepa_n_met + (1 if rs_ok else 0)
            if full_pass:
                sepa_label = f'🏆 SEPA 8/8 全通過 — Minervini 飆股體質'
                sepa_color = '#3dbb6a'; sepa_bg = '#0a2a14'
            elif sepa_passed:
                sepa_label = f'🥈 SEPA 7/7 通過 + RS 待確認'
                sepa_color = '#7abadd'; sepa_bg = '#0a1828'
            elif sepa_n_met >= 6:
                sepa_label = f'🥈 SEPA {sepa_n_met}/7（接近過關，僅差 {7-sepa_n_met} 條）'
                sepa_color = '#e8a020'; sepa_bg = '#1a1408'
            else:
                sepa_label = f'❌ SEPA {sepa_n_met}/7（差 {7-sepa_n_met} 條，不適用）'
                sepa_color = '#aa6655'; sepa_bg = '#1a0808'

            # VCP 狀態（v9.20.12：條件具體化 + 量縮永遠顯示）
            vcp_is_vcp = vcp_info.get('is_vcp', False)
            vcp_n_contractions = vcp_info.get('n_contractions', 0)
            vcp_declines = vcp_info.get('declines_pct', [])
            vcp_pivot = vcp_info.get('pivot_price', 0)
            vcp_near_pivot_pct = vcp_info.get('near_pivot_pct', 0)
            vcp_volume_dry = vcp_info.get('volume_dry_up', False)
            vcp_is_contracting = vcp_info.get('is_contracting', False)
            vcp_near_pivot = vcp_info.get('near_pivot', False)

            # Minervini VCP 標準：2-6 次收口都行（不需固定 5 次）
            # 三條件：① 收口 ≥ 2 次 ② 振幅遞減 ③ 接近 pivot（≤5%）
            #   + bonus：量縮 (volume_dry_up)
            _vcp_cond1_count = vcp_n_contractions >= 2
            _vcp_cond2_decreasing = vcp_is_contracting
            _vcp_cond3_pivot = vcp_near_pivot

            # 永遠顯示三條件 + 量縮狀態（不論是否 trigger）
            _vcp_cond_rows = [
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _vcp_cond1_count else "#aa6655")};margin:1px 0">'
                f'{"✅" if _vcp_cond1_count else "❌"} ① 收口 ≥ 2 次（目前 {vcp_n_contractions} 次）</div>',
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _vcp_cond2_decreasing else "#aa6655")};margin:1px 0">'
                f'{"✅" if _vcp_cond2_decreasing else "❌"} ② 振幅依次遞減：{vcp_declines if vcp_declines else "—"}</div>',
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _vcp_cond3_pivot else "#aa6655")};margin:1px 0">'
                f'{"✅" if _vcp_cond3_pivot else "❌"} ③ 接近 pivot ${vcp_pivot:.2f}（距 {vcp_near_pivot_pct:+.2f}%）</div>',
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if vcp_volume_dry else "#7a8899")};margin:1px 0">'
                f'{"✅" if vcp_volume_dry else "⚪"} ④ 量縮 ≥ 30%（bonus，非必要）</div>',
            ]
            _vcp_cond_html = ''.join(_vcp_cond_rows)

            if vcp_is_vcp:
                vcp_label = '📐 VCP 形態成立 — Minervini 進場時機'
                vcp_color = '#3dbb6a'
            elif vcp_n_contractions >= 2 and (vcp_is_contracting or vcp_near_pivot):
                vcp_label = '🟡 部分 VCP 條件達成（觀察）'
                vcp_color = '#e8a020'
            else:
                vcp_label = '⚪ 無 VCP 形態'
                vcp_color = '#7a8899'
            vcp_detail = _vcp_cond_html

            # 進出場判斷
            close = d.get('close', 0)
            sma50_v = d.get('sma50', 0)
            entry_advice = ''
            exit_advice = ''
            if full_pass and vcp_is_vcp:
                entry_advice = ('🏆 <b>完整 Minervini setup — 強烈進場候選</b>'
                                 '<br>OOS 驗證：🇹🇼 win 47%/+11.2%/82d ｜ 🇺🇸 win 54%/+4.6%/83d')
            elif full_pass:
                entry_advice = ('✅ <b>SEPA 全通過 + RS 強，可進場</b>'
                                 '<br>OOS：🇹🇼 win 47%/+10%/82d ｜ 🇺🇸 win 51%/+5%/82d')
            elif sepa_passed:
                # 🆕 v9.20.10：依 RS Rating 是否可得分流
                if rs_rating is None:
                    entry_advice = ('🟡 SEPA 體質達標（RS 不在 universe，無從評等）'
                                     '<br>本股可能是 ETF / 新上市 / 6位數TW票，'
                                     '不參與 universe-wide RS 排名')
                else:
                    entry_advice = (f'🟡 SEPA 體質達標但 RS Rating {rs_rating:.0f} < 70'
                                     '<br>需 RS 達標才符合 Minervini 完整 setup')
            elif sepa_n_met >= 6:
                entry_advice = f'🥈 接近 SEPA 過關，watchlist 候選（差 {7-sepa_n_met} 條）'
            else:
                entry_advice = '❌ 不符合 Minervini 體質，建議避開'

            # 出場判斷（依 Minervini）
            if sma50_v and close:
                if close < (close * 0.92):  # placeholder（need entry price，這裡就現價×0.92 提醒）
                    pass
                if close < sma50_v:
                    exit_advice = (f'🚪 <b>跌破 SMA50（${sma50_v:.2f}）</b> — Minervini 出場警示'
                                    '<br>若量增 ≥ 1.3x，OOS 驗證為主動出場時機')
                elif sma200_real and close < sma200_real:
                    exit_advice = f'🚨 <b>跌破 SMA200（${sma200_real:.2f}）</b> — 必須出場（趨勢死亡）'
                else:
                    # 安全持倉
                    safety_pct = ((close - sma50_v) / sma50_v * 100) if sma50_v > 0 else 0
                    exit_advice = (f'✅ 持倉安全：距 SMA50 ${sma50_v:.2f} +{safety_pct:.1f}%'
                                    f'<br>持倉建議：fixed_90d 持有；停損 -8% from entry')

            swing_rows.append(
                f'<div style="background:{sepa_bg};border-left:4px solid {sepa_color};'
                f'padding:8px 12px;border-radius:4px;margin-top:6px">'
                f'<b style="color:{sepa_color};font-size:.92rem">{sepa_label}</b>'
                f'<div style="margin-top:4px">{"".join(cond_rows)}</div>'
                f'<div style="margin-top:6px;padding:6px;background:#08131f;border-radius:3px">'
                f'<b style="color:{vcp_color};font-size:.78rem">{vcp_label}</b>'
                f'<div style="font-size:.7rem;color:#a8c0d0;margin-top:2px">{vcp_detail}</div>'
                f'</div>'
                f'<div style="margin-top:6px;padding:6px;background:#0a1422;border-radius:3px;border-left:2px solid #3dbb6a">'
                f'<div style="font-size:.74rem;color:#c8e0d0">📌 進場：{entry_advice}</div>'
                f'</div>'
                + (f'<div style="margin-top:4px;padding:6px;background:#0a1422;border-radius:3px;border-left:2px solid #ff9944">'
                   f'<div style="font-size:.74rem;color:#e0c0a0">🚪 出場：{exit_advice}</div>'
                   f'</div>' if exit_advice else '')
                + f'</div>'
            )
    except Exception:
        pass

    # ── 🆕 v9.28：個股備註 banner（用戶寫的筆記）─────────────────
    try:
        from pathlib import Path as _P
        import json as _json
        _wl_path = _P(__file__).parent / 'watchlists.json'
        _user_note = None
        # 從 watchlists.json 撈 _ticker_notes
        if _wl_path.exists():
            try:
                _wld = _json.loads(_wl_path.read_text(encoding='utf-8'))
                _notes = _wld.get('_ticker_notes') or {}
                _tk_pure = ticker.replace('.TW', '').upper()
                # 同時試 ticker 和純 ticker
                _user_note = _notes.get(ticker) or _notes.get(_tk_pure)
            except Exception: pass
        # 也試 localStorage（雲端版）
        if not _user_note:
            try:
                from streamlit_local_storage import LocalStorage
                _ls = LocalStorage()
                _v = _ls.getItem('stock001_watchlists')
                if _v:
                    _wld = _json.loads(_v) if isinstance(_v, str) else _v
                    _notes = _wld.get('_ticker_notes') or {}
                    _tk_pure = ticker.replace('.TW', '').upper()
                    _user_note = _notes.get(ticker) or _notes.get(_tk_pure)
            except Exception: pass

        if _user_note:
            # 把備註當成 entry_rows 第一條（很顯眼）
            entry_rows.insert(0,
                f'<div style="background:linear-gradient(90deg,#2a1a3a,#1a2a4a);'
                f'border-left:4px solid #ffcc55;padding:8px 12px;'
                f'border-radius:6px;margin-bottom:6px">'
                f'<div style="font-size:.72rem;color:#ffcc55;font-weight:700;margin-bottom:2px">'
                f'📝 我的備註'
                f'</div>'
                f'<div style="font-size:.85rem;color:#ffe">'
                f'{_user_note}'
                f'</div>'
                f'</div>'
            )
    except Exception:
        pass

    # ── 🆕 v9.25.3：Sympathy Play 補漲訊號 detail card banner ─────
    try:
        from pathlib import Path as _P
        import json as _json
        sp = _P(__file__).parent / 'sympathy_latest.json'
        if sp.exists():
            d_sym = _json.loads(sp.read_text(encoding='utf-8'))
            _tk_search = ticker.replace('.TW', '')
            _sym_match = None
            for c in d_sym.get('candidates', []):
                if c.get('ticker') in (ticker, _tk_search) or \
                   c.get('ticker', '').replace('.TW', '') == _tk_search:
                    _sym_match = c
                    break
            if _sym_match:
                from sympathy.signal_integrator import apply_sympathy_bonus
                base_score = d.get('decision_score', 0) or 0
                bonus_info = apply_sympathy_bonus(base_score, _sym_match)
                _color = ('#3dbb6a' if _sym_match['score'] >= 0.75 else
                          '#7acc7a' if _sym_match['score'] >= 0.60 else '#a8d4a8')
                swing_rows.append(
                    f'<div style="background:linear-gradient(90deg,#0a2810,#0d1828);'
                    f'border-left:4px solid #66ff99;padding:8px 10px;'
                    f'border-radius:6px;margin-top:6px">'
                    f'<div style="font-size:.78rem;color:{_color};font-weight:700;margin-bottom:3px">'
                    f'🚀 補漲候選 · Score {_sym_match["score"]:.3f} · '
                    f'Leader {_sym_match["leader"]} ({_sym_match.get("group","")})'
                    f'</div>'
                    f'<div style="font-size:.7rem;color:#9fcc9f">'
                    f'Corr60d <b>{_sym_match["corr_60d"]:.3f}</b>｜'
                    f'SprdP <b>{_sym_match["spread_pctile"]:.3f}</b>｜'
                    f'今日落後 <b>+{_sym_match["lag_today"]*100:.2f}%</b>｜'
                    f'T3 加成 <b>+{bonus_info["bonus_applied"]:.0f}</b> 分'
                    f' (到 {bonus_info["signal_expires"]})'
                    f'</div>'
                    f'</div>'
                )
    except Exception:
        pass

    # ── 🆕 v9.24：RS Leading High（紫色點訊號）detail card banner ─────
    try:
        # 試從 screener_results.json 撈該 ticker 的訊號
        _rsh_data = None
        try:
            from pathlib import Path as _P
            import json as _json
            _sj = _P(__file__).parent / 'screener_results.json'
            if _sj.exists():
                _sd = _json.load(open(_sj, encoding='utf-8'))
                _bf = _sd.get('by_filter', {})
                _tk_pure = ticker.replace('.TW', '')
                for _fname, _items in _bf.items():
                    if not _fname.startswith('🟣 RS 領先創新高'): continue
                    for _it in _items:
                        if _it.get('ticker') == _tk_pure and _it.get('rs_leading_high_score') is not None:
                            _rsh_data = _it
                            break
                    if _rsh_data: break
        except Exception:
            _rsh_data = None

        if _rsh_data:
            _score = _rsh_data.get('rs_leading_high_score') or 0
            _purp = _rsh_data.get('rs_leading_high_purple_dots') or 0
            _dist = _rsh_data.get('rs_leading_high_distance') or 0
            _rank = _rsh_data.get('rs_leading_high_rank') or '?'
            _theme = _rsh_data.get('rs_leading_high_theme') or ''
            _theme_badge = (f' · <span style="color:#ffaa55">[{_theme}]</span>'
                            if _theme else '')
            # 顏色按分數
            _color = ('#3dbb6a' if _score >= 80 else
                      '#a866ff' if _score >= 60 else
                      '#c294d6')
            swing_rows.append(
                f'<div style="background:linear-gradient(90deg,#1a0828,#0d1828);'
                f'border-left:4px solid #b266ff;padding:8px 10px;'
                f'border-radius:6px;margin-top:6px">'
                f'<div style="font-size:.78rem;color:{_color};font-weight:700;margin-bottom:3px">'
                f'🟣 RS 領先創新高 · Score {_score:.1f}/100 · Rank #{_rank}{_theme_badge}'
                f'</div>'
                f'<div style="font-size:.7rem;color:#a87acc">'
                f'紫色點 <b>{_purp}</b> 次（近 20d）｜距 63d 高 <b>{_dist:.1f}%</b>｜'
                f'機構累積足跡訊號 — RS 線創高，但股價仍在整理'
                f'</div>'
                f'</div>'
            )
    except Exception:
        pass

    # ── 🆕 v9.22：雙底雙頂進階分析（職業交易員五大關鍵 + 三段建倉） ─────
    try:
        _db = d.get('double_bottom_info') or {}
        _dt = d.get('double_top_info') or {}
        _has_db = _db.get('is_double_bottom', False)
        _has_dt = _dt.get('is_double_top', False)

        def _build_double_block(info, side='bull'):
            """建構雙底/雙頂 detail card 區塊"""
            _st = info.get('status', 'none')
            _stage = info.get('entry_stage', 'wait')
            _grade = info.get('quality_grade', 'D')
            _score = info.get('quality_score', 0)
            _decay = info.get('momentum_decay') or {}
            _rxn = info.get('reaction_kbar') or {}
            _liq = info.get('liquidity_sweep', False)
            _valid = info.get('breakout_validity') or {}
            _pos = info.get('position_context', 'unknown')
            _pos_q = info.get('position_quality', 'neutral')

            # Label / 顏色 by status
            if side == 'bull':
                _label_map = {
                    'B_breakout_buy': ('🟢 W 底突破 — 補滿區（B 段進場）', '#3dbb6a', '#0a2a14'),
                    'A_test_buy':     ('🟡 W 底試單區（A 段 1/3 試單）', '#e8c050', '#1a1408'),
                    'C_retest_buy':   ('🟢 W 底回踩 neckline 補滿（C 段）', '#3dbb6a', '#0a2a14'),
                    'confirmed':      ('🟡 W 底成形 — 等突破', '#e8a020', '#1a1408'),
                    'forming':        ('🔵 W 底形成中', '#7abadd', '#0a1828'),
                    'failed':         ('❌ W 底失敗（已跌破第 1 底）', '#aa4040', '#1a0808'),
                }
                _lbl, _c, _bg = _label_map.get(_st, ('🟡 W 底', '#e8a020', '#1a1408'))
                _l_label = '左底'; _r_label = '右底'
                _l_data = info.get('left_bottom') or {}
                _r_data = info.get('right_bottom') or {}
                _m_data = info.get('middle_peak') or {}
                _decay_label_map = {
                    'volume_ratio': ('量縮（第2次/第1次）', 0.8, '<', '量縮 → 空方力竭'),
                    'body_ratio':   ('黑K實體比', 0.7, '<', '黑K縮短 → 空方無力'),
                    'shadow_ratio': ('下影比', 1.2, '>', '下影增 → 多方反擊增'),
                    'rev_kbar_count_diff': ('紅K數量差', 0, '>', '紅K增 → 多方接管'),
                }
            else:
                _label_map = {
                    'B_breakdown_short': ('🔴 M 頂跌破 — 補滿放空（B 段）', '#ff5555', '#3a0a0a'),
                    'A_test_short':       ('🟠 M 頂試單區（A 段 1/3 放空）', '#ff9944', '#2a1605'),
                    'C_retest_short':     ('🔴 M 頂反彈 neckline 補滿（C 段）', '#ff5555', '#3a0a0a'),
                    'confirmed':           ('🟠 M 頂成形 — 等跌破', '#ff9944', '#2a1605'),
                    'forming':             ('🔵 M 頂形成中', '#7abadd', '#0a1828'),
                    'failed':              ('❌ M 頂失敗（已突破第 1 頂）', '#40aa40', '#0a2a10'),
                }
                _lbl, _c, _bg = _label_map.get(_st, ('🟠 M 頂', '#ff9944', '#2a1605'))
                _l_label = '左頂'; _r_label = '右頂'
                _l_data = info.get('left_top') or {}
                _r_data = info.get('right_top') or {}
                _m_data = info.get('middle_trough') or {}
                _decay_label_map = {
                    'volume_ratio': ('量縮（第2次/第1次）', 0.8, '<', '量縮 → 多方力竭'),
                    'body_ratio':   ('紅K實體比', 0.7, '<', '紅K縮短 → 多方無力'),
                    'shadow_ratio': ('上影比', 1.2, '>', '上影增 → 空方反擊增'),
                    'rev_kbar_count_diff': ('黑K數量差', 0, '>', '黑K增 → 空方接管'),
                }

            _neck = info.get('neckline_price', 0)
            _tgt = info.get('target_price', 0)
            _sim = info.get('similarity_pct', 0)
            _sep = info.get('separation_days', 0)
            _stop = info.get('stop_loss', 0)
            _cur = d.get('close', 0) or 0
            _mid_label = '中間 peak' if side == 'bull' else '中間 trough'

            # ─── 五大關鍵打勾 ───
            _key_rows = []
            # 1. 位置
            _pos_ok = _pos_q == 'high_prob'
            _key_rows.append(
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _pos_ok else "#aa6655")}">'
                f'{"✅" if _pos_ok else "⚠️"} 1. 位置：{_pos}（{_pos_q}）</div>'
            )
            # 2. 動能衰減
            _decay_score = _decay.get('decay_score', 0)
            _decay_ok = _decay_score >= 2
            _decay_lines = [f'{"✅" if _decay_ok else "⚠️"} 2. 動能衰減 {_decay_score}/4：']
            for _key, (_n, _th, _op, _desc) in _decay_label_map.items():
                _v = _decay.get(_key, 0)
                if _op == '<':
                    _good = _v < _th
                else:
                    _good = _v > _th
                _icon = '✓' if _good else '·'
                _decay_lines.append(f'　{_icon} {_n} {_v}（{"達標" if _good else "未達"}{_op}{_th}）')
            _key_rows.append(
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _decay_ok else "#aa6655")}">'
                + '<br>'.join(_decay_lines) +
                f'</div>'
            )
            # 3. 反應 K 棒
            _rxn_ok = _rxn.get('has_reaction', False)
            _rxn_desc = _rxn.get('desc', '無') if _rxn_ok else '無強反應 K 棒'
            _key_rows.append(
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _rxn_ok else "#aa6655")}">'
                f'{"✅" if _rxn_ok else "⚠️"} 3. 表態 K 棒：{_rxn_desc}</div>'
            )
            # 4. 掃流動性 + 頸線突破
            _valid_score = _valid.get('validity_score', 0)
            _bo_ok = _valid_score >= 2
            _bo_parts = []
            _bo_parts.append(f'量增 {"✓" if _valid.get("volume_surge") else "✗"}')
            _bo_parts.append(f'實體 {"✓" if _valid.get("body_breakout") else "✗"}')
            _bo_parts.append(f'無回打 {"✓" if _valid.get("no_pull_back") else "✗"}')
            _liq_str = (' + 掃流動性 ✓' if _liq else '')
            _key_rows.append(
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _bo_ok else "#aa6655")}">'
                f'{"✅" if _bo_ok else "⚠️"} 4. 頸線突破 {_valid_score}/3：'
                f'{" / ".join(_bo_parts)}{_liq_str}</div>'
            )
            # 5. Pattern strength
            _strength_ok = _sim <= 3 and (info.get('rebound_pct', 0) or info.get('pullback_pct', 0)) >= 12
            _key_rows.append(
                f'<div style="font-size:.66rem;color:{("#3dbb6a" if _strength_ok else "#aa6655")}">'
                f'{"✅" if _strength_ok else "⚠️"} 5. Pattern 強度：兩端差 {_sim:.2f}%（≤3）'
                f' + 反彈 {(info.get("rebound_pct") or info.get("pullback_pct") or 0):.1f}%（≥12）</div>'
            )

            # 三段建倉行動
            _action_lines = []
            if _stage == 'A_test':
                _action_lines.append(
                    f'<b>📍 A 段：底部試單 1/3</b><br>'
                    f'進場：${_cur:.2f}（反應 K 出現）<br>'
                    f'停損：${_stop:.2f}（{_r_label}下方）<br>'
                    f'下個補滿點：頸線 ${_neck} 帶量突破')
            elif _stage == 'B_breakout':
                _action_lines.append(
                    f'<b>📍 B 段：頸線突破補滿</b><br>'
                    f'進場：${_cur:.2f}（已突破頸線）<br>'
                    f'停損：${_stop:.2f}（neckline 下方 3%）<br>'
                    f'目標：${_tgt}（測量移動）<br>'
                    f'下個補滿點：回踩 neckline 不破 + 反應 K')
            elif _stage == 'C_retest':
                _action_lines.append(
                    f'<b>📍 C 段：回踩補滿</b><br>'
                    f'進場：${_cur:.2f}（回踩 neckline 有效）<br>'
                    f'停損：${_stop:.2f}（反應 K 棒下方）<br>'
                    f'目標：${_tgt}')
            elif _stage == 'wait':
                if _st == 'forming':
                    _action_lines.append(
                        f'⏸ 等反應 K 棒出現（{_r_label}剛現，太早進場風險高）')
                elif _st == 'failed':
                    _action_lines.append(
                        f'❌ Pattern 失敗，避開')
                else:
                    _action_lines.append(
                        f'⏸ 等突破：頸線 ${_neck}<br>'
                        f'watchlist：每天看是否量增實體突破')

            # Quality Grade badge
            _grade_color = ({'A': '#ffd700', 'B': '#3dbb6a',
                             'C': '#7abadd', 'D': '#7a8899'}).get(_grade, '#7a8899')
            _grade_badge = (
                f'<span style="background:{_grade_color}22;color:{_grade_color};'
                f'border:1px solid {_grade_color}66;padding:1px 7px;'
                f'border-radius:10px;font-size:.66rem;font-weight:700">'
                f'Grade {_grade} ({_score}/5)</span>'
            )

            return (
                f'<div style="background:{_bg};border-left:4px solid {_c};'
                f'padding:8px 12px;border-radius:4px;margin-top:6px">'
                f'<b style="color:{_c};font-size:.92rem">{_lbl}</b> {_grade_badge}'
                f'<div style="font-size:.7rem;color:#a8c0d0;margin-top:4px;line-height:1.5">'
                f'{_l_label} {_l_data.get("date", "—")} ${_l_data.get("price", 0):.2f} ｜ '
                f'{_r_label} {_r_data.get("date", "—")} ${_r_data.get("price", 0):.2f} '
                f'(差 {_sim:.2f}%)<br>'
                f'{_mid_label} {_m_data.get("date", "—")} ${_neck:.2f} = neckline'
                f' ｜ 間距 {_sep} 天'
                f'</div>'
                f'<div style="margin-top:6px;padding:6px;background:#0a1422;border-radius:3px">'
                f'<b style="color:#7abadd;font-size:.74rem">🔍 五大關鍵分析</b>'
                f'{"".join(_key_rows)}'
                f'</div>'
                f'<div style="margin-top:6px;padding:6px;background:#0a1828;border-radius:3px;'
                f'border-left:2px solid #3dbb6a">'
                f'<div style="font-size:.74rem;color:#c8e0d0">'
                f'{("<br>".join(_action_lines))}</div>'
                f'</div>'
                f'</div>'
            )

        if _has_db:
            swing_rows.append(_build_double_block(_db, side='bull'))
        if _has_dt:
            swing_rows.append(_build_double_block(_dt, side='bear'))

        # 🆕 v9.23：ZigZag 對照圖（W/M/VCP 共用）
        _zz_data_uri = _build_zigzag_chart_img(d, ticker_local=ticker)
        if _zz_data_uri:
            swing_rows.append(
                f'<div style="margin-top:8px;padding:6px;background:#0a1828;'
                f'border:1px solid #1a2a3a;border-radius:6px">'
                f'<div style="font-size:.7rem;color:#7abadd;font-weight:700;'
                f'margin-bottom:4px">📊 ZigZag 對照圖（ATR×1.3）— W 底 / M 頂 / VCP 共用視覺</div>'
                f'<img src="{_zz_data_uri}" style="width:100%;border-radius:4px"/>'
                f'</div>'
            )
    except Exception:
        pass

    # ── 🆕 v9.28：杯柄 / 平台底 / Stan Weinstein 階段分析 banner ────
    try:
        from patterns.cup_and_handle import detect_cup_and_handle as _detect_cup
        from patterns.flat_base import detect_flat_base as _detect_flat
        from patterns.stage_analysis import classify_stage as _classify_stage

        _df_pat = None
        # 重用 swing 診斷的 df，若無則 fallback
        try:
            _df_pat = _df_for_state
        except NameError:
            _df_pat = None
        if _df_pat is None and ticker:
            try:
                import data_loader as _dl_pat
                _df_pat = _dl_pat.load_from_cache(ticker)
            except Exception:
                _df_pat = None

        if _df_pat is not None and len(_df_pat) >= 200:
            _rs_for_pat = d.get('rs_rating') if d else None
            # ─── Stage Analysis ────────────────────────────────
            try:
                _sg = _classify_stage(_df_pat, rs_rating=_rs_for_pat)
            except Exception:
                _sg = None
            if _sg is not None and _sg.stage > 0:
                _stage_colors = {
                    1: ('#7abadd', '#0a1828', '🏗️'),     # Basing
                    2: ('#3dbb6a', '#0a2a14', '🚀'),     # Advancing
                    3: ('#e8a020', '#1a1408', '⚠️'),     # Top
                    4: ('#ff5555', '#3a0a0a', '❌'),     # Declining
                }
                _sc, _sbg, _sicon = _stage_colors.get(_sg.stage, ('#7a8899', '#0a1422', '⚪'))
                _sub_label = {'early': '早期', 'mid': '中期', 'late': '末期'}.get(_sg.sub_stage, '')
                _transitions_html = ''
                if _sg.transition_signals:
                    _transitions_html = (
                        '<div style="margin-top:4px;font-size:.7rem;color:#ffd070">'
                        + ' ｜ '.join(f'⚡ {t}' for t in _sg.transition_signals)
                        + '</div>'
                    )
                _stage_help = {
                    1: 'Basing：均線走平，價在底部整理 — 等待突破訊號',
                    2: 'Advancing：均線上揚，價在均線上 — 持股 / 加碼期',
                    3: 'Top：均線走平，價偏高 — 風險區，建議減倉',
                    4: 'Declining：均線下彎，價跌破均線 — 不可介入',
                }.get(_sg.stage, '')
                swing_rows.append(
                    f'<div style="background:{_sbg};border-left:4px solid {_sc};'
                    f'padding:8px 12px;border-radius:4px;margin-top:6px">'
                    f'<b style="color:{_sc};font-size:.92rem">'
                    f'{_sicon} Stan Weinstein — Stage {_sg.stage} {_sg.stage_name} ({_sub_label})</b>'
                    f'<div style="font-size:.72rem;color:#c8dff0;margin-top:3px">'
                    f'30W SMA 斜率 <b>{_sg.sma30w_slope*100:+.2f}%</b> ｜ '
                    f'價偏離 SMA <b>{_sg.price_vs_sma30w*100:+.2f}%</b> ｜ '
                    f'信心 <b>{_sg.confidence*100:.0f}%</b>'
                    f'</div>'
                    f'<div style="font-size:.7rem;color:#a8c0d0;margin-top:2px">{_stage_help}</div>'
                    f'{_transitions_html}'
                    f'</div>'
                )

            # ─── Cup and Handle ───────────────────────────────
            try:
                _cup = _detect_cup(_df_pat, rs_rating=_rs_for_pat)
            except Exception:
                _cup = None
            if _cup is not None and _cup.detected and _cup.score > 0:
                if _cup.score >= 75:
                    _cc, _cbg, _clab = '#3dbb6a', '#0a2a14', '🏆 杯柄高品質'
                elif _cup.score >= 60:
                    _cc, _cbg, _clab = '#7abadd', '#0a1828', '⭐ 杯柄強訊號'
                elif _cup.score >= 40:
                    _cc, _cbg, _clab = '#e8a020', '#1a1408', '🟡 杯柄成形中'
                else:
                    _cc, _cbg, _clab = '#7a8899', '#0a1422', '⚪ 杯柄低分'
                _cup_breakout_str = ''
                if _cup.reasons and any('突破完成' in r for r in _cup.reasons):
                    _cup_breakout_str = '<span style="color:#3dbb6a">🚀 突破完成（量爆）</span> ｜ '
                elif _cup.reasons and any('突破但量' in r for r in _cup.reasons):
                    _cup_breakout_str = '<span style="color:#e8a020">⚠️ 突破但量未跟</span> ｜ '
                _cup_pivot_str = f'${_cup.pivot_price:.2f}' if _cup.pivot_price else '—'
                _cup_target_str = f'${_cup.target_price:.2f}' if _cup.target_price else '—'
                _cup_stop_str = f'${_cup.stop_loss:.2f}' if _cup.stop_loss else '—'
                _cup_reasons_html = ''
                if _cup.reasons:
                    _cup_reasons_html = (
                        '<div style="font-size:.66rem;color:#a8c0d0;margin-top:2px">'
                        + ' ｜ '.join(_cup.reasons[:3])
                        + '</div>'
                    )
                swing_rows.append(
                    f'<div style="background:{_cbg};border-left:4px solid {_cc};'
                    f'padding:8px 12px;border-radius:4px;margin-top:6px">'
                    f'<b style="color:{_cc};font-size:.92rem">'
                    f'☕ Cup and Handle — {_clab} ({_cup.pattern_variant}) · Score {_cup.score:.1f}/100</b>'
                    f'<div style="font-size:.72rem;color:#c8dff0;margin-top:3px">'
                    f'{_cup_breakout_str}'
                    f'Pivot <b>{_cup_pivot_str}</b> ｜ '
                    f'Target <b>{_cup_target_str}</b> ｜ '
                    f'Stop <b>{_cup_stop_str}</b>'
                    f'</div>'
                    f'{_cup_reasons_html}'
                    f'</div>'
                )

            # ─── Flat Base ────────────────────────────────────
            try:
                _flat = _detect_flat(_df_pat, rs_rating=_rs_for_pat)
            except Exception:
                _flat = None
            if _flat is not None and _flat.detected and _flat.score > 0:
                if _flat.score >= 75:
                    _fc, _fbg, _flab = '#3dbb6a', '#0a2a14', '🏆 平台底高品質'
                elif _flat.score >= 60:
                    _fc, _fbg, _flab = '#7abadd', '#0a1828', '⭐ 平台底強訊號'
                elif _flat.score >= 40:
                    _fc, _fbg, _flab = '#e8a020', '#1a1408', '🟡 平台底成形中'
                else:
                    _fc, _fbg, _flab = '#7a8899', '#0a1422', '⚪ 平台底低分'
                _flat_bo_str = ''
                if _flat.breakout:
                    _bvr = _flat.breakout_volume_ratio or 0
                    _flat_bo_str = (f'<span style="color:#3dbb6a">'
                                     f'🚀 突破完成（量增 {_bvr:.2f}x）</span> ｜ ')
                _flat_pivot_str = f'${_flat.pivot_point:.2f}' if _flat.pivot_point else '—'
                _flat_target_str = f'${_flat.target_price:.2f}' if _flat.target_price else '—'
                _flat_stop_str = f'${_flat.stop_loss:.2f}' if _flat.stop_loss else '—'
                _flat_extra = (f'Base #{_flat.base_count} ｜ '
                                f'深度 {_flat.base_depth*100:.1f}% ｜ '
                                f'時間 {_flat.base_duration_days}d')
                _flat_notes_html = ''
                if _flat.notes:
                    _flat_notes_html = (
                        '<div style="font-size:.66rem;color:#a8c0d0;margin-top:2px">'
                        + ' ｜ '.join(_flat.notes[:3])
                        + '</div>'
                    )
                swing_rows.append(
                    f'<div style="background:{_fbg};border-left:4px solid {_fc};'
                    f'padding:8px 12px;border-radius:4px;margin-top:6px">'
                    f'<b style="color:{_fc};font-size:.92rem">'
                    f'🟨 Flat Base — {_flab} · Score {_flat.score:.1f}/100</b>'
                    f'<div style="font-size:.72rem;color:#c8dff0;margin-top:3px">'
                    f'{_flat_bo_str}'
                    f'Pivot <b>{_flat_pivot_str}</b> ｜ '
                    f'Target <b>{_flat_target_str}</b> ｜ '
                    f'Stop <b>{_flat_stop_str}</b>'
                    f'</div>'
                    f'<div style="font-size:.7rem;color:#a8c0d0;margin-top:2px">{_flat_extra}</div>'
                    f'{_flat_notes_html}'
                    f'</div>'
                )
    except Exception:
        pass

    # ── 🆕 v9.20：綜合決策得分（Strategy B + SEPA + Recipes 加總） ──
    try:
        score = 0
        score_reasons = []

        # +2: SEPA full setup (passed + RS≥70)
        if d.get('sepa_passed') and (d.get('rs_rating') or 0) >= 70:
            score += 2
            score_reasons.append('+2 SEPA 8/8 完整')
        elif d.get('sepa_passed'):
            score += 1
            score_reasons.append('+1 SEPA 7/7（RS 未確認）')
        elif (d.get('sepa_n_met') or 0) >= 6:
            score += 0.5
            score_reasons.append('+0.5 SEPA 6/7 接近')

        # +1: VCP pattern
        _vcp = d.get('vcp_info') or {}
        if _vcp.get('is_vcp'):
            score += 1
            score_reasons.append('+1 VCP 形態')

        # +1: Strategy B 入場條件達成（多頭+ADX≥22+from_high<1+vol>1.5+RSI<70）
        _is_bull_d = d.get('is_bull') if 'is_bull' in d else (d.get('ema20', 0) > d.get('ema60', 0))
        _adx_d = d.get('adx', 0) or 0
        _from_high_d = d.get('from_high_pct', 99) or d.get('from_high', 99) or 99
        _vol_d = d.get('volume', 0) or 0
        _vol_ma_d = d.get('vol_ma20', 1) or 1
        _vol_ratio_d = (_vol_d / _vol_ma_d) if _vol_ma_d > 0 else 1
        _rsi_d = d.get('rsi', 50) or 50
        if _is_bull_d and _adx_d >= 22 and _from_high_d < 1 and _vol_ratio_d > 1.5 and _rsi_d < 70:
            score += 1
            score_reasons.append('+1 Strategy B 入場')

        # +1: T1 fresh cross
        if (d.get('ema20_cross_days') or 0) and 0 < d.get('ema20_cross_days') <= 10:
            score += 1
            score_reasons.append('+1 T1 黃金交叉 0-10d')

        # -1: imminent_dc
        if d.get('imminent_dc'):
            score -= 1
            score_reasons.append('-1 即將死叉')

        # -1: RSI ≥ 80 過熱
        if _rsi_d >= 80:
            score -= 1
            score_reasons.append('-1 RSI≥80 過熱')

        # 🆕 v9.22：雙底雙頂加減分（依 Quality Grade）
        _db_info = d.get('double_bottom_info') or {}
        _dt_info = d.get('double_top_info') or {}
        _db_st = _db_info.get('status', 'none')
        _dt_st = _dt_info.get('status', 'none')
        _db_g = _db_info.get('quality_grade', 'D')
        _dt_g = _dt_info.get('quality_grade', 'D')
        # Grade A 加分多，D 加分少
        _db_bonus_map = {'A': 2.5, 'B': 2.0, 'C': 1.5, 'D': 1.0}
        _dt_bonus_map = {'A': -2.5, 'B': -2.0, 'C': -1.5, 'D': -1.0}
        if _db_st in ('B_breakout_buy', 'A_test_buy', 'C_retest_buy', 'breakout'):
            _bonus = _db_bonus_map.get(_db_g, 1.0)
            score += _bonus
            score_reasons.append(f'+{_bonus} 雙底進場 (Grade {_db_g})')
        elif _db_st == 'confirmed':
            score += 0.5
            score_reasons.append('+0.5 雙底成形（待突破）')
        if _dt_st in ('B_breakdown_short', 'A_test_short', 'C_retest_short', 'breakdown'):
            _bonus = _dt_bonus_map.get(_dt_g, -1.0)
            score += _bonus
            score_reasons.append(f'{_bonus:+.1f} 雙頂出場 (Grade {_dt_g})')
        elif _dt_st == 'confirmed':
            score -= 0.5
            score_reasons.append('-0.5 雙頂成形（風險）')

        # 總分判斷
        score_max = 5
        if score >= 4:
            verdict = '🏆 強烈進場（多訊號疊加）'
            v_color = '#3dbb6a'; v_bg = '#0a2a14'
        elif score >= 2.5:
            verdict = '✅ 適合進場'
            v_color = '#7abadd'; v_bg = '#0a1828'
        elif score >= 1:
            verdict = '🟡 觀望候選'
            v_color = '#e8a020'; v_bg = '#1a1408'
        elif score >= 0:
            verdict = '⚪ 中性，無明顯訊號'
            v_color = '#7a8899'; v_bg = '#0a1422'
        else:
            verdict = '❌ 風險訊號多，避開'
            v_color = '#ff5555'; v_bg = '#1a0808'

        # 🐛 fix v9.20.1：用 () 包裹 if/else 避免 Python operator precedence 切斷 div
        _score_reasons_str = ' ; '.join(score_reasons) if score_reasons else '無訊號加分'
        swing_rows.append(
            f'<div style="background:{v_bg};border-left:4px solid {v_color};'
            f'padding:8px 12px;border-radius:4px;margin-top:6px">'
            f'<b style="color:{v_color};font-size:.92rem">🎯 綜合決策：{verdict}</b>'
            f'<div style="font-size:.74rem;color:#c8dff0;margin-top:3px">'
            f'總分 {score:+.1f} / {score_max}  ｜  {_score_reasons_str}'
            f'</div></div>'
        )
    except Exception:
        pass

    # ── ③ 出場停損（ATR動態停損價）──────────────────────────────
    risk_rows = []
    _atr_mult   = 2.0 if (not is_bull and _t4_rising) else 2.5
    _atr_mult_s = "2.0（T4反彈嚴格停損）" if _atr_mult == 2.0 else "2.5"
    if atr14 is not None and close is not None and close > 0:
        stop_dist  = atr14 * _atr_mult
        stop_price = close - stop_dist
        stop_pct   = stop_dist / close * 100
        risk_rows.append(
            f'<div>🛡️ <b>停損價 <span style="color:#ff7a7a">{stop_price:.2f}</span></b>'
            f'&nbsp;<span style="color:#7a8899">（收盤 {close:.2f} − ATR×{_atr_mult_s} {stop_dist:.2f}'
            f' = -{stop_pct:.1f}%）</span></div>'
        )

        # 🆕 停損觸發後的 VWAP 出場建議（盯盤限定）
        if vwap_today and close <= stop_price:
            _vw_above = vwap_today > close
            if _vw_above:
                risk_rows.append(
                    f'<div style="background:#1a0808;border-left:2px solid #ff9944;'
                    f'padding:5px 8px;margin:5px 0;border-radius:3px">'
                    f'<span style="color:#ff9944;font-size:.78rem">'
                    f'⚠️ <b>停損已觸發</b>：收盤 {close:.2f} ≤ 停損 {stop_price:.2f}，'
                    f'但 VWAP {vwap_today:.2f} 仍 <b>高於現價 {((vwap_today-close)/close*100):.1f}%</b>。'
                    f'<br>📌 <b>盯盤可掛 ≥ {vwap_today:.2f} 限價賣單</b>（搶反彈出場）；'
                    f'<b>不能盯盤直接市價出</b>（避免續跌擴大虧損）'
                    f'</span></div>'
                )
            else:
                risk_rows.append(
                    f'<div style="background:#1a0808;border-left:2px solid #ff5555;'
                    f'padding:5px 8px;margin:5px 0;border-radius:3px">'
                    f'<span style="color:#ff5555;font-size:.78rem">'
                    f'🚨 <b>停損已觸發 + 全日下跌</b>：收盤 {close:.2f} 連 VWAP '
                    f'{vwap_today:.2f} 都跌破，<b>立即市價出場</b>，VWAP 不適用'
                    f'</span></div>'
                )

    else:
        risk_rows.append('<div style="color:#7a8899">ATR 資料不足，無法計算動態停損</div>')

    # ── ④ 出場獲利（v3：ATR/Price 自動分類出場規則）──────────────
    exit_rows = []

    if is_bull:
        ema_gap_pct = (ema20 - ema60) / ema60 * 100 if ema60 else None
        # 計算 ATR/Price 判斷股性
        _rel_atr    = (atr14 / close * 100) if (atr14 and close and close > 0) else 0
        _is_hv      = _rel_atr > 3.5   # 高波動飆股

        # 顯示股性分類標籤
        if _rel_atr > 0:
            _hv_label = (f'🚀 高波動飆股模式（ATR/P {_rel_atr:.1f}% &gt; 3.5%）'
                         if _is_hv else
                         f'🛡️ 穩健股模式（ATR/P {_rel_atr:.1f}% ≤ 3.5%）')
            _hv_color = "#e8c050" if _is_hv else "#8ab0c8"
            exit_rows.append(f'<div style="margin-bottom:3px"><span style="color:{_hv_color};font-size:.78rem">{_hv_label}</span></div>')

        # EMA 差距 → 死亡交叉遠近（所有股票共用）
        # 🆕 v9.10s：剛黃金交叉時 gap 自然小，不該標「即將死叉」
        if ema_gap_pct is not None:
            _just_crossed_up = (cross_days is not None and 0 < cross_days <= 10)
            if ema_gap_pct < 1.0 and _just_crossed_up:
                # 剛黃金交叉的擴張期
                gap_color = "#3dbb6a"; gap_icon = "🔥"
                gap_note = f"黃金交叉 {cross_days} 天前（擴張期，差距正常）"
            elif ema_gap_pct < 1.0:
                gap_color = "#ff5555"; gap_icon = "🚨"; gap_note = "即將死叉！準備出場"
            elif ema_gap_pct < 3.0:
                gap_color = "#e8a020"; gap_icon = "⚠️"; gap_note = "接近死叉，密切關注"
            else:
                gap_color = "#3dbb6a"; gap_icon = "✅"; gap_note = "趨勢持續"
            exit_rows.append(
                f'<div>📤 <span style="color:#8ab0c8">EMA死亡交叉：</span>'
                f'<span style="color:{gap_color}">{gap_icon} 差距 {ema_gap_pct:.1f}%（{gap_note}）</span></div>'
            )

        if _is_hv:
            # ── 高波動飆股：只守EMA死叉，不設RSI出場 ──
            exit_rows.append(
                f'<div>📤 <span style="color:#c8b87a">RSI出場：</span>'
                f'<span style="color:#7a8899">停用（飆股模式：RSI出場砍掉主升段，回測損失 +400%）</span></div>'
            )
            exit_rows.append(
                '<div><span style="color:#c8b87a;font-size:.72rem">'
                '🚀 持倉到EMA死叉為止，不提前出場</span></div>'
            )
            # badge
            _ema_danger  = ema_gap_pct is not None and ema_gap_pct < 1.0
            _ema_warning = ema_gap_pct is not None and 1.0 <= ema_gap_pct < 3.0
            if _ema_danger:
                _exit_label = "⚠️ 出場訊號"; _exit_bg = "#2a0808"; _exit_fg = "#ff5555"
            elif _ema_warning:
                _exit_label = "注意觀察";    _exit_bg = "#1a1200"; _exit_fg = "#e8a020"
            else:
                _exit_label = "安全持倉";    _exit_bg = "#0a1e10"; _exit_fg = "#3dbb6a"
        else:
            # ── 穩健股：EMA死叉 + ADX<25時RSI>75 ──
            if adx is not None and adx < 25:
                rsi_gap = (75 - rsi) if rsi is not None else None
                if rsi is not None and rsi > 75:
                    rsi_color = "#ff5555"
                    rsi_note  = f"🚨 RSI {rsi_str} > 75，出場條件已觸發！"
                elif rsi_gap is not None and rsi_gap < 5:
                    rsi_color = "#e8a020"
                    rsi_note  = f"⚠️ RSI {rsi_str}，接近門檻 75（還差 {rsi_gap:.1f} 點）"
                else:
                    _gap_s    = f"{rsi_gap:.1f}" if rsi_gap is not None else "N/A"
                    rsi_color = "#c8b87a"
                    rsi_note  = f"RSI {rsi_str}，距出場門檻 75 還差 {_gap_s} 點"
                exit_rows.append(
                    f'<div>📤 <span style="color:#8ab0c8">ADX {adx_str} &lt; 25 → RSI出場（門檻75）：</span>'
                    f'<span style="color:{rsi_color}">{rsi_note}</span></div>'
                )
            else:
                exit_rows.append(
                    f'<div>📤 <span style="color:#7a8899">ADX {adx_str} ≥ 25 → 強趨勢，持到死叉</span></div>'
                )
            exit_rows.append(
                '<div><span style="color:#c8b87a;font-size:.72rem">'
                '🛡️ 穩健股：停損 ATR×2.5（ADX≥30 用 ×3.0）</span></div>'
            )
            # badge
            _rsi_triggered = adx is not None and adx < 25 and rsi is not None and rsi > 75
            _ema_danger    = ema_gap_pct is not None and ema_gap_pct < 1.0
            _rsi_warning   = adx is not None and adx < 25 and rsi is not None and rsi >= 70
            _ema_warning   = ema_gap_pct is not None and 1.0 <= ema_gap_pct < 3.0
            if _rsi_triggered or _ema_danger:
                _exit_label = "⚠️ 出場訊號"; _exit_bg = "#2a0808"; _exit_fg = "#ff5555"
            elif _rsi_warning or _ema_warning:
                _exit_label = "注意觀察";    _exit_bg = "#1a1200"; _exit_fg = "#e8a020"
            else:
                _exit_label = "安全持倉";    _exit_bg = "#0a1e10"; _exit_fg = "#3dbb6a"
    elif _t4_rising:
        exit_rows.append(
            '<div>📤 <span style="color:#ff9944">T4反彈出場：RSI 回升至 &gt; 55 或 EMA 黃金交叉時出場</span></div>'
        )
        _exit_label = "T4 出場條件"; _exit_bg = "#2a1500"; _exit_fg = "#ff9944"
    else:
        exit_rows.append(
            '<div style="color:#7a8899">空頭期間無持倉，不需出場訊號。等待 EMA 黃金交叉後重新評估。</div>'
        )
        _exit_label = "空頭 — 不持倉"; _exit_bg = "#0a1020"; _exit_fg = "#555e6a"

    # 🆕 VWAP 出場側建議（93 檔回測 +VWAPEXEC：出場價 = max(close, VWAP)）
    # 適用於「獲利了結 / 訊號出場」；停損出場另有規範（見 ③ 停損區塊）
    if vwap_today and close:
        if close > vwap_today:
            # 出場有利：藍色強調框
            _vw_pct = (close - vwap_today) / vwap_today * 100
            exit_rows.append(
                f'<div style="background:#08131f;border-left:3px solid #7abadd;'
                f'padding:6px 10px;margin:5px 0;border-radius:3px">'
                f'<span style="color:#7abadd;font-size:.85rem">'
                f'<b>📈 VWAP 出場建議</b>　收盤 {close:.2f} 高於 VWAP '
                f'<b style="font-size:.95rem">{vwap_today:.2f}</b> '
                f'(<b>+{_vw_pct:.1f}%</b>)，若觸發出場訊號（RSI&gt;70 / 死叉 / 高乖離），'
                f'<b>盤中可在 ≥ {vwap_today:.2f} 掛賣單賣得更貴</b>'
                f'</span></div>'
            )
        else:
            # close ≤ VWAP：警告框（黃色），告知賣價不利
            _vw_pct = (vwap_today - close) / vwap_today * 100
            exit_rows.append(
                f'<div style="background:#1a1605;border-left:3px solid #d4a020;'
                f'padding:6px 10px;margin:5px 0;border-radius:3px">'
                f'<span style="color:#e8b830;font-size:.85rem">'
                f'<b>⚠️ VWAP 出場提醒</b>　收盤 {close:.2f} 低於 VWAP '
                f'<b style="font-size:.95rem">{vwap_today:.2f}</b> '
                f'(<b>-{_vw_pct:.1f}%</b>)，<b>賣價不利</b>；'
                f'若非觸發停損，可等盤中反彈至 ≥ {vwap_today:.2f} 再賣'
                f'</span></div>'
            )

    # ── ④ 推薦策略 ────────────────────────────────────────────
    # 根據 ADX、EMA、RSI、黃金交叉時間，自動選出最大化獲利的策略
    rec_rows = []

    if not is_bull:
        if _t4_rising:
            _rec_name   = "T4 空頭反彈"
            _rec_color  = "#ff9944"
            _rec_badge  = "background:#2a1500;color:#ff9944;border:1px solid #ff994455"
            _rec_reason = "空頭市場中 RSI 連2日回升，短線逆勢反彈機會"
            _rec_entry  = "立即可進，部位縮小至 1/2"
            _rec_exit   = "RSI 回升至 55 或 EMA 黃金交叉出場"
            _rec_stop   = "ATR × 2.0（比多頭更緊）"
            _rec_warn   = "⚠️ 逆趨勢操作，嚴格停損，不加碼"
        else:
            _rec_name   = "不操作 — 等待訊號"
            _rec_color  = "#7a8899"
            _rec_badge  = "background:#0a1020;color:#7a8899;border:1px solid #7a889944"
            _rec_reason = "空頭市場，無論哪種主動策略勝率均低"
            _rec_entry  = "等待 EMA 黃金交叉（EMA20 穿越 EMA60）後重新評估"
            _rec_exit   = "—"
            _rec_stop   = "—"
            _rec_warn   = ""
    elif not adx_ok:
        _rec_name   = "不操作 — 假多頭"
        _rec_color  = "#e8a020"
        _rec_badge  = "background:#1a1200;color:#e8a020;border:1px solid #e8a02044"
        _rec_reason = f"ADX {adx_str} < 22，趨勢強度不足，回測顯示此類市況進場均虧損"
        _rec_entry  = "等待 ADX 回升至 22+ 後重新評估"
        _rec_exit   = "—"
        _rec_stop   = "—"
        _rec_warn   = ""
    else:
        # 多頭 + ADX ≥ 22，根據強度與時機選策略
        _is_strong   = (adx is not None and adx >= 30)
        _is_fresh    = (cross_days is not None and 0 < cross_days <= 10)
        _is_pullback = (rsi is not None and rsi < 50)
        _is_hot      = (rsi is not None and rsi >= 70)
        _adx_rising  = (d.get("adx_prev") is not None and adx is not None
                        and adx > d.get("adx_prev", adx))

        # 🆕 v9.11：被即將死叉阻擋 → 整段推薦策略改成「不進場」訊息
        if _entry_blocked_by_dc:
            _rec_name   = "🛑 不進場 — 即將死叉"
            _rec_color  = "#ff7755"
            _rec_badge  = "background:#2a0a0a;color:#ff7755;border:1px solid #ff775566"
            _rec_reason = (f"雖然 RSI {rsi_str} < 50 / 黃金交叉 {cross_days} 天前 等進場條件成立，"
                           f"但 EMA20 距 EMA60 僅 {(ema20-ema60)/atr14:.2f} ATR + 多頭已 {cross_days} 天，"
                           f"隨時可能死叉。此時進場將陷入「進場 → 立刻死叉 → 出場」尷尬。")
            _rec_entry  = "❌ 不進場（等死叉發生後再評估，或改觀察 T4 反彈條件）"
            _rec_exit   = "—"
            _rec_stop   = "—"
            _rec_warn   = ("📊 已驗證：在多頭排列下，早期看空訊號 alpha 極弱（差距<1%）；"
                            "imminent_dc + ATR 停損是最佳保護組合")
        elif _is_strong and _is_fresh:
            # ADX ≥ 30 + 剛黃金交叉 → 飆股模式，②趨勢最大化
            _rec_name   = "② 趨勢EMA（飆股模式）"
            _rec_color  = "#f0c030"
            _rec_badge  = "background:#1a1400;color:#f0c030;border:1px solid #f0c03055"
            _rec_reason = (f"ADX {adx_str} ≥ 30 且黃金交叉剛發生（{cross_days} 天前），"
                           f"強趨勢啟動初期，②趨勢EMA回測勝率最高")
            _rec_entry  = "立即進場（T1 黃金交叉），不等拉回"
            _rec_exit   = "持到 EMA 死亡交叉才出場（回測：RSI出場會砍掉飆股主升段）"
            _rec_stop   = "ATR × 2.5 作底線停損，獲利 ≥ 30% 後改用死叉追蹤"
            _rec_warn   = "🚀 強趨勢股不設獲利目標，回測 8021 +1410%、3167 +440%"
        elif _is_strong and _is_pullback:
            # ADX ≥ 30 + 已在多頭 + RSI 拉回 → ⑦T3 最佳買點
            _rec_name   = "⑦ 自適應T3（強趨勢拉回買點）"
            _rec_color  = "#3dbb6a"
            _rec_badge  = "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55"
            _rec_reason = (f"ADX {adx_str} ≥ 30 強趨勢，RSI {rsi_str} < 50 回調到位，"
                           f"這是強勢股最佳加碼點")
            _rec_entry  = f"立即進場（RSI {rsi_str} < 50，T3 拉回進場）"
            _rec_exit   = "EMA 死亡交叉出場（ADX≥30強趨，不設RSI出場目標）"
            _rec_stop   = "ATR × 2.5（ADX≥30 用 ×3.0）"
            _rec_warn   = "🚀 強趨勢拉回是加碼點，RSI出場會提早離場"
        elif _is_strong and not _is_pullback and not _is_hot:
            # ADX ≥ 30 但 RSI 在中間帶（50~70）
            _rec_name   = "⑦ 自適應T3（等待拉回）"
            _rec_color  = "#7abadd"
            _rec_badge  = "background:#0a1628;color:#7abadd;border:1px solid #7abadd44"
            _rec_reason = (f"ADX {adx_str} 強，但 RSI {rsi_str} 偏高，"
                           f"等待回調至 RSI < 50 再進場獲得更佳風報比")
            _rec_entry  = "等待 RSI 回落至 50 以下（T3）再進場"
            _rec_exit   = "EMA 死亡交叉出場（強趨勢不提前出，等趨勢結束）"
            _rec_stop   = "ATR × 2.5（ADX≥30 用 ×3.0）"
            _rec_warn   = ""
        elif not _is_strong and _is_fresh:
            # ADX 22~30 + 剛黃金交叉 → ⑦T1 穩健進場
            _rec_name   = "⑦ 自適應T1（穩健進場）"
            _rec_color  = "#3dbb6a"
            _rec_badge  = "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55"
            _rec_reason = (f"黃金交叉 {cross_days} 天前，ADX {adx_str}（22~30 穩健趨勢），"
                           f"⑦T1 進場配合 ATR 停損，風報比合理")
            _rec_entry  = f"立即進場（黃金交叉 {cross_days} 天前，T1）"
            _rec_exit   = "ADX < 25 時 RSI > 70 提前出場；ADX ≥ 25 持到死叉"
            _rec_stop   = "ATR × 2.5（ADX≥30 用 ×3.0）"
            _rec_warn   = "⚠️ 趨勢強度中等，需更嚴守停損"
        elif not _is_strong and _is_pullback:
            # ADX 22~30 + RSI 拉回 → ⑦T3
            _rec_name   = "⑦ 自適應T3（拉回進場）"
            _rec_color  = "#3dbb6a"
            _rec_badge  = "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55"
            _rec_reason = (f"多頭中段，RSI {rsi_str} < 50 拉回，ADX {adx_str} 趨勢確認中，"
                           f"⑦T3 是此情境下勝率最高的進場方式")
            _rec_entry  = f"立即進場（RSI {rsi_str} < 50，T3 拉回進場）"
            _rec_exit   = "ADX < 25 時 RSI > 70 出場；ADX ≥ 25 持到死叉"
            _rec_stop   = "ATR × 2.5（ADX≥30 用 ×3.0）"
            _rec_warn   = ""
        elif _is_hot:
            # RSI ≥ 70
            _rec_name   = "等待回調 — 不追高"
            _rec_color  = "#c8b87a"
            _rec_badge  = "background:#1a1805;color:#c8b87a;border:1px solid #c8b87a44"
            _adx_note   = "（弱趨勢，過熱後易反轉）" if not _is_strong else "（強趨勢，但短期過熱）"
            _rec_reason = (f"RSI {rsi_str} ≥ 70 多頭過熱{_adx_note}，"
                           f"追高進場勝率低，等待 RSI 回落至 50 以下再進")
            _rec_entry  = "等待 RSI 回落至 50（T3）再進場"
            _rec_exit   = "—"
            _rec_stop   = "—"
            _rec_warn   = ""
        else:
            # RSI 50~70，ADX 22~30，無交叉（v7 已移除 T2，改為等待 T3 確認）
            _rec_name   = "⑦ 等待 T3 拉回"
            _rec_color  = "#c8b87a"
            _rec_badge  = "background:#1a1805;color:#c8b87a;border:1px solid #c8b87a44"
            _rec_reason = (f"多頭市場中段，RSI {rsi_str} 偏高，"
                           f"等 T3（RSI<50）拉回再進場（v7 已移除 T2 中段進場）")
            _rec_entry  = "等待 RSI < 50 出現 T3 拉回信號再進場"
            _rec_exit   = "ADX < 25 時 RSI > 70 出場；ADX ≥ 25 持到死叉"
            _rec_stop   = "ATR × 2.5（ADX≥30 用 ×3.0）"
            _rec_warn   = ""

    # ── 🆕 策略風險匹配檢查（防 2313 型「保守選股 vs 飆股訊號」衝突）──
    _mismatch_warns = []
    if is_bull and adx_ok and _is_conservative_style:
        # 保守風格遇到飆股訊號：警告
        if _is_high_vol:
            _mismatch_warns.append(
                f"當前策略是<b>保守風格</b>，但這檔 ATR/P {_rel_atr_global:.1f}% > 5% 屬<b>高波動飆股</b>，"
                f"歷史風報比 0.99 是建立在低波動股，不適用此標的"
            )
        if _is_extended:
            _mismatch_warns.append(
                f"股價已距 SMA200 約 <b>+{(_ext_200-1)*100:.0f}%</b>（過度延伸），"
                f"保守風格設計為低基期長線進場，此時追高勝率偏低"
            )
        if _weak_support:
            _mismatch_warns.append(
                f"收盤距 EMA60 僅 <b>{_ema60_atr_dist:.2f} ATR</b>（弱支撐），"
                f"明日小跌即可能跌破，停損 -{(atr14*2.5/close*100):.0f}% 風險已逼近"
            )

    # 若有警告，把建議降級
    if _mismatch_warns and _rec_name not in ("不操作 — 等待訊號", "不操作 — 假多頭",
                                              "等待回調 — 不追高", "⑦ 等待 T3 拉回"):
        _rec_name_orig = _rec_name
        _rec_name = "⚠️ 不建議進場（保守風格 vs 高風險訊號）"
        _rec_color = "#e8a020"
        _rec_badge = "background:#1a1200;color:#e8a020;border:1px solid #e8a02055"
        _rec_reason = (
            f"原訊號「{_rec_name_orig}」技術上有效，但與你選的「保守」策略風險不匹配。"
            f"建議：①縮減部位至 1/2 ②手動上移停損至 EMA60 下方 ③或改選「平衡/進攻」風格"
        )
        _rec_warn = "🛡️ 保守風格：低基期 + 低波動才是甜蜜區"

    # 進攻風格反向警告（過度保守）
    if (is_bull and adx_ok and _is_aggressive_style and not _is_high_vol
            and _ext_200 is not None and _ext_200 < 1.05 and not (t1_ok or _is_pullback if 'is_pullback' in dir() else False)):
        # 進攻風格在低波動 + 未延伸標的 → 提示資金效率
        pass  # 暫不警告，避免過度提示

    # 推薦策略 HTML 組裝
    rec_badge_html = (
        f'<span style="{_rec_badge};border-radius:4px;padding:2px 8px;'
        f'font-size:.72rem;font-weight:700">{_rec_name}</span>'
    )
    rec_rows.append(
        f'<div style="margin-bottom:4px">{rec_badge_html}'
        f'&nbsp;<span style="color:#8ab0c8;font-size:.75rem">{_rec_reason}</span></div>'
    )

    # 顯示風險匹配警告列表
    if _mismatch_warns:
        for w in _mismatch_warns:
            rec_rows.append(
                f'<div style="display:flex;gap:6px;margin-top:3px">'
                f'<span style="color:#e8a020;font-size:.7rem;white-space:nowrap">⚠️ 風險</span>'
                f'<span style="color:#f0c890;font-size:.74rem">{w}</span></div>'
            )

    # 🆕 接刀風險警告（B 方案：警告但不修改回測邏輯）
    # 實證統計來自全市場 5306 筆 T4 反彈交易（2020-2026）
    if _is_falling_knife:
        if _imminent_dc:
            _dc_status = (f"⏳ <b>EMA20 即將死叉</b>"
                          f"（距 EMA60 僅 {(ema20-ema60):.2f} 元 &lt; 1 ATR）")
        else:
            _dc_status = f"💀 死叉 <b>{abs(cross_days)}</b> 天前"
        _knife_header = (
            f"<b>🔪 接刀風險偵測</b>：{_dc_status} + "
            f"從 60 日高 <b>{high60:.2f}</b> 跌至 <b>{close:.2f}</b>"
            f"（-{_drawdown_pct:.1f}%）+ %B {pct_b_now*100:.0f}% 在下軌"
        )
        _knife_stats = (
            "<b>歷史實證（全市場 299 筆接刀情境）</b>："
            "<br>📈 噴出 &gt; +10%：<b style='color:#3dbb6a'>21.7%</b>"
            "（一般 T4 為 20.9%，<b>機率相當</b>）"
            "<br>📉 重摔 &lt; -10%：<b style='color:#ff5555'>16.1%</b>"
            "（一般 T4 僅 7.4%，<b>×2.2 倍風險</b>）"
            "<br>⚖️ 平均報酬：<b>+1.19%</b>（一般 +2.66%，期望值砍半）"
            "<br>✅ 勝率：53.2%（一般 58.4%）"
        )
        _knife_action = (
            "<b style='color:#f0c030'>建議</b>：部位 ×0.5、ATR×2.0 嚴格停損 "
            "→ 把單筆下檔壓到 ~5%，16% 重摔機率就無關緊要"
        )
        rec_rows.append(
            f'<div style="margin-top:8px;background:#2a1500;'
            f'border-left:3px solid #ff5555;padding:8px 10px;border-radius:3px;'
            f'font-size:.74rem;line-height:1.7">'
            f'<div style="color:#ffb090">{_knife_header}</div>'
            f'<div style="color:#c8dff0;margin-top:5px;padding-top:5px;'
            f'border-top:1px solid #5a2010">{_knife_stats}</div>'
            f'<div style="color:#ffd980;margin-top:5px">{_knife_action}</div>'
            f'</div>'
        )
    if _rec_entry != "—":
        rec_rows.append(
            f'<div style="display:flex;gap:6px">'
            f'<span style="color:#5a9acf;font-size:.7rem;white-space:nowrap">📥 進場</span>'
            f'<span style="color:#c8dff0;font-size:.75rem">{_rec_entry}</span></div>'
        )
    if _rec_exit != "—":
        rec_rows.append(
            f'<div style="display:flex;gap:6px">'
            f'<span style="color:#5a9acf;font-size:.7rem;white-space:nowrap">📤 出場</span>'
            f'<span style="color:#c8dff0;font-size:.75rem">{_rec_exit}</span></div>'
        )
    if _rec_stop != "—":
        rec_rows.append(
            f'<div style="display:flex;gap:6px">'
            f'<span style="color:#5a9acf;font-size:.7rem;white-space:nowrap">🛡️ 停損</span>'
            f'<span style="color:#c8dff0;font-size:.75rem">{_rec_stop}</span></div>'
        )
    if _rec_warn:
        rec_rows.append(
            f'<div style="color:#f0c030;font-size:.73rem;margin-top:3px">{_rec_warn}</div>'
        )

    # ── 🎯 當前選擇的策略風格徽章（從 session_state 讀取）──
    style_info_local = None
    try:
        style_info_local = st.session_state.get('active_strategy')
    except Exception:
        pass

    style_badge_html = ""
    if style_info_local:
        # 🆕 v9.10n：偵測 ticker 與策略的市場匹配
        _style_mode = style_info_local.get('mode', '')
        _style_is_tw = ('VWAPEXEC' in _style_mode or 'IND' in _style_mode
                        or 'DXY' in _style_mode or 'WRSI' in _style_mode
                        or 'WADX' in _style_mode)
        _style_is_us_only = ('ADX18' in _style_mode and not _style_is_tw)
        _ticker_label = ('🇹🇼 TW' if not (_is_us or _is_crypto)
                         else ('🇺🇸 US' if _is_us else '🪙 Crypto'))

        # 不匹配警告
        warn_html = ""
        if (_is_us or _is_crypto) and _style_is_tw:
            warn_html = (
                f'<div style="margin-top:4px;color:#ff7755;font-size:.68rem">'
                f'⚠️ 此風格用 TW 跨市場過濾（IND/DXY/VWAPEXEC），'
                f'對 {_ticker_label} 個股無效或產生極少訊號。'
                f'建議改選 <b>⭐ US 最佳 (P10+POS+ADX18)</b></div>'
            )
        elif not (_is_us or _is_crypto) and _style_is_us_only:
            warn_html = (
                f'<div style="margin-top:4px;color:#ff7755;font-size:.68rem">'
                f'⚠️ 此風格針對 US 高流動股調校（ADX18），'
                f'對 🇹🇼 TW 個股可能不是最佳。'
                f'建議改選 <b>⭐ TW 最佳 (P5+VWAPEXEC)</b></div>'
            )

        style_badge_html = (
            f'<div style="background:{style_info_local["color"]}22;'
            f'border-left:3px solid {style_info_local["color"]};'
            f'border-radius:4px;padding:4px 10px;margin-top:6px;font-size:.7rem">'
            f'{style_info_local["icon"]} 當前策略風格：'
            f'<b style="color:{style_info_local["color"]}">{style_info_local["mode"]}</b>'
            f'　TEST 均報 +{style_info_local["mean"]:.1f}% ｜ TEST RR {style_info_local["sharpe"]:.3f}'
            f'{warn_html}</div>'
        )

    # ── ✨ 接近條件預警（即使尚未觸發 T1/T3/停損也提示）──
    proximity_alerts = _get_proximity_alerts(d)
    alert_html = ""
    if proximity_alerts:
        alert_lines = []
        for level, txt in proximity_alerts:
            bg = "#0d1f30" if level == 'info' else "#1a1505"
            border = "#2a4060" if level == 'info' else "#5a4a10"
            alert_lines.append(
                f'<div style="background:{bg};border:1px solid {border};'
                f'border-radius:4px;padding:4px 10px;margin-top:3px;font-size:.72rem">'
                f'{txt}</div>'
            )
        alert_html = (
            f'<div style="margin-top:10px;border-top:1px solid #1a2f48;padding-top:8px">'
            f'<div style="font-size:.7rem;color:#5a8ab0;margin-bottom:4px;letter-spacing:.05em">'
            f'🔔 接近條件預警（雖未觸發但已接近）</div>'
            f'{"".join(alert_lines)}'
            f'</div>'
        )

    # ── 組合 HTML ────────────────────────────────────────────
    label_tag = (
        f'<span style="background:{action_bg};color:{action_fg};'
        f'border:1px solid {action_fg}44;border-radius:4px;'
        f'padding:2px 9px;font-size:.72rem;font-weight:700;margin-left:8px">'
        f'{action_label}</span>'
    )

    sec_style = ("display:flex;gap:8px;align-items:flex-start;margin-bottom:6px")
    tag_style = ("background:#0a1e30;border-radius:4px;padding:1px 7px;"
                 "font-size:.68rem;font-weight:700;color:#5a9acf;"
                 "white-space:nowrap;margin-top:2px")
    val_style = "font-size:.78rem;line-height:1.8;color:#c8dff0"

    # 🆕 K 線型態（近 5 日）— 顯示於標題下、①市場環境上
    _klines_local = d.get("kline_patterns", []) if d else []
    _kline_inline = ""
    if _klines_local:
        _side_color = {'bull': '#3dbb6a', 'bear': '#ff5555', 'neutral': '#7a8899'}
        _chips = []
        for k in _klines_local:
            _color = _side_color.get(k['side'], '#7a8899')
            _day_label = ('今日' if k['days_ago'] == 0 else
                          '昨日' if k['days_ago'] == 1 else
                          f"{k['days_ago']} 日前")
            _chips.append(
                f'<span style="background:{_color}22;color:{_color};'
                f'border:1px solid {_color}66;border-radius:10px;'
                f'padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                f'white-space:nowrap;display:inline-block" '
                f'title="{k["note"]}">'
                f'{k["name_zh"]} <span style="opacity:0.7">· {_day_label}</span></span>'
            )
        _kline_inline = (
            f'<div style="display:flex;gap:6px;align-items:flex-start;'
            f'margin:0 0 8px;padding:6px 8px;background:#08131f;'
            f'border-radius:5px;border-left:2px solid #3a5a7a">'
            f'<span style="color:#7ab0d0;font-size:.66rem;font-weight:700;'
            f'white-space:nowrap;flex-shrink:0">📐 K 線</span>'
            f'<div style="line-height:1.7">{"".join(_chips)}</div>'
            f'</div>'
        )

    # 🆕 v9.12：BB 狀態 inline（依 OANDA BB 文章 8 種判斷）
    _bb_inline = ""
    _bb_pct_b = d.get('bb_pct_b')
    _bb_bw = d.get('bb_bandwidth')
    _bb_squeeze_pct = d.get('bb_squeeze_pct')
    _bb_sma = d.get('bb_sma')
    if _bb_pct_b is not None and close is not None:
        _bb_chips = []
        # %B 狀態
        if _bb_pct_b > 1.0:
            _bb_chips.append('<span style="background:#3a0a0a;color:#ff5555;border:1px solid #ff555566;'
                             'border-radius:10px;padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                             f'white-space:nowrap" title="收盤超出上軌（過熱）">%B {_bb_pct_b:.2f}>1 過熱</span>')
        elif _bb_pct_b < 0:
            _bb_chips.append('<span style="background:#0a2a18;color:#3dbb6a;border:1px solid #3dbb6a66;'
                             'border-radius:10px;padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                             f'white-space:nowrap" title="收盤跌破下軌（過冷反彈訊號 +3.32% 30d）">%B {_bb_pct_b:.2f}<0 過冷反彈 ★</span>')
        elif _bb_pct_b > 0.85:
            _bb_chips.append(f'<span style="background:#1a1408;color:#e8a020;border:1px solid #e8a02055;'
                             'border-radius:10px;padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                             f'white-space:nowrap">%B {_bb_pct_b:.2f} 偏熱</span>')
        elif _bb_pct_b < 0.15:
            _bb_chips.append(f'<span style="background:#0a1828;color:#7abadd;border:1px solid #7abadd55;'
                             'border-radius:10px;padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                             f'white-space:nowrap">%B {_bb_pct_b:.2f} 偏冷</span>')
        else:
            _bb_chips.append(f'<span style="background:#0a1828;color:#7a8899;border:1px solid #7a889944;'
                             'border-radius:10px;padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                             f'white-space:nowrap">%B {_bb_pct_b:.2f}</span>')

        # 距中軌
        if _bb_sma and _bb_sma > 0 and close:
            dist_mid = (close - _bb_sma) / _bb_sma * 100
            _color = '#3dbb6a' if dist_mid > 0 else '#ff5555'
            _bb_chips.append(f'<span style="color:{_color};font-size:.7rem;'
                             f'margin-right:8px">距中 {dist_mid:+.2f}%</span>')

        # Squeeze percentile
        if _bb_squeeze_pct is not None:
            if _bb_squeeze_pct <= 20:
                _bb_chips.append(f'<span style="background:#1a1408;color:#e8a020;border:1px solid #e8a02055;'
                                 'border-radius:10px;padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                                 f'white-space:nowrap" title="頻寬近 120 日最低 20%（大行情前兆）">'
                                 f'⚡ Squeeze (BW {_bb_squeeze_pct:.0f}%ile)</span>')
            elif _bb_squeeze_pct >= 80:
                _bb_chips.append(f'<span style="background:#0a1828;color:#7abadd;border:1px solid #7abadd55;'
                                 'border-radius:10px;padding:1px 7px;margin:2px 4px 2px 0;font-size:.66rem;'
                                 f'white-space:nowrap" title="頻寬近 120 日最高 20%（趨勢中）">'
                                 f'🌊 Expansion (BW {_bb_squeeze_pct:.0f}%ile)</span>')
            else:
                _bb_chips.append(f'<span style="color:#7a8899;font-size:.7rem;margin-right:8px">'
                                 f'BW {_bb_squeeze_pct:.0f}%ile</span>')

        # bandwidth 數值
        if _bb_bw is not None:
            _bb_chips.append(f'<span style="color:#5a7a99;font-size:.66rem;font-family:monospace">'
                             f'BW={_bb_bw:.1f}%</span>')

        if _bb_chips:
            _bb_inline = (
                f'<div style="display:flex;gap:6px;align-items:center;'
                f'margin:0 0 8px;padding:6px 8px;background:#0a1626;'
                f'border-radius:5px;border-left:2px solid #4a8cbf">'
                f'<span style="color:#7ab0d0;font-size:.66rem;font-weight:700;'
                f'white-space:nowrap;flex-shrink:0">📊 BB</span>'
                f'<div style="line-height:1.7">{"".join(_bb_chips)}</div>'
                f'</div>'
            )

    html = (
        f'<div style="background:#050e1a;border:1px solid #1a3050;border-radius:8px;'
        f'padding:10px 14px;margin-bottom:12px">'
        # ⓪ 特殊標的警告（若有）
        f'{special_banner}'
        # 標題
        f'<div style="font-size:.82rem;font-weight:700;color:#4a8cbf;margin-bottom:8px">'
        f'📊 ⑦ 自適應趨勢 操作建議{label_tag}</div>'
        # 🆕 v9.11：明確結論橫幅（狀態 + 原因 + 持倉者建議）
        f'<div style="background:{action_bg};border-left:4px solid {action_fg};'
        f'padding:10px 14px;margin:6px 0 10px;border-radius:4px">'
        f'<div style="color:{action_fg};font-weight:700;font-size:1.0rem;'
        f'margin-bottom:3px">{action_label}</div>'
        f'<div style="color:#c8dff0;font-size:.78rem;line-height:1.6">{action_reason}</div>'
        + (
            # 持倉者建議（在不建議進場時也提供）
            f'<div style="color:#ffd070;font-size:.72rem;margin-top:6px;'
            f'border-top:1px dashed #ff775544;padding-top:5px">'
            f'💼 <b>若已持倉</b>：建議減碼或設緊停損（離場價 {close - atr14*2.5:.2f}），'
            f'準備死叉出場'
            f'</div>'
            if (_entry_blocked_by_dc and atr14 and close) else
            f'<div style="color:#ffaaaa;font-size:.72rem;margin-top:6px;'
            f'border-top:1px dashed #ff444444;padding-top:5px">'
            f'💼 <b>若已持倉</b>：嚴守停損 + 看是否觸發 RSI&gt;70 出場條件'
            f'</div>'
            if (rsi is not None and rsi >= 75 and is_bull) else
            f'<div style="color:#a8c8d8;font-size:.72rem;margin-top:6px;'
            f'border-top:1px dashed #1a3050;padding-top:5px">'
            f'💼 <b>若已持倉</b>：繼續持有，留意 EMA 死叉與停損價'
            f'</div>'
            if is_bull else
            ''
        )
        + f'</div>'
        # 📐 K 線型態（標題下、①上）
        f'{_kline_inline}'
        # 📊 BB 狀態（K 線下、①上）— v9.12 OANDA BB 文章判斷
        f'{_bb_inline}'
        # ①
        f'<div style="{sec_style}">'
        f'<span style="{tag_style}">①市場環境</span>'
        f'<div style="{val_style}">'
        f'<span style="color:{env_color};font-weight:700">{env_icon} {env_tag}</span>'
        f'&nbsp;<span style="color:#8ab0c8">{env_desc}</span>'
        f'</div></div>'
        # ②
        f'<div style="{sec_style}">'
        f'<span style="{tag_style}">②進場判斷</span>'
        f'<div style="{val_style}">{"".join(entry_rows)}</div>'
        f'</div>'
        # 🌊 波段診斷（v9.15 OOS 驗證）— 只在 trigger 時顯示
        + (
            f'<div style="{sec_style}">'
            f'<span style="{tag_style};background:#0a2535;color:#5dccdd">🌊波段診斷</span>'
            f'<div style="{val_style}">{"".join(swing_rows)}</div>'
            f'</div>'
            if swing_rows else ''
        )
        +
        # ③ 出場停損
        f'<div style="{sec_style}">'
        f'<span style="{tag_style}">③出場停損</span>'
        f'<div style="{val_style}">{"".join(risk_rows)}</div>'
        f'</div>'
        # ④ 出場獲利（新）
        f'<div style="{sec_style}">'
        f'<span style="{tag_style}">④出場獲利</span>'
        f'<div style="{val_style}">'
        f'<span style="display:inline-block;padding:1px 8px;border-radius:4px;font-size:.7rem;font-weight:700;'
        f'background:{_exit_bg};color:{_exit_fg};border:1px solid {_exit_fg}44;margin-bottom:4px">'
        f'{_exit_label}</span>'
        f'{"".join(exit_rows)}</div>'
        f'</div>'
        # ⑤ 推薦策略
        f'<div style="border-top:1px solid #0f2035;margin-top:8px;padding-top:8px">'
        f'<div style="{sec_style.replace("margin-bottom:6px","")}">'
        f'<span style="{tag_style};background:#0f2040;color:#f0c030">⑤推薦策略</span>'
        f'<div style="{val_style}">{"".join(rec_rows)}</div>'
        f'</div></div>'
        # 🎯 策略風格徽章（從側邊欄選擇）
        f'{style_badge_html}'
        # 🌃 美股盤後預警 + 美股連動度（v9.10t）
        f'{us_alert_html}'
        # ⑥ 接近條件預警（即使未觸發也提示）
        f'{alert_html}'
        f'</div>'
    )
    return html

