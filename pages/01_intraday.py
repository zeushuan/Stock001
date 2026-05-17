"""Intraday Analysis Page — Stock001 v9.30
=============================================

每個 timeframe 一個 tab，內容**完全等同 tv_app 個股詳細卡**（用同一套
detail_card_render module 渲染），唯一差別是底層資料是該 TF 的 bar。

啟動：
  streamlit run tv_app.py
  → 側邊欄選 "intraday"
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from intraday.config import TIMEFRAMES, get_tf_config
from intraday.data import get_intraday, market_info
from intraday.builder import build_d_from_intraday
from detail_card_render import (
    GROUP_NAMES, GROUP_WEIGHTS, GROUP_COLORS,
    TREND_W, POSITION_W, MOMENTUM_W, AUX_W,
    judge_trend, judge_position, judge_momentum, judge_aux,
    calc_summary, _calc_aux_summary, compute_momentum_grade,
    _rec, apply_cap, badge, get_rec_label, render_detail,
)
# v9.31：操作建議 + 所有 banner（SEPA/VCP/Stage/Cup/Flat/雙底/綜合決策）
from operation_advice import get_operation_advice


st.set_page_config(page_title="Intraday 個股詳細 | Stock001",
                    page_icon="⏱️", layout="wide")


# 共享 session state
if 'selected_ticker_intraday' not in st.session_state:
    inherited = st.session_state.get('current_ticker') or 'AAPL'
    st.session_state['selected_ticker_intraday'] = inherited

# 🆕 v9.32：偵測 Streamlit theme，預設用 Auto（跟隨系統）
if 'intraday_theme_mode' not in st.session_state:
    # 試 Streamlit 1.31+ 的 st.context.theme
    try:
        _detected = getattr(st.context.theme, 'type', None)
        st.session_state['intraday_theme_mode'] = _detected or 'auto'
    except Exception:
        st.session_state['intraday_theme_mode'] = 'auto'


def _build_dark_css() -> str:
    """Dark mode（原本就有的）— class 樣式定義"""
    return """
.ind-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(180px, 1fr)); gap:6px; margin-bottom:8px; }
.ind-item { background:#0a1628; border:1px solid #1a2f48; border-radius:6px; padding:7px 10px; }
.ind-item.ind-buy   { border-color:#1a4a80; background:#08152a; }
.ind-item.ind-sell  { border-color:#6a1a1a; background:#1a0808; }
.ind-item.ind-neu   { border-color:#1a2f48; }
.ind-label { display:block; font-size:.68rem; color:#7ab0d0; margin-bottom:3px; }
.ind-val   { font-size:.9rem; color:#e8f4fd; font-family:'IBM Plex Mono','SF Mono',Consolas,monospace; font-weight:600; }
.badge { display:inline-block; padding:2px 8px; border-radius:4px; font-size:.7rem; font-weight:700; margin:0 4px; }
.badge-strong-buy { background:#0D47A1; color:#60CFFF; }
.badge-buy        { background:#0D2E50; color:#60B3FF; }
.badge-buy-limit  { background:#3A2A00; color:#F0C030; }
.badge-strong-sell{ background:#4A0A0A; color:#FF6B6B; }
.badge-sell       { background:#3B0D0D; color:#FF8080; }
.badge-overheat   { background:#3A1800; color:#FF8830; }
.badge-bearish    { background:#3A0808; color:#FF5555; }
.badge-neutral    { background:#1A2030; color:#9AAABB; }
"""


def _build_light_css() -> str:
    """Light mode — 反過來 + 用 attribute selector + !important 蓋過 inline 深色"""
    return """
/* —— class-based 樣式（淺色版）—— */
.ind-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(180px, 1fr)); gap:6px; margin-bottom:8px; }
.ind-item { background:#f5f7fa !important; border:1px solid #d0d7e0 !important; border-radius:6px; padding:7px 10px; }
.ind-item.ind-buy   { border-color:#8eb5e8 !important; background:#e8f1ff !important; }
.ind-item.ind-sell  { border-color:#e89090 !important; background:#ffeaea !important; }
.ind-item.ind-neu   { border-color:#d0d7e0 !important; }
.ind-label { display:block; font-size:.68rem; color:#4a6c88 !important; margin-bottom:3px; }
.ind-val   { font-size:.9rem; color:#1a2a40 !important; font-family:'IBM Plex Mono','SF Mono',Consolas,monospace; font-weight:600; }

.badge { display:inline-block; padding:2px 8px; border-radius:4px; font-size:.7rem; font-weight:700; margin:0 4px; }
.badge-strong-buy { background:#1565C0 !important; color:#ffffff !important; }
.badge-buy        { background:#1976D2 !important; color:#ffffff !important; }
.badge-buy-limit  { background:#F0A030 !important; color:#ffffff !important; }
.badge-strong-sell{ background:#B71C1C !important; color:#ffffff !important; }
.badge-sell       { background:#D32F2F !important; color:#ffffff !important; }
.badge-overheat   { background:#E65100 !important; color:#ffffff !important; }
.badge-bearish    { background:#C62828 !important; color:#ffffff !important; }
.badge-neutral    { background:#9AAABB !important; color:#ffffff !important; }

/* —— inline-style overrides（蓋過 detail_card_render / operation_advice 寫死的深色）—— */
/* 常見深色 panel 背景 → 淺色 */
[style*="background:#0a1628"], [style*="background:#08131f"], [style*="background:#0a1422"],
[style*="background:#08152a"], [style*="background:#0a1828"], [style*="background:#0a1020"],
[style*="background:#050e1a"], [style*="background:#0f1f33"], [style*="background:#0a1e30"],
[style*="background:#0f2535"]
  { background:#f5f7fa !important; }

[style*="background:#1a0808"], [style*="background:#3a0a0a"], [style*="background:#2a0a0a"],
[style*="background:#1a0010"], [style*="background:#3B0D0D"], [style*="background:#3a0808"],
[style*="background:#4A0A0A"]
  { background:#ffeaea !important; }

[style*="background:#0a2a14"], [style*="background:#0d2a10"], [style*="background:#0a1a0a"]
  { background:#e6f7ec !important; }

[style*="background:#1a1408"], [style*="background:#1a1805"], [style*="background:#2a1605"],
[style*="background:#1a1200"], [style*="background:#1a1400"], [style*="background:#3A2A00"],
[style*="background:#2a1500"], [style*="background:#3A1800"]
  { background:#fff4d6 !important; }

[style*="background:#1a0808"], [style*="background:#2a0008"], [style*="background:#1a0a08"]
  { background:#ffeaea !important; }

/* 深色文字 → 深色字（淺底配深字）*/
[style*="color:#e8f4fd"], [style*="color:#c8dff0"], [style*="color:#a8c0d0"]
  { color:#1a2a40 !important; }

[style*="color:#7ab0d0"], [style*="color:#7abadd"], [style*="color:#5dccdd"],
[style*="color:#7a9ab0"], [style*="color:#8ab0c8"], [style*="color:#7a8899"],
[style*="color:#5a8ab0"], [style*="color:#9aaabb"], [style*="color:#3a5a7a"],
[style*="color:#5a7a9a"]
  { color:#4a6c88 !important; }

/* 強調色（買/賣/中性）— 維持原色但加深一點 */
[style*="color:#3b9eff"] { color:#1976d2 !important; }
[style*="color:#60B3FF"] { color:#1565C0 !important; }
[style*="color:#60CFFF"] { color:#0D47A1 !important; }
[style*="color:#ff5555"] { color:#c62828 !important; }
[style*="color:#FF6B6B"], [style*="color:#FF8080"], [style*="color:#FF7755"] { color:#b71c1c !important; }
[style*="color:#3dbb6a"], [style*="color:#88c8a8"], [style*="color:#7acc7a"],
[style*="color:#40c070"] { color:#2e7d32 !important; }
[style*="color:#e8a020"], [style*="color:#ffaa55"], [style*="color:#e8c030"],
[style*="color:#c8b87a"] { color:#ed6c02 !important; }
[style*="color:#aa66ff"], [style*="color:#a866ff"], [style*="color:#a060ff"],
[style*="color:#b266ff"] { color:#5e35b1 !important; }

/* 邊框 / 分隔線 */
[style*="border-top:1px solid #0f1f33"], [style*="border-top:1px solid #1a2f48"]
  { border-top-color:#d0d7e0 !important; }
[style*="border:1px solid #1a2f48"], [style*="border:1px solid #1a3055"],
[style*="border:1px solid #2a3f5f"]
  { border-color:#d0d7e0 !important; }
"""


# ── 主題 toggle ──
_t1, _t2 = st.columns([5, 1])
with _t2:
    _theme_choice = st.radio(
        '🌓 主題',
        options=['Auto', 'Dark', 'Light'],
        index={'auto':0,'dark':1,'light':2}.get(
            st.session_state['intraday_theme_mode'], 0),
        horizontal=True,
        key='_theme_radio',
    )
    st.session_state['intraday_theme_mode'] = _theme_choice.lower()


# ── 注入 CSS（Auto = 用 prefers-color-scheme media query）──
_theme = st.session_state['intraday_theme_mode']
if _theme == 'dark':
    st.markdown(f'<style>{_build_dark_css()}</style>', unsafe_allow_html=True)
elif _theme == 'light':
    st.markdown(
        f'<style>{_build_dark_css()}\n{_build_light_css()}</style>',
        unsafe_allow_html=True)
else:   # auto — system 偏好決定（光標暗黑就暗黑）
    st.markdown(f"""
<style>
{_build_dark_css()}

@media (prefers-color-scheme: light) {{
{_build_light_css()}
}}
</style>
""", unsafe_allow_html=True)


# ── 頂部標題 + 控制 ──
st.title("⏱️ Intraday 個股詳細指標（多時間框架）")
st.caption("與 tv_app 主頁同樣的 4 群指標 + 操作建議邏輯，唯一差別是底層 bar 為該 TF（1m/5m/15m/30m/1h/1d）")

c1, c2, c3 = st.columns([3, 2, 1])
with c1:
    ticker_input = st.text_input(
        "股票代號",
        value=st.session_state['selected_ticker_intraday'],
        help="US: AAPL / TW: 2330（不需 .TW）",
    )
    st.session_state['selected_ticker_intraday'] = ticker_input.strip().upper()
ticker = st.session_state['selected_ticker_intraday']

with c2:
    timeframes_selected = st.multiselect(
        "Timeframes",
        options=['1m', '5m', '15m', '30m', '1h', '1d'],
        default=['5m', '15m', '1h', '1d'],
    )

with c3:
    if st.button("🔄 重抓所有 TF"):
        for tf in timeframes_selected:
            get_intraday(ticker, tf, refresh=True)
        st.success("已重抓")
        st.rerun()

if not timeframes_selected:
    st.warning("至少選一個 timeframe")
    st.stop()


# ── 市場 metadata ──
info = market_info(ticker)
st.markdown(
    f"**{info['ticker']}** · {('🇺🇸 US' if info['market']=='us' else '🇹🇼 TW')}"
    f" · session: `{info['session_hours']}` · yf_symbol: `{info['yf_symbol']}`"
)

st.divider()


# ── 每個 TF 一個 tab ──
tabs = st.tabs([f"⏱️ {tf}" for tf in timeframes_selected])

for tab, tf in zip(tabs, timeframes_selected):
    with tab:
        with st.spinner(f"計算 {ticker} @ {tf}..."):
            df = get_intraday(ticker, tf, market=info['market'])
            if df is None or len(df) < 30:
                st.error(f"⚠️ {tf}: 資料不足或抓取失敗（bars={len(df) if df is not None else 0}）")
                continue

            d = build_d_from_intraday(df, tf=tf, ticker=ticker, market=info['market'])

            if d.get('_error'):
                st.error(f"⚠️ {tf}: {d['_error']}")
                continue

            # 顯示 TF metadata
            cfg = get_tf_config(tf)
            st.caption(
                f"📊 **{tf}** ｜ {len(df)} bars ｜ "
                f"每根 {cfg.minutes_per_bar} 分鐘 ｜ "
                f"last bar: `{d.get('_intraday_last_ts', '?')}` "
                f"｜ 週線 resample: {len(df)} bars / {cfg.bars_per_day*5:.0f} bar/week"
            )

            # 警告短期 TF 不適用某些指標
            warnings_list = []
            if not cfg.supports_stage:
                warnings_list.append("Stage 分析 (30W SMA) 對 " + tf + " 無意義 → 顯示但不要過度解讀")
            if not cfg.supports_sepa:
                warnings_list.append("SEPA Template (52w 高低) 對 " + tf + " 無意義")
            if d.get('w_close') is None:
                warnings_list.append("週線資料不足（< 20 週）→ 週線結構欄位會顯示 N/A")
            if warnings_list:
                st.warning("⚠️ TF 適用性提示：" + " ｜ ".join(warnings_list))

        # 跑完整 tv_app 詳細卡渲染流程
        try:
            gt = judge_trend(d)
            gp = judge_position(d)
            gm = judge_momentum(d)
            ga = judge_aux(d)
            ts = calc_summary(gt, TREND_W)
            ps = calc_summary(gp, POSITION_W)
            ms_b, ms_s, ms_n, _ = calc_summary(gm, MOMENTUM_W)
            mg = compute_momentum_grade(d)
            ms = (ms_b, ms_s, ms_n, mg)
            xs = _calc_aux_summary(ga, AUX_W)
            tb = round(ts[0] + ps[0] + ms[0] + xs[0], 1)
            ts_ = round(ts[1] + ps[1] + ms[1] + xs[1], 1)
            tn_ = round(ts[2] + ps[2] + ms[2] + xs[2], 1)
            verdict_raw = _rec(tb, ts_)
            verdict, cap = apply_cap(verdict_raw, d, mg)
            groups = (gt, gp, gm, ga)
            summs = (ts, ps, ms, xs)
            tsumm = (tb, ts_, tn_, verdict)

            # ── ④ 推薦策略 label（tv_app 表格欄那個小 badge）──
            rec_label, rec_style = get_rec_label(d, ticker=ticker)
            st.markdown(
                f'<div style="display:inline-block;{rec_style};'
                f'padding:5px 12px;border-radius:5px;font-size:.85rem;'
                f'font-weight:700;margin-bottom:8px">'
                f'④ 推薦策略：{rec_label}'
                f'</div>',
                unsafe_allow_html=True)

            # ── 整體 verdict badge ──
            cap_html = (
                f'<span style="color:#aa6655;font-size:.72rem;margin-left:6px">{cap}</span>'
                if cap else ''
            )
            st.markdown(
                f'<div style="margin:6px 0;font-size:.95rem">'
                f'<span style="color:#7ab0d0">{tf} 綜合判讀：</span>'
                f'{badge(verdict)}'
                f'<span style="color:#7a8899;font-size:.7rem;margin-left:8px">'
                f'(買 {tb} ｜ 賣 {ts_} ｜ 中 {tn_})</span>'
                f'{cap_html}'
                f'</div>',
                unsafe_allow_html=True)

            # ── 完整 detail card（含 SEPA/VCP/Stage/Cup/Flat/雙底/綜合決策所有 banner）──
            # 🆕 v9.31：傳入 get_operation_advice 讓 banner 也出現
            html = render_detail(
                ticker, d, groups, summs, tsumm, cap,
                market=info['market'],
                advice_fn=get_operation_advice,   # 完整操作建議 + 所有 banner
                news_fn=None,                       # 新聞不分 TF（跨 TF 一樣）
                concepts_fn=None,                   # 概念股不分 TF（跨 TF 一樣）
            )
            st.markdown(html, unsafe_allow_html=True)

        except Exception as e:
            import traceback
            st.error(f"渲染失敗：{type(e).__name__}: {e}")
            st.code(traceback.format_exc())


st.divider()
st.caption(
    f"Stock001 v9.30 ｜ {ticker} ｜ TFs: {', '.join(timeframes_selected)} ｜ "
    f"使用 detail_card_render module（與 tv_app 主頁 100% 相同邏輯）"
)
