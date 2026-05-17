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
from intraday.charts import build_zigzag_compare_chart, build_zigzag_chart_plotly
from intraday.settings import get_zigzag_atr_mult, set_zigzag_atr_mult
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
    st.session_state['intraday_theme_mode'] = 'auto'


# 🆕 v9.32：HTML 端硬性替換深色 hex（CSS attribute selector 對 inline style 不可靠）
# 把 detail_card_render / operation_advice 產生的 HTML 內的深色背景/文字 hex
# 替換成淺色版，這比 CSS [style*=] selector 更可靠
_HEX_DARK_TO_LIGHT = {
    # ─ 深藍底（17 個）→ 淺藍 #f5f7fa
    '#050e1a':'#f5f7fa','#08131f':'#f5f7fa','#0a1020':'#f5f7fa',
    '#0a1422':'#f5f7fa','#0a1626':'#f5f7fa','#0a1628':'#f5f7fa',
    '#0a1825':'#f5f7fa','#0a1828':'#f5f7fa','#0a1830':'#f5f7fa',
    '#0a1a2a':'#f5f7fa','#0a1e30':'#eef3f8','#0a2535':'#eef3f8',
    '#0d1825':'#f5f7fa','#0f1f33':'#eef3f8','#0f2040':'#eef3f8',
    '#0f2535':'#eef3f8','#08152a':'#e8f1ff',
    # ─ 深紅底（賣警告）→ 淺紅 #ffeaea
    '#1a0010':'#ffeaea','#1a0808':'#ffeaea','#1a0a00':'#ffeaea',
    '#1a0a08':'#ffeaea','#1a1410':'#ffeaea','#2a0a0a':'#ffd5d5',
    '#2a0008':'#ffd5d5','#3a0a0a':'#ffc8c8','#3a0808':'#ffc8c8',
    '#3B0D0D':'#ffc8c8','#4A0A0A':'#ffbaba','#3b0d0d':'#ffc8c8',
    '#4a0a0a':'#ffbaba',
    '#1a0505':'#ffeaea','#2a0808':'#ffd5d5','#1a1505':'#ffeaea',  # 🆕 v9.32 補漏
    # ─ 深綠底（OK / 進場 OK）→ 淺綠 #e6f7ec
    '#0a1a0a':'#e6f7ec','#0a2014':'#d6f0df','#0a2018':'#d6f0df',
    '#0a2a14':'#d6f0df','#0a2a18':'#d6f0df','#0d1f0d':'#e6f7ec',
    '#0d2a10':'#d6f0df',
    '#0a1e10':'#e6f7ec','#0a2810':'#d6f0df','#0a2a10':'#d6f0df',  # 🆕 v9.32 補漏
    # ─ 深黃橘底（觀望警告）→ 淺黃 #fff4d6
    '#1a1200':'#fff4d6','#1a1208':'#fff4d6','#1a1400':'#fff4d6',
    '#1a1408':'#fff4d6','#1a1500':'#fff4d6','#1a1605':'#fff4d6',
    '#1a1805':'#fff4d6','#2a1500':'#ffe9b3','#2a1605':'#ffe9b3',
    '#3A2A00':'#ffd97a','#3A1800':'#ffd97a','#3a2a00':'#ffd97a',
    '#3a1800':'#ffd97a',
    '#1a1a05':'#fff4d6',  # 🆕 v9.32 補漏（ADX 不足，觀望）
    # ─ 補漏的深藍 / 深紫
    '#0d1828':'#f5f7fa','#1a0828':'#ecedf8',
    # ─ 文字色：淺色字（深底用）→ 深字
    '#e8f4fd':'#1a2a40','#c8dff0':'#1a2a40','#a8c0d0':'#1a2a40',
    '#c8e0d0':'#1a2a40','#a8cce8':'#1a2a40',
    # 中間色字
    '#7ab0d0':'#4a6c88','#7abadd':'#4a6c88','#5dccdd':'#4a6c88',
    '#7a9ab0':'#5a7090','#8ab0c8':'#5a7090','#7a8899':'#5a7090',
    '#5a8ab0':'#5a7090','#9aaabb':'#5a7090','#3a5a7a':'#5a7090',
    '#5a7a9a':'#5a7090','#9abacf':'#5a7090','#9fcc9f':'#5a7090',
    '#90d0a0':'#5a7090','#a87acc':'#5a7090','#c294d6':'#5a7090',
    # 強調色 — 加深適配淺底
    # 注意：badge inline 用的色我們不動（強烈標示意義），但放在淺底也要可讀
    # ─ 邊框（深 → 淺灰）
    '#1a2f48':'#d0d7e0','#1a3055':'#d0d7e0','#2a3f5f':'#d0d7e0',
    '#1a4030':'#bfe0c9','#1a6030':'#bfe0c9','#2a4060':'#d0d7e0',
    '#5a4a10':'#e0d090','#1a4a80':'#a8c5e8','#6a1a1a':'#e0a8a8',
    '#1a3050':'#d0d7e0',
}


def _convert_html_for_light_mode(html: str) -> str:
    """直接替換 HTML 字串內的深色 hex（最可靠的 light mode 實現）"""
    out = html
    for dark, light in _HEX_DARK_TO_LIGHT.items():
        out = out.replace(dark, light)
        # 大小寫變體
        if dark.lower() != dark:
            out = out.replace(dark.lower(), light)
        if dark.upper() != dark:
            out = out.replace(dark.upper(), light)
    return out


def _detect_streamlit_theme() -> str:
    """多重 fallback 偵測 Streamlit 實際顯示 theme（不是 OS preference）
    return 'light' or 'dark'
    """
    # 1. Streamlit 1.31+ context.theme.type
    try:
        t = getattr(st.context.theme, 'type', None)
        if t in ('light', 'dark'):
            return t
    except Exception:
        pass
    # 2. config.toml 的 theme.base
    try:
        base = st.get_option('theme.base')
        if base in ('light', 'dark'):
            return base
    except Exception:
        pass
    # 3. 用 JavaScript-driven CSS class 偵測（Streamlit 在 body 有 theme indicator）
    # fallback：light（多數情境）
    return 'light'


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
    """Light mode — class 樣式 + inline 深色 attribute selector + !important
    完整 cover 全部 36 個 inline background hex（v9.32 統一）"""
    # 4 大色組：深藍底（最常見） / 深紅（賣警告） / 深綠（買訊號 / OK） / 深黃橘（觀望警告）
    blue_dark  = ['#050e1a','#08131f','#0a1020','#0a1422','#0a1626','#0a1628','#0a1825',
                   '#0a1828','#0a1830','#0a1a2a','#0a1e30','#0a2535','#0d1825','#0f1f33',
                   '#0f2040','#0f2535','#08152a']
    red_dark   = ['#1a0010','#1a0808','#1a0a00','#1a0a08','#1a1410','#2a0a0a','#2a0008',
                   '#3a0a0a','#3a0808','#3B0D0D','#4A0A0A']
    green_dark = ['#0a1a0a','#0a2014','#0a2018','#0a2a14','#0a2a18','#0d1f0d','#0d2a10']
    amber_dark = ['#1a1200','#1a1208','#1a1400','#1a1408','#1a1500','#1a1605','#1a1805',
                   '#2a1500','#2a1605','#3A2A00','#3A1800']

    def _sel(hexes):
        # 同時 cover「background:#XXX」「background: #XXX」「background-color:#XXX」三種寫法
        out = []
        for h in hexes:
            hl = h.lower()
            out.append(f'[style*="background:{hl}"]')
            out.append(f'[style*="background: {hl}"]')
            out.append(f'[style*="background-color:{hl}"]')
            out.append(f'[style*="background-color: {hl}"]')
            # 大寫版本
            hu = h.upper()
            if hu != hl:
                out.append(f'[style*="background:{hu}"]')
                out.append(f'[style*="background: {hu}"]')
        return ',\n'.join(out)

    return f"""
/* —— class-based 樣式（淺色版）—— */
.ind-grid {{ display:grid; grid-template-columns:repeat(auto-fill, minmax(180px, 1fr)); gap:6px; margin-bottom:8px; }}
.ind-item {{ background:#f5f7fa !important; border:1px solid #d0d7e0 !important; border-radius:6px; padding:7px 10px; }}
.ind-item.ind-buy   {{ border-color:#8eb5e8 !important; background:#e8f1ff !important; }}
.ind-item.ind-sell  {{ border-color:#e89090 !important; background:#ffeaea !important; }}
.ind-item.ind-neu   {{ border-color:#d0d7e0 !important; }}
.ind-label {{ display:block; font-size:.68rem; color:#4a6c88 !important; margin-bottom:3px; }}
.ind-val   {{ font-size:.9rem; color:#1a2a40 !important; font-family:'IBM Plex Mono','SF Mono',Consolas,monospace; font-weight:600; }}

.badge {{ display:inline-block; padding:2px 8px; border-radius:4px; font-size:.7rem; font-weight:700; margin:0 4px; }}
.badge-strong-buy {{ background:#1565C0 !important; color:#ffffff !important; }}
.badge-buy        {{ background:#1976D2 !important; color:#ffffff !important; }}
.badge-buy-limit  {{ background:#F0A030 !important; color:#ffffff !important; }}
.badge-strong-sell{{ background:#B71C1C !important; color:#ffffff !important; }}
.badge-sell       {{ background:#D32F2F !important; color:#ffffff !important; }}
.badge-overheat   {{ background:#E65100 !important; color:#ffffff !important; }}
.badge-bearish    {{ background:#C62828 !important; color:#ffffff !important; }}
.badge-neutral    {{ background:#9AAABB !important; color:#ffffff !important; }}

/* ——— inline-style overrides ——— */
/* 深藍系 panel 背景 → 淺底 */
{_sel(blue_dark)}
  {{ background:#f5f7fa !important; background-color:#f5f7fa !important; color:#1a2a40 !important; }}

/* 深紅系（賣警告）→ 淺紅底 */
{_sel(red_dark)}
  {{ background:#ffeaea !important; background-color:#ffeaea !important; color:#b71c1c !important; }}

/* 深綠系（OK / 進場 OK）→ 淺綠底 */
{_sel(green_dark)}
  {{ background:#e6f7ec !important; background-color:#e6f7ec !important; color:#1b5e20 !important; }}

/* 深黃橘系（警告 / 等待）→ 淺黃底 */
{_sel(amber_dark)}
  {{ background:#fff4d6 !important; background-color:#fff4d6 !important; color:#7a4a00 !important; }}

/* ——— 文字色 override ——— */
/* 淺色文字（深底時用）→ 換深色 */
[style*="color:#e8f4fd"], [style*="color:#c8dff0"], [style*="color:#a8c0d0"],
[style*="color:#c8e0d0"], [style*="color:#a8cce8"], [style*="color:#ffe"], [style*="color:#ffc"],
[style*="color:#ffd"], [style*="color:#fff"]
  {{ color:#1a2a40 !important; }}

[style*="color:#7ab0d0"], [style*="color:#7abadd"], [style*="color:#5dccdd"],
[style*="color:#7a9ab0"], [style*="color:#8ab0c8"], [style*="color:#7a8899"],
[style*="color:#5a8ab0"], [style*="color:#9aaabb"], [style*="color:#3a5a7a"],
[style*="color:#5a7a9a"], [style*="color:#9abacf"], [style*="color:#9fcc9f"],
[style*="color:#90d0a0"], [style*="color:#a87acc"], [style*="color:#c294d6"],
[style*="color:#4a6070"], [style*="color:#334455"], [style*="color:#7a8899"]
  {{ color:#5a7090 !important; }}

/* 強調色（買/賣/中性/警告）─ 加深一點適配淺底 */
[style*="color:#3b9eff"], [style*="color:#60B3FF"], [style*="color:#60CFFF"],
[style*="color:#5a9acf"]
  {{ color:#1565C0 !important; }}
[style*="color:#ff5555"], [style*="color:#FF6B6B"], [style*="color:#FF8080"],
[style*="color:#FF7755"], [style*="color:#ff7755"], [style*="color:#ff7a7a"],
[style*="color:#ff8888"], [style*="color:#ff9944"], [style*="color:#ff5555"]
  {{ color:#c62828 !important; }}
[style*="color:#3dbb6a"], [style*="color:#88c8a8"], [style*="color:#7acc7a"],
[style*="color:#40c070"], [style*="color:#66ff99"], [style*="color:#a8d4a8"]
  {{ color:#2e7d32 !important; }}
[style*="color:#e8a020"], [style*="color:#ffaa55"], [style*="color:#e8c030"],
[style*="color:#c8b87a"], [style*="color:#ddc080"], [style*="color:#f0c030"],
[style*="color:#ffcc55"], [style*="color:#e8c050"]
  {{ color:#b26500 !important; }}
[style*="color:#aa66ff"], [style*="color:#a866ff"], [style*="color:#a060ff"],
[style*="color:#b266ff"]
  {{ color:#5e35b1 !important; }}

/* 邊框淺色化 */
[style*="border:1px solid #1a2f48"], [style*="border:1px solid #1a3055"],
[style*="border:1px solid #2a3f5f"], [style*="border:1px solid #1a4030"],
[style*="border:1px solid #1a6030"], [style*="border:1px solid #2a4060"],
[style*="border:1px solid #5a4a10"], [style*="border:1px solid #1a4a80"],
[style*="border:1px solid #6a1a1a"]
  {{ border-color:#d0d7e0 !important; }}
[style*="border-top:1px solid #0f1f33"], [style*="border-top:1px solid #1a2f48"],
[style*="border-top:1px solid #2a3f5f"]
  {{ border-top-color:#d0d7e0 !important; }}
[style*="border-bottom:1px solid #0f1f33"]
  {{ border-bottom-color:#d0d7e0 !important; }}
[style*="border-left:4px solid #888"], [style*="border-left:4px solid #5dccdd"]
  {{ border-left-color:#9aaabb !important; }}
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
        help='Auto = 跟隨 Streamlit 設定（不靠 OS）；強制 Light/Dark 用 toggle',
    )
    st.session_state['intraday_theme_mode'] = _theme_choice.lower()


# ── 注入 CSS ──
_theme = st.session_state['intraday_theme_mode']
if _theme == 'auto':
    # Auto = 偵測 Streamlit 實際 theme（不靠 OS prefers-color-scheme）
    _theme = _detect_streamlit_theme()
    st.caption(f'🌓 Auto 偵測到 Streamlit theme: **{_theme}**')

if _theme == 'dark':
    st.markdown(f'<style>{_build_dark_css()}</style>', unsafe_allow_html=True)
else:  # light
    st.markdown(
        f'<style>{_build_dark_css()}\n{_build_light_css()}</style>',
        unsafe_allow_html=True)


# ── 頂部標題 + 控制 ──
st.title("⏱️ Intraday 個股詳細指標（多時間框架）")
st.caption("與 tv_app 主頁同樣的 4 群指標 + 操作建議邏輯，唯一差別是底層 bar 為該 TF（1m/5m/15m/30m/1h/1d）")


# 🆕 v9.32：自選股 + preset 載入
@st.cache_data(ttl=60, show_spinner=False)
def _load_all_watchlists() -> dict:
    """合併 watchlists_user.json (本地) + watchlists_presets.json (預設)
    + Streamlit localStorage（雲端唯一持久化）
    回傳 {分類名: [ticker1, ticker2, ...]}
    """
    import json
    from pathlib import Path
    base = Path(__file__).parent.parent
    out = {}

    # ① watchlists_user.json — 用戶清單（newline 分隔字串）
    p_user = base / 'watchlists_user.json'
    if p_user.exists():
        try:
            d = json.loads(p_user.read_text(encoding='utf-8'))
            for name, val in d.items():
                if name.startswith('_'):
                    continue
                if isinstance(val, str):
                    tks = [t.strip().upper() for t in val.split('\n') if t.strip()]
                elif isinstance(val, list):
                    tks = [str(t).strip().upper() for t in val if str(t).strip()]
                else:
                    continue
                if tks:
                    out[f'👤 {name}'] = tks
        except Exception:
            pass

    # ② watchlists_presets.json — 預設清單
    p_pre = base / 'watchlists_presets.json'
    if p_pre.exists():
        try:
            d = json.loads(p_pre.read_text(encoding='utf-8'))
            presets = d.get('presets', {})
            for name, info in presets.items():
                tks = info.get('tickers', [])
                if tks:
                    out[name] = [str(t).strip().upper() for t in tks]
        except Exception:
            pass

    # ③ Streamlit localStorage（雲端模式）
    try:
        from streamlit_local_storage import LocalStorage
        _ls = LocalStorage()
        v = _ls.getItem("stock001_watchlists")
        if v:
            if isinstance(v, str):
                d = json.loads(v)
            elif isinstance(v, dict):
                d = v
            else:
                d = {}
            for name, val in d.items():
                if name.startswith('_'):
                    continue
                if isinstance(val, str):
                    tks = [t.strip().upper() for t in val.split('\n') if t.strip()]
                elif isinstance(val, list):
                    tks = [str(t).strip().upper() for t in val if str(t).strip()]
                else:
                    continue
                if tks and f'👤 {name}' not in out:   # 不覆寫本地版
                    out[f'👤 {name}'] = tks
    except Exception:
        pass

    return out


_all_watchlists = _load_all_watchlists()
_watchlist_names = list(_all_watchlists.keys())

# 控制列：自選股 → ticker → TF → 重抓
c1, c2, c3, c4 = st.columns([2, 2, 2, 1])

with c1:
    _wl_options = ['（手動輸入）'] + _watchlist_names
    _wl_choice = st.selectbox(
        "📋 自選股清單",
        options=_wl_options,
        index=0,
        key='_wl_picker',
        help='挑一個 watchlist 從裡面選股；或選「手動輸入」自己打 ticker',
    )

with c2:
    if _wl_choice and _wl_choice != '（手動輸入）':
        _wl_tickers = _all_watchlists.get(_wl_choice, [])
        # 預設值：若 session_state 的 ticker 已在這 watchlist 內就保留，否則取第一個
        _cur_tk = st.session_state.get('selected_ticker_intraday', '').upper()
        _default_idx = (_wl_tickers.index(_cur_tk)
                         if _cur_tk in _wl_tickers else 0)
        _picked = st.selectbox(
            f"選股（{len(_wl_tickers)} 檔）",
            options=_wl_tickers,
            index=_default_idx,
            key=f'_ticker_from_wl_{_wl_choice}',
        )
        ticker = _picked.strip().upper()
        st.session_state['selected_ticker_intraday'] = ticker
    else:
        ticker_input = st.text_input(
            "股票代號",
            value=st.session_state.get('selected_ticker_intraday', 'AAPL'),
            key='_manual_ticker',
            help="US: AAPL / TW: 2330（不需 .TW）",
        )
        ticker = ticker_input.strip().upper()
        st.session_state['selected_ticker_intraday'] = ticker

with c3:
    timeframes_selected = st.multiselect(
        "Timeframes",
        options=['1m', '5m', '15m', '30m', '1h', '1d'],
        default=['1m', '5m', '15m', '30m', '1h', '1d'],
    )

with c4:
    if st.button("🔄 重抓", help='強制重抓所有 TF 的資料'):
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


# 🆕 v9.32：多時間框架對齊表（pre-compute 所有 TF 的判讀彙總）
# ──────────────────────────────────────────────────────────────
tf_summaries = {}    # {tf: {df, d, groups, summs, tsumm, cap, rec_label, rec_style}}
with st.spinner(f"計算 {ticker} 跨 {len(timeframes_selected)} 個 timeframe..."):
    for _tf in timeframes_selected:
        try:
            _df = get_intraday(ticker, _tf, market=info['market'])
            if _df is None or len(_df) < 30:
                tf_summaries[_tf] = {'error': f'資料不足 ({len(_df) if _df is not None else 0} bars)'}
                continue
            _d = build_d_from_intraday(_df, tf=_tf, ticker=ticker, market=info['market'])
            if _d.get('_error'):
                tf_summaries[_tf] = {'error': _d['_error']}
                continue
            _gt = judge_trend(_d); _gp = judge_position(_d)
            _gm = judge_momentum(_d); _ga = judge_aux(_d)
            _ts = calc_summary(_gt, TREND_W)
            _ps = calc_summary(_gp, POSITION_W)
            _ms_b, _ms_s, _ms_n, _ = calc_summary(_gm, MOMENTUM_W)
            _mg = compute_momentum_grade(_d)
            _ms = (_ms_b, _ms_s, _ms_n, _mg)
            _xs = _calc_aux_summary(_ga, AUX_W)
            _tb = round(_ts[0] + _ps[0] + _ms[0] + _xs[0], 1)
            _ts_ = round(_ts[1] + _ps[1] + _ms[1] + _xs[1], 1)
            _tn_ = round(_ts[2] + _ps[2] + _ms[2] + _xs[2], 1)
            _v_raw = _rec(_tb, _ts_)
            _verdict, _cap = apply_cap(_v_raw, _d, _mg)
            _rec_label, _rec_style = get_rec_label(_d, ticker=ticker)
            tf_summaries[_tf] = {
                'df': _df, 'd': _d,
                'groups': (_gt, _gp, _gm, _ga),
                'summs': (_ts, _ps, _ms, _xs),
                'tsumm': (_tb, _ts_, _tn_, _verdict),
                'cap': _cap,
                'rec_label': _rec_label,
                'rec_style': _rec_style,
                'verdict': _verdict,
                'mg': _mg,
            }
        except Exception as _e:
            tf_summaries[_tf] = {'error': f'{type(_e).__name__}: {str(_e)[:60]}'}


# ── 多時間框架對齊表 ──
def _verdict_badge_html(verdict: str) -> str:
    """整體 verdict 用顏色 badge 標"""
    colors = {
        '強力買入': ('#0D47A1', '#60CFFF'),
        '買入': ('#0D2E50', '#60B3FF'),
        '上限買入｜持有/短線': ('#3A2A00', '#F0C030'),
        '中立': ('#1A2030', '#9AAABB'),
        '賣出': ('#3B0D0D', '#FF8080'),
        '強力賣出': ('#4A0A0A', '#FF6B6B'),
        '過熱觀望｜禁止新倉': ('#3A1800', '#FF8830'),
        '空頭，不買': ('#3A0808', '#FF5555'),
    }
    bg, fg = colors.get(verdict, ('#1A2030', '#9AAABB'))
    return (f'<span style="background:{bg};color:{fg};'
            f'padding:2px 8px;border-radius:4px;font-size:.72rem;'
            f'font-weight:700;white-space:nowrap">{verdict}</span>')


st.markdown('### 🎯 多時間框架對齊（一覽各 TF 的操作判讀）')
_rows = []
for _tf in timeframes_selected:
    _summ = tf_summaries.get(_tf, {})
    if _summ.get('error'):
        _rows.append(
            f'<tr style="border-bottom:1px solid #1a2f48">'
            f'<td style="padding:6px 8px;font-weight:700">{_tf}</td>'
            f'<td colspan="8" style="padding:6px 8px;color:#aa6655">'
            f'⚠️ {_summ["error"]}</td></tr>'
        )
        continue
    _d = _summ['d']
    _rec_label = _summ['rec_label']
    _rec_style = _summ['rec_style']
    _verdict = _summ['verdict']
    _cap = _summ['cap']
    _tsumm = _summ['tsumm']
    # 取重點指標
    _close = _d.get('close', 0)
    _change_pct = _d.get('change_pct', 0) or 0
    _rsi_v = _d.get('rsi'); _adx_v = _d.get('adx')
    _ema20 = _d.get('ema20'); _ema60 = _d.get('ema60')
    _is_bull = (_ema20 is not None and _ema60 is not None and _ema20 > _ema60)
    _is_bear = (_ema20 is not None and _ema60 is not None and _ema20 < _ema60)
    _trend_str = ('🟢 多頭' if _is_bull else
                   ('🔴 空頭' if _is_bear else '⚪ 整理'))
    _cross_days = _d.get('ema20_cross_days')
    if _cross_days is not None and _cross_days != 0:
        _cross_str = (f'金叉 {_cross_days}b 前' if _cross_days > 0
                       else f'死叉 {abs(_cross_days)}b 前')
    else:
        _cross_str = '-'
    # 漲跌幅顏色
    _chg_color = ('#3dbb6a' if _change_pct >= 0 else '#ff5555')
    _chg_str = f'{_change_pct:+.2f}%' if _change_pct else '0%'
    # cap warning
    _cap_html = (f'<br><span style="font-size:.62rem;color:#aa6655">{_cap}</span>'
                  if _cap else '')

    _rows.append(
        f'<tr style="border-bottom:1px solid #1a2f48">'
        # TF
        f'<td style="padding:7px 8px;font-weight:700;font-size:.88rem">'
        f'{_tf}</td>'
        # Close + 漲跌幅
        f'<td style="padding:7px 8px;text-align:right;font-family:monospace">'
        f'${_close:.2f}<br>'
        f'<span style="color:{_chg_color};font-size:.7rem">{_chg_str}</span></td>'
        # 推薦策略
        f'<td style="padding:7px 8px"><div style="{_rec_style};'
        f'padding:3px 8px;border-radius:4px;font-size:.74rem;'
        f'font-weight:700;display:inline-block;white-space:nowrap">'
        f'{_rec_label}</div></td>'
        # Verdict
        f'<td style="padding:7px 8px">{_verdict_badge_html(_verdict)}{_cap_html}</td>'
        # 多空
        f'<td style="padding:7px 8px;font-size:.78rem">{_trend_str}</td>'
        # Cross days
        f'<td style="padding:7px 8px;font-size:.72rem;color:#a8c0d0">{_cross_str}</td>'
        # RSI
        f'<td style="padding:7px 8px;text-align:center;font-family:monospace;'
        f'color:{"#ff5555" if (_rsi_v and _rsi_v >= 70) else "#3dbb6a" if (_rsi_v and _rsi_v <= 30) else "#c8dff0"}">'
        f'{_rsi_v:.1f}</td>' if _rsi_v else f'<td>-</td>'
    )
    # ADX
    _adx_color = ('#3dbb6a' if _adx_v and _adx_v >= 25 else
                   '#e8a020' if _adx_v and _adx_v >= 22 else '#7a8899')
    _rows[-1] = _rows[-1] + (
        f'<td style="padding:7px 8px;text-align:center;font-family:monospace;'
        f'color:{_adx_color}">{_adx_v:.1f}</td>' if _adx_v else f'<td>-</td>'
    )
    # 加 closing </tr>
    _rows[-1] = _rows[-1] + '</tr>'

_table_html = (
    '<div style="overflow-x:auto;background:#0a1628;border:1px solid #1a2f48;'
    'border-radius:8px;padding:4px;margin-bottom:12px">'
    '<table style="width:100%;border-collapse:collapse">'
    '<thead><tr style="background:#0a1828;border-bottom:2px solid #1a3055">'
    '<th style="padding:8px;text-align:left;color:#7ab0d0;font-size:.74rem;font-weight:700">TF</th>'
    '<th style="padding:8px;text-align:right;color:#7ab0d0;font-size:.74rem;font-weight:700">Close</th>'
    '<th style="padding:8px;text-align:left;color:#7ab0d0;font-size:.74rem;font-weight:700">④ 推薦策略</th>'
    '<th style="padding:8px;text-align:left;color:#7ab0d0;font-size:.74rem;font-weight:700">⑦ 整體 Verdict</th>'
    '<th style="padding:8px;text-align:left;color:#7ab0d0;font-size:.74rem;font-weight:700">EMA 排列</th>'
    '<th style="padding:8px;text-align:left;color:#7ab0d0;font-size:.74rem;font-weight:700">交叉</th>'
    '<th style="padding:8px;text-align:center;color:#7ab0d0;font-size:.74rem;font-weight:700">RSI</th>'
    '<th style="padding:8px;text-align:center;color:#7ab0d0;font-size:.74rem;font-weight:700">ADX</th>'
    '</tr></thead>'
    '<tbody>' + ''.join(_rows) + '</tbody>'
    '</table></div>'
)
if _theme == 'light':
    _table_html = _convert_html_for_light_mode(_table_html)
st.markdown(_table_html, unsafe_allow_html=True)

st.divider()


# ── 每個 TF 一個 tab ──
tabs = st.tabs([f"⏱️ {tf}" for tf in timeframes_selected])

for tab, tf in zip(tabs, timeframes_selected):
    with tab:
        # 🆕 v9.32：reuse 上方對齊表 pre-computed 結果（省重複計算）
        _summ = tf_summaries.get(tf, {})
        if _summ.get('error'):
            st.error(f"⚠️ {tf}: {_summ['error']}")
            continue
        df = _summ['df']
        d = _summ['d']

        with st.spinner(f"渲染 {tf}..."):
            # 顯示 TF metadata
            cfg = get_tf_config(tf)
            st.caption(
                f"📊 **{tf}** ｜ {len(df)} bars ｜ "
                f"每根 {cfg.minutes_per_bar} 分鐘 ｜ "
                f"last bar: `{d.get('_intraday_last_ts', '?')}` "
                f"｜ 所有 period 用 bar 數計算（30W SMA = 150 bars，跨 TF 統一意義）"
            )

            # 🆕 v9.32：detail card 上方互動式 ZigZag chart（plotly）
            _main_cols = st.columns([3, 1])
            with _main_cols[0]:
                _main_chart_bars = st.slider(
                    "📊 主圖顯示最後 N bars",
                    min_value=60, max_value=min(500, len(df)),
                    value=min(180, len(df)),
                    step=20, key=f'_main_chart_bars_{tf}',
                )
            with _main_cols[1]:
                _main_show_macd = st.checkbox(
                    'MACD', value=True, key=f'_main_show_macd_{tf}',
                    help='顯示 MACD(12,26,9) 子圖')
            with st.spinner(f"渲染 {tf} 主 ZigZag chart..."):
                _atr_global = get_zigzag_atr_mult()
                main_fig = build_zigzag_chart_plotly(
                    df,
                    atr_mult=_atr_global,
                    title=f'{ticker} {tf} — ZigZag (ATR×{_atr_global:.2f}) + BB + EMA  '
                          f'｜ 滑鼠 hover 看 OHLC',
                    max_bars=_main_chart_bars,
                    show_bb=True,
                    show_emas=[5, 20, 50, 150, 200],
                    show_macd=_main_show_macd,
                    theme=_theme,
                )
            if main_fig is not None:
                st.plotly_chart(main_fig, use_container_width=True,
                                  key=f'_main_zz_plotly_{tf}')

        # 🆕 v9.32：reuse pre-computed 結果（從 tf_summaries 拿）
        try:
            groups = _summ['groups']
            summs = _summ['summs']
            tsumm = _summ['tsumm']
            cap = _summ['cap']
            rec_label = _summ['rec_label']
            rec_style = _summ['rec_style']
            verdict = _summ['verdict']
            tb, ts_, tn_, _ = tsumm
            _badge_html = (
                f'<div style="display:inline-block;{rec_style};'
                f'padding:5px 12px;border-radius:5px;font-size:.85rem;'
                f'font-weight:700;margin-bottom:8px">'
                f'④ 推薦策略：{rec_label}'
                f'</div>'
            )
            if _theme == 'light':
                _badge_html = _convert_html_for_light_mode(_badge_html)
            st.markdown(_badge_html, unsafe_allow_html=True)

            # ── 整體 verdict badge ──
            cap_html = (
                f'<span style="color:#aa6655;font-size:.72rem;margin-left:6px">{cap}</span>'
                if cap else ''
            )
            _verdict_html = (
                f'<div style="margin:6px 0;font-size:.95rem">'
                f'<span style="color:#7ab0d0">{tf} 綜合判讀：</span>'
                f'{badge(verdict)}'
                f'<span style="color:#7a8899;font-size:.7rem;margin-left:8px">'
                f'(買 {tb} ｜ 賣 {ts_} ｜ 中 {tn_})</span>'
                f'{cap_html}'
                f'</div>'
            )
            if _theme == 'light':
                _verdict_html = _convert_html_for_light_mode(_verdict_html)
            st.markdown(_verdict_html, unsafe_allow_html=True)

            # ── 完整 detail card（含 SEPA/VCP/Stage/Cup/Flat/雙底/綜合決策所有 banner）──
            # 🆕 v9.31：傳入 get_operation_advice 讓 banner 也出現
            html = render_detail(
                ticker, d, groups, summs, tsumm, cap,
                market=info['market'],
                advice_fn=get_operation_advice,   # 完整操作建議 + 所有 banner
                news_fn=None,                       # 新聞不分 TF（跨 TF 一樣）
                concepts_fn=None,                   # 概念股不分 TF（跨 TF 一樣）
            )
            # 🆕 v9.32：light mode 直接替換 HTML 字串內深色 hex（比 CSS 可靠）
            if _theme == 'light':
                html = _convert_html_for_light_mode(html)
            st.markdown(html, unsafe_allow_html=True)

            # 🆕 v9.32：ZigZag ATR 倍數判讀工具（單張可調，「套用」後全域生效）
            with st.expander(
                f"📊 {tf} ZigZag ATR 調整工具（目前全域 ATR×{get_zigzag_atr_mult():.2f}）",
                expanded=False,
            ):
                st.caption(
                    '👉 拉滑桿預覽不同 ATR 倍數的 ZigZag，決定後按「套用此 ATR」— '
                    '套用後**全域生效**：詳細卡上方 ZigZag 圖、雙底/雙頂、VCP 偵測都會用新值'
                )
                _cols = st.columns([3, 1])
                with _cols[0]:
                    _atr_preview = st.slider(
                        "ATR 倍數預覽（0.5-5.0）",
                        min_value=0.5, max_value=5.0,
                        value=float(get_zigzag_atr_mult()),
                        step=0.05,
                        key=f'_atr_preview_{tf}',
                    )
                    _max_bars_zz = st.slider(
                        "顯示最後 N bars",
                        min_value=60, max_value=min(500, len(df)),
                        value=min(180, len(df)),
                        step=20, key=f'_zz_max_bars_{tf}',
                    )
                    # 🆕 v9.32：疊圖選項
                    _overlay_cols = st.columns([1, 4])
                    with _overlay_cols[0]:
                        _show_bb = st.checkbox(
                            'BB(20,2σ)', value=True, key=f'_show_bb_{tf}',
                            help='布林通道：中軌 (20MA) ± 2 標準差')
                    with _overlay_cols[1]:
                        _emas_selected = st.multiselect(
                            'EMA 線',
                            options=[5, 20, 50, 150, 200],
                            default=[5, 20, 50, 150, 200],
                            key=f'_show_emas_{tf}',
                        )
                with _cols[1]:
                    st.markdown(
                        f'**目前全域**<br>'
                        f'<span style="font-size:1.3rem;color:#3b9eff">ATR×{get_zigzag_atr_mult():.2f}</span>',
                        unsafe_allow_html=True)
                    st.markdown(
                        f'**預覽**<br>'
                        f'<span style="font-size:1.3rem;color:#e8a020">ATR×{_atr_preview:.2f}</span>',
                        unsafe_allow_html=True)
                    if st.button(
                        f'✅ 套用 ATR×{_atr_preview:.2f}',
                        key=f'_atr_apply_{tf}',
                        type='primary',
                        use_container_width=True,
                    ):
                        set_zigzag_atr_mult(_atr_preview)
                        st.success(f'已套用 ATR×{_atr_preview:.2f}，重新整理頁面後全域生效')
                        st.rerun()
                    if st.button(
                        '↻ 重置為 1.30',
                        key=f'_atr_reset_{tf}',
                        use_container_width=True,
                    ):
                        set_zigzag_atr_mult(1.30)
                        st.success('已重置為 1.30')
                        st.rerun()

                # 🆕 v9.32：互動式 plotly 圖（hover 顯示 OHLC）+ static fallback
                _use_interactive = st.checkbox(
                    '🖱️ 互動模式（hover 看 OHLC、滾輪縮放）',
                    value=True, key=f'_interactive_{tf}',
                )
                with st.spinner(f"渲染 ATR×{_atr_preview:.2f} 預覽..."):
                    if _use_interactive:
                        fig = build_zigzag_chart_plotly(
                            df,
                            atr_mult=_atr_preview,
                            title=f'{ticker} {tf} — ZigZag (ATR×{_atr_preview:.2f}) + BB + EMA',
                            max_bars=_max_bars_zz,
                            show_bb=_show_bb,
                            show_emas=_emas_selected,
                            show_macd=True,
                            theme=_theme,
                        )
                        if fig is not None:
                            st.plotly_chart(fig, use_container_width=True,
                                              key=f'_plotly_chart_{tf}')
                        else:
                            st.warning('plotly 未安裝或資料不足，請改用靜態模式')
                    else:
                        png = build_zigzag_compare_chart(
                            df,
                            atr_mults=[_atr_preview],
                            title=f'{ticker} {tf} — ZigZag (ATR×{_atr_preview:.2f}) + BB + EMA',
                            max_bars=_max_bars_zz,
                            show_bb=_show_bb,
                            show_emas=_emas_selected,
                        )
                        if png:
                            st.image(png, use_container_width=True)
                        else:
                            st.warning('資料不足，無法渲染')

        except Exception as e:
            import traceback
            st.error(f"渲染失敗：{type(e).__name__}: {e}")
            st.code(traceback.format_exc())


st.divider()
st.caption(
    f"Stock001 v9.30 ｜ {ticker} ｜ TFs: {', '.join(timeframes_selected)} ｜ "
    f"使用 detail_card_render module（與 tv_app 主頁 100% 相同邏輯）"
)
