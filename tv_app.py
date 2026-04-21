#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TradingView Indicator Scanner  — Streamlit Cloud Edition
"""

import io, time
from datetime import datetime

import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TV Indicator Scanner",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* ── Header ── */
.tv-header {
    background: linear-gradient(135deg, #0a0f1e 0%, #0d2137 60%, #0a1628 100%);
    border-bottom: 1px solid #1e3a5f;
    padding: 20px 28px 16px;
    margin: -1rem -1rem 1.5rem -1rem;
    display: flex; align-items: center; gap: 14px;
}
.tv-header h1 {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.3rem; font-weight: 600;
    color: #e8f4fd; margin: 0; letter-spacing: 0.04em;
}
.tv-header .sub { font-size: 0.75rem; color: #5a8ab0; margin-top: 2px; }
.tv-logo { font-size: 1.8rem; }

/* ── Summary cards ── */
.cards-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 1.4rem; }
.card {
    flex: 1; min-width: 140px;
    background: #0d1b2e;
    border: 1px solid #1e3a5f;
    border-radius: 10px;
    padding: 14px 16px;
    text-align: center;
}
.card .c-label { font-size: 0.7rem; color: #5a8ab0; letter-spacing: 0.08em; text-transform: uppercase; }
.card .c-value { font-size: 1.6rem; font-weight: 700; margin-top: 2px; }
.card.buy   .c-value { color: #3b9eff; }
.card.sell  .c-value { color: #ff4d4d; }
.card.neu   .c-value { color: #8899aa; }
.card.total .c-value { color: #f0f4ff; }

/* ── Result table ── */
.res-table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
.res-table th {
    background: #0a1628; color: #5a8ab0;
    font-size: 0.68rem; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.07em;
    padding: 8px 10px; text-align: center;
    border-bottom: 2px solid #1e3a5f;
    white-space: nowrap;
}
.res-table td {
    padding: 7px 10px; text-align: center;
    border-bottom: 1px solid #0f1f33;
    white-space: nowrap;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
}
.res-table tr:hover td { background: rgba(30,58,95,0.35); }

.ticker-cell { font-weight: 700; color: #e8f4fd !important; font-size: 0.9rem !important; }
.market-cell { color: #5a8ab0 !important; font-size: 0.72rem !important; }

.j-buy     { color: #3b9eff; font-weight: 600; }
.j-sell    { color: #ff5555; font-weight: 600; }
.j-neutral { color: #556677; }
.j-na      { color: #334455; font-style: italic; }

.badge {
    display: inline-block; padding: 2px 9px;
    border-radius: 20px; font-size: 0.72rem; font-weight: 700;
    font-family: 'IBM Plex Sans', sans-serif;
    letter-spacing: 0.03em;
}
.badge-strong-buy  { background: #0d3b6e; color: #3b9eff; border: 1px solid #1a5fa8; }
.badge-buy         { background: #0d2e50; color: #60b3ff; border: 1px solid #154d84; }
.badge-strong-sell { background: #4a0a0a; color: #ff6b6b; border: 1px solid #8b1a1a; }
.badge-sell        { background: #3b0d0d; color: #ff8080; border: 1px solid #6b1515; }
.badge-neutral     { background: #1a2030; color: #8899aa; border: 1px solid #2a3545; }

/* ── Detail expander indicators ── */
.ind-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(195px, 1fr)); gap: 8px; margin: 10px 0; }
.ind-item {
    background: #0d1b2e; border: 1px solid #1a2f48;
    border-radius: 8px; padding: 9px 12px;
    display: flex; justify-content: space-between; align-items: center;
}
.ind-label { color: #5a8ab0; font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }
.ind-val   { font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
.ind-buy   .ind-val { color: #3b9eff; }
.ind-sell  .ind-val { color: #ff5555; }
.ind-neu   .ind-val { color: #556677; }

.section-title {
    font-size: 0.65rem; color: #3a5a7a; text-transform: uppercase;
    letter-spacing: 0.1em; font-weight: 700;
    padding: 6px 0 4px; margin-top: 8px;
    border-top: 1px solid #0f1f33;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #080e1a;
    border-right: 1px solid #1e3a5f;
}
section[data-testid="stSidebar"] .stTextArea textarea {
    background: #0d1b2e !important;
    color: #c8dff0 !important;
    border: 1px solid #1e3a5f !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.82rem !important;
}

/* ── Buttons ── */
.stButton button {
    background: linear-gradient(135deg, #0d4a8a, #0a6dd4) !important;
    color: white !important; border: none !important;
    border-radius: 8px !important; font-weight: 600 !important;
    letter-spacing: 0.03em !important;
}
.stButton button:hover {
    background: linear-gradient(135deg, #1055a0, #1478e0) !important;
}
.stDownloadButton button {
    background: linear-gradient(135deg, #0d5c30, #0a8040) !important;
    color: white !important; border: none !important;
    border-radius: 8px !important; font-weight: 600 !important;
}

/* ── Progress ── */
.stProgress > div > div { background: #0a6dd4 !important; }

/* ── General dark theme ── */
.main { background: #060c18; }
.stExpander { border: 1px solid #1a2f48 !important; border-radius: 10px !important; background: #080e1a !important; }
.stExpander summary { color: #c8dff0 !important; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────
TV_SCAN_URL = "https://scanner.tradingview.com/scan"

TV_COLUMNS = [
    "RSI", "Stoch.K", "CCI20", "ADX", "AO", "Mom",
    "MACD.macd", "Stoch.RSI.K", "W.R", "BBPower", "UO",
    "EMA10", "SMA10", "EMA20", "SMA20", "EMA30", "SMA30",
    "EMA50", "SMA50", "EMA100", "SMA100", "EMA200", "SMA200",
    "Ichimoku.BLine", "VWMA", "HullMA9",
    "close", "BB.upper", "BB.lower", "BB.basis"
]

OSC_LABELS = [
    "RSI(14)", "隨機%K", "CCI(20)", "ADX(14)", "AO",
    "動量(10)", "MACD", "StochRSI", "威廉%R", "牛熊力度", "終極震盪", "布林%B"
]
MA_LABELS = [
    "EMA(10)", "SMA(10)", "EMA(20)", "SMA(20)", "EMA(30)", "SMA(30)",
    "EMA(50)", "SMA(50)", "EMA(100)", "SMA(100)", "EMA(200)", "SMA(200)",
    "一目均衡", "VWMA(20)", "Hull MA(9)"
]
MA_KEYS = [
    "EMA10", "SMA10", "EMA20", "SMA20", "EMA30", "SMA30",
    "EMA50", "SMA50", "EMA100", "SMA100", "EMA200", "SMA200",
    "Ichimoku.BLine", "VWMA", "HullMA9"
]

TV_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Referer": "https://www.tradingview.com/",
    "Origin":  "https://www.tradingview.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ─────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────
def fetch_data(symbol: str) -> dict | None:
    payload = {"symbols": {"tickers": [symbol]}, "columns": TV_COLUMNS}
    for attempt in range(2):
        try:
            r = requests.post(TV_SCAN_URL, json=payload,
                              headers=TV_HEADERS, timeout=12)
            r.raise_for_status()
            data = r.json()
            if data.get("data") and data["data"][0].get("d"):
                return dict(zip(TV_COLUMNS, data["data"][0]["d"]))
        except Exception:
            if attempt == 0:
                time.sleep(1.5)
    return None

# ─────────────────────────────────────────────────────────────────
# JUDGMENT LOGIC
# ─────────────────────────────────────────────────────────────────
def _j(v, lo, hi): return "買入" if v < lo else ("賣出" if v > hi else "中立")
def _jz(v):        return "買入" if v > 0  else ("賣出" if v < 0  else "中立")
def _rec(b, s):
    if   b > s * 2:      return "強力買入"
    elif b > s:          return "買入"
    elif s > b * 2:      return "強力賣出"
    elif s > b:          return "賣出"
    else:                return "中立"

def judge_oscillators(d: dict) -> list:
    g = lambda k, dv=0: d.get(k) or dv
    close, bbu, bbl = g("close"), g("BB.upper"), g("BB.lower")
    pct_b = ((close - bbl) / (bbu - bbl) * 100) if (bbu - bbl) != 0 else None
    bb_j  = ("賣出" if pct_b and pct_b > 100 else
             "買入" if pct_b is not None and pct_b < 0 else "中立")
    rsi, stk, cci, adx = g("RSI",50), g("Stoch.K",50), g("CCI20"), g("ADX")
    ao, mom, macd      = g("AO"), g("Mom"), g("MACD.macd")
    srsi, wr, bbp, uo  = g("Stoch.RSI.K",50), g("W.R",-50), g("BBPower"), g("UO",50)
    return [
        (f"{rsi:.2f}",   _j(rsi, 30, 70)),
        (f"{stk:.2f}",   _j(stk, 20, 80)),
        (f"{cci:.2f}",   _j(cci, -100, 100)),
        (f"{adx:.2f}",   "中立"),
        (f"{ao:.2f}",    _jz(ao)),
        (f"{mom:.2f}",   _jz(mom)),
        (f"{macd:.2f}",  _jz(macd)),
        (f"{srsi:.2f}",  _j(srsi, 20, 80)),
        (f"{wr:.2f}",    "買入" if wr < -80 else ("賣出" if wr > -20 else "中立")),
        (f"{bbp:.2f}",   _jz(bbp)),
        (f"{uo:.2f}",    _j(uo, 30, 70)),
        (f"{pct_b:.1f}%" if pct_b is not None else "N/A", bb_j),
    ]

def judge_mas(d: dict) -> list:
    close = d.get("close") or 0
    return [(f"{d.get(k) or 0:.2f}",
             "買入" if close > (d.get(k) or 0) else
             "賣出" if close < (d.get(k) or 0) else "中立")
            for k in MA_KEYS]

def calc_summary(items):
    b = sum(1 for _, j in items if j == "買入")
    s = sum(1 for _, j in items if j == "賣出")
    n = sum(1 for _, j in items if j == "中立")
    return b, s, n, _rec(b, s)

# ─────────────────────────────────────────────────────────────────
# INPUT PARSING
# ─────────────────────────────────────────────────────────────────
def parse_input(text: str) -> list:
    stocks = []
    for raw in text.strip().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        ticker = parts[0].upper()
        try:
            int(ticker)
            stocks.append((ticker, "台股", f"TWSE:{ticker}"))
        except ValueError:
            market = parts[1].upper() if len(parts) > 1 else "NASDAQ"
            stocks.append((ticker, market, f"{market}:{ticker}"))
    return stocks

# ─────────────────────────────────────────────────────────────────
# HTML HELPERS
# ─────────────────────────────────────────────────────────────────
def badge(rec: str) -> str:
    cls = {
        "強力買入": "badge-strong-buy",  "買入": "badge-buy",
        "強力賣出": "badge-strong-sell", "賣出": "badge-sell",
    }.get(rec, "badge-neutral")
    return f'<span class="badge {cls}">{rec}</span>'

def jcell(val: str, judg: str) -> str:
    cls = {"買入": "j-buy", "賣出": "j-sell"}.get(judg, "j-neutral")
    return f'<td class="{cls}">{val}</td>'

def render_summary_table(results: list) -> str:
    rows = ""
    for ticker, market, _, d, error, osc, mas, osummary, msummary, tsummary in results:
        if error or not d:
            rows += (f'<tr><td class="ticker-cell">{ticker}</td>'
                     f'<td class="market-cell">{market}</td>'
                     f'<td colspan="30" class="j-na">— 無資料 —</td></tr>')
            continue
        ob, os_, on_, or_ = osummary
        mb, ms_, mn_, mr_ = msummary
        tb, ts_, tn_, tr_  = tsummary

        osc_cells = "".join(jcell(v, j) for v, j in osc)
        ma_cells  = "".join(jcell(v, j) for v, j in mas)

        osc_sum = f"買:{ob} 賣:{os_} 中:{on_} {badge(or_)}"
        ma_sum  = f"買:{mb} 賣:{ms_} 中:{mn_} {badge(mr_)}"
        tot_sum = f"買:{tb} 賣:{ts_} 中:{tn_} {badge(tr_)}"

        rows += (f'<tr>'
                 f'<td class="ticker-cell">{ticker}</td>'
                 f'<td class="market-cell">{market}</td>'
                 f'{osc_cells}'
                 f'<td style="background:#0a1628;color:#8899aa;font-size:0.72rem">{osc_sum}</td>'
                 f'{ma_cells}'
                 f'<td style="background:#0a1628;color:#8899aa;font-size:0.72rem">{ma_sum}</td>'
                 f'<td style="background:#060c18;font-size:0.72rem">{tot_sum}</td>'
                 f'</tr>')

    osc_ths = "".join(f'<th>{h}</th>' for h in OSC_LABELS)
    ma_ths  = "".join(f'<th>{h}</th>' for h in MA_LABELS)

    return f"""
    <div style="overflow-x:auto; background:#060c18; border-radius:12px;
                border:1px solid #1e3a5f; padding:4px;">
      <table class="res-table">
        <thead><tr>
          <th>代號</th><th>市場</th>
          {osc_ths}
          <th style="background:#0a1628">震盪小結</th>
          {ma_ths}
          <th style="background:#0a1628">均線小結</th>
          <th style="background:#060c18">整體建議</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""

def render_detail(ticker, market, d, osc, mas, osummary, msummary, tsummary) -> str:
    ob, os_, on_, or_ = osummary
    mb, ms_, mn_, mr_ = msummary
    tb, ts_, tn_, tr_ = tsummary

    def ind(label, val, judg):
        cls = {"買入":"ind-buy","賣出":"ind-sell"}.get(judg,"ind-neu")
        return (f'<div class="ind-item {cls}">'
                f'<span class="ind-label">{label}</span>'
                f'<span class="ind-val">{val} / {judg}</span></div>')

    osc_items = "".join(ind(OSC_LABELS[i], v, j) for i, (v,j) in enumerate(osc))
    ma_items  = "".join(ind(MA_LABELS[i],  v, j) for i, (v,j) in enumerate(mas))
    close_val = d.get("close") or 0

    return f"""
    <div style="padding:4px 8px">
      <div style="display:flex;gap:16px;margin-bottom:12px;flex-wrap:wrap">
        <span style="color:#5a8ab0;font-size:0.75rem">
          收盤價 <b style="color:#e8f4fd;font-family:IBM Plex Mono">{close_val:.2f}</b>
        </span>
        <span>震盪：{badge(or_)}
          <span style="color:#445566;font-size:0.72rem"> 買:{ob} 賣:{os_} 中:{on_}</span>
        </span>
        <span>均線：{badge(mr_)}
          <span style="color:#445566;font-size:0.72rem"> 買:{mb} 賣:{ms_} 中:{mn_}</span>
        </span>
        <span>整體：{badge(tr_)}
          <span style="color:#445566;font-size:0.72rem"> 買:{tb} 賣:{ts_} 中:{tn_}</span>
        </span>
      </div>
      <div class="section-title">震盪指標</div>
      <div class="ind-grid">{osc_items}</div>
      <div class="section-title">移動均線</div>
      <div class="ind-grid">{ma_items}</div>
    </div>"""

# ─────────────────────────────────────────────────────────────────
# EXCEL EXPORT (BytesIO)
# ─────────────────────────────────────────────────────────────────
def build_excel(results: list) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "指標報告"
    ws.sheet_view.showGridLines = False

    def fill(h): return PatternFill("solid", start_color=h, fgColor=h)
    def font(c="000000", sz=9, bd=False): return Font(name="Arial", size=sz, bold=bd, color=c)
    mid = Side(style="medium")
    ctr = Alignment(horizontal="center", vertical="center")

    JCOL = {"買入":"0D47A1","賣出":"C0392B","中立":"888888",
            "強力買入":"0D47A1","強力賣出":"C0392B"}

    col_defs = (
        [("代號",8,"1E3A5F"),("市場",7,"1E3A5F")]
        + [(h,14,"1A3A6C") for h in OSC_LABELS]
        + [("震盪小結",28,"0D2244")]
        + [(h,14,"1A4A2C") for h in MA_LABELS]
        + [("均線小結",28,"0D3320"),("整體建議",30,"2C1654")]
    )
    for ci,(label,width,bg) in enumerate(col_defs,1):
        c = ws.cell(1,ci,label)
        c.font=font("FFFFFF",9,True); c.fill=fill(bg)
        c.alignment=ctr; c.border=Border(bottom=mid)
        ws.column_dimensions[get_column_letter(ci)].width=width
    ws.row_dimensions[1].height=22; ws.freeze_panes="C2"

    rf_e={"osc":"EBF0FA","os":"D6E4FF","ma":"EAFAF1","ms":"D5F5E3","base":"F0F4FF"}
    rf_o={"osc":"F5F8FF","os":"E8F0FF","ma":"F5FFF8","ms":"E8FFF0","base":"FFFFFF"}

    for ri,(ticker,market,_,d,error,osc,mas,osumm,msumm,tsumm) in enumerate(results,2):
        ws.row_dimensions[ri].height=18
        rf=rf_e if ri%2==0 else rf_o
        def cell(col,val,bg,fc="000000",sz=9,bd=False):
            c=ws.cell(ri,col,val); c.font=font(fc,sz,bd)
            c.fill=fill(bg); c.alignment=ctr; return c
        cell(1,ticker,rf["base"],"1E3A5F",10,True)
        cell(2,market,rf["base"],"555555",9)
        if error or not d:
            for ci in range(3,len(col_defs)+1): cell(ci,"無資料",rf["base"],"AAAAAA",9)
            continue
        ob,os_,on_,or_=osumm; mb,ms_,mn_,mr_=msumm; tb,ts_,tn_,tr_=tsumm
        ci=3
        for v,j in osc:
            cell(ci,f"{v} / {j}",rf["osc"],JCOL.get(j,"000000"),9,j!="中立"); ci+=1
        cell(ci,f"買入:{ob}  賣出:{os_}  中立:{on_}  →  {or_}",
             rf["os"],JCOL.get(or_,"444444"),9,True); ci+=1
        for v,j in mas:
            cell(ci,f"{v} / {j}",rf["ma"],JCOL.get(j,"000000"),9,j!="中立"); ci+=1
        cell(ci,f"買入:{mb}  賣出:{ms_}  中立:{mn_}  →  {mr_}",
             rf["ms"],JCOL.get(mr_,"444444"),9,True); ci+=1
        tot_bg=({"強力買入":"1A5276","買入":"2471A3",
                 "強力賣出":"922B21","賣出":"C0392B"}.get(tr_,"626567"))
        cell(ci,f"買入:{tb}  賣出:{ts_}  中立:{tn_}  →  {tr_}",
             tot_bg,"FFFFFF",10,True)

    ws.cell(len(results)+3,1,
            f"產出時間：{datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}"
            ).font=font("999999",8)
    buf=io.BytesIO(); wb.save(buf); return buf.getvalue()

# ─────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────
# ── Header ──────────────────────────────────────────────────────
st.markdown("""
<div class="tv-header">
  <div class="tv-logo">📊</div>
  <div>
    <h1>TradingView Indicator Scanner</h1>
    <div class="sub">12 震盪指標 · 15 移動均線 · 布林通道 %B · 即時抓取 · Excel 匯出</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📋 股票清單")
    st.markdown("""
<div style="font-size:0.72rem;color:#5a8ab0;line-height:1.7;margin-bottom:8px">
  <b style="color:#8ab8d8">台股</b>：直接輸入代號（純數字）<br>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">2330</code>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">00878</code><br>
  <b style="color:#8ab8d8">美股</b>：代號,交易所<br>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">BOTZ,NASDAQ</code>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">AAPL,NYSE</code><br>
  <span style="color:#334455"># 開頭為註解行</span>
</div>
""", unsafe_allow_html=True)

    stock_input = st.text_area(
        "輸入股票清單",
        value="2330\n2317\n00878\nBOTZ,NASDAQ\nNVDA,NASDAQ\nAAPL,NASDAQ",
        height=220,
        label_visibility="collapsed",
        placeholder="每行一個...",
    )

    st.markdown("<br>", unsafe_allow_html=True)
    fetch_btn = st.button("🔍  開始抓取資料", type="primary", use_container_width=True)

    st.markdown("---")
    st.markdown("""
<div style="font-size:0.68rem;color:#334455;line-height:1.8">
  <b style="color:#3a5a7a">震盪指標（12）</b><br>
  RSI · 隨機%K · CCI · ADX<br>AO · 動量 · MACD · StochRSI<br>
  威廉%R · 牛熊力度 · 終極震盪 · 布林%B<br><br>
  <b style="color:#3a5a7a">移動均線（15）</b><br>
  EMA/SMA 10/20/30/50/100/200<br>
  一目均衡基準線 · VWMA · Hull MA
</div>
""", unsafe_allow_html=True)

# ── Main Area ────────────────────────────────────────────────────
if not fetch_btn:
    st.markdown("""
<div style="text-align:center;padding:60px 20px;color:#1e3a5f">
  <div style="font-size:3rem;margin-bottom:16px">📈</div>
  <div style="font-size:1rem;color:#3a6a9a">在左側輸入股票代號，點擊「開始抓取資料」</div>
  <div style="font-size:0.78rem;color:#1e3a5f;margin-top:8px">
    支援台股（TWSE）· NASDAQ · NYSE · AMEX · OTC
  </div>
</div>
""", unsafe_allow_html=True)
    st.stop()

# ── Fetch ────────────────────────────────────────────────────────
stocks = parse_input(stock_input)
if not stocks:
    st.error("股票清單為空，請重新輸入")
    st.stop()

progress_bar = st.progress(0, text="準備中...")
status_text  = st.empty()

results = []
for i, (ticker, market, symbol) in enumerate(stocks):
    progress_bar.progress((i) / len(stocks), text=f"抓取 {symbol}  ({i+1}/{len(stocks)})")
    status_text.markdown(
        f'<div style="font-size:0.78rem;color:#5a8ab0;text-align:center">'
        f'正在抓取 <b style="color:#8ab8d8">{symbol}</b>...</div>',
        unsafe_allow_html=True)
    d = fetch_data(symbol)
    if d:
        osc  = judge_oscillators(d)
        mas  = judge_mas(d)
        osumm = calc_summary(osc)
        msumm = calc_summary(mas)
        ob, os_, on_, or_ = osumm
        mb, ms_, mn_, mr_ = msumm
        tb = ob+mb; ts = os_+ms_; tn = on_+mn_
        tr = _rec(tb, ts)
        results.append((ticker, market, symbol, d, False,
                        osc, mas, osumm, msumm, (tb,ts,tn,tr)))
    else:
        results.append((ticker, market, symbol, None, True,
                        [], [], (0,0,0,"中立"), (0,0,0,"中立"), (0,0,0,"中立")))
    if i < len(stocks) - 1:
        time.sleep(0.35)

progress_bar.progress(1.0, text="完成 ✓")
status_text.empty()

# ── Aggregate stats ───────────────────────────────────────────────
total     = len(results)
ok        = sum(1 for r in results if not r[4])
buy_count = sum(1 for r in results if not r[4] and r[9][3] in ("買入","強力買入"))
sell_count= sum(1 for r in results if not r[4] and r[9][3] in ("賣出","強力賣出"))
neu_count = sum(1 for r in results if not r[4] and r[9][3] == "中立")

st.markdown(f"""
<div class="cards-row">
  <div class="card total">
    <div class="c-label">抓取完成</div>
    <div class="c-value">{ok}<span style="font-size:1rem;color:#3a5a7a">/{total}</span></div>
  </div>
  <div class="card buy">
    <div class="c-label">整體偏買入</div>
    <div class="c-value">{buy_count}</div>
  </div>
  <div class="card sell">
    <div class="c-label">整體偏賣出</div>
    <div class="c-value">{sell_count}</div>
  </div>
  <div class="card neu">
    <div class="c-label">中立</div>
    <div class="c-value">{neu_count}</div>
  </div>
  <div class="card total">
    <div class="c-label">更新時間</div>
    <div class="c-value" style="font-size:0.95rem">{datetime.now().strftime("%H:%M")}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Full Table ────────────────────────────────────────────────────
st.markdown("#### 完整指標一覽表")
st.markdown(render_summary_table(results), unsafe_allow_html=True)

# ── Detail Expanders ──────────────────────────────────────────────
st.markdown("<br>#### 個股指標詳細", unsafe_allow_html=True)
for ticker, market, symbol, d, error, osc, mas, osumm, msumm, tsumm in results:
    _, _, _, tr_ = tsumm
    title = f"{ticker}  {market}  {tr_}" if not error else f"{ticker}  —  無資料"
    with st.expander(title, expanded=False):
        if error or not d:
            st.markdown('<div style="color:#334455;padding:12px">無法取得資料，請確認代號與交易所是否正確</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown(render_detail(ticker, market, d, osc, mas,
                                      osumm, msumm, tsumm),
                        unsafe_allow_html=True)

# ── Download ──────────────────────────────────────────────────────
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    excel_bytes = build_excel(results)
    filename = f"TradingView_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.download_button(
        label="📥  下載 Excel 報告",
        data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.markdown(
        f'<div style="text-align:center;font-size:0.7rem;color:#334455;margin-top:6px">'
        f'{total} 支股票 · {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>',
        unsafe_allow_html=True)
