#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re, io, warnings, requests, time
from datetime import datetime

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────
# 應用版本資訊
# ─────────────────────────────────────────────────────────────────
APP_VERSION   = "v9.11"
APP_UPDATED   = "2026-04-30 16:00"
APP_NOTES     = (
    "🥇 OOS 真王者：倒鎚 hold=30 + max_pos=50 + drop_deep priority → "
    "OOS CAGR +7.30% / Sharpe 1.74 / MDD -4.64% ｜ "
    "🆕 倒鎚月份效應：4 月黃金 (89.5% 漲 / +15.91%)、3 月地雷 (-7.94%) — 警報自動標示 ｜ "
    "T1_V7 + fixed_10% 止損 → CAGR +10.87% / MDD -8.24%（雙改善）｜ "
    "美股 TOP 200 + T1_V7 → CAGR +14.35% / Sharpe 2.41（universe 篩選 > tuning）｜ "
    "Walk-forward 推翻過擬合：T1_V7 hold=60 是 trap (Sharpe 1.92→0.17) ｜ "
    "LINE Bot 推送 + alerts quality_score 排序 + Live 命中率追蹤"
)
APP_VALIDATIONS = (
    "🚀 倒鎚 OOS 2024+ 71.8% 勝率 / +9.35% 30d / PF 5.5（無過擬合，反而強化）｜ "
    "🎯 T1_V7 hold=30 OOS CAGR +14.78% / Sharpe 0.81 — 1M 投組真贏家 ｜ "
    "⚠️ T1_V7 hold=60 是過擬合 trap (Sharpe 1.92→0.17) — 勿用 ｜ "
    "💎 max_pos=50 + drop_deep priority → 倒鎚 Sharpe 0.54→1.99 / MDD -3.44% ｜ "
    "🇺🇸 US TOP 200 + T1_V7 → CAGR +14.35% / Sharpe 2.41（universe 篩選比 tuning 重要）｜ "
    "📊 P5+VWAPEXEC TEST 22月勝率 56.2% / RR 0.611（baseline 0.223）2.7×提升 ｜ "
    "🇺🇸 P10+POS+ADX18 高流動 555 檔 RR 0.496 / 勝率 55% ｜ "
    "📅 TW Walk-Forward 7 年：6/7 年正 RR ｜ 🪙 v8 不適用加密貨幣"
)

import numpy as np
import pandas as pd
import ta
import yfinance as yf

# 設定 yfinance 請求 headers，避免被 Yahoo Finance 封鎖
try:
    import requests as _req
    _session = _req.Session()
    _session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
    })
    yf.set_config(session=_session)
except Exception:
    pass

# twstock loaded on demand via cached function
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Indicator Scanner", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
.tv-header{background:linear-gradient(135deg,#0a0f1e 0%,#0d2137 60%,#0a1628 100%);border-bottom:1px solid #1e3a5f;padding:20px 28px 16px;margin:-1rem -1rem 1.5rem -1rem;display:flex;align-items:center;gap:14px;}
.tv-header h1{font-family:'IBM Plex Mono',monospace;font-size:1.3rem;font-weight:600;color:#e8f4fd;margin:0;letter-spacing:.04em;}
.tv-header .sub{font-size:.75rem;color:#5a8ab0;margin-top:2px;}
.cards-row{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:1.4rem;}
.card{flex:1;min-width:160px;background:#0d1b2e;border:1px solid #1e3a5f;border-radius:10px;padding:12px 14px;text-align:center;display:flex;flex-direction:column;align-items:center;}
.card .c-label{font-size:.7rem;letter-spacing:.08em;text-transform:uppercase;}
.card .c-value{font-size:1.6rem;font-weight:700;margin-top:2px;}
.card.total{background:#0d1b2e;}
.card.total .c-label{color:#5a8ab0;} .card.total .c-value{color:#f0f4ff;}
/* 🆕 進場：綠 */
.card.entry{background:#0a1e10;border-color:#3dbb6a55;}
.card.entry .c-label{color:#3dbb6a;} .card.entry .c-value{color:#3dbb6a;}
/* 🆕 出場：紅 */
.card.exit{background:#1a0808;border-color:#ff555555;}
.card.exit .c-label{color:#ff7777;} .card.exit .c-value{color:#ff7777;}
/* 🆕 持倉：藍 */
.card.hold{background:#0a1830;border-color:#5a8ab055;}
.card.hold .c-label{color:#7abadd;} .card.hold .c-value{color:#7abadd;}
/* 觀望：灰 */
.card.neu{background:#0d1b2e;}
.card.neu .c-label{color:#7a8899;} .card.neu .c-value{color:#7a8899;}
/* 舊 buy / sell 對齊（avoid breaking other usages）*/
.card.buy .c-value{color:#3dbb6a;}.card.sell .c-value{color:#ff7777;}
.res-table{width:100%;border-collapse:collapse;font-size:.82rem;}
.res-table th{background:#0a1628;color:#5a8ab0;font-size:.68rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;padding:8px 10px;text-align:center;border-bottom:2px solid #1e3a5f;white-space:nowrap;}
.res-table td{padding:7px 10px;text-align:center;border-bottom:1px solid #0f1f33;white-space:nowrap;font-family:'IBM Plex Mono',monospace;font-size:.78rem;}
.res-table tr:hover td{background:rgba(30,58,95,.35);}
.ticker-cell{font-weight:700;color:#e8f4fd !important;font-size:.9rem !important;}
.market-cell{color:#7ab0d0 !important;font-size:.72rem !important;}
.j-buy{color:#3b9eff;font-weight:600;}.j-sell{color:#ff5555;font-weight:600;}.j-neutral{color:#8899aa;}.j-na{color:#4a6070;font-style:italic;}
.badge{display:inline-block;padding:2px 9px;border-radius:20px;font-size:.72rem;font-weight:700;font-family:'IBM Plex Sans',sans-serif;letter-spacing:.03em;}
.badge-strong-buy{background:#0d3b6e;color:#3b9eff;border:1px solid #1a5fa8;}
.badge-buy{background:#0d2e50;color:#60b3ff;border:1px solid #154d84;}
.badge-strong-sell{background:#4a0a0a;color:#ff6b6b;border:1px solid #8b1a1a;}
.badge-sell{background:#3b0d0d;color:#ff8080;border:1px solid #6b1515;}
.badge-neutral{background:#1a2030;color:#8899aa;border:1px solid #2a3545;}
.badge-buy-limit{background:#3a2a00;color:#f0c030;border:1px solid #8a6000;}
.badge-overheat{background:#3a1800;color:#ff8830;border:1px solid #8a3800;}
.badge-bearish{background:#3a0808;color:#ff5555;border:1px solid #7a1010;}
.ind-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:8px;margin:10px 0;}
.ind-item{background:#0d1b2e;border:1px solid #1e3550;border-radius:8px;padding:10px 13px;display:flex;flex-direction:column;gap:5px;}
.ind-label{color:#7aaac8;font-size:.72rem;font-weight:600;letter-spacing:.03em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ind-val{font-family:'IBM Plex Sans',sans-serif;font-size:.82rem;font-weight:600;word-break:break-word;line-height:1.35;}
.ind-buy .ind-val{color:#3b9eff;}.ind-sell .ind-val{color:#ff5555;}.ind-neu .ind-val{color:#9aaabb;}
.section-title{font-size:.65rem;color:#3a5a7a;text-transform:uppercase;letter-spacing:.1em;font-weight:700;padding:6px 0 4px;margin-top:8px;border-top:1px solid #0f1f33;}
section[data-testid="stSidebar"]{background:#080e1a;border-right:1px solid #1e3a5f;}
section[data-testid="stSidebar"] .stTextArea textarea{background:#0d1b2e !important;color:#c8dff0 !important;border:1px solid #1e3a5f !important;font-family:'IBM Plex Mono',monospace !important;font-size:.82rem !important;}
.stButton button{background:linear-gradient(135deg,#0d4a8a,#0a6dd4) !important;color:white !important;border:none !important;border-radius:8px !important;font-weight:600 !important;}
.stDownloadButton button{background:linear-gradient(135deg,#0d5c30,#0a8040) !important;color:white !important;border:none !important;border-radius:8px !important;font-weight:600 !important;}
.stProgress > div > div{background:#0a6dd4 !important;}
.main{background:#060c18;}
.stExpander{border:1px solid #1a2f48 !important;border-radius:10px !important;background:#080e1a !important;}
.res-table a:hover{text-decoration:underline !important;opacity:.85;}
</style>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────
# LABELS
# ─────────────────────────────────────────────────────────────────
OSC_LABELS = ["RSI(14)","隨機%K","CCI(20)","ADX(14)","AO","動量(10)","MACD",
              "StochRSI","威廉%R","牛熊力度","終極震盪","布林%B"]
MA_LABELS  = ["EMA(10)","SMA(10)","EMA(20)","SMA(20)","EMA(30)","SMA(30)",
              "EMA(50)","SMA(50)","EMA(60)","SMA(60)","EMA(100)","SMA(100)",
              "EMA(200)","SMA(200)","一目均衡基準線","VWMA(20)","Hull MA(9)"]

# ── 四群組加權（依使用者指定：趨勢40/位置30/動能20/輔助10）────────
GROUP_NAMES   = ["趨勢結構", "位置風險", "動能確認", "輔助指標"]
GROUP_WEIGHTS = [40, 30, 20, 10]   # 各群組佔總分百分比
GROUP_COLORS  = ["#3b9eff", "#f0a030", "#a060ff", "#7a8899"]  # 趨勢/位置/動能/輔助

# Excel 匯出仍使用舊格式（保留相容性）
OSC_WEIGHTS = [2.0] * 12
MA_WEIGHTS  = [1.5]*6 + [1.2]*4 + [1.0]*7

# ── 指數 / 特殊代號別名對照 ──────────────────────────────────────
SYMBOL_ALIASES = {
    "DJI":"^DJI","DJIA":"^DJI","SPX":"^GSPC","SP500":"^GSPC",
    "NDX":"^NDX","NASDAQ":"^IXIC","COMP":"^IXIC",
    "VIX":"^VIX","RUT":"^RUT",
    "TWII":"^TWII","TWI":"^TWII",
    "N225":"^N225","NIKKEI":"^N225",
    "HSI":"^HSI","KOSPI":"^KS11",
    "SSE":"000001.SS","SHCOMP":"000001.SS",
    "DAX":"^GDAXI","FTSE":"^FTSE","CAC":"^FCHI",
    "GOLD":"GC=F","OIL":"CL=F","WTI":"CL=F",
    "USDJPY":"JPY=X","EURUSD":"EURUSD=X","USDTWD":"TWD=X",
}

INDEX_NAMES = {
    "^DJI":"道瓊工業指數","^GSPC":"S&P 500","^IXIC":"NASDAQ 綜合指數",
    "^NDX":"NASDAQ 100","^VIX":"VIX 恐慌指數","^RUT":"羅素 2000",
    "^TWII":"台灣加權指數","^N225":"日經 225","^HSI":"恒生指數",
    "^KS11":"韓國 KOSPI","000001.SS":"上證指數",
    "^GDAXI":"德國 DAX","^FTSE":"英國富時 100","^FCHI":"法國 CAC 40",
    "GC=F":"黃金期貨","CL=F":"WTI 原油期貨",
    "JPY=X":"USD/JPY","EURUSD=X":"EUR/USD","TWD=X":"USD/TWD",
}

# ── 台股代號識別 ─────────────────────────────────────────────────
def is_tw_stock(ticker: str) -> bool:
    """台股：純數字 或 數字+單一英文字母結尾（00632R、006205L）"""
    import re
    return bool(re.match(r'^\d+[A-Z]?$', ticker))

def get_yf_symbol(ticker: str) -> str:
    """將使用者輸入代號轉換為 yfinance 查詢格式"""
    if ticker in SYMBOL_ALIASES:
        return SYMBOL_ALIASES[ticker]
    if is_tw_stock(ticker):
        # 🆕 OTC 上櫃股直接用 .TWO（避免 .TW fallback 浪費時間）
        if ticker in _OTC_TICKERS:
            return ticker + ".TWO"
        return ticker + ".TW"
    return ticker


# 🆕 OTC 上櫃股清單（加速 fetch — 跳過 .TW 失敗 retry）
@st.cache_data
def _load_otc_tickers():
    try:
        from pathlib import Path as _P
        import json as _json
        p = _P(__file__).parent / 'otc_tickers.json'
        if p.exists():
            return set(_json.load(open(p, encoding='utf-8')))
    except Exception:
        pass
    return set()
_OTC_TICKERS = _load_otc_tickers()


# 🆕 VWAPEXEC 適用清單（從 build_applicable_list.py 產出）
@st.cache_data(ttl=3600)
def _load_vwap_applicable():
    """載入 VWAPEXEC 適用分級：{ticker: {tier: 'TOP'/'OK'/'NA', delta: float}}"""
    try:
        from pathlib import Path as _P
        import json as _json
        p = _P(__file__).parent / 'vwap_applicable.json'
        if p.exists():
            with open(p, encoding='utf-8') as f:
                return _json.load(f)
    except Exception:
        pass
    return {}


def get_vwap_tier(ticker: str) -> dict:
    """取得 VWAPEXEC 適用分級（tier + delta）"""
    data = _load_vwap_applicable()
    return data.get(ticker, {})

# ── TradingView 圖表連結對照（ticker → TV symbol） ───────────────
TV_CHART_MAP = {
    "DJI":"DJ:DJI","DJIA":"DJ:DJI",
    "SPX":"SP:SPX","SP500":"SP:SPX",
    "NDX":"NASDAQ:NDX",
    "NASDAQ":"NASDAQ:COMP","COMP":"NASDAQ:COMP",
    "VIX":"CBOE:VIX",
    "RUT":"TVC:RUT",
    "TWII":"TWSE:TAIEX","TWI":"TWSE:TAIEX",
    "N225":"INDEX:NKY","NIKKEI":"INDEX:NKY",
    "HSI":"HSI:HSI",
    "KOSPI":"KRX:KOSPI",
    "SSE":"SSE:000001","SHCOMP":"SSE:000001",
    "DAX":"XETR:DAX","FTSE":"LSE:UKX","CAC":"EURONEXT:PX1",
    "GOLD":"TVC:GOLD","OIL":"TVC:USOIL","WTI":"TVC:USOIL",
    "USDJPY":"FX:USDJPY","EURUSD":"FX:EURUSD","USDTWD":"FX:USDTWD",
}

def get_tv_url(ticker: str, market: str) -> str:
    """產生 TradingView 圖表連結"""
    base = "https://www.tradingview.com/chart/?symbol="
    if ticker in TV_CHART_MAP:
        return base + TV_CHART_MAP[ticker]
    if market == "台股":
        # 上櫃(.TWO)用 TPEX，上市(.TW)用 TWSE
        tw_names = _get_tw_names()
        # twstock codes 包含市場資訊
        try:
            import twstock
            info = twstock.codes.get(ticker)
            if info and getattr(info, 'market', '') in ('上櫃', 'OTC'):
                return base + f"TPEX:{ticker}"
        except Exception:
            pass
        return base + f"TWSE:{ticker}"
    if market in ("NASDAQ","NYSE","AMEX","OTC"):
        return base + f"{market}:{ticker}"
    return base + ticker

def get_ai_url(ticker: str, name: str, d: dict,
               platform_url_tpl: str = "https://www.perplexity.ai/search?q={prompt}") -> str:
    import urllib.parse
    close  = d.get("close")  or 0
    sma50  = d.get("sma50")  or 0
    sma200 = d.get("sma200") or 0
    bbu    = d.get("bbu")    or 0
    bbl    = d.get("bbl")    or 0
    bbm    = d.get("ema20")  or 0
    display = f"{ticker}（{name}）" if name and name != ticker else ticker
    lines = [
        f"你是一位專業量化交易員，現在請你只針對 {display} 這檔標的，使用日線圖與布林通道為主的技術分析，幫我判斷短中期的買點與賣點。",
        "",
        "分析要求：",
        f"先簡要描述目前 {ticker} 價格相對於 20 日均線（布林中軌）、50 日均線、200 日均線的位置，以及整體趨勢是偏多頭、盤整還是空頭。",
        f"現價約 {close:.2f}，50 日均線約 {sma50:.2f}，200 日均線約 {sma200:.2f}，請一併納入考量。",
        "",
        "說明布林通道三條線的狀態：",
        f"20MA 中軌（目前約 {bbm:.2f}）",
        f"上軌 = 中軌 + 2 倍標準差（目前約 {bbu:.2f}）",
        f"下軌 = 中軌 − 2 倍標準差（目前約 {bbl:.2f}）",
        "並判斷通道目前是「張口擴大」（走趨勢）還是「收斂變窄」（盤整壓縮）。",
        "",
        "依照以下布林通道操作原則，具體列出：",
        "可能的「低風險買點」條件：例如股價由下往上突破下軌、或由下往上突破中軌且帶量，代表跌勢鈍化或多頭啟動，可分批布局或加碼。",
        "可能的「獲利了結／賣點」條件：例如股價接近或碰到上軌出現明顯壓回、或由上往下跌破中軌，代表多頭力道轉弱，可減碼或出場。",
        "若價格在中軌與上軌之間且沿著上軌上行，視為強勢多頭，只調整移動停損，不急著賣出。",
        "",
        "如果布林通道出現「收口壓縮」的型態，請說明：這代表波動縮小、可能醞釀後續大的突破。價格向上突破上軌搭配帶寬擴大時，可以視為順勢做多訊號；價格向下跌破下軌搭配帶寬擴大時，可以視為順勢做空或觀望不買的訊號。",
        "",
        "最後請整理成：",
        "1～2 個「建議買進區間與條件」",
        "1～2 個「建議停利／停損的區間與條件」",
        "每個條件用條列式說明「價格位置 + 布林通道狀態 + 風險說明」，並提醒這只是技術面機率，不是保證。",
        "",
        "請用繁體中文作答，條列清楚，避免空洞的形容詞，重點放在可執行的「條件式規則」。",
        "所有的分析以表格呈現。",
    ]
    prompt = "\n".join(lines)
    encoded = urllib.parse.quote(prompt)
    return platform_url_tpl.replace("{prompt}", encoded)

def get_prompt_text(ticker: str, name: str, d: dict) -> str:
    """回傳純文字提示詞（供複製用）"""
    close  = d.get("close")  or 0
    sma50  = d.get("sma50")  or 0
    sma200 = d.get("sma200") or 0
    bbu    = d.get("bbu")    or 0
    bbl    = d.get("bbl")    or 0
    bbm    = d.get("ema20")  or 0
    display = f"{ticker}（{name}）" if name and name != ticker else ticker
    lines = [
        f"你是一位專業量化交易員，現在請你只針對 {display} 這檔標的，使用日線圖與布林通道為主的技術分析，幫我判斷短中期的買點與賣點。",
        "",
        f"現價約 {close:.2f}，50 日均線約 {sma50:.2f}，200 日均線約 {sma200:.2f}，布林上軌約 {bbu:.2f}，布林中軌約 {bbm:.2f}，布林下軌約 {bbl:.2f}。",
        "",
        "請分析：1) 趨勢與均線位置 2) 布林通道狀態（張口/收口）3) 買點條件 4) 賣點條件 5) 停損停利建議。",
        "",
        "請用繁體中文作答，條列清楚，所有的分析以表格呈現。",
    ]
    return "\n".join(lines)


# ── 🆕 v9.10t：美股盤後快訊（即時抓昨夜美股報酬）──────
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


# ── 🆕 v9.10u：5 大發現整合 ────────────────────────────────────
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


@st.cache_data(ttl=86400, show_spinner=False)
def _get_sector_ranking_recent() -> list:
    """過去 1 個月各產業平均報酬排名（用 vwap_applicable 中的個股月報酬）
    回傳 [(sector, monthly_return%, n_stocks), ...] 由強到弱排序"""
    from pathlib import Path as _P
    import json as _json
    # 讀行業
    industry_map = {}
    p = _P(__file__).parent / 'tw_universe.txt'
    if not p.exists(): return []
    for line in p.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 5 and parts[4]:
            industry_map[parts[0].strip()] = parts[4].strip()

    # 用 yfinance 抓近 30 日資料（避免依賴 data_cache）
    sectors = {}
    for tk, ind in industry_map.items():
        sectors.setdefault(ind, []).append(tk)

    # 只跑流動性夠的（取 vwap_applicable）
    vwap_path = _P(__file__).parent / 'vwap_applicable.json'
    top_set = set()
    if vwap_path.exists():
        try:
            vp = _json.loads(vwap_path.read_text(encoding='utf-8'))
            top_set = set(t for t, info in vp.items()
                          if info.get('tier') in ('TOP', 'OK'))
        except: pass

    # 對每產業計算月報酬 (用 yfinance batch)
    out = []
    for sec, members in sectors.items():
        members = [t for t in members if t in top_set][:20]  # 每產業最多 20 檔
        if len(members) < 5: continue
        rets = []
        try:
            yf_tk = [f'{t}.TW' for t in members]
            df = yf.download(yf_tk, period='1mo', interval='1d',
                             progress=False, group_by='ticker', threads=True,
                             auto_adjust=False)
            if df is None or df.empty: continue
            for t in yf_tk:
                try:
                    sub = df[t] if len(yf_tk) > 1 else df
                    sub = sub.dropna(how='all')
                    if len(sub) < 15: continue
                    ret = (sub['Close'].iloc[-1] - sub['Close'].iloc[0]) / sub['Close'].iloc[0] * 100
                    rets.append(ret)
                except: continue
        except Exception:
            continue
        if rets:
            out.append((sec, float(np.mean(rets)), len(rets)))
    out.sort(key=lambda x: -x[1])
    return out


@st.cache_data(ttl=3600, show_spinner=False)
# ── 🆕 市場廣度警報（D：偵測「指數漲、廣度差」失效市況）──────
@st.cache_data(ttl=3600, show_spinner=False)
def _get_market_breadth() -> dict:
    """從 yfinance 取台股大盤 + 計算廣度。
    回傳 {twii_60d_chg, breadth_pct, alert_level, msg}
    """
    try:
        twii = yf.Ticker('^TWII').history(period='3mo', auto_adjust=True)
        if len(twii) < 60: return {'has_data': False}
        cur = float(twii['Close'].iloc[-1])
        c60 = float(twii['Close'].iloc[-60])
        twii_60d_chg = (cur - c60) / c60 * 100

        # 用幾檔權值股代理計算廣度（取重要 30 檔，每檔看是否仍 > EMA60）
        # 這是廣度的快速近似，避免抓全市場
        proxies = ['2330','2317','2454','2412','2882','2891','2308','2382',
                   '2884','2603','2885','2886','3008','2880','2207','1101',
                   '1216','1303','2002','2615','2609','2610','3711','2474',
                   '6505','3034','2618','3037','2890','2887']
        above_ema60 = 0; total = 0
        for t in proxies:
            try:
                df = yf.Ticker(f'{t}.TW').history(period='3mo',
                                                    auto_adjust=True)
                if len(df) < 60: continue
                ema60 = df['Close'].ewm(span=60, adjust=False).mean().iloc[-1]
                cur_p = df['Close'].iloc[-1]
                if cur_p > ema60: above_ema60 += 1
                total += 1
            except Exception:
                continue
        if total < 10: return {'has_data': False}
        breadth_pct = above_ema60 / total * 100

        # 警戒級別
        if twii_60d_chg > 5 and breadth_pct < 40:
            level = 'red'
            msg = (f'⚠️ 市場廣度警示：大盤 60 日 +{twii_60d_chg:.1f}% '
                   f'但僅 {breadth_pct:.0f}% 權值股站上 EMA60。'
                   f'指數由少數股拉動、多數股盤整下跌——v8 系統可能結構性失效（2024 範式）')
        elif twii_60d_chg > 5 and breadth_pct < 60:
            level = 'yellow'
            msg = (f'⚠️ 市場廣度偏窄：大盤 60 日 +{twii_60d_chg:.1f}% '
                   f'但僅 {breadth_pct:.0f}% 權值股站上 EMA60，主升段集中於少數股')
        elif twii_60d_chg < -5 and breadth_pct < 40:
            level = 'red_bear'
            msg = (f'⚠️ 全面空頭：大盤 60 日 {twii_60d_chg:+.1f}% + '
                   f'僅 {breadth_pct:.0f}% 權值股站上 EMA60')
        else:
            level = 'green'
            msg = (f'✓ 市場廣度健康：大盤 60 日 {twii_60d_chg:+.1f}%、'
                   f'{breadth_pct:.0f}% 權值股站上 EMA60')
        return {
            'has_data': True,
            'twii_60d_chg': twii_60d_chg,
            'breadth_pct': breadth_pct,
            'level': level, 'msg': msg,
        }
    except Exception:
        return {'has_data': False}


@st.cache_data(ttl=86400, show_spinner=False)
def _get_tw_names() -> dict:
    """台股中文名稱字典：合併 twstock + 靜態 tw_universe.txt（雲端可靠）"""
    out = {}
    # 1. 靜態檔（雲端首選，本地 fallback）
    try:
        from pathlib import Path as _P
        f = _P(__file__).parent / 'tw_universe.txt'
        if f.exists():
            for line in f.read_text(encoding='utf-8').splitlines():
                if not line or line.startswith('#'): continue
                parts = line.split('|')
                if len(parts) >= 2:
                    out[parts[0].strip()] = parts[1].strip()
    except Exception:
        pass
    # 2. twstock 動態（本地有，會覆蓋為最新）
    try:
        import twstock
        for code, info in twstock.codes.items():
            out[str(code)] = info.name
    except Exception:
        pass
    return out

# ── 概念股標籤 ─────────────────────────────────────────────────
# 美股 GICS sector 中文對照
_US_SECTOR_ZH = {
    "Information Technology": "資訊科技",
    "Financials": "金融",
    "Health Care": "醫療保健",
    "Consumer Discretionary": "非必需消費",
    "Industrials": "工業",
    "Communication Services": "通訊服務",
    "Consumer Staples": "必需消費",
    "Energy": "能源",
    "Utilities": "公用事業",
    "Real Estate": "不動產",
    "Materials": "原材料",
    "ETF": "ETF",
}

@st.cache_data(ttl=86400, show_spinner=False)
def _load_concept_tags() -> dict:
    """主題概念股對照（純 concept_tags.json，不含產業）：
    AI / 矽光子 / CoWoS / HBM / 散熱 / 軍工 / 蘋果 …"""
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
    """取得主題概念股標籤（純 concept_tags.json，不含產業類別，最多 max_n 個）"""
    return _load_concept_tags().get(ticker, [])[:max_n]

@st.cache_data(ttl=86400, show_spinner=False)
def _load_industry_map() -> dict:
    """單一字串產業類別（簡短）：
    台股：tw_universe.txt 第 5 欄；ETF/ETN/特別股顯示類型
    美股：us_sectors.txt sector（中譯）"""
    from pathlib import Path as _P
    base = _P(__file__).parent
    out = {}
    p2 = base / 'tw_universe.txt'
    if p2.exists():
        try:
            for line in p2.read_text(encoding='utf-8').splitlines():
                if not line or line.startswith('#'): continue
                parts = line.split('|')
                if len(parts) < 5: continue
                ticker, _n, _typ, _mkt, industry = parts[:5]
                if industry:
                    out[ticker] = industry
                elif _typ in ('ETF','ETN','特別股','臺灣存託憑證(TDR)'):
                    out[ticker] = ('TDR' if _typ=='臺灣存託憑證(TDR)'
                                   else _typ)
        except Exception:
            pass
    p3 = base / 'us_sectors.txt'
    if p3.exists():
        try:
            for line in p3.read_text(encoding='utf-8').splitlines():
                if not line or line.startswith('#'): continue
                parts = line.split('|')
                if len(parts) < 4: continue
                ticker, _name, sector, _sub = parts[:4]
                if sector and sector != 'NO_SECTOR':
                    out[ticker] = _US_SECTOR_ZH.get(sector, sector)
        except Exception:
            pass
    return out

def _get_industry(ticker: str) -> str:
    return _load_industry_map().get(ticker, "")

# 概念顏色配對（依首字 hash 對應顏色，視覺穩定）
_CONCEPT_COLORS = [
    "#3b9eff", "#ff6dc8", "#9d6dff", "#10c0c0", "#3dbb6a",
    "#f0a030", "#ff5555", "#7abadd", "#c0a060", "#a060ff",
]
def _concept_chip_html(c: str) -> str:
    color = _CONCEPT_COLORS[hash(c) % len(_CONCEPT_COLORS)]
    return (f'<span style="background:{color}22;color:{color};'
            f'border:1px solid {color}66;border-radius:10px;'
            f'padding:1px 7px;margin:1px 3px 1px 0;font-size:.66rem;'
            f'white-space:nowrap;display:inline-block">{c}</span>')

# 常用美股/ETF 靜態名稱對照（避免 get_info() 在雲端失敗）
US_NAMES = {
    "BOTZ": "Global X Robotics & AI ETF",
    "NVDA": "NVIDIA Corporation",
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "TSLA": "Tesla Inc.",
    "AMZN": "Amazon.com Inc.",
    "GOOGL": "Alphabet Inc.",
    "META": "Meta Platforms Inc.",
    "SPY": "SPDR S&P 500 ETF",
    "QQQ": "Invesco QQQ Trust",
    "ARKK": "ARK Innovation ETF",
    "GLD": "SPDR Gold Shares",
    "TLT": "iShares 20+ Year Treasury Bond ETF",
}

@st.cache_data(ttl=86400, show_spinner=False)
def _get_us_names_static() -> dict:
    """從 us_names.txt 載入靜態名稱對照（~8000 檔）"""
    out = {}
    try:
        from pathlib import Path as _P
        f = _P(__file__).parent / 'us_names.txt'
        if f.exists():
            for line in f.read_text(encoding='utf-8').splitlines():
                if not line or line.startswith('#'): continue
                parts = line.split('|', 1)
                if len(parts) >= 2:
                    out[parts[0].strip()] = parts[1].strip()
    except Exception:
        pass
    return out

def _get_stock_name(ticker: str, symbol: str) -> str:
    """取得股票中文/英文名稱"""
    try:
        # 1. 指數/特殊代號靜態對照
        if symbol in INDEX_NAMES:
            return INDEX_NAMES[symbol]
        # 2. 台股 twstock + 靜態 tw_universe.txt
        if is_tw_stock(ticker):
            tw_names = _get_tw_names()
            return tw_names.get(ticker, ticker)
        # 3. 常用美股靜態對照（內建 13 檔 + us_names.txt 8000+ 檔）
        if ticker in US_NAMES:
            return US_NAMES[ticker]
        us_static = _get_us_names_static()
        if ticker in us_static:
            return us_static[ticker]
        # 4. yfinance 動態查詢（最後 fallback）
        try:
            t = yf.Ticker(symbol)
            info = t.get_info()
            return info.get("longName") or info.get("shortName") or ticker
        except Exception:
            return ticker
    except Exception:
        return ticker

def hull_ma(series: pd.Series, n: int = 9) -> pd.Series:
    """Hull Moving Average"""
    half = max(n // 2, 1)
    w1 = series.rolling(half).apply(
        lambda x: np.average(x, weights=range(1, len(x)+1)), raw=True)
    w2 = series.rolling(n).apply(
        lambda x: np.average(x, weights=range(1, len(x)+1)), raw=True)
    diff = 2 * w1 - w2
    sqn = max(int(np.sqrt(n)), 1)
    return diff.rolling(sqn).apply(
        lambda x: np.average(x, weights=range(1, len(x)+1)), raw=True)

def vwma(close: pd.Series, volume: pd.Series, n: int = 20) -> pd.Series:
    """Volume Weighted Moving Average"""
    return (close * volume).rolling(n).sum() / volume.rolling(n).sum()


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_news(ticker: str, market: str) -> list:
    """取得最新 3 條新聞：台股用 Google News RSS，美股用 yfinance"""
    if is_tw_stock(ticker):
        return _fetch_news_google(ticker, market)
    try:
        symbol = get_yf_symbol(ticker)
        news = yf.Ticker(symbol).news or []
        result = []
        for item in news[:3]:
            content = item.get("content", {})
            title   = content.get("title") or item.get("title", "")
            link    = (content.get("canonicalUrl", {}).get("url")
                       or content.get("clickThroughUrl", {}).get("url")
                       or item.get("link", ""))
            pub     = (content.get("provider", {}).get("displayName")
                       or item.get("publisher", ""))
            if title and link:
                result.append({"title": title, "link": link, "publisher": pub})
        return result if result else _fetch_news_google(ticker, market)
    except Exception:
        return _fetch_news_google(ticker, market)


# ── 🆕 v3 金融領域情感系統（純規則，移除 SnowNLP）─────────────────
# 設計原則：
#   1. SnowNLP 訓練資料是電商評論，對金融標題誤判嚴重 → 完全移除
#   2. 純金融字典（正/負各 100+ 詞）
#   3. regex 數字強度 + 詞彙否定處理
#   4. 短語優先於單字（避免「成長」匹配到「衰退成長率」）

import re as _re

# ─── 強烈正面（每個 +0.30 ~ +0.40）─────────────────
_FIN_POS_STRONG = {
    # 價格行為
    '大漲': 0.35, '飆漲': 0.40, '飆升': 0.40, '飆破': 0.35, '創新高': 0.40,
    '創歷史新高': 0.45, '締造紀錄': 0.35, '長紅': 0.30, '攻頂': 0.30,
    # 業績/財報
    '業績亮眼': 0.40, '業績超標': 0.40, '業績爆發': 0.45, '業績大躍進': 0.45,
    '獲利大增': 0.35, '獲利躍進': 0.40, '獲利暴衝': 0.40, '營收創新高': 0.40,
    '盈餘上修': 0.35, '財報優': 0.35, '財測上修': 0.35,
    # 訂單/需求
    '訂單滿載': 0.40, '訂單爆量': 0.40, '需求強勁': 0.35, '需求暢旺': 0.35,
    '出貨暢旺': 0.35, '搶到訂單': 0.35, '取得大單': 0.35, '簽下訂單': 0.30,
    # 商機/題材
    '爆發商機': 0.40, '商機爆發': 0.40, '商機湧現': 0.35, '全面引爆': 0.40,
    '激增': 0.35, '暴增': 0.35, '蜂擁': 0.30, '搶買': 0.30, '搶購': 0.30,
}

# ─── 中等正面（每個 +0.15 ~ +0.25）─────────────────
_FIN_POS_MEDIUM = {
    # 價格
    '上漲': 0.18, '收漲': 0.18, '勁揚': 0.25, '走揚': 0.20, '反彈': 0.15,
    '回升': 0.18, '止跌': 0.18, '突破': 0.20, '攻上': 0.22, '直衝': 0.22,
    '創高': 0.25, '改寫紀錄': 0.25, '達陣': 0.20, '達標': 0.20,
    # 評等
    '看好': 0.25, '看多': 0.25, '加碼': 0.22, '增持': 0.22, '買進': 0.20,
    '推薦': 0.18, '優於大盤': 0.22, 'Outperform': 0.22, '持續看好': 0.25,
    # 利多
    '利多': 0.25, '利好': 0.25, '受惠': 0.20, '受惠商機': 0.30, '受惠政策': 0.20,
    '搶單': 0.25, '搶先': 0.18, '卡位': 0.18, '搶占': 0.20, '搶攻': 0.20,
    # 業績
    '亮眼': 0.22, '亮麗': 0.22, '佳音': 0.20, '報喜': 0.22, '看漲': 0.22,
    '獲利成長': 0.22, '營收成長': 0.22, '上修': 0.22, '調升': 0.20,
    '營收年增': 0.22, '展望樂觀': 0.20,
    # 法人
    '法人加碼': 0.22, '法人買超': 0.20, '外資買超': 0.22, '外資加碼': 0.22,
    '投信買超': 0.18, '主力買超': 0.18,
    # 訂單能見度
    '訂單能見度': 0.20, '能見度高': 0.22, '產能滿載': 0.30,
    # 題材/趨勢
    '藍海': 0.25, '商機': 0.18, '爆發': 0.18, '引爆': 0.20,
    'AI 概念': 0.15, 'AI受惠': 0.22, '熱錢': 0.15, '吸金': 0.18,
}

# ─── 微弱正面（每個 +0.05 ~ +0.10）─────────────────
_FIN_POS_WEAK = {
    '高達': 0.12, '達到': 0.05, '創高': 0.10, '增': 0.05, '揚': 0.08,
    '年增率': 0.12, '年增': 0.12, '月增': 0.08, '季增': 0.10,
    '提升': 0.10, '改善': 0.12, '回神': 0.12, '熱絡': 0.12,
    '入列': 0.10, '入選': 0.10, '受邀': 0.10, '指名': 0.12, '點名': 0.10,
    '看好': 0.18, '受惠': 0.15, '搶手': 0.15, '搶卡': 0.15,
    '佳績': 0.18, '佳音': 0.18, '佳評': 0.15, '長期看好': 0.20,
}

# ─── 強烈負面（每個 -0.30 ~ -0.40）─────────────────
_FIN_NEG_STRONG = {
    # 價格行為
    '大跌': -0.35, '崩跌': -0.40, '崩盤': -0.45, '暴跌': -0.40, '重挫': -0.35,
    '跌停': -0.45, '跌停板': -0.45, '停板': -0.35, '創新低': -0.30,
    '探底': -0.25, '腰斬': -0.40,
    # 業績/財報
    '虧損': -0.35, '財報差': -0.35, '獲利衰退': -0.35, '獲利大減': -0.35,
    '營收下滑': -0.30, '營收衰退': -0.35, '盈餘下修': -0.35, '財測下修': -0.35,
    # 公司危機
    '違約': -0.40, '掏空': -0.45, '破產': -0.45, '下市': -0.45,
    '停牌': -0.40, '查封': -0.40, '裁員': -0.30, '縮編': -0.25,
    # 情緒
    '恐慌': -0.30, '崩潰': -0.35, '股災': -0.40, '股殤': -0.40,
    # 利空
    '利空': -0.30, '警訊': -0.25, '警告': -0.25, '危機': -0.30,
}

# ─── 中等負面（每個 -0.15 ~ -0.25）─────────────────
_FIN_NEG_MEDIUM = {
    # 價格
    '下跌': -0.18, '收跌': -0.18, '走低': -0.18, '挫低': -0.22, '修正': -0.15,
    '跌深': -0.22, '跌破': -0.20, '反轉': -0.18, '跌幅擴大': -0.22,
    # 評等
    '看壞': -0.25, '看空': -0.25, '減碼': -0.22, '降評': -0.22, '不推': -0.22,
    '賣超': -0.15, '出脫': -0.18, '劣於大盤': -0.22, 'Underperform': -0.22,
    # 業績
    '衰退': -0.25, '疲軟': -0.20, '低迷': -0.22, '陰霾': -0.20, '失利': -0.20,
    '營收年減': -0.22, '下修': -0.22, '調降': -0.20, '展望保守': -0.18,
    '展望黯淡': -0.25, '展望疲弱': -0.22,
    # 法人
    '法人賣超': -0.22, '法人撤': -0.20, '外資賣超': -0.22, '外資撤': -0.22,
    '主力賣超': -0.18,
    # 套牢
    '套牢': -0.22, '失血': -0.22, '出走': -0.18, '逃命': -0.20, '出清': -0.18,
    # 失守/警訊
    '失守': -0.20, '空頭': -0.22, '空襲': -0.22, '失靈': -0.20,
}

# ─── 微弱負面（每個 -0.05 ~ -0.10）─────────────────
_FIN_NEG_WEAK = {
    '減': -0.05, '降': -0.05, '少': -0.03, '弱': -0.08,
    '年減': -0.15, '月減': -0.10, '季減': -0.10, '月減率': -0.12,
    '滑落': -0.12, '下滑': -0.12, '收斂': -0.10,
    '盤整': -0.05, '震盪': -0.05, '觀望': -0.05,
    '持平': 0.00,
}

# 合併
_FIN_POS_KEYWORDS = {**_FIN_POS_STRONG, **_FIN_POS_MEDIUM, **_FIN_POS_WEAK}
_FIN_NEG_KEYWORDS = {**_FIN_NEG_STRONG, **_FIN_NEG_MEDIUM, **_FIN_NEG_WEAK}

# 否定詞（出現在關鍵字前 → 反向）
_NEGATION_WORDS = ['不', '未', '沒', '無', '非']

# ─── 數字模式（regex）─────────────────────────
_PCT_POS_PATTERNS = [
    (r'年增[率]?[\s高達]*(\d+(?:\.\d+)?)\s*%',     0.006),  # 年增率 146% → +0.50（封頂）
    (r'(?:月增|季增)[率]?(\d+(?:\.\d+)?)\s*%',     0.004),
    (r'營收[漲增上]\s*(\d+(?:\.\d+)?)\s*%',         0.005),
    (r'(?:大漲|飆升|飆漲|急漲|暴漲)\s*(\d+(?:\.\d+)?)\s*%', 0.012),
    (r'獲利(?:成長|大增)\s*(\d+(?:\.\d+)?)\s*[倍%]', 0.05),  # N 倍 → 強放大
]
_PCT_NEG_PATTERNS = [
    (r'(?:年減|月減|季減)[率]?(\d+(?:\.\d+)?)\s*%', -0.006),
    (r'營收[減衰退]\s*(\d+(?:\.\d+)?)\s*%',         -0.005),
    (r'(?:大跌|暴跌|崩跌|重挫|急跌)\s*(\d+(?:\.\d+)?)\s*%', -0.012),
    (r'獲利(?:衰退|減少)\s*(\d+(?:\.\d+)?)\s*%',    -0.006),
]


# 🆕 v9.9e：BERT 中文情感（IDEA-CCNL/Erlangshen-Roberta-110M-Sentiment）
# 與規則混合（規則 70% + BERT 30%），規則對金融術語更準
@st.cache_resource(show_spinner=False)
def _load_sentiment_bert():
    try:
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        import torch
        mid = 'IDEA-CCNL/Erlangshen-Roberta-110M-Sentiment'
        tok = AutoTokenizer.from_pretrained(mid)
        mdl = AutoModelForSequenceClassification.from_pretrained(mid)
        mdl.eval()
        return tok, mdl, torch
    except Exception:
        return None, None, None


def _bert_sentiment(title: str) -> float:
    """BERT 情感分數 -1 ~ +1。失敗 → 0"""
    if not title: return 0.0
    tok, mdl, torch = _load_sentiment_bert()
    if tok is None: return 0.0
    try:
        inputs = tok(title, return_tensors='pt', truncation=True, max_length=128)
        with torch.no_grad():
            logits = mdl(**inputs).logits
        probs = torch.softmax(logits, dim=-1)[0]
        return float(probs[1] - probs[0])  # pos - neg
    except Exception:
        return 0.0


def _score_news_title(title: str) -> float:
    """v9.9e 混合評分：規則 70% + BERT 30%（金融術語規則為主，通用情緒 BERT 補強）
    回傳 -1.0 ~ +1.0
    """
    if not title: return 0.0
    score = 0.0
    matched_pos = []
    matched_neg = []

    # 1. 正向關鍵字（按長度排序，優先匹配長詞，避免「商機」優先於「商機爆發」）
    sorted_pos = sorted(_FIN_POS_KEYWORDS.items(), key=lambda x: -len(x[0]))
    sorted_neg = sorted(_FIN_NEG_KEYWORDS.items(), key=lambda x: -len(x[0]))

    consumed_pos = title  # 移除已匹配段落，避免短詞重複命中
    for kw, w in sorted_pos:
        if kw in consumed_pos:
            # 否定處理：檢查 kw 前 2 字是否含否定詞
            idx = consumed_pos.find(kw)
            prefix = consumed_pos[max(0, idx-2):idx]
            if any(neg in prefix for neg in _NEGATION_WORDS):
                # 否定 → 反向 50%
                score -= w * 0.5
                matched_neg.append((f'不{kw}', -w*0.5))
            else:
                score += w
                matched_pos.append((kw, w))
            consumed_pos = consumed_pos.replace(kw, '_'*len(kw), 1)

    consumed_neg = title
    for kw, w in sorted_neg:
        if kw in consumed_neg:
            idx = consumed_neg.find(kw)
            prefix = consumed_neg[max(0, idx-2):idx]
            if any(neg in prefix for neg in _NEGATION_WORDS):
                score -= w * 0.5  # 不跌 → 正面
                matched_pos.append((f'不{kw}', -w*0.5))
            else:
                score += w
                matched_neg.append((kw, w))
            consumed_neg = consumed_neg.replace(kw, '_'*len(kw), 1)

    # 2. 數字模式
    for pat, mul in _PCT_POS_PATTERNS:
        for m in _re.finditer(pat, title):
            try:
                pct = float(m.group(1))
                score += min(0.5, pct * mul)
            except Exception: pass
    for pat, mul in _PCT_NEG_PATTERNS:
        for m in _re.finditer(pat, title):
            try:
                pct = float(m.group(1))
                score += max(-0.5, pct * mul)
            except Exception: pass

    # 3. BERT 補強（規則 70% + BERT 30%）
    bert_s = _bert_sentiment(title)
    rule_score = score
    if matched_pos or matched_neg:
        # 有規則命中 → 規則為主，BERT 微調
        score = rule_score * 0.7 + bert_s * 0.3
    else:
        # 規則無命中 → 完全靠 BERT（縮小至 ±0.5 避免過度極端）
        score = bert_s * 0.5

    return max(-1.0, min(1.0, score))


@st.cache_data(ttl=1800)
def get_news_sentiment(ticker: str, market: str) -> dict:
    """為個股新聞算情感分數。
    回傳：{ avg_score: float, n: int, headlines: [(title, score, link), ...] }"""
    news = fetch_news(ticker, market)
    if not news:
        return {'avg_score': 0.0, 'n': 0, 'headlines': []}
    headlines = []
    for n in news:
        title = n.get('title', '')
        link = n.get('link', '')
        s = _score_news_title(title)
        headlines.append((title, s, link, n.get('publisher', '')))
    avg = sum(h[1] for h in headlines) / len(headlines)
    return {'avg_score': round(avg, 3), 'n': len(headlines), 'headlines': headlines}


def _fetch_news_google(ticker: str, market: str) -> list:
    """Google News RSS 備援（支援台股）"""
    try:
        import xml.etree.ElementTree as ET
        # 台股加上公司名稱一起搜尋，命中率更高
        name = ""
        if is_tw_stock(ticker):
            tw_names = _get_tw_names()
            name = tw_names.get(ticker, "")
        query = f"{ticker} {name}".strip() if name else ticker
        import urllib.parse
        url = (f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}"
               f"&hl=zh-TW&gl=TW&ceid=TW:zh-Hant")
        resp = requests.get(url, timeout=6,
                            headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return []
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        result = []
        for item in items[:3]:
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            pub   = (item.findtext("source") or "").strip()
            if title and link:
                result.append({"title": title, "link": link, "publisher": pub})
        return result
    except Exception:
        return []


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_indicators(ticker: str, market: str, end_date: str = ""):
    """end_date: 'YYYY-MM-DD' 指定截止日期（空字串=最新）；歷史日期快取永久有效"""
    symbol = get_yf_symbol(ticker)
    df = None
    last_err = None
    is_tw = symbol.endswith(".TW") or symbol.endswith(".TWO")

    # 計算 end 參數（yfinance 的 end 是「不包含」那天，所以要加 1 天）
    import datetime as _dt
    if end_date:
        _end_dt   = _dt.date.fromisoformat(end_date) + _dt.timedelta(days=1)
        _end_str  = _end_dt.isoformat()
        _start_dt = _dt.date.fromisoformat(end_date) - _dt.timedelta(days=730)
        _start_str = _start_dt.isoformat()
    else:
        _end_str = _start_str = None   # 使用 period 模式

    def _try_tw_download(sym):
        """嘗試下載台股資料，回傳 DataFrame 或 None
        修正：閾值 20 → 100。20 rows 對 indicator 計算太少（需 200+ 日暖機）
        且會誤觸 .TW（上市）剛好給 20 行卻沒觸發 .TWO fallback 的 bug。
        """
        MIN_ROWS = 100
        if _start_str:
            raw = yf.download(
                sym, start=_start_str, end=_end_str, interval="1d",
                progress=False, auto_adjust=False, multi_level_index=False,
            )
            if raw is not None and len(raw) >= MIN_ROWS:
                _c = raw.get("Close", raw.get("close", pd.Series()))
                if not _c.dropna().empty:
                    return raw
            return None
        for _period, _adj in [("2y", False), ("1y", False), ("2y", True), ("1y", True)]:
            raw = yf.download(
                sym, period=_period, interval="1d",
                progress=False, auto_adjust=_adj,
                multi_level_index=False,
            )
            if raw is not None and len(raw) >= MIN_ROWS:
                _c = raw.get("Close", raw.get("close", pd.Series()))
                if not _c.dropna().empty:
                    return raw
        return None

    for attempt in range(3):
        try:
            if is_tw:
                # 先試 .TW（上市），失敗再試 .TWO（上櫃）
                df = _try_tw_download(symbol)
                if df is None and symbol.endswith(".TW"):
                    alt = ticker + ".TWO"
                    df = _try_tw_download(alt)
                    if df is not None:
                        symbol = alt  # 更新為正確代號
            else:
                if _start_str:
                    df = yf.Ticker(symbol).history(start=_start_str, end=_end_str, interval="1d")
                else:
                    df = yf.Ticker(symbol).history(period="1y", interval="1d")
                if df is not None and len(df) < 30:
                    df = None
            if df is not None and len(df) >= 30:
                break
            last_err = f"rows={len(df) if df is not None else 0}"
            df = None
        except Exception as e:
            last_err = str(e)[:120]
            df = None
        if df is None and attempt < 2:
            time.sleep(3 + attempt * 3)
    if df is None or len(df) < 20:
        rows = len(df) if df is not None else 0
        return {"_error": f"rows={rows}"}
    try:
        # 抓取股票名稱
        name = _get_stock_name(ticker, symbol)
        # 統一欄位名稱 (Ticker.history 和 yf.download 格式不同)
        # yf.download 可能有 MultiIndex (Price, Ticker) 或 (Price,) 格式
        if hasattr(df.columns, 'levels'):
            # MultiIndex - 取第一層
            df.columns = [str(col[0]).strip().capitalize()
                          if isinstance(col, tuple) else str(col).strip().capitalize()
                          for col in df.columns]
        else:
            df.columns = [str(col).strip().capitalize() for col in df.columns]
        # 確認必要欄位存在
        col_map = {c.lower(): c for c in df.columns}
        def get_col(name):
            return df[col_map[name]] if name in col_map else pd.Series(dtype=float)
        c = get_col("close")
        h = get_col("high")
        l = get_col("low")
        v = get_col("volume")
        if c.dropna().empty:
            return {"_error": "close all NaN"}

        def last(s):
            # 取最後一個非 NaN 值（避免最新行有 NaN 的問題）
            s_clean = s.dropna()
            if s_clean.empty:
                return None
            return float(s_clean.iloc[-1])

        bb      = ta.volatility.BollingerBands(c, 20, 2)
        ema13   = ta.trend.EMAIndicator(c, 13).ema_indicator()
        ichi    = ta.trend.IchimokuIndicator(h, l, 9, 26, 52)
        ema5_s  = ta.trend.EMAIndicator(c, 5).ema_indicator()
        ema20_s = ta.trend.EMAIndicator(c, 20).ema_indicator()
        ema60_s = ta.trend.EMAIndicator(c, 60).ema_indicator()
        sma200_s= ta.trend.SMAIndicator(c, 200).sma_indicator()
        adx_obj = ta.trend.ADXIndicator(h, l, c, 14)
        adx_s   = adx_obj.adx()
        macd_obj    = ta.trend.MACD(c)
        macd_hist_s = macd_obj.macd_diff()
        atr_s   = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
        rsi_s   = ta.momentum.RSIIndicator(c, 14).rsi()
        vol_ma20_s  = ta.trend.SMAIndicator(v, 20).sma_indicator() if not v.dropna().empty else pd.Series(dtype=float)

        # 動量: 取當前值與前一期方向比較（TV邏輯）
        stoch_obj    = ta.momentum.StochasticOscillator(h, l, c, 14, 3)
        stoch_k_s    = stoch_obj.stoch()
        stoch_d_s    = stoch_obj.stoch_signal()   # %D，TV用此判斷（crossover邏輯）

        ao_series    = ta.momentum.AwesomeOscillatorIndicator(h, l).awesome_oscillator()
        mom_series   = c - c.shift(10)

        stochrsi_obj = ta.momentum.StochRSIIndicator(c, 14, 3, 3)
        stochrsi_d_s = stochrsi_obj.stochrsi_d() * 100  # %D×100

        willr_s      = ta.momentum.WilliamsRIndicator(h, l, c, 14).williams_r()
        bbpower_s    = c - ema13

        def prev(s, n=1):
            idx = -(n+1)
            return float(s.iloc[idx]) if len(s) >= abs(idx) and pd.notna(s.iloc[idx]) else None

        # 🆕 T3 信心度：5 個指標加總（每命中 1 分）
        def _t3_confidence(close_now, ema5_now, ema20_now,
                           ema5_5d_ago, ema20_5d_ago):
            score = 0
            hits = []
            try:
                # C1: close > EMA20
                if close_now is not None and ema20_now is not None and close_now > ema20_now:
                    score += 1; hits.append('close>EMA20')
                # C3: EMA20 5d 斜率為正
                if ema20_now is not None and ema20_5d_ago is not None and ema20_now > ema20_5d_ago:
                    score += 1; hits.append('EMA20上升')
                # E5: EMA5 5d 斜率為正
                e5_up = (ema5_now is not None and ema5_5d_ago is not None and ema5_now > ema5_5d_ago)
                if e5_up:
                    score += 1; hits.append('EMA5上升')
                # E5>E20: 多頭排列
                if ema5_now is not None and ema20_now is not None and ema5_now > ema20_now:
                    score += 1; hits.append('EMA5>EMA20')
                # 雙均線都升
                e20_up = (ema20_now is not None and ema20_5d_ago is not None
                          and ema20_now > ema20_5d_ago)
                if e5_up and e20_up:
                    score += 1; hits.append('雙均線都升')
            except Exception:
                pass
            return score, hits

        # 🆕 T3 拉回天數：RSI 連續低於 50 的天數
        def _t3_pullback_days(rsi_series):
            try:
                arr = rsi_series.dropna().values
                if len(arr) == 0 or arr[-1] >= 50: return 0
                # 倒數，看連續 < 50 多少天
                cnt = 0
                for v in reversed(arr):
                    if v < 50: cnt += 1
                    else: break
                return cnt if cnt > 0 else 0
            except Exception:
                return 0

        # 🆕 T4 反彈天數：RSI < 32 且連續上升的天數
        def _t4_rising_days(rsi_series):
            try:
                arr = rsi_series.dropna().values
                if len(arr) < 3 or arr[-1] >= 32: return 0
                # 從最後一天往前數連續上升
                cnt = 1  # 包含當天
                for i in range(len(arr) - 1, 0, -1):
                    if arr[i] > arr[i-1] and arr[i] < 32:
                        cnt += 1
                    else:
                        break
                return cnt
            except Exception:
                return 0

        close_val = last(c)
        prev_close_val = prev(c)
        change_pct = ((close_val - prev_close_val) / prev_close_val * 100
                      if close_val and prev_close_val and prev_close_val != 0 else None)
        change_amt = (close_val - prev_close_val
                      if close_val is not None and prev_close_val is not None else None)

        # EMA20/60 黃金/死亡交叉距今天數（正=黃金交叉，負=死亡交叉）
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

        # 🆕 v9.11：T1 觸發至今的漲跌（cross day → today）
        cross_day_close = None
        cross_change_pct = None
        if ema20_cross_days is not None:
            try:
                _k = abs(ema20_cross_days)
                if _k < len(c):
                    cross_day_close = float(c.iloc[-_k - 1])  # cross 發生那天的收盤
                    if cross_day_close and cross_day_close > 0 and close_val is not None:
                        cross_change_pct = (close_val - cross_day_close) / cross_day_close * 100
            except Exception:
                pass

        # 週線結構（日線 resample，無需額外 API）
        w_close_v = w_ma10_v = w_ma20_v = None
        try:
            c_tz = c.copy()
            if hasattr(c_tz.index, 'tz') and c_tz.index.tz is not None:
                c_tz.index = c_tz.index.tz_localize(None)
            wc = c_tz.resample('W').last().dropna()
            if len(wc) >= 20:
                w_close_v = float(wc.iloc[-1])
                w_ma10_v  = last(ta.trend.SMAIndicator(wc, 10).sma_indicator())
                w_ma20_v  = last(ta.trend.SMAIndicator(wc, 20).sma_indicator())
        except Exception:
            pass

        d = {
            "name":         name,
            "close":        close_val,
            "prev_close":   prev_close_val,
            "change_pct":   change_pct,
            "change_amt":   change_amt,
            "rsi":          last(rsi_s),
            "rsi_prev":     prev(rsi_s),
            "rsi_prev2":    prev(rsi_s, 2),             # T4連續2天上升判斷用
            # 🆕 T3/T4 天數計算
            "t3_pullback_days": _t3_pullback_days(rsi_s),  # RSI<50 連續多少天
            "t4_rising_days":   _t4_rising_days(rsi_s),    # RSI<32 且上升多少天
            # 🆕 EMA5 + T3 信心度（v9.9t）
            "ema5":         last(ema5_s),
            "ema5_5d_ago":  prev(ema5_s, 5),
            "ema20_5d_ago": prev(ema20_s, 5),
            "atr14":        last(atr_s),
            "stoch_k":      last(stoch_k_s),
            "stoch_d":      last(stoch_d_s),
            "stoch_d_prev": prev(stoch_d_s),        # crossover判斷用
            "cci":          last(ta.trend.CCIIndicator(h, l, c, 20).cci()),
            "adx":          last(adx_s),
            "adx_prev":     prev(adx_s),
            "adx_pos":      last(adx_obj.adx_pos()),
            "adx_neg":      last(adx_obj.adx_neg()),
            "macd_hist":      last(macd_hist_s),
            "macd_hist_prev": prev(macd_hist_s),
            "volume":       last(v),
            "vol_ma20":     last(vol_ma20_s) if not vol_ma20_s.empty else None,
            "ao":           last(ao_series),
            "ao_prev":      prev(ao_series),
            "ao_prev2":     prev(ao_series, 2),
            "mom":          last(mom_series),
            "mom_prev":     prev(mom_series),
            "macd":         last(macd_obj.macd()),
            "stochrsi":     last(stochrsi_d_s),
            "stochrsi_prev":prev(stochrsi_d_s),     # crossover判斷用
            "willr":        last(willr_s),
            "willr_prev":   prev(willr_s),           # crossover判斷用
            "bbpower":      last(bbpower_s),
            "bbpower_prev": prev(bbpower_s),
            "uo":           last(ta.momentum.UltimateOscillator(h, l, c, 7, 14, 28).ultimate_oscillator()),
            "bbu":      last(bb.bollinger_hband()),
            "bbl":      last(bb.bollinger_lband()),
            "ema10":    last(ta.trend.EMAIndicator(c, 10).ema_indicator()),
            "sma10":    last(ta.trend.SMAIndicator(c, 10).sma_indicator()),
            "ema20":      last(ema20_s),
            "ema20_prev": prev(ema20_s),
            "sma20":    last(ta.trend.SMAIndicator(c, 20).sma_indicator()),
            "ema30":    last(ta.trend.EMAIndicator(c, 30).ema_indicator()),
            "sma30":    last(ta.trend.SMAIndicator(c, 30).sma_indicator()),
            "ema50":    last(ta.trend.EMAIndicator(c, 50).ema_indicator()),
            "sma50":    last(ta.trend.SMAIndicator(c, 50).sma_indicator()),
            "ema60":      last(ema60_s),
            "ema60_prev": prev(ema60_s),
            "ema20_cross_days": ema20_cross_days,
            "cross_day_close":  cross_day_close,    # 🆕 v9.11：cross 那天的 close
            "cross_change_pct": cross_change_pct,   # 🆕 v9.11：cross 至今 % 變化
            "w_close":  w_close_v,
            "w_ma10":   w_ma10_v,
            "w_ma20":   w_ma20_v,
            "w_dev":    ((w_close_v - w_ma10_v) / w_ma10_v * 100
                         if w_close_v and w_ma10_v and w_ma10_v != 0 else None),
            "sma60":    last(ta.trend.SMAIndicator(c, 60).sma_indicator()),
            "ema100":   last(ta.trend.EMAIndicator(c, 100).ema_indicator()),
            "sma100":   last(ta.trend.SMAIndicator(c, 100).sma_indicator()),
            "ema200":      last(ta.trend.EMAIndicator(c, 200).ema_indicator()),
            "sma200":      last(sma200_s),
            "sma200_prev": prev(sma200_s),
            "ichimoku": last(ichi.ichimoku_base_line()),
            "vwma":     last(vwma(c, v, 20)),
            "hma":      last(hull_ma(c, 9)),
            # 60 日高點（接刀風險警告用）
            "high60":   float(h.iloc[-60:].max()) if len(h) >= 60 else None,
            # 🆕 v9.10x：60 日低點（底部反彈訊號用）
            "low60":    float(l.iloc[-60:].min()) if len(l) >= 60 else None,
        }

        # K 線型態偵測（最近 5 個交易日內出現的型態）
        try:
            from kline_patterns import detect_recent
            o = get_col("open")
            atr_s = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
            kdf = pd.DataFrame({
                'Open': o, 'High': h, 'Low': l, 'Close': c, 'atr': atr_s
            })
            kdf.index = c.index
            d['kline_patterns'] = detect_recent(kdf, lookback=5)
        except Exception:
            d['kline_patterns'] = []

        # 🆕 VWAP 今日值（盤中執行優化建議）— 僅台股
        try:
            if is_tw:
                from fugle_connector import get_minute_candles
                from vwap_loader import compute_daily_vwap
                # 抓近 1 天 5m 資料（即時值）
                tk = ticker.replace('.TW', '').replace('.TWO', '')
                m_df = get_minute_candles(tk, freq='5m', use_cache=False)
                if m_df is not None and not m_df.empty:
                    # 只取最後一天
                    last_date = m_df.index.normalize().max()
                    last_day = m_df[m_df.index >= last_date - pd.Timedelta(hours=8)]
                    if len(last_day) >= 3:
                        vw_df = compute_daily_vwap(last_day)
                        if vw_df is not None and not vw_df.empty:
                            d['vwap_today'] = float(vw_df['VWAP'].iloc[-1])
        except Exception:
            pass

        # 🆕 EPS / PER / PBR / 殖利率（優先 FinMind per_cache，否則 yfinance fallback）
        try:
            from pathlib import Path as _P
            tk = ticker.replace('.TW', '').replace('.TWO', '').replace('.', '')
            pe_path = _P(__file__).parent / 'per_cache' / f'{tk}.parquet'
            got_from_cache = False
            if is_tw and pe_path.exists():
                pe_df = pd.read_parquet(pe_path)
                if not pe_df.empty:
                    last_row = pe_df.iloc[-1]
                    per_v = last_row.get('PER')
                    pbr_v = last_row.get('PBR')
                    div_v = last_row.get('dividend_yield')
                    # FinMind PER=0 表示虧損（EPS ≤ 0）
                    if per_v is not None and not pd.isna(per_v):
                        if per_v > 0:
                            d['per'] = float(per_v); got_from_cache = True
                            d['per_kind'] = 'TTM'
                        else:
                            # PER=0 → 虧損
                            d['per_kind'] = 'LOSS'
                            got_from_cache = True   # 不再 fallback yfinance
                    if pbr_v and not pd.isna(pbr_v):
                        d['pbr'] = float(pbr_v)
                    if div_v and not pd.isna(div_v):
                        d['div_yield'] = float(div_v)
                    if per_v and not pd.isna(per_v) and per_v > 0:
                        close_v = d.get('close')
                        if close_v:
                            d['eps_ttm'] = round(close_v / per_v, 2)
                    # PER 60 日動量
                    if len(pe_df) >= 60:
                        pe_60d = pe_df['PER'].iloc[-60]
                        if pe_60d and per_v and not pd.isna(pe_60d) and not pd.isna(per_v) and pe_60d > 0:
                            d['per_60d_chg_pct'] = round((per_v - pe_60d) / pe_60d * 100, 1)
                    # PER 相對 90 日中位
                    if len(pe_df) >= 90:
                        pe_med90_arr = pe_df['PER'].iloc[-90:].dropna()
                        if len(pe_med90_arr) > 0:
                            pe_med90 = float(pe_med90_arr.median())
                            if pe_med90 and per_v:
                                d['per_vs_med90'] = round((per_v - pe_med90) / pe_med90 * 100, 1)

            # 🆕 券資比（margin_cache）— 僅台股
            if is_tw:
                ms_path = _P(__file__).parent / 'margin_cache' / f'{tk}.parquet'
                if ms_path.exists():
                    ms_df = pd.read_parquet(ms_path)
                    if not ms_df.empty:
                        last = ms_df.iloc[-1]
                        ms_now = last.get('msratio')
                        if ms_now is not None and not pd.isna(ms_now):
                            d['msratio'] = round(float(ms_now), 2)
                        if 'margin_balance' in ms_df.columns:
                            mb = last.get('margin_balance')
                            if mb is not None and not pd.isna(mb):
                                d['margin_balance'] = int(mb)
                        if 'short_balance' in ms_df.columns:
                            sb = last.get('short_balance')
                            if sb is not None and not pd.isna(sb):
                                d['short_balance'] = int(sb)
                        # 60 日動量
                        if len(ms_df) >= 60:
                            ms_60d = ms_df['msratio'].iloc[-60]
                            if ms_60d and ms_now and not pd.isna(ms_60d) and not pd.isna(ms_now) and ms_60d > 0:
                                d['msratio_60d_chg_pct'] = round((ms_now - ms_60d) / ms_60d * 100, 1)

            # 🆕 yfinance fallback（雲端 / 美股 / per_cache 缺檔時）
            if not got_from_cache:
                try:
                    info = yf.Ticker(symbol).info
                    # 優先 trailingPE，無則 forwardPE（標 F 區分）
                    per_yf = info.get('trailingPE')
                    if per_yf and not pd.isna(per_yf) and 0 < per_yf < 1000:
                        d['per'] = float(per_yf)
                        d['per_kind'] = 'TTM'
                    else:
                        fwd_pe = info.get('forwardPE')
                        if fwd_pe and not pd.isna(fwd_pe) and 0 < fwd_pe < 1000:
                            d['per'] = float(fwd_pe)
                            d['per_kind'] = 'FWD'
                        else:
                            # 真正虧損 → 標示 EPS<0
                            eps_t = info.get('trailingEps')
                            if eps_t is not None and not pd.isna(eps_t) and eps_t < 0:
                                d['per_kind'] = 'LOSS'
                    eps_yf = info.get('trailingEps')
                    if eps_yf is not None and not pd.isna(eps_yf):
                        d['eps_ttm'] = round(float(eps_yf), 2)
                    pbr_yf = info.get('priceToBook')
                    if pbr_yf and not pd.isna(pbr_yf) and 0 < pbr_yf < 100:
                        d['pbr'] = round(float(pbr_yf), 2)
                    div_yf = info.get('trailingAnnualDividendYield') or info.get('dividendYield')
                    if div_yf and not pd.isna(div_yf):
                        dv = float(div_yf)
                        if dv < 1.0:
                            d['div_yield'] = round(dv * 100, 2)
                        else:
                            d['div_yield'] = round(dv, 2)
                except Exception:
                    pass
        except Exception:
            pass

        # 🆕 v9.9t：T3 信心度計算
        try:
            score, hits = _t3_confidence(
                d.get('close'), d.get('ema5'), d.get('ema20'),
                d.get('ema5_5d_ago'), d.get('ema20_5d_ago'))
            d['t3_confidence'] = score
            d['t3_confidence_hits'] = hits
        except Exception:
            d['t3_confidence'] = 0
            d['t3_confidence_hits'] = []

        return d
    except Exception as e:
        return {"_error": str(e)[:120]}


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_indicators_range(ticker: str, market: str, start_date: str, end_date: str):
    """下載一次資料，計算全部 TA 序列後，回傳 [start_date, end_date] 內每個交易日的指標字典。
    返回：list of (date_str, d_dict)，依日期升冪排列。
    """
    import datetime as _dt
    symbol = get_yf_symbol(ticker)
    is_tw  = symbol.endswith(".TW") or symbol.endswith(".TWO")

    # 往前多拉 400 天讓 SMA200 等長周期指標穩定
    _end_dt   = _dt.date.fromisoformat(end_date)   + _dt.timedelta(days=1)
    _start_dt = _dt.date.fromisoformat(start_date) - _dt.timedelta(days=400)
    _end_str, _start_str = _end_dt.isoformat(), _start_dt.isoformat()

    df = None
    for attempt in range(3):
        try:
            if is_tw:
                raw = yf.download(symbol, start=_start_str, end=_end_str,
                                  interval="1d", progress=False,
                                  auto_adjust=False, multi_level_index=False)
                if raw is None or len(raw) < 30:
                    alt  = ticker + ".TWO"
                    raw2 = yf.download(alt, start=_start_str, end=_end_str,
                                       interval="1d", progress=False,
                                       auto_adjust=False, multi_level_index=False)
                    if raw2 is not None and len(raw2) > (len(raw) if raw is not None else 0):
                        raw, symbol = raw2, alt
                df = raw if raw is not None and len(raw) >= 30 else None
            else:
                df = yf.Ticker(symbol).history(start=_start_str, end=_end_str, interval="1d")
                if df is not None and len(df) < 30:
                    df = None
            if df is not None and len(df) >= 30:
                break
        except Exception:
            df = None
        if df is None and attempt < 2:
            time.sleep(3 + attempt * 3)

    if df is None or len(df) < 30:
        return []

    try:
        if hasattr(df.columns, 'levels'):
            df.columns = [str(col[0]).strip().capitalize()
                          if isinstance(col, tuple) else str(col).strip().capitalize()
                          for col in df.columns]
        else:
            df.columns = [str(col).strip().capitalize() for col in df.columns]
        col_map = {cn.lower(): cn for cn in df.columns}
        def _gc(n):
            return df[col_map[n]] if n in col_map else pd.Series(dtype=float, index=df.index)
        c  = _gc("close"); h = _gc("high"); l = _gc("low"); v = _gc("volume")
        if c.dropna().empty:
            return []

        stock_name = _get_stock_name(ticker, symbol)

        # ── 計算所有 TA 序列（一次性） ──────────────────────────────
        bb       = ta.volatility.BollingerBands(c, 20, 2)
        ema13_s  = ta.trend.EMAIndicator(c, 13).ema_indicator()
        ema10_s  = ta.trend.EMAIndicator(c, 10).ema_indicator()
        sma10_s  = ta.trend.SMAIndicator(c, 10).sma_indicator()
        ema20_s  = ta.trend.EMAIndicator(c, 20).ema_indicator()
        sma20_s  = ta.trend.SMAIndicator(c, 20).sma_indicator()
        ema30_s  = ta.trend.EMAIndicator(c, 30).ema_indicator()
        sma30_s  = ta.trend.SMAIndicator(c, 30).sma_indicator()
        ema50_s  = ta.trend.EMAIndicator(c, 50).ema_indicator()
        sma50_s  = ta.trend.SMAIndicator(c, 50).sma_indicator()
        ema60_s  = ta.trend.EMAIndicator(c, 60).ema_indicator()
        sma60_s  = ta.trend.SMAIndicator(c, 60).sma_indicator()
        ema100_s = ta.trend.EMAIndicator(c, 100).ema_indicator()
        sma100_s = ta.trend.SMAIndicator(c, 100).sma_indicator()
        ema200_s = ta.trend.EMAIndicator(c, 200).ema_indicator()
        sma200_s = ta.trend.SMAIndicator(c, 200).sma_indicator()
        adx_obj  = ta.trend.ADXIndicator(h, l, c, 14)
        adx_s    = adx_obj.adx()
        adxp_s   = adx_obj.adx_pos()
        adxn_s   = adx_obj.adx_neg()
        macd_obj     = ta.trend.MACD(c)
        macd_s       = macd_obj.macd()
        macd_hist_s  = macd_obj.macd_diff()
        rsi_s    = ta.momentum.RSIIndicator(c, 14).rsi()
        stoch_obj= ta.momentum.StochasticOscillator(h, l, c, 14, 3)
        stochk_s = stoch_obj.stoch()
        stochd_s = stoch_obj.stoch_signal()
        ao_s     = ta.momentum.AwesomeOscillatorIndicator(h, l).awesome_oscillator()
        mom_s    = c - c.shift(10)
        srsi_s   = ta.momentum.StochRSIIndicator(c, 14, 3, 3).stochrsi_d() * 100
        willr_s  = ta.momentum.WilliamsRIndicator(h, l, c, 14).williams_r()
        bbp_s    = c - ema13_s
        uo_s     = ta.momentum.UltimateOscillator(h, l, c, 7, 14, 28).ultimate_oscillator()
        cci_s    = ta.trend.CCIIndicator(h, l, c, 20).cci()
        ichi_s   = ta.trend.IchimokuIndicator(h, l, 9, 26, 52).ichimoku_base_line()
        bbu_s    = bb.bollinger_hband()
        bbl_s    = bb.bollinger_lband()
        vol_ma20_s = (ta.trend.SMAIndicator(v, 20).sma_indicator()
                      if not v.dropna().empty else pd.Series(dtype=float, index=c.index))
        vwma_s   = (vwma(c, v, 20) if not v.dropna().empty
                    else pd.Series(dtype=float, index=c.index))
        hma_s    = hull_ma(c, 9)
        diff_s   = ema20_s - ema60_s     # EMA20/60 差值，用於交叉偵測

        # 週線序列（日線 resample）
        wc_ser = wm10_ser = wm20_ser = None
        try:
            c_tz = c.copy()
            if hasattr(c_tz.index, 'tz') and c_tz.index.tz is not None:
                c_tz.index = c_tz.index.tz_localize(None)
            wc_raw = c_tz.resample('W').last().dropna()
            if len(wc_raw) >= 20:
                wc_ser   = wc_raw
                wm10_ser = ta.trend.SMAIndicator(wc_raw, 10).sma_indicator()
                wm20_ser = ta.trend.SMAIndicator(wc_raw, 20).sma_indicator()
        except Exception:
            pass

        # 取第 i 行純量
        def at(s, i):
            if s is None or i < 0 or i >= len(s): return None
            val = s.iloc[i]
            return float(val) if pd.notna(val) else None

        start_dt = _dt.date.fromisoformat(start_date)
        end_dt_  = _dt.date.fromisoformat(end_date)

        out = []
        for idx in range(len(c)):
            ts_idx = c.index[idx]
            row_date = ts_idx.date() if hasattr(ts_idx, 'date') else pd.Timestamp(ts_idx).date()
            if row_date < start_dt or row_date > end_dt_:
                continue
            if idx < 2:      # ao_prev2 需要 idx-2
                continue
            close_v = at(c, idx)
            if close_v is None:
                continue

            prev_close = at(c, idx-1)
            change_pct = ((close_v - prev_close) / prev_close * 100
                          if close_v and prev_close and prev_close != 0 else None)
            change_amt = (close_v - prev_close
                          if close_v is not None and prev_close is not None else None)

            # EMA20/60 交叉距今天數
            cross_days = None
            for _k in range(1, min(idx, 120)):
                d1 = at(diff_s, idx - _k + 1)
                d0 = at(diff_s, idx - _k)
                if d1 is not None and d0 is not None:
                    if d0 < 0 and d1 >= 0:
                        cross_days = _k;  break
                    elif d0 > 0 and d1 <= 0:
                        cross_days = -_k; break

            # 🆕 v9.11：T1 觸發至今的漲跌（cross day → 此 idx）
            cross_day_close = None
            cross_change_pct = None
            if cross_days is not None:
                _ki = abs(cross_days)
                if idx - _ki >= 0:
                    cross_day_close = at(c, idx - _ki)
                    if cross_day_close and cross_day_close > 0 and close_v is not None:
                        cross_change_pct = (close_v - cross_day_close) / cross_day_close * 100

            # 週線對應值
            w_close_v = w_ma10_v = w_ma20_v = None
            if wc_ser is not None and wm10_ser is not None:
                try:
                    row_ts_naive = pd.Timestamp(row_date)
                    widx = int(wc_ser.index.searchsorted(row_ts_naive, side='right')) - 1
                    if widx >= 0:
                        w_close_v = float(wc_ser.iloc[widx]) if pd.notna(wc_ser.iloc[widx]) else None
                        w_ma10_v  = at(wm10_ser, widx)
                        w_ma20_v  = at(wm20_ser, widx)
                except Exception:
                    pass

            d_dict = {
                "name":         stock_name,
                "close":        close_v,
                "prev_close":   prev_close,
                "change_pct":   change_pct,
                "change_amt":   change_amt,
                "rsi":          at(rsi_s, idx),
                "stoch_k":      at(stochk_s, idx),
                "stoch_d":      at(stochd_s, idx),
                "stoch_d_prev": at(stochd_s, idx-1),
                "cci":          at(cci_s, idx),
                "adx":          at(adx_s, idx),
                "adx_prev":     at(adx_s, idx-1),
                "adx_pos":      at(adxp_s, idx),
                "adx_neg":      at(adxn_s, idx),
                "macd_hist":    at(macd_hist_s, idx),
                "macd_hist_prev": at(macd_hist_s, idx-1),
                "volume":       at(v, idx),
                "vol_ma20":     at(vol_ma20_s, idx),
                "ao":           at(ao_s, idx),
                "ao_prev":      at(ao_s, idx-1),
                "ao_prev2":     at(ao_s, idx-2),
                "mom":          at(mom_s, idx),
                "mom_prev":     at(mom_s, idx-1),
                "macd":         at(macd_s, idx),
                "stochrsi":     at(srsi_s, idx),
                "stochrsi_prev": at(srsi_s, idx-1),
                "willr":        at(willr_s, idx),
                "willr_prev":   at(willr_s, idx-1),
                "bbpower":      at(bbp_s, idx),
                "bbpower_prev": at(bbp_s, idx-1),
                "uo":           at(uo_s, idx),
                "bbu":          at(bbu_s, idx),
                "bbl":          at(bbl_s, idx),
                "ema10":        at(ema10_s, idx),
                "sma10":        at(sma10_s, idx),
                "ema20":        at(ema20_s, idx),
                "ema20_prev":   at(ema20_s, idx-1),
                "sma20":        at(sma20_s, idx),
                "ema30":        at(ema30_s, idx),
                "sma30":        at(sma30_s, idx),
                "ema50":        at(ema50_s, idx),
                "sma50":        at(sma50_s, idx),
                "ema60":        at(ema60_s, idx),
                "ema60_prev":   at(ema60_s, idx-1),
                "ema20_cross_days": cross_days,
                "cross_day_close":  cross_day_close,    # 🆕 v9.11
                "cross_change_pct": cross_change_pct,   # 🆕 v9.11
                "w_close":      w_close_v,
                "w_ma10":       w_ma10_v,
                "w_ma20":       w_ma20_v,
                "w_dev":        ((w_close_v - w_ma10_v) / w_ma10_v * 100
                                 if w_close_v and w_ma10_v and w_ma10_v != 0 else None),
                "sma60":        at(sma60_s, idx),
                "ema100":       at(ema100_s, idx),
                "sma100":       at(sma100_s, idx),
                "ema200":       at(ema200_s, idx),
                "sma200":       at(sma200_s, idx),
                "sma200_prev":  at(sma200_s, idx-1),
                "ichimoku":     at(ichi_s, idx),
                "vwma":         at(vwma_s, idx),
                "hma":          at(hma_s, idx),
            }
            out.append((row_date.isoformat(), d_dict))

        return out
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────
# JUDGMENT LOGIC
# ─────────────────────────────────────────────────────────────────
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

def fmt(v, d=2): return f"{v:.{d}f}" if v is not None else "N/A"

def _jadx(adx, pos, neg):
    """ADX 方向判斷：> 25 且 +DI > -DI = 買入；> 25 且 -DI > +DI = 賣出；其餘中立"""
    if adx is None or pos is None or neg is None:
        return "中立"
    if adx > 25:
        return "買入" if pos > neg else "賣出"
    return "中立"

def judge_oscillators(d: dict) -> list:
    close, bbu, bbl = d["close"], d["bbu"], d["bbl"]

    # 布林 %B
    pct_b = None
    if bbu and bbl and (bbu - bbl) != 0:
        pct_b = (close - bbl) / (bbu - bbl) * 100
    bb_j = ("賣出" if pct_b is not None and pct_b > 100 else
            "買入" if pct_b is not None and pct_b < 0 else "中立")

    # 隨機%K — 顯示%K，TV用%D的crossover判斷
    # BUY:  %D 從下往上穿越 20（prev < 20, curr > 20）或 %D < 20（持續超賣）
    # SELL: %D 從上往下穿越 80（prev > 80, curr < 80）或 %D > 80（持續超買）
    stoch_d  = d.get("stoch_d")
    stoch_d1 = d.get("stoch_d_prev")
    if stoch_d is not None:
        if stoch_d < 20:
            stoch_j = "買入"
        elif stoch_d > 80:
            # crossunder邏輯：剛穿越80向下才算賣出，持續在80以上=中立
            if stoch_d1 is not None and stoch_d1 <= 80:
                stoch_j = "賣出"
            else:
                stoch_j = "中立"
        else:
            stoch_j = "中立"
    else:
        stoch_j = "中立"

    # AO — TV碟形/零軸交叉邏輯
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

    # 動量 — TV：當前Mom > 前期Mom = 買入
    mom, mom1 = d.get("mom"), d.get("mom_prev")
    if mom is not None and mom1 is not None:
        mom_j = "買入" if mom > mom1 else ("賣出" if mom < mom1 else "中立")
    else:
        mom_j = "中立"

    # StochRSI — TV用%D的crossover邏輯
    # BUY:  %D 穿越上 20 或持續在 20 以下
    # SELL: %D 穿越下 80 或持續在 80 以上
    sr  = d.get("stochrsi")
    sr1 = d.get("stochrsi_prev")
    if sr is not None:
        if sr < 20:
            sr_j = "買入"
        elif sr > 80:
            # TV: crossunder 80 = sell, still above 80 = neutral
            # 當%D > 80但沒有剛穿越時，TV顯示中立
            if sr1 is not None and sr1 <= 80:
                sr_j = "賣出"   # 剛穿越 80
            else:
                sr_j = "中立"   # 持續在 80 以上 = 中立
        else:
            sr_j = "中立"
    else:
        sr_j = "中立"

    # 威廉%R — TV crossover邏輯
    # BUY:  WR 從下往上穿越 -80（超賣反轉）
    # SELL: WR 從上往下穿越 -20（超買反轉）
    # 持續停在區間內但未穿越 = 中立
    wr  = d.get("willr")
    wr1 = d.get("willr_prev")
    if wr is not None and wr1 is not None:
        if wr1 < -80 and wr >= -80:      # 穿越上 -80 = 買入
            wr_j = "買入"
        elif wr1 > -20 and wr <= -20:    # 穿越下 -20 = 賣出
            wr_j = "賣出"
        else:
            wr_j = "中立"               # 未穿越閾值 = 中立
    else:
        wr_j = "中立"

    # 牛熊力度 — TV：zero crossover邏輯
    # BUY:  BBP 從負穿越到正（prev < 0, curr > 0）
    # SELL: BBP 從正穿越到負（prev > 0, curr < 0）
    # 持續正或負未穿越 = 中立
    bbp, bbp1 = d.get("bbpower"), d.get("bbpower_prev")
    if bbp is not None and bbp1 is not None:
        if bbp1 <= 0 and bbp > 0:
            bbp_j = "買入"   # 穿越零軸向上
        elif bbp1 >= 0 and bbp < 0:
            bbp_j = "賣出"   # 穿越零軸向下
        else:
            bbp_j = "中立"   # 持續同側 = 中立
    else:
        bbp_j = "中立"

    # 終極震盪 — TV：> 70 = 買入，< 30 = 賣出
    uo = d.get("uo")
    uo_j = ("買入" if uo is not None and uo > 70 else
            "賣出" if uo is not None and uo < 30 else "中立")

    return [
        (fmt(d["rsi"]),       _j(d["rsi"],   30,  70)),
        (fmt(d["stoch_k"]),   stoch_j),          # 顯示%K，判斷用%D
        (fmt(d["cci"]),       _j(d["cci"],  -100, 100)),
        (fmt(d["adx"]),       _jadx(d.get("adx"), d.get("adx_pos"), d.get("adx_neg"))),
        (fmt(ao),             ao_j),              # 碟形/零軸交叉
        (fmt(mom),            mom_j),             # 方向比較
        (fmt(d["macd"]),      _jz(d["macd"])),
        (fmt(sr),             sr_j),              # %D判斷
        (fmt(wr),             wr_j),
        (fmt(d["bbpower"]),   bbp_j),             # 正負+方向
        (fmt(uo),             uo_j),              # >70買入 <30賣出
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
    # items 可以是 (val, judg) 或 (label, val, judg)
    pairs = [(it[-2], it[-1]) if len(it) == 3 else it for it in items]
    if weights is None:
        weights = [1.0] * len(pairs)
    b = sum(w for (_, j), w in zip(pairs, weights) if j == "買入")
    s = sum(w for (_, j), w in zip(pairs, weights) if j == "賣出")
    n = sum(w for (_, j), w in zip(pairs, weights) if j == "中立")
    return round(b, 1), round(s, 1), round(n, 1), _rec(b, s)

# ─────────────────────────────────────────────────────────────────
# 四群組判斷函數
# ─────────────────────────────────────────────────────────────────
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
        # 接近死叉 / 黃金交叉預警（EMA20/60 差距 < 2%）
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

    # ── 2. 趨勢強度：ADX>25 門檻 + +DI/-DI 方向 ─────────────────
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

    # ── 3. 多頭階段：依乖離+ADX決定 Phase1/2/3 ──────────────────
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

    # ── 4. 乖離風險：EMA20 乖離 % + 布林 %B ──────────────────────
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

    # ── 5. 週線結構：週 MA10 vs MA20 ─────────────────────────────
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

TREND_W = [5.0, 4.0, 3.0, 3.0, 2.0]   # 合計 17 → 代表 40%

def judge_position(d: dict) -> list:
    """位置風險 (30%)：EMA20 乖離 + RSI(14) + 布林 %B"""
    close = d["close"]
    rsi   = d.get("rsi")
    ema20 = d.get("ema20")
    bbu, bbl = d.get("bbu"), d.get("bbl")

    # EMA20 乖離 %
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

    # RSI 區間（回測修訂：細分 30-40 近超賣；55-65 偏高轉中立作為潛在出場警示）
    if rsi is not None:
        if rsi < 30:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 超賣"
        elif rsi < 40:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 近超賣"
        elif rsi < 55:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 健康"
        elif rsi < 65:
            rsi_j, rsi_desc = "中立", f"RSI {rsi:.1f} 偏高"   # 空頭反彈出場訊號區
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

POSITION_W = [4.0, 3.5, 2.5]   # 合計 10 → 代表 30%

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

    # MACD 柱體方向：>0 且放大=買入，>0 但縮小=中立，<0=賣出
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

    # 動量(10)：>0=買入，<0=賣出
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

MOMENTUM_W = [3.5, 2.5, 2.0, 1.5, 1.5]  # 合計 11 → 代表 20%

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
                          and macd_hist > 0 and macd_hist < macd_hist_prev
                          # shrinking for at least 1 bar
                          )
    mom_pos    = mom is not None and mom > 0
    vol_strong = (volume is not None and vol_ma is not None and vol_ma > 0
                  and volume > vol_ma * 1.5)
    vol_expand = (volume is not None and vol_ma is not None and vol_ma > 0
                  and volume > vol_ma * 1.2)
    above_ma10 = ema10 is not None and close > ema10
    below_ma20 = ema20 is not None and close < ema20

    # Condition A: 強力買入（所有確認）
    if macd_pos and hist_expanding and mom_pos and vol_strong and above_ma10:
        return "強力買入"
    # Condition B: 買入（核心三項確認）
    if macd_pos and hist_ok and mom_pos:
        return "買入"
    # Condition D: 賣出
    if (macd is not None and macd < 0 and below_ma20) or \
       (mom is not None and mom < 0 and vol_expand):
        return "賣出"
    # Condition C: 中立
    return "中立"

def judge_aux(d: dict) -> list:
    """輔助指標 (10%)：震盪指標6項 + 核心均線 EMA10/EMA60/SMA200 + Hull MA(出場提醒)"""
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

    # Hull MA：短線出場提醒（close < hma = 出場警示，顯示為賣出）
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

AUX_W = [1.0, 1.0, 1.0, 0.8, 0.7, 0.7,  # 震盪6項
          1.2, 1.5, 1.5, 0.5]             # EMA10/EMA60/SMA200/Hull

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

def apply_cap(verdict: str, d: dict, mom_grade: str = "中立") -> tuple:
    """Hard Limits（寫死，不可被加權覆蓋）"""
    ema20, ema60 = d.get("ema20"), d.get("ema60")
    sma200 = d.get("sma200")
    adx    = d.get("adx")
    close, bbu, bbl = d.get("close"), d.get("bbu"), d.get("bbl")
    rsi    = d.get("rsi")
    w_dev  = d.get("w_dev")
    dev_pct = ((close - ema20) / ema20 * 100) if ema20 and close else None

    # ① EMA20 < EMA60：空頭封頂（最高優先）
    # 三層框架：RSI<30 極度超賣可留意 / RSI<32 接近進場區 / 其餘一般空頭不提超賣
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

    # ② 乖離 > 15% 或 RSI > 78：過熱禁新倉
    if (dev_pct is not None and dev_pct > 15) or (rsi is not None and rsi > 78):
        note = f"乖離{dev_pct:.1f}%" if dev_pct and dev_pct > 15 else f"RSI{rsi:.1f}"
        return "過熱觀望｜禁止新倉", f"⚠️ {note} 過熱"

    # ③ 週線過熱（週乖離 > 20%）：日線最高「買入」
    if w_dev is not None and w_dev > 20 and verdict == "強力買入":
        return "買入", f"⚠️ 週線乖離{w_dev:.1f}%（過熱，日線降級）"

    # ④ 動能中立/賣出：整體不超過中立
    if mom_grade in ("中立", "賣出") and verdict in ("強力買入", "買入"):
        return "中立", f"⚠️ 動能{mom_grade}（壓制整體評等）"

    # ⑤ 乖離 > 10%：強力買入 → 上限買入
    if dev_pct is not None and dev_pct > 10 and verdict == "強力買入":
        return "上限買入｜持有/短線", f"⚠️ EMA20 乖離{dev_pct:.1f}%（Phase3 禁加碼）"

    # ⑥ ADX < 25：強力買入 → 買入
    if adx is not None and adx < 25 and verdict == "強力買入":
        return "買入", f"⚠️ ADX {adx:.1f} < 25（趨勢偏弱）"

    return verdict, None

# ─────────────────────────────────────────────────────────────────
# 接近條件預警 — 即使尚未觸發 T1/T3/停損，也預先提示可能即將發生
# ─────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────
# ⑦ 自適應趨勢策略 v7 + v8（2026-04-26 完整版）
#
# 【v7 base 基礎策略】1263 檔均值 +72.79%
# 進場兩觸發：T1 黃金交叉 | T3 多頭拉回 RSI<50
# 共同前提：EMA20>EMA60 + ADX≥22 + EMA120 60日跌幅<2%（防死亡迴圈）
# 出場 ATR/Price 自動分類：高波動 EMA死叉 / 穩健 EMA+RSI+ATR
# 長持鎖定：持倉>200天 + 浮動>50% + EMA120上升 → EMA60/120 慢出場
# 反向ETF：ATR×1.5 + RSI>70 出場（無 T4）
#
# 【v8 金字塔加碼】同股不限倉位（架構性突破）
# 模式：P{N}_{signals}+{filters}
#   P0_T1T3            +197.48% （最大獲利，最差 -290%，σ=12.7）
#   P0_T1T3+CB30       +134.07% （風控版，最差 -166%，風報比 0.81，σ=12.6）
# ★ P0_T1T3+POS        +141.68% （生產推薦，最差 -166%，風報比 0.85，σ=8.9）
#   P0_T1T3+PS         +110.26% （最保守，最差 -149%）
#
# 【POS 自適應規則】（深度分析後發現）
# 規則：累積已實現損益為正才允許加碼（自我驗證機制）
#   - 死亡迴圈股：累積永負 → 永不加碼 → 與 v7 相同
#   - 強趨勢股：累積快速為正 → 正常加碼 → 完整受益
#   - 震盪股：累積零附近 → 加碼受限 → 避免惡化
# 跨年度 σ=8.9（vs P0/CB30 σ=12.7），全部變體最穩定
#
# 【健壯性驗證】
# - Walk-forward EARLY (2020-2022) +54.6 / LATE (2023-2026) +54.4 → Edge 穩定
# - 扣 0.4275% 台股交易成本後 P0_T1T3 仍 +184.33%
# - Monte Carlo：EMA120/ATR 健壯、ADX 中度敏感、RSI 脆弱
# - Cohen's d：v7<0 → 退步機率 10×（POS 自動處理）
#
# 【實證測試結論】
# 進場過濾普遍誤殺強勢股；退場優化大多切碎飆股
# 加碼自適應控制（POS）才是改善風報比的關鍵
# ─────────────────────────────────────────────────────────────────

# 反向ETF：使用 ⑦反向ETF策略（T1/T3 based on own chart，無T4，ATR×1.5，RSI>70出場）
# 核心邏輯：反向ETF的EMA黃金交叉 = 大盤開始下跌 → 此時正是進場時機
# 回測驗證（2020~2026）：RSI>70出場（-9.49%）優於ADX<25→RSI>65（-19.57%）
_INVERSE_ETF_TICKERS = {"00632R", "00633L", "00648U", "00675L", "00676L"}

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

# 一般特殊標的警告（非反向ETF，但操作需特別注意）
_SPECIAL_TICKER_WARN = {
    "00633L": ("2倍槓桿ETF（台灣50正2）",
               "2倍正向槓桿，波動劇烈且有衰減成本。適用⑦反向ETF版策略規則：ATR×1.5嚴格停損，RSI>70即出場（回測最佳）。"),
    "00648U": ("原油正2ETF",
               "商品槓桿ETF，受期貨轉倉成本侵蝕，不適合長期持有。建議以短線波段操作為主。"),
}

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
        if t1_ok and t3_ok:
            trigger = f"T1 黃金交叉 {cross_days} 天前 + T3 RSI {rsi_str}<50 拉回"
        elif t1_ok:
            trigger = f"T1 黃金交叉 {cross_days} 天前"
        else:
            trigger = f"T3 RSI {rsi_str}<50 拉回到位"
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


# ─────────────────────────────────────────────────────────────────
# INPUT PARSING  ← FIX: 修正 NameError（ticker 未定義的問題）
# ─────────────────────────────────────────────────────────────────
def parse_input(text: str) -> list:
    stocks = []
    seen   = set()
    for raw in text.strip().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts  = [p.strip() for p in line.split(",")]
        ticker = parts[0].upper()
        if ticker in seen:
            continue
        seen.add(ticker)
        if is_tw_stock(ticker):
            stocks.append((ticker, "台股"))
        elif ticker in SYMBOL_ALIASES:
            stocks.append((ticker, "指數"))
        else:
            market = parts[1].upper() if len(parts) > 1 else "NASDAQ"
            stocks.append((ticker, market))
    return stocks

# ─────────────────────────────────────────────────────────────────
# HTML HELPERS
# ─────────────────────────────────────────────────────────────────
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

def jcell(val, judg):
    cls = {"買入":"j-buy","賣出":"j-sell"}.get(judg, "j-neutral")
    return f'<td class="{cls}">{val}</td>'

def classify_action(d: dict) -> str:
    """
    🆕 v8 操作分類（每檔股票歸類為四種狀態之一）
    回傳：'ENTRY' / 'EXIT' / 'HOLD' / 'WAIT'

    ENTRY (可進場): 多頭 + ADX≥22 + (T1 黃金交叉 ≤ 10 天 OR T3 RSI<50 拉回) OR T4 反彈條件達成
    EXIT  (應出倉): 多頭中但出現出場訊號（高 RSI、EMA 死叉迫近、深度乖離等）
    HOLD  (持倉中): 多頭 + ADX≥22 但無新進場訊號（安全持倉或注意觀察）
    WAIT  (觀望中): 空頭 / 假多頭 / 資料不足 / EMA 死叉等
    """
    ema20      = d.get("ema20")
    ema60      = d.get("ema60")
    adx        = d.get("adx")
    rsi        = d.get("rsi")
    rsi_prev   = d.get("rsi_prev")
    rsi_prev2  = d.get("rsi_prev2")
    atr14      = d.get("atr14")
    close      = d.get("close")
    cross_days = d.get("ema20_cross_days")

    if ema20 is None or ema60 is None:
        return 'WAIT'

    is_bull = ema20 > ema60
    adx_ok  = (adx is not None and adx >= 22)

    # 空頭：唯一進場 = T4 反彈
    if not is_bull:
        t4_rising = (rsi is not None and rsi < 32 and
                     rsi_prev is not None and rsi > rsi_prev and
                     rsi_prev2 is not None and rsi_prev > rsi_prev2)
        if t4_rising:
            return 'ENTRY'  # T4 條件達成
        return 'WAIT'

    # 多頭但 ADX 不足
    if not adx_ok:
        return 'WAIT'

    # 多頭 + ADX≥22：判斷進場 / 出倉 / 持倉
    # 出場訊號優先（保守起見）
    rel_atr = (atr14 / close * 100) if (atr14 and close and close > 0) else 0
    is_high_vol = rel_atr > 3.5
    ema_gap_pct = (ema20 - ema60) / ema60 * 100 if ema60 else None
    ema_danger  = ema_gap_pct is not None and ema_gap_pct < 1.0   # EMA 即將死叉

    if is_high_vol:
        # 飆股模式：只看 EMA 距離
        if ema_danger:
            return 'EXIT'
    else:
        # 穩健股：EMA 死叉迫近 OR (ADX<25 + RSI>75)
        rsi_triggered = (adx is not None and adx < 25 and rsi is not None and rsi > 75)
        if rsi_triggered or ema_danger:
            return 'EXIT'

    # 進場觸發
    t1_ok = (cross_days is not None and 0 < cross_days <= 10)
    t3_ok = (rsi is not None and rsi < 50)
    if t1_ok or t3_ok:
        return 'ENTRY'

    # 否則為持倉中（安全持倉 / 等待 T3 拉回）
    return 'HOLD'


def get_exit_signal(d: dict) -> tuple:
    """
    ④出場獲利 輕量版：回傳 (status_label, badge_inline_style) 供表格使用。
    v3：依 ATR/Price 自動分類出場規則
      高波動（ATR/P > 3.5%）→ 只看EMA死叉，不設RSI出場門檻
      穩健（ATR/P ≤ 3.5%）  → EMA死叉 + ADX<25時RSI>75
    """
    ema20     = d.get("ema20")
    ema60     = d.get("ema60")
    adx       = d.get("adx")
    rsi       = d.get("rsi")
    rsi_prev  = d.get("rsi_prev")
    rsi_prev2 = d.get("rsi_prev2")
    atr14     = d.get("atr14")
    close_v   = d.get("close")

    if ema20 is None or ema60 is None:
        return ("—", "background:#0a1020;color:#555e6a")

    is_bull = ema20 > ema60

    if not is_bull:
        _t4 = (rsi is not None and rsi < 32 and
               rsi_prev is not None and rsi > rsi_prev and
               rsi_prev2 is not None and rsi_prev > rsi_prev2)
        if _t4:
            return ("T4 RSI>55出場", "background:#2a1500;color:#ff9944;border:1px solid #ff994455")
        return ("空頭 — 不持倉", "background:#0a1020;color:#555e6a;border:1px solid #2a334455")

    # 計算 ATR/Price 判斷股性
    rel_atr = (atr14 / close_v * 100) if (atr14 and close_v and close_v > 0) else 0
    is_high_vol = rel_atr > 3.5   # 高波動飆股：只守EMA死叉

    ema_gap   = (ema20 - ema60) / ema60 * 100 if ema60 else None
    ema_danger  = ema_gap is not None and ema_gap < 1.0
    ema_warning = ema_gap is not None and 1.0 <= ema_gap < 3.0

    if is_high_vol:
        # 飆股模式：只看EMA距離，不設RSI出場
        if ema_danger:
            return ("⚠️ 出場訊號", "background:#2a0808;color:#ff5555;border:1px solid #ff555555")
        if ema_warning:
            return ("注意觀察",   "background:#1a1200;color:#e8a020;border:1px solid #e8a02044")
        return ("安全持倉",       "background:#0a1e10;color:#3dbb6a;border:1px solid #3dbb6a44")
    else:
        # 穩健股模式：EMA死叉 + ADX<25時RSI>75
        rsi_triggered = (adx is not None and adx < 25 and rsi is not None and rsi > 75)
        rsi_warning   = (adx is not None and adx < 25 and rsi is not None and rsi >= 70)
        if rsi_triggered or ema_danger:
            return ("⚠️ 出場訊號", "background:#2a0808;color:#ff5555;border:1px solid #ff555555")
        if rsi_warning or ema_warning:
            return ("注意觀察",   "background:#1a1200;color:#e8a020;border:1px solid #e8a02044")
        return ("安全持倉",       "background:#0a1e10;color:#3dbb6a;border:1px solid #3dbb6a44")


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


def get_rec_label(d: dict, ticker: str = "") -> tuple:
    """
    ④推薦策略 輕量版：回傳 (rec_name, badge_inline_style) 供表格「操作建議」欄使用。
    與 get_operation_advice() 使用完全相同的決策樹邏輯，確保兩者一致。

    🆕 v9.10：自動偵測 TW vs US ticker 用不同閾值
      - TW（4 位數字）: ADX≥22 / 加碼門檻 P5（5%）/ V7 預設
      - US（純大寫字母 / -USD crypto）: ADX≥18 / 加碼門檻 P10（10%）/ 美股研究最佳
    🆕 v9.11：加入 _imminent_dc 阻擋邏輯（跟 get_operation_advice 一致）
    """
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
    # 🆕 偵測 TW / US / Crypto
    _tk_clean = _tk_upper.replace('-USD', '').replace('-', '')
    _is_us = _tk_clean.isalpha() and _tk_clean.isupper() and not _is_inverse
    _is_crypto = _tk_upper.endswith('-USD')
    is_bull    = ema20 > ema60
    # 🆕 美股用 ADX18，台股用 ADX22（依美股研究最佳變體 P10+POS+ADX18）
    _adx_th    = 18 if (_is_us or _is_crypto) else 22
    adx_ok     = (adx is not None and adx >= _adx_th)

    # 🆕 v9.11：即將死叉判斷（與 get_operation_advice 完全一致）
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
    # T1/T3 條件預估（同 get_operation_advice）
    _t1_ok = (cross_days is not None and 0 < cross_days <= 10)
    _t3_ok = (rsi is not None and rsi < 50)
    _entry_blocked_by_dc = (is_bull and adx_ok and _imminent_dc and (_t1_ok or _t3_ok))

    # 阻擋進場：表格欄優先顯示
    if _entry_blocked_by_dc and not _is_inverse:
        return ("🛑 不進場 即將死叉",
                "background:#2a0a0a;color:#ff7755;border:1px solid #ff775566")

    if _is_inverse:
        # 反向ETF：自身趨勢判斷，只用T1/T3（v7 已移除 T2）
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

    # 🆕 v9.9s：精簡格式 + T1/T4 顯示天數（T3 不顯示天數）
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
            return ("飆股",                 "background:#1a1400;color:#f0c030;border:1px solid #f0c03055")
        elif _is_strong and _is_pullback:
            return ("T3 強趨勢拉回",         "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
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


def render_table(results, platform_url_tpl: str = "https://www.perplexity.ai/search?q={prompt}",
                 selected_platform: str = "Perplexity") -> str:
    rows = ""
    for r in results:
        ticker, market, d, error = r[0], r[1], r[2], r[3]
        group_summs = r[5]   # (ts, ps, ms, xs)
        tsumm  = r[6]        # (tb, ts_, tn_, verdict)
        cap    = r[7]
        name = d.get("name", ticker) if d else ticker

        if error or not d:
            tv_url_err = get_tv_url(ticker, market)
            err_concepts = _get_concepts(ticker, max_n=4)
            err_concept_html = "".join(_concept_chip_html(c) for c in err_concepts) \
                       if err_concepts else '<span style="color:#334455;font-size:.7rem">—</span>'
            rows += (f'<tr>'
                     f'<td class="ticker-cell"><a href="{tv_url_err}" target="_blank" style="color:#e8f4fd;text-decoration:none;">{ticker}</a></td>'
                     f'<td style="color:#a8cce8;font-size:.78rem">—</td>'
                     f'<td class="j-na">—</td><td class="j-na">—</td>'
                     f'<td class="j-na">—</td>'
                     f'<td class="j-na">— 無資料 —</td><td class="j-na">—</td>'
                     f'<td>{err_concept_html}</td></tr>')
            continue

        close_val  = d.get("close")
        change_pct = d.get("change_pct")
        change_amt = d.get("change_amt")
        price_str  = fmt(close_val) if close_val is not None else "N/A"
        if change_pct is not None:
            chg_color = "#ff4444" if change_pct >= 0 else "#22cc88"
            chg_sign  = "▲" if change_pct >= 0 else "▼"
            amt_str   = f'{abs(change_amt):.2f}' if change_amt is not None else ""
            chg_str   = f'{amt_str} {chg_sign} {abs(change_pct):.2f}%'
        else:
            chg_color, chg_str = "#8899aa", "N/A"

        price_cell = (f'<td style="font-family:\'IBM Plex Mono\',monospace;font-size:.82rem;color:#e8f4fd">{price_str}</td>')
        chg_cell   = (f'<td style="font-family:\'IBM Plex Mono\',monospace;font-size:.82rem;color:{chg_color};font-weight:600">{chg_str}</td>')

        # 🆕 P/E 顯示（顏色判定 + 60 日動量箭頭 + 虧損標示）
        per_v = d.get('per')
        per_kind = d.get('per_kind', '')
        per_60d_chg = d.get('per_60d_chg_pct')
        if per_v is None:
            if per_kind == 'LOSS':
                pe_cell = ('<td style="color:#ff7777;font-size:.72rem;text-align:center;'
                           'font-weight:700" title="EPS ≤ 0 公司虧損中">虧損</td>')
            else:
                pe_cell = '<td style="color:#334455;font-size:.78rem;text-align:center">—</td>'
        else:
            if per_v <= 0 or per_v > 100:
                pe_color = '#ff5555'
            elif per_v < 10:
                pe_color = '#3dbb6a'
            elif per_v <= 20:
                pe_color = '#3dbb6a'
            elif per_v <= 30:
                pe_color = '#c8b87a'
            elif per_v <= 50:
                pe_color = '#e8a020'
            else:
                pe_color = '#ff5555'
            # 動量箭頭：60 日 PE 顯著降 → 🔻 (盈餘上修)；顯著升 → 🔺
            arrow = ''
            if per_60d_chg is not None:
                if per_60d_chg <= -15:
                    arrow = ' <span style="color:#3dbb6a;font-size:.7rem" title="60d PER 大降，盈餘上修">▼▼</span>'
                elif per_60d_chg <= -5:
                    arrow = ' <span style="color:#3dbb6a;font-size:.7rem" title="60d PER 微降">▼</span>'
                elif per_60d_chg >= 15:
                    arrow = ' <span style="color:#ff5555;font-size:.7rem" title="60d PER 大漲，盈餘下修風險">▲▲</span>'
                elif per_60d_chg >= 5:
                    arrow = ' <span style="color:#e8a020;font-size:.7rem" title="60d PER 微升">▲</span>'
            pe_cell = (f'<td style="font-family:\'IBM Plex Mono\',monospace;font-size:.82rem;'
                       f'color:{pe_color};font-weight:600;text-align:right">'
                       f'{per_v:.1f}{arrow}</td>')

        _rlabel, _rbadge = get_rec_label(d, ticker)
        _xlabel, _xbadge = get_exit_signal(d)
        # 🆕 v9.9u：T3 信心度只顯示於 T3 相關標籤（T1/飆股/T4/觀望不顯示）
        _is_t3_label = 'T3' in _rlabel
        _conf_score = d.get('t3_confidence', 0) or 0
        _conf_html = (render_confidence_dots(_conf_score)
                      if _is_t3_label and _conf_score > 0 else '')
        tot_cell = (f'<td style="background:#060c18;font-size:.82rem;font-weight:700;line-height:1.6">'
                    f'<span style="display:inline-block;padding:2px 7px;border-radius:4px;'
                    f'font-size:.76rem;white-space:nowrap;{_rbadge}">{_rlabel}</span>'
                    f'{("&nbsp;" + _conf_html) if _conf_html else ""}'
                    f'</td>')
        exit_cell = (f'<td style="background:#060c18;font-size:.82rem;line-height:1.6">'
                     f'<span style="display:inline-block;padding:2px 7px;border-radius:4px;'
                     f'font-size:.76rem;white-space:nowrap;{_xbadge}">{_xlabel}</span></td>')

        tv_url = get_tv_url(ticker, market)
        is_perplexity = "{prompt}" in platform_url_tpl and "perplexity" in platform_url_tpl
        ai_url    = get_ai_url(ticker, name, d, platform_url_tpl)
        prompt_js = get_prompt_text(ticker, name, d).replace("\\n", "\\\\n").replace("'", "\\'")
        if is_perplexity:
            ticker_link = f'<a href="{ai_url}" target="_blank" title="Perplexity 技術分析" style="color:#e8f4fd;text-decoration:none;">{ticker}</a>'
        else:
            homepage = platform_url_tpl.split("?")[0].split("{")[0]
            ticker_link = (
                f'<a href="#" onclick="navigator.clipboard.writeText(\'{prompt_js}\').then(()=>{{'
                f'window.open(\'{homepage}\',\'_blank\');'
                f'alert(\'提示詞已複製！請在新視窗中貼上(Ctrl+V / Cmd+V)\');}});return false;"'
                f' title="複製提示詞並開啟{selected_platform}" style="color:#e8f4fd;text-decoration:none;">{ticker}</a>'
            )
        # 概念股標籤
        concepts = _get_concepts(ticker, max_n=4)
        concept_html = "".join(_concept_chip_html(c) for c in concepts) \
                       if concepts else '<span style="color:#334455;font-size:.7rem">—</span>'
        concept_cell = (f'<td style="max-width:220px;line-height:1.7">{concept_html}</td>')

        # 🆕 VWAPEXEC 適用分級徽章
        vw_tier = get_vwap_tier(ticker)
        tier_badge = ''
        _delta_v = vw_tier.get('delta', 0)
        if vw_tier.get('tier') == 'TOP':
            _title = f"VWAPEXEC TOP 200 — 歷史 Δ {_delta_v:+.0f}%"
            tier_badge = (
                f'<span title="{_title}" '
                f'style="display:inline-block;padding:1px 5px;background:#0a3a1f;color:#3dbb6a;'
                f'border-radius:3px;font-size:.65rem;font-weight:700;margin-right:4px">⭐</span>'
            )
        elif vw_tier.get('tier') == 'NA':
            _title = f"VWAPEXEC 不適用 — Δ {_delta_v:+.0f}%"
            tier_badge = (
                f'<span title="{_title}" '
                f'style="display:inline-block;padding:1px 5px;background:#3a0a0a;color:#ff8888;'
                f'border-radius:3px;font-size:.65rem;font-weight:700;margin-right:4px">⚠️</span>'
            )
        # OK tier 不顯示徽章（保持簡潔）

        rows += (f'<tr>'
                 f'<td class="ticker-cell">{tier_badge}{ticker_link}</td>'
                 f'<td style="color:#a8cce8;font-size:.78rem;white-space:nowrap;max-width:150px;overflow:hidden;text-overflow:ellipsis">'
                 f'<a href="{tv_url}" target="_blank" style="color:#a8cce8;text-decoration:none;">{name}</a></td>'
                 f'{price_cell}{chg_cell}{pe_cell}{tot_cell}{exit_cell}{concept_cell}</tr>')

    return (f'<div style="background:#060c18;border-radius:12px;border:1px solid #1e3a5f;padding:4px">'
            f'<table class="res-table"><thead><tr>'
            f'<th>代號</th><th>名稱</th><th>現價</th><th>漲跌幅</th>'
            f'<th title="本益比（顏色：綠合理 / 黃合理偏高 / 橘偏高 / 紅虧損或過熱；▼=PER 60日下降=盈餘上修)">P/E</th>'
            f'<th style="background:#060c18;min-width:140px">操作建議</th>'
            f'<th style="background:#060c18;min-width:120px">④出場獲利</th>'
            f'<th style="background:#060c18;min-width:140px">概念股</th>'
            f'</tr></thead><tbody>{rows}</tbody></table></div>')

def render_detail(ticker, d, groups, group_summs, tsumm, cap, market: str = "") -> str:
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

    # cap_html 已移除：操作建議改用④推薦策略決策樹，舊加權系統的 cap 警告不再顯示

    summary_row = (
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;align-items:center">'
        f'<div style="background:#0a1628;border:1px solid #1a2f48;border-radius:8px;padding:7px 12px">'
        f'<span style="color:#7ab0d0;font-size:.68rem">收盤價 </span>'
        f'<b style="color:#e8f4fd;font-family:\'IBM Plex Mono\',monospace">{fmt(d["close"])}</b></div>'
    )
    # 同樣只顯示前 3 個群組的摘要（隱藏輔助指標）
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

    # ⚙️ 只顯示「實際驅動操作建議」的 3 個群組
    # 隱藏「輔助指標」（KD/CCI/StochRSI/威廉%R/牛熊力度/終極震盪/Hull MA 等
    # 都只是 informational scoring，不影響 T1/T3/T4 / cap / 接刀警告等判斷）
    sections = (
        group_section("趨勢結構", gc[0], g_trend,    ts_s) +
        group_section("位置風險", gc[1], g_position,  ps_s) +
        group_section("動能確認", gc[2], g_momentum,  ms_s)
    )

    advice_html = get_operation_advice(d, ticker=ticker)

    # 概念股標籤區塊
    concepts = _get_concepts(ticker, max_n=10)
    concept_html = ""
    if concepts:
        chips = "".join(_concept_chip_html(c) for c in concepts)
        concept_html = (
            f'<div style="background:#0a1628;border:1px solid #1a2f48;'
            f'border-radius:8px;padding:8px 12px;margin-bottom:10px">'
            f'<span style="color:#7ab0d0;font-size:.68rem;font-weight:700;'
            f'margin-right:8px">🏷️ 概念股</span>{chips}</div>'
        )

    # K 線型態已移至 get_operation_advice 內部（標題下、①市場環境上）

    # 🆕 新聞情感（在「當前策略風格」之後、「收盤價」之前）
    sent_html = ""
    if market:
        try:
            sent = get_news_sentiment(ticker, market)
            if sent['n'] > 0:
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
                for i, (title, s, link, pub) in enumerate(sent['headlines']):
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
                    f'（{sent["n"]} 篇平均，規則 70% + Erlangshen BERT 30%）</span>'
                    f'</div>'
                    f'{rows_html}'
                    f'</div>'
                )
        except Exception:
            pass

    return f'<div style="padding:4px 8px">{advice_html}{concept_html}{sent_html}{summary_row}{sections}</div>'

# ─────────────────────────────────────────────────────────────────
# EXCEL EXPORT  （四群組版）
# ─────────────────────────────────────────────────────────────────
def build_excel(results) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "指標報告"
    ws.sheet_view.showGridLines = False

    def fill(h): return PatternFill("solid", start_color=h, fgColor=h)
    def fnt(c="000000", sz=9, bd=False): return Font(name="Arial", size=sz, bold=bd, color=c)
    ctr  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    mid  = Side(style="medium")
    thin = Side(style="thin")

    # ── 判斷顏色對照 ──────────────────────────────────────────────
    JUDG_FC = {"買入":"1565C0","強力買入":"0D47A1",
               "賣出":"B71C1C","強力賣出":"7B1616",
               "中立":"555555"}
    VERDICT_BG = {
        "強力買入":          "0D3B6E",
        "買入":              "0D2E50",
        "上限買入｜持有/短線": "3A2A00",
        "中立":              "1A2030",
        "賣出":              "3B0D0D",
        "強力賣出":          "4A0A0A",
        "過熱觀望｜禁止新倉": "3A1800",
        "空頭，不買":        "3A0808",
    }
    VERDICT_FC = {
        "強力買入":          "60CFFF",
        "買入":              "60B3FF",
        "上限買入｜持有/短線": "F0C030",
        "中立":              "9AAABB",
        "賣出":              "FF8080",
        "強力賣出":          "FF6B6B",
        "過熱觀望｜禁止新倉": "FF8830",
        "空頭，不買":        "FF5555",
    }

    # ── 欄位定義：(標題, 寬度, 背景hex, 群組key) ─────────────────
    # 群組 key: T=趨勢, P=位置, M=動能, X=輔助, O=總覽, Z=基本
    G_HDR = {"Z":"1E3A5F","T":"0D2040","P":"2D1A00","M":"1A0D30","X":"1A1A28","O":"2C1654"}
    G_DAT = {"Z":("F0F4FF","F8FAFF"),
             "T":("E8F0FB","F3F7FE"), "P":("FFF3E0","FFFBF0"),
             "M":("F3E8FF","FAF3FF"), "X":("ECEFF4","F5F7FA"),
             "O":("EDE7F6","F5F0FF")}

    col_defs = [
        # 基本
        ("代號",    9, "Z"), ("名稱",    18, "Z"),
        ("現價",    10, "Z"), ("漲跌%",  10, "Z"),
        # 趨勢結構 (5項 + 小結)
        ("趨勢方向", 22, "T"), ("趨勢強度", 20, "T"),
        ("多頭階段", 22, "T"), ("乖離風險", 18, "T"), ("週線結構", 20, "T"),
        ("趨勢小結(40%)", 26, "T"),
        # 位置風險 (3項 + 小結)
        ("EMA20乖離", 16, "P"), ("RSI(14)", 14, "P"), ("布林%B", 14, "P"),
        ("位置小結(30%)", 26, "P"),
        # 動能確認 (5項 + 小結)
        ("MACD零軸", 14, "M"), ("MACD柱體", 16, "M"),
        ("動量(10)", 14, "M"), ("量能比率", 14, "M"), ("MA10位置", 14, "M"),
        ("動能小結(20%)", 26, "M"),
        # 輔助指標 (10項 + 小結)
        ("隨機%K",  12, "X"), ("CCI(20)",  12, "X"), ("StochRSI", 12, "X"),
        ("威廉%R",  12, "X"), ("牛熊力度", 12, "X"), ("終極震盪", 12, "X"),
        ("EMA(10)", 12, "X"), ("EMA(60)",  12, "X"), ("SMA(200)", 12, "X"),
        ("Hull MA", 12, "X"),
        ("輔助小結(10%)", 26, "X"),
        # 整體
        ("操作建議", 28, "O"), ("Cap說明",  36, "O"),
    ]

    # ── 寫標題行 ──────────────────────────────────────────────────
    for ci, (label, width, gkey) in enumerate(col_defs, 1):
        c = ws.cell(1, ci, label)
        c.font = fnt("FFFFFF", 9, True)
        c.fill = fill(G_HDR[gkey])
        c.alignment = ctr
        c.border = Border(bottom=mid, right=thin)
        ws.column_dimensions[get_column_letter(ci)].width = width
    ws.row_dimensions[1].height = 28
    ws.freeze_panes = "E2"

    def summ_str(b, s, n, v): return f"買:{b} 賣:{s} 中:{n} → {v}"

    def ind_val(item):
        """從 (label, val, judg) 或 (val, judg) 中取出 val 和 judg"""
        if len(item) == 3:
            return item[1], item[2]
        return item[0], item[1]

    for ri, item in enumerate(results, 2):
        ticker, market, d, error = item[0], item[1], item[2], item[3]
        groups      = item[4]   # (g_trend, g_position, g_momentum, g_aux)
        group_summs = item[5]   # (ts, ps, ms, xs)
        tsumm       = item[6]   # (tb, ts_, tn_, verdict)
        cap         = item[7]

        ws.row_dimensions[ri].height = 20
        is_even = (ri % 2 == 0)

        def cell(col, val, gkey, fc="000000", sz=9, bd=False, align=ctr):
            bg = G_DAT[gkey][0] if is_even else G_DAT[gkey][1]
            c = ws.cell(ri, col, val)
            c.font = fnt(fc, sz, bd)
            c.fill = fill(bg)
            c.alignment = align
            c.border = Border(right=thin)
            return c

        def verdict_cell(col, verdict, gkey):
            bg  = VERDICT_BG.get(verdict, "626567")
            fc  = VERDICT_FC.get(verdict, "FFFFFF")
            c = ws.cell(ri, col, verdict)
            c.font = fnt(fc, 9, True)
            c.fill = fill(bg)
            c.alignment = ctr
            c.border = Border(right=Side(style="medium"))
            return c

        # 基本資訊
        name = d.get("name", ticker) if d else ticker
        cell(1, ticker, "Z", "1565C0", 10, True)
        cell(2, name,   "Z", "333333", 9, False, left)

        if error or not d:
            for ci in range(3, len(col_defs)+1):
                cell(ci, "無資料", "Z", "AAAAAA", 9)
            continue

        close_val  = d.get("close")
        change_pct = d.get("change_pct")
        cell(3, f"{close_val:.2f}" if close_val else "N/A", "Z", "222222", 9)
        if change_pct is not None:
            chg_fc = "B71C1C" if change_pct >= 0 else "1B5E20"
            cell(4, f"{'▲' if change_pct>=0 else '▼'}{abs(change_pct):.2f}%", "Z", chg_fc, 9, True)
        else:
            cell(4, "N/A", "Z", "888888", 9)

        g_trend, g_position, g_momentum, g_aux = groups
        ts_s, ps_s, ms_s, xs_s = group_summs
        tb, ts_, tn_, tr_ = tsumm

        # ── 趨勢結構 (5項 + 小結) ──────────────────────────────
        ci = 5
        for it in g_trend[:5]:
            v, j = ind_val(it)
            cell(ci, f"{v} / {j}", "T", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci += 1
        verdict_cell(ci, ts_s[3], "T")
        ws.cell(ri, ci).value = summ_str(*ts_s); ci += 1

        # ── 位置風險 (3項 + 小結) ──────────────────────────────
        for it in g_position[:3]:
            v, j = ind_val(it)
            cell(ci, f"{v} / {j}", "P", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci += 1
        verdict_cell(ci, ps_s[3], "P")
        ws.cell(ri, ci).value = summ_str(*ps_s); ci += 1

        # ── 動能確認 (5項 + 小結) ──────────────────────────────
        for it in g_momentum[:5]:
            v, j = ind_val(it)
            cell(ci, f"{v} / {j}", "M", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci += 1
        verdict_cell(ci, ms_s[3], "M")
        ws.cell(ri, ci).value = summ_str(*ms_s); ci += 1

        # ── 輔助指標 (10項 + 小結) ─────────────────────────────
        aux_start = ci
        for it in g_aux[:10]:
            v, j = ind_val(it)
            cell(ci, f"{v} / {j}", "X", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci += 1
        # pad if fewer than 10 items（與 build_stock_range_excel 保持一致）
        while ci < aux_start + 10:
            cell(ci, "", "X"); ci += 1
        verdict_cell(ci, xs_s[3], "X")
        ws.cell(ri, ci).value = summ_str(*xs_s); ci += 1

        # ── 操作建議 + Cap ──────────────────────────────────────
        verdict_cell(ci, tr_, "O")
        ws.cell(ri, ci).value = tr_; ci += 1
        cap_txt = cap if cap else "—"
        cell(ci, cap_txt, "O", "884400" if cap else "888888", 9, False, left)

    # 時間戳記
    ws.cell(len(results) + 3, 1,
            f"產出時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}").font = fnt("999999", 8)

    buf = io.BytesIO(); wb.save(buf); return buf.getvalue()


# ─── 🆕 v9.10p：PDF 完整指標報告 ──────────────────────────────
def build_pdf(results) -> bytes:
    """產生個股完整指標分析 PDF（圖文並茂）
    包含：封面 + 每股一頁完整指標 + 操作建議"""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm, mm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                     Table, TableStyle, PageBreak)
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont
    import io as _io
    from datetime import datetime as _dt

    # 註冊中文字體（CID 內建，雲端可用）
    try:
        pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
        CN_FONT = 'STSong-Light'
    except Exception:
        CN_FONT = 'Helvetica'

    buf = _io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             topMargin=1.5*cm, bottomMargin=1.5*cm,
                             leftMargin=1.5*cm, rightMargin=1.5*cm)
    story = []
    styles = getSampleStyleSheet()
    title_st = ParagraphStyle('Title', parent=styles['Title'],
                               fontName=CN_FONT, fontSize=20,
                               textColor=colors.HexColor('#3b9eff'),
                               alignment=1, spaceAfter=10)
    h1_st = ParagraphStyle('H1', parent=styles['Heading1'],
                            fontName=CN_FONT, fontSize=14,
                            textColor=colors.HexColor('#7abadd'),
                            spaceBefore=8, spaceAfter=4)
    h2_st = ParagraphStyle('H2', parent=styles['Heading2'],
                            fontName=CN_FONT, fontSize=11,
                            textColor=colors.HexColor('#3dbb6a'),
                            spaceBefore=6, spaceAfter=3)
    body_st = ParagraphStyle('Body', parent=styles['Normal'],
                              fontName=CN_FONT, fontSize=9,
                              textColor=colors.HexColor('#333333'),
                              leading=13)
    small_st = ParagraphStyle('Small', parent=styles['Normal'],
                               fontName=CN_FONT, fontSize=7,
                               textColor=colors.HexColor('#666666'))

    # ── 封面 ──
    story.append(Paragraph("📊 Stock001 個股指標分析報告", title_st))
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(
        f"報告生成: {_dt.now().strftime('%Y-%m-%d %H:%M:%S')}", body_st))
    story.append(Paragraph(
        f"涵蓋股票: {len(results)} 支", body_st))
    story.append(Spacer(1, 0.5*cm))

    # 摘要表
    summary_rows = [['#', 'Ticker', '名稱', '收盤', 'RSI', 'ADX',
                      'EMA20/60', '建議']]
    for i, item in enumerate(results, 1):
        try:
            ticker = item[0] if len(item) > 0 else ''
            d = item[2] if len(item) > 2 and item[2] else None
            if d is None:
                summary_rows.append([str(i), ticker, '—', '—', '—', '—', '—', '無資料'])
                continue
            name = (d.get('name') or '')[:12]
            close = d.get('close', 0) or 0
            rsi = d.get('rsi', 0) or 0
            adx = d.get('adx', 0) or 0
            e20 = d.get('ema20', 0) or 0
            e60 = d.get('ema60', 0) or 0
            bull = '多頭' if (e20 > e60) else '空頭'
            try:
                rec_label, _ = get_rec_label(d, ticker)
            except Exception:
                rec_label = '—'
            summary_rows.append([
                str(i), ticker, name, f'{close:.2f}',
                f'{rsi:.1f}', f'{adx:.1f}', bull, rec_label[:14]
            ])
        except Exception:
            continue

    summary_tbl = Table(summary_rows, colWidths=[
        0.8*cm, 2.2*cm, 4*cm, 1.8*cm, 1.5*cm, 1.5*cm, 1.5*cm, 4*cm])
    summary_tbl.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), CN_FONT),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0a1830')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1),
         [colors.white, colors.HexColor('#f5f8fa')]),
    ]))
    story.append(Paragraph("📋 個股摘要", h1_st))
    story.append(summary_tbl)
    story.append(PageBreak())

    # ── 每股一頁詳細 ──
    for idx, item in enumerate(results, 1):
        try:
            ticker = item[0] if len(item) > 0 else ''
            market = item[1] if len(item) > 1 else 'TW'
            d = item[2] if len(item) > 2 else None
            if d is None: continue

            name = d.get('name', '')
            close = d.get('close', 0) or 0
            rsi = d.get('rsi', 0) or 0
            rsi_prev = d.get('rsi_prev', 0) or 0
            adx = d.get('adx', 0) or 0
            ema20 = d.get('ema20', 0) or 0
            ema60 = d.get('ema60', 0) or 0
            ema5 = d.get('ema5', 0) or 0
            ema120 = d.get('ema120', 0) or 0
            atr14 = d.get('atr14', 0) or 0
            sma200 = d.get('sma200', 0) or 0
            cd = d.get('ema20_cross_days', 0) or 0
            per = d.get('per')
            pbr = d.get('pbr')
            div = d.get('div_yield')
            eps = d.get('eps_ttm')
            change_pct = d.get('change_pct', 0) or 0

            is_bull = ema20 > ema60
            adx_th = 18 if (ticker.upper().replace('-USD','').isalpha()
                             and ticker.upper().isupper()) else 22
            mkt_tag = ('🇺🇸 US' if (ticker.upper().isalpha()
                                      and ticker.upper().isupper()
                                      and not ticker.endswith('-USD'))
                       else ('🪙 Crypto' if ticker.endswith('-USD')
                             else '🇹🇼 TW'))

            # 標題
            story.append(Paragraph(
                f"#{idx} {ticker} {name}", h1_st))
            story.append(Paragraph(
                f"市場: {mkt_tag} ｜ 收盤: <b>{close:.2f}</b> "
                f"({change_pct:+.2f}%)", body_st))
            story.append(Spacer(1, 0.2*cm))

            # ① 市場環境
            story.append(Paragraph("① 市場環境", h2_st))
            adx_ok = adx >= adx_th
            env_text = (
                f"狀態: <b>{'✅ 多頭市場' if is_bull and adx_ok else ('⚠️ 假多頭' if is_bull else '🚫 空頭')}</b><br/>"
                f"EMA20: {ema20:.2f} ｜ EMA60: {ema60:.2f} "
                f"({'多' if is_bull else '空'}, 差距 {(ema20-ema60)/ema60*100:+.1f}%)<br/>"
                f"ADX: <b>{adx:.1f}</b> "
                f"({'≥' if adx_ok else '<'}{adx_th} {'達標' if adx_ok else '不達標'})<br/>"
                f"黃金交叉: {cd if cd > 0 else '空頭'} 天前"
            )
            if cd and 1 <= cd <= 10:
                if (ticker.upper().isalpha() and ticker.upper().isupper()):
                    if 1 <= cd <= 5:
                        env_text += " ⚡ 早鳥期"
                    else:
                        env_text += " ⚠️ 已過早鳥"
                else:
                    if 5 <= cd <= 7:
                        env_text += " ⭐ Sweet Spot"
            story.append(Paragraph(env_text, body_st))
            story.append(Spacer(1, 0.2*cm))

            # ② 進場判斷
            story.append(Paragraph("② 進場判斷", h2_st))
            t1_ok = is_bull and adx_ok and 0 < cd <= 10
            t3_ok = is_bull and adx_ok and rsi < 50
            t4_ok = (not is_bull) and rsi < 32 and rsi > rsi_prev
            entry_lines = [
                f"T1 黃金交叉 (≤10d): {'✅' if t1_ok else '☐'} (cross={cd}d)",
                f"T3 多頭拉回 (RSI<50): {'✅' if t3_ok else '☐'} (RSI={rsi:.1f})",
                f"T4 空頭反彈 (RSI<32+上升): {'✅' if t4_ok else '☐'}",
            ]
            t3_conf = d.get('t3_confidence', 0) or 0
            if t3_conf > 0:
                entry_lines.append(
                    f"T3 信心度: {'●'*t3_conf}{'○'*(5-t3_conf)} {t3_conf}/5")
            story.append(Paragraph("<br/>".join(entry_lines), body_st))
            story.append(Spacer(1, 0.2*cm))

            # ③ 出場停損
            story.append(Paragraph("③ 出場停損", h2_st))
            atr_mult = 3.0 if adx >= 30 else (
                2.0 if (not is_bull and t4_ok) else 2.5)
            stop_p = close - atr14 * atr_mult
            stop_pct = -atr14 * atr_mult / close * 100 if close > 0 else 0
            story.append(Paragraph(
                f"停損價: <b>{stop_p:.2f}</b> "
                f"(收盤 {close:.2f} − ATR×{atr_mult} {atr14*atr_mult:.2f} "
                f"= {stop_pct:.1f}%)<br/>"
                f"ATR 14: {atr14:.2f} ｜ ATR/Price: {atr14/close*100:.2f}%",
                body_st))
            story.append(Spacer(1, 0.2*cm))

            # ④ 出場獲利
            story.append(Paragraph("④ 出場獲利", h2_st))
            ema_gap = (ema20 - ema60) / ema60 * 100 if ema60 > 0 else 0
            is_high_vol = atr14 / close * 100 > 3.5 if close > 0 else False
            if is_high_vol:
                exit_text = (
                    "🚀 飆股模式 (ATR/P > 3.5%)<br/>"
                    f"EMA死叉差距: {ema_gap:.1f}%<br/>"
                    "RSI 出場: 停用 (飆股 RSI 出場會砍主升段)<br/>"
                    "持倉到 EMA 死叉為止"
                )
            else:
                exit_text = (
                    "🛡 穩健股模式 (ATR/P ≤ 3.5%)<br/>"
                    f"EMA死叉差距: {ema_gap:.1f}%<br/>"
                    f"RSI 出場: ADX<25 + RSI>75 (現 RSI {rsi:.1f})<br/>"
                    "持到 EMA 死叉或停損觸發"
                )
            story.append(Paragraph(exit_text, body_st))
            story.append(Spacer(1, 0.2*cm))

            # 估值參考
            if any(v is not None for v in [eps, per, pbr, div]):
                story.append(Paragraph("💰 估值參考", h2_st))
                val_text = []
                if eps is not None: val_text.append(f"EPS(TTM): {eps:.2f}")
                if per is not None:
                    pe_tag = '虧損' if per <= 0 else (
                        '便宜' if per < 20 else (
                            '合理' if per <= 30 else (
                                '偏高' if per <= 50 else '過熱')))
                    val_text.append(f"PER: {per:.1f} ({pe_tag})")
                if pbr is not None: val_text.append(f"PBR: {pbr:.2f}")
                if div is not None: val_text.append(f"殖利率: {div:.2f}%")
                story.append(Paragraph(" ｜ ".join(val_text), body_st))
                story.append(Spacer(1, 0.2*cm))

            # ⑤ 推薦策略
            story.append(Paragraph("⑤ 推薦策略", h2_st))
            try:
                rec_label, _ = get_rec_label(d, ticker)
            except Exception:
                rec_label = '—'
            story.append(Paragraph(f"<b>{rec_label}</b>", body_st))

            # 完整指標表
            story.append(Paragraph("📐 完整指標數值", h2_st))
            ind_data = [
                ['指標', '值', '指標', '值'],
                ['EMA5', f'{ema5:.2f}' if ema5 else '—',
                 'EMA20', f'{ema20:.2f}'],
                ['EMA60', f'{ema60:.2f}',
                 'EMA120', f'{ema120:.2f}' if ema120 else '—'],
                ['SMA200', f'{sma200:.2f}' if sma200 else '—',
                 'ATR 14', f'{atr14:.2f}'],
                ['RSI 14', f'{rsi:.1f}',
                 'ADX 14', f'{adx:.1f}'],
                ['cross_days', f'{cd}',
                 'ATR/Price%', f'{atr14/close*100:.2f}%' if close else '—'],
            ]
            ind_tbl = Table(ind_data, colWidths=[3*cm, 3*cm, 3*cm, 3*cm])
            ind_tbl.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), CN_FONT),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#7abadd')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#cccccc')),
            ]))
            story.append(ind_tbl)

            story.append(Spacer(1, 0.3*cm))
            story.append(Paragraph(
                "⚠️ 本報告僅供參考，投資需自負風險。"
                "詳細策略邏輯請見 INDICATORS.md / MANUAL.md",
                small_st))
            if idx < len(results):
                story.append(PageBreak())
        except Exception as _e:
            story.append(Paragraph(
                f"#{idx} {ticker} 產生失敗: {str(_e)[:80]}", small_st))
            story.append(PageBreak())
            continue

    doc.build(story)
    return buf.getvalue()


def build_stock_range_excel(ticker: str, market: str,
                             start_date: str, end_date: str) -> bytes:
    """產生單一股票指定時間範圍的 Excel 報告（每個交易日一列）"""
    date_rows = fetch_indicators_range(ticker, market, start_date, end_date)

    wb = Workbook()
    ws = wb.active
    ws.title = (ticker[:20] + " 歷史")
    ws.sheet_view.showGridLines = False

    def _fl(hx): return PatternFill("solid", start_color=hx, fgColor=hx)
    def _fn(fc="000000", sz=9, bd=False): return Font(name="Arial", size=sz, bold=bd, color=fc)
    _ctr  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    _left = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    _mid  = Side(style="medium")
    _thn  = Side(style="thin")

    JUDG_FC = {"買入":"1565C0","強力買入":"0D47A1",
               "賣出":"B71C1C","強力賣出":"7B1616","中立":"555555"}
    VERDICT_BG = {
        "強力買入":"0D3B6E","買入":"0D2E50",
        "上限買入｜持有/短線":"3A2A00","中立":"1A2030",
        "賣出":"3B0D0D","強力賣出":"4A0A0A",
        "過熱觀望｜禁止新倉":"3A1800","空頭，不買":"3A0808",
    }
    VERDICT_FC = {
        "強力買入":"60CFFF","買入":"60B3FF",
        "上限買入｜持有/短線":"F0C030","中立":"9AAABB",
        "賣出":"FF8080","強力賣出":"FF6B6B",
        "過熱觀望｜禁止新倉":"FF8830","空頭，不買":"FF5555",
    }
    G_HDR = {"Z":"1E3A5F","T":"0D2040","P":"2D1A00","M":"1A0D30","X":"1A1A28","O":"2C1654"}
    G_DAT = {"Z":("F0F4FF","F8FAFF"),
             "T":("E8F0FB","F3F7FE"),"P":("FFF3E0","FFFBF0"),
             "M":("F3E8FF","FAF3FF"),"X":("ECEFF4","F5F7FA"),
             "O":("EDE7F6","F5F0FF")}

    # 欄位定義：日期 / 現價 / 漲跌% / 趨勢(5+小結) / 位置(3+小結) / 動能(5+小結) / 輔助(10+小結) / 整體+Cap
    col_defs = [
        ("日期",    12, "Z"), ("現價",   10, "Z"), ("漲跌%",  10, "Z"),
        # 趨勢
        ("趨勢方向", 22, "T"), ("趨勢強度", 20, "T"), ("多頭階段", 22, "T"),
        ("乖離風險", 18, "T"), ("週線結構", 20, "T"), ("趨勢小結(40%)", 26, "T"),
        # 位置
        ("EMA20乖離", 16, "P"), ("RSI(14)", 14, "P"), ("布林%B", 14, "P"),
        ("位置小結(30%)", 26, "P"),
        # 動能
        ("MACD零軸", 14, "M"), ("MACD柱體", 16, "M"), ("動量(10)", 14, "M"),
        ("量能比率", 14, "M"), ("MA10位置", 14, "M"), ("動能小結(20%)", 26, "M"),
        # 輔助
        ("隨機%K",  12, "X"), ("CCI(20)",  12, "X"), ("StochRSI", 12, "X"),
        ("威廉%R",  12, "X"), ("牛熊力度", 12, "X"), ("終極震盪", 12, "X"),
        ("EMA(10)", 12, "X"), ("EMA(60)",  12, "X"), ("SMA(200)", 12, "X"),
        ("Hull MA", 12, "X"), ("輔助小結(10%)", 26, "X"),
        # 整體
        ("操作建議", 28, "O"), ("Cap說明",  36, "O"),
    ]   # 共 32 欄

    for ci, (label, width, gkey) in enumerate(col_defs, 1):
        cl = ws.cell(1, ci, label)
        cl.font      = _fn("FFFFFF", 9, True)
        cl.fill      = _fl(G_HDR[gkey])
        cl.alignment = _ctr
        cl.border    = Border(bottom=_mid, right=_thn)
        ws.column_dimensions[get_column_letter(ci)].width = width
    ws.row_dimensions[1].height = 28
    ws.freeze_panes = "D2"   # 凍結前三欄（日期/現價/漲跌%）

    if not date_rows:
        cl = ws.cell(2, 1, "無資料：請確認代號或日期範圍")
        cl.font = _fn("CC4400", 10, True)
        buf = io.BytesIO(); wb.save(buf); return buf.getvalue()

    def _summ(b, s, n, v): return f"買:{b} 賣:{s} 中:{n} → {v}"
    def _ival(item): return (item[1], item[2]) if len(item) == 3 else (item[0], item[1])

    for ri, (date_str, d_dict) in enumerate(date_rows, 2):
        ws.row_dimensions[ri].height = 18
        is_even = (ri % 2 == 0)

        def mk(col, val, gkey, fc="000000", sz=9, bd=False, al=_ctr):
            bg = G_DAT[gkey][0] if is_even else G_DAT[gkey][1]
            cl = ws.cell(ri, col, val)
            cl.font = _fn(fc, sz, bd); cl.fill = _fl(bg)
            cl.alignment = al; cl.border = Border(right=_thn)
            return cl

        def mv(col, verdict, gkey):
            cl = ws.cell(ri, col, verdict)
            cl.font  = _fn(VERDICT_FC.get(verdict, "FFFFFF"), 9, True)
            cl.fill  = _fl(VERDICT_BG.get(verdict, "626567"))
            cl.alignment = _ctr
            cl.border = Border(right=Side(style="medium"))
            return cl

        if not d_dict or not d_dict.get("close"):
            mk(1, date_str, "Z", "444444", 9)
            for col in range(2, len(col_defs)+1):
                mk(col, "無資料", "Z", "AAAAAA", 9)
            continue

        # ── 計算指標 ─────────────────────────────────────────────
        g_trend    = judge_trend(d_dict)
        g_position = judge_position(d_dict)
        g_momentum = judge_momentum(d_dict)
        g_aux      = judge_aux(d_dict)
        ts = calc_summary(g_trend,    TREND_W)
        ps = calc_summary(g_position, POSITION_W)
        mom_grade  = compute_momentum_grade(d_dict)
        ms_b, ms_s, ms_n, _ = calc_summary(g_momentum, MOMENTUM_W)
        ms  = (ms_b, ms_s, ms_n, mom_grade)
        xs  = _calc_aux_summary(g_aux, AUX_W)
        tb  = round(ts[0]+ps[0]+ms[0]+xs[0], 1)
        ts_ = round(ts[1]+ps[1]+ms[1]+xs[1], 1)
        tn_ = round(ts[2]+ps[2]+ms[2]+xs[2], 1)
        verdict, cap = apply_cap(_rec(tb, ts_), d_dict, mom_grade)

        close_v    = d_dict.get("close")
        change_pct = d_dict.get("change_pct")
        mk(1, date_str, "Z", "1565C0", 9, True)
        mk(2, f"{close_v:.2f}" if close_v else "N/A", "Z", "222222", 9)
        if change_pct is not None:
            mk(3, f"{'▲' if change_pct>=0 else '▼'}{abs(change_pct):.2f}%",
               "Z", "B71C1C" if change_pct >= 0 else "1B5E20", 9, True)
        else:
            mk(3, "N/A", "Z", "888888", 9)

        ci = 4
        # ── 趨勢 5+小結 ────────────────────────────────────────
        for it in g_trend[:5]:
            v2, j = _ival(it)
            mk(ci, f"{v2} / {j}", "T", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci+=1
        mv(ci, ts[3], "T"); ws.cell(ri, ci).value = _summ(*ts); ci+=1

        # ── 位置 3+小結 ────────────────────────────────────────
        for it in g_position[:3]:
            v2, j = _ival(it)
            mk(ci, f"{v2} / {j}", "P", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci+=1
        mv(ci, ps[3], "P"); ws.cell(ri, ci).value = _summ(*ps); ci+=1

        # ── 動能 5+小結 ────────────────────────────────────────
        for it in g_momentum[:5]:
            v2, j = _ival(it)
            mk(ci, f"{v2} / {j}", "M", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci+=1
        mv(ci, ms[3], "M"); ws.cell(ri, ci).value = _summ(*ms); ci+=1

        # ── 輔助 10+小結 ───────────────────────────────────────
        aux_start = ci
        for it in g_aux[:10]:
            v2, j = _ival(it)
            mk(ci, f"{v2} / {j}", "X", JUDG_FC.get(j,"444444"), 9, j!="中立"); ci+=1
        while ci < aux_start + 10:     # 補空格（項目不足 10 時）
            mk(ci, "", "X"); ci+=1
        mv(ci, xs[3], "X"); ws.cell(ri, ci).value = _summ(*xs); ci+=1

        # ── 整體 ───────────────────────────────────────────────
        mv(ci, verdict, "O"); ws.cell(ri, ci).value = verdict; ci+=1
        mk(ci, cap if cap else "—", "O", "884400" if cap else "888888", 9, False, _left)

    # 時間戳記
    ws.cell(len(date_rows)+3, 1,
            f"產出：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
            f"{ticker} | {start_date}～{end_date}"
            ).font = _fn("999999", 8)

    buf = io.BytesIO(); wb.save(buf); return buf.getvalue()


# ─────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="tv-header" style="justify-content:space-between">
  <div style="display:flex;align-items:center;gap:14px">
    <div style="font-size:1.8rem">📊</div>
    <div><h1>Indicator Scanner</h1>
    <div class="sub">四群組加權評分 · 趨勢40% / 位置30% / 動能20% / 輔助10% · Hard Limits · Excel 匯出</div></div>
  </div>
  <div style="text-align:right;font-family:'IBM Plex Mono',monospace;line-height:1.4">
    <div style="color:#3b9eff;font-size:.95rem;font-weight:700;letter-spacing:.04em">{APP_VERSION}</div>
    <div style="color:#5a8ab0;font-size:.7rem">更新：{APP_UPDATED}</div>
  </div>
</div>
<div style="background:#0d1b2e;border:1px solid #1e3a5f;border-radius:6px;padding:8px 14px;margin-bottom:.5rem;font-size:.72rem;color:#7aaac8">
  📌 <b style="color:#8ab8d8">{APP_VERSION} 更新重點</b>：{APP_NOTES}
</div>
<div style="background:#0a1628;border:1px solid #1a2f48;border-radius:6px;padding:6px 14px;margin-bottom:1rem;font-size:.68rem;color:#5a8ab0">
  🔬 <b style="color:#7aaac8">健壯性驗證</b>：{APP_VALIDATIONS}
</div>""", unsafe_allow_html=True)


# 🆕 v9.9h：TOP 200 即時掃描（永遠顯示在頂部，不受 results 影響）
# v9.10：加入更新按鈕 + 過期警示（fix 「不會自更新」）
def _signal_age_days(updated_str, market='tw'):
    """計算 updated_at 距今幾天，回傳 (days, is_stale)
    🆕 v9.10g：美股過期門檻放寬到 >2 天（時差 + 假日因素）
       台股 >1 天即視為過期"""
    import datetime as _dt
    threshold = 2.0 if market == 'us' else 1.0
    try:
        dt = _dt.datetime.strptime(updated_str.strip()[:10], '%Y-%m-%d')
        diff = (_dt.datetime.now() - dt).total_seconds() / 86400
        return diff, diff > threshold
    except Exception:
        return 99, True


def _is_cloud_env():
    """偵測是否在雲端環境（沒 data_cache）"""
    from pathlib import Path as _P
    dc = _P(__file__).parent / 'data_cache'
    if not dc.exists(): return True
    # 抽樣：data_cache 有 < 100 個 parquet → 視為雲端
    n = sum(1 for _ in dc.glob('*.parquet'))
    return n < 100


def _show_update_result_if_any():
    """如有上次更新結果，顯示在頁面頂部（不會被 rerun 清掉）
    在 panel 開頭呼叫"""
    res = st.session_state.get('_last_update_result')
    if not res: return
    market, ok, msg, ts = res
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    if ok:
        st.success(f"✅ {flag} 更新完成（{ts}）— 訊號已刷新")
    else:
        st.error(f"❌ {flag} 更新失敗（{ts}）")
    with st.expander("📋 完整 log（點擊展開）", expanded=not ok):
        st.code(msg or "(無輸出)", language='text')
    if st.button("✕ 清除此訊息", key=f"clr_update_msg_{market}"):
        del st.session_state['_last_update_result']
        st.rerun()


def _trigger_update_signals(script_name, market='tw'):
    """跑 update，回傳 (ok, msg)
    🆕 v9.10d：雲端直接 import update_signals_cloud 模組呼叫（避開 subprocess 環境問題）
    🏠 本機跑完整 update_*_signals.py（含 backtest，subprocess）"""
    from pathlib import Path as _P
    import traceback as _tb
    proj = _P(__file__).parent

    # ☁️ 雲端 → 直接 in-process import 呼叫（最可靠）
    if _is_cloud_env():
        try:
            # 動態 import（每次都 reload 確保最新）
            import importlib, sys as _sys
            spec_path = proj / 'update_signals_cloud.py'
            if not spec_path.exists():
                return False, "update_signals_cloud.py 不存在"
            # 把 proj 加 sys.path 確保 import 到對的版本
            if str(proj) not in _sys.path:
                _sys.path.insert(0, str(proj))
            if 'update_signals_cloud' in _sys.modules:
                cloud = importlib.reload(_sys.modules['update_signals_cloud'])
            else:
                cloud = importlib.import_module('update_signals_cloud')
            # 直接呼叫對應函式
            import io as _io, contextlib as _ct
            buf = _io.StringIO()
            with _ct.redirect_stdout(buf), _ct.redirect_stderr(buf):
                if market == 'tw':
                    ok = cloud.update_tw()
                elif market == 'us':
                    ok = cloud.update_us()
                else:
                    return False, f"unknown market: {market}"
            log = buf.getvalue()
            return bool(ok), log[-1500:] if log else "(無輸出)"
        except Exception as e:
            err = f"Exception: {type(e).__name__}: {e}\n\n{_tb.format_exc()[-1000:]}"
            return False, err

    # 🏠 本機 → 跑完整 subprocess
    import subprocess as _sp
    import sys as _sys
    try:
        result = _sp.run([_sys.executable, str(proj / script_name)],
                         cwd=str(proj), capture_output=True, text=True,
                         timeout=600, encoding='utf-8', errors='replace')
        return result.returncode == 0, result.stdout[-1000:] + result.stderr[-500:]
    except Exception as e:
        return False, str(e)


def _render_top200_panel():
    # （v9.10e bug 修復：result 顯示移到全頁面頂端，避免兩個 panel 重複 button key）
    try:
        from pathlib import Path as _P
        import json as _json
        sig_path = _P(__file__).parent / 'top200_signals.json'
        if not sig_path.exists():
            st.warning("⚠️ top200_signals.json 不存在 — 請先跑 `python update_daily_signals.py`")
            return
        sig_data = _json.load(open(sig_path, encoding='utf-8'))
        _e_raw = sig_data.get('entry', [])
        _x_raw = sig_data.get('exit', [])
        _h_raw = sig_data.get('hold', [])
        _updated = sig_data.get('updated_at', '?')
        age_days, is_stale = _signal_age_days(_updated)

        # 🆕 過期警示 + 一鍵更新（雲端 yfinance 即時抓取版）
        is_cloud = _is_cloud_env()
        if is_stale or _updated == 'unknown':
            col1, col2 = st.columns([5, 1])
            with col1:
                env_str = '☁️ 雲端 yfinance 即時抓取' if is_cloud else '本機 data_cache'
                st.warning(
                    f"⚠️ TOP 200 訊號已過期 **{age_days:.1f} 天**（最後更新 {_updated}）— "
                    f"{env_str}，點右側按鈕立即更新")
            with col2:
                btn_label = "🔄 雲端更新" if is_cloud else "🔄 立即更新"
                btn_help = ("yfinance 抓最新一日 ~10 秒" if is_cloud
                            else "完整 update 約 2-5 分鐘")
                if st.button(btn_label, key="refresh_top200",
                             help=btn_help, use_container_width=True):
                    spinner_msg = "雲端 yfinance 抓取中…（~10 秒）" if is_cloud \
                                  else "正在更新 TOP 200 訊號…（約 2-5 分鐘）"
                    import datetime as _dt
                    import traceback as _tb
                    ts = _dt.datetime.now().strftime('%H:%M:%S')
                    # 🛡 全外層 try/except 防止 exception 吞掉 result
                    try:
                        with st.spinner(spinner_msg):
                            ok, msg = _trigger_update_signals(
                                'update_daily_signals.py', market='tw')
                        st.session_state['_last_update_result'] = (
                            'tw', ok, msg, ts)
                    except Exception as _e:
                        st.session_state['_last_update_result'] = (
                            'tw', False,
                            f"Outer exception: {type(_e).__name__}: {_e}\n\n"
                            f"{_tb.format_exc()}",
                            ts)
                    st.cache_data.clear()
                    st.rerun()

        if not (_e_raw or _x_raw or _h_raw):
            return

        # 主面板（過期時邊框轉橙色）
        border_color = '#ff9944' if is_stale else '#3dbb6a55'
        title_color = '#ff9944' if is_stale else '#3dbb6a'
        age_str = f"⚠️ {age_days:.1f} 天前" if is_stale else "今日"
        st.markdown(
            f'<div style="background:#0a1a2a;border:1px solid {border_color};border-radius:10px;'
            f'padding:10px 14px;margin:8px 0;display:flex;gap:14px;align-items:center;flex-wrap:wrap">'
            f'<div style="font-size:1.05rem;font-weight:700;color:{title_color}">📊 {age_str} TOP 200 即時掃描</div>'
            f'<div style="color:#7abadd;font-size:.85rem">'
            f'🚀 進場 <b>{len(_e_raw)}</b>　│　🚪 出倉 <b>{len(_x_raw)}</b>　│　📌 持倉 <b>{len(_h_raw)}</b></div>'
            f'<div style="color:#7a8899;font-size:.72rem;flex:1">'
            f'⭐ TOP 200 適用清單　│　資料 {_updated}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

        def _row_html(rows, color, label, max_n=999):
            """max_n=999 預設顯示全部（先前限 15 檔遭使用者反映）
            label='' 時不顯示頂部標題（用於 expander 內，標題已在 expander label）"""
            if not rows:
                return f'<div style="color:#3a5a7a;font-size:.72rem;padding:6px">— 暫無 —</div>'
            out = ''
            if label:
                out = (f'<div style="color:{color};font-size:.85rem;font-weight:700;'
                       f'padding:4px 8px 6px">{label} ({len(rows)})</div>')
            for r in rows[:max_n]:
                sig = r.get('sig', '')
                # 🆕 T3 信心度 ●○ — 只在 T3 訊號顯示（T1/飆股不顯示）
                conf = r.get('t3_confidence', 0) or 0
                is_t3 = sig.startswith('T3') or 'T3' in sig
                if conf > 0 and is_t3:
                    conf_html = (
                        f'<span style="font-size:.65rem;letter-spacing:1px;'
                        f'font-family:monospace" title="T3 信心度 {conf}/5">'
                        f'<span style="color:#3dbb6a">{"●"*conf}</span>'
                        f'<span style="color:#3a5a7a">{"○"*(5-conf)}</span>'
                        f'</span>')
                else:
                    conf_html = ''
                # 🆕 P/E（顏色判定）
                pe_v = r.get('pe')
                if pe_v is None:
                    pe_html = '<span style="color:#3a5a7a;font-size:.7rem">PE —</span>'
                else:
                    if pe_v <= 0 or pe_v > 100:
                        pe_color = '#ff5555'
                    elif pe_v < 20:
                        pe_color = '#3dbb6a'
                    elif pe_v <= 30:
                        pe_color = '#c8b87a'
                    elif pe_v <= 50:
                        pe_color = '#e8a020'
                    else:
                        pe_color = '#ff5555'
                    pe_html = f'<span style="color:{pe_color};font-size:.7rem">PE {pe_v:.1f}</span>'
                # 🆕 v9.10g：safe formatting，避免 None 拋 exception
                _close_v = r.get("close", 0) or 0
                _delta_v = r.get("delta", 0) or 0
                # 🆕 v9.10m：delta = 60 日動量，用顏色區分漲跌
                if _delta_v >= 5:
                    _delta_color, _delta_bg = '#3dbb6a', '#0a3a1f'  # 強綠
                elif _delta_v >= 0:
                    _delta_color, _delta_bg = '#7abadd', '#0a2030'  # 弱藍
                elif _delta_v >= -10:
                    _delta_color, _delta_bg = '#e8a020', '#1a1408'  # 橙
                else:
                    _delta_color, _delta_bg = '#ff5555', '#2a0808'  # 紅
                _delta_str = f'{_delta_v:+.0f}%' if abs(_delta_v) < 1000 else f'{_delta_v:+.0f}%'
                out += (
                    f'<div style="display:flex;gap:6px;padding:3px 8px;font-size:.78rem;'
                    f'border-bottom:1px solid #1a2a3f;align-items:baseline">'
                    f'<span style="color:{color};font-weight:700;font-family:monospace;'
                    f'min-width:48px">{r.get("ticker", "?")}</span>'
                    f'<span style="color:#a8cce8;flex:1;overflow:hidden;text-overflow:ellipsis;'
                    f'white-space:nowrap;max-width:90px">{r.get("name","")}</span>'
                    f'<span style="color:#e8f4fd;font-family:monospace">{_close_v:.2f}</span>'
                    f'{pe_html}'
                    f'<span style="color:#7a8899;font-size:.7rem">{sig}</span>'
                    f'{conf_html}'
                    f'<span style="color:{_delta_color};font-size:.7rem;background:{_delta_bg};'
                    f'padding:1px 5px;border-radius:3px" title="60日動量">{_delta_str}</span>'
                    f'</div>'
                )
            if len(rows) > max_n:
                out += (f'<div style="text-align:center;padding:4px;color:#5a8ab0;font-size:.7rem">'
                        f'＋{len(rows)-max_n} 檔</div>')
            return out

        cols = st.columns(3)
        with cols[0]:
            with st.expander(f"🚀 可進場 ({len(_e_raw)})", expanded=False):
                st.markdown(
                    f'<div style="background:#0a1e10;border:1px solid #3dbb6a55;border-radius:8px;'
                    f'padding:6px 4px">{_row_html(_e_raw, "#3dbb6a", "")}</div>',
                    unsafe_allow_html=True)
            # 🆕 v9.9l：存成自選股清單（按鈕在進場欄下方）
            if _e_raw:
                _save_cols = st.columns([3, 1])
                with _save_cols[0]:
                    _wl_name = st.text_input(
                        "自選股名稱",
                        value=f"TOP200進場_{_updated}",
                        key="top200_save_name",
                        label_visibility="collapsed",
                        placeholder="自選股名稱",
                    )
                with _save_cols[1]:
                    if st.button("💾 存", key="top200_save_btn",
                                 use_container_width=True,
                                 help=f"把這 {len(_e_raw)} 檔可進場股票存成自選股清單"):
                        # 載入現有 watchlists（雙軌：localStorage + 檔案）
                        try:
                            from streamlit_local_storage import LocalStorage as _LS
                            _ls = _LS()
                        except Exception:
                            _ls = None
                        _wl_path = _P(__file__).parent / 'watchlists.json'

                        wls = {}
                        if _ls:
                            try:
                                v = _ls.getItem("stock001_watchlists")
                                if v:
                                    wls = _json.loads(v) if isinstance(v, str) else v
                            except Exception:
                                pass
                        if not wls and _wl_path.exists():
                            try:
                                wls = _json.loads(_wl_path.read_text(encoding='utf-8'))
                            except Exception:
                                pass

                        # 新增清單
                        tickers_str = "\n".join(r['ticker'] for r in _e_raw)
                        wls[_wl_name] = tickers_str
                        # 儲存
                        text = _json.dumps(wls, ensure_ascii=False, indent=2)
                        if _ls:
                            try: _ls.setItem("stock001_watchlists", text)
                            except: pass
                        try:
                            _wl_path.write_text(text, encoding='utf-8')
                        except Exception:
                            pass
                        st.success(f"✅ 已存「{_wl_name}」（{len(_e_raw)} 檔）")
                        st.rerun()
        with cols[1]:
            with st.expander(f"🚪 應出倉 ({len(_x_raw)})", expanded=False):
                st.markdown(
                    f'<div style="background:#1a0808;border:1px solid #ff555555;border-radius:8px;'
                    f'padding:6px 4px">{_row_html(_x_raw, "#ff7777", "")}</div>',
                    unsafe_allow_html=True)
        with cols[2]:
            # 持倉中 預設收合（通常較多，避免畫面太長）
            with st.expander(f"📌 持倉中 ({len(_h_raw)})", expanded=False):
                st.markdown(
                    f'<div style="background:#0a1830;border:1px solid #5a8ab055;border-radius:8px;'
                    f'padding:6px 4px">{_row_html(_h_raw, "#7abadd", "")}</div>',
                    unsafe_allow_html=True)
    except Exception as _panel_err:
        # 🆕 v9.10g：不再吞 exception — 顯示錯誤給用戶看
        import traceback as _tb
        st.error(f"❌ TOP 200 panel render 失敗：{type(_panel_err).__name__}: {_panel_err}")
        with st.expander("traceback"):
            st.code(_tb.format_exc())


# 🆕 v9.10e：在所有 panel 之前統一顯示更新結果（只呼叫一次避免 duplicate key）
_show_update_result_if_any()


# 🆕 v9.10z：上次更新時間橫幅（讓用戶清楚資料新鮮度）
def _render_update_status_bar():
    """顯示 alerts JSON 的時間戳，讓用戶確認資料新鮮度"""
    try:
        from pathlib import Path as _P
        import json as _json
        import datetime as _dt
        proj = _P(__file__).parent
        rows = []
        for fname, label, emoji in [
            ('top200_signals.json', 'TW TOP 200', '🇹🇼'),
            ('us_top200_signals.json', 'US TOP 100', '🇺🇸'),
        ]:
            p = proj / fname
            if not p.exists(): continue
            try:
                d = _json.loads(p.read_text(encoding='utf-8'))
            except: continue
            updated = d.get('updated_at', '?')
            computed = d.get('computed_at', '')
            n_alerts = len(d.get('alerts', []))
            n_entry = len(d.get('entry', []))
            source = d.get('source', '?')
            # 計算過期天數
            try:
                dt = _dt.datetime.strptime(updated[:10], '%Y-%m-%d')
                age = (_dt.datetime.now() - dt).total_seconds() / 86400
                age_str = (f'<span style="color:#3dbb6a">{age:.1f}d 前</span>'
                           if age < 1.5 else
                           f'<span style="color:#e8a020">⚠️ {age:.1f}d 前</span>'
                           if age < 3 else
                           f'<span style="color:#ff5555">🚨 {age:.1f}d 前過期</span>')
            except:
                age_str = '<span style="color:#7a8899">unknown</span>'
            rows.append({
                'emoji': emoji, 'label': label,
                'updated': updated, 'computed': computed,
                'n_alerts': n_alerts, 'n_entry': n_entry,
                'age_str': age_str, 'source': source,
            })
        if not rows: return

        cells = []
        for r in rows:
            cells.append(
                f'<div style="flex:1;min-width:220px;padding:6px 10px;'
                f'background:#0a1628;border-radius:6px;'
                f'border:1px solid #5a8ab055">'
                f'<div style="display:flex;gap:8px;align-items:center">'
                f'<span style="font-size:1rem">{r["emoji"]}</span>'
                f'<span style="color:#7abadd;font-weight:700;font-size:.82rem">'
                f'{r["label"]}</span>'
                f'<span style="color:#7a8899;font-size:.7rem">資料 {r["updated"]}</span>'
                f'<span style="font-size:.7rem;margin-left:auto">{r["age_str"]}</span>'
                f'</div>'
                f'<div style="color:#a8cce8;font-size:.72rem;margin-top:2px">'
                f'進場 <b>{r["n_entry"]}</b>檔｜警報 <b>{r["n_alerts"]}</b>檔｜'
                f'計算 {r["computed"][-8:] if r["computed"] else "?"}'
                f'</div>'
                f'</div>'
            )
        st.markdown(
            f'<div style="display:flex;gap:8px;margin:6px 0;flex-wrap:wrap">'
            + ''.join(cells) +
            f'</div>'
            f'<div style="text-align:right;color:#5a7a90;font-size:.65rem;'
            f'margin-top:-2px">⏰ 自動更新: 台股收盤後 13:30 / 美股盤後 06:00 (台北)'
            f'｜手動觸發: 各 panel 內按「🔄 更新」按鈕</div>',
            unsafe_allow_html=True
        )
    except Exception:
        pass


_render_update_status_bar()
_render_top200_panel()


# 🆕 v9.9o：美股 TOP 100 panel
# v9.10：加入更新按鈕 + 過期警示
def _render_us_top_panel():
    try:
        from pathlib import Path as _P
        import json as _json
        sig_path = _P(__file__).parent / 'us_top200_signals.json'
        if not sig_path.exists():
            st.warning("⚠️ us_top200_signals.json 不存在 — 請跑 `python update_us_signals.py`")
            return
        sig_data = _json.load(open(sig_path, encoding='utf-8'))
        _e_raw = sig_data.get('entry', [])
        _x_raw = sig_data.get('exit', [])
        _h_raw = sig_data.get('hold', [])
        _updated = sig_data.get('updated_at', '?')
        _tot = sig_data.get('top_total', 100)
        age_days, is_stale = _signal_age_days(_updated, market='us')

        # 🆕 過期警示 + 一鍵更新（雲端 yfinance 即時抓取版）
        is_cloud = _is_cloud_env()
        if is_stale or _updated == 'unknown':
            col1, col2 = st.columns([5, 1])
            with col1:
                env_str = '☁️ 雲端 yfinance 即時抓取' if is_cloud else '本機 data_cache'
                st.warning(
                    f"⚠️ US TOP 訊號已過期 **{age_days:.1f} 天**（最後更新 {_updated}）— "
                    f"{env_str}，點右側按鈕立即更新")
            with col2:
                btn_label = "🔄 雲端更新 US" if is_cloud else "🔄 立即更新 US"
                btn_help = ("yfinance 抓最新一日 ~10 秒" if is_cloud
                            else "完整 update 約 3-8 分鐘")
                if st.button(btn_label, key="refresh_us_top",
                             help=btn_help, use_container_width=True):
                    spinner_msg = "雲端 yfinance 抓取中…（~10 秒）" if is_cloud \
                                  else "正在更新 US 訊號…（約 3-8 分鐘）"
                    import datetime as _dt
                    import traceback as _tb
                    ts = _dt.datetime.now().strftime('%H:%M:%S')
                    try:
                        with st.spinner(spinner_msg):
                            ok, msg = _trigger_update_signals(
                                'update_us_signals.py', market='us')
                        st.session_state['_last_update_result'] = (
                            'us', ok, msg, ts)
                    except Exception as _e:
                        st.session_state['_last_update_result'] = (
                            'us', False,
                            f"Outer exception: {type(_e).__name__}: {_e}\n\n"
                            f"{_tb.format_exc()}",
                            ts)
                    st.cache_data.clear()
                    st.rerun()

        if not (_e_raw or _x_raw or _h_raw):
            return

        border_color = '#ff9944' if is_stale else '#5a8ab055'
        title_color = '#ff9944' if is_stale else '#7abadd'
        age_str = f"⚠️ {age_days:.1f} 天前" if is_stale else "今日"
        st.markdown(
            f'<div style="background:#0a1a2a;border:1px solid {border_color};border-radius:10px;'
            f'padding:10px 14px;margin:8px 0;display:flex;gap:14px;align-items:center;flex-wrap:wrap">'
            f'<div style="font-size:1.05rem;font-weight:700;color:{title_color}">🇺🇸 {age_str} US TOP {_tot} 即時掃描</div>'
            f'<div style="color:#a8cce8;font-size:.85rem">'
            f'🚀 進場 <b>{len(_e_raw)}</b>　│　🚪 出倉 <b>{len(_x_raw)}</b>　│　📌 持倉 <b>{len(_h_raw)}</b></div>'
            f'<div style="color:#7a8899;font-size:.72rem;flex:1">'
            f'P10_T1T3+POS+ADX18 高流動 ADV≥$104M（v9.10 美股最佳 RR 0.496）　│　資料 {_updated}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

        def _us_row_html(rows):
            if not rows:
                return f'<div style="color:#3a5a7a;font-size:.72rem;padding:6px">— 暫無 —</div>'
            out = ''
            for r in rows:
                sig = r.get('sig', '')
                _close_v = r.get("close", 0) or 0
                _delta_v = r.get("delta", 0) or 0
                _name = r.get("name", "") or ""
                # 🆕 v9.10m：delta = 60 日動量，用顏色區分
                if _delta_v >= 5:
                    _dc, _dbg = '#3dbb6a', '#0a3a1f'
                elif _delta_v >= 0:
                    _dc, _dbg = '#7abadd', '#0a2030'
                elif _delta_v >= -10:
                    _dc, _dbg = '#e8a020', '#1a1408'
                else:
                    _dc, _dbg = '#ff5555', '#2a0808'
                _dstr = f'{_delta_v:+.0f}%'
                out += (
                    f'<div style="display:flex;gap:6px;padding:3px 8px;font-size:.78rem;'
                    f'border-bottom:1px solid #1a2a3f;align-items:baseline">'
                    f'<span style="font-weight:700;font-family:monospace;'
                    f'min-width:60px;color:#e8f4fd">{r.get("ticker", "?")}</span>'
                    f'<span style="color:#a8cce8;flex:1;overflow:hidden;text-overflow:ellipsis;'
                    f'white-space:nowrap;max-width:130px;font-size:.72rem">{_name}</span>'
                    f'<span style="color:#a8cce8;font-family:monospace">{_close_v:.2f}</span>'
                    f'<span style="color:#7a8899;font-size:.7rem">{sig}</span>'
                    f'<span style="font-size:.7rem;background:{_dbg};color:{_dc};'
                    f'padding:1px 5px;border-radius:3px" title="60日動量">{_dstr}</span>'
                    f'</div>'
                )
            return out

        cols = st.columns(3)
        with cols[0]:
            with st.expander(f"🚀 可進場 ({len(_e_raw)})", expanded=False):
                st.markdown(
                    f'<div style="background:#0a1e10;border:1px solid #3dbb6a55;border-radius:8px;'
                    f'padding:6px 4px">{_us_row_html(_e_raw)}</div>',
                    unsafe_allow_html=True)
            if _e_raw:
                _us_save = st.columns([3, 1])
                with _us_save[0]:
                    _us_name = st.text_input(
                        "美股自選名稱",
                        value=f"US_進場_{_updated}",
                        key="us_save_name",
                        label_visibility="collapsed",
                    )
                with _us_save[1]:
                    if st.button("💾 存", key="us_save_btn", use_container_width=True):
                        try:
                            from streamlit_local_storage import LocalStorage as _LS
                            _ls = _LS()
                        except Exception:
                            _ls = None
                        _wl_path = _P(__file__).parent / 'watchlists.json'
                        wls = {}
                        if _ls:
                            try:
                                v = _ls.getItem("stock001_watchlists")
                                if v: wls = _json.loads(v) if isinstance(v, str) else v
                            except Exception: pass
                        if not wls and _wl_path.exists():
                            try: wls = _json.loads(_wl_path.read_text(encoding='utf-8'))
                            except Exception: pass
                        wls[_us_name] = "\n".join(r['ticker'] for r in _e_raw)
                        text = _json.dumps(wls, ensure_ascii=False, indent=2)
                        if _ls:
                            try: _ls.setItem("stock001_watchlists", text)
                            except: pass
                        try: _wl_path.write_text(text, encoding='utf-8')
                        except: pass
                        st.success(f"✅ 已存「{_us_name}」({len(_e_raw)} 檔)")
                        st.rerun()
        with cols[1]:
            with st.expander(f"🚪 應出倉 ({len(_x_raw)})", expanded=False):
                st.markdown(
                    f'<div style="background:#1a0808;border:1px solid #ff555555;border-radius:8px;'
                    f'padding:6px 4px">{_us_row_html(_x_raw)}</div>',
                    unsafe_allow_html=True)
        with cols[2]:
            with st.expander(f"📌 持倉中 ({len(_h_raw)})", expanded=False):
                st.markdown(
                    f'<div style="background:#0a1830;border:1px solid #5a8ab055;border-radius:8px;'
                    f'padding:6px 4px">{_us_row_html(_h_raw)}</div>',
                    unsafe_allow_html=True)
    except Exception as _us_err:
        # 🆕 v9.10g：不再吞 exception
        import traceback as _tb
        st.error(f"❌ US TOP panel render 失敗：{type(_us_err).__name__}: {_us_err}")
        with st.expander("traceback"):
            st.code(_tb.format_exc())


_render_us_top_panel()


# 🆕 v9.10u：產業輪動 Top 5 / Bottom 5 panel
def _render_sector_rotation_panel():
    """顯示過去 1 個月最強 5 + 最弱 5 個產業（動量延續策略）"""
    try:
        rankings = _get_sector_ranking_recent()
        if not rankings:
            return
        top5 = rankings[:5]
        bot5 = rankings[-5:][::-1]  # 由最弱開始顯示
        # Top 5 panel
        st.markdown(
            f'<div style="background:#0a1628;border:1px solid #ffd70055;'
            f'border-radius:10px;padding:10px 14px;margin:8px 0">'
            f'<div style="display:flex;gap:14px;align-items:center;'
            f'justify-content:space-between;flex-wrap:wrap;margin-bottom:8px">'
            f'<div style="font-size:1.05rem;font-weight:700;color:#ffd700">'
            f'🎯 產業輪動 Top 5 強勢（動量延續策略）</div>'
            f'<div style="color:#7a8899;font-size:.72rem">'
            f'過去 1 月平均月報酬｜回測 6 年總 +400%（vs 等權 +138%）/ Sharpe 1.24</div>'
            f'</div>'
            f'<div style="display:flex;gap:8px;flex-wrap:wrap">'
            + ''.join([
                f'<div style="background:#1a2030;border:1px solid {("#3dbb6a" if r > 0 else "#ff5555")}55;'
                f'border-radius:6px;padding:6px 10px;flex:1;min-width:150px">'
                f'<div style="color:#ffd700;font-weight:700;font-size:.85rem">'
                f'#{i+1} {sec}</div>'
                f'<div style="color:{("#3dbb6a" if r > 0 else "#ff5555")};font-size:1.1rem;font-weight:700">'
                f'{r:+.2f}%</div>'
                f'<div style="color:#7a8899;font-size:.7rem">{n} 檔平均</div>'
                f'</div>'
                for i, (sec, r, n) in enumerate(top5)
            ])
            + f'</div></div>',
            unsafe_allow_html=True
        )
        # 🆕 v9.10v：Bottom 5 弱勢產業 panel
        st.markdown(
            f'<div style="background:#1a0a0a;border:1px solid #ff555555;'
            f'border-radius:10px;padding:10px 14px;margin:8px 0">'
            f'<div style="display:flex;gap:14px;align-items:center;'
            f'justify-content:space-between;flex-wrap:wrap;margin-bottom:8px">'
            f'<div style="font-size:1.05rem;font-weight:700;color:#ff7755">'
            f'⚠️ 產業輪動 Bottom 5 弱勢（避開或反指標）</div>'
            f'<div style="color:#7a8899;font-size:.72rem">'
            f'過去 1 月最弱｜動量延續策略避開這些產業</div>'
            f'</div>'
            f'<div style="display:flex;gap:8px;flex-wrap:wrap">'
            + ''.join([
                f'<div style="background:#1a1010;border:1px solid {("#3dbb6a" if r > 0 else "#ff5555")}55;'
                f'border-radius:6px;padding:6px 10px;flex:1;min-width:150px">'
                f'<div style="color:#ff7755;font-weight:700;font-size:.85rem">'
                f'弱 {i+1} {sec}</div>'
                f'<div style="color:{("#3dbb6a" if r > 0 else "#ff5555")};font-size:1.1rem;font-weight:700">'
                f'{r:+.2f}%</div>'
                f'<div style="color:#7a8899;font-size:.7rem">{n} 檔平均</div>'
                f'</div>'
                for i, (sec, r, n) in enumerate(bot5)
            ])
            + f'</div></div>',
            unsafe_allow_html=True
        )
    except Exception:
        pass


_render_sector_rotation_panel()


# 🆕 v9.10y：警報中 panel（強訊號 + 即將觸發）
def _render_alerts_panel():
    """從 top200_signals.json + us_top200_signals.json 讀 alerts 列表並顯示"""
    try:
        from pathlib import Path as _P
        import json as _json
        all_alerts = []
        for fname, market_tag in [
            ('top200_signals.json', '🇹🇼'),
            ('us_top200_signals.json', '🇺🇸'),
        ]:
            p = _P(__file__).parent / fname
            if not p.exists(): continue
            d = _json.loads(p.read_text(encoding='utf-8'))
            for a in d.get('alerts', []):
                a = {**a, 'market': market_tag}
                all_alerts.append(a)
        if not all_alerts:
            return

        bull5 = [a for a in all_alerts if a.get('level') == 5]
        bull4 = [a for a in all_alerts if a.get('level') == 4 and a.get('side') == 'bull']
        bull3 = [a for a in all_alerts if a.get('level') == 3 and a.get('side') == 'bull']
        bear4 = [a for a in all_alerts if a.get('level') == 4 and a.get('side') == 'bear']
        bear3 = [a for a in all_alerts if a.get('level') == 3 and a.get('side') == 'bear']
        bear2 = [a for a in all_alerts if a.get('level') == 2 and a.get('side') == 'bear']
        imm_bull = [a for a in all_alerts if a.get('level') == 'imm_bull']
        imm_bear = [a for a in all_alerts if a.get('level') == 'imm_bear']

        total_strong = len(bull5) + len(bull4) + len(bull3) + len(bear4) + len(bear3) + len(bear2)
        total_imm = len(imm_bull) + len(imm_bear)
        if total_strong == 0 and total_imm == 0:
            return

        st.markdown(
            f'<div style="background:#1a0f1f;border:2px solid #ffd70066;'
            f'border-radius:10px;padding:10px 14px;margin:8px 0">'
            f'<div style="font-size:1.05rem;font-weight:700;color:#ffd700;'
            f'margin-bottom:6px">'
            f'🚨 警報中 — 強訊號 {total_strong} 檔｜即將觸發 {total_imm} 檔'
            f'</div>'
            # 🆕 v9.11：研究結論建議
            f'<div style="font-size:.7rem;color:#a8cce8;line-height:1.5">'
            f'💡 <b>OOS 最佳：倒鎚 hold=30 + max_pos=50 + drop_deep</b>'
            f'（OOS Sharpe 1.74 / MDD -4.64%）｜ '
            f'T1_V7 +10% 止損 → CAGR +10.87% ｜ '
            f'🟢 4 月黃金月 (+15.91%) / ⚠️ 3 月地雷月 (-7.94%) 警報自動標示'
            f'</div></div>',
            unsafe_allow_html=True
        )

        def _row_html(a):
            color = '#3dbb6a' if a.get('side') == 'bull' else '#ff5555'
            stars = ''
            lv = a.get('level')
            if lv == 5: stars = '★★★★★'
            elif lv == 4: stars = '★★★★'
            elif lv == 3: stars = '★★★'
            elif lv == 2: stars = '★★'
            else: stars = '⏰'
            close_v = a.get('close', 0) or 0
            # 🆕 v9.11：顯示 quality_score（priority sweep 證實能 +84% CAGR）
            qs = a.get('quality_score')
            qs_html = (f'<span style="color:#e8a020;font-family:monospace;'
                       f'min-width:55px;font-size:.7rem" title="品質分（高=優先）">'
                       f'Q{qs:+.1f}</span>'
                       if qs is not None else
                       f'<span style="min-width:55px"></span>')
            return (
                f'<div style="display:flex;gap:8px;align-items:center;'
                f'padding:4px 8px;border-bottom:1px solid #2a1830;font-size:.78rem">'
                f'<span style="font-weight:700;font-family:monospace;'
                f'min-width:60px;color:{color}">{a.get("market", "")} {a.get("ticker", "?")}</span>'
                f'<span style="color:#a8cce8;flex:1;overflow:hidden;'
                f'text-overflow:ellipsis;max-width:140px">{a.get("name", "")[:14]}</span>'
                f'<span style="color:#e8f4fd;font-family:monospace;min-width:60px">{close_v:.2f}</span>'
                f'<span style="color:#ffd700;font-family:monospace;min-width:60px">{stars}</span>'
                + qs_html +
                f'<span style="color:{color};flex:1.5;font-size:.72rem">{a.get("tag", "")}</span>'
                f'<span style="color:#7a8899;font-size:.7rem;flex:1">{a.get("expect", "")}</span>'
                f'</div>'
            )

        cols = st.columns(2)
        strong_groups = [
            ('🚀🚀 強看多 ★★★★★', bull5, '#3dbb6a'),
            ('🚀 強看多 ★★★★', bull4, '#3dbb6a'),
            ('⚡ 中強看多 ★★★', bull3, '#7abadd'),
            ('🚨🚨 強看空 ★★★★', bear4, '#ff5555'),
            ('🚨 強看空 ★★★', bear3, '#ff5555'),
            ('⚠️ 中度看空 ★★', bear2, '#e8a020'),
        ]
        with cols[0]:
            st.markdown(
                f'<div style="font-weight:700;color:#ffd700;margin-bottom:4px;'
                f'font-size:.9rem">🎯 強警報觸發中</div>',
                unsafe_allow_html=True)
            has_strong = False
            for label, items, color in strong_groups:
                if not items: continue
                has_strong = True
                st.markdown(
                    f'<div style="color:{color};font-size:.8rem;font-weight:700;'
                    f'margin-top:6px">{label} ({len(items)})</div>'
                    + ''.join(_row_html(a) for a in items[:8]),
                    unsafe_allow_html=True
                )
            if not has_strong:
                st.markdown(
                    f'<div style="color:#7a8899;font-size:.78rem;'
                    f'padding:10px">— 暫無強警報 —</div>',
                    unsafe_allow_html=True
                )

        with cols[1]:
            st.markdown(
                f'<div style="font-weight:700;color:#e8a020;margin-bottom:4px;'
                f'font-size:.9rem">⏰ 即將觸發（離條件 1 步）</div>',
                unsafe_allow_html=True)
            if imm_bull:
                st.markdown(
                    f'<div style="color:#3dbb6a;font-size:.8rem;font-weight:700;'
                    f'margin-top:6px">🌱 即將看多 ({len(imm_bull)})</div>'
                    + ''.join(_row_html(a) for a in imm_bull[:10]),
                    unsafe_allow_html=True
                )
            if imm_bear:
                st.markdown(
                    f'<div style="color:#ff7755;font-size:.8rem;font-weight:700;'
                    f'margin-top:6px">⚠️ 即將看空 ({len(imm_bear)})</div>'
                    + ''.join(_row_html(a) for a in imm_bear[:10]),
                    unsafe_allow_html=True
                )
            if not imm_bull and not imm_bear:
                st.markdown(
                    f'<div style="color:#7a8899;font-size:.78rem;'
                    f'padding:10px">— 暫無即將觸發 —</div>',
                    unsafe_allow_html=True
                )
    except Exception:
        pass


_render_alerts_panel()


def _render_hit_rate_panel():
    """🆕 v9.11：Live 命中率追蹤 — 從 alert_history.json 顯示 5/15/30d 命中率"""
    try:
        from pathlib import Path as _P
        import json as _json
        p = _P(__file__).parent / 'alert_history.json'
        if not p.exists():
            return  # 還沒有歷史資料，不顯示

        h = _json.loads(p.read_text(encoding='utf-8'))
        alerts = h.get('alerts', [])
        stats = h.get('stats', {})
        total = len(alerts)
        if total == 0:
            return

        last_update = h.get('last_outcomes_update', h.get('last_updated', ''))

        # group key → 中文標籤
        label_map = {
            'tw_5_bull':            '🇹🇼 ★★★★★ 看多 (倒鎚+RSI≤25+ADX↑)',
            'tw_4_bull':            '🇹🇼 ★★★★ 看多 (倒鎚+跌深)',
            'tw_3_bull':            '🇹🇼 ★★★ 看多 (底十字+RSI≤25)',
            'tw_4_bear':            '🇹🇼 ★★★★ 看空 (三烏+距高<5%+量縮)',
            'tw_3_bear':            '🇹🇼 ★★★ 看空 (空頭吞噬+RSI≥75)',
            'tw_2_bear':            '🇹🇼 ★★ 看空 (黃昏星+RSI≥75)',
            'tw_imm_bull_bull':     '🇹🇼 ⏰ 即將看多',
            'tw_imm_bear_bear':     '🇹🇼 ⏰ 即將看空',
            'us_5_bull':            '🇺🇸 ★★★★★ 看多',
            'us_4_bull':            '🇺🇸 ★★★★ 看多',
            'us_3_bull':            '🇺🇸 ★★★ 看多',
            'us_4_bear':            '🇺🇸 ★★★★ 看空',
            'us_3_bear':            '🇺🇸 ★★★ 看空',
            'us_2_bear':            '🇺🇸 ★★ 看空',
            'us_imm_bull_bull':     '🇺🇸 ⏰ 即將看多',
            'us_imm_bear_bear':     '🇺🇸 ⏰ 即將看空',
        }

        # 已回算的筆數（任一 horizon 有 ret_pct 就算）
        total_with_outcome = sum(
            1 for a in alerts
            if any(a.get('outcomes', {}).get(f'{n}d', {}).get('ret_pct') is not None
                   for n in [5, 15, 30])
        )

        st.markdown(
            f'<div style="background:#0d1c2e;border:2px solid #2a4a70;'
            f'border-radius:10px;padding:10px 14px;margin:12px 0">'
            f'<div style="font-size:1.05rem;font-weight:700;color:#7abadd;'
            f'margin-bottom:4px">'
            f'📈 Live 命中率追蹤 — 累積 {total} 筆 / 已回算 {total_with_outcome} 筆'
            f'</div>'
            f'<div style="font-size:.7rem;color:#7a8899">'
            f'最後回算：{last_update or "（尚未回算）"}'
            f'</div></div>',
            unsafe_allow_html=True
        )

        if not stats:
            st.markdown(
                f'<div style="color:#7a8899;font-size:.78rem;padding:8px 14px">'
                f'仍在累積資料（5/15/30 天後才有結果）</div>',
                unsafe_allow_html=True)
            return

        # 排序：看多由強→弱，看空由強→弱，imm 最後
        order = [
            'tw_5_bull', 'tw_4_bull', 'tw_3_bull',
            'us_5_bull', 'us_4_bull', 'us_3_bull',
            'tw_4_bear', 'tw_3_bear', 'tw_2_bear',
            'us_4_bear', 'us_3_bear', 'us_2_bear',
            'tw_imm_bull_bull', 'tw_imm_bear_bear',
            'us_imm_bull_bull', 'us_imm_bear_bear',
        ]
        sorted_keys = ([k for k in order if k in stats]
                       + [k for k in stats if k not in order])

        rows_html = []
        for key in sorted_keys:
            s = stats[key]
            label = label_map.get(key, key)
            is_bear = ('bear' in key)
            cells = []
            for hd in ['5d', '15d', '30d']:
                v = s.get(hd)
                if v:
                    n_, hr, mr = v['n'], v['hit_rate'], v['mean_ret']
                    # 看多: ret_pct > 0 = 命中（綠）；看空：ret_pct < 0 = 命中（綠）
                    expected_pos = (mr > 0) if not is_bear else (mr < 0)
                    color = '#3dbb6a' if expected_pos else '#ff5555' if mr != 0 else '#7a8899'
                    cells.append(
                        f'<span style="color:{color};font-family:monospace;'
                        f'font-size:.78rem;min-width:130px;display:inline-block">'
                        f'{hd}: <b>{hr:.0f}%</b> n={n_} ({mr:+.1f}%)</span>'
                    )
                else:
                    cells.append(
                        f'<span style="color:#445566;font-family:monospace;'
                        f'font-size:.78rem;min-width:130px;display:inline-block">{hd}: —</span>'
                    )
            rows_html.append(
                f'<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;'
                f'padding:5px 8px;border-bottom:1px solid #1a2a3a">'
                f'<span style="color:#aac4e0;flex:1;min-width:240px;'
                f'font-size:.82rem">{label}</span>'
                + ''.join(cells)
                + f'</div>'
            )

        st.markdown(
            f'<div style="background:#0a1428;border:1px solid #1f3550;'
            f'border-radius:8px;padding:6px 10px;margin:8px 0">'
            + ''.join(rows_html)
            + '</div>',
            unsafe_allow_html=True
        )

        # 警示文字：樣本太少時提醒
        if total_with_outcome < 10:
            st.markdown(
                f'<div style="color:#e8a020;font-size:.7rem;padding:4px 14px">'
                f'⚠️ 樣本數還小（{total_with_outcome} 筆），建議累積到 ≥30 筆再做決策'
                f'</div>',
                unsafe_allow_html=True
            )
    except Exception as e:
        try:
            st.markdown(
                f'<div style="color:#ff7755;font-size:.7rem;padding:4px 14px">'
                f'命中率 panel 載入失敗：{type(e).__name__}</div>',
                unsafe_allow_html=True)
        except:
            pass


_render_hit_rate_panel()


# 🆕 市場廣度警報（D 路徑）
_breadth = _get_market_breadth()
if _breadth.get('has_data'):
    _level_style = {
        'red':      ('#2a0a00', '#ff5555', '#ffb090'),
        'red_bear': ('#1a0a05', '#ff5555', '#ffa090'),
        'yellow':   ('#1a1200', '#f0a030', '#f0c890'),
        'green':    ('#0a1e10', '#3dbb6a', '#a8e0b8'),
    }.get(_breadth['level'], ('#0a1628', '#7aaac8', '#a0b8c8'))
    _bg, _bord, _fg = _level_style
    st.markdown(
        f'<div style="background:{_bg};border:1px solid {_bord}55;'
        f'border-left:3px solid {_bord};border-radius:6px;'
        f'padding:8px 14px;margin-bottom:1rem;font-size:.74rem;'
        f'color:{_fg}"><b style="color:{_bord}">市場廣度</b> '
        f'{_breadth["msg"]}</div>',
        unsafe_allow_html=True
    )

with st.sidebar:
    # ═══════════════════════════════════════════════════════════════
    # § 1. 股票清單（含自選股管理）
    # ═══════════════════════════════════════════════════════════════
    st.markdown(
        '<div style="font-size:.95rem;font-weight:700;color:#e8f4fd;'
        'margin:4px 0 8px;letter-spacing:.04em">📋 股票清單</div>',
        unsafe_allow_html=True
    )

    # ── 自選股持久化：localStorage（雲端用） + 本地 JSON 雙軌 ──
    import json as _json
    from pathlib import Path as _Path
    _WATCHLIST_FILE = _Path(__file__).parent / 'watchlists.json'

    # localStorage（雲端唯一可持久化選項）
    try:
        from streamlit_local_storage import LocalStorage
        _localS = LocalStorage()
    except Exception:
        _localS = None

    def _load_watchlists() -> dict:
        # ① localStorage 優先（瀏覽器端持久，雲端最可靠）
        if _localS is not None:
            try:
                v = _localS.getItem("stock001_watchlists")
                if v:
                    if isinstance(v, str):
                        return _json.loads(v)
                    if isinstance(v, dict):
                        return v
            except Exception:
                pass
        # ② 檔案 fallback（本地有效）
        if _WATCHLIST_FILE.exists():
            try:
                return _json.loads(_WATCHLIST_FILE.read_text(encoding='utf-8'))
            except Exception:
                return {}
        return {}

    def _save_watchlists(d: dict):
        text = _json.dumps(d, ensure_ascii=False, indent=2)
        # ① 寫 localStorage（雲端持久）
        if _localS is not None:
            try:
                _localS.setItem("stock001_watchlists", text)
            except Exception:
                pass
        # ② 寫檔（本地有效；雲端 ephemeral 但不傷）
        try:
            _WATCHLIST_FILE.write_text(text, encoding='utf-8')
        except Exception:
            pass

    # 從 GitHub 讀取預設清單
    GITHUB_LIST_URL = "https://raw.githubusercontent.com/zeushuan/stock001/main/stocks.txt"
    @st.cache_data(ttl=None, show_spinner=False)
    def load_default_stocks() -> str:
        try:
            r = requests.get(GITHUB_LIST_URL, timeout=6)
            if r.status_code == 200 and r.text.strip():
                lines = [l.strip() for l in r.text.splitlines() if l.strip()]
                return "\n".join(lines)
        except Exception:
            pass
        return "DJI\nSPX\n0050\n2330\n00632R\n00737\nBOTZ"

    default_stocks = load_default_stocks()

    # 載入自選股（每次 rerun 都從持久層重新讀，避免 session_state 過時）
    _wls = _load_watchlists()
    st.session_state['watchlists'] = _wls

    # 自選股下拉
    _wl_options = ["（預設清單）"] + sorted(_wls.keys())
    _selected_wl = st.selectbox(
        "自選股清單", options=_wl_options,
        index=0, key="watchlist_select",
        help="選擇已儲存的清單；下方文字框可編輯後另存新名稱"
    )

    # 依選擇載入清單內容
    if _selected_wl == "（預設清單）":
        _initial_text = default_stocks
    else:
        _initial_text = _wls.get(_selected_wl, default_stocks)

    stock_input = st.text_area(
        "代號（每行一檔）",
        value=_initial_text, height=180,
        key=f"stock_input_{_selected_wl}",
        help="台股：純數字或含字母 ETF（2330/00878/00632R）；美股：英文代號（NVDA/SPY）；# 開頭為註解",
    )

    # 儲存 / 刪除（更緊湊的 2 欄佈局）
    _c1, _c2 = st.columns([2, 1])
    with _c1:
        _save_name = st.text_input(
            "新清單名稱",
            placeholder="輸入名稱…",
            label_visibility="collapsed",
            key="wl_save_name"
        )
    with _c2:
        if st.button("💾 存", use_container_width=True, key="wl_save_btn"):
            n = _save_name.strip()
            if not n:
                st.warning("請先輸入清單名稱")
            else:
                _wls[n] = stock_input
                _save_watchlists(_wls)
                st.success(f"✓ 已存「{n}」")
                st.rerun()

    if _selected_wl != "（預設清單）":
        if st.button(f"🗑 刪除「{_selected_wl}」",
                     use_container_width=True, key="wl_del_btn"):
            _wls.pop(_selected_wl, None)
            _save_watchlists(_wls)
            st.success(f"✓ 已刪除「{_selected_wl}」")
            st.rerun()

    # ── 🆕 匯出 / 匯入 JSON 永久備份 ────────────────────────────
    with st.expander("💾 備份 / 還原 自選股清單", expanded=False):
        st.caption(
            "瀏覽器 localStorage 可能因清快取/隱私模式/閒置等原因消失。"
            "建議定期匯出 JSON 永久保存。"
        )
        # 匯出
        if _wls:
            _export_text = _json.dumps(_wls, ensure_ascii=False, indent=2)
            st.download_button(
                "📥 匯出全部清單（JSON）",
                data=_export_text,
                file_name="watchlists_backup.json",
                mime="application/json",
                use_container_width=True,
                key="wl_export_btn",
            )
        else:
            st.caption("（目前無清單可匯出）")
        # 匯入
        _uploaded = st.file_uploader(
            "📤 匯入 JSON 檔還原清單",
            type=['json'], key="wl_import_uploader"
        )
        if _uploaded is not None:
            try:
                _imported = _json.loads(_uploaded.read().decode('utf-8'))
                if isinstance(_imported, dict):
                    _merge_mode = st.radio(
                        "匯入方式",
                        ["合併（保留現有）", "覆蓋（清空再匯入）"],
                        horizontal=True, key="wl_import_mode"
                    )
                    if st.button("✅ 確認匯入", type="primary",
                                 use_container_width=True,
                                 key="wl_import_confirm"):
                        if _merge_mode.startswith("覆蓋"):
                            _wls = _imported
                        else:
                            _wls = {**_imported, **_wls}
                        _save_watchlists(_wls)
                        st.success(f"✓ 已匯入 {len(_imported)} 個清單"
                                   f"（合併後總共 {len(_wls)}）")
                        st.rerun()
                else:
                    st.error("檔案格式錯誤（不是字典結構）")
            except Exception as e:
                st.error(f"解析失敗：{e}")

    if _localS is not None:
        st.caption(
            "💾 localStorage 儲存於瀏覽器（建議定期匯出備份）"
        )

    st.markdown("---")

    # ═══════════════════════════════════════════════════════════════
    # § 2. 分析設定
    # ═══════════════════════════════════════════════════════════════
    st.markdown(
        '<div style="font-size:.95rem;font-weight:700;color:#e8f4fd;'
        'margin:4px 0 8px;letter-spacing:.04em">⚙️ 分析設定</div>',
        unsafe_allow_html=True
    )

    # ── 日期 ──
    import datetime as _dt
    use_hist = st.checkbox("📅 指定歷史日期", value=False, key="use_hist_date")
    if use_hist:
        hist_date = st.date_input(
            "選擇日期",
            value=_dt.date.today() - _dt.timedelta(days=1),
            min_value=_dt.date(2010, 1, 1),
            max_value=_dt.date.today() - _dt.timedelta(days=1),
            label_visibility="collapsed",
        )
        selected_end_date = hist_date.isoformat()
        st.markdown(
            f'<div style="font-size:.68rem;color:#f0a030;background:#1a1200;'
            f'border:1px solid #6a4a00;border-radius:6px;padding:4px 8px;'
            f'margin:2px 0 6px">⏱ 歷史模式：{hist_date.strftime("%Y-%m-%d")}</div>',
            unsafe_allow_html=True)
    else:
        selected_end_date = ""

    # ── 策略風格 ──
    # 🆕 v9.10n：加入 TW/US 真正最佳選項，並提示哪個對應哪個市場
    _STRATEGY_OPTIONS = [
        "⭐ TW 最佳 (P5+VWAPEXEC)",      # 🆕 TW 真正最佳
        "⭐ US 最佳 (P10+POS+ADX18)",    # 🆕 US 真正最佳
        "🛡️ 極致風控 (IND+DXY)",
        "🛟 超低風險 (五重保護)",
        "🌊 保守 (POS+DXY)",
        "⚖️ 平衡 (POS)",
        "🤖 RL 智能加碼",
        "🚀 進攻 (P0_T1T3)",
    ]
    strategy_style = st.selectbox(
        "🎯 策略風格", options=_STRATEGY_OPTIONS, index=0,
        key="strategy_style",
        help=("⭐ TW 最佳：搜台股 (4 位數) 用此｜"
              "⭐ US 最佳：搜美股 (英文 ticker) 用此｜"
              "其他 6 個是早期風格（TEST 期 RR 都不到 0.25）")
    )

    st.markdown("<div style='margin:10px 0'></div>", unsafe_allow_html=True)
    fetch_btn = st.button("🔍  開始抓取資料", type="primary",
                          use_container_width=True)
    st.markdown("---")

    # ── 🔎 市場掃描器（v9.0 新增）──────────────────────────────
    with st.expander("🔎 市場掃描器（找進場候選）", expanded=False):
        scan_market = st.radio(
            "市場", options=["🇹🇼 台股", "🇺🇸 美股"],
            horizontal=True, key="scan_market_choice"
        )
        scan_signal_filter = st.multiselect(
            "信號類型篩選",
            options=["T1 黃金交叉", "T3 多頭拉回", "🟢 多頭觀察"],
            default=["T1 黃金交叉", "T3 多頭拉回"],
            key="scan_signal_filter",
        )

        # 產業別篩選：依市場列出可選類別
        @st.cache_data(ttl=86400, show_spinner=False)
        def _industry_options(market_choice: str) -> list:
            """依市場回傳該市場存在的產業類別清單（去重排序）"""
            from pathlib import Path as _P
            base = _P(__file__).parent
            out = set()
            if "台股" in market_choice or "TW" in market_choice:
                p = base / 'tw_universe.txt'
                if p.exists():
                    for line in p.read_text(encoding='utf-8').splitlines():
                        if not line or line.startswith('#'): continue
                        parts = line.split('|')
                        if len(parts) >= 5 and parts[4]:
                            out.add(parts[4])
                        elif len(parts) >= 3 and parts[2] in (
                                'ETF','ETN','特別股'):
                            out.add(parts[2])
            else:
                p = base / 'us_sectors.txt'
                if p.exists():
                    for line in p.read_text(encoding='utf-8').splitlines():
                        if not line or line.startswith('#'): continue
                        parts = line.split('|')
                        if len(parts) >= 3:
                            sector = parts[2]
                            if sector and sector != 'NO_SECTOR':
                                out.add(_US_SECTOR_ZH.get(sector, sector))
            return sorted(out)

        scan_industry_filter = st.multiselect(
            "產業別篩選（不選=全部）",
            options=_industry_options(scan_market),
            default=[],
            key="scan_industry_filter",
            help="只篩選選中的產業；不選代表掃描全市場"
        )

        scan_btn = st.button("🚀 開始掃描", use_container_width=True,
                             key="scan_btn")

    st.markdown("---")

    # 各風格對應的描述（v9.10n 更新：加 TW/US 最佳 + TEST 期實測數字）
    # 數字來源：test_strategy_styles_compare.py 全市場 6 年回測（2026-04-29）
    # mean = TEST 22 月平均報酬 / sharpe = TEST RR
    _style_meta = {
        # 🆕 真正最佳（TEST out-of-sample 實測）
        "⭐ TW 最佳 (P5+VWAPEXEC)": dict(
            mode="P5_T1T3+POS+IND+DXY+VWAPEXEC",
            color="#ffd700",
            icon="⭐",
            mean=39.3, low=-64.4, sharpe=0.611, sigma=8.0,
            note="🇹🇼 全 1042 檔 TEST 22 月 RR 0.611（勝率 56% / 中位 +2.3%）｜真正最佳｜配 TOP 200 tier 年化 +110%｜需 vwap_cache (5-min bar)",
        ),
        "⭐ US 最佳 (P10+POS+ADX18)": dict(
            mode="P10_T1T3+POS+ADX18",
            color="#ffd700",
            icon="⭐",
            mean=49.5, low=-99.9, sharpe=0.496, sigma=10.0,
            note="🇺🇸 高流動 555 檔 TEST 22 月 RR 0.496（勝率 55% / 中位 +3.2%）｜vs SPY +6pp 真實 alpha｜需 ADV ≥ $104M 篩選",
        ),
        # 早期風格（保留，TEST RR 標記顯示衰退）
        "🛟 超低風險 (五重保護)": dict(
            mode="P0_T1T3+POS+IND+DXY+WRSI+WADX",
            color="#ff6dc8",
            icon="🛟",
            mean=27.0, low=-111.9, sharpe=0.241, sigma=8.50,
            note="🇹🇼 only｜TEST RR 0.241｜五重保護：POS+產業跨市場+DXY+週RSI+週ADX。最低風險僅 -88（史上最低）",
        ),
        "🤖 RL 智能加碼": dict(
            mode="P0_T1T3+RL",
            color="#10c0c0",
            icon="🤖",
            mean=29.2, low=-152.5, sharpe=0.191, sigma=11.5,
            note="🇹🇼 TEST RR 0.191｜🇺🇸 TEST RR 0.324｜Q-Learning 自動加碼。US 表現勝 TW 1.7×",
        ),
        "🛡️ 極致風控 (IND+DXY)": dict(
            mode="P0_T1T3+POS+IND+DXY",
            color="#9d6dff",
            icon="🛡️",
            mean=31.2, low=-139.6, sharpe=0.223, sigma=7.89,
            note="🇹🇼 only｜TEST RR 0.223｜產業 specific 跨市場 + DXY。FULL 6 年 RR 1.20 但 TEST 大幅衰退",
        ),
        "🌊 保守 (POS+DXY)":  dict(
            mode="P0_T1T3+POS+DXY",
            color="#3dbb6a",
            icon="🌊",
            mean=27.6, low=-139.6, sharpe=0.198, sigma=7.67,
            note="🇹🇼 only｜TEST RR 0.198｜弱美元 + 累積為正加碼。2022 熊市保護有效",
        ),
        "⚖️ 平衡 (POS)": dict(
            mode="P0_T1T3+POS",
            color="#3b9eff",
            icon="⚖️",
            mean=15.8, low=-176.0, sharpe=0.090, sigma=8.89,
            note="🇹🇼 TEST RR 0.090（最差）｜🇺🇸 TEST RR 0.341｜美股表現勝台股 3.8×（無 VWAPEXEC 時 TW 表現差）",
        ),
        "🚀 進攻 (P0_T1T3)": dict(
            mode="P0_T1T3",
            color="#f0c030",
            icon="🚀",
            mean=42.0, low=-187.7, sharpe=0.224, sigma=12.7,
            note="🇹🇼 TEST RR 0.224｜🇺🇸 TEST RR 0.324｜不限門檻最大化獲利但變動大",
        ),
    }
    style_info = _style_meta[strategy_style]
    # 🆕 v9.10n：sharpe 重新定義為 TEST RR（更有意義的指標）
    st.markdown(
        f'<div style="background:#0a1628;border:1px solid {style_info["color"]}55;'
        f'border-radius:6px;padding:8px 12px;margin-top:6px;font-size:.68rem;line-height:1.5">'
        f'<div style="color:{style_info["color"]};font-weight:700;margin-bottom:3px">'
        f'{style_info["icon"]} {style_info["mode"]}</div>'
        f'<div style="color:#7aaac8">'
        f'TEST 均報 <b style="color:{style_info["color"]}">+{style_info["mean"]:.1f}%</b> ｜ '
        f'最差 <b style="color:#ff5555">{style_info["low"]:+.1f}%</b><br>'
        f'TEST RR <b style="color:#f0c030">{style_info["sharpe"]:.3f}</b> ｜ '
        f'跨年σ <b style="color:#7abadd">{style_info["sigma"]:.1f}</b><br>'
        f'<span style="color:#5a8ab0;font-size:.67rem">{style_info["note"]}</span>'
        f'</div></div>',
        unsafe_allow_html=True
    )
    st.session_state['active_strategy'] = style_info

    st.markdown("---")
    st.markdown("""
<div style="font-size:.75rem;color:#8ab8d8;font-weight:700;letter-spacing:.06em;text-transform:uppercase;margin-bottom:6px">
  📖 使用說明
</div>
<div style="font-size:.72rem;color:#5a8ab0;line-height:2;background:#0a1220;border-radius:8px;padding:10px 12px;border:1px solid #1a2f48">
  <span style="color:#3b9eff;font-weight:700">①</span> 在上方輸入欲掃描的股票代號<br>
  <span style="color:#3b9eff;font-weight:700">②</span> 點擊「開始抓取資料」按鈕<br>
  <span style="color:#3b9eff;font-weight:700">③</span> 查看右側指標總表與個股詳情<br>
  <span style="color:#3b9eff;font-weight:700">④</span> 點擊代號連結 → AI 技術分析<br>
  <span style="color:#3b9eff;font-weight:700">⑤</span> 點擊名稱連結 → TradingView 圖表<br>
  <span style="color:#3b9eff;font-weight:700">⑥</span> 底部可下載 Excel 完整報告
</div>
<div style="font-size:.68rem;color:#334455;line-height:1.8;margin-top:10px">
  <b style="color:#3a5a7a">判斷說明</b><br>
  <span style="color:#3b9eff">■</span> 買入 / 強力買入：多頭訊號<br>
  <span style="color:#ff5555">■</span> 賣出 / 強力賣出：空頭訊號<br>
  <span style="color:#556677">■</span> 中立：無明確方向<br><br>
  <b style="color:#3a5a7a">資料來源</b>：Yahoo Finance<br>
  台股自動加 .TW（含反/槓桿 ETF）<br><br>
  <b style="color:#3a5a7a">震盪指標（12）</b><br>
  RSI · 隨機%K · CCI · ADX · AO<br>動量 · MACD · StochRSI · 威廉%R<br>牛熊力度 · 終極震盪 · 布林%B<br><br>
  <b style="color:#3a5a7a">移動均線（15）</b><br>
  EMA/SMA 10/20/30/50/100/200<br>一目均衡 · VWMA · Hull MA
</div>""", unsafe_allow_html=True)

# ── 版本標記：格式變更時自動清除舊快取 ──────────────────────────
_RESULTS_VERSION = 92  # v9.11：T1 觸發至今 % 變化（市場環境 + T1 row 都顯示累計報酬） 2026-04-30
if st.session_state.get("results_version") != _RESULTS_VERSION:
    for _k in ["results", "debug_msgs"]:
        st.session_state.pop(_k, None)
    # ⭐ 同步清 @st.cache_data 函式快取，讓資料結構變更立即生效（VWAP 等）
    try:
        fetch_indicators.clear()
    except Exception:
        pass
    try:
        fetch_indicators_range.clear()
    except Exception:
        pass
    # 🆕 v9.9c：清 Portfolio TOP 200 推薦快取（NLP 改進等格式變更需重算）
    try:
        _scan_top200_signals.clear()
        _load_vwap_applicable.clear()
        _load_otc_tickers.clear()
        get_news_sentiment.clear()
    except Exception:
        pass
    st.session_state["results_version"] = _RESULTS_VERSION

# ── 🔎 市場掃描器處理（v9.0 新增）─────────────────────────────
if scan_btn:
    is_tw = (scan_market == "🇹🇼 台股")
    st.markdown(f"""
<div style="background:#0d1b2e;border:1px solid #1e3a5f;border-radius:10px;
            padding:16px 20px;margin-bottom:1rem">
  <div style="font-size:1.05rem;font-weight:700;color:#8ab8d8">
    🔎 市場掃描中 {'🇹🇼 台股' if is_tw else '🇺🇸 美股'}
  </div>
  <div style="font-size:.75rem;color:#5a8ab0;margin-top:4px">
    策略：{style_info['icon']} {style_info['mode']}
  </div>
</div>""", unsafe_allow_html=True)

    progress = st.progress(0.0, text="準備中...")
    scan_results = []

    # 通用 yfinance 即時掃描函數（雲端 fallback / 美股皆用）
    # 改用 yf.download 批次下載（50/批），大幅加速雲端全市場掃描
    def _scan_via_yfinance(tickers, suffix="", is_tw_market=False,
                           batch_size=80):
        import yfinance as _yf
        import pandas as _pd
        import numpy as _np
        total = len(tickers)
        out = []
        if is_tw_market:
            tw_names = _get_tw_names()
            # 補入靜態 fallback（雲端 twstock 載不齊時）
            tw_names = {**(st.session_state.get('_tw_static_names') or {}),
                        **tw_names}
        else:
            # 美股：合併內建 + 靜態檔
            us_names_full = {**US_NAMES,
                             **(st.session_state.get('_us_static_names') or {})}

        def _eval_one(t, h):
            try:
                if h is None or len(h) < 60: return None
                # 必要欄位：Close + High + Low（算 ADX）
                if 'High' not in h.columns or 'Low' not in h.columns:
                    return None
                df_x = h[['Open','High','Low','Close']].dropna()
                if len(df_x) < 60: return None
                pr = df_x['Close'].values
                hi = df_x['High'].values
                lo = df_x['Low'].values
                s = _pd.Series(pr)
                e20 = s.ewm(span=20, adjust=False).mean().values
                e60 = s.ewm(span=60, adjust=False).mean().values
                delta = s.diff()
                gain = delta.where(delta > 0, 0.0).rolling(14).mean()
                loss = -delta.where(delta < 0, 0.0).rolling(14).mean()
                rs = gain / loss
                rsi = (100 - 100/(1+rs)).values

                # 計算 ADX（標準 14 期）— 排除假多頭關鍵指標
                try:
                    from ta.trend import ADXIndicator
                    adx_arr = ADXIndicator(
                        high=df_x['High'], low=df_x['Low'],
                        close=df_x['Close'], window=14, fillna=False,
                    ).adx().values
                    cur_adx = float(adx_arr[-1]) if not _np.isnan(adx_arr[-1]) else None
                except Exception:
                    cur_adx = None

                ADX_TH = 22  # 趨勢強度門檻：< 22 視為盤整／假多頭

                last = len(pr) - 1
                is_bull = e20[last] > e60[last]
                is_t1 = (last >= 1 and e20[last-1] <= e60[last-1] and
                         e20[last] > e60[last])
                cur_rsi = float(rsi[last]) if not _np.isnan(rsi[last]) else None
                adx_ok = cur_adx is not None and cur_adx >= ADX_TH

                if is_t1 and adx_ok:
                    sig, score = "T1 黃金交叉", 90
                elif is_bull and adx_ok and cur_rsi and cur_rsi < 60:
                    sig, score = "🟢 多頭觀察", 50
                elif is_bull and adx_ok and cur_rsi and cur_rsi >= 75:
                    sig, score = "⚠️ 過熱（RSI≥75）", 30
                elif is_bull and not adx_ok:
                    # 假多頭：不輸出（後面 sig in scan_signal_filter 會過濾掉）
                    sig, score = "⚠️ 假多頭（ADX弱）", 15
                else:
                    sig, score = "🔴 空頭", 10
                # 假多頭直接丟棄
                if sig == "⚠️ 假多頭（ADX弱）": return None
                if sig not in scan_signal_filter: return None
                cur_pr = float(pr[last])
                prev_pr = float(pr[last-1]) if last >= 1 else cur_pr
                chg_pct = (cur_pr - prev_pr) / prev_pr * 100 if prev_pr else 0
                name = tw_names.get(t, "") if is_tw_market \
                       else us_names_full.get(t, "")
                # TradingView 連結（台股 .TW → TWSE/TPEX 前綴；美股直接用代號）
                if is_tw_market:
                    tv_url = f"https://www.tradingview.com/chart/?symbol=TWSE:{t}"
                else:
                    tv_url = f"https://www.tradingview.com/chart/?symbol={t}"
                return dict(
                    ticker=t, name=name,
                    date=str(h.index[-1].date()),
                    price=round(cur_pr, 2),
                    change_pct=round(chg_pct, 2),
                    signal=sig,
                    industry=_get_industry(t),
                    adx=round(cur_adx, 1) if cur_adx else None,
                    is_bull=is_bull,
                    tv_url=tv_url,
                )
            except Exception:
                return None

        # 批次下載
        first_err = None
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch = tickers[batch_start:batch_end]
            yf_syms = [f"{t}{suffix}" if suffix else t for t in batch]
            progress.progress(
                batch_start / total,
                text=f"批次下載 {batch_start+1}-{batch_end}/{total}…"
            )
            try:
                df_all = _yf.download(
                    yf_syms,
                    period="1y", auto_adjust=True,
                    group_by='ticker', threads=True,
                    progress=False,
                )
            except Exception as e:
                if first_err is None:
                    first_err = f"download err: {type(e).__name__}: {e}"
                continue
            if df_all is None or df_all.empty:
                if first_err is None:
                    first_err = f"batch {batch_start}: 空 DataFrame"
                continue
            for t, sym in zip(batch, yf_syms):
                try:
                    if len(yf_syms) > 1:
                        if sym not in df_all.columns.get_level_values(0):
                            continue
                        h = df_all[sym].dropna(how='all')
                    else:
                        h = df_all
                    r = _eval_one(t, h)
                    if r: out.append(r)
                except Exception as e:
                    if first_err is None:
                        first_err = f"eval {t}: {type(e).__name__}: {e}"
                    continue
        progress.progress(1.0, text=f"完成 {total} 檔")
        # 回傳除錯資訊
        if not out and first_err:
            st.session_state['_scan_first_err'] = first_err
        return out

    # ── 載入台股全市場（優先從靜態檔，再嘗試 twstock）
    @st.cache_data(ttl=86400, show_spinner=False)
    def _get_all_tw_universe() -> tuple:
        """回傳 (tickers, name_map)。
        優先從 repo 內 tw_universe.txt 載入（雲端最可靠），
        失敗 fallback 到 twstock.codes。"""
        from pathlib import Path as _P2
        # ① 靜態檔（已 commit 至 repo）
        f = _P2(__file__).parent / 'tw_universe.txt'
        if f.exists():
            try:
                tickers, names = [], {}
                for line in f.read_text(encoding='utf-8').splitlines():
                    if not line or line.startswith('#'): continue
                    parts = line.split('|')
                    if len(parts) >= 2:
                        tickers.append(parts[0])
                        names[parts[0]] = parts[1]
                if tickers:
                    return sorted(set(tickers)), names
            except Exception:
                pass
        # ② twstock 動態載入
        try:
            import twstock
            tickers = []; names = {}
            for k, v in twstock.codes.items():
                if v.type in ('股票', 'ETF', 'ETN', '臺灣存託憑證(TDR)', '特別股'):
                    tickers.append(str(k))
                    names[str(k)] = v.name
            return sorted(set(tickers)), names
        except Exception:
            return [], {}

    # ── 動態載入：美股全市場 + 名稱對照
    @st.cache_data(ttl=86400, show_spinner=False)
    def _get_all_us_universe() -> tuple:
        """回傳 (tickers, name_map)。
        優先從 repo us_names.txt 讀（含名稱），失敗 fallback 到 GitHub 純代號清單。"""
        from pathlib import Path as _P3
        # ① 靜態檔（已 commit，含 8000+ 代號 + 名稱）
        f = _P3(__file__).parent / 'us_names.txt'
        if f.exists():
            try:
                tickers, names = [], {}
                for line in f.read_text(encoding='utf-8').splitlines():
                    if not line or line.startswith('#'): continue
                    parts = line.split('|', 1)
                    if len(parts) >= 2:
                        sym = parts[0].strip()
                        # yfinance 不支援的特殊代號跳過
                        if any(c in sym for c in ('$','/','^','.','=')): continue
                        if not sym.replace('-','').isalpha(): continue
                        tickers.append(sym)
                        names[sym] = parts[1].strip()
                if tickers:
                    return sorted(set(tickers)), names
            except Exception:
                pass
        # ② GitHub 純代號 fallback
        url = ("https://raw.githubusercontent.com/rreichel3/"
               "US-Stock-Symbols/main/all/all_tickers.txt")
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200: return [], {}
            out = set()
            for line in r.text.splitlines():
                s = line.strip().upper()
                if not s or len(s) > 6: continue
                if any(c in s for c in ('$','/','^','.','=')): continue
                if not s.replace('-','').isalpha(): continue
                out.add(s)
            return sorted(out), {}
        except Exception:
            return [], {}

    # 個股精選清單（雲端 fallback；twstock 失敗時使用）
    TW_INDIVIDUAL_TICKERS = [
        # 半導體
        "2330","2454","2303","3711","6669","3034","3017","3661","6488","8081",
        "6552","2382","3037","2379","6770","3231","2451","2441","8299","6239",
        "2408","6515","3035","3014","6147","6438","3325","3658","3105","6285",
        # 電子/PC/伺服器/網通/EMS
        "2317","2308","2474","2376","2356","3596","2353","2324","2354",
        "3702","3045","2412","4904","3045","2383","2385","2360","6230",
        "8261","6669","6781","2492","3653","6285","8210","3596","3088",
        # 金融
        "2881","2882","2891","2884","2885","2886","2887","2890","2892","5880",
        "2883","2880","2812","2823","2845","2849","2855","2867","2888","2889",
        # 傳產/食品/塑化/紡織/水泥/航運/鋼鐵
        "1101","1102","2002","1216","1301","1303","2105","6505","9904","2912",
        "2207","1326","1722","1227","1402","1907","2027","2014","2030",
        "2603","2609","2615","2618","2606","2606","9904","9910","9921",
        "1605","2812","2371","2362","2438","6121","2049","2049","6213",
        # 生技/光電/車用/其他
        "3008","4123","6446","4128","6691","4736","6446","4906","8454",
        "1519","2059","2049","8086","5483","3443","2059","1907","6121",
    ]

    # 動態取得所有上市/上櫃 ETF（00 開頭）
    @st.cache_data(ttl=3600, show_spinner=False)
    def _get_all_tw_etfs() -> list:
        try:
            import twstock
            etfs = []
            for code, info in twstock.codes.items():
                # ETF 篩選：代號以 00 開頭或 group=='ETF'
                if (str(code).startswith('00') or
                    getattr(info, 'group', '') == 'ETF' or
                    'ETF' in (getattr(info, 'name', '') or '')):
                    etfs.append(str(code))
            return sorted(set(etfs))
        except Exception:
            # fallback：手動清單
            return [
                "0050","0051","0052","0053","0055","0056","0057","0061",
                "006201","006203","006204","006208",
                "00631L","00632R","00633L","00634R","00635U","00636",
                "00637L","00638R","00640L","00641R","00642U","00643K",
                "00646","00650L","00651R","00652","00655L","00656R","00657",
                "00660","00661","00662","00663L","00664R","00665L","00666R",
                "00668","00670L","00671R","00672L","00673R","00674R",
                "00675L","00676R","00678","00679B","00680L","00681R","00682U",
                "00687B","00688L","00689R","00690","00691R","00692","00693U",
                "00694B","00695B","00696B","00697B","00700","00701","00709",
                "00710B","00711B","00712","00713","00714","00715L","00717",
                "00718B","00720B","00724B","00725B","00727B","00730","00731",
                "00733","00735","00736","00737","00739","00740B","00742",
                "00750","00751B","00752","00755","00757","00762","00763U",
                "00770","00771","00773B","00775B","00777","00778B","00783",
                "00850","00851","00861","00865B","00876","00878","00881",
                "00885","00891","00892","00893","00894","00895","00896",
                "00900","00901","00904","00905","00906","00907","00909",
                "00911","00912","00913","00915","00916","00918","00919",
                "00920","00921","00922","00923","00925","00927B","00929",
                "00930","00931B","00932","00933","00934B","00935B","00936",
                "00937B","00939","00940","00941","00943","00944","00945",
                "00946B","00947B","00948","00949","00951","00952","00953B",
                "00954B","00955B","00956B","00957B",
            ]

    # 美股大幅擴充：個股 ~150 + ETF ~80
    US_INDIVIDUAL_TICKERS = [
        # 科技七雄 + 半導體
        "AAPL","MSFT","NVDA","GOOGL","GOOG","AMZN","META","TSLA",
        "AVGO","AMD","INTC","QCOM","TXN","MU","AMAT","LRCX","KLAC","ADI",
        "MRVL","ASML","TSM","ARM","SMCI","ON","NXPI","MCHP","SWKS","QRVO",
        # 軟體 / 雲 / SaaS
        "ORCL","CRM","ADBE","NOW","INTU","WDAY","SNOW","PLTR","DDOG","NET",
        "MDB","CRWD","ZS","OKTA","TEAM","ZM","PANW","FTNT","CYBR",
        "SHOP","SQ","PYPL","COIN","HOOD","SOFI","AFRM","UPST",
        # 網路 / 媒體 / 通訊
        "NFLX","DIS","CMCSA","T","VZ","TMUS","CHTR","WBD","PARA","ROKU",
        "SPOT","PINS","SNAP","RBLX","EA","TTWO","UBER","LYFT","ABNB","DASH",
        # 金融
        "JPM","BAC","WFC","C","GS","MS","BLK","SCHW","AXP","COF",
        "USB","PNC","TFC","BK","STT","MET","PRU","AIG","TRV","ALL",
        "V","MA","FIS","FISV","ICE","CME","SPGI","MCO","MSCI",
        # 消費 / 零售 / 餐飲
        "WMT","COST","HD","LOW","TGT","DG","DLTR","BJ","KR",
        "MCD","SBUX","CMG","YUM","DPZ","WEN","QSR","MNST","KO","PEP",
        "PG","CL","KMB","CHD","CLX","UL","EL","NKE","LULU","UAA","DECK",
        # 工業 / 國防 / 運輸
        "BA","CAT","DE","GE","HON","LMT","RTX","NOC","GD","BAH",
        "UNP","CSX","NSC","UPS","FDX","JBHT","CHRW",
        # 能源 / 原物料
        "XOM","CVX","COP","PSX","SLB","HAL","BKR","OXY","EOG","DVN",
        "FCX","NEM","GOLD","AA","CLF","X","MP","LIN","APD","SHW",
        # 醫療 / 藥廠 / 生技
        "JNJ","PFE","MRK","ABBV","BMY","LLY","TMO","ABT","DHR","UNH",
        "CVS","CI","HUM","ANTM","MDT","SYK","BSX","ISRG","ZBH","BAX",
        "AMGN","GILD","BIIB","REGN","VRTX","MRNA","BNTX","NVAX",
        # 公用 / REIT
        "NEE","DUK","SO","D","AEP","EXC","SRE","XEL","ED",
        "AMT","PLD","CCI","EQIX","DLR","SPG","O","WELL","PSA","AVB",
    ]

    US_ETF_TICKERS = [
        # 大盤指數
        "SPY","VOO","IVV","QQQ","DIA","IWM","VTI","VT","VEA","VWO",
        # 板塊 SPDR
        "XLK","XLF","XLE","XLV","XLI","XLY","XLP","XLU","XLB","XLRE","XLC",
        # 半導體 / 科技主題
        "SMH","SOXX","SOXL","TQQQ","SQQQ","TECL","TECS","FNGU","FNGD",
        "BOTZ","ROBO","ARKK","ARKW","ARKG","ARKQ","ARKF","ARKX",
        "IGV","HACK","CIBR","CLOU","SKYY","WCLD","FDN",
        # 因子 / 風格
        "MTUM","QUAL","USMV","VLUE","SIZE","MOAT","SPLV","SPHQ",
        # 國際
        "EFA","EEM","FXI","INDA","EWJ","EWZ","EWG","EWU","EWT","RSX",
        # 固定收益
        "AGG","BND","TLT","IEF","SHY","LQD","HYG","JNK","MUB","TIP",
        # 商品 / 黃金 / 能源
        "GLD","IAU","SLV","USO","UNG","DBA","DBC","UUP","FXE","FXY",
        # 房地產 / REIT
        "VNQ","SCHH","REM","MORT",
        # 高股息 / 收益
        "SCHD","VYM","HDV","DVY","SPHD","PFF","DGRO","NOBL",
        # 中小型 / 微型
        "VB","IJH","IJR","SCHA","SCHM",
    ]
    US_HOT_TICKERS = US_INDIVIDUAL_TICKERS + US_ETF_TICKERS

    if is_tw:
        # 台股：優先使用本地 data_cache，雲端 fallback 至 yfinance
        local_ok = False
        try:
            import data_loader as _dl
            import variant_strategy as _vs
            import daily_scanner as _ds
            cache_dir = _dl.CACHE_DIR
            files = sorted(cache_dir.glob('*.parquet'))
            # 產業篩選（本地入口）
            if files and scan_industry_filter:
                files = [fp for fp in files
                         if _get_industry(fp.stem) in scan_industry_filter]
            if files:
                tw_names = _get_tw_names()
                mode = style_info['mode']
                total = len(files)
                for i, fp in enumerate(files):
                    if i % 20 == 0:
                        progress.progress(i / total,
                                          text=f"掃描中 {i+1}/{total}…（本地快取）")
                    ticker = fp.stem
                    try:
                        r = _ds.scan_one(ticker, str(fp), mode=mode)
                        if r is not None and r.get('signal') in scan_signal_filter:
                            # 補上名稱與漲跌
                            try:
                                df_local = pd.read_parquet(fp)
                                if len(df_local) >= 2:
                                    cur_p = float(df_local['Close'].iloc[-1])
                                    prev_p = float(df_local['Close'].iloc[-2])
                                    r['change_pct'] = round(
                                        (cur_p - prev_p) / prev_p * 100, 2) \
                                        if prev_p else 0
                            except Exception:
                                r['change_pct'] = None
                            r['name'] = tw_names.get(ticker, "")
                            r['price'] = round(r.get('price', 0), 2)
                            r['industry'] = _get_industry(ticker)
                            r['tv_url'] = f"https://www.tradingview.com/chart/?symbol=TWSE:{ticker}"
                            # 移除掃描表不需要的欄位
                            r.pop('score', None)
                            r.pop('rsi', None)
                            scan_results.append(r)
                    except Exception:
                        continue
                progress.progress(1.0, text=f"完成 {total} 檔（本地快取）")
                local_ok = True
        except ImportError:
            pass

        if not local_ok:
            # 雲端：載入台股全市場（優先 tw_universe.txt → twstock → 手動）
            full, tw_static_names = _get_all_tw_universe()
            if not full:
                full = list(dict.fromkeys(
                    TW_INDIVIDUAL_TICKERS + _get_all_tw_etfs()))
                tw_static_names = {}
            st.session_state['_tw_static_names'] = tw_static_names
            # 產業篩選（雲端入口）
            if scan_industry_filter:
                full = [t for t in full if _get_industry(t) in scan_industry_filter]
                st.info(
                    f"📡 雲端模式：依產業篩選後掃描 **{len(full)}** 檔"
                    f"（產業：{'、'.join(scan_industry_filter)}）"
                )
            else:
                st.info(
                    f"📡 雲端模式：批次掃描台股全市場 **{len(full)}** 檔"
                    f"（含上市/上櫃/ETF/ETN/TDR/特別股）"
                )
            scan_results = _scan_via_yfinance(full, suffix=".TW",
                                              is_tw_market=True)
    else:
        # 美股：靜態檔 us_names.txt（~8000 檔，含名稱）
        full_us, us_static_names = _get_all_us_universe()
        if full_us and len(full_us) > 1000:
            extra_hot = list(dict.fromkeys(US_HOT_TICKERS + full_us))
            st.session_state['_us_static_names'] = us_static_names
            # 產業篩選（美股）
            if scan_industry_filter:
                extra_hot = [t for t in extra_hot
                             if _get_industry(t) in scan_industry_filter]
                st.info(
                    f"📡 依產業篩選後掃描美股 **{len(extra_hot)}** 檔"
                    f"（產業：{'、'.join(scan_industry_filter)}）"
                )
            else:
                st.info(
                    f"📡 即時抓取美股全市場 **{len(extra_hot)}** 檔"
                    f"（NASDAQ + NYSE + AMEX，預估 3-5 分鐘）"
                )
            scan_results = _scan_via_yfinance(extra_hot, is_tw_market=False)
        else:
            st.info(
                f"📡 即時抓取美股 {len(US_HOT_TICKERS)} 檔"
                f"（個股 {len(US_INDIVIDUAL_TICKERS)} + ETF {len(US_ETF_TICKERS)}）"
                f"（無法載入 NASDAQ 全清單，使用內建熱門池）"
            )
            scan_results = _scan_via_yfinance(US_HOT_TICKERS, is_tw_market=False)

    if scan_results:
        df_scan = pd.DataFrame(scan_results)
        if 'score' in df_scan.columns:
            df_scan = df_scan.sort_values('score', ascending=False)
        else:
            df_scan = df_scan.sort_values('change_pct', ascending=False)
        df_scan = df_scan.reset_index(drop=True)
        # 移除掃描表不需要顯示的欄位
        for col in ('score', 'rsi'):
            if col in df_scan.columns:
                df_scan = df_scan.drop(columns=[col])
        # 欄位順序：tv_url 在 name 後面（讓 LinkColumn 顯示為「📈 連結」）
        preferred_cols = ['ticker', 'name', 'tv_url',
                          'price', 'change_pct',
                          'industry',
                          'signal', 'adx', 'is_bull',
                          'cross_days', 'date']
        ordered = [c for c in preferred_cols if c in df_scan.columns]
        ordered += [c for c in df_scan.columns if c not in ordered]
        df_scan = df_scan[ordered]

        st.markdown(
            f'<div style="font-size:.85rem;color:#3dbb6a;margin:.5rem 0">'
            f'✓ 找到 <b>{len(df_scan)}</b> 檔候選。勾選左側方框可多選後加入清單。</div>',
            unsafe_allow_html=True)

        # 持久化已存自選股清單（雲端 localStorage）
        _wls_for_scan = _load_watchlists()

        # 用 st.dataframe + selection_mode 達成多選
        sel_event = st.dataframe(
            df_scan,
            use_container_width=True,
            height=600,
            on_select="rerun",
            selection_mode="multi-row",
            key="scan_table_select",
            column_config={
                "ticker": st.column_config.TextColumn("代號", width="small"),
                "name": st.column_config.TextColumn("名稱", width="medium"),
                "tv_url": st.column_config.LinkColumn(
                    "圖表", display_text="📈 TV", width="small"),
                "price": st.column_config.NumberColumn("收盤價", format="%.2f"),
                "change_pct": st.column_config.NumberColumn(
                    "漲跌%", format="%+.2f%%"),
                "industry": st.column_config.TextColumn(
                    "產業別", width="small"),
                "signal": st.column_config.TextColumn("信號", width="medium"),
                "adx": st.column_config.NumberColumn("ADX", format="%.1f"),
                "is_bull": st.column_config.CheckboxColumn("多頭"),
                "cross_days": st.column_config.NumberColumn(
                    "交叉天", format="%.0f"),
                "date": st.column_config.TextColumn("日期"),
            },
        )

        # 取出選中的列
        try:
            sel_rows = sel_event.selection.rows  # type: ignore
        except Exception:
            sel_rows = []
        sel_tickers = [df_scan.iloc[i]['ticker'] for i in sel_rows] if sel_rows else []

        # ── 加入清單操作面板 ──────────────────────────────────────
        st.markdown(
            f'<div style="background:#0a1628;border:1px solid #1a2f48;'
            f'border-radius:10px;padding:12px 16px;margin-top:10px">'
            f'<div style="color:#7ab0d0;font-size:.78rem;margin-bottom:8px">'
            f'已選擇 <b style="color:#3dbb6a">{len(sel_tickers)}</b> 檔'
            f'{"：" + "、".join(sel_tickers[:8]) + ("…" if len(sel_tickers)>8 else "") if sel_tickers else ""}'
            f'</div></div>',
            unsafe_allow_html=True
        )

        if sel_tickers:
            _add_targets = ["（覆蓋目前清單）"] + sorted(_wls_for_scan.keys())
            ca, cb, cc = st.columns([2, 1, 1])
            with ca:
                _add_to = st.selectbox(
                    "加入到清單", options=_add_targets,
                    key="scan_add_target",
                    help="選擇現有清單將以「合併」方式加入（不會覆蓋既有代號）；選『覆蓋目前清單』則將目前文字框替換為選中項。"
                )
            with cb:
                if st.button("➕ 加入清單", use_container_width=True,
                             type="primary", key="scan_append_btn"):
                    if _add_to == "（覆蓋目前清單）":
                        # 覆蓋目前 textarea
                        st.session_state[f"stock_input_{_selected_wl}"] = \
                            "\n".join(sel_tickers)
                        st.success(f"✓ 已覆蓋目前清單為 {len(sel_tickers)} 檔")
                    else:
                        # 合併到指定 saved 清單
                        existing = _wls_for_scan.get(_add_to, "")
                        existing_set = {l.strip() for l in existing.splitlines()
                                        if l.strip() and not l.strip().startswith('#')}
                        new_set = set(sel_tickers) - existing_set
                        merged = (existing.rstrip() + "\n" if existing.strip() else "") + \
                                 "\n".join(sel_tickers)
                        # 去重保序
                        seen = set(); out_lines = []
                        for ln in merged.splitlines():
                            s = ln.strip()
                            if not s or s.startswith('#'):
                                out_lines.append(ln); continue
                            if s in seen: continue
                            seen.add(s); out_lines.append(ln)
                        _wls_for_scan[_add_to] = "\n".join(out_lines)
                        _save_watchlists(_wls_for_scan)
                        st.success(
                            f"✓ 加入「{_add_to}」（新增 {len(new_set)} 檔，"
                            f"原 {len(existing_set)} → {len(seen)}）"
                        )
                    st.rerun()
            with cc:
                if st.button("📋 全部填入文字框", use_container_width=True,
                             key="scan_fill_btn"):
                    st.session_state[f"stock_input_{_selected_wl}"] = \
                        "\n".join(df_scan['ticker'].tolist())
                    st.success(f"✓ 已填入 {len(df_scan)} 檔")
                    st.rerun()
        else:
            st.caption("👆 在表格左側勾選想加入清單的股票（可多選）")
    else:
        err = st.session_state.pop('_scan_first_err', None)
        if err:
            st.error(
                f"⚠️ 沒有符合條件的股票（且批次下載出錯）\n\n"
                f"第一個錯誤：`{err}`\n\n"
                f"可能原因：yfinance 在 Streamlit Cloud 被限速、"
                f"或 yfinance 版本與此程式不相容。"
            )
        else:
            st.warning("沒有符合條件的股票（資料下載成功，但全市場都不符合篩選條件）")
    st.stop()

# ── 用 session_state 儲存結果，避免下拉選單觸發重跑時資料消失 ──
if fetch_btn:
    # 歷史模式快取永不過期（數據不變）；即時模式才清快取
    if not selected_end_date:
        fetch_indicators.clear()
        fetch_news.clear()
    for k in list(st.session_state.keys()):
        if k.startswith("ai_") or k == "results" or k == "debug_msgs":
            del st.session_state[k]
    st.session_state["fetch_end_date"] = selected_end_date
    stocks = parse_input(stock_input)
    if not stocks:
        st.error("股票清單為空，請重新輸入"); st.stop()

    progress_bar = st.progress(0, text="準備中...")
    status_ph    = st.empty()
    results    = []
    debug_msgs = []

    for i, (ticker, market) in enumerate(stocks):
        progress_bar.progress(i / len(stocks), text=f"抓取 {ticker}  ({i+1}/{len(stocks)})")
        status_ph.markdown(
            f'<div style="font-size:.78rem;color:#5a8ab0;text-align:center">'
            f'正在抓取 <b style="color:#8ab8d8">{ticker}</b>...</div>',
            unsafe_allow_html=True)
        _end = st.session_state.get("fetch_end_date", "")
        d = fetch_indicators(ticker, market, _end)
        if d and d.get("_error"):
            debug_msgs.append(f"❌ {ticker} ({get_yf_symbol(ticker)}): {d['_error']}")
            d = None
        if d and d.get("close"):
            g_trend    = judge_trend(d)
            g_position = judge_position(d)
            g_momentum = judge_momentum(d)
            g_aux      = judge_aux(d)
            ts = calc_summary(g_trend,    TREND_W)
            ps = calc_summary(g_position, POSITION_W)
            mom_grade  = compute_momentum_grade(d)
            ms_b, ms_s, ms_n, _ = calc_summary(g_momentum, MOMENTUM_W)
            ms = (ms_b, ms_s, ms_n, mom_grade)   # direct verdict overrides sum
            xs = _calc_aux_summary(g_aux, AUX_W)
            tb = round(ts[0]+ps[0]+ms[0]+xs[0], 1)
            ts_= round(ts[1]+ps[1]+ms[1]+xs[1], 1)
            tn_= round(ts[2]+ps[2]+ms[2]+xs[2], 1)
            raw_verdict = _rec(tb, ts_)
            verdict, cap = apply_cap(raw_verdict, d, mom_grade)
            # 保留舊格式供 Excel 相容
            osc = judge_oscillators(d)
            mas = judge_mas(d)
            results.append((ticker, market, d, False,
                            (g_trend, g_position, g_momentum, g_aux),
                            (ts, ps, ms, xs),
                            (tb, ts_, tn_, verdict), cap,
                            osc, mas))
        else:
            if not any(m.startswith(f"❌ {ticker}") for m in debug_msgs):
                debug_msgs.append(f"❌ {ticker} ({get_yf_symbol(ticker)}): 無資料或資料不足")
            results.append((ticker, market, None, True,
                            ([], [], [], []), ((0,0,0,"中立"),)*4,
                            (0, 0, 0, "中立"), None, [], []))

    progress_bar.progress(1.0, text="完成 ✓")
    status_ph.empty()

    # 儲存到 session_state
    st.session_state["results"]         = results
    st.session_state["debug_msgs"]      = debug_msgs
    st.session_state["results_version"] = _RESULTS_VERSION

# 從 session_state 讀取（保持結果在模型切換時不消失）
if "results" not in st.session_state:
    st.markdown("""
<div style="text-align:center;padding:30px 20px 12px">
  <div style="font-size:2rem;margin-bottom:8px">📈</div>
  <div style="font-size:.95rem;color:#3a6a9a">在左側輸入股票代號，點擊「開始抓取資料」</div>
  <div style="font-size:.72rem;color:#1e3a5f;margin-top:4px">支援台股 · NASDAQ · NYSE · 任何 Yahoo Finance 代號</div>
</div>""", unsafe_allow_html=True)
    # TOP 200 已移到健壯性驗證下方（永遠顯示），這裡不重複
    st.stop()

results    = st.session_state["results"]
debug_msgs = st.session_state.get("debug_msgs", [])

error_msgs = [m for m in debug_msgs if m.startswith("❌")]
if error_msgs:
    with st.expander(f"⚠️ {len(error_msgs)} 筆無法取得資料（點擊展開查看原因）", expanded=False):
        for msg in error_msgs:
            st.markdown(f"<div style='font-size:.8rem;color:#ff8080;font-family:monospace'>{msg}</div>",
                        unsafe_allow_html=True)
        st.markdown("<div style='font-size:.75rem;color:#556677;margin-top:8px'>可能原因：代號格式不正確、Yahoo Finance 暫時無法存取、或此標的在 Yahoo Finance 不存在</div>",
                    unsafe_allow_html=True)

total      = len(results)
ok         = sum(1 for r in results if not r[3])
# 🆕 改為 v8 操作分類 + 收集 ticker 清單
_entry_tks = []; _exit_tks = []; _hold_tks = []; _wait_tks = []
for r in results:
    if r[3] or not r[2]: continue
    a = classify_action(r[2])
    if   a == 'ENTRY': _entry_tks.append(r[0])
    elif a == 'EXIT':  _exit_tks.append(r[0])
    elif a == 'HOLD':  _hold_tks.append(r[0])
    elif a == 'WAIT':  _wait_tks.append(r[0])
entry_count = len(_entry_tks)
exit_count  = len(_exit_tks)
hold_count  = len(_hold_tks)
wait_count  = len(_wait_tks)


def _tickers_chips(tks, max_show=8, color='#a8cce8'):
    """卡片底下顯示 ticker 代碼（最多 max_show 個，超過用 +N more）"""
    if not tks:
        return '<div style="font-size:.62rem;color:#3a5a7a;margin-top:4px">—</div>'
    show = tks[:max_show]
    more = len(tks) - len(show)
    chips = ''.join(
        f'<span style="display:inline-block;padding:1px 4px;background:#0a1830;'
        f'color:{color};font-size:.62rem;font-family:\'IBM Plex Mono\',monospace;'
        f'border-radius:3px;margin:2px 2px 0 0">{t}</span>'
        for t in show
    )
    if more > 0:
        chips += (f'<span style="font-size:.6rem;color:#5a7a99;margin-left:2px">+{more}</span>')
    return f'<div style="margin-top:4px;line-height:1.5">{chips}</div>'


st.markdown(f"""
<div class="cards-row">
  <div class="card total"><div class="c-label">抓取完成</div>
    <div class="c-value">{ok}<span style="font-size:1rem;color:#3a5a7a">/{total}</span></div></div>
  <div class="card entry"><div class="c-label">可進場</div>
    <div class="c-value">{entry_count}</div>{_tickers_chips(_entry_tks, color='#3dbb6a')}</div>
  <div class="card exit"><div class="c-label">應出倉</div>
    <div class="c-value">{exit_count}</div>{_tickers_chips(_exit_tks, color='#ff7777')}</div>
  <div class="card hold"><div class="c-label">持倉中</div>
    <div class="c-value">{hold_count}</div>{_tickers_chips(_hold_tks, color='#7abadd')}</div>
  <div class="card neu"><div class="c-label">觀望</div>
    <div class="c-value">{wait_count}</div></div>
  <div class="card total"><div class="c-label">{'歷史日期' if st.session_state.get('fetch_end_date') else '更新時間'}</div>
    <div class="c-value" style="font-size:{'0.78rem' if st.session_state.get('fetch_end_date') else '.95rem'};color:{'#f0a030' if st.session_state.get('fetch_end_date') else '#f0f4ff'}">{st.session_state.get('fetch_end_date') or datetime.now().strftime("%H:%M")}</div></div>
</div>""", unsafe_allow_html=True)

platform_url_tpl = "https://www.perplexity.ai/search?q={prompt}"
selected_platform = "Perplexity"

# 🆕 📊 今日 Portfolio 推薦（嚴格依 TOP 200 tier，從 data_cache 全市場掃）
@st.cache_data(ttl=3600)
def _scan_top200_signals():
    """從 data_cache 直接讀全 TOP 200 個股的最新指標 + 分類動作。
    比起依賴 results（僅含使用者搜尋），這個函式掃整個 TOP 200 清單。
    使用 data_cache parquet（已含 e20/e60/rsi/adx 等指標），快速。
    """
    import pandas as _pd
    import numpy as _np
    from pathlib import Path as _P
    tier_data = _load_vwap_applicable()
    top_tickers = [t for t, info in tier_data.items() if info.get('tier') == 'TOP']

    # 載入 stock 名稱映射
    name_map = {}
    try:
        sj = _P(__file__).parent / 'tw_stock_list.json'
        if sj.exists():
            import json as _json
            with open(sj, encoding='utf-8') as _f:
                _data = _json.load(_f)
            if isinstance(_data, dict):
                if 'tickers' in _data:
                    _data = _data['tickers']
                for k, v in _data.items():
                    if isinstance(v, dict):
                        name_map[k] = v.get('name', '')
    except Exception:
        pass

    # 概念股名稱備援
    if not name_map:
        for t in top_tickers:
            name_map[t] = ''

    entry, exit_, hold = [], [], []
    for t in top_tickers:
        try:
            cache_path = _P(__file__).parent / 'data_cache' / f'{t}.parquet'
            if not cache_path.exists():
                continue
            df = _pd.read_parquet(cache_path)
            if df is None or len(df) < 30:
                continue
            last_idx = -1
            close = float(df['Close'].iloc[last_idx])

            # 建構 d dict (符合 classify_action 介面)
            ema20 = float(df['e20'].iloc[last_idx]) if 'e20' in df.columns and not _pd.isna(df['e20'].iloc[last_idx]) else None
            ema60 = float(df['e60'].iloc[last_idx]) if 'e60' in df.columns and not _pd.isna(df['e60'].iloc[last_idx]) else None
            rsi = float(df['rsi'].iloc[last_idx]) if 'rsi' in df.columns and not _pd.isna(df['rsi'].iloc[last_idx]) else None
            rsi_prev = float(df['rsi'].iloc[last_idx-1]) if len(df) >= 2 and 'rsi' in df.columns and not _pd.isna(df['rsi'].iloc[last_idx-1]) else None
            rsi_prev2 = float(df['rsi'].iloc[last_idx-2]) if len(df) >= 3 and 'rsi' in df.columns and not _pd.isna(df['rsi'].iloc[last_idx-2]) else None
            adx = float(df['adx'].iloc[last_idx]) if 'adx' in df.columns and not _pd.isna(df['adx'].iloc[last_idx]) else None
            atr14 = float(df['atr'].iloc[last_idx]) if 'atr' in df.columns and not _pd.isna(df['atr'].iloc[last_idx]) else None

            # 計算 ema20_cross_days
            cross_days = None
            if 'e20' in df.columns and 'e60' in df.columns and len(df) >= 30:
                e20s = df['e20'].values
                e60s = df['e60'].values
                cur_bull = e20s[last_idx] > e60s[last_idx] if not (_np.isnan(e20s[last_idx]) or _np.isnan(e60s[last_idx])) else None
                if cur_bull is not None:
                    for k in range(1, min(60, len(df))):
                        if _np.isnan(e20s[last_idx-k]) or _np.isnan(e60s[last_idx-k]):
                            continue
                        prev_bull = e20s[last_idx-k] > e60s[last_idx-k]
                        if prev_bull != cur_bull:
                            cross_days = k if cur_bull else -k
                            break

            d = {
                'close': close,
                'ema20': ema20, 'ema60': ema60,
                'rsi': rsi, 'rsi_prev': rsi_prev, 'rsi_prev2': rsi_prev2,
                'adx': adx, 'atr14': atr14,
                'ema20_cross_days': cross_days,
            }
            action = classify_action(d)
            tier_info = tier_data.get(t, {})
            delta_v = tier_info.get('delta', 0)
            row = (t, name_map.get(t, t), d, delta_v)
            if action == 'ENTRY':
                entry.append(row)
            elif action == 'EXIT':
                exit_.append(row)
            elif action == 'HOLD':
                hold.append(row)
        except Exception:
            continue

    entry.sort(key=lambda x: -x[3])
    exit_.sort(key=lambda x: -x[3])
    hold.sort(key=lambda x: -x[3])
    return entry, exit_, hold


_top_entry_picks, _top_exit_picks, _top_hold_picks = _scan_top200_signals()

if _top_entry_picks or _top_exit_picks or _top_hold_picks:
    def _pick_html(picks, max_show=8, accent='#3dbb6a'):
        if not picks:
            return '<div style="color:#3a5a7a;font-size:.72rem;padding:4px 8px">— 暫無 —</div>'
        out = ''
        for t, name, d, delta in picks[:max_show]:
            close = d.get('close')
            close_str = f'{close:.2f}' if close else '—'
            rsi = d.get('rsi')
            rsi_str = f'RSI {rsi:.0f}' if rsi else ''
            per = d.get('per')
            pe_str = f'PE {per:.1f}' if per else ''
            cd = d.get('ema20_cross_days')
            cd_str = ''
            if cd is not None and 0 < cd <= 10:
                cd_str = f'<span style="color:#5a8ab0;font-size:.62rem">T1 {cd}d</span>　'
            elif rsi is not None and rsi < 50:
                cd_str = f'<span style="color:#5a8ab0;font-size:.62rem">T3 拉回</span>　'
            out += (
                f'<div style="display:flex;align-items:baseline;gap:6px;'
                f'padding:5px 10px;font-size:.78rem;border-bottom:1px solid #1a2a3f">'
                f'<span style="color:{accent};font-weight:700;font-family:monospace;'
                f'min-width:48px">{t}</span>'
                f'<span style="color:#a8cce8;flex:1;overflow:hidden;text-overflow:ellipsis;'
                f'white-space:nowrap;max-width:90px">{name}</span>'
                f'<span style="color:#e8f4fd;font-family:monospace;font-size:.74rem">{close_str}</span>'
                f'<span style="color:#7a8899;font-size:.7rem">{rsi_str}</span>'
                f'<span style="color:#7a8899;font-size:.7rem">{pe_str}</span>'
                f'{cd_str}'
                f'<span style="color:#3dbb6a;font-size:.65rem;background:#0a3a1f;'
                f'padding:1px 5px;border-radius:3px">Δ {delta:+.0f}%</span>'
                f'</div>'
            )
        if len(picks) > max_show:
            out += (f'<div style="text-align:center;padding:5px;color:#5a8ab0;font-size:.7rem">'
                    f'＋{len(picks)-max_show} 檔在表格內</div>')
        return out

    # Header with explanation
    st.markdown(
        '<div style="background:#0a1a2a;border:1px solid #3dbb6a55;border-radius:10px;'
        'padding:10px 14px;margin:8px 0;display:flex;align-items:center;gap:14px;'
        'flex-wrap:wrap">'
        '<div style="font-size:1.05rem;font-weight:700;color:#3dbb6a">📊 今日 Portfolio 推薦</div>'
        f'<div style="color:#7abadd;font-size:.78rem">'
        f'進場 <b>{len(_top_entry_picks)}</b>　│　'
        f'出倉 <b>{len(_top_exit_picks)}</b>　│　'
        f'持倉 <b>{len(_top_hold_picks)}</b></div>'
        '<div style="color:#7a8899;font-size:.72rem;flex:1">'
        '嚴格從 ⭐ <b style="color:#3dbb6a">TOP 200 tier</b> 篩選'
        '（v8+P5+VWAPEXEC 適用清單，預期年化 ~52%）'
        '</div></div>',
        unsafe_allow_html=True
    )

    # 三欄
    cols = st.columns(3)
    with cols[0]:
        st.markdown(
            '<div style="background:#0a1e10;border:1px solid #3dbb6a55;border-radius:8px;'
            'padding:6px 4px;margin-bottom:8px">'
            '<div style="color:#3dbb6a;font-weight:700;padding:0 8px 4px;font-size:.85rem">'
            f'🚀 進場（{len(_top_entry_picks)}）</div>'
            + _pick_html(_top_entry_picks, accent='#3dbb6a')
            + '</div>', unsafe_allow_html=True)
    with cols[1]:
        st.markdown(
            '<div style="background:#1a0808;border:1px solid #ff555555;border-radius:8px;'
            'padding:6px 4px;margin-bottom:8px">'
            '<div style="color:#ff7777;font-weight:700;padding:0 8px 4px;font-size:.85rem">'
            f'🚪 出倉（{len(_top_exit_picks)}）</div>'
            + _pick_html(_top_exit_picks, accent='#ff7777')
            + '</div>', unsafe_allow_html=True)
    with cols[2]:
        st.markdown(
            '<div style="background:#0a1830;border:1px solid #5a8ab055;border-radius:8px;'
            'padding:6px 4px;margin-bottom:8px">'
            '<div style="color:#7abadd;font-weight:700;padding:0 8px 4px;font-size:.85rem">'
            f'📌 持倉中（{len(_top_hold_picks)}）</div>'
            + _pick_html(_top_hold_picks, accent='#7abadd')
            + '</div>', unsafe_allow_html=True)

# 🆕 📊 Portfolio Simulator —————————————————————————————————
with st.expander("📊 投組模擬器（基於 1028 檔 22 個月 out-of-sample 結果）", expanded=False):
    @st.cache_data(ttl=86400)
    def _load_portfolio_data():
        """載入 portfolio simulation 基礎資料"""
        import json as _json
        try:
            from pathlib import Path as _P
            with open(_P(__file__).parent / 'full_market_results.json', encoding='utf-8') as f:
                data = _json.load(f)
            with open(_P(__file__).parent / 'vwap_applicable.json', encoding='utf-8') as f:
                tier_data = _json.load(f)
            vwap_test = data.get('B VWAPEXEC|TEST', {})
            base_test = data.get('A baseline|TEST', {})
            pnl = dict(zip(vwap_test['tickers'], vwap_test['pnl_pcts']))
            base_pnl = dict(zip(base_test['tickers'], base_test['pnl_pcts']))
            return pnl, base_pnl, tier_data
        except Exception:
            return {}, {}, {}

    _pnl_t, _base_t, _tier_d = _load_portfolio_data()

    if not _pnl_t:
        st.markdown('<div style="color:#7a8899;padding:8px">portfolio 資料尚未產生（需 full_market_results.json）</div>',
                    unsafe_allow_html=True)
    else:
        import numpy as np
        # 上方：說明
        st.markdown(
            '<div style="font-size:.8rem;color:#5a8ab0;line-height:1.7;padding:6px 0">'
            '基於回測 1028 檔 × 22 月（2024.6-2026.4）TEST 期 out-of-sample 報酬。'
            '<b style="color:#f0a030">⚠️ Top N 是事後 cherry-pick</b>，未來無法複製；'
            '實務可用是 <b style="color:#3dbb6a">⭐ TOP 200 tier</b>（前向確定的清單）。'
            '</div>', unsafe_allow_html=True
        )

        # 互動：投入金額 + 組合選擇
        col_a, col_b, col_c = st.columns([1, 1.2, 1.5])
        with col_a:
            inv_amount = st.number_input("投入金額（萬）",
                                          min_value=10, max_value=10000,
                                          value=100, step=10, key="pf_amt")
        with col_b:
            combo_choice = st.selectbox(
                "組合策略",
                ["⭐ TOP 200 tier（前向可用）",
                 "全市場 1028 檔（無篩選）",
                 "Top 50（事後 cherry-pick）",
                 "Top 100（事後 cherry-pick）",
                 "Tier OK 675 檔",
                 "Tier NA 153 檔（不適用）"],
                key="pf_combo")
        with col_c:
            st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)
            st.markdown(
                '<div style="font-size:.7rem;color:#7a8899;line-height:1.6">'
                '對照：22 個月 0050 BH ≈ <b style="color:#3dbb6a">+90%</b>'
                '（年化 +49%）'
                '</div>', unsafe_allow_html=True)

        # 計算組合報酬
        deltas = []
        for t in _pnl_t:
            if t in _base_t:
                deltas.append((t, _pnl_t[t] - _base_t[t], _pnl_t[t]))
        deltas.sort(key=lambda x: -x[1])

        if combo_choice.startswith("⭐ TOP"):
            picks = [_pnl_t[t] for t, info in _tier_d.items()
                     if info.get('tier') == 'TOP' and t in _pnl_t]
            label = "TOP 200"
        elif "全市場" in combo_choice:
            picks = list(_pnl_t.values())
            label = "全市場"
        elif "Top 50" in combo_choice:
            picks = [d[2] for d in deltas[:50]]
            label = "Top 50（事後）"
        elif "Top 100" in combo_choice:
            picks = [d[2] for d in deltas[:100]]
            label = "Top 100（事後）"
        elif "OK" in combo_choice:
            picks = [_pnl_t[t] for t, info in _tier_d.items()
                     if info.get('tier') == 'OK' and t in _pnl_t]
            label = "OK"
        else:
            picks = [_pnl_t[t] for t, info in _tier_d.items()
                     if info.get('tier') == 'NA' and t in _pnl_t]
            label = "NA"

        if picks:
            arr = np.array(picks)
            avg_ret = arr.mean() / 100
            initial = inv_amount * 10000
            end_val = initial * (1 + avg_ret)
            years = 22 / 12
            ann = ((end_val/initial)**(1/years) - 1) * 100 if years > 0 else 0
            win_rate = (arr > 0).mean() * 100
            worst = arr.min()
            best = arr.max()
            std = arr.std()

            color_main = '#3dbb6a' if avg_ret > 0 else '#ff7777'

            # 結果卡片
            st.markdown(
                f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;'
                f'margin:12px 0">'
                # 終值
                f'<div style="background:#0a1e10;border:1px solid {color_main}55;'
                f'border-radius:8px;padding:14px;text-align:center">'
                f'<div style="color:#5a8ab0;font-size:.7rem">22 月終值</div>'
                f'<div style="color:{color_main};font-size:1.4rem;font-weight:700">'
                f'{end_val/10000:,.1f} 萬</div>'
                f'<div style="color:#7a8899;font-size:.7rem">初始 {inv_amount} 萬</div>'
                f'</div>'
                # 22 月報酬
                f'<div style="background:#0a1830;border:1px solid #5a8ab055;'
                f'border-radius:8px;padding:14px;text-align:center">'
                f'<div style="color:#5a8ab0;font-size:.7rem">22 月報酬</div>'
                f'<div style="color:{color_main};font-size:1.4rem;font-weight:700">'
                f'{avg_ret*100:+.1f}%</div>'
                f'<div style="color:#7a8899;font-size:.7rem">{label}</div>'
                f'</div>'
                # 年化
                f'<div style="background:#0a1830;border:1px solid #5a8ab055;'
                f'border-radius:8px;padding:14px;text-align:center">'
                f'<div style="color:#5a8ab0;font-size:.7rem">年化報酬</div>'
                f'<div style="color:{color_main};font-size:1.4rem;font-weight:700">'
                f'{ann:+.1f}%</div>'
                f'<div style="color:#7a8899;font-size:.7rem">CAGR</div>'
                f'</div>'
                # 勝率
                f'<div style="background:#0a1830;border:1px solid #5a8ab055;'
                f'border-radius:8px;padding:14px;text-align:center">'
                f'<div style="color:#5a8ab0;font-size:.7rem">個股勝率</div>'
                f'<div style="color:#3dbb6a;font-size:1.4rem;font-weight:700">'
                f'{win_rate:.1f}%</div>'
                f'<div style="color:#7a8899;font-size:.7rem">n={len(picks)}</div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True)

            # 風險細節
            st.markdown(
                f'<div style="background:#080f1c;border:1px solid #1e3a5f;border-radius:8px;'
                f'padding:8px 14px;font-size:.78rem;color:#a8cce8">'
                f'📈 最佳個股 <b style="color:#3dbb6a">+{best:.0f}%</b> ｜ '
                f'📉 最差個股 <b style="color:#ff7777">{worst:.1f}%</b> ｜ '
                f'波動 σ <b>{std:.0f}%</b> ｜ '
                f'Sharpe~ <b>{(avg_ret*100/std if std else 0):.3f}</b>'
                f'</div>',
                unsafe_allow_html=True)

            # 警示框
            if 'cherry-pick' in combo_choice or 'Top 50' in combo_choice or 'Top 100' in combo_choice:
                st.markdown(
                    '<div style="background:#2a1a05;border:1px solid #f0a030;border-radius:8px;'
                    'padding:8px 14px;margin-top:8px;font-size:.78rem;color:#f0a030">'
                    '⚠️ <b>事後 cherry-pick 警告</b>：此結果是「歷史最佳 N 檔」回推，'
                    '實際操作不可能事先知道。建議參考但不可作為前向預期。'
                    '</div>',
                    unsafe_allow_html=True)
            elif 'NA' in combo_choice:
                st.markdown(
                    '<div style="background:#2a0808;border:1px solid #ff5555;border-radius:8px;'
                    'padding:8px 14px;margin-top:8px;font-size:.78rem;color:#ff7777">'
                    '⚠️ <b>NA tier 警告</b>：此 153 檔 VWAPEXEC 反而傷害，'
                    '不建議使用 P5+VWAPEXEC 策略。'
                    '</div>',
                    unsafe_allow_html=True)
            elif 'TOP 200' in combo_choice:
                st.markdown(
                    '<div style="background:#0a2a18;border:1px solid #3dbb6a;border-radius:8px;'
                    'padding:8px 14px;margin-top:8px;font-size:.78rem;color:#3dbb6a">'
                    '✅ <b>前向可用</b>：TOP 200 tier 是事先確定的 200 檔清單，'
                    '實際操作可實踐。預期年化 ~52%（含適度 alpha）。'
                    '</div>',
                    unsafe_allow_html=True)

st.markdown("#### 完整指標一覽表")

# 🆕 圖示說明區
st.markdown(
    '<div style="background:#080f1c;border:1px solid #1e3a5f;border-radius:8px;'
    'padding:8px 14px;margin:4px 0 10px;font-size:.74rem;color:#7abadd;'
    'display:flex;flex-wrap:wrap;gap:14px;align-items:center">'
    '<b style="color:#5a8ab0">圖示說明：</b>'
    '<span><span style="display:inline-block;padding:1px 5px;background:#0a3a1f;color:#3dbb6a;'
    'border-radius:3px;font-size:.65rem;font-weight:700">⭐</span>'
    '<span style="color:#a8cce8">　= VWAPEXEC TOP 200（歷史 Δ 顯著正向，最該用 VWAP 限價）</span></span>'
    '<span><span style="display:inline-block;padding:1px 5px;background:#3a0a0a;color:#ff8888;'
    'border-radius:3px;font-size:.65rem;font-weight:700">⚠️</span>'
    '<span style="color:#a8cce8">　= VWAPEXEC NA（Δ ≤ 0，VWAP 反而傷害，避開）</span></span>'
    '<span style="color:#7a8899">（無徽章 = OK 一般適用 / 不在歷史測試樣本）</span>'
    '</div>'
    '<div style="background:#080f1c;border:1px solid #1e3a5f;border-radius:8px;'
    'padding:8px 14px;margin-bottom:10px;font-size:.74rem;color:#7abadd;'
    'display:flex;flex-wrap:wrap;gap:14px;align-items:center">'
    '<b style="color:#5a8ab0">P/E 顏色：</b>'
    '<span><span style="color:#3dbb6a">綠</span> &lt; 20 合理偏低</span>'
    '<span><span style="color:#c8b87a">黃</span> 20-30 合理</span>'
    '<span><span style="color:#e8a020">橘</span> 30-50 偏高（成長股）</span>'
    '<span><span style="color:#ff5555">紅</span> &gt; 50 或虧損 過熱</span>'
    '<span><span style="color:#3dbb6a">▼▼</span> = 60 日 PER 大降（盈餘上修信號）</span>'
    '<span><span style="color:#ff5555">▲▲</span> = 60 日 PER 大漲（盈餘下修風險）</span>'
    '</div>',
    unsafe_allow_html=True)
st.markdown(render_table(results, platform_url_tpl, selected_platform), unsafe_allow_html=True)

st.markdown("<br>#### 個股指標詳細", unsafe_allow_html=True)
for item in results:
    ticker, market, d, error = item[0], item[1], item[2], item[3]
    groups      = item[4]   # (g_trend, g_position, g_momentum, g_aux)
    group_summs = item[5]   # (ts, ps, ms, xs)
    tsumm       = item[6]   # (tb, ts_, tn_, verdict)
    cap         = item[7]
    _, _, _, tr_ = tsumm
    name  = d.get("name", ticker) if d else ticker
    label = f"{ticker}  {name}" if name and name != ticker else ticker
    _rlabel_exp, _ = get_rec_label(d, ticker) if (d and not error) else ("無資料", "")
    title = f"{label}  {_rlabel_exp}" if not error else f"{label}  —  無資料"
    with st.expander(title, expanded=False):
        if error or not d:
            st.markdown('<div style="color:#334455;padding:12px">無法取得資料，請確認代號是否正確</div>',
                        unsafe_allow_html=True)
        else:
            # 新聞情感已整合進 render_detail（位於「當前策略風格」下方、「收盤價」上方）
            st.markdown(render_detail(ticker, d, groups, group_summs, tsumm, cap, market=market),
                        unsafe_allow_html=True)

            # ── 歷史區間 Excel ────────────────────────────────────────
            st.markdown(
                '<div style="border-top:1px solid #1a2f48;margin:14px 0 10px;padding-top:10px">'
                '<span style="font-size:.75rem;color:#5a8ab0;font-weight:700;letter-spacing:.05em">'
                '📊 歷史區間 Excel</span>'
                '<span style="font-size:.68rem;color:#334455;margin-left:8px">'
                '選擇起訖日後點「產生」，完成後出現下載按鈕</span></div>',
                unsafe_allow_html=True)
            _col_s, _col_e, _col_btn = st.columns([2, 2, 1])
            import datetime as _dt   # 確保已載入（sidebar 已 import，此行為保險）
            _today = _dt.date.today()
            with _col_s:
                _r_start = st.date_input(
                    "起始日",
                    value=_today - _dt.timedelta(days=90),
                    min_value=_dt.date(2010, 1, 1),
                    max_value=_today - _dt.timedelta(days=2),
                    key=f"rng_s_{ticker}",
                )
            with _col_e:
                _r_end = st.date_input(
                    "結束日",
                    value=_today - _dt.timedelta(days=1),
                    min_value=_dt.date(2010, 1, 2),
                    max_value=_today - _dt.timedelta(days=1),
                    key=f"rng_e_{ticker}",
                )
            with _col_btn:
                st.markdown("<div style='margin-top:26px'>", unsafe_allow_html=True)
                _gen_btn = st.button("🔄 產生", key=f"gen_rng_{ticker}",
                                     use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

            if _gen_btn:
                if _r_start >= _r_end:
                    st.error("起始日必須早於結束日")
                else:
                    with st.spinner(
                        f"正在計算 {ticker} {_r_start}～{_r_end} 歷史資料，請稍候…"
                    ):
                        _xl = build_stock_range_excel(
                            ticker, market,
                            _r_start.isoformat(), _r_end.isoformat()
                        )
                    st.session_state[f"rng_xl_{ticker}"]       = _xl
                    st.session_state[f"rng_xl_dates_{ticker}"] = (
                        _r_start.isoformat(), _r_end.isoformat())
                    st.success("✅ 產生完成，點擊下方按鈕下載")

            if f"rng_xl_{ticker}" in st.session_state:
                _sd, _ed = st.session_state.get(f"rng_xl_dates_{ticker}", ("", ""))
                st.download_button(
                    label=f"📥 下載 {ticker}  {_sd} ～ {_ed}",
                    data=st.session_state[f"rng_xl_{ticker}"],
                    file_name=f"{ticker}_{_sd}_{_ed}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_rng_{ticker}",
                    use_container_width=True,
                )


st.markdown("---")
# 🆕 v9.10p：底部下載按鈕 — PDF 為主、Excel 為備份
_, col_pdf, col_xls, _ = st.columns([1, 2, 2, 1])
with col_pdf:
    try:
        pdf_bytes = build_pdf(results)
        pdf_filename = f"Stock001_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        st.download_button(
            label="📕  下載 PDF 完整報告",
            data=pdf_bytes,
            file_name=pdf_filename,
            mime="application/pdf",
            use_container_width=True,
            type="primary",
            help="包含每股完整指標 + 操作建議 + 估值參考"
        )
    except Exception as _e:
        st.error(f"PDF 生成失敗：{str(_e)[:100]}")
with col_xls:
    excel_bytes = build_excel(results)
    xls_filename = f"Indicators_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.download_button(
        label="📊  Excel 表格（備份）",
        data=excel_bytes,
        file_name=xls_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
st.markdown(
    f'<div style="text-align:center;font-size:.7rem;color:#334455;margin-top:6px">'
    f'{total} 支股票 · {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} · '
    f'PDF 含每股 1 頁完整分析｜Excel 為原始指標表</div>',
    unsafe_allow_html=True)
