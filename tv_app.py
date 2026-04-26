#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re, io, warnings, requests, time
from datetime import datetime

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────
# 應用版本資訊
# ─────────────────────────────────────────────────────────────────
APP_VERSION   = "v9.0"
APP_UPDATED   = "2026-04-26"
APP_NOTES     = (
    "🆕 自選股清單儲存 ｜ 🆕 市場掃描器（台股/美股 進場候選） ｜ "
    "🆕 策略風格下拉選單（移至日期下方） ｜ "
    "🛡️ IND+DXY (+122/1.03 最佳)"
)
APP_VALIDATIONS = (
    "RL 從 199,886 樣本學到的規則與人工 POS 高度一致 ｜ "
    "純 RL 獲利 +153 但風險 -227 ｜ POS+IND+DXY 仍是最佳風報比 1.03"
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
.card{flex:1;min-width:140px;background:#0d1b2e;border:1px solid #1e3a5f;border-radius:10px;padding:14px 16px;text-align:center;}
.card .c-label{font-size:.7rem;color:#5a8ab0;letter-spacing:.08em;text-transform:uppercase;}
.card .c-value{font-size:1.6rem;font-weight:700;margin-top:2px;}
.card.buy .c-value{color:#3b9eff;}.card.sell .c-value{color:#ff4d4d;}.card.neu .c-value{color:#8899aa;}.card.total .c-value{color:#f0f4ff;}
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
    return ticker + ".TW" if is_tw_stock(ticker) else ticker

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


@st.cache_data(ttl=3600, show_spinner=False)
def _get_tw_names() -> dict:
    """台股中文名稱字典，從 twstock 載入並快取"""
    try:
        import twstock
        return {code: info.name for code, info in twstock.codes.items()}
    except Exception:
        return {}

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

def _get_stock_name(ticker: str, symbol: str) -> str:
    """取得股票中文/英文名稱"""
    try:
        # 1. 指數/特殊代號靜態對照
        if symbol in INDEX_NAMES:
            return INDEX_NAMES[symbol]
        # 2. 台股 twstock
        if is_tw_stock(ticker):
            tw_names = _get_tw_names()
            return tw_names.get(ticker, ticker)
        # 3. 常用美股靜態對照
        if ticker in US_NAMES:
            return US_NAMES[ticker]
        # 4. yfinance 動態查詢（fallback）
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
        """嘗試下載台股資料，回傳 DataFrame 或 None"""
        if _start_str:
            # 指定日期模式
            raw = yf.download(
                sym, start=_start_str, end=_end_str, interval="1d",
                progress=False, auto_adjust=False, multi_level_index=False,
            )
            if raw is not None and len(raw) >= 20:
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
            if raw is not None and len(raw) >= 20:
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

        return {
            "name":         name,
            "close":        close_val,
            "prev_close":   prev_close_val,
            "change_pct":   change_pct,
            "change_amt":   change_amt,
            "rsi":          last(rsi_s),
            "rsi_prev":     prev(rsi_s),
            "rsi_prev2":    prev(rsi_s, 2),             # T4連續2天上升判斷用
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
        }
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
    cross_days = d.get("ema20_cross_days")   # +N=黃金交叉N天前, -N=死亡交叉N天前

    if ema20 is None or ema60 is None:
        return ""

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

    is_bull  = ema20 > ema60
    adx_ok   = (adx is not None and adx >= 22)   # v3改良④：18→22 防假多頭
    rsi_str  = f"{rsi:.1f}" if rsi is not None else "N/A"
    adx_str  = f"{adx:.1f}" if adx is not None else "N/A"

    # ── ① 環境判斷 ────────────────────────────────────────────
    if not is_bull:
        # 空頭：細分嚴重程度
        if cross_days is not None and cross_days < 0:
            cross_txt = f"，死亡交叉 {abs(cross_days)} 天前"
        else:
            cross_txt = ""
        # T4 條件：RSI<32 且連續2天上升（v3改良③）
        _t4_rising = (rsi is not None and rsi < 32 and
                      rsi_prev is not None and rsi > rsi_prev and
                      rsi_prev2 is not None and rsi_prev > rsi_prev2)
        if _t4_rising:
            env_color, env_icon = "#ff9944", "🟡"
            env_tag   = "空頭 — 超賣反彈觀察（T4）"
            env_desc  = (f"EMA20 &lt; EMA60{cross_txt}｜RSI {rsi_str} &lt; 32 且<b>連續2天止跌回升</b>"
                         f"（{rsi_prev2:.1f}→{rsi_prev:.1f}→{rsi_str}），T4反彈條件達成（ATR×2.0嚴格停損）")
        elif rsi is not None and rsi < 32:
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
        env_desc  = (f"EMA20 &gt; EMA60，但 ADX {adx_str} &lt; 22，趨勢強度不足。"
                     f"回測驗證：00737 型假多頭進場虧損 -7%，⑦策略設 ADX≥22 前提（v3改良）")
    else:
        if cross_days is not None and 0 < cross_days <= 10:
            cross_info = f"<b style='color:#3dbb6a'>黃金交叉 {cross_days} 天前 🔥</b>｜"
        elif cross_days is not None and cross_days > 0:
            cross_info = f"黃金交叉 {cross_days} 天前｜"
        else:
            cross_info = ""
        env_color, env_icon = "#3b9eff", "✅"
        env_tag   = "多頭市場"
        env_desc  = f"{cross_info}EMA20 &gt; EMA60｜ADX {adx_str} ≥ 22（趨勢有效）"

    # ── ② 進場判斷（三觸發，僅多頭+ADX≥22 有效）────────────────
    entry_rows  = []
    t1_ok = t3_ok = t2_ok = False

    if is_bull and adx_ok:
        # T1：黃金交叉（距今 ≤ 10 天）——新多頭啟動，積極進場
        t1_ok = (cross_days is not None and 0 < cross_days <= 10)
        t1c   = "#3dbb6a" if t1_ok else "#4a6070"
        t1d   = f"{cross_days} 天前" if (cross_days and cross_days > 0) else "尚未發生"
        entry_rows.append(
            f'<div style="display:flex;gap:6px;align-items:baseline">'
            f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
            f'font-size:.65rem;color:#5a9acf;white-space:nowrap">T1 黃金交叉</span>'
            f'<span style="color:{t1c}">{"✅" if t1_ok else "⬜"} {t1d}'
            f'{"　← 積極進場" if t1_ok else ""}</span></div>'
        )

        # T3：多頭拉回 RSI < 50——停損後再入場 / 回調機會
        t3_ok = (rsi is not None and rsi < 50)
        t3c   = "#3dbb6a" if t3_ok else "#4a6070"
        if rsi is not None:
            t3_gap = f"（還差 {50 - rsi:.1f} 點）" if not t3_ok else ""
        else:
            t3_gap = ""
        entry_rows.append(
            f'<div style="display:flex;gap:6px;align-items:baseline">'
            f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
            f'font-size:.65rem;color:#5a9acf;white-space:nowrap">T3 多頭拉回</span>'
            f'<span style="color:{t3c}">{"✅" if t3_ok else "⬜"} RSI {rsi_str}'
            f' {"< 50 拉回到位" if t3_ok else f"≥ 50，等待拉回{t3_gap}"}'
            f'{"　← 可進場" if t3_ok else ""}</span></div>'
        )

        # （v7 已移除 T2 強制進場；多頭中段顯示等待 T3 拉回）
        if rsi is not None and 50 <= rsi < 65 and not t1_ok and not t3_ok:
            to50 = f"{rsi - 50:.1f}"
            entry_rows.append(
                f'<div style="display:flex;gap:6px;align-items:baseline">'
                f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
                f'font-size:.65rem;color:#7a8899;white-space:nowrap">等待 T3</span>'
                f'<span style="color:#c8b87a">📌 RSI {rsi_str}，多頭中段，'
                f'等待 RSI &lt; 50（再距 {to50} 點）確認 T3 拉回再進場</span></div>'
            )
        elif rsi is not None and rsi >= 65 and not t1_ok:
            entry_rows.append(
                f'<div style="color:#7a8899;font-size:.75rem">'
                f'RSI {rsi_str} ≥ 65，多頭偏熱，{'過熱，不進場' if rsi >= 75 else "等待回落至 RSI &lt; 50 再進場"}'
                f'</div>'
            )

    elif is_bull and not adx_ok:
        entry_rows.append(
            f'<div style="color:#e8a020">ADX {adx_str} &lt; 22，趨勢強度不足，'
            f'等待 ADX ≥ 22 後進場</div>'
        )
    else:  # 空頭
        if _t4_rising:
            entry_rows.append(
                f'<div style="color:#ff9944">T4反彈條件達成：RSI {rsi_str} &lt; 32 且<b>連續2天止跌回升</b>，'
                f'可短線觀察反彈（ATR×<b>2.0</b> 嚴格停損，非多頭策略）</div>'
            )
        elif rsi is not None and rsi < 32:
            _need = "（僅差1天，再觀察1日）" if (rsi_prev is not None and rsi > rsi_prev) else "（RSI尚未止跌）"
            entry_rows.append(
                f'<div style="color:#7a8899">RSI {rsi_str} 超賣但T4條件未達{_need}，'
                f'等待連續2天RSI回升後再評估</div>'
            )
        else:
            entry_rows.append(
                f'<div style="color:#7a8899">空頭期間不進場，'
                f'等待 EMA20 穿越 EMA60（黃金交叉）後重新評估</div>'
            )

    # 進場動作標籤
    if not is_bull:
        if _t4_rising:
            action_label, action_bg, action_fg = "T4反彈條件達成 🟡", "#2a1500", "#ff9944"
        else:
            action_label, action_bg, action_fg = "空頭不交易",         "#1a0505", "#ff5555"
    elif not adx_ok:
        action_label, action_bg, action_fg = "假多頭暫不操作", "#1a1200", "#e8a020"
    elif t1_ok or t3_ok:
        action_label, action_bg, action_fg = "進場條件達成 ✅", "#0d2a10", "#3dbb6a"
    elif t2_ok:
        action_label, action_bg, action_fg = "可觀察進場",     "#1a1a05", "#c8b87a"
    else:
        action_label, action_bg, action_fg = "等待拉回",       "#0a1628", "#7a9ab0"

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
        if ema_gap_pct is not None:
            if ema_gap_pct < 1.0:
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

        if _is_strong and _is_fresh:
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

    # 推薦策略 HTML 組裝
    rec_badge_html = (
        f'<span style="{_rec_badge};border-radius:4px;padding:2px 8px;'
        f'font-size:.72rem;font-weight:700">{_rec_name}</span>'
    )
    rec_rows.append(
        f'<div style="margin-bottom:4px">{rec_badge_html}'
        f'&nbsp;<span style="color:#8ab0c8;font-size:.75rem">{_rec_reason}</span></div>'
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
        style_badge_html = (
            f'<div style="background:{style_info_local["color"]}22;'
            f'border-left:3px solid {style_info_local["color"]};'
            f'border-radius:4px;padding:4px 10px;margin-top:6px;font-size:.7rem">'
            f'{style_info_local["icon"]} 當前策略風格：'
            f'<b style="color:{style_info_local["color"]}">{style_info_local["mode"]}</b>'
            f'　獲利 +{style_info_local["mean"]:.0f}% ｜ 風報比 {style_info_local["sharpe"]:.2f}'
            f'</div>'
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

    html = (
        f'<div style="background:#050e1a;border:1px solid #1a3050;border-radius:8px;'
        f'padding:10px 14px;margin-bottom:12px">'
        # ⓪ 特殊標的警告（若有）
        f'{special_banner}'
        # 標題
        f'<div style="font-size:.82rem;font-weight:700;color:#4a8cbf;margin-bottom:8px">'
        f'📊 ⑦ 自適應趨勢 操作建議{label_tag}</div>'
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


def get_rec_label(d: dict, ticker: str = "") -> tuple:
    """
    ④推薦策略 輕量版：回傳 (rec_name, badge_inline_style) 供表格「操作建議」欄使用。
    與 get_operation_advice() 使用完全相同的決策樹邏輯，確保兩者一致。
    """
    ema20      = d.get("ema20")
    ema60      = d.get("ema60")
    adx        = d.get("adx")
    rsi        = d.get("rsi")
    rsi_prev   = d.get("rsi_prev")
    rsi_prev2  = d.get("rsi_prev2")
    cross_days = d.get("ema20_cross_days")

    if ema20 is None or ema60 is None:
        return ("—", "background:#0a1020;color:#7a8899")

    _tk_upper  = ticker.upper().replace(".TW", "").replace(".TWO", "")
    _is_inverse = _tk_upper in _INVERSE_ETF_TICKERS
    is_bull    = ema20 > ema60
    adx_ok     = (adx is not None and adx >= 22)

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

    if not is_bull:
        if _t4_rising:
            return ("T4 空頭反彈",          "background:#2a1500;color:#ff9944;border:1px solid #ff994455")
        else:
            return ("不操作 — 等待訊號",    "background:#0a1020;color:#7a8899;border:1px solid #7a889944")
    elif not adx_ok:
        return ("不操作 — 假多頭",          "background:#1a1200;color:#e8a020;border:1px solid #e8a02044")
    else:
        _is_strong   = (adx is not None and adx >= 30)
        _is_fresh    = (cross_days is not None and 0 < cross_days <= 10)
        _is_pullback = (rsi is not None and rsi < 50)
        _is_hot      = (rsi is not None and rsi >= 70)

        if _is_strong and _is_fresh:
            return ("② 趨勢EMA（飆股）",    "background:#1a1400;color:#f0c030;border:1px solid #f0c03055")
        elif _is_strong and _is_pullback:
            return ("⑦T3 強趨勢拉回",       "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
        elif _is_strong and not _is_pullback and not _is_hot:
            return ("⑦T3 等待拉回",         "background:#0a1628;color:#7abadd;border:1px solid #7abadd44")
        elif not _is_strong and _is_fresh:
            return ("⑦T1 穩健進場",         "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
        elif not _is_strong and _is_pullback:
            return ("⑦T3 拉回進場",         "background:#0d2a10;color:#3dbb6a;border:1px solid #3dbb6a55")
        elif _is_hot:
            return ("等待回調 — 不追高",     "background:#1a1805;color:#c8b87a;border:1px solid #c8b87a44")
        else:
            return ("⑦ 等待T3 拉回",         "background:#1a1805;color:#c8b87a;border:1px solid #c8b87a44")


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
            rows += (f'<tr>'
                     f'<td class="ticker-cell"><a href="{tv_url_err}" target="_blank" style="color:#e8f4fd;text-decoration:none;">{ticker}</a></td>'
                     f'<td style="color:#a8cce8;font-size:.78rem">—</td>'
                     f'<td class="j-na">—</td><td class="j-na">—</td>'
                     f'<td class="j-na">— 無資料 —</td><td class="j-na">—</td></tr>')
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

        _rlabel, _rbadge = get_rec_label(d, ticker)
        _xlabel, _xbadge = get_exit_signal(d)
        tot_cell = (f'<td style="background:#060c18;font-size:.82rem;font-weight:700;line-height:1.6">'
                    f'<span style="display:inline-block;padding:2px 7px;border-radius:4px;'
                    f'font-size:.76rem;white-space:nowrap;{_rbadge}">{_rlabel}</span></td>')
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
        rows += (f'<tr>'
                 f'<td class="ticker-cell">{ticker_link}</td>'
                 f'<td style="color:#a8cce8;font-size:.78rem;white-space:nowrap;max-width:150px;overflow:hidden;text-overflow:ellipsis">'
                 f'<a href="{tv_url}" target="_blank" style="color:#a8cce8;text-decoration:none;">{name}</a></td>'
                 f'{price_cell}{chg_cell}{tot_cell}{exit_cell}</tr>')

    return (f'<div style="background:#060c18;border-radius:12px;border:1px solid #1e3a5f;padding:4px">'
            f'<table class="res-table"><thead><tr>'
            f'<th>代號</th><th>名稱</th><th>現價</th><th>漲跌幅</th>'
            f'<th style="background:#060c18;min-width:140px">操作建議</th>'
            f'<th style="background:#060c18;min-width:120px">④出場獲利</th>'
            f'</tr></thead><tbody>{rows}</tbody></table></div>')

def render_detail(ticker, d, groups, group_summs, tsumm, cap) -> str:
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
    for gname, color, pct, summ in zip(GROUP_NAMES, gc, GROUP_WEIGHTS, [ts_s, ps_s, ms_s, xs_s]):
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
        group_section("動能確認", gc[2], g_momentum,  ms_s) +
        group_section("輔助指標", gc[3], g_aux,       xs_s)
    )

    advice_html = get_operation_advice(d, ticker=ticker)

    return f'<div style="padding:4px 8px">{advice_html}{summary_row}{sections}</div>'

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

with st.sidebar:
    st.markdown("### 📋 股票清單")
    st.markdown("""
<div style="font-size:.72rem;color:#5a8ab0;line-height:1.8;margin-bottom:8px">
  <b style="color:#8ab8d8">台股</b>：直接輸入代號（純數字 或 含字母 ETF）<br>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">2330</code>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">00878</code>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">00632R</code><br>
  <b style="color:#8ab8d8">美股</b>：直接輸入代號<br>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">BOTZ</code>
  <code style="background:#0d1b2e;padding:1px 4px;border-radius:3px">NVDA</code><br>
  <span style="color:#334455"># 開頭為註解行</span>
</div>""", unsafe_allow_html=True)

    # 從 GitHub 讀取預設清單
    GITHUB_LIST_URL = "https://raw.githubusercontent.com/zeushuan/stock001/main/stocks.txt"
    @st.cache_data(ttl=None, show_spinner=False)
    def load_default_stocks() -> str:
        try:
            r = requests.get(GITHUB_LIST_URL, timeout=6)
            if r.status_code == 200 and r.text.strip():
                # 清除空行及多餘空白
                lines = [l.strip() for l in r.text.splitlines() if l.strip()]
                return "\n".join(lines)
        except Exception:
            pass
        return "DJI\nSPX\n0050\n2330\n00632R\n00737\nBOTZ"

    default_stocks = load_default_stocks()

    # ── 📑 自選股清單（v9.0 新增：本地 JSON 持久化）────────────
    import json as _json
    from pathlib import Path as _Path
    _WATCHLIST_FILE = _Path(__file__).parent / 'watchlists.json'

    def _load_watchlists() -> dict:
        if _WATCHLIST_FILE.exists():
            try:
                return _json.loads(_WATCHLIST_FILE.read_text(encoding='utf-8'))
            except Exception:
                return {}
        return {}

    def _save_watchlists(d: dict):
        try:
            _WATCHLIST_FILE.write_text(
                _json.dumps(d, ensure_ascii=False, indent=2),
                encoding='utf-8')
        except Exception as e:
            st.error(f"儲存失敗：{e}")

    if 'watchlists' not in st.session_state:
        st.session_state['watchlists'] = _load_watchlists()
    _wls = st.session_state['watchlists']

    st.markdown("<div style='font-size:.72rem;color:#5a8ab0;margin-bottom:2px'>📑 自選股清單</div>",
                unsafe_allow_html=True)
    _wl_options = ["（預設清單）"] + sorted(_wls.keys())
    _selected_wl = st.selectbox(
        "自選股", options=_wl_options,
        index=0, label_visibility="collapsed", key="watchlist_select"
    )

    # 依選擇載入清單內容
    if _selected_wl == "（預設清單）":
        _initial_text = default_stocks
    else:
        _initial_text = _wls.get(_selected_wl, default_stocks)

    stock_input = st.text_area(
        "輸入股票清單", label_visibility="collapsed",
        value=_initial_text, height=200, key=f"stock_input_{_selected_wl}"
    )

    # 儲存 / 刪除 按鈕
    _c1, _c2 = st.columns([3, 1])
    with _c1:
        _save_name = st.text_input(
            "另存為", placeholder="輸入清單名稱…",
            label_visibility="collapsed", key="wl_save_name"
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
        if st.button(f"🗑 刪除「{_selected_wl}」", use_container_width=True,
                     key="wl_del_btn"):
            _wls.pop(_selected_wl, None)
            _save_watchlists(_wls)
            st.success(f"✓ 已刪除「{_selected_wl}」")
            st.rerun()

    # ── 日期選擇 ──────────────────────────────────────────────────
    import datetime as _dt
    st.markdown("<div style='font-size:.72rem;color:#5a8ab0;margin-top:6px;margin-bottom:2px'>📅 資料截止日期</div>",
                unsafe_allow_html=True)
    use_hist = st.checkbox("指定歷史日期", value=False, key="use_hist_date")
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
            f'border:1px solid #6a4a00;border-radius:6px;padding:5px 8px;margin-top:4px">'
            f'⏱ 歷史模式：{hist_date.strftime("%Y-%m-%d")} 當日收盤</div>',
            unsafe_allow_html=True)
    else:
        selected_end_date = ""
        st.markdown(
            '<div style="font-size:.68rem;color:#3a6a3a;background:#0a1a0a;'
            'border:1px solid #2a5a2a;border-radius:6px;padding:5px 8px;margin-top:4px">'
            '✓ 即時模式：最新收盤資料</div>',
            unsafe_allow_html=True)

    # ── 🎯 策略風格（v9.0：移至日期下方，改為下拉選單）─────────
    st.markdown("<div style='font-size:.72rem;color:#5a8ab0;margin-top:10px;margin-bottom:2px'>🎯 策略風格</div>",
                unsafe_allow_html=True)
    _STRATEGY_OPTIONS = [
        "🛡️ 極致風控 (IND+DXY)",
        "🛟 超低風險 (五重保護)",
        "🌊 保守 (POS+DXY)",
        "⚖️ 平衡 (POS)",
        "🤖 RL 智能加碼",
        "🚀 進攻 (P0_T1T3)",
    ]
    strategy_style = st.selectbox(
        label="策略風格",
        options=_STRATEGY_OPTIONS,
        index=0,
        label_visibility="collapsed",
        key="strategy_style"
    )

    st.markdown("<br>", unsafe_allow_html=True)
    fetch_btn = st.button("🔍  開始抓取資料", type="primary", use_container_width=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # ── 🔎 市場掃描器（v9.0 新增）──────────────────────────────
    with st.expander("🔎 市場掃描器（找進場候選）", expanded=False):
        scan_market = st.radio(
            "市場", options=["🇹🇼 台股", "🇺🇸 美股"],
            horizontal=True, key="scan_market_choice"
        )
        scan_top_n = st.slider("顯示前 N 檔", 10, 100, 30, key="scan_top_n")
        scan_signal_filter = st.multiselect(
            "信號類型篩選",
            options=["T1 黃金交叉", "T3 多頭拉回", "🟢 多頭觀察"],
            default=["T1 黃金交叉", "T3 多頭拉回"],
            key="scan_signal_filter",
        )
        scan_btn = st.button("🚀 開始掃描", use_container_width=True,
                             key="scan_btn")

    st.markdown("---")

    # 各風格對應的描述（提到外層供策略 selectbox 與市場掃描共用）
    _style_meta = {
        "🛟 超低風險 (五重保護)": dict(
            mode="P0_T1T3+POS+IND+DXY+WRSI+WADX",
            color="#ff6dc8",
            icon="🛟",
            mean=83.04, low=-88.3, sharpe=0.94, sigma=8.50,
            note="五重保護：POS+產業跨市場+DXY+週RSI+週ADX。最低風險僅 -88（史上最低），適合資金規模大需嚴控下檔",
        ),
        "🤖 RL 智能加碼": dict(
            mode="P0_T1T3+RL",
            color="#10c0c0",
            icon="🤖",
            mean=153.03, low=-227, sharpe=0.67, sigma=11.5,
            note="Tabular Q-Learning 訓練 199,886 樣本，學到「累積<0 不加碼、SPX多頭+RSI30-50 加碼」等規則。獲利 +153% 略高於純 POS，自動發現的智能規則",
        ),
        "🛡️ 極致風控 (IND+DXY)": dict(
            mode="P0_T1T3+POS+IND+DXY",
            color="#9d6dff",
            icon="🛡️",
            mean=122.25, low=-118.7, sharpe=1.03, sigma=7.89,
            note="產業 specific 跨市場 + 全局 DXY。風報比 1.03 全部最佳！半導體配 SOX、景氣循環配 HG、其他配 DXY",
        ),
        "🌊 保守 (POS+DXY)":  dict(
            mode="P0_T1T3+POS+DXY",
            color="#3dbb6a",
            icon="🌊",
            mean=120.51, low=-122, sharpe=0.99, sigma=7.67,
            note="弱美元才進場 + 累積為正才加碼。2022 熊市 -0.46% 保護有效，跨年度 σ 最穩",
        ),
        "⚖️ 平衡 (POS)": dict(
            mode="P0_T1T3+POS",
            color="#3b9eff",
            icon="⚖️",
            mean=141.68, low=-166, sharpe=0.85, sigma=8.89,
            note="累積已實現損益為正才加碼，跨年度穩定，獲利與保護平衡",
        ),
        "🚀 進攻 (P0_T1T3)": dict(
            mode="P0_T1T3",
            color="#f0c030",
            icon="🚀",
            mean=197.48, low=-290, sharpe=0.68, sigma=12.7,
            note="不限門檻 T1+T3 加碼，最大化獲利但變動大",
        ),
    }
    style_info = _style_meta[strategy_style]
    st.markdown(
        f'<div style="background:#0a1628;border:1px solid {style_info["color"]}55;'
        f'border-radius:6px;padding:8px 12px;margin-top:6px;font-size:.68rem;line-height:1.5">'
        f'<div style="color:{style_info["color"]};font-weight:700;margin-bottom:3px">'
        f'{style_info["icon"]} {style_info["mode"]}</div>'
        f'<div style="color:#7aaac8">'
        f'均值 <b style="color:{style_info["color"]}">+{style_info["mean"]:.0f}%</b> ｜ '
        f'最差 <b style="color:#ff5555">{style_info["low"]:+.0f}%</b><br>'
        f'風報比 <b style="color:#f0c030">{style_info["sharpe"]:.2f}</b> ｜ '
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
_RESULTS_VERSION = 22  # v9.0：自選股持久化、市場掃描器、策略下拉選單 2026-04-26
if st.session_state.get("results_version") != _RESULTS_VERSION:
    for _k in ["results", "debug_msgs"]:
        st.session_state.pop(_k, None)
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
                pr = h['Close'].dropna().values
                if len(pr) < 60: return None
                s = _pd.Series(pr)
                e20 = s.ewm(span=20, adjust=False).mean().values
                e60 = s.ewm(span=60, adjust=False).mean().values
                delta = s.diff()
                gain = delta.where(delta > 0, 0.0).rolling(14).mean()
                loss = -delta.where(delta < 0, 0.0).rolling(14).mean()
                rs = gain / loss
                rsi = (100 - 100/(1+rs)).values
                last = len(pr) - 1
                is_bull = e20[last] > e60[last]
                is_t1 = (last >= 1 and e20[last-1] <= e60[last-1] and
                         e20[last] > e60[last])
                cur_rsi = float(rsi[last]) if not _np.isnan(rsi[last]) else None
                if is_t1:
                    sig, score = "T1 黃金交叉", 90
                elif is_bull and cur_rsi and cur_rsi < 60:
                    sig, score = "🟢 多頭觀察", 50
                elif is_bull and cur_rsi and cur_rsi >= 75:
                    sig, score = "⚠️ 過熱（RSI≥75）", 30
                else:
                    sig, score = "🔴 空頭", 10
                if sig not in scan_signal_filter: return None
                cur_pr = float(pr[last])
                prev_pr = float(pr[last-1]) if last >= 1 else cur_pr
                chg_pct = (cur_pr - prev_pr) / prev_pr * 100 if prev_pr else 0
                name = tw_names.get(t, "") if is_tw_market \
                       else us_names_full.get(t, "")
                return dict(
                    ticker=t, name=name,
                    date=str(h.index[-1].date()),
                    price=round(cur_pr, 2),
                    change_pct=round(chg_pct, 2),
                    signal=sig, score=score,
                    rsi=round(cur_rsi, 1) if cur_rsi else None,
                    is_bull=is_bull,
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
                # 兩條路都失敗 → 精選 + 手動 ETF 清單
                full = list(dict.fromkeys(
                    TW_INDIVIDUAL_TICKERS + _get_all_tw_etfs()))
                tw_static_names = {}
            # 將靜態名稱注入 session，供 _scan_via_yfinance 使用
            st.session_state['_tw_static_names'] = tw_static_names
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
        df_scan = pd.DataFrame(scan_results).sort_values(
            'score', ascending=False).head(scan_top_n)
        # 重新整理欄位順序，把名稱/價格/漲跌放到代號旁邊
        preferred_cols = ['ticker', 'name', 'price', 'change_pct',
                          'signal', 'score', 'rsi', 'adx', 'is_bull',
                          'cross_days', 'date']
        ordered = [c for c in preferred_cols if c in df_scan.columns]
        ordered += [c for c in df_scan.columns if c not in ordered]
        df_scan = df_scan[ordered]
        st.markdown(
            f'<div style="font-size:.85rem;color:#3dbb6a;margin:.5rem 0">'
            f'✓ 找到 <b>{len(scan_results)}</b> 檔候選，'
            f'顯示前 <b>{len(df_scan)}</b> 檔（依 score 排序）</div>',
            unsafe_allow_html=True)
        st.dataframe(
            df_scan,
            use_container_width=True,
            height=600,
            column_config={
                "ticker": st.column_config.TextColumn("代號", width="small"),
                "name": st.column_config.TextColumn("名稱", width="medium"),
                "price": st.column_config.NumberColumn("收盤價", format="%.2f"),
                "change_pct": st.column_config.NumberColumn(
                    "漲跌%", format="%+.2f%%"),
                "signal": st.column_config.TextColumn("信號", width="medium"),
                "score": st.column_config.NumberColumn("分數", format="%.0f"),
                "rsi": st.column_config.NumberColumn("RSI", format="%.1f"),
                "adx": st.column_config.NumberColumn("ADX", format="%.1f"),
                "is_bull": st.column_config.CheckboxColumn("多頭"),
                "cross_days": st.column_config.NumberColumn(
                    "交叉天", format="%.0f"),
                "date": st.column_config.TextColumn("日期"),
            },
        )
        # 一鍵填入清單按鈕
        if st.button("📋 將掃描結果填入股票清單"):
            txt = "\n".join(df_scan['ticker'].tolist())
            st.session_state[f"stock_input_{_selected_wl}"] = txt
            st.rerun()
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
<div style="text-align:center;padding:60px 20px">
  <div style="font-size:3rem;margin-bottom:16px">📈</div>
  <div style="font-size:1rem;color:#3a6a9a">在左側輸入股票代號，點擊「開始抓取資料」</div>
  <div style="font-size:.78rem;color:#1e3a5f;margin-top:8px">支援台股（含反/槓桿 ETF）· NASDAQ · NYSE · 任何 Yahoo Finance 代號</div>
</div>""", unsafe_allow_html=True)
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
buy_count  = sum(1 for r in results if not r[3] and r[6][3] in ("買入","強力買入"))
sell_count = sum(1 for r in results if not r[3] and r[6][3] in ("賣出","強力賣出"))
neu_count  = sum(1 for r in results if not r[3] and r[6][3] == "中立")

st.markdown(f"""
<div class="cards-row">
  <div class="card total"><div class="c-label">抓取完成</div>
    <div class="c-value">{ok}<span style="font-size:1rem;color:#3a5a7a">/{total}</span></div></div>
  <div class="card buy"><div class="c-label">整體偏買入</div><div class="c-value">{buy_count}</div></div>
  <div class="card sell"><div class="c-label">整體偏賣出</div><div class="c-value">{sell_count}</div></div>
  <div class="card neu"><div class="c-label">中立</div><div class="c-value">{neu_count}</div></div>
  <div class="card total"><div class="c-label">{'歷史日期' if st.session_state.get('fetch_end_date') else '更新時間'}</div>
    <div class="c-value" style="font-size:{'0.78rem' if st.session_state.get('fetch_end_date') else '.95rem'};color:{'#f0a030' if st.session_state.get('fetch_end_date') else '#f0f4ff'}">{st.session_state.get('fetch_end_date') or datetime.now().strftime("%H:%M")}</div></div>
</div>""", unsafe_allow_html=True)

platform_url_tpl = "https://www.perplexity.ai/search?q={prompt}"
selected_platform = "Perplexity"

st.markdown("#### 完整指標一覽表")
with st.expander("⚖️ 四群組加權說明（點擊展開）", expanded=False):
    st.markdown("""
<div style="font-size:.73rem;color:#8ab8d0;line-height:2.1">
指標依<b style="color:#a8cce8">功能屬性</b>分為四群組，反映「先看趨勢，再看位置，再確認動能，輔助佐證」的分析邏輯。<br><br>
<span style="color:#3b9eff;font-weight:700">趨勢結構 40%</span>　EMA20/60 交叉 · EMA(60) 位置 · SMA(200) 位置 · ADX(14) 方向<br>
<span style="color:#f0a030;font-weight:700">位置風險 30%</span>　RSI 區間評分（超賣/低位=買、超買=賣）· 布林 %B（&lt;20%=買、&gt;80%=賣）<br>
<span style="color:#a060ff;font-weight:700">動能確認 20%</span>　MACD 零軸位置 · 動量(10) 方向 · AO 碟形/零軸交叉<br>
<span style="color:#7a8899;font-weight:700">輔助指標 10%</span>　KD · CCI · StochRSI · 威廉%R · 牛熊力度 · 終極震盪 · EMA/SMA 10~50 · VWMA · Hull MA<br><br>
<b style="color:#c8a030">⚠ 上限機制（Cap）</b>：以下情況最高評等限制為「買入」，不顯示「強力買入」：<br>
&nbsp;&nbsp;• EMA20 &lt; EMA60（趨勢偏空）<br>
&nbsp;&nbsp;• ADX &lt; 20（盤整，訊號可信度低）<br>
&nbsp;&nbsp;• RSI &gt; 75 且布林 %B &gt; 100%（位置風險過高）
</div>""", unsafe_allow_html=True)
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
            st.markdown(render_detail(ticker, d, groups, group_summs, tsumm, cap),
                        unsafe_allow_html=True)
            news = fetch_news(ticker, market)
            if news:
                news_html = '<div class="section-title" style="margin-top:12px">最新新聞</div>'
                for i, n in enumerate(news):
                    pub  = f'<span style="color:#3a5a7a;font-size:.68rem">{n["publisher"]}</span>' if n.get("publisher") else ""
                    news_html += (
                        f'<div style="display:flex;align-items:baseline;gap:8px;padding:5px 0;'
                        f'border-bottom:1px solid #0f1f33">'
                        f'<span style="color:#3b9eff;font-size:.72rem;font-weight:700;flex-shrink:0">{i+1}</span>'
                        f'<a href="{n["link"]}" target="_blank" '
                        f'style="color:#c8dff0;font-size:.78rem;text-decoration:none;line-height:1.5;'
                        f'flex:1" onmouseover="this.style.color=\'#fff\'" onmouseout="this.style.color=\'#c8dff0\'">'
                        f'{n["title"]}</a>{pub}</div>'
                    )
                st.markdown(f'<div style="padding:4px 8px 8px">{news_html}</div>',
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
_, col2, _ = st.columns([1, 2, 1])
with col2:
    excel_bytes = build_excel(results)
    filename = f"Indicators_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.download_button(label="📥  下載 Excel 報告", data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True)
    st.markdown(
        f'<div style="text-align:center;font-size:.7rem;color:#334455;margin-top:6px">'
        f'{total} 支股票 · {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>',
        unsafe_allow_html=True)
