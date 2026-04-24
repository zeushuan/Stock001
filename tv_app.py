#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re, io, warnings, requests, time
from datetime import datetime

warnings.filterwarnings("ignore")

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
# ⑦ 自適應趨勢策略 操作建議
# 回測驗證：13 檔平均 +70.1%，唯一主動策略平均超過 70% 的方法
# 進場三觸發：T1 黃金交叉 | T3 多頭拉回 RSI<50 | T2 多頭背景 RSI<65
# 共同前提：EMA20>EMA60 + ADX≥18
# 出場：EMA 死亡交叉（不用 RSI 出場，讓趨勢跑完）
# 停損：ATR×2.5 動態計算
# ─────────────────────────────────────────────────────────────────
def get_operation_advice(d: dict) -> str:
    """
    依 ⑦自適應趨勢 框架輸出 HTML 操作建議卡片。
    回傳空字串表示無資料可顯示。
    """
    ema20      = d.get("ema20")
    ema60      = d.get("ema60")
    adx        = d.get("adx")
    rsi        = d.get("rsi")
    rsi_prev   = d.get("rsi_prev")
    atr14      = d.get("atr14")
    close      = d.get("close")
    cross_days = d.get("ema20_cross_days")   # +N=黃金交叉N天前, -N=死亡交叉N天前

    if ema20 is None or ema60 is None:
        return ""

    is_bull  = ema20 > ema60
    adx_ok   = (adx is not None and adx >= 18)
    rsi_str  = f"{rsi:.1f}" if rsi is not None else "N/A"
    adx_str  = f"{adx:.1f}" if adx is not None else "N/A"

    # ── ① 環境判斷 ────────────────────────────────────────────
    if not is_bull:
        # 空頭：細分嚴重程度
        if cross_days is not None and cross_days < 0:
            cross_txt = f"，死亡交叉 {abs(cross_days)} 天前"
        else:
            cross_txt = ""
        if rsi is not None and rsi < 32 and rsi_prev is not None and rsi > rsi_prev:
            env_color, env_icon = "#ff9944", "🟡"
            env_tag   = "空頭 — 超賣反彈觀察"
            env_desc  = (f"EMA20 &lt; EMA60{cross_txt}｜RSI {rsi_str} &lt; 32 且止跌回升"
                         f"（{rsi_prev:.1f}→{rsi_str}），可觀察反彈機會（嚴格停損）")
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
        env_desc  = (f"EMA20 &gt; EMA60，但 ADX {adx_str} &lt; 18，趨勢強度不足。"
                     f"回測驗證：00737 型假多頭進場虧損 -7%，⑦策略設 ADX≥18 前提")
    else:
        if cross_days is not None and 0 < cross_days <= 10:
            cross_info = f"<b style='color:#3dbb6a'>黃金交叉 {cross_days} 天前 🔥</b>｜"
        elif cross_days is not None and cross_days > 0:
            cross_info = f"黃金交叉 {cross_days} 天前｜"
        else:
            cross_info = ""
        env_color, env_icon = "#3b9eff", "✅"
        env_tag   = "多頭市場"
        env_desc  = f"{cross_info}EMA20 &gt; EMA60｜ADX {adx_str} ≥ 18（趨勢有效）"

    # ── ② 進場判斷（三觸發，僅多頭+ADX≥18 有效）────────────────
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

        # T2：多頭 + RSI < 65（背景可進場，T1/T3 未達時顯示）
        t2_ok = (rsi is not None and 50 <= rsi < 65)
        if t2_ok and not t1_ok and not t3_ok:
            to50 = f"{rsi - 50:.1f}"
            entry_rows.append(
                f'<div style="display:flex;gap:6px;align-items:baseline">'
                f'<span style="background:#0f2535;border-radius:3px;padding:0 5px;'
                f'font-size:.65rem;color:#5a9acf;white-space:nowrap">T2 背景可進</span>'
                f'<span style="color:#c8b87a">📌 RSI {rsi_str} &lt; 65，多頭中段可酌量進場，'
                f'但 T3（RSI&lt;50）再距 {to50} 點，等 T3 確認更安全</span></div>'
            )
        elif rsi is not None and rsi >= 65 and not t1_ok:
            entry_rows.append(
                f'<div style="color:#7a8899;font-size:.75rem">'
                f'RSI {rsi_str} ≥ 65，多頭偏熱，{'過熱，不進場' if rsi >= 75 else "等待回落至 RSI &lt; 50 再進場"}'
                f'</div>'
            )

    elif is_bull and not adx_ok:
        entry_rows.append(
            f'<div style="color:#e8a020">ADX {adx_str} &lt; 18，趨勢強度不足，'
            f'等待 ADX ≥ 18 後進場</div>'
        )
    else:  # 空頭
        if rsi is not None and rsi < 32 and rsi_prev is not None and rsi > rsi_prev:
            entry_rows.append(
                f'<div style="color:#ff9944">RSI {rsi_str} 空頭超賣止跌，'
                f'若要操作需嚴格設定 ATR×2.5 停損</div>'
            )
        else:
            entry_rows.append(
                f'<div style="color:#7a8899">空頭期間不進場，'
                f'等待 EMA20 穿越 EMA60（黃金交叉）後重新評估</div>'
            )

    # 進場動作標籤
    if not is_bull:
        if rsi is not None and rsi < 32 and rsi_prev is not None and rsi > rsi_prev:
            action_label, action_bg, action_fg = "空頭反彈觀察", "#2a1500", "#ff9944"
        else:
            action_label, action_bg, action_fg = "空頭不交易",   "#1a0505", "#ff5555"
    elif not adx_ok:
        action_label, action_bg, action_fg = "假多頭暫不操作", "#1a1200", "#e8a020"
    elif t1_ok or t3_ok:
        action_label, action_bg, action_fg = "進場條件達成 ✅", "#0d2a10", "#3dbb6a"
    elif t2_ok:
        action_label, action_bg, action_fg = "可觀察進場",     "#1a1a05", "#c8b87a"
    else:
        action_label, action_bg, action_fg = "等待拉回",       "#0a1628", "#7a9ab0"

    # ── ③ 出場 / 停損 ──────────────────────────────────────────
    risk_rows = []

    # 停損：ATR × 2.5
    if atr14 is not None and close is not None and close > 0:
        stop_dist  = atr14 * 2.5
        stop_price = close - stop_dist
        stop_pct   = stop_dist / close * 100
        risk_rows.append(
            f'<div>🛡️ <b>停損價 <span style="color:#ff7a7a">{stop_price:.2f}</span></b>'
            f'&nbsp;<span style="color:#7a8899">（收盤 {close:.2f} − ATR×2.5 {stop_dist:.2f}'
            f' = -{stop_pct:.1f}%）</span></div>'
        )
    else:
        risk_rows.append('<div style="color:#7a8899">ATR 資料不足，無法計算動態停損</div>')

    # 出場：EMA 死亡交叉（⑦ 核心：不用 RSI 出場）
    if is_bull:
        ema_gap_pct = (ema20 - ema60) / ema60 * 100 if ema60 else None
        if ema_gap_pct is not None and ema_gap_pct < 1.0:
            risk_rows.append(
                f'<div>⚠️ <span style="color:#ff9944"><b>出場警示</b>：EMA20/60 差距僅'
                f' {ema_gap_pct:.2f}%，接近死亡交叉，隨時準備出場！</span></div>'
            )
        elif ema_gap_pct is not None:
            risk_rows.append(
                f'<div>📌 <span style="color:#7abadd">出場條件：EMA 死亡交叉時出場'
                f'（目前差距 {ema_gap_pct:.1f}%，趨勢持續）</span></div>'
            )
        else:
            risk_rows.append(
                '<div><span style="color:#7abadd">📌 出場條件：EMA 死亡交叉時出場</span></div>'
            )
        risk_rows.append(
            '<div><span style="color:#c8b87a">🚀 飆股模式：持倉獲利 ≥ +30% 時，'
            '停損改用 EMA 死亡交叉出場（回測：固定停損會在 +21% 砍掉最終 +308% 的票）</span></div>'
        )
    else:
        risk_rows.append(
            '<div><span style="color:#7abadd">📌 轉多條件：等待 EMA 黃金交叉（EMA20 穿越 EMA60）</span></div>'
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
        # ③
        f'<div style="{sec_style.replace("margin-bottom:6px","")}">'
        f'<span style="{tag_style}">③出場停損</span>'
        f'<div style="{val_style}">{"".join(risk_rows)}</div>'
        f'</div>'
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
                     f'<td class="j-na">—</td><td class="j-na">—</td>'
                     f'<td class="j-na">—</td><td class="j-na">— 無資料 —</td></tr>')
            continue

        tb, ts_, tn_, tr_ = tsumm
        ts_s, ps_s, ms_s, xs_s = group_summs

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

        def gcell(summ, bg="#0d1b2e"):
            b,s,n,r_ = summ
            return (f'<td style="background:{bg};font-size:.82rem;line-height:1.6">'
                    f'<span style="font-family:\'IBM Plex Mono\',monospace;color:#8ab0c8">{b}:{s}:{n}</span>'
                    f'&nbsp;{badge(r_)}</td>')

        cap_icon = '<span title="' + cap.replace('"',"'") + '" style="color:#f0a030;margin-left:4px;cursor:help">⚠</span>' if cap else ""
        tot_cell = (f'<td style="background:#060c18;font-size:.82rem;font-weight:700;line-height:1.6">'
                    f'{badge(tr_)}{cap_icon}</td>')

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
                 f'{price_cell}{chg_cell}'
                 f'{gcell(ts_s, "#0a1628")}{gcell(ps_s, "#0a1628")}{gcell(ms_s, "#0a1628")}'
                 f'{tot_cell}</tr>')

    gc = GROUP_COLORS
    return (f'<div style="background:#060c18;border-radius:12px;border:1px solid #1e3a5f;padding:4px">'
            f'<table class="res-table"><thead><tr>'
            f'<th>代號</th><th>名稱</th><th>現價</th><th>漲跌幅</th>'
            f'<th style="background:#0a1628;min-width:160px"><span style="color:{gc[0]}">趨勢結構</span><br>'
            f'<span style="color:#6a8faa;font-weight:400;font-size:.63rem">買:賣:中 (40%)</span></th>'
            f'<th style="background:#0a1628;min-width:160px"><span style="color:{gc[1]}">位置風險</span><br>'
            f'<span style="color:#6a8faa;font-weight:400;font-size:.63rem">買:賣:中 (30%)</span></th>'
            f'<th style="background:#0a1628;min-width:160px"><span style="color:{gc[2]}">動能確認</span><br>'
            f'<span style="color:#6a8faa;font-weight:400;font-size:.63rem">買:賣:中 (20%)</span></th>'
            f'<th style="background:#060c18;min-width:120px">整體建議</th>'
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

    cap_html = ""
    if cap:
        cap_html = (
            f'<div style="background:#1a1200;border:1px solid #6a4a00;border-radius:8px;'
            f'padding:8px 12px;margin-bottom:12px;color:#f0a030;font-size:.78rem">{cap}</div>'
        )

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

    advice_html = get_operation_advice(d)

    return f'<div style="padding:4px 8px">{cap_html}{advice_html}{summary_row}{sections}</div>'

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
        ("整體建議", 28, "O"), ("Cap說明",  36, "O"),
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

        # ── 整體建議 + Cap ──────────────────────────────────────
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
        ("整體建議", 28, "O"), ("Cap說明",  36, "O"),
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
st.markdown("""
<div class="tv-header">
  <div style="font-size:1.8rem">📊</div>
  <div><h1>Indicator Scanner</h1>
  <div class="sub">四群組加權評分 · 趨勢40% / 位置30% / 動能20% / 輔助10% · Hard Limits · Excel 匯出</div></div>
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
    stock_input = st.text_area("輸入股票清單", label_visibility="collapsed",
        value=default_stocks, height=220)

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

    st.markdown("<br>", unsafe_allow_html=True)
    fetch_btn = st.button("🔍  開始抓取資料", type="primary", use_container_width=True)
    st.markdown("<br>", unsafe_allow_html=True)
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
_RESULTS_VERSION = 8   # 每次 tuple 格式或評分邏輯變更時 +1（⑦自適應趨勢建議卡片 2026-04-24）
if st.session_state.get("results_version") != _RESULTS_VERSION:
    for _k in ["results", "debug_msgs"]:
        st.session_state.pop(_k, None)
    st.session_state["results_version"] = _RESULTS_VERSION

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
    title = f"{label}  {tr_}" if not error else f"{label}  —  無資料"
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
