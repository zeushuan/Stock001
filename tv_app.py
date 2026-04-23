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
.ind-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px;margin:10px 0;}
.ind-item{background:#0d1b2e;border:1px solid #1e3550;border-radius:8px;padding:11px 14px;display:flex;justify-content:space-between;align-items:center;}
.ind-label{color:#90bcd8;font-size:.76rem;font-weight:600;text-transform:uppercase;letter-spacing:.04em;}
.ind-val{font-family:'IBM Plex Mono',monospace;font-size:.85rem;font-weight:600;}
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
def fetch_indicators(ticker: str, market: str):
    symbol = get_yf_symbol(ticker)
    df = None
    last_err = None
    is_tw = symbol.endswith(".TW") or symbol.endswith(".TWO")

    def _try_tw_download(sym):
        """嘗試下載台股資料，回傳 DataFrame 或 None"""
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
                yf_obj = yf.Ticker(symbol)
                df = yf_obj.history(period="1y", interval="1d")
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

        bb   = ta.volatility.BollingerBands(c, 20, 2)
        ema13 = ta.trend.EMAIndicator(c, 13).ema_indicator()
        ichi  = ta.trend.IchimokuIndicator(h, l, 9, 26, 52)
        ema20_s = ta.trend.EMAIndicator(c, 20).ema_indicator()
        ema60_s = ta.trend.EMAIndicator(c, 60).ema_indicator()
        adx_obj = ta.trend.ADXIndicator(h, l, c, 14)
        adx_s   = adx_obj.adx()

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
            "rsi":          last(ta.momentum.RSIIndicator(c, 14).rsi()),
            "stoch_k":      last(stoch_k_s),
            "stoch_d":      last(stoch_d_s),
            "stoch_d_prev": prev(stoch_d_s),        # crossover判斷用
            "cci":          last(ta.trend.CCIIndicator(h, l, c, 20).cci()),
            "adx":          last(adx_s),
            "adx_prev":     prev(adx_s),
            "adx_pos":      last(adx_obj.adx_pos()),
            "adx_neg":      last(adx_obj.adx_neg()),
            "ao":           last(ao_series),
            "ao_prev":      prev(ao_series),
            "ao_prev2":     prev(ao_series, 2),
            "mom":          last(mom_series),
            "mom_prev":     prev(mom_series),
            "macd":         last(ta.trend.MACD(c).macd()),
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
            "ema20":    last(ema20_s),
            "sma20":    last(ta.trend.SMAIndicator(c, 20).sma_indicator()),
            "ema30":    last(ta.trend.EMAIndicator(c, 30).ema_indicator()),
            "sma30":    last(ta.trend.SMAIndicator(c, 30).sma_indicator()),
            "ema50":    last(ta.trend.EMAIndicator(c, 50).ema_indicator()),
            "sma50":    last(ta.trend.SMAIndicator(c, 50).sma_indicator()),
            "ema60":    last(ema60_s),
            "ema20_cross_days": ema20_cross_days,
            "w_close":  w_close_v,
            "w_ma10":   w_ma10_v,
            "w_ma20":   w_ma20_v,
            "sma60":    last(ta.trend.SMAIndicator(c, 60).sma_indicator()),
            "ema100":   last(ta.trend.EMAIndicator(c, 100).ema_indicator()),
            "sma100":   last(ta.trend.SMAIndicator(c, 100).sma_indicator()),
            "ema200":   last(ta.trend.EMAIndicator(c, 200).ema_indicator()),
            "sma200":   last(ta.trend.SMAIndicator(c, 200).sma_indicator()),
            "ichimoku": last(ichi.ichimoku_base_line()),
            "vwma":     last(vwma(c, v, 20)),
            "hma":      last(hull_ma(c, 9)),
        }
    except Exception as e:
        return {"_error": str(e)[:120]}

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

    # ── 1. 趨勢方向：EMA20/60 + SMA200 位置 ──────────────────────
    if ema20 is not None and ema60 is not None:
        above200 = (sma200 is None or close > sma200)
        if ema20 > ema60 and above200:
            dir_val, dir_j = "多頭", "買入"
        elif ema20 > ema60:
            dir_val, dir_j = "偏多(MA分歧)", "中立"
        elif ema20 < ema60 and not above200:
            dir_val, dir_j = "空頭", "賣出"
        else:
            dir_val, dir_j = "盤整", "中立"
    else:
        dir_val, dir_j = "N/A", "中立"
    sma200_note = f" SMA200:{fmt(sma200)}" if sma200 else ""
    dir_disp = f"EMA20:{fmt(ema20)}/{fmt(ema60)}{sma200_note}"

    # ── 2. 趨勢強度：ADX 等級 + +DI/-DI 方向 ────────────────────
    if adx is not None:
        adx_rising = (adx_prev is not None and adx > adx_prev)
        if adx < 20:
            str_val, str_j = f"弱·盤整 (ADX {adx:.1f})", "中立"
        elif adx < 40:
            str_val = f"中 (ADX {adx:.1f}{'↑' if adx_rising else ''})"
            str_j   = "買入" if (adx_pos and adx_neg and adx_pos > adx_neg) else "賣出"
        elif adx < 60:
            str_val = f"強 (ADX {adx:.1f}{'↑' if adx_rising else ''})"
            str_j   = "買入" if (adx_pos and adx_neg and adx_pos > adx_neg) else "賣出"
        else:
            str_val, str_j = f"過熱 (ADX {adx:.1f})", "中立"
    else:
        str_val, str_j = "N/A", "中立"

    # ── 3. 多頭階段：EMA20/60 交叉時間 + ADX 斜率 ────────────────
    cross = d.get("ema20_cross_days")
    adx_rising = (adx is not None and adx_prev is not None and adx > adx_prev)
    if cross is not None and cross > 0:
        if cross <= 20:
            phase_val = f"啟動期 ({cross}日前黃金交叉)"
            phase_j   = "買入"
        elif cross <= 60 and adx_rising:
            phase_val = f"主升段 ({cross}日前交叉, ADX↑)"
            phase_j   = "買入"
        elif cross > 60 and adx is not None and adx > 40:
            phase_val = f"加速段 ({cross}日, ADX強)"
            phase_j   = "買入"
        else:
            phase_val = f"多頭持續 ({cross}日)"
            phase_j   = "中立"
    elif cross is not None and cross < 0:
        phase_val = f"死亡交叉 ({-cross}日前)"
        phase_j   = "賣出"
    else:
        phase_val, phase_j = "無明確交叉訊號", "中立"

    # ── 4. 乖離風險：EMA20 乖離 % + 布林 %B ──────────────────────
    bbu, bbl = d.get("bbu"), d.get("bbl")
    pct_b = ((close - bbl) / (bbu - bbl) * 100
             if bbu and bbl and (bbu - bbl) != 0 else None)
    if ema20:
        dev = (close - ema20) / ema20 * 100
        if abs(dev) < 3 and (pct_b is None or pct_b < 70):
            dev_val = f"低 (乖離{dev:+.1f}%)"
            dev_j   = "買入"
        elif abs(dev) >= 8 or (pct_b is not None and pct_b > 85):
            dev_val = f"高 (乖離{dev:+.1f}%, 禁加碼)"
            dev_j   = "賣出"
        else:
            dev_val = f"中 (乖離{dev:+.1f}%)"
            dev_j   = "中立"
    else:
        dev_val, dev_j = "N/A", "中立"

    # ── 5. 週線結構：週 MA10 vs MA20 ─────────────────────────────
    wc, wm10, wm20 = d.get("w_close"), d.get("w_ma10"), d.get("w_ma20")
    if wc and wm10 and wm20:
        if wc > wm10 > wm20:
            week_val = f"多頭排列 (週MA10>{fmt(wm20)})"
            week_j   = "買入"
        elif wc < wm10 < wm20:
            week_val = f"空頭排列 (週MA10<{fmt(wm20)})"
            week_j   = "賣出"
        else:
            week_val = f"週線整理 (MA10:{fmt(wm10)}/MA20:{fmt(wm20)})"
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
    """位置風險 (30%)：RSI區間評分、布林%B"""
    close = d["close"]
    rsi   = d.get("rsi")
    bbu, bbl = d.get("bbu"), d.get("bbl")

    if rsi is not None:
        if rsi < 30:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 超賣"
        elif rsi <= 55:
            rsi_j, rsi_desc = "買入", f"RSI {rsi:.1f} 低位"
        elif rsi <= 70:
            rsi_j, rsi_desc = "中立", f"RSI {rsi:.1f} 中位"
        else:
            rsi_j, rsi_desc = "賣出", f"RSI {rsi:.1f} 超買"
    else:
        rsi_j, rsi_desc = "中立", "RSI"

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
        (rsi_desc, fmt(rsi) if rsi is not None else "N/A", rsi_j),
        (bb_desc,  f"{pct_b:.1f}%" if pct_b is not None else "N/A", bb_j),
    ]

POSITION_W = [4.0, 3.5]   # 合計 7.5 → 代表 30%

def judge_momentum(d: dict) -> list:
    """動能確認 (20%)：MACD零軸位置、動量方向、AO"""
    macd = d.get("macd")
    macd_j = ("買入" if macd and macd > 0 else "賣出" if macd and macd < 0 else "中立")

    mom, mom1 = d.get("mom"), d.get("mom_prev")
    mom_j = ("買入" if mom is not None and mom1 is not None and mom > mom1 else
             "賣出" if mom is not None and mom1 is not None and mom < mom1 else "中立")

    ao, ao1, ao2 = d.get("ao"), d.get("ao_prev"), d.get("ao_prev2")
    if ao is not None and ao1 is not None and ao2 is not None:
        if (ao > 0 and ao > ao1 and ao1 < ao2) or (ao1 < 0 and ao > 0):
            ao_j = "買入"
        elif (ao < 0 and ao < ao1 and ao1 > ao2) or (ao1 > 0 and ao < 0):
            ao_j = "賣出"
        else:
            ao_j = "中立"
    else:
        ao_j = "中立"

    return [
        ("MACD 零軸", fmt(macd), macd_j),
        ("動量(10)",  fmt(mom),  mom_j),
        ("AO",        fmt(ao),   ao_j),
    ]

MOMENTUM_W = [3.5, 2.5, 2.0]  # 合計 8 → 代表 20%

def judge_aux(d: dict) -> list:
    """輔助指標 (10%)：KD、CCI、StochRSI、Williams%R、牛熊力度、UO + 主要均線"""
    close = d["close"]

    stoch_d, stoch_d1 = d.get("stoch_d"), d.get("stoch_d_prev")
    if stoch_d is not None:
        stoch_j = ("買入" if stoch_d < 20 else
                   "賣出" if stoch_d > 80 and stoch_d1 is not None and stoch_d1 <= 80 else "中立")
    else:
        stoch_j = "中立"

    sr, sr1 = d.get("stochrsi"), d.get("stochrsi_prev")
    if sr is not None:
        sr_j = ("買入" if sr < 20 else
                "賣出" if sr > 80 and sr1 is not None and sr1 <= 80 else "中立")
    else:
        sr_j = "中立"

    wr, wr1 = d.get("willr"), d.get("willr_prev")
    if wr is not None and wr1 is not None:
        wr_j = ("買入" if wr1 < -80 and wr >= -80 else
                "賣出" if wr1 > -20 and wr <= -20 else "中立")
    else:
        wr_j = "中立"

    bbp, bbp1 = d.get("bbpower"), d.get("bbpower_prev")
    if bbp is not None and bbp1 is not None:
        bbp_j = ("買入" if bbp1 <= 0 and bbp > 0 else
                 "賣出" if bbp1 >= 0 and bbp < 0 else "中立")
    else:
        bbp_j = "中立"

    uo = d.get("uo")
    uo_j = "買入" if uo and uo > 70 else "賣出" if uo and uo < 30 else "中立"

    def mj(k): v=d.get(k); return ("買入" if v and close>v else "賣出" if v and close<v else "中立")

    return [
        ("隨機%K",      fmt(d.get("stoch_k")), stoch_j),
        ("CCI(20)",     fmt(d.get("cci")),      _j(d.get("cci"), -100, 100)),
        ("StochRSI",    fmt(d.get("stochrsi")), sr_j),
        ("威廉%R",      fmt(d.get("willr")),    wr_j),
        ("牛熊力度",    fmt(d.get("bbpower")),  bbp_j),
        ("終極震盪",    fmt(uo),                uo_j),
        ("EMA(10)",     fmt(d.get("ema10")),    mj("ema10")),
        ("SMA(10)",     fmt(d.get("sma10")),    mj("sma10")),
        ("EMA(20)",     fmt(d.get("ema20")),    mj("ema20")),
        ("SMA(20)",     fmt(d.get("sma20")),    mj("sma20")),
        ("EMA(30)",     fmt(d.get("ema30")),    mj("ema30")),
        ("SMA(30)",     fmt(d.get("sma30")),    mj("sma30")),
        ("EMA(50)",     fmt(d.get("ema50")),    mj("ema50")),
        ("SMA(50)",     fmt(d.get("sma50")),    mj("sma50")),
        ("VWMA(20)",    fmt(d.get("vwma")),     mj("vwma")),
        ("Hull MA(9)",  fmt(d.get("hma")),      mj("hma")),
    ]

AUX_W = [1.0, 1.0, 1.0, 0.8, 0.7, 0.7,
          1.5, 1.5, 1.5, 1.5, 1.0, 1.0, 1.0, 1.0, 0.8, 0.8]

def apply_cap(verdict: str, d: dict) -> tuple:
    """矩陣式上限機制：趨勢+強度+乖離三條件同時成立才允許強力買入"""
    ema20, ema60 = d.get("ema20"), d.get("ema60")
    sma200 = d.get("sma200")
    adx, adx_pos, adx_neg = d.get("adx"), d.get("adx_pos"), d.get("adx_neg")
    close, bbu, bbl = d.get("close"), d.get("bbu"), d.get("bbl")
    pct_b = ((close - bbl) / (bbu - bbl) * 100
             if bbu and bbl and (bbu - bbl) != 0 else None)
    dev_pct = (abs(close - ema20) / ema20 * 100 if ema20 and close else None)

    # ── 條件一：趨勢必須為多頭 ───────────────────────────────────
    is_bull = (ema20 and ema60 and ema20 > ema60 and
               (sma200 is None or close > sma200))
    # ── 條件二：強度必須為中~強（ADX ≥ 20，且 +DI > -DI）────────
    is_trending = (adx is not None and adx >= 20 and
                   (adx_pos is None or adx_neg is None or adx_pos > adx_neg))
    # ── 條件三：乖離必須為低（EMA20 乖離 < 8%，%B < 85%）──────
    is_low_dev = ((dev_pct is None or dev_pct < 8) and
                  (pct_b is None or pct_b < 85))

    if verdict != "強力買入":
        return verdict, None

    reasons = []
    if not is_bull:
        if ema20 and ema60 and ema20 < ema60:
            reasons.append("EMA20 < EMA60（空頭趨勢）")
        elif sma200 and close < sma200:
            reasons.append("價格低於 SMA200（長期空頭）")
        else:
            reasons.append("趨勢未確認多頭")
    if not is_trending:
        if adx is not None and adx < 20:
            reasons.append("ADX < 20（盤整）")
        elif adx_pos and adx_neg and adx_neg > adx_pos:
            reasons.append("-DI > +DI（方向偏空）")
    if not is_low_dev:
        if dev_pct and dev_pct >= 8:
            reasons.append(f"EMA20 乖離 {dev_pct:.1f}%（高）")
        elif pct_b and pct_b >= 85:
            reasons.append(f"布林 %B {pct_b:.1f}%（高）")

    if reasons:
        hint = "持有/短線" if (is_bull and is_trending) else "觀察"
        return "買入", f"⚠️ 上限買入 [{hint}]｜" + "、".join(reasons)

    return verdict, None

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
    cls = {"強力買入":"badge-strong-buy","買入":"badge-buy",
           "強力賣出":"badge-strong-sell","賣出":"badge-sell"}.get(rec, "badge-neutral")
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
        return (f'<div class="ind-item {cls}">'
                f'<span class="ind-label">{label}</span>'
                f'<span class="ind-val">{val} / {judg}</span></div>')

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

    return f'<div style="padding:4px 8px">{cap_html}{summary_row}{sections}</div>'

# ─────────────────────────────────────────────────────────────────
# EXCEL EXPORT
# ─────────────────────────────────────────────────────────────────
def build_excel(results) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "指標報告"
    ws.sheet_view.showGridLines = False
    def fill(h): return PatternFill("solid", start_color=h, fgColor=h)
    def fnt(c="000000", sz=9, bd=False): return Font(name="Arial", size=sz, bold=bd, color=c)
    mid = Side(style="medium"); ctr = Alignment(horizontal="center", vertical="center")
    JCOL = {"買入":"0D47A1","賣出":"C0392B","中立":"888888","強力買入":"0D47A1","強力賣出":"C0392B"}
    col_defs = ([("代號",8,"1E3A5F"),("市場",7,"1E3A5F")]
                + [(h,14,"1A3A6C") for h in OSC_LABELS]
                + [("震盪小結",28,"0D2244")]
                + [(h,14,"1A4A2C") for h in MA_LABELS]
                + [("均線小結",28,"0D3320"),("整體建議",30,"2C1654")])
    for ci, (label, width, bg) in enumerate(col_defs, 1):
        c = ws.cell(1, ci, label)
        c.font=fnt("FFFFFF",9,True); c.fill=fill(bg); c.alignment=ctr
        c.border=Border(bottom=mid)
        ws.column_dimensions[get_column_letter(ci)].width = width
    ws.row_dimensions[1].height = 22; ws.freeze_panes = "C2"
    rf_e = {"osc":"EBF0FA","os":"D6E4FF","ma":"EAFAF1","ms":"D5F5E3","base":"F0F4FF"}
    rf_o = {"osc":"F5F8FF","os":"E8F0FF","ma":"F5FFF8","ms":"E8FFF0","base":"FFFFFF"}
    for ri, item in enumerate(results, 2):
        ticker, market, d, error = item[0], item[1], item[2], item[3]
        osc, mas = item[8], item[9]
        tsumm = item[6]
        ws.row_dimensions[ri].height = 18
        rf = rf_e if ri % 2 == 0 else rf_o
        def cell(col, val, bg, fc="000000", sz=9, bd=False):
            c = ws.cell(ri, col, val); c.font=fnt(fc,sz,bd); c.fill=fill(bg); c.alignment=ctr; return c
        cell(1, ticker, rf["base"], "1E3A5F", 10, True)
        cell(2, market, rf["base"], "555555", 9)
        if error or not d:
            for ci in range(3, len(col_defs)+1): cell(ci, "無資料", rf["base"], "AAAAAA", 9)
            continue
        osumm = calc_summary(osc, OSC_WEIGHTS)
        msumm = calc_summary(mas, MA_WEIGHTS)
        ob,os_,on_,or_ = osumm; mb,ms_,mn_,mr_ = msumm; tb,ts_,tn_,tr_ = tsumm
        ci = 3
        for v, j in osc:
            cell(ci, f"{v} / {j}", rf["osc"], JCOL.get(j,"000000"), 9, j!="中立"); ci += 1
        cell(ci, f"買入:{ob} 賣出:{os_} 中立:{on_} → {or_}", rf["os"], JCOL.get(or_,"444444"), 9, True); ci += 1
        for v, j in mas:
            cell(ci, f"{v} / {j}", rf["ma"], JCOL.get(j,"000000"), 9, j!="中立"); ci += 1
        cell(ci, f"買入:{mb} 賣出:{ms_} 中立:{mn_} → {mr_}", rf["ms"], JCOL.get(mr_,"444444"), 9, True); ci += 1
        tot_bg = {"強力買入":"1A5276","買入":"2471A3","強力賣出":"922B21","賣出":"C0392B"}.get(tr_,"626567")
        cell(ci, f"買入:{tb} 賣出:{ts_} 中立:{tn_} → {tr_}", tot_bg, "FFFFFF", 10, True)
    ws.cell(len(results)+3, 1,
            f"產出時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}").font = fnt("999999", 8)
    buf = io.BytesIO(); wb.save(buf); return buf.getvalue()

# ─────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────
st.markdown("""
<div class="tv-header">
  <div style="font-size:1.8rem">📊</div>
  <div><h1>Indicator Scanner</h1>
  <div class="sub">12 震盪指標 · 15 移動均線 · 布林通道 %B · Yahoo Finance 數據 · Excel 匯出</div></div>
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
_RESULTS_VERSION = 2   # 每次 tuple 格式變更時 +1
if st.session_state.get("results_version") != _RESULTS_VERSION:
    for _k in ["results", "debug_msgs"]:
        st.session_state.pop(_k, None)
    st.session_state["results_version"] = _RESULTS_VERSION

# ── 用 session_state 儲存結果，避免下拉選單觸發重跑時資料消失 ──
if fetch_btn:
    # 清除舊快取，確保重新抓取最新資料
    fetch_indicators.clear()
    fetch_news.clear()
    for k in list(st.session_state.keys()):
        if k.startswith("ai_") or k == "results" or k == "debug_msgs":
            del st.session_state[k]
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
        d = fetch_indicators(ticker, market)
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
            ms = calc_summary(g_momentum, MOMENTUM_W)
            xs = calc_summary(g_aux,      AUX_W)
            tb = round(ts[0]+ps[0]+ms[0]+xs[0], 1)
            ts_= round(ts[1]+ps[1]+ms[1]+xs[1], 1)
            tn_= round(ts[2]+ps[2]+ms[2]+xs[2], 1)
            raw_verdict = _rec(tb, ts_)
            verdict, cap = apply_cap(raw_verdict, d)
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
  <div class="card total"><div class="c-label">更新時間</div>
    <div class="c-value" style="font-size:.95rem">{datetime.now().strftime("%H:%M")}</div></div>
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
