#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re, io, warnings
from datetime import datetime

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import ta
import yfinance as yf
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
.market-cell{color:#5a8ab0 !important;font-size:.72rem !important;}
.j-buy{color:#3b9eff;font-weight:600;}.j-sell{color:#ff5555;font-weight:600;}.j-neutral{color:#556677;}.j-na{color:#334455;font-style:italic;}
.badge{display:inline-block;padding:2px 9px;border-radius:20px;font-size:.72rem;font-weight:700;font-family:'IBM Plex Sans',sans-serif;letter-spacing:.03em;}
.badge-strong-buy{background:#0d3b6e;color:#3b9eff;border:1px solid #1a5fa8;}
.badge-buy{background:#0d2e50;color:#60b3ff;border:1px solid #154d84;}
.badge-strong-sell{background:#4a0a0a;color:#ff6b6b;border:1px solid #8b1a1a;}
.badge-sell{background:#3b0d0d;color:#ff8080;border:1px solid #6b1515;}
.badge-neutral{background:#1a2030;color:#8899aa;border:1px solid #2a3545;}
.ind-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(195px,1fr));gap:8px;margin:10px 0;}
.ind-item{background:#0d1b2e;border:1px solid #1a2f48;border-radius:8px;padding:9px 12px;display:flex;justify-content:space-between;align-items:center;}
.ind-label{color:#5a8ab0;font-size:.72rem;font-weight:600;text-transform:uppercase;letter-spacing:.05em;}
.ind-val{font-family:'IBM Plex Mono',monospace;font-size:.8rem;}
.ind-buy .ind-val{color:#3b9eff;}.ind-sell .ind-val{color:#ff5555;}.ind-neu .ind-val{color:#556677;}
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
              "EMA(50)","SMA(50)","EMA(100)","SMA(100)","EMA(200)","SMA(200)",
              "一目均衡基準線","VWMA(20)","Hull MA(9)"]

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
        return base + f"TWSE:{ticker}"
    if market in ("NASDAQ","NYSE","AMEX","OTC"):
        return base + f"{market}:{ticker}"
    return base + ticker

def get_perplexity_url(ticker: str, name: str, d: dict) -> str:
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
    ]
    prompt = "\n".join(lines)
    return "https://www.perplexity.ai/search?q=" + urllib.parse.quote(prompt) + "&copilot=true"


@st.cache_data(ttl=3600, show_spinner=False)
def _get_tw_names() -> dict:
    """台股中文名稱字典，從 twstock 載入並快取"""
    try:
        import twstock
        return {code: info.name for code, info in twstock.codes.items()}
    except Exception:
        return {}

def _get_stock_name(ticker: str, symbol: str) -> str:
    """取得股票中文/英文名稱"""
    try:
        # 指數/特殊代號先查靜態對照表
        if symbol in INDEX_NAMES:
            return INDEX_NAMES[symbol]
        if is_tw_stock(ticker):
            tw_names = _get_tw_names()
            return tw_names.get(ticker, ticker)
        else:
            t = yf.Ticker(symbol)
            try:
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

@st.cache_data(ttl=300, show_spinner=False)
def fetch_indicators(ticker: str, market: str):
    symbol = get_yf_symbol(ticker)
    df = None
    last_err = None
    for attempt in range(2):
        try:
            yf_obj = yf.Ticker(symbol)
            df = yf_obj.history(period="1y", interval="1d")
            if df is not None and len(df) >= 30:
                break
            last_err = f"rows={len(df) if df is not None else 0}"
            df = None
        except Exception as e:
            last_err = str(e)[:120]
            df = None
            if attempt == 0:
                import time; time.sleep(1.5)
    if df is None or len(df) < 30:
        return {"_error": last_err or "no data"}
    try:
        # 抓取股票名稱
        name = _get_stock_name(ticker, symbol)
        df.columns = [c.capitalize() for c in df.columns]
        c, h, l, v = df["Close"], df["High"], df["Low"], df["Volume"]

        def last(s):
            val = s.iloc[-1]
            return float(val) if pd.notna(val) else None

        bb   = ta.volatility.BollingerBands(c, 20, 2)
        ema13 = ta.trend.EMAIndicator(c, 13).ema_indicator()
        ichi  = ta.trend.IchimokuIndicator(h, l, 9, 26, 52)

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

        return {
            "name":         name,
            "close":        last(c),
            "rsi":          last(ta.momentum.RSIIndicator(c, 14).rsi()),
            "stoch_k":      last(stoch_k_s),
            "stoch_d":      last(stoch_d_s),
            "stoch_d_prev": prev(stoch_d_s),        # crossover判斷用
            "cci":          last(ta.trend.CCIIndicator(h, l, c, 20).cci()),
            "adx":          last(ta.trend.ADXIndicator(h, l, c, 14).adx()),
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
            "ema20":    last(ta.trend.EMAIndicator(c, 20).ema_indicator()),
            "sma20":    last(ta.trend.SMAIndicator(c, 20).sma_indicator()),
            "ema30":    last(ta.trend.EMAIndicator(c, 30).ema_indicator()),
            "sma30":    last(ta.trend.SMAIndicator(c, 30).sma_indicator()),
            "ema50":    last(ta.trend.EMAIndicator(c, 50).ema_indicator()),
            "sma50":    last(ta.trend.SMAIndicator(c, 50).sma_indicator()),
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
        (fmt(d["adx"]),       "中立"),
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

def calc_summary(items):
    b = sum(1 for _, j in items if j == "買入")
    s = sum(1 for _, j in items if j == "賣出")
    n = sum(1 for _, j in items if j == "中立")
    return b, s, n, _rec(b, s)

# ─────────────────────────────────────────────────────────────────
# INPUT PARSING  ← FIX: 修正 NameError（ticker 未定義的問題）
# ─────────────────────────────────────────────────────────────────
def parse_input(text: str) -> list:
    stocks = []
    for raw in text.strip().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts  = [p.strip() for p in line.split(",")]
        ticker = parts[0].upper()
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

def render_table(results) -> str:
    rows = ""
    for ticker, market, d, error, osc, mas, osumm, msumm, tsumm in results:
        name = d.get("name", ticker) if d else ticker
        if error or not d:
            tv_url_err  = get_tv_url(ticker, market)
            rows += (f'<tr>'
                     f'<td class="ticker-cell"><a href="{tv_url_err}" target="_blank" style="color:#e8f4fd;text-decoration:none;">{ticker}</a></td>'
                     f'<td style="color:#5a8ab0;font-size:.78rem">—</td>'
                     f'<td class="market-cell">{market}</td>'
                     f'<td class="j-na">— 無資料 —</td>'
                     f'<td class="j-na">— 無資料 —</td>'
                     f'<td class="j-na">— 無資料 —</td></tr>')
            continue
        ob,os_,on_,or_ = osumm; mb,ms_,mn_,mr_ = msumm; tb,ts_,tn_,tr_ = tsumm
        osc_cell = f'<td style="background:#0d1b2e;font-size:.82rem">買:{ob} 賣:{os_} 中:{on_} {badge(or_)}</td>'
        ma_cell  = f'<td style="background:#0d1b2e;font-size:.82rem">買:{mb} 賣:{ms_} 中:{mn_} {badge(mr_)}</td>'
        tot_cell = f'<td style="background:#060c18;font-size:.82rem;font-weight:700">買:{tb} 賣:{ts_} 中:{tn_} {badge(tr_)}</td>'
        tv_url  = get_tv_url(ticker, market)
        ppl_url = get_perplexity_url(ticker, name, d)
        rows += (f'<tr>'
                 f'<td class="ticker-cell">'
                 f'<a href="{ppl_url}" target="_blank" title="Perplexity 技術分析" style="color:#e8f4fd;text-decoration:none;">{ticker}</a>'
                 f'</td>'
                 f'<td style="color:#8ab8d8;font-size:.78rem;white-space:nowrap;max-width:160px;overflow:hidden;text-overflow:ellipsis">'
                 f'<a href="{tv_url}" target="_blank" title="TradingView 圖表" style="color:#8ab8d8;text-decoration:none;">{name}</a>'
                 f'</td>'
                 f'<td class="market-cell">{market}</td>{osc_cell}{ma_cell}{tot_cell}</tr>')
    return (f'<div style="background:#060c18;border-radius:12px;border:1px solid #1e3a5f;padding:4px">'
            f'<table class="res-table"><thead><tr>'
            f'<th>代號</th><th>名稱</th><th>市場</th>'
            f'<th style="background:#0d1b2e;min-width:220px">震盪小結</th>'
            f'<th style="background:#0d1b2e;min-width:220px">均線小結</th>'
            f'<th style="background:#060c18;min-width:220px">整體建議</th>'
            f'</tr></thead><tbody>{rows}</tbody></table></div>')

def render_detail(ticker, d, osc, mas, osumm, msumm, tsumm) -> str:
    ob,os_,on_,or_ = osumm; mb,ms_,mn_,mr_ = msumm; tb,ts_,tn_,tr_ = tsumm
    def ind(label, val, judg):
        cls = {"買入":"ind-buy","賣出":"ind-sell"}.get(judg, "ind-neu")
        return (f'<div class="ind-item {cls}">'
                f'<span class="ind-label">{label}</span>'
                f'<span class="ind-val">{val} / {judg}</span></div>')
    osc_items = "".join(ind(OSC_LABELS[i], v, j) for i, (v, j) in enumerate(osc))
    ma_items  = "".join(ind(MA_LABELS[i],  v, j) for i, (v, j) in enumerate(mas))
    return (f'<div style="padding:4px 8px">'
            f'<div style="display:flex;gap:16px;margin-bottom:12px;flex-wrap:wrap">'
            f'<span style="color:#5a8ab0;font-size:.75rem">收盤價 <b style="color:#e8f4fd">{fmt(d["close"])}</b></span>'
            f'<span>震盪：{badge(or_)} <span style="color:#445566;font-size:.72rem">買:{ob} 賣:{os_} 中:{on_}</span></span>'
            f'<span>均線：{badge(mr_)} <span style="color:#445566;font-size:.72rem">買:{mb} 賣:{ms_} 中:{mn_}</span></span>'
            f'<span>整體：{badge(tr_)} <span style="color:#445566;font-size:.72rem">買:{tb} 賣:{ts_} 中:{tn_}</span></span>'
            f'</div>'
            f'<div class="section-title">震盪指標</div><div class="ind-grid">{osc_items}</div>'
            f'<div class="section-title">移動均線</div><div class="ind-grid">{ma_items}</div>'
            f'</div>')

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
    for ri, (ticker, market, d, error, osc, mas, osumm, msumm, tsumm) in enumerate(results, 2):
        ws.row_dimensions[ri].height = 18
        rf = rf_e if ri % 2 == 0 else rf_o
        def cell(col, val, bg, fc="000000", sz=9, bd=False):
            c = ws.cell(ri, col, val); c.font=fnt(fc,sz,bd); c.fill=fill(bg); c.alignment=ctr; return c
        cell(1, ticker, rf["base"], "1E3A5F", 10, True)
        cell(2, market, rf["base"], "555555", 9)
        if error or not d:
            for ci in range(3, len(col_defs)+1): cell(ci, "無資料", rf["base"], "AAAAAA", 9)
            continue
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

    stock_input = st.text_area("輸入股票清單", label_visibility="collapsed",
        value="2330\n2317\n00878\n00632R\nBOTZ\nNVDA\nAAPL", height=220)
    st.markdown("<br>", unsafe_allow_html=True)
    fetch_btn = st.button("🔍  開始抓取資料", type="primary", use_container_width=True)
    st.markdown("---")
    st.markdown("""
<div style="font-size:.68rem;color:#334455;line-height:1.8">
  <b style="color:#3a5a7a">資料來源</b>：Yahoo Finance<br>
  台股自動加 .TW（含 00632R 等反/槓桿 ETF）<br><br>
  <b style="color:#3a5a7a">震盪指標（12）</b><br>
  RSI · 隨機%K · CCI · ADX · AO<br>動量 · MACD · StochRSI · 威廉%R<br>牛熊力度 · 終極震盪 · 布林%B<br><br>
  <b style="color:#3a5a7a">移動均線（15）</b><br>
  EMA/SMA 10/20/30/50/100/200<br>一目均衡 · VWMA · Hull MA
</div>""", unsafe_allow_html=True)

if not fetch_btn:
    st.markdown("""
<div style="text-align:center;padding:60px 20px">
  <div style="font-size:3rem;margin-bottom:16px">📈</div>
  <div style="font-size:1rem;color:#3a6a9a">在左側輸入股票代號，點擊「開始抓取資料」</div>
  <div style="font-size:.78rem;color:#1e3a5f;margin-top:8px">支援台股（含反/槓桿 ETF）· NASDAQ · NYSE · 任何 Yahoo Finance 代號</div>
</div>""", unsafe_allow_html=True)
    st.stop()

stocks = parse_input(stock_input)
if not stocks:
    st.error("股票清單為空，請重新輸入"); st.stop()

progress_bar = st.progress(0, text="準備中...")
status_ph    = st.empty()
results = []
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
        osc   = judge_oscillators(d)
        mas   = judge_mas(d)
        osumm = calc_summary(osc)
        msumm = calc_summary(mas)
        ob,os_,on_,or_ = osumm; mb,ms_,mn_,mr_ = msumm
        tb,ts_,tn_ = ob+mb, os_+ms_, on_+mn_
        results.append((ticker, market, d, False, osc, mas, osumm, msumm, (tb,ts_,tn_,_rec(tb,ts_))))
    else:
        if not any(m.startswith(f"❌ {ticker}") for m in debug_msgs):
            debug_msgs.append(f"❌ {ticker} ({get_yf_symbol(ticker)}): 無資料或資料不足")
        results.append((ticker, market, None, True, [], [],
                        (0,0,0,"中立"), (0,0,0,"中立"), (0,0,0,"中立")))

progress_bar.progress(1.0, text="完成 ✓")
status_ph.empty()
if debug_msgs:
    with st.expander(f"⚠️ {len(debug_msgs)} 筆無法取得資料（點擊展開查看原因）", expanded=True):
        for msg in debug_msgs:
            st.markdown(f"<div style='font-size:.8rem;color:#ff8080;font-family:monospace'>{msg}</div>",
                        unsafe_allow_html=True)
        st.markdown("<div style='font-size:.75rem;color:#556677;margin-top:8px'>可能原因：代號格式不正確、Yahoo Finance 暫時無法存取、或此標的在 Yahoo Finance 不存在</div>",
                    unsafe_allow_html=True)

total      = len(results)
ok         = sum(1 for r in results if not r[3])
buy_count  = sum(1 for r in results if not r[3] and r[8][3] in ("買入","強力買入"))
sell_count = sum(1 for r in results if not r[3] and r[8][3] in ("賣出","強力賣出"))
neu_count  = sum(1 for r in results if not r[3] and r[8][3] == "中立")

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

st.markdown("#### 完整指標一覽表")
st.markdown(render_table(results), unsafe_allow_html=True)

st.markdown("<br>#### 個股指標詳細", unsafe_allow_html=True)
for ticker, market, d, error, osc, mas, osumm, msumm, tsumm in results:
    _, _, _, tr_ = tsumm
    title = f"{ticker}  {market}  {tr_}" if not error else f"{ticker}  —  無資料"
    with st.expander(title, expanded=False):
        if error or not d:
            st.markdown('<div style="color:#334455;padding:12px">無法取得資料，請確認代號是否正確</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown(render_detail(ticker, d, osc, mas, osumm, msumm, tsumm),
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
