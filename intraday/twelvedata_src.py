"""Twelve Data 資料源 — 僅供 day-trade 頁使用 (v9.50)
=====================================================

設計（配合 Twelve Data Free：8 calls/min、800 credits/day）：
  - 每檔股票只抓一次 1m，其餘 ≤1h 週期本地 resample → 大幅省 credit
  - 4h / 1d 抓 TD 原生 interval（resample 出來 bar 太少），長 TTL 快取
  - 自帶 TTL 快取：同一檔短時間內多週期請求共用同一次 API call
  - 抓不到 / 沒 key / credit 用完 → 回傳 None，呼叫端可 fallback 回 Alpaca

對外 API：
  has_twelvedata() -> bool
  fetch_td(ticker, tf) -> pd.DataFrame | None   （naive UTC index, OHLCV）
  td_call_count() -> int          本 process 已發出的真實 API 呼叫數
  td_api_usage() -> dict | None   TD 官方今日用量（會花呼叫，請少用）
"""
from __future__ import annotations

import os
import json
import time
import urllib.request
import urllib.parse
from typing import Optional

import pandas as pd

_TD_BASE = 'https://api.twelvedata.com'
_TD_CACHE: dict = {}      # (ticker, interval) -> (fetch_ts, df)
_TD_CALLS = 0             # 本 process 真實 API 呼叫計數
_KEY_CACHE: list = []     # 簡單 memo（[None] 代表查過但沒有）

# interval 快取秒數：1m 短（求即時）、4h/1d 長（盤中幾乎不變）
_TD_CACHE_TTL = {'1min': 55, '4h': 1500, '1day': 3600}

# tf -> pandas resample rule（用 'min' 避免 'H' 在新版 pandas 的 deprecation）
_RESAMPLE_RULE = {'5m': '5min', '15m': '15min', '30m': '30min', '1h': '60min'}
_AGG = {'Open': 'first', 'High': 'max', 'Low': 'min',
        'Close': 'last', 'Volume': 'sum'}


# ────────────────────────────────────────────────────────────────
# API key
# ────────────────────────────────────────────────────────────────

def _td_key() -> Optional[str]:
    """讀 TWELVE_DATA_API_KEY：環境變數 → 專案 .env → Streamlit Cloud secrets。"""
    if _KEY_CACHE:
        return _KEY_CACHE[0]
    k = os.getenv('TWELVE_DATA_API_KEY')
    if not k:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        envp = os.path.join(root, '.env')
        if os.path.exists(envp):
            try:
                for ln in open(envp, encoding='utf-8'):
                    if ln.strip().startswith('TWELVE_DATA_API_KEY='):
                        k = ln.split('=', 1)[1].strip()
                        break
            except Exception:
                k = None
    if not k:    # 部署在 Streamlit Cloud 時走 st.secrets
        try:
            import streamlit as st
            k = st.secrets.get('TWELVE_DATA_API_KEY', None)
        except Exception:
            k = None
    k = (k or '').strip() or None
    _KEY_CACHE.append(k)
    return k


def has_twelvedata() -> bool:
    """是否已設定 Twelve Data API key。"""
    return _td_key() is not None


def td_call_count() -> int:
    """本 process 已發出的真實 Twelve Data API 呼叫數。"""
    return _TD_CALLS


# ────────────────────────────────────────────────────────────────
# REST 呼叫
# ────────────────────────────────────────────────────────────────

def _td_get_json(endpoint: str, **params) -> Optional[dict]:
    """呼叫 TD REST endpoint，回傳 JSON dict（失敗回 None）。"""
    global _TD_CALLS
    key = _td_key()
    if not key:
        return None
    params['apikey'] = key
    url = f'{_TD_BASE}/{endpoint}?' + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=25) as r:
            data = json.loads(r.read().decode('utf-8'))
        _TD_CALLS += 1
        return data
    except Exception as e:
        print(f'  [twelvedata] {endpoint} {params.get("symbol", "")}: '
              f'{type(e).__name__}: {str(e)[:80]}')
        return None


def _td_time_series(ticker: str, interval: str,
                     outputsize: int) -> Optional[pd.DataFrame]:
    """抓 TD time_series → OHLCV DataFrame（naive UTC index, 由舊到新）。"""
    d = _td_get_json('time_series', symbol=ticker, interval=interval,
                      outputsize=outputsize, timezone='UTC', order='ASC')
    if not d or d.get('status') != 'ok' or not d.get('values'):
        if d and d.get('status') == 'error':
            print(f'  [twelvedata] {ticker} {interval}: '
                  f'{d.get("code")} {str(d.get("message", ""))[:90]}')
        return None
    df = pd.DataFrame(d['values'])
    if 'datetime' not in df.columns:
        return None
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime']).set_index('datetime').sort_index()
    df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                             'close': 'Close', 'volume': 'Volume'})
    for c in ('Open', 'High', 'Low', 'Close', 'Volume'):
        df[c] = pd.to_numeric(df[c], errors='coerce') if c in df.columns else 0.0
    df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna(
        subset=['Open', 'High', 'Low', 'Close'])
    return df if len(df) > 0 else None


def _td_get_cached(ticker: str, interval: str,
                    outputsize: int) -> Optional[pd.DataFrame]:
    """帶 TTL 快取的 TD 抓取：同檔同 interval 短時間內共用一次 API call。"""
    ttl = _TD_CACHE_TTL.get(interval, 55)
    ck = (ticker, interval)
    now = time.time()
    hit = _TD_CACHE.get(ck)
    if hit and (now - hit[0]) < ttl:
        return hit[1]
    df = _td_time_series(ticker, interval, outputsize)
    if df is not None:
        _TD_CACHE[ck] = (now, df)
        return df
    return hit[1] if hit else None    # 抓失敗 → 沿用舊快取避免閃爍


def _resample(df: pd.DataFrame, rule: str) -> Optional[pd.DataFrame]:
    """1m OHLCV → 指定週期（移除無資料的隔夜空 bin）。"""
    out = df.resample(rule).agg(_AGG).dropna(
        subset=['Open', 'High', 'Low', 'Close'])
    return out if len(out) > 0 else None


# ────────────────────────────────────────────────────────────────
# 對外主入口
# ────────────────────────────────────────────────────────────────

def fetch_td(ticker: str, tf: str) -> Optional[pd.DataFrame]:
    """day-trade 頁取資料主入口。回傳 OHLCV df（naive UTC index）或 None。

    1m / 5m / 15m / 30m / 1h：抓一次 1m 再本地 resample（省 credit）
    4h / 1d：抓 TD 原生 interval（長 TTL 快取）
    """
    ticker = (ticker or '').strip().upper()
    if not ticker or not has_twelvedata():
        return None
    try:
        if tf in ('1m', '5m', '15m', '30m', '1h'):
            base = _td_get_cached(ticker, '1min', 5000)
            if base is None or len(base) < 30:
                return None
            if tf == '1m':
                return base
            return _resample(base, _RESAMPLE_RULE[tf])
        if tf == '4h':
            return _td_get_cached(ticker, '4h', 600)
        if tf == '1d':
            return _td_get_cached(ticker, '1day', 600)
    except Exception as e:
        print(f'  [twelvedata] fetch_td {ticker} {tf}: '
              f'{type(e).__name__}: {str(e)[:80]}')
        return None
    return None


def td_api_usage() -> Optional[dict]:
    """查 TD 官方今日用量。注意：此呼叫本身可能算 1 credit，請少用。"""
    return _td_get_json('api_usage')


def clear_td_cache(ticker: Optional[str] = None) -> None:
    """清除 TD TTL 快取（ticker=None 清全部，否則只清該檔）。"""
    if ticker is None:
        _TD_CACHE.clear()
        return
    tk = (ticker or '').strip().upper()
    for k in list(_TD_CACHE.keys()):
        if k[0] == tk:
            del _TD_CACHE[k]


def fetch_intraday(ticker: str, tf: str, market: str = 'us',
                    refresh: bool = False) -> Optional[pd.DataFrame]:
    """day-trade / intraday 頁共用取資料：

      US        → Twelve Data 優先，抓不到再 fallback get_intraday（Alpaca）
      非 US（TW）→ 直接 get_intraday（TD 免費版不含台股）

    回傳 OHLCV DataFrame（naive UTC index）或 None。
    """
    from intraday.data import get_intraday
    mkt = (market or 'us').strip().lower()
    df = None
    if mkt == 'us':
        try:
            if refresh:
                clear_td_cache(ticker)
            if has_twelvedata():
                df = fetch_td(ticker, tf)
        except Exception:
            df = None
    if df is None or len(df) < 30:
        try:
            df = get_intraday(ticker, tf=tf, market=mkt, refresh=refresh)
        except Exception:
            df = None
    return df
