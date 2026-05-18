"""Intraday Data Layer — Stock001 v9.42
========================================

統一資料取得介面。
TW: fugle_connector
US: 🆕 v9.42 優先 Alpaca (真即時 + 含 pre/post-hours)，fallback yfinance
共用 intraday_cache/{ticker}_{freq}.parquet 快取。

對外 API：
    get_intraday(ticker, tf='5m', market='auto', refresh=False) -> DataFrame
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import pandas as pd
import numpy as np

from intraday.config import TIMEFRAMES, get_tf_config


# ─── .env loader (沿用 fugle_connector 模式) ─────────────────────
def _load_env():
    env_file = Path(__file__).parent.parent / '.env'
    if env_file.exists():
        for line in env_file.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#'): continue
            if '=' in line:
                k, _, v = line.partition('=')
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def _load_streamlit_secrets():
    try:
        import streamlit as st
        if hasattr(st, 'secrets'):
            for key in ('ALPACA_API_KEY', 'ALPACA_API_SECRET', 'ALPACA_PAPER'):
                try:
                    v = st.secrets.get(key, None)
                    if v: os.environ.setdefault(key, str(v))
                except Exception:
                    pass
    except ImportError:
        pass

_load_env()
_load_streamlit_secrets()


CACHE_DIR = Path(__file__).parent.parent / 'intraday_cache'
CACHE_DIR.mkdir(exist_ok=True)


# ─── Alpaca config ──────────────────────────────────────────────
ALPACA_API_KEY = os.environ.get('ALPACA_API_KEY', '').strip()
ALPACA_API_SECRET = os.environ.get('ALPACA_API_SECRET', '').strip()
ALPACA_PAPER = os.environ.get('ALPACA_PAPER', 'true').lower() == 'true'

def _has_alpaca() -> bool:
    return bool(ALPACA_API_KEY) and bool(ALPACA_API_SECRET)

_alpaca_client = None
def _get_alpaca_client():
    """Lazy init Alpaca StockHistoricalDataClient"""
    global _alpaca_client
    if _alpaca_client is None and _has_alpaca():
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            _alpaca_client = StockHistoricalDataClient(
                ALPACA_API_KEY, ALPACA_API_SECRET)
        except Exception as e:
            print(f"  [alpaca] init 失敗: {type(e).__name__}: {e}")
            _alpaca_client = None
    return _alpaca_client


# ─── 🆕 v9.44 Polygon.io config (overnight ETF 覆蓋) ────────────
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY', '').strip()

def _has_polygon() -> bool:
    return bool(POLYGON_API_KEY)

_polygon_client = None
def _get_polygon_client():
    """Lazy init Polygon RESTClient"""
    global _polygon_client
    if _polygon_client is None and _has_polygon():
        try:
            from polygon import RESTClient
            _polygon_client = RESTClient(POLYGON_API_KEY)
        except Exception as e:
            print(f"  [polygon] init 失敗: {type(e).__name__}: {e}")
            _polygon_client = None
    return _polygon_client


def _detect_market(ticker: str) -> str:
    """簡單判斷 ticker 屬於哪個市場"""
    t = ticker.upper()
    # TW: 4 位數 (2330) 或 4 位數+L/R (00631L) 或 .TW
    if '.TW' in t:
        return 'tw'
    pure = t.replace('.TW', '')
    if pure.isdigit() and 4 <= len(pure) <= 6:
        return 'tw'
    if pure[:4].isdigit() and len(pure) <= 6:  # ETF 含 L/R/K/U 字尾
        return 'tw'
    # 其它視為美股
    return 'us'


def _cache_path(ticker: str, tf: str) -> Path:
    pure = ticker.replace('.TW', '').upper()
    return CACHE_DIR / f"{pure}_{tf}.parquet"


def _is_cache_fresh(path: Path, ttl_seconds: int) -> bool:
    if not path.exists():
        return False
    age = (datetime.now().timestamp() - path.stat().st_mtime)
    return age < ttl_seconds


def _save_cache(path: Path, df: pd.DataFrame):
    try:
        df.to_parquet(path, compression='snappy')
    except Exception as e:
        print(f"  [intraday cache] {path.name} save fail: {e}")


def _sanitize_ohlc(df: pd.DataFrame, ticker: str = '',
                     tf: str = '') -> pd.DataFrame:
    """🆕 v9.34：過濾 yfinance 壞 tick（單筆異常成交）

    偵測規則（OR — 任一條件成立即視為異常）：
      A. wick > 5 × ATR(14)（相對 vs 近期波動）
      B. wick / Close > X%（絕對門檻，依 TF 調整）
         intraday (1m-4h): 12%
         1d: 35% (容忍 meme/squeeze 行情的合理大 wick)

    被判定異常 → 把該 wick clamp 至 max/min(Open, Close)
    保留 bar 數量不變，只修正異常 wick。
    """
    if df is None or len(df) < 15:
        return df

    o, h, l, c = df['Open'], df['High'], df['Low'], df['Close']
    body_top = pd.concat([o, c], axis=1).max(axis=1)
    body_bot = pd.concat([o, c], axis=1).min(axis=1)
    upper_wick = h - body_top
    lower_wick = body_bot - l

    # 規則 A：用 True Range 算 ATR(14)
    tr1 = h - l
    tr2 = (h - c.shift(1)).abs()
    tr3 = (l - c.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14, min_periods=5).mean()

    ATR_THRESH = 5.0
    high_bad_a = (upper_wick > atr * ATR_THRESH) & atr.notna()
    low_bad_a = (lower_wick > atr * ATR_THRESH) & atr.notna()

    # 規則 B：絕對 % 門檻（防 ATR self-pollution）
    # 1d 用寬鬆門檻避免誤殺合理大 wick（GME squeeze / earnings flash crash）
    is_daily = ('1d' in (tf or '').lower() or 'day' in (tf or '').lower())
    PCT_THRESH = 0.35 if is_daily else 0.12
    safe_close = c.where(c > 0)
    high_bad_b = (upper_wick / safe_close) > PCT_THRESH
    low_bad_b = (lower_wick / safe_close) > PCT_THRESH

    high_bad = (high_bad_a | high_bad_b).fillna(False)
    low_bad = (low_bad_a | low_bad_b).fillna(False)

    n_h = int(high_bad.sum())
    n_l = int(low_bad.sum())
    if n_h == 0 and n_l == 0:
        return df

    df = df.copy()
    if n_h > 0:
        df.loc[high_bad, 'High'] = body_top[high_bad]
    if n_l > 0:
        df.loc[low_bad, 'Low'] = body_bot[low_bad]

    tk = ticker or 'unknown'
    print(f"  [intraday] {tk}: sanitize wick — {n_h} bad High, {n_l} bad Low（已 clamp 至 body）")
    return df


def _load_cache(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _fetch_polygon(ticker: str, tf: str) -> Optional[pd.DataFrame]:
    """🆕 v9.44 Polygon.io fetcher — 覆蓋 ARCA Overnight + Extended Hours

    免費版限制：
      - 5 calls/分鐘
      - 15-min 延遲
      - 2 年歷史
      - 但 **包含 20:00-04:00 ET ARCA Overnight bars**（其他 free source 沒有）

    Args:
        ticker: 股票代號（US）
        tf: timeframe ('1m', '5m', '15m', '30m', '1h', '4h', '1d')

    Returns: DataFrame 或 None
    """
    if not _has_polygon():
        return None
    client = _get_polygon_client()
    if client is None:
        return None

    # TF mapping → Polygon (multiplier, timespan)
    tf_map = {
        '1m':  (1,   'minute'),
        '5m':  (5,   'minute'),
        '15m': (15,  'minute'),
        '30m': (30,  'minute'),
        '1h':  (1,   'hour'),
        '4h':  (4,   'hour'),
        '1d':  (1,   'day'),
    }
    if tf not in tf_map:
        return None
    multiplier, timespan = tf_map[tf]

    # 抓多少歷史
    days_map = {
        '1m':  7, '5m': 30, '15m': 60, '30m': 60,
        '1h':  365, '4h': 365, '1d': 365,    # free 限 2 年，留 buffer
    }
    days_back = days_map.get(tf, 30)
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)

    try:
        aggs = client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=start.isoformat(),
            to=end.isoformat(),
            adjusted=True,
            sort='asc',
            limit=50000,
        )
        rows = []
        for a in aggs:
            # Polygon 用 ms timestamp
            ts = pd.Timestamp(a.timestamp, unit='ms', tz='UTC').tz_localize(None)
            rows.append({
                'time': ts,
                'Open': float(a.open),
                'High': float(a.high),
                'Low':  float(a.low),
                'Close': float(a.close),
                'Volume': float(a.volume) if a.volume else 0,
            })
        if not rows:
            return None
        df = pd.DataFrame(rows).set_index('time').sort_index()
        return df if len(df) > 0 else None
    except Exception as e:
        print(f"  [polygon] {ticker} {tf}: {type(e).__name__}: {str(e)[:80]}")
        return None


def _fetch_alpaca(ticker: str, tf: str, prepost: bool = True) -> Optional[pd.DataFrame]:
    """🆕 v9.42 Alpaca Markets fetcher — 真即時 + 含 pre/post-hours

    Alpaca 優勢：
      - 真即時資料（vs yfinance 15min 延遲）
      - 完整 4 AM-8 PM ET 含 pre/post-market
      - 免費 paper trading 帳戶就有 IEX feed
      - 1m / 5m / 15m / 30m / 1h / 1d 全支援

    Args:
        ticker: 股票代號（US，如 'NVDA'）
        tf: timeframe
        prepost: 是否含夜盤（Alpaca 預設都含，此參數保留相容性）

    Returns: pandas DataFrame 或 None
    """
    if not _has_alpaca():
        return None
    client = _get_alpaca_client()
    if client is None:
        return None

    try:
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        from alpaca.data.enums import DataFeed
    except Exception:
        return None

    # TF mapping
    tf_map = {
        '1m':  TimeFrame.Minute,
        '5m':  TimeFrame(5,  TimeFrameUnit.Minute),
        '15m': TimeFrame(15, TimeFrameUnit.Minute),
        '30m': TimeFrame(30, TimeFrameUnit.Minute),
        '1h':  TimeFrame.Hour,
        '4h':  TimeFrame(4,  TimeFrameUnit.Hour),
        '1d':  TimeFrame.Day,
    }
    if tf not in tf_map:
        return None
    timeframe = tf_map[tf]

    # 抓多少歷史？(配合 yfinance 慣例)
    days_map = {
        '1m':  7, '5m': 60, '15m': 60, '30m': 60,
        '1h':  730, '4h': 730, '1d': 3650,
    }
    days_back = days_map.get(tf, 60)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back)

    # feed: paper 帳戶用 'iex'，live 用 'sip'
    feed = DataFeed.IEX if ALPACA_PAPER else DataFeed.SIP

    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            feed=feed,
        )
        bars = client.get_stock_bars(req)
        df = bars.df
        if df is None or len(df) == 0:
            return None

        # 處理 multi-index (symbol, timestamp)
        if isinstance(df.index, pd.MultiIndex):
            df = df.droplevel('symbol')

        # tz strip
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_convert(None) if df.index.tz else df.index
            df.index = df.index.tz_localize(None)

        # 統一欄位 capitalize
        rename_map = {'open':'Open','high':'High','low':'Low',
                       'close':'Close','volume':'Volume'}
        df = df.rename(columns=rename_map)
        keep = [c for c in ['Open','High','Low','Close','Volume'] if c in df.columns]
        if not keep: return None
        df = df[keep].dropna(how='all')
        return df if len(df) > 0 else None
    except Exception as e:
        print(f"  [alpaca] {ticker} {tf}: {type(e).__name__}: {str(e)[:80]}")
        return None


def _fetch_yfinance(ticker: str, tf: str, market: str,
                      prepost: bool = True) -> Optional[pd.DataFrame]:
    """yfinance 抓取 — US 用裸 ticker、TW 加 .TW
    🆕 v9.32：prepost=True 預設開啟（含夜盤 pre/post-market）
    🆕 v9.32.1：period fallback 鏈 — 新上市股 730d 會 fail，自動降到 60d
    """
    try:
        import yfinance as yf
    except ImportError:
        return None
    cfg = get_tf_config(tf)
    sym = ticker if market == 'us' else f"{ticker.replace('.TW','')}.TW"
    # TW 股票沒有夜盤（期貨才有），prepost 對 .TW stock 等於 noop
    use_prepost = prepost and market == 'us' and tf != '1d'

    # period fallback 鏈：依 TF 不同設定多個嘗試 period
    # 新上市股票如 NVD（GraniteShares 2x Short NVDA）歷史 < 730d 會直接 fail
    period_chain_by_tf = {
        '1m':  ['7d'],                                  # yf 硬限制 7d
        '5m':  ['60d', '30d', '14d'],
        '15m': ['60d', '30d', '14d'],
        '30m': ['60d', '30d', '14d'],
        '1h':  ['730d', '365d', '180d', '60d'],         # 1h 最易 fail
        '1d':  ['10y', '5y', '2y', '1y', '6mo', '3mo'],
    }
    periods_to_try = period_chain_by_tf.get(tf, [cfg.yf_max_period])

    last_err = None
    for period in periods_to_try:
        try:
            df = yf.download(sym, period=period,
                             interval=cfg.yf_interval,
                             progress=False, auto_adjust=False, threads=False,
                             prepost=use_prepost)
            if df is None or df.empty:
                continue
            # 攤平 multi-index
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            # 移除 tz info（統一）
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            # 只保留 OHLCV
            keep = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in df.columns]
            if not keep:
                continue
            df = df[keep].dropna(how='all')
            if len(df) == 0:
                continue
            # 成功
            if period != periods_to_try[0]:
                print(f"  [intraday] {sym} {tf}: 用 fallback period={period} 抓到 {len(df)} bars")
            return df
        except Exception as e:
            last_err = e
            continue

    if last_err:
        print(f"  [intraday] yfinance 全部 fallback 都失敗 {sym} {tf}: {type(last_err).__name__}: {str(last_err)[:80]}")
    return None


def _fetch_fugle(ticker: str, tf: str) -> Optional[pd.DataFrame]:
    """走 fugle_connector（TW 專用、有完整歷史）"""
    try:
        from fugle_connector import get_minute_candles, _has_fugle
        if not _has_fugle():
            return None
        cfg = get_tf_config(tf)
        # fugle_connector 的 freq 跟 yfinance 一致（1m/5m/15m/30m/60m/1d）
        df = get_minute_candles(ticker.replace('.TW', ''),
                                  freq=cfg.fugle_freq, use_cache=False)
        return df if df is not None and len(df) > 0 else None
    except Exception as e:
        print(f"  [intraday] fugle fail {ticker} {tf}: {type(e).__name__}: {str(e)[:60]}")
        return None


def _resample_to_4h(df_1h: pd.DataFrame) -> Optional[pd.DataFrame]:
    """把 1h DataFrame resample 成 4h"""
    if df_1h is None or len(df_1h) == 0:
        return None
    try:
        df_4h = df_1h.resample('4h').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
        }).dropna(subset=['Close'])
        return df_4h if len(df_4h) > 0 else None
    except Exception as e:
        print(f"  [intraday] 4h resample 失敗: {type(e).__name__}: {str(e)[:60]}")
        return None


def get_intraday(ticker: str, tf: str = '5m',
                  market: str = 'auto',
                  refresh: bool = False,
                  prepost: bool = True) -> Optional[pd.DataFrame]:
    """取得 intraday 資料

    Args:
        ticker: 股票代號（TW: '2330' 或 'AAPL'）
        tf: '1m' / '5m' / '15m' / '30m' / '1h' / '4h' / '1d'
        market: 'tw' / 'us' / 'auto'（自動判斷）
        refresh: 強制重抓（跳過快取）
        prepost: 是否含夜盤（pre/post-market）— 預設 True（v9.32）

    Returns:
        DataFrame [Open, High, Low, Close, Volume]，index=DatetimeIndex
        失敗回 None
    """
    if tf not in TIMEFRAMES:
        raise ValueError(f"未支援 tf '{tf}'，可用 {list(TIMEFRAMES.keys())}")

    if market == 'auto':
        market = _detect_market(ticker)

    # 🆕 v9.34：4h 沒有原生 source — 抓 1h 再 resample
    if tf == '4h':
        cpath_4h = _cache_path(ticker, '4h')
        cfg_4h = get_tf_config('4h')
        if not refresh and _is_cache_fresh(cpath_4h, cfg_4h.cache_ttl_seconds):
            df_cached = _load_cache(cpath_4h)
            if df_cached is not None and len(df_cached) > 0:
                return df_cached
        df_1h = get_intraday(ticker, '1h', market=market,
                              refresh=refresh, prepost=prepost)
        if df_1h is None or len(df_1h) == 0:
            return None
        df_4h = _resample_to_4h(df_1h)
        if df_4h is not None and len(df_4h) > 0:
            _save_cache(cpath_4h, df_4h)
        return df_4h

    cfg = get_tf_config(tf)
    cpath = _cache_path(ticker, tf)

    # 1. 快取（如果新鮮）
    if not refresh and _is_cache_fresh(cpath, cfg.cache_ttl_seconds):
        df = _load_cache(cpath)
        if df is not None and len(df) > 0:
            # 🆕 v9.34：cache 也可能含壞 tick（舊版抓的）→ 過載時清理
            return _sanitize_ohlc(df, ticker=f'{ticker} {tf} (cache)', tf=tf)

    # 2. TW 優先試 Fugle（如果有 API key）— TW 股票沒夜盤，不需 prepost
    df = None
    if market == 'tw':
        df = _fetch_fugle(ticker, tf)
        # 🆕 v9.33：Fugle 預設只抓近 30 天，1d 通常給 20 bars 不夠用 → fallback
        # 對其他 TF：若給的少於該 TF 合理量也要 fallback
        _min_bars = {
            '1m': 100, '5m': 200, '15m': 100,
            '30m': 100, '1h': 100, '1d': 200,
        }
        if df is not None and len(df) < _min_bars.get(tf, 100):
            print(f"  [intraday] {ticker} {tf}: Fugle 只給 {len(df)} bars "
                  f"(< {_min_bars.get(tf, 100)}) → fallback yfinance")
            df = None

    # 🆕 v9.42-44：US 三路智能合併 (Alpaca 即時 + yfinance 盤後 + Polygon overnight)
    if (df is None or len(df) == 0) and market == 'us' and _has_alpaca():
        df = _fetch_alpaca(ticker, tf, prepost=prepost)
        if df is not None and len(df) > 0:
            print(f"  [intraday] {ticker} {tf}: Alpaca 抓到 {len(df)} bars（真即時+夜盤）")

            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

            def _merge_in(df_base, df_new, src_name):
                """把 df_new 合進 df_base，回傳 (df_merged, n_added)"""
                if df_new is None or len(df_new) == 0:
                    return df_base, 0
                last_old = df_base.index[-1]
                last_new = df_new.index[-1]
                if last_new <= last_old:
                    return df_base, 0
                merged = pd.concat([df_base, df_new]).sort_index()
                merged = merged[~merged.index.duplicated(keep='last')]
                n_added = (merged.index > last_old).sum()
                return merged, n_added

            try:
                last_alpaca = df.index[-1]
                age_min = (now_utc - last_alpaca).total_seconds() / 60

                # 🆕 v9.43：若 Alpaca 最新 bar > 30 min 前，補 yfinance 盤後
                if age_min > 30 and tf in ('1m', '5m', '15m', '30m', '1h'):
                    df_yf = _fetch_yfinance(ticker, tf, market, prepost=prepost)
                    df, n_yf = _merge_in(df, df_yf, 'yfinance')
                    if n_yf > 0:
                        print(f"  [intraday] {ticker} {tf}: 補 yfinance {n_yf} 個盤後 bars "
                              f"(最新 {df.index[-1]})")

                # 🆕 v9.44：若仍 > 30 min 前，補 Polygon (overnight)
                last_after_yf = df.index[-1]
                age_after_yf = (now_utc - last_after_yf).total_seconds() / 60
                if age_after_yf > 30 and _has_polygon() and tf in ('1m','5m','15m','30m','1h'):
                    df_pg = _fetch_polygon(ticker, tf)
                    df, n_pg = _merge_in(df, df_pg, 'polygon')
                    if n_pg > 0:
                        print(f"  [intraday] {ticker} {tf}: 補 Polygon {n_pg} 個 overnight bars "
                              f"(最新 {df.index[-1]})")
            except Exception as _merge_err:
                print(f"  [intraday] 智能合併失敗: {type(_merge_err).__name__}: {_merge_err}")

    # 3. yfinance fallback（無 Alpaca 時走 yfinance）
    if df is None or len(df) == 0:
        df = _fetch_yfinance(ticker, tf, market, prepost=prepost)

    # 🆕 v9.44：若連 yfinance 都失敗，最後試 Polygon
    if (df is None or len(df) == 0) and market == 'us' and _has_polygon():
        df = _fetch_polygon(ticker, tf)
        if df is not None and len(df) > 0:
            print(f"  [intraday] {ticker} {tf}: Polygon fallback 抓到 {len(df)} bars")

    # 4. 仍然失敗 → 試讀過期快取（總比沒有好）
    if df is None or len(df) == 0:
        df = _load_cache(cpath)
        if df is not None and len(df) > 0:
            print(f"  [intraday] {ticker} {tf} 用過期快取（fresh fetch 失敗）")
        return df

    # 5. 🆕 v9.34：過濾壞 tick（異常 wick） + 寫快取
    df = _sanitize_ohlc(df, ticker=f'{ticker} {tf}', tf=tf)
    _save_cache(cpath, df)
    return df


def get_session_bars_today(ticker: str, tf: str = '5m',
                            market: str = 'auto') -> Optional[pd.DataFrame]:
    """只取「今日 session」的 bars（用於 intraday-only 分析）"""
    df = get_intraday(ticker, tf, market)
    if df is None or len(df) == 0:
        return None
    last_date = df.index[-1].date()
    today_df = df[df.index.date == last_date]
    return today_df if len(today_df) > 0 else None


def market_info(ticker: str) -> dict:
    """回傳 ticker 的市場 metadata"""
    market = _detect_market(ticker)
    return {
        'ticker': ticker.replace('.TW', '').upper(),
        'market': market,
        'yf_symbol': ticker if market == 'us' else f"{ticker.replace('.TW','')}.TW",
        'session_hours': '09:00-13:30 (TW)' if market == 'tw' else '09:30-16:00 ET (US)',
    }
