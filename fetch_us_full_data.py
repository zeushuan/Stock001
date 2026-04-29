"""抓 us_full_tickers.json 全部 5,629 檔 US 股票 6 年日線
============================================================
平行 ThreadPoolExecutor（yfinance 是 I/O bound）
跳過已快取，斷點續傳安全
"""
import sys, time, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import yfinance as yf
import pandas as pd
import ta

CACHE = Path('data_cache')
CACHE.mkdir(exist_ok=True)
WORKERS = 12  # yfinance 可承受 ~10-15 並行
MIN_DAYS = 280  # v8 需要至少 280 日（120 EMA 預熱）


def calc_ind(df):
    if df is None or len(df) < MIN_DAYS: return None
    df = df.copy()
    df['e10']  = ta.trend.ema_indicator(df['Close'], window=10)
    df['e20']  = ta.trend.ema_indicator(df['Close'], window=20)
    df['e60']  = ta.trend.ema_indicator(df['Close'], window=60)
    df['e120'] = ta.trend.ema_indicator(df['Close'], window=120)
    df['rsi']  = ta.momentum.rsi(df['Close'], window=14)
    df['adx']  = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    macd = ta.trend.MACD(df['Close'])
    df['mh']   = macd.macd_diff()
    df['atr']  = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], 14)
    bb = ta.volatility.BollingerBands(df['Close'], window=20)
    df['pctb'] = bb.bollinger_pband()
    return df


def fetch_one(ticker):
    out = CACHE / f'{ticker}.parquet'
    if out.exists():
        try:
            df = pd.read_parquet(out)
            if len(df) >= MIN_DAYS: return ('skip', ticker)
        except: pass
    try:
        df = yf.Ticker(ticker).history(period='6y', interval='1d', auto_adjust=False)
        if df is None or df.empty:
            return ('empty', ticker)
        if len(df) < MIN_DAYS:
            return ('short', ticker)
        df = df[['Open','High','Low','Close','Volume']]
        df = calc_ind(df)
        if df is None: return ('calc_fail', ticker)
        df.to_parquet(out)
        return ('ok', ticker)
    except Exception as e:
        msg = str(e)[:80]
        return ('err:' + msg, ticker)


def main():
    with open('us_full_tickers.json', encoding='utf-8') as f:
        meta = json.load(f)
    all_tickers = meta['tickers']
    print(f"📋 us_full_tickers.json 共 {len(all_tickers)} 檔")

    todo = [t for t in all_tickers if not (CACHE / f'{t}.parquet').exists()]
    skipped = len(all_tickers) - len(todo)
    print(f"  已快取：{skipped}")
    print(f"  待抓：{len(todo)}")
    if not todo:
        print("\n✅ 全部已快取")
        return

    print(f"\n🔄 平行抓取（workers={WORKERS}）...\n")
    t0 = time.time()
    counts = {}
    n_done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_one, t): t for t in todo}
        for fut in as_completed(futures):
            status, ticker = fut.result()
            base_status = status.split(':')[0]
            counts[base_status] = counts.get(base_status, 0) + 1
            n_done += 1
            if n_done % 100 == 0:
                elapsed = (time.time() - t0) / 60
                eta = elapsed / n_done * (len(todo) - n_done)
                print(f"[{n_done:5d}/{len(todo)}] "
                      f"ok={counts.get('ok', 0)} "
                      f"empty={counts.get('empty', 0)} "
                      f"short={counts.get('short', 0)} "
                      f"err={counts.get('err', 0)}  "
                      f"已 {elapsed:.1f}min，ETA {eta:.1f}min", flush=True)

    elapsed = (time.time() - t0) / 60
    print(f"\n📊 完成 {elapsed:.1f}min")
    print(f"   ok      = {counts.get('ok', 0)}")
    print(f"   skip    = {skipped}")
    print(f"   empty   = {counts.get('empty', 0)}")
    print(f"   short   = {counts.get('short', 0)} (< {MIN_DAYS} 日)")
    print(f"   err     = {counts.get('err', 0)}")
    print(f"\n💾 data_cache 現有 US 股票（純大寫）...")
    existing = sum(1 for p in CACHE.glob('*.parquet')
                   if p.stem.isalpha() and p.stem.isupper())
    print(f"   {existing} 檔")


if __name__ == '__main__':
    main()
