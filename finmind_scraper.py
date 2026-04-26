"""
FinMind 三大法人爬蟲（替代受封鎖的 TWSE 直連）

API: https://api.finmindtrade.com/api/v4/data
Dataset: TaiwanStockInstitutionalInvestorsBuySell

特性：
  - 免費版每張票一次拉 6 年（一個請求 = 一檔完整時序）
  - name 欄位 = Foreign_Investor / Investment_Trust / Dealer_self /
                 Dealer_Hedging / Foreign_Dealer_Self
  - 已內建保守速率（每請求間隔 1.2s，避免免費版限流）

輸出：
  inst_per_ticker/{ticker}.parquet  每股 wide-format（date × inv_type buy/sell/net）

用法：
  python finmind_scraper.py                 # 抓全部 cached tickers
  python finmind_scraper.py --max 50        # 只抓前 50 檔
  python finmind_scraper.py --tickers 2330,2317
  python finmind_scraper.py --info
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import argparse
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

ROOT = Path(__file__).parent
DATA_CACHE = ROOT / 'data_cache'
INST_DIR = ROOT / 'inst_per_ticker'
INST_DIR.mkdir(exist_ok=True)

API_URL = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanStockInstitutionalInvestorsBuySell"

START_DEFAULT = '2020-01-02'
END_DEFAULT = '2026-04-25'

# FinMind 免費版限流：~600 req/hour
RATE_DELAY = 1.2


def fetch_one_ticker(ticker: str, start: str, end: str,
                     retry: int = 2, force: bool = False) -> pd.DataFrame:
    out = INST_DIR / f"{ticker}.parquet"
    if out.exists() and not force:
        try: return pd.read_parquet(out)
        except: pass

    params = dict(dataset=DATASET, data_id=ticker,
                  start_date=start, end_date=end)
    for attempt in range(retry + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=30)
            j = r.json()
            if j.get('status') != 200:
                msg = str(j.get('msg', ''))[:120]
                if 'level' in msg.lower() or 'limit' in msg.lower():
                    # 達免費版限流：等較久
                    time.sleep(60)
                    continue
                # 該股無資料：寫空檔避免重試
                pd.DataFrame().to_parquet(out)
                return pd.DataFrame()
            rows = j.get('data', [])
            if not rows:
                pd.DataFrame().to_parquet(out)
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            df['net'] = df['buy'] - df['sell']
            # 樞紐成 wide：date × name → buy/sell/net
            wide = df.pivot_table(
                index='date', columns='name',
                values=['buy', 'sell', 'net'],
                aggfunc='sum', fill_value=0,
            )
            wide.columns = [f"{a}_{b}" for a, b in wide.columns]
            wide = wide.reset_index()
            wide['date'] = pd.to_datetime(wide['date'])
            wide.to_parquet(out)
            return wide
        except Exception as e:
            if attempt == retry:
                print(f"  [失敗] {ticker}: {e}", flush=True)
                return pd.DataFrame()
            time.sleep(3.0)
    return pd.DataFrame()


def fetch_all(tickers: list, start: str, end: str,
              max_workers: int = 2, force: bool = False):
    todo = [t for t in tickers
            if force or not (INST_DIR / f"{t}.parquet").exists()]
    print(f"[FinMind 三大法人] {len(tickers)} 檔，待抓 {len(todo)} 檔")
    print(f"  範圍：{start} ~ {end}")
    print(f"  workers: {max_workers}, 速率延遲: {RATE_DELAY}s/req\n")

    if not todo:
        print("全部已快取")
        return

    t0 = time.time()
    done = 0
    ok = 0
    empty = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {}
        for t in todo:
            futures[ex.submit(fetch_one_ticker, t, start, end)] = t
            time.sleep(RATE_DELAY)    # submit-side 節流

        for fut in as_completed(futures):
            t = futures[fut]
            done += 1
            try:
                df = fut.result(timeout=60)
                if df.empty:
                    empty += 1
                else:
                    ok += 1
                eta = (time.time() - t0) / done * (len(todo) - done)
                if done % 20 == 0 or done == len(todo):
                    print(f"  [{done}/{len(todo)}] {t}: {len(df)} 列  "
                          f"ok={ok} empty={empty}  ETA {eta/60:.1f}min",
                          flush=True)
            except Exception as e:
                print(f"  [{done}/{len(todo)}] {t}: ERROR {e}")

    print(f"\n總耗時：{(time.time()-t0)/60:.1f} min")
    print(f"成功：{ok}，無資料：{empty}")


def info():
    files = sorted(INST_DIR.glob("*.parquet"))
    if not files:
        print("無快取")
        return
    total = sum(f.stat().st_size for f in files)
    non_empty = 0
    sample = None
    for f in files:
        try:
            df = pd.read_parquet(f)
            if not df.empty:
                non_empty += 1
                if sample is None: sample = (f.stem, df)
        except: pass
    print(f"快取：{INST_DIR}")
    print(f"  檔數：{len(files)}（有資料 {non_empty}）")
    print(f"  總大小：{total/1024/1024:.1f} MB")
    if sample:
        name, df = sample
        print(f"\n樣本 {name}：{len(df)} 列")
        print(f"  日期：{df['date'].min().date()} ~ {df['date'].max().date()}")
        print(f"  欄位：{list(df.columns)}")


def list_cached_tickers() -> list:
    return sorted(p.stem for p in DATA_CACHE.glob("*.parquet"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default=START_DEFAULT)
    ap.add_argument('--end', default=END_DEFAULT)
    ap.add_argument('--max', type=int, default=None, help='限制檔數')
    ap.add_argument('--tickers', default=None, help='逗號分隔')
    ap.add_argument('--workers', type=int, default=2)
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--info', action='store_true')
    args = ap.parse_args()

    if args.info:
        info()
        return

    if args.tickers:
        ts = [t.strip() for t in args.tickers.split(',') if t.strip()]
    else:
        ts = list_cached_tickers()
        if args.max: ts = ts[:args.max]

    fetch_all(ts, args.start, args.end,
              max_workers=args.workers, force=args.force)


if __name__ == '__main__':
    main()
