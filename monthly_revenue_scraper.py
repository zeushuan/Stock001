"""
台股月營收爬蟲（FinMind 來源）

dataset: TaiwanStockMonthRevenue
每股 76 個月（2020-01 ~ 2026-04）
date = 公布月首日（如 2026-04-01 對應 3 月營收）

輸出：
  monthly_revenue.parquet  單一聚合檔（multi-index ticker × date）

用法：
  python monthly_revenue_scraper.py            # 抓全 universe
  python monthly_revenue_scraper.py --max 100
  python monthly_revenue_scraper.py --info
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import argparse
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

ROOT = Path(__file__).parent
OUT_FILE = ROOT / 'monthly_revenue.parquet'
UNIVERSE_FILE = ROOT / 'tw_universe.txt'

API = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanStockMonthRevenue"
START = "2019-01-01"   # 多拉一年方便算 YoY
END_DEFAULT = "2026-04-25"

WRITE_LOCK = threading.Lock()


def load_universe() -> list:
    """從 tw_universe.txt 取得股票清單（只取股票，不含 ETF/ETN/特別股）"""
    out = []
    if not UNIVERSE_FILE.exists(): return out
    for line in UNIVERSE_FILE.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 3 and parts[2] == '股票':
            out.append(parts[0])
    return sorted(set(out))


def fetch_one(ticker: str, end_date: str) -> pd.DataFrame:
    try:
        r = requests.get(API, params=dict(
            dataset=DATASET, data_id=ticker,
            start_date=START, end_date=end_date,
        ), timeout=20)
        j = r.json()
        if j.get('status') != 200: return None
        rows = j.get('data', [])
        if not rows: return None
        df = pd.DataFrame(rows)
        df['ticker'] = ticker
        df['date'] = pd.to_datetime(df['date'])
        return df[['ticker', 'date', 'revenue', 'revenue_year', 'revenue_month']]
    except Exception:
        return None


def scrape(max_workers=4, limit=None, end_date=END_DEFAULT):
    universe = load_universe()
    if limit: universe = universe[:limit]
    print(f"[掃描] 股票 {len(universe)} 檔，end={end_date}")

    all_rows = []
    success = 0; failed = 0
    t0 = time.time()

    def _task(t):
        time.sleep(0.4)
        return t, fetch_one(t, end_date)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_task, t) for t in universe]
        for i, fut in enumerate(as_completed(futures)):
            try:
                ticker, df = fut.result(timeout=30)
            except Exception:
                failed += 1; continue
            if df is None or df.empty:
                failed += 1
            else:
                all_rows.append(df)
                success += 1
            if (i+1) % 50 == 0:
                eta = (time.time()-t0) / (i+1) * (len(universe)-i-1)
                print(f"  [{i+1}/{len(universe)}]  ok={success} fail={failed}  "
                      f"ETA {eta/60:.1f}min", flush=True)

    if not all_rows:
        print("無資料"); return
    full = pd.concat(all_rows, ignore_index=True)
    full = full.sort_values(['ticker', 'date']).reset_index(drop=True)
    full.to_parquet(OUT_FILE)
    sz = OUT_FILE.stat().st_size / 1024
    print(f"\n總耗時 {(time.time()-t0)/60:.1f}min  寫入 {OUT_FILE} ({sz:.0f} KB)")
    print(f"  個股: {success}  總筆數: {len(full):,}")


def info():
    if not OUT_FILE.exists():
        print("無快取"); return
    df = pd.read_parquet(OUT_FILE)
    print(f"檔案: {OUT_FILE} ({OUT_FILE.stat().st_size/1024:.0f} KB)")
    print(f"  總筆數: {len(df):,}")
    print(f"  個股數: {df['ticker'].nunique()}")
    print(f"  日期範圍: {df['date'].min().date()} ~ {df['date'].max().date()}")
    sample_t = df['ticker'].iloc[0]
    sample = df[df['ticker'] == sample_t].tail(5)
    print(f"\n樣本 ({sample_t}):")
    for _, r in sample.iterrows():
        print(f"  {r['date'].date()}  營收={r['revenue']/1e8:.1f} 億")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--max', type=int, default=None)
    ap.add_argument('--end', default=END_DEFAULT)
    ap.add_argument('--info', action='store_true')
    args = ap.parse_args()
    if args.info: info(); return
    scrape(max_workers=args.workers, limit=args.max, end_date=args.end)


if __name__ == '__main__':
    main()
