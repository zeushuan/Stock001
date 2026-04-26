"""
台股毛利率爬蟲（FinMind 財報資料）

dataset: TaiwanStockFinancialStatements
每股 24 季（6 年 × 4 季）
取 Revenue + GrossProfit → 計算毛利率

輸出：
  margin_quarterly.parquet   ticker × date(季底) × revenue / gross_profit / margin

用法：
  python margin_scraper.py
  python margin_scraper.py --info
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
OUT_FILE = ROOT / 'margin_quarterly.parquet'
UNIVERSE_FILE = ROOT / 'tw_universe.txt'

API = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanStockFinancialStatements"
START = "2019-01-01"
END_DEFAULT = "2026-04-25"

WRITE_LOCK = threading.Lock()


def load_universe() -> list:
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
        # 取 Revenue 與 GrossProfit
        rev = df[df['type'] == 'Revenue'][['date', 'value']].rename(
            columns={'value': 'revenue'})
        gp = df[df['type'] == 'GrossProfit'][['date', 'value']].rename(
            columns={'value': 'gross_profit'})
        if rev.empty or gp.empty: return None
        merged = rev.merge(gp, on='date', how='inner')
        merged['ticker'] = ticker
        merged['date'] = pd.to_datetime(merged['date'])
        merged['margin'] = merged['gross_profit'] / merged['revenue']
        return merged[['ticker', 'date', 'revenue', 'gross_profit', 'margin']]
    except Exception:
        return None


def scrape(max_workers=4, limit=None, end_date=END_DEFAULT,
           resume: bool = True):
    universe = load_universe()
    if limit: universe = universe[:limit]

    # 續抓模式：跳過已經有資料的 ticker
    existing_tickers = set()
    existing_df = None
    if resume and OUT_FILE.exists():
        try:
            existing_df = pd.read_parquet(OUT_FILE)
            existing_tickers = set(existing_df['ticker'].unique())
            print(f"[續抓] 已有 {len(existing_tickers)} 檔資料，跳過")
        except Exception:
            existing_df = None

    todo = [t for t in universe if t not in existing_tickers]
    print(f"[掃描] 股票 {len(universe)} 檔（毛利率），待抓 {len(todo)} 檔")
    if not todo:
        print("全部已抓完")
        return

    all_rows = [existing_df] if existing_df is not None else []
    universe = todo
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
    print(f"\n毛利率分布:")
    print(f"  中位數: {df['margin'].median()*100:.1f}%")
    print(f"  平均:   {df['margin'].mean()*100:.1f}%")
    sample_t = '2330'
    sample = df[df['ticker'] == sample_t].tail(8)
    print(f"\n2330 台積電最近 8 季：")
    for _, r in sample.iterrows():
        print(f"  {r['date'].date()}  營收 {r['revenue']/1e8:>6.0f}億  毛利 "
              f"{r['gross_profit']/1e8:>5.0f}億  毛利率 {r['margin']*100:>5.1f}%")


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
