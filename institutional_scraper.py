"""
三大法人買賣超爬蟲（TWSE 官方資料）

資料來源：
  https://www.twse.com.tw/rwd/zh/fund/T86?date=YYYYMMDD&selectType=ALL&response=json

抓取內容（每日全市場）：
  外資買賣超股數
  投信買賣超股數
  自營商買賣超股數
  三大法人合計買賣超

儲存：
  inst_cache/{YYYY-MM-DD}.parquet  每日一個檔（1500 個交易日）
  inst_aggregated.parquet           聚合後 multi-index (date, ticker)

用法：
  python institutional_scraper.py --start 2020-01-02 --end 2026-04-25
  python institutional_scraper.py --recent 30   # 最近 30 個交易日
  python institutional_scraper.py --info        # 看快取狀態
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import argparse
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CACHE_DIR = Path(__file__).parent / 'inst_cache'
CACHE_DIR.mkdir(exist_ok=True)

TWSE_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"


def cache_path(date: str) -> Path:
    return CACHE_DIR / f"{date}.parquet"


def fetch_one_day(date: str, retry=2) -> pd.DataFrame:
    """抓單日全市場三大法人資料"""
    p = cache_path(date)
    if p.exists():
        try:
            return pd.read_parquet(p)
        except:
            pass

    date_no_dash = date.replace('-', '')
    params = {
        "date": date_no_dash,
        "selectType": "ALL",
        "response": "json",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    for attempt in range(retry + 1):
        try:
            r = requests.get(TWSE_URL, params=params, headers=headers,
                             timeout=15, verify=False)
            if r.status_code != 200:
                continue
            data = r.json()
            if data.get('stat') != 'OK':
                # 該日無資料（假日）
                empty_df = pd.DataFrame()
                empty_df.to_parquet(p)
                return empty_df
            fields = data.get('fields', [])
            rows = data.get('data', [])
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=fields)
            # 標準化欄位名
            df.columns = [c.strip() for c in df.columns]
            # 找 ticker 欄位
            ticker_col = None
            for c in df.columns:
                if '證券代號' in c or 'Securities' in c:
                    ticker_col = c
                    break
            if ticker_col:
                df = df.rename(columns={ticker_col: 'ticker'})
                df['ticker'] = df['ticker'].astype(str).str.strip()
            df['date'] = date
            # 數值欄位轉 numeric
            for c in df.columns:
                if c in ('ticker', 'date'): continue
                df[c] = pd.to_numeric(
                    df[c].astype(str).str.replace(',', ''),
                    errors='coerce'
                )
            df.to_parquet(p)
            return df
        except Exception as e:
            if attempt == retry:
                print(f"  [失敗] {date}: {e}", flush=True)
                return pd.DataFrame()
            time.sleep(1.5)
    return pd.DataFrame()


def get_business_days(start: str, end: str) -> list:
    """產生交易日清單（簡單版，不考慮假日，由 fetch 時處理）"""
    s = datetime.strptime(start, '%Y-%m-%d')
    e = datetime.strptime(end, '%Y-%m-%d')
    days = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:    # 週一到五
            days.append(cur.strftime('%Y-%m-%d'))
        cur += timedelta(days=1)
    return days


def fetch_range(start: str, end: str, max_workers: int = 5):
    """批次抓取日期區間"""
    days = get_business_days(start, end)
    todo = [d for d in days if not cache_path(d).exists()]
    print(f"[範圍] {start} ~ {end}：{len(days)} 個交易日，待抓 {len(todo)} 個\n")

    if not todo:
        print("全部已快取")
        return

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_one_day, d): d for d in todo}
        for fut in as_completed(futures):
            d = futures[fut]
            done += 1
            try:
                df = fut.result(timeout=30)
                rows = len(df)
                eta = (time.time() - t0) / done * (len(todo) - done)
                print(f"  [{done}/{len(todo)}] {d}: {rows} 檔  ETA {eta:.0f}s",
                      flush=True)
            except Exception as e:
                print(f"  [{done}/{len(todo)}] {d}: ERROR {e}")
            time.sleep(0.4)    # 避免被 rate limit

    print(f"\n總耗時：{time.time()-t0:.1f}s")


def aggregate_to_single():
    """合併所有日檔為一個 multi-index DataFrame"""
    files = sorted(CACHE_DIR.glob("*.parquet"))
    print(f"合併 {len(files)} 個日檔...")

    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if not df.empty and 'ticker' in df.columns:
                dfs.append(df)
        except: pass

    if not dfs:
        print("無資料")
        return

    full = pd.concat(dfs, ignore_index=True)
    print(f"總筆數：{len(full):,}")
    print(f"日期範圍：{full['date'].min()} ~ {full['date'].max()}")

    # 印欄位
    print("\n欄位清單：")
    for c in full.columns[:15]:
        print(f"  {c}")

    out = Path(__file__).parent / 'inst_aggregated.parquet'
    full.to_parquet(out)
    print(f"\n已輸出：{out}（{out.stat().st_size / 1024 / 1024:.1f} MB）")
    return full


def info():
    """檢查快取狀態"""
    files = sorted(CACHE_DIR.glob("*.parquet"))
    if not files:
        print("快取為空")
        return
    total_size = sum(f.stat().st_size for f in files)
    print(f"快取目錄：{CACHE_DIR}")
    print(f"  日檔數：{len(files)}")
    print(f"  總大小：{total_size / 1024 / 1024:.1f} MB")
    print(f"  首日：{files[0].stem}")
    print(f"  末日：{files[-1].stem}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2020-01-02')
    ap.add_argument('--end', default=None, help='預設今日')
    ap.add_argument('--recent', type=int, default=None, help='只抓最近 N 天')
    ap.add_argument('--info', action='store_true')
    ap.add_argument('--aggregate', action='store_true', help='合併所有日檔')
    ap.add_argument('--workers', type=int, default=3)
    args = ap.parse_args()

    if args.info:
        info()
        return
    if args.aggregate:
        aggregate_to_single()
        return

    end = args.end or datetime.now().strftime('%Y-%m-%d')
    if args.recent:
        end_dt = datetime.strptime(end, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=args.recent * 2)
        start = start_dt.strftime('%Y-%m-%d')
    else:
        start = args.start

    fetch_range(start, end, max_workers=args.workers)


if __name__ == '__main__':
    main()
