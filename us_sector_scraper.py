"""
美股 GICS Sector 補完爬蟲（yfinance .info）

目的：us_sectors.txt 目前僅 S&P 500 + NASDAQ ETF（~1700 有 sector），
     剩餘 ~6400 NYSE 中小型股需逐檔抓 yfinance .info 補上 sector。

用法：
  python us_sector_scraper.py                # 抓所有缺失的
  python us_sector_scraper.py --max 500      # 限制本次處理數量
  python us_sector_scraper.py --workers 6    # 多執行緒（預設 4）
  python us_sector_scraper.py --info         # 看進度

特性：
  - 進度持久化：每 50 筆寫一次 us_sectors.txt（中斷可續抓）
  - 失敗自動標記為 NO_SECTOR，避免重抓
  - 速率：每執行緒 0.3s 間隔，4 worker 約 13 req/s，~6400 檔 ~8 分鐘
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import argparse
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

ROOT = Path(__file__).parent
SECTORS_FILE = ROOT / 'us_sectors.txt'
NAMES_FILE = ROOT / 'us_names.txt'

WRITE_LOCK = threading.Lock()


def load_existing() -> dict:
    """載入現有 us_sectors.txt → {ticker: (name, sector, sub)}"""
    out = {}
    if not SECTORS_FILE.exists(): return out
    for line in SECTORS_FILE.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 4:
            out[parts[0].strip()] = (parts[1], parts[2], parts[3])
    return out


def load_universe() -> dict:
    """從 us_names.txt 載入全代號 → name"""
    out = {}
    if not NAMES_FILE.exists(): return out
    for line in NAMES_FILE.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|', 1)
        if len(parts) >= 2:
            out[parts[0].strip()] = parts[1].strip()
    return out


def write_sectors(data: dict):
    """寫回 us_sectors.txt（atomic）"""
    tmp = SECTORS_FILE.with_suffix('.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        f.write('# ticker|name|sector|sub_industry\n')
        for s in sorted(data):
            n, sec, sub = data[s]
            f.write(f'{s}|{n}|{sec}|{sub}\n')
    tmp.replace(SECTORS_FILE)


def fetch_one(ticker: str, name_fallback: str = "") -> tuple:
    """抓單檔 sector / sub-industry。失敗回 (name, NO_SECTOR, '')"""
    try:
        info = yf.Ticker(ticker).get_info()
        if not info or not isinstance(info, dict):
            return (name_fallback, 'NO_SECTOR', '')
        sector = (info.get('sector') or '').strip()
        industry = (info.get('industry') or '').strip()
        long_name = (info.get('longName') or info.get('shortName') or
                     name_fallback or '').strip()
        # 判斷 ETF / 基金
        quote_type = (info.get('quoteType') or '').upper()
        if quote_type == 'ETF' and not sector:
            sector = 'ETF'
        return (long_name, sector or 'NO_SECTOR', industry)
    except Exception:
        return (name_fallback, 'NO_SECTOR', '')


def scrape(max_workers: int = 4, limit: int = None, retry_no_sector: bool = False):
    universe = load_universe()
    existing = load_existing()

    todo = []
    for sym, name in universe.items():
        if sym in existing:
            cur_sec = existing[sym][1]
            # 已有 sector 且非 NO_SECTOR：跳過
            if cur_sec and cur_sec != 'NO_SECTOR':
                continue
            # NO_SECTOR：除非 --retry 才重抓
            if cur_sec == 'NO_SECTOR' and not retry_no_sector:
                continue
        todo.append((sym, name))

    if limit:
        todo = todo[:limit]
    print(f"[補完] 全 universe {len(universe)}，已有 sector {len(existing)}，"
          f"本次處理 {len(todo)} 檔（workers={max_workers}）")
    if not todo:
        print("無需處理"); return

    t0 = time.time()
    done = 0
    new = 0
    sectors_local = dict(existing)

    def _task(item):
        sym, name = item
        # 速率：每 thread 0.3s 間隔
        time.sleep(0.3)
        return sym, fetch_one(sym, name)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_task, x) for x in todo]
        for fut in as_completed(futures):
            try:
                sym, (n, sec, sub) = fut.result(timeout=30)
            except Exception:
                continue
            done += 1
            sectors_local[sym] = (n, sec, sub)
            if sec and sec != 'NO_SECTOR':
                new += 1
            # 每 50 筆寫一次
            if done % 50 == 0 or done == len(todo):
                with WRITE_LOCK:
                    write_sectors(sectors_local)
                eta = (time.time()-t0) / done * (len(todo)-done)
                print(f"  [{done}/{len(todo)}] 新增 sector: {new}  "
                      f"ETA {eta/60:.1f}min", flush=True)

    print(f"\n總耗時 {(time.time()-t0)/60:.1f} min")
    print(f"成功補上 sector：{new} / {len(todo)}")


def info():
    universe = load_universe()
    existing = load_existing()
    have_sector = sum(1 for _, sec, _ in existing.values()
                      if sec and sec != 'NO_SECTOR')
    no_sector = sum(1 for _, sec, _ in existing.values() if sec == 'NO_SECTOR')
    print(f"universe (us_names.txt): {len(universe)}")
    print(f"已紀錄 (us_sectors.txt): {len(existing)}")
    print(f"  ├ 有 sector：{have_sector}")
    print(f"  ├ 無資料 (NO_SECTOR)：{no_sector}")
    print(f"  └ 待抓：{len(universe) - len(existing)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--max', type=int, default=None)
    ap.add_argument('--retry-no-sector', action='store_true',
                    help='重抓之前標為 NO_SECTOR 的')
    ap.add_argument('--info', action='store_true')
    args = ap.parse_args()
    if args.info:
        info(); return
    scrape(max_workers=args.workers, limit=args.max,
           retry_no_sector=args.retry_no_sector)


if __name__ == '__main__':
    main()
