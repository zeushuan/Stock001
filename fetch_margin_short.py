"""抓全市場每日融資/融券餘額（FinMind，多 token 平行）
======================================================
資料源：TaiwanStockMarginPurchaseShortSale
輸出：margin_cache/{ticker}.parquet
欄位：date / margin_balance / short_balance / msratio (券資比 %)
"""
import sys, os, time, requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import pandas as pd

CACHE_DIR = Path(__file__).parent / 'margin_cache'
CACHE_DIR.mkdir(exist_ok=True)
DATA_DIR = Path(__file__).parent / 'data_cache'
URL = 'https://api.finmindtrade.com/api/v4/data'


def _load_env():
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        for line in env_file.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#'): continue
            if '=' in line:
                k, _, v = line.partition('=')
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
_load_env()


def collect_tokens():
    tokens = []
    p = os.environ.get('FINMIND_TOKEN', '').strip()
    if p: tokens.append(p)
    for i in range(2, 11):
        t = os.environ.get(f'FINMIND_TOKEN{i}', '').strip()
        if t: tokens.append(t)
    return tokens


def fetch_one(ticker: str, token: str,
              start='2020-01-01', end='2026-04-27') -> pd.DataFrame:
    out = CACHE_DIR / f'{ticker}.parquet'
    if out.exists():
        try: return pd.read_parquet(out)
        except: pass

    params = {
        'dataset': 'TaiwanStockMarginPurchaseShortSale',
        'data_id': ticker,
        'start_date': start,
        'end_date': end,
        'token': token,
    }
    for retry in range(3):
        try:
            r = requests.get(URL, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                rows = data.get('data', [])
                if not rows: return None
                df = pd.DataFrame(rows)
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').sort_index()
                # 抽取關鍵欄位 + 計算券資比
                d = pd.DataFrame(index=df.index)
                d['margin_balance'] = df['MarginPurchaseTodayBalance']
                d['short_balance']  = df['ShortSaleTodayBalance']
                # 券資比 = 融券餘額 / 融資餘額 × 100%
                d['msratio'] = (d['short_balance'] / d['margin_balance'].replace(0, 1)) * 100
                d.to_parquet(out)
                return d
            elif r.status_code == 402:
                time.sleep(60); continue
            else:
                return None
        except Exception:
            time.sleep(3); continue
    return None


_lock = Lock()
_progress = {'ok': 0, 'fail': 0, 'done': 0}


def worker_thread(token: str, ticker_q, total: int, sleep_s: float, t0: float):
    while True:
        try:
            ticker = ticker_q.pop(0)
        except IndexError:
            break
        df = fetch_one(ticker, token)
        with _lock:
            _progress['done'] += 1
            if df is not None and len(df) > 0:
                _progress['ok'] += 1
            else:
                _progress['fail'] += 1
            done = _progress['done']
            ok = _progress['ok']
            fail = _progress['fail']
        if done % 50 == 0:
            elapsed = (time.time() - t0) / 60
            print(f"[{done:4d}/{total}] ok={ok} fail={fail}  "
                  f"已 {elapsed:.1f} min", flush=True)
        time.sleep(sleep_s)


def main():
    tokens = collect_tokens()
    if not tokens:
        print("❌ 未設定 FINMIND_TOKEN")
        return

    universe = sorted(p.stem for p in DATA_DIR.glob('*.parquet')
                      if p.stem and p.stem[0].isdigit() and len(p.stem) == 4)
    have = set(p.stem for p in CACHE_DIR.glob('*.parquet'))
    todo = [t for t in universe if t not in have]
    print(f"全市場：{len(universe)} 檔 / 待抓 {len(todo)} 檔 / Tokens: {len(tokens)}")
    sleep_s = 1.5
    print(f"預估：{len(todo) * sleep_s / len(tokens) / 60:.1f} min\n")

    if not todo:
        print("✅ 全部已快取")
        return

    t0 = time.time()
    tq = list(todo)
    with ThreadPoolExecutor(max_workers=len(tokens)) as ex:
        futures = [ex.submit(worker_thread, tok, tq, len(todo), sleep_s, t0)
                   for tok in tokens]
        for f in as_completed(futures):
            f.result()

    elapsed = (time.time() - t0) / 60
    print(f"\n總耗時 {elapsed:.1f} min  成功 {_progress['ok']}/{len(todo)}  失敗 {_progress['fail']}")


if __name__ == '__main__':
    main()
