"""抓全市場集保戶股權分散（FinMind）
=========================================
Dataset: TaiwanStockShareholding
單次抓 6 年完整資料（不限速）
雙 token 平行
"""
import sys, os, time, requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import pandas as pd

CACHE_DIR = Path(__file__).parent / 'shareholding_cache'
CACHE_DIR.mkdir(exist_ok=True)
DATA_DIR = Path(__file__).parent / 'data_cache'
URL = 'https://api.finmindtrade.com/api/v4/data'


def _load_env():
    env = Path(__file__).parent / '.env'
    if env.exists():
        for line in env.read_text(encoding='utf-8').splitlines():
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


def fetch_one(ticker: str, token: str):
    out = CACHE_DIR / f'{ticker}.parquet'
    if out.exists():
        try: return pd.read_parquet(out)
        except: pass
    params = {
        'dataset': 'TaiwanStockShareholding',
        'data_id': ticker,
        'start_date': '2020-01-01', 'end_date': '2026-04-27',
        'token': token,
    }
    for _ in range(3):
        try:
            r = requests.get(URL, params=params, timeout=30)
            if r.status_code == 200:
                rows = r.json().get('data', [])
                if not rows: return None
                df = pd.DataFrame(rows)
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').sort_index()
                # 留關鍵欄位
                keep = ['ForeignInvestmentSharesRatio',
                        'ForeignInvestmentRemainRatio',
                        'NumberOfSharesIssued']
                df = df[[c for c in keep if c in df.columns]]
                df = df.rename(columns={
                    'ForeignInvestmentSharesRatio': 'foreign_pct',
                    'ForeignInvestmentRemainRatio': 'foreign_remain_pct',
                    'NumberOfSharesIssued': 'total_shares',
                })
                df.to_parquet(out)
                return df
            elif r.status_code == 402:
                time.sleep(60); continue
            else:
                return None
        except Exception:
            time.sleep(2); continue
    return None


_lock = Lock()
_progress = {'ok': 0, 'fail': 0, 'done': 0}


def worker(token, q, total, sleep_s, t0):
    while True:
        try: ticker = q.pop(0)
        except IndexError: break
        df = fetch_one(ticker, token)
        with _lock:
            _progress['done'] += 1
            if df is not None and len(df) > 0: _progress['ok'] += 1
            else: _progress['fail'] += 1
            if _progress['done'] % 50 == 0:
                e = (time.time() - t0) / 60
                print(f"[{_progress['done']:4d}/{total}] ok={_progress['ok']} "
                      f"fail={_progress['fail']}  已 {e:.1f} min", flush=True)
        time.sleep(sleep_s)


def main():
    tokens = collect_tokens()
    if not tokens:
        print("❌ 未設定 FINMIND_TOKEN")
        return
    universe = sorted([p.stem for p in DATA_DIR.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4])
    have = set(p.stem for p in CACHE_DIR.glob('*.parquet'))
    todo = [t for t in universe if t not in have]
    print(f"全市場 {len(universe)} 檔 / 待抓 {len(todo)} / Tokens {len(tokens)}")
    sleep_s = 1.5
    print(f"預估：{len(todo) * sleep_s / len(tokens) / 60:.1f} min\n")
    if not todo:
        print("✅ 全部已快取"); return
    t0 = time.time()
    q = list(todo)
    with ThreadPoolExecutor(max_workers=len(tokens)) as ex:
        futures = [ex.submit(worker, tok, q, len(todo), sleep_s, t0) for tok in tokens]
        for f in as_completed(futures): f.result()
    e = (time.time() - t0) / 60
    print(f"\n總耗時 {e:.1f} min  ok={_progress['ok']}  fail={_progress['fail']}")


if __name__ == '__main__':
    main()
