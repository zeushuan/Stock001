"""抓全市場每日 PER / PBR / 殖利率（FinMind API，多 token 平行）
==================================================================
資料來源：https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPER

Token 設定（.env）：
  FINMIND_TOKEN=主token
  FINMIND_TOKEN2=備援1（自動偵測）
  FINMIND_TOKEN3=備援2（自動偵測）
  ...

每 token 一條 thread 平行抓取。
"""
import sys, os, time, requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import pandas as pd

CACHE_DIR = Path(__file__).parent / 'per_cache'
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
    """從環境變數收集所有 FINMIND_TOKEN / FINMIND_TOKEN2 / FINMIND_TOKEN3 ..."""
    tokens = []
    primary = os.environ.get('FINMIND_TOKEN', '').strip()
    if primary: tokens.append(primary)
    for i in range(2, 11):
        t = os.environ.get(f'FINMIND_TOKEN{i}', '').strip()
        if t: tokens.append(t)
    return tokens


def fetch_one(ticker: str, token: str, sleep_s: float,
              start='2020-01-01', end='2026-04-27') -> pd.DataFrame:
    """單一 ticker fetch（用指定 token）"""
    out = CACHE_DIR / f'{ticker}.parquet'
    if out.exists():
        try: return pd.read_parquet(out)
        except: pass

    params = {
        'dataset': 'TaiwanStockPER',
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
                df = df[['PER', 'PBR', 'dividend_yield']]
                df.to_parquet(out)
                return df
            elif r.status_code == 402:
                time.sleep(60)
                continue
            else:
                return None
        except Exception:
            time.sleep(3)
            continue
    return None


# 全域進度計數器
_lock = Lock()
_progress = {'ok': 0, 'fail': 0, 'done': 0}


def worker_thread(token: str, ticker_q, total: int, sleep_s: float, t0: float):
    """每個 thread 持有一個 token，從 queue 拿 ticker 處理"""
    while True:
        try:
            ticker = ticker_q.pop(0)
        except IndexError:
            break
        df = fetch_one(ticker, token, sleep_s)
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
        print("❌ 未設定任何 FINMIND_TOKEN，請編輯 .env")
        return

    universe = sorted(p.stem for p in DATA_DIR.glob('*.parquet')
                      if p.stem and p.stem[0].isdigit() and len(p.stem) == 4)
    have = set(p.stem for p in CACHE_DIR.glob('*.parquet'))
    todo = [t for t in universe if t not in have]

    print(f"全市場：{len(universe)} 檔")
    print(f"已快取：{len(universe) - len(todo)}")
    print(f"待抓：{len(todo)}")
    print(f"Token 數：{len(tokens)}（每條 thread 一個 token）")

    sleep_s = 1.5
    n_threads = len(tokens)
    rate = n_threads / sleep_s * 60  # req/min
    print(f"每 thread 1.5s/req  → 整體 ~{rate:.0f} req/min")
    print(f"預估：{len(todo) / max(rate, 1):.1f} min")
    print()

    if not todo:
        print("✅ 全部已快取")
        return

    t0 = time.time()
    # 用簡單的共享 list 當 queue（thread-safe via .pop with try/except）
    tq = list(todo)
    threads = []
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [ex.submit(worker_thread, tok, tq, len(todo), sleep_s, t0)
                   for tok in tokens]
        for f in as_completed(futures):
            f.result()

    elapsed = (time.time() - t0) / 60
    print(f"\n總耗時 {elapsed:.1f} min  "
          f"成功 {_progress['ok']}/{len(todo)}  失敗 {_progress['fail']}")


if __name__ == '__main__':
    main()
