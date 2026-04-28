"""抓全市場每日 PER / PBR / 殖利率（FinMind 公開 API）
==========================================================
資料來源：https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPER
無需註冊，但限速保守處理（每秒 1 req）。

每檔輸出：per_cache/{ticker}.parquet
欄位：date, PER, PBR, dividend_yield
"""
import sys, time, requests
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import pandas as pd

CACHE_DIR = Path(__file__).parent / 'per_cache'
CACHE_DIR.mkdir(exist_ok=True)
DATA_DIR = Path(__file__).parent / 'data_cache'

URL = 'https://api.finmindtrade.com/api/v4/data'


def fetch_one(ticker: str, start='2020-01-01', end='2026-04-27',
              force=False) -> pd.DataFrame:
    out = CACHE_DIR / f'{ticker}.parquet'
    if out.exists() and not force:
        try: return pd.read_parquet(out)
        except: pass

    params = {
        'dataset': 'TaiwanStockPER',
        'data_id': ticker,
        'start_date': start,
        'end_date': end,
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
                print(f'  [{ticker}] 限速等 60s', flush=True)
                time.sleep(60)
                continue
            else:
                return None
        except Exception as e:
            time.sleep(3)
            continue
    return None


def main():
    # 全市場：data_cache 中所有 4 位數股票
    universe = sorted(p.stem for p in DATA_DIR.glob('*.parquet')
                      if p.stem and p.stem[0].isdigit() and len(p.stem) == 4)
    print(f"全市場：{len(universe)} 檔")

    have = set(p.stem for p in CACHE_DIR.glob('*.parquet'))
    todo = [t for t in universe if t not in have]
    print(f"已快取：{len(universe) - len(todo)}")
    print(f"待抓：{len(todo)}")
    print(f"預估：{len(todo) * 1.0 / 60:.1f} min\n")

    t0 = time.time()
    ok = 0; fail = 0
    for i, t in enumerate(todo, 1):
        df = fetch_one(t)
        if df is not None and len(df) > 0:
            ok += 1
        else:
            fail += 1
        if i % 50 == 0:
            elapsed = (time.time() - t0) / 60
            print(f"[{i:4d}/{len(todo)}] ok={ok} fail={fail}  "
                  f"已 {elapsed:.1f} min", flush=True)
        time.sleep(0.7)  # 限速保護

    print(f"\n總耗時 {(time.time()-t0)/60:.1f} min  成功 {ok}/{len(todo)}")


if __name__ == '__main__':
    main()
