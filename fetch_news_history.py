"""抓 FinMind TOP 200 歷史新聞（用於 NLP 回測）
============================================
TaiwanStockNews 限制：每次只能取單日資料。
策略：TOP 200 × 180 天 = 36000 calls，雙 token 1.5s/call ≈ 7.5 小時

每檔輸出：news_cache/{ticker}.parquet
欄位：date / title / sentiment_score
"""
import sys, os, time, requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime, timedelta
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import pandas as pd
import json

CACHE_DIR = Path(__file__).parent / 'news_cache'
CACHE_DIR.mkdir(exist_ok=True)
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


def _score_title(title: str) -> float:
    """快速情感評分（與 tv_app 同邏輯）"""
    if not title: return 0.0
    POS = {
        '大漲':0.30,'創新高':0.30,'飆漲':0.30,'突破':0.20,'上漲':0.15,
        '看好':0.20,'看多':0.20,'加碼':0.15,'增持':0.15,'推薦':0.15,
        '利多':0.25,'利好':0.25,'訂單滿載':0.30,'需求強勁':0.25,
        '業績亮眼':0.30,'業績超標':0.30,'財報優':0.25,'獲利成長':0.20,
        '營收成長':0.20,'受惠':0.15,'搶單':0.20,'報喜':0.20,
        '強勁':0.15,'強漲':0.20,'盈餘上修':0.25,'上修':0.20,
    }
    NEG = {
        '大跌':-0.30,'崩跌':-0.35,'重挫':-0.30,'暴跌':-0.30,'下跌':-0.15,
        '看壞':-0.20,'看空':-0.20,'減碼':-0.15,'降評':-0.15,'不推':-0.15,
        '利空':-0.25,'警訊':-0.20,'警告':-0.20,'衰退':-0.25,'虧損':-0.25,
        '財報差':-0.25,'獲利衰退':-0.25,'營收下滑':-0.25,'裁員':-0.20,
        '違約':-0.30,'掏空':-0.30,'破產':-0.35,'下修':-0.20,'失利':-0.20,
        '失守':-0.20,'跌破':-0.20,'恐慌':-0.25,'崩盤':-0.35,'空頭':-0.20,
    }
    try:
        from snownlp import SnowNLP
        score = (float(SnowNLP(title).sentiments) - 0.5) * 0.6
    except Exception:
        score = 0.0
    for k, w in POS.items():
        if k in title: score += w
    for k, w in NEG.items():
        if k in title: score += w
    return max(-1.0, min(1.0, score))


def fetch_one_day(ticker: str, date: str, token: str):
    """抓某 ticker 某日新聞（單一 API call）"""
    params = {
        'dataset': 'TaiwanStockNews',
        'data_id': ticker,
        'start_date': date,
        'token': token,
    }
    for retry in range(3):
        try:
            r = requests.get(URL, params=params, timeout=20)
            if r.status_code == 200:
                return r.json().get('data', [])
            elif r.status_code == 402:
                time.sleep(60); continue
            else:
                return []
        except Exception:
            time.sleep(2); continue
    return []


_lock = Lock()
_progress = {'done': 0, 'news_total': 0}


def worker(token: str, task_q, total: int, sleep_s: float, t0: float, output_dict):
    while True:
        try:
            ticker, dates = task_q.pop(0)
        except IndexError:
            break
        all_news = []
        for d in dates:
            news = fetch_one_day(ticker, d, token)
            for n in news:
                title = n.get('title', '')
                all_news.append({
                    'date': n.get('date', d),
                    'title': title,
                    'sentiment': _score_title(title),
                })
            time.sleep(sleep_s)

        # 寫檔
        if all_news:
            df = pd.DataFrame(all_news)
            df['date'] = pd.to_datetime(df['date'])
            out = CACHE_DIR / f'{ticker}.parquet'
            df.to_parquet(out)

        with _lock:
            _progress['done'] += 1
            _progress['news_total'] += len(all_news)
            done = _progress['done']
            total_news = _progress['news_total']

        elapsed = (time.time() - t0) / 60
        if done % 10 == 0:
            print(f"[{done:3d}/{total}] news={total_news}  已 {elapsed:.1f} min", flush=True)


def main():
    tokens = collect_tokens()
    if not tokens:
        print("❌ 未設定 FINMIND_TOKEN")
        return

    # 🆕 全市場（從 data_cache 讀，4 位數）
    DATA_DIR = Path(__file__).parent / 'data_cache'
    universe = sorted([p.stem for p in DATA_DIR.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4])

    # 過濾掉已快取
    have = set(p.stem for p in CACHE_DIR.glob('*.parquet'))
    todo = [t for t in universe if t not in have]

    # 日期範圍：近 60 個交易日（約 3 個月，先做快速 MVP）
    end_date = datetime.now()
    dates = []
    cur = end_date
    while len(dates) < 60:
        if cur.weekday() < 5:
            dates.append(cur.strftime('%Y-%m-%d'))
        cur -= timedelta(days=1)
    dates.reverse()

    print(f"全市場: {len(universe)} 檔 / 待抓 {len(todo)}")
    print(f"日期範圍: {dates[0]} ~ {dates[-1]} ({len(dates)} 交易日)")
    print(f"Tokens: {len(tokens)}（每 thread 1.5s/call）")
    print(f"預估 calls: {len(todo) * len(dates)} / 預估時間: "
          f"{len(todo) * len(dates) * 1.5 / len(tokens) / 3600:.1f} hr\n")

    if not todo:
        print("✅ 全部已快取")
        return

    # 任務 = (ticker, dates)
    tasks = [(t, dates) for t in todo]

    t0 = time.time()
    sleep_s = 1.5
    output = {}
    with ThreadPoolExecutor(max_workers=len(tokens)) as ex:
        futures = [ex.submit(worker, tok, tasks, len(todo), sleep_s, t0, output)
                   for tok in tokens]
        for f in as_completed(futures):
            f.result()

    elapsed = (time.time() - t0) / 60
    print(f"\n總耗時 {elapsed:.1f} min  完成 {_progress['done']}/{len(todo)}  "
          f"新聞 {_progress['news_total']} 筆")


if __name__ == '__main__':
    main()
