"""
F3 即時掃描器：每日跑當天信號清單

依據 v8 P0_T1T3+CB30 策略邏輯，產生「今日有信號」的股票清單。

輸出：
  daily_signals_{date}.csv     當日所有信號（T1/T3/出場/加碼）
  附帶當下 RSI/ADX/EMA 狀態與建議動作

用法：
  python daily_scanner.py                    # 掃描全市場
  python daily_scanner.py --refresh          # 強制重新下載
  python daily_scanner.py --tickers 2330,2317 # 指定股票
  python daily_scanner.py --top 30           # 只顯示信號最強的 N 檔
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np

import data_loader as dl
import variant_strategy as vs


def scan_one(ticker: str, file_path: str, mode: str = 'P0_T1T3+CB30'):
    """對單支股票檢查當日信號狀態"""
    import pandas as pd
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        return None

    # 套日期過濾，留到最新
    df_f = vs._filter_period(df)
    if df_f is None or df_f.empty or len(df_f) < 60:
        return None

    flags = vs._decode_mode(mode)
    last_idx = len(df_f) - 1
    last_date = df_f.index[last_idx]

    # 提取最後一日指標狀態
    pr = df_f['Close'].values
    e20 = df_f['e20'].values
    e60 = df_f['e60'].values
    e120 = df_f['e120'].values
    rsi = df_f['rsi'].values
    adx = df_f['adx'].values

    cur_pr  = float(pr[last_idx])
    cur_e20 = float(e20[last_idx])  if not np.isnan(e20[last_idx])  else None
    cur_e60 = float(e60[last_idx])  if not np.isnan(e60[last_idx])  else None
    cur_e120= float(e120[last_idx]) if not np.isnan(e120[last_idx]) else None
    cur_rsi = float(rsi[last_idx])  if not np.isnan(rsi[last_idx])  else None
    cur_adx = float(adx[last_idx])  if not np.isnan(adx[last_idx])  else None

    # 趨勢狀態
    is_bull = (cur_e20 is not None and cur_e60 is not None and cur_e20 > cur_e60)
    adx_thresh = flags.get('adx_th') or 22
    adx_ok  = (cur_adx is not None and cur_adx >= adx_thresh)

    # 黃金交叉檢測（前一日 vs 今日）
    if last_idx >= 1:
        is_t1 = (not any(np.isnan([e20[last_idx-1], e60[last_idx-1], e20[last_idx], e60[last_idx]]))
                 and e20[last_idx-1] <= e60[last_idx-1] and e20[last_idx] > e60[last_idx])
    else:
        is_t1 = False

    # T3 條件檢測
    is_t3 = False
    if is_bull and adx_ok and last_idx >= 60:
        if not any(np.isnan([e120[last_idx], e120[last_idx-60]])) and e120[last_idx-60] != 0:
            e120_pct = (e120[last_idx] - e120[last_idx-60]) / abs(e120[last_idx-60]) * 100
            if e120_pct >= -2.0 and cur_rsi is not None and cur_rsi < 50:
                is_t3 = True

    # 黃金交叉天數
    cross_days = None
    for k in range(1, min(last_idx, 60)):
        if last_idx - k - 1 >= 0:
            prev = e20[last_idx-k-1]
            cur = e20[last_idx-k]
            ref_prev = e60[last_idx-k-1]
            ref_cur = e60[last_idx-k]
            if not any(np.isnan([prev, cur, ref_prev, ref_cur])):
                if prev <= ref_prev and cur > ref_cur:
                    cross_days = k
                    break

    # 信號分類
    if is_t1:
        signal = 'T1 黃金交叉'
        score = 90 + (cur_adx if cur_adx else 0) / 5
    elif is_t3:
        signal = 'T3 多頭拉回'
        score = 70 + (50 - cur_rsi if cur_rsi else 0)
    elif is_bull and adx_ok:
        if cur_rsi is not None and cur_rsi >= 75:
            signal = '⚠️ 過熱（RSI≥75）'
            score = 30
        elif cur_rsi is not None and cur_rsi >= 65:
            signal = '🟡 多頭中段'
            score = 40
        else:
            signal = '🟢 多頭觀察'
            score = 50
    elif is_bull:
        signal = '⚠️ 假多頭（ADX 弱）'
        score = 20
    else:
        signal = '🔴 空頭'
        score = 10

    return dict(
        ticker=ticker,
        date=last_date.strftime('%Y-%m-%d'),
        price=cur_pr,
        signal=signal,
        score=score,
        rsi=cur_rsi,
        adx=cur_adx,
        is_bull=is_bull,
        cross_days=cross_days,
        e20=cur_e20,
        e60=cur_e60,
        e120=cur_e120,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=12)
    ap.add_argument('--refresh', action='store_true')
    ap.add_argument('--tickers', default=None, help='逗號分隔股票代號清單')
    ap.add_argument('--top', type=int, default=50, help='只顯示信號最強 N 檔')
    ap.add_argument('--output', default=None)
    args = ap.parse_args()

    # 載入清單
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',') if t.strip()]
    else:
        from v8_runner import load_tickers
        tickers = load_tickers()

    print(f"━━━ 即時掃描器 ━━━")
    print(f"股票數：{len(tickers)}")
    print(f"workers：{args.workers}")
    print()

    t0 = time.time()
    print("[1/2] 載入快取資料...")
    data_map = dl.batch_get_all(tickers, force_refresh=args.refresh, verbose=True)
    t_load = time.time() - t0
    print(f"      載入完成：{t_load:.1f}s\n")

    print(f"[2/2] 平行掃描 {len(data_map)} 檔...")
    t1 = time.time()
    results = []
    tasks = [(tk, str(dl.cache_path(tk))) for tk in data_map.keys()]

    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(scan_one, tk, fp): tk for tk, fp in tasks}
        for fut in as_completed(futures):
            try:
                r = fut.result(timeout=10)
                if r is not None:
                    results.append(r)
            except Exception:
                pass
    t_scan = time.time() - t1
    print(f"      掃描完成：{t_scan:.1f}s\n")

    # 排序：依 score 倒序，優先看 T1/T3
    results.sort(key=lambda x: -x['score'])

    # 輸出 CSV
    output = args.output or f"daily_signals_{datetime.now().strftime('%Y%m%d')}.csv"
    fields = ['ticker', 'date', 'price', 'signal', 'score',
              'rsi', 'adx', 'is_bull', 'cross_days', 'e20', 'e60', 'e120']
    with open(output, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        for r in results:
            w.writerow(r)

    # 印出 TOP N
    print(f"━━━ TOP {args.top} 信號股 ━━━")
    print(f"{'代號':<7} {'日期':<11} {'收盤':>8} {'信號':<18} {'score':>6} {'RSI':>5} {'ADX':>5}")
    print('-' * 75)
    for r in results[:args.top]:
        rsi_s = f"{r['rsi']:.1f}" if r['rsi'] else 'N/A'
        adx_s = f"{r['adx']:.1f}" if r['adx'] else 'N/A'
        print(f"  {r['ticker']:<6} {r['date']:<11} {r['price']:>7.1f}  "
              f"{r['signal']:<16} {r['score']:>5.0f} {rsi_s:>5} {adx_s:>5}")

    print(f"\n結果已儲存：{output}")
    print(f"總時間：{time.time()-t0:.1f}s")

    # 統計
    counts = {}
    for r in results:
        sig = r['signal'].split(' ')[0] if ' ' in r['signal'] else r['signal'][:6]
        counts[sig] = counts.get(sig, 0) + 1
    print(f"\n━━━ 信號分佈 ━━━")
    for sig, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {sig:<10} {n} 檔  ({n/len(results)*100:.1f}%)")


if __name__ == '__main__':
    main()
