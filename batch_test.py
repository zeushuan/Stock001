"""
變體平行測試器（v8 優化基礎建設）

同時跑多個策略變體，每個變體用 N 個 process workers。
適用於 12C/16T 機器，可實現 4 變體 × 4 workers = 16 邏輯核心全用滿。

用法：
  python batch_test.py --variants base,T30,W,AA --workers-per 4
  python batch_test.py --variants base,T30,T45,T60 --workers-per 4
  python batch_test.py --variants base,W,T30,AA --workers-per 4 --refresh
  python batch_test.py --compare results_*.csv  # 對比已有結果

設計：
  1. 啟動 N 個 subprocess，每個跑 v8_runner.py --mode XXX
  2. 平行執行（OS 排程到不同核心）
  3. 全部完成後彙總比較
"""
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

import argparse
import csv
import subprocess
import time
from pathlib import Path

import numpy as np


def parallel_run(variants: list, workers_per: int, refresh: bool,
                 tickers_file: str = None):
    """同時啟動多個 v8_runner 進程"""
    print(f"━━━━━━ 平行測試：{len(variants)} 變體 × {workers_per} workers/變體 ━━━━━━")
    print(f"  總計使用核心：{len(variants) * workers_per} 邏輯核心")
    print(f"  變體：{', '.join(variants)}\n")

    procs = []
    t0 = time.time()
    for v in variants:
        cmd = [sys.executable, 'v8_runner.py',
               '--mode', v,
               '--workers', str(workers_per),
               '--quiet',
               '--output', f"results_{v}.csv"]
        if refresh:
            cmd.append('--refresh')
        if tickers_file:
            cmd.extend(['--tickers-file', tickers_file])
        print(f"  [啟動] {v} ...", flush=True)
        # stdout 留給子進程印出（會打散，但能看到進度）
        p = subprocess.Popen(cmd)
        procs.append((v, p))

    print(f"\n  所有進程已啟動，等待完成...\n")
    # 等待全部完成
    for v, p in procs:
        p.wait()
        elapsed = time.time() - t0
        print(f"  [完成] {v}  累計 {elapsed:.1f}s")

    print(f"\n━━━━━━ 平行測試結束，總耗時 {time.time()-t0:.1f}s ━━━━━━\n")


def compare_results(csv_paths: list, ref_mode: str = None):
    """彙總比較多個變體結果"""
    print(f"━━━━━━ 變體結果對比（共 {len(csv_paths)} 檔） ━━━━━━")

    # 讀取每個變體
    by_mode = {}
    for path in csv_paths:
        p = Path(path)
        if not p.exists():
            print(f"  [警告] {path} 不存在")
            continue
        rows = list(csv.DictReader(open(p, encoding='utf-8-sig')))
        if not rows: continue
        mode = rows[0].get('mode', p.stem.replace('results_', ''))
        by_mode[mode] = {r['ticker']: float(r['pnl_pct']) for r in rows}

    if not by_mode:
        print("  無有效資料")
        return

    if ref_mode is None:
        ref_mode = 'base' if 'base' in by_mode else next(iter(by_mode))

    # 統計
    print(f"\n  {'模式':<10} {'樣本':>5} {'均值%':>10} {'與':>6} {ref_mode:<5} {'最高%':>10} {'最低%':>10}")
    print(f"  {'-'*70}")

    rows_summary = []
    ref_set = set(by_mode[ref_mode].keys()) if ref_mode in by_mode else set()
    for mode, data in by_mode.items():
        common = set(data.keys()) & ref_set if ref_set else set(data.keys())
        if not common:
            continue
        vals = [data[t] for t in common if data[t] == data[t]]
        if not vals: continue
        mean_v = np.mean(vals)
        if ref_mode in by_mode and mode != ref_mode:
            ref_vals = [by_mode[ref_mode][t] for t in common if by_mode[ref_mode][t] == by_mode[ref_mode][t]]
            diff = mean_v - np.mean(ref_vals) if ref_vals else 0
            diff_s = f"{diff:+.2f}"
        else:
            diff_s = '  --'
        print(f"  {mode:<10} {len(vals):>5} {mean_v:>+9.2f}% {diff_s:>10}   {max(vals):>+9.1f}% {min(vals):>+9.1f}%")
        rows_summary.append((mode, mean_v, len(vals)))

    # 找出最佳
    if rows_summary:
        best = max(rows_summary, key=lambda x: x[1])
        print(f"\n  ◆ 最佳變體：{best[0]}（均值 {best[1]:+.2f}%）")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--variants', default='base',
                    help='逗號分隔的變體清單，例：base,T30,W,AA')
    ap.add_argument('--workers-per', type=int, default=4,
                    help='每個變體的 worker 數（建議 4，4變體×4=16核心）')
    ap.add_argument('--refresh', action='store_true', help='強制重新下載資料')
    ap.add_argument('--compare-only', action='store_true',
                    help='不執行，僅對比已存在的 results_*.csv')
    ap.add_argument('--tickers-file', default=None,
                    help='傳遞給 v8_runner 的自訂股票清單檔')
    args = ap.parse_args()

    variants = [v.strip() for v in args.variants.split(',') if v.strip()]
    if not variants:
        print("請指定 --variants")
        sys.exit(1)

    if not args.compare_only:
        parallel_run(variants, args.workers_per, args.refresh,
                     tickers_file=args.tickers_file)

    # 自動對比
    csv_paths = [f"results_{v}.csv" for v in variants]
    csv_paths = [p for p in csv_paths if Path(p).exists()]
    if csv_paths:
        compare_results(csv_paths, ref_mode=variants[0])
    else:
        print("[警告] 找不到任何 results_*.csv")


if __name__ == '__main__':
    main()
