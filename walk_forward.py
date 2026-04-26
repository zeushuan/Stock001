"""
E1 + E4：Walk-forward 驗證 + 交易成本影響

目的：
  確認 v8 P0_T1T3 不是因 2020-2026 特定行情而過擬合

設計：
  Window A：2020-01-02 ~ 2022-12-31（含 COVID 反彈、2022 修正，3年）
  Window B：2023-01-01 ~ 2026-04-25（AI 大行情，3.3年）
  Window F：2020-01-02 ~ 2026-04-25（完整期，作為比較基準）

  比較 base / P5_T3 / P0_T1T3 在三個窗口的表現

評估指標：
  pnl_pct           毛報酬均值
  pnl_pct_net       扣 0.4275% 雙邊成本後淨報酬均值
  >+5% 改善比例     vs base 改善的股票比例
  最高 / 最低       尾部分布

判定：
  若 P0_T1T3 在 Window A 與 B 表現一致 → 真有 Edge，可放心使用
  若 A 強 B 弱 → 可能僅吃 2022 之前行情；2024+ 不一定可靠
  若 A 弱 B 強 → 可能僅吃 AI 行情；歷史時期不一定可靠
"""
import sys, io
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception: pass

import argparse
import csv
import json
import subprocess
import time
from pathlib import Path

import numpy as np


# 三個測試窗口
WINDOWS = [
    ("FULL",   "2020-01-02", "2026-04-25"),  # 完整基準
    ("EARLY",  "2020-01-02", "2022-12-31"),  # 訓練期（COVID + 2022 修正）
    ("LATE",   "2023-01-01", "2026-04-25"),  # 測試期（AI 行情）
]

# 要比較的策略
MODES = ['base', 'P5_T3', 'P0_T1T3']

TX_COST = 0.4275   # 台股實際雙邊成本 %


def run_one(mode: str, win_label: str, start: str, end: str,
            tx_cost: float = TX_COST, workers: int = 12) -> str:
    """跑一個 mode × window 組合，回傳 CSV 路徑"""
    output = f"wf_{mode}_{win_label}.csv"
    cmd = [sys.executable, 'v8_runner.py',
           '--mode', mode,
           '--workers', str(workers),
           '--quiet',
           '--start', start,
           '--end', end,
           '--tx-cost', str(tx_cost),
           '--output', output]
    print(f"  [{win_label}/{mode}] start...", flush=True)
    t0 = time.time()
    subprocess.run(cmd, check=True)
    print(f"  [{win_label}/{mode}] done {time.time()-t0:.1f}s", flush=True)
    return output


def load_csv(path: str) -> dict:
    d = {}
    for r in csv.DictReader(open(path, encoding='utf-8-sig')):
        try:
            d[r['ticker']] = (
                float(r['pnl_pct']),
                float(r.get('pnl_pct_net') or r['pnl_pct']),
                float(r['bh_pct'] or 0),
                int(r['n_trades']),
            )
        except: pass
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=12)
    ap.add_argument('--parallel-modes', action='store_true',
                    help='不同 mode 用 subprocess 平行（4 並行）；預設 sequential 用滿 workers')
    args = ap.parse_args()

    print("━━━━━━━━━━ Walk-Forward 驗證 + 交易成本測試 ━━━━━━━━━━\n")
    print(f"窗口：")
    for label, s, e in WINDOWS:
        print(f"  {label:<8} {s} ~ {e}")
    print(f"\n策略：{', '.join(MODES)}")
    print(f"交易成本：每筆雙邊 {TX_COST}%（台股實際）\n")

    t_total = time.time()

    # 序列執行所有 mode × window，每個都用 12 workers
    csv_paths = {}
    for mode in MODES:
        for label, start, end in WINDOWS:
            path = run_one(mode, label, start, end, workers=args.workers)
            csv_paths[(mode, label)] = path

    print(f"\n總執行時間：{time.time()-t_total:.1f}s\n")

    # ─── 彙總比較 ─────────────────────────────────────────────
    print("━━━━━━━━━━ 結果矩陣（毛報酬均值 / 淨報酬均值）━━━━━━━━━━")
    print(f"{'策略':<12}", end='')
    for label, _, _ in WINDOWS:
        print(f" {label:>10}", end='')
    print()
    print("-" * 50)

    # 毛報酬
    print(f"{'毛報酬%':<12}")
    for mode in MODES:
        print(f"  {mode:<10}", end='')
        for label, _, _ in WINDOWS:
            d = load_csv(csv_paths[(mode, label)])
            vals = [v[0] for v in d.values()]
            print(f" {np.mean(vals):>+9.2f}", end='')
        print()
    print()

    # 淨報酬
    print(f"{'淨(扣成本)%':<12}")
    for mode in MODES:
        print(f"  {mode:<10}", end='')
        for label, _, _ in WINDOWS:
            d = load_csv(csv_paths[(mode, label)])
            vals = [v[1] for v in d.values()]
            print(f" {np.mean(vals):>+9.2f}", end='')
        print()
    print()

    # 樣本數
    print(f"{'樣本數':<12}")
    for mode in MODES:
        print(f"  {mode:<10}", end='')
        for label, _, _ in WINDOWS:
            d = load_csv(csv_paths[(mode, label)])
            print(f" {len(d):>10}", end='')
        print()
    print()

    # vs base 改善統計（同窗口比較）
    print("━━━━━━━━━━ 各窗口內 P0_T1T3 vs base 統計 ━━━━━━━━━━")
    for label, _, _ in WINDOWS:
        base_d = load_csv(csv_paths[('base', label)])
        p0_d   = load_csv(csv_paths[('P0_T1T3', label)])
        diffs = []
        for tk in base_d.keys() & p0_d.keys():
            diffs.append(p0_d[tk][0] - base_d[tk][0])
        if diffs:
            arr = np.array(diffs)
            print(f"\n  {label}（共 {len(arr)} 檔）")
            print(f"    平均改善：     {np.mean(arr):+.2f}%")
            print(f"    改善 >+5%：    {np.sum(arr > 5)} 檔 ({np.sum(arr>5)/len(arr)*100:.0f}%)")
            print(f"    退步 <-5%：    {np.sum(arr < -5)} 檔 ({np.sum(arr<-5)/len(arr)*100:.0f}%)")
            print(f"    最大改善：     {np.max(arr):+.0f}%")
            print(f"    最大退步：     {np.min(arr):+.0f}%")

    # 健壯性判定
    print("\n━━━━━━━━━━ 健壯性判定（P0_T1T3）━━━━━━━━━━")
    p0_full  = np.mean([v[0] for v in load_csv(csv_paths[('P0_T1T3','FULL')]).values()])
    p0_early = np.mean([v[0] for v in load_csv(csv_paths[('P0_T1T3','EARLY')]).values()])
    p0_late  = np.mean([v[0] for v in load_csv(csv_paths[('P0_T1T3','LATE')]).values()])
    base_full  = np.mean([v[0] for v in load_csv(csv_paths[('base','FULL')]).values()])
    base_early = np.mean([v[0] for v in load_csv(csv_paths[('base','EARLY')]).values()])
    base_late  = np.mean([v[0] for v in load_csv(csv_paths[('base','LATE')]).values()])

    print(f"  P0_T1T3：FULL={p0_full:+.1f}%  EARLY={p0_early:+.1f}%  LATE={p0_late:+.1f}%")
    print(f"  base：   FULL={base_full:+.1f}%  EARLY={base_early:+.1f}%  LATE={base_late:+.1f}%")
    print()
    print(f"  P0 vs base：FULL +{p0_full-base_full:.1f}  EARLY +{p0_early-base_early:.1f}  LATE +{p0_late-base_late:.1f}")
    print()

    # 判定文字
    edge_early = p0_early - base_early
    edge_late  = p0_late  - base_late
    if edge_early > 30 and edge_late > 30:
        verdict = "✅ 兩個窗口 P0 vs base 都顯著正向，策略 Edge 穩定，非過擬合"
    elif edge_early > 30 > edge_late:
        verdict = "⚠️ EARLY 強 LATE 弱，可能依賴 2020-2022 行情，AI 期間優勢消失"
    elif edge_late > 30 > edge_early:
        verdict = "⚠️ LATE 強 EARLY 弱，可能僅 AI 行情有效，歷史驗證不足"
    elif edge_early < 10 and edge_late < 10:
        verdict = "❌ 兩窗口 Edge 都微弱，可能是雜訊或過擬合，需重新審視"
    else:
        verdict = "🟡 邊際情況，需深入分析"
    print(f"  → {verdict}")


if __name__ == '__main__':
    main()
