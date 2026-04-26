"""
時序分析：策略在不同年度的表現演變

切分：
  2020 (COVID 暴跌+反彈)
  2021 (流動性牛市)
  2022 (升息熊市)
  2023 (AI 啟動)
  2024 (AI 主升段)
  2025+(成熟期)

驗證：
  - P0 是否在某個特定年度才特別有效？
  - POS 是否在牛熊都健壯？
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import subprocess
import time
from pathlib import Path

import numpy as np


WINDOWS = [
    ("2020",   "2020-01-02", "2020-12-31"),
    ("2021",   "2021-01-01", "2021-12-31"),
    ("2022",   "2022-01-01", "2022-12-31"),
    ("2023",   "2023-01-01", "2023-12-31"),
    ("2024",   "2024-01-01", "2024-12-31"),
    ("2025+",  "2025-01-01", "2026-04-25"),
]

MODES = ['base', 'P0_T1T3', 'P0_T1T3+CB30', 'P0_T1T3+POS']


def run_one(mode, win_label, start, end):
    output = f"yr_{mode.replace('+', '_')}_{win_label}.csv"
    cmd = [sys.executable, 'v8_runner.py',
           '--mode', mode, '--workers', '12', '--quiet',
           '--start', start, '--end', end,
           '--output', output]
    subprocess.run(cmd, check=True)
    return output


def load_mean(path):
    vals = []
    for r in csv.DictReader(open(path, encoding='utf-8-sig')):
        try: vals.append(float(r['pnl_pct']))
        except: pass
    return float(np.mean(vals)) if vals else float('nan')


def main():
    print("━━━━━━ 時序分析：6 年逐年表現 ━━━━━━\n")
    print(f"窗口：{[w[0] for w in WINDOWS]}")
    print(f"策略：{', '.join(MODES)}\n")

    t_total = time.time()
    results = {}
    for mode in MODES:
        for label, start, end in WINDOWS:
            print(f"  [{label}/{mode}]...", flush=True)
            t0 = time.time()
            path = run_one(mode, label, start, end)
            mean = load_mean(path)
            results[(mode, label)] = mean
            print(f"  [{label}/{mode}] {mean:+.2f}%  ({time.time()-t0:.0f}s)", flush=True)

    print(f"\n總耗時：{time.time()-t_total:.1f}s\n")

    # ─── 矩陣展示 ───────────────────────────────────────
    print("━━━ 年度均值矩陣 ━━━")
    print(f"  {'策略':<22}", end='')
    for label, _, _ in WINDOWS:
        print(f" {label:>9}", end='')
    print()
    print('  ' + '-' * 80)
    for mode in MODES:
        print(f"  {mode:<22}", end='')
        for label, _, _ in WINDOWS:
            print(f" {results[(mode, label)]:>+8.2f}", end='')
        print()

    # ─── 比較 P0 vs base 在各年度的 Edge ──────────────
    print("\n━━━ 各策略 vs base 的年度 Edge ━━━")
    print(f"  {'策略':<22}", end='')
    for label, _, _ in WINDOWS:
        print(f" {label:>9}", end='')
    print()
    print('  ' + '-' * 80)
    for mode in MODES[1:]:  # 跳過 base
        print(f"  {mode:<22}", end='')
        for label, _, _ in WINDOWS:
            edge = results[(mode, label)] - results[('base', label)]
            print(f" {edge:>+8.2f}", end='')
        print()

    # ─── 健壯性判定 ─────────────────────────────────
    print("\n━━━ 各策略年度 Edge 標準差（越小越穩定）━━━")
    base_yearly = [results[('base', w[0])] for w in WINDOWS]
    for mode in MODES[1:]:
        edges = [results[(mode, w[0])] - results[('base', w[0])] for w in WINDOWS]
        sd = np.std(edges)
        avg = np.mean(edges)
        print(f"  {mode:<22} 平均 Edge {avg:>+7.2f}  σ={sd:>5.2f}"
              f"  範圍 [{min(edges):+.1f}, {max(edges):+.1f}]")

    # ─── BH 各年度均值（市況參考）────────────────────
    print("\n━━━ 各年度市況參考（base 均值代表市場行情）━━━")
    for label, start, end in WINDOWS:
        v = results[('base', label)]
        if v > 30:    market = "🟢 多頭"
        elif v > 5:   market = "🟢 微多"
        elif v > -10: market = "🟡 盤整"
        elif v > -30: market = "🔴 修正"
        else:         market = "🔴 空頭"
        print(f"  {label}: base 均值 {v:+.1f}%  → {market}")


if __name__ == '__main__':
    main()
