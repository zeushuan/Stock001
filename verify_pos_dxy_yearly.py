"""驗證 POS+DXY 跨年度穩定性（風報比 0.99 是否持續）"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import subprocess, csv, time
import numpy as np

WINDOWS = [
    ("2020",   "2020-01-02", "2020-12-31"),
    ("2021",   "2021-01-01", "2021-12-31"),
    ("2022",   "2022-01-01", "2022-12-31"),
    ("2023",   "2023-01-01", "2023-12-31"),
    ("2024",   "2024-01-01", "2024-12-31"),
    ("2025+",  "2025-01-01", "2026-04-25"),
]

MODES = ['base', 'P0_T1T3+POS', 'P0_T1T3+POS+DXY', 'P0_T1T3+POS+VIX30+DXY']

def run(mode, label, s, e):
    out = f"yr2_{mode.replace('+', '_')}_{label}.csv"
    subprocess.run([sys.executable, 'v8_runner.py', '--mode', mode,
                    '--workers', '12', '--quiet',
                    '--start', s, '--end', e, '--output', out], check=True)
    return out

def load_stats(path):
    vals = []
    for r in csv.DictReader(open(path, encoding='utf-8-sig')):
        try: vals.append(float(r['pnl_pct']))
        except: pass
    if not vals: return None
    return dict(mean=np.mean(vals), max=max(vals), min=min(vals))

print("━━━━━━ POS+DXY 跨年度驗證 ━━━━━━\n")
results = {}
t0 = time.time()
for mode in MODES:
    for label, s, e in WINDOWS:
        print(f"  [{label}/{mode}]...", flush=True)
        path = run(mode, label, s, e)
        stat = load_stats(path)
        results[(mode, label)] = stat
        print(f"  [{label}/{mode}] mean={stat['mean']:+.2f}  min={stat['min']:+.0f}",
              flush=True)

print(f"\n總時間 {time.time()-t0:.1f}s\n")

# ── 矩陣 ──
print("━━━ 各策略年度均值 ━━━")
print(f"{'策略':<25}", end='')
for label, _, _ in WINDOWS:
    print(f" {label:>9}", end='')
print(f"  {'σ':>5}  {'平均Edge':>9}")
print('-' * 95)
base_yr = [results[('base', w[0])]['mean'] for w in WINDOWS]
for mode in MODES:
    yr = [results[(mode, w[0])]['mean'] for w in WINDOWS]
    print(f"{mode:<25}", end='')
    for v in yr:
        print(f" {v:>+8.2f}", end='')
    if mode == 'base':
        print(f"  {np.std(yr):>5.2f}")
    else:
        edges = [yr[i] - base_yr[i] for i in range(len(yr))]
        print(f"  {np.std(edges):>5.2f}  {np.mean(edges):>+8.2f}")

# ── 各策略 vs base 跨年度 σ ──
print("\n━━━ Edge 跨年度標準差（越小越穩定）━━━")
for mode in MODES[1:]:
    edges = [results[(mode, w[0])]['mean'] - results[('base', w[0])]['mean']
             for w in WINDOWS]
    print(f"{mode:<25} 平均 Edge {np.mean(edges):>+7.2f}  σ={np.std(edges):>5.2f}"
          f"  範圍 [{min(edges):+.1f}, {max(edges):+.1f}]")
