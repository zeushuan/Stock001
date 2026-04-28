"""EPS / P/E 估值過濾變體驗證
================================================
測試多種估值過濾組合，疊加在 VWAPEXEC 之上：

baseline      : POS+IND+DXY
B VWAPEXEC    : 已知贏家
C +PEPOS      : 過濾虧損公司（PER > 0 且 < 200）
D +PEMAX30    : PER ≤ 30（合理估值）
E +PEMAX50    : PER ≤ 50（更寬鬆）
F +PEMIN10    : PER ≥ 10（避免估值過低 = 警訊）
G +PEMID      : 10 < PER < 30（甜蜜點）
H +DIV3       : 殖利率 ≥ 3%（高殖利率）
I +PBR2       : PBR ≤ 2（價值股）
J PEMID+DIV3  : 估值合理 + 高殖利率
"""
import sys, time
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
from concurrent.futures import ProcessPoolExecutor

import data_loader as dl
import variant_strategy as vs

WORKERS = 16

VARIANTS = [
    ('A baseline',           'P0_T1T3+POS+IND+DXY'),
    ('B VWAPEXEC',           'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
    # 靜態估值過濾
    ('C +PEPOS',             'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEPOS'),
    ('D +PEMAX30',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEMAX30'),
    ('E +PEMAX50',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEMAX50'),
    ('F +PEMID',             'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEMID'),
    ('G +DIV3',              'P0_T1T3+POS+IND+DXY+VWAPEXEC+DIV3'),
    ('H +PBR2',              'P0_T1T3+POS+IND+DXY+VWAPEXEC+PBR2'),
    ('I PEMID+DIV3',         'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEMID+DIV3'),
    # 🆕 動態估值（盈餘動量 / 相對自己歷史）
    ('J +PEMOM10',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEMOM10'),  # 60d -10%
    ('K +PEMOM20',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEMOM20'),  # 60d -20%
    ('L +PEREL5',            'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEREL5'),   # 比 90d 中位低 5%
    ('M +PEREL10',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEREL10'),  # 比 90d 中位低 10%
    ('N +PEAVG',             'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEAVG'),    # < 90d avg
    ('O PEMOM10+PEAVG',      'P0_T1T3+POS+IND+DXY+VWAPEXEC+PEMOM10+PEAVG'),
]
WINDOWS = [
    ('FULL',  '2020-01-02', '2026-04-25'),
    ('TEST',  '2024-06-01', '2026-04-25'),
]


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, None)
        return (label, r['pnl_pct'])
    except Exception:
        return (label, None)


def metrics(returns):
    if not returns: return None
    arr = np.array(returns)
    return dict(
        n=len(arr),
        mean=float(arr.mean()),
        worst=float(arr.min()),
        win=float((arr > 0).mean() * 100),
        rr=float((arr.mean() / abs(arr.min())) if arr.min() < 0 else 0),
    )


def main():
    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    per_cache = set(p.stem for p in Path('per_cache').glob('*.parquet'))
    universe = sorted(t for t in (data_cache & vwap_cache & per_cache)
                      if t and t[0].isdigit() and len(t) == 4
                      and not t.startswith('00'))
    print(f"workers = {WORKERS}")
    print(f"data ∩ vwap ∩ per: {len(universe)} 檔\n")

    if len(universe) < 50:
        print("⚠️ per_cache 太少，等抓完再跑")
        return

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            for t in universe:
                all_tasks.append((t, mode, start, end, (var_name, win_name)))
    print(f"總任務：{len(all_tasks)}\n")

    t0 = time.time()
    bucket = {}
    n_done = 0
    milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, pnl in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if pnl is not None:
                bucket.setdefault(label, []).append(pnl)
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                rate = n_done / max(time.time()-t0, 0.1)
                print(f"  {pct:.0f}%  {rate:.0f}/s", flush=True)

    elapsed = time.time() - t0
    print(f"\n完成 {elapsed:.1f}s\n")

    print("=" * 90)
    print(f"EPS / P/E 估值過濾變體（{len(universe)} 檔）")
    print("=" * 90)
    print(f"{'變體':<22} {'Period':<6} {'n':>5} {'均值%':>9} {'最差%':>9} "
          f"{'勝率%':>7} {'RR':>7}  {'Δ vs B':>9}")
    print("-" * 90)
    base_rrs = {}
    for var_name, _ in VARIANTS:
        for win_name, _, _ in WINDOWS:
            m = metrics(bucket.get((var_name, win_name), []))
            if m:
                if var_name == 'B VWAPEXEC':
                    base_rrs[win_name] = m['rr']
                base = base_rrs.get(win_name, m['rr'])
                delta = m['rr'] - base
                marker = '⭐' if delta > 0.05 else ('❌' if delta < -0.05 else '')
                print(f"{var_name:<22} {win_name:<6} {m['n']:>5} "
                      f"{m['mean']:>+9.1f} {m['worst']:>+9.1f} "
                      f"{m['win']:>7.1f} {m['rr']:>7.3f}  {delta:>+9.3f} {marker}")
        print()

    # 看看 TEST 期最佳前 5
    print("=" * 90)
    print("TEST 期 RR Top 5")
    print("=" * 90)
    test_results = []
    for var_name, _ in VARIANTS:
        m = metrics(bucket.get((var_name, 'TEST'), []))
        if m: test_results.append((var_name, m['rr'], m['n']))
    test_results.sort(key=lambda x: -x[1])
    for i, (v, rr, n) in enumerate(test_results[:5], 1):
        print(f"  {i}. {v}: RR {rr:.3f}  (n={n})")


if __name__ == '__main__':
    main()
