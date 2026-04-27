"""黑天鵝防護驗證
==================
A baseline                       POS+IND+DXY
B +BSGUARD（暫停 T1/T3 進場）    POS+IND+DXY+BSGUARD
C +VWAPEXEC （已驗證）            POS+IND+DXY+VWAPEXEC
D VWAPEXEC + BSGUARD（疊加）     POS+IND+DXY+VWAPEXEC+BSGUARD

關注指標：
  - 整體 RR 是否提升
  - 最差個股是否改善（黑天鵝主要打擊在 worst case）
  - 危險窗期間單獨表現對比
"""
import sys
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import time
import numpy as np
from concurrent.futures import ProcessPoolExecutor

import data_loader as dl
import variant_strategy as vs

WORKERS = 16

VARIANTS = [
    ('A baseline',          'P0_T1T3+POS+IND+DXY'),
    ('B +BSGUARD',          'P0_T1T3+POS+IND+DXY+BSGUARD'),
    ('C +VWAPEXEC',         'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('D VWAPEXEC+BSGUARD',  'P0_T1T3+POS+IND+DXY+VWAPEXEC+BSGUARD'),
]
WINDOWS = [
    ('FULL',  '2020-01-02', '2026-04-25'),
    ('TRAIN', '2020-01-02', '2024-05-31'),
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
        mean=arr.mean(),
        worst=arr.min(),
        best=arr.max(),
        win=(arr > 0).mean() * 100,
        rr=(arr.mean() / abs(arr.min())) if arr.min() < 0 else 0,
    )


def main():
    # 樣本：所有有 vwap_cache 的（保證 4 變體公平比較）
    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = sorted(data_cache & vwap_cache)
    print(f"workers = {WORKERS}\n樣本 (data ∩ vwap): {len(universe)} 檔\n")

    # 建任務
    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            key = (var_name, win_name)
            for t in universe:
                all_tasks.append((t, mode, start, end, key))
    print(f"總任務：{len(all_tasks)}\n")

    t0 = time.time()
    bucket = {}
    n_done = 0
    milestone = max(1, len(all_tasks) // 10)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, pnl in ex.map(run_one, all_tasks, chunksize=50):
            n_done += 1
            if pnl is not None:
                bucket.setdefault(label, []).append(pnl)
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                print(f"  {pct:.0f}% ({n_done}/{len(all_tasks)})", flush=True)

    print(f"\n完成 {time.time()-t0:.1f}s\n")

    # 輸出
    print("=" * 80)
    print(f"📊 黑天鵝防護驗證（{len(universe)} 檔）")
    print("=" * 80)
    print(f"{'變體':<22} {'Period':<7} {'n':>4} {'均值%':>9} {'最差%':>9} "
          f"{'最佳%':>9} {'勝率%':>7} {'RR':>7}")
    print("-" * 80)
    for var_name, _ in VARIANTS:
        for win_name, _, _ in WINDOWS:
            m = metrics(bucket.get((var_name, win_name), []))
            if m:
                print(f"{var_name:<22} {win_name:<7} {m['n']:>4} "
                      f"{m['mean']:>+9.1f} {m['worst']:>+9.1f} "
                      f"{m['best']:>+9.1f} {m['win']:>7.1f} {m['rr']:>7.3f}")
        print()

    # Δ RR 比較
    print("=" * 80)
    print("Δ RR 對比")
    print("=" * 80)
    for win_name, _, _ in WINDOWS:
        a = metrics(bucket.get(('A baseline', win_name), []))
        b = metrics(bucket.get(('B +BSGUARD', win_name), []))
        c = metrics(bucket.get(('C +VWAPEXEC', win_name), []))
        d = metrics(bucket.get(('D VWAPEXEC+BSGUARD', win_name), []))
        if a and b and c and d:
            print(f"\n[{win_name}]")
            print(f"  A baseline             : RR {a['rr']:.3f}")
            print(f"  B +BSGUARD             : RR {b['rr']:.3f}  Δ {b['rr']-a['rr']:+.3f}")
            print(f"  C +VWAPEXEC（已知贏家）  : RR {c['rr']:.3f}  Δ {c['rr']-a['rr']:+.3f}")
            print(f"  D VWAPEXEC+BSGUARD     : RR {d['rr']:.3f}  Δ {d['rr']-a['rr']:+.3f}")
            print(f"  → BSGUARD 邊際貢獻     : Δ vs C = {d['rr']-c['rr']:+.3f}")
            print(f"  → BSGUARD 改善最差個股 : {a['worst']:+.1f}% → {b['worst']:+.1f}% "
                  f"({b['worst']-a['worst']:+.1f}pp)")

    # 結論
    print("\n" + "=" * 80)
    bs_test = metrics(bucket.get(('B +BSGUARD', 'TEST'), []))
    a_test  = metrics(bucket.get(('A baseline', 'TEST'), []))
    if bs_test and a_test:
        delta = bs_test['rr'] - a_test['rr']
        if delta > 0.1:
            print(f"  ✅ BSGUARD 在 TEST 期 Δ RR {delta:+.3f} → 黑天鵝防護有效")
        elif delta > 0:
            print(f"  ⚠️ BSGUARD Δ RR {delta:+.3f} 微弱正向 → 邊際效益有限")
        else:
            print(f"  ❌ BSGUARD Δ RR {delta:+.3f} → 切過頭，砍掉太多正常進場機會")


if __name__ == '__main__':
    main()
