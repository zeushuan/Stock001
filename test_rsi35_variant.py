"""T3 RSI 門檻變體測試
========================
比較 v8 不同 RSI 進場門檻：
  baseline = P5+VWAPEXEC（預設 RSI<50）
  +RSI45 = T3 改 RSI<45
  +RSI40 = T3 改 RSI<40
  +RSI35 = T3 改 RSI<35（深度拉回）
  +RSI30 = T3 改 RSI<30（極深拉回）
"""
import sys, time
from pathlib import Path
import numpy as np
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs

WORKERS = 16

VARIANTS = [
    ('A baseline',         'P0_T1T3+POS+IND+DXY'),
    ('B P5+VWAPEXEC',      'P5_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('C +RSI45',           'P5_T1T3+POS+IND+DXY+VWAPEXEC+RSI45'),
    ('D +RSI40',           'P5_T1T3+POS+IND+DXY+VWAPEXEC+RSI40'),
    ('E +RSI35 (T3DEEP)',  'P5_T1T3+POS+IND+DXY+VWAPEXEC+RSI35'),
    ('F +RSI30',           'P5_T1T3+POS+IND+DXY+VWAPEXEC+RSI30'),
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
        if r is None or r.get('n_trades', 0) == 0: return (label, None)
        return (label, r['pnl_pct'])
    except Exception:
        return (label, None)


def metrics(returns):
    if not returns: return None
    arr = np.array(returns)
    return dict(n=len(arr), mean=float(arr.mean()), worst=float(arr.min()),
                win=float((arr > 0).mean() * 100),
                rr=float(arr.mean() / abs(arr.min())) if arr.min() < 0 else 0)


def main():
    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = sorted(t for t in (data_cache & vwap_cache)
                      if t and t[0].isdigit() and len(t) == 4
                      and not t.startswith('00'))
    print(f"workers = {WORKERS}, universe = {len(universe)} 檔\n")

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
                print(f"  {pct:.0f}%", flush=True)

    print(f"\n完成 {time.time()-t0:.1f}s\n")

    print("=" * 90)
    print(f"T3 RSI 門檻變體驗證（{len(universe)} 檔）")
    print("=" * 90)
    print(f"{'變體':<24} {'Period':<6} {'n':>5} {'均值%':>9} {'最差%':>9} "
          f"{'勝率%':>7} {'RR':>7}  {'Δ vs B':>8}")
    print("-" * 90)
    base_rrs = {}
    for var_name, _ in VARIANTS:
        for win_name, _, _ in WINDOWS:
            m = metrics(bucket.get((var_name, win_name), []))
            if m:
                if var_name == 'B P5+VWAPEXEC':
                    base_rrs[win_name] = m['rr']
                base = base_rrs.get(win_name, m['rr'])
                delta = m['rr'] - base
                marker = '⭐' if delta > 0.05 else ('❌' if delta < -0.05 else '➖')
                print(f"{var_name:<24} {win_name:<6} {m['n']:>5} "
                      f"{m['mean']:>+9.1f} {m['worst']:>+9.1f} "
                      f"{m['win']:>7.1f} {m['rr']:>7.3f}  {delta:>+8.3f} {marker}")
        print()

    # TEST 期排名
    print("=" * 90)
    print("TEST 期 RR 排名")
    print("=" * 90)
    test_results = []
    for var_name, _ in VARIANTS:
        m = metrics(bucket.get((var_name, 'TEST'), []))
        if m: test_results.append((var_name, m))
    test_results.sort(key=lambda x: -x[1]['rr'])
    for i, (v, m) in enumerate(test_results, 1):
        print(f"  {i}. {v}: RR {m['rr']:.3f} (n={m['n']}, mean {m['mean']:+.1f}, "
              f"worst {m['worst']:+.1f}, win {m['win']:.1f}%)")


if __name__ == '__main__':
    main()
