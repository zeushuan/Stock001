"""動態停損 + 加碼點 × VWAPEXEC 完整驗證
==================================================
D 動態 ATR 停損（持倉>30d & 獲利>20% → ATR×1.5 trailing）
E 加碼點 P5/P10/P15/P20 + VWAPEXEC 重測（之前無 VWAPEXEC 全敗）

共 9 變體 × 全市場 1050 檔 × 2 時間窗 = 18900 任務
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
    # D 動態停損
    ('C +DYNSTOP',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+DYNSTOP'),
    # E 加碼點 + VWAPEXEC
    ('D P5+VWAPEXEC',        'P5_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('E P10+VWAPEXEC',       'P10_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('F P15+VWAPEXEC',       'P15_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('G P20+VWAPEXEC',       'P20_T1T3+POS+IND+DXY+VWAPEXEC'),
    # 組合：DYNSTOP + 最佳加碼
    ('H P5+VWAPEXEC+DYN',    'P5_T1T3+POS+IND+DXY+VWAPEXEC+DYNSTOP'),
    ('I P10+VWAPEXEC+DYN',   'P10_T1T3+POS+IND+DXY+VWAPEXEC+DYNSTOP'),
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
                rate = n_done / max(time.time()-t0, 0.1)
                print(f"  {pct:.0f}%  {rate:.0f}/s", flush=True)

    elapsed = time.time() - t0
    print(f"\n完成 {elapsed:.1f}s\n")

    print("=" * 90)
    print(f"DYNSTOP + 加碼點 × VWAPEXEC 全市場驗證（{len(universe)} 檔）")
    print("=" * 90)
    print(f"{'變體':<25} {'Period':<6} {'n':>5} {'均值%':>9} {'最差%':>9} "
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
                print(f"{var_name:<25} {win_name:<6} {m['n']:>5} "
                      f"{m['mean']:>+9.1f} {m['worst']:>+9.1f} "
                      f"{m['win']:>7.1f} {m['rr']:>7.3f}  {delta:>+9.3f} {marker}")
        print()

    print("=" * 90)
    print("TEST 期 RR Top 5")
    print("=" * 90)
    test_results = []
    for var_name, _ in VARIANTS:
        m = metrics(bucket.get((var_name, 'TEST'), []))
        if m: test_results.append((var_name, m['rr'], m['mean'], m['worst'], m['win'], m['n']))
    test_results.sort(key=lambda x: -x[1])
    for i, (v, rr, mean, worst, win, n) in enumerate(test_results[:5], 1):
        print(f"  {i}. {v}: RR {rr:.3f} (mean {mean:+.1f} worst {worst:+.1f} win {win:.1f}% n={n})")


if __name__ == '__main__':
    main()
