"""VWAP 進階變體驗證 — 全市場 1050 檔
=================================================
新變體：
  VWAPDEV{N}    : 偏離 N% 以上才進場（N=1/2/3）
  VWAPBAND{N}   : close ≤ VWAP - N×(range/4) 才進場（N=1/2）
  STRONGCL      : 前日 close 位於日內 70% 以上（強勢追勢）
  WEAKCL        : 前日 close 位於日內 30% 以下（逢低）

對比基準：
  baseline      : POS+IND+DXY
  VWAPEXEC      : 已知贏家（5 年研究最大突破）

組合測試：
  各新變體單獨    + VWAPEXEC
  STRONGCL/WEAKCL + VWAPEXEC（看日內結構是否提升）
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
    ('C VWAPDEV1',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPDEV1'),
    ('D VWAPDEV2',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPDEV2'),
    ('E VWAPDEV3',           'P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPDEV3'),
    ('F VWAPBAND1',          'P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPBAND1'),
    ('G VWAPBAND2',          'P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPBAND2'),
    ('H +STRONGCL',          'P0_T1T3+POS+IND+DXY+VWAPEXEC+STRONGCL'),
    ('I +WEAKCL',            'P0_T1T3+POS+IND+DXY+VWAPEXEC+WEAKCL'),
]
WINDOWS = [
    ('FULL',  '2020-01-02', '2026-04-25'),
    ('TEST',  '2024-06-01', '2026-04-25'),
]


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, ticker, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, ticker, None)
        return (label, ticker, r['pnl_pct'])
    except Exception:
        return (label, ticker, None)


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
        for label, ticker, pnl in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if pnl is not None:
                bucket.setdefault(label, []).append((ticker, pnl))
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                rate = n_done / max(time.time()-t0, 0.1)
                print(f"  {pct:.0f}% ({n_done}/{len(all_tasks)})  {rate:.0f}/s", flush=True)

    elapsed = time.time() - t0
    print(f"\n完成 {elapsed:.1f}s ({len(all_tasks)/elapsed:.0f} task/s)\n")

    # 列印結果
    print("=" * 88)
    print(f"全市場 {len(universe)} 檔 VWAP 進階變體驗證")
    print("=" * 88)
    print(f"{'變體':<22} {'Period':<6} {'n':>5} {'均值%':>9} {'最差%':>9} "
          f"{'勝率%':>7} {'RR':>7}  {'Δ vs B':>8}")
    print("-" * 88)
    base_rrs = {}
    for var_name, _ in VARIANTS:
        for win_name, _, _ in WINDOWS:
            pnls = [p for t, p in bucket.get((var_name, win_name), [])]
            m = metrics(pnls)
            if m:
                if var_name == 'B VWAPEXEC':
                    base_rrs[win_name] = m['rr']
                base = base_rrs.get(win_name, m['rr'])
                delta = m['rr'] - base
                marker = '⭐' if delta > 0.05 else ('❌' if delta < -0.05 else '')
                print(f"{var_name:<22} {win_name:<6} {m['n']:>5} "
                      f"{m['mean']:>+9.1f} {m['worst']:>+9.1f} "
                      f"{m['win']:>7.1f} {m['rr']:>7.3f}  {delta:>+8.3f} {marker}")
        print()

    # 找最佳
    print("=" * 88)
    print("最佳變體（Δ vs VWAPEXEC > 0）")
    print("=" * 88)
    best_per_window = {}
    for win_name, _, _ in WINDOWS:
        win_results = []
        for var_name, _ in VARIANTS:
            pnls = [p for t, p in bucket.get((var_name, win_name), [])]
            m = metrics(pnls)
            if m: win_results.append((var_name, m['rr']))
        win_results.sort(key=lambda x: -x[1])
        print(f"\n[{win_name}]")
        for i, (v, rr) in enumerate(win_results[:5], 1):
            print(f"  {i}. {v}: RR {rr:.3f}")


if __name__ == '__main__':
    main()
