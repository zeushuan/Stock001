"""VWAP 完整驗證（單次跑全部）+ CPU 滿載
================================================
合併「test_vwap_100 + test_vwap_nostop」為一支腳本：
  - 4 變體 × 3 時間窗 × N 股 = 全部塞進單一 ProcessPool
  - workers = 16（12C/16T 機台滿載）
  - 中小型樣本 + 大型樣本 + 合併樣本三組對照
"""
import sys, time
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
from concurrent.futures import ProcessPoolExecutor

import data_loader as dl
import variant_strategy as vs

WORKERS = 16  # 12C/16T 滿載

VARIANTS = [
    ('A baseline',        'P0_T1T3+POS+IND+DXY'),
    ('B VWAPEXEC',        'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('C VWAPEXEC+NOSTOP', 'P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPNOSTOP'),
]
WINDOWS = [
    ('FULL',  '2020-01-02', '2026-04-25'),
    ('TRAIN', '2020-01-02', '2024-05-31'),
    ('TEST',  '2024-06-01', '2026-04-25'),
]


def run_one(args):
    """ProcessPool unit task — must be module-level."""
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
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
    # 三組樣本：大型（前 100）、中小型（後抓 100）、合併
    from fetch_100 import TICKERS_100 as LARGE
    try:
        from fetch_smallmid_100 import SMALLMID_100
    except ImportError:
        SMALLMID_100 = []

    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    available = data_cache & vwap_cache

    large_uni    = sorted(set(LARGE) & available)
    smallmid_uni = sorted(set(SMALLMID_100) & available) if SMALLMID_100 else []
    combined_uni = sorted(set(LARGE + SMALLMID_100) & available) if SMALLMID_100 else large_uni

    SAMPLES = [
        ('🏛️ 大型權值股 (top 100)', large_uni),
    ]
    if smallmid_uni:
        SAMPLES.append(('📊 中小型股 (next 100)', smallmid_uni))
    if smallmid_uni and combined_uni != large_uni:
        SAMPLES.append(('🔗 合併樣本', combined_uni))

    print(f"workers = {WORKERS}（12C/16T 滿載）\n")
    for label, uni in SAMPLES:
        print(f"  {label}: {len(uni)} 檔")
    print()

    # 一次塞所有 (ticker, mode, window, label) 進 pool
    all_tasks = []
    for samp_label, uni in SAMPLES:
        for win_name, start, end in WINDOWS:
            for var_name, mode in VARIANTS:
                key = (samp_label, var_name, win_name)
                for t in uni:
                    all_tasks.append((t, mode, start, end, key))

    print(f"總任務數：{len(all_tasks)} 個 (ticker, variant, window) tuples")
    print(f"開始 {WORKERS}-worker 平行運算...\n")

    t0 = time.time()
    bucket = {}  # key -> list of pnl_pct
    n_done = 0
    n_milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, pnl in ex.map(run_one, all_tasks, chunksize=50):
            n_done += 1
            if pnl is not None:
                bucket.setdefault(label, []).append(pnl)
            if n_done % n_milestone == 0:
                pct = n_done / len(all_tasks) * 100
                rate = n_done / max(time.time()-t0, 0.1)
                eta = (len(all_tasks) - n_done) / max(rate, 0.1)
                print(f"  進度 {n_done}/{len(all_tasks)} ({pct:.0f}%)  "
                      f"速率 {rate:.0f}/s  ETA {eta:.0f}s")

    elapsed = time.time() - t0
    print(f"\n完成！總耗時 {elapsed:.1f}s ({len(all_tasks)/elapsed:.0f} tasks/s)\n")

    # 列印每組樣本的結果
    for samp_label, uni in SAMPLES:
        print("=" * 78)
        print(f"{samp_label} (n={len(uni)})")
        print("=" * 78)
        print(f"{'變體':<20} {'Period':<7} {'n':>4} {'均值%':>9} {'最差%':>9} "
              f"{'最佳%':>9} {'勝率%':>7} {'RR':>7}")
        print("-" * 78)
        for var_name, _ in VARIANTS:
            for win_name, _, _ in WINDOWS:
                key = (samp_label, var_name, win_name)
                m = metrics(bucket.get(key, []))
                if m:
                    print(f"{var_name:<20} {win_name:<7} {m['n']:>4} "
                          f"{m['mean']:>+9.1f} {m['worst']:>+9.1f} "
                          f"{m['best']:>+9.1f} {m['win']:>7.1f} {m['rr']:>7.3f}")
            print()

        # 三變體的 Δ RR 比較
        print(f"  Δ RR 拆解（本樣本）:")
        for win_name, _, _ in WINDOWS:
            a = metrics(bucket.get((samp_label, 'A baseline', win_name), []))
            b = metrics(bucket.get((samp_label, 'B VWAPEXEC', win_name), []))
            c = metrics(bucket.get((samp_label, 'C VWAPEXEC+NOSTOP', win_name), []))
            if a and b and c:
                tot   = b['rr'] - a['rr']
                stop  = b['rr'] - c['rr']
                nstop = c['rr'] - a['rr']
                ratio = stop / tot * 100 if tot != 0 else 0
                print(f"    {win_name:<6}  總 Δ {tot:+.3f}  "
                      f"非停損貢獻 {nstop:+.3f} ({100-ratio:.0f}%)  "
                      f"停損貢獻 {stop:+.3f} ({ratio:.0f}%)")
        print()


if __name__ == '__main__':
    main()
