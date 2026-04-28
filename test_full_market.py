"""全市場 VWAP 驗證 — 1277 檔 × 3 變體 × 3 時間窗
================================================================
規模：~11500 任務、16 worker、預估 ~50-90 分鐘

重要：data_cache 中可能含 ETF / 指數 / 反向ETF，這些不該納入。
過濾規則：股票代號 = 4 位數字 + 第一位 ≠ 00（排除 ETF）
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
RESULT_FILE = 'full_market_results.json'

VARIANTS = [
    ('A baseline',          'P0_T1T3+POS+IND+DXY'),
    ('B VWAPEXEC',          'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('C VWAPEXEC+NOSTOP',   'P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPNOSTOP'),
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
        best=float(arr.max()),
        win=float((arr > 0).mean() * 100),
        rr=float((arr.mean() / abs(arr.min())) if arr.min() < 0 else 0),
    )


def main():
    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = sorted(t for t in (data_cache & vwap_cache)
                      if t and t[0].isdigit() and len(t) == 4
                      and not t.startswith('00'))  # 排除 ETF

    print(f"Workers = {WORKERS}")
    print(f"全市場 VWAP 交集（4 位數字、非 00 開頭）: {len(universe)} 檔")
    print()

    # 額外切組：權值 / 中型 / 小型
    # 為了 segment 分析，依市值 proxy 用「樣本內最大 PnL」事後切
    # 這裡用簡化規則：4 位數字  + 1xxx/2xxx 為傳統大型；3xxx-5xxx 為中型；6xxx+ 為小型
    SEGMENTS = [
        ('🌐 全市場',      universe),
        ('🏛️ 1xxx-2xxx',   [t for t in universe if t[0] in '12']),
        ('🏗️ 3xxx-5xxx',   [t for t in universe if t[0] in '345']),
        ('🚀 6xxx+',       [t for t in universe if t[0] in '6789']),
    ]
    for name, uni in SEGMENTS:
        print(f"  {name}: {len(uni)} 檔")
    print()

    # 一次塞所有任務
    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            for t in universe:
                all_tasks.append((t, mode, start, end, (var_name, win_name)))
    print(f"總任務：{len(all_tasks)}")
    print(f"開始平行運算...\n")

    t0 = time.time()
    bucket = {}  # (var, win) -> [(ticker, pnl_pct), ...]
    n_done = 0
    milestone = max(1, len(all_tasks) // 25)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, pnl in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if pnl is not None:
                bucket.setdefault(label, []).append((ticker, pnl))
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                rate = n_done / max(time.time()-t0, 0.1)
                eta = (len(all_tasks) - n_done) / max(rate, 0.1) / 60
                print(f"  {pct:.0f}% ({n_done}/{len(all_tasks)})  "
                      f"{rate:.0f}/s  ETA {eta:.1f} min", flush=True)

    elapsed = time.time() - t0
    print(f"\n完成！{elapsed/60:.1f} min ({len(all_tasks)/elapsed:.1f} task/s)\n")

    # 各 segment 報告
    for seg_name, seg_uni in SEGMENTS:
        seg_set = set(seg_uni)
        print("=" * 78)
        print(f"{seg_name}  (n={len(seg_uni)})")
        print("=" * 78)
        print(f"{'變體':<22} {'Period':<7} {'n':>4} {'均值%':>9} {'最差%':>9} "
              f"{'勝率%':>7} {'RR':>7}")
        print("-" * 78)
        seg_metrics = {}
        for var_name, _ in VARIANTS:
            for win_name, _, _ in WINDOWS:
                pnls = [p for t, p in bucket.get((var_name, win_name), [])
                        if t in seg_set]
                m = metrics(pnls)
                if m:
                    seg_metrics[(var_name, win_name)] = m
                    print(f"{var_name:<22} {win_name:<7} {m['n']:>4} "
                          f"{m['mean']:>+9.1f} {m['worst']:>+9.1f} "
                          f"{m['win']:>7.1f} {m['rr']:>7.3f}")
            print()

        # 三變體 Δ RR
        print(f"  Δ RR（本 segment）:")
        for win_name, _, _ in WINDOWS:
            a = seg_metrics.get(('A baseline', win_name))
            b = seg_metrics.get(('B VWAPEXEC', win_name))
            c = seg_metrics.get(('C VWAPEXEC+NOSTOP', win_name))
            if a and b and c:
                tot = b['rr'] - a['rr']
                stp = b['rr'] - c['rr']
                ns  = c['rr'] - a['rr']
                ratio = stp/tot*100 if tot != 0 else 0
                print(f"    {win_name:<6}  總 Δ {tot:+.3f}   "
                      f"非停損 {ns:+.3f} ({100-ratio:.0f}%)   "
                      f"停損 {stp:+.3f} ({ratio:.0f}%)")
        print()

    # 存檔便於後續引用
    import json
    with open(RESULT_FILE, 'w', encoding='utf-8') as f:
        save_data = {}
        for (var, win), pnls in bucket.items():
            key = f"{var}|{win}"
            save_data[key] = {
                'tickers':  [t for t, p in pnls],
                'pnl_pcts': [p for t, p in pnls],
            }
        json.dump(save_data, f, indent=2, ensure_ascii=False)
    print(f"✅ 個別結果已存入 {RESULT_FILE}")


if __name__ == '__main__':
    main()
