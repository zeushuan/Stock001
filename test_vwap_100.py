"""100 檔擴大樣本驗證：baseline vs +VWAPEXEC
================================================
雙方法檢驗：
  1. 全期間 RR
  2. Walk-Forward 7:3（Train 2020-2024.5 / Test 2024.6-2026.4）

只用同時有 data_cache + vwap_cache 的股票。
"""
import sys
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
from concurrent.futures import ProcessPoolExecutor

import data_loader as dl
import variant_strategy as vs


def run_one(args):
    ticker, mode, start, end = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0: return None
        return r['pnl_pct']
    except Exception:
        return None


def metrics(returns):
    if not returns: return None
    arr = np.array(returns)
    return dict(
        n=len(arr),
        mean=arr.mean(),
        worst=arr.min(),
        win=(arr > 0).mean() * 100,
        rr=(arr.mean() / abs(arr.min())) if arr.min() < 0 else 0,
    )


def run_slice(tickers, mode, start, end, workers=8):
    args = [(t, mode, start, end) for t in tickers]
    rets = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for r in ex.map(run_one, args, chunksize=10):
            if r is not None: rets.append(r)
    return metrics(rets)


def main():
    # 取得交集：data_cache + vwap_cache 都有
    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = sorted(data_cache & vwap_cache)
    print(f"data_cache: {len(data_cache)} 檔")
    print(f"vwap_cache: {len(vwap_cache)} 檔")
    print(f"交集 (可分析): {len(universe)} 檔\n")

    if len(universe) < 30:
        print(f"❌ 樣本不足 ({len(universe)} < 30)，請先抓更多 VWAP")
        return

    VARIANTS = [
        ('baseline', 'P0_T1T3+POS+IND+DXY'),
        ('VWAPEXEC', 'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
    ]
    WINDOWS = [
        ('FULL',  '2020-01-02', '2026-04-25'),
        ('TRAIN', '2020-01-02', '2024-05-31'),
        ('TEST',  '2024-06-01', '2026-04-25'),
    ]

    results = {}  # (variant_name, window_label) -> metrics
    for win_name, start, end in WINDOWS:
        print(f"\n[{win_name}] {start} ~ {end}")
        print("-" * 70)
        for var_name, mode in VARIANTS:
            print(f"  {var_name}: ", end='', flush=True)
            m = run_slice(universe, mode, start, end)
            if m:
                print(f"  n={m['n']}  mean={m['mean']:+.2f}%  worst={m['worst']:+.2f}%  "
                      f"win={m['win']:.1f}%  RR={m['rr']:.3f}")
                results[(var_name, win_name)] = m
            else:
                print("  (no data)")

    # 結果彙總
    print("\n" + "=" * 70)
    print("📊 100 檔擴大樣本驗證結果")
    print("=" * 70)
    print(f"\n{'變體':<10} {'Period':<7} {'n':>5} {'均值%':>9} {'最差%':>9} {'勝率%':>8} {'RR':>8}")
    print("-" * 70)
    for var_name, _ in VARIANTS:
        for win_name, _, _ in WINDOWS:
            m = results.get((var_name, win_name))
            if m:
                print(f"{var_name:<10} {win_name:<7} {m['n']:>5} "
                      f"{m['mean']:>+9.2f} {m['worst']:>+9.2f} "
                      f"{m['win']:>8.1f} {m['rr']:>8.3f}")
        print()

    # Δ RR 比較
    print("=" * 70)
    print("Δ RR (VWAPEXEC - baseline)")
    print("=" * 70)
    for win_name, _, _ in WINDOWS:
        b = results.get(('baseline', win_name))
        v = results.get(('VWAPEXEC', win_name))
        if b and v:
            delta = v['rr'] - b['rr']
            marker = '⭐' if delta > 0.1 else ('✅' if delta > 0 else ('❌' if delta < -0.1 else '➖'))
            print(f"  {win_name:<6}  baseline {b['rr']:>6.3f}  →  VWAPEXEC {v['rr']:>6.3f}  "
                  f"Δ {delta:+.3f}  {marker}")

    # 最終結論
    print("\n" + "=" * 70)
    test_b = results.get(('baseline', 'TEST'))
    test_v = results.get(('VWAPEXEC', 'TEST'))
    if test_b and test_v:
        delta = test_v['rr'] - test_b['rr']
        if delta > 0.1:
            print(f"  ✅ TEST 期 Δ RR +{delta:.3f} > 0.1 → out-of-sample 有效，VWAPEXEC 是真 alpha")
        elif delta > 0:
            print(f"  ⚠️ TEST 期 Δ RR +{delta:.3f}（輕微正向）→ 有效但偏弱")
        else:
            print(f"  ❌ TEST 期 Δ RR {delta:+.3f} → 過擬合，樣本內 alpha 不能推廣")


if __name__ == '__main__':
    main()
