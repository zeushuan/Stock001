"""VWAP 停損出場隔離驗證
==================================
A: baseline                  POS+IND+DXY                       (no VWAP)
B: VWAPEXEC                  POS+IND+DXY+VWAPEXEC              (all exits 用 VWAP)
C: VWAPEXEC+NOSTOP           POS+IND+DXY+VWAPEXEC+VWAPNOSTOP   (停損用市價)

判讀：
  C ≈ B   → 停損 VWAP 貢獻小，UI 警告風險合理
  C << B  → 停損 VWAP 貢獻大，必須提醒使用者「沒盯盤直接市價」
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
    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = sorted(data_cache & vwap_cache)
    print(f"可分析 {len(universe)} 檔（data_cache ∩ vwap_cache）\n")

    VARIANTS = [
        ('A: baseline',       'P0_T1T3+POS+IND+DXY'),
        ('B: VWAPEXEC',       'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
        ('C: VWAPEXEC+NOSTOP','P0_T1T3+POS+IND+DXY+VWAPEXEC+VWAPNOSTOP'),
    ]
    WINDOWS = [
        ('FULL',  '2020-01-02', '2026-04-25'),
        ('TEST',  '2024-06-01', '2026-04-25'),
    ]

    results = {}
    for win_name, start, end in WINDOWS:
        print(f"\n[{win_name}] {start} ~ {end}")
        print("-" * 75)
        for var_name, mode in VARIANTS:
            print(f"  {var_name:<22}: ", end='', flush=True)
            m = run_slice(universe, mode, start, end)
            if m:
                print(f"  n={m['n']}  mean={m['mean']:+.2f}%  "
                      f"worst={m['worst']:+.2f}%  win={m['win']:.1f}%  RR={m['rr']:.3f}")
                results[(var_name, win_name)] = m

    # 結論
    print("\n" + "=" * 75)
    print("📊 停損出場 VWAP 貢獻拆解")
    print("=" * 75)

    for win_name, _, _ in WINDOWS:
        a = results.get(('A: baseline', win_name))
        b = results.get(('B: VWAPEXEC', win_name))
        c = results.get(('C: VWAPEXEC+NOSTOP', win_name))
        if a and b and c:
            print(f"\n[{win_name}]")
            print(f"  A baseline RR             : {a['rr']:.3f}")
            print(f"  B VWAPEXEC (all)          : {b['rr']:.3f}  Δ vs A: {b['rr']-a['rr']:+.3f}")
            print(f"  C VWAPEXEC (skip stop)    : {c['rr']:.3f}  Δ vs A: {c['rr']-a['rr']:+.3f}")
            print(f"  → 停損 VWAP 貢獻 (B - C)  : {b['rr']-c['rr']:+.3f}")
            print(f"  → 非停損 VWAP 貢獻 (C - A): {c['rr']-a['rr']:+.3f}")

            ratio = (b['rr']-c['rr']) / (b['rr']-a['rr']) * 100 if (b['rr']-a['rr']) != 0 else 0
            print(f"  → 停損占總提升比例        : {ratio:.1f}%")

    print("\n" + "=" * 75)
    print("解讀")
    print("-" * 75)
    print("  停損占比 < 30% → 主要靠非停損（獲利出場）優化，停損風險可控")
    print("  停損占比 > 50% → 大部分提升來自停損 VWAP，要警告盯盤需求")


if __name__ == '__main__':
    main()
