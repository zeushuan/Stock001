"""1m VWAP 精度驗證 — 30 檔大型股 vs 現有 5m
============================================
1m bar 已抓完，這次只跑分析比對。
"""
import sys, time, shutil
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor

import data_loader as dl
import variant_strategy as vs
from fugle_connector import get_minute_candles
from vwap_loader import compute_daily_vwap

CACHE_5M = Path(__file__).parent / 'vwap_cache'
CACHE_1M = Path(__file__).parent / 'vwap_cache_1m'
CACHE_1M.mkdir(exist_ok=True)

SAMPLE_30 = [
    '2330','2317','2454','2412','2308','2882','2891','2886','2884','2603',
    '2610','3008','6505','2885','2880','2883','3034','2474','2382','6669',
    '8081','2207','2002','1216','1303','1101','2615','2618','2890','2887',
]

# Module level worker（避免 pickle 錯誤）
def _run_one(args):
    ticker, mode, start, end = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0: return None
        return r['pnl_pct']
    except Exception:
        return None


def main():
    print(f"📊 1m VWAP 精度驗證（{len(SAMPLE_30)} 檔大型股）\n")

    # Step 1: 補抓未完成的 1m bar
    print("Step 1: 確保 1m bar 完整...")
    missing = [t for t in SAMPLE_30 if not (CACHE_1M / f'{t}.parquet').exists()]
    if missing:
        print(f"  待抓 {len(missing)} 檔: {missing}")
        for t in missing:
            try:
                df = get_minute_candles(t, start='2024-06-01', end='2026-04-27',
                                         freq='1m', use_cache=False)
                if df is not None and not df.empty:
                    vw = compute_daily_vwap(df)
                    if vw is not None:
                        vw.to_parquet(CACHE_1M / f'{t}.parquet')
                        print(f"  [{t}] ✅ {len(vw)} 日")
                time.sleep(1.0)
            except Exception as e:
                print(f"  [{t}] ❌ {str(e)[:50]}")
    else:
        print("  ✅ 全部 30 檔 1m bar 已快取\n")

    # Step 2: 比較 1m vs 5m VWAP
    print("\n" + "=" * 70)
    print("Step 2: 1m vs 5m VWAP 數值差距")
    print("=" * 70)
    print(f"{'代號':<6} {'匹配日':>6} {'平均差%':>8} {'中位差%':>8} {'最大差%':>8} {'差>0.3%':>8}")
    print("-" * 60)

    all_diffs = []
    for t in SAMPLE_30:
        p5 = CACHE_5M / f'{t}.parquet'
        p1 = CACHE_1M / f'{t}.parquet'
        if not (p5.exists() and p1.exists()): continue
        df5 = pd.read_parquet(p5)
        df1 = pd.read_parquet(p1)
        common = df5.index.intersection(df1.index)
        if len(common) < 10: continue

        v5 = df5.loc[common, 'VWAP']
        v1 = df1.loc[common, 'VWAP']
        diff_pct = ((v1 - v5) / v5 * 100).abs()
        all_diffs.append((t, len(common), diff_pct.mean(),
                          diff_pct.median(), diff_pct.max(),
                          (diff_pct > 0.3).sum()))
        print(f"{t:<6} {len(common):>6} {diff_pct.mean():>8.3f} "
              f"{diff_pct.median():>8.3f} {diff_pct.max():>8.3f} "
              f"{(diff_pct > 0.3).sum():>8d}")

    if all_diffs:
        avg = np.mean([d[2] for d in all_diffs])
        med = np.median([d[3] for d in all_diffs])
        mx = max(d[4] for d in all_diffs)
        big = sum(d[5] for d in all_diffs)
        total = sum(d[1] for d in all_diffs)
        print(f"\n📊 整體：30 檔平均差 {avg:.3f}% / 中位 {med:.3f}% / 最大 {mx:.3f}%")
        print(f"   差距 > 0.3% 的天數：{big}/{total} ({big/total*100:.2f}%)")

    # Step 3: 回測對比
    print("\n" + "=" * 70)
    print("Step 3: v8+P5+VWAPEXEC — 1m vs 5m 回測 RR 差異")
    print("=" * 70)

    args = [(t, 'P5_T1T3+POS+IND+DXY+VWAPEXEC', '2024-06-01', '2026-04-25')
            for t in SAMPLE_30 if (CACHE_5M / f'{t}.parquet').exists()
            and (CACHE_1M / f'{t}.parquet').exists()]

    print(f"\n→ 5m baseline...")
    with ProcessPoolExecutor(max_workers=8) as ex:
        results_5m = [r for r in ex.map(_run_one, args) if r is not None]
    arr5 = np.array(results_5m)
    rr_5m = arr5.mean() / abs(arr5.min()) if arr5.min() < 0 else 0
    print(f"   n={len(arr5)} mean={arr5.mean():+.1f}% worst={arr5.min():+.1f}% RR={rr_5m:.3f}")

    # 切換 cache 到 1m
    print(f"\n→ 切到 1m cache 跑...")
    backup = Path('vwap_cache_5m_backup_30')
    if backup.exists(): shutil.rmtree(backup)
    backup.mkdir()
    for t in SAMPLE_30:
        s = CACHE_5M / f'{t}.parquet'
        s1 = CACHE_1M / f'{t}.parquet'
        if s.exists() and s1.exists():
            shutil.copy(s, backup / f'{t}.parquet')
            shutil.copy(s1, s)

    with ProcessPoolExecutor(max_workers=8) as ex:
        results_1m = [r for r in ex.map(_run_one, args) if r is not None]

    # 還原
    for f in backup.glob('*.parquet'):
        shutil.copy(f, CACHE_5M / f.name)
    shutil.rmtree(backup)
    print("   5m cache 已還原")

    arr1 = np.array(results_1m)
    rr_1m = arr1.mean() / abs(arr1.min()) if arr1.min() < 0 else 0
    print(f"   n={len(arr1)} mean={arr1.mean():+.1f}% worst={arr1.min():+.1f}% RR={rr_1m:.3f}")

    # 結論
    print("\n" + "=" * 70)
    print("結論")
    print("=" * 70)
    delta = rr_1m - rr_5m
    print(f"  Δ RR = {delta:+.3f}")
    print(f"  Δ mean = {arr1.mean() - arr5.mean():+.2f}pp")
    print(f"  Δ worst = {arr1.min() - arr5.min():+.2f}pp")
    if abs(delta) < 0.05:
        print("\n  ⚠️ 差距 < 0.05 RR → 1m 不顯著提升")
        print("  💡 建議：不必投入 23 hr 抓全市場 1m，5m 已足夠")
    elif delta > 0.05:
        print("\n  ⭐ 1m 顯著提升 → 值得投入抓全市場")
    else:
        print("\n  ❌ 1m 反而變差")


if __name__ == '__main__':
    main()
