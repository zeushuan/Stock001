"""C5: 台股 Walk-Forward 跨年度 CV 驗證
=========================================
美股已驗證年度 RR 穩定性（2020-2026），台股還缺。
驗證 P5+VWAPEXEC 是否每年都能跑出 RR > 0。

每年 1/2 ~ 12/31 一個獨立窗口，跑 P5_T1T3+POS+IND+DXY+VWAPEXEC
"""
import sys, time, json
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs

WORKERS = 16
TW_MODE = 'P5_T1T3+POS+IND+DXY+VWAPEXEC'


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


def buy_hold(ticker, start, end):
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        s = pd.Timestamp(start).tz_localize(None)
        e = pd.Timestamp(end).tz_localize(None)
        idx = df.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx = idx.tz_localize(None)
        sub = df[(idx >= s) & (idx <= e)]
        if len(sub) < 2: return None
        return (sub['Close'].iloc[-1] - sub['Close'].iloc[0]) / sub['Close'].iloc[0] * 100
    except: return None


def metrics(arr):
    if len(arr) == 0: return None
    a = np.array(arr)
    a = a[~np.isnan(a)]
    if len(a) == 0: return None
    return {
        'n': len(a), 'mean': float(a.mean()), 'median': float(np.median(a)),
        'win': float((a > 0).mean() * 100), 'worst': float(a.min()),
        'rr': float(a.mean() / abs(a.min())) if a.min() < 0 else 0.0,
    }


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = [t for t in universe if t in vwap_set]
    print(f"🇹🇼 台股 universe (∩ vwap): {len(universe)} 檔\n")

    annual = [(f'{y}', f'{y}-01-02', f'{y}-12-31') for y in range(2020, 2027)]
    print(f"年度窗口 {len(annual)} 個\n")

    tasks = []
    for yr, s, e in annual:
        for t in universe:
            tasks.append((t, TW_MODE, s, e, yr))

    t0 = time.time()
    yearly = {}
    n_done = 0
    milestone = max(1, len(tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one, tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                yearly.setdefault(label, []).append(ret)
            if n_done % milestone == 0:
                print(f"  {n_done/len(tasks)*100:.0f}%", flush=True)
    print(f"完成 {time.time()-t0:.1f}s\n")

    print("=" * 110)
    print("📊 台股 v8 P5+VWAPEXEC Walk-Forward 跨年度（vs TWII）")
    print("=" * 110)
    print(f"{'年度':<8} {'n':>5} {'勝率%':>7} {'均報%':>9} {'中位%':>8} "
          f"{'最差%':>9} {'RR':>7}  {'TWII%':>8}  v8 vs TWII")
    print("-" * 110)
    summary = []
    rrs = []
    for yr, s, e in annual:
        m = metrics(yearly.get(yr, []))
        twii_r = buy_hold('^TWII', s, e)
        if not m: continue
        days = (pd.Timestamp(e) - pd.Timestamp(s)).days
        twii_str = f"{twii_r:>+6.1f}" if twii_r is not None else "—"
        delta = (m['mean'] - twii_r) if twii_r is not None else 0
        flag = '⭐ 勝' if delta > 0 else '✗ 輸' if twii_r is not None else ''
        print(f"{yr:<8} {m['n']:>5} {m['win']:>+7.1f} {m['mean']:>+9.1f} "
              f"{m['median']:>+8.1f} {m['worst']:>+9.1f} {m['rr']:>7.3f}  "
              f"{twii_str}  {delta:+6.1f}pp {flag}")
        summary.append({'year': yr, 'metrics': m, 'twii': twii_r,
                        'delta': delta})
        rrs.append(m['rr'])

    print("\n" + "=" * 110)
    print("📈 RR 跨年度穩定性")
    print("=" * 110)
    rr_arr = np.array(rrs)
    print(f"  RR 均值: {rr_arr.mean():.3f}")
    print(f"  RR σ:    {rr_arr.std():.3f}")
    print(f"  RR 最低: {rr_arr.min():.3f}")
    print(f"  RR 最高: {rr_arr.max():.3f}")
    print(f"  RR > 0 年數: {(rr_arr > 0).sum()} / {len(rr_arr)}")
    print(f"  RR > 0.3 年數: {(rr_arr > 0.3).sum()} / {len(rr_arr)}")

    out = {
        'mode': TW_MODE,
        'annual_summary': summary,
        'rr_stats': {
            'mean': float(rr_arr.mean()),
            'std': float(rr_arr.std()),
            'min': float(rr_arr.min()),
            'max': float(rr_arr.max()),
            'positive_years': int((rr_arr > 0).sum()),
            'total_years': len(rr_arr),
        }
    }
    with open('walkforward_tw_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 walkforward_tw_results.json")


if __name__ == '__main__':
    main()
