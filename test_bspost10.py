"""BSPOST10 驗證 — EDA 後的細節調整變體
=====================================
EDA 結論：
  ① 危險窗內：T1/T3 RR 反而比 NORMAL 更高 → 不該阻擋
  ② POST10（事件結束+1~+10 BD）：所有訊號 RR 最差（飆股 -0.027 / T4 -0.023）→ 該阻擋
  ③ POST30：T4 RR 0.143（NORMAL 0.084 的 1.7×）→ 加碼
  ④ NORMAL：基準

新變體 BSPOST10：只阻擋 POST10 假反彈期，不再粗暴阻擋整個危險窗
與舊 BSGUARD 並列比較，驗證 EDA 假設

A baseline      : P5_T1T3+POS+IND+DXY+VWAPEXEC（當前最佳）
B + BSGUARD     : 舊變體（已知失敗）
C + BSPOST10 ⭐ : 新變體（基於 EDA 設計）
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
    ('A baseline ⭐ 當前最佳', 'P5_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('B + BSGUARD（舊）',     'P5_T1T3+POS+IND+DXY+VWAPEXEC+BSGUARD'),
    ('C + BSPOST10 🆕',       'P5_T1T3+POS+IND+DXY+VWAPEXEC+BSPOST10'),
]
WINDOWS = [
    ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
    ('TRAIN (2020.1-2024.5)', '2020-01-02', '2024-05-31'),
    ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
]


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, None)
        return (label, (r['pnl_pct'], r['n_trades'], r.get('win_rate', 0)))
    except Exception:
        return (label, None)


def metrics(returns):
    if not returns: return None
    pnls = [x[0] for x in returns if x is not None]
    arr = np.array(pnls)
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0: return None
    return {
        'n': len(arr), 'mean': arr.mean(), 'median': np.median(arr),
        'win': (arr > 0).mean() * 100, 'worst': arr.min(),
        'best': arr.max(),
        'rr': arr.mean()/abs(arr.min()) if arr.min() < 0 else 0,
    }


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    VWAP = Path('vwap_cache')
    vwap_set = set(p.stem for p in VWAP.glob('*.parquet'))
    universe = [t for t in universe if t in vwap_set]
    print(f"全市場 4 位數 ∩ vwap_cache: {len(universe)} 檔\n")

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
        for label, ret in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                bucket.setdefault(label, []).append(ret)
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                print(f"  {pct:.0f}%", flush=True)

    print(f"\n完成 {time.time()-t0:.1f}s\n")

    print("=" * 100)
    print("📊 BSPOST10 vs BSGUARD vs baseline")
    print("=" * 100)

    for var_name, _ in VARIANTS:
        print(f"\n【{var_name}】")
        print(f"{'Period':<26} {'n':>5} {'勝率%':>7} {'均報%':>9} "
              f"{'中位%':>8} {'最差%':>8} {'最佳%':>10} {'RR':>7}")
        print("-" * 100)
        for win_name, _, _ in WINDOWS:
            m = metrics(bucket.get((var_name, win_name), []))
            if m:
                print(f"{win_name:<26} {m['n']:>5} {m['win']:>+7.1f} "
                      f"{m['mean']:>+9.1f} {m['median']:>+8.1f} "
                      f"{m['worst']:>+8.1f} {m['best']:>+10.1f} "
                      f"{m['rr']:>7.3f}")

    # 對比表
    print("\n" + "=" * 100)
    print("🎯 三變體 RR 對比（Δ vs A baseline）")
    print("=" * 100)
    print(f"{'Period':<26} {'A 基準':>10} {'B BSGUARD':>11}  Δ_B "
          f"{'C BSPOST10':>12}  Δ_C")
    print("-" * 100)
    for win_name, _, _ in WINDOWS:
        a = metrics(bucket.get(('A baseline ⭐ 當前最佳', win_name), []))
        b = metrics(bucket.get(('B + BSGUARD（舊）', win_name), []))
        c = metrics(bucket.get(('C + BSPOST10 🆕', win_name), []))
        if a and b and c:
            db = b['rr'] - a['rr']
            dc = c['rr'] - a['rr']
            print(f"{win_name:<26} {a['rr']:>10.3f} {b['rr']:>11.3f}  "
                  f"{db:>+5.3f} {c['rr']:>12.3f}  {dc:>+5.3f}")

    # 結論
    print("\n" + "=" * 100)
    a_test = metrics(bucket.get(('A baseline ⭐ 當前最佳', 'TEST  (2024.6-2026.4)'), []))
    c_test = metrics(bucket.get(('C + BSPOST10 🆕', 'TEST  (2024.6-2026.4)'), []))
    if a_test and c_test:
        delta = c_test['rr'] - a_test['rr']
        if delta > 0.05:
            print(f"  ✅ BSPOST10 TEST Δ RR {delta:+.3f} → EDA 假設驗證！採用")
        elif delta > 0:
            print(f"  ⚠️ BSPOST10 TEST Δ RR {delta:+.3f} → 微弱正向，邊際效益有限")
        else:
            print(f"  ❌ BSPOST10 TEST Δ RR {delta:+.3f} → EDA 假設不成立或實作有誤")


if __name__ == '__main__':
    main()
