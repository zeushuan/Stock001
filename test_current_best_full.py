"""目前最新進場條件的整體操作績效（非 30 天固定持有）
========================================================
最新最佳 = P5+VWAPEXEC (v9.7 已確認)
策略：v8 + 加碼門檻 5% + VWAP 限價執行
進場：T1 黃金交叉 / T3 拉回 / T4 反彈
出場：RSI>70 / EMA 死叉 / ATR×2.5 停損

跑全台股 2020-至今三段（FULL/TRAIN/TEST）。
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
    ('A baseline (P0)',         'P0_T1T3+POS+IND+DXY'),
    ('B P0+VWAPEXEC',           'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('C P5+VWAPEXEC ⭐ 目前最佳', 'P5_T1T3+POS+IND+DXY+VWAPEXEC'),
]
WINDOWS = [
    ('FULL  (2020.1-2026.4)',  '2020-01-02', '2026-04-25'),
    ('TRAIN (2020.1-2024.5)',  '2020-01-02', '2024-05-31'),
    ('TEST  (2024.6-2026.4)',  '2024-06-01', '2026-04-25'),
]


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, None)
        # 同時回傳 PnL 與交易筆數
        return (label, (r['pnl_pct'], r['n_trades'],
                        r.get('win_rate', 0), r.get('pnl_pct_net', r['pnl_pct'])))
    except Exception:
        return (label, None)


def metrics(returns):
    if not returns: return None
    pnls = [x[0] for x in returns if x is not None]
    trades = [x[1] for x in returns if x is not None]
    wins = [x[2] for x in returns if x is not None]
    nets = [x[3] for x in returns if x is not None]
    arr = np.array(pnls)
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0: return None
    return {
        'n': len(arr),
        'mean': arr.mean(),
        'median': np.median(arr),
        'win': (arr > 0).mean() * 100,
        'worst': arr.min(),
        'best': arr.max(),
        'std': arr.std(),
        'rr': arr.mean()/abs(arr.min()) if arr.min() < 0 else 0,
        'avg_trades': np.mean(trades) if trades else 0,
        'avg_win_rate': np.mean(wins) if wins else 0,
        'mean_net': np.mean(nets) if nets else 0,
    }


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    # 加 vwap_cache 交集（VWAPEXEC 必要）
    VWAP = Path('vwap_cache')
    vwap_set = set(p.stem for p in VWAP.glob('*.parquet'))
    universe_with_vwap = [t for t in universe if t in vwap_set]

    print(f"全市場 4 位數股票：{len(universe)} 檔")
    print(f"有 vwap_cache 可跑 VWAPEXEC：{len(universe_with_vwap)} 檔")
    print()

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            uni = universe_with_vwap if 'VWAPEXEC' in mode else universe
            for t in uni:
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
    print("全市場 v8 整體操作績效（按實際進出場規則回測）")
    print("=" * 100)

    for var_name, _ in VARIANTS:
        print(f"\n【{var_name}】")
        print(f"{'Period':<26} {'n':>5} {'勝率%':>7} {'均報%':>9} "
              f"{'中位%':>8} {'最差%':>8} {'最佳%':>10} {'σ':>7} {'RR':>7}")
        print("-" * 100)
        for win_name, _, _ in WINDOWS:
            m = metrics(bucket.get((var_name, win_name), []))
            if m:
                print(f"{win_name:<26} {m['n']:>5} {m['win']:>+7.1f} "
                      f"{m['mean']:>+9.1f} {m['median']:>+8.1f} "
                      f"{m['worst']:>+8.1f} {m['best']:>+10.1f} "
                      f"{m['std']:>7.0f} {m['rr']:>7.3f}")

    # 對比表
    print("\n" + "=" * 100)
    print("📊 三變體 RR 對比")
    print("=" * 100)
    print(f"{'Period':<26} {'A (baseline)':>15} {'B (P0+VWAP)':>15} {'C (P5+VWAP)⭐':>15}  Δ(C-A)")
    print("-" * 100)
    for win_name, _, _ in WINDOWS:
        a = metrics(bucket.get(('A baseline (P0)', win_name), []))
        b = metrics(bucket.get(('B P0+VWAPEXEC', win_name), []))
        c = metrics(bucket.get(('C P5+VWAPEXEC ⭐ 目前最佳', win_name), []))
        if a and b and c:
            d = c['rr'] - a['rr']
            print(f"{win_name:<26} {a['rr']:>15.3f} {b['rr']:>15.3f} "
                  f"{c['rr']:>15.3f}  {d:>+6.3f}")


if __name__ == '__main__':
    main()
