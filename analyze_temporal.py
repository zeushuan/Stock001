"""
時序穩健性 + Alpha 衰減分析

#2 Walk-Forward：6 風格 × 4 子期 → 看風報比是否在所有期都穩定
#4 Alpha Decay：6 風格 × 6 年度 → 看 alpha 是否隨時間衰減

每個切片裡只計算「該期間內進場」的交易。
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import data_loader as dl
import variant_strategy as vs


STYLES = [
    ('P0_T1T3', '🚀 進攻'),
    ('P0_T1T3+POS', '⚖️ 平衡'),
    ('P0_T1T3+POS+DXY', '🌊 保守'),
    ('P0_T1T3+POS+IND+DXY', '🛡️ 極致風控'),
    ('P0_T1T3+POS+IND+DXY+WRSI+WADX', '🛟 超低風險'),
    ('P0_T1T3+RL', '🤖 RL'),
]

# Walk-forward 4 期
WF_WINDOWS = [
    ('A', '2020-01-02', '2021-06-30'),  # COVID 反彈
    ('B', '2021-07-01', '2022-12-31'),  # 牛市末段+ 2022 修正
    ('C', '2023-01-01', '2024-06-30'),  # AI 啟動
    ('D', '2024-07-01', '2026-04-25'),  # AI 主升段+成熟期
]

# 年度切片
YEAR_WINDOWS = [
    ('2020', '2020-01-02', '2020-12-31'),
    ('2021', '2021-01-01', '2021-12-31'),
    ('2022', '2022-01-01', '2022-12-31'),
    ('2023', '2023-01-01', '2023-12-31'),
    ('2024', '2024-01-01', '2024-12-31'),
    ('2025+', '2025-01-01', '2026-04-25'),
]


def run_one_window(args):
    ticker, mode, start, end = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        # 用 vs.run_v7_variant 的 start/end
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r['n_trades'] == 0: return None
        return r['pnl_pct']
    except Exception:
        return None


def metrics(returns):
    if not returns: return None
    arr = np.array(returns)
    mean = arr.mean()
    worst = arr.min()
    win = (arr > 0).mean() * 100
    rr = mean / abs(worst) if worst < 0 else 0
    return dict(n=len(arr), mean=mean, worst=worst, win=win, rr=rr)


def run_slice(start, end, mode, tickers):
    args_list = [(t, mode, start, end) for t in tickers]
    rets = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for r in ex.map(run_one_window, args_list, chunksize=80):
            if r is not None:
                rets.append(r)
    return metrics(rets)


def main():
    files = sorted(Path('data_cache').glob('*.parquet'))
    tickers = [f.stem for f in files]
    print(f"載入 {len(tickers)} 檔資料")

    # ── #2 Walk-Forward ──
    print("\n" + "=" * 110)
    print("📊 Walk-Forward 4 期穩健性檢測")
    print("=" * 110)
    print(f"{'風格':22s} " + " ".join(
        f"{label:>20s}" for label in [f'{n}({s[5:7]}-{e[5:7]})' for n, s, e in WF_WINDOWS]
    ))

    wf_results = {}
    for mode, label in STYLES:
        cells = []
        for win_label, start, end in WF_WINDOWS:
            m = run_slice(start, end, mode, tickers)
            wf_results[(mode, win_label)] = m
            if m:
                cells.append(f"{m['mean']:>+5.0f}/{m['worst']:>+5.0f}/{m['rr']:.2f}")
            else:
                cells.append('-')
        print(f"{label:22s} " + " ".join(f"{c:>20s}" for c in cells))

    # 各風格的 RR 標準差（穩健性）
    print("\n各風格 4 期風報比標準差（越小越穩）：")
    for mode, label in STYLES:
        rrs = [wf_results[(mode, w[0])]['rr'] for w in WF_WINDOWS
               if wf_results.get((mode, w[0]))]
        if rrs:
            print(f"  {label:22s}  mean={np.mean(rrs):.3f}  σ={np.std(rrs):.3f}  range={max(rrs)-min(rrs):.2f}")

    # ── #4 Alpha Decay ──
    print("\n" + "=" * 110)
    print("📉 Alpha 時序衰減分析（年度別風報比）")
    print("=" * 110)
    print(f"{'風格':22s} " + " ".join(f"{n:>10s}" for n, _, _ in YEAR_WINDOWS))

    yr_results = {}
    for mode, label in STYLES:
        cells = []
        for yr, start, end in YEAR_WINDOWS:
            m = run_slice(start, end, mode, tickers)
            yr_results[(mode, yr)] = m
            if m:
                cells.append(f"{m['rr']:.2f}")
            else:
                cells.append('-')
        print(f"{label:22s} " + " ".join(f"{c:>10s}" for c in cells))

    # 衰減判定：早期 vs 晚期均值
    print("\n衰減判定（早期 2020-22 平均 vs 晚期 2024-25+ 平均）：")
    for mode, label in STYLES:
        early = [yr_results[(mode, y)]['rr'] for y in ['2020','2021','2022']
                 if yr_results.get((mode, y))]
        late  = [yr_results[(mode, y)]['rr'] for y in ['2024','2025+']
                 if yr_results.get((mode, y))]
        if early and late:
            e_avg = np.mean(early); l_avg = np.mean(late)
            decay = l_avg - e_avg
            sign = '⚠️ 衰減' if decay < -0.1 else ('✓ 穩定' if abs(decay) < 0.1 else '⭐ 改善')
            print(f"  {label:22s}  早期 {e_avg:.2f}  晚期 {l_avg:.2f}  Δ {decay:+.2f}  {sign}")


if __name__ == '__main__':
    main()
