"""
Walk-Forward 7:3 訓練/測試拆分

目的：揭示 v8 是否有過擬合（學者式驗證）

切分：
  Train: 2020-01-02 ~ 2024-05-31 (~70%)
  Test:  2024-06-01 ~ 2026-04-25 (~30%)

判定：
  - Test RR > Train RR × 0.7 → 真實 alpha
  - Test RR < Train RR × 0.5 → 過擬合
  - 中間 → 部分衰減（正常）
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
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

WINDOWS = [
    ('FULL',  '2020-01-02', '2026-04-25'),
    ('TRAIN', '2020-01-02', '2024-05-31'),  # 70%
    ('TEST',  '2024-06-01', '2026-04-25'),  # 30%
]


def run_one(args):
    ticker, mode, start, end = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
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
    args = [(t, mode, start, end) for t in tickers]
    rets = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for r in ex.map(run_one, args, chunksize=80):
            if r is not None: rets.append(r)
    return metrics(rets)


def main():
    files = sorted(Path('data_cache').glob('*.parquet'))
    tickers = [f.stem for f in files]
    print(f"載入 {len(tickers)} 檔資料\n")

    print("=" * 110)
    print("📊 Walk-Forward 7:3 訓練/測試拆分（過擬合檢測）")
    print("=" * 110)
    print(f"{'風格':18s} {'FULL 6yr':>15s} {'TRAIN 4.4yr':>15s} {'TEST 1.9yr':>15s} {'Test/Train':>12s} {'判定':>10s}")

    results = []
    for mode, label in STYLES:
        m_full  = run_slice('2020-01-02', '2026-04-25', mode, tickers)
        m_train = run_slice('2020-01-02', '2024-05-31', mode, tickers)
        m_test  = run_slice('2024-06-01', '2026-04-25', mode, tickers)

        if not (m_full and m_train and m_test): continue

        ratio = m_test['rr'] / m_train['rr'] if m_train['rr'] > 0 else 0
        if ratio >= 0.7:
            verdict = '✓ 真實'
        elif ratio >= 0.5:
            verdict = '⚠ 部分'
        else:
            verdict = '❌ 過擬合'

        cells = [
            f"{m_full['mean']:>+5.0f}/{m_full['worst']:>+5.0f}/{m_full['rr']:.2f}",
            f"{m_train['mean']:>+5.0f}/{m_train['worst']:>+5.0f}/{m_train['rr']:.2f}",
            f"{m_test['mean']:>+5.0f}/{m_test['worst']:>+5.0f}/{m_test['rr']:.2f}",
            f"{ratio:.2f}",
        ]
        print(f"{label:18s} {cells[0]:>15s} {cells[1]:>15s} {cells[2]:>15s} {cells[3]:>12s} {verdict:>10s}")
        results.append((label, m_full, m_train, m_test, ratio, verdict))

    print("\n" + "=" * 110)
    print("📊 細節：Train vs Test 風報比 衰減幅度")
    print("=" * 110)
    print(f"{'風格':18s} {'Train RR':>10s} {'Test RR':>10s} {'絕對 Δ':>10s} {'相對 %':>10s}")
    for label, _, m_train, m_test, ratio, verdict in results:
        d_abs = m_test['rr'] - m_train['rr']
        d_rel = (m_test['rr'] - m_train['rr']) / m_train['rr'] * 100 if m_train['rr'] > 0 else 0
        print(f"{label:18s} {m_train['rr']:>9.2f} {m_test['rr']:>9.2f} {d_abs:>+9.2f} {d_rel:>+9.1f}%")


if __name__ == '__main__':
    main()
