"""美股 v8 vs SPY buy-and-hold 對照
======================================
驗證 v8 美股組合是否系統性輸給最簡單的 SPY 持有

對比方式：
  1. SPY pure buy-and-hold（每段期間從首日到尾日報酬）
  2. v8 等權組合：所有有訊號的股票等權平均年化報酬
     （≈ us_full_results.json 各 ticker mean of pnl_pct → 年化）
  3. v8 高流動 tier 等權組合
  4. v8 baseline 報酬 vs 各種變體

若 v8 < SPY，研究結論很清楚：v8 不該用於美股
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WINDOWS = [
    ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
    ('TRAIN (2020.1-2024.5)', '2020-01-02', '2024-05-31'),
    ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
]


def buy_hold_return(ticker, start, end):
    """純持有報酬 %（首日買、尾日賣）"""
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        s = pd.Timestamp(start).tz_localize(None)
        e = pd.Timestamp(end).tz_localize(None)
        idx = df.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx = idx.tz_localize(None)
        mask = (idx >= s) & (idx <= e)
        sub = df.iloc[mask]
        if len(sub) < 2: return None
        return (sub['Close'].iloc[-1] - sub['Close'].iloc[0]) / sub['Close'].iloc[0] * 100
    except Exception:
        return None


def annualized(total_pct, days):
    """總報酬轉年化"""
    if days <= 0: return 0
    return ((1 + total_pct / 100) ** (365 / days) - 1) * 100


def main():
    print("=" * 100)
    print("🇺🇸 v8 vs SPY 對照（驗證 v8 在美股是否能贏大盤）")
    print("=" * 100)

    # SPY buy-hold
    print("\n📊 SPY buy-and-hold（純持有大盤）")
    print("-" * 100)
    print(f"{'Period':<26} {'起始':<11} {'結束':<11} {'總報酬%':>9} {'年化%':>8}")
    print("-" * 100)
    spy_rets = {}
    for win_name, start, end in WINDOWS:
        r = buy_hold_return('SPY', start, end)
        if r is None:
            print(f"  {win_name}: SPY 抓取失敗")
            continue
        days = (pd.Timestamp(end) - pd.Timestamp(start)).days
        ann = annualized(r, days)
        spy_rets[win_name] = (r, ann)
        print(f"{win_name:<26} {start:<11} {end:<11} {r:>+9.1f} {ann:>+8.1f}")

    # 同時抓 QQQ / IWM 作參考
    print("\n📊 QQQ / IWM 對照（NASDAQ-100 / Russell-2000）")
    print("-" * 100)
    for bench in ['QQQ', 'IWM']:
        for win_name, start, end in WINDOWS:
            r = buy_hold_return(bench, start, end)
            if r is None: continue
            days = (pd.Timestamp(end) - pd.Timestamp(start)).days
            ann = annualized(r, days)
            print(f"{bench} {win_name:<22} {r:>+9.1f}% / 年化 {ann:>+8.1f}%")
        print()

    # v8 等權組合 — 從 us_full_results.json 讀
    print("=" * 100)
    print("📊 v8 P5+POS 等權組合報酬")
    print("=" * 100)

    res_path = Path('us_full_results.json')
    if not res_path.exists():
        print("us_full_results.json 不存在 — 請先跑 test_us_current_best_full.py")
        return

    res = json.loads(res_path.read_text(encoding='utf-8'))
    pt = res['per_ticker']

    # 從 ADV 重新計算高流動清單
    DATA = Path('data_cache')
    full_tickers = set(json.loads(
        Path('us_full_tickers.json').read_text(encoding='utf-8'))['tickers'])
    high_liquid = set()
    for t in full_tickers:
        if not (DATA / f'{t}.parquet').exists(): continue
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
            if adv >= 104_000_000: high_liquid.add(t)
        except: pass

    print(f"\n{'變體 / 池':<42} {'Period':<26} "
          f"{'平均%':>10} {'中位%':>10} {'年化%':>10}  vs SPY")
    print("-" * 100)

    for var in ['A baseline (P0)', 'B +POS', 'C P5+POS ⭐ 美股最佳']:
        for win_name, start, end in WINDOWS:
            key = f'{var}|{win_name}'
            tk_rets = pt.get(key, {})
            if not tk_rets: continue
            # 全市場
            arr = np.array([float(v) for v in tk_rets.values()])
            arr = arr[~np.isnan(arr)]
            if len(arr) == 0: continue
            mean = arr.mean()
            median = np.median(arr)
            days = (pd.Timestamp(end) - pd.Timestamp(start)).days
            ann_mean = annualized(mean, days)
            spy_ann = spy_rets.get(win_name, (0, 0))[1]
            tag = '⭐ 勝 SPY' if ann_mean > spy_ann else '✗ 輸 SPY'
            print(f"{var + ' / 全市場':<42} {win_name:<26} "
                  f"{mean:>+10.1f} {median:>+10.1f} {ann_mean:>+10.1f}  {tag}")
            # 高流動
            arr_hl = np.array([float(v) for tk, v in tk_rets.items()
                               if tk in high_liquid])
            arr_hl = arr_hl[~np.isnan(arr_hl)]
            if len(arr_hl) > 0:
                mean_hl = arr_hl.mean()
                median_hl = np.median(arr_hl)
                ann_hl = annualized(mean_hl, days)
                tag_hl = '⭐ 勝 SPY' if ann_hl > spy_ann else '✗ 輸 SPY'
                print(f"{var + ' / 高流動':<42} {win_name:<26} "
                      f"{mean_hl:>+10.1f} {median_hl:>+10.1f} "
                      f"{ann_hl:>+10.1f}  {tag_hl}")
        print()

    # 結論
    print("=" * 100)
    print("💡 結論")
    print("=" * 100)
    spy_test = spy_rets.get('TEST  (2024.6-2026.4)')
    if spy_test:
        print(f"\nSPY TEST 22 月: 總 {spy_test[0]:+.1f}% / 年化 {spy_test[1]:+.1f}%")
        print(f"v8 TEST 22 月（資料表）：")
        # C P5+POS TEST
        c_test_rets = pt.get('C P5+POS ⭐ 美股最佳|TEST  (2024.6-2026.4)', {})
        if c_test_rets:
            arr_all = np.array([float(v) for v in c_test_rets.values()])
            arr_all = arr_all[~np.isnan(arr_all)]
            mean_all = arr_all.mean()
            arr_hl = np.array([float(v) for tk, v in c_test_rets.items()
                               if tk in high_liquid])
            arr_hl = arr_hl[~np.isnan(arr_hl)]
            print(f"  全市場等權: {mean_all:+.1f}% (vs SPY {spy_test[0]:+.1f}% "
                  f"→ Δ {mean_all - spy_test[0]:+.1f}pp)")
            if len(arr_hl) > 0:
                mean_hl = arr_hl.mean()
                print(f"  高流動等權: {mean_hl:+.1f}% (vs SPY {spy_test[0]:+.1f}% "
                      f"→ Δ {mean_hl - spy_test[0]:+.1f}pp)")


if __name__ == '__main__':
    main()
