"""T1 / T3 / T4 進場勝率與績效比較
=====================================
T1：多頭 + ADX≥22 + 黃金交叉 ≤ 10 天
T3：多頭 + ADX≥22 + RSI 35-50（拉回）
T4：空頭 + RSI < 32 + 連續上升（反彈）
"""
import sys, json
from pathlib import Path
import numpy as np
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

TOP200 = sorted([t for t, info in
                 json.load(open('vwap_applicable.json', encoding='utf-8')).items()
                 if info.get('tier') == 'TOP'])

HOLD_DAYS = [10, 20, 30, 60]


def analyze_one(ticker):
    df = dl.load_from_cache(ticker)
    if df is None or len(df) < 280: return None
    test_df = df[df.index >= '2024-06-01']
    if len(test_df) < 30: return None

    e20 = test_df['e20'].values
    e60 = test_df['e60'].values
    rsi = test_df['rsi'].values
    adx = test_df['adx'].values
    close = test_df['Close'].values
    n = len(test_df)

    # 計算每天「黃金交叉幾天前」
    cross_days = np.full(n, np.nan)
    for i in range(1, n):
        if not np.isnan(e20[i]) and not np.isnan(e60[i]):
            cur_bull = e20[i] > e60[i]
            for k in range(1, min(60, i)):
                if np.isnan(e20[i-k]) or np.isnan(e60[i-k]): continue
                prev_bull = e20[i-k] > e60[i-k]
                if prev_bull != cur_bull:
                    cross_days[i] = k if cur_bull else -k
                    break

    t1 = {h: [] for h in HOLD_DAYS}
    t3 = {h: [] for h in HOLD_DAYS}
    t4 = {h: [] for h in HOLD_DAYS}

    for i in range(n - max(HOLD_DAYS)):
        if any(np.isnan(x) for x in [e20[i], e60[i], rsi[i], adx[i]]): continue
        is_bull = e20[i] > e60[i]
        entry_price = close[i]

        # T1：多頭 + ADX≥22 + cross_days 1-10
        if is_bull and adx[i] >= 22:
            cd = cross_days[i]
            if not np.isnan(cd) and 0 < cd <= 10:
                for h in HOLD_DAYS:
                    if i + h < n:
                        ret = (close[i+h] - entry_price) / entry_price * 100
                        t1[h].append(ret)

        # T3：多頭 + ADX≥22 + RSI 35-50
        if is_bull and adx[i] >= 22:
            if 35 <= rsi[i] < 50:
                for h in HOLD_DAYS:
                    if i + h < n:
                        ret = (close[i+h] - entry_price) / entry_price * 100
                        t3[h].append(ret)

        # T4：空頭 + RSI < 32 + 連續 2 天上升
        if not is_bull and rsi[i] < 32 and i >= 2:
            if rsi[i] > rsi[i-1] > rsi[i-2]:
                for h in HOLD_DAYS:
                    if i + h < n:
                        ret = (close[i+h] - entry_price) / entry_price * 100
                        t4[h].append(ret)

    return {'t1': t1, 't3': t3, 't4': t4}


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    return {'n': len(a), 'mean': a.mean(), 'median': np.median(a),
            'win': (a > 0).mean() * 100, 'worst': a.min(),
            'best': a.max(), 'std': a.std(),
            'rr': a.mean()/abs(a.min()) if a.min() < 0 else 0}


def main():
    print(f"分析 TOP 200 在 TEST 期 (2024.6-2026.4) T1/T3/T4 進場勝率...\n")
    with ProcessPoolExecutor(max_workers=12) as ex:
        all_r = [r for r in ex.map(analyze_one, TOP200) if r is not None]
    print(f"成功 {len(all_r)}/{len(TOP200)}\n")

    print("=" * 90)
    print("T1 / T3 / T4 進場勝率與績效（TEST 期）")
    print("=" * 90)
    print(f"{'信號':<6} {'持有':<6} {'樣本':>7} {'勝率%':>8} {'均值%':>9} "
          f"{'中位%':>9} {'最差%':>9} {'最佳%':>9} {'σ':>7} {'RR':>7}")
    print("-" * 90)

    for sig in ['t1', 't3', 't4']:
        for h in HOLD_DAYS:
            data = []
            for r in all_r:
                data.extend(r[sig][h])
            m = metrics(data)
            if m:
                label = sig.upper()
                print(f"{label:<6} {f'{h} 天':<6} {m['n']:>7} "
                      f"{m['win']:>+8.1f} {m['mean']:>+9.2f} {m['median']:>+9.2f} "
                      f"{m['worst']:>+9.1f} {m['best']:>+9.1f} "
                      f"{m['std']:>7.1f} {m['rr']:>7.3f}")
        print()

    # 30 天重點對比
    print("=" * 90)
    print("📌 30 天重點對比")
    print("=" * 90)
    h = 30
    rows = []
    for sig in ['t1', 't3', 't4']:
        data = [x for r in all_r for x in r[sig][h]]
        m = metrics(data)
        if m: rows.append((sig.upper(), m))

    print(f"{'信號':<6} {'樣本':>7} {'勝率%':>8} {'均值%':>9} {'最差%':>9} "
          f"{'RR':>7}  特性")
    print("-" * 90)
    char_map = {
        'T1': '黃金交叉新鮮（趨勢起點）',
        'T3': '多頭拉回（最常用）',
        'T4': '空頭反彈（罕見但可獲利）',
    }
    for sig, m in rows:
        print(f"{sig:<6} {m['n']:>7} {m['win']:>+8.1f} {m['mean']:>+9.2f} "
              f"{m['worst']:>+9.1f} {m['rr']:>7.3f}  {char_map[sig]}")


if __name__ == '__main__':
    main()
