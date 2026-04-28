"""T3 + C1 (close > EMA20) + C3 (EMA20 上升) 對比
================================================
之前發現：
  C1 alone (close > EMA20)：RR 0.396 (樣本 21，太少)
  C3 alone (EMA20 5d 斜率>0)：RR 0.232 (樣本 1432)

本次測試組合：
  C1 + C3 (AND)：兩條件都滿足（最嚴格）
  C1 + C3 (OR) ：任一滿足（最寬鬆）
  另：對比 EMA5 組合
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
import ta
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

TOP200 = sorted([t for t, info in
                 json.load(open('vwap_applicable.json', encoding='utf-8')).items()
                 if info.get('tier') == 'TOP'])

HOLD = 30


def analyze_one(ticker):
    df = dl.load_from_cache(ticker)
    if df is None or len(df) < 280: return None
    test_df = df[df.index >= '2024-06-01'].copy()
    if len(test_df) < 50: return None

    test_df['e5'] = ta.trend.ema_indicator(test_df['Close'], window=5)
    test_df['e5_slope'] = test_df['e5'].diff(5)
    test_df['e20_slope'] = test_df['e20'].diff(5)

    e5 = test_df['e5'].values
    e20 = test_df['e20'].values
    e60 = test_df['e60'].values
    e5_slope = test_df['e5_slope'].values
    e20_slope = test_df['e20_slope'].values
    rsi = test_df['rsi'].values
    adx = test_df['adx'].values
    close = test_df['Close'].values
    n = len(test_df)

    results = {
        'T3 baseline': [],
        'C1 close>EMA20': [],
        'C3 EMA20 rising': [],
        'C1 AND C3': [],
        'C1 OR C3': [],
        '+EMA5+EMA20 both rising (前次最佳)': [],
        'C1 AND C3 AND EMA5 rising': [],
    }

    for i in range(2, n - HOLD):
        if any(np.isnan(x) for x in [e5[i], e20[i], e60[i], rsi[i], adx[i]]): continue
        is_bull = e20[i] > e60[i]
        if not is_bull: continue
        if adx[i] < 22: continue
        if not (35 <= rsi[i] < 50): continue

        entry = close[i]
        ret = (close[i + HOLD] - entry) / entry * 100
        results['T3 baseline'].append(ret)

        c1 = close[i] > e20[i]
        c3 = not np.isnan(e20_slope[i]) and e20_slope[i] > 0
        e5_up = not np.isnan(e5_slope[i]) and e5_slope[i] > 0

        if c1: results['C1 close>EMA20'].append(ret)
        if c3: results['C3 EMA20 rising'].append(ret)
        if c1 and c3: results['C1 AND C3'].append(ret)
        if c1 or c3:  results['C1 OR C3'].append(ret)
        if c3 and e5_up: results['+EMA5+EMA20 both rising (前次最佳)'].append(ret)
        if c1 and c3 and e5_up: results['C1 AND C3 AND EMA5 rising'].append(ret)

    return results


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    return {'n': len(a), 'mean': a.mean(),
            'win': (a > 0).mean() * 100, 'worst': a.min(),
            'best': a.max(),
            'rr': a.mean()/abs(a.min()) if a.min() < 0 else 0}


def main():
    print(f"分析 TOP 200 — T3 + C1/C3 組合對比...\n")
    with ProcessPoolExecutor(max_workers=12) as ex:
        all_r = [r for r in ex.map(analyze_one, TOP200) if r is not None]
    print(f"成功 {len(all_r)}/{len(TOP200)}\n")

    print("=" * 110)
    print(f"T3 + C1 / C3 組合（30 天持有）")
    print("=" * 110)
    print(f"{'情境':<42} {'樣本':>6} {'勝率%':>8} {'均報%':>9} {'最差%':>9} "
          f"{'最佳%':>9} {'RR':>7}  Δ")
    print("-" * 110)

    keys = ['T3 baseline',
            'C1 close>EMA20',
            'C3 EMA20 rising',
            'C1 AND C3',
            'C1 OR C3',
            '+EMA5+EMA20 both rising (前次最佳)',
            'C1 AND C3 AND EMA5 rising']

    base_m = None
    rows = []
    for key in keys:
        data = []
        for r in all_r: data.extend(r[key])
        m = metrics(data)
        if m:
            if key == 'T3 baseline': base_m = m
            rows.append((key, m))

    for key, m in rows:
        if base_m and key != 'T3 baseline':
            d = m['rr'] - base_m['rr']
            d_str = f"RR {d:+.3f}"
            mark = '⭐⭐' if d > 0.1 else ('⭐' if d > 0.03 else '')
        else:
            d_str = ''; mark = ''
        print(f"{key:<42} {m['n']:>6} {m['win']:>+8.1f} {m['mean']:>+9.2f} "
              f"{m['worst']:>+9.1f} {m['best']:>+9.1f} {m['rr']:>7.3f}  {d_str} {mark}")

    print("\n" + "=" * 110)
    print("RR 排名")
    print("=" * 110)
    rows.sort(key=lambda x: -x[1]['rr'])
    for i, (key, m) in enumerate(rows, 1):
        print(f"  {i}. {key:<45} RR {m['rr']:.3f}  win {m['win']:.1f}%  n={m['n']}")

    # 解讀
    print("\n" + "=" * 110)
    print("實務解讀（30 天）")
    print("=" * 110)
    base = next((m for k, m in rows if k == 'T3 baseline'), None)
    c1 = next((m for k, m in rows if k == 'C1 close>EMA20'), None)
    c3 = next((m for k, m in rows if k == 'C3 EMA20 rising'), None)
    c1_c3 = next((m for k, m in rows if k == 'C1 AND C3'), None)
    if all([base, c1, c3, c1_c3]):
        print(f"\n  T3 baseline (RSI 35-50)         : {base['n']:>5} 個樣本，勝率 {base['win']:.1f}%, RR {base['rr']:.3f}")
        print(f"  + close > EMA20 (C1)            : {c1['n']:>5} 個 ({c1['n']/base['n']*100:.0f}%), 勝率 {c1['win']:.1f}%, RR {c1['rr']:.3f}")
        print(f"  + EMA20 上升 (C3)               : {c3['n']:>5} 個 ({c3['n']/base['n']*100:.0f}%), 勝率 {c3['win']:.1f}%, RR {c3['rr']:.3f}")
        print(f"  + C1 AND C3                    : {c1_c3['n']:>5} 個 ({c1_c3['n']/base['n']*100:.0f}%), 勝率 {c1_c3['win']:.1f}%, RR {c1_c3['rr']:.3f}")


if __name__ == '__main__':
    main()
