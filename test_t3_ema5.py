"""T3 進場 + EMA5 / EMA20 / 多重組合輔助
==========================================
比較不同 EMA 組合的 T3 過濾效果：
  baseline T3        多頭+ADX≥22+RSI<50
  +E5_above          close > EMA5（最敏感）
  +E5_rising         EMA5 5 日斜率為正
  +E5_above_E20      EMA5 > EMA20（短中均線多頭排列）
  +E20_rising        EMA20 5 日斜率為正
  +E5+E20_both       EMA5 上升 AND EMA20 上升
  +E5_above_AND_E20  close > EMA5 AND EMA20 上升
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

    # 計算 EMA5（data_cache 沒有，現算）
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
        '+close>EMA5': [],
        '+EMA5 rising': [],
        '+EMA5>EMA20': [],
        '+EMA20 rising': [],
        '+EMA5+EMA20 both rising': [],
        '+close>EMA5 AND EMA20 rising': [],
        '+close>EMA5 AND EMA5>EMA20': [],
    }

    for i in range(2, n - HOLD):
        if any(np.isnan(x) for x in [e5[i], e20[i], e60[i], rsi[i], adx[i]]): continue
        is_bull = e20[i] > e60[i]
        if not is_bull: continue
        if adx[i] < 22: continue
        if not (35 <= rsi[i] < 50): continue

        entry_price = close[i]
        ret = (close[i + HOLD] - entry_price) / entry_price * 100
        results['T3 baseline'].append(ret)

        c_above_e5 = close[i] > e5[i]
        e5_up = not np.isnan(e5_slope[i]) and e5_slope[i] > 0
        e5_gt_e20 = e5[i] > e20[i]
        e20_up = not np.isnan(e20_slope[i]) and e20_slope[i] > 0

        if c_above_e5:
            results['+close>EMA5'].append(ret)
        if e5_up:
            results['+EMA5 rising'].append(ret)
        if e5_gt_e20:
            results['+EMA5>EMA20'].append(ret)
        if e20_up:
            results['+EMA20 rising'].append(ret)
        if e5_up and e20_up:
            results['+EMA5+EMA20 both rising'].append(ret)
        if c_above_e5 and e20_up:
            results['+close>EMA5 AND EMA20 rising'].append(ret)
        if c_above_e5 and e5_gt_e20:
            results['+close>EMA5 AND EMA5>EMA20'].append(ret)

    return results


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    return {'n': len(a), 'mean': a.mean(),
            'win': (a > 0).mean() * 100, 'worst': a.min(),
            'rr': a.mean()/abs(a.min()) if a.min() < 0 else 0}


def main():
    print(f"分析 TOP 200 在 TEST 期 — T3 + EMA5/EMA20 組合確認...\n")
    with ProcessPoolExecutor(max_workers=12) as ex:
        all_r = [r for r in ex.map(analyze_one, TOP200) if r is not None]
    print(f"成功 {len(all_r)}/{len(TOP200)}\n")

    print("=" * 105)
    print(f"T3 + EMA 確認指標（30 天持有）")
    print("=" * 105)
    print(f"{'情境':<37} {'樣本':>6} {'勝率%':>8} {'均報%':>9} {'最差%':>9} "
          f"{'RR':>7}  Δ vs baseline")
    print("-" * 105)

    keys = ['T3 baseline',
            '+close>EMA5',
            '+EMA5 rising',
            '+EMA5>EMA20',
            '+EMA20 rising',
            '+EMA5+EMA20 both rising',
            '+close>EMA5 AND EMA20 rising',
            '+close>EMA5 AND EMA5>EMA20']

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
            d_win = m['win'] - base_m['win']
            d_rr = m['rr'] - base_m['rr']
            d_str = f"win {d_win:+.1f}pp / RR {d_rr:+.3f}"
            mark = '⭐⭐' if d_rr > 0.1 else ('⭐' if d_rr > 0.03 else ('⚠️' if d_rr < -0.03 else '➖'))
        else:
            d_str = ''; mark = ''
        print(f"{key:<37} {m['n']:>6} {m['win']:>+8.1f} {m['mean']:>+9.2f} "
              f"{m['worst']:>+9.1f} {m['rr']:>7.3f}  {d_str} {mark}")

    print("\n" + "=" * 105)
    print("RR 排名（高到低）")
    print("=" * 105)
    rows.sort(key=lambda x: -x[1]['rr'])
    for i, (key, m) in enumerate(rows, 1):
        print(f"  {i}. {key:<40} RR {m['rr']:.3f}  win {m['win']:.1f}%  n={m['n']}")


if __name__ == '__main__':
    main()
