"""T3 進場輔助確認指標測試
=============================
基準 T3：多頭+ADX≥22+RSI<50

測試輔助條件（避開假拉回）：
  C1 close > EMA20         （未跌破短期均線 = 真拉回）
  C2 RSI 上升              （RSI 已開始反彈）
  C3 EMA20 斜率為正        （短均線仍上揚 = 趨勢未破）
  C4 量縮（vol < 20MA）    （拉回時量小 = 賣壓有限）
  C5 KD 黃金交叉           （Stochastic 反轉確認）
  C6 close > BB 下軌反彈   （超賣反彈訊號）
  C7 close > EMA60         （未跌破中期支撐）
  C8 多重確認（C1+C2+C3）   （最嚴格）
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

    # 補齊輔助指標
    if 'pctb' not in test_df.columns:
        bb = ta.volatility.BollingerBands(test_df['Close'], window=20)
        test_df['pctb'] = bb.bollinger_pband()
    # KD（Stochastic）
    stoch = ta.momentum.StochasticOscillator(test_df['High'], test_df['Low'],
                                              test_df['Close'], 14, 3)
    test_df['stoch_k'] = stoch.stoch()
    test_df['stoch_d'] = stoch.stoch_signal()
    # 量 20 日均
    test_df['vol_ma20'] = test_df['Volume'].rolling(20).mean()
    # EMA20 斜率（5 日變化）
    test_df['e20_slope'] = test_df['e20'].diff(5)

    e20 = test_df['e20'].values
    e60 = test_df['e60'].values
    rsi = test_df['rsi'].values
    adx = test_df['adx'].values
    close = test_df['Close'].values
    vol = test_df['Volume'].values
    vol_ma = test_df['vol_ma20'].values
    e20_slope = test_df['e20_slope'].values
    pctb = test_df['pctb'].values
    stoch_k = test_df['stoch_k'].values
    stoch_d = test_df['stoch_d'].values
    n = len(test_df)

    # 各情境的 30 天報酬
    results = {
        'T3 baseline': [],
        '+C1 close>EMA20': [],
        '+C2 RSI rising': [],
        '+C3 EMA20 rising': [],
        '+C4 vol shrink': [],
        '+C5 KD bull cross': [],
        '+C6 BB bottom rebound': [],
        '+C7 close>EMA60': [],
        '+C8 multi-confirm (C1+C2+C3)': [],
    }

    for i in range(2, n - HOLD):
        if any(np.isnan(x) for x in [e20[i], e60[i], rsi[i], adx[i]]): continue
        # T3 baseline 條件
        is_bull = e20[i] > e60[i]
        if not is_bull: continue
        if adx[i] < 22: continue
        if not (35 <= rsi[i] < 50): continue

        entry_price = close[i]
        ret = (close[i + HOLD] - entry_price) / entry_price * 100
        results['T3 baseline'].append(ret)

        # C1: close > EMA20
        if close[i] > e20[i]:
            results['+C1 close>EMA20'].append(ret)
        # C2: RSI 上升（連續 1 天）
        if rsi[i] > rsi[i-1]:
            results['+C2 RSI rising'].append(ret)
        # C3: EMA20 5 日斜率為正
        if not np.isnan(e20_slope[i]) and e20_slope[i] > 0:
            results['+C3 EMA20 rising'].append(ret)
        # C4: 量縮（vol < vol_ma20）
        if not np.isnan(vol_ma[i]) and vol[i] < vol_ma[i]:
            results['+C4 vol shrink'].append(ret)
        # C5: KD 黃金交叉（K 從 D 下方上穿）
        if (not np.isnan(stoch_k[i]) and not np.isnan(stoch_d[i])
                and not np.isnan(stoch_k[i-1]) and not np.isnan(stoch_d[i-1])):
            if stoch_k[i-1] <= stoch_d[i-1] and stoch_k[i] > stoch_d[i]:
                results['+C5 KD bull cross'].append(ret)
        # C6: BB 下軌反彈（pctb < 0.2 但開始上升）
        if not np.isnan(pctb[i]) and not np.isnan(pctb[i-1]):
            if pctb[i] < 0.2 and pctb[i] > pctb[i-1]:
                results['+C6 BB bottom rebound'].append(ret)
        # C7: close > EMA60（未跌破中期支撐）
        if close[i] > e60[i]:
            results['+C7 close>EMA60'].append(ret)
        # C8: 多重確認（C1+C2+C3）
        c1 = close[i] > e20[i]
        c2 = rsi[i] > rsi[i-1]
        c3 = not np.isnan(e20_slope[i]) and e20_slope[i] > 0
        if c1 and c2 and c3:
            results['+C8 multi-confirm (C1+C2+C3)'].append(ret)

    return results


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    return {'n': len(a), 'mean': a.mean(),
            'win': (a > 0).mean() * 100, 'worst': a.min(),
            'rr': a.mean()/abs(a.min()) if a.min() < 0 else 0}


def main():
    print(f"分析 TOP 200 在 TEST 期 (2024.6-2026.4) T3 + 輔助指標...\n")
    with ProcessPoolExecutor(max_workers=12) as ex:
        all_r = [r for r in ex.map(analyze_one, TOP200) if r is not None]
    print(f"成功 {len(all_r)}/{len(TOP200)}\n")

    # 彙總
    print("=" * 100)
    print(f"T3 + 輔助確認指標（30 天持有，TEST 期）")
    print("=" * 100)
    print(f"{'情境':<35} {'樣本':>6} {'勝率%':>8} {'均報%':>9} {'最差%':>9} "
          f"{'RR':>7}  Δ vs baseline")
    print("-" * 100)

    keys = ['T3 baseline',
            '+C1 close>EMA20',
            '+C2 RSI rising',
            '+C3 EMA20 rising',
            '+C4 vol shrink',
            '+C5 KD bull cross',
            '+C6 BB bottom rebound',
            '+C7 close>EMA60',
            '+C8 multi-confirm (C1+C2+C3)']

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
            mark = '⭐' if d_rr > 0.05 else ('⚠️' if d_rr < -0.05 else '➖')
        else:
            d_str = ''
            mark = ''
        print(f"{key:<35} {m['n']:>6} {m['win']:>+8.1f} {m['mean']:>+9.2f} "
              f"{m['worst']:>+9.1f} {m['rr']:>7.3f}  {d_str} {mark}")

    # 排名
    print("\n" + "=" * 100)
    print("RR 排名（高到低）")
    print("=" * 100)
    rows.sort(key=lambda x: -x[1]['rr'])
    for i, (key, m) in enumerate(rows, 1):
        print(f"  {i}. {key:<38} RR {m['rr']:.3f}  "
              f"勝率 {m['win']:.1f}%  樣本 {m['n']}")


if __name__ == '__main__':
    main()
