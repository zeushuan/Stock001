"""美股影響衰減速度（Lag 1-5 對齊）
====================================
驗證美股影響的 half-life：第幾天衰減一半？

Lag-0: 同日（不同時區）
Lag-1: 美股前一日 → 台股當日（最強，已知）
Lag-2: 美股 t-2 → 台股 t（殘留？）
Lag-3 ~ Lag-5: 衰減段

各市況下衰減速度可能不同：
  - 多頭：快速被消化
  - 空頭：餘震延長
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl


def load_returns(ticker):
    df = dl.load_from_cache(ticker)
    if df is None: return None
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)
    df = df[df.index >= pd.Timestamp('2020-01-01')]
    if len(df) < 100: return None
    return df['Close'].pct_change() * 100


def main():
    twii = load_returns('^TWII')
    spx = load_returns('^GSPC')
    sox = load_returns('^SOX')
    vix = load_returns('^VIX')
    tsmc = load_returns('2330')
    tsm = load_returns('TSM')

    pairs = [
        ('SPX → TWII', spx, twii),
        ('SOX → TWII', sox, twii),
        ('VIX → TWII', vix, twii) if vix is not None else None,
        ('SOX → 2330', sox, tsmc) if tsmc is not None else None,
        ('TSM → 2330', tsm, tsmc) if tsm is not None and tsmc is not None else None,
    ]
    pairs = [p for p in pairs if p is not None]

    print("=" * 100)
    print("📊 美股影響衰減速度（Lag 0-5）")
    print("=" * 100)
    print("時差邏輯：Lag 0 = 同日（時差 12-13h）/ Lag 1 = 美股前一日影響台股當日 / Lag N = 美股 t-N")
    print()
    print(f"{'指標':<20} {'Lag 0':>9} {'Lag 1':>9} {'Lag 2':>9} {'Lag 3':>9} "
          f"{'Lag 4':>9} {'Lag 5':>9}  {'half-life':>10}")
    print("-" * 100)

    all_data = {}
    for name, x_ret, y_ret in pairs:
        cells = []
        corrs = {}
        for lag in range(0, 6):
            x_lag = x_ret.shift(lag)
            df = pd.concat([x_lag, y_ret], axis=1, join='inner').dropna()
            df.columns = ['us', 'tw']
            if len(df) < 50:
                cells.append('  —  ')
                continue
            corr = df['us'].corr(df['tw'])
            corrs[lag] = float(corr)
            cells.append(f"{corr:+.3f}")
        # half-life：lag-1 corr 一半的位置
        if 1 in corrs and abs(corrs[1]) > 0:
            target = abs(corrs[1]) / 2
            half = '—'
            for L in range(2, 6):
                if L in corrs and abs(corrs[L]) <= target:
                    half = f'{L} 日後'
                    break
            else:
                # 5 日內未衰減一半
                if all(abs(corrs.get(L, 0)) > target for L in range(2, 6)):
                    half = '> 5 日'
        else:
            half = '—'
        line = f"{name:<20} " + ' '.join(f"{c:>9}" for c in cells)
        line += f"  {half:>10}"
        print(line)
        all_data[name] = {**corrs, 'half_life': half}

    # 多頭 vs 空頭分層衰減
    print("\n" + "=" * 100)
    print("📊 衰減速度依市況不同（多頭 / 空頭）")
    print("=" * 100)

    twii_df = dl.load_from_cache('^TWII')
    if hasattr(twii_df.index, 'tz') and twii_df.index.tz is not None:
        twii_df = twii_df.copy()
        twii_df.index = twii_df.index.tz_localize(None)
    twii_df = twii_df[twii_df.index >= pd.Timestamp('2020-01-01')]
    twii_df['ret_60d'] = twii_df['Close'].pct_change(60) * 100
    bull_mask = twii_df['ret_60d'] > 5
    bear_mask = twii_df['ret_60d'] < -5
    bull_dates = twii_df.index[bull_mask]
    bear_dates = twii_df.index[bear_mask]

    for name, x_ret, y_ret in pairs[:3]:  # 只看 SPX/SOX/VIX → TWII
        print(f"\n  {name}")
        print(f"  {'期間':<12} {'Lag 1':>9} {'Lag 2':>9} {'Lag 3':>9} "
              f"{'Lag 4':>9} {'Lag 5':>9}")
        for label, dates in [('多頭', bull_dates), ('空頭', bear_dates)]:
            cells = []
            for lag in range(1, 6):
                x_lag = x_ret.shift(lag)
                df = pd.concat([x_lag, y_ret], axis=1, join='inner').dropna()
                df.columns = ['us', 'tw']
                df_sub = df.loc[df.index.isin(dates)]
                if len(df_sub) < 30:
                    cells.append('—')
                    continue
                corr = df_sub['us'].corr(df_sub['tw'])
                cells.append(f"{corr:+.3f}")
            print(f"  {label:<12} " + ' '.join(f"{c:>9}" for c in cells))

    # 寫 JSON
    with open('us_decay.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n💾 寫入 us_decay.json")


if __name__ == '__main__':
    main()
