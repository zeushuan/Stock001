"""Bollinger Bands Squeeze 研究（v9.12）
==========================================
BB Squeeze 理論：頻寬收縮 → 即將爆發大行情
測試 BB 收縮後的 5/15/30 天後續：
  Q1: BB Squeeze 是否真的後續爆發？
  Q2: 多頭排列 vs 空頭排列下表現？
  Q3: 配合 K 線型態 / 動能 哪個更強？

定義：
  BB(20, 2)
  bandwidth = (BBU - BBL) / SMA20
  squeeze = bandwidth 落在過去 120 日最低 20%

測試變體（必要前提：is_bull = EMA20 > EMA60）：
  S1: BB Squeeze 持續 5 天（窄收）
  S2: S1 + 突破上軌（close > BBU）
  S3: S1 + close 在中軌附近（中性等待）
  S4: BB Squeeze + ADX≥22（趨勢未爆發）
  S5: BB Squeeze + 量縮（vol < 60d 平均）
  baseline: 多頭任意天
"""
import sys, time, json
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16
HOLD = 30
START_DATE = '2020-01-01'


def detect_one(ticker):
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return None
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        if len(df) < 200: return None

        c = df['Close'].values
        o = df['Open'].values
        v = df['Volume'].values
        n = len(df)

        # 計算 BB(20, 2)
        sma20 = pd.Series(c).rolling(20, min_periods=15).mean().values
        std20 = pd.Series(c).rolling(20, min_periods=15).std().values
        bbu = sma20 + 2 * std20
        bbl = sma20 - 2 * std20
        bandwidth = np.where(sma20 > 0, (bbu - bbl) / sma20, 0)

        e20 = df['e20'].values if 'e20' in df.columns else None
        e60 = df['e60'].values if 'e60' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        if any(x is None for x in [e20, e60, adx]):
            return None

        vol60 = pd.Series(v).rolling(60, min_periods=30).mean().values

        result = {f'S{i}': [] for i in range(1, 6)}
        result['baseline_bull'] = []

        for t in range(120, n - HOLD - 1):
            if any(np.isnan(x) for x in [e20[t], e60[t], adx[t], bandwidth[t]]):
                continue
            if e20[t] <= e60[t]: continue   # 多頭

            # 30d 後 ret
            ent = o[t+1]; exi = o[t+1+HOLD]
            if np.isnan(ent) or np.isnan(exi): continue
            if ent <= 0 or exi <= 0: continue
            ret = (exi - ent) / ent

            result['baseline_bull'].append(ret)

            # BW 是否落在過去 120 日最低 20%
            bw_window = bandwidth[t-120:t]
            bw_window = bw_window[~np.isnan(bw_window)]
            if len(bw_window) < 60: continue
            threshold = np.percentile(bw_window, 20)
            current_squeeze = bandwidth[t] <= threshold

            # 連 5 天 squeeze
            recent_5 = bandwidth[t-4:t+1]
            recent_5 = recent_5[~np.isnan(recent_5)]
            if len(recent_5) < 5: continue
            five_day_squeeze = all(b <= threshold for b in recent_5)

            if not five_day_squeeze: continue

            # S1: 連 5 天 squeeze
            result['S1'].append(ret)

            # S2: S1 + 突破上軌（close > BBU 當天 OR 前 1 天）
            if not np.isnan(bbu[t]) and (c[t] > bbu[t] or (t>=1 and c[t-1] > bbu[t-1])):
                result['S2'].append(ret)

            # S3: S1 + close 在中軌附近（|close-SMA20|/SMA20 < 1%）
            if not np.isnan(sma20[t]) and sma20[t] > 0:
                pct_to_mid = abs(c[t] - sma20[t]) / sma20[t] * 100
                if pct_to_mid < 1.0:
                    result['S3'].append(ret)

            # S4: S1 + ADX≥22
            if adx[t] >= 22:
                result['S4'].append(ret)

            # S5: S1 + 量縮
            if not np.isnan(vol60[t]) and vol60[t] > 0 and v[t] < vol60[t]:
                result['S5'].append(ret)

        return result
    except Exception:
        return None


def main():
    from pathlib import Path
    DATA = Path('data_cache')
    universe = sorted([
        p.stem for p in DATA.glob('*.parquet')
        if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
        and not p.stem.startswith('00')
    ])
    print(f"🇹🇼 Universe: {len(universe)} 檔, hold={HOLD}d")
    print()

    print(f"📊 跑 BB Squeeze 5 個變體...")
    t0 = time.time()
    aggregated = defaultdict(list)
    n_ok = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for r in ex.map(detect_one, universe, chunksize=50):
            if r is None: continue
            n_ok += 1
            for k, v in r.items():
                aggregated[k].extend(v)
    print(f"  完成 {time.time()-t0:.1f}s ({n_ok}/{len(universe)} 檔)\n")

    desc = {
        'baseline_bull': '多頭排列基準（任意天）',
        'S1': 'BB Squeeze 5 天連窄',
        'S2': 'S1 + 突破上軌',
        'S3': 'S1 + close 在中軌',
        'S4': 'S1 + ADX≥22',
        'S5': 'S1 + 量縮',
    }

    print("=" * 90)
    print(f"{'Variant':>6}{'n':>10}{'Win%':>8}{'Mean':>9}{'Median':>9}{'Std':>8}{'PF':>6}  說明")
    print("=" * 90)
    rows = []
    for k in ['baseline_bull', 'S1', 'S2', 'S3', 'S4', 'S5']:
        rets = aggregated.get(k, [])
        if len(rets) < 30:
            print(f"  {k}: n={len(rets)} (太少略過)")
            continue
        a = np.array(rets)
        n = len(a)
        win = (a > 0).mean() * 100
        mean = a.mean() * 100
        median = np.median(a) * 100
        std = a.std() * 100
        pos_sum = a[a > 0].sum()
        neg_sum = -a[a < 0].sum() if (a < 0).any() else 0.001
        pf = pos_sum / neg_sum if neg_sum > 0 else 999
        marker = ''
        if k != 'baseline_bull':
            base = aggregated.get('baseline_bull', [])
            if base and mean - np.mean(base)*100 > 1:
                marker = ' 🚀'
            elif base and mean - np.mean(base)*100 > 0.3:
                marker = ' ★'
        print(f"  {k:>6}{n:>10}{win:>7.1f}%{mean:>+8.2f}%{median:>+8.2f}%{std:>7.2f}%{pf:>6.2f}  {desc[k]}{marker}")
        rows.append({
            'variant': k, 'desc': desc[k], 'n': n,
            'win_pct': float(win), 'mean_pct': float(mean),
            'median_pct': float(median), 'std_pct': float(std),
            'pf': float(pf),
        })

    base_row = next((r for r in rows if r['variant'] == 'baseline_bull'), None)
    if base_row:
        print()
        print("與 baseline 比較:")
        for r in rows:
            if r['variant'] == 'baseline_bull': continue
            d = r['mean_pct'] - base_row['mean_pct']
            tag = '🚀' if d > 1 else '★' if d > 0.3 else '⚪' if abs(d) < 0.3 else '🔻'
            print(f"  {tag} {r['variant']}: Δmean={d:+.2f}%, Δwin={r['win_pct']-base_row['win_pct']:+.1f}%, n={r['n']}")

    with open('analyze_bb_squeeze_results.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 analyze_bb_squeeze_results.json")


if __name__ == '__main__':
    main()
