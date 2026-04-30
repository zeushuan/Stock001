"""K 線底部反轉型態 6 年回測（找「即將反彈」真訊號）
================================================================
測 7 個經典底部反轉型態：
  1. 鎚子 (Hammer)
  2. 倒鎚 (Inverted Hammer)
  3. 多頭吞噬 (Bullish Engulfing)
  4. 旭日東昇 (Piercing)
  5. 啟明星 (Morning Star)
  6. 紅三兵 (Three White Soldiers)
  7. 底部十字星 (Doji at Bottom)

對每型態：
  全 1058 檔台股 6 年所有觸發點
  5/10/30 日後報酬
  漲機率 / 平均報酬 / 最佳/最差

過濾：必須在「下跌段低位」（過去 30 天跌 > 5%）
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16


def detect_patterns(df):
    if len(df) < 60: return {}
    o = df['Open'].values if 'Open' in df.columns else df['Close'].values
    h = df['High'].values if 'High' in df.columns else df['Close'].values
    l = df['Low'].values if 'Low' in df.columns else df['Close'].values
    c = df['Close'].values
    n = len(df)

    body = np.abs(c - o)
    rng = h - l
    upper = h - np.maximum(c, o)
    lower = np.minimum(c, o) - l
    is_red = c < o
    is_green = c > o

    avg_body = pd.Series(body).rolling(14, min_periods=5).mean().values
    big = body > avg_body * 1.2

    # 「下跌後低位」: 過去 30 天跌 > 5%
    drop_30d = np.zeros(n)
    for i in range(30, n):
        if c[i-30] > 0:
            drop_30d[i] = (c[i] - c[i-30]) / c[i-30] * 100
    is_low_position = drop_30d < -5

    patterns = {
        '1_鎚子': [], '2_倒鎚': [], '3_多頭吞噬': [],
        '4_旭日東昇': [], '5_啟明星': [], '6_紅三兵': [],
        '7_底部十字星': [],
    }

    for i in range(30, n - 1):
        if not is_low_position[i]: continue

        # 1. 鎚子 (下影 ≥ 2× 實體, 上影小)
        if rng[i] > 0:
            hammer = (lower[i] >= body[i] * 2.0) and \
                     (upper[i] < body[i] * 0.3) and \
                     (body[i] > 0.0001 * c[i])
            if hammer and drop_30d[i] < -8:
                patterns['1_鎚子'].append(i)

            # 2. 倒鎚 (上影 ≥ 2× 實體, 下影小)
            inv_hammer = (upper[i] >= body[i] * 2.0) and \
                         (lower[i] < body[i] * 0.3) and \
                         (body[i] > 0.0001 * c[i])
            if inv_hammer and drop_30d[i] < -8:
                patterns['2_倒鎚'].append(i)

            # 7. 底部十字星
            doji = body[i] < rng[i] * 0.1
            if doji and drop_30d[i] < -8:
                patterns['7_底部十字星'].append(i)

        # 3. 多頭吞噬
        if i >= 1:
            day1_red = is_red[i-1]
            day2_green = is_green[i]
            engulf = (o[i] <= c[i-1]) and (c[i] >= o[i-1]) and body[i] > body[i-1]
            if day1_red and day2_green and engulf:
                patterns['3_多頭吞噬'].append(i)

            # 4. 旭日東昇 (gap down + 收盤超過前根紅 K 中點)
            day1_big_red = is_red[i-1] and big[i-1]
            gap_down = o[i] < l[i-1]
            close_above_mid = is_green[i] and c[i] > (o[i-1] + c[i-1]) / 2 and c[i] < o[i-1]
            if day1_big_red and gap_down and close_above_mid:
                patterns['4_旭日東昇'].append(i)

        # 5. 啟明星
        if i >= 2:
            day1_big_red = is_red[i-2] and big[i-2]
            day2_small = body[i-1] < avg_body[i-1] * 0.5 if not np.isnan(avg_body[i-1]) else False
            day2_gap_down = h[i-1] < c[i-2]
            day3_big_green = is_green[i] and big[i]
            day3_into_day1 = c[i] > (o[i-2] + c[i-2]) / 2
            if day1_big_red and day2_small and day2_gap_down and day3_big_green and day3_into_day1:
                patterns['5_啟明星'].append(i)

            # 6. 紅三兵
            three_green = is_green[i-2] and is_green[i-1] and is_green[i]
            three_big = big[i-2] and big[i-1] and big[i]
            ok_open = (o[i-1] > o[i-2]) and (o[i] > o[i-1])
            ok_close = c[i-1] > c[i-2] and c[i] > c[i-1]
            if three_green and three_big and ok_open and ok_close:
                patterns['6_紅三兵'].append(i)

    return patterns


def analyze_one(ticker):
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp('2020-01-01')]
        if len(df) < 280: return (ticker, None)

        patterns = detect_patterns(df)
        c = df['Close'].values
        n = len(df)
        results = {p: {'5d': [], '10d': [], '30d': []} for p in patterns}
        for pname, indices in patterns.items():
            for idx in indices:
                for hold, key in [(5, '5d'), (10, '10d'), (30, '30d')]:
                    if idx + hold < n:
                        ret = (c[idx + hold] - c[idx]) / c[idx] * 100
                        results[pname][key].append(ret)
        return (ticker, results)
    except Exception:
        return (ticker, None)


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    a = a[~np.isnan(a)]
    if len(a) < 5: return None
    return {
        'n': len(a),
        'mean': float(a.mean()),
        'up_prob': float((a > 0).mean() * 100),
        'avg_gain': float(a[a > 0].mean()) if (a > 0).any() else 0,
        'worst': float(a.min()),
        'best': float(a.max()),
    }


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    print(f"🇹🇼 TW universe: {len(universe)} 檔\n")

    print("📊 偵測 7 個底部反轉型態...")
    t0 = time.time()
    aggregated = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker, r in ex.map(analyze_one, universe, chunksize=50):
            if r is not None:
                for pname, holds in r.items():
                    if pname not in aggregated:
                        aggregated[pname] = {'5d': [], '10d': [], '30d': []}
                    for k, v in holds.items():
                        aggregated[pname][k].extend(v)
    print(f"  完成 {time.time()-t0:.1f}s\n")

    print("=" * 110)
    print("📊 7 個底部反轉型態回測結果（台股 1058 檔 × 6 年）")
    print("=" * 110)
    pattern_names = {
        '1_鎚子':       '鎚子           ★★★',
        '2_倒鎚':       '倒鎚           ★★★',
        '3_多頭吞噬':   '多頭吞噬       ★★★★',
        '4_旭日東昇':   '旭日東昇       ★★★★',
        '5_啟明星':     '啟明星         ★★★★★',
        '6_紅三兵':     '紅三兵         ★★★★★',
        '7_底部十字星': '底部十字星     ★★',
    }

    for pkey, plabel in pattern_names.items():
        print(f"\n【{plabel}】")
        print(f"  {'持有期':<6} {'樣本':>7} {'漲機率%':>8} {'平均報酬%':>10} "
              f"{'平均漲幅%':>10} {'最差%':>8} {'最佳%':>8}")
        print("-" * 110)
        for hold_key, hold_label in [('5d', '5 日'), ('10d', '10 日'), ('30d', '30 日')]:
            arr = aggregated.get(pkey, {}).get(hold_key, [])
            m = metrics(arr)
            if m:
                print(f"  {hold_label:<6} {m['n']:>7} {m['up_prob']:>+8.1f} "
                      f"{m['mean']:>+10.2f} {m['avg_gain']:>+10.2f} "
                      f"{m['worst']:>+8.1f} {m['best']:>+8.1f}")

    print("\n" + "=" * 110)
    print("🏆 30 天「漲機率」排行（越高越強反彈訊號）")
    print("=" * 110)
    rows = []
    for pkey, plabel in pattern_names.items():
        arr = aggregated.get(pkey, {}).get('30d', [])
        m = metrics(arr)
        if m:
            rows.append((plabel, m))
    rows.sort(key=lambda x: -x[1]['up_prob'])
    print(f"  {'排名':<3} {'型態':<25} {'樣本':>7} {'30d 漲機率':>11} {'30d 均報':>10} {'平均漲幅':>10}")
    print("-" * 110)
    for i, (label, m) in enumerate(rows, 1):
        marker = ''
        if m['up_prob'] > 60: marker = ' 🔥'
        elif m['up_prob'] > 55: marker = ' ✓'
        print(f"  {i:<3} {label:<25} {m['n']:>7} {m['up_prob']:>+11.1f} "
              f"{m['mean']:>+10.2f} {m['avg_gain']:>+10.2f}{marker}")

    out = {p: {h: metrics(v) for h, v in holds.items()}
           for p, holds in aggregated.items()}
    with open('bullish_patterns.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 bullish_patterns.json")


if __name__ == '__main__':
    main()
