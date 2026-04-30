"""K 線頂部反轉型態 6 年回測（台股全市場）
====================================================
測 7 個經典頂部反轉型態：
  1. 三隻烏鴉 (Three Black Crows)
  2. 黃昏之星 (Evening Star)
  3. 空頭吞噬 (Bearish Engulfing) — 高位
  4. 烏雲蓋頂 (Dark Cloud Cover)
  5. 流星線 (Shooting Star)
  6. 吊人線 (Hanging Man)
  7. 頂部十字星 (Doji at Top)

對每個型態：
  - 找全 1058 檔台股 6 年所有觸發點
  - 計算 5/10/30 日後報酬
  - 衡量勝率（跌的機率）/ 平均跌幅 / 最差跌幅
  - 過濾條件：必須在「上漲後高位」
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
    """對單檔 df，偵測所有頂部反轉型態觸發點
    回傳 dict {pattern_name: [trigger_idx, ...]}"""
    if len(df) < 60: return {}
    o = df['Open'].values if 'Open' in df.columns else df['Close'].values
    h = df['High'].values if 'High' in df.columns else df['Close'].values
    l = df['Low'].values if 'Low' in df.columns else df['Close'].values
    c = df['Close'].values
    n = len(df)

    # K 線屬性
    body = np.abs(c - o)
    rng = h - l
    upper = h - np.maximum(c, o)
    lower = np.minimum(c, o) - l
    is_red = c < o    # 陰線
    is_green = c > o  # 陽線

    # 平均實體（過去 14 日）— 判斷「大」K
    avg_body = pd.Series(body).rolling(14, min_periods=5).mean().values
    big = body > avg_body * 1.2

    # 「高位」: close 過去 30 日漲幅 > 5%
    rise_30d = np.zeros(n)
    for i in range(30, n):
        if c[i-30] > 0:
            rise_30d[i] = (c[i] - c[i-30]) / c[i-30] * 100
    is_high_position = rise_30d > 5

    patterns = {
        '1_三隻烏鴉': [],
        '2_黃昏之星': [],
        '3_空頭吞噬': [],
        '4_烏雲蓋頂': [],
        '5_流星線': [],
        '6_吊人線': [],
        '7_頂部十字星': [],
    }

    for i in range(30, n - 1):
        if not is_high_position[i]: continue

        # 1. 三隻烏鴉：連 3 根大陰線（i-2, i-1, i）
        if i >= 2:
            three_red = is_red[i-2] and is_red[i-1] and is_red[i]
            three_big = big[i-2] and big[i-1] and big[i]
            # 開盤在前一根實體內
            ok_open = (o[i-1] < c[i-2]) and (o[i] < c[i-1])
            # 收盤遞減
            ok_close = c[i-1] < c[i-2] and c[i] < c[i-1]
            if three_red and three_big and ok_open and ok_close:
                patterns['1_三隻烏鴉'].append(i)

        # 2. 黃昏之星
        if i >= 2:
            day1_big_green = is_green[i-2] and big[i-2]
            day2_small = body[i-1] < avg_body[i-1] * 0.5 if not np.isnan(avg_body[i-1]) else False
            day2_gap_up = l[i-1] > c[i-2]
            day3_big_red = is_red[i] and big[i]
            day3_into_day1 = c[i] < (o[i-2] + c[i-2]) / 2
            if day1_big_green and day2_small and day2_gap_up and day3_big_red and day3_into_day1:
                patterns['2_黃昏之星'].append(i)

        # 3. 空頭吞噬（高位才有效）
        if i >= 1:
            day1_green = is_green[i-1]
            day2_red = is_red[i]
            engulf = (o[i] >= c[i-1]) and (c[i] <= o[i-1]) and body[i] > body[i-1]
            if day1_green and day2_red and engulf:
                patterns['3_空頭吞噬'].append(i)

        # 4. 烏雲蓋頂
        if i >= 1:
            day1_big_green = is_green[i-1] and big[i-1]
            day2_gap_up = o[i] > h[i-1]
            day2_red_into_day1 = is_red[i] and c[i] < (o[i-1] + c[i-1]) / 2 and c[i] > o[i-1]
            if day1_big_green and day2_gap_up and day2_red_into_day1:
                patterns['4_烏雲蓋頂'].append(i)

        # 5. 流星線（單根）
        # 上影 ≥ 2× 實體 / 下影 < 0.3× 實體 / 實體在下半部
        if rng[i] > 0:
            shooting = (upper[i] >= body[i] * 2.0) and \
                       (lower[i] < body[i] * 0.3) and \
                       (body[i] > 0.0001 * c[i])
            if shooting and rise_30d[i] > 8:  # 更嚴格高位
                patterns['5_流星線'].append(i)

        # 6. 吊人線（單根）
        # 下影 ≥ 2× 實體 / 上影 < 0.3× 實體
        if rng[i] > 0:
            hanging = (lower[i] >= body[i] * 2.0) and \
                       (upper[i] < body[i] * 0.3) and \
                       (body[i] > 0.0001 * c[i])
            if hanging and rise_30d[i] > 8:
                patterns['6_吊人線'].append(i)

        # 7. 頂部十字星
        # 實體 < 10% range
        if rng[i] > 0:
            doji = body[i] < rng[i] * 0.1
            if doji and rise_30d[i] > 8:
                patterns['7_頂部十字星'].append(i)

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

        # 對每個觸發計算 5/10/30 日後報酬
        results = {p: {'5d': [], '10d': [], '30d': []}
                   for p in patterns}

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
    if len(a) == 0: return None
    return {
        'n': len(a),
        'mean': float(a.mean()),
        'median': float(np.median(a)),
        'down_prob': float((a < 0).mean() * 100),  # 跌的機率
        'avg_drop': float(a[a < 0].mean()) if (a < 0).any() else 0,
        'worst': float(a.min()),
        'best': float(a.max()),
    }


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    print(f"🇹🇼 TW universe: {len(universe)} 檔\n")

    print("📊 偵測 7 個頂部反轉型態...")
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
    print("📊 7 個頂部反轉型態回測結果（台股 1058 檔 × 6 年）")
    print("=" * 110)
    pattern_names = {
        '1_三隻烏鴉': '三隻烏鴉 ★★★★★',
        '2_黃昏之星': '黃昏之星 ★★★★★',
        '3_空頭吞噬': '空頭吞噬 ★★★★',
        '4_烏雲蓋頂': '烏雲蓋頂 ★★★★',
        '5_流星線':   '流星線   ★★★',
        '6_吊人線':   '吊人線   ★★★',
        '7_頂部十字星': '頂部十字星 ★★',
    }

    for pkey, plabel in pattern_names.items():
        print(f"\n【{plabel}】")
        print(f"  {'持有期':<6} {'樣本':>7} {'跌機率%':>8} {'平均報酬%':>10} "
              f"{'平均跌幅%':>10} {'最差%':>8} {'最佳%':>8}")
        print("-" * 110)
        for hold_key, hold_label in [('5d', '5 日'), ('10d', '10 日'), ('30d', '30 日')]:
            arr = aggregated.get(pkey, {}).get(hold_key, [])
            m = metrics(arr)
            if m:
                print(f"  {hold_label:<6} {m['n']:>7} {m['down_prob']:>+8.1f} "
                      f"{m['mean']:>+10.2f} {m['avg_drop']:>+10.2f} "
                      f"{m['worst']:>+8.1f} {m['best']:>+8.1f}")
            else:
                print(f"  {hold_label:<6} (樣本不足)")

    # 排行榜（依 30 天平均跌幅）
    print("\n" + "=" * 110)
    print("🏆 30 天「跌機率」排行（越高越強空頭訊號）")
    print("=" * 110)
    rows = []
    for pkey, plabel in pattern_names.items():
        arr = aggregated.get(pkey, {}).get('30d', [])
        m = metrics(arr)
        if m:
            rows.append((plabel, m))
    rows.sort(key=lambda x: -x[1]['down_prob'])
    print(f"  {'排名':<3} {'型態':<22} {'樣本':>7} {'30d 跌機率':>11} {'30d 均報':>10} {'平均跌幅':>10}")
    print("-" * 110)
    for i, (label, m) in enumerate(rows, 1):
        marker = ' 🔥' if m['down_prob'] > 55 else (' ✓' if m['down_prob'] > 50 else '')
        print(f"  {i:<3} {label:<22} {m['n']:>7} {m['down_prob']:>+11.1f} "
              f"{m['mean']:>+10.2f} {m['avg_drop']:>+10.2f}{marker}")

    # 寫 JSON
    out = {p: {h: metrics(v) for h, v in holds.items()}
           for p, holds in aggregated.items()}
    with open('bearish_patterns.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 bearish_patterns.json")


if __name__ == '__main__':
    main()
