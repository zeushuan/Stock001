"""K 線頂部反轉型態 + 配合指標研究
=========================================
找出哪些指標配合 K 線型態能真正提升預測準確度

Filter 條件矩陣：
  a. RSI ≥ 70（過熱）
  b. RSI ≥ 75（極度過熱）
  c. close < EMA20（已跌破短均線）
  d. ADX 5 日下降（趨勢轉弱）
  e. 距 60d 高 < 5%（高位）
  f. 距 SMA200 > 25%（過度延伸）
  g. 量爆 (vol > 1.5× 60d avg)
  h. 量縮 (vol < 0.7× 60d avg)
  i. RSI≥70 AND close<EMA20（高位 + 跌破）
  j. RSI≥75 AND ADX 下降（最強組合）
  k. 距高點 < 5% AND 過度延伸（雙頂風險）

評估：
  每個 (pattern, filter) 組合的 30 天跌機率 + 平均跌幅
  比 baseline pattern 提升多少？
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


def detect_with_filters(df):
    """偵測型態 + 同時記錄當下 filter 條件"""
    if len(df) < 60: return {}
    o = df['Open'].values if 'Open' in df.columns else df['Close'].values
    h = df['High'].values if 'High' in df.columns else df['Close'].values
    l = df['Low'].values if 'Low' in df.columns else df['Close'].values
    c = df['Close'].values
    v = df['Volume'].values if 'Volume' in df.columns else np.zeros_like(c)
    n = len(df)

    body = np.abs(c - o)
    rng = h - l
    upper = h - np.maximum(c, o)
    lower = np.minimum(c, o) - l
    is_red = c < o
    is_green = c > o

    avg_body = pd.Series(body).rolling(14, min_periods=5).mean().values
    big = body > avg_body * 1.2

    # 高位過濾
    rise_30d = np.zeros(n)
    for i in range(30, n):
        if c[i-30] > 0:
            rise_30d[i] = (c[i] - c[i-30]) / c[i-30] * 100
    is_high = rise_30d > 5

    # filter 指標
    rsi = df['rsi'].values if 'rsi' in df.columns else None
    adx = df['adx'].values if 'adx' in df.columns else None
    e20 = df['e20'].values if 'e20' in df.columns else None
    e60 = df['e60'].values if 'e60' in df.columns else None
    e120 = df['e120'].values if 'e120' in df.columns else None
    if rsi is None or adx is None or e20 is None or e60 is None:
        return {}

    # 60d high
    h60 = np.array([c[max(0, i-60):i].max() if i >= 1 else c[i] for i in range(n)])
    # SMA200
    sma200 = pd.Series(c).rolling(200, min_periods=100).mean().values
    # 60d avg vol
    vol60 = pd.Series(v).rolling(60, min_periods=30).mean().values

    # 7 個型態
    triggers = {p: [] for p in ['1_三隻烏鴉', '2_黃昏之星', '3_空頭吞噬',
                                  '4_烏雲蓋頂', '5_流星線', '6_吊人線',
                                  '7_頂部十字星']}

    for i in range(60, n - 1):
        if not is_high[i]: continue
        if any(np.isnan(x) for x in [rsi[i], adx[i], e20[i], e60[i]]): continue

        # 偵測 7 型態（同 analyze_bearish_patterns.py 邏輯）
        if i >= 2:
            if (is_red[i-2] and is_red[i-1] and is_red[i]
                    and big[i-2] and big[i-1] and big[i]
                    and o[i-1] < c[i-2] and o[i] < c[i-1]
                    and c[i-1] < c[i-2] and c[i] < c[i-1]):
                triggers['1_三隻烏鴉'].append(i)

            if (is_green[i-2] and big[i-2]
                    and (body[i-1] < avg_body[i-1] * 0.5 if not np.isnan(avg_body[i-1]) else False)
                    and l[i-1] > c[i-2]
                    and is_red[i] and big[i]
                    and c[i] < (o[i-2] + c[i-2]) / 2):
                triggers['2_黃昏之星'].append(i)

        if i >= 1:
            if (is_green[i-1] and is_red[i]
                    and o[i] >= c[i-1] and c[i] <= o[i-1]
                    and body[i] > body[i-1]):
                triggers['3_空頭吞噬'].append(i)

            if (is_green[i-1] and big[i-1]
                    and o[i] > h[i-1]
                    and is_red[i] and c[i] < (o[i-1] + c[i-1]) / 2 and c[i] > o[i-1]):
                triggers['4_烏雲蓋頂'].append(i)

        if rng[i] > 0:
            shooting = (upper[i] >= body[i] * 2.0) and (lower[i] < body[i] * 0.3) and (body[i] > 0.0001 * c[i])
            if shooting and rise_30d[i] > 8:
                triggers['5_流星線'].append(i)
            hanging = (lower[i] >= body[i] * 2.0) and (upper[i] < body[i] * 0.3) and (body[i] > 0.0001 * c[i])
            if hanging and rise_30d[i] > 8:
                triggers['6_吊人線'].append(i)
            doji = body[i] < rng[i] * 0.1
            if doji and rise_30d[i] > 8:
                triggers['7_頂部十字星'].append(i)

    # 對每個 trigger 記錄 filter 條件 + 30d 報酬
    results = {p: {} for p in triggers}
    for pname, indices in triggers.items():
        # 各 filter 列表
        filters_data = {
            'baseline': [],
            'a_RSI70': [], 'b_RSI75': [],
            'c_close_below_EMA20': [],
            'd_ADX_falling': [],
            'e_near_60d_high': [],
            'f_extended_SMA200': [],
            'g_volume_spike': [],
            'h_volume_dry': [],
            'i_RSI70_below_EMA20': [],
            'j_RSI75_ADX_falling': [],
            'k_top_extended': [],
        }
        for idx in indices:
            if idx + 30 >= n: continue
            ret_30d = (c[idx + 30] - c[idx]) / c[idx] * 100

            # filter 條件
            r = rsi[idx]
            a = adx[idx]
            close_v = c[idx]
            e20_v = e20[idx]
            adx_5d = adx[idx-5] if idx >= 5 and not np.isnan(adx[idx-5]) else a
            adx_falling = a < adx_5d
            from_high_pct = (h60[idx] - close_v) / h60[idx] * 100 if h60[idx] > 0 else 0
            sma200_v = sma200[idx] if not np.isnan(sma200[idx]) else None
            extended = (close_v / sma200_v - 1) * 100 if sma200_v and sma200_v > 0 else 0
            vol60_v = vol60[idx] if not np.isnan(vol60[idx]) else 0
            vol_ratio = v[idx] / vol60_v if vol60_v > 0 else 1

            # baseline: 所有 trigger
            filters_data['baseline'].append(ret_30d)
            # a/b
            if r >= 70: filters_data['a_RSI70'].append(ret_30d)
            if r >= 75: filters_data['b_RSI75'].append(ret_30d)
            # c
            if close_v < e20_v: filters_data['c_close_below_EMA20'].append(ret_30d)
            # d
            if adx_falling: filters_data['d_ADX_falling'].append(ret_30d)
            # e
            if from_high_pct < 5: filters_data['e_near_60d_high'].append(ret_30d)
            # f
            if extended > 25: filters_data['f_extended_SMA200'].append(ret_30d)
            # g/h
            if vol_ratio > 1.5: filters_data['g_volume_spike'].append(ret_30d)
            if vol_ratio < 0.7: filters_data['h_volume_dry'].append(ret_30d)
            # i
            if r >= 70 and close_v < e20_v:
                filters_data['i_RSI70_below_EMA20'].append(ret_30d)
            # j
            if r >= 75 and adx_falling:
                filters_data['j_RSI75_ADX_falling'].append(ret_30d)
            # k
            if from_high_pct < 5 and extended > 25:
                filters_data['k_top_extended'].append(ret_30d)
        results[pname] = filters_data
    return results


def analyze_one(ticker):
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp('2020-01-01')]
        if len(df) < 280: return (ticker, None)
        return (ticker, detect_with_filters(df))
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
        'down_prob': float((a < 0).mean() * 100),
    }


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    print(f"🇹🇼 TW universe: {len(universe)} 檔\n")

    print("📊 偵測型態 + filter 條件...")
    t0 = time.time()
    aggregated = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker, r in ex.map(analyze_one, universe, chunksize=50):
            if r is not None:
                for pname, filters in r.items():
                    if pname not in aggregated:
                        aggregated[pname] = {fname: [] for fname in filters}
                    for fname, rets in filters.items():
                        aggregated[pname][fname].extend(rets)
    print(f"  完成 {time.time()-t0:.1f}s\n")

    pattern_labels = {
        '1_三隻烏鴉': '三隻烏鴉',
        '2_黃昏之星': '黃昏之星',
        '3_空頭吞噬': '空頭吞噬',
        '4_烏雲蓋頂': '烏雲蓋頂',
        '5_流星線': '流星線',
        '6_吊人線': '吊人線',
        '7_頂部十字星': '頂部十字星',
    }
    filter_labels = {
        'baseline': 'BASELINE (無 filter)',
        'a_RSI70': '+ RSI≥70',
        'b_RSI75': '+ RSI≥75',
        'c_close_below_EMA20': '+ 跌破 EMA20',
        'd_ADX_falling': '+ ADX 下降',
        'e_near_60d_high': '+ 距 60d 高<5%',
        'f_extended_SMA200': '+ 距 SMA200>25%',
        'g_volume_spike': '+ 量爆 (>1.5×)',
        'h_volume_dry': '+ 量縮 (<0.7×)',
        'i_RSI70_below_EMA20': '+ RSI≥70 + 跌破 EMA20',
        'j_RSI75_ADX_falling': '+ RSI≥75 + ADX 下降',
        'k_top_extended': '+ 距高<5% + 延伸>25%',
    }

    # 對每個型態列出 filter 結果
    for pkey, plabel in pattern_labels.items():
        if pkey not in aggregated: continue
        print("\n" + "=" * 100)
        print(f"📊 {plabel}：各 filter 30 天結果（樣本/跌機率%/均報%/Δ vs baseline）")
        print("=" * 100)
        base_m = metrics(aggregated[pkey].get('baseline', []))
        if not base_m:
            print("  baseline 樣本不足")
            continue
        base_dp = base_m['down_prob']
        base_mean = base_m['mean']
        rows = []
        for fkey, flabel in filter_labels.items():
            arr = aggregated[pkey].get(fkey, [])
            m = metrics(arr)
            if not m: continue
            dp_delta = m['down_prob'] - base_dp
            mean_delta = m['mean'] - base_mean
            rows.append((flabel, m, dp_delta, mean_delta))
        # 排序：依跌機率提升
        rows.sort(key=lambda x: -x[2])
        print(f"  {'Filter':<28} {'樣本':>6} {'跌%':>7} {'均報%':>9} "
              f"{'Δ跌%':>7} {'Δ均%':>8}")
        print("-" * 100)
        for flabel, m, dpd, md in rows:
            marker = ''
            if dpd > 5 and m['mean'] < -1: marker = ' 🔥'
            elif dpd > 3 and m['mean'] < 0: marker = ' ✓'
            elif dpd < -3: marker = ' ✗'
            base_tag = ' ⭐' if 'BASELINE' in flabel else ''
            print(f"  {flabel:<28} {m['n']:>6} {m['down_prob']:>+7.1f} "
                  f"{m['mean']:>+9.2f} {dpd:>+7.1f} {md:>+8.2f}{marker}{base_tag}")

    # 跨型態最佳 filter 排行
    print("\n" + "=" * 100)
    print("🏆 跨型態：最強 filter 組合（依 30d 均報越負越強）")
    print("=" * 100)
    print(f"  {'型態':<12} {'Filter':<28} {'樣本':>6} {'跌%':>7} {'均報%':>9}")
    print("-" * 100)
    all_combos = []
    for pkey in pattern_labels:
        for fkey in filter_labels:
            if fkey == 'baseline': continue
            arr = aggregated.get(pkey, {}).get(fkey, [])
            m = metrics(arr)
            if m and m['n'] >= 30:
                all_combos.append((pattern_labels[pkey], filter_labels[fkey], m))
    all_combos.sort(key=lambda x: x[2]['mean'])
    for plabel, flabel, m in all_combos[:15]:
        marker = ''
        if m['mean'] < -2: marker = ' 🔥'
        elif m['mean'] < 0: marker = ' ✓'
        print(f"  {plabel:<12} {flabel:<28} {m['n']:>6} "
              f"{m['down_prob']:>+7.1f} {m['mean']:>+9.2f}{marker}")

    # 寫 JSON
    out = {pkey: {fkey: metrics(arr) for fkey, arr in filters.items()}
           for pkey, filters in aggregated.items()}
    with open('bearish_patterns_filtered.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 bearish_patterns_filtered.json")


if __name__ == '__main__':
    main()
