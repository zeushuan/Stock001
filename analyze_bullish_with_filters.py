"""K 線底部反轉型態 + 配合指標研究
=========================================
找出哪些指標配合底部反轉型態能真正提升「即將反彈」的預測

Filter 條件：
  a. RSI ≤ 30（超賣）
  b. RSI ≤ 25（極度超賣）
  c. close > EMA20（已突破）
  d. ADX 5 日上升（趨勢轉強）
  e. 距 60d 低 < 5%（極低位）
  f. 距 SMA200 < -25%（過度跌深）
  g. 量爆 (vol > 1.5× 60d avg)
  h. 量縮 (vol < 0.7×)
  i. RSI≤30 AND close>EMA20（超賣+已突破）
  j. RSI≤25 AND ADX 上升（最強組合）
  k. 距低<5% AND 跌深>25%（雙底/接刀候選）
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

    drop_30d = np.zeros(n)
    for i in range(30, n):
        if c[i-30] > 0:
            drop_30d[i] = (c[i] - c[i-30]) / c[i-30] * 100
    is_low = drop_30d < -5

    rsi = df['rsi'].values if 'rsi' in df.columns else None
    adx = df['adx'].values if 'adx' in df.columns else None
    e20 = df['e20'].values if 'e20' in df.columns else None
    e60 = df['e60'].values if 'e60' in df.columns else None
    if rsi is None or adx is None or e20 is None or e60 is None:
        return {}

    l60 = np.array([c[max(0, i-60):i].min() if i >= 1 else c[i] for i in range(n)])
    sma200 = pd.Series(c).rolling(200, min_periods=100).mean().values
    vol60 = pd.Series(v).rolling(60, min_periods=30).mean().values

    triggers = {p: [] for p in ['1_鎚子', '2_倒鎚', '3_多頭吞噬',
                                 '4_旭日東昇', '5_啟明星', '6_紅三兵',
                                 '7_底部十字星']}

    for i in range(60, n - 1):
        if not is_low[i]: continue
        if any(np.isnan(x) for x in [rsi[i], adx[i], e20[i], e60[i]]): continue

        if rng[i] > 0:
            hammer = (lower[i] >= body[i] * 2.0) and (upper[i] < body[i] * 0.3) and (body[i] > 0.0001 * c[i])
            if hammer and drop_30d[i] < -8:
                triggers['1_鎚子'].append(i)
            inv_hammer = (upper[i] >= body[i] * 2.0) and (lower[i] < body[i] * 0.3) and (body[i] > 0.0001 * c[i])
            if inv_hammer and drop_30d[i] < -8:
                triggers['2_倒鎚'].append(i)
            doji = body[i] < rng[i] * 0.1
            if doji and drop_30d[i] < -8:
                triggers['7_底部十字星'].append(i)

        if i >= 1:
            if (is_red[i-1] and is_green[i]
                    and o[i] <= c[i-1] and c[i] >= o[i-1]
                    and body[i] > body[i-1]):
                triggers['3_多頭吞噬'].append(i)
            day1_big_red = is_red[i-1] and big[i-1]
            gap_down = o[i] < l[i-1]
            close_above_mid = is_green[i] and c[i] > (o[i-1] + c[i-1]) / 2 and c[i] < o[i-1]
            if day1_big_red and gap_down and close_above_mid:
                triggers['4_旭日東昇'].append(i)

        if i >= 2:
            if (is_red[i-2] and big[i-2]
                    and (body[i-1] < avg_body[i-1] * 0.5 if not np.isnan(avg_body[i-1]) else False)
                    and h[i-1] < c[i-2]
                    and is_green[i] and big[i]
                    and c[i] > (o[i-2] + c[i-2]) / 2):
                triggers['5_啟明星'].append(i)
            three_green = is_green[i-2] and is_green[i-1] and is_green[i]
            three_big = big[i-2] and big[i-1] and big[i]
            if three_green and three_big and o[i-1] > o[i-2] and o[i] > o[i-1] and c[i-1] > c[i-2] and c[i] > c[i-1]:
                triggers['6_紅三兵'].append(i)

    results = {p: {} for p in triggers}
    for pname, indices in triggers.items():
        filters_data = {
            'baseline': [],
            'a_RSI30': [], 'b_RSI25': [],
            'c_close_above_EMA20': [],
            'd_ADX_rising': [],
            'e_near_60d_low': [],
            'f_extended_down': [],
            'g_volume_spike': [],
            'h_volume_dry': [],
            'i_RSI30_above_EMA20': [],
            'j_RSI25_ADX_rising': [],
            'k_bottom_extended': [],
        }
        for idx in indices:
            if idx + 30 >= n: continue
            ret_30d = (c[idx + 30] - c[idx]) / c[idx] * 100

            r = rsi[idx]
            a = adx[idx]
            close_v = c[idx]
            e20_v = e20[idx]
            adx_5d = adx[idx-5] if idx >= 5 and not np.isnan(adx[idx-5]) else a
            adx_rising = a > adx_5d
            from_low_pct = (close_v - l60[idx]) / l60[idx] * 100 if l60[idx] > 0 else 99
            sma200_v = sma200[idx] if not np.isnan(sma200[idx]) else None
            extended_down = (close_v / sma200_v - 1) * 100 if sma200_v and sma200_v > 0 else 0
            vol60_v = vol60[idx] if not np.isnan(vol60[idx]) else 0
            vol_ratio = v[idx] / vol60_v if vol60_v > 0 else 1

            filters_data['baseline'].append(ret_30d)
            if r <= 30: filters_data['a_RSI30'].append(ret_30d)
            if r <= 25: filters_data['b_RSI25'].append(ret_30d)
            if close_v > e20_v: filters_data['c_close_above_EMA20'].append(ret_30d)
            if adx_rising: filters_data['d_ADX_rising'].append(ret_30d)
            if from_low_pct < 5: filters_data['e_near_60d_low'].append(ret_30d)
            if extended_down < -25: filters_data['f_extended_down'].append(ret_30d)
            if vol_ratio > 1.5: filters_data['g_volume_spike'].append(ret_30d)
            if vol_ratio < 0.7: filters_data['h_volume_dry'].append(ret_30d)
            if r <= 30 and close_v > e20_v:
                filters_data['i_RSI30_above_EMA20'].append(ret_30d)
            if r <= 25 and adx_rising:
                filters_data['j_RSI25_ADX_rising'].append(ret_30d)
            if from_low_pct < 5 and extended_down < -25:
                filters_data['k_bottom_extended'].append(ret_30d)
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
        'up_prob': float((a > 0).mean() * 100),
    }


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    print(f"🇹🇼 TW universe: {len(universe)} 檔\n")

    print("📊 偵測底部型態 + filter 條件...")
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
        '1_鎚子': '鎚子',
        '2_倒鎚': '倒鎚',
        '3_多頭吞噬': '多頭吞噬',
        '4_旭日東昇': '旭日東昇',
        '5_啟明星': '啟明星',
        '6_紅三兵': '紅三兵',
        '7_底部十字星': '底部十字星',
    }
    filter_labels = {
        'baseline': 'BASELINE',
        'a_RSI30': '+ RSI≤30 超賣',
        'b_RSI25': '+ RSI≤25 極超賣',
        'c_close_above_EMA20': '+ 突破 EMA20',
        'd_ADX_rising': '+ ADX 上升',
        'e_near_60d_low': '+ 距 60d 低<5%',
        'f_extended_down': '+ 距 SMA200<-25%',
        'g_volume_spike': '+ 量爆 (>1.5×)',
        'h_volume_dry': '+ 量縮 (<0.7×)',
        'i_RSI30_above_EMA20': '+ RSI≤30 + 突破 EMA20',
        'j_RSI25_ADX_rising': '+ RSI≤25 + ADX 上升',
        'k_bottom_extended': '+ 距低<5% + 跌深>25%',
    }

    for pkey, plabel in pattern_labels.items():
        if pkey not in aggregated: continue
        print("\n" + "=" * 100)
        print(f"📊 {plabel}：各 filter 30 天結果")
        print("=" * 100)
        base_m = metrics(aggregated[pkey].get('baseline', []))
        if not base_m: continue
        base_up = base_m['up_prob']
        base_mean = base_m['mean']
        rows = []
        for fkey, flabel in filter_labels.items():
            arr = aggregated[pkey].get(fkey, [])
            m = metrics(arr)
            if not m: continue
            up_delta = m['up_prob'] - base_up
            mean_delta = m['mean'] - base_mean
            rows.append((flabel, m, up_delta, mean_delta))
        rows.sort(key=lambda x: -x[2])
        print(f"  {'Filter':<28} {'樣本':>6} {'漲%':>7} {'均報%':>9} "
              f"{'Δ漲%':>7} {'Δ均%':>8}")
        print("-" * 100)
        for flabel, m, upd, md in rows:
            marker = ''
            if upd > 5 and m['mean'] > 5: marker = ' 🔥'
            elif upd > 3 and m['mean'] > 0: marker = ' ✓'
            elif upd < -3: marker = ' ✗'
            base_tag = ' ⭐' if 'BASELINE' in flabel else ''
            print(f"  {flabel:<28} {m['n']:>6} {m['up_prob']:>+7.1f} "
                  f"{m['mean']:>+9.2f} {upd:>+7.1f} {md:>+8.2f}{marker}{base_tag}")

    print("\n" + "=" * 100)
    print("🏆 跨型態：最強 filter 組合（依 30d 均報越高越強）")
    print("=" * 100)
    print(f"  {'型態':<12} {'Filter':<28} {'樣本':>6} {'漲%':>7} {'均報%':>9}")
    print("-" * 100)
    all_combos = []
    for pkey in pattern_labels:
        for fkey in filter_labels:
            if fkey == 'baseline': continue
            arr = aggregated.get(pkey, {}).get(fkey, [])
            m = metrics(arr)
            if m and m['n'] >= 30:
                all_combos.append((pattern_labels[pkey], filter_labels[fkey], m))
    all_combos.sort(key=lambda x: -x[2]['mean'])
    for plabel, flabel, m in all_combos[:15]:
        marker = ''
        if m['mean'] > 7: marker = ' 🔥'
        elif m['mean'] > 5: marker = ' ✓'
        print(f"  {plabel:<12} {flabel:<28} {m['n']:>6} "
              f"{m['up_prob']:>+7.1f} {m['mean']:>+9.2f}{marker}")

    out = {pkey: {fkey: metrics(arr) for fkey, arr in filters.items()}
           for pkey, filters in aggregated.items()}
    with open('bullish_patterns_filtered.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 bullish_patterns_filtered.json")


if __name__ == '__main__':
    main()
