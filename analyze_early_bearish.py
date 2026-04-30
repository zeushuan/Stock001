"""早期看空訊號研究（v9.11）
==================================
目的：找出比「三隻烏鴉 / 空頭吞噬 / 黃昏之星」更早期的看空訊號。
參考案例：4755（多頭中段，無極端 K 線，但連續 lower high + EMA20 轉平 → 跌 10%）

測試 7 個早期看空 filter（必要前提：is_bull = EMA20>EMA60，否則訊號意義不大）：
  E1: 連 3 天 lower high（高點 t > t-1 > t-2）
  E2: 連 3 天 close < open（連紅 3 K）
  E3: 量增價跌 3 連（紅 K + vol > 60d avg） 連 3 天
  E4: EMA20 5d 斜率轉負（之前上升，今天開始下降）
  E5: 連 5 天 close < EMA10（短期動能崩）
  E6: MACD hist 3 天連續縮短（多頭動能衰退）
  E7: from_high < 5% + RSI≥65 + 連紅 2 K（高位+偏熱+轉弱）

對所有訊號計 30d 後報酬，看：
  - 跌機率（hit rate for bearish）
  - 平均報酬（負 = 好的看空訊號）
  - 比較跟現有 baseline 隨機差多少
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
    """對單檔股票，每天每個 filter 檢查觸發。記錄 30d 後 ret"""
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return None
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        if len(df) < 60: return None

        o = df['Open'].values
        h = df['High'].values
        l = df['Low'].values
        c = df['Close'].values
        v = df['Volume'].values
        e10 = df['e10'].values if 'e10' in df.columns else None
        e20 = df['e20'].values if 'e20' in df.columns else None
        e60 = df['e60'].values if 'e60' in df.columns else None
        rsi = df['rsi'].values if 'rsi' in df.columns else None
        if any(x is None for x in [e10, e20, e60, rsi]):
            return None
        n = len(df)
        vol60 = pd.Series(v).rolling(60, min_periods=30).mean().values
        # MACD: 用 EMA12 - EMA26 近似（如果有就用）
        macd_hist = None  # 簡化，暫不算

        result = {f'E{i}': [] for i in range(1, 8)}
        result['baseline_bull'] = []  # 多頭排列時的 baseline（無 filter）

        for t in range(60, n - HOLD - 1):
            # 必要前提：仍多頭排列（否則早期看空意義不大）
            if any(np.isnan(x) for x in [e20[t], e60[t], rsi[t], e10[t]]): continue
            if e20[t] <= e60[t]: continue

            # 計 30d 後 ret
            ent = o[t+1]; exi = o[t+1+HOLD]
            if ent is None or exi is None: continue
            if np.isnan(ent) or np.isnan(exi): continue
            if ent <= 0 or exi <= 0: continue
            ret = (exi - ent) / ent  # 不扣 cost（純研究訊號方向）

            result['baseline_bull'].append(ret)

            # E1: 連 3 天 lower high
            if t >= 2 and h[t] < h[t-1] < h[t-2]:
                result['E1'].append(ret)

            # E2: 連 3 天 close < open（連紅）
            if t >= 2 and all(c[k] < o[k] for k in [t, t-1, t-2]):
                result['E2'].append(ret)

            # E3: 量增價跌 3 連
            if t >= 2:
                cond = True
                for k in [t, t-1, t-2]:
                    if not (c[k] < o[k] and not np.isnan(vol60[k]) and vol60[k] > 0
                            and v[k] > vol60[k]):
                        cond = False; break
                if cond:
                    result['E3'].append(ret)

            # E4: EMA20 5d 斜率轉負（之前上升，今天開始下降）
            if t >= 10 and not np.isnan(e20[t-5]) and not np.isnan(e20[t-10]):
                # 之前上升：e20[t-5] > e20[t-10]
                # 現在下降：e20[t] < e20[t-5]
                if e20[t-5] > e20[t-10] and e20[t] < e20[t-5]:
                    result['E4'].append(ret)

            # E5: 連 5 天 close < EMA10
            if t >= 4:
                cond = True
                for k in range(t-4, t+1):
                    if np.isnan(e10[k]) or c[k] >= e10[k]:
                        cond = False; break
                if cond:
                    result['E5'].append(ret)

            # E6: MACD hist 3 天連續縮短（簡化：用 close 動能近似）
            # close - close[t-5] 連 3 天縮短
            if t >= 7:
                m_t = c[t] - c[t-5]
                m_tm1 = c[t-1] - c[t-6]
                m_tm2 = c[t-2] - c[t-7]
                # 三個值都 > 0（仍多頭動能）但持續縮短
                if m_t > 0 and m_tm1 > 0 and m_tm2 > 0 and m_t < m_tm1 < m_tm2:
                    result['E6'].append(ret)

            # E7: from_high < 5% + RSI≥65 + 連紅 2 K
            high60 = h[max(0, t-60):t+1].max()
            from_high_pct = (high60 - c[t]) / high60 * 100 if high60 > 0 else 99
            if (from_high_pct < 5 and rsi[t] >= 65 and t >= 1
                    and c[t] < o[t] and c[t-1] < o[t-1]):
                result['E7'].append(ret)

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
    print(f"  目標：找早期看空訊號（在多頭排列下）")
    print()

    print(f"📊 跑 7 個 filter（{WORKERS} workers）...")
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

    # 計算
    print("=" * 100)
    print(f"{'Filter':>8}{'n':>10}{'Down%':>8}{'Mean':>9}{'Median':>9}{'Std':>8}{'PF(bear)':>10}{'說明'}")
    print("=" * 100)

    descriptions = {
        'baseline_bull': '多頭排列基準（任何天）',
        'E1': '連 3 天 lower high',
        'E2': '連 3 天連紅 K',
        'E3': '量增價跌 3 連',
        'E4': 'EMA20 5d 斜率轉負',
        'E5': '連 5 天 close < EMA10',
        'E6': '動能 3 天連續衰退（multi-MOM）',
        'E7': '高位 5% + RSI≥65 + 連 2 紅',
    }

    rows = []
    for k in ['baseline_bull', 'E1', 'E2', 'E3', 'E4', 'E5', 'E6', 'E7']:
        rets = aggregated.get(k, [])
        if len(rets) < 30:
            print(f"{k:>8}{len(rets):>10}  (樣本 < 30，略過)")
            continue
        a = np.array(rets)
        n = len(a)
        down_pct = (a < 0).mean() * 100  # 跌機率
        mean = a.mean() * 100
        median = np.median(a) * 100
        std = a.std() * 100
        # Profit factor for bear: 跌的總和 / 漲的總和
        pos_sum = a[a > 0].sum()
        neg_sum = -a[a < 0].sum() if (a < 0).any() else 0.001
        pf_bear = neg_sum / pos_sum if pos_sum > 0 else 999

        marker = ''
        if k != 'baseline_bull':
            base = aggregated.get('baseline_bull', [])
            if base:
                base_mean = np.mean(base) * 100
                base_down = (np.array(base) < 0).mean() * 100
                if mean < base_mean - 1 and down_pct > base_down + 3:
                    marker = ' 🚀'
                elif mean < base_mean - 0.5:
                    marker = ' ★'

        print(f"{k:>8}{n:>10}{down_pct:>7.1f}%{mean:>+8.2f}%{median:>+8.2f}%{std:>7.2f}%{pf_bear:>10.2f}  {descriptions[k]}{marker}")
        rows.append({
            'filter': k,
            'desc': descriptions[k],
            'n': n,
            'down_pct': float(down_pct),
            'mean_pct': float(mean),
            'median_pct': float(median),
            'std_pct': float(std),
            'pf_bear': float(pf_bear),
        })

    print()
    base = next((r for r in rows if r['filter'] == 'baseline_bull'), None)
    if base:
        print(f"基準（多頭排列任意天）: n={base['n']}, 跌機率 {base['down_pct']:.1f}%, 平均 {base['mean_pct']:+.2f}%")
        print()
        print("與基準比較：早期看空 alpha")
        for r in rows:
            if r['filter'] == 'baseline_bull': continue
            d_mean = r['mean_pct'] - base['mean_pct']
            d_down = r['down_pct'] - base['down_pct']
            sig = '🚀' if (d_mean < -1 and d_down > 3) else '⚪' if abs(d_mean) < 0.5 else '🔻'
            note = ''
            if d_mean < -1.5: note = '（強看空訊號）'
            elif d_mean < -0.5: note = '（弱看空訊號）'
            print(f"  {sig} {r['filter']:<5} ({r['desc']:<25}): "
                  f"Δmean={d_mean:+.2f}%  Δdown={d_down:+.1f}%  n={r['n']}{note}")

    # 找最強的 1-2 個 filter，建議加進系統
    print()
    print("=" * 100)
    print("🎯 推薦：加進 _detect_alerts 的早期看空訊號")
    print("=" * 100)
    if base:
        candidates = [r for r in rows if r['filter'] != 'baseline_bull'
                       and r['mean_pct'] - base['mean_pct'] < -1.0]
        candidates.sort(key=lambda r: r['mean_pct'])
        for c in candidates[:3]:
            print(f"  ✅ {c['filter']} ({c['desc']}): mean {c['mean_pct']:+.2f}% (vs base {base['mean_pct']:+.2f}%, Δ {c['mean_pct']-base['mean_pct']:+.2f}%)")
        if not candidates:
            print("  ⚠️ 沒有 filter 比 baseline 顯著差（差 ≥ 1%）— 早期看空 alpha 弱")

    out = {'rows': rows, 'baseline': base}
    with open('analyze_early_bearish_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 analyze_early_bearish_results.json")


if __name__ == '__main__':
    main()
