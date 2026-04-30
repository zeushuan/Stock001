"""T1 提前進場研究：在 EMA20 cross 發生前 1-3 天進場，比 T1 baseline 好嗎？
========================================================================
T1 baseline: 等 close 上穿 EMA20（cross_days=1）才進場
Early entry: 在 cross 還沒發生（close 仍 < EMA20）但「即將發生」時提前進場

研究問題：
  Q1: 提前 1-3 天進場能多賺多少？（捕捉 cross-day 的爆發）
  Q2: 哪種「即將 cross」訊號最準？（filter 的 precision）
  Q3: 風險如何？（誤判：進場後沒 cross，反而下跌）

5 個提前進場 filter（在 t-1/t-2/t-3 檢查）：
  P1: close 距 EMA20 ≤ 1%（很接近）+ close 連 2 天上升
  P2: close 距 EMA20 ≤ 1% + ADX ≥ 22（趨勢已強）
  P3: close 距 EMA20 ≤ 0.5%（極接近）
  P4: close 距 EMA20 ≤ 1% + 成交量 > 60d 平均 1.2×
  P5: close 距 EMA20 ≤ 1% + ADX ≥ 22 + ADX 5d 上升（最嚴格）

對比基準：
  B0: T1 baseline（cross_days=1，既有條件）
"""
import sys, time, json
from pathlib import Path
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
COST_RT = 0.00671  # 與 backtest_strategy.py 一致


def analyze_one(ticker):
    """對單檔股票，找：
       - 所有 T1 cross 事件（cross_days=1, ADX≥22, bull）
       - 每個事件回看 1-3 天，記錄那天的 features
       - 各 filter 觸發點 → 看後續 30d 報酬"""
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return None
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        if len(df) < 60: return None

        c = df['Close'].values
        o = df['Open'].values
        v = df['Volume'].values
        e20 = df['e20'].values if 'e20' in df.columns else None
        e60 = df['e60'].values if 'e60' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        if e20 is None or e60 is None or adx is None:
            return None

        n = len(df)
        vol60 = pd.Series(v).rolling(60, min_periods=30).mean().values

        # Find T1 trigger days: close[t-1] < e20[t-1] AND close[t] > e20[t] AND e20>e60 AND adx≥22
        # 簡化：cross_days=1 視為 close 從 below 變 above 的第一天
        t1_days = []  # i 是 cross 那天
        for i in range(60, n - HOLD):
            if any(np.isnan(x) for x in [e20[i], e60[i], adx[i], e20[i-1]]):
                continue
            if c[i-1] >= e20[i-1]: continue   # 昨天已在 EMA20 上 → 不算新 cross
            if c[i] <= e20[i]: continue        # 今天沒上穿 → 不算
            if e20[i] <= e60[i]: continue      # 不是多頭排列
            if adx[i] < 22: continue
            t1_days.append(i)

        # 每個 T1 day i 回看 1/2/3 天前，並算 30d ret
        # 結構：對每個 lookback (1/2/3) 天，記錄 t-lookback 的 features 和 30d return（從 t-lookback 進場）
        result = {
            'baseline_T1': [],   # 從 T1 day i 次日 Open 進場，30d 後 Open 出場
        }
        for lb in [1, 2, 3]:
            result[f'P1_t-{lb}'] = []
            result[f'P2_t-{lb}'] = []
            result[f'P3_t-{lb}'] = []
            result[f'P4_t-{lb}'] = []
            result[f'P5_t-{lb}'] = []

        for i in t1_days:
            # T1 baseline: 進場 i+1 open, 出場 i+1+HOLD open
            if i + 1 + HOLD < n:
                ent = o[i+1]; exi = o[i+1+HOLD]
                if ent > 0 and exi > 0:
                    ret = (exi - ent) / ent - COST_RT
                    result['baseline_T1'].append(ret)

            # 對每個 lookback，看 t-lb 那天的 features（j = i - lb）
            for lb in [1, 2, 3]:
                j = i - lb
                if j < 5: continue
                if any(np.isnan(x) for x in [e20[j], e60[j], adx[j]]): continue
                if c[j] >= e20[j]: continue   # t-lb 已在 EMA20 上 → 不是 pre-cross
                if e20[j] <= e60[j]: continue # 不是多頭排列

                dist_to_e20 = (e20[j] - c[j]) / e20[j] * 100  # 距 EMA20 多少 %
                rising_2d = (c[j] > c[j-1] and c[j-1] > c[j-2])
                vol_ratio = v[j] / vol60[j] if vol60[j] > 0 else 1
                adx_5d = adx[j-5] if j >= 5 and not np.isnan(adx[j-5]) else adx[j]
                adx_rising = adx[j] > adx_5d

                # 進場 j+1 open，出場 j+1+HOLD open
                if j + 1 + HOLD >= n: continue
                ent = o[j+1]; exi = o[j+1+HOLD]
                if ent <= 0 or exi <= 0: continue
                ret = (exi - ent) / ent - COST_RT

                # P1: 距 EMA20 ≤ 1% + 2 天連漲
                if dist_to_e20 <= 1.0 and rising_2d:
                    result[f'P1_t-{lb}'].append(ret)
                # P2: 距 ≤ 1% + ADX ≥ 22
                if dist_to_e20 <= 1.0 and adx[j] >= 22:
                    result[f'P2_t-{lb}'].append(ret)
                # P3: 距 ≤ 0.5%（極接近）
                if dist_to_e20 <= 0.5:
                    result[f'P3_t-{lb}'].append(ret)
                # P4: 距 ≤ 1% + 量爆
                if dist_to_e20 <= 1.0 and vol_ratio > 1.2:
                    result[f'P4_t-{lb}'].append(ret)
                # P5: 距 ≤ 1% + ADX ≥ 22 + ADX 上升
                if dist_to_e20 <= 1.0 and adx[j] >= 22 and adx_rising:
                    result[f'P5_t-{lb}'].append(ret)

        return result
    except Exception:
        return None


def main():
    DATA = Path('data_cache')
    universe = sorted([
        p.stem for p in DATA.glob('*.parquet')
        if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
        and not p.stem.startswith('00')
    ])
    print(f"🇹🇼 Universe: {len(universe)} 檔")
    print(f"  期間: {START_DATE} → 現在")
    print(f"  Hold: {HOLD} 天，cost: {COST_RT*100:.2f}%")
    print()

    print(f"📊 跑 T1 提前進場分析（{WORKERS} workers）...")
    t0 = time.time()
    aggregated = defaultdict(list)
    n_processed = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for r in ex.map(analyze_one, universe, chunksize=50):
            if r is not None:
                n_processed += 1
                for k, v in r.items():
                    aggregated[k].extend(v)
    print(f"  完成 {time.time()-t0:.1f}s ({n_processed}/{len(universe)} 檔有資料)")
    print()

    # 統計
    print("=" * 90)
    print(f"{'Filter':<22}{'n':>6}{'Win%':>8}{'Mean':>10}{'Median':>10}{'std':>8}{'PF':>7}")
    print("=" * 90)

    rows = []
    for filter_name in ['baseline_T1'] + [f'{p}_t-{lb}' for p in ['P1','P2','P3','P4','P5'] for lb in [1,2,3]]:
        rets = aggregated.get(filter_name, [])
        if len(rets) < 10:
            print(f"  {filter_name:<22}{len(rets):>6}  (樣本 < 10，略過)")
            continue
        a = np.array(rets)
        mean = a.mean() * 100
        median = np.median(a) * 100
        std = a.std() * 100
        win = (a > 0).mean() * 100
        pos_sum = a[a > 0].sum()
        neg_sum = -a[a < 0].sum() if (a < 0).any() else 0.001
        pf = pos_sum / neg_sum if neg_sum > 0 else 999
        marker = ' ★' if (filter_name != 'baseline_T1' and mean > 8) else ''
        print(f"  {filter_name:<22}{len(rets):>6}{win:>7.1f}%{mean:>+9.2f}%{median:>+9.2f}%{std:>7.2f}%{pf:>7.2f}{marker}")
        rows.append({
            'filter': filter_name,
            'n': len(rets),
            'win_rate_pct': round(win, 2),
            'mean_pct': round(mean, 2),
            'median_pct': round(median, 2),
            'std_pct': round(std, 2),
            'profit_factor': round(pf, 2),
        })

    # 找 best
    sorted_rows = sorted(rows, key=lambda r: r['mean_pct'], reverse=True)
    print()
    print("🏆 平均淨報酬前 5 名:")
    for r in sorted_rows[:5]:
        print(f"  {r['filter']:<22} mean={r['mean_pct']:+.2f}%  n={r['n']}  win={r['win_rate_pct']}%  PF={r['profit_factor']}")

    # 比較與 baseline
    base = next((r for r in rows if r['filter'] == 'baseline_T1'), None)
    if base:
        print()
        print(f"📋 與 baseline T1 (n={base['n']}, mean={base['mean_pct']:+.2f}%, win={base['win_rate_pct']}%) 的差異:")
        for r in rows:
            if r['filter'] == 'baseline_T1': continue
            delta_mean = r['mean_pct'] - base['mean_pct']
            delta_win = r['win_rate_pct'] - base['win_rate_pct']
            sig = '🚀' if delta_mean > 2 else '⚪' if abs(delta_mean) < 1 else '🔻'
            print(f"  {sig} {r['filter']:<22} Δmean={delta_mean:+.2f}%  Δwin={delta_win:+.1f}%  (n={r['n']})")

    out = {'config': {'hold': HOLD, 'start': START_DATE, 'cost_rt': COST_RT, 'universe': len(universe)},
           'results': rows}
    with open('analyze_t1_preentry_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 analyze_t1_preentry_results.json")


if __name__ == '__main__':
    main()
