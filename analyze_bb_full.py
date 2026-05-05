"""BB 全套判斷研究（v9.12）— 看哪些有 alpha
=======================================================
測 OANDA BB 文章 8 種主要判斷在多頭 / 空頭排列下的後續 30d 報酬。
"""
import sys, time, json
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
from bb_signals import (compute_bb, is_squeeze, is_expansion,
                          is_walking_up, is_walking_down,
                          pct_b_extreme_high, pct_b_extreme_low,
                          mean_reversion_high, mean_reversion_low,
                          is_w_bottom, is_m_top)

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
        h = df['High'].values
        l = df['Low'].values
        o = df['Open'].values
        n = len(df)

        bb = compute_bb(c)
        sma = bb['sma']; bbu = bb['bbu']; bbl = bb['bbl']
        bandwidth = bb['bandwidth']; pct_b = bb['pct_b']

        e20 = df['e20'].values if 'e20' in df.columns else None
        e60 = df['e60'].values if 'e60' in df.columns else None
        if e20 is None or e60 is None: return None

        # 兩組：multi頭 vs 空頭
        result = defaultdict(list)

        for t in range(120, n - HOLD - 1):
            if any(np.isnan(x) for x in [e20[t], e60[t]]): continue
            ent = o[t+1]; exi = o[t+1+HOLD]
            if np.isnan(ent) or np.isnan(exi): continue
            if ent <= 0 or exi <= 0: continue
            ret = (exi - ent) / ent

            is_bull = e20[t] > e60[t]
            regime = 'bull' if is_bull else 'bear'
            result[f'{regime}_baseline'].append(ret)

            # 8 個判斷
            if is_squeeze(bandwidth, t):
                result[f'{regime}_squeeze'].append(ret)
            if is_expansion(bandwidth, t):
                result[f'{regime}_expansion'].append(ret)
            if is_walking_up(c, sma, bbu, t):
                result[f'{regime}_walk_up'].append(ret)
            if is_walking_down(c, sma, bbl, t):
                result[f'{regime}_walk_down'].append(ret)
            if pct_b_extreme_high(pct_b, t):
                result[f'{regime}_pctb_high'].append(ret)
            if pct_b_extreme_low(pct_b, t):
                result[f'{regime}_pctb_low'].append(ret)
            if mean_reversion_high(c, sma, bbu, pct_b, t):
                result[f'{regime}_mean_rev_high'].append(ret)
            if mean_reversion_low(c, sma, bbl, pct_b, t):
                result[f'{regime}_mean_rev_low'].append(ret)
            if is_w_bottom(l, c, bbl, sma, t):
                result[f'{regime}_w_bottom'].append(ret)
            if is_m_top(h, c, bbu, sma, t):
                result[f'{regime}_m_top'].append(ret)

        return dict(result)
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
    print(f"🇹🇼 Universe: {len(universe)} 檔, hold={HOLD}d\n")

    print(f"📊 跑 BB 全套 8 種判斷 × 多/空頭...")
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

    # 顯示
    desc = {
        'baseline': '基準（任意天）',
        'squeeze': 'BB Squeeze 5天連窄',
        'expansion': 'BB Expansion 突放',
        'walk_up': 'Walking Up Band',
        'walk_down': 'Walking Down Band',
        'pctb_high': '%B > 1.0（過熱）',
        'pctb_low': '%B < 0（過冷）',
        'mean_rev_high': '從上方回中軌',
        'mean_rev_low': '從下方回中軌',
        'w_bottom': 'W 底（雙觸下軌）',
        'm_top': 'M 頂（雙觸上軌）',
    }

    rows = []
    for regime, regime_label in [('bull', '🐂 多頭排列'), ('bear', '🐻 空頭排列')]:
        print(f"{'='*100}")
        print(f"{regime_label} 下的 BB 判斷")
        print(f"{'='*100}")
        print(f"{'Variant':>20}{'n':>10}{'Win%':>8}{'Mean':>9}{'Median':>9}{'Std':>8}{'PF':>6}  說明")
        print("-" * 100)

        baseline_key = f'{regime}_baseline'
        baseline_rets = aggregated.get(baseline_key, [])
        if baseline_rets:
            base_mean = np.mean(baseline_rets) * 100
        else:
            base_mean = 0

        for sig_name, label in desc.items():
            k = f'{regime}_{sig_name}'
            rets = aggregated.get(k, [])
            if len(rets) < 30:
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
            d_mean = mean - base_mean
            marker = ''
            if sig_name != 'baseline':
                if d_mean > 1.5: marker = ' 🚀'
                elif d_mean > 0.5: marker = ' ★'
                elif d_mean < -1.5: marker = ' 🔻'
            print(f"  {sig_name:>18}{n:>10}{win:>7.1f}%{mean:>+8.2f}%{median:>+8.2f}%{std:>7.2f}%{pf:>6.2f}  {label}{marker}")
            rows.append({
                'regime': regime, 'signal': sig_name, 'desc': label,
                'n': n, 'win_pct': float(win), 'mean_pct': float(mean),
                'median_pct': float(median), 'std_pct': float(std), 'pf': float(pf),
                'delta_vs_base': float(d_mean),
            })
        print()

    # 找最強
    print("=" * 100)
    print("🏆 最強訊號（Δmean vs baseline > +1%，且 n ≥ 100）")
    print("=" * 100)
    strong = [r for r in rows if r['delta_vs_base'] > 1.0 and r['n'] >= 100]
    strong.sort(key=lambda r: -r['delta_vs_base'])
    for r in strong:
        print(f"  ✅ {r['regime']}_{r['signal']:<18} Δmean={r['delta_vs_base']:+.2f}%  "
              f"win={r['win_pct']:.1f}%  n={r['n']}  {r['desc']}")
    if not strong:
        print("  ⚠️ 沒有單一訊號 alpha > 1%")

    # 找最差（看空訊號）
    print()
    print("🔻 最差訊號（Δmean < -1%，潛在看空訊號）")
    bad = [r for r in rows if r['delta_vs_base'] < -1.0 and r['n'] >= 100]
    bad.sort(key=lambda r: r['delta_vs_base'])
    for r in bad:
        print(f"  🔻 {r['regime']}_{r['signal']:<18} Δmean={r['delta_vs_base']:+.2f}%  "
              f"win={r['win_pct']:.1f}%  n={r['n']}  {r['desc']}")
    if not bad:
        print("  ⚠️ 沒有單一訊號 alpha < -1%")

    with open('analyze_bb_full_results.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 analyze_bb_full_results.json")


if __name__ == '__main__':
    main()
