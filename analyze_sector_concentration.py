"""產業集中度檢查（v9.11）
=================================
目的：驗證 backtest 訊號是否集中在少數產業，特別是 2025 倒鎚爆發年。

問題：
  Q1: 2025 倒鎚 498 訊號是否集中在半導體/電子？
  Q2: T1_V7 5879 訊號是否分散？
  Q3: 若拆出半導體/電子，剩下訊號 alpha 還夠嗎？
  Q4: 「最強年」（2025、2021）是否都是同一產業驅動的？

如果集中度過高 → 投組需強制產業分散，否則隱性產業風險。
"""
import sys, json, time
from pathlib import Path
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
from backtest_strategy import (detect_inv_hammer_signals, detect_t1_v7_signals,
                                START_DATE, COST_ROUND_TRIP, get_universe)

WORKERS = 16
HOLD = 30


def load_industry_map():
    """ticker → industry"""
    d = json.load(open('tw_stock_list.json', encoding='utf-8'))
    return {t: info.get('industry', '') or '(其他)'
            for t, info in d.items() if isinstance(info, dict)}


def gen_trades_one(args):
    ticker, strategy = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return []
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        if len(df) < 60: return []

        if strategy == 'inv_hammer':
            sigs = detect_inv_hammer_signals(df)
        else:
            sigs = detect_t1_v7_signals(df)

        o = df['Open'].values
        idx = df.index
        n = len(df)
        trades = []
        for i in sigs:
            if i + 1 + HOLD >= n: continue
            ent = float(o[i+1]); exi = float(o[i+1+HOLD])
            if ent <= 0 or exi <= 0: continue
            if np.isnan(ent) or np.isnan(exi): continue
            ret = (exi - ent) / ent - COST_ROUND_TRIP
            trades.append({
                'ticker': ticker,
                'year': idx[i+1].year,
                'date': idx[i+1].strftime('%Y-%m-%d'),
                'net_ret': ret,
            })
        return trades
    except Exception:
        return []


def run(strategy='inv_hammer'):
    universe = get_universe('tw')
    industry_map = load_industry_map()
    print(f"🇹🇼 Universe: {len(universe)} 檔, Industry map: {len(industry_map)} 檔")

    print(f"\n📊 跑 {strategy} 訊號（{WORKERS} workers）...")
    t0 = time.time()
    all_trades = []
    args = [(t, strategy) for t in universe]
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for trades in ex.map(gen_trades_one, args, chunksize=50):
            all_trades.extend(trades)
    print(f"  完成 {time.time()-t0:.1f}s，共 {len(all_trades)} 筆")
    if not all_trades:
        return

    # 加 industry
    for t in all_trades:
        t['industry'] = industry_map.get(t['ticker'], '(未知)')

    df = pd.DataFrame(all_trades)

    # ── 1. 整體產業分布 ──
    print(f"\n{'='*70}")
    print(f"📊 {strategy} — 整體產業集中度（{len(df)} 筆）")
    print(f"{'='*70}")
    ind_total = df['industry'].value_counts()
    print(f"前 10 產業:")
    for i, (ind, n) in enumerate(ind_total.head(10).items(), 1):
        pct = n / len(df) * 100
        sub = df[df['industry'] == ind]
        win_pct = (sub['net_ret'] > 0).mean() * 100
        mean_ret = sub['net_ret'].mean() * 100
        print(f"  {i:2d}. {ind:<20} {n:5d} ({pct:5.1f}%)  win={win_pct:.1f}%  mean={mean_ret:+.2f}%")
    top3 = ind_total.head(3).sum() / len(df) * 100
    top5 = ind_total.head(5).sum() / len(df) * 100
    print(f"  → 前 3 產業 {top3:.1f}% / 前 5 產業 {top5:.1f}%")

    # ── 2. 逐年產業 top 3 ──
    print(f"\n{'='*70}")
    print(f"📅 逐年產業 top 3")
    print(f"{'='*70}")
    for year in sorted(df['year'].unique()):
        sub = df[df['year'] == year]
        ind_year = sub['industry'].value_counts()
        win_year = (sub['net_ret'] > 0).mean() * 100
        mean_year = sub['net_ret'].mean() * 100
        print(f"\n  {year} (n={len(sub)}, win={win_year:.1f}%, mean={mean_year:+.2f}%):")
        for i, (ind, n) in enumerate(ind_year.head(3).items(), 1):
            pct = n / len(sub) * 100
            ind_sub = sub[sub['industry'] == ind]
            ind_win = (ind_sub['net_ret'] > 0).mean() * 100
            ind_mean = ind_sub['net_ret'].mean() * 100
            print(f"    {i}. {ind:<20} {n:4d} ({pct:5.1f}%)  win={ind_win:.1f}%  mean={ind_mean:+.2f}%")

    # ── 3. 拆掉電子 + 半導體後剩餘 alpha ──
    print(f"\n{'='*70}")
    print(f"🔬 拆掉「電子+半導體系列」後剩餘 alpha")
    print(f"{'='*70}")
    electronics = ['半導體業', '光電業', '電腦及週邊設備業', '電子零組件業',
                    '其他電子業', '通信網路業', '電子通路業', '資訊服務業']
    df['is_electronics'] = df['industry'].isin(electronics)

    elec = df[df['is_electronics']]
    non_elec = df[~df['is_electronics']]
    elec_pct = len(elec) / len(df) * 100
    print(f"  電子+半導體系列: n={len(elec)} ({elec_pct:.1f}%)  "
          f"win={(elec['net_ret']>0).mean()*100:.1f}%  mean={elec['net_ret'].mean()*100:+.2f}%")
    print(f"  非電子（傳產+金融+生技等）: n={len(non_elec)} ({100-elec_pct:.1f}%)  "
          f"win={(non_elec['net_ret']>0).mean()*100:.1f}%  mean={non_elec['net_ret'].mean()*100:+.2f}%")

    # 各年電子比例
    print(f"\n  逐年「電子比例」:")
    for year in sorted(df['year'].unique()):
        sub = df[df['year'] == year]
        e = sub['is_electronics'].sum()
        e_pct = e / len(sub) * 100
        e_sub = sub[sub['is_electronics']]
        ne_sub = sub[~sub['is_electronics']]
        e_win = (e_sub['net_ret']>0).mean()*100 if len(e_sub) > 0 else 0
        ne_win = (ne_sub['net_ret']>0).mean()*100 if len(ne_sub) > 0 else 0
        e_mean = e_sub['net_ret'].mean()*100 if len(e_sub) > 0 else 0
        ne_mean = ne_sub['net_ret'].mean()*100 if len(ne_sub) > 0 else 0
        print(f"    {year}: 電子 {e}/{len(sub)} ({e_pct:.1f}%) "
              f"win={e_win:.1f}%/{ne_win:.1f}% mean={e_mean:+.2f}%/{ne_mean:+.2f}%")

    # ── 4. 半導體單獨拆出 ──
    print(f"\n{'='*70}")
    print(f"🔬 半導體業單獨")
    print(f"{'='*70}")
    semi = df[df['industry'] == '半導體業']
    non_semi = df[df['industry'] != '半導體業']
    semi_pct = len(semi) / len(df) * 100
    print(f"  半導體業: n={len(semi)} ({semi_pct:.1f}%)  "
          f"win={(semi['net_ret']>0).mean()*100:.1f}%  mean={semi['net_ret'].mean()*100:+.2f}%")
    print(f"  非半導體: n={len(non_semi)} ({100-semi_pct:.1f}%)  "
          f"win={(non_semi['net_ret']>0).mean()*100:.1f}%  mean={non_semi['net_ret'].mean()*100:+.2f}%")

    # 結論
    print(f"\n{'='*70}")
    print(f"📋 結論")
    print(f"{'='*70}")
    if elec_pct > 60:
        print(f"  🚨 電子系列佔 {elec_pct:.0f}% — 嚴重集中！需強制產業分散")
    elif elec_pct > 45:
        print(f"  ⚠️ 電子系列佔 {elec_pct:.0f}% — 中度集中，建議產業分散")
    else:
        print(f"  ✅ 電子系列佔 {elec_pct:.0f}% — 集中度尚可")
    diff = elec['net_ret'].mean() - non_elec['net_ret'].mean()
    print(f"  電子 vs 非電子 alpha 差距: {diff*100:+.2f}% (電子贏 {diff>0})")

    out = {
        'strategy': strategy,
        'total_trades': len(df),
        'electronics_pct': float(elec_pct),
        'semi_pct': float(semi_pct),
        'elec_alpha_pct': float(elec['net_ret'].mean() * 100),
        'nonelec_alpha_pct': float(non_elec['net_ret'].mean() * 100),
        'top10_industries': [
            {'industry': k, 'n': int(v),
             'win_pct': float((df[df['industry']==k]['net_ret'] > 0).mean() * 100),
             'mean_ret_pct': float(df[df['industry']==k]['net_ret'].mean() * 100)}
            for k, v in ind_total.head(10).items()
        ],
    }
    out_file = f'analyze_sector_concentration_{strategy}.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 {out_file}")


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--strategy', default='inv_hammer', choices=['inv_hammer', 't1_v7'])
    p.add_argument('--all', action='store_true')
    args = p.parse_args()
    if args.all:
        for s in ['inv_hammer', 't1_v7']:
            print(f"\n{'#'*70}\n# {s.upper()}\n{'#'*70}")
            run(s)
    else:
        run(args.strategy)
