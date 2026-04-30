"""Earnings Season 避險研究（簡化版）
======================================
台股財報公告期：
  Q1 結果：5 月（每月底前）
  Q2 結果：8 月中前
  Q3 結果：11 月中前
  Q4 結果：隔年 3 月底前

→ 「財報密集月」= 3, 5, 8, 11 (TW)
→ 「財報後月」= 4, 6, 9, 12

研究問題：
  Q1: 訊號 alpha 在「財報密集月」是否與其他月份差異大？
  Q2: 應該避開財報月嗎？或是反而進場機會多？
  Q3: 進場後 30 天若跨財報日，結果如何？
"""
import sys, time, json
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import backtest_strategy as bs

WORKERS = 16

# 台股財報密集月（資料公告月份）
TW_EARNINGS_MONTHS = {3, 5, 8, 11}
# 接續財報後（消化期）
TW_POST_EARNINGS_MONTHS = {4, 6, 9, 12}


def analyze(strategy='inv_hammer', hold_days=30):
    universe = bs.get_universe('tw')
    print(f"🇹🇼 {strategy} hold={hold_days}d, universe={len(universe)} 檔\n")

    args = [(t, hold_days, strategy) for t in universe]
    all_trades = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for trades in ex.map(bs.gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    print(f"  訊號 {len(all_trades)} 筆 ({time.time()-t0:.1f}s)\n")

    df = pd.DataFrame(all_trades)
    df['entry_dt'] = pd.to_datetime(df['entry_date'])
    df['month'] = df['entry_dt'].dt.month
    df['is_earnings_mo'] = df['month'].isin(TW_EARNINGS_MONTHS)
    df['is_post_earnings_mo'] = df['month'].isin(TW_POST_EARNINGS_MONTHS)
    df['is_quiet_mo'] = ~df['is_earnings_mo'] & ~df['is_post_earnings_mo']

    # 跨財報判斷：進場後 30 天內是否有跨財報月
    def crosses_earnings(row):
        start = row['entry_dt']
        end = start + pd.Timedelta(days=hold_days + 5)
        for m in TW_EARNINGS_MONTHS:
            for y in range(start.year, end.year + 1):
                # 各月 1 號當判斷點（簡化）
                d = pd.Timestamp(year=y, month=m, day=15)
                if start <= d <= end:
                    return True
        return False
    df['crosses_earnings'] = df.apply(crosses_earnings, axis=1)

    print("=" * 90)
    print(f"📊 {strategy} 月份分布（{len(df)} 筆訊號）")
    print("=" * 90)
    print(f"{'月份':>6}{'n':>8}{'Win%':>8}{'Mean':>10}{'PF':>7}  類型")
    for m in range(1, 13):
        sub = df[df['month'] == m]
        if len(sub) == 0:
            continue
        win = (sub['net_ret'] > 0).mean() * 100
        mean = sub['net_ret'].mean() * 100
        pos = sub.loc[sub['net_ret'] > 0, 'net_ret'].sum()
        neg = -sub.loc[sub['net_ret'] < 0, 'net_ret'].sum() if (sub['net_ret']<0).any() else 0.001
        pf = pos / neg if neg > 0 else 999
        kind = '🔥 財報密集' if m in TW_EARNINGS_MONTHS else \
               '📈 財報後' if m in TW_POST_EARNINGS_MONTHS else '💤 安靜月'
        print(f"  {m:>4}月{len(sub):>8}{win:>7.1f}%{mean:>+9.2f}%{pf:>7.2f}  {kind}")

    # 三組對比
    print("\n" + "=" * 90)
    print("📊 三組月份比較")
    print("=" * 90)
    for label, mask in [
        ('🔥 財報密集月（3,5,8,11）', df['is_earnings_mo']),
        ('📈 財報後月（4,6,9,12）', df['is_post_earnings_mo']),
        ('💤 安靜月（1,2,7,10）', df['is_quiet_mo']),
    ]:
        sub = df[mask]
        if len(sub) == 0: continue
        win = (sub['net_ret'] > 0).mean() * 100
        mean = sub['net_ret'].mean() * 100
        pos = sub.loc[sub['net_ret'] > 0, 'net_ret'].sum()
        neg = -sub.loc[sub['net_ret'] < 0, 'net_ret'].sum() if (sub['net_ret']<0).any() else 0.001
        pf = pos / neg if neg > 0 else 999
        print(f"  {label}: n={len(sub):4d}  win={win:.1f}%  mean={mean:+.2f}%  PF={pf:.2f}")

    # 跨財報 vs 不跨財報
    print("\n" + "=" * 90)
    print("📊 持有期是否跨財報日")
    print("=" * 90)
    crosses = df[df['crosses_earnings']]
    no_cross = df[~df['crosses_earnings']]
    for label, sub in [('🔄 持有期跨財報日', crosses), ('🟢 持有期不跨財報', no_cross)]:
        if len(sub) == 0: continue
        win = (sub['net_ret'] > 0).mean() * 100
        mean = sub['net_ret'].mean() * 100
        pos = sub.loc[sub['net_ret'] > 0, 'net_ret'].sum()
        neg = -sub.loc[sub['net_ret'] < 0, 'net_ret'].sum() if (sub['net_ret']<0).any() else 0.001
        pf = pos / neg if neg > 0 else 999
        print(f"  {label}: n={len(sub):4d}  win={win:.1f}%  mean={mean:+.2f}%  PF={pf:.2f}")

    # 結論
    print("\n" + "=" * 90)
    print("📋 結論")
    print("=" * 90)
    base_mean = df['net_ret'].mean() * 100
    earn = df[df['is_earnings_mo']]
    earn_mean = earn['net_ret'].mean() * 100 if len(earn) else 0
    diff = earn_mean - base_mean
    if abs(diff) < 1:
        print(f"  ✅ 財報月 vs 整體 alpha 差異微小 ({diff:+.2f}%)，不需特別避開")
    elif diff > 1:
        print(f"  💡 財報月 alpha 反而強 ({diff:+.2f}%)，可加碼")
    else:
        print(f"  ⚠️ 財報月 alpha 較弱 ({diff:+.2f}%)，考慮避開")

    cross_mean = crosses['net_ret'].mean() * 100 if len(crosses) else 0
    no_cross_mean = no_cross['net_ret'].mean() * 100 if len(no_cross) else 0
    cross_diff = cross_mean - no_cross_mean
    if abs(cross_diff) < 1:
        print(f"  ✅ 跨財報 vs 不跨 差異微小 ({cross_diff:+.2f}%)")
    elif cross_diff > 1:
        print(f"  💡 跨財報持有 alpha 反而強 ({cross_diff:+.2f}%)，財報後利多")
    else:
        print(f"  ⚠️ 跨財報持有 alpha 較弱 ({cross_diff:+.2f}%)，考慮提前出場")

    return df


if __name__ == '__main__':
    print("=" * 90)
    print("INV_HAMMER 倒鎚")
    print("=" * 90)
    analyze('inv_hammer', 30)

    print("\n" + "=" * 90)
    print("T1_V7 提前進場")
    print("=" * 90)
    analyze('t1_v7', 30)
