"""美股套用三項研究：止損 / hold / earnings season"""
import sys
sys.path.insert(0, '.')
from backtest_strategy import (gen_trades_for_one, get_universe, portfolio_sim,
                                trade_level_stats, START_DATE)
from concurrent.futures import ProcessPoolExecutor
import time
import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def main():
    print("🇺🇸 美股套用三項研究")
    print()

    universe = get_universe('us_top')
    print(f"📊 US TOP {len(universe)} 檔（已篩選 ETF / 大型股）")
    print()

    # ─── 1. 止損 sweep on US TOP T1_V7 ───
    print("=" * 80)
    print("1️⃣ 止損 sweep — US TOP + T1_V7 + hold=30 + max_pos=10")
    print("=" * 80)
    configs = [
        ('no_stop',     None, None),
        ('fixed_10',    0.10, None),
        ('fixed_15',    0.15, None),
        ('trail_10',    None, 0.10),
        ('trail_15',    None, 0.15),
    ]
    print(f"{'Config':>15}{'n':>6}{'Win%':>7}{'Mean':>9}{'PF':>6}{'CAGR':>9}{'Sharpe':>8}{'MDD':>9}{'Stopped%':>10}")
    print("-" * 80)
    for name, sp, tp in configs:
        all_trades = []
        args = [(t, 30, 't1_v7', sp, tp) for t in universe]
        with ProcessPoolExecutor(max_workers=16) as ex:
            for trades in ex.map(gen_trades_for_one, args, chunksize=50):
                all_trades.extend(trades)
        if not all_trades: continue
        df = pd.DataFrame(all_trades)
        n = len(df); win = (df['net_ret']>0).mean()*100
        mean = df['net_ret'].mean()*100
        pos_s = df.loc[df['net_ret']>0, 'net_ret'].sum()
        neg_s = -df.loc[df['net_ret']<0, 'net_ret'].sum() if (df['net_ret']<0).any() else 0.001
        pf = pos_s/neg_s if neg_s>0 else 999
        stopped = df['stopped_by'].notna().sum() / n * 100
        B = portfolio_sim(all_trades, 30, max_pos=10, priority='fifo')
        cagr = B.get('cagr_pct', 0)
        sharpe = B.get('sharpe', 0)
        mdd = B.get('max_drawdown_pct', 0)
        print(f"{name:>15}{n:>6}{win:>6.1f}%{mean:>+8.2f}%{pf:>6.2f}"
              f"{cagr:>+8.2f}%{sharpe:>8.2f}{mdd:>+8.2f}%{stopped:>9.1f}%")

    # ─── 2. Hold sweep on US TOP T1_V7 ───
    print()
    print("=" * 80)
    print("2️⃣ Hold sweep — US TOP + T1_V7 + max_pos=10 + no_stop")
    print("=" * 80)
    print(f"{'Hold':>6}{'n':>6}{'Win%':>7}{'Mean':>9}{'CAGR':>9}{'Sharpe':>8}{'MDD':>9}")
    print("-" * 60)
    for hd in [15, 30, 60]:
        all_trades = []
        args = [(t, hd, 't1_v7') for t in universe]
        with ProcessPoolExecutor(max_workers=16) as ex:
            for trades in ex.map(gen_trades_for_one, args, chunksize=50):
                all_trades.extend(trades)
        if not all_trades: continue
        A = trade_level_stats(all_trades)
        B = portfolio_sim(all_trades, hd, max_pos=10, priority='fifo')
        print(f"{hd:>5}d{A['n_trades']:>6}{A['win_rate_pct']:>6.1f}%{A['mean_net_pct']:>+8.2f}%"
              f"{B.get('cagr_pct',0):>+8.2f}%{B.get('sharpe',0):>8.2f}{B.get('max_drawdown_pct',0):>+8.2f}%")

    # ─── 3. Earnings month effect on US ───
    print()
    print("=" * 80)
    print("3️⃣ Earnings 月份效應 — US TOP + T1_V7 + hold=30")
    print("=" * 80)
    all_trades = []
    args = [(t, 30, 't1_v7') for t in universe]
    with ProcessPoolExecutor(max_workers=16) as ex:
        for trades in ex.map(gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    df = pd.DataFrame(all_trades)
    df['month'] = pd.to_datetime(df['entry_date']).dt.month
    print(f"{'Month':>6}{'n':>6}{'Win%':>7}{'Mean':>9}  US財報季")
    print("-" * 50)
    for m in range(1, 13):
        sub = df[df['month'] == m]
        if len(sub) < 30: continue
        r = sub['net_ret']
        # US earnings: Jan/Apr/Jul/Oct (Q4/Q1/Q2/Q3 reports)
        es_tag = ' 📅' if m in [1, 4, 7, 10] else ''
        print(f"{m:>5}月{len(sub):>6}{(r>0).mean()*100:>6.1f}%{r.mean()*100:>+8.2f}%{es_tag}")

    print()
    print("📋 美股結論將印在最後...")


if __name__ == '__main__':
    main()
