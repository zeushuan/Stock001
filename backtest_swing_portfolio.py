"""波段策略投組模擬：B 突破前高 + 動態出場（v9.15）
==========================================================
基於 walk-forward OOS 驗證找到的最佳組合：
  - Entry:  Strategy B（突破前高，從 screener_filters.py）
  - Exit:   rsi_70 / rsi_75 / rsi_80 / fixed_90d 動態出場
  - 跨 TW + US（兩市場 OOS robust 的唯一策略）

實驗設計
---------
- 起始資金 1M，每筆等額分配 (1M / max_pos)
- 不同 max_pos：5 / 10 / 20 / 50
- 訊號優先序：fifo / rsi_low（RSI 最低先進）/ adx_high（ADX 最高先進）
- 報告：Total return, CAGR, Sharpe, MDD, 勝率, fill rate

執行
-----
  python backtest_swing_portfolio.py                   # TW 全跑
  python backtest_swing_portfolio.py --market us       # US
  python backtest_swing_portfolio.py --market both     # 兩個都跑
  python backtest_swing_portfolio.py --oos             # 只看 2024+
"""
import sys, json, time, argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
from analyze_swing_dynamic_exit import (
    detect_swing_signals, compute_helper_arrays, walk_exit, EXIT_RULES,
    get_universe, COST_ROUND_TRIP
)

WORKERS = 12
INITIAL_CAPITAL = 1_000_000


def gen_trades_strategy_B(args):
    """單一 ticker：跑 Strategy B + 多種出場規則，回傳所有 trades。"""
    ticker, rules_dict, start_date = args
    out = {r: [] for r in rules_dict}
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280:
            return out
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(start_date)]
        if len(df) < 80:
            return out

        helpers = compute_helper_arrays(df)
        signals = detect_swing_signals(df, 'B')

        o = df['Open'].values
        idx = df.index
        n = len(df)
        rsi = df['rsi'].values
        adx = df['adx'].values

        for sig_i in signals:
            entry_i = sig_i + 1
            if entry_i >= n - 1: continue
            entry_open = float(o[entry_i])
            if entry_open <= 0 or np.isnan(entry_open): continue

            for rule_name, rule in rules_dict.items():
                exit_i, exit_price, exit_reason = walk_exit(
                    df, helpers, entry_i, entry_open, rule)
                if exit_price <= 0 or np.isnan(exit_price): continue

                gross_ret = (exit_price - entry_open) / entry_open
                net_ret = gross_ret - COST_ROUND_TRIP
                out[rule_name].append({
                    'ticker': ticker,
                    'entry_date': idx[entry_i].strftime('%Y-%m-%d'),
                    'exit_date':  idx[exit_i].strftime('%Y-%m-%d'),
                    'hold_days':  exit_i - entry_i,
                    'entry_price': round(entry_open, 4),
                    'exit_price': round(exit_price, 4),
                    'gross_ret': gross_ret,
                    'net_ret': net_ret,
                    'rsi_at_signal': float(rsi[sig_i]) if not np.isnan(rsi[sig_i]) else 50,
                    'adx_at_signal': float(adx[sig_i]) if not np.isnan(adx[sig_i]) else 0,
                    'exit_reason': exit_reason,
                })
        return out
    except Exception:
        return out


def portfolio_sim(trades, max_pos, priority='fifo'):
    """投組模擬：起始 1M，最多 max_pos 倉位（等額），FIFO 或 ranked priority。"""
    if not trades:
        return None

    pos_size = INITIAL_CAPITAL // max_pos

    df = pd.DataFrame(trades)
    df['entry_dt'] = pd.to_datetime(df['entry_date'])
    df['exit_dt'] = pd.to_datetime(df['exit_date'])

    if priority == 'rsi_low':
        sort_cols, sort_asc = ['entry_dt', 'rsi_at_signal'], [True, True]
    elif priority == 'adx_high':
        sort_cols, sort_asc = ['entry_dt', 'adx_at_signal'], [True, False]
    else:  # fifo
        sort_cols, sort_asc = ['entry_dt', 'ticker'], [True, True]
    df = df.sort_values(sort_cols, ascending=sort_asc).reset_index(drop=True)

    all_dates = sorted(set(df['entry_dt']) | set(df['exit_dt']))

    cash = INITIAL_CAPITAL
    positions = []
    daily_nav = []
    executed = []
    skipped = 0

    sig_iter = iter(df.iterrows())
    next_sig = next(sig_iter, None)

    for d in all_dates:
        # 1) Exit 今天到期
        new_pos = []
        for p in positions:
            if p['exit_dt'] == d:
                proceeds = pos_size * (1 + p['net_ret'])
                cash += proceeds
                p['close_value'] = proceeds
                executed.append(p)
            else:
                new_pos.append(p)
        positions = new_pos

        # 2) Entry 今天的訊號（受 max_pos 限制）
        while next_sig is not None and next_sig[1]['entry_dt'] == d:
            _, sig = next_sig
            if len(positions) < max_pos and cash >= pos_size:
                cash -= pos_size
                positions.append({
                    'ticker': sig['ticker'],
                    'entry_dt': sig['entry_dt'],
                    'exit_dt': sig['exit_dt'],
                    'gross_ret': sig['gross_ret'],
                    'net_ret': sig['net_ret'],
                })
            else:
                skipped += 1
            next_sig = next(sig_iter, None)

        nav = cash + len(positions) * pos_size
        daily_nav.append((d, nav))

    # 收盤平倉
    for p in positions:
        proceeds = pos_size * (1 + p['net_ret'])
        cash += proceeds
        p['close_value'] = proceeds
        executed.append(p)

    if not executed:
        return {'n_executed': 0, 'n_skipped': skipped}

    df_exec = pd.DataFrame(executed)
    df_exec['profit'] = df_exec['close_value'] - pos_size
    df_exec['exit_dt'] = pd.to_datetime(df_exec['exit_dt'])

    daily_pl = df_exec.groupby('exit_dt')['profit'].sum().sort_index()
    all_d = pd.DatetimeIndex(sorted([d for d, _ in daily_nav]))
    daily_pl = daily_pl.reindex(all_d, fill_value=0)
    nav_series = INITIAL_CAPITAL + daily_pl.cumsum()
    daily_ret = nav_series.pct_change().fillna(0)
    sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() > 0 else 0

    peak = nav_series.cummax()
    dd = (nav_series - peak) / peak
    mdd = dd.min()

    days = (all_d[-1] - all_d[0]).days if len(all_d) > 1 else 365
    years = max(days / 365.25, 0.01)
    final_value = nav_series.iloc[-1]
    cagr = (final_value / INITIAL_CAPITAL) ** (1 / years) - 1
    win = (df_exec['profit'] > 0).sum()

    return {
        'n_executed': int(len(df_exec)),
        'n_skipped': int(skipped),
        'fill_rate_pct': round(len(df_exec) / max(len(df_exec) + skipped, 1) * 100, 1),
        'final_value': round(float(final_value), 0),
        'total_return_pct': round((final_value / INITIAL_CAPITAL - 1) * 100, 2),
        'cagr_pct': round(cagr * 100, 2),
        'sharpe': round(float(sharpe), 2),
        'max_drawdown_pct': round(float(mdd) * 100, 2),
        'win_rate_pct': round(float(win / len(df_exec)) * 100, 2),
        'avg_profit': round(df_exec['profit'].mean(), 0),
        'total_profit': round(df_exec['profit'].sum(), 0),
        'years': round(years, 2),
        'pos_size': pos_size,
    }


def run_backtest(market='tw', oos_only=False):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    start = '2024-01-01' if oos_only else '2020-01-01'
    period_label = 'OOS only (2024-)' if oos_only else 'Full period (2020-)'

    print(f"\n{flag} Strategy B 突破前高 投組回測  {period_label}")
    print(f"  Universe: {len(universe)} 檔  Capital: {INITIAL_CAPITAL:,}")
    print(f"  起始日期: {start}")
    print()

    # 跑 4 種出場規則
    rules = {k: EXIT_RULES[k] for k in ['rsi_70', 'rsi_75', 'rsi_80', 'fixed_90d']}

    print(f"📊 跑訊號 + walk exits（{WORKERS} workers）...")
    t0 = time.time()
    args = [(t, rules, start) for t in universe]
    all_by_rule = {r: [] for r in rules}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for one_out in ex.map(gen_trades_strategy_B, args, chunksize=80):
            for r, ts in one_out.items():
                all_by_rule[r].extend(ts)
    elapsed = time.time() - t0
    total = sum(len(v) for v in all_by_rule.values())
    print(f"  完成 {elapsed:.1f}s，共 {total} trades 跨 {len(rules)} rules")

    # 對每 (rule × max_pos × priority) 跑投組模擬
    POS_LIST = [5, 10, 20, 50]
    PRIO_LIST = ['fifo', 'rsi_low', 'adx_high']

    all_results = {}

    for rule_name, trades in all_by_rule.items():
        if not trades:
            continue
        print()
        print('=' * 130)
        print(f"📊 出場規則: {rule_name}  訊號數: {len(trades)}")
        print('=' * 130)
        print(f"{'priority':>10} {'max_pos':>8} {'pos_size':>10} {'n_exec':>8} {'n_skip':>8} "
              f"{'fill%':>7} {'final':>13} {'CAGR%':>9} {'Sharpe':>8} {'MDD%':>9} {'win%':>7}")
        print('-' * 130)

        for prio in PRIO_LIST:
            for mp in POS_LIST:
                B = portfolio_sim(trades, max_pos=mp, priority=prio)
                if B is None or B.get('n_executed', 0) == 0:
                    print(f"{prio:>10} {mp:>8} {'-':>10} {'(no trades)':>40}")
                    continue
                marker = ''
                if B['cagr_pct'] >= 8 and B['sharpe'] >= 1.5:
                    marker = ' ⭐'
                print(f"{prio:>10} {mp:>8} {B['pos_size']:>10,} {B['n_executed']:>8} "
                      f"{B['n_skipped']:>8} {B['fill_rate_pct']:>6.1f}% "
                      f"{B['final_value']:>13,.0f} {B['cagr_pct']:>+8.2f}% "
                      f"{B['sharpe']:>8.2f} {B['max_drawdown_pct']:>+8.2f}% "
                      f"{B['win_rate_pct']:>6.1f}%{marker}")
                all_results[f'{rule_name}_{prio}_pos{mp}'] = B

    # 寫 JSON
    out = f'backtest_swing_portfolio_{market}{"_oos" if oos_only else ""}.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'market': market, 'oos_only': oos_only, 'start_date': start,
            'universe_size': len(universe),
            'initial_capital': INITIAL_CAPITAL,
            'results': all_results,
        }, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 {out}")

    # 結論：找出 top 5 by CAGR with Sharpe ≥ 1
    valid = [(k, v) for k, v in all_results.items() if v.get('sharpe', 0) >= 1]
    valid.sort(key=lambda x: -x[1]['cagr_pct'])
    print()
    print('🏆 Top 5 配置（Sharpe ≥ 1，按 CAGR 排序）:')
    for i, (k, v) in enumerate(valid[:5], 1):
        print(f"  #{i} {k:<30} CAGR {v['cagr_pct']:+.2f}%  Sharpe {v['sharpe']:.2f}  "
              f"MDD {v['max_drawdown_pct']:.2f}%  Final {v['final_value']:,.0f}")

    return all_results


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'both'])
    p.add_argument('--oos', action='store_true',
                   help='只跑 OOS（2024+）')
    args = p.parse_args()

    markets = ['tw', 'us'] if args.market == 'both' else [args.market]
    for m in markets:
        run_backtest(market=m, oos_only=args.oos)


if __name__ == '__main__':
    main()
