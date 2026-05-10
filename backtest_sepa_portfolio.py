"""SEPA 投組回測（v9.20）
==========================================================
基於 OOS 驗證最佳組合：
- 🇹🇼 TW: SEPA + VCP + RS70 + fixed_90d
- 🇺🇸 US: SEPA + RS90 + fixed_90d

模擬實際交易：
- 起始 1M
- max_pos: 5 / 10 / 20 / 50
- priority: fifo / rs_high / sepa_n_met
- 報告：Sharpe, CAGR, MDD, fill rate, win rate, total return

執行
-----
  python backtest_sepa_portfolio.py                # TW
  python backtest_sepa_portfolio.py --market both
  python backtest_sepa_portfolio.py --oos          # 2024+
"""
import sys, json, time, argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
from analyze_swing_dynamic_exit import get_universe, COST_ROUND_TRIP
from analyze_sepa_oos import (
    compute_ticker_series, detect_sepa_entries, walk_exit,
    precompute_rs_ratings,
)

WORKERS = 12
INITIAL_CAPITAL = 1_000_000


def gen_trades_one(args):
    ticker, rs_dict, entry_variant, exit_strategy, start_date = args
    out = []
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return out
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)

        helpers = compute_ticker_series(df)
        if helpers is None: return out

        rs_arr = rs_dict.get(ticker)
        rs_series = pd.Series(rs_arr, index=df.index) if rs_arr is not None else None

        signals = detect_sepa_entries(df, helpers, rs_series,
                                       variant=entry_variant,
                                       start_date=start_date)
        n = len(df); o = df['Open'].values; idx = df.index

        for sig_i in signals:
            entry_i = sig_i + 1
            if entry_i >= n - 1: continue
            entry_open = float(o[entry_i])
            if entry_open <= 0 or np.isnan(entry_open): continue

            exit_i, exit_price, reason = walk_exit(
                df, helpers, entry_i, entry_open, exit_strategy)
            if exit_price is None or exit_price <= 0 or np.isnan(exit_price): continue

            gross_ret = (exit_price - entry_open) / entry_open
            net_ret = gross_ret - COST_ROUND_TRIP

            # RS at signal date
            rs_at_sig = float(rs_series.iloc[sig_i]) if rs_series is not None and not np.isnan(rs_series.iloc[sig_i]) else 50

            out.append({
                'ticker': ticker,
                'entry_date': idx[entry_i].strftime('%Y-%m-%d'),
                'exit_date': idx[exit_i].strftime('%Y-%m-%d') if exit_i < n else idx[-1].strftime('%Y-%m-%d'),
                'hold_days': exit_i - entry_i,
                'entry_price': round(entry_open, 4),
                'exit_price': round(exit_price, 4),
                'gross_ret': gross_ret,
                'net_ret': net_ret,
                'reason': reason,
                'rs_at_signal': rs_at_sig,
            })
        return out
    except Exception:
        return out


def portfolio_sim(trades, max_pos, priority='fifo'):
    if not trades: return None
    pos_size = INITIAL_CAPITAL // max_pos
    df = pd.DataFrame(trades)
    df['entry_dt'] = pd.to_datetime(df['entry_date'])
    df['exit_dt'] = pd.to_datetime(df['exit_date'])

    if priority == 'rs_high':
        df = df.sort_values(['entry_dt', 'rs_at_signal'], ascending=[True, False]).reset_index(drop=True)
    else:  # fifo
        df = df.sort_values(['entry_dt', 'ticker'], ascending=[True, True]).reset_index(drop=True)

    all_dates = sorted(set(df['entry_dt']) | set(df['exit_dt']))
    cash = INITIAL_CAPITAL
    positions = []
    daily_nav = []
    executed = []
    skipped = 0
    sig_iter = iter(df.iterrows())
    next_sig = next(sig_iter, None)

    for d in all_dates:
        # exit
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
        # entry
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
        daily_nav.append((d, cash + len(positions) * pos_size))

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
        'years': round(years, 2),
        'pos_size': pos_size,
    }


def run(market='tw', oos_only=False):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    start = '2024-01-01' if oos_only else '2020-01-01'
    period = 'OOS only (2024-)' if oos_only else 'Full period (2020-)'

    # Best entry per market（依 OOS 結果）
    if market == 'tw':
        ENTRY = 'SEPA_VCP_RS70'
    else:
        ENTRY = 'SEPA_RS90'
    EXIT = 'fixed_90d'

    print(f'\n{flag} SEPA 投組回測  {period}')
    print(f'  Universe: {len(universe)} 檔')
    print(f'  Entry: {ENTRY}, Exit: {EXIT}')
    print(f'  Capital: {INITIAL_CAPITAL:,}')
    print()

    print(f'📊 預計算 RS Rating...')
    rs_dict = precompute_rs_ratings(universe, start_date='2020-01-01')

    print(f'\n📊 收集 {ENTRY} 訊號 + walk exit...')
    t0 = time.time()
    args = [(t, rs_dict, ENTRY, EXIT, start) for t in universe]
    all_trades = []
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for trades in ex.map(gen_trades_one, args, chunksize=80):
            all_trades.extend(trades)
    print(f'  完成 {time.time()-t0:.1f}s，共 {len(all_trades)} trades')

    if not all_trades:
        print('❌ 沒有 trades')
        return

    POS_LIST = [5, 10, 20, 50]
    PRIO_LIST = ['fifo', 'rs_high']

    results = {}
    print()
    print('=' * 130)
    print(f'{"priority":>10} {"max_pos":>8} {"pos_size":>10} {"n_exec":>8} {"n_skip":>8} '
          f'{"fill%":>7} {"final":>13} {"CAGR%":>9} {"Sharpe":>8} {"MDD%":>9} {"win%":>7}')
    print('-' * 130)
    for prio in PRIO_LIST:
        for mp in POS_LIST:
            B = portfolio_sim(all_trades, max_pos=mp, priority=prio)
            if B is None or B.get('n_executed', 0) == 0:
                continue
            mark = ''
            if B['cagr_pct'] >= 15 and B['sharpe'] >= 1.5:
                mark = ' ⭐'
            print(f'{prio:>10} {mp:>8} {B["pos_size"]:>10,} {B["n_executed"]:>8} '
                  f'{B["n_skipped"]:>8} {B["fill_rate_pct"]:>6.1f}% '
                  f'{B["final_value"]:>13,.0f} {B["cagr_pct"]:>+8.2f}% '
                  f'{B["sharpe"]:>8.2f} {B["max_drawdown_pct"]:>+8.2f}% '
                  f'{B["win_rate_pct"]:>6.1f}%{mark}')
            results[f'{prio}_pos{mp}'] = B

    # save
    out = f'backtest_sepa_portfolio_{market}{"_oos" if oos_only else ""}.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'market': market, 'oos_only': oos_only,
            'entry': ENTRY, 'exit': EXIT, 'start_date': start,
            'universe_size': len(universe),
            'initial_capital': INITIAL_CAPITAL,
            'results': results,
        }, f, indent=2, ensure_ascii=False)
    print(f'\n✅ 寫入 {out}')

    # Top 5
    valid = [(k, v) for k, v in results.items() if v.get('sharpe', 0) >= 1]
    valid.sort(key=lambda x: -x[1]['cagr_pct'])
    print('\n🏆 Top 5 (Sharpe ≥ 1, by CAGR):')
    for i, (k, v) in enumerate(valid[:5], 1):
        print(f'  #{i} {k:<25} CAGR {v["cagr_pct"]:+.2f}%  Sharpe {v["sharpe"]:.2f}  '
              f'MDD {v["max_drawdown_pct"]:.2f}%  Final {v["final_value"]:,.0f}')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'both'])
    p.add_argument('--oos', action='store_true')
    args = p.parse_args()
    markets = ['tw', 'us'] if args.market == 'both' else [args.market]
    for m in markets:
        run(m, oos_only=args.oos)


if __name__ == '__main__':
    main()
