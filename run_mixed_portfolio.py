"""混合策略：70% T1_V7 + 30% 倒鎚 投組模擬
================================================
目的：驗證「主力 T1_V7（密集 + 容量友善）+ 加碼倒鎚（稀有但 alpha 強）」
是否比純單一策略更穩。

實作：
  - 70% 資金分給 T1_V7（max_pos=7 × 100k）
  - 30% 資金分給倒鎚（max_pos=15 × 20k for high frequency capture）
  - 兩個獨立子投組，最後合併 NAV 計指標
"""
import sys, time, json
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import numpy as np

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import backtest_strategy as bs


def gen_trades(strategy, hold_days):
    universe = bs.get_universe('tw')
    args = [(t, hold_days, strategy) for t in universe]
    all_trades = []
    with ProcessPoolExecutor(max_workers=bs.WORKERS) as ex:
        for trades in ex.map(bs.gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    return all_trades


def sub_portfolio(trades, hold_days, capital, max_pos, priority):
    """跑子投組，回傳 daily NAV series + executed trades"""
    if not trades:
        return None
    pos_size = capital // max_pos

    df_signals = pd.DataFrame(trades)
    df_signals['entry_dt'] = pd.to_datetime(df_signals['entry_date'])
    df_signals['exit_dt'] = pd.to_datetime(df_signals['exit_date'])

    if priority == 'fifo':
        sort_cols = ['entry_dt', 'ticker']; asc = [True, True]
    elif priority == 'rsi_low':
        sort_cols = ['entry_dt', 'rsi']; asc = [True, True]
    elif priority == 'drop_deep':
        sort_cols = ['entry_dt', 'drop_30d']; asc = [True, True]
    df_signals = df_signals.sort_values(sort_cols, ascending=asc).reset_index(drop=True)

    all_dates = sorted(set(df_signals['entry_dt']) | set(df_signals['exit_dt']))
    cash = capital
    positions = []
    executed = []
    daily_nav = []
    sig_iter = iter(df_signals.iterrows())
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
                    'net_ret': sig['net_ret'],
                })
            next_sig = next(sig_iter, None)

        daily_nav.append((d, cash + len(positions) * pos_size))

    # close remaining
    for p in positions:
        proceeds = pos_size * (1 + p['net_ret'])
        cash += proceeds
        p['close_value'] = proceeds
        executed.append(p)

    return executed, daily_nav, capital


def metrics_from_combined(combined_pl_dict, all_dates, total_capital):
    """從 daily P&L dict 計算 metrics"""
    if not combined_pl_dict:
        return {}
    all_d = pd.DatetimeIndex(sorted(all_dates))
    pl_series = pd.Series(combined_pl_dict).reindex(all_d, fill_value=0)
    nav_series = total_capital + pl_series.cumsum()
    daily_ret = nav_series.pct_change().fillna(0)
    sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() > 0 else 0
    peak = nav_series.cummax()
    dd = (nav_series - peak) / peak
    mdd = dd.min()
    days = (all_d[-1] - all_d[0]).days if len(all_d) > 1 else 365
    years = max(days / 365.25, 0.01)
    final = nav_series.iloc[-1]
    cagr = (final / total_capital) ** (1 / years) - 1
    return {
        'final_value': float(final),
        'total_return_pct': float((final / total_capital - 1) * 100),
        'cagr_pct': float(cagr * 100),
        'sharpe': float(sharpe),
        'max_drawdown_pct': float(mdd * 100),
        'years': float(years),
    }


def run_mix(t1_capital_ratio=0.7, t1_max_pos=7, hammer_max_pos=15, hold=30):
    print(f"\n{'='*70}")
    print(f"# 混合策略：T1_V7 {int(t1_capital_ratio*100)}% + 倒鎚 {int((1-t1_capital_ratio)*100)}%")
    print(f"# T1 max_pos={t1_max_pos} ({100_000} each), 倒鎚 max_pos={hammer_max_pos} ({int(1_000_000*(1-t1_capital_ratio)/hammer_max_pos)} each)")
    print(f"{'='*70}")
    total_cap = 1_000_000
    t1_cap = int(total_cap * t1_capital_ratio)
    hammer_cap = total_cap - t1_cap

    print(f"\n📥 跑兩個策略訊號...")
    t0 = time.time()
    t1_trades = gen_trades('t1_v7', hold)
    hammer_trades = gen_trades('inv_hammer', hold)
    print(f"  T1_V7: {len(t1_trades)} 筆, 倒鎚: {len(hammer_trades)} 筆 ({time.time()-t0:.1f}s)")

    # T1 子投組（FIFO，因 fill rate 低 priority 影響小）
    t1_exec, t1_nav, _ = sub_portfolio(t1_trades, hold, t1_cap, t1_max_pos, 'fifo')
    # 倒鎚子投組（drop_deep，OOS 驗證最佳）
    hammer_exec, hammer_nav, _ = sub_portfolio(hammer_trades, hold, hammer_cap, hammer_max_pos, 'drop_deep')

    # 合併 daily P&L（按 exit_dt 分組）
    pl_dict = {}
    for p in t1_exec:
        pl_dict[p['exit_dt']] = pl_dict.get(p['exit_dt'], 0) + (p['close_value'] - (t1_cap // t1_max_pos))
    for p in hammer_exec:
        pl_dict[p['exit_dt']] = pl_dict.get(p['exit_dt'], 0) + (p['close_value'] - (hammer_cap // hammer_max_pos))

    all_dates = set([d for d, _ in t1_nav]) | set([d for d, _ in hammer_nav])
    M = metrics_from_combined(pl_dict, all_dates, total_cap)

    print(f"\n結果（in-sample 6 年）:")
    print(f"  T1_V7 子投組: {len(t1_exec)} 筆執行")
    print(f"  倒鎚 子投組: {len(hammer_exec)} 筆執行")
    print(f"  總筆數: {len(t1_exec)+len(hammer_exec)}")
    print(f"  期末市值: {M['final_value']:,.0f} (起始 {total_cap:,})")
    print(f"  總報酬: {M['total_return_pct']:+.2f}%")
    print(f"  CAGR: {M['cagr_pct']:+.2f}%")
    print(f"  Sharpe: {M['sharpe']:.2f}")
    print(f"  Max DD: {M['max_drawdown_pct']:.2f}%")

    # OOS split
    print(f"\nWalk-forward (split 2024-01-01):")
    for label, dt_filter in [('TRAIN', lambda x: x < pd.Timestamp('2024-01-01')),
                               ('TEST OOS', lambda x: x >= pd.Timestamp('2024-01-01'))]:
        sub_pl = {d: v for d, v in pl_dict.items() if dt_filter(d)}
        sub_dates = [d for d in all_dates if dt_filter(d)]
        if not sub_pl: continue
        M_sub = metrics_from_combined(sub_pl, sub_dates, total_cap)
        print(f"  {label}: CAGR {M_sub['cagr_pct']:+.2f}%  Sharpe {M_sub['sharpe']:.2f}  "
              f"MDD {M_sub['max_drawdown_pct']:.2f}%")

    return {'mix_config': {'t1_ratio': t1_capital_ratio,
                            't1_max_pos': t1_max_pos,
                            'hammer_max_pos': hammer_max_pos,
                            'hold': hold},
             'metrics_full': M,
             'n_trades': {'t1': len(t1_exec), 'hammer': len(hammer_exec)}}


if __name__ == '__main__':
    results = {}
    # 不同混合比例
    for t1_ratio, t1_pos, ham_pos in [
        (1.0, 10, 0),          # 100% T1 baseline
        (0.7, 7, 15),          # 70/30
        (0.5, 5, 25),          # 50/50
        (0.3, 3, 35),          # 30/70
        (0.0, 0, 50),          # 100% 倒鎚 baseline
    ]:
        if t1_pos == 0:
            # 純倒鎚
            print(f"\n{'='*70}")
            print(f"# 純倒鎚 100% / max_pos={ham_pos}")
            print(f"{'='*70}")
            t0 = time.time()
            trades = gen_trades('inv_hammer', 30)
            print(f"  {len(trades)} 筆訊號 ({time.time()-t0:.1f}s)")
            exec_, nav, _ = sub_portfolio(trades, 30, 1_000_000, ham_pos, 'drop_deep')
            pl = {p['exit_dt']: pl.get(p['exit_dt'], 0) + (p['close_value'] - 1_000_000//ham_pos)
                  for p in exec_ for pl in [{}]}
            # Simpler:
            pl = {}
            for p in exec_:
                pl[p['exit_dt']] = pl.get(p['exit_dt'], 0) + (p['close_value'] - 1_000_000//ham_pos)
            M = metrics_from_combined(pl, [d for d,_ in nav], 1_000_000)
            print(f"  CAGR {M['cagr_pct']:+.2f}% Sharpe {M['sharpe']:.2f} MDD {M['max_drawdown_pct']:.2f}%")
            results['pure_hammer'] = {'metrics_full': M, 'n_trades': len(exec_)}
        elif ham_pos == 0:
            # 純 T1
            print(f"\n{'='*70}")
            print(f"# 純 T1_V7 100% / max_pos={t1_pos}")
            print(f"{'='*70}")
            trades = gen_trades('t1_v7', 30)
            print(f"  {len(trades)} 筆訊號")
            exec_, nav, _ = sub_portfolio(trades, 30, 1_000_000, t1_pos, 'fifo')
            pl = {}
            for p in exec_:
                pl[p['exit_dt']] = pl.get(p['exit_dt'], 0) + (p['close_value'] - 1_000_000//t1_pos)
            M = metrics_from_combined(pl, [d for d,_ in nav], 1_000_000)
            print(f"  CAGR {M['cagr_pct']:+.2f}% Sharpe {M['sharpe']:.2f} MDD {M['max_drawdown_pct']:.2f}%")
            results['pure_t1_v7'] = {'metrics_full': M, 'n_trades': len(exec_)}
        else:
            results[f'mix_{int(t1_ratio*100)}_{100-int(t1_ratio*100)}'] = run_mix(
                t1_ratio, t1_pos, ham_pos, 30)

    with open('backtest_mixed_portfolio.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print('\n✅ 寫入 backtest_mixed_portfolio.json')
