"""自訂 walk-forward 跑特定配置（A 倒鎚 hold=15 + max_pos=50 + drop_deep）"""
import sys, time, json
from concurrent.futures import ProcessPoolExecutor
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import backtest_strategy as bs


def run_custom_wf(strategy, hold_days, max_pos, priority,
                   stop_pct=None, trail_pct=None, split='2024-01-01'):
    universe = bs.get_universe('tw')
    print(f"\n🇹🇼 {strategy} hold={hold_days}d max_pos={max_pos} prio={priority} "
          f"stop={stop_pct} trail={trail_pct}")

    args = [(t, hold_days, strategy, stop_pct, trail_pct) for t in universe]
    all_trades = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=bs.WORKERS) as ex:
        for trades in ex.map(bs.gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    print(f"  訊號 {len(all_trades)} 筆 ({time.time()-t0:.1f}s)")

    train = [t for t in all_trades if t['entry_date'] < split]
    test = [t for t in all_trades if t['entry_date'] >= split]
    print(f"  Train {len(train)}, Test {len(test)}")

    out = {}
    for label, ts in [('TRAIN (2020-2023)', train), ('TEST (OOS 2024+)', test)]:
        if not ts: continue
        A = bs.trade_level_stats(ts)
        B = bs.portfolio_sim(ts, hold_days, max_pos=max_pos, priority=priority)
        print(f"  {label}: signal n={A['n_trades']} win={A['win_rate_pct']}% mean={A['mean_net_pct']:+.2f}% PF={A['profit_factor']}")
        if B.get('n_executed', 0) > 0:
            print(f"    投組: CAGR {B['cagr_pct']:+.2f}% Sharpe {B['sharpe']} "
                  f"MDD {B['max_drawdown_pct']:.2f}% fill {B['fill_rate_pct']}%")
        out[label] = {'A': A, 'B': B}
    return out


if __name__ == '__main__':
    results = {}
    print('=' * 70)
    print('A 變體：倒鎚 hold=15 + max_pos=50 + drop_deep')
    print('=' * 70)
    results['inv_hammer_h15_p50_drop_deep'] = run_custom_wf('inv_hammer', 15, 50, 'drop_deep')

    print('\n' + '=' * 70)
    print('A 變體+止損：倒鎚 hold=15 + max_pos=50 + drop_deep + fixed_10')
    print('=' * 70)
    results['inv_hammer_h15_p50_drop_deep_stop10'] = run_custom_wf(
        'inv_hammer', 15, 50, 'drop_deep', stop_pct=0.10)

    print('\n' + '=' * 70)
    print('對照：倒鎚 hold=30 + max_pos=50 + drop_deep（已知 in-sample）')
    print('=' * 70)
    results['inv_hammer_h30_p50_drop_deep'] = run_custom_wf('inv_hammer', 30, 50, 'drop_deep')

    print('\n' + '=' * 70)
    print('T1_V7 + fixed_10 止損：T1_V7 hold=30 + max_pos=50 + drop_deep + fixed_10')
    print('=' * 70)
    results['t1_v7_h30_p50_drop_deep_stop10'] = run_custom_wf(
        't1_v7', 30, 50, 'drop_deep', stop_pct=0.10)

    with open('backtest_custom_wf_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print('\n✅ 寫入 backtest_custom_wf_results.json')
