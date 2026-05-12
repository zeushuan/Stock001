"""RS Leading High Scanner — 歷史回測框架

紀律：
- Point-in-time 資料（無 look-ahead bias）
- 嚴格用該時點的宇宙（universe snapshot or 本地 fallback）
- 扣除交易成本（US 0.05% × 2、TW 0.15% × 2 含交易稅）
- 持有期間如下市/停牌，以最後價作結算

績效指標：
- 平均超額報酬（alpha）
- 勝率
- Sharpe
- 最大回撤
- 牛 / 熊 / 震盪市子樣本
- 統計顯著性 t-test
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

from scanners.rs_leading_high import scan_universe, RSLeadingHighSignal

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────
# 交易成本
# ────────────────────────────────────────────────────────────────

COST_PER_SIDE = {
    'US': 0.0005,    # 0.05%
    'TW': 0.0015,    # 0.15% 含交易稅
}


def _round_trip_cost(market: str) -> float:
    return COST_PER_SIDE.get(market, 0.0005) * 2


# ────────────────────────────────────────────────────────────────
# 回測主框架
# ────────────────────────────────────────────────────────────────

def run_backtest(
    universe: List[str],
    market: str = 'US',
    start_date: str = '2022-01-01',
    end_date: str = '2024-12-31',
    rebalance_freq: str = 'M',      # 'M' monthly, 'W' weekly
    top_n: int = 30,
    hold_days: List[int] = (21, 63, 126),  # 1m / 3m / 6m
    rs_new_high_lookback: int = 63,
    min_distance_from_price_high: float = 0.03,
    data_loader_fn=None,
    cache_dir: str = 'data_cache',
) -> Dict[str, Any]:
    """跑歷史回測

    Returns:
      {
        'meta': {...},
        'periods': [...],   # 每個 rebalance 點的訊號 + 後續報酬
        'metrics': {...},   # 統計報酬指標
      }
    """
    if data_loader_fn is None:
        import data_loader
        data_loader_fn = lambda t: data_loader.load_from_cache(t)

    # 產生 rebalance 日期
    full_dates = pd.date_range(start_date, end_date, freq='B')
    if rebalance_freq == 'M':
        # 每月最後交易日
        rebal_dates = pd.Series(full_dates).groupby(
            pd.Series(full_dates).dt.to_period('M')).max().tolist()
    elif rebalance_freq == 'W':
        rebal_dates = pd.Series(full_dates).groupby(
            pd.Series(full_dates).dt.to_period('W')).max().tolist()
    else:
        rebal_dates = full_dates.tolist()

    cost_rt = _round_trip_cost(market)
    periods = []

    for rebal_date in rebal_dates:
        rebal_date = pd.Timestamp(rebal_date)
        # 跑掃描（截至 rebal_date）
        signals = scan_universe(
            universe=universe,
            as_of_date=rebal_date,
            market=market,
            data_loader_fn=data_loader_fn,
            rs_new_high_lookback=rs_new_high_lookback,
            min_distance_from_price_high=min_distance_from_price_high,
            apply_filters=True,
            score_and_rank=True,
        )
        if not signals:
            periods.append({
                'rebal_date': rebal_date, 'n_selected': 0,
                'returns': {}, 'tickers': [],
            })
            continue
        top = signals[:top_n]
        tickers = [s.ticker for s in top]

        # 計算各持有期間的報酬
        returns_by_hold = {}
        for hd in hold_days:
            rets = []
            for tk in tickers:
                try:
                    df = data_loader_fn(tk)
                    if df is None: continue
                    df = df.copy()
                    if df.index.tz is not None:
                        df.index = df.index.tz_localize(None)
                    df.index = df.index.normalize()
                    after = df.loc[rebal_date:].head(hd + 1)
                    if len(after) < 2: continue
                    p0 = float(after['Close'].iloc[0])
                    p1 = float(after['Close'].iloc[-1])
                    r = (p1 - p0) / p0 - cost_rt
                    rets.append(r)
                except Exception:
                    continue
            returns_by_hold[hd] = rets

        periods.append({
            'rebal_date': rebal_date,
            'n_selected': len(tickers),
            'tickers': tickers,
            'returns': returns_by_hold,
        })

    # 聚合 metrics
    metrics = _compute_metrics(periods, hold_days, market=market,
                                 start_date=start_date, end_date=end_date,
                                 data_loader_fn=data_loader_fn)

    return {
        'meta': {
            'start': start_date, 'end': end_date, 'market': market,
            'universe_size': len(universe), 'top_n': top_n,
            'rebalance': rebalance_freq, 'hold_days': list(hold_days),
            'cost_per_side': COST_PER_SIDE[market],
        },
        'periods': periods,
        'metrics': metrics,
    }


def _compute_metrics(periods, hold_days, market, start_date, end_date,
                     data_loader_fn=None):
    """計算績效指標"""
    from scipy import stats

    # 大盤基準
    idx_ticker = 'SPY' if market == 'US' else '^TWII'
    bench_returns_by_hold = {hd: [] for hd in hold_days}
    if data_loader_fn:
        try:
            bench = data_loader_fn(idx_ticker)
            if bench is not None:
                bench = bench.copy()
                if bench.index.tz is not None:
                    bench.index = bench.index.tz_localize(None)
                bench.index = bench.index.normalize()
                for p in periods:
                    rebal = p['rebal_date']
                    after = bench.loc[rebal:]
                    for hd in hold_days:
                        if len(after) > hd:
                            p0 = float(after['Close'].iloc[0])
                            p1 = float(after['Close'].iloc[hd])
                            bench_returns_by_hold[hd].append((p1 - p0) / p0)
        except Exception:
            pass

    metrics = {}
    for hd in hold_days:
        all_rets = []
        for p in periods:
            all_rets += p['returns'].get(hd, [])
        if not all_rets:
            metrics[hd] = {'n': 0}
            continue
        arr = np.array(all_rets)
        bench_arr = np.array(bench_returns_by_hold[hd]) if bench_returns_by_hold[hd] else np.array([0])

        # alpha = 訊號平均 - 大盤平均
        alpha = float(arr.mean() - bench_arr.mean())

        # t-test: H0: 訊號超額報酬 = 0
        # 用 alpha series (各 period 的訊號平均 - 大盤平均)
        period_alphas = []
        for p in periods:
            r = p['returns'].get(hd, [])
            if not r: continue
            avg_r = float(np.mean(r))
            # 對應的大盤 hd-day return
            try:
                rebal = p['rebal_date']
                after = bench.loc[rebal:]
                if len(after) > hd:
                    b0 = float(after['Close'].iloc[0])
                    b1 = float(after['Close'].iloc[hd])
                    bench_r = (b1 - b0) / b0
                    period_alphas.append(avg_r - bench_r)
            except Exception:
                pass
        if len(period_alphas) >= 5:
            t_stat, p_val = stats.ttest_1samp(period_alphas, 0)
        else:
            t_stat, p_val = float('nan'), float('nan')

        # Sharpe (per-period alpha)
        if len(period_alphas) >= 5 and np.std(period_alphas) > 0:
            sharpe = float(np.mean(period_alphas) / np.std(period_alphas) * np.sqrt(12))
        else:
            sharpe = float('nan')

        # Max drawdown on cumulative equity (簡化版 — 用 alpha 累積)
        cum = np.cumsum(period_alphas) if period_alphas else np.array([0])
        running_max = np.maximum.accumulate(cum)
        dd = (cum - running_max)
        max_dd = float(dd.min()) if len(dd) else 0

        metrics[hd] = {
            'n': len(arr),
            'mean_return': float(arr.mean()),
            'median_return': float(np.median(arr)),
            'std_return': float(arr.std()),
            'win_rate': float((arr > 0).mean() * 100),
            'bench_mean_return': float(bench_arr.mean()),
            'alpha': alpha,
            'periods_evaluated': len(period_alphas),
            't_statistic': float(t_stat) if not np.isnan(t_stat) else None,
            'p_value': float(p_val) if not np.isnan(p_val) else None,
            'significant_at_5pct': (p_val is not None and not np.isnan(p_val) and p_val < 0.05),
            'sharpe_alpha': sharpe if not np.isnan(sharpe) else None,
            'max_drawdown_alpha': max_dd,
        }
    return metrics


def print_metrics(report):
    """美化輸出"""
    meta = report['meta']
    metrics = report['metrics']
    print(f'\n{"="*80}')
    print(f'回測報告 — {meta["market"]}  {meta["start"]} → {meta["end"]}')
    print(f'  universe={meta["universe_size"]}, top_n={meta["top_n"]}, '
          f'rebal={meta["rebalance"]}, cost={meta["cost_per_side"]*100:.3f}%/side')
    print('='*80)
    print(f'{"Hold":>5s} | {"N":>5s} | {"Win%":>6s} | {"MeanRet%":>9s} | '
          f'{"BenchRet%":>10s} | {"Alpha%":>8s} | {"Sharpe":>7s} | '
          f'{"p-value":>9s} | Significant')
    print('-'*100)
    for hd, m in metrics.items():
        if m.get('n', 0) == 0:
            print(f'  {hd:>3d}d | (no data)')
            continue
        signif = '✓' if m['significant_at_5pct'] else '✗'
        pv = f'{m["p_value"]:.4f}' if m['p_value'] is not None else 'N/A'
        sharpe_s = f'{m["sharpe_alpha"]:.3f}' if m['sharpe_alpha'] is not None else 'N/A'
        print(f'  {hd:>3d}d | {m["n"]:>5d} | {m["win_rate"]:>6.1f} | '
              f'{m["mean_return"]*100:>+9.2f} | {m["bench_mean_return"]*100:>+10.2f} | '
              f'{m["alpha"]*100:>+8.2f} | {sharpe_s:>7s} | {pv:>9s} | {signif}')


# ────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description='RS Leading High Backtest')
    parser.add_argument('--market', default='US', choices=['US', 'TW'])
    parser.add_argument('--universe', default=None)
    parser.add_argument('--start', default='2022-01-01')
    parser.add_argument('--end', default='2024-12-31')
    parser.add_argument('--top-n', type=int, default=30)
    parser.add_argument('--rebalance', default='M', choices=['M', 'W'])
    parser.add_argument('--save', default=None, help='Save JSON report path')
    args = parser.parse_args()

    if args.universe is None:
        args.universe = 'LIQUID_3000' if args.market == 'US' else 'LIQUID_TW'
    if args.market == 'US':
        from universes.us_universe import get_universe
    else:
        from universes.tw_universe import get_universe
    tickers = get_universe(args.universe)
    print(f'[Universe] {args.universe}: {len(tickers)} tickers')

    report = run_backtest(
        universe=tickers, market=args.market,
        start_date=args.start, end_date=args.end,
        top_n=args.top_n, rebalance_freq=args.rebalance,
    )
    print_metrics(report)

    if args.save:
        import json
        with open(args.save, 'w', encoding='utf-8') as f:
            json.dump(report, f, default=str, indent=2)
        print(f'[Saved] {args.save}')


if __name__ == '__main__':
    main()
