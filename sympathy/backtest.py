"""Sympathy Play 歷史回測（Phase 2）

回測規則（依指示書 §2.6）：
  - 進場：訊號觸發隔日「開盤」買入
  - 出場（先到先出）：
      * 持有 5 個交易日後收盤賣
      * 觸及 +8% 停利（收盤計算）
      * 觸及 -4% 停損
  - 輸出：總訊號 / 勝率 / 平均 / 中位數 / max win / max loss
          按 score bucket (0.45-0.6 / 0.6-0.75 / 0.75+) 拆分
          按 group 拆分
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

from sympathy.peer_mapping import get_default_mapping
from sympathy.laggard_scorer import scan_all_groups
from sympathy._data import load_history

logger = logging.getLogger(__name__)

DEFAULT_HOLD_DAYS = 5
DEFAULT_TAKE_PROFIT = 0.08
DEFAULT_STOP_LOSS = -0.04


def simulate_trade(ticker: str, signal_date: pd.Timestamp,
                    hold_days: int = DEFAULT_HOLD_DAYS,
                    take_profit: float = DEFAULT_TAKE_PROFIT,
                    stop_loss: float = DEFAULT_STOP_LOSS) -> Optional[Dict]:
    """模擬單筆交易

    Returns:
      {
        'ticker', 'signal_date', 'entry_date', 'entry_price',
        'exit_date', 'exit_price', 'exit_reason', 'return_pct', 'days_held'
      }
      或 None（資料不足 / 無法成交）
    """
    df = load_history(ticker, lookback_days=20,
                        as_of_date=signal_date + pd.Timedelta(days=hold_days + 10))
    if df is None or len(df) < 5:
        return None

    # 切到 signal_date 後的 bars
    after = df.loc[signal_date:]
    if len(after) < 2:
        return None

    # 隔日開盤進場（first bar after signal_date）
    next_bars = after.iloc[1:]
    if len(next_bars) == 0:
        return None
    entry_bar = next_bars.iloc[0]
    entry_date = next_bars.index[0]
    entry_price = float(entry_bar['Open'])
    if entry_price <= 0:
        return None

    # 持有期間，逐日檢查停利停損
    hold_bars = next_bars.iloc[:hold_days + 1]   # +1 留下出場日
    if len(hold_bars) == 0:
        return None

    exit_idx = None
    exit_price = None
    exit_reason = None

    for i in range(len(hold_bars)):
        bar = hold_bars.iloc[i]
        close_price = float(bar['Close'])
        if close_price <= 0: continue
        ret = (close_price - entry_price) / entry_price

        if ret >= take_profit:
            exit_idx = i
            exit_price = entry_price * (1 + take_profit)  # 假設精確止盈
            exit_reason = 'TP'
            break
        if ret <= stop_loss:
            exit_idx = i
            exit_price = entry_price * (1 + stop_loss)    # 假設精確止損
            exit_reason = 'SL'
            break

    if exit_idx is None:
        # 持滿 hold_days 後收盤出場
        if len(hold_bars) >= hold_days:
            exit_idx = hold_days - 1
            exit_price = float(hold_bars.iloc[exit_idx]['Close'])
            exit_reason = 'TIME'
        else:
            # 資料不足 hold_days，無法判斷
            return None

    exit_date = hold_bars.index[exit_idx]
    final_ret = (exit_price - entry_price) / entry_price

    return {
        'ticker': ticker,
        'signal_date': signal_date.strftime('%Y-%m-%d'),
        'entry_date': entry_date.strftime('%Y-%m-%d'),
        'entry_price': round(entry_price, 4),
        'exit_date': exit_date.strftime('%Y-%m-%d'),
        'exit_price': round(exit_price, 4),
        'exit_reason': exit_reason,
        'return_pct': round(final_ret, 4),
        'days_held': exit_idx + 1,
    }


def run_backtest(start_date: str, end_date: str,
                  groups: Optional[List[str]] = None,
                  hold_days: int = DEFAULT_HOLD_DAYS,
                  take_profit: float = DEFAULT_TAKE_PROFIT,
                  stop_loss: float = DEFAULT_STOP_LOSS,
                  scan_interval_days: int = 1,
                  ) -> Dict:
    """跑回測

    Args:
        start_date: 'YYYY-MM-DD' 起始
        end_date: 'YYYY-MM-DD' 結束
        groups: 篩選某些 group；None = 全部
        scan_interval_days: 多少天掃一次（1=每日，5=每週）

    Returns:
        report dict
    """
    mapping = get_default_mapping()
    dates = pd.date_range(start_date, end_date, freq='B')[::scan_interval_days]

    all_signals = []   # 紀錄每個訊號 + 其 score
    all_trades = []    # 訊號對應的交易結果

    print(f'回測 {start_date} → {end_date}（共 {len(dates)} 個掃描日）')
    for d in tqdm(dates, desc='Scanning'):
        try:
            candidates = scan_all_groups(d, mapping, group_filter=groups)
        except Exception as e:
            logger.debug(f'{d}: scan error: {e}')
            continue

        for c in candidates:
            all_signals.append(c)
            trade = simulate_trade(c['ticker'], d,
                                     hold_days=hold_days,
                                     take_profit=take_profit,
                                     stop_loss=stop_loss)
            if trade is not None:
                trade.update({
                    'score': c['score'],
                    'group': c.get('group', '?'),
                    'leader': c['leader'],
                })
                all_trades.append(trade)

    # ─── 聚合指標 ────────
    report = _compute_report(all_signals, all_trades, start_date, end_date)
    return report


def _compute_report(signals, trades, start_date, end_date):
    n_sigs = len(signals)
    n_trades = len(trades)
    if n_trades == 0:
        return {
            'meta': {'start': start_date, 'end': end_date,
                      'total_signals': n_sigs, 'total_trades': 0},
            'overall': {}, 'by_score_bucket': {}, 'by_group': {},
        }

    rets = np.array([t['return_pct'] for t in trades])
    overall = {
        'total_signals': n_sigs,
        'total_trades': n_trades,
        'win_rate': float((rets > 0).mean() * 100),
        'mean_return': float(rets.mean() * 100),
        'median_return': float(np.median(rets) * 100),
        'std_return': float(rets.std() * 100),
        'max_win': float(rets.max() * 100),
        'max_loss': float(rets.min() * 100),
        'tp_hits': sum(1 for t in trades if t['exit_reason'] == 'TP'),
        'sl_hits': sum(1 for t in trades if t['exit_reason'] == 'SL'),
        'time_exits': sum(1 for t in trades if t['exit_reason'] == 'TIME'),
        'avg_days_held': float(np.mean([t['days_held'] for t in trades])),
    }

    # 按 score bucket
    buckets = {
        '0.45-0.60': [t for t in trades if 0.45 <= t['score'] < 0.60],
        '0.60-0.75': [t for t in trades if 0.60 <= t['score'] < 0.75],
        '0.75+':     [t for t in trades if t['score'] >= 0.75],
    }
    by_bucket = {}
    for name, ts in buckets.items():
        if not ts:
            by_bucket[name] = {'n': 0}
            continue
        r = np.array([t['return_pct'] for t in ts])
        by_bucket[name] = {
            'n': len(ts),
            'win_rate': float((r > 0).mean() * 100),
            'mean_return': float(r.mean() * 100),
            'median_return': float(np.median(r) * 100),
        }

    # 按 group
    groups = sorted(set(t['group'] for t in trades))
    by_group = {}
    for g in groups:
        ts = [t for t in trades if t['group'] == g]
        r = np.array([t['return_pct'] for t in ts])
        by_group[g] = {
            'n': len(ts),
            'win_rate': float((r > 0).mean() * 100),
            'mean_return': float(r.mean() * 100),
        }

    return {
        'meta': {'start': start_date, 'end': end_date,
                  'total_signals': n_sigs, 'total_trades': n_trades},
        'overall': overall,
        'by_score_bucket': by_bucket,
        'by_group': by_group,
        'trades': trades,
    }


def print_report(report: Dict):
    print('\n' + '='*70)
    print(f'Sympathy Play Backtest Report')
    print(f'Period: {report["meta"]["start"]} to {report["meta"]["end"]}')
    print(f'Total signals: {report["meta"]["total_signals"]}'
          f' | Trades evaluated: {report["meta"]["total_trades"]}')
    print('='*70)

    if not report['overall']:
        print('(no trades — skip stats)')
        return

    o = report['overall']
    print('\nOverall:')
    print(f'  Win rate:       {o["win_rate"]:>6.1f}%')
    print(f'  Avg return:     {o["mean_return"]:>+6.2f}%')
    print(f'  Median return:  {o["median_return"]:>+6.2f}%')
    print(f'  Max win:        {o["max_win"]:>+6.2f}%')
    print(f'  Max loss:       {o["max_loss"]:>+6.2f}%')
    print(f'  TP hits:        {o["tp_hits"]:>6d}')
    print(f'  SL hits:        {o["sl_hits"]:>6d}')
    print(f'  TIME exits:     {o["time_exits"]:>6d}')
    print(f'  Avg days held:  {o["avg_days_held"]:>6.1f}')

    print('\nBy score bucket:')
    for name, b in report['by_score_bucket'].items():
        if b['n'] == 0:
            print(f'  {name}: (n=0)')
        else:
            print(f'  {name}: win {b["win_rate"]:>5.1f}%, '
                  f'avg {b["mean_return"]:>+5.2f}%, '
                  f'median {b["median_return"]:>+5.2f}%  (n={b["n"]})')

    print('\nBy group:')
    for g, info in sorted(report['by_group'].items(),
                            key=lambda x: -x[1]['mean_return']):
        print(f'  {g:<20s}  win {info["win_rate"]:>5.1f}%, '
              f'avg {info["mean_return"]:>+5.2f}%  (n={info["n"]})')


def main():
    p = argparse.ArgumentParser(description='Sympathy Play Backtest (Phase 2)')
    p.add_argument('--months', type=int, default=12)
    p.add_argument('--start', type=str, default=None)
    p.add_argument('--end', type=str, default=None)
    p.add_argument('--hold-days', type=int, default=DEFAULT_HOLD_DAYS)
    p.add_argument('--take-profit', type=float, default=DEFAULT_TAKE_PROFIT)
    p.add_argument('--stop-loss', type=float, default=DEFAULT_STOP_LOSS)
    p.add_argument('--groups', type=str, default=None)
    p.add_argument('--interval', type=int, default=1,
                    help='Scan interval days (1=daily, 5=weekly)')
    p.add_argument('--save', type=str, default=None)
    args = p.parse_args()

    if args.end is None:
        # 預設用 data_cache 最新可用日期當 end（往前推一個月避免邊界）
        args.end = (pd.Timestamp.now() - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
    if args.start is None:
        args.start = (pd.Timestamp(args.end)
                       - pd.Timedelta(days=args.months * 30)).strftime('%Y-%m-%d')

    groups = args.groups.split(',') if args.groups else None
    report = run_backtest(args.start, args.end,
                           groups=groups,
                           hold_days=args.hold_days,
                           take_profit=args.take_profit,
                           stop_loss=args.stop_loss,
                           scan_interval_days=args.interval)
    print_report(report)

    if args.save:
        os.makedirs(os.path.dirname(args.save) or '.', exist_ok=True)
        with open(args.save, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print(f'\n[Saved] {args.save}')


if __name__ == '__main__':
    main()
