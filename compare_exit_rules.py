"""賣點規則比較回測 v9.34
==========================

對前次 LIQUID_3000 RS≥70 篩出的 363 檔，跑 9 種 exit rule 比較：

  1. lookforward_swing   — look-forward swing high midpoint（作弊上限基準）
  2. chandelier_3atr     — Chandelier trailing stop
  3. ema20_trail         — Close 連 2b < EMA20
  4. bb_upper_reject     — High ≥ BB Upper 且 Close < BB Upper
  5. climax_reverse      — 量爆 + 寬幅 + 紅變黑
  6. death_cross         — EMA20 < EMA60 連 2b
  7. sma40_break         — Close < SMA40 (8 週)
  8. time_stop_30        — 30 bar 強制出
  9. hybrid              — chandelier OR ema20 OR climax 任一

進場條件全部相同：setup + buypoint
防守性停損（hard stop -1.5×ATR / -3%）所有規則都套用
"""
from __future__ import annotations

import sys
import io
import os
import time
import argparse
from pathlib import Path

try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                                         line_buffering=True)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8',
                                         line_buffering=True)
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from intraday.data import get_intraday
from intraday.strategy import scan_with_exit_rule, summarize_trades, EXIT_RULES


EXIT_RULE_ORDER = [
    'lookforward_swing',
    'chandelier_3atr',
    'ema20_trail',
    'bb_upper_reject',
    'climax_reverse',
    'death_cross',
    'sma40_break',
    'time_stop_30',
    'hybrid',
    'time30_or_sma40',    # 🆕 v9.34 推薦混合
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='backtest_swing_us_LIQUID_3000_RS70_1d_252b.csv',
                         help='前次 RS≥70 篩選結果 CSV')
    parser.add_argument('--tf', default='1d')
    parser.add_argument('--lookback', type=int, default=252)
    parser.add_argument('--limit', type=int, default=0,
                         help='只跑前 N 檔（0=全部，debug 用）')
    parser.add_argument('--out', default='exit_rule_comparison.csv')
    args = parser.parse_args()

    # 載入 ticker list
    p = Path(args.input)
    if not p.exists():
        print(f'❌ Input 檔不存在：{p}')
        return
    df_in = pd.read_csv(p)
    tickers = df_in['ticker'].tolist()
    if args.limit > 0:
        tickers = tickers[:args.limit]
    print(f'{"="*70}')
    print(f'賣點規則比較回測 — {len(tickers)} 檔 (TF={args.tf}, lookback={args.lookback}b)')
    print(f'{"="*70}\n')

    # 預先把所有 DataFrame 載入記憶體（一次 IO，重複給 9 個規則用）
    print(f'[Phase 1] 載入 {len(tickers)} 檔資料...')
    t0 = time.time()
    dfs = {}
    for i, tk in enumerate(tickers):
        if i > 0 and i % 50 == 0:
            print(f'  [{i:3d}/{len(tickers)}] {time.time()-t0:.0f}s')
        try:
            df = get_intraday(tk, args.tf, market='us')
            if df is not None and len(df) >= 50:
                dfs[tk] = df
        except Exception:
            pass
    print(f'  已載入 {len(dfs)} 檔｜{time.time()-t0:.0f}s\n')

    # 逐規則回測
    results_per_rule = {}
    all_trades_per_rule = {}

    for rule in EXIT_RULE_ORDER:
        print(f'[Rule] {rule}...')
        t1 = time.time()
        all_trades = []
        per_ticker = []
        for tk, df in dfs.items():
            try:
                trades = scan_with_exit_rule(
                    df, market='us', lookback_bars=args.lookback,
                    tf=args.tf, exit_rule=rule)
                if trades:
                    all_trades.extend(trades)
                    stats = summarize_trades(trades)
                    per_ticker.append({
                        'ticker': tk,
                        'trades': stats.get('n', 0),
                        'win_rate': stats.get('win_rate', 0),
                        'avg_pnl_pct': stats.get('avg_pnl_pct', 0),
                        'total_pnl_pct': round(
                            sum(t['pnl_pct'] for t in trades
                                if not t.get('open', False)), 2),
                        'open_n': stats.get('open', 0),
                    })
            except Exception as e:
                print(f'    ❌ {tk}: {type(e).__name__}: {str(e)[:50]}')

        agg = summarize_trades(all_trades)
        results_per_rule[rule] = {
            **agg,
            'tickers_with_trades': len(per_ticker),
            'total_pnl_sum': round(
                sum(t['pnl_pct'] for t in all_trades
                    if not t.get('open', False)), 2),
            'elapsed_s': round(time.time() - t1, 1),
        }
        all_trades_per_rule[rule] = per_ticker
        n = agg.get('n', 0)
        wr = agg.get('win_rate', 0)
        avg = agg.get('avg_pnl_pct', 0)
        print(f'    trades: {n} | wr: {wr:.1f}% | avg P/L: {avg:+.2f}% | '
              f'time: {time.time()-t1:.0f}s')

    # ── Summary table ──
    print(f'\n{"="*100}')
    print('賣點規則比較總覽（按 avg P/L 排序）')
    print(f'{"="*100}')

    summary_rows = []
    for rule, stats in results_per_rule.items():
        summary_rows.append({
            'rule': rule,
            'trades': stats.get('n', 0),
            'win_rate': stats.get('win_rate', 0),
            'avg_pnl_pct': stats.get('avg_pnl_pct', 0),
            'best_pct': stats.get('best_pnl_pct', 0),
            'worst_pct': stats.get('worst_pnl_pct', 0),
            'avg_hold_bars': stats.get('avg_holding_bars', 0),
            'tickers_traded': stats.get('tickers_with_trades', 0),
            'total_sum_pct': stats.get('total_pnl_sum', 0),
            'open_n': stats.get('open', 0),
            'time_s': stats.get('elapsed_s', 0),
        })
    df_summary = pd.DataFrame(summary_rows).sort_values(
        'avg_pnl_pct', ascending=False)
    print(df_summary.to_string(index=False))

    # 儲存
    df_summary.to_csv(args.out, index=False, encoding='utf-8-sig')
    print(f'\n✅ 結果存：{args.out}')


if __name__ == '__main__':
    main()
