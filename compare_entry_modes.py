"""進場模式比較：pullback vs breakout vs both
=================================================

對 363 RS≥70 + 5 指定 ticker，跑 3 種 entry_mode × time_stop_30
"""
from __future__ import annotations

import sys
import io
import os
import time
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
from intraday.strategy import scan_with_exit_rule, summarize_trades

EXTRA = ['BE', 'FCEL', 'AMDL', 'NVDL']
MODES = ['pullback', 'breakout', 'sepa_vcp', 'all']


def main():
    df_in = pd.read_csv('backtest_swing_us_LIQUID_3000_RS70_1d_252b.csv')
    base = df_in['ticker'].tolist()
    all_tickers = base + [t for t in EXTRA if t not in base]

    # 載入資料
    print(f'載入 {len(all_tickers)} 檔...')
    t0 = time.time()
    dfs = {}
    for tk in all_tickers:
        try:
            df = get_intraday(tk, '1d', market='us')
            if df is not None and len(df) >= 50:
                dfs[tk] = df
        except Exception:
            pass
    print(f'  {len(dfs)} 檔成功｜{time.time()-t0:.0f}s\n')

    # 對 3 種 mode 跑回測
    results_per_mode = {}
    for mode in MODES:
        print(f'[Mode] {mode}...')
        t1 = time.time()
        all_trades = []
        per_ticker = {}
        for tk, df in dfs.items():
            trades = scan_with_exit_rule(
                df, market='us', lookback_bars=252, tf='1d',
                exit_rule='time_stop_30', entry_mode=mode)
            if trades:
                all_trades.extend(trades)
                stats = summarize_trades(trades)
                bo_n = sum(1 for t in trades if 'BO' in str(t.get('entry_mode','')))
                pb_n = sum(1 for t in trades if 'PB' in str(t.get('entry_mode','')))
                sv_n = sum(1 for t in trades if 'SV' in str(t.get('entry_mode','')))
                total_pnl = sum(t['pnl_pct'] for t in trades if not t.get('open', False))
                per_ticker[tk] = {
                    **stats, 'BO': bo_n, 'PB': pb_n, 'SV': sv_n,
                    'total_pnl_pct': round(total_pnl, 2),
                }
        agg = summarize_trades(all_trades)
        bo_total = sum(1 for t in all_trades if 'BO' in str(t.get('entry_mode','')))
        pb_total = sum(1 for t in all_trades if 'PB' in str(t.get('entry_mode','')))
        sv_total = sum(1 for t in all_trades if 'SV' in str(t.get('entry_mode','')))
        agg['BO_total'] = bo_total
        agg['PB_total'] = pb_total
        agg['SV_total'] = sv_total
        agg['elapsed'] = time.time() - t1
        results_per_mode[mode] = (agg, per_ticker)
        print(f'  trades: {agg["n"]} (PB {pb_total}/BO {bo_total}/SV {sv_total}) | '
              f'wr {agg.get("win_rate",0):.1f}% | '
              f'avg {agg.get("avg_pnl_pct",0):+.2f}% | '
              f'{agg["elapsed"]:.0f}s')

    # ── 整體對比 ──
    print(f'\n{"="*90}')
    print('整體對比（363 + 5 = 368 tickers）')
    print(f'{"="*90}')
    print(f"{'Mode':<10} {'Trades':>7} {'PB':>5} {'BO':>5} {'SV':>5} {'WR%':>6} {'Avg P/L':>9} {'Best':>8} {'Σ':>9}")
    for mode in MODES:
        agg = results_per_mode[mode][0]
        n = agg.get('n', 0)
        wr = agg.get('win_rate', 0)
        avg = agg.get('avg_pnl_pct', 0)
        best = agg.get('best_pnl_pct', 0)
        pb = agg.get('PB_total', 0)
        bo = agg.get('BO_total', 0)
        sv = agg.get('SV_total', 0)
        total_sum = sum(stats['total_pnl_pct']
                         for stats in results_per_mode[mode][1].values())
        print(f"{mode:<10} {n:>7} {pb:>5} {bo:>5} {sv:>5} {wr:>6.1f} {avg:>+9.2f} {best:>+8.2f} {total_sum:>+9.0f}")

    # ── 5 檔指定 ticker 細節 ──
    print(f'\n{"="*90}')
    print('指定 5 檔（BE/FCEL/AMDL/NVDL）每 mode 結果')
    print(f'{"="*90}')
    for tk in EXTRA:
        print(f'\n── {tk} ──')
        for mode in MODES:
            per_t = results_per_mode[mode][1]
            if tk not in per_t:
                print(f'  {mode:9s}: 無交易')
                continue
            s = per_t[tk]
            print(f'  {mode:9s}: {s["n"]} trades (PB{s["PB"]}/BO{s["BO"]}/SV{s.get("SV",0)}) | '
                  f'wr {s.get("win_rate",0):.1f}% | '
                  f'avg {s.get("avg_pnl_pct",0):+.2f}% | '
                  f'Σ {s["total_pnl_pct"]:+.2f}%')


if __name__ == '__main__':
    main()
