"""波段戰法 — 美股 RS≥70 全市場回測  v9.34
==================================================

對美股 universe（預設 LIQUID_3000）每檔：
  1. 抓 1d 資料 → 算 13w/26w/39w/52w 報酬
  2. 計算 RS rating（Minervini 加權 percentile，0-100）
  3. 篩選 RS ≥ 70 的標的
  4. 跑 scan_swing_profit_signals 回測
  5. 輸出 CSV + 主控台 summary

用法:
  python backtest_swing_us.py                                # 預設 LIQUID_3000, TF=1d, RS≥70
  python backtest_swing_us.py --rs-min 80                    # 改篩 RS≥80
  python backtest_swing_us.py --universe SP500               # 改用 SP500_CORE 樣本
  python backtest_swing_us.py --tf 4h --lookback 365         # 改用 4h TF
  python backtest_swing_us.py --limit 100                    # 只跑前 100 檔（debug）
"""
from __future__ import annotations

import sys
import io
import os
import time
import argparse
from pathlib import Path

# Windows console: force UTF-8 output (line-buffered，避免 progress 卡住)
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

from universes.us_universe import get_universe
from intraday.data import get_intraday
from intraday.strategy import scan_swing_profit_signals, summarize_trades
from sepa_vcp import compute_rs_ratings, compute_returns


def _fmt_eta(elapsed: float, done: int, total: int) -> str:
    if done == 0:
        return '?'
    rate = elapsed / done
    remaining = rate * (total - done)
    m, s = divmod(int(remaining), 60)
    return f'{m}m{s:02d}s'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--universe', default='LIQUID_3000',
                         choices=['SP500', 'RUSSELL1000', 'LIQUID_3000', 'ALL_US'])
    parser.add_argument('--rs-min', type=int, default=70,
                         help='最低 RS rating（預設 70）')
    parser.add_argument('--tf', default='1d',
                         help='回測 TF: 1d / 4h / 1h / 30m / 15m')
    parser.add_argument('--lookback', type=int, default=252,
                         help='回測 lookback bars（預設 252 ≈ 1 year on 1d）')
    parser.add_argument('--limit', type=int, default=0,
                         help='只跑前 N 檔（0 = 全部，debug 用）')
    parser.add_argument('--out', default='',
                         help='輸出 CSV 檔名（預設自動命名）')
    parser.add_argument('--min-trades', type=int, default=1,
                         help='Summary 只顯示至少 N 筆交易的標的（預設 1）')
    args = parser.parse_args()

    # 自動命名 output
    if not args.out:
        args.out = (f'backtest_swing_us_{args.universe}_RS{args.rs_min}_'
                    f'{args.tf}_{args.lookback}b.csv')

    print(f'{"="*70}')
    print(f'波段戰法 — 美股 RS≥{args.rs_min} 回測')
    print(f'  Universe : {args.universe}')
    print(f'  TF       : {args.tf}')
    print(f'  Lookback : {args.lookback} bars')
    print(f'  Limit    : {args.limit or "ALL"}')
    print(f'  Output   : {args.out}')
    print(f'{"="*70}')

    # ── Phase 1: 載入 universe ──
    tickers = get_universe(args.universe)
    if args.limit > 0:
        tickers = tickers[:args.limit]
    print(f'\n[Phase 1] Universe: {len(tickers)} tickers')

    # ── Phase 2: 抓 1d 資料 + 計算 returns ──
    print(f'\n[Phase 2] 抓 1d 資料 + 計算 returns (給 RS 用)...')
    returns_dict: dict = {}
    dfs_1d: dict = {}
    skipped: list = []
    t0 = time.time()
    PROGRESS_EVERY = max(1, len(tickers) // 30)

    for i, tk in enumerate(tickers):
        if i % PROGRESS_EVERY == 0 and i > 0:
            elapsed = time.time() - t0
            eta = _fmt_eta(elapsed, i, len(tickers))
            print(f'  [{i:4d}/{len(tickers)}] {elapsed:.0f}s elapsed, ETA {eta}, '
                  f'cached: {len(returns_dict)}, skipped: {len(skipped)}')
        try:
            df = get_intraday(tk, '1d', market='us')
            if df is None or len(df) < 260:
                skipped.append(tk)
                continue
            r = compute_returns(df)
            if not r:
                skipped.append(tk)
                continue
            returns_dict[tk] = r
            dfs_1d[tk] = df
        except Exception:
            skipped.append(tk)

    phase2_elapsed = time.time() - t0
    print(f'  資料抓取完成: {len(returns_dict)}/{len(tickers)} 有效 '
          f'(skipped {len(skipped)}) ｜ {phase2_elapsed:.0f}s')

    # ── Phase 3: 計算 RS rating ──
    print(f'\n[Phase 3] 計算 RS rating...')
    rs_ratings = compute_rs_ratings(returns_dict)
    print(f'  RS rating 完成: {len(rs_ratings)} 檔')

    # RS 分佈
    if rs_ratings:
        rs_vals = list(rs_ratings.values())
        print(f'  RS 分佈: min={min(rs_vals):.1f}, '
              f'med={np.median(rs_vals):.1f}, max={max(rs_vals):.1f}')

    # ── Phase 4: 篩 RS ≥ min ──
    selected = sorted(
        [(tk, r) for tk, r in rs_ratings.items() if r >= args.rs_min],
        key=lambda x: -x[1]    # RS 高到低
    )
    print(f'\n[Phase 4] RS ≥ {args.rs_min}: {len(selected)} 檔 '
          f'({len(selected)/max(1,len(rs_ratings))*100:.1f}% of universe)')
    if not selected:
        print('  ⚠️  沒有任何標的達到 RS 門檻，結束。')
        return

    # ── Phase 5: 回測每一檔 ──
    print(f'\n[Phase 5] 回測 {len(selected)} 檔 (TF={args.tf}, '
          f'lookback={args.lookback}b)...')
    results: list = []
    t1 = time.time()
    PROG_BT = max(1, len(selected) // 20)

    for i, (tk, rs) in enumerate(selected):
        if i % PROG_BT == 0 and i > 0:
            elapsed = time.time() - t1
            eta = _fmt_eta(elapsed, i, len(selected))
            print(f'  [{i:4d}/{len(selected)}] {elapsed:.0f}s elapsed, ETA {eta}')
        try:
            # 1d 可重用，其他 TF 重抓
            if args.tf == '1d':
                df = dfs_1d.get(tk)
            else:
                df = get_intraday(tk, args.tf, market='us')

            if df is None or len(df) < 50:
                continue

            trades = scan_swing_profit_signals(
                df, market='us', lookback_bars=args.lookback, tf=args.tf)
            stats = summarize_trades(trades)
            n = stats.get('n', 0)

            results.append({
                'ticker': tk,
                'rs': round(rs, 1),
                'trades': n,
                'win_n': stats.get('win_n', 0),
                'loss_n': stats.get('loss_n', 0),
                'win_rate': stats.get('win_rate', 0),
                'avg_pnl_pct': stats.get('avg_pnl_pct', 0),
                'best_pct': stats.get('best_pnl_pct', 0),
                'worst_pct': stats.get('worst_pnl_pct', 0),
                'avg_hold_bars': stats.get('avg_holding_bars', 0),
                'open_pos': stats.get('open', 0),
                # 累計 P/L（買入 1 股的累計 dollar 報酬）
                'total_pnl_pct': round(
                    sum(t['pnl_pct'] for t in trades if not t.get('open', False)), 2),
            })
        except Exception as e:
            print(f'  ❌ {tk}: {type(e).__name__}: {str(e)[:60]}')

    phase5_elapsed = time.time() - t1
    print(f'  回測完成: {len(results)} 檔 ｜ {phase5_elapsed:.0f}s')

    if not results:
        print('  ⚠️  沒有回測結果。')
        return

    # ── Phase 6: 排序 + 輸出 ──
    df_out = pd.DataFrame(results)
    df_out = df_out.sort_values('avg_pnl_pct', ascending=False)
    df_out.to_csv(args.out, index=False, encoding='utf-8-sig')
    print(f'\n[Phase 6] 結果已存: {args.out} ({len(df_out)} rows)')

    # ── Summary ──
    df_with = df_out[df_out['trades'] >= args.min_trades].copy()
    print(f'\n{"="*70}')
    print(f'總覽（{len(df_with)} 檔有 ≥ {args.min_trades} 筆交易）')
    print(f'{"="*70}')

    if len(df_with) == 0:
        print('  ⚠️  無交易標的，可能因 buypoint 太嚴格。')
        return

    total_wins = int(df_with['win_n'].sum())
    total_loss = int(df_with['loss_n'].sum())
    total_trades = total_wins + total_loss
    overall_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0
    weighted_avg = (
        (df_with['avg_pnl_pct'] * df_with['trades']).sum() / df_with['trades'].sum()
        if df_with['trades'].sum() > 0 else 0
    )

    print(f'  累計交易    : {total_trades} 筆')
    print(f'  整體勝率    : {overall_wr:.1f}% ({total_wins}W / {total_loss}L)')
    print(f'  加權平均 P/L: {weighted_avg:+.2f}%')
    print(f'  最佳單檔    : {df_with.iloc[0]["ticker"]} '
          f'avg {df_with["avg_pnl_pct"].max():+.2f}% '
          f'(RS={df_with.iloc[0]["rs"]:.0f}, '
          f'{int(df_with.iloc[0]["trades"])} trades)')
    df_with_sorted_asc = df_with.sort_values('avg_pnl_pct')
    print(f'  最差單檔    : {df_with_sorted_asc.iloc[0]["ticker"]} '
          f'avg {df_with["avg_pnl_pct"].min():+.2f}% '
          f'(RS={df_with_sorted_asc.iloc[0]["rs"]:.0f}, '
          f'{int(df_with_sorted_asc.iloc[0]["trades"])} trades)')

    # Top 20 / Bottom 5
    print(f'\n── Top 20 (by avg P/L) ──')
    print(df_with.head(20).to_string(index=False))
    if len(df_with) > 25:
        print(f'\n── Bottom 5 (by avg P/L) ──')
        print(df_with.tail(5).to_string(index=False))


if __name__ == '__main__':
    main()
