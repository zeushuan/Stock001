"""重入場分佈分析 + 指定 ticker 回測 v9.34
============================================

1. 對前次 363 檔 RS≥70 加上指定 5 檔，跑 time_stop_30
2. 分析每檔的「重入場分佈」：
   - 總交易筆數
   - 砍倉次數（hard stop / 急停損）
   - Time 30b 出場次數
   - 同一 trade 是否來自「砍倉後重入場」
3. 個別 ticker 詳細交易序列
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
from intraday.strategy import scan_with_exit_rule


# 指定的 5 檔
EXTRA_TICKERS = ['BE', 'FCEL', 'AMDL', 'NVDL', 'CEJ']


def classify_exit(reason: str) -> str:
    """歸類出場原因"""
    if not reason: return 'open'
    r = reason
    if 'Time stop' in r: return 'time30'
    if '停損' in r or '-3' in r or 'stop' in r.lower(): return 'stop'
    if '持倉' in r or 'lookback' in r: return 'open'
    return 'other'


def main():
    input_csv = 'backtest_swing_us_LIQUID_3000_RS70_1d_252b.csv'
    p = Path(input_csv)
    if not p.exists():
        print(f'❌ 找不到 {input_csv}')
        return
    df_in = pd.read_csv(p)
    base_tickers = df_in['ticker'].tolist()

    # 合併指定 ticker（dedupe）
    extra_new = [t for t in EXTRA_TICKERS if t not in base_tickers]
    extra_already = [t for t in EXTRA_TICKERS if t in base_tickers]
    all_tickers = base_tickers + extra_new

    print(f'{"="*70}')
    print(f'重入場分佈 + 指定 ticker 回測')
    print(f'  Base: {len(base_tickers)} 檔 RS≥70')
    print(f'  追加: {len(extra_new)} 檔 (新): {extra_new}')
    print(f'        {len(extra_already)} 檔 (已在 base): {extra_already}')
    print(f'  Total: {len(all_tickers)} 檔')
    print(f'  Rule: time_stop_30, lookback 252b')
    print(f'{"="*70}\n')

    # 回測
    print('[Phase 1] 回測中...')
    t0 = time.time()
    ticker_results = {}

    for i, tk in enumerate(all_tickers):
        if i > 0 and i % 60 == 0:
            print(f'  [{i:3d}/{len(all_tickers)}] {time.time()-t0:.0f}s')
        try:
            df = get_intraday(tk, '1d', market='us')
            if df is None or len(df) < 50:
                continue
            trades = scan_with_exit_rule(
                df, market='us', lookback_bars=252, tf='1d',
                exit_rule='time_stop_30')
            ticker_results[tk] = trades
        except Exception as e:
            print(f'  ❌ {tk}: {type(e).__name__}: {str(e)[:50]}')

    print(f'  回測完成 {len(ticker_results)} 檔｜{time.time()-t0:.0f}s\n')

    # ── 分析 1：重入場分佈統計 ──
    print('═'*70)
    print('【分析 1】重入場分佈統計')
    print('═'*70)

    n_trades_dist = []   # 每檔交易筆數分佈
    n_stops_dist = []    # 每檔砍倉次數分佈
    n_t30_dist = []      # 每檔 time_stop 次數分佈
    re_entry_after_stop_total = 0   # 砍倉後重入場的 trade 總數

    detail_rows = []

    for tk, trades in ticker_results.items():
        n = len(trades)
        if n == 0:
            n_trades_dist.append(0)
            continue
        exits_kind = [classify_exit(t.get('exit_reason', '')) for t in trades]
        n_stops = exits_kind.count('stop')
        n_t30 = exits_kind.count('time30')
        n_open = exits_kind.count('open')

        # 算重入場（任何 trade #2+ 都算重入場；其中 "砍倉後" 重入場 = prev_kind=stop）
        re_after_stop = 0
        for k in range(1, n):
            if exits_kind[k - 1] == 'stop':
                re_after_stop += 1
        re_entry_after_stop_total += re_after_stop

        n_trades_dist.append(n)
        n_stops_dist.append(n_stops)
        n_t30_dist.append(n_t30)

        # 計算累計 P/L 與勝率
        closed = [t for t in trades if not t.get('open', False)]
        wins = sum(1 for t in closed if t['pnl_pct'] > 0)
        losses = len(closed) - wins
        total_pnl = sum(t['pnl_pct'] for t in closed)
        avg_pnl = total_pnl / len(closed) if closed else 0

        detail_rows.append({
            'ticker': tk,
            'trades': n,
            'wins': wins,
            'losses': losses,
            'time30': n_t30,
            'stops': n_stops,
            'open': n_open,
            'reentry_after_stop': re_after_stop,
            'avg_pnl_pct': round(avg_pnl, 2),
            'total_pnl_pct': round(total_pnl, 2),
        })

    df_det = pd.DataFrame(detail_rows)

    # 全市場分佈
    n_trades_dist = pd.Series(n_trades_dist)
    print(f'\n單檔交易筆數分佈（總 {len(ticker_results)} 檔）:')
    print(f'  0 trades : {(n_trades_dist == 0).sum():3d} 檔')
    print(f'  1 trade  : {(n_trades_dist == 1).sum():3d} 檔')
    print(f'  2 trades : {(n_trades_dist == 2).sum():3d} 檔')
    print(f'  3 trades : {(n_trades_dist == 3).sum():3d} 檔')
    print(f'  4 trades : {(n_trades_dist == 4).sum():3d} 檔')
    print(f'  5 trades : {(n_trades_dist == 5).sum():3d} 檔')
    print(f'  6+ trades: {(n_trades_dist >= 6).sum():3d} 檔')
    print(f'  平均      : {n_trades_dist.mean():.2f} 筆/檔')
    print(f'  最多      : {n_trades_dist.max()} 筆')

    if len(df_det) > 0:
        total_trades = int(df_det['trades'].sum())
        total_stops = int(df_det['stops'].sum())
        total_t30 = int(df_det['time30'].sum())
        total_open = int(df_det['open'].sum())
        print(f'\n出場類型分佈:')
        print(f'  總交易筆數         : {total_trades}')
        print(f'  Time stop 30b      : {total_t30:4d} ({total_t30/max(1,total_trades)*100:.1f}%)')
        print(f'  砍倉 (-1.5ATR/-3%) : {total_stops:4d} ({total_stops/max(1,total_trades)*100:.1f}%)')
        print(f'  Open (lookback 結束): {total_open:4d} ({total_open/max(1,total_trades)*100:.1f}%)')
        print(f'\n  「砍倉後重入場」次數: {re_entry_after_stop_total} '
              f'({re_entry_after_stop_total/max(1,total_trades)*100:.1f}% of total)')

    # ── 分析 2：「最頻繁重入場」TOP 10 ──
    print(f'\n═'*36)
    print(f'\n【分析 2】Top 10 — 最常砍倉後重入場（看不適合的標的）')
    print('═'*70)
    top_reentry = df_det.sort_values(
        'reentry_after_stop', ascending=False).head(10)
    print(top_reentry.to_string(index=False))

    # ── 分析 3：5 檔指定 ticker 詳細 ──
    print(f'\n═'*36)
    print(f'\n【分析 3】指定 5 檔 ticker 詳細交易序列')
    print('═'*70)

    for tk in EXTRA_TICKERS:
        print(f'\n──── {tk} ────')
        if tk not in ticker_results:
            print(f'  ⚠️  資料不足或抓取失敗')
            continue
        trades = ticker_results[tk]
        if len(trades) == 0:
            print(f'  ⏸️  無交易（lookback 252b 內無 setup+buypoint 觸發）')
            continue

        # 詳細 row
        row = df_det[df_det['ticker'] == tk]
        if len(row) > 0:
            r = row.iloc[0]
            print(f'  總計: {int(r["trades"])} trades | '
                  f'{int(r["wins"])} 勝 / {int(r["losses"])} 敗 | '
                  f'avg P/L {r["avg_pnl_pct"]:+.2f}% | '
                  f'累計 {r["total_pnl_pct"]:+.2f}%')
            print(f'  出場: Time30 {int(r["time30"])} | 砍倉 {int(r["stops"])} | '
                  f'Open {int(r["open"])} | 砍後重入 {int(r["reentry_after_stop"])}')

        # 每筆 trade 詳細
        for i_t, t in enumerate(trades, 1):
            et = t['entry_time'].strftime('%Y-%m-%d')
            xt = (t['exit_time'].strftime('%Y-%m-%d')
                   if t.get('exit_time') is not None else 'open')
            reason = (t.get('exit_reason') or '').encode('ascii', 'ignore').decode().strip()
            kind = classify_exit(t.get('exit_reason', ''))
            kind_emoji = {'time30':'⏰','stop':'🛑','open':'🟡','other':'❓'}.get(kind,'?')
            pnl = t['pnl_pct']
            pnl_emoji = '💰' if pnl > 0 else '📉' if pnl < 0 else '⚪'
            exit_price = t.get('exit_price') or 0
            print(f'  #{i_t}: {et} → {xt} | ${t["entry_price"]:.2f} → ${exit_price:.2f} | '
                  f'{pnl_emoji}{pnl:+6.2f}% | hold {t["holding_bars"]:>3}b | {kind_emoji} {reason[:30]}')

    # 寫 detail CSV
    out_csv = 'reentry_analysis.csv'
    df_det.sort_values('total_pnl_pct', ascending=False).to_csv(
        out_csv, index=False, encoding='utf-8-sig')
    print(f'\n\n✅ Detail CSV: {out_csv}')


if __name__ == '__main__':
    main()
