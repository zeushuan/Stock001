"""雙底雙頂 OOS 回測（v9.22.1）
==========================================================
測試假設：依職業交易員方法論，Grade A 訊號是否真的勝率最高？
       三段建倉是否有差別？

實驗設計
---------
- Universe: TW + US data_cache
- 期間：full (2020-2026) + OOS only (2024-2026)
- 進場：每個歷史的雙底/雙頂 breakout 訊號
  → 細分 Grade A / B / C / D
  → 細分 stage A_test / B_breakout / C_retest
- 出場：
  1. target_price（測量移動 = neckline + height）
  2. stop_loss（結構低點）
  3. fixed_30d / fixed_60d / fixed_90d
  4. rsi_80
  5. minervini_combo (target OR stop OR max_90d)

執行
-----
  python analyze_double_oos.py --market both
  python analyze_double_oos.py --oos
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
from double_pattern import detect_double_bottom, detect_double_top

WORKERS = 12


# ────────────────────────────────────────────────────────────────
# Step 1: 在 ticker df 上每隔 N 天掃描，找歷史 Breakout 訊號
# ────────────────────────────────────────────────────────────────

def find_historical_db_signals(df, start_date='2020-01-01',
                                 scan_step=5, side='bull'):
    """為單一 ticker 找所有歷史 double bottom/top breakout 訊號

    Args:
      scan_step: 每 N 天掃一次（5 天足夠）
      side: 'bull' = double bottom, 'bear' = double top

    回傳 list of {
      'idx': 訊號日 idx (= 觸發 breakout 的當天),
      'grade', 'score', 'stage',
      'neckline', 'target', 'stop_loss',
      ... 其他元數據
    }
    """
    if df is None or len(df) < 100:
        return []
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)
    df = df[df.index >= pd.Timestamp(start_date)]
    if len(df) < 80:
        return []

    n = len(df)
    signals = []
    # 🐛 fix v9.22.1：dedupe by pattern (左底/右底日期組合) 避免同 pattern 重複觸發
    seen_patterns = set()

    fn = detect_double_bottom if side == 'bull' else detect_double_top
    breakout_states = ({'B_breakout_buy', 'A_test_buy', 'C_retest_buy'}
                        if side == 'bull' else
                        {'B_breakdown_short', 'A_test_short', 'C_retest_short'})

    for i in range(60, n, scan_step):
        sub_df = df.iloc[:i+1]
        try:
            result = fn(sub_df)
        except Exception:
            continue
        if not result: continue
        if result.get('status') not in breakout_states:
            continue
        # pattern 唯一 key = (左底/頂日期, 右底/頂日期)
        if side == 'bull':
            l_data = result.get('left_bottom') or {}
            r_data = result.get('right_bottom') or {}
        else:
            l_data = result.get('left_top') or {}
            r_data = result.get('right_top') or {}
        pat_key = (l_data.get('date'), r_data.get('date'))
        if pat_key in seen_patterns:
            continue   # 同 pattern 已記錄 → 跳過
        seen_patterns.add(pat_key)

        # 進一步：訊號日必須接近 breakout_idx（避免在突破後很久才記錄）
        valid = result.get('breakout_validity') or {}
        bo_idx = valid.get('breakout_idx')
        if bo_idx is not None:
            # 訊號 idx 應接近 breakout_idx（差 ≤ 3 天）
            if abs(i - bo_idx) > 3:
                # 太晚記錄 → 修正成 breakout_idx + 1
                i = min(bo_idx + 1, n - 1)

        signals.append({
            'idx': i,
            'date': df.index[i].strftime('%Y-%m-%d'),
            'status': result['status'],
            'stage': result.get('entry_stage', 'wait'),
            'grade': result.get('quality_grade', 'D'),
            'score': result.get('quality_score', 0),
            'neckline': result.get('neckline_price', 0),
            'target': result.get('target_price', 0),
            'stop_loss': result.get('stop_loss', 0),
            'side': side,
        })

    return signals


# ────────────────────────────────────────────────────────────────
# Step 2: walk exit
# ────────────────────────────────────────────────────────────────

def walk_exit_double(df, sig, exit_strategy='composite', max_hold=120):
    """從訊號日後一天開始 walk，套用 exit 策略
    回傳 (exit_idx, exit_price, reason)"""
    n = len(df)
    sig_idx = sig['idx']
    entry_i = sig_idx + 1
    if entry_i >= n - 1:
        return None
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    rsi = df['rsi'].values if 'rsi' in df.columns else None

    entry_open = float(o[entry_i])
    if entry_open <= 0 or np.isnan(entry_open):
        return None

    target = sig.get('target', 0)
    stop_loss = sig.get('stop_loss', 0)
    side = sig.get('side', 'bull')

    end = min(entry_i + max_hold, n - 1)

    for k in range(entry_i + 1, end + 1):
        h_k = h[k]; l_k = l[k]; c_k = c[k]
        if np.isnan(h_k) or np.isnan(l_k) or np.isnan(c_k):
            continue

        if exit_strategy == 'composite':
            # 雙底：先測 target，再測 stop
            if side == 'bull':
                if target > 0 and h_k >= target:
                    return (k, target, 'target')
                if stop_loss > 0 and l_k <= stop_loss:
                    return (k, stop_loss, 'stop_loss')
            else:
                if target > 0 and l_k <= target:
                    return (k, target, 'target')
                if stop_loss > 0 and h_k >= stop_loss:
                    return (k, stop_loss, 'stop_loss')
            if k - entry_i >= 90:
                return (k, c_k, 'max_90d')

        elif exit_strategy == 'target_only':
            if side == 'bull' and target > 0 and h_k >= target:
                return (k, target, 'target')
            if side == 'bear' and target > 0 and l_k <= target:
                return (k, target, 'target')
            if k - entry_i >= 120:
                return (k, c_k, 'max_120d')

        elif exit_strategy == 'fixed_30d':
            if k - entry_i >= 30:
                return (k, float(o[k]) if not np.isnan(o[k]) else c_k, 'fixed_30d')
        elif exit_strategy == 'fixed_60d':
            if k - entry_i >= 60:
                return (k, float(o[k]) if not np.isnan(o[k]) else c_k, 'fixed_60d')
        elif exit_strategy == 'fixed_90d':
            if k - entry_i >= 90:
                return (k, float(o[k]) if not np.isnan(o[k]) else c_k, 'fixed_90d')

        elif exit_strategy == 'rsi_80':
            if (side == 'bull' and rsi is not None and not np.isnan(rsi[k])
                and rsi[k] >= 80):
                if k+1 < n and not np.isnan(o[k+1]):
                    return (k+1, float(o[k+1]), 'rsi_80')
                return (k, c_k, 'rsi_80_close')
            if k - entry_i >= 90:
                return (k, c_k, 'max_90d')

    if end + 1 >= n:
        return (n-1, float(c[n-1]) if not np.isnan(c[n-1]) else entry_open, 'end_of_data')
    return (end, float(o[end]) if not np.isnan(o[end]) else c[end], 'max_hold')


# ────────────────────────────────────────────────────────────────
# Step 3: 處理一個 ticker
# ────────────────────────────────────────────────────────────────

EXIT_STRATEGIES = ['composite', 'target_only', 'fixed_30d',
                    'fixed_60d', 'fixed_90d', 'rsi_80']


def process_ticker(args):
    """單 ticker：找所有 double bottom/top 訊號，walk 所有 exit"""
    ticker, start_date = args
    out = {(side, ex): [] for side in ('bull', 'bear') for ex in EXIT_STRATEGIES}
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 100: return out

        for side in ('bull', 'bear'):
            signals = find_historical_db_signals(df, start_date=start_date,
                                                   scan_step=5, side=side)
            for sig in signals:
                entry_i = sig['idx'] + 1
                if entry_i >= len(df) - 1: continue
                entry_open = float(df['Open'].iloc[entry_i])
                if entry_open <= 0 or np.isnan(entry_open): continue

                for exit_strat in EXIT_STRATEGIES:
                    result = walk_exit_double(df, sig, exit_strategy=exit_strat)
                    if not result: continue
                    exit_i, exit_price, reason = result
                    if exit_price is None or exit_price <= 0 or np.isnan(exit_price):
                        continue

                    # 雙底：long position；雙頂：short position（rev gross_ret）
                    if side == 'bull':
                        gross_ret = (exit_price - entry_open) / entry_open
                    else:
                        gross_ret = (entry_open - exit_price) / entry_open
                    net_ret = gross_ret - COST_ROUND_TRIP

                    out[(side, exit_strat)].append({
                        'ticker': ticker,
                        'signal_date': sig['date'],
                        'entry_date': df.index[entry_i].strftime('%Y-%m-%d'),
                        'exit_date': df.index[exit_i].strftime('%Y-%m-%d') if exit_i < len(df) else df.index[-1].strftime('%Y-%m-%d'),
                        'hold_days': exit_i - entry_i,
                        'entry_price': round(entry_open, 4),
                        'exit_price': round(exit_price, 4),
                        'gross_ret': gross_ret,
                        'net_ret': net_ret,
                        'reason': reason,
                        'grade': sig['grade'],
                        'score': sig['score'],
                        'stage': sig['stage'],
                    })
        return out
    except Exception:
        return out


# ────────────────────────────────────────────────────────────────
# Stats + Main
# ────────────────────────────────────────────────────────────────

def trade_stats(trades):
    if not trades: return None
    df = pd.DataFrame(trades)
    n = len(df)
    win_pct = (df['net_ret'] > 0).mean() * 100
    mean_pct = df['net_ret'].mean() * 100
    median_pct = df['net_ret'].median() * 100
    pos_sum = df.loc[df['net_ret'] > 0, 'net_ret'].sum()
    neg_sum = -df.loc[df['net_ret'] < 0, 'net_ret'].sum()
    pf = pos_sum / neg_sum if neg_sum > 0 else 999
    avg_hold = df['hold_days'].mean()
    mean_per_day = mean_pct / avg_hold if avg_hold > 0 else 0

    return {
        'n': n,
        'win_pct': round(win_pct, 2),
        'mean_pct': round(mean_pct, 2),
        'median_pct': round(median_pct, 2),
        'pf': round(pf, 2),
        'avg_hold': round(avg_hold, 1),
        'mean_per_day': round(mean_per_day, 3),
        'best_pct': round(df['net_ret'].max() * 100, 2),
        'worst_pct': round(df['net_ret'].min() * 100, 2),
    }


def run_analysis(market='tw', oos_only=False):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    start = '2024-01-01' if oos_only else '2020-01-01'
    period = 'OOS (2024+)' if oos_only else 'Full (2020+)'

    print(f'\n{flag} 雙底雙頂 OOS 回測  {period}')
    print(f'  Universe: {len(universe)} 檔')
    print(f'  Entry: detect_double_bottom / detect_double_top')
    print(f'  Cost round-trip: {COST_ROUND_TRIP*100:.2f}%')
    print()

    print(f'📊 掃描歷史訊號 + walk exits（{WORKERS} workers）...')
    t0 = time.time()
    args = [(t, start) for t in universe]
    by_combo = {(side, ex): [] for side in ('bull', 'bear') for ex in EXIT_STRATEGIES}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker_out in ex.map(process_ticker, args, chunksize=80):
            for k, trades in ticker_out.items():
                by_combo[k].extend(trades)
    print(f'  完成 {time.time()-t0:.1f}s')

    # 統計：分 grade × stage
    summary_by_grade = {}
    summary_by_stage = {}
    all_summaries = {}

    for (side, exit_strat), trades in by_combo.items():
        if not trades: continue
        df = pd.DataFrame(trades)
        # 整體
        all_summaries[f'{side}_{exit_strat}_all'] = trade_stats(trades)
        # By grade
        for g in ['A', 'B', 'C', 'D']:
            sub = df[df['grade'] == g]
            if len(sub) > 0:
                summary_by_grade[f'{side}_{exit_strat}_grade_{g}'] = trade_stats(sub.to_dict('records'))
        # By stage
        for st in df['stage'].unique():
            sub = df[df['stage'] == st]
            if len(sub) > 0:
                summary_by_stage[f'{side}_{exit_strat}_stage_{st}'] = trade_stats(sub.to_dict('records'))

    # 印重要結果
    print('\n' + '=' * 120)
    print('🏆 整體 by exit strategy（雙底 long）')
    print('=' * 120)
    print(f'{"exit":>15} {"n":>6} {"win%":>7} {"mean%":>8} {"med%":>8} {"PF":>5} {"avgHold":>8} {"mean/d":>7}')
    print('-' * 80)
    for exit_strat in EXIT_STRATEGIES:
        key = f'bull_{exit_strat}_all'
        st = all_summaries.get(key)
        if not st: continue
        print(f'{exit_strat:>15} {st["n"]:>6} {st["win_pct"]:>5.1f}% '
              f'{st["mean_pct"]:>+7.2f}% {st["median_pct"]:>+7.2f}% '
              f'{st["pf"]:>5.2f} {st["avg_hold"]:>7.1f} {st["mean_per_day"]:>+6.3f}')

    print('\n' + '=' * 120)
    print('🏆 整體 by exit strategy（雙頂 short）')
    print('=' * 120)
    print(f'{"exit":>15} {"n":>6} {"win%":>7} {"mean%":>8} {"med%":>8} {"PF":>5} {"avgHold":>8}')
    print('-' * 80)
    for exit_strat in EXIT_STRATEGIES:
        key = f'bear_{exit_strat}_all'
        st = all_summaries.get(key)
        if not st: continue
        print(f'{exit_strat:>15} {st["n"]:>6} {st["win_pct"]:>5.1f}% '
              f'{st["mean_pct"]:>+7.2f}% {st["median_pct"]:>+7.2f}% '
              f'{st["pf"]:>5.2f} {st["avg_hold"]:>7.1f}')

    print('\n' + '=' * 120)
    print('🏆 雙底 by Quality Grade × exit（composite）')
    print('=' * 120)
    print(f'{"grade":>8} {"n":>6} {"win%":>7} {"mean%":>8} {"PF":>5} {"avgHold":>8}')
    print('-' * 60)
    for g in ['A', 'B', 'C', 'D']:
        key = f'bull_composite_grade_{g}'
        st = summary_by_grade.get(key)
        if not st: continue
        print(f'  {g:>6} {st["n"]:>6} {st["win_pct"]:>5.1f}% '
              f'{st["mean_pct"]:>+7.2f}% {st["pf"]:>5.2f} {st["avg_hold"]:>7.1f}')

    print('\n' + '=' * 120)
    print('🏆 雙頂 by Quality Grade × exit（composite）')
    print('=' * 120)
    print(f'{"grade":>8} {"n":>6} {"win%":>7} {"mean%":>8} {"PF":>5}')
    print('-' * 60)
    for g in ['A', 'B', 'C', 'D']:
        key = f'bear_composite_grade_{g}'
        st = summary_by_grade.get(key)
        if not st: continue
        print(f'  {g:>6} {st["n"]:>6} {st["win_pct"]:>5.1f}% '
              f'{st["mean_pct"]:>+7.2f}% {st["pf"]:>5.2f}')

    print('\n' + '=' * 120)
    print('🏆 雙底 by Entry Stage × composite exit')
    print('=' * 120)
    print(f'{"stage":>12} {"n":>6} {"win%":>7} {"mean%":>8} {"PF":>5} {"avgHold":>8}')
    print('-' * 60)
    for st_name in ['A_test', 'B_breakout', 'C_retest']:
        key = f'bull_composite_stage_{st_name}'
        st = summary_by_stage.get(key)
        if not st: continue
        print(f'  {st_name:>10} {st["n"]:>6} {st["win_pct"]:>5.1f}% '
              f'{st["mean_pct"]:>+7.2f}% {st["pf"]:>5.2f} {st["avg_hold"]:>7.1f}')

    # 寫 JSON
    out = f'analyze_double_oos_{market}{"_oos" if oos_only else ""}.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'market': market, 'oos_only': oos_only,
            'start_date': start,
            'universe_size': len(universe),
            'all': all_summaries,
            'by_grade': summary_by_grade,
            'by_stage': summary_by_stage,
        }, f, indent=2, ensure_ascii=False)
    print(f'\n✅ 寫入 {out}')
    return all_summaries, summary_by_grade, summary_by_stage


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'both'])
    p.add_argument('--oos', action='store_true')
    args = p.parse_args()
    markets = ['tw', 'us'] if args.market == 'both' else [args.market]
    for m in markets:
        run_analysis(m, oos_only=args.oos)


if __name__ == '__main__':
    main()
