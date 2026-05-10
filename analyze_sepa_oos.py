"""SEPA / VCP / RS Rating OOS 驗證（v9.19 Phase D）
==========================================================
目的
-----
驗證 Mark Minervini 的 SEPA + VCP + RS Rating 策略 OOS（2020-2026）
是否真的比目前 Strategy B 突破前高 + rsi_80 出場 更好。

實驗設計
---------
**進場（6 種對比）**:
1. SEPA_7of7        — SEPA Trend Template 7 條件全過
2. SEPA_RS70        — SEPA + RS Rating ≥ 70（Minervini standard）
3. SEPA_RS80        — SEPA + RS ≥ 80
4. SEPA_RS90        — SEPA + RS ≥ 90（飆股候選）
5. SEPA_VCP_RS70    — 完整 Minervini setup
6. baseline_B       — Strategy B 突破前高（既有 OOS 驗證冠軍）

**出場（6 種對比）**:
1. minervini        — -8% stop OR SMA50 break+vol OR SMA200 break
2. fixed_8pct_stop  — 純 -8% 停損 + max 90d
3. fixed_60d        — 60 天固定
4. fixed_90d        — 90 天固定
5. rsi_80           — RSI≥80 出場（Strategy B 的 baseline）
6. trail_15         — 從 peak 回跌 15%

執行
-----
  python analyze_sepa_oos.py                # TW
  python analyze_sepa_oos.py --market both
  python analyze_sepa_oos.py --oos          # 2024+ only
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

WORKERS = 12


# ────────────────────────────────────────────────────────────────
# Step 1：對單一 ticker 計算所有「歷史每日」需要的指標
# ────────────────────────────────────────────────────────────────

def compute_ticker_series(df):
    """為單一 ticker 計算歷史每日的 SEPA helpers + 報酬"""
    if df is None or len(df) < 280:
        return None
    n = len(df)
    c = df['Close'].values
    h = df['High'].values
    l = df['Low'].values

    # SMA series
    sma50_s = pd.Series(c).rolling(50).mean().values
    sma150_s = pd.Series(c).rolling(150).mean().values
    sma200_s = pd.Series(c).rolling(200).mean().values
    # SMA200 30 天前的值
    sma200_30d_ago = np.full(n, np.nan)
    for i in range(230, n):
        sma200_30d_ago[i] = sma200_s[i-30]

    # 52w high/low（rolling）
    high_52w = pd.Series(h).rolling(252).max().values
    low_52w = pd.Series(l).rolling(252).min().values

    # Returns（給 RS 用）
    ret_13w = np.full(n, np.nan)
    ret_26w = np.full(n, np.nan)
    ret_39w = np.full(n, np.nan)
    ret_52w = np.full(n, np.nan)
    for i in range(252, n):
        if c[i-65] > 0: ret_13w[i] = (c[i] / c[i-65] - 1) * 100
        if c[i-130] > 0: ret_26w[i] = (c[i] / c[i-130] - 1) * 100
        if c[i-195] > 0: ret_39w[i] = (c[i] / c[i-195] - 1) * 100
        if c[i-252] > 0: ret_52w[i] = (c[i] / c[i-252] - 1) * 100

    return {
        'sma50': sma50_s, 'sma150': sma150_s, 'sma200': sma200_s,
        'sma200_30d_ago': sma200_30d_ago,
        'high_52w': high_52w, 'low_52w': low_52w,
        'ret_13w': ret_13w, 'ret_26w': ret_26w,
        'ret_39w': ret_39w, 'ret_52w': ret_52w,
    }


def is_sepa_passed_at(c, sma50, sma150, sma200, sma200_30d_ago, h52w, l52w):
    """單日 SEPA 7 條件檢查（vectorized friendly）"""
    if any(np.isnan(x) or x is None for x in [c, sma50, sma150, sma200,
                                                sma200_30d_ago, h52w, l52w]):
        return False
    if sma200 == 0 or sma150 == 0 or sma50 == 0:
        return False
    return (c > sma150 and c > sma200 and
            sma150 > sma200 and
            sma200 > sma200_30d_ago and
            sma50 > sma150 and sma50 > sma200 and
            c > sma50 and
            c >= l52w * 1.30 and
            c >= h52w * 0.75)


# ────────────────────────────────────────────────────────────────
# Step 2：偵測 entry signals（multiple variants）
# ────────────────────────────────────────────────────────────────

def detect_sepa_entries(df, helpers, rs_series, variant='SEPA_RS70',
                         start_date='2020-01-01'):
    """偵測歷史 SEPA 進場訊號

    rs_series: pd.Series indexed by date, values are RS ratings (0-100) or None
    variant:
        SEPA_7of7
        SEPA_RS70 / SEPA_RS80 / SEPA_RS90
        SEPA_VCP_RS70
    """
    n = len(df)
    c = df['Close'].values
    sma50 = helpers['sma50']; sma150 = helpers['sma150']; sma200 = helpers['sma200']
    sma200_30 = helpers['sma200_30d_ago']
    h52w = helpers['high_52w']; l52w = helpers['low_52w']

    start_idx = 0
    if start_date:
        try:
            start_ts = pd.Timestamp(start_date)
            for i, ts in enumerate(df.index):
                if ts >= start_ts:
                    start_idx = i; break
        except Exception: pass

    rs_threshold = 0
    if 'RS70' in variant: rs_threshold = 70
    elif 'RS80' in variant: rs_threshold = 80
    elif 'RS90' in variant: rs_threshold = 90

    need_vcp = 'VCP' in variant

    signals = []
    last_signal_i = -30   # 同一檔股票兩個訊號至少差 30 天（避免 over-trade）
    for i in range(max(start_idx, 252), n - 1):
        # SEPA 7 條件
        if not is_sepa_passed_at(c[i], sma50[i], sma150[i], sma200[i],
                                  sma200_30[i], h52w[i], l52w[i]):
            continue

        # RS 過濾
        if rs_threshold > 0:
            try:
                rs = rs_series.iloc[i] if rs_series is not None else None
                if rs is None or np.isnan(rs) or rs < rs_threshold:
                    continue
            except Exception:
                continue

        # VCP 過濾（簡化：當前 close 在最近 60 日 max 的 95% 以上 + 最近振幅在縮）
        if need_vcp:
            if i < 60: continue
            recent_high = float(np.nanmax(df['High'].values[i-60:i+1]))
            if recent_high <= 0 or c[i] / recent_high < 0.92:
                continue
            # 簡化 VCP：最近 30d 振幅 < 之前 30d 振幅
            recent_range = (np.nanmax(df['High'].values[i-30:i+1])
                              - np.nanmin(df['Low'].values[i-30:i+1]))
            prior_range = (np.nanmax(df['High'].values[i-60:i-30])
                             - np.nanmin(df['Low'].values[i-60:i-30]))
            if prior_range <= 0 or recent_range >= prior_range * 0.9:
                continue

        # 30 天去重
        if i - last_signal_i < 30:
            continue
        signals.append(i)
        last_signal_i = i

    return signals


# ────────────────────────────────────────────────────────────────
# Step 3：出場規則
# ────────────────────────────────────────────────────────────────

def walk_exit(df, helpers, entry_i, entry_open, exit_strategy):
    """從 entry_i+1 開始 walk，套用 exit_strategy。回傳 (exit_i, price, reason)"""
    n = len(df)
    o = df['Open'].values
    h_arr = df['High'].values
    l_arr = df['Low'].values
    c = df['Close'].values
    rsi = df['rsi'].values if 'rsi' in df.columns else None
    sma50 = helpers['sma50']
    sma200 = helpers['sma200']
    v = df['Volume'].values

    running_peak = entry_open
    max_hold = 180

    for k in range(entry_i + 1, min(entry_i + max_hold, n)):
        h_k = h_arr[k]; l_k = l_arr[k]; c_k = c[k]
        if np.isnan(h_k) or np.isnan(l_k) or np.isnan(c_k): continue
        if h_k > running_peak: running_peak = h_k

        if exit_strategy == 'minervini':
            # -8% 停損
            if l_k <= entry_open * 0.92:
                return (k, entry_open * 0.92, 'minervini_stop_8pct')
            # SMA200 跌破
            if not np.isnan(sma200[k]) and c_k < sma200[k]:
                return (k, c_k, 'minervini_sma200_break')
            # SMA50 連 2 天破 + 量增
            if k >= 1 and not np.isnan(sma50[k]) and not np.isnan(sma50[k-1]):
                if c_k < sma50[k] and c[k-1] < sma50[k-1]:
                    if k >= 20:
                        v_avg = np.nanmean(v[k-20:k])
                        if v_avg > 0 and v[k] / v_avg > 1.3:
                            return (k, c_k, 'minervini_sma50_break_vol')

        elif exit_strategy == 'fixed_8pct_stop':
            if l_k <= entry_open * 0.92:
                return (k, entry_open * 0.92, 'stop_8pct')
            if k - entry_i >= 90:
                return (k, c_k, 'max_90d')

        elif exit_strategy == 'fixed_60d':
            if k - entry_i >= 60:
                return (k, float(o[k]) if not np.isnan(o[k]) else c_k, 'fixed_60d')

        elif exit_strategy == 'fixed_90d':
            if k - entry_i >= 90:
                return (k, float(o[k]) if not np.isnan(o[k]) else c_k, 'fixed_90d')

        elif exit_strategy == 'rsi_80':
            if rsi is not None and not np.isnan(rsi[k]) and rsi[k] >= 80:
                if k+1 < n and not np.isnan(o[k+1]):
                    return (k+1, float(o[k+1]), 'rsi_80')
                return (k, c_k, 'rsi_80_close')
            if k - entry_i >= 90:
                return (k, c_k, 'max_90d')

        elif exit_strategy == 'trail_15':
            if running_peak > entry_open:
                stop = running_peak * 0.85
                if l_k <= stop:
                    return (k, stop, 'trail_15')
            if k - entry_i >= 90:
                return (k, c_k, 'max_90d')

    # max hold 到了
    end = min(entry_i + max_hold, n - 1)
    return (end, float(o[end]) if not np.isnan(o[end]) else c[end], 'max_hold_safety')


# ────────────────────────────────────────────────────────────────
# Step 4：多 worker 處理
# ────────────────────────────────────────────────────────────────

# 全域 RS table (entry × exit 各個 worker 共用 — pickle 需要 module-level var)
_RS_TABLE = None  # dict[ticker] -> pd.Series(date_idx -> RS rating)
_START_DATE = '2020-01-01'

ENTRY_VARIANTS = ['SEPA_7of7', 'SEPA_RS70', 'SEPA_RS80', 'SEPA_RS90', 'SEPA_VCP_RS70']
EXIT_STRATEGIES = ['minervini', 'fixed_8pct_stop', 'fixed_60d', 'fixed_90d',
                    'rsi_80', 'trail_15']


def process_ticker(args):
    """單 ticker：對所有 entry × exit 組合產出 trades"""
    # 🐛 fix v9.19.1：start_date 必須從 args 傳，不能用 global
    # （ProcessPoolExecutor 工作 process 不繼承 main 的全域變數修改）
    if len(args) == 3:
        ticker, rs_series_dict, start_date = args
    else:
        ticker, rs_series_dict = args
        start_date = _START_DATE
    out = {(e, x): [] for e in ENTRY_VARIANTS for x in EXIT_STRATEGIES}
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280:
            return out
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)

        helpers = compute_ticker_series(df)
        if helpers is None:
            return out

        rs_arr = rs_series_dict.get(ticker)  # numpy array aligned with df index

        n = len(df)
        o = df['Open'].values
        idx = df.index

        for entry_variant in ENTRY_VARIANTS:
            # rs_series for this ticker
            rs_series = pd.Series(rs_arr, index=df.index) if rs_arr is not None else None
            signals = detect_sepa_entries(df, helpers, rs_series,
                                           variant=entry_variant,
                                           start_date=start_date)

            for sig_i in signals:
                entry_i = sig_i + 1
                if entry_i >= n - 1: continue
                entry_open = float(o[entry_i])
                if entry_open <= 0 or np.isnan(entry_open): continue

                for exit_strat in EXIT_STRATEGIES:
                    exit_i, exit_price, reason = walk_exit(
                        df, helpers, entry_i, entry_open, exit_strat)
                    if exit_price is None or exit_price <= 0 or np.isnan(exit_price):
                        continue
                    gross_ret = (exit_price - entry_open) / entry_open
                    net_ret = gross_ret - COST_ROUND_TRIP
                    out[(entry_variant, exit_strat)].append({
                        'ticker': ticker,
                        'entry_date': idx[entry_i].strftime('%Y-%m-%d'),
                        'exit_date': idx[exit_i].strftime('%Y-%m-%d') if exit_i < n else idx[-1].strftime('%Y-%m-%d'),
                        'hold_days': exit_i - entry_i,
                        'entry_price': round(entry_open, 4),
                        'exit_price': round(exit_price, 4),
                        'gross_ret': gross_ret,
                        'net_ret': net_ret,
                        'reason': reason,
                    })
        return out
    except Exception as e:
        return out


# ────────────────────────────────────────────────────────────────
# Step 5：RS Rating 預計算（universe-wide rolling）
# ────────────────────────────────────────────────────────────────

def precompute_rs_ratings(universe, start_date='2020-01-01'):
    """為 universe 預計算每一交易日的 RS Rating（percentile）

    回傳 dict[ticker] -> np.array (aligned with df.index, 值為 RS rating 0-100)
    """
    print(f'📊 Pre-computing RS ratings for {len(universe)} tickers...')
    t0 = time.time()
    df_per_ticker = {}
    all_dates = set()
    for tk in universe:
        try:
            df = dl.load_from_cache(tk)
            if df is None or len(df) < 280: continue
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df = df.copy()
                df.index = df.index.tz_localize(None)
            df = df[df.index >= pd.Timestamp(start_date)]
            if len(df) < 30: continue
            df_per_ticker[tk] = df
            all_dates.update(df.index)
        except Exception:
            continue

    print(f'  載入 {len(df_per_ticker)} tickers, {len(all_dates)} 個唯一日期 ({time.time()-t0:.1f}s)')

    # Build common date index
    sorted_dates = sorted(all_dates)
    date_idx = pd.DatetimeIndex(sorted_dates)

    # Returns matrix
    returns_13w = pd.DataFrame(index=date_idx, columns=df_per_ticker.keys(), dtype=float)
    returns_26w = pd.DataFrame(index=date_idx, columns=df_per_ticker.keys(), dtype=float)
    returns_39w = pd.DataFrame(index=date_idx, columns=df_per_ticker.keys(), dtype=float)
    returns_52w = pd.DataFrame(index=date_idx, columns=df_per_ticker.keys(), dtype=float)

    for tk, df in df_per_ticker.items():
        c = df['Close']
        returns_13w[tk] = (c / c.shift(65) - 1) * 100
        returns_26w[tk] = (c / c.shift(130) - 1) * 100
        returns_39w[tk] = (c / c.shift(195) - 1) * 100
        returns_52w[tk] = (c / c.shift(252) - 1) * 100

    # Composite (Minervini 加權: 2*13 + 26 + 39 + 52)
    composite = 2 * returns_13w + returns_26w + returns_39w + returns_52w
    # Rank within each row → percentile
    rs_df = composite.rank(axis=1, pct=True) * 100
    print(f'  RS percentile rank: {time.time()-t0:.1f}s')

    # Convert back to per-ticker arrays aligned with each ticker's df.index
    rs_dict = {}
    for tk, df in df_per_ticker.items():
        if tk in rs_df.columns:
            aligned = rs_df[tk].reindex(df.index).values
            rs_dict[tk] = aligned
    print(f'  完成 RS 計算 ({time.time()-t0:.1f}s)')
    return rs_dict


# ────────────────────────────────────────────────────────────────
# Step 6：跑 + 統計
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
        'n': n, 'win_pct': round(win_pct, 2),
        'mean_pct': round(mean_pct, 2), 'median_pct': round(median_pct, 2),
        'pf': round(pf, 2), 'avg_hold': round(avg_hold, 1),
        'mean_per_day': round(mean_per_day, 3),
        'best_pct': round(df['net_ret'].max() * 100, 2),
        'worst_pct': round(df['net_ret'].min() * 100, 2),
    }


def run_analysis(market='tw', oos_only=False):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    global _START_DATE
    _START_DATE = '2024-01-01' if oos_only else '2020-01-01'
    period_label = 'OOS only (2024-)' if oos_only else 'Full (2020-)'

    print(f'\n{flag} SEPA / VCP / RS Rating OOS 驗證  {period_label}')
    print(f'  Universe: {len(universe)} 檔')
    print(f'  成本 round-trip: {COST_ROUND_TRIP*100:.2f}%')

    # Pre-compute RS ratings
    rs_dict = precompute_rs_ratings(universe, start_date='2020-01-01')

    # Run all combinations
    print(f'\n📊 跑所有 {len(ENTRY_VARIANTS)} entry × {len(EXIT_STRATEGIES)} exit = '
          f'{len(ENTRY_VARIANTS) * len(EXIT_STRATEGIES)} 組合（{WORKERS} workers）...')
    t0 = time.time()
    # 🐛 fix v9.19.1：把 start_date 傳給每個 worker，避免 global 變數不傳遞
    args = [(t, rs_dict, _START_DATE) for t in universe]
    by_combo = {(e, x): [] for e in ENTRY_VARIANTS for x in EXIT_STRATEGIES}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker_out in ex.map(process_ticker, args, chunksize=80):
            for k, trades in ticker_out.items():
                by_combo[k].extend(trades)
    print(f'  完成 {time.time()-t0:.1f}s')

    # Print results
    print('\n' + '=' * 130)
    for entry_variant in ENTRY_VARIANTS:
        # 找這個 entry 變體下的所有 exit 變化
        sub_keys = [(e, x) for (e, x) in by_combo if e == entry_variant]
        if not sub_keys: continue
        first_n = len(by_combo[(entry_variant, EXIT_STRATEGIES[0])])
        if first_n == 0:
            print(f'\n📊 {entry_variant}: 0 訊號 — 跳過')
            continue
        print(f'\n📊 入場：{entry_variant}（{first_n} 訊號）')
        print(f'{"exit":>20} {"n":>6} {"win%":>6} {"mean%":>8} {"med%":>7} '
              f'{"PF":>5} {"hold":>6} {"mean/d":>7} {"best%":>9} {"worst%":>9}')
        print('-' * 100)
        for exit_strat in EXIT_STRATEGIES:
            trades = by_combo[(entry_variant, exit_strat)]
            st = trade_stats(trades)
            if st is None:
                print(f'{exit_strat:>20}  (no trades)')
                continue
            mark = ' ⭐' if (st['win_pct'] >= 50 and st['mean_per_day'] >= 0.05) else ''
            print(f'{exit_strat:>20} {st["n"]:>6} {st["win_pct"]:>5.1f}% '
                  f'{st["mean_pct"]:>+7.2f}% {st["median_pct"]:>+6.2f}% '
                  f'{st["pf"]:>5.2f} {st["avg_hold"]:>6.1f} {st["mean_per_day"]:>+6.3f} '
                  f'{st["best_pct"]:>+8.2f}% {st["worst_pct"]:>+8.2f}%{mark}')

    # Save JSON
    summary = {}
    for (e, x), trades in by_combo.items():
        st = trade_stats(trades)
        if st: summary[f'{e}_{x}'] = st
    out = f'analyze_sepa_oos_{market}{"_oos" if oos_only else ""}.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'market': market, 'oos_only': oos_only,
            'start_date': _START_DATE,
            'universe_size': len(universe),
            'results': summary,
        }, f, indent=2, ensure_ascii=False)
    print(f'\n✅ 寫入 {out}')

    # 排名
    print('\n' + '=' * 130)
    print('🏆 Top 8 組合（按 mean% 排序，需 win%≥50）')
    print('=' * 130)
    valid = [(k, v) for k, v in summary.items() if v['win_pct'] >= 50]
    valid.sort(key=lambda x: -x[1]['mean_pct'])
    for i, (k, st) in enumerate(valid[:8], 1):
        print(f'  #{i} {k:<35} n={st["n"]:>5}  win {st["win_pct"]:.1f}%  '
              f'mean {st["mean_pct"]:+.2f}%  hold {st["avg_hold"]:.1f}d  PF {st["pf"]:.2f}')

    print('\n🏆 Top 8 組合（按 mean/day 效率排序）')
    by_eff = sorted(summary.items(), key=lambda x: -x[1]['mean_per_day'])
    for i, (k, st) in enumerate(by_eff[:8], 1):
        print(f'  #{i} {k:<35} mean/d {st["mean_per_day"]:+.3f}  '
              f'mean {st["mean_pct"]:+.2f}%  win {st["win_pct"]:.1f}%  hold {st["avg_hold"]:.1f}d')

    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'both'])
    p.add_argument('--oos', action='store_true')
    args = p.parse_args()
    markets = ['tw', 'us'] if args.market == 'both' else [args.market]
    for m in markets:
        run_analysis(market=m, oos_only=args.oos)


if __name__ == '__main__':
    main()
