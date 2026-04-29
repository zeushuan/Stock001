"""美股 — 非 trend-following 風格對比
=======================================
v8 在美股 RR 0.033（虧錢）。測試 3 個經典另類風格：

1. Mean Reversion — RSI(5)<30 進場 / RSI(5)>70 出場
   論點：美股短期 mean-revert 強

2. Breakout (Donchian-20) — 20 日新高進場 / 跌破 20 日低點出場
   論點：美股 momentum 強，突破跟著走

3. Momentum 12-1 — 過去 12 個月（不含最近 1 月）報酬最高 decile
   論點：學術經典 Jegadeesh-Titman，美股已驗證

每個風格全市場 + 高流動 tier 雙跑，三個 windows。
持有期：dynamic exit（不固定 30 天）。
"""
import sys, time, json
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16
MIN_ADV = 104_000_000

WINDOWS = [
    ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
    ('TRAIN (2020.1-2024.5)', '2020-01-02', '2024-05-31'),
    ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
]


def slice_period(df, start, end):
    s = pd.Timestamp(start).tz_localize(None)
    e = pd.Timestamp(end).tz_localize(None)
    idx = df.index
    if hasattr(idx, 'tz') and idx.tz is not None:
        idx = idx.tz_localize(None)
    mask = (idx >= s) & (idx <= e)
    return df.iloc[mask]


def mean_reversion(df, start, end):
    """RSI(5) < 30 進場 / RSI(5) > 70 出場 / ATR×2.5 停損"""
    import ta
    sub = slice_period(df, start, end).copy()
    if len(sub) < 30: return []
    rsi5 = ta.momentum.rsi(sub['Close'], window=5)
    atr = sub['atr'].values if 'atr' in sub.columns else \
          ta.volatility.average_true_range(sub['High'], sub['Low'], sub['Close'], 14).values
    close = sub['Close'].values
    rsi_arr = rsi5.values
    n = len(sub)
    trades = []
    in_mkt = False
    ep = stop_p = 0.0
    for i in range(5, n):
        if any(np.isnan(x) for x in [rsi_arr[i], atr[i]]): continue
        if not in_mkt:
            if rsi_arr[i] < 30:
                in_mkt = True
                ep = close[i]
                stop_p = close[i] - atr[i] * 2.5
        else:
            hit_stop = close[i] < stop_p
            do_exit = rsi_arr[i] > 70 or hit_stop
            if do_exit:
                trades.append((close[i] - ep) / ep * 100)
                in_mkt = False
    return trades


def breakout_donchian(df, start, end):
    """20 日新高進場 / 跌破 10 日低點出場 / ATR×2.5 停損"""
    sub = slice_period(df, start, end).copy()
    if len(sub) < 30: return []
    high = sub['High'].values
    low = sub['Low'].values
    close = sub['Close'].values
    atr = sub['atr'].values if 'atr' in sub.columns else None
    n = len(sub)
    trades = []
    in_mkt = False
    ep = stop_p = 0.0
    for i in range(20, n):
        if atr is None or np.isnan(atr[i]): continue
        h20 = high[i-20:i].max()
        l10 = low[i-10:i].min()
        if not in_mkt:
            if close[i] > h20:
                in_mkt = True
                ep = close[i]
                stop_p = close[i] - atr[i] * 2.5
        else:
            hit_stop = close[i] < stop_p
            do_exit = close[i] < l10 or hit_stop
            if do_exit:
                trades.append((close[i] - ep) / ep * 100)
                in_mkt = False
    return trades


def momentum_12_1(df, start, end):
    """每月最後一日：12-1 月報酬，報酬>15% 進場、<0% 出場（個股 long-only）
    跨樣本長期 momentum 個股版（簡化 cross-sectional 為 individual）"""
    sub = slice_period(df, start, end).copy()
    if len(sub) < 252: return []
    close = sub['Close']
    n = len(sub)
    trades = []
    in_mkt = False
    ep = 0.0
    last_check_idx = -22
    for i in range(252, n):
        if i - last_check_idx < 22: continue  # 月度檢查
        last_check_idx = i
        # 12-1 月報酬：i-252 → i-22
        ret_12_1 = (close.iloc[i-22] / close.iloc[i-252] - 1) * 100
        if not in_mkt:
            if ret_12_1 > 15:
                in_mkt = True
                ep = close.iloc[i]
        else:
            if ret_12_1 < 0:
                trades.append((close.iloc[i] - ep) / ep * 100)
                in_mkt = False
    # 強制收尾
    if in_mkt:
        trades.append((close.iloc[-1] - ep) / ep * 100)
    return trades


STYLES = [
    ('1 Mean Reversion (RSI5)', mean_reversion),
    ('2 Breakout Donchian-20',  breakout_donchian),
    ('3 Momentum 12-1',         momentum_12_1),
]


def run_one(args):
    ticker, style_name, style_fn, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, ticker, None)
        trades = style_fn(df, start, end)
        if not trades: return (label, ticker, None)
        # pnl_pct = sum of trades (compound assumption simplified to additive %)
        return (label, ticker, sum(trades))
    except Exception:
        return (label, ticker, None)


def metrics(returns):
    if not returns: return None
    arr = np.array([x for x in returns if x is not None])
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0: return None
    return {
        'n': len(arr), 'mean': arr.mean(), 'median': np.median(arr),
        'win': (arr > 0).mean() * 100, 'worst': arr.min(),
        'rr': arr.mean()/abs(arr.min()) if arr.min() < 0 else 0,
    }


def main():
    DATA = Path('data_cache')
    full_path = Path('us_full_tickers.json')
    meta = json.loads(full_path.read_text(encoding='utf-8'))
    full_tickers = set(meta['tickers'])
    universe = sorted([t for t in full_tickers
                       if (DATA / f'{t}.parquet').exists()])
    print(f"美股 universe: {len(universe)} 檔\n")

    # 計算 ADV 區分高流動
    high_liquid = set()
    for t in universe:
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
            if adv >= MIN_ADV:
                high_liquid.add(t)
        except: pass
    print(f"高流動 tier (≥${MIN_ADV/1e6:.0f}M): {len(high_liquid)} 檔\n")

    # 任務：3 風格 × 3 windows × universe
    all_tasks = []
    for win_name, start, end in WINDOWS:
        for sname, sfn in STYLES:
            for t in universe:
                all_tasks.append((t, sname, sfn, start, end,
                                  (sname, win_name, t)))
    print(f"總任務 = {len(all_tasks)}\n")

    t0 = time.time()
    bucket = {}        # (style, win, all/HL): [returns]
    n_done = 0
    milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                sname, win_name, _ticker = label
                bucket.setdefault((sname, win_name, 'all'), []).append(ret)
                if _ticker in high_liquid:
                    bucket.setdefault((sname, win_name, 'HL'), []).append(ret)
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                print(f"  {pct:.0f}%", flush=True)

    print(f"\n完成 {time.time()-t0:.1f}s\n")

    print("=" * 110)
    print("📊 美股另類風格對比（vs v8 baseline P5+POS RR 0.033 / 高流動 0.348）")
    print("=" * 110)
    print(f"{'風格':<28} {'池':<6} {'Period':<26} {'n':>5} {'勝率%':>7} "
          f"{'均報%':>9} {'中位%':>8} {'最差%':>8} {'RR':>7}")
    print("-" * 110)
    for sname, _ in STYLES:
        for pool in ['all', 'HL']:
            for win_name, _, _ in WINDOWS:
                m = metrics(bucket.get((sname, win_name, pool), []))
                if m:
                    pool_label = '全市場' if pool == 'all' else '高流動'
                    print(f"{sname:<28} {pool_label:<6} {win_name:<26} "
                          f"{m['n']:>5} {m['win']:>+7.1f} {m['mean']:>+9.1f} "
                          f"{m['median']:>+8.1f} {m['worst']:>+8.1f} "
                          f"{m['rr']:>7.3f}")
        print()

    # TEST RR 對比 v8
    print("=" * 110)
    print("🌏 TEST 期 RR 對比（vs v8 baseline）")
    print("=" * 110)
    print(f"{'風格 / 池':<40} {'TEST_RR':>10} {'TEST_勝率':>10} {'TEST_中位':>10}  vs v8")
    print("-" * 110)
    print(f"{'v8 P5+POS / 全市場':<40} {'0.033':>10} {'43.9%':>10} {'-4.4%':>10}  baseline")
    print(f"{'v8 P5+POS / 高流動':<40} {'0.348':>10} {'51.3%':>10} {'+0.5%':>10}  baseline")
    for sname, _ in STYLES:
        for pool in ['all', 'HL']:
            m = metrics(bucket.get((sname, 'TEST  (2024.6-2026.4)', pool), []))
            if m:
                pool_label = '全市場' if pool == 'all' else '高流動'
                vs_str = ''
                if pool == 'all' and m['rr'] > 0.033 + 0.05: vs_str = '⭐ 勝 v8'
                elif pool == 'HL' and m['rr'] > 0.348 + 0.05: vs_str = '⭐ 勝 v8'
                elif pool == 'all' and m['rr'] > 0.033: vs_str = '✓ 微勝'
                elif pool == 'HL' and m['rr'] > 0.348: vs_str = '✓ 微勝'
                else: vs_str = '✗ 輸'
                print(f"{sname + ' / ' + pool_label:<40} {m['rr']:>10.3f} "
                      f"{m['win']:>9.1f}% {m['median']:>+9.1f}%  {vs_str}")

    out = {
        'metrics': {f'{s}|{w}|{p}': metrics(bucket.get((s, w, p), []))
                    for s, _ in STYLES for w, _, _ in WINDOWS for p in ['all', 'HL']},
    }
    with open('us_alt_styles_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("\n💾 寫入 us_alt_styles_results.json")


if __name__ == '__main__':
    main()
