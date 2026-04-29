"""D11: v8 套到加密貨幣 — 24/7 市場是否一樣有效？
==================================================
測試 9 個主流加密貨幣 + 4 個 layer-1：
  BTC / ETH / SOL / BNB / XRP / ADA / DOGE / AVAX / LINK
  + DOT / MATIC / LTC / NEAR

策略對比：
  baseline P0_T1T3
  P5+POS（台股最佳對應）
  P10+POS+ADX18（美股最佳對應）

vs BTC buy-hold 對標
"""
import sys, time, json
from pathlib import Path
import numpy as np
import pandas as pd
import yfinance as yf
import ta
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs

WORKERS = 12

CRYPTO_TICKERS = [
    'BTC-USD', 'ETH-USD', 'SOL-USD', 'BNB-USD', 'XRP-USD',
    'ADA-USD', 'DOGE-USD', 'AVAX-USD', 'LINK-USD',
    'DOT-USD', 'MATIC-USD', 'LTC-USD', 'NEAR-USD', 'ATOM-USD',
    'TRX-USD', 'UNI-USD', 'BCH-USD', 'XLM-USD', 'FIL-USD', 'ETC-USD',
]
CACHE = Path('data_cache')

VARIANTS = [
    ('A baseline P0',          'P0_T1T3'),
    ('B P5+POS (台股最佳對應)', 'P5_T1T3+POS'),
    ('C P10+POS+ADX18 (美股最佳對應)', 'P10_T1T3+POS+ADX18'),
    ('D P0+POS+VIX25',         'P0_T1T3+POS+VIX25'),  # 用 VIX 過濾 crypto
]
WINDOWS = [
    ('FULL  (2020-2026)',  '2020-01-02', '2026-04-25'),
    ('TRAIN (2020-2024)',  '2020-01-02', '2024-05-31'),
    ('TEST  (2024-2026)',  '2024-06-01', '2026-04-25'),
]


def calc_ind(df):
    if df is None or len(df) < 280: return None
    df = df.copy()
    df['e10']  = ta.trend.ema_indicator(df['Close'], window=10)
    df['e20']  = ta.trend.ema_indicator(df['Close'], window=20)
    df['e60']  = ta.trend.ema_indicator(df['Close'], window=60)
    df['e120'] = ta.trend.ema_indicator(df['Close'], window=120)
    df['rsi']  = ta.momentum.rsi(df['Close'], window=14)
    df['adx']  = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    macd = ta.trend.MACD(df['Close'])
    df['mh']   = macd.macd_diff()
    df['atr']  = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], 14)
    bb = ta.volatility.BollingerBands(df['Close'], window=20)
    df['pctb'] = bb.bollinger_pband()
    return df


def fetch_crypto():
    print("📥 抓加密貨幣資料 (yfinance)...\n")
    todo = [t for t in CRYPTO_TICKERS if not (CACHE / f'{t}.parquet').exists()]
    print(f"  已快取: {len(CRYPTO_TICKERS) - len(todo)}")
    print(f"  待抓: {len(todo)}\n")
    for t in todo:
        try:
            df = yf.Ticker(t).history(period='6y', interval='1d', auto_adjust=False)
            if df is None or df.empty or len(df) < 280:
                print(f"  ❌ {t}: 資料不足")
                continue
            df = df[['Open','High','Low','Close','Volume']]
            df = calc_ind(df)
            if df is None: continue
            df.to_parquet(CACHE / f'{t}.parquet')
            print(f"  ✅ {t}: {len(df)} 日")
        except Exception as e:
            print(f"  ❌ {t}: {str(e)[:60]}")


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, ticker, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, ticker, None)
        return (label, ticker, r['pnl_pct'])
    except Exception:
        return (label, ticker, None)


def buy_hold(ticker, start, end):
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        s = pd.Timestamp(start).tz_localize(None)
        e = pd.Timestamp(end).tz_localize(None)
        idx = df.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx = idx.tz_localize(None)
        sub = df[(idx >= s) & (idx <= e)]
        if len(sub) < 2: return None
        return (sub['Close'].iloc[-1] - sub['Close'].iloc[0]) / sub['Close'].iloc[0] * 100
    except: return None


def metrics(arr):
    if len(arr) == 0: return None
    a = np.array(arr)
    a = a[~np.isnan(a)]
    if len(a) == 0: return None
    return {
        'n': len(a), 'mean': float(a.mean()), 'median': float(np.median(a)),
        'win': float((a > 0).mean() * 100), 'worst': float(a.min()),
        'best': float(a.max()),
        'rr': float(a.mean() / abs(a.min())) if a.min() < 0 else 0,
    }


def main():
    fetch_crypto()
    universe = sorted([t for t in CRYPTO_TICKERS
                       if (CACHE / f'{t}.parquet').exists()])
    print(f"\n可用 crypto: {len(universe)} 檔: {universe}\n")

    if not universe:
        print("沒有可用 crypto 資料")
        return

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            for t in universe:
                all_tasks.append((t, mode, start, end, (var_name, win_name)))

    print(f"任務 {len(all_tasks)}\n")
    bucket = {}
    pt = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one, all_tasks, chunksize=10):
            if ret is not None:
                bucket.setdefault(label, []).append(ret)
                pt.setdefault(label, {})[ticker] = ret

    # 對比表
    print("=" * 110)
    print(f"📊 加密貨幣 v8 對比（{len(universe)} 個）")
    print("=" * 110)

    for var, _ in VARIANTS:
        print(f"\n【{var}】")
        print(f"{'Period':<22} {'n':>3} {'勝率%':>7} {'均報%':>10} "
              f"{'中位%':>9} {'最差%':>9} {'最佳%':>10} {'RR':>7}")
        print("-" * 110)
        for win_name, _, _ in WINDOWS:
            m = metrics(bucket.get((var, win_name), []))
            if m:
                print(f"{win_name:<22} {m['n']:>3} {m['win']:>+7.1f} "
                      f"{m['mean']:>+10.1f} {m['median']:>+9.1f} "
                      f"{m['worst']:>+9.1f} {m['best']:>+10.1f} {m['rr']:>7.3f}")

    # vs BTC buy-hold
    print("\n" + "=" * 110)
    print("📊 vs BTC buy-hold 對標")
    print("=" * 110)
    print(f"\n{'Period':<22} {'BTC%':>10} {'最佳變體':<35} {'變體均報%':>11}  vs BTC")
    print("-" * 110)
    for win_name, start, end in WINDOWS:
        btc = buy_hold('BTC-USD', start, end)
        if btc is None: continue
        # 找 RR 最高的變體
        best_var = None
        best_rr = -999
        for var, _ in VARIANTS:
            m = metrics(bucket.get((var, win_name), []))
            if m and m['rr'] > best_rr:
                best_rr = m['rr']
                best_var = (var, m)
        if best_var:
            var, m = best_var
            delta = m['mean'] - btc
            tag = '⭐ 勝' if delta > 0 else '✗ 輸'
            print(f"{win_name:<22} {btc:>+10.1f} {var:<35} {m['mean']:>+11.1f}  "
                  f"{delta:+.1f}pp {tag}")

    # 個股 PnL detail（FULL 期）
    print("\n" + "=" * 110)
    print("📊 各 crypto FULL 期報酬（v8 P5+POS）")
    print("=" * 110)
    full_pt = pt.get(('B P5+POS (台股最佳對應)', 'FULL  (2020-2026)'), {})
    if full_pt:
        sorted_t = sorted(full_pt.items(), key=lambda x: -x[1])
        print(f"{'Ticker':<12} {'PnL%':>10}  {'BTC buy-hold% 同期':>10}")
        print("-" * 60)
        btc_full = buy_hold('BTC-USD', '2020-01-02', '2026-04-25') or 0
        for t, r in sorted_t:
            tag = '⭐勝BTC' if r > btc_full else '✗輸BTC'
            print(f"{t:<12} {r:>+10.1f}  ({btc_full:+.1f}%) {tag}")

    out = {
        'universe': universe,
        'variants': [v[0] for v in VARIANTS],
        'metrics': {f'{var}|{win}': metrics(bucket.get((var, win), []))
                    for var, _ in VARIANTS for win, _, _ in WINDOWS},
        'per_ticker_full': {t: r for t, r in
                             pt.get(('B P5+POS (台股最佳對應)',
                                     'FULL  (2020-2026)'), {}).items()},
    }
    with open('crypto_v8_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 crypto_v8_results.json")


if __name__ == '__main__':
    main()
