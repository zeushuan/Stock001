"""T1 imminent 命中率回算器（v9.12）
============================================
讀 t1_imminent_history.json，對「日子已到但還沒回算」的候選：
  - 是否真的 cross 上 EMA20？（precision check）
  - 5/15/30 天後實際漲跌？（alpha check）

執行：
  python update_t1_outcomes.py        # 全部回算
  python update_t1_outcomes.py --tw   # 只回算 TW
  python update_t1_outcomes.py --us   # 只回算 US
"""
import sys, json, time, argparse
from pathlib import Path
from collections import defaultdict
import pandas as pd
import numpy as np
import yfinance as yf
import ta

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

HORIZONS = [5, 15, 30]


def _yf_symbol(ticker, market):
    if market == 'tw':
        return f"{ticker}.TW" if not ticker.endswith('.TW') else ticker
    return ticker


def _get_status_at(df, target_date, ema20_at_entry):
    """從 df 找 target_date 當天或之後第一個交易日的狀態。
    回傳 (close, crossed_above_ema20, actual_date) 或 (None, None, None)"""
    if df is None or df.empty:
        return None, None, None
    # 計算 EMA20
    if 'e20' not in df.columns:
        df = df.copy()
        df['e20'] = ta.trend.ema_indicator(df['Close'], window=20)
    after = df[df.index >= target_date]
    if len(after) == 0:
        return None, None, None
    row = after.iloc[0]
    close = float(row['Close'])
    ema20 = float(row['e20']) if not pd.isna(row['e20']) else None
    crossed = bool(close > ema20) if ema20 is not None else None
    return close, crossed, after.index[0].strftime('%Y-%m-%d')


def update(market_filter=None, hist_path='t1_imminent_history.json'):
    """回算 outcomes"""
    if not Path(hist_path).exists():
        print(f'❌ {hist_path} 不存在')
        return False

    hist = json.load(open(hist_path, encoding='utf-8'))
    candidates = hist.get('candidates', [])
    if not candidates:
        print('history 空')
        return False

    today = pd.Timestamp.now().normalize()
    print(f'今天: {today.strftime("%Y-%m-%d")}, 歷史筆數: {len(candidates)}')

    # 找出待回算
    pending = []
    for i, c in enumerate(candidates):
        if market_filter and c.get('market') != market_filter:
            continue
        try:
            scan_date = pd.Timestamp(c['scan_date'])
        except Exception:
            continue
        for n in HORIZONS:
            target = scan_date + pd.Timedelta(days=n)
            if today < target:
                continue
            if c.get('outcomes', {}).get(f'{n}d', {}).get('ret_pct') is not None:
                continue
            pending.append((i, n, target))

    if not pending:
        print('✅ 無需更新')
        return False

    print(f'📋 待回算: {len(pending)} 筆')

    # 收集 ticker + market
    tickers = {}
    for i, n, target in pending:
        c = candidates[i]
        key = (c['ticker'], c.get('market', 'tw'))
        tickers.setdefault(key, []).append((i, n, target))

    print(f'📥 下載 {len(tickers)} 個 ticker（period=3mo）...')
    price_cache = {}
    failed = []
    for idx, (ticker, market) in enumerate(tickers, 1):
        sym = _yf_symbol(ticker, market)
        try:
            df = yf.download(sym, period='3mo', progress=False, auto_adjust=False)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    try:
                        df = df.xs(sym, level=1, axis=1)
                    except Exception:
                        df.columns = df.columns.get_level_values(0)
                price_cache[(ticker, market)] = df
            else:
                failed.append(sym)
        except Exception as e:
            failed.append(f'{sym} ({type(e).__name__})')
        if idx % 30 == 0:
            print(f'  進度 {idx}/{len(tickers)}')

    if failed:
        print(f'⚠️ {len(failed)} 個下載失敗，將 skip')

    # 回算
    updated = 0
    for i, n, target in pending:
        c = candidates[i]
        df = price_cache.get((c['ticker'], c.get('market', 'tw')))
        if df is None:
            continue
        close, crossed, actual_date = _get_status_at(df, target, c.get('ema20'))
        if close is None:
            continue
        entry = c.get('entry_price') or 0
        if entry <= 0:
            continue
        ret_pct = (close - entry) / entry * 100
        c.setdefault('outcomes', {})[f'{n}d'] = {
            'close': round(close, 2),
            'crossed': crossed,  # 是否上穿 EMA20
            'ret_pct': round(ret_pct, 2),
            'actual_date': actual_date,
            'checked_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        }
        updated += 1

    # 計 stats
    stats = _compute_stats(candidates)
    hist['stats'] = stats
    hist['last_outcomes_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    with open(hist_path, 'w', encoding='utf-8') as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)

    print(f'\n✅ 更新 {updated}/{len(pending)} 筆')
    print(f'\n📊 命中率（precision = 是否真的 cross 上 EMA20）:')
    for k, v in sorted(stats.items()):
        print(f'  {k}:')
        for hd in ['5d', '15d', '30d']:
            s = v.get(hd)
            if s:
                print(f'    {hd}: n={s["n"]}, cross_rate={s["cross_rate"]}%, '
                      f'mean_ret={s["mean_ret"]:+.2f}%, win_rate={s["win_rate"]}%')


def _compute_stats(candidates):
    """計算各 (market, tier) 組合的 cross precision + 報酬"""
    by_group = defaultdict(lambda: {f'{n}d': [] for n in HORIZONS})
    for c in candidates:
        key = f"{c.get('market','?')}_{c.get('tier','?')}"
        if c.get('imminent_dc'): key += '_dc'
        for n in HORIZONS:
            o = c.get('outcomes', {}).get(f'{n}d', {})
            if o.get('ret_pct') is not None:
                by_group[key][f'{n}d'].append({
                    'ret': o['ret_pct'],
                    'crossed': o.get('crossed'),
                })

    out = {}
    for k, horizons in by_group.items():
        out[k] = {}
        for hd, rows in horizons.items():
            if not rows:
                out[k][hd] = None
                continue
            n = len(rows)
            crossed = sum(1 for r in rows if r['crossed'])
            wins = sum(1 for r in rows if r['ret'] > 0)
            mean_ret = sum(r['ret'] for r in rows) / n
            out[k][hd] = {
                'n': n,
                'cross_rate': round(crossed / n * 100, 1),  # 真的上穿 %
                'mean_ret': round(mean_ret, 2),
                'win_rate': round(wins / n * 100, 1),
            }
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tw', action='store_true')
    p.add_argument('--us', action='store_true')
    args = p.parse_args()
    market = None
    if args.tw and not args.us: market = 'tw'
    elif args.us and not args.tw: market = 'us'

    t0 = time.time()
    update(market_filter=market)
    print(f'⏱ {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
