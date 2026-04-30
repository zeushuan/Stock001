"""命中率回算器（v9.11）
=============================
讀 alert_history.json，對「日子已到但還沒回算」的 alerts，
用 yfinance 抓 5/15/30 天後實際 close，計算 ret_pct + 更新 stats。

執行：
  python update_alert_outcomes.py        # 全部 markets
  python update_alert_outcomes.py --tw   # 只回算 TW
  python update_alert_outcomes.py --us   # 只回算 US

對 GitHub Actions 友善：
  - 沒事做就 print「無需更新」並 exit 0
  - 抓不到資料的 ticker 自動跳過，不影響流程
  - idempotent：同一筆 alert 已回算就不重算
"""
import sys, json, time, argparse
from pathlib import Path
from collections import defaultdict
import pandas as pd
import yfinance as yf

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

# 命中率回算用的時點（calendar days，避免處理交易日 calendar 太麻煩）
HORIZONS = [5, 15, 30]


def _yf_symbol(ticker, market):
    """轉 yfinance symbol。TW 加 .TW，US 不變。"""
    if market == 'tw':
        return f"{ticker}.TW" if not ticker.endswith('.TW') else ticker
    return ticker


def _get_close_at(df, target_date):
    """從 df 找 target_date 當天或之後第一個交易日的 close。
    回傳 (close, actual_date_str) 或 (None, None)。"""
    if df is None or df.empty:
        return None, None
    after = df[df.index >= target_date]
    if len(after) == 0:
        return None, None  # 還沒到那天（極少見：alert_date 太新）
    row = after.iloc[0]
    return float(row['Close']), after.index[0].strftime('%Y-%m-%d')


def _compute_stats(alerts):
    """計算各 (market, level, side) 組合的命中率 + 平均 ret。"""
    by_group = defaultdict(lambda: {f'{n}d': [] for n in HORIZONS})
    for a in alerts:
        # group key: market + level + side（imm_bull/imm_bear 也單獨分組）
        key = f"{a.get('market','?')}_{a.get('level','?')}_{a.get('side','?')}"
        for n in HORIZONS:
            r = a.get('outcomes', {}).get(f'{n}d', {}).get('ret_pct')
            if r is not None:
                by_group[key][f'{n}d'].append(r)

    out = {}
    for k, horizons in by_group.items():
        side = k.split('_')[-1]
        out[k] = {}
        for hd, rets in horizons.items():
            if not rets:
                out[k][hd] = None
                continue
            mean_ret = sum(rets) / len(rets)
            # bull → 漲 = 命中（ret > 0）；bear → 跌 = 命中（ret < 0）
            if side == 'bull':
                hits = sum(1 for r in rets if r > 0)
            else:
                hits = sum(1 for r in rets if r < 0)
            out[k][hd] = {
                'n': len(rets),
                'mean_ret': round(mean_ret, 2),
                'hit_rate': round(hits / len(rets) * 100, 1),
            }
    return out


def update(market_filter=None, hist_path='alert_history.json'):
    """回算 outcomes。market_filter='tw'/'us'/None（None=全部）"""
    if not Path(hist_path).exists():
        print(f'❌ {hist_path} 不存在，無 alerts 可回算')
        return False

    hist = json.load(open(hist_path, encoding='utf-8'))
    alerts = hist.get('alerts', [])
    if not alerts:
        print('history 內無 alerts')
        return False

    today = pd.Timestamp.now().normalize()
    print(f'今天: {today.strftime("%Y-%m-%d")}, 歷史筆數: {len(alerts)}')

    # 找出所有「日子已到但還沒回算」的 (alert_index, horizon, target_date)
    pending = []
    for i, a in enumerate(alerts):
        if market_filter and a.get('market') != market_filter:
            continue
        try:
            alert_date = pd.Timestamp(a['alert_date'])
        except Exception:
            continue
        outcomes = a.get('outcomes', {})
        for n in HORIZONS:
            target = alert_date + pd.Timedelta(days=n)
            if today < target:
                continue  # 還沒到那天
            if outcomes.get(f'{n}d', {}).get('ret_pct') is not None:
                continue  # 已回算
            pending.append((i, n, target))

    if not pending:
        print('✅ 無需更新（沒有日子已到但還沒回算的 alerts）')
        return False

    print(f'📋 待回算: {len(pending)} 筆')

    # 收集需下載的 ticker 集合
    tickers = {}  # (ticker, market) → list of (alert_idx, horizon, target_date)
    for i, n, target in pending:
        a = alerts[i]
        key = (a['ticker'], a.get('market', 'tw'))
        tickers.setdefault(key, []).append((i, n, target))

    print(f'📥 下載 {len(tickers)} 個獨立 ticker（最多 3 個月歷史）...')

    # 下載 — 對每個 ticker 抓 3 個月歷史
    price_cache = {}
    failed = []
    for idx, (ticker, market) in enumerate(tickers, 1):
        sym = _yf_symbol(ticker, market)
        try:
            df = yf.download(sym, period='3mo', progress=False, auto_adjust=False)
            if df is not None and not df.empty:
                # Multi-index column 處理（單檔通常不會但保險）
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
        if idx % 20 == 0:
            print(f'  進度: {idx}/{len(tickers)}')

    if failed:
        print(f'⚠️  {len(failed)} 個下載失敗（會 skip）: {failed[:5]}...')

    # 回算
    updated = 0
    for i, n, target in pending:
        a = alerts[i]
        df = price_cache.get((a['ticker'], a.get('market', 'tw')))
        if df is None:
            continue
        close, actual_date = _get_close_at(df, target)
        if close is None:
            continue
        entry = a.get('entry_price') or 0
        if entry <= 0:
            continue
        ret_pct = (close - entry) / entry * 100
        a.setdefault('outcomes', {})[f'{n}d'] = {
            'close': round(close, 2),
            'actual_date': actual_date,
            'ret_pct': round(ret_pct, 2),
            'checked_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        }
        updated += 1

    # 重算 stats
    stats = _compute_stats(alerts)
    hist['stats'] = stats
    hist['last_outcomes_update'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    with open(hist_path, 'w', encoding='utf-8') as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)

    print(f'\n✅ 更新 {updated}/{len(pending)} 筆 outcome')
    print('\n📊 命中率 (live):')
    for k, v in sorted(stats.items()):
        line = f'  {k}:'
        for hd in ['5d', '15d', '30d']:
            s = v.get(hd)
            if s:
                line += f' {hd}=n{s["n"]}/{s["hit_rate"]}%/{s["mean_ret"]:+.1f}%'
            else:
                line += f' {hd}=-'
        print(line)

    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tw', action='store_true', help='只回算 TW')
    p.add_argument('--us', action='store_true', help='只回算 US')
    args = p.parse_args()

    market = None
    if args.tw and not args.us: market = 'tw'
    elif args.us and not args.tw: market = 'us'

    t0 = time.time()
    update(market_filter=market)
    print(f'⏱  {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
