"""local 版：用 data_cache 直接跑（10-15 秒）→ screener_results.json
跟 screener_full_cloud.py 同樣輸出格式，但用 local parquet 不爆 yfinance"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
from screener_filters import _get_state, FILTERS

US_ETF_EXCLUDE = {'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
                   'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
                   'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
                   'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
                   'ARKK','ARKG','ARKF','ARKW','ARKQ'}


def get_universe(market):
    DATA = Path('data_cache')
    if market == 'tw':
        return sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    elif market == 'us':
        return sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem.isalpha() and p.stem.isupper()
                       and 1 <= len(p.stem) <= 5 and p.stem not in US_ETF_EXCLUDE])
    return []


def load_name_maps():
    name_map = {}
    if Path('tw_stock_list.json').exists():
        try:
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            for t, info in d.items():
                if isinstance(info, dict): name_map[t] = info.get('name', '')
        except Exception: pass
    if Path('us_full_tickers.json').exists():
        try:
            full = json.load(open('us_full_tickers.json', encoding='utf-8'))
            for x in full.get('detail', []):
                sym = x.get('symbol', '')
                nm = x.get('name', '')
                for sep in [' - ', ' Common ', ' Class ', ' Ordinary ']:
                    if sep in nm: nm = nm.split(sep)[0]; break
                if sym: name_map[sym] = nm[:40]
        except Exception: pass
    return name_map


def scan(market, tickers, name_map):
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"{flag} {market.upper()} 掃描 {len(tickers)} 檔...")
    t0 = time.time()
    min_vol = 500_000 if market == 'tw' else 1_000_000
    min_price = 5.0
    by_filter = {fname: [] for fname in FILTERS}

    for ticker in tickers:
        try:
            df = dl.load_from_cache(ticker)
            if df is None or len(df) < 60: continue
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df = df.copy(); df.index = df.index.tz_localize(None)

            try:
                close = float(df['Close'].iloc[-1])
                if close < min_price: continue
                v_arr = df['Volume'].values
                if len(v_arr) >= 60:
                    avg_vol = float(np.mean(v_arr[-60:]))
                    if avg_vol < min_vol: continue
            except Exception: continue

            state = _get_state(df, market)
            if state is None: continue

            for fname, fn in FILTERS.items():
                try:
                    if fn(state):
                        by_filter[fname].append({
                            'ticker': ticker,
                            'name': name_map.get(ticker, ''),
                            'market': market,
                            'close': round(state['close'], 2),
                            'rsi': round(state['rsi'], 1) if state.get('rsi') else None,
                            'adx': round(state['adx'], 1) if state.get('adx') else None,
                            'is_bull': state['is_bull'],
                            'cross_days': state.get('cross_days'),
                            'pct_b': round(state['bb_pct_b'], 2) if state.get('bb_pct_b') is not None else None,
                            'from_high': round(state['from_high'], 1),
                            'from_low': round(state['from_low'], 1),
                            'imminent_dc': state.get('imminent_dc', False),
                            'date': state['date'],
                        })
                except Exception: continue
        except Exception: continue

    print(f"  完成 {time.time()-t0:.1f}s")
    return by_filter


def main():
    name_map = load_name_maps()
    output = {
        'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': '全市場篩選器預計算（local data_cache 版）',
        'by_filter': {},
    }
    all_combined = {}
    for market in ['tw', 'us']:
        uni = get_universe(market)
        if not uni: continue
        results = scan(market, uni, name_map)
        for fname, items in results.items():
            all_combined.setdefault(fname, []).extend(items)
    output['by_filter'] = all_combined

    with open('screener_results.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 screener_results.json")
    print(f"\n📊 各 filter 結果:")
    for fname, items in sorted(all_combined.items(), key=lambda x: -len(x[1])):
        if items:
            tw_n = sum(1 for i in items if i.get('market') == 'tw')
            us_n = sum(1 for i in items if i.get('market') == 'us')
            print(f"  {fname}: 共 {len(items)} (TW {tw_n} / US {us_n})")


if __name__ == '__main__':
    main()
