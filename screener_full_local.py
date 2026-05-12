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
    """🆕 v9.20.11：universe 涵蓋更廣
    - TW: 4-6 字元數字開頭（含 ETF 00*、active ETF 00981A、權證等）
    - US: 1-5 字元純字母，移除 leveraged/volatility ETF（保留主流 ETF）"""
    DATA = Path('data_cache')
    if market == 'tw':
        # 包含 4 位數個股 + 5-6 位數 ETF/權證 + 0 開頭 ETF
        return sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and 4 <= len(p.stem) <= 7])
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
    """🆕 v9.20.4：3-pass — state collect → RS Rating → filter（universe-wide RS）"""
    from sepa_vcp import compute_rs_ratings
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"{flag} {market.upper()} 掃描 {len(tickers)} 檔...")
    t0 = time.time()
    min_vol = 500_000 if market == 'tw' else 1_000_000
    min_price = 5.0
    by_filter = {fname: [] for fname in FILTERS}

    # ── Pass 1：state + returns ──
    ticker_states = {}
    ticker_returns = {}
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
            ticker_states[ticker] = state
            ticker_returns[ticker] = {
                '13w': state.get('returns_13w', 0),
                '26w': state.get('returns_26w', 0),
                '39w': state.get('returns_39w', 0),
                '52w': state.get('returns_52w', 0),
            }
        except Exception: continue

    # ── Pass 2：RS Rating（universe-wide）──
    rs_ratings = compute_rs_ratings(ticker_returns)
    for ticker, rs in rs_ratings.items():
        if ticker in ticker_states:
            ticker_states[ticker]['rs_rating'] = rs

    # ── 🆕 v9.24 Pass 2.5：RS Leading High（紫色點訊號）──
    try:
        from scanners.rs_leading_high import (detect_rs_leading_high,
                                                apply_quality_filters, score_signal)
        from universes.us_universe import get_theme_for_ticker
        idx_tk = '^GSPC' if market == 'us' else '^TWII'
        idx_df = dl.load_from_cache(idx_tk)
        if idx_df is not None:
            idx_close = idx_df['Close'].astype(float)
            raw_sigs = []
            for ticker, state in ticker_states.items():
                try:
                    df = dl.load_from_cache(ticker)
                    if df is None or len(df) < 200: continue
                    sig = detect_rs_leading_high(
                        stock_prices=df['Close'].astype(float),
                        index_prices=idx_close,
                        stock_volumes=df['Volume'].astype(float),
                        ticker=ticker,
                        as_of_date=df.index[-1],
                    )
                    if sig is None: continue
                    if apply_quality_filters(sig, df['Close'], df['Volume'],
                                              market=('US' if market == 'us' else 'TW')):
                        raw_sigs.append((ticker, sig, df['Close']))
                except Exception: continue

            # 用 universe context 算分
            if raw_sigs:
                rs_slopes = [s.rs_slope_50d for _, s, _ in raw_sigs]
                ctx = {'rs_slopes': rs_slopes}
                for tk, sig, sp in raw_sigs:
                    recent = sp.tail(30)
                    if len(recent) >= 10 and recent.mean() > 0:
                        ctx[f'cv_{tk}'] = float(recent.std() / recent.mean())
                for tk, sig, _ in raw_sigs:
                    score_signal(sig, ctx)
                # 排序給 rank
                raw_sigs.sort(key=lambda x: -(x[1].quality_score or 0))
                for r, (tk, sig, _) in enumerate(raw_sigs):
                    ticker_states[tk]['rs_leading_high_passed'] = True
                    ticker_states[tk]['rs_leading_high_score'] = sig.quality_score
                    ticker_states[tk]['rs_leading_high_purple_dots'] = sig.purple_dot_count_recent
                    ticker_states[tk]['rs_leading_high_distance'] = round(sig.stock_distance_from_high_pct * 100, 2)
                    ticker_states[tk]['rs_leading_high_rank'] = r + 1
                    ticker_states[tk]['rs_leading_high_theme'] = sig.theme
            print(f"  RS leading high: {len(raw_sigs)} signals")
    except Exception as e:
        print(f"  RS leading high skipped: {type(e).__name__}: {e}")

    # ── Pass 3：filter ──
    for ticker, state in ticker_states.items():
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
                        'rs_rating': state.get('rs_rating'),
                        'sepa_n_met': state.get('sepa_n_met', 0),
                        'vcp_is_vcp': state.get('vcp_is_vcp', False),
                        'vcp_near_pivot_pct': state.get('vcp_near_pivot_pct', 0),
                        # 🆕 v9.24 RS Leading High
                        'rs_leading_high_score': state.get('rs_leading_high_score'),
                        'rs_leading_high_purple_dots': state.get('rs_leading_high_purple_dots'),
                        'rs_leading_high_distance': state.get('rs_leading_high_distance'),
                        'rs_leading_high_rank': state.get('rs_leading_high_rank'),
                        'rs_leading_high_theme': state.get('rs_leading_high_theme'),
                    })
            except Exception: continue

    print(f"  完成 {time.time()-t0:.1f}s ({len(ticker_states)} states + RS)")
    # 🆕 v9.20.5：回傳 rs_ratings 給主流程整合到 JSON
    market_rs = {tk: round(s.get('rs_rating'), 1) for tk, s in ticker_states.items()
                 if s.get('rs_rating') is not None}
    return by_filter, market_rs


def main():
    name_map = load_name_maps()
    output = {
        'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': '全市場篩選器預計算（local data_cache 版）',
        'by_filter': {},
        'rs_ratings': {},  # 🆕 v9.20.5：所有 ticker 的 RS Rating（不分是否在 filter 內）
    }
    all_combined = {}
    rs_all = {}
    for market in ['tw', 'us']:
        uni = get_universe(market)
        if not uni: continue
        results, market_rs = scan(market, uni, name_map)
        for fname, items in results.items():
            all_combined.setdefault(fname, []).extend(items)
        rs_all.update(market_rs)
    output['by_filter'] = all_combined
    output['rs_ratings'] = rs_all

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
