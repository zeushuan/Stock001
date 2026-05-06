"""☁️ 全市場篩選器預計算（cloud 版，v9.13）
=================================================
不依賴 data_cache，用 yfinance batch 抓資料 → 跑全部 34 filter →
輸出 screener_results.json（tv_app cloud 環境直接讀）

執行：
  python screener_full_cloud.py        # 兩市場
  python screener_full_cloud.py --tw
  python screener_full_cloud.py --us

預估時間：35-40 分鐘（同 weekly_full_scan）
"""
import sys, json, time, argparse
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
import ta

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
    'ARKK','ARKG','ARKF','ARKW','ARKQ',
}


def _calc_ind(df):
    if df is None or len(df) < 60: return None
    df = df.copy()
    df['e10'] = ta.trend.ema_indicator(df['Close'], window=10)
    df['e20'] = ta.trend.ema_indicator(df['Close'], window=20)
    df['e60'] = ta.trend.ema_indicator(df['Close'], window=60)
    df['rsi'] = ta.momentum.rsi(df['Close'], window=14)
    df['adx'] = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    df['atr'] = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], 14)
    return df


def _fetch_batch(tickers, period='1y', is_tw=False):
    """🆕 v9.13：TW 用較小 batch (25) + 失敗時 retry，避免 GitHub Actions 環境下大批 .TW 抓不到"""
    out = {}
    BATCH = 25 if is_tw else 50  # TW 用較小 batch
    failed_tickers = []
    for i in range(0, len(tickers), BATCH):
        batch = tickers[i:i+BATCH]
        success = False
        for retry in range(2):  # 最多 2 次嘗試
            try:
                df = yf.download(batch, period=period, interval='1d',
                                 auto_adjust=False, progress=False,
                                 group_by='ticker', threads=True)
                if df is None or df.empty:
                    if retry == 0:
                        time.sleep(2); continue
                    break
                batch_count = 0
                for t in batch:
                    try:
                        if len(batch) == 1: sub = df
                        else: sub = df[t]
                        sub = sub.dropna(how='all')
                        if len(sub) >= 60:
                            out[t] = sub
                            batch_count += 1
                    except Exception: continue
                success = True
                break
            except Exception as e:
                if retry == 0:
                    print(f"  ⚠️ batch {i//BATCH+1} 第 1 次失敗 (retry): {str(e)[:60]}")
                    time.sleep(3)
                else:
                    print(f"  ❌ batch {i//BATCH+1} 兩次失敗: {str(e)[:80]}")
        if not success:
            failed_tickers.extend(batch)
        if (i // BATCH + 1) % 5 == 0:
            print(f"  📦 進度 {i//BATCH+1}/{(len(tickers)+BATCH-1)//BATCH}: 累計 {len(out)} 檔")

    # 失敗的 ticker 個別重試
    if failed_tickers:
        print(f"  🔁 個別重試 {len(failed_tickers)} 檔失敗...")
        for t in failed_tickers[:30]:  # 限 30 個避免太慢
            try:
                df = yf.download(t, period=period, interval='1d',
                                 auto_adjust=False, progress=False)
                if df is not None and len(df) >= 60:
                    out[t] = df.dropna(how='all')
            except Exception: continue
    return out


def get_full_universe(market):
    if market == 'tw':
        if Path('tw_stock_list.json').exists():
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            return sorted([t for t, info in d.items()
                           if isinstance(info, dict)
                           and info.get('type') != 'ETF'
                           and t and t[0].isdigit() and len(t) == 4
                           and not t.startswith('00')])
    elif market == 'us':
        if Path('us_applicable.json').exists():
            d = json.load(open('us_applicable.json', encoding='utf-8'))
            return sorted([t for t, info in d.items()
                           if isinstance(info, dict)
                           and info.get('tier') != 'NA'
                           and t.isalpha() and t.isupper()
                           and 1 <= len(t) <= 5
                           and t not in US_ETF_EXCLUDE])
    return []


def _fetch_tw_via_twstock(tickers):
    """🆕 v9.13：用 twstock 抓 TW 資料（不依賴 yfinance .TW，免被 GitHub runner 阻擋）
    每檔抓 ~13 個月（涵蓋 EMA60/EMA120 等指標需求）"""
    import twstock
    from datetime import datetime
    out = {}
    today = datetime.now()
    start_year = today.year - 1
    start_month = today.month
    failed = 0
    for idx, t in enumerate(tickers, 1):
        try:
            stock = twstock.Stock(t)
            stock.fetch_from(start_year, start_month)
            if len(stock.price) < 60:
                failed += 1; continue
            df = pd.DataFrame({
                'Open': stock.open,
                'High': stock.high,
                'Low': stock.low,
                'Close': stock.price,
                'Volume': stock.capacity,
            }, index=pd.DatetimeIndex(stock.date))
            df = df.dropna(how='all')
            if len(df) >= 60:
                out[t] = df
            else:
                failed += 1
        except Exception:
            failed += 1
        if idx % 100 == 0:
            print(f"  📦 twstock 進度 {idx}/{len(tickers)}: 累計 {len(out)} 檔 / 失敗 {failed}")
    return out


def scan_market(market, tickers, name_map):
    """掃描某市場，跑全部 filter"""
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"\n{flag} {market.upper()} 掃描: {len(tickers)} 檔")

    t0 = time.time()
    if market == 'tw':
        # 🆕 v9.13：TW 改用 twstock（yfinance .TW 在 GitHub runner 不穩定）
        print(f"  📥 twstock 抓取（period=13mo, 預估 25-35 分鐘）...")
        df_dict = _fetch_tw_via_twstock(tickers)
    else:
        # US 維持 yfinance batch
        print(f"  📥 yfinance batch（period=1y, batch=50）...")
        df_dict = _fetch_batch(tickers, period='1y', is_tw=False)
    print(f"  完成 {time.time()-t0:.1f}s，成功 {len(df_dict)}/{len(tickers)}")

    # 算指標 + 跑所有 filter
    print(f"  🧮 計算指標 + 跑 filter...")
    t0 = time.time()
    from screener_filters import _get_state, FILTERS

    min_vol = 500_000 if market == 'tw' else 1_000_000
    min_price = 5.0

    # filter_name → list of matching tickers
    by_filter = {fname: [] for fname in FILTERS}

    for yf_t, df in df_dict.items():
        # 🆕 v9.13：TW twstock 已用純 ticker，不需 strip .TW；US 維持 yfinance ticker
        ticker = yf_t.replace('.TW', '') if (market == 'tw' and '.TW' in yf_t) else yf_t
        df_ind = _calc_ind(df)
        if df_ind is None: continue
        # 質量過濾
        try:
            v_arr = df_ind['Volume'].values
            close = float(df_ind['Close'].iloc[-1])
            if close < min_price: continue
            if len(v_arr) >= 60:
                avg_vol = float(np.mean(v_arr[-60:]))
                if avg_vol < min_vol: continue
        except Exception:
            continue

        state = _get_state(df_ind, market)
        if state is None: continue

        # 跑所有 filter，符合就 append
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
            except Exception:
                continue

    print(f"  完成 {time.time()-t0:.1f}s")
    return by_filter


def load_name_maps(market):
    name_map = {}
    if market == 'tw' and Path('tw_stock_list.json').exists():
        try:
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            for t, info in d.items():
                if isinstance(info, dict): name_map[t] = info.get('name', '')
        except Exception: pass
    elif market == 'us' and Path('us_full_tickers.json').exists():
        try:
            full = json.load(open('us_full_tickers.json', encoding='utf-8'))
            for x in full.get('detail', []):
                sym = x.get('symbol', '')
                nm = x.get('name', '')
                for sep in [' - ', ' Common ', ' Class ', ' Ordinary ']:
                    if sep in nm:
                        nm = nm.split(sep)[0]; break
                if sym: name_map[sym] = nm[:40]
        except Exception: pass
    return name_map


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tw', action='store_true')
    p.add_argument('--us', action='store_true')
    args = p.parse_args()
    do_tw = args.tw or not (args.tw or args.us)
    do_us = args.us or not (args.tw or args.us)

    output = {
        'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': '全市場篩選器預計算（34 filter × 全 universe）',
        'by_filter': {},  # filter_name → list of stocks
    }

    t_start = time.time()
    all_combined = {}

    if do_tw:
        tw_uni = get_full_universe('tw')
        if tw_uni:
            tw_name = load_name_maps('tw')
            tw_results = scan_market('tw', tw_uni, tw_name)
            for fname, items in tw_results.items():
                all_combined.setdefault(fname, []).extend(items)
    if do_us:
        us_uni = get_full_universe('us')
        if us_uni:
            us_name = load_name_maps('us')
            us_results = scan_market('us', us_uni, us_name)
            for fname, items in us_results.items():
                all_combined.setdefault(fname, []).extend(items)

    output['by_filter'] = all_combined

    out_file = 'screener_results.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    elapsed = time.time() - t_start
    print(f"\n✅ 寫入 {out_file} (總耗時 {elapsed:.1f}s)")

    # 印各 filter 統計
    print(f"\n📊 各 filter 結果統計:")
    for fname, items in sorted(all_combined.items(), key=lambda x: -len(x[1])):
        if items:
            tw_n = sum(1 for i in items if i.get('market') == 'tw')
            us_n = sum(1 for i in items if i.get('market') == 'us')
            print(f"  {fname}: 共 {len(items)} 檔 (TW {tw_n} / US {us_n})")


if __name__ == '__main__':
    main()
