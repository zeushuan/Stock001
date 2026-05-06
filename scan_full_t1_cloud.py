"""☁️ 全市場 T1 即將上穿掃描器 — Cloud 版（不依賴 data_cache）
================================================================
直接用 yfinance 批量抓 1y 資料 → 計算 EMA + ADX + RSI → 篩 T1 即將上穿候選

對應 local 版 scan_full_t1_imminent.py，但適用 GitHub Actions runner（無 data_cache）

支援：
  全部 TW 4-digit common stocks（從 vwap_applicable.json 拿全清單）
  全部 US（從 us_applicable.json 拿全清單，含 NA tier）

預估時間：
  TW ~1500 檔：30 batches × 30s ≈ 15 分鐘
  US ~2000 檔：40 batches × 30s ≈ 20 分鐘
  指標計算 ~2 分鐘
  總計 ~35-40 分鐘（每週跑一次足夠）

執行：
  python scan_full_t1_cloud.py        # 兩市場
  python scan_full_t1_cloud.py --tw
  python scan_full_t1_cloud.py --us
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

# 質量過濾
TW_MIN_VOL = 500_000
TW_MIN_PRICE = 5.0
US_MIN_VOL = 1_000_000
US_MIN_PRICE = 5.0


def _calc_ind(df):
    """計算 EMA20 / EMA60 / ADX / RSI / ATR"""
    if df is None or len(df) < 60:
        return None
    df = df.copy()
    df['e20'] = ta.trend.ema_indicator(df['Close'], window=20)
    df['e60'] = ta.trend.ema_indicator(df['Close'], window=60)
    df['rsi'] = ta.momentum.rsi(df['Close'], window=14)
    df['adx'] = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    df['atr'] = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], 14)
    return df


def _fetch_batch(tickers, period='1y'):
    """批量 yfinance 抓取"""
    out = {}
    BATCH = 50
    for i in range(0, len(tickers), BATCH):
        batch = tickers[i:i+BATCH]
        try:
            df = yf.download(batch, period=period, interval='1d',
                             auto_adjust=False, progress=False,
                             group_by='ticker', threads=True)
            if df is None or df.empty: continue
            for t in batch:
                try:
                    if len(batch) == 1:
                        sub = df
                    else:
                        sub = df[t]
                    sub = sub.dropna(how='all')
                    if len(sub) < 60: continue
                    out[t] = sub
                except Exception:
                    continue
        except Exception as e:
            print(f"  ❌ batch {i//BATCH+1} 失敗: {str(e)[:80]}")
        if (i // BATCH + 1) % 5 == 0:
            print(f"  📦 進度 {i//BATCH+1}/{(len(tickers)+BATCH-1)//BATCH}: 累計成功 {len(out)} 檔")
    return out


def scan_universe(market, tickers):
    """掃描某市場的 ticker 清單，回傳 T1 即將上穿候選 list"""
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"\n{flag} {market.upper()} 全市場掃描: {len(tickers)} 檔")

    # 🆕 v9.14：TW 改用 TWSE/TPEX 官方 API（不依賴 yfinance .TW，GitHub runner 友善）
    t0 = time.time()
    if market == 'tw':
        try:
            from fetch_tw_official import fetch_all_tw_history
            print(f"  📥 TWSE/TPEX 官方 API（200 個交易日，預估 6-8 分鐘）...")
            all_data = fetch_all_tw_history(days=200)
            requested = set(tickers)
            df_dict = {t: df for t, df in all_data.items() if t in requested}
            print(f"  完成 {time.time()-t0:.1f}s，universe 命中 {len(df_dict)}/{len(tickers)}")
        except Exception as e:
            print(f"  ⚠️ TWSE/TPEX 失敗 ({e})，fallback 到 yfinance")
            yf_tickers = [f'{t}.TW' for t in tickers]
            df_dict = _fetch_batch(yf_tickers, period='1y')
            print(f"  完成 {time.time()-t0:.1f}s，成功 {len(df_dict)}/{len(yf_tickers)}")
    else:
        # US 維持 yfinance batch
        yf_tickers = tickers
        print(f"  📥 yfinance 抓取（period=1y, batch=50）...")
        df_dict = _fetch_batch(yf_tickers, period='1y')
        print(f"  完成 {time.time()-t0:.1f}s，成功 {len(df_dict)}/{len(yf_tickers)}")

    # 名稱對應
    name_map = {}
    if market == 'tw' and Path('tw_stock_list.json').exists():
        try:
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            for t, info in d.items():
                if isinstance(info, dict):
                    name_map[t] = info.get('name', '')
        except Exception: pass
    elif market == 'us' and Path('us_full_tickers.json').exists():
        try:
            full = json.load(open('us_full_tickers.json', encoding='utf-8'))
            for x in full.get('detail', []):
                sym = x.get('symbol', '')
                nm = x.get('name', '')
                for sep in [' - ', ' Common ', ' Class ', ' Ordinary ']:
                    if sep in nm:
                        nm = nm.split(sep)[0]
                        break
                if sym: name_map[sym] = nm[:40]
        except Exception: pass

    # 計算指標 + 篩選
    print(f"  🧮 計算指標 + 篩選...")
    t0 = time.time()
    results = []
    min_vol = TW_MIN_VOL if market == 'tw' else US_MIN_VOL
    min_price = TW_MIN_PRICE if market == 'tw' else US_MIN_PRICE

    for yf_t, df in df_dict.items():
        # 🆕 v9.14：fetch_tw_official 已用純 ticker，不需 strip .TW
        ticker = yf_t.replace('.TW', '') if (market == 'tw' and '.TW' in yf_t) else yf_t
        df_ind = _calc_ind(df)
        if df_ind is None: continue
        if len(df_ind) < 60: continue

        c_arr = df_ind['Close'].values
        v_arr = df_ind['Volume'].values
        e20_arr = df_ind['e20'].values
        e60_arr = df_ind['e60'].values
        adx_arr = df_ind['adx'].values
        rsi_arr = df_ind['rsi'].values
        atr_arr = df_ind['atr'].values if 'atr' in df_ind.columns else None

        i = len(df_ind) - 1
        close = float(c_arr[i])
        e20 = float(e20_arr[i]) if not np.isnan(e20_arr[i]) else None
        e60 = float(e60_arr[i]) if not np.isnan(e60_arr[i]) else None
        adx = float(adx_arr[i]) if not np.isnan(adx_arr[i]) else None
        rsi = float(rsi_arr[i]) if not np.isnan(rsi_arr[i]) else None
        atr = float(atr_arr[i]) if atr_arr is not None and not np.isnan(atr_arr[i]) else None

        if e20 is None or e60 is None: continue
        if e20 <= e60: continue
        if close >= e20: continue
        if close < min_price: continue
        if i < 60: continue
        avg_vol = float(np.mean(v_arr[-60:]))
        if avg_vol < min_vol: continue

        if i < 2: continue
        if not (close > c_arr[i-1] > c_arr[i-2]): continue

        dist_pct = (e20 - close) / e20 * 100

        if dist_pct <= 1.0 and adx is not None and adx >= 22:
            tier = 'L1_strict'
        elif dist_pct <= 2.0 and adx is not None and adx >= 22:
            tier = 'L2_medium'
        elif dist_pct <= 3.0:
            tier = 'L3_loose'
        else:
            continue

        # 🆕 v9.12：imminent_dc 偵測
        cross_days = None
        try:
            diff_arr = e20_arr - e60_arr
            for _k in range(1, min(i, 200)):
                d1 = diff_arr[i - _k + 1]
                d0 = diff_arr[i - _k]
                if not np.isnan(d1) and not np.isnan(d0):
                    if d0 < 0 and d1 >= 0:
                        cross_days = _k; break
                    elif d0 > 0 and d1 <= 0:
                        cross_days = -_k; break
        except Exception: pass

        imminent_dc = False
        if (cross_days is not None and cross_days > 10
                and atr is not None and atr > 0
                and e20 > e60 and (e20 - e60) < atr):
            e20_5d = e20_arr[i - 5] if i >= 5 and not np.isnan(e20_arr[i-5]) else None
            ema20_falling = (e20_5d is not None and e20 < e20_5d)
            if ema20_falling or cross_days > 30:
                imminent_dc = True

        quality = (50 - rsi) if rsi is not None else 0
        if imminent_dc:
            quality -= 20

        results.append({
            'ticker': ticker,
            'name': name_map.get(ticker, ''),
            'market': market,
            'tier': tier,
            'close': round(close, 2),
            'ema20': round(e20, 2),
            'dist_pct': round(dist_pct, 2),
            'adx': round(adx, 1) if adx else None,
            'rsi': round(rsi, 1) if rsi else None,
            'quality_score': round(quality, 1),
            'date': df_ind.index[i].strftime('%Y-%m-%d'),
            'cross_days': cross_days,
            'imminent_dc': bool(imminent_dc),
            'gap_atr': round((e20 - e60) / atr, 2) if atr and atr > 0 else None,
        })

    print(f"  完成 {time.time()-t0:.1f}s — 找到 {len(results)} 檔候選")
    return results


def _append_to_history(output, hist_path='t1_imminent_history.json'):
    """🆕 v9.12：把當次掃描的 L1/L2 候選 append 到歷史檔，後續回算 hit-rate
    重複日期 + ticker + tier 不重加（idempotent）"""
    scan_date = output.get('updated_at', pd.Timestamp.now().strftime('%Y-%m-%d'))

    # 載入既有歷史
    if Path(hist_path).exists():
        try:
            hist = json.load(open(hist_path, encoding='utf-8'))
            if not isinstance(hist, dict) or 'candidates' not in hist:
                hist = {'candidates': []}
        except Exception:
            hist = {'candidates': []}
    else:
        hist = {'candidates': []}

    existing = set()
    for c in hist['candidates']:
        existing.add((c['scan_date'], c['ticker'], c.get('tier')))

    added = 0
    # 只追蹤 L1/L2（L3 太寬鬆不適合追蹤）
    for r in output.get('tw', []) + output.get('us', []):
        if r.get('tier') not in ('L1_strict', 'L2_medium'):
            continue
        key = (scan_date, r['ticker'], r['tier'])
        if key in existing:
            continue
        existing.add(key)
        hist['candidates'].append({
            'scan_date': scan_date,
            'ticker': r['ticker'],
            'name': r.get('name', ''),
            'market': r.get('market', ''),
            'tier': r['tier'],
            'entry_price': r.get('close'),  # 進場價（cross 那天近似）
            'ema20': r.get('ema20'),
            'dist_pct': r.get('dist_pct'),
            'adx': r.get('adx'),
            'rsi': r.get('rsi'),
            'imminent_dc': r.get('imminent_dc', False),
            'outcomes': {
                '5d':  {'close': None, 'crossed': None, 'ret_pct': None, 'checked_at': None},
                '15d': {'close': None, 'crossed': None, 'ret_pct': None, 'checked_at': None},
                '30d': {'close': None, 'crossed': None, 'ret_pct': None, 'checked_at': None},
            },
        })
        added += 1

    hist['last_updated'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    hist['total_candidates'] = len(hist['candidates'])

    with open(hist_path, 'w', encoding='utf-8') as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)

    print(f"📝 t1_imminent_history: 新增 {added} 筆 (總 {len(hist['candidates'])})")
    return added


def get_full_universe(market):
    """從 *_applicable.json 拿全清單（含 OK + NA tier，跳出 TOP 限制）"""
    if market == 'tw':
        # 從 tw_stock_list.json 拿全部 4-digit
        if Path('tw_stock_list.json').exists():
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            return sorted([t for t, info in d.items()
                           if isinstance(info, dict)
                           and info.get('type') != 'ETF'
                           and t and t[0].isdigit() and len(t) == 4
                           and not t.startswith('00')])
    elif market == 'us':
        # 從 us_applicable.json 拿全 (TOP + OK，排除 NA)
        if Path('us_applicable.json').exists():
            d = json.load(open('us_applicable.json', encoding='utf-8'))
            return sorted([t for t, info in d.items()
                           if isinstance(info, dict)
                           and info.get('tier') != 'NA'
                           and t.isalpha() and t.isupper()
                           and 1 <= len(t) <= 5
                           and t not in US_ETF_EXCLUDE])
    return []


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
        'description': 'T1 即將上穿全市場掃描（雲端版，yfinance 直抓）',
        'tw': [],
        'us': [],
    }

    t_start = time.time()
    if do_tw:
        tw_uni = get_full_universe('tw')
        if tw_uni:
            output['tw'] = scan_universe('tw', tw_uni)
        else:
            print("⚠️ tw_stock_list.json 不存在，跳過 TW")
    if do_us:
        us_uni = get_full_universe('us')
        if us_uni:
            output['us'] = scan_universe('us', us_uni)
        else:
            print("⚠️ us_applicable.json 不存在，跳過 US")

    # 排序
    tier_order = {'L1_strict': 0, 'L2_medium': 1, 'L3_loose': 2}
    for k in ['tw', 'us']:
        output[k].sort(key=lambda r: (tier_order.get(r['tier'], 9), -r['quality_score']))

    out_file = 't1_imminent_full.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # 🆕 v9.12：append 進歷史檔（用於後續 hit-rate 回算）
    _append_to_history(output)

    elapsed = time.time() - t_start
    print(f"\n✅ 寫入 {out_file} (總耗時 {elapsed:.1f}s)")
    print(f"  🇹🇼 TW: {len(output['tw'])} 檔")
    print(f"  🇺🇸 US: {len(output['us'])} 檔")

    # 印 tier 統計
    for k, label in [('tw', '🇹🇼 TW'), ('us', '🇺🇸 US')]:
        items = output[k]
        if not items: continue
        by_tier = {}
        for r in items:
            by_tier.setdefault(r['tier'], 0)
            by_tier[r['tier']] += 1
        print(f"  {label}: " + " / ".join(f"{t}:{n}" for t, n in by_tier.items()))


if __name__ == '__main__':
    main()
