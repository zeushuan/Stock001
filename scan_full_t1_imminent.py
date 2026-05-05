"""全市場 T1 即將上穿掃描器（v9.12）
============================================
跳出 TOP 200 限制，用 data_cache 掃描：
  🇹🇼 全部 1925 檔台股
  🇺🇸 全部 2254 檔美股（已過濾 ETF）

三層條件（從嚴到寬）：
  L1（V7 嚴格）：距 EMA20 ≤ 1% + 連 2 漲 + ADX≥22 + 多頭
  L2（V8 中等）：距 EMA20 ≤ 2% + 連 2 漲 + ADX≥22 + 多頭
  L3（V9 寬鬆）：距 EMA20 ≤ 3% + 連 2 漲 + 多頭（ADX 不要求）

質量過濾（避免雞蛋水餃股雜訊）：
  - 平均日成交量 ≥ 50 萬股（TW）/ 100 萬股（US）
  - 收盤 ≥ 5 元（TW）/ 5 美元（US）

輸出：t1_imminent_full.json（tv_app 讀取顯示專屬 panel）

執行：
  python scan_full_t1_imminent.py            # 兩市場都掃
  python scan_full_t1_imminent.py --tw-only
  python scan_full_t1_imminent.py --us-only
"""
import sys, json, time, argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16

# 質量過濾
TW_MIN_VOL = 500_000      # 50 萬股
TW_MIN_PRICE = 5.0
US_MIN_VOL = 1_000_000    # 100 萬股
US_MIN_PRICE = 5.0

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
    'ARKK','ARKG','ARKF','ARKW','ARKQ',
}


def scan_one(args):
    """單檔掃描，回傳 list of dict（如有 T1 即將上穿訊號）
    🆕 v9.12：加 imminent_dc 偵測（即將死叉），衝突時標警告"""
    ticker, market = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 60:
            return []
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        if len(df) < 60:
            return []

        c_arr = df['Close'].values
        v_arr = df['Volume'].values if 'Volume' in df.columns else None
        e20_arr = df['e20'].values if 'e20' in df.columns else None
        e60_arr = df['e60'].values if 'e60' in df.columns else None
        adx_arr = df['adx'].values if 'adx' in df.columns else None
        rsi_arr = df['rsi'].values if 'rsi' in df.columns else None
        atr_arr = df['atr'].values if 'atr' in df.columns else None

        if any(x is None for x in [c_arr, e20_arr, e60_arr, adx_arr]):
            return []

        i = len(df) - 1
        close = float(c_arr[i])
        e20 = float(e20_arr[i]) if not np.isnan(e20_arr[i]) else None
        e60 = float(e60_arr[i]) if not np.isnan(e60_arr[i]) else None
        adx = float(adx_arr[i]) if not np.isnan(adx_arr[i]) else None
        rsi = float(rsi_arr[i]) if rsi_arr is not None and not np.isnan(rsi_arr[i]) else None
        atr = float(atr_arr[i]) if atr_arr is not None and not np.isnan(atr_arr[i]) else None

        if e20 is None or e60 is None: return []
        if e20 <= e60: return []
        if close >= e20: return []

        min_vol = TW_MIN_VOL if market == 'tw' else US_MIN_VOL
        min_price = TW_MIN_PRICE if market == 'tw' else US_MIN_PRICE
        if close < min_price: return []
        if v_arr is not None and len(v_arr) >= 60:
            avg_vol = float(np.mean(v_arr[-60:]))
            if avg_vol < min_vol: return []

        if i < 2: return []
        c_t = close
        c_t1 = float(c_arr[i-1])
        c_t2 = float(c_arr[i-2])
        if not (c_t > c_t1 > c_t2): return []

        dist_pct = (e20 - close) / e20 * 100

        if dist_pct <= 1.0 and adx is not None and adx >= 22:
            tier = 'L1_strict'
        elif dist_pct <= 2.0 and adx is not None and adx >= 22:
            tier = 'L2_medium'
        elif dist_pct <= 3.0:
            tier = 'L3_loose'
        else:
            return []

        # 🆕 v9.12：偵測 imminent_dc（即將死叉）
        # 找最近一次 cross_days
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

        # imminent_dc 條件（與 tv_app/get_operation_advice 一致）
        imminent_dc = False
        if (cross_days is not None and cross_days > 10
                and atr is not None and atr > 0
                and e20 > e60 and (e20 - e60) < atr):
            e20_5d = e20_arr[i - 5] if i >= 5 and not np.isnan(e20_arr[i-5]) else None
            ema20_falling = (e20_5d is not None and e20 < e20_5d)
            if ema20_falling or cross_days > 30:
                imminent_dc = True

        # 計算 quality_score（rsi_low priority）
        # 🆕 imminent_dc 觸發時 quality_score 降低（懲罰矛盾訊號）
        quality = (50 - rsi) if rsi is not None else 0
        if imminent_dc:
            quality -= 20  # 大幅降級

        return [{
            'ticker': ticker,
            'market': market,
            'tier': tier,
            'close': round(close, 2),
            'ema20': round(e20, 2),
            'dist_pct': round(dist_pct, 2),
            'adx': round(adx, 1) if adx else None,
            'rsi': round(rsi, 1) if rsi else None,
            'quality_score': round(quality, 1),
            'date': df.index[i].strftime('%Y-%m-%d'),
            # 🆕 v9.12：警告欄位
            'cross_days': cross_days,
            'imminent_dc': bool(imminent_dc),
            'gap_atr': round((e20 - e60) / atr, 2) if atr and atr > 0 else None,
        }]
    except Exception:
        return []


def _append_to_history(output, hist_path='t1_imminent_history.json'):
    """🆕 v9.12：把當次掃描的 L1/L2 候選 append 到歷史檔（idempotent）"""
    scan_date = output.get('updated_at', pd.Timestamp.now().strftime('%Y-%m-%d'))

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
            'entry_price': r.get('close'),
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


def get_universe(market):
    DATA = Path('data_cache')
    if market == 'tw':
        return sorted([
            p.stem for p in DATA.glob('*.parquet')
            if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
            and not p.stem.startswith('00')
        ])
    elif market == 'us':
        return sorted([
            p.stem for p in DATA.glob('*.parquet')
            if p.stem and p.stem.isalpha() and p.stem.isupper()
            and 1 <= len(p.stem) <= 5
            and p.stem not in US_ETF_EXCLUDE
        ])
    return []


def load_name_maps():
    """載入 ticker → name 對應"""
    name_map = {}
    # TW
    if Path('tw_stock_list.json').exists():
        try:
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            for t, info in d.items():
                if isinstance(info, dict):
                    name_map[t] = info.get('name', '')
        except Exception: pass
    # US
    if Path('us_full_tickers.json').exists():
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
    return name_map


def run(market):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"{flag} 全市場掃描: {len(universe)} 檔 {market.upper()}")

    t0 = time.time()
    args = [(t, market) for t in universe]
    all_results = []
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for r in ex.map(scan_one, args, chunksize=50):
            all_results.extend(r)
    print(f"  完成 {time.time()-t0:.1f}s — 找到 {len(all_results)} 檔候選")

    # 加 name
    name_map = load_name_maps()
    for r in all_results:
        r['name'] = name_map.get(r['ticker'], '')

    # 排序：tier 嚴格 > 寬鬆，內部按 quality_score 降序
    tier_order = {'L1_strict': 0, 'L2_medium': 1, 'L3_loose': 2}
    all_results.sort(key=lambda r: (tier_order.get(r['tier'], 9), -r['quality_score']))

    # 統計
    by_tier = {}
    for r in all_results:
        by_tier.setdefault(r['tier'], []).append(r)
    print(f"  L1_strict (距≤1%+ADX≥22): {len(by_tier.get('L1_strict', []))}")
    print(f"  L2_medium (距≤2%+ADX≥22): {len(by_tier.get('L2_medium', []))}")
    print(f"  L3_loose  (距≤3%):       {len(by_tier.get('L3_loose', []))}")

    return all_results


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tw-only', action='store_true')
    p.add_argument('--us-only', action='store_true')
    args = p.parse_args()

    do_tw = not args.us_only
    do_us = not args.tw_only

    output = {
        'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': 'T1 即將上穿全市場掃描（跳出 TOP 200 限制）',
        'tw': [],
        'us': [],
    }

    if do_tw:
        output['tw'] = run('tw')
    if do_us:
        output['us'] = run('us')

    out_file = 't1_imminent_full.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # 🆕 v9.12：append 進歷史檔
    _append_to_history(output)

    print(f"\n✅ 寫入 {out_file}")
    print(f"  TW: {len(output['tw'])} / US: {len(output['us'])}")

    # 印前 20 筆預覽
    for market in ['tw', 'us']:
        if output[market]:
            print(f"\n{'🇹🇼 TW' if market=='tw' else '🇺🇸 US'} 前 20 筆候選:")
            print(f"  {'Ticker':>8}{'Tier':>10}{'Close':>10}{'Dist':>8}{'ADX':>7}{'RSI':>7}{'Qty':>7}  Name")
            for r in output[market][:20]:
                print(f"  {r['ticker']:>8}{r['tier']:>10}{r['close']:>10.2f}"
                      f"{r['dist_pct']:>+7.2f}%{r.get('adx',0) or 0:>7.1f}"
                      f"{r.get('rsi',0) or 0:>7.1f}{r['quality_score']:>+7.1f}  {r.get('name','')[:30]}")


if __name__ == '__main__':
    main()
