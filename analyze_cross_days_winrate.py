"""黃金交叉後天數 × 勝率分析
==================================
目的：T1 黃金交叉後不同天數進場的勝率/RR 差異

分組：逐日 Day 1 ~ Day 15

對每個進場用 30 天持有計勝率/平均/RR
分 TW（1042 檔）vs US（高流動 555 檔）vs 同樣 ADX 22/18 條件
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
HOLD = 30  # 30 天持有
MIN_ADV = 104_000_000

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','IEF','SHY','BIL','GLD','SLV','USO','UNG','UCO','SCO','BOIL',
    'KOLD','UNL','IAU','PALL','PPLT','DBA','DBC','GSG','DBO','DBE','EEM',
    'EWJ','EWZ','EWY','FXI','MCHI','INDA','EWG','EWU','EWC','EWA','EWT','EWS',
    'EWH','EWP','EWQ','EWI','EWN','EWL','EWO','XLK','XLF','XLV','XLE','XLY',
    'XLP','XLI','XLU','XLB','XLRE','XLC','XOP','XBI','XME','XHB','XRT','XPH',
    'XAR','XSD','XSW','XTL','SMH','SOXX','IBB','XHE','SCHB','VGT','VHT','VFH',
    'VIS','VDE','VNQ','VOX','VPU','VAW','VCR','VDC','VYM','ARKK','ARKQ','ARKW',
    'ARKG','ARKF','ARKX','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','SVXY','UVXY',
    'VXX','VIXY','NUGT','DUST','JNUG','JDST','GUSH','DRIP','LABU','LABD','TMF',
    'TMV','TNA','TZA','UDOW','SDOW','SPXL','SPXS','UWM','TWM','URTY','SRTY',
    'YINN','YANG','EDC','EDZ','BOND','RWM','SH','SDS','SSO','QID','QLD','AGGY',
    'SCHO','SCHR','SCHZ','VCIT','VCSH','VCLT','MBB','MUB','HYS','JETS','MOON',
    'JEPI','JEPQ','SCHD','DIVO','VOOV','VOOG','SPLG','SPLV',
}


def analyze_one(args):
    """對單檔股票，找出所有 T1 進場機會，依 cross_days 分組存報酬"""
    ticker, market = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        # 只取 2020-2026
        idx = df.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx = idx.tz_localize(None)
        mask = (idx >= pd.Timestamp('2020-01-01'))
        df = df[mask]
        if len(df) < 280: return (ticker, None)

        e20 = df['e20'].values
        e60 = df['e60'].values
        rsi = df['rsi'].values if 'rsi' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        close = df['Close'].values
        n = len(df)

        # ADX 門檻：TW=22 / US=18
        adx_th = 18 if market == 'us' else 22

        # 逐日分組 Day 1 ~ Day 15
        results = {f'd{d}': [] for d in range(1, 16)}

        for i in range(60, n - HOLD):
            if rsi is None or adx is None: continue
            if any(np.isnan(x) for x in [e20[i], e60[i], rsi[i], adx[i]]): continue
            if e20[i] <= e60[i]: continue
            if adx[i] < adx_th: continue

            # 找最近一次黃金交叉
            cd = None
            for k in range(1, min(60, i)):
                if np.isnan(e20[i-k]) or np.isnan(e60[i-k]): continue
                if e20[i-k] <= e60[i-k]:
                    cd = k
                    break
            if cd is None or cd > 15: continue   # 只取 Day 1 ~ 15

            ret = (close[i + HOLD] - close[i]) / close[i] * 100
            results[f'd{cd}'].append(ret)

        return (ticker, results)
    except Exception:
        return (ticker, None)


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    a = a[~np.isnan(a)]
    if len(a) == 0: return None
    return {
        'n': len(a), 'mean': float(a.mean()),
        'median': float(np.median(a)),
        'win': float((a > 0).mean() * 100),
        'worst': float(a.min()),
        'best': float(a.max()),
        'rr': float(a.mean() / abs(a.min())) if a.min() < 0 else 0,
    }


def main():
    DATA = Path('data_cache')
    # TW universe
    tw_universe = sorted([p.stem for p in DATA.glob('*.parquet')
                          if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                          and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    tw_universe = [t for t in tw_universe if t in vwap_set]

    # US universe (高流動)
    us_full = json.loads(Path('us_full_tickers.json').read_text(encoding='utf-8'))
    us_high_liquid = []
    for t in sorted(us_full['tickers']):
        if t in US_ETF_EXCLUDE: continue
        if not (DATA / f'{t}.parquet').exists(): continue
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
            if adv >= MIN_ADV: us_high_liquid.append(t)
        except: pass

    print(f"🇹🇼 TW universe: {len(tw_universe)} 檔")
    print(f"🇺🇸 US 高流動 universe: {len(us_high_liquid)} 檔\n")

    DAY_KEYS = [f'd{d}' for d in range(1, 16)]

    # 跑 TW
    print("📊 跑 TW T1 進場分析...")
    t0 = time.time()
    tw_results = {k: [] for k in DAY_KEYS}
    tasks = [(t, 'tw') for t in tw_universe]
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker, r in ex.map(analyze_one, tasks, chunksize=80):
            if r is not None:
                for k, v in r.items():
                    tw_results[k].extend(v)
    print(f"  完成 {time.time()-t0:.1f}s\n")

    # 跑 US
    print("📊 跑 US T1 進場分析...")
    t0 = time.time()
    us_results = {k: [] for k in DAY_KEYS}
    tasks = [(t, 'us') for t in us_high_liquid]
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker, r in ex.map(analyze_one, tasks, chunksize=80):
            if r is not None:
                for k, v in r.items():
                    us_results[k].extend(v)
    print(f"  完成 {time.time()-t0:.1f}s\n")

    # 報告（逐日 Day 1 ~ 15）
    bucket_labels = [(f'd{d}', f'Day {d:>2}') for d in range(1, 16)]

    for market_name, results, adx_th in [
        ('🇹🇼 TW (ADX≥22)', tw_results, 22),
        ('🇺🇸 US 高流動 (ADX≥18)', us_results, 18),
    ]:
        print("=" * 100)
        print(f"📊 {market_name} — 黃金交叉後 N 天進場 × 30 天持有")
        print("=" * 100)
        print(f"{'分組':<10} {'樣本':>8} {'勝率%':>8} {'均報%':>9} "
              f"{'中位%':>8} {'最差%':>9} {'RR':>7}  視覺化")
        print("-" * 100)
        for key, label in bucket_labels:
            m = metrics(results[key])
            if m:
                bar = '█' * max(0, int(m['rr'] * 30))
                print(f"{label:<10} {m['n']:>8} {m['win']:>+8.1f} "
                      f"{m['mean']:>+9.2f} {m['median']:>+8.2f} "
                      f"{m['worst']:>+9.1f} {m['rr']:>7.3f}  {bar[:30]}")
            else:
                print(f"{label:<10} (無資料)")
        print()

    # 跨市場 RR 排行
    print("=" * 100)
    print("🏆 跨市場 RR 排行 Top 10")
    print("=" * 100)
    print(f"{'市場/Day':<24} {'樣本':>8} {'勝率%':>8} {'均報%':>9} {'RR':>7}")
    print("-" * 100)
    rows = []
    for mkt_name, results in [('🇹🇼 TW', tw_results), ('🇺🇸 US', us_results)]:
        for key, label in bucket_labels:
            m = metrics(results[key])
            if m:
                rows.append((mkt_name, label, m))
    rows.sort(key=lambda x: -x[2]['rr'])
    for mkt, label, m in rows[:10]:
        print(f"{mkt + ' / ' + label:<24} {m['n']:>8} {m['win']:>+8.1f} "
              f"{m['mean']:>+9.2f} {m['rr']:>7.3f}")

    # 趨勢摘要
    print("\n" + "=" * 100)
    print("📈 衰減趨勢摘要")
    print("=" * 100)
    for mkt_name, results in [('🇹🇼 TW', tw_results), ('🇺🇸 US', us_results)]:
        rrs = []
        wins = []
        for d in range(1, 16):
            m = metrics(results[f'd{d}'])
            if m:
                rrs.append((d, m['rr']))
                wins.append((d, m['win']))
        if rrs:
            best_rr = max(rrs, key=lambda x: x[1])
            best_win = max(wins, key=lambda x: x[1])
            d1_rr = next((rr for d, rr in rrs if d == 1), None)
            d10_rr = next((rr for d, rr in rrs if d == 10), None)
            d15_rr = next((rr for d, rr in rrs if d == 15), None)
            print(f"\n  {mkt_name}:")
            print(f"    最佳 RR Day {best_rr[0]} = {best_rr[1]:.3f}")
            print(f"    最高勝率 Day {best_win[0]} = {best_win[1]:.1f}%")
            if d1_rr is not None and d10_rr is not None:
                print(f"    Day 1 RR {d1_rr:.3f} → Day 10 RR {d10_rr:.3f} "
                      f"(衰減 {((d10_rr-d1_rr)/d1_rr*100):+.0f}%)")
            if d10_rr is not None and d15_rr is not None:
                print(f"    Day 10 RR {d10_rr:.3f} → Day 15 RR {d15_rr:.3f}")

    # 寫 JSON
    out = {
        'tw_metrics': {k: metrics(v) for k, v in tw_results.items()},
        'us_metrics': {k: metrics(v) for k, v in us_results.items()},
        'config': {
            'tw_adx_th': 22, 'us_adx_th': 18,
            'hold_days': HOLD,
            'tw_universe_size': len(tw_universe),
            'us_universe_size': len(us_high_liquid),
        }
    }
    with open('cross_days_winrate.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("\n💾 寫入 cross_days_winrate.json")


if __name__ == '__main__':
    main()
