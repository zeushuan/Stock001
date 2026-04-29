"""美股高流動 tier (ADV ≥ $104M / 556 檔) 內變體優化
========================================================
全市場 baseline RR 0.033，但高流動 tier 已達 RR 0.348
測試在這個小池子裡，再加變體能否突破 0.4 RR 門檻
"""
import sys, time, json
from pathlib import Path
import numpy as np
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs

WORKERS = 16
MIN_ADV = 104_000_000  # ≥ $104M（從 us_full_results 得 p75）

VARIANTS = [
    ('A baseline (current)',  'P5_T1T3+POS'),
    ('B ATH3.5 寬停損',       'P5_T1T3+POS+ATH3.5'),
    ('C ATH4.0 超寬停損',     'P5_T1T3+POS+ATH4.0'),
    ('D ADX25 嚴格趨勢',      'P5_T1T3+POS+ADX25'),
    ('E ADX18 寬鬆趨勢',      'P5_T1T3+POS+ADX18'),
    ('F RSI40 T3嚴格',        'P5_T1T3+POS+RSI40'),
    ('G RSI45',               'P5_T1T3+POS+RSI45'),
    ('H +MK SP500多頭',       'P5_T1T3+POS+MK'),
    ('I +DYNSTOP 動態停損',   'P5_T1T3+POS+DYNSTOP'),
    ('J +CB30 累損熔斷',      'P5_T1T3+POS+CB30'),
    ('K T1 only',             'P5_T1+POS'),
    ('L T3 only',             'P5_T3+POS'),
    ('M P0+POS (無 P5)',      'P0_T1T3+POS'),
    ('N P10+POS (高門檻)',    'P10_T1T3+POS'),
]
WINDOWS = [
    ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
    ('TRAIN (2020.1-2024.5)', '2020-01-02', '2024-05-31'),
    ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
]

US_ETF_EXCLUDE = {  # 同 test_us_strategies.py
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA',
    'AGG','LQD','HYG','IEF','SHY','BIL','GLD','SLV','USO','UNG','UCO',
    'SCO','BOIL','KOLD','UNL','IAU','PALL','PPLT','DBA','DBC','GSG',
    'DBO','DBE','EEM','EWJ','EWZ','EWY','FXI','MCHI','INDA','EWG','EWU',
    'EWC','EWA','EWT','EWS','EWH','EWP','EWQ','EWI','EWN','EWL','EWO',
    'XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLRE','XLC',
    'XOP','XBI','XME','XHB','XRT','XPH','XAR','XSD','XSW','XTL','SMH',
    'SOXX','IBB','XHE','SCHB','VGT','VHT','VFH','VIS','VDE','VNQ','VOX',
    'VPU','VAW','VCR','VDC','VYM','ARKK','ARKQ','ARKW','ARKG','ARKF',
    'ARKX','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','SVXY','UVXY','VXX',
    'VIXY','NUGT','DUST','JNUG','JDST','GUSH','DRIP','LABU','LABD','TMF',
    'TMV','TNA','TZA','UDOW','SDOW','SPXL','SPXS','UWM','TWM','URTY',
    'SRTY','YINN','YANG','EDC','EDZ','BOND','RWM','SH','SDS','SSO','QID',
    'QLD','AGGY','SCHO','SCHR','SCHZ','VCIT','VCSH','VCLT','MBB','MUB',
    'HYS','JETS','MOON','JEPI','JEPQ','SCHD','DIVO','VOOV','VOOG','SPLG',
    'SPLV',
}


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

    # 計算 ADV，篩高流動
    print(f"📊 計算高流動篩選（ADV ≥ ${MIN_ADV/1e6:.0f}M）...")
    high_liquid = []
    for t in sorted(full_tickers):
        if t in US_ETF_EXCLUDE: continue
        if not (DATA / f'{t}.parquet').exists(): continue
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
            if adv >= MIN_ADV:
                high_liquid.append(t)
        except: pass

    print(f"  高流動 tier: {len(high_liquid)} 檔\n")
    universe = high_liquid

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            for t in universe:
                all_tasks.append((t, mode, start, end, (var_name, win_name)))
    print(f"變體數 {len(VARIANTS)} × universe {len(universe)} × windows {len(WINDOWS)} "
          f"= {len(all_tasks)} 任務\n")

    t0 = time.time()
    bucket = {}
    n_done = 0
    milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                bucket.setdefault(label, []).append(ret)
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                print(f"  {pct:.0f}%", flush=True)

    print(f"\n完成 {time.time()-t0:.1f}s\n")

    print("=" * 110)
    print(f"📊 高流動 tier (ADV ≥ ${MIN_ADV/1e6:.0f}M / {len(universe)} 檔) "
          f"× 14 變體 RR 對比")
    print("=" * 110)
    print(f"{'變體':<28} {'FULL_RR':>9} {'TRAIN_RR':>10} {'TEST_RR':>9} "
          f"{'TEST_勝率%':>11} {'TEST_中位%':>11} {'TEST_n':>8}  Δ_TEST")
    print("-" * 110)

    a_test_rr = None
    rows = []
    for var_name, _ in VARIANTS:
        f = metrics(bucket.get((var_name, 'FULL  (2020.1-2026.4)'), []))
        tr = metrics(bucket.get((var_name, 'TRAIN (2020.1-2024.5)'), []))
        te = metrics(bucket.get((var_name, 'TEST  (2024.6-2026.4)'), []))
        if not (f and tr and te): continue
        if a_test_rr is None and var_name.startswith('A'):
            a_test_rr = te['rr']
        delta = (te['rr'] - a_test_rr) if a_test_rr is not None else 0.0
        rows.append((var_name, f, tr, te, delta))
        marker = ''
        if a_test_rr is not None and not var_name.startswith('A'):
            if delta > 0.05: marker = ' ⭐'
            elif delta > 0.01: marker = ' ✓'
            elif delta < -0.01: marker = ' ✗'
        print(f"{var_name:<28} {f['rr']:>9.3f} {tr['rr']:>10.3f} "
              f"{te['rr']:>9.3f} {te['win']:>+11.1f} {te['median']:>+11.1f} "
              f"{te['n']:>8}  {delta:>+6.3f}{marker}")

    print("\n🏆 TEST RR 排行")
    print("-" * 110)
    rows.sort(key=lambda r: -r[3]['rr'])
    for i, (v, f, tr, te, d) in enumerate(rows, 1):
        bar = '█' * max(0, int(te['rr'] * 50))
        print(f"  {i:>2}. {v:<28} TEST RR {te['rr']:>+6.3f}  Δ {d:>+6.3f}  "
              f"win {te['win']:>4.1f}%  med {te['median']:>+5.1f}%  {bar[:30]}")

    out = {
        'tier': f'ADV ≥ ${MIN_ADV/1e6:.0f}M',
        'universe_size': len(universe),
        'metrics': {f'{v}|{w}': metrics(bucket.get((v, w), []))
                    for v, _ in VARIANTS for w, _, _ in WINDOWS},
    }
    with open('us_high_liquid_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("\n💾 寫入 us_high_liquid_results.json")


if __name__ == '__main__':
    main()
