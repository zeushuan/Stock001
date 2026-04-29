"""美股全市場 v8 多策略對比
==============================
基於 baseline (P5+POS) RR 0.033 / 勝率 43.9%（虧錢）的弱訊號
測試 10 個調整方向，找出對美股有效的變體

調整方向：
  ATR 倍數（美股波動率更高 → 寬停損？）
  ADX 門檻（強趨勢過濾？）
  RSI T3 門檻（拉回深度？）
  跨市場過濾（VIX 恐慌期 / SP500 大盤多頭 / DXY 美元）
  出場優化（DYNSTOP 動態停損 / CB30 累損熔斷）
  訊號子集（T1 only / T3 only）
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

VARIANTS = [
    ('A baseline (current)',  'P5_T1T3+POS'),
    # ATR 倍數（美股波動更高，可能要寬停損）
    ('B ATH3.5 寬停損',       'P5_T1T3+POS+ATH3.5'),
    ('C ATH4.0 超寬停損',     'P5_T1T3+POS+ATH4.0'),
    # ADX 趨勢強度
    ('D ADX25 嚴格趨勢',      'P5_T1T3+POS+ADX25'),
    ('E ADX18 寬鬆趨勢',      'P5_T1T3+POS+ADX18'),
    # T3 RSI 門檻
    ('F RSI40 T3嚴格',        'P5_T1T3+POS+RSI40'),
    # 跨市場過濾
    ('G +VIX25 恐慌過濾',     'P5_T1T3+POS+VIX25'),
    ('H +MK SP500多頭',       'P5_T1T3+POS+MK'),
    ('I +DXY 美元下行',       'P5_T1T3+POS+DXY'),
    # 出場優化
    ('J +DYNSTOP 動態停損',   'P5_T1T3+POS+DYNSTOP'),
    # 訊號子集
    ('K T1 only',             'P5_T1+POS'),
]
WINDOWS = [
    ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
    ('TRAIN (2020.1-2024.5)', '2020-01-02', '2024-05-31'),
    ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
]

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT',
    'EFA','AGG','LQD','HYG','IEF','SHY','BIL','GLD','SLV','USO',
    'UNG','UCO','SCO','BOIL','KOLD','UNL','IAU','PALL','PPLT',
    'DBA','DBC','GSG','DBO','DBE','EEM','EWJ','EWZ','EWY','FXI',
    'MCHI','INDA','EWG','EWU','EWC','EWA','EWT','EWS','EWH','EWP',
    'EWQ','EWI','EWN','EWL','EWO','XLK','XLF','XLV','XLE','XLY',
    'XLP','XLI','XLU','XLB','XLRE','XLC','XOP','XBI','XME','XHB',
    'XRT','XPH','XAR','XSD','XSW','XTL','SMH','SOXX','IBB','XHE',
    'SCHB','VGT','VHT','VFH','VIS','VDE','VNQ','VOX','VPU','VAW',
    'VCR','VDC','VYM','ARKK','ARKQ','ARKW','ARKG','ARKF','ARKX',
    'TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','SVXY','UVXY','VXX',
    'VIXY','NUGT','DUST','JNUG','JDST','GUSH','DRIP','LABU','LABD',
    'TMF','TMV','TNA','TZA','UDOW','SDOW','SPXL','SPXS','UWM','TWM',
    'URTY','SRTY','YINN','YANG','EDC','EDZ','BOND','RWM','SH','SDS',
    'SSO','QID','QLD','AGGY','SCHO','SCHR','SCHZ','VCIT','VCSH',
    'VCLT','MBB','MUB','HYS','JETS','MOON','JEPI','JEPQ','SCHD',
    'DIVO','VOOV','VOOG','SPLG','SPLV',
}


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, ticker, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, ticker, None)
        return (label, ticker, (r['pnl_pct'], r['n_trades'],
                                 r.get('win_rate', 0)))
    except Exception:
        return (label, ticker, None)


def metrics(returns):
    if not returns: return None
    pnls = [x[0] for x in returns if x is not None]
    arr = np.array(pnls)
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
    if full_path.exists():
        meta = json.loads(full_path.read_text(encoding='utf-8'))
        full_tickers = set(meta['tickers'])
        universe = sorted([t for t in full_tickers
                           if (DATA / f'{t}.parquet').exists()
                           and t not in US_ETF_EXCLUDE])
    else:
        universe = sorted([p.stem for p in DATA.glob('*.parquet')
                           if p.stem and p.stem.isalpha() and p.stem.isupper()
                           and p.stem not in US_ETF_EXCLUDE])
    print(f"美股 universe: {len(universe)} 檔\n")

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            for t in universe:
                all_tasks.append((t, mode, start, end, (var_name, win_name)))
    print(f"變體數 {len(VARIANTS)} × universe {len(universe)} × windows {len(WINDOWS)} "
          f"= {len(all_tasks)} 任務\n")

    t0 = time.time()
    bucket = {}
    per_ticker = {}
    n_done = 0
    milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                bucket.setdefault(label, []).append(ret)
                per_ticker.setdefault(label, {})[ticker] = ret[0]
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                print(f"  {pct:.0f}%", flush=True)

    print(f"\n完成 {time.time()-t0:.1f}s\n")

    # 對比表（以 TEST 為主，FULL/TRAIN 副）
    print("=" * 110)
    print("📊 美股 11 變體 RR 對比（baseline = A）")
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
        if not (f and tr and te):
            continue
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

    # TEST RR 排行
    print("\n" + "=" * 110)
    print("🏆 TEST RR 排行")
    print("=" * 110)
    rows.sort(key=lambda r: -r[3]['rr'])
    for i, (v, f, tr, te, d) in enumerate(rows, 1):
        bar = '█' * max(0, int(te['rr'] * 80))
        print(f"  {i:>2}. {v:<28} TEST RR {te['rr']:>+6.3f}  Δ {d:>+6.3f}  "
              f"win {te['win']:>4.1f}%  {bar[:50]}")

    # 寫詳細
    pt_serial = {f'{var}|{win}': per_ticker.get((var, win), {})
                 for var, _ in VARIANTS for win, _, _ in WINDOWS}
    out = {
        'universe_size': len(universe),
        'metrics': {f'{var}|{win}': metrics(bucket.get((var, win), []))
                    for var, _ in VARIANTS for win, _, _ in WINDOWS},
        'per_ticker': pt_serial,
    }
    with open('us_strategies_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n💾 寫入 us_strategies_results.json")


if __name__ == '__main__':
    main()
