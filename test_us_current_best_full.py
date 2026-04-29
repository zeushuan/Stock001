"""美股全市場 v8 整體操作績效（2020-至今三段，5629 檔目標）
================================================================
US 沒有 VWAPEXEC（無 Fugle 5-min bar），最接近 v8 = P5_T1T3+POS

A baseline                 P0_T1T3
B +POS                     P0_T1T3+POS
C P5+POS ⭐ 美股最佳        P5_T1T3+POS

進場：T1 黃金交叉 / T3 拉回 / T4 反彈
出場：RSI>70 / EMA 死叉 / ATR×2.5 停損
資料：us_full_tickers.json（NYSE+NASDAQ 全部）∩ data_cache

額外輸出：
  - 流動性分位（高/中/低 ADV）的 RR 對比（類似台股 TOP 200 tier）
  - 各 tier 的勝率分佈
  - JSON 詳細結果存檔，供後續分析
"""
import sys, time
from pathlib import Path
import numpy as np
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs

WORKERS = 16

VARIANTS = [
    ('A baseline (P0)',    'P0_T1T3'),
    ('B +POS',             'P0_T1T3+POS'),
    ('C P5+POS ⭐ 美股最佳', 'P5_T1T3+POS'),
]
WINDOWS = [
    ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
    ('TRAIN (2020.1-2024.5)', '2020-01-02', '2024-05-31'),
    ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
]

# 排除 US ETF（同 update_us_signals.py 列表）
US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT',
    'EFA','AGG','LQD','HYG','IEF','SHY','BIL',
    'GLD','SLV','USO','UNG','UCO','SCO','BOIL','KOLD','UNL',
    'IAU','PALL','PPLT','DBA','DBC','GSG','DBO','DBE',
    'EEM','EWJ','EWZ','EWY','FXI','MCHI','INDA','EWG','EWU','EWC',
    'EWA','EWT','EWS','EWH','EWP','EWQ','EWI','EWN','EWL','EWO',
    'XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLRE','XLC',
    'XOP','XBI','XME','XHB','XRT','XPH','XAR','XSD','XSW','XTL',
    'SMH','SOXX','IBB','XHE','SCHB','VGT','VHT','VFH','VIS','VDE',
    'VNQ','VOX','VPU','VAW','VCR','VDC','VYM',
    'ARKK','ARKQ','ARKW','ARKG','ARKF','ARKX',
    'TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','SVXY','UVXY','VXX','VIXY',
    'NUGT','DUST','JNUG','JDST','GUSH','DRIP','LABU','LABD','TMF','TMV',
    'TNA','TZA','UDOW','SDOW','SPXL','SPXS','UWM','TWM','URTY','SRTY',
    'YINN','YANG','EDC','EDZ','BOND','RWM','SH','SDS','SSO','QID','QLD',
    'AGGY','SCHO','SCHR','SCHZ','VCIT','VCSH','VCLT','MBB','MUB','HYS',
    'JETS','MOON','JEPI','JEPQ','SCHD','DIVO','VOOV','VOOG','SPLG','SPLV',
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
                                 r.get('win_rate', 0),
                                 r.get('pnl_pct_net', r['pnl_pct'])))
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
        'best': arr.max(), 'std': arr.std(),
        'rr': arr.mean()/abs(arr.min()) if arr.min() < 0 else 0,
    }


def main():
    DATA = Path('data_cache')

    # 優先讀 us_full_tickers.json（NYSE+NASDAQ 完整清單），fallback 到 glob
    import json as _json
    full_path = Path('us_full_tickers.json')
    if full_path.exists():
        meta = _json.loads(full_path.read_text(encoding='utf-8'))
        full_tickers = set(meta['tickers'])
        # 過濾 data_cache 實際存在的
        universe = sorted([t for t in full_tickers
                           if (DATA / f'{t}.parquet').exists()
                           and t not in US_ETF_EXCLUDE])
        print(f"美股全市場 (us_full_tickers ∩ data_cache)：{len(universe)} 檔")
        print(f"  全清單共 {len(full_tickers)} 檔，其中 {len(full_tickers)-len(universe)} 檔尚未抓取")
    else:
        universe = sorted([p.stem for p in DATA.glob('*.parquet')
                           if p.stem and p.stem.isalpha() and p.stem.isupper()
                           and p.stem not in US_ETF_EXCLUDE])
        print(f"美股純股票（fallback glob）：{len(universe)} 檔（排除 ETF/反向）")
    print()

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            for t in universe:
                all_tasks.append((t, mode, start, end, (var_name, win_name)))
    print(f"總任務：{len(all_tasks)}\n")

    t0 = time.time()
    bucket = {}            # (var, win) → [pnl_tuple, ...]   for aggregate
    per_ticker = {}        # (var, win) → {ticker: pnl}      for tier
    n_done = 0
    milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                bucket.setdefault(label, []).append(ret)
                per_ticker.setdefault(label, {})[ticker] = ret[0]  # pnl_pct
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                print(f"  {pct:.0f}%", flush=True)

    print(f"\n完成 {time.time()-t0:.1f}s\n")

    print("=" * 100)
    print("🇺🇸 美股 v8 整體操作績效（按實際進出場規則回測）")
    print("=" * 100)

    for var_name, _ in VARIANTS:
        print(f"\n【{var_name}】")
        print(f"{'Period':<26} {'n':>5} {'勝率%':>7} {'均報%':>9} "
              f"{'中位%':>8} {'最差%':>8} {'最佳%':>10} {'σ':>7} {'RR':>7}")
        print("-" * 100)
        for win_name, _, _ in WINDOWS:
            m = metrics(bucket.get((var_name, win_name), []))
            if m:
                print(f"{win_name:<26} {m['n']:>5} {m['win']:>+7.1f} "
                      f"{m['mean']:>+9.1f} {m['median']:>+8.1f} "
                      f"{m['worst']:>+8.1f} {m['best']:>+10.1f} "
                      f"{m['std']:>7.0f} {m['rr']:>7.3f}")

    # 對比表
    print("\n" + "=" * 100)
    print("📊 三變體 RR 對比")
    print("=" * 100)
    print(f"{'Period':<26} {'A baseline':>12} {'B +POS':>12} {'C P5+POS':>15}  Δ(C-A)")
    print("-" * 100)
    for win_name, _, _ in WINDOWS:
        a = metrics(bucket.get(('A baseline (P0)', win_name), []))
        b = metrics(bucket.get(('B +POS', win_name), []))
        c = metrics(bucket.get(('C P5+POS ⭐ 美股最佳', win_name), []))
        if a and b and c:
            d = c['rr'] - a['rr']
            print(f"{win_name:<26} {a['rr']:>12.3f} {b['rr']:>12.3f} "
                  f"{c['rr']:>15.3f}  {d:>+6.3f}")

    # 跨市場對比（vs 台股 P5+VWAPEXEC TEST 0.611）
    print("\n" + "=" * 100)
    print("🌏 跨市場對比（TEST 22 月 out-of-sample）")
    print("=" * 100)
    c_test = metrics(bucket.get(('C P5+POS ⭐ 美股最佳', 'TEST  (2024.6-2026.4)'), []))
    if c_test:
        print(f"  🇺🇸 美股 P5+POS（無 VWAPEXEC）: RR {c_test['rr']:.3f} / "
              f"勝率 {c_test['win']:.1f}% / 中位 {c_test['median']:+.1f}%")
    print(f"  🇹🇼 台股 P5+VWAPEXEC          : RR 0.611 / 勝率 56.2% / 中位 +2.3%")

    # 🆕 流動性 tier 分析（基於 ADV / market cap）
    print("\n" + "=" * 100)
    print("💧 流動性 tier 分析（TEST 期，C P5+POS）")
    print("=" * 100)
    print("  以 data_cache 中個股最後 60 日均成交額分位：")
    print("   高（≥ p75）/ 中（p25-p75）/ 低（< p25）")

    # 按 ticker 算最後 60 日 ADV（成交額 USD）
    import data_loader as dl
    adv_map = {}
    for t in universe:
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            tail = df.tail(60)
            adv = (tail['Close'] * tail['Volume']).mean()
            if adv > 0: adv_map[t] = adv
        except: pass

    if not adv_map:
        print("  (ADV 計算失敗)")
    else:
        adv_arr = np.array(sorted(adv_map.values()))
        p25 = np.quantile(adv_arr, 0.25)
        p75 = np.quantile(adv_arr, 0.75)
        print(f"  ADV 分位：p25 = ${p25:,.0f} / p75 = ${p75:,.0f}")

        # 用 per_ticker 對 C P5+POS TEST 期分組
        c_test_label = ('C P5+POS ⭐ 美股最佳', 'TEST  (2024.6-2026.4)')
        c_test_pt = per_ticker.get(c_test_label, {})

        def tier_metrics(tier_set, name):
            arr = np.array([v for t, v in c_test_pt.items() if t in tier_set])
            arr = arr[~np.isnan(arr)]
            if len(arr) == 0:
                print(f"  {name}: (無資料)")
                return
            rr = arr.mean()/abs(arr.min()) if arr.min() < 0 else 0
            win = (arr > 0).mean() * 100
            print(f"  {name}: n={len(arr):>4}  勝率 {win:>5.1f}%  "
                  f"均報 {arr.mean():>+7.1f}%  中位 {np.median(arr):>+6.1f}%  "
                  f"最差 {arr.min():>+7.1f}%  RR {rr:.3f}")

        high_t = set(t for t, v in adv_map.items() if v >= p75)
        mid_t  = set(t for t, v in adv_map.items() if p25 <= v < p75)
        low_t  = set(t for t, v in adv_map.items() if v < p25)
        print(f"\n  TEST 期 C P5+POS 分 tier 表現：")
        tier_metrics(high_t, "高流動 (ADV ≥ p75)")
        tier_metrics(mid_t,  "中流動 (p25-p75) ")
        tier_metrics(low_t,  "低流動 (< p25)   ")

    # 寫詳細 JSON 給後續 tier 分析用
    # per_ticker 結構：{var|win: {ticker: pnl_pct}} 用於畫 winners/losers
    pt_serial = {f'{var}|{win}': per_ticker.get((var, win), {})
                 for var, _ in VARIANTS for win, _, _ in WINDOWS}
    out = {
        'universe_size': len(universe),
        'variants': [v[0] for v in VARIANTS],
        'windows':  [w[0] for w in WINDOWS],
        'metrics':  {f'{var}|{win}': metrics(bucket.get((var, win), []))
                     for var, _ in VARIANTS for win, _, _ in WINDOWS},
        'adv_quantiles': {'p25': float(p25), 'p75': float(p75)} if adv_map else None,
        'per_ticker': pt_serial,
    }
    with open('us_full_results.json', 'w', encoding='utf-8') as f:
        import json as _j
        _j.dump(out, f, indent=2, default=str)
    print(f"\n💾 寫入 us_full_results.json")


if __name__ == '__main__':
    main()
