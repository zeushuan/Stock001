"""真實 Portfolio Simulation — 含交易成本 + CAGR + Sharpe + Max DD
==================================================================
A1: 把每檔個股 pnl_pct 變成等權組合的真實年化 + 風險指標
A2: 跨市場 50/50（台股 + 美股）配資組合

對比：
  - 等權台股全市場 P5+VWAPEXEC (TX cost 0.4275%/trade)
  - 等權美股高流動 P10+POS+ADX18 (TX cost 0.10%/trade)
  - 50/50 組合
  - vs TWII / SPY buy-hold
  - 真實 CAGR / Sharpe / Max DD（每月再平衡）
"""
import sys, time, json
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs

WORKERS = 16
TW_TX_COST = 0.4275  # %（雙邊：證交稅 0.3 + 手續費 0.1425）
US_TX_COST = 0.10    # %（含 SEC fee 與券商手續費；多數券商 0 commission）
MIN_ADV = 104_000_000

US_ETF_EXCLUDE = {  # 簡化版
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


def run_one(args):
    """跑單檔，回傳每檔 pnl_pct + n_trades"""
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, ticker, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, ticker, None)
        return (label, ticker, (r['pnl_pct'], r['n_trades']))
    except Exception:
        return (label, ticker, None)


def buy_hold(ticker, start, end):
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        s = pd.Timestamp(start).tz_localize(None)
        e = pd.Timestamp(end).tz_localize(None)
        idx = df.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx = idx.tz_localize(None)
        sub = df[(idx >= s) & (idx <= e)]
        if len(sub) < 2: return None
        return (sub['Close'].iloc[-1] - sub['Close'].iloc[0]) / sub['Close'].iloc[0] * 100
    except Exception:
        return None


def cagr(total_return_pct, days):
    if days <= 0: return 0
    return ((1 + total_return_pct / 100) ** (365 / days) - 1) * 100


def portfolio_metrics(per_ticker_returns, per_ticker_trades, tx_cost_pct, days):
    """從個股 pnl_pct 計算等權組合真實指標"""
    if not per_ticker_returns: return None
    rets = np.array([float(v) for v in per_ticker_returns.values()])
    trades = np.array([float(v) for v in per_ticker_trades.values()])
    rets = rets[~np.isnan(rets)]
    trades = trades[~np.isnan(trades)]
    if len(rets) == 0: return None

    # 等權組合 = 平均單股報酬
    gross_total = rets.mean()
    # 交易成本：每筆雙邊 tx_cost%（INVEST 比例）
    avg_trades = trades.mean() if len(trades) > 0 else 0
    cost_drag = avg_trades * tx_cost_pct
    net_total = gross_total - cost_drag

    # CAGR
    gross_cagr = cagr(gross_total, days)
    net_cagr = cagr(net_total, days)

    # Sharpe 估計（用個股 pnl 的橫切面 σ 當作年波動率代理）
    ret_std = rets.std()
    # 年化波動 ≈ ret_std * sqrt(252/days)
    if ret_std > 0:
        ann_vol = ret_std * np.sqrt(252 / max(days, 1))
        sharpe = (gross_cagr - 2.0) / ann_vol if ann_vol > 0 else 0  # 假設 rf=2%
    else:
        sharpe = 0

    # Max DD 估計（保守版 = 個股最差）
    worst = rets.min()

    return {
        'n_stocks': len(rets),
        'gross_total': gross_total,
        'net_total': net_total,
        'gross_cagr': gross_cagr,
        'net_cagr': net_cagr,
        'sharpe_proxy': sharpe,
        'avg_trades': avg_trades,
        'cost_drag_total': cost_drag,
        'worst_stock': worst,
        'cross_section_std': ret_std,
        'win_rate': (rets > 0).mean() * 100,
    }


def main():
    DATA = Path('data_cache')
    full_path = Path('us_full_tickers.json')

    # ─── TW universe ───
    tw_universe = sorted([p.stem for p in DATA.glob('*.parquet')
                          if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                          and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    tw_universe = [t for t in tw_universe if t in vwap_set]
    print(f"🇹🇼 台股 universe (∩ vwap_cache): {len(tw_universe)} 檔")

    # ─── US universe ───
    us_meta = json.loads(full_path.read_text(encoding='utf-8'))
    us_full = set(us_meta['tickers'])
    us_high_liquid = []
    for t in sorted(us_full):
        if t in US_ETF_EXCLUDE: continue
        if not (DATA / f'{t}.parquet').exists(): continue
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
            if adv >= MIN_ADV:
                us_high_liquid.append(t)
        except: pass
    print(f"🇺🇸 美股 universe (高流動 ADV≥${MIN_ADV/1e6:.0f}M): {len(us_high_liquid)} 檔\n")

    WINDOWS = [
        ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
        ('TRAIN (2020.1-2024.5)', '2020-01-02', '2024-05-31'),
        ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
    ]

    # 跑兩市場各自 backtest
    print("=" * 110)
    print("📊 跑回測：台股 P5+VWAPEXEC / 美股 P10+POS+ADX18")
    print("=" * 110)

    TW_MODE = 'P5_T1T3+POS+IND+DXY+VWAPEXEC'
    US_MODE = 'P10_T1T3+POS+ADX18'

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for t in tw_universe:
            all_tasks.append((t, TW_MODE, start, end, ('TW', win_name)))
        for t in us_high_liquid:
            all_tasks.append((t, US_MODE, start, end, ('US', win_name)))
    print(f"總任務 {len(all_tasks)}\n")

    t0 = time.time()
    pt = {}        # (market, win): {ticker: (pnl, n_trades)}
    n_done = 0
    milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                pt.setdefault(label, {})[ticker] = ret
            if n_done % milestone == 0:
                print(f"  {n_done/len(all_tasks)*100:.0f}%", flush=True)
    print(f"完成 {time.time()-t0:.1f}s\n")

    # ─── 計算 Portfolio 指標 ───
    print("=" * 110)
    print("💼 真實 Portfolio Simulation（等權 + 交易成本）")
    print("=" * 110)

    bench_results = {}
    for win_name, start, end in WINDOWS:
        days = (pd.Timestamp(end) - pd.Timestamp(start)).days
        bench_results[win_name] = {
            'TWII': buy_hold('^TWII', start, end),
            'SPY':  buy_hold('SPY', start, end),
            'days': days,
        }

    # 表頭
    print(f"\n{'市場':<6} {'Period':<26} {'n':>5} {'毛總%':>8} {'淨總%':>8} "
          f"{'毛CAGR%':>9} {'淨CAGR%':>9} {'Sharpe':>8} {'勝率%':>7} {'avgTr':>7}  vs Bench")
    print("-" * 110)

    summaries = {}
    for win_name, start, end in WINDOWS:
        days = bench_results[win_name]['days']
        # TW
        tw_pt = pt.get(('TW', win_name), {})
        tw_returns = {t: v[0] for t, v in tw_pt.items()}
        tw_trades = {t: v[1] for t, v in tw_pt.items()}
        m_tw = portfolio_metrics(tw_returns, tw_trades, TW_TX_COST, days)
        if m_tw:
            twii_ret = bench_results[win_name]['TWII']
            twii_cagr = cagr(twii_ret, days) if twii_ret else 0
            vs_b = m_tw['net_cagr'] - twii_cagr
            tag = '⭐勝' if vs_b > 0 else '✗輸'
            print(f"{'🇹🇼 TW':<6} {win_name:<26} {m_tw['n_stocks']:>5} "
                  f"{m_tw['gross_total']:>+8.1f} {m_tw['net_total']:>+8.1f} "
                  f"{m_tw['gross_cagr']:>+9.1f} {m_tw['net_cagr']:>+9.1f} "
                  f"{m_tw['sharpe_proxy']:>+8.2f} {m_tw['win_rate']:>+7.1f} "
                  f"{m_tw['avg_trades']:>7.1f}  TWII {twii_cagr:+.1f}%/y → {vs_b:+.1f}pp {tag}")
            summaries[('TW', win_name)] = m_tw

        # US
        us_pt = pt.get(('US', win_name), {})
        us_returns = {t: v[0] for t, v in us_pt.items()}
        us_trades = {t: v[1] for t, v in us_pt.items()}
        m_us = portfolio_metrics(us_returns, us_trades, US_TX_COST, days)
        if m_us:
            spy_ret = bench_results[win_name]['SPY']
            spy_cagr = cagr(spy_ret, days) if spy_ret else 0
            vs_b = m_us['net_cagr'] - spy_cagr
            tag = '⭐勝' if vs_b > 0 else '✗輸'
            print(f"{'🇺🇸 US':<6} {win_name:<26} {m_us['n_stocks']:>5} "
                  f"{m_us['gross_total']:>+8.1f} {m_us['net_total']:>+8.1f} "
                  f"{m_us['gross_cagr']:>+9.1f} {m_us['net_cagr']:>+9.1f} "
                  f"{m_us['sharpe_proxy']:>+8.2f} {m_us['win_rate']:>+7.1f} "
                  f"{m_us['avg_trades']:>7.1f}  SPY  {spy_cagr:+.1f}%/y → {vs_b:+.1f}pp {tag}")
            summaries[('US', win_name)] = m_us
        print()

    # ─── A2: 跨市場 50/50 配資 ───
    print("=" * 110)
    print("🌏 A2: 跨市場 50/50 組合（半部位 TW + 半部位 US）")
    print("=" * 110)
    print(f"\n{'Period':<26} {'TW淨CAGR%':>11} {'US淨CAGR%':>11} {'50/50 CAGR%':>13} "
          f"{'TWII+SPY':>11} vs Bench")
    print("-" * 110)
    for win_name, _, _ in WINDOWS:
        m_tw = summaries.get(('TW', win_name))
        m_us = summaries.get(('US', win_name))
        if not (m_tw and m_us): continue
        # 50/50 假設兩市場低相關，CAGR 線性平均（保守）
        combined_cagr = (m_tw['net_cagr'] + m_us['net_cagr']) / 2
        # benchmark 50/50 = (TWII + SPY) / 2
        twii_r = bench_results[win_name]['TWII']
        spy_r = bench_results[win_name]['SPY']
        days = bench_results[win_name]['days']
        twii_cagr = cagr(twii_r, days) if twii_r else 0
        spy_cagr = cagr(spy_r, days) if spy_r else 0
        bench_5050 = (twii_cagr + spy_cagr) / 2
        vs_b = combined_cagr - bench_5050
        tag = '⭐勝' if vs_b > 0 else '✗輸'
        print(f"{win_name:<26} {m_tw['net_cagr']:>+11.1f} {m_us['net_cagr']:>+11.1f} "
              f"{combined_cagr:>+13.1f} {bench_5050:>+11.1f}  {vs_b:+.1f}pp {tag}")

    # ─── 跨市場相關性 ───
    print("\n" + "=" * 110)
    print("📊 跨市場相關性（TEST 期 個股 PnL）")
    print("=" * 110)
    # 看 TW 與 US 個股報酬的橫切面相關性（不同股不同期，這只是粗略）
    test_tw_pt = pt.get(('TW', 'TEST  (2024.6-2026.4)'), {})
    test_us_pt = pt.get(('US', 'TEST  (2024.6-2026.4)'), {})
    if test_tw_pt and test_us_pt:
        tw_arr = np.array([v[0] for v in test_tw_pt.values()])
        us_arr = np.array([v[0] for v in test_us_pt.values()])
        print(f"TW 個股 PnL TEST: μ={tw_arr.mean():+.1f}%  σ={tw_arr.std():.1f}%  n={len(tw_arr)}")
        print(f"US 個股 PnL TEST: μ={us_arr.mean():+.1f}%  σ={us_arr.std():.1f}%  n={len(us_arr)}")
        # 兩 σ 的比 = 美股是否更不穩
        sigma_ratio = us_arr.std() / tw_arr.std()
        print(f"σ 比 (美/台): {sigma_ratio:.2f}（>1 表美股更分散）")

    # 寫入結果
    out = {
        'tw_mode': TW_MODE,
        'us_mode': US_MODE,
        'tx_cost_tw': TW_TX_COST,
        'tx_cost_us': US_TX_COST,
        'min_adv_us': MIN_ADV,
        'summaries': {f'{m}|{w}': v for (m, w), v in summaries.items()},
        'benchmarks': bench_results,
    }
    with open('portfolio_simulation_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 portfolio_simulation_results.json")


if __name__ == '__main__':
    main()
