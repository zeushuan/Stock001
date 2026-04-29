"""美股最終整合分析（一次跑完 5 件事）
=========================================
從 us_high_liquid 與 us_p10_optimize 的回測結果延伸：

1. Momentum 12-1 + v8 P10+ADX18 組合測試（取兩策略 OR / AND 的個股報酬）
2. Walk-Forward 跨年度穩健性（2020/21/22/23/24/25 RR）
3. 年化報酬對標 SPY 17.2%/年
4. 產業細分（按 ticker 對應 sector）
5. Winners / Losers top 20
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs

WORKERS = 16
MIN_ADV = 104_000_000

US_ETF_EXCLUDE = {
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


# ─────── Walk-Forward 年度回測 ───────
def annual_windows():
    """每年 1/2 ~ 12/31 一個窗"""
    return [(f'{y}', f'{y}-01-02', f'{y}-12-31') for y in range(2020, 2027)]


def run_one_variant(args):
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


# ─────── Momentum 12-1 計算（標單）───────
def momentum_12_1_signal(df, start, end):
    """每月最後一日：12-1 月報酬，>15% 進場、<0% 出場"""
    s = pd.Timestamp(start).tz_localize(None)
    e = pd.Timestamp(end).tz_localize(None)
    idx = df.index
    if hasattr(idx, 'tz') and idx.tz is not None:
        idx = idx.tz_localize(None)
    mask = (idx >= s) & (idx <= e)
    sub = df.iloc[mask]
    if len(sub) < 252: return []
    close = sub['Close']
    n = len(sub)
    trades = []
    in_mkt = False
    ep = 0.0
    last_check = -22
    for i in range(252, n):
        if i - last_check < 22: continue
        last_check = i
        ret_12_1 = (close.iloc[i-22] / close.iloc[i-252] - 1) * 100
        if not in_mkt:
            if ret_12_1 > 15:
                in_mkt = True
                ep = close.iloc[i]
        else:
            if ret_12_1 < 0:
                trades.append((close.iloc[i] - ep) / ep * 100)
                in_mkt = False
    if in_mkt:
        trades.append((close.iloc[-1] - ep) / ep * 100)
    return trades


def run_one_momentum(args):
    ticker, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, ticker, None)
        trades = momentum_12_1_signal(df, start, end)
        if not trades: return (label, ticker, None)
        return (label, ticker, sum(trades))
    except Exception:
        return (label, ticker, None)


def metrics(returns):
    if not returns: return None
    arr = np.array([x for x in returns if x is not None])
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0: return None
    return {
        'n': len(arr), 'mean': float(arr.mean()),
        'median': float(np.median(arr)),
        'win': float((arr > 0).mean() * 100),
        'worst': float(arr.min()),
        'best': float(arr.max()),
        'rr': float(arr.mean()/abs(arr.min())) if arr.min() < 0 else 0,
    }


def main():
    DATA = Path('data_cache')
    full_path = Path('us_full_tickers.json')
    meta = json.loads(full_path.read_text(encoding='utf-8'))
    full_tickers = set(meta['tickers'])

    # Sector 對應（從 us_full_tickers detail）
    name_map = {x['symbol']: x.get('name', '') for x in meta.get('detail', [])}

    print("📊 計算高流動 tier...")
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

    # ============ 任務 1：Walk-Forward 年度（v8 P10+POS+ADX18）============
    BEST_MODE = 'P10_T1T3+POS+ADX18'
    print("="*100)
    print(f"📊 任務 1+2: Walk-Forward 年度 + 對標 SPY (策略 {BEST_MODE})")
    print("="*100)
    annual = annual_windows()

    tasks = []
    for yr, s, e in annual:
        for t in high_liquid:
            tasks.append((t, BEST_MODE, s, e, yr))

    t0 = time.time()
    yearly = {}
    pt_yearly = {}
    n_done = 0
    milestone = max(1, len(tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one_variant, tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                yearly.setdefault(label, []).append(ret)
                pt_yearly.setdefault(label, {})[ticker] = ret
            if n_done % milestone == 0:
                print(f"  {n_done/len(tasks)*100:.0f}%", flush=True)
    print(f"完成 {time.time()-t0:.1f}s\n")

    # SPY 年度報酬
    def spy_annual(start, end):
        try:
            df = dl.load_from_cache('SPY')
            s = pd.Timestamp(start).tz_localize(None)
            e = pd.Timestamp(end).tz_localize(None)
            idx = df.index
            if hasattr(idx, 'tz') and idx.tz is not None:
                idx = idx.tz_localize(None)
            mask = (idx >= s) & (idx <= e)
            sub = df.iloc[mask]
            if len(sub) < 2: return None
            return (sub['Close'].iloc[-1] - sub['Close'].iloc[0]) / sub['Close'].iloc[0] * 100
        except: return None

    print(f"{'年度':<8} {'n':>5} {'勝率%':>7} {'均報%':>9} {'中位%':>8} "
          f"{'最差%':>9} {'RR':>7}  {'SPY%':>7}  v8 vs SPY")
    print("-" * 100)
    annual_summary = []
    for yr, s, e in annual:
        m = metrics(yearly.get(yr, []))
        spy_r = spy_annual(s, e)
        if not m: continue
        spy_str = f"{spy_r:>+6.1f}" if spy_r is not None else "—"
        delta = (m['mean'] - spy_r) if spy_r is not None else 0
        flag = '⭐ 勝' if delta > 0 else '✗ 輸' if spy_r is not None else ''
        print(f"{yr:<8} {m['n']:>5} {m['win']:>+7.1f} {m['mean']:>+9.1f} "
              f"{m['median']:>+8.1f} {m['worst']:>+9.1f} {m['rr']:>7.3f}  "
              f"{spy_str}  {delta:+6.1f}pp {flag}")
        annual_summary.append({'year': yr, 'metrics': m, 'spy': spy_r,
                               'delta_vs_spy': delta})

    # ============ 任務 3：Momentum 12-1 + v8 組合 ============
    print("\n" + "="*100)
    print("📊 任務 3: Momentum 12-1 + v8 P10+POS+ADX18 組合測試")
    print("="*100)

    # Momentum 12-1 跑高流動全期
    mom_tasks = [(t, '2020-01-02', '2026-04-25', 'momentum_full')
                 for t in high_liquid]
    mom_returns = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one_momentum, mom_tasks, chunksize=80):
            if ret is not None:
                mom_returns[ticker] = ret

    # v8 P10+POS+ADX18 跑高流動全期
    v8_tasks = [(t, BEST_MODE, '2020-01-02', '2026-04-25', 'v8_full')
                for t in high_liquid]
    v8_returns = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ticker, ret in ex.map(run_one_variant, v8_tasks, chunksize=80):
            if ret is not None:
                v8_returns[ticker] = ret

    # 組合：對每股，取 max(mom, v8)（若兩者皆有訊號）/ 均值（OR mode）
    common = set(mom_returns) & set(v8_returns)
    print(f"\n  Momentum 有訊號股: {len(mom_returns)}")
    print(f"  v8 P10 有訊號股:    {len(v8_returns)}")
    print(f"  兩者皆有訊號:       {len(common)} 檔\n")

    combine_max = []  # 取兩者 max（取最佳訊號）
    combine_mean = []  # 取兩者 mean（兩者皆要求進場）
    combine_or = []   # 取兩者 sum (兩者都進場)
    for t in common:
        combine_max.append(max(mom_returns[t], v8_returns[t]))
        combine_mean.append((mom_returns[t] + v8_returns[t]) / 2)
        combine_or.append(mom_returns[t] + v8_returns[t])

    print("各組合方式 FULL 6 年:")
    print(f"  v8 P10+ADX18 only :  {metrics(list(v8_returns.values()))}")
    print(f"  Momentum only     :  {metrics(list(mom_returns.values()))}")
    print(f"  COMBINE max       :  {metrics(combine_max)}")
    print(f"  COMBINE mean      :  {metrics(combine_mean)}")
    print(f"  COMBINE sum (兩倍部位): {metrics(combine_or)}")

    # ============ 任務 4：Winners / Losers Top 20（v8 P10+POS+ADX18 FULL）============
    print("\n" + "="*100)
    print("📊 任務 4: Winners / Losers Top 20（v8 P10+POS+ADX18 FULL 6年）")
    print("="*100)

    # 用 v8_returns
    sorted_winners = sorted(v8_returns.items(), key=lambda x: -x[1])[:20]
    sorted_losers = sorted(v8_returns.items(), key=lambda x: x[1])[:20]

    print(f"\n🏆 Top 20 Winners")
    print(f"{'#':<3} {'Ticker':<8} {'PnL%':>10}  {'公司名稱'}")
    print("-" * 100)
    for i, (t, r) in enumerate(sorted_winners, 1):
        nm = name_map.get(t, '')[:60]
        print(f"{i:<3} {t:<8} {r:>+10.1f}  {nm}")

    print(f"\n📉 Top 20 Losers")
    print(f"{'#':<3} {'Ticker':<8} {'PnL%':>10}  {'公司名稱'}")
    print("-" * 100)
    for i, (t, r) in enumerate(sorted_losers, 1):
        nm = name_map.get(t, '')[:60]
        print(f"{i:<3} {t:<8} {r:>+10.1f}  {nm}")

    # ============ 任務 5：產業 / Ticker prefix 分析 ============
    print("\n" + "="*100)
    print("📊 任務 5: 簡易產業分組（依公司名關鍵詞）")
    print("="*100)

    # 簡易分類（沒 sector 資料，先用名稱關鍵詞）
    sector_kw = {
        '半導體':    ['semiconductor', 'silicon', 'chips', 'micro', 'AMD', 'NVDA'],
        '生技醫療':  ['pharmaceuticals', 'biotech', 'therapeut', 'medical', 'health'],
        '金融':      ['bank', 'capital', 'financial', 'insurance', 'savings'],
        '能源':      ['energy', 'petroleum', 'oil', 'gas', 'resources'],
        '地產 REIT': ['realty', 'reit', 'properties', 'real estate'],
        '科技軟體':  ['software', 'cloud', 'systems', 'cyber', 'digital'],
        '消費品':    ['consumer', 'retail', 'restaurant', 'apparel', 'beverage'],
    }
    sector_returns = {k: [] for k in sector_kw}
    sector_returns['其他'] = []
    for t, r in v8_returns.items():
        nm = name_map.get(t, '').lower()
        matched = False
        for sec, kws in sector_kw.items():
            if any(kw.lower() in nm for kw in kws):
                sector_returns[sec].append(r)
                matched = True
                break
        if not matched:
            sector_returns['其他'].append(r)

    print(f"\n{'產業':<12} {'n':>5} {'勝率%':>7} {'均報%':>9} {'中位%':>8} {'RR':>7}")
    print("-" * 100)
    for sec, rets in sorted(sector_returns.items(),
                             key=lambda kv: -metrics(kv[1])['rr']
                             if metrics(kv[1]) else 0):
        m = metrics(rets)
        if not m: continue
        print(f"{sec:<12} {m['n']:>5} {m['win']:>+7.1f} {m['mean']:>+9.1f} "
              f"{m['median']:>+8.1f} {m['rr']:>7.3f}")

    # ============ 寫入結果 JSON ============
    out = {
        'best_mode': BEST_MODE,
        'tier': f'ADV ≥ ${MIN_ADV/1e6:.0f}M / {len(high_liquid)} 檔',
        'walkforward_annual': annual_summary,
        'combine_metrics': {
            'v8_only': metrics(list(v8_returns.values())),
            'momentum_only': metrics(list(mom_returns.values())),
            'combine_max': metrics(combine_max),
            'combine_mean': metrics(combine_mean),
        },
        'winners_top20': [(t, r) for t, r in sorted_winners],
        'losers_top20': [(t, r) for t, r in sorted_losers],
        'sector_metrics': {sec: metrics(rets) for sec, rets in sector_returns.items()
                          if metrics(rets)},
    }
    with open('us_final_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n💾 寫入 us_final_analysis.json")


if __name__ == '__main__':
    main()
