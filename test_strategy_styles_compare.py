"""6 個策略風格 × TW + US 雙市場績效重新驗證
=================================================
tv_app 側邊欄目前 6 個風格，數字是早期 TW 研究：
  🛡️ 極致風控 (IND+DXY)         mean=122% sharpe=1.03
  🛟 超低風險 (五重保護)         mean=83%  sharpe=0.94
  🌊 保守 (POS+DXY)              mean=121% sharpe=0.99
  ⚖️ 平衡 (POS)                  mean=142% sharpe=0.85
  🤖 RL 智能加碼                 mean=153% sharpe=0.67
  🚀 進攻 (P0_T1T3)              mean=197% sharpe=0.68

問題：
  1. 沒分 TW vs US（IND/DXY 是 TW 跨市場資料，US 沒）
  2. 沒納入 VWAPEXEC（TW 真正最佳）
  3. 沒納入 P10+POS+ADX18（US 最佳）

加入兩個「最佳組合」作對比基準。
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
MIN_ADV = 104_000_000

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
}

# 6 個 tv_app 風格 + 2 個最佳基準
STYLES = [
    # tv_app 側邊欄 6 個
    ('🚀 進攻 (P0_T1T3)',          'P0_T1T3'),
    ('⚖️ 平衡 (POS)',               'P0_T1T3+POS'),
    ('🌊 保守 (POS+DXY)',          'P0_T1T3+POS+DXY'),
    ('🛡️ 極致風控 (IND+DXY)',     'P0_T1T3+POS+IND+DXY'),
    ('🛟 超低風險 (五重保護)',      'P0_T1T3+POS+IND+DXY+WRSI+WADX'),
    ('🤖 RL 智能加碼',              'P0_T1T3+RL'),
    # 真正最佳（基準）
    ('⭐ TW 最佳 P5+VWAPEXEC',     'P5_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('⭐ US 最佳 P10+POS+ADX18',   'P10_T1T3+POS+ADX18'),
]
WINDOWS = [
    ('FULL  (2020.1-2026.4)', '2020-01-02', '2026-04-25'),
    ('TEST  (2024.6-2026.4)', '2024-06-01', '2026-04-25'),
]


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, None)
        return (label, r['pnl_pct'])
    except Exception:
        return (label, None)


def metrics(arr):
    if not arr: return None
    a = np.array([x for x in arr if x is not None])
    a = a[~np.isnan(a)]
    if len(a) == 0: return None
    return {
        'n': len(a), 'mean': float(a.mean()),
        'median': float(np.median(a)),
        'win': float((a > 0).mean() * 100),
        'worst': float(a.min()),
        'rr': float(a.mean() / abs(a.min())) if a.min() < 0 else 0,
    }


def main():
    DATA = Path('data_cache')
    tw_universe = sorted([p.stem for p in DATA.glob('*.parquet')
                          if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                          and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    tw_universe = [t for t in tw_universe if t in vwap_set]

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
    print(f"🇺🇸 US 高流動: {len(us_high_liquid)} 檔\n")

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for style_name, mode in STYLES:
            for t in tw_universe:
                all_tasks.append((t, mode, start, end, ('TW', style_name, win_name)))
            for t in us_high_liquid:
                all_tasks.append((t, mode, start, end, ('US', style_name, win_name)))

    print(f"總任務 {len(all_tasks)}\n")
    t0 = time.time()
    bucket = {}
    n_done = 0
    milestone = max(1, len(all_tasks) // 20)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, ret in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if ret is not None:
                bucket.setdefault(label, []).append(ret)
            if n_done % milestone == 0:
                print(f"  {n_done/len(all_tasks)*100:.0f}%", flush=True)
    print(f"\n完成 {time.time()-t0:.1f}s\n")

    # 報告：兩市場分別列表
    for market in ['TW', 'US']:
        market_label = '🇹🇼 TW (1058 檔)' if market == 'TW' else '🇺🇸 US 高流動 (555 檔)'
        print("=" * 110)
        print(f"📊 {market_label} — 8 個策略風格雙期績效")
        print("=" * 110)
        for win_name, _, _ in WINDOWS:
            print(f"\n【{win_name}】")
            print(f"{'風格':<32} {'樣本':>5} {'勝率%':>7} {'均報%':>9} "
                  f"{'中位%':>8} {'最差%':>9} {'RR':>7}")
            print("-" * 110)
            rows = []
            for style_name, _ in STYLES:
                m = metrics(bucket.get((market, style_name, win_name), []))
                if m: rows.append((style_name, m))
            # 按 RR 排序
            rows.sort(key=lambda x: -x[1]['rr'])
            for style, m in rows:
                marker = ''
                if '最佳' in style: marker = ' 🏆'
                print(f"{style:<32} {m['n']:>5} {m['win']:>+7.1f} "
                      f"{m['mean']:>+9.1f} {m['median']:>+8.1f} "
                      f"{m['worst']:>+9.1f} {m['rr']:>7.3f}{marker}")
        print()

    # 對比表：每個風格 TW vs US TEST RR
    print("=" * 110)
    print("🌏 跨市場對比（TEST RR）")
    print("=" * 110)
    print(f"{'風格':<32} {'TW RR':>8} {'US RR':>8} {'TW 勝率':>8} {'US 勝率':>8}  雙市場行為")
    print("-" * 110)
    for style_name, _ in STYLES:
        tw_m = metrics(bucket.get(('TW', style_name, 'TEST  (2024.6-2026.4)'), []))
        us_m = metrics(bucket.get(('US', style_name, 'TEST  (2024.6-2026.4)'), []))
        if tw_m and us_m:
            tw_rr, us_rr = tw_m['rr'], us_m['rr']
            tw_win, us_win = tw_m['win'], us_m['win']
            # 行為標籤
            if tw_rr > 0.4 and us_rr > 0.3:
                behavior = '✅ 兩市場都好'
            elif tw_rr > us_rr + 0.2:
                behavior = '🇹🇼 TW 顯著好'
            elif us_rr > tw_rr + 0.2:
                behavior = '🇺🇸 US 顯著好'
            elif tw_rr > 0.4:
                behavior = '🇹🇼 TW 適合'
            elif us_rr > 0.3:
                behavior = '🇺🇸 US 適合'
            else:
                behavior = '⚠️ 兩市場都弱'
            print(f"{style_name:<32} {tw_rr:>+8.3f} {us_rr:>+8.3f} "
                  f"{tw_win:>+7.1f}% {us_win:>+7.1f}%  {behavior}")

    # 寫 JSON
    out = {f'{m}|{s}|{w}': metrics(bucket.get((m, s, w), []))
           for m in ['TW', 'US']
           for s, _ in STYLES
           for w, _, _ in WINDOWS}
    with open('strategy_styles_compare.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("\n💾 寫入 strategy_styles_compare.json")


if __name__ == '__main__':
    main()
