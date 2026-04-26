"""
Test the new T3 filter (EMA120 rising vs 60 days ago) on key stocks.
Compare current version vs v3 baseline from CSV.
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import csv, numpy as np

bt.START = '2020-01-02'
bt.END   = '2026-04-25'

v3_map = {}
try:
    with open('tw_all_results_20260425.csv', encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            v3_map[r['ticker']] = (float(r['ret_t7']), r['name'])
except: pass

print(f"{'代號':<8}  {'名稱':<10}  {'BH%':>8}  {'v6%':>8}  {'new%':>8}  {'v6→new':>8}  trades")
print("-" * 78)

groups = [
    ("【T3死亡迴圈：預期改善】",
     ['2939','2498','5203','3702','2404']),
    ("【大型穩健股：必須不變差】",
     ['2317','2330','2454','2382','6669']),
    ("【BH 500%+ 超大趨勢股】",
     ['2368','3017','6442','2383','6139','3661','3443','6531']),
    ("【v3超越BH的好股票：不應變差】",
     ['4961','2609','5608','3035','4989','6443','2485','3669']),
]

for label, tickers in groups:
    print(f'\n{label}')
    new_vals, v6_vals = [], []
    for ticker in tickers:
        r = bt.analyze(ticker)
        if r:
            bh  = r['bh_pnl'] / bt.INVEST * 100
            new = r['pnl7']   / bt.INVEST * 100
            nt  = len(r['t7'])
            v6, nm = v3_map.get(ticker, (float('nan'), ticker))
            diff = new - v6 if v6 == v6 else float('nan')
            diff_s = f'{diff:>+8.1f}%' if diff == diff else '       N/A'
            v6_s   = f'{v6:>+8.1f}%'   if v6 == v6   else '       N/A'
            flag = ' !!BAD!!' if (diff == diff and diff < -5) else (' ✓+' if (diff == diff and diff > 5) else '')
            print(f'  {ticker:<8}  {nm[:8]:<10}  {bh:>+8.1f}%  {v6_s}  {new:>+8.1f}%  {diff_s}  {nt}{flag}')
            new_vals.append(new)
            if v6 == v6: v6_vals.append(v6)
        else:
            print(f'  {ticker:<8}: 無資料')
    if new_vals:
        print(f'  {"平均":<18}  {"":>8}  {np.mean(v6_vals):>+8.1f}%  {np.mean(new_vals):>+8.1f}%  {np.mean(new_vals)-np.mean(v6_vals):>+8.1f}%')
