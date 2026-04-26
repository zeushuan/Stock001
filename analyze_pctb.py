"""
分析 %b 在進場/出場時的分佈
觀察：
  1. 好股票的 T3 進場時 %b 大概在哪裡？
  2. 壞股票的 T3 進場時 %b 大概在哪裡？
  3. 獲利出場 vs 停損出場 時 %b 有差異嗎？
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import numpy as np
from ta.volatility import BollingerBands

bt.START = '2020-01-02'
bt.END   = '2026-04-25'
bt.INVEST = 100_000

BB_WINDOW = 20   # 標準布林帶

def get_pctb(df, window=BB_WINDOW):
    bb = BollingerBands(close=df['Close'], window=window, window_dev=2)
    return bb.bollinger_pband().values   # %b

groups = [
    ("【好股票 - 大趨勢】",        ['6139','4961','2454','2609','3017','6442']),
    ("【好股票 - 穩健型】",        ['2317','2330','2382','3035','6443']),
    ("【壞股票 - T3死亡迴圈】",    ['2939','2498','5203','1732','4133']),
]

print(f"{'代號':<8} {'名稱':<10} {'v7%':>8}  {'進場%b':>8}  {'T3進場%b':>9}  {'獲利出場%b':>11}  {'停損出場%b':>11}  trades")
print('-'*90)

all_entry_pctb   = []
all_t3_pctb      = []
all_profit_exit  = []
all_loss_exit    = []

for label, tickers in groups:
    print(f'\n{label}')
    grp_entry, grp_t3, grp_profit, grp_loss = [], [], [], []
    for ticker in tickers:
        r = bt.analyze(ticker)
        if not r: continue

        df = bt.download(ticker)
        df = bt.calc_ind(df)
        df = df[df.index >= bt.START]
        pctb = get_pctb(df)
        dates = df.index.tolist()
        date_idx = {d: i for i, d in enumerate(dates)}

        v7 = r['pnl7'] / bt.INVEST * 100
        entry_pbs, t3_pbs, profit_pbs, loss_pbs = [], [], [], []

        for t in r['t7']:
            ed = t['ed']
            xd = t['xd']
            ret = (t['xp'] - t['ep']) / t['ep'] * 100

            ei = date_idx.get(ed)
            xi = date_idx.get(xd)
            if ei is None or xi is None: continue

            pb_entry = pctb[ei]
            pb_exit  = pctb[xi]
            if np.isnan(pb_entry) or np.isnan(pb_exit): continue

            entry_pbs.append(pb_entry)

            # 判斷是否為 T3（非黃金交叉）
            e20 = df['e20'].values
            e60 = df['e60'].values
            if ei >= 1 and not any(np.isnan([e20[ei-1], e60[ei-1], e20[ei], e60[ei]])):
                is_t1 = (e20[ei-1] <= e60[ei-1] and e20[ei] > e60[ei])
            else:
                is_t1 = False
            if not is_t1:
                t3_pbs.append(pb_entry)

            if ret > 0:
                profit_pbs.append(pb_exit)
            else:
                loss_pbs.append(pb_exit)

        def avg(lst): return np.mean(lst) if lst else float('nan')
        def fmt(v): return f'{v:>8.2f}' if not np.isnan(v) else '     N/A'

        print(f"  {ticker:<8} {'':10} {v7:>+8.1f}%  {fmt(avg(entry_pbs))}  "
              f"{fmt(avg(t3_pbs))}   {fmt(avg(profit_pbs))}   {fmt(avg(loss_pbs))}  "
              f"{len(r['t7'])}")

        grp_entry  += entry_pbs
        grp_t3     += t3_pbs
        grp_profit += profit_pbs
        grp_loss   += loss_pbs
        all_entry_pctb  += entry_pbs
        all_t3_pctb     += t3_pbs
        all_profit_exit += profit_pbs
        all_loss_exit   += loss_pbs

    if grp_entry:
        def fmt(v): return f'{v:>8.2f}' if not np.isnan(v) else '     N/A'
        print(f"  {'群組平均':<18}         "
              f"{fmt(np.mean(grp_entry))}  {fmt(np.mean(grp_t3) if grp_t3 else float('nan'))}   "
              f"{fmt(np.mean(grp_profit) if grp_profit else float('nan'))}   "
              f"{fmt(np.mean(grp_loss) if grp_loss else float('nan'))}")

print('\n' + '='*90)
print('全體統計：')
print(f"  所有進場時 %b 均值:     {np.mean(all_entry_pctb):.3f}   (n={len(all_entry_pctb)})")
print(f"  T3進場時  %b 均值:     {np.mean(all_t3_pctb):.3f}   (n={len(all_t3_pctb)})")
print(f"  獲利出場時 %b 均值:     {np.mean(all_profit_exit):.3f}   (n={len(all_profit_exit)})")
print(f"  停損出場時 %b 均值:     {np.mean(all_loss_exit):.3f}   (n={len(all_loss_exit)})")

# 分佈直方圖（文字版）
def hist_txt(data, label, bins=10):
    if not data: return
    d = [x for x in data if not np.isnan(x)]
    mn, mx = min(d), max(d)
    w = (mx - mn) / bins
    counts = [0]*bins
    for v in d:
        b = min(int((v - mn) / w), bins-1)
        counts[b] += 1
    print(f'\n  {label} 分佈 (n={len(d)}, min={mn:.2f}, max={mx:.2f}):')
    for i, c in enumerate(counts):
        lo = mn + i*w
        bar = '█' * int(c / max(counts) * 30)
        print(f'    {lo:>5.2f}~{lo+w:.2f}  {bar:<30}  {c}')

hist_txt(all_t3_pctb,      'T3進場 %b')
hist_txt(all_profit_exit,  '獲利出場 %b')
hist_txt(all_loss_exit,    '停損出場 %b')
