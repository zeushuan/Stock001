"""
深度分析：進場前後的 %b 歷史模式

布林通道核心理念：
  - %b 不是點，是軌跡（trajectory）
  - 「走上軌」vs「到達上軌後反轉」是完全不同的意義
  - 進場前的 %b 歷史路徑，決定這是「真反彈」還是「死貓」

觀察指標：
  pctb_hi20   進場前20日內 %b 最高值（確認曾有強勢）
  pctb_lo10   進場前10日內 %b 最低值（確認真正回檔）
  pctb_ma20   進場前20日 %b 平均（整體趨勢位置）
  pctb_slope  進場前10日 %b 斜率（方向）
  pctb_walk   進場前30日中，%b > 0.7 的天數（走上軌強度）
  pctb_bear   進場前30日中，%b < 0.2 的天數（下軌停留）

出場模式：
  xpctb       出場當天 %b
  xpctb_hi5   出場前5日內 %b 最高值
  持倉期間 %b 最高值
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import numpy as np
from ta.volatility import BollingerBands

bt.START = '2020-01-02'
bt.END   = '2026-04-25'
bt.INVEST = 100_000

def get_pctb(df, window=20):
    return BollingerBands(close=df['Close'], window=window, window_dev=2).bollinger_pband().values

def analyze_pattern(ticker, label=''):
    r = bt.analyze(ticker)
    if not r: return

    df = bt.download(ticker)
    df = bt.calc_ind(df)
    df = df[df.index >= bt.START].copy()
    pctb  = get_pctb(df)
    dates = df.index.tolist()
    pr    = df['Close'].values
    e20   = df['e20'].values
    e60   = df['e60'].values
    date_idx = {d: i for i, d in enumerate(dates)}

    bh  = r['bh_pnl'] / bt.INVEST * 100
    v7  = r['pnl7']   / bt.INVEST * 100
    print(f"\n{'='*70}")
    print(f"  {ticker} {label}   BH={bh:+.1f}%  v7={v7:+.1f}%  trades={len(r['t7'])}")
    print(f"  {'日期':10} {'進出':4} {'ret%':>6}  "
          f"{'hi20':>5} {'lo10':>5} {'ma20':>5} {'walk':>4} {'bear':>4}  "
          f"{'當天%b':>6} {'入場型':6}")

    win_patterns  = []
    loss_patterns = []

    for t in sorted(r['t7'], key=lambda x: x['ed']):
        ei = date_idx.get(t['ed'])
        xi = date_idx.get(t['xd'])
        if ei is None or xi is None or ei < 30: continue

        ret = (t['xp'] - t['ep']) / t['ep'] * 100

        # 進場前歷史模式
        w20 = pctb[ei-20:ei]
        w10 = pctb[ei-10:ei]
        w30 = pctb[ei-30:ei]

        hi20  = np.nanmax(w20)   if len(w20) else np.nan
        lo10  = np.nanmin(w10)   if len(w10) else np.nan
        ma20  = np.nanmean(w20)  if len(w20) else np.nan
        walk  = int(np.nansum(np.array(w30) > 0.7))   # 走上軌天數
        bear  = int(np.nansum(np.array(w30) < 0.2))   # 下軌停留天數
        pb_now = pctb[ei] if not np.isnan(pctb[ei]) else np.nan

        # 判斷入場型態
        # 斜率：10日內 %b 是上升還是下降
        slope = (np.nanmean(pctb[ei-3:ei]) - np.nanmean(pctb[ei-10:ei-7])) if ei >= 10 else np.nan

        if not any(np.isnan([e20[ei-1], e60[ei-1], e20[ei], e60[ei]])):
            is_t1 = (e20[ei-1] <= e60[ei-1] and e20[ei] > e60[ei])
        else:
            is_t1 = False

        # 進場型態分類
        if is_t1:
            if hi20 < 0.40:
                typ = 'T1-底部'   # T1 發生在長期低位 → 真底部
            elif hi20 > 0.65 and ma20 > 0.45:
                typ = 'T1-高位'   # T1 發生在最近高位 → 死貓?
            else:
                typ = 'T1-中性'
        else:
            if hi20 > 0.60 and pb_now < 0.30:
                typ = 'T3-回檔'   # 從高位回檔到低位 → 好的T3
            elif hi20 < 0.40:
                typ = 'T3-弱位'   # 從來沒到過高位就拉回 → 危險
            else:
                typ = 'T3-中性'

        win_tag = '✓' if ret > 0 else '✗'
        print(f"  {t['ed'].strftime('%Y-%m-%d')} {win_tag}  {ret:>+6.1f}%  "
              f"{hi20:>5.2f} {lo10:>5.2f} {ma20:>5.2f} {walk:>4} {bear:>4}  "
              f"{pb_now:>6.2f}  {typ}")

        pat = dict(hi20=hi20, lo10=lo10, ma20=ma20, walk=walk, bear=bear,
                   pb_now=pb_now, typ=typ, ret=ret)
        if ret > 0: win_patterns.append(pat)
        else:       loss_patterns.append(pat)

    def avg(lst, key):
        vals = [p[key] for p in lst if not np.isnan(p[key])]
        return np.mean(vals) if vals else float('nan')

    print(f"\n  ── 獲利進場平均（n={len(win_patterns)}）──")
    print(f"     hi20={avg(win_patterns,'hi20'):.2f}  lo10={avg(win_patterns,'lo10'):.2f}  "
          f"ma20={avg(win_patterns,'ma20'):.2f}  walk={avg(win_patterns,'walk'):.1f}  "
          f"bear={avg(win_patterns,'bear'):.1f}  pb_now={avg(win_patterns,'pb_now'):.2f}")
    print(f"  ── 虧損進場平均（n={len(loss_patterns)}）──")
    print(f"     hi20={avg(loss_patterns,'hi20'):.2f}  lo10={avg(loss_patterns,'lo10'):.2f}  "
          f"ma20={avg(loss_patterns,'ma20'):.2f}  walk={avg(loss_patterns,'walk'):.1f}  "
          f"bear={avg(loss_patterns,'bear'):.1f}  pb_now={avg(loss_patterns,'pb_now'):.2f}")


# ── 測試股票 ────────────────────────────────────────────────────
print("【壞股票：死亡迴圈】")
for t, n in [('2939','永邑'), ('2498','宏達電'), ('5203','訊連'),
             ('1732','毛寶'), ('4133','亞諾法')]:
    analyze_pattern(t, n)

print("\n\n【好股票：大趨勢】")
for t, n in [('6139','亞翔'), ('4961','天鈺'), ('2609','陽明')]:
    analyze_pattern(t, n)

print("\n\n【好股票：穩健】")
for t, n in [('2317','鴻海'), ('6443','元晶'), ('2454','聯發科')]:
    analyze_pattern(t, n)
