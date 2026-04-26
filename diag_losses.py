"""
診斷 Bottom 10 股票的損失結構

問題：v7 在死亡迴圈股 (-100%~-140%) 為何仍持續產生損失？

分析三個面向：
  1. 損失來源：T1 黃金交叉 vs T3 拉回，哪個產生更多虧損？
  2. 進場後行為：被停損 vs EMA死叉自然出場？
  3. 進場間隔：上次出場到下次進場間隔分佈
  4. 被停損後再進場的時間
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import numpy as np

bt.START = '2020-01-02'
bt.END   = '2026-04-25'
bt.INVEST = 100_000

losers = ['1732', '4133', '2939', '5203', '4943', '6598', '00680L', '6657', '3041', '2642']
labels = {'1732':'毛寶', '4133':'亞諾法', '2939':'永邑', '5203':'訊連',
          '4943':'康控', '6598':'ABC', '00680L':'美債20正2',
          '6657':'華安', '3041':'揚智', '2642':'宅配通'}

print(f"{'代號':<8} {'名稱':<10} {'BH%':>7} {'v7%':>7} {'T1次':>4} {'T3次':>4} {'T1平均':>8} {'T3平均':>8} {'停損%':>7} {'快入':>4}")
print('-'*90)

for tk in losers:
    r = bt.analyze(tk)
    if not r: continue

    bh = r['bh_pnl'] / bt.INVEST * 100
    v7 = r['pnl7']   / bt.INVEST * 100

    df = bt.download(tk)
    df = bt.calc_ind(df)
    df = df[df.index >= bt.START]
    dates = df.index.tolist()
    e20   = df['e20'].values
    e60   = df['e60'].values
    date_idx = {d: i for i, d in enumerate(dates)}

    t1_pnl, t3_pnl = [], []
    n_stop  = 0       # 停損次數
    fast_re = 0       # 上次出場後 30 日內再進場次數
    last_xd = None

    for t in sorted(r['t7'], key=lambda x: x['ed']):
        ei = date_idx.get(t['ed'])
        if ei is None or ei < 1: continue
        # 判定 T1 vs T3
        is_t1 = False
        if not any(np.isnan([e20[ei-1], e60[ei-1], e20[ei], e60[ei]])):
            is_t1 = (e20[ei-1] <= e60[ei-1] and e20[ei] > e60[ei])
        ret = (t['xp'] - t['ep']) / t['ep'] * 100
        if is_t1: t1_pnl.append(ret)
        else:     t3_pnl.append(ret)

        if t.get('stop'): n_stop += 1
        if last_xd is not None:
            gap = (t['ed'] - last_xd).days
            if gap <= 30: fast_re += 1
        last_xd = t['xd']

    n_total = len(r['t7'])
    stop_pct = n_stop / n_total * 100 if n_total else 0
    t1_avg = np.mean(t1_pnl) if t1_pnl else float('nan')
    t3_avg = np.mean(t3_pnl) if t3_pnl else float('nan')
    print(f"  {tk:<8} {labels.get(tk, ''):<10} {bh:>+7.1f} {v7:>+7.1f} "
          f"{len(t1_pnl):>4} {len(t3_pnl):>4} "
          f"{t1_avg:>+8.1f}% {t3_avg:>+8.1f}% "
          f"{stop_pct:>6.0f}% {fast_re:>4}")

print('\n注：「快入」= 上次出場後 30 日內又進場的次數（疑似死貓再陷阱）')
