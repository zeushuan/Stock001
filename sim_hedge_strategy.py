"""模擬：盤前等值買雙邊(SOXS+SOXL) → 開盤後出現賣出訊號砍掉一邊
======================================================================
A 案（使用者構想）：開盤等值買 SOXS+SOXL 各半（各 $5,000）；戰法出場訊號
                    先觸發的那邊砍掉，另一邊抱到收盤。
B 案（建議做法）：  不買雙邊。等到「同一個訊號時刻」，把全部 $10,000 押
                    贏的那邊（被留下的那邊），抱到收盤。
C 案（事後上限）：  開盤就只買對的那邊 $10,000，抱到收盤。
進場價以 RTH 09:30 開盤價近似「盤前接近開盤」。
戰法出場訊號 = Close < BB中軌(SMA20) 且 EMA5、EMA20 的 5-bar 斜率 < 0。
"""
import sys, io, os
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                                   line_buffering=True)
except Exception:
    pass
from datetime import time as dtime
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from intraday.twelvedata_src import fetch_td

ET = 'America/New_York'
CAP = 10000.0          # 每個策略總投入資金（A 案兩邊各半）


def exit_signal(c):
    """戰法 v9.38 出場條件序列。"""
    e5 = c.ewm(span=5, adjust=False).mean()
    e20 = c.ewm(span=20, adjust=False).mean()
    e5s = (e5 - e5.shift(5)) / e5.shift(5)
    e20s = (e20 - e20.shift(5)) / e20.shift(5)
    bb = c.rolling(20).mean()
    return (c < bb) & (e5s < 0) & (e20s < 0)


soxs = fetch_td('SOXS', '1m').copy()
soxl = fetch_td('SOXL', '1m').copy()
for df in (soxs, soxl):
    df.index = df.index.tz_localize('UTC').tz_convert(ET)
soxs['sig'] = exit_signal(soxs['Close'])
soxl['sig'] = exit_signal(soxl['Close'])

days = sorted(set(soxs.index.date) & set(soxl.index.date))

print(f'{"日期":<11}{"砍邊":<7}{"砍時間":<8}{"留":<6}'
      f'{"A 砍一邊":>11}{"B 等訊號押一":>14}{"C 開盤押對":>12}')
print('-' * 72)

tot_a = tot_b = tot_c = 0.0
n = a_win = b_win = ab = 0
for d in days:
    sx = soxs[(soxs.index.date == d) & (soxs.index.time >= dtime(9, 30))
              & (soxs.index.time < dtime(16, 0))]
    sl = soxl[(soxl.index.date == d) & (soxl.index.time >= dtime(9, 30))
              & (soxl.index.time < dtime(16, 0))]
    if len(sx) < 300 or len(sl) < 300:
        continue
    n += 1
    sx_e, sl_e = float(sx.iloc[0]['Open']), float(sl.iloc[0]['Open'])
    sx_c, sl_c = float(sx.iloc[-1]['Close']), float(sl.iloc[-1]['Close'])
    sx_sh, sl_sh = (CAP / 2) / sx_e, (CAP / 2) / sl_e
    sx_ex, sl_ex = sx[sx['sig']], sl[sl['sig']]
    sx_t = sx_ex.index[0] if len(sx_ex) else None
    sl_t = sl_ex.index[0] if len(sl_ex) else None

    if sx_t is not None and (sl_t is None or sx_t <= sl_t):
        cut, cut_t, keep = 'SOXS', sx_t, 'SOXL'
        cut_px = float(sx.loc[cut_t, 'Close'])
        a = (cut_px - sx_e) * sx_sh + (sl_c - sl_e) * sl_sh
        sl_atcut = float(sl['Close'].asof(cut_t))
        b = (sl_c - sl_atcut) * (CAP / sl_atcut)
        c_ = (sl_c - sl_e) * (CAP / sl_e)
    elif sl_t is not None:
        cut, cut_t, keep = 'SOXL', sl_t, 'SOXS'
        cut_px = float(sl.loc[cut_t, 'Close'])
        a = (cut_px - sl_e) * sl_sh + (sx_c - sx_e) * sx_sh
        sx_atcut = float(sx['Close'].asof(cut_t))
        b = (sx_c - sx_atcut) * (CAP / sx_atcut)
        c_ = (sx_c - sx_e) * (CAP / sx_e)
    else:
        cut, cut_t, keep = None, None, '—'
        a = (sx_c - sx_e) * sx_sh + (sl_c - sl_e) * sl_sh
        b = c_ = 0.0

    tot_a += a; tot_b += b; tot_c += c_
    if a > 0: a_win += 1
    if b > 0: b_win += 1
    if a >= b: ab += 1
    ct = cut_t.strftime('%H:%M') if cut_t is not None else '—'
    print(f'{str(d):<11}{(cut or "無訊號"):<7}{ct:<8}{keep:<6}'
          f'{a:>+11.0f}{b:>+14.0f}{c_:>+12.0f}')

print('-' * 72)
print(f'{"合計":<32}{tot_a:>+11.0f}{tot_b:>+14.0f}{tot_c:>+12.0f}')
if n:
    print(f'{"平均/日":<32}{tot_a/n:>+11.0f}{tot_b/n:>+14.0f}{tot_c/n:>+12.0f}')
    print()
    print(f'  測試 {n} 天 · 每策略每日投入 ${CAP:,.0f}')
    print(f'  A 案賺錢 {a_win}/{n} 天 ｜ B 案賺錢 {b_win}/{n} 天 '
          f'｜ A≥B 的天數 {ab}/{n}')
