"""分析使用者 SOXS 2026-05-19 實際成交 — 對照戰法訊號與情緒陷阱
================================================================
資料來自帳戶 67801309「訂單現況」截圖（7 筆 SOXS 成交）。
"""
import sys, io, os
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                                   line_buffering=True)
except Exception:
    pass
from datetime import datetime, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from intraday.data import ALPACA_API_KEY, ALPACA_API_SECRET

ET = ZoneInfo('America/New_York')
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
end = datetime.now(timezone.utc)
start = end - timedelta(days=6)
req = StockBarsRequest(symbol_or_symbols='SOXS', timeframe=TimeFrame.Minute,
                        start=start, end=end, feed=DataFeed.IEX)
df = client.get_stock_bars(req).df
if isinstance(df.index, pd.MultiIndex):
    df = df.droplevel('symbol')
df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                         'close': 'Close', 'volume': 'Volume'})
df.index = df.index.tz_convert(ET)
day = df[df.index.date == pd.Timestamp('2026-05-19').date()].copy()
rth = day[(day.index.time >= dtime(9, 30)) & (day.index.time < dtime(16, 0))]

o = float(rth.iloc[0]['Open']); c = float(rth.iloc[-1]['Close'])
hi = float(rth['High'].max()); lo = float(rth['Low'].min())
hi_t = rth['High'].idxmax().strftime('%H:%M')
lo_t = rth['Low'].idxmin().strftime('%H:%M')

print('=' * 60)
print('  SOXS 2026-05-19  RTH 概況')
print('=' * 60)
print(f'  開 ${o:.4f}  收 ${c:.4f}  ({(c-o)/o*100:+.2f}%)')
print(f'  最高 ${hi:.4f} @ {hi_t}   最低 ${lo:.4f} @ {lo_t}')
print(f'  全日振幅 ${hi-lo:.4f}（{(hi-lo)/lo*100:.1f}%）')

# 使用者成交（截圖由上而下 = 新到舊）
fills = [
    ('賣出', 1500, 9.74),
    ('賣出', 100, 10.23),
    ('買進', 500, 10.52),
    ('買進', 1000, 10.13),
    ('買進', 1000, 9.89),
    ('賣出', 1000, 10.21),
    ('買進', 100, 10.51),
]

print()
print('  ── 每筆成交在當日的時間位置 ──')
for side, qty, px in fills:
    hit = rth[(rth['Low'] <= px) & (rth['High'] >= px)]
    rng_pct = (px - lo) / (hi - lo) * 100 if hi > lo else 0
    if len(hit):
        t0 = hit.index[0].strftime('%H:%M')
        t1 = hit.index[-1].strftime('%H:%M')
        win = f'{t0}' if t0 == t1 else f'{t0}–{t1}（{len(hit)} 根 K）'
        print(f'  {side} {qty:>4} 股 @ ${px:<6} | 觸及 {win:<22} '
              f'| 位於當日區間 {rng_pct:.0f}%')
    else:
        print(f'  {side} {qty:>4} 股 @ ${px:<6} | RTH 未觸及（盤前/盤後）'
              f' | 位於當日區間 {rng_pct:.0f}%')

# 聚合損益
buys = [(q, p) for s, q, p in fills if s == '買進']
sells = [(q, p) for s, q, p in fills if s == '賣出']
bq = sum(q for q, p in buys); bc = sum(q * p for q, p in buys)
sq = sum(q for q, p in sells); sc = sum(q * p for q, p in sells)

print()
print('  ── 聚合損益（畫面可見 7 筆）──')
print(f'  買進 {bq} 股 · 均價 ${bc/bq:.4f} · 成本 ${bc:,.0f}')
print(f'  賣出 {sq} 股 · 均價 ${sc/sq:.4f} · 收入 ${sc:,.0f}')
print(f'  淨損益 ${sc-bc:+,.0f}  （{(sc-bc)/bc*100:+.2f}%）')

# 戰法對照
ENTRY_T, ENTRY_P = '09:50', 10.375
EXIT_T, EXIT_P = '10:29', 10.845
print()
print('  ── 對照戰法 v9.38（bb_p1sig 進場 / mid_ema_down 出場）──')
print(f'  戰法進場 {ENTRY_T} ${ENTRY_P}  →  戰法出場 {EXIT_T} ${EXIT_P}'
      f'  （+{(EXIT_P-ENTRY_P)/ENTRY_P*100:.2f}%）')
# 最痛一筆：賣 1500 @ 9.74
worst_q, worst_p = 1500, 9.74
lost_vs_exit = (EXIT_P - worst_p) * worst_q
print(f'  你最大一筆「賣出 {worst_q} 股 @ ${worst_p}」：')
print(f'    若改在戰法出場價 ${EXIT_P} 賣 → 多收 ${lost_vs_exit:,.0f}')
print(f'    ${worst_p} 距全日最低 ${lo} 僅 {(worst_p-lo)/lo*100:.1f}%（賣在地板附近）')
# 全部照戰法出場
cf = EXIT_P * bq - bc
print(f'  假設 {bq} 股全在戰法出場價 ${EXIT_P} 賣出：損益 ${cf:+,.0f}')
print(f'  → 紀律 vs 情緒的代價 ≈ ${cf-(sc-bc):,.0f}')
print('=' * 60)
