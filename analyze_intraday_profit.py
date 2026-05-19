"""倍數 ETF 當日進場 → 收盤 P&L 分析
=========================================

回答：每天哪個時間點進場「BUY-Hold to Close」期望報酬最高？

對每個時間 T 計算 3 種策略：
  策略 A：「跟漲」— 若 Close@T > Open 則 BUY，持有到 15:55
  策略 B：「無腦 BUY」— 不管方向，T 時刻直接 BUY 持有到 15:55
  策略 C：「順勢做空」— Close@T < Open 則 SHORT (假設可放空)

每策略統計：
  - 適用日數
  - 勝率 P(P&L > 0)
  - 平均 P&L %
  - Median / max / min P&L
  - Sharpe-like 指標 (avg / std)
"""
from __future__ import annotations
import sys, io, os, time
try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                                         line_buffering=True)
except Exception:
    pass

from datetime import datetime, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from intraday.data import _has_alpaca, ALPACA_API_KEY, ALPACA_API_SECRET, ALPACA_PAPER

ETFS = ['AMDL', 'ERX', 'NVDL', 'SOXL', 'SPXL', 'TECL']
YEARS = 3
ET = ZoneInfo('America/New_York')
RTH_OPEN = dtime(9, 30)
RTH_CLOSE = dtime(16, 0)
DOJI_PCT = 0.1

CANDIDATE_TIMES = [
    (30, '10:00'), (60, '10:30'), (90, '11:00'), (120, '11:30'),
    (150, '12:00'), (180, '12:30'), (210, '13:00'), (240, '13:30'),
    (270, '14:00'), (300, '14:30'), (330, '15:00'),
]


def fetch_5m(ticker, years):
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from alpaca.data.enums import DataFeed
    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
    feed = DataFeed.IEX if ALPACA_PAPER else DataFeed.SIP
    end = datetime.now(timezone.utc); start = end - timedelta(days=years*365)
    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(5, TimeFrameUnit.Minute),
            start=start, end=end, feed=feed,
        )
        df = client.get_stock_bars(req).df
        if df is None or len(df) == 0: return None
        if isinstance(df.index, pd.MultiIndex): df = df.droplevel('symbol')
        return df.rename(columns={'open':'Open','high':'High','low':'Low',
                                    'close':'Close','volume':'Volume'})
    except Exception as e:
        print(f'  ❌ {ticker}: {type(e).__name__}: {str(e)[:80]}')
        return None


def prep(df):
    df = df.copy()
    if df.index.tz is None: df.index = df.index.tz_localize('UTC')
    df['et_time'] = df.index.tz_convert(ET)
    df['date'] = df['et_time'].dt.date
    df['time_only'] = df['et_time'].dt.time
    return df[(df['time_only'] >= RTH_OPEN) & (df['time_only'] < RTH_CLOSE)]


def compute_pnl_table(df_rth):
    """每日：對每個 T，price@T + 方向 + Long P&L + Short P&L"""
    rows = []
    for date_val, day_df in df_rth.groupby('date'):
        if len(day_df) < 30: continue
        day_open = float(day_df.iloc[0]['Open'])
        if day_open <= 0: continue
        close_p = float(day_df.iloc[-1]['Close'])
        day_ret = (close_p - day_open) / day_open * 100
        final_dir = ('DOJI' if abs(day_ret) < DOJI_PCT
                     else ('UP' if day_ret > 0 else 'DOWN'))
        r = {'date': date_val, 'open': day_open, 'close': close_p,
             'day_ret_pct': day_ret, 'final_dir': final_dir}
        mkt_open = datetime.combine(date_val, RTH_OPEN, tzinfo=ET)
        for min_off, label in CANDIDATE_TIMES:
            target = mkt_open + timedelta(minutes=min_off)
            before = day_df[day_df['et_time'] <= target]
            if len(before) == 0:
                r[f'price_{label}'] = None
                r[f'dir_{label}'] = None
                r[f'long_pnl_{label}'] = None
                continue
            entry_p = float(before.iloc[-1]['Close'])
            r[f'price_{label}'] = entry_p
            ret_t = (entry_p - day_open) / day_open * 100
            r[f'dir_{label}'] = ('DOJI' if abs(ret_t) < DOJI_PCT
                                 else ('UP' if ret_t > 0 else 'DOWN'))
            # Long: 在 T 買，15:55 賣
            r[f'long_pnl_{label}'] = (close_p - entry_p) / entry_p * 100
        rows.append(r)
    return pd.DataFrame(rows)


def summarize_strategies(df, ticker):
    """對每個時間 T 計算 3 種策略"""
    print(f'\n{"═"*78}')
    print(f'  📊 {ticker} — 進場 T → 收盤 P&L (3 策略)')
    print(f'{"═"*78}')
    print(f'  總交易日 {len(df)} (UP {(df["final_dir"]=="UP").sum()}, '
          f'DOWN {(df["final_dir"]=="DOWN").sum()})\n')

    print(f'  ── 策略 A：「跟漲」(dir@T=UP 才 BUY) ──')
    print(f'  {"時間":<7} {"N":>4} {"勝率":>6} {"Avg P&L":>9} {"Median":>8} {"Max":>8} {"Min":>8}')
    best_A = (None, -999)
    for _, label in CANDIDATE_TIMES:
        d = df[df[f'dir_{label}'] == 'UP'].dropna(subset=[f'long_pnl_{label}'])
        if len(d) == 0: continue
        pnl = d[f'long_pnl_{label}']
        avg = pnl.mean()
        win = (pnl > 0).mean() * 100
        if avg > best_A[1]: best_A = (label, avg)
        marker = '⭐' if avg > 1.0 else ('🟢' if avg > 0.3 else '🟡' if avg > 0 else '🔴')
        print(f'  {label:<7} {len(d):>4} {win:>5.1f}% {avg:>+8.2f}% {pnl.median():>+7.2f}% '
              f'{pnl.max():>+7.2f}% {pnl.min():>+7.2f}%  {marker}')
    print(f'  ➜ 最佳 entry: {best_A[0]} (avg {best_A[1]:+.2f}%)')

    print(f'\n  ── 策略 B：「無腦 BUY」(不看方向, T 時刻直接買) ──')
    print(f'  {"時間":<7} {"N":>4} {"勝率":>6} {"Avg P&L":>9} {"Median":>8} {"Max":>8} {"Min":>8}')
    best_B = (None, -999)
    for _, label in CANDIDATE_TIMES:
        d = df.dropna(subset=[f'long_pnl_{label}'])
        if len(d) == 0: continue
        pnl = d[f'long_pnl_{label}']
        avg = pnl.mean()
        win = (pnl > 0).mean() * 100
        if avg > best_B[1]: best_B = (label, avg)
        marker = '⭐' if avg > 1.0 else ('🟢' if avg > 0.3 else '🟡' if avg > 0 else '🔴')
        print(f'  {label:<7} {len(d):>4} {win:>5.1f}% {avg:>+8.2f}% {pnl.median():>+7.2f}% '
              f'{pnl.max():>+7.2f}% {pnl.min():>+7.2f}%  {marker}')
    print(f'  ➜ 最佳 entry: {best_B[0]} (avg {best_B[1]:+.2f}%)')

    print(f'\n  ── 策略 C：「順勢做空」(dir@T=DOWN 才 SHORT, P&L = entry - close) ──')
    print(f'  {"時間":<7} {"N":>4} {"勝率":>6} {"Avg P&L":>9} {"Median":>8} {"Max":>8} {"Min":>8}')
    best_C = (None, -999)
    for _, label in CANDIDATE_TIMES:
        d = df[df[f'dir_{label}'] == 'DOWN'].dropna(subset=[f'long_pnl_{label}'])
        if len(d) == 0: continue
        # SHORT P&L = -LONG P&L
        pnl = -d[f'long_pnl_{label}']
        avg = pnl.mean()
        win = (pnl > 0).mean() * 100
        if avg > best_C[1]: best_C = (label, avg)
        marker = '⭐' if avg > 1.0 else ('🟢' if avg > 0.3 else '🟡' if avg > 0 else '🔴')
        print(f'  {label:<7} {len(d):>4} {win:>5.1f}% {avg:>+8.2f}% {pnl.median():>+7.2f}% '
              f'{pnl.max():>+7.2f}% {pnl.min():>+7.2f}%  {marker}')
    print(f'  ➜ 最佳 entry: {best_C[0]} (avg {best_C[1]:+.2f}%)')

    return {'ticker': ticker, 'best_A': best_A, 'best_B': best_B, 'best_C': best_C,
            'days': len(df)}


def main():
    if not _has_alpaca():
        print('❌ Alpaca 未設定'); return
    print('═'*70)
    print('  倍數 ETF 進場 T → 收盤 P&L 分析 (3 策略)')
    print('═'*70)

    all_best = []
    t0 = time.time()
    for tk in ETFS:
        print(f'\n[{tk}] 抓 + 分析中...')
        df = fetch_5m(tk, YEARS)
        if df is None or len(df) < 1000:
            print(f'  ⚠️  資料不足'); continue
        df_rth = prep(df)
        pnl_table = compute_pnl_table(df_rth)
        if len(pnl_table) == 0: continue
        pnl_table.to_csv(f'pnl_{tk}.csv', index=False, encoding='utf-8-sig')
        best = summarize_strategies(pnl_table, tk)
        all_best.append(best)

    # 跨 ETF 最佳 entry 排行
    if all_best:
        print(f'\n\n{"═"*78}')
        print('  🌍 跨 ETF：最佳進場時刻排行')
        print('═'*78)
        print(f'\n  策略 A 「跟漲」最佳 entry:')
        for b in sorted(all_best, key=lambda x: -x['best_A'][1]):
            print(f'    {b["ticker"]:<8} {b["best_A"][0]:>6}  avg {b["best_A"][1]:+.2f}%')
        print(f'\n  策略 B 「無腦 BUY」最佳 entry:')
        for b in sorted(all_best, key=lambda x: -x['best_B'][1]):
            print(f'    {b["ticker"]:<8} {b["best_B"][0]:>6}  avg {b["best_B"][1]:+.2f}%')
        print(f'\n  策略 C 「順勢做空」最佳 entry:')
        for b in sorted(all_best, key=lambda x: -x['best_C'][1]):
            print(f'    {b["ticker"]:<8} {b["best_C"][0]:>6}  avg {b["best_C"][1]:+.2f}%')

    print(f'\n總耗時 {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
