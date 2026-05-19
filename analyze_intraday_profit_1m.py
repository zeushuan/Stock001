"""倍數 ETF 進場 P&L 分析 — 1m 解析度版
==========================================

5m 版發現甜蜜段在 10:00-12:00 ET，但缺少 09:35-09:55 的細節。
1m 版新增 5-min 精度的早盤掃描，找次黃金點：
  09:35 / 09:40 / 09:45 / 09:50 / 09:55 / 10:00 / 10:05 / 10:10 ...

要回答的問題：
  「10:00 ET 是真的最強，還是 09:45 / 09:55 更強？」
  「11:30 跟 11:35 的差別大嗎？」
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

# 🆕 1m 版：09:35-10:30 每 5min，之後遞減頻率
CANDIDATE_TIMES = [
    (5, '09:35'), (10, '09:40'), (15, '09:45'), (20, '09:50'),
    (25, '09:55'), (30, '10:00'), (35, '10:05'), (40, '10:10'),
    (45, '10:15'), (50, '10:20'), (55, '10:25'), (60, '10:30'),
    (75, '10:45'), (90, '11:00'), (105, '11:15'), (120, '11:30'),
    (150, '12:00'), (180, '12:30'), (210, '13:00'), (240, '13:30'),
    (270, '14:00'), (300, '14:30'), (330, '15:00'),
]


def fetch_1m(ticker, years):
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    from alpaca.data.enums import DataFeed
    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
    feed = DataFeed.IEX if ALPACA_PAPER else DataFeed.SIP
    end = datetime.now(timezone.utc); start = end - timedelta(days=years*365)
    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame.Minute,
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
    rows = []
    for date_val, day_df in df_rth.groupby('date'):
        if len(day_df) < 100: continue   # 1m 需要更多 bars
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
            # 1m 精度：找 = target time 那根 bar
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
            r[f'long_pnl_{label}'] = (close_p - entry_p) / entry_p * 100
        rows.append(r)
    return pd.DataFrame(rows)


def summarize(df, ticker):
    print(f'\n{"═"*82}')
    print(f'  📊 {ticker} — 1m 解析度 P&L (策略 A: dir@T=UP 才 BUY)')
    print(f'{"═"*82}')
    print(f'  總交易日 {len(df)} '
          f'(UP {(df["final_dir"]=="UP").sum()}, '
          f'DOWN {(df["final_dir"]=="DOWN").sum()})\n')

    print(f'  {"時間":<7} {"N":>4} {"勝率":>6} {"Avg P&L":>9} {"Median":>8} {"Max":>8} {"Min":>8}')
    best = (None, -999)
    early_best = (None, -999)   # 09:35-10:30
    rows_for_csv = []
    for _, label in CANDIDATE_TIMES:
        d = df[df[f'dir_{label}'] == 'UP'].dropna(subset=[f'long_pnl_{label}'])
        if len(d) == 0: continue
        pnl = d[f'long_pnl_{label}']
        avg = pnl.mean()
        win = (pnl > 0).mean() * 100
        if avg > best[1]: best = (label, avg)
        # 早盤段 09:35-10:30 = min_off in [5, 60]
        idx = next((i for i, (m, l) in enumerate(CANDIDATE_TIMES) if l == label), -1)
        min_off = CANDIDATE_TIMES[idx][0] if idx >= 0 else 999
        if 5 <= min_off <= 60 and avg > early_best[1]:
            early_best = (label, avg)
        marker = '⭐' if avg >= 0.4 else ('🟢' if avg >= 0.2 else
                                          '🟡' if avg >= 0 else '🔴')
        print(f'  {label:<7} {len(d):>4} {win:>5.1f}% {avg:>+8.2f}% '
              f'{pnl.median():>+7.2f}% {pnl.max():>+7.2f}% {pnl.min():>+7.2f}%  {marker}')
        rows_for_csv.append({'time': label, 'n': len(d), 'win_pct': win,
                              'avg_pnl': avg, 'median_pnl': pnl.median(),
                              'max_pnl': pnl.max(), 'min_pnl': pnl.min()})
    print(f'  ➜ 全 segment 最佳: {best[0]} (avg {best[1]:+.2f}%)')
    print(f'  ➜ 早盤 09:35-10:30 最佳: {early_best[0]} (avg {early_best[1]:+.2f}%)')

    pd.DataFrame(rows_for_csv).to_csv(f'pnl_1m_{ticker}.csv', index=False, encoding='utf-8-sig')
    return {'ticker': ticker, 'best': best, 'early_best': early_best,
            'days': len(df)}


def main():
    if not _has_alpaca():
        print('❌ Alpaca 未設定'); return
    print('═'*70)
    print(f'  倍數 ETF — 1m 解析度進場 P&L (5-min precision 早盤)')
    print('═'*70)
    print(f'  時段: 09:30-16:00 ET ｜ Bar: 1 minute ｜ 樣本: {YEARS} 年')

    all_best = []
    t0 = time.time()
    for tk in ETFS:
        print(f'\n[{tk}] 抓 1m × {YEARS}y...')
        df = fetch_1m(tk, YEARS)
        if df is None or len(df) < 10000:
            n = len(df) if df is not None else 0
            print(f'  ⚠️  資料不足 ({n} bars)'); continue
        print(f'  抓到 {len(df):,} bars, {(time.time()-t0):.0f}s elapsed')

        df_rth = prep(df)
        pnl_table = compute_pnl_table(df_rth)
        if len(pnl_table) == 0: continue
        all_best.append(summarize(pnl_table, tk))

    # 跨 ETF 排行 (早盤段)
    if all_best:
        print(f'\n\n{"═"*82}')
        print('  🌍 跨 ETF — 早盤 09:35-10:30 最佳進場時刻')
        print('═'*82)
        for b in sorted(all_best, key=lambda x: -x['early_best'][1]):
            print(f'  {b["ticker"]:<8} {b["early_best"][0]:>6}  avg {b["early_best"][1]:+.2f}%')

        print(f'\n{"═"*82}')
        print('  🌍 跨 ETF — 全 segment 最佳進場時刻')
        print('═'*82)
        for b in sorted(all_best, key=lambda x: -x['best'][1]):
            print(f'  {b["ticker"]:<8} {b["best"][0]:>6}  avg {b["best"][1]:+.2f}%')

    print(f'\n總耗時 {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
