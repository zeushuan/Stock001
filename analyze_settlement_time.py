"""倍數 ETF 每日趨勢「抵定時刻」分析
========================================

研究問題：每天盤中，價格何時「鎖定」最終方向不再翻轉？

方法論：
  • 方向定義: Close vs Open (日内)
  • 抵定時刻 T = 最後一次穿越 Open 的時間（Last-Cross-Open）
    - UP day:   T = 最後一個 Low ≤ Open 的 bar
    - DOWN day: T = 最後一個 High ≥ Open 的 bar
  • T 之後價格再也沒回到 Open 同方向
  • 時段限制: 09:30-16:00 ET Regular Hours (排除 pre/post-market)
  • Bar size: 5 分鐘
  • 資料源: Alpaca paper (IEX feed)
  • 樣本: 3 年歷史 (AMDL launch 2023-09 約 2.5y)

ETFs: AMDL / ERX / NVDL / SOXL / SPXL / TECL
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
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from intraday.data import (
    _has_alpaca, ALPACA_API_KEY, ALPACA_API_SECRET, ALPACA_PAPER,
)


ETFS = ['AMDL', 'ERX', 'NVDL', 'SOXL', 'SPXL', 'TECL']
YEARS = 3
TF = '5m'

ET = ZoneInfo('America/New_York')
RTH_OPEN = dtime(9, 30)
RTH_CLOSE = dtime(16, 0)
DOJI_THRESHOLD_PCT = 0.1     # |Close-Open|/Open < 0.1% 視為 doji


def fetch_5m_history(ticker: str, years: int = 3) -> pd.DataFrame | None:
    """抓 N 年 5m bars (繞過 _fetch_alpaca 的 60-day 限制)"""
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from alpaca.data.enums import DataFeed

    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
    feed = DataFeed.IEX if ALPACA_PAPER else DataFeed.SIP

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=years * 365)

    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(5, TimeFrameUnit.Minute),
            start=start, end=end,
            feed=feed,
        )
        bars = client.get_stock_bars(req)
        df = bars.df
        if df is None or len(df) == 0:
            return None
        if isinstance(df.index, pd.MultiIndex):
            df = df.droplevel('symbol')
        df = df.rename(columns={
            'open':'Open','high':'High','low':'Low',
            'close':'Close','volume':'Volume',
        })
        return df
    except Exception as e:
        print(f'  ❌ {ticker}: {type(e).__name__}: {str(e)[:80]}')
        return None


def compute_settlement(df: pd.DataFrame) -> pd.DataFrame:
    """計算每日的 settlement time + 相關 statistics"""
    if df is None or len(df) == 0:
        return pd.DataFrame()

    # tz 轉 ET
    df = df.copy()
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    else:
        df.index = df.index.tz_convert('UTC')
    df['et_time'] = df.index.tz_convert(ET)
    df['date'] = df['et_time'].dt.date
    df['time_only'] = df['et_time'].dt.time

    # 過濾 regular hours 9:30-16:00 ET
    rth_mask = (df['time_only'] >= RTH_OPEN) & (df['time_only'] < RTH_CLOSE)
    df = df[rth_mask]
    if len(df) == 0:
        return pd.DataFrame()

    results = []
    for date_val, day_df in df.groupby('date'):
        # 至少 30 個 5m bars (約 2.5hr)
        if len(day_df) < 30:
            continue

        day_open = float(day_df.iloc[0]['Open'])
        day_close = float(day_df.iloc[-1]['Close'])
        if day_open <= 0:
            continue
        ret_pct = (day_close - day_open) / day_open * 100

        # 方向
        if abs(ret_pct) < DOJI_THRESHOLD_PCT:
            direction = 'DOJI'
        else:
            direction = 'UP' if ret_pct > 0 else 'DOWN'

        # Last-Cross-Open
        if direction == 'UP':
            cross_mask = day_df['Low'] <= day_open
        elif direction == 'DOWN':
            cross_mask = day_df['High'] >= day_open
        else:
            cross_mask = pd.Series([True] * len(day_df), index=day_df.index)

        cross_indices = day_df.index[cross_mask]
        if len(cross_indices) == 0:
            last_cross = day_df.index[0]
        else:
            last_cross = cross_indices[-1]
        settled_et = day_df.loc[last_cross, 'et_time']

        # 分鐘數（自 09:30 起算）
        mkt_open_dt = datetime.combine(date_val, RTH_OPEN, tzinfo=ET)
        settle_min = (settled_et - mkt_open_dt).total_seconds() / 60

        results.append({
            'date': date_val,
            'direction': direction,
            'open': day_open,
            'close': day_close,
            'return_pct': round(ret_pct, 2),
            'settle_time_et': settled_et.strftime('%H:%M'),
            'settle_minutes': round(settle_min, 0),
        })

    return pd.DataFrame(results)


def print_summary(df_res: pd.DataFrame, ticker: str) -> dict:
    """印出單檔統計，並回傳 summary dict"""
    n_total = len(df_res)
    n_up = (df_res['direction'] == 'UP').sum()
    n_down = (df_res['direction'] == 'DOWN').sum()
    n_doji = (df_res['direction'] == 'DOJI').sum()

    df_dir = df_res[df_res['direction'].isin(['UP', 'DOWN'])]
    summary = {'ticker': ticker, 'days': n_total, 'up': n_up,
               'down': n_down, 'doji': n_doji}

    if len(df_dir) == 0:
        print(f'\n══ {ticker}: 無有效 day data ══')
        return summary

    settle = df_dir['settle_minutes']
    summary['median_min'] = settle.median()
    summary['p25_min'] = settle.quantile(0.25)
    summary['p75_min'] = settle.quantile(0.75)

    print(f'\n══ {ticker} ({n_total} days: {n_up} UP / {n_down} DOWN / {n_doji} Doji) ══')
    def _to_clock(m):
        h = 9 + int(m + 30) // 60
        mi = int(m + 30) % 60
        return f'{h:02d}:{mi:02d}'
    print(f'  Settlement (min after 09:30 ET):')
    print(f'    p25 = {settle.quantile(0.25):>4.0f} min ({_to_clock(settle.quantile(0.25))} ET)')
    print(f'    p50 = {settle.median():>4.0f} min ({_to_clock(settle.median())} ET)  ← median')
    print(f'    p75 = {settle.quantile(0.75):>4.0f} min ({_to_clock(settle.quantile(0.75))} ET)')
    print(f'    p90 = {settle.quantile(0.90):>4.0f} min ({_to_clock(settle.quantile(0.90))} ET)')

    print(f'  % settled by time:')
    cum_pct = {}
    for label, m in [('10:00', 30), ('10:30', 60), ('11:00', 90),
                      ('11:30', 120), ('12:00', 150), ('13:00', 210),
                      ('14:00', 270), ('15:00', 330), ('15:30', 360)]:
        pct = (settle <= m).mean() * 100
        cum_pct[label] = pct
        bar = '█' * int(pct / 2)
        print(f'    by {label} ({m:>3}min): {bar:<50} {pct:5.1f}%')
    summary['cum_pct'] = cum_pct

    # UP vs DOWN
    up = df_dir[df_dir['direction'] == 'UP']['settle_minutes']
    down = df_dir[df_dir['direction'] == 'DOWN']['settle_minutes']
    if len(up) > 0 and len(down) > 0:
        print(f'  UP days   median: {up.median():>4.0f} min ({_to_clock(up.median())} ET)')
        print(f'  DOWN days median: {down.median():>4.0f} min ({_to_clock(down.median())} ET)')
        summary['up_median'] = up.median()
        summary['down_median'] = down.median()

    return summary


def main():
    if not _has_alpaca():
        print('❌ Alpaca 未設定')
        return

    print(f'═══ 倍數 ETF 抵定時刻分析 (5m × {YEARS} 年, RTH only) ═══')
    print(f'方法: Last-Cross-Open | Doji 門檻: |return| < {DOJI_THRESHOLD_PCT}%')
    print(f'時段: 09:30-16:00 ET (排除 pre/post-market)')

    all_summary = []
    all_data = {}
    t0 = time.time()

    for tk in ETFS:
        print(f'\n[{tk}] 抓 {YEARS}y × 5m bars...')
        df = fetch_5m_history(tk, years=YEARS)
        if df is None or len(df) < 1000:
            print(f'  ⚠️  抓不到夠多資料 (got {len(df) if df is not None else 0})')
            continue
        print(f'  共 {len(df)} bars, '
              f'從 {df.index[0]} 到 {df.index[-1]}')

        res = compute_settlement(df)
        if len(res) == 0:
            print(f'  ⚠️  Regular hours filter 後沒資料')
            continue
        all_data[tk] = res
        summary = print_summary(res, tk)
        all_summary.append(summary)

        # 存 CSV
        res.to_csv(f'settlement_{tk}.csv', index=False, encoding='utf-8-sig')

    # ────────── 跨 ETF 對比 ──────────
    if all_summary:
        print(f'\n\n{"═"*70}')
        print('  跨 ETF 對比 (median settlement minutes after 09:30 ET)')
        print('═'*70)
        df_summary = pd.DataFrame(all_summary).sort_values('median_min')
        print(f'\n  {"Ticker":<8} {"Days":>5} {"UP":>5} {"DOWN":>5} '
              f'{"Median":>7} {"UP md":>7} {"DOWN md":>7}')
        print('  ' + '─' * 60)
        for r in df_summary.to_dict('records'):
            print(f'  {r["ticker"]:<8} {r["days"]:>5} {r["up"]:>5} {r["down"]:>5} '
                  f'{r["median_min"]:>7.0f} {r.get("up_median",0):>7.0f} '
                  f'{r.get("down_median",0):>7.0f}')

        # 平均「by X 時抵定」%
        print(f'\n  各時間累計抵定 %:')
        print(f'  {"Ticker":<8} {"10:00":>7} {"10:30":>7} {"11:00":>7} '
              f'{"12:00":>7} {"13:00":>7} {"14:00":>7} {"15:00":>7}')
        print('  ' + '─' * 70)
        for r in df_summary.to_dict('records'):
            cp = r.get('cum_pct', {})
            print(f'  {r["ticker"]:<8} {cp.get("10:00",0):>6.1f}% {cp.get("10:30",0):>6.1f}% '
                  f'{cp.get("11:00",0):>6.1f}% {cp.get("12:00",0):>6.1f}% '
                  f'{cp.get("13:00",0):>6.1f}% {cp.get("14:00",0):>6.1f}% '
                  f'{cp.get("15:00",0):>6.1f}%')

        df_summary.to_csv('settlement_summary.csv', index=False, encoding='utf-8-sig')

    print(f'\n總耗時 {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
