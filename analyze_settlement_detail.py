"""倍數 ETF 抵定時刻 — 詳細分析 + 假抵定率
==================================================

新加分析：
  1. 假抵定率：在時間 t 看似已確定方向，但最終收盤反轉
     公式: 1 - P(dir@t == dir@close)
  2. 每檔 ETF 詳細報告（樣本量、UP/DOWN 統計、最佳/最差日）
  3. 跨 ETF 整體報告
  4. 按年份分組（2023/2024/2025/2026）

對應問題：
  「在 10:00 ET 看到 +0.5% 是否能放心 hold 到收盤？」
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
ET = ZoneInfo('America/New_York')
RTH_OPEN = dtime(9, 30)
RTH_CLOSE = dtime(16, 0)
DOJI_PCT = 0.1

CANDIDATE_TIMES = [
    (30, '10:00'), (60, '10:30'), (90, '11:00'),
    (120, '11:30'), (150, '12:00'), (180, '12:30'),
    (210, '13:00'), (240, '13:30'), (270, '14:00'),
    (300, '14:30'), (330, '15:00'), (360, '15:30'),
]


def fetch_5m_history(ticker: str, years: int = 3):
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
            start=start, end=end, feed=feed,
        )
        bars = client.get_stock_bars(req)
        df = bars.df
        if df is None or len(df) == 0: return None
        if isinstance(df.index, pd.MultiIndex): df = df.droplevel('symbol')
        df = df.rename(columns={'open':'Open','high':'High','low':'Low',
                                  'close':'Close','volume':'Volume'})
        return df
    except Exception as e:
        print(f'  ❌ {ticker}: {type(e).__name__}: {str(e)[:80]}')
        return None


def prep_df(df: pd.DataFrame) -> pd.DataFrame:
    """tz 轉 ET + 過濾 RTH"""
    df = df.copy()
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    df['et_time'] = df.index.tz_convert(ET)
    df['date'] = df['et_time'].dt.date
    df['time_only'] = df['et_time'].dt.time
    df['year'] = df['et_time'].dt.year
    return df[(df['time_only'] >= RTH_OPEN) & (df['time_only'] < RTH_CLOSE)]


def analyze_per_day(df: pd.DataFrame) -> pd.DataFrame:
    """每日的 settlement + 各候選時間點方向"""
    results = []
    for date_val, day_df in df.groupby('date'):
        if len(day_df) < 30: continue
        day_open = float(day_df.iloc[0]['Open'])
        day_close = float(day_df.iloc[-1]['Close'])
        if day_open <= 0: continue
        ret_pct = (day_close - day_open) / day_open * 100
        if abs(ret_pct) < DOJI_PCT:
            final_dir = 'DOJI'
        else:
            final_dir = 'UP' if ret_pct > 0 else 'DOWN'

        # Last-cross-open
        if final_dir == 'UP':
            cross_mask = day_df['Low'] <= day_open
        elif final_dir == 'DOWN':
            cross_mask = day_df['High'] >= day_open
        else:
            cross_mask = pd.Series([True]*len(day_df), index=day_df.index)
        cross_idx = day_df.index[cross_mask]
        last_cross = cross_idx[-1] if len(cross_idx) > 0 else day_df.index[0]
        settled_et = day_df.loc[last_cross, 'et_time']
        mkt_open = datetime.combine(date_val, RTH_OPEN, tzinfo=ET)
        settle_min = (settled_et - mkt_open).total_seconds() / 60

        # 各候選時間點當下方向
        candidate_dirs = {}
        for min_off, label in CANDIDATE_TIMES:
            target = mkt_open + timedelta(minutes=min_off)
            before = day_df[day_df['et_time'] <= target]
            if len(before) == 0:
                candidate_dirs[label] = None; continue
            close_at_t = float(before.iloc[-1]['Close'])
            pct_at_t = (close_at_t - day_open) / day_open * 100
            if abs(pct_at_t) < DOJI_PCT:
                candidate_dirs[label] = 'DOJI'
            else:
                candidate_dirs[label] = 'UP' if pct_at_t > 0 else 'DOWN'

        result = {
            'date': date_val,
            'year': date_val.year,
            'open': day_open,
            'close': day_close,
            'return_pct': round(ret_pct, 2),
            'direction': final_dir,
            'settle_time': settled_et.strftime('%H:%M'),
            'settle_min': round(settle_min, 0),
        }
        for label in [l for _, l in CANDIDATE_TIMES]:
            result[f'dir_{label}'] = candidate_dirs.get(label)
        results.append(result)
    return pd.DataFrame(results)


def compute_false_settlement(df_res: pd.DataFrame) -> dict:
    """計算各時間點的「假抵定率」

    假抵定率 = P(dir@t != final_dir | 排除 dir@t==DOJI)
    """
    df = df_res[df_res['direction'].isin(['UP', 'DOWN'])].copy()
    rates = {}
    for _, label in CANDIDATE_TIMES:
        col = f'dir_{label}'
        sub = df[df[col].isin(['UP', 'DOWN'])]
        if len(sub) == 0:
            rates[label] = {'n': 0, 'correct_pct': 0, 'false_pct': 0}
            continue
        correct = (sub[col] == sub['direction']).mean() * 100
        rates[label] = {
            'n': len(sub),
            'correct_pct': round(correct, 1),
            'false_pct': round(100 - correct, 1),
        }
    return rates


def per_etf_detail(df_res: pd.DataFrame, ticker: str) -> dict:
    """單檔詳細統計"""
    n_total = len(df_res)
    by_dir = df_res['direction'].value_counts().to_dict()
    n_up = by_dir.get('UP', 0)
    n_down = by_dir.get('DOWN', 0)
    n_doji = by_dir.get('DOJI', 0)

    # Settlement stats
    df_dir = df_res[df_res['direction'].isin(['UP', 'DOWN'])]
    settle = df_dir['settle_min']

    # 平均報酬
    ret_up = df_dir[df_dir['direction']=='UP']['return_pct']
    ret_down = df_dir[df_dir['direction']=='DOWN']['return_pct']

    # 最佳/最差日
    best = df_dir.nlargest(3, 'return_pct')[['date','return_pct','settle_time']]
    worst = df_dir.nsmallest(3, 'return_pct')[['date','return_pct','settle_time']]

    # 按年份
    by_year = {}
    for yr, yr_df in df_res.groupby('year'):
        if len(yr_df) < 20: continue
        ydir = yr_df[yr_df['direction'].isin(['UP', 'DOWN'])]
        if len(ydir) == 0: continue
        by_year[yr] = {
            'n': len(ydir),
            'up_pct': (ydir['direction']=='UP').mean() * 100,
            'median_settle': ydir['settle_min'].median(),
            'avg_return_up': ydir[ydir['direction']=='UP']['return_pct'].mean(),
            'avg_return_down': ydir[ydir['direction']=='DOWN']['return_pct'].mean(),
        }

    # 假抵定率
    false_rates = compute_false_settlement(df_res)

    return {
        'ticker': ticker, 'days': n_total,
        'up': n_up, 'down': n_down, 'doji': n_doji,
        'median_settle': settle.median(),
        'p25_settle': settle.quantile(0.25),
        'p75_settle': settle.quantile(0.75),
        'avg_return_up': ret_up.mean(),
        'avg_return_down': ret_down.mean(),
        'max_up': ret_up.max() if len(ret_up)>0 else 0,
        'max_down': ret_down.min() if len(ret_down)>0 else 0,
        'best_days': best.to_dict('records'),
        'worst_days': worst.to_dict('records'),
        'by_year': by_year,
        'false_rates': false_rates,
    }


def print_etf_report(detail: dict):
    """單檔 detailed 報告"""
    tk = detail['ticker']
    print(f'\n{"═"*70}')
    print(f'  📊 {tk} — 詳細報告')
    print(f'{"═"*70}')

    print(f'\n  ── 基本統計 ──')
    print(f'  總交易日: {detail["days"]} (UP: {detail["up"]} / DOWN: {detail["down"]} / Doji: {detail["doji"]})')
    up_rate = detail["up"] / max(1, detail["up"]+detail["down"]) * 100
    print(f'  UP 比率: {up_rate:.1f}%')

    print(f'\n  ── 抵定時刻 (min after 09:30 ET) ──')
    print(f'  Median: {detail["median_settle"]:.0f} min  (≈ {_to_clock(detail["median_settle"])} ET)')
    print(f'  p25:    {detail["p25_settle"]:.0f} min  (≈ {_to_clock(detail["p25_settle"])} ET)')
    print(f'  p75:    {detail["p75_settle"]:.0f} min  (≈ {_to_clock(detail["p75_settle"])} ET)')

    print(f'\n  ── UP/DOWN day 報酬 ──')
    print(f'  UP 平均:   {detail["avg_return_up"]:+.2f}%   (最大: {detail["max_up"]:+.2f}%)')
    print(f'  DOWN 平均: {detail["avg_return_down"]:+.2f}%   (最小: {detail["max_down"]:+.2f}%)')

    print(f'\n  ── 假抵定率（看到方向但收盤反轉）──')
    print(f'  {"時間":<6} {"樣本":>5} {"正確%":>7} {"假抵定%":>9} {"視覺":<30}')
    for _, label in CANDIDATE_TIMES:
        r = detail['false_rates'][label]
        if r['n'] == 0: continue
        bar = '█' * int(r['correct_pct'] / 4)
        marker = '✅' if r['correct_pct']>=85 else ('🟡' if r['correct_pct']>=75 else '🔴')
        print(f'  {label:<6} {r["n"]:>5} {r["correct_pct"]:>6.1f}% {r["false_pct"]:>8.1f}% {bar:<25} {marker}')

    print(f'\n  ── 按年份 ──')
    if detail['by_year']:
        print(f'  {"年份":<6} {"Days":>5} {"UP%":>6} {"Settle":>7} {"UP avg":>8} {"DOWN avg":>9}')
        for yr in sorted(detail['by_year'].keys()):
            y = detail['by_year'][yr]
            print(f'  {yr:<6} {y["n"]:>5} {y["up_pct"]:>5.1f}% '
                  f'{y["median_settle"]:>6.0f}m '
                  f'{y["avg_return_up"]:>+7.2f}% '
                  f'{y["avg_return_down"]:>+8.2f}%')

    print(f'\n  ── Top 3 暴漲日 ──')
    for d in detail['best_days']:
        print(f'    {d["date"]}: {d["return_pct"]:+.2f}%  (settled {d["settle_time"]} ET)')
    print(f'\n  ── Top 3 暴跌日 ──')
    for d in detail['worst_days']:
        print(f'    {d["date"]}: {d["return_pct"]:+.2f}%  (settled {d["settle_time"]} ET)')


def _to_clock(min_after_open):
    h = 9 + int(min_after_open + 30) // 60
    m = int(min_after_open + 30) % 60
    return f'{h:02d}:{m:02d}'


def print_overall_report(all_details):
    """跨 ETF 整體報告"""
    print(f'\n\n{"═"*78}')
    print(f'  🌍 整體報告 (跨 6 檔倍數 ETF, 3 年 RTH-only)')
    print(f'{"═"*78}')

    # 1) 抵定時間對比
    print(f'\n  ── 1. 抵定時間中位數對比 ──')
    print(f'  {"Ticker":<8} {"Days":>5} {"UP%":>6} {"Median":>7} {"p25":>5} {"p75":>5}')
    for d in sorted(all_details, key=lambda x: x['median_settle']):
        up_r = d['up']/max(1,d['up']+d['down'])*100
        print(f'  {d["ticker"]:<8} {d["days"]:>5} {up_r:>5.1f}% '
              f'{d["median_settle"]:>6.0f}m '
              f'{d["p25_settle"]:>4.0f}m '
              f'{d["p75_settle"]:>4.0f}m')

    # 2) 假抵定率交叉表
    print(f'\n  ── 2. 各時間點「方向正確率」(% of days where dir@t == final dir) ──')
    print(f'  {"Ticker":<8}', end='')
    for _, label in CANDIDATE_TIMES:
        print(f' {label:>6}', end='')
    print()
    print(f'  {"─"*8}', end='')
    for _ in CANDIDATE_TIMES:
        print('  ─────', end='')
    print()
    for d in sorted(all_details, key=lambda x: x['ticker']):
        print(f'  {d["ticker"]:<8}', end='')
        for _, label in CANDIDATE_TIMES:
            r = d['false_rates'][label]
            print(f' {r["correct_pct"]:>5.1f}%', end='')
        print()

    # 3) 各時間點 cross-ETF 平均
    print(f'\n  ── 3. 跨 ETF 平均「方向正確率」──')
    print(f'  {"時間":<7} {"平均正確%":>10} {"≥85% 檔數":>10} {"視覺"}')
    for _, label in CANDIDATE_TIMES:
        correct_list = [d['false_rates'][label]['correct_pct'] for d in all_details
                        if d['false_rates'][label]['n'] > 0]
        if not correct_list: continue
        avg = sum(correct_list)/len(correct_list)
        n_85 = sum(1 for c in correct_list if c >= 85)
        bar = '█' * int(avg/2.5)
        marker = '🟢' if avg>=85 else ('🟡' if avg>=75 else '🔴')
        print(f'  {label:<7} {avg:>8.1f}% {n_85:>6}/{len(correct_list)}    {bar:<30} {marker}')

    # 4) 推薦的「保守 entry 時刻」
    print(f'\n  ── 4. 建議 entry 時刻 ──')
    print(f'  跨 ETF 平均方向正確率：')
    for _, label in CANDIDATE_TIMES:
        correct_list = [d['false_rates'][label]['correct_pct'] for d in all_details
                        if d['false_rates'][label]['n'] > 0]
        if not correct_list: continue
        avg = sum(correct_list)/len(correct_list)
        if 84 <= avg <= 87:
            print(f'    📍 {label} ET ({avg:.1f}% 正確) — 較保守 entry 時機')


def main():
    if not _has_alpaca():
        print('❌ Alpaca 未設定')
        return

    print(f'═'*70)
    print('  倍數 ETF 抵定時刻 — 詳細分析 + 假抵定率')
    print('═'*70)
    print(f'方法: Last-Cross-Open ｜ 樣本: 3 年 ｜ 時段: 09:30-16:00 ET')
    print(f'假抵定率定義: P(dir@t != final_dir)')

    all_details = []
    t0 = time.time()

    for tk in ETFS:
        print(f'\n[{tk}] 抓 + 分析中...')
        df = fetch_5m_history(tk, years=YEARS)
        if df is None or len(df) < 1000:
            print(f'  ⚠️  資料不足')
            continue
        df_rth = prep_df(df)
        res = analyze_per_day(df_rth)
        if len(res) == 0:
            print(f'  ⚠️  無有效 day')
            continue

        detail = per_etf_detail(res, tk)
        all_details.append(detail)
        print_etf_report(detail)

        # 存 CSV
        res.to_csv(f'settle_detail_{tk}.csv', index=False, encoding='utf-8-sig')

    if all_details:
        print_overall_report(all_details)

    print(f'\n總耗時 {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
