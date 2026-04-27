"""
VWAP 載入器
============
從 Fugle 抓 5-min bar 計算每日 VWAP（成交量加權平均價）。

VWAP = sum(price × volume) / sum(volume)
其中 price 用 typical price = (H + L + C) / 3

輸出：
  vwap_cache/{ticker}.parquet  index=date, cols=VWAP / Volume

策略應用：
  - 進場：當收盤價 < 當日 VWAP → 進場（買在均價以下）
  - 出場：當收盤價 > 當日 VWAP → 出場（賣在均價以上）
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import argparse
import time
from pathlib import Path
import numpy as np
import pandas as pd

from fugle_connector import get_minute_candles

CACHE_DIR = Path(__file__).parent / 'vwap_cache'
CACHE_DIR.mkdir(exist_ok=True)


def compute_daily_vwap(minute_df: pd.DataFrame) -> pd.DataFrame:
    """從分鐘 bar 計算每日 VWAP。
    輸入：DataFrame index=Timestamp, cols=Open/High/Low/Close/Volume
    輸出：DataFrame index=Date, cols=VWAP/Volume/HighOfDay/LowOfDay/Close
    """
    if minute_df is None or minute_df.empty: return None
    df = minute_df.copy()
    df.index = pd.to_datetime(df.index)
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df['typical'] = (df['High'] + df['Low'] + df['Close']) / 3
    df['pv'] = df['typical'] * df['Volume']

    # 按日聚合（用 _day 避免與 index 名稱衝突）
    df = df.reset_index().rename(columns={df.index.name or 'index': 'ts'})
    if 'date' in df.columns:
        df = df.rename(columns={'date': 'ts'})
    df['_day'] = pd.to_datetime(df['ts']).dt.date
    daily = df.groupby('_day').agg(
        VWAP_num=('pv', 'sum'),
        VWAP_den=('Volume', 'sum'),
        Volume=('Volume', 'sum'),
        HighOfDay=('High', 'max'),
        LowOfDay=('Low', 'min'),
        Close=('Close', 'last'),
        Open=('Open', 'first'),
    )
    daily['VWAP'] = np.where(
        daily['VWAP_den'] > 0,
        daily['VWAP_num'] / daily['VWAP_den'],
        daily['Close'],
    )
    daily.index = pd.to_datetime(daily.index)
    return daily[['VWAP', 'Volume', 'HighOfDay', 'LowOfDay', 'Open', 'Close']]


def fetch_and_cache_vwap(ticker: str, start='2020-01-01', end='2026-04-27',
                         freq='5m', force=False) -> pd.DataFrame:
    """抓 Fugle 5-min bar → 計算 VWAP → 快取"""
    cache_path = CACHE_DIR / f'{ticker}.parquet'
    if cache_path.exists() and not force:
        try:
            return pd.read_parquet(cache_path)
        except: pass

    print(f"  [{ticker}] 抓 {freq} bar...", end=' ', flush=True)
    minute_df = get_minute_candles(ticker, start=start, end=end,
                                    freq=freq, use_cache=False)
    if minute_df is None or minute_df.empty:
        print('❌')
        return None

    vwap_df = compute_daily_vwap(minute_df)
    if vwap_df is None:
        print('❌ 計算失敗')
        return None

    vwap_df.to_parquet(cache_path)
    print(f'✅ {len(vwap_df)} 日')
    return vwap_df


# ─── 全市場下載（樣本版）─────────────────────────────────────
SAMPLE_TICKERS = [
    # 半導體 + 大型權值（先試 30 檔最有交易量的）
    '2330', '2317', '2454', '2412', '2308', '2882', '2891', '2886',
    '2884', '2603', '2610', '3008', '6505', '2882', '2885', '2880',
    '2883', '3034', '2474', '2382', '6669', '8081', '2207', '2002',
    '1216', '1303', '1101', '2615', '2618', '2890', '2887', '5871',
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2020-01-01')
    ap.add_argument('--end', default='2026-04-27')
    ap.add_argument('--freq', default='5m', help='5m / 15m / 60m')
    ap.add_argument('--tickers', default=None, help='comma-separated')
    ap.add_argument('--max', type=int, default=None)
    ap.add_argument('--info', action='store_true')
    args = ap.parse_args()

    if args.info:
        files = sorted(CACHE_DIR.glob('*.parquet'))
        total = sum(f.stat().st_size for f in files)
        print(f"VWAP cache: {len(files)} 檔 / {total/1024:.0f} KB")
        if files:
            sample = pd.read_parquet(files[0])
            print(f"\n樣本 {files[0].stem}：")
            print(sample.tail(3))
        return

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',')]
    else:
        tickers = SAMPLE_TICKERS
    if args.max: tickers = tickers[:args.max]
    tickers = list(dict.fromkeys(tickers))  # de-dup

    print(f"[抓 VWAP] {len(tickers)} 檔 × {args.start} ~ {args.end} freq={args.freq}\n")
    t0 = time.time()
    success = 0
    for i, t in enumerate(tickers):
        df = fetch_and_cache_vwap(t, start=args.start, end=args.end,
                                   freq=args.freq)
        if df is not None: success += 1
        if (i+1) % 5 == 0:
            print(f"  進度 {i+1}/{len(tickers)} ok={success}")

    print(f"\n總耗時 {(time.time()-t0)/60:.1f} min  成功 {success}/{len(tickers)}")


if __name__ == '__main__':
    main()
