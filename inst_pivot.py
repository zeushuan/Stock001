"""
把每日全市場三大法人資料 pivot 為「每股時序」格式

輸入：inst_cache/{date}.parquet（每日一檔，全市場）
輸出：inst_per_ticker/{ticker}.parquet（每股一檔，時序）
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import time
from pathlib import Path

import pandas as pd

CACHE_DIR = Path(__file__).parent / 'inst_cache'
OUT_DIR = Path(__file__).parent / 'inst_per_ticker'
OUT_DIR.mkdir(exist_ok=True)


def main():
    files = sorted(CACHE_DIR.glob('*.parquet'))
    print(f"輸入：{len(files)} 個日檔")

    # 合併所有日檔
    print("合併中...")
    t0 = time.time()
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if not df.empty and 'ticker' in df.columns:
                dfs.append(df)
        except: pass
    full = pd.concat(dfs, ignore_index=True)
    full['date'] = pd.to_datetime(full['date'])
    print(f"合併完成：{len(full):,} 筆 / {time.time()-t0:.1f}s")

    # 按 ticker 分組儲存
    print("\n按 ticker 拆分...")
    t1 = time.time()
    n_saved = 0
    for ticker, sub in full.groupby('ticker'):
        if not isinstance(ticker, str): continue
        ticker = ticker.strip()
        if not ticker: continue
        sub = sub.sort_values('date').set_index('date')
        # 移除非數值欄位（除了證券名稱保留）
        keep_cols = [
            '三大法人買賣超股數',
            '外陸資買賣超股數(不含外資自營商)',
            '投信買賣超股數',
            '自營商買賣超股數',
        ]
        keep = [c for c in keep_cols if c in sub.columns]
        if not keep: continue
        sub_clean = sub[keep].copy()
        # 轉 numeric
        for c in keep:
            sub_clean[c] = pd.to_numeric(sub_clean[c], errors='coerce')
        sub_clean.to_parquet(OUT_DIR / f'{ticker}.parquet')
        n_saved += 1

    print(f"完成：{n_saved} 檔股票 / {time.time()-t1:.1f}s")
    print(f"輸出目錄：{OUT_DIR}")


if __name__ == '__main__':
    main()
