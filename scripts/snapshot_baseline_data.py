"""下載固定的歷史資料快照，作為 RS 驗證測試的基準

執行：python scripts/snapshot_baseline_data.py
"""
import os
import sys
import yfinance as yf
import pandas as pd

TICKERS = ['NVDA', 'MSFT', 'AAPL', 'SPY', '^GSPC',
            '2330.TW', '^TWII',
            'RDDT',  # 2024-03-21 IPO（短歷史測試）
            ]

OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'tests', 'rs_validation')
OUT_PATH = os.path.join(OUT_DIR, 'baseline_snapshot.parquet')


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f'下載 {len(TICKERS)} 個 ticker...')
    df = yf.download(TICKERS,
                       start='2023-01-01', end='2024-12-31',
                       auto_adjust=True, progress=True,
                       group_by='ticker')
    print(f'shape: {df.shape}')
    print(f'columns: {df.columns.tolist()[:8]} ...')
    df.to_parquet(OUT_PATH)
    print(f'\nSaved: {OUT_PATH}')

    # 簡單驗證
    df_loaded = pd.read_parquet(OUT_PATH)
    print(f'重新讀取 OK, shape: {df_loaded.shape}')


if __name__ == '__main__':
    main()
