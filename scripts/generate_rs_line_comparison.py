"""Phase 2 — 生成 Stock001 RS Line 圖以對照 TraderLion 截圖

使用 baseline_snapshot.parquet 對 NVDA / MSFT / 2330 / AAPL 算 RS Line，
產出 docs/rs_validation_report/stock001_<ticker>.png 供視覺對照。

時間區間：2024-01-02 → 2024-06-28
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matplotlib
matplotlib.use('Agg')
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib as mpl

from rs_line import calculate_rs_line, detect_rs_new_high

mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei',
                                    'SimHei', 'Arial Unicode MS']
mpl.rcParams['axes.unicode_minus'] = False

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        'docs', 'rs_validation_report')
SNAPSHOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                'tests', 'rs_validation', 'baseline_snapshot.parquet')

PAIRS = [
    ('NVDA',    '^GSPC'),
    ('MSFT',    '^GSPC'),
    ('AAPL',    '^GSPC'),
    ('2330.TW', '^TWII'),
]

START = '2024-01-02'
END = '2024-06-28'


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    snap = pd.read_parquet(SNAPSHOT_PATH)
    print(f'Snapshot shape: {snap.shape}')

    for ticker, index_tk in PAIRS:
        if (ticker, 'Close') not in snap.columns:
            print(f'  Skip {ticker}: not in snapshot')
            continue
        if (index_tk, 'Close') not in snap.columns:
            print(f'  Skip {ticker}: {index_tk} not in snapshot')
            continue
        stock_close = snap[(ticker, 'Close')].dropna()
        index_close = snap[(index_tk, 'Close')].dropna()

        # 切時間區間
        stock_close = stock_close.loc[START:END]
        index_close = index_close.loc[START:END]

        # 計算 RS Line + new high 標記
        rs = calculate_rs_line(stock_close, index_close)
        rs_wma21 = calculate_rs_line(stock_close, index_close, smooth_wma=21)
        is_high = detect_rs_new_high(rs, lookback=50)

        # 繪圖：上股價，下 RS Line
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8),
                                          gridspec_kw={'height_ratios': [2, 1]},
                                          sharex=True)
        ax1.plot(stock_close.index, stock_close.values,
                 color='#222', linewidth=1.3, label=f'{ticker} Close')
        ax1.set_title(f'{ticker} vs {index_tk} — Stock001 RS Line ({START} ~ {END})',
                       fontsize=12, fontweight='bold')
        ax1.set_ylabel(f'{ticker} Price')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)

        ax2.plot(rs.index, rs.values, color='#0066cc', linewidth=1.4,
                  label='RS Line (Price / Index)')
        ax2.plot(rs_wma21.index, rs_wma21.values, color='#ff6b35',
                  linewidth=1.0, linestyle='--', label='21d WMA')
        # 紫色點：RS 創新高（50d）
        purple_x = rs.index[is_high]
        purple_y = rs.values[is_high.values]
        ax2.scatter(purple_x, purple_y, color='#b266ff', s=40,
                     zorder=5, label='RS 創 50d 新高 (紫色點)')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('RS Line')
        ax2.legend(loc='upper left', fontsize=9)
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_locator(mdates.MonthLocator())
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        plt.tight_layout()
        out_path = os.path.join(OUT_DIR,
                                 f'stock001_{ticker.replace(".","_")}.png')
        plt.savefig(out_path, dpi=110, bbox_inches='tight')
        plt.close()
        print(f'  Saved: {out_path}')

    # 寫一個 comparison_report.md 模板
    report = """# RS Line 視覺對照報告

> 用 baseline_snapshot.parquet（2023-2024 固定資料）
> 時間區間 2024-01-02 ~ 2024-06-28
> Stock001 圖已生成；TraderLion 截圖需用戶手動補

## 對照清單

| 標的 | Stock001 圖 | TraderLion 截圖 | 比對結果 |
|---|---|---|---|
| NVDA | [stock001_NVDA.png](stock001_NVDA.png) | _(待補)_ | _(待人工檢視)_ |
| MSFT | [stock001_MSFT.png](stock001_MSFT.png) | _(待補)_ | _(待人工檢視)_ |
| AAPL | [stock001_AAPL.png](stock001_AAPL.png) | _(待補)_ | _(待人工檢視)_ |
| 2330.TW | [stock001_2330_TW.png](stock001_2330_TW.png) | _(待補)_ | _(待人工檢視)_ |

## 比對要點

1. **RS Line 形狀**：整體走勢方向與轉折是否一致
2. **重要高低點**：日期誤差 ≤ ±1 個交易日
3. **紫色點位置**：RS Line 創 50d 新高的點，位置是否吻合

## 已知差異（可接受）

1. **絕對數值**：TraderLion 可能有正規化（除以某基準日）；Stock001 是純比值
2. **WMA 平滑**：兩邊 WMA 期數可能不同（Stock001 用 21d）

## 截圖步驟（給 Eddy）

1. 打開 TradingView，載入 TraderLion Relative Strength 指標
2. 對 NVDA / MSFT / AAPL 用 SPX 為基準；2330 用 TWII 為基準
3. 時間區間切到 2024-01-02 → 2024-06-28
4. 截圖保存為 `docs/rs_validation_report/tradingview_<ticker>.png`
5. 與本目錄下 stock001_<ticker>.png 並排對照填寫結果
"""
    with open(os.path.join(OUT_DIR, 'comparison_report.md'), 'w', encoding='utf-8') as f:
        f.write(report)
    print(f'\nComparison report: {os.path.join(OUT_DIR, "comparison_report.md")}')


if __name__ == '__main__':
    main()
