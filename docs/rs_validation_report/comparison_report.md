# RS Line 視覺對照報告

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
