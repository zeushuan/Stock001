# Stock001 RS 計算規格書

> **文件版本**：v1.0
> **建立日期**：2026-05-12
> **目的**：明確記錄 Stock001 採用的 RS 定義、實作位置、設計決策與已知限制

---

## 1. 採用的 RS 定義

Stock001 採用的是 **IBD / Mark Minervini 風格的 universe-wide percentile RS Rating**，**不是** TraderLion 的 RS Line。

### 1.1 公式

```
score(ticker) = 2 × r13w(ticker) + 1 × r26w(ticker) + 1 × r39w(ticker) + 1 × r52w(ticker)

RS_Rating(ticker) = percentile_rank(score(ticker), all_tickers) × 100
```

其中 `rNw` 是 N 週期間的累積百分比報酬（用 daily close）：

```
rNw = (Close_today / Close_today-Ndays - 1) × 100
```

- 13w ≈ 65 trading days
- 26w ≈ 130 trading days
- 39w ≈ 195 trading days
- 52w ≈ 252 trading days

### 1.2 與其他常見 RS 的差異

| 定義 | Stock001？ | 說明 |
|---|---|---|
| **IBD RS Rating（百分位 1-99）** | ✅ 採用（部分） | 是百分位，但 IBD 經典用 12-1 動能；Stock001 用 Minervini 4 期加權 |
| **Mark Minervini 4 期加權** | ✅ 完全採用 | 來源 Trade Like a Stock Market Wizard 第 7 章 |
| **TraderLion RS Line（Price/Index 比值）** | ❌ 未實作 | Stock001 沒有「股票 vs 指數」的時間序列比值 |
| **Mansfield RS（zero-line oscillator）** | ❌ 未實作 | 沒有圍繞零軸的振盪指標 |
| **學術 12-1 動能因子** | ❌ 未實作 | 沒有「過去 12 個月排除最近 1 個月」 |

---

## 2. 程式碼位置

| 函式 | 檔案 | 用途 |
|---|---|---|
| `compute_returns(df, periods_days)` | `sepa_vcp.py:269` | 計算單支股票的多期間百分比報酬 |
| `compute_rs_ratings(returns_dict, weights)` | `sepa_vcp.py:226` | 用全 universe 報酬計算 percentile RS Rating |
| `calculate_rs_line(stock, index)` | `rs_line.py`（新增）| TraderLion 風格 RS Line（給 Phase 1 測試用） |

呼叫鏈：

```
screener_full_cloud.py / screener_full_local.py
  ├─ 對每個 ticker 算 compute_returns(df) → returns_dict[ticker]
  └─ 跨全 universe 算 compute_rs_ratings(returns_dict)
       → 結果寫入 screener_results.json['rs_ratings']
```

---

## 3. 設計決策

### 3.1 比對基準

- **不比對大盤指數**（不像 TraderLion RS Line）
- 比對 = **同 universe 內所有股票互相 percentile**
- universe 分為 **TW universe**（~700 ticker）和 **US universe**（~2300 ticker）
- 兩個 universe **獨立計算** rank（一檔台股的 RS Rating = 在 TW 內的百分位，不會跟美股混算）

### 3.2 價格資料來源

- yfinance 用 `auto_adjust=True` → 拿到 **還原權息後** 的 close
- 已自動處理：股票分割、現金股息（不必再除權息）
- 台股額外用 `fetch_tw_official`（FinMind）抓官方收盤價當 fallback

### 3.3 缺失資料處理策略

| 情境 | 處理 |
|---|---|
| 該 ticker `df` 為空或 < 30 bars | `compute_returns` 返回 `{}`，不參與 RS 排名 |
| 過去 N 日 close 為 0 或 NaN | 該期間 return = 0（不丟例外） |
| `returns_dict[ticker]` 為空 dict | `compute_rs_ratings` 跳過該 ticker |

### 3.4 加權設計（Minervini 原則）

```
weights = (2, 1, 1, 1)
```

- 短期（13w）權重最重 = 反應近期動能變化
- 長期（52w）權重正常 = 反應趨勢持續性
- 整體偏向「近期表現好的股票」優先

---

## 4. 已知限制

### 4.1 universe 偏小
- IBD 用 ~6000 美股 → Stock001 用 ~2300 → percentile 在絕對值上會偏高
- **但排名順序應該一致**（同樣的股票相對位置不變）

### 4.2 不適合 cross-market comparison
- TW 的 RS=80 ≠ US 的 RS=80（不同 universe）
- 跨市場排名沒有共同基準

### 4.3 無時間序列概念
- 只有「今天的 RS」（截面快照），不像 RS Line 有歷史曲線
- 不能畫「RS 趨勢」或「RS 領先新高」

### 4.4 短歷史股票
- 上市未滿 252 個交易日的股票：較長期間 return = 0（不是 N/A）
- 這會讓 score 偏低，RS 排名靠後 → 不會有「靜默誤導」的問題

---

## 5. 與 task 指示書的對應

由於指示書範本 `calculate_rs_line(stock, index)` 與 Stock001 的 `compute_rs_ratings(returns_dict)` 語意不同，本驗證任務：

1. **新增** `rs_line.py` 實作 TraderLion 風格 RS Line（給 Phase 1 / Phase 2 用）
2. **保留** 既有 `compute_rs_ratings` 為主要生產環境使用
3. **Phase 1** 同時測試兩種 RS（synthetic）
4. **Phase 2** 用 RS Line 對照 TraderLion（容易視覺驗證）

---

## 6. 修改歷史

| 版本 | 日期 | 變更 |
|---|---|---|
| v1.0 | 2026-05-12 | 首版規格書，識別現有實作 |
