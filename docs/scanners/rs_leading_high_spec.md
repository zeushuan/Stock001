# RS Leading High Scanner — 規格文件

> **版本**：v1.0
> **建立日期**：2026-05-12
> **狀態**：Phase 1-5 完成，20 個測試全綠

---

## 1. 訊號定義

**RS 線創新高，但股價尚未創新高** — TraderLion 紫色點訊號。

對應 William O'Neil / Mark Minervini 視為機構累積足跡。

### 三個必要條件

| 條件 | 預設 | 可調參數 |
|---|---|---|
| A. RS 在最近 N₁ 日內，創過 N₂ 期間新高 | N₁=5, N₂=63 | `--recency`, `--lookback` |
| B. 股價在 N₂ 期間「未」創新高，且距高點 ≥ N % | N=3% | `--min-distance` |
| C. 資料長度足夠 | ≥ 63 bars | （自動） |

---

## 2. 計算流程

```
detect_rs_leading_high(stock, index, vol, ticker, as_of_date, ...)
    │
    ├─ tz 正規化 + 對齊到共同交易日
    ├─ 切片到 as_of_date（inclusive；無 look-ahead）
    ├─ calculate_rs_line(stock, index)   ← 用 Phase 1 驗證過的 RS Line
    ├─ 檢查條件 A / B / C
    └─ 計算附加指標：
        - purple_dot_count (近 N 日 RS 創高次數)
        - rs_above_wma21, rs_slope_50d, rs_long_term_trend_up
        - volume_ratio (當日 / 50d 平均)
        - above_sma200, dollar_volume_50d
    → RSLeadingHighSignal

apply_quality_filters(signal, ...)
    F1. RS 50d slope > 0
    F2. RS > 21d WMA
    F3. 股價 > 200d SMA
    F4. 流動性 ≥ 門檻（US $10M, TW NT$50M）
    F5. 上市滿一年
    → True/False + 失敗原因列表

score_signal(signal, universe_context)
    維度 1: 紫色點頻率（0-20）
    維度 2: RS 斜率強度（截面標準化，0-20）
    維度 3: 股價整理品質（CV 倒數，0-20）
    維度 4: 成交量結構（量 ratio，0-20）
    維度 5: 距離高點程度（5-15% 滿分，倒 U 型，0-20）
    → 總分 0-100
```

---

## 3. CLI 使用範例

```bash
# 美股 SP500 今日掃描，console + JSON 輸出
python -m scanners.rs_leading_high --market US --universe SP500 --top-n 30

# 台股 TW50 指定日期
python -m scanners.rs_leading_high --market TW --universe TW50 --date 2024-04-15

# 自訂參數
python -m scanners.rs_leading_high --market US --lookback 252 --min-distance 0.05

# 多格式輸出
python -m scanners.rs_leading_high --market US --output console,excel,json

# 對指定主題掃描
python -m scanners.rs_leading_high --market US --universe EDDY_AI_STORAGE
python -m scanners.rs_leading_high --market US --universe EDDY_AI_ENERGY
```

---

## 4. 程式碼位置

| 檔案 | 角色 |
|---|---|
| `scanners/rs_leading_high.py` | 核心偵測 + filter + scoring + CLI |
| `universes/us_universe.py` | US 宇宙（SP500/RUSSELL1000/LIQUID_3000 + Eddy 主題） |
| `universes/tw_universe.py` | TW 宇宙（TW50/TW0050_0056/LIQUID_TW） |
| `integrations/t3_export.py` | T3 系統 JSON schema 整合 |
| `integrations/excel_export.py` | Excel 報表（summary + signals sheet） |
| `backtests/rs_leading_high_backtest.py` | Phase 5 歷史回測 |
| `tests/scanners/test_rs_leading_high.py` | 20 個單元/整合測試 |
| `rs_line.py` | TraderLion 風格 RS Line API（補完任務） |

---

## 5. 輸出 schema

### JSON (T3 標準介面)

```json
{
  "metadata": {
    "schema_version": "1.0",
    "generated_at": "2026-05-12T15:00:00",
    "scanner": "rs_leading_high",
    "total_signals": 30,
    "source": "Stock001"
  },
  "features": [
    {
      "ticker": "NVDA",
      "asof_date": "2026-05-12",
      "rank": 1,
      "technical": {
        "rs_leading_high_score": 85.5,
        "rs_value": 2.18,
        "rs_lookback_high": 2.18,
        "days_since_rs_high": 0,
        "purple_dot_count": 7,
        "stock_price": 215.20,
        "distance_from_high_pct": 0.082,
        "rs_above_wma21": true,
        "rs_long_term_trend_up": true,
        "volume_ratio": 1.32,
        "rs_slope_50d": 0.00045,
        "above_sma200": true,
        "dollar_volume_50d": 9.5e9
      },
      "score_breakdown": {...},
      "themes": ["AI_storage"],
      "passed_quality_filters": true
    }
  ]
}
```

### Excel

- **summary sheet**：scan 元資料 + 統計
- **signals sheet**：完整訊號表（rank, ticker, score, breakdown, …）

---

## 6. 主題分類（Eddy 整合）

訊號自動標記是否屬於以下主題：

| 主題 | 標的 |
|---|---|
| `AI_storage` | MU / SNDK / STX / WDC |
| `AI_energy` | CEG / VST / GEV / PWR / OKLO / BE / CCJ |

CLI 也可直接掃 `--universe EDDY_AI_STORAGE` 或 `EDDY_AI_ENERGY`。

---

## 7. 已知限制

### 7.1 訊號是必要條件而非充分條件
RS 領先創新高 = 進場時機的一個訊號，**仍需配合**：
- 突破關鍵價位（pivot point）
- 量價配合（突破日帶量）
- 大盤環境（不要逆勢買進）

### 7.2 延伸風險（Extension Risk）
本掃描器以「距離高點 5-15% 滿分」處理，但仍需注意：
- 距離 < 3% → 訊號可能已過時
- 距離 > 25% → 整理未完成或結構受損

### 7.3 短歷史限制
新上市 < 252 日的個股預設排除（`--include-ipos` 可改）。

### 7.4 Survivorship Bias
本框架支援 `save_snapshot` / `_load_snapshot`，但目前 SP500/R1000 用核心 50 檔，不是完整 500/1000 檔，且未做歷史月度快照。**Phase 5 回測結果應視為「現在版本宇宙的歷史 backtest」**，有 survivorship 偏誤的風險。

### 7.5 統計顯著性
Phase 5 回測在 SP500 樣本上：
- 21d hold: +5.82% alpha, p=0.20 → **不顯著**
- 63d hold: +2.59% alpha, p=0.66 → **不顯著**
- 126d hold: +4.89% alpha, p=0.74 → **不顯著**

→ 訊號平均報酬看似正向，但樣本數不足以斷言「統計顯著」。詳見回測報告。

---

## 8. 參數預設值（已驗證）

```python
rs_new_high_lookback = 63            # 3 月（中期動能 SNR 最佳）
rs_high_recency_days = 5             # RS 新高需在最近 5 日內
min_distance_from_price_high = 0.03  # 股價距高點 ≥ 3%
purple_dot_window = 20               # 紫色點頻率視窗
min_dollar_vol_us = 1e7              # $10M
min_dollar_vol_tw = 5e7              # NT$50M
```

所有參數可透過 CLI 覆寫或函式呼叫覆寫。
