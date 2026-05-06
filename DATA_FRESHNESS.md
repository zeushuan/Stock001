# 資料新鮮度保證（v9.14）

## 🎯 SLA 承諾

| 元件 | 資料來源 | 最大延遲 |
|---|---|---|
| **篩選器（screener）** | `screener_results.json`（cron 8 次/天）| **< 3 小時**（工作日）|
| **TOP 200 警報** | `top200_signals.json` + `us_top200_signals.json`（cron 2 次/天）| **< 12 小時** |
| **個股 detail card** | yfinance 即時 fetch | **即時**（< 15 分鐘延遲）|
| **TradingView 圖表** | TradingView 自家 | 即時 |

## 🔄 Cron 排程（24 小時）

工作日（週一至週五）一天執行 8 次 `weekly_full_scan.yml`：

| 台北時間 | UTC | 用途 |
|---|---|---|
| 07:00 | 23:00 (前一日) | 隔日盤前 |
| **09:00** | 01:00 | 盤前 |
| 12:00 | 04:00 | 盤中半段 |
| **13:35** | 05:35 | **台股盤後即時** |
| 17:00 | 09:00 | 晚間 1 |
| 21:00 | 13:00 | 美盤前 |
| 01:00（隔日）| 17:00 | 美盤中半段 |
| **05:05（隔日）** | 21:05 | **美股盤後即時** |

每次跑約 **15-25 分鐘**（TWSE/TPEX 官方 API + yfinance batch）。
中間最大間隔 **3 小時**（工作日中）/ 14 小時（週末）。

## 🛠️ 資料來源

### TW 股票（≈1900 檔）
- **TWSE 上市**：MI_INDEX 端點（每日全市場）
- **TPEX 上櫃**：daily_close_quotes 端點（每日全市場）
- **不依賴**：~~yfinance .TW~~（GitHub runner 抓不到）/ ~~twstock 套件~~

### US 股票（≈5600 檔）
- **yfinance**：batch=50 抓全 NASDAQ + NYSE + AMEX
- **Universe 來源**：`us_full_tickers.json`（5629 檔）

## ⚠️ 不一致時的判斷

如果**篩選器**跟 **detail card** 顯示不同：

1. 看篩選器 panel **頂部資料新鮮度** banner：
   - 🟢 < 6h：很新鮮，幾乎不會差
   - 🔵 < 24h：cross_days 可能差 1-2 天
   - 🟡 < 48h：建議手動觸發 cron
   - 🔴 ≥ 48h：cron 失敗了，請看 Actions log

2. 篩選器 row 顯示「+Xd → 今 +Yd」（推算今日 cross_days）：
   - X 是 JSON 寫入時的 cross_days
   - Y 是按 JSON 年齡推算的今日值
   - Y 跟 detail card 的 cross_days 應該很接近

## 🚨 故障排除

### 篩選器資料嚴重過時（>48h）

**手動觸發**：
1. https://github.com/zeushuan/Stock001/actions/workflows/weekly_full_scan.yml
2. Run workflow → Branch: main → Run

15-25 分鐘後完成 + 自動 commit + Streamlit Cloud 拉到。

### 看到 `tw_stock_list.json 不存在`

`tw_stock_list.json` 必須在 repo 中（v9.14 已修，移出 .gitignore）。
若再次出現，檢查 `.gitignore` 是否誤加。

### 看到 `TW: 0 檔`

代表 TWSE/TPEX 抓取失敗。檢查：
1. workflow log 中「Scan full TW + US T1 imminent」步驟有「[DEBUG] TWSE...」訊息嗎？
2. status 200 但解析 0 → 可能 TWSE 改 API 格式
3. status 4xx/5xx → IP 被擋（極少見）

### Cron 沒跑

GitHub Actions 偶爾會延遲。檢查：
- https://github.com/zeushuan/Stock001/actions
- 看是否有 queued / failed 的 run

## 📊 資料管線（v9.14）

```
TWSE/TPEX 官方 API ─┐
                   ├─→ fetch_tw_official.py
                   │          ↓
yfinance ──────────┴─→ screener_full_cloud.py (TW + US)
                              ↓
                   screener_results.json
                   t1_imminent_full.json
                              ↓
                   git auto-commit
                              ↓
                   GitHub repo
                              ↓
                   Streamlit Cloud 拉取（1-2 分鐘）
                              ↓
                   tv_app 篩選器 panel
```

## 🔧 自助檢查指令

```bash
# 看當前 JSON 多舊
python -c "
import json, datetime
d = json.load(open('screener_results.json', encoding='utf-8'))
print(f'computed_at: {d[\"computed_at\"]}')
"

# 重新 local 抓最新（需網路）
python screener_full_local.py

# 強制 push
git add screener_results.json && git commit -m 'manual refresh' && git push
```
