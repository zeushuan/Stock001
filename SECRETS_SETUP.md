# Streamlit Cloud Secrets 設定指南

> 設定後雲端 v9.4 就會自動使用 Fugle API（包含 VWAP 進出場建議）。
> 若沒設定，會自動 fallback 到 yfinance（沒有 VWAP 功能）。

---

## 一、Streamlit Cloud 上設定

### Step 1：登入 Streamlit Cloud
- 進入 https://share.streamlit.io
- 找到你的 app（應該是 `tv_app` 或類似名稱）

### Step 2：開啟 Settings → Secrets
- 點 app 右下角的 **⋮ (三個點)** 或 **Manage app**
- 左側選 **Settings**
- 找到 **Secrets** 區塊（TOML 格式）

### Step 3：貼入下列內容
```toml
FUGLE_API_KEY = "Zjk0MTM3ZTctN2Q3OS00YmFmLWI3OGYtNmM5MzhjNjY4NzlkIGIyMWI5MWY2LWNmNmYtNDdlZS1hNzlhLWMxYTM0OGIyYTAwMA=="
```

### Step 4：按下 **Save**
- Streamlit 會自動重啟 app（約 30 秒）
- 重啟後 VWAP 功能就會啟用

---

## 二、本地開發保留 .env

本機可以繼續用 `.env`：

```ini
FUGLE_API_KEY=Zjk0MTM3ZTctN2Q3OS00YmFmLWI3OGYtNmM5MzhjNjY4NzlkIGIyMWI5MWY2LWNmNmYtNDdlZS1hNzlhLWMxYTM0OGIyYTAwMA==
```

讀取優先序（fugle_connector.py 自動處理）：
1. `st.secrets`（Streamlit Cloud）
2. `.env`（本地）
3. 系統環境變數
4. fallback 到 yfinance（沒 VWAP）

---

## 三、驗證設定成功

雲端：
- 開 app 進入「個股 / 操作建議」分頁，輸入 `2330.TW`
- 若有 VWAP 區塊（綠/藍背景框）→ 設定成功 ✅
- 若沒有 → 檢查 secrets 是否正確、Streamlit Cloud 是否重啟

本機：
```bash
python fugle_connector.py
```
應該看到：
```
FUGLE_API_KEY: ✅ 已設定
Fugle client: ✅ 初始化成功
```

---

## 四、安全提醒

- ✅ `.env` 已加入 `.gitignore`，不會被推上 GitHub
- ✅ Streamlit secrets 只有 app owner 看得到，不會公開
- ❌ **不要** 把 API key 寫進 source code
- ❌ **不要** 在公開對話/截圖貼出 key
- ⚠️ 若 key 外洩，到 Fugle 後台重新產生即可

---

## 五、API 額度

Fugle 個人版：
- 5-min/15-min/60-min/1day bar：免費
- 即時 quote：每日 100 次
- 歷史資料：通常 < 3 年

VWAP 主要使用 5-min historical，每天每檔個股約 1 次請求即可。

---

## 六、疑難排解

| 症狀 | 原因 | 解方 |
|------|------|------|
| Cloud 啟動失敗 | secrets 格式錯誤 | 確認 TOML 語法（key = "value"） |
| VWAP 區塊不出現 | 該檔沒有 vwap_cache | 是美股或 OTC，會自動隱藏 |
| 「Resource Not Found」 | 該股代號 Fugle 沒收錄 | 屬正常，跳過 |
| 「Rate limit exceeded」 | 太多請求同時 | 等 15 秒會自動重試 |

