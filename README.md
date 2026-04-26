# Stock001 — 台股全市場策略回測系統

> 自動辨別台股 / 美股 / 指數 ｜ 6 種策略並行回測 ｜ 金字塔加碼架構 ｜ 38× 平行加速

[![Strategy](https://img.shields.io/badge/Strategy-v8-3b9eff)](https://github.com/zeushuan/Stock001)
[![Performance](https://img.shields.io/badge/v8%20P0__T1T3%20%2B197.48%25-success)](https://github.com/zeushuan/Stock001)
[![Walk-forward](https://img.shields.io/badge/Walk--forward-Validated-green)](https://github.com/zeushuan/Stock001)

---

## 📊 績效一覽

**全市場 1263 檔台股，2020-01-02 ~ 2026-04-25（含 COVID/AI 兩大週期）**

| 策略 | 均值報酬 | 最大單股獲利 | 最大單股損失 | 風報比 |
|------|---------|------------|------------|------|
| ① 持有 BH | +163.94% | +8390% | -96% | 1.71 |
| ⑦ v7 base | +72.99% | +2441% | -140% | 0.52 |
| **v8 P0_T1T3** | **+197.48%** ⭐ | +10299% | -290% | 0.68 |
| **v8 P0_T1T3+CB30** ★生產推薦 | **+134.07%** | +3376% | **-166%** | **0.81** |
| v8 P0_T1T3+PS | +110.26% | +6718% | -149% | 0.74 |

✅ **Walk-forward 驗證**：EARLY (2020-2022) +54.6% vs LATE (2023-2026) +54.4% — Edge 結構穩定，非過擬合
✅ **扣 0.4275% 台股交易成本**：P0_T1T3 仍 +184.33%

---

## 🚀 快速開始

### 安裝依賴
```bash
pip install yfinance pandas numpy ta pyarrow streamlit openpyxl requests
```

### 第一次跑（建立快取，~3 分鐘）
```bash
python backtest_tw_all.py
```

### 跑變體比較（已快取後）
```bash
# 單變體全市場（~12 秒）
python v8_runner.py --mode P0_T1T3+CB30 --workers 12

# 多變體並行測試（~20 秒）
python batch_test.py --variants base,P0_T1T3,P0_T1T3+CB30 --workers-per 4

# 每日信號掃描（~3 秒）
python daily_scanner.py --top 30
```

### 啟動 Web UI（即時掃描器）
```bash
streamlit run tv_app.py
```

---

## 🏗️ 架構

### 三層平行化（38× 加速）

```
原本 backtest_tw_all：3.4 分鐘  →  v8 三層架構：11 秒（單變體）/ 21 秒（4 變體）

L1：磁碟快取（data_loader.py）
   parquet 格式儲存所有股票指標 (~230MB / 1263 檔)
   12 小時 TTL，自動失效重抓

L2：ProcessPool 突破 GIL（v8_runner.py）
   12 物理核心真正平行
   分析階段：50s → 5s

L3：變體級平行（batch_test.py）
   subprocess 同時啟動多個 runner
   4 變體 × 4 workers = 16 邏輯核心全用滿
```

### 程式碼結構

```
backtest_all.py          v7 主策略（6 種策略並行）
backtest_tw_all.py       全市場批次回測（建快取）
data_loader.py           parquet 快取層
variant_strategy.py      v8 參數化策略（含金字塔加碼）
v8_runner.py             單變體 ProcessPool 執行器
batch_test.py            多變體並行測試器
walk_forward.py          E1 walk-forward + 交易成本
monte_carlo.py           E2 參數敏感度
survivorship_analysis.py E3 下市股偏差估計
daily_scanner.py         F3 每日信號掃描器
tv_app.py                Streamlit Web UI
verify_consistency.py    一致性驗證
```

---

## 📈 v7 → v8 演進

### v7 base（單倉位策略，+72.79%）

**進場兩觸發**：
- **T1 黃金交叉**：EMA20 由下穿越 EMA60（不加過濾）
- **T3 RSI 拉回**：EMA20>EMA60 + ADX≥22 + RSI<50 + EMA120 60日跌幅<2%

**出場分類**：
- 高波動股（ATR/Price > 3.5%）→ 只守 EMA20/60 死叉，無停損
- 穩健股（ATR/Price ≤ 3.5%）→ EMA死叉 + ADX<25時RSI>75 + 動態 ATR 停損

**長持鎖定**：持倉>200天 + 浮動>50% + EMA120上升 → EMA60/120 慢出場

### v8 金字塔加碼（架構性突破，+197.48%）

**核心理念**：v7 一次只持 1 倉，主升段多次回檔信號被浪費  
**v8 解法**：同股不限倉位，每次 T3 拉回信號都可累加部位

**模式語法**：`P{threshold}_{signals}+{filters}`
- `P0_T1T3`：不限門檻、T1+T3 皆可加碼
- `P5_T3`：所有現有倉位需 +5% 以上才加碼，僅 T3 信號
- `P0_T1T3+CB30`：累積虧損達 30% 即停止加碼

**為何不影響死亡迴圈股**：首倉永遠虧損 → 永不觸發加碼 → 損失與 v7 相同

---

## 🎯 策略變體完整清單

### 基礎變體
| 旗標 | 說明 |
|------|------|
| `base` | v7 原版 |
| `P{N}_{sig}` | 金字塔加碼（N=門檻%，sig=T1/T3/T1T3） |

### A 類：尾部風險控制
| 旗標 | 說明 | 效果 |
|------|------|------|
| `+PD` | 倉位遞減 0.8^(N-1) | 中度控險 |
| `+CB{N}` | 累積虧損熔斷 N% | ⭐ 最佳風控 |
| `+VA` | 動態 ATR | 無效 |
| `+TS` | 加碼後緊縮停損 | 微效 |

### B 類：進場品質
| 旗標 | 說明 | 效果 |
|------|------|------|
| `+VC` | 量能確認（≥20MA） | 過嚴 |
| `+DP` | 拉回深度（≥5%） | 微傷 |

### C 類：出場優化（普遍無效）
| 旗標 | 說明 |
|------|------|
| `+TR` | 跟蹤 ATR 停損 |
| `+PT` | 階段獲利目標 |
| `+ED` | EMA20 連 5 天下彎 |
| `+RH` | RSI>75 連 5 天 + 下彎 |

### D 類：金字塔精化
| 旗標 | 說明 | 效果 |
|------|------|------|
| `+PS` | 階梯式門檻 +5% | ⭐ 控險最強 |
| `+PG{N}` | 加碼間距 N 天 | 中度 |
| `+PSL` | 軟上限 | 微效 |

### F 類：架構擴充
| 旗標 | 說明 |
|------|------|
| `+WK` | 週線多頭確認 |
| `+MK` | TWII 大盤多頭過濾 |

### E2 可調參數
| 旗標 | 範例 | 預設 |
|------|------|------|
| `ADX{N}` | `ADX24` | 22 |
| `E120{N}` | `E120-3` | -2 |
| `RSI{N}` | `RSI55` | 50 |
| `ATL{F}` | `ATL2.0` | 2.5 |
| `ATH{F}` | `ATH3.5` | 3.0 |

---

## 🔬 健壯性驗證

### Walk-Forward 驗證（E1）
```
EARLY (2020-2022):  P0_T1T3 vs base 改善 +54.6%
LATE  (2023-2026):  P0_T1T3 vs base 改善 +54.4%
差距僅 0.2% → Edge 結構穩定，非過擬合
```

### Monte Carlo 參數敏感度（E2）
```
✅ EMA120 過濾門檻（-1~-5%）：±1.62%   健壯
✅ ATR 倍數（2.0~3.5）        ：±2.30%   健壯
🟡 ADX 進場門檻（20~26）      ：±24.94%  中度敏感
❌ RSI T3 上限（45~55）       ：±37.08%  脆弱
```

⚠️ **重大發現**：RSI 55 比 50 多賺 +37%，但屬參數脆弱性，未來市況可能反轉，保留 RSI 50 為「歷史驗證最佳」

### Survivorship Bias 估計（E3）
```
若 10% 下市股 PnL=-100% → 均值偏差 -21%（→ +113%）
若 15% 下市股 PnL=-100% → 均值偏差 -30%（→ +103%）
即使悲觀假設，P0_T1T3+CB30 仍超越 v7
```

### 交易成本（E4）
```
扣台股實際雙邊 0.4275%（手續費 + 證交稅）：
  base    +72.99 → +67.16 (-5.83)
  P5_T3   +115.30 → +107.56 (-7.74)
  P0_T1T3 +197.48 → +184.33 (-13.15)
```

---

## 📁 重要檔案說明

### 主要腳本
| 檔案 | 用途 |
|------|------|
| `backtest_all.py` | 6 策略並行回測核心 |
| `backtest_tw_all.py` | 全市場批次回測（建快取） |
| `tv_app.py` | Streamlit Web UI |
| `daily_scanner.py` | 即時信號掃描 |

### 研究分析
| 檔案 | 用途 |
|------|------|
| `walk_forward.py` | Walk-forward 驗證 |
| `monte_carlo.py` | 參數敏感度測試 |
| `survivorship_analysis.py` | 下市股偏差估計 |
| `verify_consistency.py` | base 模式一致性驗證 |

### 文件
| 檔案 | 內容 |
|------|------|
| `PHASE_REVIEW.md` | A~F 全面實作總覽 |
| `README.md` | 本檔案 |

### 自動產生（已 gitignore）
- `data_cache/*.parquet` - 歷史 K 線 + 指標快取
- `tw_all_results_*.csv` - 全市場回測結果
- `results_*.csv` - 變體測試結果
- `daily_signals_*.csv` - 每日信號清單

---

## 💡 核心研究結論

### ✅ 有效的優化（採納）
1. **金字塔加碼**：核心突破，+124% 改善
2. **A2 累積虧損熔斷 CB30**：尾部風險 -290 → -166
3. **D1 階梯式門檻 PS**：尾部風險 -290 → -149

### ❌ 失敗的方向（已驗證、捨棄）
- **進場過濾普遍有害**：W-Bottom、ADX/EMA120 加強過濾、量能確認 → 都會誤殺強勢股
- **退場優化大多無效**：跟蹤 ATR、EMA20 跟蹤、RSI 高位下彎 → 都會切碎飆股
- **冷卻期/連敗淘汰**：4961 天鈺 -666%、6139 亞翔 -2406%

### 🔑 關鍵教訓
> **「進場過濾普遍誤殺強勢股；退場優化大多切碎飆股；加碼控制（CB30、PS）才是改善風報比的關鍵」**

---

## 🛠️ 開發路線圖

### 已完成
- [x] v7 base 策略（EMA120 過濾 + ATR/P 自動分類）
- [x] v8 金字塔加碼架構
- [x] 16 種變體旗標（A~F 全部類別）
- [x] 三層平行化（快取 + ProcessPool + 變體並行）
- [x] Walk-forward 驗證
- [x] Monte Carlo 敏感度測試
- [x] Survivorship Bias 估計
- [x] 交易成本計算
- [x] 每日即時掃描器
- [x] Streamlit Web UI

### 未來方向（如需）
- [ ] 自動下載下市股完整資料（需 TWSE 公告整合）
- [ ] 即時 Broker API 串接（實盤交易）
- [ ] 產業指數第二維度（資料源待定）
- [ ] 每日定時掃描 + 推播通知

---

## 📜 開發歷程（重要里程碑）

| Commit | 日期 | 重點 |
|--------|------|------|
| `2443892` | 2026-04-25 | ⑦自適應 v3 ATR/P 自動分類整合 |
| `80ffd6e` | 2026-04-26 | v7：T3 EMA120 過濾 + 全市場批次回測 |
| `0158dea` | 2026-04-26 | 三層平行化架構（38× 加速） |
| `1e52234` | 2026-04-26 | **v8 金字塔加碼（+197.48%）** ⭐ |
| `60b64dd` | 2026-04-26 | A~F 全面實作（16 種變體） |
| `f7fb48c` | 2026-04-26 | E2/E3/F3 方法論驗證 + 即時掃描器 |

---

## 📄 License

MIT License — 自由使用、修改、散佈

---

## 🤝 鳴謝

策略設計與實作 by Eddy Huang  
研究助手：Claude Sonnet 4.6 (via Claude Code)
