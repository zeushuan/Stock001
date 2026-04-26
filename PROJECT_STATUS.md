# 專案最終狀態總結

> 從 v7 (+72.99%) 到 v8.9（POS+IND+DXY 風報比 1.03）的完整研究紀錄

---

## 🎯 最終最佳策略譜系（六檔）

| 風格 | 模式 | 均值 | 最低 | 風報比 | 跨年σ |
|------|------|------|------|------|------|
| 🛟 超低風險 | POS+IND+DXY+WRSI+WADX | +83% | **-88** | 0.94 | 8.50 |
| 🛡️ **極致風控** ⭐ | **POS+IND+DXY** | +122% | -119 | **1.03** | 7.89 |
| 🌊 保守 | POS+DXY | +121% | -122 | 0.99 | 7.67 |
| ⚖️ 平衡 | POS | +142% | -166 | 0.85 | 8.89 |
| 🤖 RL 智能 | P0_T1T3+RL | +153% | -227 | 0.67 | — |
| 🚀 進攻 | P0_T1T3 | +197% | -290 | 0.68 | 12.7 |

**Baseline 對照**：
- ① BH 持有：+163.94% / -96% / 1.71
- ⑦ v7 base：+72.99% / -140% / 0.52

---

## 📊 30+ 變體完整測試清單

### A. 加碼控制
- ✅ POS（累積為正才加碼）→ 0.85
- ❌ POS5/POS10/POS20 → 越嚴越差
- ❌ POS+PR 停損後重置 → 尾部惡化
- ❌ AT 線上自適應門檻 → 無感

### B. 跨市場過濾
- ⭐ DXY 弱美元 → 0.99（最佳單一過濾）
- ✅ VIX30 過濾 → 0.91
- ⭐ IND 產業 specific + DXY → 1.03（局部最佳）
- ❌ TNX、GLD、HG、SOX 等 → 無顯著效果或負面

### C. 出場優化
- ❌ CB30/TR/PT/ED/RH → 全部切碎主升段

### D. 多時間框架
- ⚠️ WK/WRSI/WADX → 單獨用效果有限
- ⭐ WRSI+WADX 組合 → 最低尾部 -88

### E. 基本面
- ❌ ER 月份避險 → 太粗糙
- ⏸ 真實財報日 → 需 TWSE 爬蟲

### F. 執行優化
- ⚠️ LIQ 流動性 → 大樣本無感
- ⚠️ SLP 滑價 → -9% 真實衝擊

### G. 全新方法
- ⚠️ ML 決策樹 → 75.6% 準確率，驗證 POS 已抓 80%
- ⚠️ RL Q-Learning → 學到的規則與人工一致
- ❌ EV Ensemble Voting → 重複過濾
- ❌ VR 波動率 regime → 無感
- ❌ ANOM 異常偵測 → 略傷獲利
- ⏸ 三大法人籌碼 → 爬蟲被 TWSE rate limit 阻擋

---

## 🔬 8 大關鍵發現

1. **POS 規則達到資訊上限**：ML 75.6% vs v7>0 規則 69.3%，差距僅 6%
2. **DXY = 全球流動性綜合指標**：弱美元包含 3 層宏觀資訊
3. **產業 specific 邊際提升**：IND+DXY 1.03 vs DXY 0.99
4. **2022 熊市保護有效**：POS+DXY -0.46% vs base -2.56%
5. **加碼次數 = 趨勢強度**：30+ 次加碼股平均 +397%
6. **RL 自動學到 POS 規則**：從 19.9 萬樣本獨立發現
7. **跨年度 σ 最佳 7.14**：POS+VIX30+DXY
8. **最低尾部 -88**：五重保護組合

---

## 📁 完整檔案清單

### 主程式
- `backtest_all.py` ─ 6 策略並行回測核心
- `backtest_tw_all.py` ─ 全市場批次回測（建快取）
- `tv_app.py` ─ Streamlit Web UI（v8.9，6 檔策略選擇器）
- `daily_scanner.py` ─ 每日信號掃描

### 基礎建設
- `data_loader.py` ─ parquet 快取層
- `variant_strategy.py` ─ 30+ 旗標的參數化策略核心
- `v8_runner.py` ─ ProcessPool 12 worker 平行執行
- `batch_test.py` ─ 多變體並行測試

### 研究分析
- `walk_forward.py` ─ Walk-forward + 交易成本驗證
- `monte_carlo.py` ─ 參數敏感度測試
- `survivorship_analysis.py` ─ 倖存偏差估計
- `feature_engineering.py` ─ 1243 檔 14 維特徵
- `cluster_analysis.py` ─ Cohen's d 區辨力分析
- `industry_pos_analysis.py` ─ 產業別四模式對比
- `year_by_year.py` ─ 時序穩定性驗證
- `verify_pos_dxy_yearly.py` ─ DXY 跨年度驗證
- `per_stock_analysis.py` ─ 全市場每股深度分析
- `ml_decision_tree.py` ─ ML 決策樹
- `rl_trainer.py` ─ Tabular Q-Learning 訓練器

### 資料層
- `institutional_scraper.py` ─ TWSE 三大法人爬蟲（待解鎖）
- `inst_pivot.py` ─ 每日全市場 → 每股時序

### 文件
- `README.md` ─ 專案說明
- `PHASE_REVIEW.md` ─ A~F 全面實作總覽
- `DEEP_ANALYSIS.md` ─ 特徵工程與分群分析
- `CROSS_MARKET_REVIEW.md` ─ 跨市場研究
- `ML_AND_FUTURE_DIRECTIONS.md` ─ ML 與未來方向
- `FINAL_RESEARCH.md` ─ 完整研究最終報告
- `PROJECT_STATUS.md` ─ 本檔（最終狀態）

---

## 🚧 環境限制與未完成方向

### 受限於環境
1. **三大法人籌碼**：TWSE 爬蟲被限速 + IP 暫時封鎖
   - 已抓到 57 天近期資料
   - 程式碼基礎建設已完成（INST、FORN 旗標）
   - 等爬蟲環境恢復或改用 FinMind/券商 API

### 需要新資料源（未實作）
2. **真實財報日**：需精準個別公司資料
3. **盤中分鐘級資料**：需券商 API
4. **法人分點進出**：需付費資料
5. **新聞情感分析**：需 NLP 處理
6. **基本面整合**：ROE/P/E/營收年增率

### 架構性改變（被排除或工程量大）
7. **Pairs Trading**（配對交易）
8. **Portfolio Optimization**（被排除）
9. **複利模式**（被排除）
10. **NN-based RL**（取代 tabular Q）

---

## 💡 為什麼沒有突破風報比 1.03？

```
ML/RL 雙重證明：
  DecisionTree CV   75.6%
  簡單規則 v7>0     69.3%
  差距僅 6%

→ 即時可用特徵的訊息已被人工規則充分利用
→ 額外 6% 來自「事後特徵」（無法即時取得）
→ 真正突破需要：
   1. 新資料源（財報日、盤中、法人籌碼）
   2. 新架構（pairs trading、NN-based）
```

---

## 🛠️ 工程基礎建設成就

### 性能
- 單變體全市場（已快取）：~12 秒
- 4 變體並行測試：~30 秒
- RL Q-Learning 訓練：~7 秒（199,886 樣本）
- 38× 加速（vs 原 backtest_tw_all 3.4 分鐘）

### 規模
- 1263 檔台股 × 6 年 4 個月（含 COVID/AI 兩大週期）
- 30+ 策略變體完整測試
- Walk-forward / Monte Carlo / Survivorship 全方法論驗證
- ML / RL 雙重驗證

---

## 🎉 最終結論

> **POS+IND+DXY 風報比 1.03 是當前架構下的局部最佳**
> 
> 從 v7 +72.99% → v8.9 共完成 9 個版本演進
> 提供 6 檔策略風格供使用者依風險偏好選擇
> 完整研究框架可重現、可擴展，留待未來新資料源整合

---

## Git 全部 commit 軌跡

```
2443892  ⑦自適應 v3 ATR/P 自動分類
80ffd6e  v7 base + EMA120 過濾
0158dea  三層平行化（38× 加速）
1e52234  v8 金字塔加碼（+197.48%）⭐
60b64dd  A~F 全面實作（16 變體）
efe359c  POS 變體發現（風報比 0.85）
066418a  跨市場（DXY 風報比 0.99）
894c1e4  時序驗證（DXY σ=7.67）
d14c3f8  tv_app v8.6 策略風格選擇器
cfb61ed  IND+DXY 風報比 1.03 突破 ⭐
632904f  ML 決策樹驗證 POS
cd2abed  五重保護最低 -88
07527aa  RL Q-Learning 加碼決策
c885459  tv_app v8.9 RL 選項
b395290  AT 線上自適應 + 完整研究報告
613fa22  三大法人爬蟲基礎建設
99aae96  三大法人探討（受環境限制）
```
