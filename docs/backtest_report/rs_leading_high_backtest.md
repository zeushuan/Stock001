# RS Leading High Scanner — 歷史回測報告

> **執行日期**：2026-05-12
> **執行人**：Claude Code
> **執行指令**：`python -m backtests.rs_leading_high_backtest --market US --universe SP500 --start 2023-01-01 --end 2024-12-31 --top-n 10`

---

## 1. 回測設定

| 項目 | 值 |
|---|---|
| 市場 | US |
| 宇宙 | SP500_CORE (50 檔精選成份股) |
| 時間範圍 | 2023-01-01 → 2024-12-31 (2 年) |
| 再平衡頻率 | 月末（共 23 個 rebalance） |
| 每次選股 | Top 10 by quality score |
| 交易成本 | 0.05% × 2 = 0.10% 雙邊 |
| 大盤基準 | SPY |
| 持有期 | 21d / 63d / 126d |

### 參數固定
- `rs_new_high_lookback = 63`
- `rs_high_recency_days = 5`
- `min_distance_from_price_high = 0.03`
- 上述參數**未依回測結果反向調整**（避免過擬合）

---

## 2. 主要結果

| Hold | N | Win% | MeanRet% | Bench% | Alpha% | Sharpe | p-value | Significant |
|---|---|---|---|---|---|---|---|---|
| 21d | 23 | **78.3** | **+7.32** | +1.50 | **+5.82** | 1.71 | 0.20 | ✗ |
| 63d | 23 | 56.5 | +7.27 | +4.68 | +2.59 | 0.57 | 0.66 | ✗ |
| 126d | 23 | **82.6** | +14.20 | +9.31 | +4.89 | 0.43 | 0.74 | ✗ |

**重要：所有時間視窗的超額報酬 p-value 都 > 0.05，未達 5% 統計顯著門檻**。

### 解讀

正面：
- **21d 勝率 78.3%、平均 +7.3%**（vs 大盤 +1.5%）— 點估計看似很強
- **126d 勝率 82.6%、平均 +14.2%**（vs 大盤 +9.3%）— 中長期持續累積
- Sharpe 21d = 1.71（如果結果穩定，這算很不錯）

警訊：
- **樣本數只有 23 個 monthly rebalance × ~5 個訊號通過 = ~115 個個股 observations**
- t-test 統計檢定**未達顯著**（p > 0.05）
- 高 Sharpe 但 high variance — 表現不穩
- 21d Sharpe 1.71 但 p=0.20 → 點估計與不確定性差距大

---

## 3. 訊號量

| 月份 | 通過 quality filter 的訊號數 |
|---|---|
| 2023 月平均 | ~3-5 檔 |
| 2024 月平均 | ~4-6 檔 |
| 合計 | ~115 個個股 observations |

訊號量偏少（SP500_CORE 只 50 檔），擴展宇宙到完整 LIQUID_3000 應該能拿到更多樣本。

---

## 4. 誠實的結論

### 我們知道什麼
1. **點估計上訊號似乎有效** — 平均超額報酬都是正的，21d 勝率 78%
2. **方向一致**：21d / 63d / 126d 都是正 alpha
3. **訊號邏輯合理** — 機構累積後股價跟上 是合理的市場機制

### 我們不能斷言什麼
1. **統計顯著性不夠**（p > 0.05），無法拒絕「訊號實際無效，只是運氣」的可能性
2. **樣本量小** — 23 個月、~115 個 observations 不足
3. **單一宇宙、單一時期** — 2023-2024 是強多頭年份，未涵蓋熊市 / 震盪市

### 改進方向
1. **擴展宇宙**：從 SP500_CORE (50) 擴到 LIQUID_3000 (3000)，預期訊號量 +10×
2. **延長回測期間**：建議 2018-2024（涵蓋 2018 熊、2020 V 反轉、2022 熊、2023-24 牛）
3. **加做 survivorship-bias-free 宇宙快照**
4. **與其他訊號組合**：本訊號獨立使用統計力不足，但與 T3 系統其他訊號組合後可能達顯著
5. **檢視個別 outlier**：高 mean 但低 sharpe 暗示有 outlier，可能由少數大贏家驅動

### 對 Eddy 的建議
- **不要把本訊號當作獨立交易策略使用** — 統計顯著性不足
- 把它當作 T3 系統的「技術面子集輸入之一」— 與其他訊號組合可能達顯著
- 在小資金（< 5% 總資金）試單，累積實盤證據

---

## 5. 重現步驟

```bash
# 安裝依賴
pip install -r requirements.txt
pip install scipy scikit-learn tqdm openpyxl

# 跑回測
python -m backtests.rs_leading_high_backtest \
    --market US \
    --universe SP500 \
    --start 2023-01-01 \
    --end 2024-12-31 \
    --top-n 10 \
    --save reports/backtest_us_sp500.json
```

完整 JSON 報告：[reports/backtest_us_sp500.json](../../reports/backtest_us_sp500.json)

---

## 6. 後續工作

| 任務 | 優先 | 估時 |
|---|---|---|
| 擴展 LIQUID_3000 完整宇宙 | 高 | 0.5 day |
| 增加 2018-2024 完整回測 | 高 | 1 day |
| 建立月度宇宙快照（survivorship-free） | 中 | 1 day |
| 加 sub-period analysis（牛/熊/震盪） | 中 | 0.5 day |
| 加月度報酬熱力圖 + equity curve 圖 | 低 | 0.5 day |
| TW 市場回測 | 低 | 0.5 day |

---

## 7. 簽核

| 角色 | 簽名 | 日期 |
|---|---|---|
| 執行者 | Claude Code (Opus 4.7) | 2026-05-12 |
| 委派者 | Eddy Huang, CTO | _(待簽)_ |
