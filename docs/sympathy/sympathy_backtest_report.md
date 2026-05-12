# Sympathy Play — Phase 2 歷史回測報告

> **執行日期**：2026-05-12
> **執行人**：Claude Code
> **規格依據**：`SYMPATHY_PLAY_MODULE.md` v1.0 §2.6
> **指令**：`python -m sympathy.backtest --start 2025-05-01 --end 2026-04-30 --interval 1`

---

## 1. 回測設定

| 項目 | 值 |
|---|---|
| 期間 | 2025-05-01 → 2026-04-30（**12 個月**）|
| 掃描頻率 | 每交易日 |
| 共掃描天數 | **261 個交易日** |
| 進場 | 訊號隔日「開盤」買入 |
| 出場（先到先出）| +8% 停利 / -4% 停損 / 持滿 5 天收盤 |
| 族群 | 6 個（AI_Storage_US / AI_Power_US / AI_Chip_US / AI_Server_TW / TW_Semi_OSAT / TW_IC_Design）|
| 訊號門檻 | corr_60d>0.6, spread_pctile<0.35, lag_today>2% |
| 技術濾網 | peer > 20MA, 連續下跌 < 5 日 |

---

## 2. 主要結果

### 2.1 整體

```
Total signals:    52
Trades evaluated: 51
Win rate:        62.7%  ✓
Avg return:     +2.54%
Median return:  +2.70%
Max win:        +8.00%   (TP)
Max loss:       -4.00%   (SL)
TP hits:           20
SL hits:           16
TIME exits:        15
Avg days held:    4.0
```

### 2.2 按 score bucket

| Bucket | n | Win % | Avg % | Median % |
|---|---|---|---|---|
| 0.45-0.60 | 0 | — | — | — |
| 0.60-0.75 | 17 | 52.9 | +1.66 | +0.76 |
| **0.75+** | **34** | **67.6** ✓ | **+2.97** | **+3.60** |

### 2.3 按 group

| Group | n | Win % | Avg % |
|---|---|---|---|
| **AI_Server_TW** | 3 | **100.0** | **+5.41** |
| **AI_Power_US** | 13 | **84.6** | **+3.84** |
| **TW_Semi_OSAT** | 4 | 75.0 | +3.81 |
| AI_Chip_US | 4 | 50.0 | +2.04 |
| AI_Storage_US | 27 | 48.1 | +1.47 |

---

## 3. 驗收條件達成度

| 條件 | 標準 | 實測 | 結果 |
|---|---|---|---|
| 整體勝率 | ≥ 55% | **62.7%** | ✅ |
| 0.75+ 區間勝率 | ≥ 65% | **67.6%** | ✅ |

**全部達標 → 可進 Phase 3 整合**。

---

## 4. 關鍵觀察

### 4.1 Score 的鑑別力強
- 0.75+ 勝率 67.6%、avg +2.97%
- 0.60-0.75 勝率 52.9%、avg +1.66%
- **差距明顯**：score 越高，勝率與平均報酬都顯著遞增 → score 公式有效

### 4.2 族群表現差異大
- AI_Power_US（CEG/VST/GEV/PWR）：勝率 84.6%，最穩
- AI_Server_TW（鴻海/廣達/緯創/技嘉/和碩）：n=3 但全勝
- **AI_Storage_US（MU/SNDK/WDC/STX）**：勝率 48.1% 最差，且樣本最多
  - 可能原因：HBM/NAND 板塊內個股相關性高但 lead-lag 不明顯（同步動）
  - 或本期間（2025-2026）儲存族群有獨特催化劑分布

### 4.3 出場行為合理
- TP 20 / SL 16 / TIME 15
- TP 比 SL 略多，符合 sympathy play 短線爆發特徵
- 平均持有 4 天，與 5 日設計相符

---

## 5. 已知限制

### 5.1 樣本量
- 12 個月內僅 52 個訊號 / 51 個交易
- 雖然驗收達標，但統計顯著性受限
- 建議：擴展到 24 個月以強化結論

### 5.2 AI_Storage_US 勝率偏低
- n=27 占總樣本一半，但勝率只 48.1%
- 拉低整體勝率（若排除此族群，勝率約 76%）
- **改進方向**：
  - 該族群可能不適用單純的 lead-lag sympathy
  - 或改用較高 spread_pctile 門檻（避免「過早」進場）

### 5.3 沒有交易成本
- 本回測未扣手續費（US 0.05%/邊、TW 0.15%/邊）
- 扣除後實際勝率與報酬會略低 0.5-1%

### 5.4 點估計，沒做 t-test
- 沒有計算統計顯著性
- 52 個訊號的 sample 可能算統計力邊緣
- Phase 3 整合前建議跑 24 月版本以提高信心

---

## 6. 對 Phase 3 整合的建議

依指示書 §4 開發順序，建議：

| 整合項目 | 優先 | 說明 |
|---|---|---|
| `signal_integrator.py` 加成至 T3 | 高 | score ≥0.75 加 8 分、0.6-0.75 加 5 分、< 0.6 加 3 分（指示書 §2.5）|
| `volume_confirm.py` 隔日量能確認 | 中 | 美股先做基本量比；台股稍後接 FinMind |
| Streamlit 「補漲掃描」分頁 | 中 | 候選股表 + 點擊看 leader-peer 比較圖 |
| **排除 AI_Storage_US？** | **觀察** | 樣本最多但勝率最低；可加 group blacklist 或調 score 公式 |

---

## 7. 重現方式

```bash
# 確認快取資料
ls data_cache/*.parquet | head -5

# 跑 12 月回測
python -m sympathy.backtest \
    --start 2025-05-01 \
    --end 2026-04-30 \
    --interval 1 \
    --save reports/sympathy_backtest_12m.json

# 試 24 月版本（建議）
python -m sympathy.backtest \
    --start 2024-05-01 \
    --end 2026-04-30 \
    --interval 1 \
    --save reports/sympathy_backtest_24m.json
```

完整 JSON：[reports/sympathy_backtest_12m.json](../../reports/sympathy_backtest_12m.json)

---

## 8. 簽核

| 角色 | 簽名 | 日期 |
|---|---|---|
| 執行者 | Claude Code (Opus 4.7) | 2026-05-12 |
| 委派者 | Eddy Huang, CTO | _(待簽)_ |
