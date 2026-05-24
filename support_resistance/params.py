"""support_resistance.params — 預設參數（規格 §7）
======================================================

集中管理 S/R 演算法所有可調參數的預設值。
所有預設值對齊「tv_app 壓力／支撐區偵測 v0.1」規格 §7。

各子系統函式都允許傳入覆寫，但呼叫端不傳時一律吃這裡的常數。
"""
from __future__ import annotations


# ── 子系統 A：Swing-Cluster（規格 §3.1）─────────────────────────
SWING_WINDOW_DAILY   = 5     # 日線左右視窗
SWING_WINDOW_MINUTE  = 3     # 分鐘線左右視窗（盤中用）
CLUSTER_ATR_MULT     = 1.0   # 區寬 = median(ATR) × k；k 預設 1.0
MIN_TOUCHES          = 2     # 觸及次數低於此值的區捨棄
LOOKBACK_BARS        = 250   # 回看 bar 數（規格 §7 預設 250）

# ── 子系統 B：Volume Profile（規格 §3.2）────────────────────────
N_BINS                = 50     # 預設 50 bin；或用 ATR×0.5 動態 bin 寬
BIN_WIDTH_ATR_MULT    = 0.5    # 若用動態 bin 寬，bin_width = median(ATR) × 此值
VALUE_AREA_PCT        = 0.70   # Value Area = 70% 累積成交量
HVN_MULT              = 1.5    # V[bin] ≥ mean × hvn_mult → 高量區
LVN_MULT              = 0.4    # V[bin] ≤ mean × lvn_mult → 真空區
RTH_FILTER_DEFAULT    = True   # Volume Profile 預設只用 RTH（規格 §2.4）

# ── 子系統 C：融合 + 評分（規格 §3.3）───────────────────────────
W_TOUCH      = 0.30   # 觸及次數 分量權重
W_VOLUME     = 0.30   # 量能       分量權重
W_RECENCY    = 0.20   # 近度       分量權重
W_CONFLUENCE = 0.20   # 多源重疊   分量權重

SUPPORT_BIAS = 1.05   # 支撐區 strength × 此值（規格 §3.3 / §1.1 實證）

ROUND_NUMBER_STEPS = (1.00, 0.50)  # .00 / .50（規格 §1.4 / §2.1）
ROUND_NUMBER_WEIGHT = 0.10         # 整數價位獨立加分（低權重，輔助性質）

# ── T3 接口（規格 §4）────────────────────────────────────────────
PROXIMITY_PCT          = 1.5    # 現價距離區中心 ≤ 1.5% → 觸發情境修正
T3_ADJUSTMENT_CAP      = 15     # 最大 ±15 分調整
ADX_TREND_DAMPING      = 0.5    # adjustment × (1 − min(ADX/50, 1)·此係數)
ADX_DAMPING_REFERENCE  = 50     # ADX 50 = 完全強趨勢

# 安全範圍（給 sr_engine 驗證輸入）
MIN_BARS_FOR_DETECTION = 30     # 少於此數的 OHLCV 不能可靠偵測
