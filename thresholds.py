"""tv_app 全域閾值集中管理 — Stage 2 single source of truth
================================================================

任何引擎都從此讀取閾值；禁止在邏輯中重新寫死數字。
新增閾值時，請優先擴充 ``MarketThresholds`` 而非散落到各檔。

值來源（Stage 1 後現行程式設定）：
  - adx_entry:   TW=22 / US=18  （US 依美股研究 P10+POS+ADX18 RR 0.496 局部最佳）
  - adx_strong:  30             （強趨勢 / 最佳加碼）
  - t4_rsi:      35             （T4 空頭反彈 RSI 上限，與回測一致；v9.41 統一）
  - t3_rsi:      50             （T3 多頭拉回 RSI 上限）
  - rsi_hot:     78             （RSI 過熱、封頂禁新倉）
  - rsi_exit_hi: 75             （高 RSI 出場）
  - dev_high:    10             （乖離「高」%，禁加碼）
  - dev_hot:     15             （乖離「過熱」%，禁新倉）
  - t1_max_days: 10             （T1 黃金交叉有效天數）

用法：

    import thresholds
    th = thresholds.for_ticker(is_us, is_crypto)
    if adx >= th.adx_entry:
        ...
    if rsi < th.t4_rsi:
        ...
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketThresholds:
    """單一市場的進出場閾值。frozen=True 確保不可變、可雜湊。"""
    adx_entry:   float       # 進場最低 ADX
    adx_strong:  float       # 強趨勢 ADX（最佳加碼）
    t4_rsi:      float       # T4 空頭反彈 RSI 上限
    t3_rsi:      float       # T3 多頭拉回 RSI 上限
    rsi_hot:     float       # RSI 過熱（封頂禁新倉）
    rsi_exit_hi: float       # 高 RSI 出場
    dev_high:    float       # 乖離「高」(%) — 禁加碼
    dev_hot:     float       # 乖離「過熱」(%) — 禁新倉
    t1_max_days: int         # T1 黃金交叉有效天數


# ── 台股：ADX 22（依台股 P5+VWAPEXEC 預設）────────────────────────────
TW = MarketThresholds(
    adx_entry=22, adx_strong=30,
    t4_rsi=35, t3_rsi=50,
    rsi_hot=78, rsi_exit_hi=75,
    dev_high=10, dev_hot=15,
    t1_max_days=10,
)

# ── 美股：ADX 18（依美股研究 P10+POS+ADX18 RR 0.496 局部最佳）─────────
US = MarketThresholds(
    adx_entry=18, adx_strong=30,
    t4_rsi=35, t3_rsi=50,
    rsi_hot=78, rsi_exit_hi=75,
    dev_high=10, dev_hot=15,
    t1_max_days=10,
)

# ── 加密貨幣：目前與美股同；日後要拆再分 ──────────────────────────────
CRYPTO = US


def for_ticker(is_us: bool, is_crypto: bool) -> MarketThresholds:
    """依市場旗標回傳對應的閾值組。

    Args:
        is_us:     是否為美股
        is_crypto: 是否為加密貨幣（-USD 結尾）

    Returns:
        MarketThresholds: 對應市場的閾值；台股為預設。
    """
    if is_crypto:
        return CRYPTO
    if is_us:
        return US
    return TW
