"""support_resistance.types — shared dataclasses
==================================================

對應規格 §2.5 / §3 的資料模型。子系統 A / B / C 都使用這裡的型別。

設計重點：
- 一律以「區（band）」表示，不用單一線（規格 §2.2）
- 中心、邊界、強度、來源、觸及次數、近度都記下，
  方便評分模組 (sr_engine) 與 T3 接口讀取
- frozen=False（評分階段需要 update strength）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ── 子系統 A 用：原始 swing pivot ───────────────────────────────
@dataclass
class Pivot:
    """單一 swing 轉折點（局部高或低）。"""
    idx:    int           # 在輸入 OHLCV 中的列索引
    price:  float         # 該 pivot 的價格（high 用 High、low 用 Low）
    kind:   str           # 'high'（壓力候選）或 'low'（支撐候選）
    volume: float = 0.0   # 該根 bar 的成交量，作為加權


# ── 子系統 B 用：volume profile 輸出 ───────────────────────────
@dataclass
class VolumeProfile:
    """量價分布結果（規格 §3.2）。"""
    bin_edges: list       # 長度 = n_bins + 1，bins[i] 的價格範圍 = [edges[i], edges[i+1])
    bin_volumes: list     # 長度 = n_bins，每個 bin 累積的成交量
    poc: float            # Point of Control（最大量價位中心）
    val: float            # Value Area Low
    vah: float            # Value Area High
    hvn_zones: list       # list[tuple[float, float]] — 連續 HVN 區的 [low, high]
    lvn_zones: list       # list[tuple[float, float]] — 連續 LVN 區的 [low, high]（真空區）

    def total_volume(self) -> float:
        return float(sum(self.bin_volumes))


# ── 共用：S/R Zone ─────────────────────────────────────────────
@dataclass
class SRZone:
    """單一支撐／壓力區（規格 §2.2 / §2.5 / §3.3）。

    對應規格 §2.5 的 sr_zones 表欄位：
        symbol, kind, low, high, center, touches, strength, source, computed_at
    （symbol / computed_at 由呼叫方注入；此 dataclass 只放演算法輸出）
    """
    kind:           str                 # 'support' / 'resistance'
    low:            float               # 區下緣
    high:           float               # 區上緣
    center:         float               # 區中心（量能加權或 POC）
    touches:        int = 0             # 觸及次數（swing 來源計算）
    strength:       float = 0.0         # 0-100；由 sr_engine.score_zones 填入
    source:         str = 'swing'       # 'swing' / 'profile' / 'round' / 'fused'
    last_touch_idx: int = -1            # 最後一次觸及的 bar 索引（recency 用）
    role_reversal:  bool = False        # 曾被穿越兩側（壓力→支撐 / 反之）
    components:     dict = field(default_factory=dict)
    # ^^ 評分四分量原始值 {'touch': 0-1, 'volume': 0-1, 'recency': 0-1, 'confluence': 0-1}
    # 方便除錯與 detail card 顯示「為什麼這個區強」

    def contains(self, price: float) -> bool:
        """價格是否落在此區內。"""
        return self.low <= price <= self.high

    def overlaps(self, other: 'SRZone', tol: float = 0.0) -> bool:
        """兩區是否重疊（容差 tol；用於融合判定）。"""
        return not (self.high + tol < other.low or other.high + tol < self.low)

    def width(self) -> float:
        return self.high - self.low
