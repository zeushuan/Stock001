"""support_resistance — Stock001 S/R 偵測子系統 (v0.1)
======================================================

對應規格《tv_app 壓力／支撐區偵測：方法論與實作規格 v0.1》Phase 1。

三個子系統（package 結構）：
  - swing_cluster   (Subsystem A) — Swing pivots + ATR adaptive clustering
  - volume_profile  (Subsystem B) — POC / VA / HVN / LVN
  - sr_engine       (Subsystem C) — 融合 + 評分 + 整數價位 + T3 接口

對外 public API（推薦從這裡 import）：

    from support_resistance import (
        detect_sr_zones,          # 端到端：OHLCV → list[SRZone]
        sr_context_for_t3,        # T3 Confidence 情境修正
        SRZone,
    )

如果只要使用單一子系統，直接 import 子模組：

    from support_resistance.swing_cluster import detect_swing_zones
    from support_resistance.volume_profile import compute_profile, profile_to_zones
    from support_resistance.sr_engine import round_number_zones, fuse_zones, score_zones

所有預設參數來自 support_resistance.params（對齊規格 §7）。
"""

# 子系統 A
from .swing_cluster import (
    compute_atr,
    find_pivots,
    cluster_pivots,
    detect_swing_zones,
)

# 子系統 B
from .volume_profile import (
    filter_rth,
    compute_profile,
    profile_to_zones,
)

# 子系統 C
from .sr_engine import (
    round_number_zones,
    fuse_zones,
    score_zones,
    detect_role_reversal,
    detect_sr_zones,
    sr_context_for_t3,
)

# Backtest (Phase 7 — 規格 §8)
from .backtest import (
    TouchEvent,
    backtest_one,
    aggregate,
    format_report,
)

# Types
from .types import (
    Pivot,
    VolumeProfile,
    SRZone,
)

# Params（給 caller 想看預設值或客製化時用）
from . import params

__all__ = [
    # 端到端
    'detect_sr_zones', 'sr_context_for_t3',
    # 子系統 A
    'compute_atr', 'find_pivots', 'cluster_pivots', 'detect_swing_zones',
    # 子系統 B
    'filter_rth', 'compute_profile', 'profile_to_zones',
    # 子系統 C
    'round_number_zones', 'fuse_zones', 'score_zones', 'detect_role_reversal',
    # Backtest
    'TouchEvent', 'backtest_one', 'aggregate', 'format_report',
    # Types
    'Pivot', 'VolumeProfile', 'SRZone',
    # Params
    'params',
]
