"""Intraday Pattern Adapters

把現有 patterns/（stage / cup / flat）依 timeframe 自動 scale，
而不是 fork 一份 intraday 專屬實作。

對外：
    classify_stage_tf(df, tf='1h')
    detect_cup_handle_tf(df, tf='1h')
    detect_flat_base_tf(df, tf='1h')
"""
from intraday.patterns.scaled import (
    classify_stage_tf, detect_cup_handle_tf, detect_flat_base_tf,
)
