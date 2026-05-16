"""Intraday Analysis Module — Stock001 v9.29
=========================================
多時間框架（1m / 5m / 15m / 30m / 1h）個股分析

子模組：
  config       — TIMEFRAMES 註冊表（period、bars_per_week、cache TTL）
  data         — 統一資料介面（fugle_connector + yfinance，TW + US）
  indicators   — VWAP / ORB / floor pivots + 標準技術指標
  alignment    — MTF state engine（1m/5m/15m/1h/1d 多時間框架對齊）
  patterns/    — Pattern adapter（stage/cup/flat 的 intraday scaled 版本）

對外 API：
  from intraday.data import get_intraday
  from intraday.alignment import compute_mtf_state
  from intraday.indicators import vwap_session, orb_levels, floor_pivots
"""
from intraday.config import TIMEFRAMES, BARS_PER_WEEK, get_tf_config
