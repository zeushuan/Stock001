"""Intraday Settings — Stock001 v9.32
========================================

集中管理 intraday-related 可調參數。目前主要是 ZigZag ATR 倍數
（之前全部寫死 1.3，現在從 intraday_config.json 動態讀）。

對外 API：
  get_zigzag_atr_mult() -> float
  set_zigzag_atr_mult(val: float) -> None
  reset_zigzag_atr_mult() -> None
  get_zigzag_max_bars() -> int            🆕 v9.47
  set_zigzag_max_bars(val: int) -> None   🆕 v9.47
  reset_zigzag_max_bars() -> None         🆕 v9.47
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


# Config 檔位置 — 放專案根目錄
_CONFIG_PATH = Path(__file__).parent.parent / 'intraday_config.json'

# 預設值（OOS 驗證 VCP Sharpe 最佳 = 1.30）
_DEFAULT_ATR_MULT = 1.30

# 🆕 v9.47：ZigZag chart 顯示 bar 數預設（fetch_indicators 存 252 bars，安全範圍 30-252）
_DEFAULT_MAX_BARS = 80
_MAX_BARS_MIN = 30
_MAX_BARS_MAX = 252

# 簡單 in-process cache（避免每次 import 都讀檔）
_cache: Optional[dict] = None


def _load() -> dict:
    """讀 config（並做 cache）"""
    global _cache
    if _cache is not None:
        return _cache
    if not _CONFIG_PATH.exists():
        _cache = {}
        return _cache
    try:
        _cache = json.loads(_CONFIG_PATH.read_text(encoding='utf-8'))
    except Exception:
        _cache = {}
    return _cache


def _save(d: dict) -> None:
    global _cache
    try:
        _CONFIG_PATH.write_text(
            json.dumps(d, indent=2, ensure_ascii=False),
            encoding='utf-8')
        _cache = d
    except Exception as e:
        print(f"  [intraday.settings] save fail: {e}")


def _reload() -> dict:
    """強制重讀（清 cache）— config 被外部修改時用"""
    global _cache
    _cache = None
    return _load()


# ─── ZigZag ATR 倍數 ──

def get_zigzag_atr_mult() -> float:
    """取得目前 ZigZag ATR 倍數設定（沒設定回 _DEFAULT_ATR_MULT）"""
    val = _load().get('zigzag_atr_mult', _DEFAULT_ATR_MULT)
    try:
        v = float(val)
        # 安全範圍 0.5 - 5.0
        return max(0.5, min(5.0, v))
    except Exception:
        return _DEFAULT_ATR_MULT


def set_zigzag_atr_mult(val: float) -> None:
    """設定 ZigZag ATR 倍數（會寫入 config 檔，立即生效）"""
    v = float(val)
    if not (0.5 <= v <= 5.0):
        raise ValueError(f"ATR 倍數須在 0.5-5.0 之間，got {v}")
    d = _load()
    d['zigzag_atr_mult'] = round(v, 2)
    d['_updated_at'] = __import__('datetime').datetime.now().isoformat()
    _save(d)


def reset_zigzag_atr_mult() -> None:
    """重置為預設值 1.30"""
    set_zigzag_atr_mult(_DEFAULT_ATR_MULT)


# ─── 🆕 v9.47：ZigZag chart 顯示 bar 數 ──

def get_zigzag_max_bars() -> int:
    """取得目前 ZigZag chart 顯示的最大 bar 數（沒設定回 _DEFAULT_MAX_BARS=80）

    安全範圍 30-252（fetch_indicators 存 252 bars 為上限）。
    """
    val = _load().get('zigzag_max_bars', _DEFAULT_MAX_BARS)
    try:
        v = int(val)
        return max(_MAX_BARS_MIN, min(_MAX_BARS_MAX, v))
    except Exception:
        return _DEFAULT_MAX_BARS


def set_zigzag_max_bars(val: int) -> None:
    """設定 ZigZag chart 顯示 bar 數（會寫入 config 檔，立即生效）"""
    v = int(val)
    if not (_MAX_BARS_MIN <= v <= _MAX_BARS_MAX):
        raise ValueError(
            f"max_bars 須在 {_MAX_BARS_MIN}-{_MAX_BARS_MAX} 之間，got {v}")
    d = _load()
    d['zigzag_max_bars'] = v
    d['_updated_at'] = __import__('datetime').datetime.now().isoformat()
    _save(d)


def reset_zigzag_max_bars() -> None:
    """重置為預設值 80"""
    set_zigzag_max_bars(_DEFAULT_MAX_BARS)


def all_settings() -> dict:
    """回傳目前所有設定（debug 用）"""
    return _reload()
