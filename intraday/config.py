"""Intraday Timeframe Registry — Stock001 v9.29
================================================

集中管理所有 timeframe 相關常數。
其他模組（data / indicators / patterns）一律從這裡讀。
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TFConfig:
    """單一 timeframe 的設定"""
    code: str               # '1m' / '5m' / '15m' / '30m' / '1h' / '1d'
    yf_interval: str        # yfinance interval 字串
    fugle_freq: str         # fugle_connector 使用的 freq 字串
    yf_max_period: str      # yfinance 最大可取期間
    minutes_per_bar: int    # 每根 bar 多少分鐘（1d 設 390 = 一個美股交易日）
    bars_per_day: float     # 一個交易日約多少根 bar（用 US 6.5hr session 算）
    cache_ttl_seconds: int  # 快取多久才重新抓
    # ── Pattern scaling ──
    supports_stage: bool     # 是否適合做 Stan Weinstein Stage（30W SMA）
    supports_sepa: bool      # 是否適合 SEPA Trend Template（50/150/200 bar）
    supports_vcp: bool
    supports_flat_base: bool
    supports_cup_handle: bool
    supports_orb: bool       # Opening Range Breakout（只有 intraday 適用）
    supports_vwap_session: bool   # 每日 reset 的 session VWAP


# ── 註冊所有 timeframe ──
TIMEFRAMES = {
    '1m': TFConfig(
        code='1m', yf_interval='1m', fugle_freq='1m',
        yf_max_period='7d', minutes_per_bar=1, bars_per_day=390,
        cache_ttl_seconds=60,
        supports_stage=False, supports_sepa=False, supports_vcp=False,
        supports_flat_base=False, supports_cup_handle=False,
        supports_orb=True, supports_vwap_session=True,
    ),
    '5m': TFConfig(
        code='5m', yf_interval='5m', fugle_freq='5m',
        yf_max_period='60d', minutes_per_bar=5, bars_per_day=78,
        cache_ttl_seconds=300,
        supports_stage=False, supports_sepa=False, supports_vcp=False,
        supports_flat_base=False, supports_cup_handle=False,
        supports_orb=True, supports_vwap_session=True,
    ),
    '15m': TFConfig(
        code='15m', yf_interval='15m', fugle_freq='15m',
        yf_max_period='60d', minutes_per_bar=15, bars_per_day=26,
        cache_ttl_seconds=900,
        supports_stage=False, supports_sepa=False, supports_vcp=True,
        supports_flat_base=True, supports_cup_handle=True,
        supports_orb=True, supports_vwap_session=True,
    ),
    '30m': TFConfig(
        code='30m', yf_interval='30m', fugle_freq='30m',
        yf_max_period='60d', minutes_per_bar=30, bars_per_day=13,
        cache_ttl_seconds=1800,
        supports_stage=False, supports_sepa=False, supports_vcp=True,
        supports_flat_base=True, supports_cup_handle=True,
        supports_orb=True, supports_vwap_session=True,
    ),
    '1h': TFConfig(
        code='1h', yf_interval='60m', fugle_freq='60m',
        yf_max_period='730d', minutes_per_bar=60, bars_per_day=6.5,
        cache_ttl_seconds=1800,
        supports_stage=True, supports_sepa=True, supports_vcp=True,
        supports_flat_base=True, supports_cup_handle=True,
        supports_orb=False, supports_vwap_session=True,
    ),
    '1d': TFConfig(
        code='1d', yf_interval='1d', fugle_freq='1d',
        yf_max_period='10y', minutes_per_bar=390, bars_per_day=1,
        cache_ttl_seconds=43200,   # 12h
        supports_stage=True, supports_sepa=True, supports_vcp=True,
        supports_flat_base=True, supports_cup_handle=True,
        supports_orb=False, supports_vwap_session=False,
    ),
}


# ── Bars per week（pattern scaling 用）──
# 美股一週 5 天，每天約 bars_per_day 根
BARS_PER_WEEK = {
    tf: max(1, int(round(cfg.bars_per_day * 5)))
    for tf, cfg in TIMEFRAMES.items()
}


def get_tf_config(tf: str) -> TFConfig:
    """取得 timeframe config；不存在時 raise"""
    if tf not in TIMEFRAMES:
        raise ValueError(f"未支援的 timeframe '{tf}'，可用：{list(TIMEFRAMES.keys())}")
    return TIMEFRAMES[tf]


def list_supported_for(*, stage: bool = False, vcp: bool = False,
                        flat_base: bool = False, cup_handle: bool = False,
                        orb: bool = False, vwap_session: bool = False) -> list:
    """列出支援某 pattern 的 timeframes"""
    out = []
    for tf, cfg in TIMEFRAMES.items():
        ok = True
        if stage and not cfg.supports_stage: ok = False
        if vcp and not cfg.supports_vcp: ok = False
        if flat_base and not cfg.supports_flat_base: ok = False
        if cup_handle and not cfg.supports_cup_handle: ok = False
        if orb and not cfg.supports_orb: ok = False
        if vwap_session and not cfg.supports_vwap_session: ok = False
        if ok: out.append(tf)
    return out


class NotApplicable(Exception):
    """指標/pattern 在當前 timeframe 不適用時 raise"""
    pass
