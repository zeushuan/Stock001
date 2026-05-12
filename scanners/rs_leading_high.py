"""RS Leading High Scanner — 紫色點訊號掃描器

訊號定義：「RS 線創新高，但股價尚未創新高」
- 對應 TraderLion 指標的紫色點
- William O'Neil / Mark Minervini / David Ryan 視為機構累積足跡

三個必要條件：
  A. RS 在最近 N₁ 日內，創過 N₂ 個交易日的新高（N₁=5, N₂=63 預設）
  B. 股價在 N₂ 期間「未」創新高，且距高點 ≥ min_distance_pct（預設 3%）
  C. 資料長度足夠（≥ N₂ 個交易日）

整個 pipeline：
  detect_rs_leading_high(stock, index) → RSLeadingHighSignal | None
                                          │
            apply_quality_filters(signal) → True/False  (Phase 2)
                                          │
            score_signal(signal, ctx)     → 0-100      (Phase 3)
                                          │
            scan_universe + rank          → top N      (CLI)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime

import numpy as np
import pandas as pd

from rs_line import calculate_rs_line, detect_rs_new_high

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────
# 訊號資料結構（T3 標準介面）
# ────────────────────────────────────────────────────────────────

@dataclass
class RSLeadingHighSignal:
    """單一股票在某一日的 RS 領先創新高訊號。

    這個 dataclass 是掃描器與 T3 評分系統之間的標準介面。
    """
    ticker: str
    signal_date: pd.Timestamp
    rs_value: float                      # 當日 RS Line 數值
    rs_lookback_high: float              # 過去 N 日 RS 最高值
    days_since_rs_high: int              # 距離 RS 最近一次創高的交易日數
    purple_dot_count_recent: int         # 近 20 日出現紫色點次數
    stock_price: float                   # 當日收盤價
    stock_distance_from_high_pct: float  # 股價距 N 日高點的百分比 (positive = 還有空間)
    rs_above_wma21: bool                 # RS 是否在 21 日 WMA 之上
    rs_long_term_trend_up: bool          # RS 長期趨勢是否向上（50d slope > 0）
    volume_ratio: float                  # 當日量 / 50 日均量
    # Phase 2 過濾結果
    passed_quality_filters: bool = False
    filter_failed_reasons: List[str] = field(default_factory=list)
    # Phase 3 評分
    quality_score: Optional[float] = None
    score_breakdown: Dict[str, float] = field(default_factory=dict)
    rank: Optional[int] = None
    # 主題分類（Eddy 整合）
    theme: Optional[str] = None
    # 附加診斷（debug 用）
    above_sma200: Optional[bool] = None
    dollar_volume_50d: Optional[float] = None
    rs_slope_50d: Optional[float] = None


# ────────────────────────────────────────────────────────────────
# Phase 1: 核心偵測函式
# ────────────────────────────────────────────────────────────────

def detect_rs_leading_high(
    stock_prices: pd.Series,
    index_prices: pd.Series,
    stock_volumes: pd.Series,
    ticker: str,
    as_of_date: pd.Timestamp,
    rs_new_high_lookback: int = 63,
    rs_high_recency_days: int = 5,
    min_distance_from_price_high: float = 0.03,
    purple_dot_window: int = 20,
) -> Optional[RSLeadingHighSignal]:
    """判斷某股票在 as_of_date 是否符合「RS 領先創新高」訊號。

    所有資料嚴格截至 as_of_date（含），無 look-ahead bias。

    Args:
        stock_prices: 股票調整後收盤價（DatetimeIndex）
        index_prices: 對應大盤指數收盤價
        stock_volumes: 股票成交量（與 stock_prices 同 index）
        ticker: ticker 符號
        as_of_date: 評估日（pd.Timestamp）
        rs_new_high_lookback: 「新高」的回看天數（預設 63 ≈ 3 月）
        rs_high_recency_days: RS 最近 N 日內須創過新高（預設 5）
        min_distance_from_price_high: 股價距高點至少 N%（預設 3%）
        purple_dot_window: 紫色點頻率計算視窗（預設 20）

    Returns:
        RSLeadingHighSignal 若符合，否則 None
    """
    # ── Step 1: 對齊 + 切片到 as_of_date ──
    if not isinstance(as_of_date, pd.Timestamp):
        as_of_date = pd.Timestamp(as_of_date)
    if as_of_date.tz is not None:
        as_of_date = as_of_date.tz_localize(None)

    # 🆕 時區正規化（避免 NVDA tz=NY vs ^GSPC tz=None 導致 intersection 空）
    def _norm_idx(s):
        s = s.copy()
        if s.index.tz is not None:
            s.index = s.index.tz_localize(None)
        s.index = s.index.normalize()
        return s

    stock_prices = _norm_idx(stock_prices)
    index_prices = _norm_idx(index_prices)
    stock_volumes = _norm_idx(stock_volumes)

    common = stock_prices.index.intersection(index_prices.index)
    if len(common) < rs_new_high_lookback + 1:
        return None

    stock = stock_prices.loc[common].astype(float)
    index = index_prices.loc[common].astype(float)
    vol = stock_volumes.reindex(common).astype(float)

    # 切片到 as_of_date（inclusive）
    stock = stock.loc[:as_of_date]
    index = index.loc[:as_of_date]
    vol = vol.loc[:as_of_date]

    if len(stock) < rs_new_high_lookback + 1:
        return None

    # ── Step 2: 計算 RS Line（截至當日）──
    rs = calculate_rs_line(stock, index)
    if len(rs) < rs_new_high_lookback + 1:
        return None

    cur_rs = float(rs.iloc[-1])
    cur_price = float(stock.iloc[-1])
    if not np.isfinite(cur_rs) or not np.isfinite(cur_price) or cur_price <= 0:
        return None

    # ── Step 3: 三個必要條件 ──
    rs_window = rs.iloc[-rs_new_high_lookback:]
    rs_lookback_high = float(rs_window.max())
    # 找 RS 最近一次創 lookback_high 的日期 index
    days_since_rs_high = _days_since_recent_high(rs_window)

    # 條件 A: RS 在 recency_days 內創過 lookback_high
    cond_a = (days_since_rs_high <= rs_high_recency_days)
    if not cond_a:
        return None

    # 條件 B: 股價未創新高（距高點 >= min_distance_pct）
    price_window = stock.iloc[-rs_new_high_lookback:]
    price_lookback_high = float(price_window.max())
    if price_lookback_high <= 0:
        return None
    distance_pct = (price_lookback_high - cur_price) / price_lookback_high
    cond_b = (distance_pct >= min_distance_from_price_high)
    if not cond_b:
        return None

    # ── Step 4: 計算附加指標（filters + scoring 用）──

    # 4a. 紫色點頻率：近 N 日中 RS 創過 lookback 新高的次數
    pd_window = rs.iloc[-purple_dot_window:]
    pd_is_high = detect_rs_new_high(rs.iloc[-(rs_new_high_lookback + purple_dot_window):],
                                       lookback=rs_new_high_lookback)
    purple_dots_recent = int(pd_is_high.iloc[-purple_dot_window:].sum())

    # 4b. RS vs 21d WMA
    rs_wma21 = _wma(rs.iloc[-50:], 21) if len(rs) >= 21 else pd.Series([cur_rs])
    rs_above_wma21 = bool(cur_rs > float(rs_wma21.iloc[-1]))

    # 4c. RS 50d slope（線性回歸）
    rs_slope_50d = _linear_slope(rs.iloc[-50:]) if len(rs) >= 50 else 0.0
    rs_long_term_trend_up = bool(rs_slope_50d > 0)

    # 4d. Volume ratio
    if len(vol.dropna()) >= 50:
        vol_avg_50 = float(vol.tail(50).mean())
        cur_vol = float(vol.iloc[-1])
        vol_ratio = (cur_vol / vol_avg_50) if vol_avg_50 > 0 else 1.0
    else:
        vol_ratio = 1.0

    # 4e. SMA200
    if len(stock) >= 200:
        sma200 = float(stock.tail(200).mean())
        above_sma200 = bool(cur_price > sma200)
    else:
        above_sma200 = None

    # 4f. Dollar volume 50d
    if len(vol.dropna()) >= 50 and len(stock) >= 50:
        dollar_vol_50d = float((stock.tail(50) * vol.tail(50)).mean())
    else:
        dollar_vol_50d = None

    # ── Step 5: 主題分類 ──
    try:
        from universes.us_universe import get_theme_for_ticker
        theme = get_theme_for_ticker(ticker)
    except Exception:
        theme = None

    return RSLeadingHighSignal(
        ticker=ticker,
        signal_date=as_of_date,
        rs_value=cur_rs,
        rs_lookback_high=rs_lookback_high,
        days_since_rs_high=days_since_rs_high,
        purple_dot_count_recent=purple_dots_recent,
        stock_price=cur_price,
        stock_distance_from_high_pct=float(distance_pct),
        rs_above_wma21=rs_above_wma21,
        rs_long_term_trend_up=rs_long_term_trend_up,
        volume_ratio=float(vol_ratio),
        above_sma200=above_sma200,
        dollar_volume_50d=dollar_vol_50d,
        rs_slope_50d=float(rs_slope_50d),
        theme=theme,
    )


# ────────────────────────────────────────────────────────────────
# Phase 2: 品質濾網
# ────────────────────────────────────────────────────────────────

def apply_quality_filters(
    signal: RSLeadingHighSignal,
    stock_prices: pd.Series,
    stock_volumes: pd.Series,
    market: str = 'US',
    include_recent_ipos: bool = False,
    min_dollar_vol_us: float = 1e7,    # $10M
    min_dollar_vol_tw: float = 5e7,    # NT$50M
) -> bool:
    """套用所有品質濾網，回傳 True 通過、False 淘汰。

    濾網列表：
      F1. RS 長期趨勢向上（50d slope > 0）
      F2. RS 線在 21d WMA 之上
      F3. 股價在 200d SMA 之上（William O'Neil 鐵律）
      F4. 流動性達標
      F5. 上市滿一年
    """
    reasons = []

    # F1
    if not signal.rs_long_term_trend_up:
        reasons.append(f'F1: RS 50d slope <= 0 ({signal.rs_slope_50d:.6f})')

    # F2
    if not signal.rs_above_wma21:
        reasons.append('F2: RS 在 21d WMA 之下')

    # F3
    if signal.above_sma200 is False:
        reasons.append('F3: 股價在 200d SMA 之下')
    elif signal.above_sma200 is None:
        # 不夠 200d 歷史
        if not include_recent_ipos:
            reasons.append('F3+F5: 上市未滿 200d，無法計算 SMA200')

    # F4: 流動性
    min_vol = min_dollar_vol_us if market == 'US' else min_dollar_vol_tw
    if signal.dollar_volume_50d is None:
        reasons.append('F4: 50d 流動性無法計算')
    elif signal.dollar_volume_50d < min_vol:
        reasons.append(f'F4: 流動性 {signal.dollar_volume_50d:.0f} < 門檻 {min_vol:.0f}')

    # F5: 上市滿一年
    if len(stock_prices.dropna()) < 252 and not include_recent_ipos:
        reasons.append(f'F5: 歷史 < 252d ({len(stock_prices.dropna())})')

    signal.filter_failed_reasons = reasons
    signal.passed_quality_filters = (len(reasons) == 0)

    if reasons:
        logger.debug(f'{signal.ticker} filtered out: {reasons}')

    return signal.passed_quality_filters


# ────────────────────────────────────────────────────────────────
# Phase 3: 訊號品質評分（0-100）
# ────────────────────────────────────────────────────────────────

def score_signal(
    signal: RSLeadingHighSignal,
    universe_context: Dict[str, Any],
) -> float:
    """計算單一訊號的綜合品質分數（0-100）。

    五個維度，每個 0-20 分：
      1. Purple Dot Frequency
      2. RS Slope Strength（截面標準化）
      3. Price Base Quality
      4. Volume Pattern
      5. Distance from Price High（倒 U 型函數）
    """
    sc1 = _score_purple_dot_frequency(signal.purple_dot_count_recent)
    sc2 = _score_rs_slope(signal, universe_context)
    sc3 = _score_base_quality(signal, universe_context)
    sc4 = _score_volume(signal)
    sc5 = _score_distance_from_high(signal.stock_distance_from_high_pct)

    total = sc1 + sc2 + sc3 + sc4 + sc5
    signal.score_breakdown = {
        'purple_dots': sc1, 'rs_slope': sc2, 'base_quality': sc3,
        'volume': sc4, 'distance': sc5,
    }
    signal.quality_score = round(total, 2)
    return signal.quality_score


def _score_purple_dot_frequency(count: int) -> float:
    """近 20 日紫色點次數 → 0-20 分
    1 次 → 4，5 次以上 → 20，線性遞增"""
    if count <= 0: return 0.0
    return min(20.0, count * 4.0)


def _score_rs_slope(signal, ctx) -> float:
    """RS 20d 對數迴歸斜率截面標準化 → 0-20 分
    斜率位於前 20%（pct >= 80）→ 滿分；中位數 → 10 分"""
    slope = signal.rs_slope_50d  # 用 50d slope 當代理
    slopes = ctx.get('rs_slopes', [])
    if not slopes or slope is None:
        return 10.0  # default 中位數
    # 計算 percentile
    rank = float(np.sum(np.asarray(slopes) <= slope)) / len(slopes) * 100
    # rank 0 → 0 分, rank 50 → 10 分, rank 100 → 20 分
    return float(rank / 100 * 20)


def _score_base_quality(signal, ctx) -> float:
    """股價整理品質：近 30 日 CV 越小越好 → 0-20 分"""
    cv = ctx.get(f'cv_{signal.ticker}')
    if cv is None:
        return 10.0
    # CV 越小越好；經驗：CV < 2% 滿分、CV > 10% 0 分
    if cv < 0.02: return 20.0
    if cv > 0.10: return 0.0
    return float(20 - (cv - 0.02) / 0.08 * 20)


def _score_volume(signal) -> float:
    """成交量結構：當日量 / 50d 均量 > 1.5 滿分"""
    v = signal.volume_ratio
    if v >= 1.5: return 20.0
    if v <= 0.7: return 0.0
    return float((v - 0.7) / 0.8 * 20)


def _score_distance_from_high(dist_pct: float) -> float:
    """距離高點程度：5%-15% 區間滿分（倒 U 型）"""
    # dist_pct: 0.05 → 滿分, 0.15 → 滿分；< 0.03 / > 0.25 → 0 分
    if dist_pct < 0.03 or dist_pct > 0.25:
        return 0.0
    if 0.05 <= dist_pct <= 0.15:
        return 20.0
    if dist_pct < 0.05:
        return float((dist_pct - 0.03) / 0.02 * 20)
    # dist_pct in (0.15, 0.25]
    return float((0.25 - dist_pct) / 0.10 * 20)


# ────────────────────────────────────────────────────────────────
# Phase 1+3: 完整宇宙掃描 + 排序
# ────────────────────────────────────────────────────────────────

def scan_universe(
    universe: List[str],
    as_of_date: pd.Timestamp,
    market: str = 'US',
    data_loader_fn=None,
    index_ticker: Optional[str] = None,
    apply_filters: bool = True,
    score_and_rank: bool = True,
    rs_new_high_lookback: int = 63,
    rs_high_recency_days: int = 5,
    min_distance_from_price_high: float = 0.03,
    include_recent_ipos: bool = False,
    cache_dir: str = 'data_cache',
) -> List[RSLeadingHighSignal]:
    """對整個宇宙掃描，回傳排序後的訊號清單。

    Args:
        universe: ticker 清單
        as_of_date: 評估日
        market: 'US' or 'TW'
        data_loader_fn: 自訂載入函式 (ticker) → pd.DataFrame；預設用 data_cache
        index_ticker: 大盤索引 ticker；預設 US=SPY/^GSPC, TW=^TWII
        apply_filters: 套用 Phase 2 濾網
        score_and_rank: 跑 Phase 3 評分 + 排序

    Returns:
        排序後的訊號清單（按 quality_score DESC，若無評分則保持原序）
    """
    if not isinstance(as_of_date, pd.Timestamp):
        as_of_date = pd.Timestamp(as_of_date)

    # 載入指數
    if index_ticker is None:
        index_ticker = '^GSPC' if market == 'US' else '^TWII'

    if data_loader_fn is None:
        data_loader_fn = _default_data_loader(cache_dir)

    try:
        idx_df = data_loader_fn(index_ticker)
    except Exception as e:
        logger.error(f'無法載入指數 {index_ticker}: {e}')
        return []
    if idx_df is None or len(idx_df) < rs_new_high_lookback:
        logger.error(f'指數 {index_ticker} 資料不足')
        return []

    index_prices = idx_df['Close'].astype(float)

    # 偵測訊號
    raw_signals = []
    for ticker in universe:
        try:
            df = data_loader_fn(ticker)
            if df is None or len(df) < rs_new_high_lookback:
                continue
            stock_prices = df['Close'].astype(float)
            stock_volumes = df['Volume'].astype(float)
            sig = detect_rs_leading_high(
                stock_prices=stock_prices,
                index_prices=index_prices,
                stock_volumes=stock_volumes,
                ticker=ticker,
                as_of_date=as_of_date,
                rs_new_high_lookback=rs_new_high_lookback,
                rs_high_recency_days=rs_high_recency_days,
                min_distance_from_price_high=min_distance_from_price_high,
            )
            if sig is not None:
                raw_signals.append((sig, stock_prices, stock_volumes))
        except Exception as e:
            logger.debug(f'掃描 {ticker} 例外: {e}')
            continue

    if not raw_signals:
        return []

    # 套用 Phase 2 濾網
    if apply_filters:
        passed = []
        for sig, sp, sv in raw_signals:
            if apply_quality_filters(sig, sp, sv, market=market,
                                       include_recent_ipos=include_recent_ipos):
                passed.append((sig, sp, sv))
        raw_signals = passed

    if not raw_signals:
        return []

    # 建 universe_context 給 scoring
    if score_and_rank:
        rs_slopes = [s.rs_slope_50d for s, _, _ in raw_signals
                      if s.rs_slope_50d is not None]
        ctx = {'rs_slopes': rs_slopes}
        for sig, sp, _ in raw_signals:
            # 30d CV — tz 正規化避免 slice 錯誤
            sp_n = sp.copy()
            if sp_n.index.tz is not None:
                sp_n.index = sp_n.index.tz_localize(None)
            sp_n.index = sp_n.index.normalize()
            recent = sp_n.loc[:as_of_date].tail(30)
            if len(recent) >= 10:
                cv = float(recent.std() / recent.mean()) if recent.mean() > 0 else 1.0
                ctx[f'cv_{sig.ticker}'] = cv

        for sig, _, _ in raw_signals:
            score_signal(sig, ctx)

        # 排序 + 填 rank
        raw_signals.sort(key=lambda x: -(x[0].quality_score or 0))
        for i, (sig, _, _) in enumerate(raw_signals):
            sig.rank = i + 1

    return [s for s, _, _ in raw_signals]


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def _days_since_recent_high(s: pd.Series) -> int:
    """回傳序列中距今最近一次達到 max 的日數"""
    m = s.max()
    eq_idx = np.where(s.values >= m * (1 - 1e-9))[0]
    if len(eq_idx) == 0: return -1
    return int(len(s) - 1 - eq_idx[-1])


def _wma(s: pd.Series, period: int) -> pd.Series:
    """加權移動平均"""
    weights = np.arange(1, period + 1) / np.arange(1, period + 1).sum()
    return s.rolling(window=period).apply(
        lambda x: np.dot(x, weights), raw=True)


def _linear_slope(s: pd.Series) -> float:
    """簡單線性回歸 slope (per-day)"""
    s = s.dropna()
    if len(s) < 5:
        return 0.0
    x = np.arange(len(s))
    y = s.values
    slope, _ = np.polyfit(x, y, 1)
    return float(slope)


def _default_data_loader(cache_dir: str):
    """預設用 data_loader.load_from_cache"""
    def loader(ticker):
        try:
            import data_loader
            return data_loader.load_from_cache(ticker)
        except Exception:
            return None
    return loader


# ────────────────────────────────────────────────────────────────
# CLI / 報表輸出
# ────────────────────────────────────────────────────────────────

def signal_to_dict(s: RSLeadingHighSignal) -> dict:
    """轉成 JSON-friendly dict"""
    d = asdict(s)
    d['signal_date'] = s.signal_date.strftime('%Y-%m-%d') if isinstance(s.signal_date, pd.Timestamp) else str(s.signal_date)
    return d


def export_signals_to_json(signals: List[RSLeadingHighSignal], path: str):
    """匯出 JSON（T3 系統可吸收的格式）"""
    import json
    data = {
        'meta': {
            'generated_at': datetime.now().isoformat(),
            'scanner': 'rs_leading_high',
            'version': '1.0',
            'total_signals': len(signals),
        },
        'signals': [signal_to_dict(s) for s in signals],
    }
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def print_console_report(signals: List[RSLeadingHighSignal], top_n: int = 30):
    """終端機表格輸出"""
    if not signals:
        print('(no signals found)')
        return

    n_show = min(top_n, len(signals))
    print(f'\n{"="*120}')
    print(f'RS 領先創新高訊號  —  {signals[0].signal_date.strftime("%Y-%m-%d")}'
          f'  |  通過品質濾網 {len(signals)} 檔，顯示前 {n_show}')
    print('='*120)
    header = (f'{"Rank":>4s}  {"Ticker":<10s} {"Score":>7s} {"Price":>9s} '
              f'{"DistHi%":>8s} {"PurpD":>6s} {"VolR":>6s} '
              f'{"RSslope":>9s}  {"Theme":<14s}')
    print(header)
    print('-' * 120)
    for s in signals[:n_show]:
        theme = (s.theme or '')[:14]
        print(f'{s.rank:>4d}  {s.ticker:<10s} '
              f'{s.quality_score:>7.1f} ${s.stock_price:>8.2f} '
              f'{s.stock_distance_from_high_pct*100:>7.2f}% '
              f'{s.purple_dot_count_recent:>6d} {s.volume_ratio:>6.2f} '
              f'{s.rs_slope_50d:>9.4f}  {theme:<14s}')
    print()


# ────────────────────────────────────────────────────────────────
# Main CLI
# ────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description='RS Leading High Scanner')
    parser.add_argument('--market', type=str, default='US',
                          choices=['US', 'TW'])
    parser.add_argument('--universe', type=str, default=None,
                          help='Universe name (e.g. SP500, LIQUID_3000, TW50)')
    parser.add_argument('--date', type=str, default=None,
                          help='As-of date YYYY-MM-DD (default: latest)')
    parser.add_argument('--lookback', type=int, default=63)
    parser.add_argument('--recency', type=int, default=5)
    parser.add_argument('--min-distance', type=float, default=0.03)
    parser.add_argument('--top-n', type=int, default=30)
    parser.add_argument('--include-ipos', action='store_true')
    parser.add_argument('--output', type=str, default='console',
                          help='comma-separated: console,json,excel')
    parser.add_argument('--out-dir', type=str, default='reports')

    args = parser.parse_args()

    # 取得 universe
    if args.universe is None:
        args.universe = 'LIQUID_3000' if args.market == 'US' else 'LIQUID_TW'

    if args.market == 'US':
        from universes.us_universe import get_universe
    else:
        from universes.tw_universe import get_universe
    tickers = get_universe(args.universe)
    print(f'[Universe] {args.universe}: {len(tickers)} tickers')

    # As-of date
    as_of = pd.Timestamp(args.date) if args.date else pd.Timestamp.now().normalize()
    print(f'[Date] {as_of.strftime("%Y-%m-%d")}')

    # 掃描
    signals = scan_universe(
        universe=tickers,
        as_of_date=as_of,
        market=args.market,
        rs_new_high_lookback=args.lookback,
        rs_high_recency_days=args.recency,
        min_distance_from_price_high=args.min_distance,
        include_recent_ipos=args.include_ipos,
    )

    # 輸出
    outputs = [o.strip() for o in args.output.split(',')]
    os.makedirs(args.out_dir, exist_ok=True)
    date_tag = as_of.strftime('%Y-%m-%d')
    base = f'rs_leading_high_{args.market}_{date_tag}'

    if 'console' in outputs:
        print_console_report(signals, top_n=args.top_n)

    if 'json' in outputs:
        json_path = os.path.join(args.out_dir, f'{base}.json')
        export_signals_to_json(signals[:args.top_n], json_path)
        print(f'[Saved] {json_path}')

    if 'excel' in outputs:
        try:
            from integrations.excel_export import export_signals_to_excel
            xlsx_path = os.path.join(args.out_dir, f'{base}.xlsx')
            export_signals_to_excel(signals[:args.top_n], xlsx_path, as_of_date=as_of)
            print(f'[Saved] {xlsx_path}')
        except ImportError:
            print('[Warn] excel_export module not found; skip xlsx output')

    return signals


if __name__ == '__main__':
    main()
