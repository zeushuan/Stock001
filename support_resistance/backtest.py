"""support_resistance.backtest — Phase 7 命中率驗證
======================================================

對應規格《tv_app 壓力／支撐區偵測》§8 驗證方法論：

  1. 趨勢中斷命中率 (trend-interruption accuracy)：
     價格接觸區後 X 根 bar 內是否反轉（Osler / Zapranis 核心指標）
  2. 支撐 vs 壓力分組比較：
     驗證 §1.1「支撐預測力較佳」(Zapranis 2012)
  3. 多源 confluence 區 vs 單源區：
     驗證 §2.1 融合假設 — 多源重疊命中率較高

設計：walk-forward
  - 對每根 bar j，用 [0..j-1] 算 zones（嚴格無 look-ahead）
  - 若 bar j 的 High/Low 落入任一強度足夠 zone，記為一次 touch event
  - 看接下來 reaction_window 根 bar 的收盤是否反轉 ≥ reversal_pct%

公開 API:
  - backtest_one(df, **kwargs) -> dict   單檔 walk-forward
  - aggregate(results) -> dict           多檔合併聚合
  - format_report(stats) -> str          人類可讀文字報表
  - main(argv) -> None                   CLI 進入點

CLI:
  python -m support_resistance.backtest SPY NVDA AAPL [--period 1y]
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field, replace
from typing import Optional

import numpy as np
import pandas as pd

from .params import MIN_BARS_FOR_DETECTION
from .sr_engine import detect_sr_zones
from .types import SRZone


# ── Baseline helper: 隨機化 zone 位置 ──────────────────────────
def _randomize_zones(
    zones: list[SRZone],
    df: pd.DataFrame,
    rng: np.random.Generator,
) -> list[SRZone]:
    """為驗證演算法資訊量產生「null model」zones。

    保持 kind / 寬度 / strength / source_count 不變（讓所有非位置的
    可能 confound 都受控），只把 center 改為在 df 全範圍內 uniform random。
    """
    if not zones or df is None or len(df) == 0:
        return []
    lo = float(df['Low'].min())
    hi = float(df['High'].max())
    if hi <= lo:
        return zones
    out: list[SRZone] = []
    for z in zones:
        width = max(z.high - z.low, 0.01)
        # 隨機中心，保證 [lo, hi] 完全涵蓋
        new_center = float(rng.uniform(lo + width / 2, hi - width / 2))
        out.append(replace(
            z,
            low=new_center - width / 2,
            high=new_center + width / 2,
            center=new_center,
            source='random',
            role_reversal=False,
        ))
    return out


# ── 回測事件 dataclass ──────────────────────────────────────────
@dataclass
class TouchEvent:
    """單一觸及事件。"""
    symbol:       str
    bar_idx:      int          # 觸及發生的 bar
    zone_kind:    str          # 'support' / 'resistance'
    zone_source:  str          # 'swing' / 'profile' / 'round' / 'fused'
    zone_strength: float       # 0-100
    zone_source_count: int     # 1 / 2 / 3（多源重疊度）
    touched_price: float       # bar j 的 High（res 觸及）或 Low（sup 觸及）
    reversal_pct: float        # 觸及後 reaction_window 內最大反轉 %
    reversed:     bool         # reversal_pct >= 門檻


# ── 主邏輯：walk-forward 單檔回測 ───────────────────────────────
def backtest_one(
    df: pd.DataFrame,
    symbol: str = '?',
    warmup: int = 60,
    reaction_window: int = 5,
    reversal_pct: float = 1.0,
    min_strength: float = 30.0,
    recompute_every: int = 5,
    detect_kwargs: Optional[dict] = None,
    baseline_seed: Optional[int] = None,
) -> list[TouchEvent]:
    """Walk-forward backtest 單一 symbol。

    Args:
        df: OHLCV (High, Low, Close, Volume；index 任意)
        symbol: 標的代號，記在 event 內
        warmup: 從第 N 根 bar 開始測（給 SR 偵測一個 warmup）
        reaction_window: 觸及後看幾根 bar 判斷反轉
        reversal_pct: 反轉 % 門檻（close 偏離觸及價的最大幅度）
        min_strength: 只測強度 ≥ 此值的 zone
        recompute_every: 每 N 根 bar 重算一次 SR（=1 嚴格無 lookahead 但慢;
                         >1 略加速，最新 zones 略落後）
        detect_kwargs: 傳入 detect_sr_zones 的額外參數
        baseline_seed: 🆕 v9.47.4：若設，把 detect 出的 zones 用同寬度同 kind
                       隨機重新放置（null model；驗證演算法是否真有資訊量）

    Returns:
        list[TouchEvent]：每根 bar 至多一筆（觸及最強 zone）
    """
    if df is None or len(df) < warmup + reaction_window + 1:
        return []
    detect_kwargs = detect_kwargs or {}
    n = len(df)
    events: list[TouchEvent] = []
    zones: list[SRZone] = []
    last_recompute = -10**9
    rng = (np.random.default_rng(baseline_seed)
           if baseline_seed is not None else None)

    for j in range(warmup, n - reaction_window - 1):
        # 重算 SR：用 [0:j]（嚴格無未來）
        if j - last_recompute >= recompute_every:
            sub = df.iloc[:j]
            if len(sub) >= MIN_BARS_FOR_DETECTION:
                zones = [z for z in detect_sr_zones(sub, **detect_kwargs)
                          if z.strength >= min_strength]
                # 🆕 baseline：把真實 zones 隨機重新放置
                if rng is not None:
                    zones = _randomize_zones(zones, sub, rng)
            last_recompute = j

        if not zones:
            continue

        bar_j = df.iloc[j]
        hi_j, lo_j = float(bar_j['High']), float(bar_j['Low'])
        # 找該 bar 觸及的最強 zone（每根 bar 至多 1 個 event）
        best_evt: Optional[TouchEvent] = None
        for z in zones:
            if z.kind == 'resistance':
                touched = (z.low <= hi_j <= z.high)
                touch_price = hi_j
            else:
                touched = (z.low <= lo_j <= z.high)
                touch_price = lo_j
            if not touched:
                continue

            # 反轉判定：看 [j+1, j+1+reaction_window]
            end = min(j + 1 + reaction_window, n)
            if end <= j + 1:
                continue
            future_close = df['Close'].iloc[j + 1:end]
            if z.kind == 'resistance':
                # 期望跌：min(future_close) 距 touch_price 越大越好
                drop_pct = (touch_price - float(future_close.min())) / touch_price * 100
                rev_mag = drop_pct
            else:
                # 期望漲：max(future_close) 距 touch_price 越大越好
                rise_pct = (float(future_close.max()) - touch_price) / touch_price * 100
                rev_mag = rise_pct

            src_set = (z.components.get('_source_set', {z.source})
                       if isinstance(z.components, dict) else {z.source})
            evt = TouchEvent(
                symbol=symbol, bar_idx=j,
                zone_kind=z.kind, zone_source=z.source,
                zone_strength=z.strength,
                zone_source_count=len(src_set),
                touched_price=float(touch_price),
                reversal_pct=float(rev_mag),
                reversed=(rev_mag >= reversal_pct),
            )
            # 取最強 zone 的 event
            if best_evt is None or z.strength > best_evt.zone_strength:
                best_evt = evt
        if best_evt is not None:
            events.append(best_evt)

    return events


# ── 聚合 ────────────────────────────────────────────────────────
def aggregate(events: list[TouchEvent]) -> dict:
    """把 events 聚合成 by-kind / by-source-count / by-strength 命中率。"""
    if not events:
        return {
            'n_events': 0,
            'overall_hit_rate': None,
            'by_kind': {},
            'by_source_count': {},
            'by_strength': {},
            'mean_reversal_pct': None,
        }

    def _stats(evts: list[TouchEvent]) -> dict:
        if not evts:
            return {'n': 0, 'hit_rate': None, 'mean_reversal_pct': None}
        n = len(evts)
        hits = sum(1 for e in evts if e.reversed)
        return {
            'n': n,
            'hit_rate': hits / n,
            'mean_reversal_pct': float(np.mean([e.reversal_pct for e in evts])),
        }

    by_kind = {
        'support':    _stats([e for e in events if e.zone_kind == 'support']),
        'resistance': _stats([e for e in events if e.zone_kind == 'resistance']),
    }
    by_source_count = {
        c: _stats([e for e in events if e.zone_source_count == c])
        for c in (1, 2, 3)
    }
    by_strength = {
        'low(30-50)':  _stats([e for e in events if 30 <= e.zone_strength < 50]),
        'mid(50-75)':  _stats([e for e in events if 50 <= e.zone_strength < 75]),
        'high(75-100)':_stats([e for e in events if 75 <= e.zone_strength <= 100]),
    }
    overall = _stats(events)
    return {
        'n_events': overall['n'],
        'overall_hit_rate': overall['hit_rate'],
        'mean_reversal_pct': overall['mean_reversal_pct'],
        'by_kind': by_kind,
        'by_source_count': by_source_count,
        'by_strength': by_strength,
    }


# ── 文字報表格式化 ──────────────────────────────────────────────
def format_report(stats: dict, title: str = 'S/R Backtest Report') -> str:
    """把 aggregate 結果格式化成人類可讀文字報表。"""
    def _row(name: str, s: dict) -> str:
        n = s.get('n', 0)
        hr = s.get('hit_rate')
        mr = s.get('mean_reversal_pct')
        hr_s = f'{hr:6.1%}' if hr is not None else '   --'
        mr_s = f'{mr:5.2f}%' if mr is not None else '   --'
        return f'  {name:16s}  n={n:5d}  hit_rate={hr_s}  mean_rev={mr_s}'

    lines = [
        '=' * 70,
        title,
        '=' * 70,
        f'  n_events: {stats["n_events"]}'
        f'   overall hit_rate: {stats["overall_hit_rate"]:.1%}'
        if stats['n_events'] else '  (no events)',
        '',
        'By kind (規格 §1.1：支撐預測力應較佳):',
    ]
    for k, s in stats.get('by_kind', {}).items():
        lines.append(_row(k, s))
    lines.append('')
    lines.append('By source count (規格 §2.1：多源 confluence 應較高):')
    for c, s in stats.get('by_source_count', {}).items():
        lines.append(_row(f'{c} source(s)', s))
    lines.append('')
    lines.append('By strength bucket:')
    for k, s in stats.get('by_strength', {}).items():
        lines.append(_row(k, s))
    lines.append('=' * 70)
    return '\n'.join(lines)


# ── CLI ─────────────────────────────────────────────────────────
def main(argv: Optional[list[str]] = None) -> int:
    """python -m support_resistance.backtest SPY NVDA AAPL [--period 1y]"""
    parser = argparse.ArgumentParser(
        prog='support_resistance.backtest',
        description='Walk-forward S/R zone hit-rate backtest (規格 §8)',
    )
    parser.add_argument('symbols', nargs='+', help='Ticker symbols (yfinance compatible)')
    parser.add_argument('--period', default='1y',
                        help='yfinance history period (default 1y)')
    parser.add_argument('--warmup', type=int, default=60)
    parser.add_argument('--reaction-window', type=int, default=5)
    parser.add_argument('--reversal-pct', type=float, default=1.0,
                        help='反轉幅度門檻 %% (default 1.0)')
    parser.add_argument('--min-strength', type=float, default=30.0)
    parser.add_argument('--recompute-every', type=int, default=5)
    parser.add_argument('--baseline', action='store_true',
                        help='另外跑一次 random-zone baseline 比較 lift')
    parser.add_argument('--baseline-seed', type=int, default=42,
                        help='baseline 隨機種子（預設 42）')
    args = parser.parse_args(argv)

    try:
        import yfinance as yf
    except ImportError:
        print('需要 yfinance：pip install yfinance', file=sys.stderr)
        return 2

    all_events: list[TouchEvent] = []
    all_baseline_events: list[TouchEvent] = []
    per_symbol: dict[str, dict] = {}
    for sym in args.symbols:
        try:
            df = yf.Ticker(sym).history(period=args.period, auto_adjust=True)
            if df is None or len(df) < args.warmup + 10:
                print(f'[{sym}] insufficient data ({len(df) if df is not None else 0} bars), skip')
                continue
            df = df.rename(columns=str.title)[['Open', 'High', 'Low', 'Close', 'Volume']]
            evts = backtest_one(
                df, symbol=sym,
                warmup=args.warmup,
                reaction_window=args.reaction_window,
                reversal_pct=args.reversal_pct,
                min_strength=args.min_strength,
                recompute_every=args.recompute_every,
            )
            stats = aggregate(evts)
            per_symbol[sym] = stats
            all_events.extend(evts)
            print(format_report(stats, title=f'{sym}  ({len(df)} bars, period={args.period})'))

            # 🆕 baseline 比較
            if args.baseline:
                evts_b = backtest_one(
                    df, symbol=sym + '[BL]',
                    warmup=args.warmup,
                    reaction_window=args.reaction_window,
                    reversal_pct=args.reversal_pct,
                    min_strength=args.min_strength,
                    recompute_every=args.recompute_every,
                    baseline_seed=args.baseline_seed,
                )
                stats_b = aggregate(evts_b)
                all_baseline_events.extend(evts_b)
                print(format_report(stats_b,
                                     title=f'{sym} — RANDOM BASELINE (seed={args.baseline_seed})'))
                if stats['overall_hit_rate'] and stats_b['overall_hit_rate']:
                    lift = (stats['overall_hit_rate'] - stats_b['overall_hit_rate']) * 100
                    print(f'  >> Lift: {lift:+.1f}pp  '
                           f'(real {stats["overall_hit_rate"]:.1%}  '
                           f'vs random {stats_b["overall_hit_rate"]:.1%})')
            print()
        except Exception as e:
            print(f'[{sym}] error: {type(e).__name__}: {e}', file=sys.stderr)

    if len(args.symbols) > 1:
        agg = aggregate(all_events)
        print(format_report(agg, title=f'AGGREGATE ({len(args.symbols)} symbols)'))
        if args.baseline:
            agg_b = aggregate(all_baseline_events)
            print(format_report(agg_b, title=f'AGGREGATE — RANDOM BASELINE'))
            if agg['overall_hit_rate'] and agg_b['overall_hit_rate']:
                lift = (agg['overall_hit_rate'] - agg_b['overall_hit_rate']) * 100
                print(f'\n  >> Overall Lift: {lift:+.1f}pp')
                # by-kind lift
                for k in ('support', 'resistance'):
                    real = agg['by_kind'].get(k, {}).get('hit_rate')
                    rand = agg_b['by_kind'].get(k, {}).get('hit_rate')
                    if real and rand:
                        print(f'  >> {k:11s} Lift: {(real - rand) * 100:+.1f}pp  '
                               f'(real {real:.1%} vs random {rand:.1%})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
