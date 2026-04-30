"""Pairs Trading 跨市場套利研究
======================================
基於前面研究：
  TSM ADR → 2330 Lag-1 相關 +0.523（最強指引）
  SOX → 2330 +0.474

研究問題：
  Q1: TSM 隔夜漲跌 → 2330 開盤反應幅度規律
  Q2: 「TSM 漲 X% 但 2330 開盤跟漲不足」→ 補漲機率
  Q3: SOX vs TSM 哪個是更好的領先指標
  Q4: 其他半導體配對（AMD↔聯電、INTC↔聯電 等）
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl


def load_close(ticker):
    df = dl.load_from_cache(ticker)
    if df is None: return None
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)
    df = df[df.index >= pd.Timestamp('2020-01-01')]
    return df


def analyze_pair(us_name, us_df, tw_name, tw_df, label):
    """美股 t-1 漲跌 vs 台股 t 反應"""
    us_ret = us_df['Close'].pct_change() * 100
    tw_close = tw_df['Close']
    tw_open = tw_df['Open'] if 'Open' in tw_df.columns else tw_close
    tw_close_ret = tw_close.pct_change() * 100
    # gap = (today_open - prev_close) / prev_close
    tw_gap = (tw_open / tw_close.shift(1) - 1) * 100
    # full-day = close-to-close
    tw_full = tw_close_ret

    us_lagged = us_ret.shift(1)
    df = pd.concat([us_lagged, tw_gap, tw_full], axis=1, join='inner').dropna()
    df.columns = ['us_prev', 'tw_gap', 'tw_full']

    if len(df) < 100:
        print(f"  {label}: 樣本不足")
        return None

    print(f"\n📊 {label}（{us_name} → {tw_name}）")

    # 分群分析
    bins = [(-100, -3), (-3, -1.5), (-1.5, -0.5), (-0.5, 0.5),
            (0.5, 1.5), (1.5, 3), (3, 100)]
    labels = ['<-3%', '-3~-1.5%', '-1.5~-0.5%', '-0.5~+0.5%',
              '+0.5~+1.5%', '+1.5~+3%', '>+3%']
    print(f"  {'美股漲跌':<14} {'樣本':>5} {'TW gap%':>8} {'TW full%':>9} "
          f"{'gap/us 比':>10} {'full/us 比':>11} {'補漲機率%':>10}")
    print("-" * 100)

    rows = {}
    for (lo, hi), lbl in zip(bins, labels):
        mask = (df['us_prev'] >= lo) & (df['us_prev'] < hi)
        sub = df[mask]
        if len(sub) < 5: continue
        avg_gap = sub['tw_gap'].mean()
        avg_full = sub['tw_full'].mean()
        avg_us = sub['us_prev'].mean()
        gap_ratio = avg_gap / avg_us if abs(avg_us) > 0.01 else 0
        full_ratio = avg_full / avg_us if abs(avg_us) > 0.01 else 0
        # 補漲機率 = US 漲且 TW gap 小於預期的後續日內補漲機率
        if avg_us > 0:
            # gap 小於應有比例（< gap_ratio 平均）的次數，盤中補漲到 full > gap
            chasers = sub[sub['tw_full'] > sub['tw_gap']]
            chase_prob = len(chasers) / len(sub) * 100 if len(sub) > 0 else 0
        else:
            chase_prob = 0
        rows[lbl] = {
            'n': int(len(sub)),
            'avg_gap': float(avg_gap), 'avg_full': float(avg_full),
            'gap_ratio': float(gap_ratio), 'full_ratio': float(full_ratio),
            'chase_prob': float(chase_prob),
        }
        print(f"  {lbl:<14} {len(sub):>5} {avg_gap:>+8.2f} {avg_full:>+9.2f} "
              f"{gap_ratio:>+10.2f} {full_ratio:>+11.2f} {chase_prob:>+10.1f}")
    return rows


def pairs_strategy_simulation(us_df, tw_df, threshold_us=2.0, threshold_diff=0.5):
    """Pairs Trading 模擬：
    當 US 漲 > threshold_us% 但 TW gap 跟漲 < (threshold_us × beta - threshold_diff)
    → 開盤買 TW，預期日內補漲
    """
    us_ret = us_df['Close'].pct_change() * 100
    tw_close = tw_df['Close']
    tw_open = tw_df['Open'] if 'Open' in tw_df.columns else tw_close
    tw_gap = (tw_open / tw_close.shift(1) - 1) * 100
    tw_full = tw_close.pct_change() * 100
    # 日內 = full - gap
    tw_intraday = tw_full - tw_gap
    us_lagged = us_ret.shift(1)

    df = pd.concat([us_lagged, tw_gap, tw_intraday, tw_full],
                   axis=1, join='inner').dropna()
    df.columns = ['us_prev', 'tw_gap', 'tw_intraday', 'tw_full']

    # 估計 beta（從歷史 us → tw_gap 算）
    if np.var(df['us_prev']) > 0:
        beta_gap = np.cov(df['us_prev'], df['tw_gap'])[0, 1] / np.var(df['us_prev'])
    else:
        return None

    # 套利機會：US 漲 > 閾值 + TW 跟漲不足
    expected_gap = beta_gap * df['us_prev']
    deviation = df['tw_gap'] - expected_gap
    # 條件：US 漲多但 gap 顯著低於 expected
    long_signal = (df['us_prev'] > threshold_us) & (deviation < -threshold_diff)
    short_signal = (df['us_prev'] < -threshold_us) & (deviation > threshold_diff)

    long_hits = df[long_signal]
    short_hits = df[short_signal]

    print(f"\n📊 Pairs Trading 模擬（買 = US 漲多但 TW 開盤跟漲不足）")
    print(f"  Beta(gap) = {beta_gap:.3f}")
    print(f"  US 漲 > {threshold_us}% + TW gap < expected - {threshold_diff}%")
    print(f"  Long 訊號: {len(long_hits)} 次")
    if len(long_hits) > 0:
        print(f"    日內補漲均報: {long_hits['tw_intraday'].mean():+.2f}%")
        print(f"    全日均報:     {long_hits['tw_full'].mean():+.2f}%")
        print(f"    補漲機率:     {(long_hits['tw_intraday'] > 0).mean()*100:.1f}%")
    print(f"  Short 訊號: {len(short_hits)} 次")
    if len(short_hits) > 0:
        print(f"    日內續跌均報: {short_hits['tw_intraday'].mean():+.2f}%")
    return {
        'beta_gap': float(beta_gap),
        'long_n': int(len(long_hits)),
        'long_intraday': float(long_hits['tw_intraday'].mean()) if len(long_hits) > 0 else 0,
        'long_full': float(long_hits['tw_full'].mean()) if len(long_hits) > 0 else 0,
        'long_win': float((long_hits['tw_intraday'] > 0).mean() * 100) if len(long_hits) > 0 else 0,
        'short_n': int(len(short_hits)),
    }


def main():
    print("=" * 100)
    print("🌏 Pairs Trading 跨市場套利研究")
    print("=" * 100)

    pairs_data = {}

    # ── 主要配對 ──
    pairs = [
        ('TSM', '2330', 'TSM ADR ↔ 台積電'),
        ('AMD', '2303', 'AMD ↔ 聯電'),
        ('INTC', '2303', 'Intel ↔ 聯電'),
        ('NVDA', '2330', 'NVIDIA ↔ 台積電'),
        ('NVDA', '2454', 'NVIDIA ↔ 聯發科'),
        ('AAPL', '2317', 'Apple ↔ 鴻海'),
        ('AAPL', '2382', 'Apple ↔ 廣達'),
        ('META', '2330', 'Meta ↔ 台積電'),
        ('AMZN', '2330', 'Amazon ↔ 台積電'),
        ('MU', '2408', 'Micron ↔ 南亞科'),
    ]

    for us_t, tw_t, label in pairs:
        us_df = load_close(us_t)
        tw_df = load_close(tw_t)
        if us_df is None or tw_df is None:
            print(f"  {label}: 資料缺")
            continue
        rows = analyze_pair(us_t, us_df, tw_t, tw_df, label)
        if rows:
            pairs_data[label] = rows

    # ── Pairs Trading 模擬：TSM ↔ 2330 ──
    print("\n" + "=" * 100)
    print("⭐ Pairs Trading 模擬詳細（TSM ↔ 2330）")
    print("=" * 100)
    tsm = load_close('TSM')
    tsmc = load_close('2330')
    if tsm is not None and tsmc is not None:
        for thr_us, thr_diff in [(2.0, 0.5), (1.5, 0.3), (3.0, 1.0)]:
            r = pairs_strategy_simulation(tsm, tsmc, thr_us, thr_diff)
            if r:
                pairs_data[f'TSM_2330_thr{thr_us}_{thr_diff}'] = r

    # ── 寫 JSON ──
    with open('pairs_trading.json', 'w', encoding='utf-8') as f:
        json.dump(pairs_data, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 pairs_trading.json")


if __name__ == '__main__':
    main()
