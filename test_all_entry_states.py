"""所有進場狀態勝率比較
=============================
按 v8 / get_rec_label 邏輯分類進場狀態：

  飆股            多頭 + ADX≥30 + cross_days ≤ 10（強+新交叉）
  T1 穩健進場     多頭 + ADX 22-30 + cross_days ≤ 10（一般金叉）
  T3 強趨勢拉回   多頭 + ADX≥30 + RSI 35-50（強趨勢中拉回）
  T3 拉回進場     多頭 + ADX 22-30 + RSI 35-50（一般拉回）
  T4 反彈        空頭 + RSI<32 + 連續上升
  等待 T3 拉回    多頭 + ADX≥22 + RSI 50-65（等待中）
  等待回調       多頭 + ADX≥22 + RSI ≥ 70（過熱）

每個狀態 30 天持有後計算勝率 / 均報酬 / RR。
另：對 T3 訊號加入「信心度」分組（高 4-5 分 / 中 2-3 分 / 低 0-1 分）。
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
import ta
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

# 🆕 全市場：data_cache 中所有 4 位數股票（排除 00-prefix ETF）
DATA = Path('data_cache')
UNIVERSE = sorted([p.stem for p in DATA.glob('*.parquet')
                   if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                   and not p.stem.startswith('00')])

HOLD = 30


def compute_t3_conf(close_v, e5_now, e20_now, e5_5d, e20_5d):
    """T3 信心度 0-5"""
    score = 0
    if e20_now is not None and close_v > e20_now: score += 1
    e20_up = e20_now is not None and e20_5d is not None and e20_now > e20_5d
    if e20_up: score += 1
    e5_up = e5_now is not None and e5_5d is not None and e5_now > e5_5d
    if e5_up: score += 1
    if e5_now is not None and e20_now is not None and e5_now > e20_now: score += 1
    if e5_up and e20_up: score += 1
    return score


def analyze_one(ticker):
    df = dl.load_from_cache(ticker)
    if df is None or len(df) < 280: return None
    test_df = df[df.index >= '2020-01-01'].copy()
    if len(test_df) < 50: return None

    test_df['e5'] = ta.trend.ema_indicator(test_df['Close'], window=5)

    e5 = test_df['e5'].values
    e20 = test_df['e20'].values
    e60 = test_df['e60'].values
    rsi = test_df['rsi'].values
    adx = test_df['adx'].values
    close = test_df['Close'].values
    n = len(test_df)

    # 計算每天 cross_days
    cross_days = np.full(n, np.nan)
    for i in range(1, n):
        if not np.isnan(e20[i]) and not np.isnan(e60[i]):
            cur_bull = e20[i] > e60[i]
            for k in range(1, min(60, i)):
                if np.isnan(e20[i-k]) or np.isnan(e60[i-k]): continue
                prev_bull = e20[i-k] > e60[i-k]
                if prev_bull != cur_bull:
                    cross_days[i] = k if cur_bull else -k
                    break

    states = {
        '飆股 (T1+ADX≥30)':   [],
        'T1 穩健進場':        [],
        'T3 強趨勢拉回':      [],
        'T3 拉回進場':        [],
        'T3 高信心 (4-5)':    [],
        'T3 中信心 (2-3)':    [],
        'T3 低信心 (0-1)':    [],
        'T4 反彈':            [],
        '等待 T3 (RSI 50-65)':[],
        '等待回調 (RSI≥70)':  [],
    }

    for i in range(5, n - HOLD):
        if any(np.isnan(x) for x in [e5[i], e20[i], e60[i], rsi[i], adx[i]]): continue
        is_bull = e20[i] > e60[i]
        cd = cross_days[i]
        entry = close[i]
        ret = (close[i + HOLD] - entry) / entry * 100

        if not is_bull:
            # T4 反彈：RSI<32 + 連續 2 天上升
            if i >= 2 and rsi[i] < 32 and rsi[i] > rsi[i-1] > rsi[i-2]:
                states['T4 反彈'].append(ret)
            continue

        # 多頭
        if adx[i] < 22: continue

        is_strong = adx[i] >= 30
        is_fresh = not np.isnan(cd) and 0 < cd <= 10
        is_pullback = 35 <= rsi[i] < 50
        is_wait_t3 = 50 <= rsi[i] < 65
        is_hot = rsi[i] >= 70

        # T1 系列
        if is_fresh and is_strong:
            states['飆股 (T1+ADX≥30)'].append(ret)
        elif is_fresh and not is_strong:
            states['T1 穩健進場'].append(ret)

        # T3 系列
        if is_pullback:
            if is_strong:
                states['T3 強趨勢拉回'].append(ret)
            else:
                states['T3 拉回進場'].append(ret)

            # T3 信心度分組
            e5_5d = e5[i-5] if not np.isnan(e5[i-5]) else None
            e20_5d = e20[i-5] if not np.isnan(e20[i-5]) else None
            conf = compute_t3_conf(close[i], e5[i], e20[i], e5_5d, e20_5d)
            if conf >= 4: states['T3 高信心 (4-5)'].append(ret)
            elif conf >= 2: states['T3 中信心 (2-3)'].append(ret)
            else: states['T3 低信心 (0-1)'].append(ret)

        # 等待狀態
        if is_wait_t3 and not is_fresh:
            states['等待 T3 (RSI 50-65)'].append(ret)
        if is_hot and not is_fresh:
            states['等待回調 (RSI≥70)'].append(ret)

    return states


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    a = a[~np.isnan(a)]   # 過濾 NaN（下市股 / 缺資料）
    if len(a) == 0: return None
    return {'n': len(a), 'mean': a.mean(),
            'win': (a > 0).mean() * 100, 'worst': a.min(),
            'best': a.max(),
            'rr': a.mean()/abs(a.min()) if a.min() < 0 else 0}


def main():
    print(f"分析全台股 ({len(UNIVERSE)} 檔) — 所有進場狀態勝率比較（30 天）...\n")
    with ProcessPoolExecutor(max_workers=16) as ex:
        all_r = [r for r in ex.map(analyze_one, UNIVERSE) if r is not None]
    print(f"成功 {len(all_r)}/{len(UNIVERSE)}\n")

    # 彙總
    state_keys = [
        '飆股 (T1+ADX≥30)',
        'T1 穩健進場',
        'T3 強趨勢拉回',
        'T3 拉回進場',
        '— T3 信心度分組 —',
        'T3 高信心 (4-5)',
        'T3 中信心 (2-3)',
        'T3 低信心 (0-1)',
        '— 其他狀態 —',
        'T4 反彈',
        '等待 T3 (RSI 50-65)',
        '等待回調 (RSI≥70)',
    ]

    rows = []
    for key in state_keys:
        if key.startswith('—'):
            rows.append((key, None))
            continue
        data = []
        for r in all_r: data.extend(r.get(key, []))
        m = metrics(data)
        rows.append((key, m))

    print("=" * 100)
    print(f"📊 所有進場狀態勝率與績效（30 天持有，2020-至今 6 年，全市場）")
    print("=" * 100)
    print(f"{'狀態':<25} {'樣本':>7} {'勝率%':>8} {'均報%':>9} {'最差%':>9} "
          f"{'最佳%':>9} {'RR':>7}")
    print("-" * 100)

    for key, m in rows:
        if m is None:
            print(f"\n  {key}")
            print("-" * 100)
            continue
        print(f"{key:<25} {m['n']:>7} {m['win']:>+8.1f} {m['mean']:>+9.2f} "
              f"{m['worst']:>+9.1f} {m['best']:>+9.1f} {m['rr']:>7.3f}")

    # RR 排名
    print("\n" + "=" * 100)
    print("🏆 RR 排名（高到低）")
    print("=" * 100)
    valid = [(k, m) for k, m in rows if m is not None]
    valid.sort(key=lambda x: -x[1]['rr'])
    for i, (k, m) in enumerate(valid, 1):
        bar_len = int(m['rr'] * 30)
        bar = '█' * bar_len + '·' * (15 - bar_len) if bar_len <= 15 else '█' * 15
        print(f"  {i:>2}. {k:<25} RR {m['rr']:.3f} {bar}  win {m['win']:.1f}%  n={m['n']}")


if __name__ == '__main__':
    main()
