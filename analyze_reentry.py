"""進場後加碼規則回測 v9.38
========================================

從 bb_p1sig 進場 → mid_ema_down 出場「之間」的時段
測試 5 種加碼（pyramid）規則：

  R1. r_mid_bounce  — Close 回測 BB Mid 後反彈（Close>prev）
  R2. r_ema20       — Low 觸 EMA20 (距 ≤ 0.3 ATR) + Close 紅
  R3. r_p1sig_redo  — 離開 BB+1σ 後重新回到 ≥ +1σ
  R4. r_20d_high    — Close 破近 20b 最高
  R5. r_ema5_pull   — Low 觸 EMA5 + Close 收紅

每個加碼以加碼當時 Close 進場，與主部位一起出場（mid_ema_down）。
回測：對每檔的 main position 找所有 r-entry，計算各 r-entry P/L。
"""
from __future__ import annotations

import sys
import io
import os
import time
from pathlib import Path

try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8',
                                         line_buffering=True)
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from intraday.data import get_intraday
from intraday.strategy import (
    _compute_indicators, _check_entry_bb_p1sig,
    scan_with_exit_rule,
)


# ────────────────────────────────────────────────────────────────
# 5 種 R-entry 規則
# ────────────────────────────────────────────────────────────────

def _v(ind, key, i):
    try:
        x = ind[key].iloc[i]
        return None if pd.isna(x) else float(x)
    except Exception:
        return None


def r_mid_bounce(df, ind, j, j_prev_touched_mid):
    """R1: Close 跌至 BB Mid 後反彈
       條件：今日 Close > prev Close + 任一近 5 bar 內 Low ≤ Mid
    """
    c = _v(ind, 'close', j); c_prev = _v(ind, 'close', j - 1)
    bb_mid = _v(ind, 'bb_mid', j)
    if c is None or c_prev is None or bb_mid is None: return False
    # 必須今日紅 K
    if c <= c_prev: return False
    # 必須在 Mid 上方
    if c <= bb_mid: return False
    # 近 5 bar 內曾觸 Mid
    touched = False
    for k in range(max(0, j - 5), j + 1):
        l = _v(ind, 'low', k)
        m = _v(ind, 'bb_mid', k)
        if l is not None and m is not None and l <= m:
            touched = True; break
    return touched


def r_ema20(df, ind, j, *_):
    """R2: Low 觸 EMA20 (距 ≤ 0.3 ATR) + Close 收紅
    """
    c = _v(ind, 'close', j); o = _v(ind, 'open', j); l = _v(ind, 'low', j)
    e20 = _v(ind, 'ema20', j); atr_v = _v(ind, 'atr', j)
    if None in (c, o, l, e20, atr_v): return False
    if atr_v <= 0: return False
    # Low 接近 EMA20
    dist = abs(l - e20) / atr_v
    if dist > 0.3: return False
    # Close 收紅（Close > Open）或 hammer
    return c > o


def r_p1sig_redo(df, ind, j, history):
    """R3: 離開 BB+1σ 後重新回到 ≥ +1σ（mini-bb_p1sig 再觸發）
    """
    c = _v(ind, 'close', j); bb_p1 = _v(ind, 'bb_p1sigma', j)
    atr_v = _v(ind, 'atr', j)
    if None in (c, bb_p1, atr_v): return False
    if atr_v <= 0: return False
    # 必須 Close ≥ +1σ
    if c < bb_p1 - atr_v * 0.3: return False
    # 必須之前曾經 Close < +1σ（至少 2 bar）
    n_below = 0
    for k in range(max(0, j - 10), j):
        ck = _v(ind, 'close', k); bk = _v(ind, 'bb_p1sigma', k)
        if ck is not None and bk is not None and ck < bk:
            n_below += 1
    return n_below >= 2


def r_20d_high(df, ind, j, *_):
    """R4: Close > 近 20b 最高 (excluding today)
    """
    c = _v(ind, 'close', j)
    if c is None: return False
    if j < 20: return False
    max_h = df['High'].iloc[max(0, j - 20):j].max()
    return c > float(max_h)


def r_ema5_pull(df, ind, j, *_):
    """R5: Low 觸 EMA5 + Close 收紅
    """
    c = _v(ind, 'close', j); o = _v(ind, 'open', j); l = _v(ind, 'low', j)
    e5 = _v(ind, 'ema5', j); atr_v = _v(ind, 'atr', j)
    if None in (c, o, l, e5, atr_v): return False
    if atr_v <= 0: return False
    # Low 接近 EMA5（距 ≤ 0.3 ATR）
    dist = abs(l - e5) / atr_v
    if dist > 0.3: return False
    return c > o


RENTRY_RULES = {
    'r_mid_bounce':  r_mid_bounce,
    'r_ema20':       r_ema20,
    'r_p1sig_redo':  r_p1sig_redo,
    'r_20d_high':    r_20d_high,
    'r_ema5_pull':   r_ema5_pull,
}


# ────────────────────────────────────────────────────────────────
# 主分析
# ────────────────────────────────────────────────────────────────

def analyze_ticker(df, lookback=252):
    """對單檔 df，找所有 main positions，每個 position 內找所有 r-entry 候選"""
    # 用 scan_with_exit_rule 找 main positions
    main_trades = scan_with_exit_rule(
        df, market='us', lookback_bars=lookback, tf='1d',
        exit_rule='mid_ema_down', entry_mode='bb_p1sig')

    if not main_trades:
        return []

    ind = _compute_indicators(df)
    results = []
    for tr in main_trades:
        entry_idx = tr['entry_idx']
        exit_idx = tr.get('exit_idx')
        is_open = tr.get('open', False)
        if exit_idx is None:
            exit_idx = len(df) - 1
        exit_price = tr.get('exit_price') or float(df['Close'].iloc[exit_idx])

        # 對每個 r-rule，找該 position 內所有 r-entry bars
        reentries_per_rule = {name: [] for name in RENTRY_RULES}
        for j in range(entry_idx + 1, exit_idx):
            for rule_name, rule_fn in RENTRY_RULES.items():
                try:
                    if rule_fn(df, ind, j, None):
                        r_price = float(df['Close'].iloc[j])
                        r_pnl_pct = (exit_price - r_price) / r_price * 100
                        reentries_per_rule[rule_name].append({
                            'r_idx': j,
                            'r_time': df.index[j],
                            'r_price': r_price,
                            'r_pnl_pct': r_pnl_pct,
                        })
                except Exception:
                    pass
        results.append({
            'entry_idx': entry_idx, 'exit_idx': exit_idx,
            'entry_price': tr['entry_price'], 'exit_price': exit_price,
            'main_pnl_pct': tr['pnl_pct'],
            'is_open': is_open,
            'reentries': reentries_per_rule,
        })
    return results


def main():
    df_in = pd.read_csv('backtest_swing_us_LIQUID_3000_RS70_1d_252b.csv')
    tickers = df_in['ticker'].tolist() + ['BE', 'FCEL', 'AMDL', 'NVDL']
    tickers = list(dict.fromkeys(tickers))

    print(f'\n══ 加碼規則回測 — {len(tickers)} 檔（1d × 252b）══\n')

    # 跨檔聚合
    rule_stats = {name: {
        'n': 0, 'wins': 0, 'losses': 0,
        'sum_pnl': 0.0, 'best': -999, 'worst': 999,
    } for name in RENTRY_RULES}
    n_main_positions = 0
    n_processed = 0

    t0 = time.time()
    for i, tk in enumerate(tickers):
        if i % 60 == 0 and i > 0:
            print(f'  [{i}/{len(tickers)}] {time.time()-t0:.0f}s')
        try:
            df = get_intraday(tk, '1d', market='us')
            if df is None or len(df) < 250:
                continue
            positions = analyze_ticker(df, lookback=252)
            if not positions: continue
            n_processed += 1
            n_main_positions += len(positions)
            for pos in positions:
                for rule_name, rentries in pos['reentries'].items():
                    s = rule_stats[rule_name]
                    for r in rentries:
                        s['n'] += 1
                        s['sum_pnl'] += r['r_pnl_pct']
                        if r['r_pnl_pct'] > 0: s['wins'] += 1
                        else: s['losses'] += 1
                        s['best'] = max(s['best'], r['r_pnl_pct'])
                        s['worst'] = min(s['worst'], r['r_pnl_pct'])
        except Exception:
            pass

    print(f'\n處理 {n_processed} 檔，找到 {n_main_positions} 個 main positions')
    print(f'執行 {time.time()-t0:.0f}s\n')

    # 結果表
    print('═' * 90)
    print(f'{"Rule":<18}{"r-entries":>12}{"WR":>8}{"Avg P/L":>10}{"Best":>10}{"Worst":>10}{"Σ":>12}{"per main":>10}')
    print('─' * 90)
    rule_order = sorted(rule_stats.keys(),
                          key=lambda k: -rule_stats[k]['sum_pnl'])
    for name in rule_order:
        s = rule_stats[name]
        n = s['n']
        wr = (s['wins'] / max(1, n)) * 100
        avg = s['sum_pnl'] / max(1, n)
        per_main = n / max(1, n_main_positions)
        print(f'{name:<18}{n:>12}{wr:>7.1f}%{avg:>+9.2f}%{s["best"]:>+9.2f}%'
              f'{s["worst"]:>+9.2f}%{s["sum_pnl"]:>+11.0f}%{per_main:>9.2f}')

    print()
    # 對重點 ticker 顯示 r-entries
    print('── 個股 r-entry 摘要 ──')
    for tk in ['NVDA', 'AMD', 'BE', 'GOOGL', 'AVGO', 'FCEL']:
        try:
            df = get_intraday(tk, '1d', market='us')
            positions = analyze_ticker(df, lookback=252)
            if not positions: continue
            print(f'\n● {tk} ({len(positions)} positions):')
            for p in positions:
                et = df.index[p['entry_idx']].strftime('%Y-%m-%d')
                xt = df.index[p['exit_idx']].strftime('%Y-%m-%d')
                tag = '(open)' if p['is_open'] else ''
                print(f'  Position {et}→{xt} {tag} '
                      f'(${p["entry_price"]:.2f}→${p["exit_price"]:.2f}, '
                      f'main {p["main_pnl_pct"]:+.2f}%)')
                for rule_name, rentries in p['reentries'].items():
                    if not rentries: continue
                    avg = sum(r['r_pnl_pct'] for r in rentries) / len(rentries)
                    print(f'    {rule_name:<15} ×{len(rentries):>2}  '
                          f'avg {avg:+.2f}% '
                          f'(times: {",".join(r["r_time"].strftime("%m-%d") for r in rentries[:5])})')
        except Exception as e:
            print(f'  {tk}: err {e}')


if __name__ == '__main__':
    main()
