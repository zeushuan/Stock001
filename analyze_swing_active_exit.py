"""主動波段出場研究（v9.16）
==========================================================
依用戶需求設計：
- 進場：Strategy B 突破前高（已 OOS 驗證）
- 出場：8 種規則對比（baseline + 7 主動）
- min_hold = 3 天（避免訊號日洗盤）
- max_hold = 180 天（不設限的回測 safety net）
- 哲學：早出比晚出好；飆就繼續，跌就快走

8 個 exit 規則
-------------
1. baseline_rsi80_90d  — OOS 既有冠軍：RSI≥80 賣 + 最長 90 天
2. only_ema20_break    — close < EMA20 連 2 天（E1 單獨）
3. only_adx5d_down     — ADX 5d 下降 ≥ 5 點（E3 單獨）
4. recipe_A 保守快出    — E1 OR E2 (一觸發就走)
5. recipe_B 平衡 ⭐     — E1 AND E3 (結構+動能雙重確認)
6. recipe_C 飆股模式    — close < EMA10 連 2 天（更緊）
7. recipe_D ATR 動態    — close < peak − 2.5 ATR
8. recipe_E 全方位主動  — E1 OR E2 OR E3 OR E10break (任一觸發)

評估指標
---------
- n / win% / mean% / median%
- avg_hold（持有天數平均）
- mean_per_day = mean% / avg_hold（**效率指標**）
- best% / worst%
- exit reason 分佈（看哪個 rule 主導）

執行
-----
  python analyze_swing_active_exit.py                  # TW
  python analyze_swing_active_exit.py --market us
  python analyze_swing_active_exit.py --market both
  python analyze_swing_active_exit.py --oos            # 只看 2024+
"""
import sys, json, time, argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
from analyze_swing_dynamic_exit import (
    detect_swing_signals, compute_helper_arrays,
    get_universe, COST_ROUND_TRIP
)

WORKERS = 12


# ─────────────────────────────────────────────────────────────────────────
# Active Exit 規則 — 每個都接收 (df, k, entry_i, running_peak, entry_open)
# 回傳 (triggered: bool, exit_price: float | None, reason: str | None)
# ─────────────────────────────────────────────────────────────────────────

def E1_ema20_break(df, k, entry_i, running_peak, entry_open):
    """E1: close < EMA20 連 2 天（趨勢結構破壞）"""
    if k < 1: return (False, None, None)
    e20 = df['e20'].values
    c = df['Close'].values
    if np.isnan(e20[k]) or np.isnan(e20[k-1]) or np.isnan(c[k]) or np.isnan(c[k-1]):
        return (False, None, None)
    if c[k] < e20[k] and c[k-1] < e20[k-1]:
        return (True, float(c[k]), 'E1_ema20_break')
    return (False, None, None)


def E2_three_black_vol(df, k, entry_i, running_peak, entry_open):
    """E2: 3 連黑K + 量增（量比 > 1.3x，出貨訊號）"""
    if k < 2: return (False, None, None)
    o = df['Open'].values; c = df['Close'].values; v = df['Volume'].values
    if any(np.isnan(x) for x in [o[k], c[k], o[k-1], c[k-1], o[k-2], c[k-2]]):
        return (False, None, None)
    blacks = sum(1 for j in [k, k-1, k-2] if c[j] < o[j])
    if blacks < 3: return (False, None, None)
    if k < 20: return (False, None, None)
    v_avg = np.nanmean(v[k-20:k])
    if v_avg <= 0: return (False, None, None)
    if v[k] / v_avg > 1.3:
        return (True, float(c[k]), 'E2_3black_vol')
    return (False, None, None)


def E3_adx5d_down(df, k, entry_i, running_peak, entry_open):
    """E3: ADX 5 日下降 ≥ 5 點（動能消失）"""
    if k < 5: return (False, None, None)
    adx = df['adx'].values
    c = df['Close'].values
    if np.isnan(adx[k]) or np.isnan(adx[k-5]): return (False, None, None)
    if adx[k] < adx[k-5] - 5:
        return (True, float(c[k]), 'E3_adx_decay')
    return (False, None, None)


def E10_break(df, k, entry_i, running_peak, entry_open):
    """飆股版：close < EMA10 連 2 天（更緊）"""
    if k < 1: return (False, None, None)
    e10 = df['e10'].values
    c = df['Close'].values
    if np.isnan(e10[k]) or np.isnan(e10[k-1]): return (False, None, None)
    if c[k] < e10[k] and c[k-1] < e10[k-1]:
        return (True, float(c[k]), 'E10_break')
    return (False, None, None)


def ATR25_trail(df, k, entry_i, running_peak, entry_open):
    """ATR 動態：close ≤ peak − 2.5 ATR（必須先有獲利）"""
    atr = df['atr'].values
    c = df['Close'].values
    if np.isnan(atr[k]) or np.isnan(c[k]) or atr[k] <= 0:
        return (False, None, None)
    if running_peak <= entry_open:
        return (False, None, None)  # 未獲利不啟動
    threshold = running_peak - 2.5 * atr[k]
    if threshold > 0 and c[k] <= threshold:
        return (True, float(threshold), 'ATR25_trail')
    return (False, None, None)


# ─── Recipe 組合（user choice） ─────────────────────────────────

def recipe_A_conservative(df, k, entry_i, running_peak, entry_open):
    """🛡️ 保守快出：E1 OR E2（一觸發就走）"""
    for fn in [E1_ema20_break, E2_three_black_vol]:
        ok, p, r = fn(df, k, entry_i, running_peak, entry_open)
        if ok: return (True, p, f'A_{r}')
    return (False, None, None)


def recipe_B_balanced(df, k, entry_i, running_peak, entry_open):
    """⚖️ 平衡（user default）：E1 AND E3（結構破壞 + 動能消失）"""
    e1_ok, _, _ = E1_ema20_break(df, k, entry_i, running_peak, entry_open)
    e3_ok, _, _ = E3_adx5d_down(df, k, entry_i, running_peak, entry_open)
    if e1_ok and e3_ok:
        c = df['Close'].values
        return (True, float(c[k]), 'B_E1+E3')
    return (False, None, None)


def recipe_C_momentum(df, k, entry_i, running_peak, entry_open):
    """🚀 飆股模式：close < EMA10 連 2 天"""
    return E10_break(df, k, entry_i, running_peak, entry_open)


def recipe_D_atr(df, k, entry_i, running_peak, entry_open):
    """🎯 ATR 動態：close ≤ peak − 2.5 ATR"""
    return ATR25_trail(df, k, entry_i, running_peak, entry_open)


def recipe_E_hybrid(df, k, entry_i, running_peak, entry_open):
    """🌪️ 全方位主動：E1 OR E2 OR E3 OR E10break（任一觸發）"""
    for fn in [E1_ema20_break, E2_three_black_vol, E3_adx5d_down, E10_break]:
        ok, p, r = fn(df, k, entry_i, running_peak, entry_open)
        if ok: return (True, p, f'E_{r}')
    return (False, None, None)


def baseline_rsi80_90d(df, k, entry_i, running_peak, entry_open):
    """📊 Baseline：RSI ≥ 80 → 隔日 open 出場（max 90 天）"""
    rsi = df['rsi'].values
    o = df['Open'].values
    n = len(df)
    if k - entry_i >= 90:  # 90 天 max hold
        return (True, float(o[k]) if not np.isnan(o[k]) else float(df['Close'].values[k]),
                'baseline_max90d')
    if not np.isnan(rsi[k]) and rsi[k] >= 80:
        if k + 1 < n and not np.isnan(o[k+1]):
            return (True, float(o[k+1]), 'baseline_rsi80')
        return (True, float(df['Close'].values[k]), 'baseline_rsi80_close')
    return (False, None, None)


EXIT_RULES = {
    'baseline_rsi80_90d':    {'fn': baseline_rsi80_90d,    'min_hold': 0,  'max_hold': 90,
                              'desc': 'Baseline OOS：RSI≥80 賣 + 90d'},
    'only_ema20_break':      {'fn': E1_ema20_break,        'min_hold': 3,  'max_hold': 180,
                              'desc': 'E1 單獨：close < EMA20 連 2 天'},
    'only_adx5d_down':       {'fn': E3_adx5d_down,         'min_hold': 3,  'max_hold': 180,
                              'desc': 'E3 單獨：ADX 5d 下降 ≥5'},
    'recipe_A_conservative': {'fn': recipe_A_conservative, 'min_hold': 3,  'max_hold': 180,
                              'desc': '🛡️ 保守快出：E1 OR E2'},
    'recipe_B_balanced':     {'fn': recipe_B_balanced,     'min_hold': 3,  'max_hold': 180,
                              'desc': '⚖️ 平衡 (default)：E1 AND E3'},
    'recipe_C_momentum':     {'fn': recipe_C_momentum,     'min_hold': 3,  'max_hold': 180,
                              'desc': '🚀 飆股：close < EMA10 連 2 天'},
    'recipe_D_atr':          {'fn': recipe_D_atr,          'min_hold': 3,  'max_hold': 180,
                              'desc': '🎯 ATR 動態：peak − 2.5 ATR'},
    'recipe_E_hybrid':       {'fn': recipe_E_hybrid,       'min_hold': 3,  'max_hold': 180,
                              'desc': '🌪️ 全方位：E1 OR E2 OR E3 OR E10'},
}


# ─────────────────────────────────────────────────────────────────────────
# Walk exit
# ─────────────────────────────────────────────────────────────────────────
def walk_active_exit(df, entry_i, entry_open, rule_cfg):
    """從 entry_i+1 開始 walk，套用主動 exit rule。回傳 (exit_i, price, reason)"""
    n = len(df)
    o = df['Open'].values
    h_arr = df['High'].values
    c = df['Close'].values

    fn = rule_cfg['fn']
    min_hold = rule_cfg.get('min_hold', 0)
    max_hold = rule_cfg.get('max_hold', 180)

    running_peak = entry_open
    end = min(entry_i + max_hold, n - 1)

    for k in range(entry_i + 1, end + 1):
        h_k = h_arr[k]
        if not np.isnan(h_k) and h_k > running_peak:
            running_peak = h_k

        # min_hold check
        if k - entry_i < min_hold:
            continue

        ok, exit_price, reason = fn(df, k, entry_i, running_peak, entry_open)
        if ok:
            # 隔日 open 出場（避免 hindsight，但 baseline 自己處理隔日邏輯）
            if reason and reason.startswith('baseline_rsi80') and not reason.endswith('close'):
                return (k + 1 if k + 1 < n else k, exit_price, reason)
            # 其他用 close 當天
            return (k, exit_price, reason)

    # max_hold 到了
    if not np.isnan(o[end]):
        return (end, float(o[end]), 'max_hold_safety')
    return (end, float(c[end]) if not np.isnan(c[end]) else entry_open, 'max_hold_safety')


def gen_trades_one(args):
    """一個 ticker：跑所有 exit rule。"""
    ticker, rules_dict, start_date = args
    out = {r: [] for r in rules_dict}
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280:
            return out
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(start_date)]
        if len(df) < 80:
            return out

        helpers = compute_helper_arrays(df)
        signals = detect_swing_signals(df, 'B')   # 進場 = Strategy B（最 OOS robust）

        o = df['Open'].values
        idx = df.index
        n = len(df)
        rsi_arr = df['rsi'].values
        adx_arr = df['adx'].values

        for sig_i in signals:
            entry_i = sig_i + 1
            if entry_i >= n - 1: continue
            entry_open = float(o[entry_i])
            if entry_open <= 0 or np.isnan(entry_open): continue

            for rule_name, cfg in rules_dict.items():
                exit_i, exit_price, reason = walk_active_exit(df, entry_i, entry_open, cfg)
                if exit_price is None or exit_price <= 0 or np.isnan(exit_price): continue

                gross_ret = (exit_price - entry_open) / entry_open
                net_ret = gross_ret - COST_ROUND_TRIP
                out[rule_name].append({
                    'ticker': ticker,
                    'rule': rule_name,
                    'entry_date': idx[entry_i].strftime('%Y-%m-%d'),
                    'exit_date':  idx[exit_i].strftime('%Y-%m-%d') if exit_i < n else idx[-1].strftime('%Y-%m-%d'),
                    'hold_days':  exit_i - entry_i,
                    'entry_price': round(entry_open, 4),
                    'exit_price': round(exit_price, 4),
                    'gross_ret': gross_ret,
                    'net_ret': net_ret,
                    'reason': reason,
                    'rsi_at_signal': float(rsi_arr[sig_i]) if not np.isnan(rsi_arr[sig_i]) else 50,
                    'adx_at_signal': float(adx_arr[sig_i]) if not np.isnan(adx_arr[sig_i]) else 0,
                })
        return out
    except Exception:
        return out


# ─────────────────────────────────────────────────────────────────────────
# Stats
# ─────────────────────────────────────────────────────────────────────────
def trade_stats(trades):
    if not trades: return None
    df = pd.DataFrame(trades)
    n = len(df)
    win = (df['net_ret'] > 0).sum()
    win_pct = win / n * 100
    mean_pct = df['net_ret'].mean() * 100
    median_pct = df['net_ret'].median() * 100
    std_pct = df['net_ret'].std() * 100
    pos_sum = df.loc[df['net_ret'] > 0, 'net_ret'].sum()
    neg_sum = -df.loc[df['net_ret'] < 0, 'net_ret'].sum()
    pf = pos_sum / neg_sum if neg_sum > 0 else 999
    avg_hold = df['hold_days'].mean()
    max_hold_actual = df['hold_days'].max()
    pct_long_held = (df['hold_days'] >= 60).mean() * 100   # 持有超過 60d 比例（看是否真有飆股案例）
    mean_per_day = mean_pct / avg_hold if avg_hold > 0 else 0
    reasons = df['reason'].value_counts(normalize=True).head(5).to_dict()

    return {
        'n': n,
        'win_pct': round(win_pct, 2),
        'mean_pct': round(mean_pct, 2),
        'median_pct': round(median_pct, 2),
        'std_pct': round(std_pct, 2),
        'pf': round(pf, 2),
        'avg_hold': round(avg_hold, 1),
        'max_hold_actual': int(max_hold_actual),
        'pct_long_held_60d': round(pct_long_held, 1),
        'mean_per_day': round(mean_per_day, 3),
        'best_pct': round(df['net_ret'].max() * 100, 2),
        'worst_pct': round(df['net_ret'].min() * 100, 2),
        'top_reasons': {k: round(v * 100, 1) for k, v in reasons.items()},
    }


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
def run_analysis(market='tw', oos_only=False):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    start = '2024-01-01' if oos_only else '2020-01-01'
    period = 'OOS only (2024-)' if oos_only else 'Full period (2020-)'

    print(f"\n{flag} 主動波段出場研究  {period}")
    print(f"  Universe: {len(universe)} 檔  進場 = Strategy B 突破前高")
    print(f"  成本 round-trip: {COST_ROUND_TRIP*100:.2f}%")
    print()

    print(f"📊 跑所有 exit rules（{WORKERS} workers）...")
    t0 = time.time()
    args = [(t, EXIT_RULES, start) for t in universe]
    by_rule = {r: [] for r in EXIT_RULES}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for one_out in ex.map(gen_trades_one, args, chunksize=80):
            for r, ts in one_out.items():
                by_rule[r].extend(ts)
    print(f"  完成 {time.time()-t0:.1f}s\n")

    # 表格輸出
    print('=' * 130)
    print(f"{'rule':>22} {'desc':<35} {'n':>7} {'win%':>6} {'mean%':>8} {'med%':>7} "
          f"{'PF':>5} {'avgHold':>8} {'mean/d':>7} {'≥60d%':>6}")
    print('-' * 130)

    all_stats = {}
    for rule_name, cfg in EXIT_RULES.items():
        trades = by_rule[rule_name]
        st = trade_stats(trades)
        if st is None:
            print(f"{rule_name:>22}  (no trades)")
            continue
        all_stats[rule_name] = {**st, 'desc': cfg['desc']}
        marker = ''
        if st['mean_per_day'] >= 0.10 and st['win_pct'] >= 50:
            marker = ' ⭐'
        print(f"{rule_name:>22} {cfg['desc'][:35]:<35} {st['n']:>7} "
              f"{st['win_pct']:>5.1f}% {st['mean_pct']:>+7.2f}% {st['median_pct']:>+6.2f}% "
              f"{st['pf']:>5.2f} {st['avg_hold']:>7.1f} {st['mean_per_day']:>+6.3f} "
              f"{st['pct_long_held_60d']:>5.1f}%{marker}")

    # 寫 JSON
    out_file = f'analyze_swing_active_exit_{market}{"_oos" if oos_only else ""}.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump({
            'market': market, 'oos_only': oos_only, 'start_date': start,
            'universe_size': len(universe),
            'results': all_stats,
        }, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 {out_file}")

    # 排名摘要
    print('\n' + '=' * 130)
    print('🏆 排名摘要')
    print('=' * 130)

    # 按 mean% 排
    sorted_by_mean = sorted(all_stats.items(),
                             key=lambda x: -x[1]['mean_pct'])
    print('\n📈 按 mean% 排序：')
    for i, (rn, st) in enumerate(sorted_by_mean[:5], 1):
        print(f"  #{i} {rn:<22} mean {st['mean_pct']:+.2f}%  "
              f"win {st['win_pct']:.1f}%  hold {st['avg_hold']:.1f}d")

    # 按效率（mean/day）排
    sorted_by_eff = sorted(all_stats.items(),
                            key=lambda x: -x[1]['mean_per_day'])
    print('\n⚡ 按效率（mean% / day）排序：')
    for i, (rn, st) in enumerate(sorted_by_eff[:5], 1):
        print(f"  #{i} {rn:<22} mean/d {st['mean_per_day']:+.3f}  "
              f"mean {st['mean_pct']:+.2f}%  hold {st['avg_hold']:.1f}d")

    # 按 win rate 排
    sorted_by_win = sorted(all_stats.items(),
                            key=lambda x: -x[1]['win_pct'])
    print('\n🎯 按 win%（勝率）排序：')
    for i, (rn, st) in enumerate(sorted_by_win[:5], 1):
        print(f"  #{i} {rn:<22} win {st['win_pct']:.1f}%  "
              f"mean {st['mean_pct']:+.2f}%  hold {st['avg_hold']:.1f}d")

    return all_stats


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'both'])
    p.add_argument('--oos', action='store_true', help='只跑 OOS（2024+）')
    args = p.parse_args()

    markets = ['tw', 'us'] if args.market == 'both' else [args.market]
    for m in markets:
        run_analysis(market=m, oos_only=args.oos)


if __name__ == '__main__':
    main()
