"""波段策略 × 動態出場 研究（v9.14）
================================================================
目的
-----
回答用戶 3 個關鍵問題：
1. 有以波段為主的操作模式嗎？
2. 應該用動態出場 確保獲利更多 勝率更高
3. 做研究先

實驗設計
---------
**入場 (4 個波段策略，跟 screener_filters.py 對齊)**:
  A. trend_continuation : 多頭 + ADX≥25 + RSI 45-65 + cross_days 5-15 + from_high>2 + 非 imminent_dc
  B. breakout            : 多頭 + ADX≥22 + from_high<1 + vol_ratio>1.5 + RSI<75
  C. pullback_ema20      : 多頭 + ADX≥22 + |close-EMA20|/EMA20<2% + RSI 40-55 + 非 imminent_dc
  D. momentum_accel      : 多頭 + ADX≥25 + ADX 5d 上升≥5 + RSI<70

**出場規則 (15 種，含基準與動態)**:
  Baseline (固定 N 天):
    fixed_30d, fixed_60d, fixed_90d

  RSI 過熱:
    rsi_70, rsi_75, rsi_80                (max_hold=90 安全網)

  EMA 結構破壞:
    ema20_break       (close < EMA20 兩天)
    death_cross       (EMA20 < EMA60)

  Trailing stop %:
    trail_5, trail_8, trail_10, trail_15  (max_hold=90)

  ATR-based:
    atr_2, atr_3                           (peak - N×ATR)

  Target price:
    target_15, target_20

  動態組合 (核心研究):
    combo_rsi75_trail10    : RSI>75 OR trail 10% OR max 60d
    combo_target15_trail10 : target +15% OR trail 10% OR max 60d
    combo_smart            : RSI>75 OR trail 10% OR close<EMA20 OR max 90d

**評估指標**:
  - n: 訊號數
  - win%: 勝率
  - mean_net%: 平均淨報酬（已扣 0.67% round-trip cost）
  - median%: 中位數
  - PF: profit factor
  - avg_hold: 平均持有天數
  - early_exit%: 提前出場比例（< max_hold）
  - sharpe (per trade): mean / std

執行
-----
  python analyze_swing_dynamic_exit.py                # TW + US 完整跑
  python analyze_swing_dynamic_exit.py --market tw    # 僅 TW
  python analyze_swing_dynamic_exit.py --strategy A   # 僅入場策略 A
  python analyze_swing_dynamic_exit.py --quick        # 快速版（僅核心 6 個出場規則）
"""
import sys, json, time, argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 12
START_DATE = '2020-01-01'
COST_ROUND_TRIP = 0.00171 + 0.003 + 0.002   # = 0.671%

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
    'ARKK','ARKG','ARKF','ARKW','ARKQ',
}


# ─────────────────────────────────────────────────────────────────────────
# Universe
# ─────────────────────────────────────────────────────────────────────────
def get_universe(market='tw'):
    DATA = Path('data_cache')
    if market == 'tw':
        return sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    elif market == 'us':
        return sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem.isalpha() and p.stem.isupper()
                       and 1 <= len(p.stem) <= 5
                       and p.stem not in US_ETF_EXCLUDE])
    else:
        raise ValueError(f"unknown market: {market}")


# ─────────────────────────────────────────────────────────────────────────
# 衍生指標：cross_days / from_high / vol_ratio / imminent_dc
# ─────────────────────────────────────────────────────────────────────────
def compute_helper_arrays(df):
    """一次算出所有 entry signal 需要的衍生欄位 array。
    回傳 dict of np arrays（長度 = len(df)）"""
    n = len(df)
    e20 = df['e20'].values
    e60 = df['e60'].values
    c = df['Close'].values
    v = df['Volume'].values
    atr = df['atr'].values

    # cross_days: e20 > e60 連續天數（正＝多頭，負＝空頭）
    bull_mask = e20 > e60
    cross_days = np.zeros(n, dtype=int)
    streak = 0
    prev_state = None
    for i in range(n):
        if np.isnan(e20[i]) or np.isnan(e60[i]):
            cross_days[i] = 0
            streak = 0
            prev_state = None
            continue
        cur_state = bool(bull_mask[i])
        if cur_state != prev_state:
            streak = 1
        else:
            streak += 1
        cross_days[i] = streak if cur_state else -streak
        prev_state = cur_state

    # from_high: (close - max(close[-60:])) / max * 100；負值 = 距離高點 X%
    from_high = np.zeros(n)
    for i in range(n):
        s = max(0, i - 60 + 1)
        window = c[s:i+1]
        if len(window) > 0 and np.nanmax(window) > 0:
            from_high[i] = (c[i] - np.nanmax(window)) / np.nanmax(window) * 100
    # 正值代表「從高點下跌的 abs %」（screener 的用法是「from_high>2 = 至少 2% 下跌」）
    from_high_pos = -from_high   # 反號，>0 = 已從高點回落 X%

    # vol_ratio: vol / mean(vol[-20:])
    vol_ratio = np.zeros(n)
    for i in range(20, n):
        ref = np.nanmean(v[i-20:i])
        if ref > 0:
            vol_ratio[i] = v[i] / ref

    # is_bull: e20 > e60 AND close > e20
    is_bull = np.zeros(n, dtype=bool)
    for i in range(n):
        if np.isnan(e20[i]) or np.isnan(e60[i]) or np.isnan(c[i]): continue
        is_bull[i] = (e20[i] > e60[i]) and (c[i] > e20[i])

    # dist_to_ema20: |close - e20| / e20 * 100
    dist_ema20 = np.full(n, 99.0)
    for i in range(n):
        if not np.isnan(e20[i]) and e20[i] > 0 and not np.isnan(c[i]):
            dist_ema20[i] = abs(c[i] - e20[i]) / e20[i] * 100

    # imminent_dc: gap (e20-e60) < 1×ATR AND cross_days > 10 AND e20 下降
    imminent_dc = np.zeros(n, dtype=bool)
    for i in range(3, n):
        if np.isnan(e20[i]) or np.isnan(e60[i]) or np.isnan(atr[i]): continue
        if not bull_mask[i]: continue
        gap = e20[i] - e60[i]
        if atr[i] > 0 and gap < atr[i]:
            if cross_days[i] > 10:
                if e20[i] < e20[i-3]:
                    imminent_dc[i] = True

    return {
        'cross_days': cross_days,
        'from_high_pos': from_high_pos,
        'vol_ratio': vol_ratio,
        'is_bull': is_bull,
        'dist_ema20': dist_ema20,
        'imminent_dc': imminent_dc,
    }


# ─────────────────────────────────────────────────────────────────────────
# 入場策略：A/B/C/D
# ─────────────────────────────────────────────────────────────────────────
def detect_swing_signals(df, strategy):
    """偵測一個 ticker 的所有入場訊號 index list。
    strategy: 'A' / 'B' / 'C' / 'D'"""
    if len(df) < 80: return []

    rsi = df['rsi'].values
    adx = df['adx'].values
    h = compute_helper_arrays(df)

    n = len(df)
    sigs = []

    for i in range(60, n - 1):
        if np.isnan(rsi[i]) or np.isnan(adx[i]): continue
        if not h['is_bull'][i]: continue

        if strategy == 'A':
            # 趨勢延續
            if not (adx[i] >= 25): continue
            if not (45 <= rsi[i] <= 65): continue
            cd = h['cross_days'][i]
            if not (5 <= cd <= 15): continue
            if not (h['from_high_pos'][i] > 2): continue
            if h['imminent_dc'][i]: continue
        elif strategy == 'B':
            # 突破前高
            if not (adx[i] >= 22): continue
            if not (h['from_high_pos'][i] < 1): continue
            if not (h['vol_ratio'][i] > 1.5): continue
            if not (rsi[i] < 75): continue
        elif strategy == 'C':
            # 拉回 EMA20
            if not (adx[i] >= 22): continue
            if not (h['dist_ema20'][i] < 2): continue
            if not (40 <= rsi[i] <= 55): continue
            if h['imminent_dc'][i]: continue
        elif strategy == 'D':
            # 動能加速
            if not (adx[i] >= 25): continue
            if i < 5 or np.isnan(adx[i-5]): continue
            if not ((adx[i] - adx[i-5]) >= 5): continue
            if not (rsi[i] < 70): continue
        else:
            continue

        sigs.append(i)
    return sigs


# ─────────────────────────────────────────────────────────────────────────
# 出場規則
# ─────────────────────────────────────────────────────────────────────────
EXIT_RULES = {
    # Baseline 固定 N 天
    'fixed_30d':            {'max_hold': 30},
    'fixed_60d':            {'max_hold': 60},
    'fixed_90d':            {'max_hold': 90},

    # RSI 過熱
    'rsi_70':               {'rsi_above': 70, 'max_hold': 90},
    'rsi_75':               {'rsi_above': 75, 'max_hold': 90},
    'rsi_80':               {'rsi_above': 80, 'max_hold': 90},

    # EMA 結構破壞
    'ema20_break':          {'close_below_ema20': True, 'max_hold': 90},
    'death_cross':          {'ema20_below_ema60': True, 'max_hold': 90},

    # Trailing stop %
    'trail_5':              {'trail_pct': 0.05, 'max_hold': 90},
    'trail_8':              {'trail_pct': 0.08, 'max_hold': 90},
    'trail_10':             {'trail_pct': 0.10, 'max_hold': 90},
    'trail_15':             {'trail_pct': 0.15, 'max_hold': 90},

    # ATR 倍數
    'atr_2':                {'trail_atr_mult': 2.0, 'max_hold': 90},
    'atr_3':                {'trail_atr_mult': 3.0, 'max_hold': 90},

    # Target price
    'target_15':            {'target_pct': 0.15, 'max_hold': 90},
    'target_20':            {'target_pct': 0.20, 'max_hold': 90},

    # 動態組合（研究核心）
    'combo_rsi75_trail10':  {'rsi_above': 75, 'trail_pct': 0.10, 'max_hold': 60},
    'combo_tgt15_trail10':  {'target_pct': 0.15, 'trail_pct': 0.10, 'max_hold': 60},
    'combo_smart':          {'rsi_above': 75, 'trail_pct': 0.10,
                             'close_below_ema20': True, 'max_hold': 90},
}

QUICK_RULES = ['fixed_30d', 'fixed_60d', 'rsi_75', 'trail_10',
               'combo_rsi75_trail10', 'combo_smart']


def walk_exit(df, helpers, entry_i, entry_open, rule):
    """從 entry_i+1 起逐日 walk，根據 rule 找出場日 + 價格。
    回傳 (exit_i, exit_price, exit_reason)"""
    n = len(df)
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    rsi = df['rsi'].values
    e20 = df['e20'].values
    e60 = df['e60'].values
    atr = df['atr'].values

    max_hold = rule.get('max_hold', 90)
    rsi_above = rule.get('rsi_above')
    close_below_ema20 = rule.get('close_below_ema20', False)
    ema20_below_ema60 = rule.get('ema20_below_ema60', False)
    trail_pct = rule.get('trail_pct')
    trail_atr_mult = rule.get('trail_atr_mult')
    target_pct = rule.get('target_pct')

    running_peak = entry_open
    target_price = entry_open * (1 + target_pct) if target_pct else None

    exit_end = min(entry_i + max_hold, n - 1)
    consec_below_ema20 = 0

    for k in range(entry_i + 1, exit_end + 1):
        h_k = h[k]; l_k = l[k]; c_k = c[k]
        if np.isnan(h_k) or np.isnan(l_k) or np.isnan(c_k):
            continue

        # 更新 running peak
        if h_k > running_peak:
            running_peak = h_k

        # ① Target hit (用當天 high 觸發)
        if target_price and h_k >= target_price:
            return k, target_price, 'target'

        # ② RSI 過熱（隔日 open 出場 — 避免 hindsight bias）
        if rsi_above and not np.isnan(rsi[k]) and rsi[k] >= rsi_above:
            if k + 1 < n and not np.isnan(o[k+1]):
                return k + 1, float(o[k+1]), 'rsi'
            return k, float(c_k), 'rsi'

        # ③ EMA20 break (close < e20 連 2 天，隔日 open 出場)
        if close_below_ema20 and not np.isnan(e20[k]) and c_k < e20[k]:
            consec_below_ema20 += 1
            if consec_below_ema20 >= 2:
                if k + 1 < n and not np.isnan(o[k+1]):
                    return k + 1, float(o[k+1]), 'ema20'
                return k, float(c_k), 'ema20'
        else:
            consec_below_ema20 = 0

        # ④ Death cross (e20 < e60，隔日 open 出場)
        if ema20_below_ema60 and not np.isnan(e20[k]) and not np.isnan(e60[k]):
            if e20[k] < e60[k]:
                if k + 1 < n and not np.isnan(o[k+1]):
                    return k + 1, float(o[k+1]), 'death_cross'
                return k, float(c_k), 'death_cross'

        # ⑤ Trailing stop %（從 peak 回跌 X%；當天 low 觸發）
        if trail_pct and running_peak > entry_open:
            stop_price = running_peak * (1 - trail_pct)
            if l_k <= stop_price:
                return k, stop_price, 'trail_pct'

        # ⑥ ATR 倍數移動止損（peak - N×ATR；當天 low 觸發）
        if trail_atr_mult and running_peak > entry_open and not np.isnan(atr[k]):
            stop_price = running_peak - trail_atr_mult * atr[k]
            if stop_price > 0 and l_k <= stop_price:
                return k, stop_price, 'trail_atr'

    # Max hold 到了 — 隔日 open 出場（這裡是 exit_end，已是隔日 open）
    if exit_end + 1 < n and not np.isnan(o[exit_end]):
        return exit_end, float(o[exit_end]), 'max_hold'
    return exit_end, float(c[exit_end]) if not np.isnan(c[exit_end]) else entry_open, 'max_hold'


# ─────────────────────────────────────────────────────────────────────────
# 一個 ticker — 同時產出所有 (strategy × rule) 組合的 trades（高效率版）
# ─────────────────────────────────────────────────────────────────────────
def gen_all_trades_one(args):
    """args = (ticker, strategies_list, rules_dict)
    回傳：dict[(strategy, rule_name)] -> list of trades"""
    ticker, strategies_list, rules_dict = args
    out = {(s, r): [] for s in strategies_list for r in rules_dict}
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280:
            return out
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        if len(df) < 80:
            return out

        helpers = compute_helper_arrays(df)
        o = df['Open'].values
        idx = df.index
        n = len(df)

        # 對每個 strategy，先一次找出所有 signals
        signals_per_strat = {s: detect_swing_signals(df, s) for s in strategies_list}

        for strat, sigs in signals_per_strat.items():
            for sig_i in sigs:
                entry_i = sig_i + 1
                if entry_i >= n - 1: continue
                entry_open = float(o[entry_i])
                if entry_open <= 0 or np.isnan(entry_open): continue

                # 對每個 exit rule 都從同一個 entry 點 walk
                for rule_name, rule in rules_dict.items():
                    exit_i, exit_price, exit_reason = walk_exit(
                        df, helpers, entry_i, entry_open, rule)
                    if exit_price <= 0 or np.isnan(exit_price): continue

                    gross_ret = (exit_price - entry_open) / entry_open
                    net_ret = gross_ret - COST_ROUND_TRIP
                    out[(strat, rule_name)].append({
                        'ticker': ticker,
                        'strategy': strat,
                        'rule': rule_name,
                        'signal_date': idx[sig_i].strftime('%Y-%m-%d'),
                        'entry_date': idx[entry_i].strftime('%Y-%m-%d'),
                        'exit_date':  idx[exit_i].strftime('%Y-%m-%d'),
                        'hold_days':  exit_i - entry_i,
                        'entry_price': round(entry_open, 4),
                        'exit_price': round(exit_price, 4),
                        'gross_ret': gross_ret,
                        'net_ret': net_ret,
                        'exit_reason': exit_reason,
                    })
        return out
    except Exception:
        return out


# ─────────────────────────────────────────────────────────────────────────
# 統計
# ─────────────────────────────────────────────────────────────────────────
def trade_stats(trades, max_hold):
    if not trades: return None
    df = pd.DataFrame(trades)
    n = len(df)
    win_pct = (df['net_ret'] > 0).mean() * 100
    mean_pct = df['net_ret'].mean() * 100
    median_pct = df['net_ret'].median() * 100
    std_pct = df['net_ret'].std() * 100
    pos_sum = df.loc[df['net_ret'] > 0, 'net_ret'].sum()
    neg_sum = -df.loc[df['net_ret'] < 0, 'net_ret'].sum()
    pf = pos_sum / neg_sum if neg_sum > 0 else 999
    avg_hold = df['hold_days'].mean()
    early_exit = (df['hold_days'] < max_hold).mean() * 100

    # per-trade Sharpe（粗略）
    per_sharpe = (mean_pct / std_pct) if std_pct > 0 else 0

    # exit reason 分佈
    reasons = df['exit_reason'].value_counts(normalize=True).to_dict()

    return {
        'n': n,
        'win_pct': win_pct,
        'mean_pct': mean_pct,
        'median_pct': median_pct,
        'std_pct': std_pct,
        'pf': pf,
        'avg_hold': avg_hold,
        'early_exit_pct': early_exit,
        'per_trade_sharpe': per_sharpe,
        'best_pct': df['net_ret'].max() * 100,
        'worst_pct': df['net_ret'].min() * 100,
        'reasons': {k: round(v * 100, 1) for k, v in reasons.items()},
    }


# ─────────────────────────────────────────────────────────────────────────
# 跑全 universe — 一次 walk 所有 (strategy × rule) 組合
# ─────────────────────────────────────────────────────────────────────────
def run_full_sweep(universe, strategies, rules_dict):
    """對 universe 跑一次，同時收集所有 (strategy × rule) 組合的 trades。"""
    args = [(t, strategies, rules_dict) for t in universe]
    combined = {(s, r): [] for s in strategies for r in rules_dict}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker_out in ex.map(gen_all_trades_one, args, chunksize=80):
            for key, trades in ticker_out.items():
                combined[key].extend(trades)
    return combined


# ─────────────────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────────────────
def run_research(market='tw', strategies=None, quick=False):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"\n{flag} Universe: {len(universe)} 檔  期間 {START_DATE} → 現在")
    print(f"  成本 round-trip: {COST_ROUND_TRIP*100:.2f}%")

    if strategies is None:
        strategies = ['A', 'B', 'C', 'D']
    rule_keys = QUICK_RULES if quick else list(EXIT_RULES.keys())
    rules_dict = {k: EXIT_RULES[k] for k in rule_keys}

    strategy_names = {
        'A': '🌊 趨勢延續',
        'B': '🚀 突破前高',
        'C': '💧 拉回 EMA20',
        'D': '⚡ 動能加速',
    }

    print(f"  策略: {strategies}  Rules: {len(rule_keys)} 個\n")
    print(f"📊 跑 universe（所有組合一次完成）...")
    t0 = time.time()
    combined = run_full_sweep(universe, strategies, rules_dict)
    print(f"  完成 {time.time()-t0:.1f}s\n")

    all_results = {}
    for strat in strategies:
        print('=' * 110)
        print(f"📊 入場策略 {strat} — {strategy_names.get(strat, strat)}")
        print('=' * 110)
        print(f"{'rule':>22}{'n':>7}{'win%':>7}{'mean%':>9}{'med%':>9}"
              f"{'PF':>7}{'avgHold':>9}{'early%':>8}{'best%':>9}{'worst%':>9}")
        print('-' * 110)

        strat_results = {}
        baseline_mean = None
        baseline_win = None
        for rule_name in rule_keys:
            rule = EXIT_RULES[rule_name]
            trades = combined.get((strat, rule_name), [])
            stats = trade_stats(trades, rule.get('max_hold', 90))
            if stats is None:
                print(f"{rule_name:>22}  (no trades)")
                continue
            marker = ''
            if rule_name == 'fixed_30d':
                baseline_mean = stats['mean_pct']
                baseline_win = stats['win_pct']
                marker = ' [baseline]'
            else:
                if baseline_mean is not None:
                    if (stats['mean_pct'] > baseline_mean + 0.3 and
                        stats['win_pct'] >= baseline_win - 1):
                        marker = ' ✅ 雙改善'
                    elif stats['win_pct'] > baseline_win + 3:
                        marker = ' 🟢 win+'
                    elif stats['mean_pct'] > baseline_mean + 1:
                        marker = ' 🔵 mean+'
            print(f"{rule_name:>22}{stats['n']:>7}"
                  f"{stats['win_pct']:>6.1f}%"
                  f"{stats['mean_pct']:>+8.2f}%"
                  f"{stats['median_pct']:>+8.2f}%"
                  f"{stats['pf']:>7.2f}"
                  f"{stats['avg_hold']:>9.1f}"
                  f"{stats['early_exit_pct']:>7.1f}%"
                  f"{stats['best_pct']:>+8.2f}%"
                  f"{stats['worst_pct']:>+8.2f}%{marker}")
            strat_results[rule_name] = stats

        all_results[strat] = strat_results
        print()

    # 寫 JSON（轉 numpy 為 native，避免 json fail）
    def jsonify(obj):
        if isinstance(obj, dict): return {k: jsonify(v) for k, v in obj.items()}
        if isinstance(obj, list): return [jsonify(v) for v in obj]
        if isinstance(obj, (np.integer,)): return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        if isinstance(obj, np.bool_): return bool(obj)
        return obj

    out = f'analyze_swing_dynamic_exit_{market}.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'market': market,
            'universe_size': len(universe),
            'start_date': START_DATE,
            'cost_round_trip_pct': COST_ROUND_TRIP * 100,
            'results': jsonify(all_results),
        }, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 {out}")

    # 總結 — 找出每個 entry 策略的 Top 3 出場規則（按 Sharpe per trade 排序）
    print('\n' + '=' * 110)
    print('🏆 結論：每個入場策略的 Top 3 出場規則（按 mean_net% 排序，需 win%≥50）')
    print('=' * 110)
    for strat, results in all_results.items():
        print(f"\n{strategy_names.get(strat, strat)} ({strat}):")
        sorted_by_mean = sorted(
            [(k, v) for k, v in results.items() if v['win_pct'] >= 50],
            key=lambda x: x[1]['mean_pct'],
            reverse=True)
        if not sorted_by_mean:
            sorted_by_mean = sorted(
                results.items(), key=lambda x: x[1]['mean_pct'], reverse=True)
            print(f"  ⚠️ 沒有 win%≥50 的；改按 mean% 直接排")
        for rank, (rname, st) in enumerate(sorted_by_mean[:3], 1):
            print(f"  #{rank} {rname:<25}  mean {st['mean_pct']:+.2f}%  "
                  f"win {st['win_pct']:.1f}%  PF {st['pf']:.2f}  "
                  f"avgHold {st['avg_hold']:.1f}d")

    return all_results


# ─────────────────────────────────────────────────────────────────────────
# Walk-forward OOS 驗證
# ─────────────────────────────────────────────────────────────────────────
WALKFORWARD_RULES = [
    'fixed_30d',           # baseline
    'fixed_60d',
    'fixed_90d',
    'rsi_70',
    'rsi_75',
    'rsi_80',
    'target_15',
    'target_20',
    'trail_15',
    'death_cross',
]


def split_trades_by_date(trades, split_date):
    """把 trades 按 entry_date 切兩段（train: < split_date, test: >= split_date）"""
    train, test = [], []
    for t in trades:
        if t['entry_date'] < split_date:
            train.append(t)
        else:
            test.append(t)
    return train, test


def fmt_change_marker(train_w, test_w, train_m, test_m):
    """根據 win% 跟 mean% 的變化，給一個視覺標記"""
    dw = test_w - train_w
    dm = test_m - train_m
    if dm < -3 or dw < -8:
        return '🚨 嚴重 decay'
    elif dm < -1 or dw < -3:
        return '⚠️ 輕微 decay'
    elif dm > 1 and dw > 0:
        return '✅ OOS 更好'
    else:
        return '✓ 穩定'


def run_walkforward(market='tw', strategies=None, split_date='2024-01-01'):
    """🆕 Walk-forward OOS 驗證：
    Train = 2020-01 → split_date
    Test  = split_date → 現在
    對 4 入場 × 10 出場 = 40 組合各跑 train/test 比對
    """
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"\n{flag} Walk-forward OOS  Universe: {len(universe)} 檔")
    print(f"  Train: {START_DATE} → {split_date}")
    print(f"  Test:  {split_date} → 現在 (OOS)")
    print(f"  成本 round-trip: {COST_ROUND_TRIP*100:.2f}%")

    if strategies is None:
        strategies = ['A', 'B', 'C', 'D']
    rule_keys = WALKFORWARD_RULES
    rules_dict = {k: EXIT_RULES[k] for k in rule_keys}

    strategy_names = {
        'A': '🌊 趨勢延續',
        'B': '🚀 突破前高',
        'C': '💧 拉回 EMA20',
        'D': '⚡ 動能加速',
    }

    print(f"  策略: {strategies}  Rules: {len(rule_keys)} 個\n")

    print(f"📊 跑 universe（一次完成所有訊號）...")
    t0 = time.time()
    combined = run_full_sweep(universe, strategies, rules_dict)
    print(f"  完成 {time.time()-t0:.1f}s\n")

    all_results = {}
    for strat in strategies:
        print('=' * 130)
        print(f"📊 入場策略 {strat} — {strategy_names.get(strat, strat)}  (TRAIN | TEST(OOS) | Δ)")
        print('=' * 130)
        print(f"{'rule':>14} | "
              f"{'TRAIN n':>8} {'win%':>6} {'mean%':>8} {'PF':>5} | "
              f"{'TEST n':>7} {'win%':>6} {'mean%':>8} {'PF':>5} | "
              f"{'Δwin':>7} {'Δmean':>8} | status")
        print('-' * 130)

        strat_results = {}
        for rule_name in rule_keys:
            rule = EXIT_RULES[rule_name]
            trades = combined.get((strat, rule_name), [])
            train_trades, test_trades = split_trades_by_date(trades, split_date)
            train_st = trade_stats(train_trades, rule.get('max_hold', 90))
            test_st = trade_stats(test_trades, rule.get('max_hold', 90))
            if train_st is None or test_st is None:
                print(f"{rule_name:>14}   (insufficient data)")
                continue
            dw = test_st['win_pct'] - train_st['win_pct']
            dm = test_st['mean_pct'] - train_st['mean_pct']
            marker = fmt_change_marker(train_st['win_pct'], test_st['win_pct'],
                                        train_st['mean_pct'], test_st['mean_pct'])
            print(f"{rule_name:>14} | "
                  f"{train_st['n']:>8} {train_st['win_pct']:>5.1f}% {train_st['mean_pct']:>+7.2f}% {train_st['pf']:>5.2f} | "
                  f"{test_st['n']:>7} {test_st['win_pct']:>5.1f}% {test_st['mean_pct']:>+7.2f}% {test_st['pf']:>5.2f} | "
                  f"{dw:>+6.1f} {dm:>+7.2f} | {marker}")
            strat_results[rule_name] = {
                'train': train_st, 'test': test_st,
                'delta_win': dw, 'delta_mean': dm, 'status': marker,
            }
        all_results[strat] = strat_results
        print()

    # 寫 JSON
    def jsonify(obj):
        if isinstance(obj, dict): return {k: jsonify(v) for k, v in obj.items()}
        if isinstance(obj, list): return [jsonify(v) for v in obj]
        if isinstance(obj, (np.integer,)): return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        if isinstance(obj, np.bool_): return bool(obj)
        return obj

    out = f'walkforward_swing_{market}.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'market': market,
            'universe_size': len(universe),
            'start_date': START_DATE,
            'split_date': split_date,
            'results': jsonify(all_results),
        }, f, indent=2, ensure_ascii=False)
    print(f"✅ 寫入 {out}")

    # 總結：哪些組合 OOS 仍然有效（穩定 or 更好）
    print('\n' + '=' * 130)
    print('🏆 OOS 仍然有效的組合（按 TEST mean% 排序，需 win%≥50 且非「嚴重 decay」）')
    print('=' * 130)
    for strat, results in all_results.items():
        valid = [(rn, r) for rn, r in results.items()
                 if r['test']['win_pct'] >= 50 and '嚴重' not in r['status']]
        valid.sort(key=lambda x: -x[1]['test']['mean_pct'])
        print(f"\n{strategy_names.get(strat, strat)} ({strat}):")
        if not valid:
            print(f"  ⚠️ 沒有「win%≥50 且穩定」的組合 → 此策略 OOS 表現可疑")
            continue
        for rank, (rn, r) in enumerate(valid[:5], 1):
            print(f"  #{rank} {rn:<14}  TEST  win {r['test']['win_pct']:.1f}%  "
                  f"mean {r['test']['mean_pct']:+.2f}%  PF {r['test']['pf']:.2f}  "
                  f"({r['status']})")

    return all_results


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'both'],
                   help='市場：tw / us / both（預設 tw）')
    p.add_argument('--strategy', type=str, default=None,
                   help='只跑單一入場策略（A/B/C/D），預設全跑')
    p.add_argument('--quick', action='store_true',
                   help='只跑核心 6 個出場規則（節省時間）')
    p.add_argument('--walkforward', action='store_true',
                   help='跑 walk-forward OOS 驗證（train 2020-2023, test 2024-現在）')
    p.add_argument('--split', type=str, default='2024-01-01',
                   help='walk-forward 切分日期（預設 2024-01-01）')
    args = p.parse_args()

    strategies = [args.strategy] if args.strategy else None
    markets = ['tw', 'us'] if args.market == 'both' else [args.market]

    if args.walkforward:
        for m in markets:
            run_walkforward(market=m, strategies=strategies, split_date=args.split)
    else:
        for m in markets:
            run_research(market=m, strategies=strategies, quick=args.quick)


if __name__ == '__main__':
    main()
