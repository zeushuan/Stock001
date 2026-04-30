"""完整策略回測：倒鎚 + RSI≤25 + ADX↑（v9.11）
========================================================
從之前研究得知這是台股最強單一信號（71.4% 漲、+9.36% 30d）。
本腳本把它變成**完整可交易策略**並做嚴格回測：

策略規則
---------
- Universe: TW 4-digit common stocks (data_cache/{ticker}.parquet)
- 訊號條件:
    * 偵測到「倒鎚」型態 (上影線≥body×2 / 下影線<body×0.3 / drop_30d<-8%)
    * RSI(t) ≤ 25
    * ADX(t) > ADX(t-5)  (ADX 上升)
- 進場: 訊號日次日 Open 買入（避免後驗偏誤）
- 出場: 持有 N 個交易日後，N 天後的 Open 賣出（N=30 預設，也測 15/60）
- 成本: 約 0.67% round-trip
    * 手續費 0.1425% × 0.6 折 × 雙邊 = 0.171%
    * 證交稅 0.3%（賣方）
    * 滑價 0.2%（買賣各 0.1%）

兩種報告
---------
A. 訊號級統計（無資金限制）：所有訊號當作 1 筆獨立交易，看 win rate / mean ret / distribution
B. 投資組合模擬（1M 資金、10 倉位、FIFO 排隊）：得到 equity curve / Sharpe / MDD / CAGR

執行
-----
  python backtest_strategy.py            # 預設 N=30
  python backtest_strategy.py --hold 15  # 持有 15 天
  python backtest_strategy.py --hold 60  # 持有 60 天
  python backtest_strategy.py --all      # 跑 15/30/60 三種比較
"""
import sys, json, time, argparse
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16

# 成本：手續費 0.1425% × 0.6 折 = 0.0855% × 雙邊 = 0.171%；證交稅 0.3% 賣方；滑價 0.1%×2 = 0.2%
COST_ROUND_TRIP = 0.00171 + 0.003 + 0.002  # = 0.671%

START_DATE = '2020-01-01'  # 6 年回測
INITIAL_CAPITAL = 1_000_000
POS_PER_TRADE = 100_000     # 每筆 10 萬，配合 max_positions=10 = 100% 資金
MAX_POSITIONS = 10          # 最多 10 倉位

# US ETF 排除清單（避免被當成普通股回測）
US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
    'ARKK','ARKG','ARKF','ARKW','ARKQ',
}


def get_universe(market='tw'):
    """取得 universe ticker list。"""
    DATA = Path('data_cache')
    if market == 'tw':
        return sorted([
            p.stem for p in DATA.glob('*.parquet')
            if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
            and not p.stem.startswith('00')
        ])
    elif market == 'us':
        return sorted([
            p.stem for p in DATA.glob('*.parquet')
            if p.stem and p.stem.isalpha() and p.stem.isupper()
            and 1 <= len(p.stem) <= 5
            and p.stem not in US_ETF_EXCLUDE
        ])
    elif market == 'us_top':
        # 只用 us_applicable.json 標 TOP 的 200 檔
        if not Path('us_applicable.json').exists():
            print("⚠️ us_applicable.json 不存在，fallback to full us")
            return get_universe('us')
        d = json.load(open('us_applicable.json', encoding='utf-8'))
        top = sorted([t for t, info in d.items() if info.get('tier') == 'TOP'])
        # 也要存在 data_cache 才用
        return sorted([t for t in top if (DATA / f'{t}.parquet').exists()])
    else:
        raise ValueError(f"unknown market: {market}")


def detect_inv_hammer_signals(df):
    """倒鎚 + RSI≤25 + ADX↑ + drop_30d<-8%（強看多）"""
    if len(df) < 60:
        return []
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    body = np.abs(c - o)
    rng = h - l
    upper = h - np.maximum(c, o)
    lower = np.minimum(c, o) - l
    n = len(df)

    drop_30d = np.zeros(n)
    for i in range(30, n):
        if c[i-30] > 0:
            drop_30d[i] = (c[i] - c[i-30]) / c[i-30] * 100

    rsi = df['rsi'].values if 'rsi' in df.columns else None
    adx = df['adx'].values if 'adx' in df.columns else None
    if rsi is None or adx is None:
        return []

    signals = []
    for i in range(60, n - 1):
        if rng[i] <= 0: continue
        if np.isnan(rsi[i]) or np.isnan(adx[i]): continue
        is_inv_hammer = (
            upper[i] >= body[i] * 2.0
            and lower[i] < body[i] * 0.3
            and body[i] > 0.0001 * c[i]
        )
        if not is_inv_hammer: continue
        if drop_30d[i] >= -8: continue
        if rsi[i] > 25: continue
        if i < 5 or np.isnan(adx[i-5]): continue
        if adx[i] <= adx[i-5]: continue
        signals.append(i)
    return signals


def detect_t1_v7_signals(df):
    """T1 即將上穿 V7：距 EMA20 ≤ 1% + 連 2 漲 + 多頭 + ADX≥22
    Forward 6yr: 49.4% win, +2.21% 30d, n=5879"""
    if len(df) < 60:
        return []
    c = df['Close'].values
    e20 = df['e20'].values if 'e20' in df.columns else None
    e60 = df['e60'].values if 'e60' in df.columns else None
    adx = df['adx'].values if 'adx' in df.columns else None
    if e20 is None or e60 is None or adx is None:
        return []
    n = len(df)
    signals = []
    for i in range(60, n - 1):
        if any(np.isnan(x) for x in [e20[i], e60[i], adx[i]]): continue
        if e20[i] <= e60[i]: continue            # 多頭排列
        if c[i] >= e20[i]: continue              # 還沒上穿
        dist_pct = (e20[i] - c[i]) / e20[i] * 100
        if dist_pct > 1.0: continue              # 距 ≤ 1%
        if i < 2: continue
        if not (c[i] > c[i-1] > c[i-2]): continue  # 連 2 天上漲
        if adx[i] < 22: continue                 # ADX≥22
        signals.append(i)
    return signals


def detect_combo_signals(df):
    """🆕 v9.11：倒鎚 + T1_V7 組合（5 天內共同觸發 = 雙重確認）。
    回傳 union of inv_hammer 和 t1_v7 訊號，但只保留有對方訊號在 ±5 天內的。"""
    if len(df) < 60:
        return []
    inv_h_idx = set(detect_inv_hammer_signals(df))
    t1_idx = set(detect_t1_v7_signals(df))
    if not inv_h_idx or not t1_idx:
        return []

    confirmed = set()
    for i in inv_h_idx:
        # 倒鎚當天 i：檢查附近有 t1_v7
        for j in range(max(0, i-5), min(len(df), i+6)):
            if j in t1_idx:
                confirmed.add(i)
                break
    for j in t1_idx:
        for i in range(max(0, j-5), min(len(df), j+6)):
            if i in inv_h_idx:
                confirmed.add(j)
                break
    return sorted(confirmed)


# 策略名稱 → 偵測函數
STRATEGIES = {
    'inv_hammer': detect_inv_hammer_signals,
    't1_v7': detect_t1_v7_signals,
    'combo': detect_combo_signals,
}


def detect_signals(df, strategy='inv_hammer'):
    """Dispatcher。"""
    fn = STRATEGIES.get(strategy)
    if fn is None:
        raise ValueError(f"未知策略: {strategy}（可選 {list(STRATEGIES)}）")
    return fn(df)


def gen_trades_for_one(args):
    """單一 ticker：產出所有 trades。
    args 可選長度：
      (ticker, hold_days)
      (ticker, hold_days, strategy)
      (ticker, hold_days, strategy, stop_pct, trail_pct)
    stop_pct: 固定止損（離 entry_open）。如 0.10 = 跌 10% 平倉
    trail_pct: 移動止損（離高點）。如 0.10 = 從高點回跌 10% 平倉
    """
    ticker = args[0]
    hold_days = args[1]
    strategy = args[2] if len(args) > 2 else 'inv_hammer'
    stop_pct = args[3] if len(args) > 3 else None
    trail_pct = args[4] if len(args) > 4 else None

    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280:
            return []
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        if len(df) < 60:
            return []

        signals = detect_signals(df, strategy=strategy)
        trades = []
        o = df['Open'].values
        h = df['High'].values
        l = df['Low'].values
        c = df['Close'].values
        idx = df.index
        n = len(df)
        rsi_arr = df['rsi'].values if 'rsi' in df.columns else None
        adx_arr = df['adx'].values if 'adx' in df.columns else None
        e20_arr = df['e20'].values if 'e20' in df.columns else None

        for sig_i in signals:
            entry_i = sig_i + 1
            exit_i_max = entry_i + hold_days
            if exit_i_max >= n:
                continue
            entry_open = float(o[entry_i])
            if entry_open <= 0 or np.isnan(entry_open):
                continue

            # 預設：hold N 天後出場
            actual_exit_i = exit_i_max
            actual_exit_price = float(o[exit_i_max])
            stopped_by = None

            # 止損檢查（逐日 walk）
            if stop_pct or trail_pct:
                running_peak = entry_open
                fixed_stop_price = entry_open * (1 - stop_pct) if stop_pct else None
                for k in range(entry_i + 1, exit_i_max + 1):
                    if k >= n: break
                    h_k = float(h[k]) if not np.isnan(h[k]) else None
                    l_k = float(l[k]) if not np.isnan(l[k]) else None
                    if h_k is None or l_k is None: continue
                    # 更新 running peak（用 high）
                    if h_k > running_peak:
                        running_peak = h_k
                    # 固定止損：當天 low 觸發
                    if fixed_stop_price and l_k <= fixed_stop_price:
                        actual_exit_i = k
                        actual_exit_price = fixed_stop_price
                        stopped_by = 'fixed'
                        break
                    # 移動止損：當天 low 觸碰 trailing stop
                    if trail_pct:
                        trail_price = running_peak * (1 - trail_pct)
                        # 必須先有獲利才啟動 trailing（避免立刻被洗）
                        if running_peak > entry_open and l_k <= trail_price:
                            actual_exit_i = k
                            actual_exit_price = trail_price
                            stopped_by = 'trailing'
                            break

            if actual_exit_price <= 0 or np.isnan(actual_exit_price):
                continue
            gross_ret = (actual_exit_price - entry_open) / entry_open
            net_ret = gross_ret - COST_ROUND_TRIP

            rsi_v = float(rsi_arr[sig_i]) if rsi_arr is not None and not np.isnan(rsi_arr[sig_i]) else 50.0
            adx_v = float(adx_arr[sig_i]) if adx_arr is not None and not np.isnan(adx_arr[sig_i]) else 0.0
            close_v = float(c[sig_i])
            e20_v = float(e20_arr[sig_i]) if e20_arr is not None and not np.isnan(e20_arr[sig_i]) else close_v
            drop_30d = ((close_v - c[sig_i-30]) / c[sig_i-30] * 100) if sig_i >= 30 and c[sig_i-30] > 0 else 0
            dist_pct = (e20_v - close_v) / e20_v * 100 if e20_v > 0 else 0

            trades.append({
                'ticker': ticker,
                'signal_date': idx[sig_i].strftime('%Y-%m-%d'),
                'entry_date': idx[entry_i].strftime('%Y-%m-%d'),
                'exit_date': idx[actual_exit_i].strftime('%Y-%m-%d'),
                'entry_price': round(entry_open, 2),
                'exit_price': round(actual_exit_price, 2),
                'gross_ret': gross_ret,
                'net_ret': net_ret,
                'hold_days': actual_exit_i - entry_i,
                'stopped_by': stopped_by,  # None / 'fixed' / 'trailing'
                'rsi': rsi_v,
                'adx': adx_v,
                'drop_30d': drop_30d,
                'dist_to_ema20': dist_pct,
            })
        return trades
    except Exception:
        return []


def trade_level_stats(trades):
    """A: 訊號級統計（每筆訊號 = 1 筆獨立交易）"""
    if not trades:
        return {}
    df = pd.DataFrame(trades)
    n = len(df)
    win = (df['net_ret'] > 0).sum()
    mean_gross = df['gross_ret'].mean()
    mean_net = df['net_ret'].mean()
    median_net = df['net_ret'].median()
    std_net = df['net_ret'].std()
    best = df['net_ret'].max()
    worst = df['net_ret'].min()

    # 分位數
    pcts = df['net_ret'].quantile([0.05, 0.25, 0.5, 0.75, 0.95]).to_dict()

    # Year-by-year
    df['year'] = pd.to_datetime(df['entry_date']).dt.year
    by_year = {}
    for y, sub in df.groupby('year'):
        by_year[int(y)] = {
            'n': len(sub),
            'win_rate': float((sub['net_ret'] > 0).mean() * 100),
            'mean_net': float(sub['net_ret'].mean() * 100),
        }

    return {
        'n_trades': int(n),
        'win_rate_pct': round(win / n * 100, 2),
        'mean_gross_pct': round(mean_gross * 100, 2),
        'mean_net_pct': round(mean_net * 100, 2),
        'median_net_pct': round(median_net * 100, 2),
        'std_net_pct': round(std_net * 100, 2),
        'best_pct': round(best * 100, 2),
        'worst_pct': round(worst * 100, 2),
        'percentiles_net_pct': {f'p{int(k*100)}': round(v * 100, 2)
                                for k, v in pcts.items()},
        'by_year': by_year,
        'expectancy_pct': round(mean_net * 100, 2),  # 同 mean_net
        'profit_factor': round(
            df.loc[df['net_ret'] > 0, 'net_ret'].sum() /
            abs(df.loc[df['net_ret'] < 0, 'net_ret'].sum())
            if (df['net_ret'] < 0).any() else 999, 2),
    }


def portfolio_sim(trades, hold_days, max_pos=None, pos_size=None,
                  priority='fifo'):
    """B: 投資組合模擬。

    規則：
      - 初始資金 1M
      - max_pos: 同時最多倉位數（預設 10）
      - pos_size: 每筆金額（預設 100k = INITIAL_CAPITAL / max_pos）
      - priority: 同一天多訊號時的優先序
          'fifo'      : 先到先進場（按 entry_date + ticker 字母）
          'rsi_low'   : RSI 越低越優先（適用 inv_hammer）
          'drop_deep' : 跌幅越大越優先（適用 inv_hammer）
          'dist_close': 距 EMA20 越近越優先（適用 t1_v7）
          'adx_high'  : ADX 越高越優先（趨勢強）
          'oracle'    : 後驗最佳（peek 真實 ret，不可實戰，只當上界 reference）
      - 每天 NAV = 現金 + 持倉 mark-to-market
    """
    if max_pos is None:
        max_pos = MAX_POSITIONS
    if pos_size is None:
        pos_size = INITIAL_CAPITAL // max_pos
    if not trades:
        return {}

    # 排序所有 entry 訊號：先按 entry_date，同日內按 priority 規則
    df_signals = pd.DataFrame(trades)
    df_signals['entry_dt'] = pd.to_datetime(df_signals['entry_date'])
    df_signals['exit_dt'] = pd.to_datetime(df_signals['exit_date'])

    # priority 排序：在 entry_date 相同時，按指定欄位排序
    if priority == 'fifo':
        sort_cols, sort_asc = ['entry_dt', 'ticker'], [True, True]
    elif priority == 'rsi_low':
        sort_cols, sort_asc = ['entry_dt', 'rsi'], [True, True]   # RSI 越低越好（升序）
    elif priority == 'drop_deep':
        sort_cols, sort_asc = ['entry_dt', 'drop_30d'], [True, True]  # 跌幅越大（值越負，升序）
    elif priority == 'dist_close':
        sort_cols, sort_asc = ['entry_dt', 'dist_to_ema20'], [True, True]  # 距越近（升序）
    elif priority == 'adx_high':
        sort_cols, sort_asc = ['entry_dt', 'adx'], [True, False]  # ADX 越高越好（降序）
    elif priority == 'oracle':
        sort_cols, sort_asc = ['entry_dt', 'gross_ret'], [True, False]  # 真實 ret 高優先（peek）
    else:
        sort_cols, sort_asc = ['entry_dt', 'ticker'], [True, True]
    df_signals = df_signals.sort_values(sort_cols, ascending=sort_asc).reset_index(drop=True)

    # 取所有可能的交易日：合併 entry_dt 和 exit_dt 的 union
    all_dates = sorted(set(df_signals['entry_dt']) | set(df_signals['exit_dt']))

    cash = INITIAL_CAPITAL
    positions = []  # list of {entry_date, exit_date, entry_price, ticker, shares, exit_price?}
    daily_nav = []  # (date, nav)
    executed_trades = []  # 真正進場的（受 max_positions 限制）
    skipped = 0

    sig_iter = iter(df_signals.iterrows())
    next_sig = next(sig_iter, None)

    for d in all_dates:
        # 1) 處理今天到期的 exit
        new_positions = []
        for p in positions:
            if p['exit_dt'] == d:
                # 平倉：用 net_ret（已扣 round-trip cost），跟 A 統計完全一致
                proceeds = pos_size * (1 + p['net_ret'])
                cash += proceeds
                p['close_value'] = proceeds
                executed_trades.append(p)
            else:
                new_positions.append(p)
        positions = new_positions

        # 2) 處理今天新訊號 entry（受倉位上限限制）
        while next_sig is not None and next_sig[1]['entry_dt'] == d:
            _, sig = next_sig
            if len(positions) < max_pos and cash >= pos_size:
                cash -= pos_size
                positions.append({
                    'ticker': sig['ticker'],
                    'entry_dt': sig['entry_dt'],
                    'exit_dt': sig['exit_dt'],
                    'entry_price': sig['entry_price'],
                    'gross_ret': sig['gross_ret'],
                    'net_ret': sig['net_ret'],
                })
            else:
                skipped += 1
            next_sig = next(sig_iter, None)

        # 3) NAV：現金 + 持倉 mark-to-market（簡化：用 entry price 的 100k 做基準，
        #    當天的 unrealized 由「按 hold_days 線性」近似 — 實際我們不精確 mark）
        #    為簡化，這裡用 cash + 持倉數 × 100k 當作 NAV 的下界，會略保守但可比較。
        #    更精確需要每天每檔的收盤價，會慢很多。
        nav = cash + len(positions) * pos_size
        daily_nav.append((d, nav))

    # 收盤：強制平掉殘倉（按各自 net_ret）
    for p in positions:
        proceeds = pos_size * (1 + p['net_ret'])
        cash += proceeds
        p['close_value'] = proceeds
        executed_trades.append(p)

    # 計算指標
    if not executed_trades:
        return {'n_executed': 0, 'n_skipped': skipped}

    df_exec = pd.DataFrame(executed_trades)
    df_exec['profit'] = df_exec['close_value'] - pos_size
    df_exec['exit_dt'] = pd.to_datetime(df_exec['exit_dt'])

    # 用 exit 日期分組計 daily P&L → 換算 daily return → Sharpe
    daily_pl = df_exec.groupby('exit_dt')['profit'].sum().sort_index()
    # 把 daily_pl reindex 到所有交易日
    all_d = pd.DatetimeIndex(sorted([d for d, _ in daily_nav]))
    daily_pl = daily_pl.reindex(all_d, fill_value=0)
    # Equity curve（簡化：cash + 持倉是 100k 的等權，用累積 P&L 做 NAV）
    nav_series = INITIAL_CAPITAL + daily_pl.cumsum()
    daily_ret = nav_series.pct_change().fillna(0)
    sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() > 0 else 0

    # Max drawdown
    peak = nav_series.cummax()
    dd = (nav_series - peak) / peak
    mdd = dd.min()

    # CAGR
    days = (all_d[-1] - all_d[0]).days if len(all_d) > 1 else 365
    years = max(days / 365.25, 0.01)
    final_value = nav_series.iloc[-1]
    cagr = (final_value / INITIAL_CAPITAL) ** (1 / years) - 1

    win = (df_exec['profit'] > 0).sum()

    # 選擇偏誤：比較 executed vs skipped 的訊號品質
    df_all = df_signals[['gross_ret', 'net_ret']].copy()
    exec_signals = df_signals.iloc[:0]  # placeholder
    if 'ticker' in df_exec.columns and 'entry_dt' in df_exec.columns:
        # 用 ticker + entry_dt 去配對 executed
        keys = set(zip(df_exec['ticker'], pd.to_datetime(df_exec['entry_dt'])))
        df_signals_keyed = df_signals.copy()
        df_signals_keyed['key'] = list(zip(df_signals_keyed['ticker'],
                                            df_signals_keyed['entry_dt']))
        exec_signals = df_signals_keyed[df_signals_keyed['key'].isin(keys)]
        skipped_signals = df_signals_keyed[~df_signals_keyed['key'].isin(keys)]
        bias_info = {
            'exec_mean_net_pct': round(exec_signals['net_ret'].mean() * 100, 2),
            'skipped_mean_net_pct': round(skipped_signals['net_ret'].mean() * 100, 2)
                if len(skipped_signals) > 0 else None,
            'exec_win_rate_pct': round((exec_signals['net_ret'] > 0).mean() * 100, 2),
            'skipped_win_rate_pct': round((skipped_signals['net_ret'] > 0).mean() * 100, 2)
                if len(skipped_signals) > 0 else None,
        }
    else:
        bias_info = {}

    return {
        'n_executed': int(len(df_exec)),
        'n_skipped': int(skipped),
        'fill_rate_pct': round(len(df_exec) / (len(df_exec) + skipped) * 100, 1)
            if (len(df_exec) + skipped) > 0 else 100,
        'final_value': round(float(final_value), 0),
        'total_return_pct': round((final_value / INITIAL_CAPITAL - 1) * 100, 2),
        'cagr_pct': round(cagr * 100, 2),
        'sharpe': round(float(sharpe), 2),
        'max_drawdown_pct': round(float(mdd) * 100, 2),
        'win_rate_pct': round(float(win / len(df_exec)) * 100, 2),
        'avg_profit': round(df_exec['profit'].mean(), 0),
        'total_profit': round(df_exec['profit'].sum(), 0),
        'years': round(years, 2),
        'selection_bias': bias_info,
    }


def run_backtest(hold_days=30, strategy='inv_hammer', market='tw'):
    universe = get_universe(market)
    flag = '🇹🇼' if market == 'tw' else ('🇺🇸' + ('(TOP)' if market == 'us_top' else ''))
    print(f"{flag} Strategy: {strategy}  Universe: {len(universe)} 檔  hold={hold_days}d")
    print(f"  期間: {START_DATE} → 現在")
    print(f"  成本: round-trip {COST_ROUND_TRIP*100:.2f}%")
    print(f"  資金: 初始 {INITIAL_CAPITAL:,}, 每筆 {POS_PER_TRADE:,}, 最多 {MAX_POSITIONS} 倉")
    print()

    print(f"📊 跑訊號 + 計算 trades（{WORKERS} workers）...")
    t0 = time.time()
    all_trades = []
    args = [(t, hold_days, strategy) for t in universe]
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for trades in ex.map(gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    print(f"  完成 {time.time()-t0:.1f}s，共 {len(all_trades)} 筆訊號")

    if not all_trades:
        print("❌ 沒有訊號")
        return None

    print()
    print("=" * 70)
    print(f"📈 A. 訊號級統計（不限資金，每筆訊號=獨立 trade）")
    print("=" * 70)
    A = trade_level_stats(all_trades)
    print(f"  訊號數: {A['n_trades']}")
    print(f"  勝率: {A['win_rate_pct']}%   平均淨報酬: {A['mean_net_pct']:+.2f}%")
    print(f"  毛報酬: {A['mean_gross_pct']:+.2f}%   中位數淨: {A['median_net_pct']:+.2f}%")
    print(f"  std: {A['std_net_pct']:.2f}%  best: {A['best_pct']:+.2f}%  worst: {A['worst_pct']:+.2f}%")
    print(f"  Profit factor: {A['profit_factor']}")
    print(f"  分位數淨報酬:")
    for k, v in A['percentiles_net_pct'].items():
        print(f"    {k}: {v:+.2f}%")
    print(f"  逐年:")
    for y, s in sorted(A['by_year'].items()):
        print(f"    {y}: n={s['n']:4d}  win={s['win_rate']:.1f}%  mean={s['mean_net']:+.2f}%")

    print()
    print("=" * 70)
    print(f"📊 B. 投資組合模擬（1M 資金、最多 {MAX_POSITIONS} 倉、FIFO 排隊）")
    print("=" * 70)
    B = portfolio_sim(all_trades, hold_days)
    if B.get('n_executed', 0) > 0:
        print(f"  執行交易: {B['n_executed']} 筆 / 跳過: {B['n_skipped']} 筆 (fill rate {B['fill_rate_pct']}%)")
        print(f"  期末市值: {B['final_value']:,}  (起始 {INITIAL_CAPITAL:,})")
        print(f"  總報酬: {B['total_return_pct']:+.2f}%   CAGR: {B['cagr_pct']:+.2f}%")
        print(f"  Sharpe: {B['sharpe']}   Max DD: {B['max_drawdown_pct']:.2f}%")
        print(f"  勝率(資金限): {B['win_rate_pct']}%   平均利潤: {B['avg_profit']:,.0f}")
        print(f"  總利潤: {B['total_profit']:,.0f}   回測年數: {B['years']}")
        bias = B.get('selection_bias', {})
        if bias and bias.get('skipped_mean_net_pct') is not None:
            print(f"\n  ⚠️ 選擇偏誤（FIFO + 容量限制）:")
            print(f"     執行訊號平均淨: {bias['exec_mean_net_pct']:+.2f}% (win {bias['exec_win_rate_pct']}%)")
            print(f"     跳過訊號平均淨: {bias['skipped_mean_net_pct']:+.2f}% (win {bias['skipped_win_rate_pct']}%)")
            gap = bias['skipped_mean_net_pct'] - bias['exec_mean_net_pct']
            if gap > 1:
                print(f"     → 跳過的訊號比執行的好 {gap:.2f} 點 — 容量是主要瓶頸")
            elif gap < -1:
                print(f"     → 執行的比跳過的好 {-gap:.2f} 點 — FIFO 反而選到好的")
            else:
                print(f"     → 執行與跳過品質相近（{gap:+.2f} 點）")
    else:
        print(f"  ❌ 沒有執行的交易（跳過 {B.get('n_skipped', 0)}）")

    return {
        'config': {
            'strategy': strategy,
            'hold_days': hold_days,
            'start_date': START_DATE,
            'universe_size': len(universe),
            'cost_round_trip_pct': COST_ROUND_TRIP * 100,
            'initial_capital': INITIAL_CAPITAL,
            'max_positions': MAX_POSITIONS,
            'pos_per_trade': POS_PER_TRADE,
        },
        'A_signal_level': A,
        'B_portfolio': B,
    }


def run_stoploss_sweep(strategy='inv_hammer', hold_days=30, max_pos=10):
    """🆕 v9.11：止損掃描。
    比較：no stop / fixed -10% / fixed -15% / trailing 10% / trailing 15%"""
    DATA = Path('data_cache')
    universe = sorted([
        p.stem for p in DATA.glob('*.parquet')
        if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
        and not p.stem.startswith('00')
    ])
    print(f"🇹🇼 Stoploss sweep: {strategy} hold={hold_days}d max_pos={max_pos}")
    print()

    configs = [
        ('no_stop',     None, None),
        ('fixed_10',    0.10, None),
        ('fixed_15',    0.15, None),
        ('trail_10',    None, 0.10),
        ('trail_15',    None, 0.15),
        ('fixed_10_trail_15', 0.10, 0.15),  # 雙層保護
    ]

    print("=" * 110)
    print(f"{'Config':>20}{'n':>6}{'Win%':>7}{'Mean':>9}{'PF':>6}"
          f"{'CAGR%':>9}{'Sharpe':>8}{'MDD%':>9}{'AvgHold':>9}{'Stopped%':>10}")
    print("=" * 110)
    rows = []
    for name, stop_pct, trail_pct in configs:
        t0 = time.time()
        all_trades = []
        args = [(t, hold_days, strategy, stop_pct, trail_pct) for t in universe]
        with ProcessPoolExecutor(max_workers=WORKERS) as ex:
            for trades in ex.map(gen_trades_for_one, args, chunksize=50):
                all_trades.extend(trades)

        if not all_trades:
            continue
        df = pd.DataFrame(all_trades)
        n = len(df)
        win = (df['net_ret'] > 0).mean() * 100
        mean = df['net_ret'].mean() * 100
        pos_sum = df.loc[df['net_ret']>0, 'net_ret'].sum()
        neg_sum = -df.loc[df['net_ret']<0, 'net_ret'].sum() if (df['net_ret']<0).any() else 0.001
        pf = pos_sum / neg_sum if neg_sum > 0 else 999
        avg_hold = df['hold_days'].mean()
        stopped_pct = df['stopped_by'].notna().sum() / n * 100

        # 投組
        B = portfolio_sim(all_trades, hold_days, max_pos=max_pos, priority='drop_deep')

        cagr = B.get('cagr_pct', 0)
        sharpe = B.get('sharpe', 0)
        mdd = B.get('max_drawdown_pct', 0)
        marker = ' ★' if cagr > 8 and sharpe > 1.5 else ''
        print(f"{name:>20}{n:>6}{win:>6.1f}%{mean:>+8.2f}%{pf:>6.2f}"
              f"{cagr:>+8.2f}%{sharpe:>8.2f}{mdd:>+8.2f}%{avg_hold:>9.1f}{stopped_pct:>9.1f}%{marker}")
        rows.append({
            'config': name, 'n': n, 'win_pct': win, 'mean_pct': mean, 'pf': pf,
            'cagr_pct': cagr, 'sharpe': sharpe, 'mdd_pct': mdd,
            'avg_hold': float(avg_hold), 'stopped_pct': float(stopped_pct),
        })
    return rows


def run_priority_sweep(strategy='inv_hammer', hold_days=30, max_pos=10):
    """🆕 v9.11：訊號優先序敏感性。FIFO vs ranked priorities."""
    DATA = Path('data_cache')
    universe = sorted([
        p.stem for p in DATA.glob('*.parquet')
        if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
        and not p.stem.startswith('00')
    ])
    print(f"🇹🇼 Priority sweep: {strategy} hold={hold_days}d max_pos={max_pos}")
    print()

    print(f"📊 跑訊號（{WORKERS} workers）...")
    t0 = time.time()
    all_trades = []
    args = [(t, hold_days, strategy) for t in universe]
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for trades in ex.map(gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    print(f"  完成 {time.time()-t0:.1f}s，共 {len(all_trades)} 筆訊號")

    priorities = ['fifo', 'rsi_low', 'drop_deep', 'dist_close', 'adx_high', 'oracle']
    print()
    print("=" * 100)
    print(f"{'Priority':>14}{'n_exec':>8}{'fill%':>7}{'CAGR%':>9}{'Sharpe':>8}{'MDD%':>9}{'final':>14}")
    print("=" * 100)
    rows = []
    for prio in priorities:
        B = portfolio_sim(all_trades, hold_days, max_pos=max_pos, priority=prio)
        if B.get('n_executed', 0) > 0:
            print(f"{prio:>14}{B['n_executed']:>8}{B['fill_rate_pct']:>6.1f}%"
                  f"{B['cagr_pct']:>+8.2f}%{B['sharpe']:>8}"
                  f"{B['max_drawdown_pct']:>+8.2f}%{B['final_value']:>14,.0f}")
            rows.append({'priority': prio, **B})
        else:
            print(f"{prio:>14}  (no executed)")
    return rows


def run_positions_sweep(strategy='inv_hammer', hold_days=30,
                         positions_list=None):
    """🆕 v9.11：max_positions 敏感性分析。
    對 [5, 10, 20, 50, 100, 200] 各跑投組模擬，看 fill rate 與 CAGR 變化。"""
    if positions_list is None:
        positions_list = [5, 10, 20, 50, 100, 200]

    DATA = Path('data_cache')
    universe = sorted([
        p.stem for p in DATA.glob('*.parquet')
        if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
        and not p.stem.startswith('00')
    ])
    print(f"🇹🇼 Positions sweep: {strategy} hold={hold_days}d")
    print(f"  測試 max_pos: {positions_list}")
    print()

    print(f"📊 跑訊號（{WORKERS} workers）...")
    t0 = time.time()
    all_trades = []
    args = [(t, hold_days, strategy) for t in universe]
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for trades in ex.map(gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    print(f"  完成 {time.time()-t0:.1f}s，共 {len(all_trades)} 筆訊號")

    # 對每個 max_pos 跑投組
    rows = []
    print()
    print("=" * 100)
    print(f"{'max_pos':>8}{'pos_size':>12}{'n_exec':>8}{'n_skip':>8}{'fill%':>7}"
          f"{'CAGR%':>9}{'Sharpe':>8}{'MDD%':>9}{'final':>14}")
    print("=" * 100)
    for mp in positions_list:
        pos_size = INITIAL_CAPITAL // mp
        B = portfolio_sim(all_trades, hold_days, max_pos=mp, pos_size=pos_size)
        if B.get('n_executed', 0) > 0:
            print(f"{mp:>8}{pos_size:>12,}{B['n_executed']:>8}{B['n_skipped']:>8}"
                  f"{B['fill_rate_pct']:>6.1f}%{B['cagr_pct']:>+8.2f}%"
                  f"{B['sharpe']:>8}{B['max_drawdown_pct']:>+8.2f}%"
                  f"{B['final_value']:>14,.0f}")
            rows.append({
                'max_pos': mp,
                'pos_size': pos_size,
                **B,
            })
        else:
            print(f"{mp:>8}{pos_size:>12,}  (no executed)")
    return rows


def run_walkforward(strategy='inv_hammer', hold_days=30,
                    split_date='2024-01-01'):
    """🆕 v9.11：Walk-forward OOS 驗證。
    Train period: START_DATE - split_date
    Test period: split_date - 現在
    用 entry_date 切兩段，比較訊號級 + 投組級指標"""
    DATA = Path('data_cache')
    universe = sorted([
        p.stem for p in DATA.glob('*.parquet')
        if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
        and not p.stem.startswith('00')
    ])
    print(f"🇹🇼 Walk-forward {strategy} hold={hold_days}d  split={split_date}")
    print(f"  Train: {START_DATE} → {split_date}")
    print(f"  Test:  {split_date} → 現在")
    print()

    # 跑全部訊號（一次）
    print(f"📊 跑訊號（{WORKERS} workers）...")
    t0 = time.time()
    all_trades = []
    args = [(t, hold_days, strategy) for t in universe]
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for trades in ex.map(gen_trades_for_one, args, chunksize=50):
            all_trades.extend(trades)
    print(f"  完成 {time.time()-t0:.1f}s，共 {len(all_trades)} 筆訊號")

    # 用 entry_date 切
    train_trades = [t for t in all_trades if t['entry_date'] < split_date]
    test_trades = [t for t in all_trades if t['entry_date'] >= split_date]
    print(f"  Train: {len(train_trades)} 筆, Test: {len(test_trades)} 筆")

    if not train_trades or not test_trades:
        print("❌ 訓練或測試集為空")
        return None

    # 各自計算 trade-level + portfolio
    print()
    print("=" * 70)
    print(f"📈 TRAIN (2020-{split_date}): 訊號級 + 投組")
    print("=" * 70)
    train_A = trade_level_stats(train_trades)
    train_B = portfolio_sim(train_trades, hold_days)
    print(f"  訊號數: {train_A['n_trades']}, 勝率: {train_A['win_rate_pct']}%, "
          f"mean: {train_A['mean_net_pct']:+.2f}%, PF: {train_A['profit_factor']}")
    if train_B.get('n_executed', 0) > 0:
        print(f"  投組: CAGR {train_B['cagr_pct']:+.2f}%, Sharpe {train_B['sharpe']}, "
              f"MDD {train_B['max_drawdown_pct']:.2f}%, fill {train_B['fill_rate_pct']}%")

    print()
    print("=" * 70)
    print(f"📊 TEST (OOS, {split_date}-現在): 訊號級 + 投組")
    print("=" * 70)
    test_A = trade_level_stats(test_trades)
    test_B = portfolio_sim(test_trades, hold_days)
    print(f"  訊號數: {test_A['n_trades']}, 勝率: {test_A['win_rate_pct']}%, "
          f"mean: {test_A['mean_net_pct']:+.2f}%, PF: {test_A['profit_factor']}")
    if test_B.get('n_executed', 0) > 0:
        print(f"  投組: CAGR {test_B['cagr_pct']:+.2f}%, Sharpe {test_B['sharpe']}, "
              f"MDD {test_B['max_drawdown_pct']:.2f}%, fill {test_B['fill_rate_pct']}%")

    # 比較 — alpha decay?
    print()
    print("=" * 70)
    print("🔬 ALPHA DECAY 分析")
    print("=" * 70)
    d_win = test_A['win_rate_pct'] - train_A['win_rate_pct']
    d_mean = test_A['mean_net_pct'] - train_A['mean_net_pct']
    d_pf = test_A['profit_factor'] - train_A['profit_factor']
    decay_emoji = '🚨 嚴重 decay' if d_mean < -2 else '⚠️ 輕微 decay' if d_mean < -0.5 else '✅ 穩定'
    print(f"  訊號級 Δ:")
    print(f"    勝率: {train_A['win_rate_pct']:.1f}% → {test_A['win_rate_pct']:.1f}% ({d_win:+.1f}%)")
    print(f"    平均: {train_A['mean_net_pct']:+.2f}% → {test_A['mean_net_pct']:+.2f}% ({d_mean:+.2f}%)")
    print(f"    PF:   {train_A['profit_factor']:.2f} → {test_A['profit_factor']:.2f} ({d_pf:+.2f})")
    print(f"  ➜ 結論: {decay_emoji}")

    if (train_B.get('n_executed', 0) > 0 and test_B.get('n_executed', 0) > 0):
        d_cagr = test_B['cagr_pct'] - train_B['cagr_pct']
        d_sharpe = test_B['sharpe'] - train_B['sharpe']
        d_mdd = test_B['max_drawdown_pct'] - train_B['max_drawdown_pct']
        print(f"  投組級 Δ:")
        print(f"    CAGR:   {train_B['cagr_pct']:+.2f}% → {test_B['cagr_pct']:+.2f}% ({d_cagr:+.2f}%)")
        print(f"    Sharpe: {train_B['sharpe']} → {test_B['sharpe']} ({d_sharpe:+.2f})")
        print(f"    MDD:    {train_B['max_drawdown_pct']:.2f}% → {test_B['max_drawdown_pct']:.2f}% ({d_mdd:+.2f}%)")

    return {
        'config': {'strategy': strategy, 'hold_days': hold_days,
                   'split_date': split_date,
                   'universe_size': len(universe)},
        'train': {'A_signal_level': train_A, 'B_portfolio': train_B,
                  'period': f'{START_DATE} - {split_date}'},
        'test':  {'A_signal_level': test_A, 'B_portfolio': test_B,
                  'period': f'{split_date} - 現在 (OOS)'},
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--hold', type=int, default=30, help='持有天數（預設 30）')
    p.add_argument('--strategy', type=str, default='inv_hammer',
                   choices=['inv_hammer', 't1_v7', 'combo'],
                   help='策略：inv_hammer (預設) / t1_v7 / combo（兩者 5 天內雙重確認）')
    p.add_argument('--all', action='store_true',
                   help='跑 hold=15/30/60 × strategy=inv_hammer/t1_v7 完整對比')
    p.add_argument('--walkforward', action='store_true',
                   help='跑 walk-forward OOS 驗證（train 2020-2023, test 2024-現在）')
    p.add_argument('--split', type=str, default='2024-01-01',
                   help='walk-forward 切分日期（預設 2024-01-01）')
    p.add_argument('--positions-sweep', action='store_true',
                   help='跑 max_positions 敏感性分析（5/10/20/50/100/200）')
    p.add_argument('--priority-sweep', action='store_true',
                   help='跑訊號優先序敏感性（fifo / rsi_low / drop_deep / 等）')
    p.add_argument('--stoploss-sweep', action='store_true',
                   help='跑止損敏感性（no/fixed_10/fixed_15/trail_10/trail_15）')
    p.add_argument('--max-pos', type=int, default=10,
                   help='priority sweep 用的 max_positions（預設 10）')
    p.add_argument('--market', type=str, default='tw',
                   choices=['tw', 'us', 'us_top'],
                   help='市場：tw (預設) / us / us_top（只用 us_applicable.json TOP 200 檔）')
    args = p.parse_args()

    if args.stoploss_sweep:
        results = {}
        for strat in ['inv_hammer', 't1_v7']:
            for hd in [30]:
                key = f'{strat}_hold{hd}d_pos{args.max_pos}'
                print(f"\n{'#'*70}")
                print(f"# STOPLOSS SWEEP {strat.upper()} hold={hd} max_pos={args.max_pos}")
                print(f"{'#'*70}")
                results[key] = run_stoploss_sweep(strategy=strat, hold_days=hd,
                                                    max_pos=args.max_pos)
        out = f'backtest_stoploss_sweep_pos{args.max_pos}.json'
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n✅ 寫入 {out}")
    elif args.priority_sweep:
        results = {}
        for strat in ['inv_hammer', 't1_v7']:
            for hd in [30]:
                key = f'{strat}_hold{hd}d_pos{args.max_pos}'
                print(f"\n{'#'*70}")
                print(f"# PRIORITY SWEEP {strat.upper()}  hold={hd} max_pos={args.max_pos}")
                print(f"{'#'*70}")
                results[key] = run_priority_sweep(strategy=strat, hold_days=hd,
                                                   max_pos=args.max_pos)
        out = f'backtest_priority_sweep_pos{args.max_pos}.json'
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n✅ 寫入 {out}")
    elif args.positions_sweep:
        results = {}
        for strat in ['inv_hammer', 't1_v7']:
            for hd in [30]:  # 用 OOS 最佳 hold
                key = f'{strat}_hold{hd}d'
                print(f"\n{'#'*70}")
                print(f"# POSITIONS SWEEP {strat.upper()}  hold={hd}")
                print(f"{'#'*70}")
                results[key] = run_positions_sweep(strategy=strat, hold_days=hd)
        out = 'backtest_positions_sweep.json'
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n✅ 寫入 {out}")
    elif args.walkforward:
        results = {}
        for strat in ['inv_hammer', 't1_v7']:
            for hd in [30, 60]:
                key = f'{strat}_hold{hd}d'
                print(f"\n{'#'*70}")
                print(f"# WALK-FORWARD {strat.upper()}  Hold {hd} days")
                print(f"{'#'*70}")
                r = run_walkforward(strategy=strat, hold_days=hd, split_date=args.split)
                if r: results[key] = r
        out = 'backtest_walkforward_results.json'
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n✅ 寫入 {out}")
    elif args.all:
        results = {}
        for strat in ['inv_hammer', 't1_v7']:
            for hd in [15, 30, 60]:
                key = f'{strat}_hold{hd}d'
                print(f"\n{'#'*70}")
                print(f"# {strat.upper()}  Hold {hd} days")
                print(f"{'#'*70}")
                results[key] = run_backtest(hd, strategy=strat)
        out = 'backtest_strategy_results.json'
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n✅ 寫入 {out}")
    else:
        r = run_backtest(args.hold, strategy=args.strategy, market=args.market)
        if r:
            out = f'backtest_{args.market}_{args.strategy}_hold{args.hold}d.json'
            with open(out, 'w', encoding='utf-8') as f:
                json.dump(r, f, indent=2, ensure_ascii=False)
            print(f"\n✅ 寫入 {out}")


if __name__ == '__main__':
    main()
