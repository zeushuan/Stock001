"""不同 ATR 乘數的 W 底 / VCP 偵測準確度 OOS 驗證

方法：
  1. 對每個 ticker（從 data_cache）取完整歷史
  2. 每月取一個 scan_date（在歷史中段）作為「假設今天」
  3. 對每個 ATR_mult，用截至 scan_date 的資料偵測 W / VCP
  4. 追蹤未來 30 / 60 / 90 日 forward return
  5. 統計每個 ATR_mult 的 win rate / 平均 return / Sharpe

評估：高 win rate × 高平均 return × 適中訊號量 = 最佳設定
"""
import os, sys, glob, traceback
from datetime import timedelta
import pandas as pd
import numpy as np

from zigzag import zigzag, compute_atr
from vcp_from_pivots import detect_vcp_from_pivots

# ────────────────────────────────────────────────────────────────
# 設定
# ────────────────────────────────────────────────────────────────
ATR_MULTS_TEST = [1.5, 2.0, 2.5, 3.0]    # 要驗的 ATR 乘數
FORWARD_DAYS = [10, 30, 60, 90]           # forward return 窗口
MIN_LOOKBACK = 200                         # 偵測時最少需要的歷史
MAX_TICKERS = 500                          # 樣本上限
SCAN_INTERVAL_DAYS = 30                    # 每隔多少天取一個 scan_date
HISTORY_TAIL_BARS = 180                    # 偵測用最近 N 天

# W 偵測門檻
W_SIM_TOL_PCT = 5.0    # 兩底差 <= 5%
W_MIN_REBOUND_PCT = 8.0  # 中間反彈 >= 8%
W_MAX_AGE_BARS = 30     # L2 距 scan_date 最多 30 bars 內（剛形成）

# VCP 門檻
VCP_MIN_CONTRACTIONS = 3
VCP_SCORE_TO_TRIGGER = 3


def find_w_pairs(df, pivots, n_bars, sim_tol_pct=W_SIM_TOL_PCT,
                  min_rebound=W_MIN_REBOUND_PCT, max_age=W_MAX_AGE_BARS):
    """從 ZigZag pivots 找剛形成的 W 配對

    回傳 None 或 dict（最新一個合格 W）
    """
    last = None
    for i in range(len(pivots) - 2):
        a, b, c = pivots[i], pivots[i+1], pivots[i+2]
        if a['type'] != 'L' or b['type'] != 'H' or c['type'] != 'L':
            continue
        sim = abs(a['price'] - c['price']) / min(a['price'], c['price']) * 100
        if sim > sim_tol_pct: continue
        reb = (b['price'] - a['price']) / a['price'] * 100
        if reb < min_rebound: continue
        age = n_bars - 1 - c['idx']
        if age > max_age: continue
        last = {
            'L1_idx': a['idx'], 'L1_price': a['price'],
            'NK_idx': b['idx'], 'NK_price': b['price'],
            'L2_idx': c['idx'], 'L2_price': c['price'],
            'sim_pct': sim, 'rebound_pct': reb, 'age': age,
        }
    return last


def forward_return(df_full, entry_idx, days):
    """從 entry_idx 開始 N 個 trading days 的 return（用 Close）"""
    if entry_idx + days >= len(df_full): return None
    entry_p = float(df_full['Close'].iloc[entry_idx])
    exit_p = float(df_full['Close'].iloc[entry_idx + days])
    if entry_p <= 0: return None
    return (exit_p - entry_p) / entry_p * 100


def run_validation(cache_dir='data_cache', max_files=MAX_TICKERS):
    files = sorted(glob.glob(os.path.join(cache_dir, '*.parquet')))[:max_files]
    print(f'掃 {len(files)} 個 ticker...\n')

    # 結果累積：(atr_mult, pattern, forward_days) -> list of returns
    w_results = {(am, fd): [] for am in ATR_MULTS_TEST for fd in FORWARD_DAYS}
    vcp_results = {(am, fd): [] for am in ATR_MULTS_TEST for fd in FORWARD_DAYS}

    n_scanned = 0
    n_w_detected = {am: 0 for am in ATR_MULTS_TEST}
    n_vcp_detected = {am: 0 for am in ATR_MULTS_TEST}

    for fi, fp in enumerate(files):
        sym = os.path.basename(fp).replace('.parquet', '')
        try:
            df_full = pd.read_parquet(fp)
            df_full.index = pd.to_datetime(df_full.index)
            df_full = df_full.dropna()
            n = len(df_full)
            if n < MIN_LOOKBACK + max(FORWARD_DAYS) + 30:
                continue

            # 每隔 SCAN_INTERVAL_DAYS 取一個 scan date
            scan_indices = list(range(MIN_LOOKBACK,
                                        n - max(FORWARD_DAYS) - 5,
                                        SCAN_INTERVAL_DAYS))

            for scan_idx in scan_indices:
                df_slice = df_full.iloc[max(0, scan_idx - HISTORY_TAIL_BARS):scan_idx+1].copy()
                if len(df_slice) < 60: continue
                n_bars = len(df_slice)

                for atr_mult in ATR_MULTS_TEST:
                    try:
                        pivots = zigzag(df_slice, mode='atr',
                                          atr_mult=atr_mult, atr_period=14)
                    except Exception:
                        continue
                    if len(pivots) < 3: continue

                    # W 偵測
                    w = find_w_pairs(df_slice, pivots, n_bars)
                    if w is not None:
                        n_w_detected[atr_mult] += 1
                        for fd in FORWARD_DAYS:
                            r = forward_return(df_full, scan_idx, fd)
                            if r is not None:
                                w_results[(atr_mult, fd)].append(r)

                    # VCP 偵測
                    try:
                        vcp = detect_vcp_from_pivots(df_slice, pivots,
                                                      min_contractions=VCP_MIN_CONTRACTIONS)
                    except Exception:
                        vcp = {}
                    if vcp.get('is_vcp') and vcp.get('vcp_score', 0) >= VCP_SCORE_TO_TRIGGER:
                        n_vcp_detected[atr_mult] += 1
                        for fd in FORWARD_DAYS:
                            r = forward_return(df_full, scan_idx, fd)
                            if r is not None:
                                vcp_results[(atr_mult, fd)].append(r)

                n_scanned += 1

        except Exception:
            continue

        if (fi + 1) % 100 == 0:
            print(f'進度 {fi+1}/{len(files)} | scans {n_scanned} | '
                  f'W: ' + '/'.join([f'{n_w_detected[am]}' for am in ATR_MULTS_TEST]) +
                  ' | VCP: ' + '/'.join([f'{n_vcp_detected[am]}' for am in ATR_MULTS_TEST]))

    print(f'\n共掃描 {n_scanned} 個 (ticker, date) 組合\n')
    print(f'W 偵測數量: ' + ', '.join([f'ATR×{am}={n_w_detected[am]}' for am in ATR_MULTS_TEST]))
    print(f'VCP 偵測數量: ' + ', '.join([f'ATR×{am}={n_vcp_detected[am]}' for am in ATR_MULTS_TEST]))

    return w_results, vcp_results, n_w_detected, n_vcp_detected


def report(w_results, vcp_results, n_w_detected, n_vcp_detected):
    """產出統計表"""
    def stats(rs):
        if not rs: return None
        arr = np.array(rs)
        return {
            'n': len(arr),
            'mean': arr.mean(),
            'median': np.median(arr),
            'std': arr.std(),
            'win': (arr > 0).sum() / len(arr) * 100,
            'sharpe': arr.mean() / arr.std() if arr.std() > 0 else 0,
            'max': arr.max(),
            'min': arr.min(),
        }

    print('\n' + '='*100)
    print('W 底偵測 OOS 績效')
    print('='*100)
    header = f'{"ATR":>6s} | {"fd":>4s} | {"n":>5s} | {"win%":>6s} | {"mean%":>7s} | {"med%":>7s} | {"std%":>6s} | {"sharpe":>7s} | {"max%":>7s} | {"min%":>7s}'
    print(header)
    print('-' * len(header))
    for atr_mult in ATR_MULTS_TEST:
        for fd in FORWARD_DAYS:
            s = stats(w_results[(atr_mult, fd)])
            if s is None:
                print(f'  {atr_mult:>4.1f} | {fd:>4d} | (no signal)')
            else:
                print(f'  {atr_mult:>4.1f} | {fd:>4d} | {s["n"]:>5d} | '
                      f'{s["win"]:>6.1f} | {s["mean"]:>+7.2f} | {s["median"]:>+7.2f} | '
                      f'{s["std"]:>6.2f} | {s["sharpe"]:>7.3f} | '
                      f'{s["max"]:>+7.1f} | {s["min"]:>+7.1f}')
        print()

    print('\n' + '='*100)
    print('VCP 偵測 OOS 績效')
    print('='*100)
    print(header)
    print('-' * len(header))
    for atr_mult in ATR_MULTS_TEST:
        for fd in FORWARD_DAYS:
            s = stats(vcp_results[(atr_mult, fd)])
            if s is None:
                print(f'  {atr_mult:>4.1f} | {fd:>4d} | (no signal)')
            else:
                print(f'  {atr_mult:>4.1f} | {fd:>4d} | {s["n"]:>5d} | '
                      f'{s["win"]:>6.1f} | {s["mean"]:>+7.2f} | {s["median"]:>+7.2f} | '
                      f'{s["std"]:>6.2f} | {s["sharpe"]:>7.3f} | '
                      f'{s["max"]:>+7.1f} | {s["min"]:>+7.1f}')
        print()


if __name__ == '__main__':
    w_results, vcp_results, n_w, n_vcp = run_validation(max_files=MAX_TICKERS)
    report(w_results, vcp_results, n_w, n_vcp)
