"""細粒度 ATR 乘數 OOS 驗證 — 1.25/1.3/1.35/1.4/1.45/1.5"""
import os, glob
from datetime import timedelta
import pandas as pd
import numpy as np

from zigzag import zigzag, compute_atr
from vcp_from_pivots import detect_vcp_from_pivots

ATR_MULTS_TEST = [1.25, 1.30, 1.35, 1.40, 1.45, 1.50]
FORWARD_DAYS = [30, 60, 90]
MIN_LOOKBACK = 200
MAX_TICKERS = 500
SCAN_INTERVAL_DAYS = 30
HISTORY_TAIL_BARS = 180

W_SIM_TOL_PCT = 5.0
W_MIN_REBOUND_PCT = 8.0
W_MAX_AGE_BARS = 30

VCP_MIN_CONTRACTIONS = 3
VCP_SCORE_TO_TRIGGER = 3


def find_w_pairs(df, pivots, n_bars):
    last = None
    for i in range(len(pivots) - 2):
        a, b, c = pivots[i], pivots[i+1], pivots[i+2]
        if a['type'] != 'L' or b['type'] != 'H' or c['type'] != 'L': continue
        sim = abs(a['price'] - c['price']) / min(a['price'], c['price']) * 100
        if sim > W_SIM_TOL_PCT: continue
        reb = (b['price'] - a['price']) / a['price'] * 100
        if reb < W_MIN_REBOUND_PCT: continue
        age = n_bars - 1 - c['idx']
        if age > W_MAX_AGE_BARS: continue
        last = (a, b, c)
    return last


def forward_return(df_full, entry_idx, days):
    if entry_idx + days >= len(df_full): return None
    entry_p = float(df_full['Close'].iloc[entry_idx])
    exit_p = float(df_full['Close'].iloc[entry_idx + days])
    if entry_p <= 0: return None
    return (exit_p - entry_p) / entry_p * 100


def main():
    files = sorted(glob.glob(os.path.join('data_cache', '*.parquet')))[:MAX_TICKERS]
    print(f'掃 {len(files)} 個 ticker, ATR 細粒度: {ATR_MULTS_TEST}\n')

    w_results = {(am, fd): [] for am in ATR_MULTS_TEST for fd in FORWARD_DAYS}
    vcp_results = {(am, fd): [] for am in ATR_MULTS_TEST for fd in FORWARD_DAYS}
    n_scanned = 0
    n_w = {am: 0 for am in ATR_MULTS_TEST}
    n_vcp = {am: 0 for am in ATR_MULTS_TEST}

    for fi, fp in enumerate(files):
        try:
            df_full = pd.read_parquet(fp)
            df_full.index = pd.to_datetime(df_full.index)
            df_full = df_full.dropna()
            n = len(df_full)
            if n < MIN_LOOKBACK + max(FORWARD_DAYS) + 30: continue

            scan_indices = list(range(MIN_LOOKBACK, n - max(FORWARD_DAYS) - 5,
                                        SCAN_INTERVAL_DAYS))
            for scan_idx in scan_indices:
                df_slice = df_full.iloc[max(0, scan_idx - HISTORY_TAIL_BARS):scan_idx+1].copy()
                if len(df_slice) < 60: continue
                n_bars = len(df_slice)

                for atr_mult in ATR_MULTS_TEST:
                    try:
                        pivots = zigzag(df_slice, mode='atr', atr_mult=atr_mult, atr_period=14)
                    except Exception:
                        continue
                    if len(pivots) < 3: continue

                    w = find_w_pairs(df_slice, pivots, n_bars)
                    if w is not None:
                        n_w[atr_mult] += 1
                        for fd in FORWARD_DAYS:
                            r = forward_return(df_full, scan_idx, fd)
                            if r is not None:
                                w_results[(atr_mult, fd)].append(r)

                    try:
                        vcp = detect_vcp_from_pivots(df_slice, pivots,
                                                       min_contractions=VCP_MIN_CONTRACTIONS)
                    except Exception:
                        vcp = {}
                    if vcp.get('is_vcp') and vcp.get('vcp_score', 0) >= VCP_SCORE_TO_TRIGGER:
                        n_vcp[atr_mult] += 1
                        for fd in FORWARD_DAYS:
                            r = forward_return(df_full, scan_idx, fd)
                            if r is not None:
                                vcp_results[(atr_mult, fd)].append(r)

                n_scanned += 1
        except Exception:
            continue

        if (fi + 1) % 100 == 0:
            print(f'進度 {fi+1}/{len(files)} | scans {n_scanned}')

    print(f'\n共 {n_scanned} 個 scan')
    print(f'W 偵測: {dict(n_w)}')
    print(f'VCP 偵測: {dict(n_vcp)}')

    def stats(rs):
        if not rs: return None
        arr = np.array(rs)
        return (len(arr), arr.mean(), (arr > 0).sum() / len(arr) * 100,
                 arr.mean()/arr.std() if arr.std() > 0 else 0)

    print('\n' + '='*95)
    print('W 底 OOS 績效（細粒度 ATR）')
    print('='*95)
    print(f'{"ATR":>6s} | {"fd":>4s} | {"n":>5s} | {"win%":>6s} | {"mean%":>7s} | {"sharpe":>7s}')
    print('-'*55)
    for atr_mult in ATR_MULTS_TEST:
        for fd in FORWARD_DAYS:
            s = stats(w_results[(atr_mult, fd)])
            if s is None:
                print(f'  {atr_mult:>4.2f} | {fd:>4d} | (no signal)')
            else:
                n, m, w, sr = s
                print(f'  {atr_mult:>4.2f} | {fd:>4d} | {n:>5d} | {w:>6.1f} | {m:>+7.2f} | {sr:>7.3f}')
        print()

    print('\n' + '='*95)
    print('VCP OOS 績效（細粒度 ATR）')
    print('='*95)
    print(f'{"ATR":>6s} | {"fd":>4s} | {"n":>5s} | {"win%":>6s} | {"mean%":>7s} | {"sharpe":>7s}')
    print('-'*55)
    for atr_mult in ATR_MULTS_TEST:
        for fd in FORWARD_DAYS:
            s = stats(vcp_results[(atr_mult, fd)])
            if s is None:
                print(f'  {atr_mult:>4.2f} | {fd:>4d} | (no signal)')
            else:
                n, m, w, sr = s
                print(f'  {atr_mult:>4.2f} | {fd:>4d} | {n:>5d} | {w:>6.1f} | {m:>+7.2f} | {sr:>7.3f}')
        print()


if __name__ == '__main__':
    main()
