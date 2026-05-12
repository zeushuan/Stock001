"""Investigate AI_Storage_US — output to file directly"""
import json, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np

import data_loader as dl

# Force UTF-8 output
import io
output = io.StringIO()

def w(msg):
    output.write(str(msg) + '\n')

# Load backtest
with open('reports/sympathy_backtest_12m.json', 'r', encoding='utf-8') as f:
    rep = json.load(f)
trades = rep.get('trades', [])
storage_trades = [t for t in trades if t['group'] == 'AI_Storage_US']
w(f'=== AI_Storage_US trades: {len(storage_trades)} ===\n')

# Bucket
buckets = {
    '0.60-0.70': [t for t in storage_trades if 0.60 <= t['score'] < 0.70],
    '0.70-0.80': [t for t in storage_trades if 0.70 <= t['score'] < 0.80],
    '0.80-0.90': [t for t in storage_trades if 0.80 <= t['score'] < 0.90],
    '0.90+':     [t for t in storage_trades if t['score'] >= 0.90],
}
w('Score bucket breakdown:')
w(f'{"Bucket":<14s} {"n":>3s}  {"Win%":>6s}  {"AvgRet%":>8s}  {"TP/SL/TIME":>14s}')
for name, ts in buckets.items():
    if not ts: continue
    r = np.array([t['return_pct'] for t in ts])
    tp = sum(1 for t in ts if t['exit_reason'] == 'TP')
    sl = sum(1 for t in ts if t['exit_reason'] == 'SL')
    tm = sum(1 for t in ts if t['exit_reason'] == 'TIME')
    w(f'{name:<14s} {len(ts):>3d}  '
      f'{(r > 0).mean()*100:>5.1f}%  {r.mean()*100:>+7.2f}%  '
      f'{tp:>3d}/{sl:>2d}/{tm:>3d}')

# By leader
w('\nBy leader:')
leader_stats = {}
for t in storage_trades:
    leader_stats.setdefault(t['leader'], []).append(t)
for ld, ts in leader_stats.items():
    r = np.array([t['return_pct'] for t in ts])
    w(f'  Leader={ld:<6s} n={len(ts):>2d}  '
      f'Win={(r>0).mean()*100:>5.1f}%  Avg={r.mean()*100:>+6.2f}%')

# By peer
w('\nBy peer ticker:')
peer_stats = {}
for t in storage_trades:
    peer_stats.setdefault(t['ticker'], []).append(t)
for pk, ts in sorted(peer_stats.items(), key=lambda x: -len(x[1])):
    r = np.array([t['return_pct'] for t in ts])
    w(f'  Peer={pk:<6s} n={len(ts):>2d}  '
      f'Win={(r>0).mean()*100:>5.1f}%  Avg={r.mean()*100:>+6.2f}%')

# Correlation matrix
w('\n=== 4-member correlation matrix (last 12M) ===')
tks = ['MU', 'SNDK', 'WDC', 'STX']
prices = {}
for t in tks:
    df = dl.load_from_cache(t)
    if df is not None:
        df = df.copy()
        if df.index.tz is not None: df.index = df.index.tz_localize(None)
        df.index = df.index.normalize()
        rets = df.loc['2025-05-01':'2026-04-30', 'Close'].pct_change().dropna()
        if len(rets) > 50:
            prices[t] = rets
        else:
            w(f'  {t}: insufficient data ({len(rets)} bars)')
    else:
        w(f'  {t}: data_cache miss')

df_rets = pd.DataFrame(prices).dropna()
w(f'Common trading days: {len(df_rets)}, tickers: {list(df_rets.columns)}')
corr = df_rets.corr()
w(corr.round(3).to_string())

if len(corr) >= 2:
    # 平均對外相關（排除自己）
    n = len(corr)
    mean_corr = float((corr.values.sum() - n) / (n*(n-1)))
    w(f'\nAvg pairwise correlation: {mean_corr:.3f}')

# Same-day leader vs peer return
w('\n=== Sample leader-peer 比較 ===')
for t in storage_trades[:10]:
    w(f'  {t["signal_date"]}  Leader={t["leader"]:<5s} → Peer={t["ticker"]:<5s}  '
      f'Score={t["score"]:.3f}  Exit={t["exit_reason"]}  Return={t["return_pct"]*100:+.2f}%')

# Save to file
out_path = 'investigation_storage.txt'
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(output.getvalue())
print(f'Saved: {out_path}')
print(output.getvalue())
