"""調查 AI_Storage_US 勝率偏低的根因"""
import json
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np

# 載入回測結果
with open('reports/sympathy_backtest_12m.json', 'r', encoding='utf-8') as f:
    rep = json.load(f)

trades = rep.get('trades', [])
storage_trades = [t for t in trades if t['group'] == 'AI_Storage_US']
sys.stdout.writef'AI_Storage_US 交易數: {len(storage_trades)}\n')

# 1. 按 score bucket 看勝率
buckets = {
    '0.45-0.60': [t for t in storage_trades if 0.45 <= t['score'] < 0.60],
    '0.60-0.75': [t for t in storage_trades if 0.60 <= t['score'] < 0.75],
    '0.75-0.85': [t for t in storage_trades if 0.75 <= t['score'] < 0.85],
    '0.85+':     [t for t in storage_trades if t['score'] >= 0.85],
}
sys.stdout.write'=== AI_Storage_US 按細粒 score bucket ===')
sys.stdout.writef'{"Bucket":<14s} {"n":>3s}  {"Win%":>6s}  {"AvgRet%":>8s}  {"TP":>3s} {"SL":>3s} {"TIME":>4s}')
for name, ts in buckets.items():
    if not ts: continue
    r = np.array([t['return_pct'] for t in ts])
    tp = sum(1 for t in ts if t['exit_reason'] == 'TP')
    sl = sum(1 for t in ts if t['exit_reason'] == 'SL')
    tm = sum(1 for t in ts if t['exit_reason'] == 'TIME')
    sys.stdout.writef'{name:<14s} {len(ts):>3d}  '
          f'{(r > 0).mean()*100:>6.1f}  {r.mean()*100:>+8.2f}  '
          f'{tp:>3d} {sl:>3d} {tm:>4d}')

# 2. 看 leader 分布
sys.stdout.write'\n=== AI_Storage_US 按 leader 拆分 ===')
leader_stats = {}
for t in storage_trades:
    ld = t['leader']
    leader_stats.setdefault(ld, []).append(t)
for ld, ts in leader_stats.items():
    r = np.array([t['return_pct'] for t in ts])
    sys.stdout.writef'  Leader={ld:<8s} n={len(ts):>2d}  Win={(r>0).mean()*100:>5.1f}%  Avg={r.mean()*100:>+6.2f}%')

# 3. 看 ticker 分布
sys.stdout.write'\n=== AI_Storage_US 按 peer ticker 拆分 ===')
peer_stats = {}
for t in storage_trades:
    pk = t['ticker']
    peer_stats.setdefault(pk, []).append(t)
for pk, ts in sorted(peer_stats.items(), key=lambda x: -len(x[1])):
    r = np.array([t['return_pct'] for t in ts])
    sys.stdout.writef'  Ticker={pk:<8s} n={len(ts):>2d}  Win={(r>0).mean()*100:>5.1f}%  Avg={r.mean()*100:>+6.2f}%')

# 4. 算 4 檔之間的 60d rolling 平均相關性
sys.stdout.write'\n=== AI_Storage_US 成員相關性矩陣（過去 12 個月）===')
import yfinance as yf
import io, contextlib
tks = ['MU', 'SNDK', 'WDC', 'STX']
import data_loader as dl
prices = {}
for t in tks:
    df = dl.load_from_cache(t)
    if df is not None:
        df = df.copy()
        if df.index.tz is not None: df.index = df.index.tz_localize(None)
        df.index = df.index.normalize()
        # 取 2025-05 到 2026-04 的資料
        prices[t] = df.loc['2025-05-01':'2026-04-30', 'Close'].pct_change().dropna()

# 對齊
df_rets = pd.DataFrame(prices).dropna()
sys.stdout.writef'共同交易日: {len(df_rets)}')
corr_matrix = df_rets.corr()
sys.stdout.writecorr_matrix.round(3))

# 5. 平均日報酬與標準差（族群同步性指標）
sys.stdout.write'\n=== 族群同步性檢測 ===')
mean_pairwise_corr = float((corr_matrix.values.sum() - 4) / (4*3))  # 排除對角線
sys.stdout.writef'平均成對相關係數: {mean_pairwise_corr:.3f}')
if mean_pairwise_corr > 0.7:
    sys.stdout.write'   → 高度同步！同族群一起漲，lead-lag pattern 不存在')
elif mean_pairwise_corr > 0.5:
    sys.stdout.write'   → 中度同步')
else:
    sys.stdout.write'   → 低度同步')

# 6. 哪幾天 leader 漲，peer 也漲？（同步 vs 真 laggard）
sys.stdout.write'\n=== Leader 漲幅 vs Peer 同日漲幅 ===')
for t in storage_trades[:5]:
    sys.stdout.writef'  {t["signal_date"]}: Leader={t["leader"]} → Peer={t["ticker"]} '
          f'(Score {t["score"]:.3f}, Exit {t["exit_reason"]}, Return {t["return_pct"]*100:+.2f}%)')
