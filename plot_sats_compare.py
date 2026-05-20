"""SATS — ATR×1.5 vs ATR×1.0 ZigZag 對照"""
import os
import matplotlib
matplotlib.use('Agg')

import pandas as pd, yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
import matplotlib as mpl

from zigzag import zigzag
from double_pattern import detect_double_bottom, detect_double_top, detect_vcp_zigzag

mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei',
                                    'SimHei', 'Arial Unicode MS']
mpl.rcParams['axes.unicode_minus'] = False

SYMBOL = 'SATS'
LOOKBACK = 180

df = yf.download(SYMBOL, period='1y', progress=False, auto_adjust=True)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)
df = df.dropna().tail(LOOKBACK).copy()

print(f'=== {SYMBOL} ({len(df)} bars, ${df["Close"].iloc[-1]:.2f}) ===\n')

ATR_MULTS = [1.5, 1.25, 1.0]
fig, axes = plt.subplots(3, 1, figsize=(20, 15), sharex=True)

for ax, atr_mult, color in zip(axes, ATR_MULTS, ['#ff6b35', '#0066cc', '#9c27b0']):
    pivots = zigzag(df, mode='atr', atr_mult=atr_mult, atr_period=14)

    # 蠟燭線
    for d, row in df.iterrows():
        cl = '#26a69a' if row['Close'] >= row['Open'] else '#ef5350'
        ax.plot([d, d], [row['Low'], row['High']],
                 color=cl, linewidth=0.5, alpha=0.6, zorder=1)
        ax.add_patch(plt.Rectangle(
            (mdates.date2num(d) - 0.3, min(row['Open'], row['Close'])),
            0.6, abs(row['Close'] - row['Open']),
            facecolor=cl, edgecolor=cl, alpha=0.6, zorder=2))

    # ZigZag
    xs = [p['date'] for p in pivots]
    ys = [p['price'] for p in pivots]
    ax.plot(xs, ys, color=color, linewidth=2.5, alpha=0.9,
             marker='o', markersize=9,
             markerfacecolor='gold', markeredgecolor=color,
             markeredgewidth=2, zorder=5,
             label=f'ZigZag ATR×{atr_mult} — {len(pivots)} pivots')

    # 標日期 + 價
    for p in pivots:
        is_low = (p['type'] == 'L')
        y_off = -18 if is_low else 18
        cl = '#c62828' if is_low else '#1565c0'
        ax.annotate(f"{p['date'].strftime('%m/%d')}\n${p['price']:.1f}",
                     xy=(p['date'], p['price']),
                     xytext=(0, y_off), textcoords='offset points',
                     fontsize=8, fontweight='bold', color=cl,
                     ha='center',
                     bbox=dict(boxstyle='round,pad=0.2',
                                 facecolor='white', edgecolor=cl, alpha=0.9))

    # 跑 W / VCP
    if atr_mult == 1.5:
        db = detect_double_bottom(df, lookback_days=LOOKBACK)
        dt = detect_double_top(df, lookback_days=LOOKBACK)
        vcp = detect_vcp_zigzag(df, lookback_days=LOOKBACK)
    else:
        # 暫時手動跑（ATR×1.0 / ATR×1.25）
        from vcp_from_pivots import detect_vcp_from_pivots
        vcp = detect_vcp_from_pivots(df, pivots)
        # W: 找 L-H-L 三元組
        db_pairs = []
        for i in range(len(pivots)-2):
            a, b, c = pivots[i], pivots[i+1], pivots[i+2]
            if a['type'] == 'L' and b['type'] == 'H' and c['type'] == 'L':
                sim = abs(a['price']-c['price'])/min(a['price'],c['price'])
                reb = (b['price']-a['price'])/a['price']*100
                if sim <= 0.05 and reb >= 8:
                    db_pairs.append((a, b, c, sim, reb))
        # 把最近的 W 候選變成完整 dict 結構
        if db_pairs:
            a, b, c, sim, reb = db_pairs[-1]
            db = {
                'is_double_bottom': True,
                'n_candidates': len(db_pairs),
                'quality_grade': '-',
                'status': 'candidate',
                'left_bottom':  {'date': a['date'].strftime('%Y-%m-%d'), 'price': a['price']},
                'right_bottom': {'date': c['date'].strftime('%Y-%m-%d'), 'price': c['price']},
                'middle_peak':  {'date': b['date'].strftime('%Y-%m-%d'), 'price': b['price']},
            }
        else:
            db = {'is_double_bottom': False, 'n_candidates': 0}
        dt = {}

    # 標 W
    if isinstance(db, dict) and db.get('is_double_bottom'):
        L1d = pd.to_datetime(db['left_bottom']['date'])
        L2d = pd.to_datetime(db['right_bottom']['date'])
        NKd = pd.to_datetime(db['middle_peak']['date'])
        L1p = db['left_bottom']['price']
        L2p = db['right_bottom']['price']
        NKp = db['middle_peak']['price']
        ax.scatter([L1d, L2d], [L1p, L2p], s=350, facecolor='none',
                    edgecolor='red', linewidth=3, zorder=8,
                    label=f'W底 Grade {db.get("quality_grade","")}')
        ax.scatter([NKd], [NKp], s=350, facecolor='none',
                    edgecolor='blue', linewidth=3, zorder=8)

    # VCP 收口 box
    ctr = (vcp or {}).get('contractions') or []
    for ci, c in enumerate(ctr):
        cc = ['#42a5f5', '#1e88e5', '#1565c0', '#0d47a1'][ci % 4]
        x_s = min(c['L_date'], c['H_date'])
        x_e = max(c['L_date'], c['H_date'])
        rect = patches.Rectangle(
            (mdates.date2num(x_s), c['L_price']),
            mdates.date2num(x_e) - mdates.date2num(x_s),
            c['H_price'] - c['L_price'],
            linewidth=1.2, edgecolor=cc, facecolor=cc,
            alpha=0.10, zorder=3, linestyle='--')
        ax.add_patch(rect)

    tags = []
    if isinstance(db, dict):
        if db.get('is_double_bottom'):
            tags.append(f'W底-{db.get("quality_grade","")} ({db.get("status","")})')
        elif db.get('n_candidates'):
            tags.append(f'W底候選 {db["n_candidates"]}')
    if isinstance(dt, dict) and dt.get('is_double_top'):
        tags.append(f'M頂-{dt.get("quality_grade","")}')
    if vcp.get('is_vcp'):
        tags.append(f'VCP-{vcp.get("vcp_grade","")} ({vcp.get("num_contractions",0)}收口 {vcp.get("breakout_status","")})')
    elif ctr:
        tags.append(f'VCP候選 ({len(ctr)}收口, score {vcp.get("vcp_score",0)})')

    title = f'ATR × {atr_mult} → {len(pivots)} pivots'
    if tags: title += '  |  ' + '  ｜  '.join(tags)
    ax.set_title(title, fontsize=11, fontweight='bold', loc='left')
    ax.set_ylabel('Price', fontsize=10)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3)

axes[-1].xaxis.set_major_locator(mdates.MonthLocator())
axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.xticks(rotation=20)

fig.suptitle(f'{SYMBOL}  —  ZigZag ATR×1.5 vs ATR×1.0 對照',
              fontsize=13, fontweight='bold', y=1.0)
plt.tight_layout()
out = f'{SYMBOL}_zigzag_compare.png'
plt.savefig(out, dpi=110, bbox_inches='tight')
plt.close()
print(f'Saved: {out}')

# 印 pivots
for atr_mult in [1.5, 1.25, 1.0]:
    pivs = zigzag(df, mode='atr', atr_mult=atr_mult)
    print(f'\n=== SATS ATR × {atr_mult} ({len(pivs)} pivots) ===')
    for p in pivs:
        tag = p['type'] + ('?' if p.get('tentative') else '')
        print(f'  {tag}  {p["date"].strftime("%Y-%m-%d")}  ${p["price"]:.2f}')
