"""SATS — 6 個 ATR 乘數的 ZigZag 並排對照"""
import os
import matplotlib
matplotlib.use('Agg')

import pandas as pd, yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
import matplotlib as mpl

from zigzag import zigzag
from vcp_from_pivots import detect_vcp_from_pivots

mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei',
                                    'SimHei', 'Arial Unicode MS']
mpl.rcParams['axes.unicode_minus'] = False

SYMBOL = 'SATS'
LOOKBACK = 180
ATR_MULTS = [1.25, 1.30, 1.35, 1.40, 1.45, 1.50]
COLORS = ['#9c27b0', '#3f51b5', '#0066cc', '#00897b', '#ff6b35', '#e63946']

df = yf.download(SYMBOL, period='1y', progress=False, auto_adjust=True)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)
df = df.dropna().tail(LOOKBACK).copy()

fig, axes = plt.subplots(6, 1, figsize=(22, 24), sharex=True)

print(f'=== {SYMBOL} ({len(df)} bars, ${df["Close"].iloc[-1]:.2f}) ===\n')

for ax, atr_mult, color in zip(axes, ATR_MULTS, COLORS):
    pivots = zigzag(df, mode='atr', atr_mult=atr_mult, atr_period=14)

    # 蠟燭線
    for d, row in df.iterrows():
        cl = '#26a69a' if row['Close'] >= row['Open'] else '#ef5350'
        ax.plot([d, d], [row['Low'], row['High']],
                 color=cl, linewidth=0.5, alpha=0.55, zorder=1)
        ax.add_patch(plt.Rectangle(
            (mdates.date2num(d) - 0.3, min(row['Open'], row['Close'])),
            0.6, abs(row['Close'] - row['Open']),
            facecolor=cl, edgecolor=cl, alpha=0.55, zorder=2))

    # ZigZag
    xs = [p['date'] for p in pivots]
    ys = [p['price'] for p in pivots]
    ax.plot(xs, ys, color=color, linewidth=2.4, alpha=0.92,
             marker='o', markersize=8,
             markerfacecolor='gold', markeredgecolor=color,
             markeredgewidth=1.8, zorder=5)

    # 標價格
    for p in pivots:
        is_low = (p['type'] == 'L')
        y_off = -14 if is_low else 14
        cl = '#c62828' if is_low else '#1565c0'
        ax.annotate(f"${p['price']:.0f}",
                     xy=(p['date'], p['price']),
                     xytext=(0, y_off), textcoords='offset points',
                     fontsize=7.5, fontweight='bold', color=cl,
                     ha='center')

    # 跑 VCP（檢查 score）
    try:
        vcp = detect_vcp_from_pivots(df, pivots)
    except Exception:
        vcp = {}

    # VCP 收口 box
    ctr = vcp.get('contractions') or []
    for ci, c in enumerate(ctr):
        cc = ['#42a5f5', '#1e88e5', '#1565c0', '#0d47a1'][ci % 4]
        x_s = min(c['L_date'], c['H_date'])
        x_e = max(c['L_date'], c['H_date'])
        rect = patches.Rectangle(
            (mdates.date2num(x_s), c['L_price']),
            mdates.date2num(x_e) - mdates.date2num(x_s),
            c['H_price'] - c['L_price'],
            linewidth=1.1, edgecolor=cc, facecolor=cc,
            alpha=0.08, zorder=3, linestyle='--')
        ax.add_patch(rect)

    vcp_tag = ''
    if vcp.get('is_vcp'):
        vcp_tag = f'VCP-{vcp.get("vcp_grade","")} ({vcp.get("num_contractions",0)}收口 {vcp.get("breakout_status","")})'
    elif ctr:
        vcp_tag = f'VCP候選 ({len(ctr)}收口, score {vcp.get("vcp_score",0)})'

    title = f'ATR × {atr_mult:.2f}   →   {len(pivots)} pivots'
    if vcp_tag: title += f'   |   {vcp_tag}'
    ax.set_title(title, fontsize=12, fontweight='bold', loc='left', pad=4)
    ax.set_ylabel('Price', fontsize=10)
    ax.grid(True, alpha=0.3)

    print(f'ATR × {atr_mult:.2f}: {len(pivots)} pivots, VCP score {vcp.get("vcp_score",0)}')

axes[-1].xaxis.set_major_locator(mdates.MonthLocator())
axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=20)

fig.suptitle(f'{SYMBOL}  —  ZigZag 6 個 ATR 乘數對照',
              fontsize=14, fontweight='bold', y=0.998)
plt.tight_layout()
out = f'{SYMBOL}_atr_grid.png'
plt.savefig(out, dpi=110, bbox_inches='tight')
plt.close()
print(f'\nSaved: {out}')
