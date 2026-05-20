"""ATR 細粒度 OOS 結果視覺對照"""
import os
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np

mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei',
                                    'SimHei', 'Arial Unicode MS']
mpl.rcParams['axes.unicode_minus'] = False

# 從 validate_atr_fine.py 跑出來的數據
ATR = [1.25, 1.30, 1.35, 1.40, 1.45, 1.50]

# W 底
W_signals = [6901, 6974, 7030, 7066, 7126, 7169]
W_win60 = [48.4, 48.5, 48.7, 48.9, 48.8, 49.1]
W_mean60 = [3.07, 3.07, 3.07, 3.16, 3.14, 3.40]
W_sharpe60 = [0.148, 0.151, 0.151, 0.157, 0.156, 0.152]
W_win90 = [48.2, 48.4, 48.7, 48.7, 48.7, 48.9]
W_mean90 = [4.77, 4.77, 4.76, 4.84, 4.86, 5.11]
W_sharpe90 = [0.174, 0.177, 0.178, 0.182, 0.178, 0.175]

# VCP
V_signals = [7645, 7724, 7713, 7703, 7654, 7621]
V_win60 = [53.1, 53.6, 53.3, 53.5, 53.6, 53.7]
V_mean60 = [2.92, 3.07, 2.96, 3.01, 3.03, 2.98]
V_sharpe60 = [0.145, 0.159, 0.157, 0.159, 0.160, 0.159]
V_win90 = [52.9, 53.3, 53.2, 53.5, 53.8, 53.8]
V_mean90 = [4.31, 4.51, 4.42, 4.59, 4.59, 4.57]
V_sharpe90 = [0.174, 0.186, 0.181, 0.185, 0.186, 0.184]


fig, axes = plt.subplots(3, 2, figsize=(16, 12))
W_color = '#ff6b35'
V_color = '#2196f3'

def _plot_metric(ax, vals_60, vals_90, title, ylabel, color, fmt='{:.2f}'):
    x = np.arange(len(ATR))
    width = 0.35
    b1 = ax.bar(x - width/2, vals_60, width, label='60d',
                 color=color, alpha=0.8, edgecolor='black', linewidth=0.6)
    b2 = ax.bar(x + width/2, vals_90, width, label='90d',
                 color=color, alpha=0.45, edgecolor='black', linewidth=0.6)
    # 標數字
    for b, v in zip(b1, vals_60):
        ax.text(b.get_x()+b.get_width()/2, v, fmt.format(v),
                 ha='center', va='bottom', fontsize=8, fontweight='bold')
    for b, v in zip(b2, vals_90):
        ax.text(b.get_x()+b.get_width()/2, v, fmt.format(v),
                 ha='center', va='bottom', fontsize=8, alpha=0.7)
    # 高亮最佳
    best60_i = int(np.argmax(vals_60))
    best90_i = int(np.argmax(vals_90))
    ax.scatter([best60_i - width/2], [vals_60[best60_i]], s=120,
                marker='*', color='gold', edgecolor='black', linewidth=1,
                zorder=10)
    ax.scatter([best90_i + width/2], [vals_90[best90_i]], s=120,
                marker='*', color='gold', edgecolor='black', linewidth=1,
                zorder=10)
    ax.set_xticks(x)
    ax.set_xticklabels([f'{a:.2f}' for a in ATR])
    ax.set_title(title, fontweight='bold', fontsize=11)
    ax.set_ylabel(ylabel)
    ax.set_xlabel('ATR 乘數')
    ax.legend(loc='lower right', fontsize=8)
    ax.grid(True, axis='y', alpha=0.3)


# Row 1: W 底
ax = axes[0, 0]
ax.bar(np.arange(len(ATR)), W_signals, color=W_color, alpha=0.75,
        edgecolor='black', linewidth=0.6)
for i, v in enumerate(W_signals):
    ax.text(i, v, str(v), ha='center', va='bottom', fontsize=9, fontweight='bold')
best_i = int(np.argmax(W_signals))
ax.scatter([best_i], [W_signals[best_i]], s=120, marker='*',
            color='gold', edgecolor='black', linewidth=1, zorder=10)
ax.set_xticks(np.arange(len(ATR)))
ax.set_xticklabels([f'{a:.2f}' for a in ATR])
ax.set_title('W底 訊號量', fontweight='bold', fontsize=11)
ax.set_ylabel('# signals')
ax.set_xlabel('ATR 乘數')
ax.grid(True, axis='y', alpha=0.3)

_plot_metric(axes[0, 1], W_win60, W_win90, 'W底 win rate (%)', 'Win %',
              W_color, '{:.1f}')

# Row 2: VCP
ax = axes[1, 0]
ax.bar(np.arange(len(ATR)), V_signals, color=V_color, alpha=0.75,
        edgecolor='black', linewidth=0.6)
for i, v in enumerate(V_signals):
    ax.text(i, v, str(v), ha='center', va='bottom', fontsize=9, fontweight='bold')
best_i = int(np.argmax(V_signals))
ax.scatter([best_i], [V_signals[best_i]], s=120, marker='*',
            color='gold', edgecolor='black', linewidth=1, zorder=10)
ax.set_xticks(np.arange(len(ATR)))
ax.set_xticklabels([f'{a:.2f}' for a in ATR])
ax.set_title('VCP 訊號量', fontweight='bold', fontsize=11)
ax.set_ylabel('# signals')
ax.set_xlabel('ATR 乘數')
ax.grid(True, axis='y', alpha=0.3)

_plot_metric(axes[1, 1], V_win60, V_win90, 'VCP win rate (%)', 'Win %',
              V_color, '{:.1f}')

# Row 3: Sharpe（最關鍵指標）
_plot_metric(axes[2, 0], W_sharpe60, W_sharpe90, 'W底 Sharpe（風險調整後）',
              'Sharpe', W_color, '{:.3f}')
_plot_metric(axes[2, 1], V_sharpe60, V_sharpe90, 'VCP Sharpe（風險調整後）',
              'Sharpe', V_color, '{:.3f}')


fig.suptitle('ATR 乘數 OOS 對照 — 500 ticker × 18 月 × 6 ATR = 17,701 scans\n'
              '⭐ = 該指標最佳值',
              fontsize=13, fontweight='bold', y=1.0)
plt.tight_layout()
out = 'atr_oos_compare.png'
plt.savefig(out, dpi=120, bbox_inches='tight')
plt.close()
print(f'Saved: {out}')
