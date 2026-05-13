"""即時盯盤圖表（mplfinance, embedded as base64 data URI）"""
import io
import base64
from typing import Optional, List, Tuple

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib as mpl
import pandas as pd
import numpy as np


mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei',
                                    'SimHei', 'Arial Unicode MS']
mpl.rcParams['axes.unicode_minus'] = False


def _draw_candles(ax, df: pd.DataFrame, width_ratio: float = 0.6):
    """畫蠟燭線（依時間間隔自動算 bar width）"""
    if df is None or len(df) == 0: return

    # 自動算 bar width = 連續 bar 平均時間間隔的 60%
    if len(df) >= 2:
        deltas = (df.index[1:] - df.index[:-1]).total_seconds().values
        # 取中位數避免被跨日大 gap 影響
        median_sec = float(pd.Series(deltas).median())
        bar_w = (median_sec / 86400) * width_ratio   # 轉為「天」單位
    else:
        bar_w = 0.6   # 預設日線寬度

    for idx, row in df.iterrows():
        c = '#26a69a' if row['Close'] >= row['Open'] else '#ef5350'
        x = mdates.date2num(idx)
        ax.plot([x, x], [row['Low'], row['High']],
                 color=c, linewidth=0.5, alpha=0.85, zorder=1)
        rect = mpatches.Rectangle(
            (x - bar_w/2, min(row['Open'], row['Close'])),
            bar_w, max(abs(row['Close'] - row['Open']), 0.001),
            facecolor=c, edgecolor=c, zorder=2,
        )
        ax.add_patch(rect)


def make_chart_data_uri(daily_df: pd.DataFrame,
                          intraday_df: Optional[pd.DataFrame],
                          signal: dict,
                          ticker: str = '?') -> Optional[str]:
    """產生雙 pane chart 的 base64 data URI

    左：日線 60d（context + VCP/W 標註）
    右：5m intraday（entry timing + VWAP）
    """
    try:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5),
                                    gridspec_kw={'width_ratios': [1, 1]})
        ax_d, ax_i = axes

        # ─── 左：日線 ─────────
        d = daily_df.tail(80).copy() if daily_df is not None else None
        if d is not None and len(d) > 0:
            _draw_candles(ax_d, d, width_ratio=0.6)

            # SMA20 / 50
            if len(d) >= 20:
                sma20 = d['Close'].rolling(20).mean()
                ax_d.plot(d.index, sma20, color='#ff6b35', linewidth=1.2,
                          alpha=0.85, label='SMA20')
            if len(d) >= 50:
                sma50 = d['Close'].rolling(50).mean()
                ax_d.plot(d.index, sma50, color='#2196f3', linewidth=1.2,
                          alpha=0.85, label='SMA50')

            # Pivot
            if signal.get('pivot_price'):
                ax_d.axhline(signal['pivot_price'], color='purple',
                             linewidth=1.4, alpha=0.75,
                             label=f'Pivot ${signal["pivot_price"]:.2f}')

            # Stop loss
            if signal.get('stop_loss'):
                ax_d.axhline(signal['stop_loss'], color='red',
                             linewidth=1.2, linestyle=':', alpha=0.7,
                             label=f'Stop ${signal["stop_loss"]:.2f}')

            # Target
            if signal.get('target_price'):
                ax_d.axhline(signal['target_price'], color='green',
                             linewidth=1.2, linestyle='--', alpha=0.7,
                             label=f'Target ${signal["target_price"]:.2f}')

            # W 底標
            db = signal.get('db_info') or {}
            if db.get('is_double_bottom'):
                try:
                    L1d = pd.to_datetime(db['left_bottom']['date'])
                    L2d = pd.to_datetime(db['right_bottom']['date'])
                    ax_d.scatter([L1d, L2d],
                                 [db['left_bottom']['price'],
                                  db['right_bottom']['price']],
                                 s=180, facecolor='none',
                                 edgecolor='red', linewidth=2.2,
                                 marker='o', zorder=8,
                                 label=f'W底 {db.get("quality_grade","")}')
                except Exception: pass

            ax_d.set_title(f'{ticker} Daily (last 80d)',
                           fontsize=10, fontweight='bold')
            ax_d.legend(loc='upper left', fontsize=7)
            ax_d.grid(True, alpha=0.3)
            ax_d.xaxis.set_major_locator(mdates.MonthLocator())
            ax_d.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.setp(ax_d.xaxis.get_majorticklabels(), rotation=30, fontsize=7)

        # ─── 右：5m intraday ─────────
        if intraday_df is not None and len(intraday_df) > 0:
            i = intraday_df.tail(78 * 2).copy()  # 2 天 ~78 個 5m bars/day
            _draw_candles(ax_i, i, width_ratio=0.5)

            # VWAP
            try:
                from realtime.intraday_loader import compute_vwap
                vwap = compute_vwap(i)
                if vwap is not None:
                    ax_i.plot(i.index, vwap, color='#9c27b0', linewidth=1.4,
                              alpha=0.85, label='VWAP')
            except Exception: pass

            # Pivot
            if signal.get('pivot_price'):
                ax_i.axhline(signal['pivot_price'], color='purple',
                             linewidth=1.4, alpha=0.75,
                             label=f'Pivot ${signal["pivot_price"]:.2f}')

            # Stop
            if signal.get('stop_loss'):
                ax_i.axhline(signal['stop_loss'], color='red',
                             linewidth=1.0, linestyle=':', alpha=0.6,
                             label=f'Stop')

            # 當前價（橫線）
            if signal.get('price'):
                ax_i.axhline(signal['price'], color='#222',
                             linewidth=1.4, linestyle='-', alpha=0.7)

            ax_i.set_title(f'{ticker} 5m intraday (last 2d) — last {signal.get("last_update","")}',
                           fontsize=10, fontweight='bold')
            ax_i.legend(loc='upper left', fontsize=7)
            ax_i.grid(True, alpha=0.3)
            ax_i.xaxis.set_major_locator(mdates.HourLocator(interval=4))
            ax_i.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            plt.setp(ax_i.xaxis.get_majorticklabels(), rotation=30, fontsize=7)

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=95, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return f'data:image/png;base64,{base64.b64encode(buf.read()).decode("ascii")}'
    except Exception as e:
        import traceback; traceback.print_exc()
        return None
