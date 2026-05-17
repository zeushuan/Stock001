"""Intraday Charts — Stock001 v9.32
======================================

可重用 chart 渲染函數，用 categorical x-axis（TradingView 風格，無 overnight gap）。

對外 API:
  build_zigzag_compare_chart(df, atr_mults=[1.0, 1.3, 1.5, 2.0, 3.0],
                                title='', max_bars=180) -> bytes  (PNG)
       多 ATR 倍數 ZigZag 對照（疊在同一張 candle 上）

  build_zigzag_single_chart(df, atr_mult=1.3, ...) -> bytes
       單一 ATR ZigZag（可獨立使用）
"""
from __future__ import annotations

import io
from typing import List, Optional

import pandas as pd
import numpy as np


# ATR 倍數對應顏色（從淺到深，亮 → 暗）
_ATR_COLORS = {
    1.0: '#ffcc70',   # 黃 — 高敏感（最多 pivot）
    1.3: '#ff8844',   # 橘
    1.5: '#ff4488',   # 粉紅
    2.0: '#aa44dd',   # 紫
    2.5: '#4488dd',   # 藍
    3.0: '#226699',   # 深藍 — 低敏感（最少 pivot）
}


def _color_for_atr(atr_mult: float) -> str:
    """找 ATR 倍數對應顏色 — 沒在表內的取最近的"""
    if atr_mult in _ATR_COLORS:
        return _ATR_COLORS[atr_mult]
    keys = sorted(_ATR_COLORS.keys())
    closest = min(keys, key=lambda k: abs(k - atr_mult))
    return _ATR_COLORS[closest]


def _setup_matplotlib():
    """設定 matplotlib 中文 + Agg backend"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    mpl.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei',
                                          'SimHei', 'Arial Unicode MS']
    mpl.rcParams['axes.unicode_minus'] = False
    return plt


def build_zigzag_compare_chart(
    df: pd.DataFrame,
    atr_mults: List[float] = None,
    title: str = '',
    max_bars: int = 180,
    figsize: tuple = (13, 7),
    show_bb: bool = True,
    show_emas: List[int] = None,
) -> Optional[bytes]:
    """畫多 ATR ZigZag 對照圖（同一張 candle、多條 ZigZag 線疊上去）

    Args:
        df: OHLCV DataFrame，需要 Open/High/Low/Close/Volume
        atr_mults: 要比較的 ATR 倍數清單
        title: 圖標題
        max_bars: 最多顯示幾根 bar（從尾端取）
        figsize: matplotlib figure size
        show_bb: 是否疊布林通道 (20, 2σ)
        show_emas: 要疊哪些 EMA 週期（預設 [5,20,50,150,200]）

    Returns:
        PNG bytes 或 None（資料不足時）
    """
    if show_emas is None:
        show_emas = [5, 20, 50, 150, 200]
    if df is None or len(df) < 30:
        return None
    if atr_mults is None:
        atr_mults = [1.0, 1.3, 1.5, 2.0, 3.0]

    plt = _setup_matplotlib()
    from zigzag import zigzag as _zz

    # 🆕 v9.32：在 slice 前先用「full df」算 EMA + BB（讓 EMA200 / BB 在 plot 範圍內有值）
    df_full = df.dropna().copy()
    close_full = df_full['Close']
    bb_mid_full = close_full.rolling(20).mean() if len(close_full) >= 20 else None
    bb_std_full = close_full.rolling(20).std() if len(close_full) >= 20 else None
    ema_full = {}
    for p in (show_emas or []):
        if len(close_full) >= p:
            ema_full[p] = close_full.ewm(span=p, adjust=False).mean()

    # 截尾
    df_plot = df_full.tail(max_bars).copy()
    if len(df_plot) < 30:
        return None
    # 把 BB / EMA 對應 slice 到 plot 範圍
    bb_mid_plot = bb_mid_full.tail(max_bars) if bb_mid_full is not None else None
    bb_std_plot = bb_std_full.tail(max_bars) if bb_std_full is not None else None
    ema_plot = {p: s.tail(max_bars) for p, s in ema_full.items()}

    # categorical x-axis
    N = len(df_plot)
    _idx_dates = pd.to_datetime(df_plot.index)
    _date_to_pos = {ts: i for i, ts in enumerate(_idx_dates)}

    def _to_pos(ts):
        try:
            ts = pd.to_datetime(ts)
            if ts in _date_to_pos:
                return _date_to_pos[ts]
            diffs = abs(_idx_dates - ts)
            return int(np.argmin(diffs))
        except Exception:
            return 0

    # bar 寬度
    bar_w = 0.65

    # 偵測 bar 間隔（給 label 用）
    try:
        diffs = df_plot.index.to_series().diff().dropna()
        bar_sec = diffs.dt.total_seconds().quantile(0.25)
    except Exception:
        bar_sec = 86400
    is_intraday = bar_sec < 86400 * 0.9

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=figsize,
        gridspec_kw={'height_ratios': [3.5, 1]}, sharex=True)

    # ── 蠟燭線（淡色，讓 ZigZag 突出）──
    for i, (_d, _row) in enumerate(df_plot.iterrows()):
        c = '#26a69a' if _row['Close'] >= _row['Open'] else '#ef5350'
        ax1.plot([i, i], [_row['Low'], _row['High']],
                  color=c, linewidth=0.6, alpha=0.45, zorder=1)
        ax1.add_patch(plt.Rectangle(
            (i - bar_w/2, min(_row['Open'], _row['Close'])),
            bar_w, abs(_row['Close'] - _row['Open']),
            facecolor=c, edgecolor=c, alpha=0.45, zorder=2))

    # ── 🆕 v9.32：布林通道 + 多 EMA 疊圖（用 full df 預算的 series）──
    x_arr = list(range(N))

    # Bollinger Bands (20, 2σ)
    if show_bb and bb_mid_plot is not None and bb_std_plot is not None:
        try:
            bb_up = bb_mid_plot + 2 * bb_std_plot
            bb_lo = bb_mid_plot - 2 * bb_std_plot
            ax1.plot(x_arr, bb_mid_plot.values, color='#9aaabb', linewidth=0.9,
                      alpha=0.6, linestyle='--', label='BB Mid(20)', zorder=3)
            ax1.plot(x_arr, bb_up.values, color='#7a8899', linewidth=0.7,
                      alpha=0.55, linestyle='-', zorder=3)
            ax1.plot(x_arr, bb_lo.values, color='#7a8899', linewidth=0.7,
                      alpha=0.55, linestyle='-', label='BB ±2σ', zorder=3)
            ax1.fill_between(x_arr, bb_up.values, bb_lo.values,
                              color='#7a8899', alpha=0.08, zorder=2)
        except Exception:
            pass

    # 5 條 EMA（用 full df 預算）
    ema_colors = {
        5:   '#ffaa55',   # 橘 — 短期快線
        20:  '#3b9eff',   # 藍 — 中短期
        50:  '#aa66ff',   # 紫 — Minervini
        150: '#ff6dc8',   # 粉 — SEPA
        200: '#cc3333',   # 紅 — 死叉指標
    }
    for p in show_emas:
        if p not in ema_plot:
            continue
        try:
            color = ema_colors.get(p, '#888888')
            lw = 1.4 if p in (20, 200) else 1.1
            ax1.plot(x_arr, ema_plot[p].values, color=color, linewidth=lw,
                      alpha=0.85, label=f'EMA{p}', zorder=4)
        except Exception:
            pass

    # ── 對每個 ATR 倍數畫 ZigZag ──
    # 從敏感（亮色）到不敏感（深色）疊上去 — 淺色在底、深色在上方
    for atr_m in sorted(atr_mults):
        try:
            pivots = _zz(df_plot, mode='atr', atr_mult=atr_m, atr_period=14)
        except Exception:
            continue
        if not pivots or len(pivots) < 2:
            continue
        color = _color_for_atr(atr_m)
        xs = [_to_pos(p['date']) for p in pivots]
        ys = [p['price'] for p in pivots]
        # 越大的 ATR 倍數 line 越粗（強調 major pivots）
        lw = 1.0 + (atr_m - 1.0) * 0.6   # ATR=1: 1.0; ATR=3: 2.2
        ax1.plot(xs, ys,
                  color=color, linewidth=lw, alpha=0.85,
                  marker='o', markersize=4 + atr_m,
                  markerfacecolor=color, markeredgecolor='white',
                  markeredgewidth=0.8,
                  label=f'ATR×{atr_m} ({len(pivots)} pivots)',
                  zorder=5 + int(atr_m * 10))

    # ── 標題 + 設定 ──
    if not title:
        title = f'ZigZag ATR 倍數對照 ({len(df_plot)} bars, 5 條線)'
    ax1.set_title(title, fontsize=11, fontweight='bold', pad=8)
    ax1.set_ylabel('Price', fontsize=9)
    # 🆕 v9.32：legend 用 2 row 排版（容納 BB + 5 EMA + ZigZag）
    ax1.legend(loc='upper left', fontsize=7.5, ncol=4,
                framealpha=0.92, columnspacing=0.8)
    ax1.grid(True, alpha=0.25)
    ax1.set_xlim(-1, N)

    # ── Volume ──
    for i, (_d, _row) in enumerate(df_plot.iterrows()):
        c = '#26a69a' if _row['Close'] >= _row['Open'] else '#ef5350'
        ax2.bar(i, _row['Volume'], color=c, alpha=0.5, width=bar_w)
    ax2.set_ylabel('Vol', fontsize=8)
    ax2.grid(True, alpha=0.25)
    ax2.set_xlim(-1, N)

    # ── x-axis tick labels ──
    n_ticks = min(8, N)
    if n_ticks > 1:
        tick_pos = sorted(set(
            int(round(i * (N - 1) / (n_ticks - 1))) for i in range(n_ticks)
        ))
        fmt = '%m-%d %H:%M' if is_intraday else '%Y-%m-%d'
        tick_labels = [_idx_dates[p].strftime(fmt) for p in tick_pos]
        ax2.set_xticks(tick_pos)
        ax2.set_xticklabels(tick_labels)
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=20, fontsize=8)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def build_zigzag_compare_chart_b64(
    df: pd.DataFrame,
    atr_mults: List[float] = None,
    title: str = '',
    max_bars: int = 180,
) -> Optional[str]:
    """同上但回傳 base64 data URI（給 HTML 嵌入）"""
    import base64
    png = build_zigzag_compare_chart(df, atr_mults, title, max_bars)
    if png is None:
        return None
    return f'data:image/png;base64,{base64.b64encode(png).decode("ascii")}'
