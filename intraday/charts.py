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

    # Bollinger Bands (20, 1σ + 2σ)
    if show_bb and bb_mid_plot is not None and bb_std_plot is not None:
        try:
            bb_up2 = bb_mid_plot + 2 * bb_std_plot
            bb_lo2 = bb_mid_plot - 2 * bb_std_plot
            bb_up1 = bb_mid_plot + 1 * bb_std_plot
            bb_lo1 = bb_mid_plot - 1 * bb_std_plot
            # 中軌（虛線）
            ax1.plot(x_arr, bb_mid_plot.values, color='#9aaabb', linewidth=0.9,
                      alpha=0.6, linestyle='--', label='BB Mid(20)', zorder=3)
            # ±2σ（外層，較淡）
            ax1.plot(x_arr, bb_up2.values, color='#7a8899', linewidth=0.7,
                      alpha=0.55, linestyle='-', zorder=3)
            ax1.plot(x_arr, bb_lo2.values, color='#7a8899', linewidth=0.7,
                      alpha=0.55, linestyle='-', label='BB ±2σ', zorder=3)
            ax1.fill_between(x_arr, bb_up2.values, bb_lo2.values,
                              color='#7a8899', alpha=0.06, zorder=2)
            # ±1σ（內層，較深的點線）
            ax1.plot(x_arr, bb_up1.values, color='#aabacc', linewidth=0.6,
                      alpha=0.7, linestyle=':', zorder=3)
            ax1.plot(x_arr, bb_lo1.values, color='#aabacc', linewidth=0.6,
                      alpha=0.7, linestyle=':', label='BB ±1σ', zorder=3)
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


# ════════════════════════════════════════════════════════════
# 🆕 v9.32：Plotly 互動版（hover 顯示 OHLC）
# ════════════════════════════════════════════════════════════

def build_zigzag_chart_plotly(
    df: pd.DataFrame,
    atr_mult: float = 1.3,
    title: str = '',
    max_bars: int = 180,
    show_bb: bool = True,
    show_emas: List[int] = None,
    theme: str = 'dark',
):
    """互動 plotly 版 ZigZag chart（hover 顯示 OHLC + 指標）

    Args:
        df: OHLCV DataFrame
        atr_mult: 單一 ATR 倍數
        title: 圖標題
        max_bars: 顯示最後 N bars
        show_bb: 是否疊 BB
        show_emas: 要顯示哪些 EMA
        theme: 'dark' / 'light'

    Returns:
        plotly Figure 物件（給 st.plotly_chart() 用）
    """
    if df is None or len(df) < 30:
        return None
    if show_emas is None:
        show_emas = [5, 20, 50, 150, 200]

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        return None
    from zigzag import zigzag as _zz

    # 在 slice 前算 BB + EMA（避免 EMA200 全 NaN）
    df_full = df.dropna().copy()
    close_full = df_full['Close']
    bb_mid_full = close_full.rolling(20).mean() if len(close_full) >= 20 else None
    bb_std_full = close_full.rolling(20).std() if len(close_full) >= 20 else None
    ema_full = {}
    for p in show_emas:
        if len(close_full) >= p:
            ema_full[p] = close_full.ewm(span=p, adjust=False).mean()

    # 截尾
    df_plot = df_full.tail(max_bars).copy()
    if len(df_plot) < 30:
        return None

    bb_mid_plot = bb_mid_full.tail(max_bars) if bb_mid_full is not None else None
    bb_std_plot = bb_std_full.tail(max_bars) if bb_std_full is not None else None
    ema_plot = {p: s.tail(max_bars) for p, s in ema_full.items()}

    # 偵測 bar 間隔
    try:
        diffs = df_plot.index.to_series().diff().dropna()
        bar_sec = diffs.dt.total_seconds().quantile(0.25)
    except Exception:
        bar_sec = 86400
    is_intraday = bar_sec < 86400 * 0.9

    # 用整數 position 當 x 軸（categorical 無 gap）
    N = len(df_plot)
    x_pos = list(range(N))
    # ts strings（hover 用）
    ts_strs = [t.strftime('%Y-%m-%d %H:%M' if is_intraday else '%Y-%m-%d')
                for t in df_plot.index]
    _idx_dates = pd.to_datetime(df_plot.index)

    def _to_pos(ts):
        try:
            ts = pd.to_datetime(ts)
            diffs = abs(_idx_dates - ts)
            return int(np.argmin(diffs))
        except Exception:
            return 0

    # ── 建立兩列 subplot ──
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.78, 0.22], vertical_spacing=0.02,
        subplot_titles=(title or '', 'Volume'),
    )

    # ── Candlestick ──
    fig.add_trace(
        go.Candlestick(
            x=x_pos,
            open=df_plot['Open'].values,
            high=df_plot['High'].values,
            low=df_plot['Low'].values,
            close=df_plot['Close'].values,
            name='K 線',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350',
            increasing_fillcolor='#26a69a',
            decreasing_fillcolor='#ef5350',
            # hover template：日期 + OHLC + 漲跌幅
            text=[
                f"<b>{ts}</b><br>"
                f"Open: ${o:.4f}<br>"
                f"High: ${h:.4f}<br>"
                f"Low: ${l:.4f}<br>"
                f"Close: ${c:.4f}<br>"
                f"漲跌: {(c-o):.4f} ({(c/o-1)*100:+.2f}%)"
                for ts, o, h, l, c in zip(
                    ts_strs, df_plot['Open'], df_plot['High'],
                    df_plot['Low'], df_plot['Close'])
            ],
            hoverinfo='text',
        ),
        row=1, col=1)

    # ── Bollinger Bands ──
    if show_bb and bb_mid_plot is not None and bb_std_plot is not None:
        bb_up2 = (bb_mid_plot + 2 * bb_std_plot).values
        bb_lo2 = (bb_mid_plot - 2 * bb_std_plot).values
        bb_up1 = (bb_mid_plot + 1 * bb_std_plot).values
        bb_lo1 = (bb_mid_plot - 1 * bb_std_plot).values

        fig.add_trace(go.Scatter(
            x=x_pos, y=bb_up2, mode='lines', name='BB +2σ',
            line=dict(color='#7a8899', width=1, dash='solid'),
            hovertemplate='BB+2σ: $%{y:.4f}<extra></extra>',
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=x_pos, y=bb_lo2, mode='lines', name='BB -2σ',
            line=dict(color='#7a8899', width=1, dash='solid'),
            fill='tonexty', fillcolor='rgba(122,136,153,0.08)',
            hovertemplate='BB-2σ: $%{y:.4f}<extra></extra>',
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=x_pos, y=bb_up1, mode='lines', name='BB +1σ',
            line=dict(color='#aabacc', width=1, dash='dot'),
            hovertemplate='BB+1σ: $%{y:.4f}<extra></extra>',
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=x_pos, y=bb_lo1, mode='lines', name='BB -1σ',
            line=dict(color='#aabacc', width=1, dash='dot'),
            hovertemplate='BB-1σ: $%{y:.4f}<extra></extra>',
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=x_pos, y=bb_mid_plot.values, mode='lines',
            name='BB Mid(20)',
            line=dict(color='#9aaabb', width=1.2, dash='dash'),
            hovertemplate='BB Mid: $%{y:.4f}<extra></extra>',
        ), row=1, col=1)

    # ── EMA 線 ──
    ema_colors = {5: '#ffaa55', 20: '#3b9eff', 50: '#aa66ff',
                   150: '#ff6dc8', 200: '#cc3333'}
    for p in show_emas:
        if p not in ema_plot:
            continue
        color = ema_colors.get(p, '#888888')
        lw = 2.0 if p in (20, 200) else 1.5
        fig.add_trace(go.Scatter(
            x=x_pos, y=ema_plot[p].values, mode='lines',
            name=f'EMA{p}',
            line=dict(color=color, width=lw),
            hovertemplate=f'EMA{p}: $%{{y:.4f}}<extra></extra>',
        ), row=1, col=1)

    # ── ZigZag ──
    try:
        pivots = _zz(df_plot, mode='atr', atr_mult=atr_mult, atr_period=14)
    except Exception:
        pivots = []
    if pivots:
        zz_xs = [_to_pos(p['date']) for p in pivots]
        zz_ys = [p['price'] for p in pivots]
        zz_types = [p['type'] for p in pivots]
        zz_text = [f"ZigZag {t}<br>${y:.4f}" for t, y in zip(zz_types, zz_ys)]
        fig.add_trace(go.Scatter(
            x=zz_xs, y=zz_ys, mode='lines+markers',
            name=f'ZigZag ATR×{atr_mult:.2f} ({len(pivots)} pivots)',
            line=dict(color='#ff6b35', width=2.5),
            marker=dict(size=8, color='gold',
                         line=dict(color='#cc4400', width=1.5)),
            text=zz_text, hoverinfo='text',
        ), row=1, col=1)

    # ── Volume bar chart ──
    vol_colors = ['#26a69a' if c >= o else '#ef5350'
                   for o, c in zip(df_plot['Open'], df_plot['Close'])]
    fig.add_trace(go.Bar(
        x=x_pos, y=df_plot['Volume'].values,
        name='Volume',
        marker=dict(color=vol_colors),
        opacity=0.55,
        text=[
            f"<b>{ts}</b><br>Vol: {v:,.0f}"
            for ts, v in zip(ts_strs, df_plot['Volume'])
        ],
        hoverinfo='text',
        showlegend=False,
    ), row=2, col=1)

    # ── X-axis tick labels ──
    n_ticks = min(10, N)
    tick_pos = sorted(set(
        int(round(i * (N - 1) / (n_ticks - 1))) for i in range(n_ticks)
    )) if n_ticks > 1 else [0]
    tick_labels = [ts_strs[p] for p in tick_pos]

    # ── 主題色 ──
    if theme == 'light':
        bg = '#ffffff'
        paper_bg = '#ffffff'
        font_color = '#1a2a40'
        grid_color = '#e0e4ea'
    else:  # dark
        bg = '#0a1628'
        paper_bg = '#08131f'
        font_color = '#c8dff0'
        grid_color = '#1a2f48'

    fig.update_layout(
        title=dict(text='', font=dict(size=14, color=font_color)),
        height=620,
        hovermode='x unified',   # 滑鼠 hover 整列高亮，所有 trace 一起顯示
        plot_bgcolor=bg,
        paper_bgcolor=paper_bg,
        font=dict(color=font_color, size=11),
        legend=dict(
            orientation='h', y=1.05, x=0,
            bgcolor='rgba(0,0,0,0)', font=dict(size=10),
        ),
        margin=dict(l=50, r=30, t=50, b=40),
        xaxis_rangeslider_visible=False,   # 關掉 candlestick 預設下方滑桿
        dragmode='pan',                     # 預設拖曳模式
    )
    fig.update_xaxes(
        tickmode='array', tickvals=tick_pos, ticktext=tick_labels,
        gridcolor=grid_color, showgrid=True, row=1, col=1,
    )
    fig.update_xaxes(
        tickmode='array', tickvals=tick_pos, ticktext=tick_labels,
        gridcolor=grid_color, showgrid=True, row=2, col=1,
    )
    fig.update_yaxes(gridcolor=grid_color, title_text='Price', row=1, col=1)
    fig.update_yaxes(gridcolor=grid_color, title_text='Volume', row=2, col=1)

    return fig
