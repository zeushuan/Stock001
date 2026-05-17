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
from typing import List, Optional, Dict

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
        show_emas = [5, 20, 60, 150, 200]
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
    show_macd: bool = True,
    theme: str = 'dark',
    swing_trades: Optional[List[Dict]] = None,
    reentry_events: Optional[List[Dict]] = None,    # 🆕 v9.40
):
    """互動 plotly 版 ZigZag chart（hover 顯示 OHLC + 指標）

    Args:
        df: OHLCV DataFrame
        atr_mult: 單一 ATR 倍數
        title: 圖標題
        max_bars: 顯示最後 N bars
        show_bb: 是否疊 BB
        show_emas: 要顯示哪些 EMA
        show_macd: 是否加 MACD 子圖
        theme: 'dark' / 'light'
        swing_trades: 戰法歷史交易（給 entry/exit marker 用）
        reentry_events: 🆕 v9.40 歷史加碼事件（黃色 marker）

    Returns:
        plotly Figure 物件（給 st.plotly_chart() 用）
    """
    if df is None or len(df) < 30:
        return None
    if show_emas is None:
        show_emas = [5, 20, 60, 150, 200]

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        return None
    from zigzag import zigzag as _zz

    # 在 slice 前算 BB + EMA + MACD（避免 EMA200 / MACD signal 全 NaN）
    df_full = df.dropna().copy()
    close_full = df_full['Close']
    bb_mid_full = close_full.rolling(20).mean() if len(close_full) >= 20 else None
    bb_std_full = close_full.rolling(20).std() if len(close_full) >= 20 else None
    ema_full = {}
    for p in show_emas:
        if len(close_full) >= p:
            ema_full[p] = close_full.ewm(span=p, adjust=False).mean()
    # MACD (12, 26, 9)
    macd_data = None
    if show_macd and len(close_full) >= 35:   # 26 EMA + 9 signal ≈ 35 bar 才穩
        ema12 = close_full.ewm(span=12, adjust=False).mean()
        ema26 = close_full.ewm(span=26, adjust=False).mean()
        macd_line_full = ema12 - ema26
        signal_full = macd_line_full.ewm(span=9, adjust=False).mean()
        hist_full = macd_line_full - signal_full
        macd_data = (macd_line_full, signal_full, hist_full)

    # 截尾
    df_plot = df_full.tail(max_bars).copy()
    if len(df_plot) < 30:
        return None

    bb_mid_plot = bb_mid_full.tail(max_bars) if bb_mid_full is not None else None
    bb_std_plot = bb_std_full.tail(max_bars) if bb_std_full is not None else None
    ema_plot = {p: s.tail(max_bars) for p, s in ema_full.items()}
    macd_plot = (
        (macd_data[0].tail(max_bars), macd_data[1].tail(max_bars),
         macd_data[2].tail(max_bars))
        if macd_data is not None else None
    )

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

    # ── 建立 subplot（3 列：Price / MACD / Volume；無 MACD 則 2 列）──
    _has_macd = (macd_plot is not None)
    if _has_macd:
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True,
            row_heights=[0.66, 0.17, 0.17],
            vertical_spacing=0.03,
        )
    else:
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            row_heights=[0.78, 0.22], vertical_spacing=0.04,
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
    ema_colors = {5: '#ffaa55', 20: '#3b9eff', 60: '#aa66ff',
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

    # ── 🆕 v9.32：EMA20/EMA60 黃金/死亡交叉標示 (K 線圖上的 T1 訊號) ──
    # EMA60 不在預設 show_emas 中，這裡獨立計算（cross 偵測不受 show_emas 影響）
    try:
        _e20_full = close_full.ewm(span=20, adjust=False).mean()
        _e60_full = close_full.ewm(span=60, adjust=False).mean()
        e20_vals = _e20_full.tail(max_bars).values
        e60_vals = _e60_full.tail(max_bars).values
    except Exception:
        e20_vals = None; e60_vals = None
    if e20_vals is not None and e60_vals is not None and len(e20_vals) >= 2:
        ema_gold_xs, ema_gold_ys, ema_gold_texts = [], [], []
        ema_death_xs, ema_death_ys, ema_death_texts = [], [], []
        close_vals = df_plot['Close'].values
        for i in range(1, len(e20_vals)):
            if any(np.isnan(x) for x in (e20_vals[i], e60_vals[i],
                                            e20_vals[i-1], e60_vals[i-1])):
                continue
            prev_diff = e20_vals[i-1] - e60_vals[i-1]
            curr_diff = e20_vals[i] - e60_vals[i]
            if prev_diff <= 0 and curr_diff > 0:
                # 黃金交叉：EMA20 上穿 EMA60
                ema_gold_xs.append(i)
                # 標記畫在該 bar 的 Low 下方一點（避免擋 candle）
                ema_gold_ys.append(df_plot['Low'].iloc[i] * 0.998)
                ema_gold_texts.append(
                    f'<b>🌟 EMA 黃金交叉</b><br>{ts_strs[i]}<br>'
                    f'EMA20: ${e20_vals[i]:.4f}<br>'
                    f'EMA60: ${e60_vals[i]:.4f}<br>'
                    f'Close: ${close_vals[i]:.4f}<br>'
                    f'<b style="color:#3dbb6a">T1 進場訊號</b>'
                )
            elif prev_diff >= 0 and curr_diff < 0:
                # 死亡交叉：EMA20 下穿 EMA60
                ema_death_xs.append(i)
                # 標記畫在該 bar 的 High 上方一點
                ema_death_ys.append(df_plot['High'].iloc[i] * 1.002)
                ema_death_texts.append(
                    f'<b>💀 EMA 死亡交叉</b><br>{ts_strs[i]}<br>'
                    f'EMA20: ${e20_vals[i]:.4f}<br>'
                    f'EMA60: ${e60_vals[i]:.4f}<br>'
                    f'Close: ${close_vals[i]:.4f}<br>'
                    f'<b style="color:#ff5555">出場 / 不進場警示</b>'
                )

        if ema_gold_xs:
            fig.add_trace(go.Scatter(
                x=ema_gold_xs, y=ema_gold_ys, mode='markers',
                name=f'🌟 EMA 金叉 ({len(ema_gold_xs)})',
                marker=dict(
                    symbol='triangle-up', size=16,
                    color='#3dbb6a',
                    line=dict(color='#0a4a14', width=2),
                ),
                text=ema_gold_texts, hoverinfo='text',
            ), row=1, col=1)
        if ema_death_xs:
            fig.add_trace(go.Scatter(
                x=ema_death_xs, y=ema_death_ys, mode='markers',
                name=f'💀 EMA 死叉 ({len(ema_death_xs)})',
                marker=dict(
                    symbol='triangle-down', size=16,
                    color='#ff5555',
                    line=dict(color='#4a0a0a', width=2),
                ),
                text=ema_death_texts, hoverinfo='text',
            ), row=1, col=1)

    # ── ZigZag 折線（🆕 v9.34：已停用，不再顯示 ATR×N 趨勢線）──
    # pivots 仍計算（其他功能可能需要），但不畫到 chart 上
    # try:
    #     pivots = _zz(df_plot, mode='atr', atr_mult=atr_mult, atr_period=14)
    # except Exception:
    #     pivots = []
    # if pivots:
    #     fig.add_trace(go.Scatter(...))   # 不再 add

    # ── 🆕 v9.33：戰法歷史 entry/exit markers ──
    if swing_trades:
        # 轉換 trade 的時間到 plot position
        entry_xs, entry_ys, entry_texts, entry_colors = [], [], [], []
        exit_xs, exit_ys, exit_texts, exit_colors = [], [], [], []
        # 連線 entry → exit
        line_segments_x = []
        line_segments_y = []
        line_colors = []
        for tr in swing_trades:
            try:
                e_ts = pd.to_datetime(tr['entry_time'])
                e_pos = _to_pos(e_ts)
                # 只顯示在 plot 範圍內的
                if e_pos < 0 or e_pos >= N:
                    continue
                entry_xs.append(e_pos)
                entry_ys.append(tr['entry_price'] * 0.997)   # 標在 bar 下方
                entry_texts.append(
                    f"<b>🟢 進場 {tr['entry_mode']}</b><br>"
                    f"{e_ts.strftime('%Y-%m-%d %H:%M')}<br>"
                    f"Entry: ${tr['entry_price']:.4f}"
                )

                if tr.get('exit_idx') is not None:
                    x_ts = pd.to_datetime(tr['exit_time'])
                    x_pos_v = _to_pos(x_ts)
                    if 0 <= x_pos_v < N:
                        exit_xs.append(x_pos_v)
                        exit_ys.append(tr['exit_price'] * 1.003)   # 標在 bar 上方
                        # exit 顏色：賺=綠、賠=紅
                        if tr['pnl_pct'] > 0:
                            exit_colors.append('#3dbb6a')
                            color_emoji = '💰'
                        else:
                            exit_colors.append('#ff5555')
                            color_emoji = '🛑'
                        exit_texts.append(
                            f"<b>{color_emoji} 出場</b><br>"
                            f"{x_ts.strftime('%Y-%m-%d %H:%M')}<br>"
                            f"Exit: ${tr['exit_price']:.4f}<br>"
                            f"P/L: {tr['pnl_pct']:+.2f}%<br>"
                            f"Reason: {tr['exit_reason']}<br>"
                            f"Holding: {tr['holding_bars']} bars"
                        )
                        # 連線
                        line_color = '#3dbb6a' if tr['pnl_pct'] > 0 else '#ff5555'
                        line_segments_x.extend([e_pos, x_pos_v, None])
                        line_segments_y.extend([
                            tr['entry_price'], tr['exit_price'], None,
                        ])
                        line_colors.append(line_color)
                else:
                    # 持倉中（open）
                    pass
            except Exception:
                continue

        # 加連線（每筆交易一段）— 使用 None 分隔
        if line_segments_x:
            fig.add_trace(go.Scatter(
                x=line_segments_x, y=line_segments_y, mode='lines',
                line=dict(color='#7a8899', width=1.2, dash='dot'),
                name='戰法 P/L 路徑',
                hoverinfo='skip',
                showlegend=False,
                opacity=0.5,
            ), row=1, col=1)

        if entry_xs:
            fig.add_trace(go.Scatter(
                x=entry_xs, y=entry_ys, mode='markers',
                name=f'🟢 戰法進場 ({len(entry_xs)})',
                marker=dict(
                    symbol='circle', size=14,
                    color='#3dbb6a',
                    line=dict(color='#0a4a14', width=2),
                ),
                text=entry_texts, hoverinfo='text',
            ), row=1, col=1)
        if exit_xs:
            fig.add_trace(go.Scatter(
                x=exit_xs, y=exit_ys, mode='markers',
                name=f'💰 戰法出場 ({len(exit_xs)})',
                marker=dict(
                    symbol='x', size=14,
                    color=exit_colors,
                    line=dict(color='#000000', width=1.5),
                ),
                text=exit_texts, hoverinfo='text',
            ), row=1, col=1)

    # ── 🆕 v9.41：加碼 marker（黃色星形 — EMA5 反轉）──
    if reentry_events:
        re_xs, re_ys, re_texts = [], [], []
        for ev in reentry_events:
            try:
                ts = pd.to_datetime(ev['time'])
                pos = _to_pos(ts)
                if pos < 0 or pos >= N:
                    continue
                re_xs.append(pos)
                re_ys.append(ev['price'] * 1.008)
                re_texts.append(
                    f"<b>🔄 加碼: EMA5 反轉</b><br>"
                    f"{ts.strftime('%Y-%m-%d %H:%M')}<br>"
                    f"${ev['price']:.4f}<br>"
                    f"建議 33% 部位"
                )
            except Exception:
                continue
        if re_xs:
            fig.add_trace(go.Scatter(
                x=re_xs, y=re_ys, mode='markers',
                name=f'🔄 加碼點 ({len(re_xs)})',
                marker=dict(
                    symbol='star', size=12,
                    color='#f0c030',
                    line=dict(color='#aa7700', width=1.5),
                ),
                text=re_texts, hoverinfo='text',
            ), row=1, col=1)

    # ── MACD subplot（row 2，如果有）──
    if _has_macd:
        macd_line, signal_line, hist = macd_plot
        macd_vals = macd_line.values
        signal_vals = signal_line.values
        hist_vals = hist.values

        # Histogram 4 色
        hist_colors = []
        for i, v in enumerate(hist_vals):
            if v >= 0:
                prev_v = hist_vals[i-1] if i > 0 else v
                hist_colors.append('#26a69a' if v >= prev_v else '#88c8a8')
            else:
                prev_v = hist_vals[i-1] if i > 0 else v
                hist_colors.append('#ef5350' if v <= prev_v else '#e89090')

        fig.add_trace(go.Bar(
            x=x_pos, y=hist_vals,
            name='MACD Hist',
            marker=dict(color=hist_colors),
            opacity=0.6,
            text=[
                f"<b>{ts}</b><br>Hist: {h:+.4f}"
                for ts, h in zip(ts_strs, hist_vals)
            ],
            hoverinfo='text',
            showlegend=False,
        ), row=2, col=1)

        fig.add_trace(go.Scatter(
            x=x_pos, y=macd_vals, mode='lines',
            name='MACD',
            line=dict(color='#3b9eff', width=1.5),
            hovertemplate='MACD: %{y:.4f}<extra></extra>',
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=x_pos, y=signal_vals, mode='lines',
            name='Signal',
            line=dict(color='#ff6b35', width=1.3),
            hovertemplate='Signal: %{y:.4f}<extra></extra>',
        ), row=2, col=1)
        # 零軸虛線
        fig.add_hline(y=0, line=dict(color='#7a8899', width=0.7, dash='dot'),
                       row=2, col=1, opacity=0.5)

        # MACD 交叉標示（v9.32 暫停）— 改用 K 線圖上的 EMA20/60 交叉作為主訊號
        # 若要恢復可解除此段註解

    # ── Volume bar chart（row 2 or 3）──
    vol_row = 3 if _has_macd else 2
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
    ), row=vol_row, col=1)

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
        title=dict(
            text=title or '',
            font=dict(size=13, color=font_color),
            x=0.5, xanchor='center',
            y=0.985, yanchor='top',
        ),
        height=820 if _has_macd else 680,    # MACD 多一列加高
        hovermode='x unified',
        plot_bgcolor=bg,
        paper_bgcolor=paper_bg,
        font=dict(color=font_color, size=11),
        legend=dict(
            orientation='h',
            y=-0.13 if _has_macd else -0.16,
            x=0.5, xanchor='center', yanchor='top',
            bgcolor='rgba(0,0,0,0)', font=dict(size=10),
        ),
        margin=dict(l=55, r=30, t=45, b=110),
        xaxis_rangeslider_visible=False,
        dragmode='pan',
    )
    # x-axis tick labels — 套到所有 row（最後一個 row 顯示，其他隱藏）
    last_row = 3 if _has_macd else 2
    for row in range(1, last_row + 1):
        fig.update_xaxes(
            tickmode='array', tickvals=tick_pos, ticktext=tick_labels,
            gridcolor=grid_color, showgrid=True,
            showticklabels=(row == last_row),   # 只最後一列顯示 tick label
            row=row, col=1,
        )
    fig.update_yaxes(gridcolor=grid_color, title_text='Price', row=1, col=1)
    if _has_macd:
        fig.update_yaxes(gridcolor=grid_color, title_text='MACD', row=2, col=1)
        fig.update_yaxes(gridcolor=grid_color, title_text='Volume', row=3, col=1)
    else:
        fig.update_yaxes(gridcolor=grid_color, title_text='Volume', row=2, col=1)

    return fig
