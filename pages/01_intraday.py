"""Intraday Analysis Page — Stock001 v9.29
=============================================

Streamlit multi-page：與 tv_app.py 共享 sidebar，但獨立操作。
網址會自動變成 /Intraday（從檔名 01_intraday → "Intraday"）

用法：
  streamlit run tv_app.py
  → 側邊欄會看到 "Intraday" 選項，點進來就是本頁
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from intraday.config import TIMEFRAMES, get_tf_config
from intraday.data import get_intraday, market_info
from intraday.indicators import (
    vwap_session, orb_levels, floor_pivots_from_df,
    gap_metrics, relative_volume, add_standard_indicators,
)
from intraday.alignment import compute_mtf_state


st.set_page_config(page_title="Intraday 分析 | Stock001",
                    page_icon="⏱️", layout="wide")


# ─── 共享 session state（與 tv_app.py 通用）──
if 'selected_ticker_intraday' not in st.session_state:
    # 試從 tv_app 主頁的選股繼承
    inherited = st.session_state.get('current_ticker') or 'AAPL'
    st.session_state['selected_ticker_intraday'] = inherited


# ─── 頂部標頭 ──
st.title("⏱️ Intraday 多時間框架分析")
st.caption("選股 + 選週期 → 跨 5m / 15m / 1h / 1d 對齊狀態 + VWAP / ORB / Pivots / Stage")


# ─── 控制列 ──
c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
with c1:
    ticker_input = st.text_input(
        "股票代號（US: AAPL / TW: 2330）",
        value=st.session_state['selected_ticker_intraday'],
        help="不需加 .TW；自動判斷市場",
    )
    st.session_state['selected_ticker_intraday'] = ticker_input.strip().upper()
ticker = st.session_state['selected_ticker_intraday']

with c2:
    primary_tf = st.selectbox(
        "主圖週期",
        options=['1m', '5m', '15m', '30m', '1h', '1d'],
        index=1,
        help="主圖顯示用的 timeframe",
    )

with c3:
    show_mtf = st.checkbox("MTF 對齊", value=True,
                            help="顯示多時間框架彙總狀態")

with c4:
    if st.button("🔄 強制重抓"):
        # 清掉 cache 並重抓
        df_test = get_intraday(ticker, primary_tf, refresh=True)
        st.success(f"已重抓 {ticker} {primary_tf}: {len(df_test) if df_test is not None else 0} bars")


# ─── 市場 metadata ──
info = market_info(ticker)
st.markdown(
    f"**{info['ticker']}** · {('🇺🇸 US' if info['market']=='us' else '🇹🇼 TW')}"
    f" · session: `{info['session']}` · yf_symbol: `{info['yf_symbol']}`"
)

st.divider()


# ─── MTF 對齊面板 ──
if show_mtf:
    st.subheader("🎯 MTF 多時間框架對齊")
    with st.spinner(f"計算 {ticker} 的 5m / 15m / 1h / 1d 狀態..."):
        mtf = compute_mtf_state(ticker, timeframes=['5m', '15m', '1h', '1d'])

    summary = mtf.get('summary', {})
    align = summary.get('alignment', 'no_data')
    align_colors = {
        'bull_aligned': ('#3dbb6a', '🟢 全多頭對齊 — 最佳進場環境'),
        'bull_leaning': ('#7abadd', '🔵 多頭偏向'),
        'bear_aligned': ('#ff5555', '🔴 全空頭對齊 — 避免買進'),
        'bear_leaning': ('#e89090', '🟠 空頭偏向'),
        'mixed': ('#e8a020', '🟡 多空交雜 — 需更多訊號'),
        'no_data': ('#7a8899', '⚪ 資料不足'),
    }
    color, label = align_colors.get(align, ('#7a8899', align))
    best = summary.get('best_entry_tf')
    if best:
        label += f"  ｜建議進場觀察 timeframe: **{best}**"
    st.markdown(
        f"<div style='background:#0a1828;border-left:4px solid {color};"
        f"padding:10px;border-radius:4px;margin-bottom:10px'>"
        f"<b style='color:{color};font-size:1rem'>{label}</b>"
        f"<div style='font-size:.85rem;color:#a8c0d0;margin-top:4px'>"
        f"多頭 {summary.get('mtf_bullish_count',0)} / "
        f"空頭 {summary.get('mtf_bearish_count',0)} / "
        f"總 {summary.get('total_tf',0)} 個 timeframe"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    # 4 個 timeframe 並列
    cols = st.columns(4)
    for col, (tf, state) in zip(cols, mtf['by_tf'].items()):
        with col:
            if not state.get('ok'):
                st.error(f"**{tf}**: {state.get('reason','失敗')}")
                continue
            trend = state.get('trend_emoji', '⚪')
            close = state.get('close', 0)
            rsi = state.get('rsi', '-')
            adx = state.get('adx', '-')
            stage_info = state.get('stage')

            st.markdown(f"### {trend} **{tf}**")
            st.markdown(f"**${close:.2f}**")
            st.markdown(f"RSI: `{rsi}` ｜ ADX: `{adx}`")

            # VWAP
            if state.get('vwap'):
                vw = state['vwap']
                vw_pct = state.get('vs_vwap_pct', 0)
                vw_color = '#3dbb6a' if state.get('above_vwap') else '#ff5555'
                st.markdown(
                    f"<div style='font-size:.85rem;color:{vw_color}'>"
                    f"VWAP <b>${vw:.2f}</b> ({vw_pct:+.2f}%)</div>",
                    unsafe_allow_html=True)

            # ORB
            orb = state.get('orb', {})
            if orb.get('or_high'):
                pos = orb.get('current_position', '?')
                pos_emoji = {'above_high': '🚀', 'inside': '⏳',
                              'below_low': '⛔'}.get(pos, '?')
                st.markdown(
                    f"<div style='font-size:.78rem'>"
                    f"ORB {pos_emoji} H<b>${orb['or_high']:.2f}</b> L<b>${orb['or_low']:.2f}</b></div>",
                    unsafe_allow_html=True)

            # Stage
            if stage_info:
                sg = stage_info
                stage_color = {1: '#7abadd', 2: '#3dbb6a',
                                3: '#e8a020', 4: '#ff5555'}.get(sg['stage'], '#7a8899')
                st.markdown(
                    f"<div style='font-size:.78rem;color:{stage_color}'>"
                    f"Stage <b>{sg['stage']}</b> {sg['name']} ({sg['sub']})"
                    f"<br>斜率 {sg['slope']:+.2f}% ｜ 信心 {int(sg['confidence']*100)}%"
                    f"</div>",
                    unsafe_allow_html=True)

            # Pattern detection
            if state.get('cup'):
                c = state['cup']
                st.markdown(
                    f"<div style='font-size:.78rem;color:#7abadd'>"
                    f"☕ Cup {c['score']:.0f} ｜ pivot ${c['pivot']:.2f}</div>",
                    unsafe_allow_html=True)
            if state.get('flat'):
                f = state['flat']
                st.markdown(
                    f"<div style='font-size:.78rem;color:#3dbb6a'>"
                    f"🟨 Flat {f['score']:.0f} ｜ pivot ${f['pivot']:.2f}</div>",
                    unsafe_allow_html=True)

    st.divider()


# ─── 主圖 ──
st.subheader(f"📈 主圖：{ticker} {primary_tf}")
df_main = get_intraday(ticker, primary_tf)
if df_main is None or len(df_main) < 5:
    st.error(f"⚠️ 無法取得 {ticker} 的 {primary_tf} 資料")
    st.stop()

df_main = add_standard_indicators(df_main)

# ── 顯示最後 N 根 ──
n_show = st.slider("顯示最後 N 根 bar", min_value=50,
                    max_value=min(2000, len(df_main)),
                    value=min(300, len(df_main)),
                    step=50)
df_plot = df_main.iloc[-n_show:].copy()

# VWAP + 標準指標
df_plot['vwap'] = vwap_session(df_plot)

# ── plotly 互動圖 ──
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(rows=3, cols=1,
                          row_heights=[0.6, 0.2, 0.2],
                          shared_xaxes=True, vertical_spacing=0.02,
                          subplot_titles=(None, "Volume", "RSI"))

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df_plot.index,
        open=df_plot['Open'], high=df_plot['High'],
        low=df_plot['Low'], close=df_plot['Close'],
        name='Price',
        increasing_line_color='#3dbb6a',
        decreasing_line_color='#ff5555',
    ), row=1, col=1)

    # EMA
    for ema, color in [('e10', '#ffaa55'), ('e20', '#5dccdd'), ('e60', '#aa66ff')]:
        if ema in df_plot.columns:
            fig.add_trace(go.Scatter(
                x=df_plot.index, y=df_plot[ema],
                name=ema.upper(), line=dict(width=1.2, color=color),
            ), row=1, col=1)

    # VWAP
    if 'vwap' in df_plot.columns and df_plot['vwap'].notna().any():
        fig.add_trace(go.Scatter(
            x=df_plot.index, y=df_plot['vwap'],
            name='VWAP', line=dict(width=1.5, color='#ffff66', dash='dot'),
        ), row=1, col=1)

    # Bollinger Bands
    if 'bb_up' in df_plot.columns and 'bb_lo' in df_plot.columns:
        fig.add_trace(go.Scatter(
            x=df_plot.index, y=df_plot['bb_up'],
            name='BB Upper', line=dict(width=0.8, color='#888', dash='dash'),
            showlegend=False,
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df_plot.index, y=df_plot['bb_lo'],
            name='BB Lower', line=dict(width=0.8, color='#888', dash='dash'),
            fill='tonexty', fillcolor='rgba(120,120,120,0.08)',
            showlegend=False,
        ), row=1, col=1)

    # Pivots — 畫水平線（從第一根 bar 拉到最後）
    pivots = floor_pivots_from_df(df_main)
    if pivots:
        for level, color, dash in [('R2', '#ff7777', 'dot'),
                                     ('R1', '#ff7777', 'dash'),
                                     ('P',  '#aaaa66', 'solid'),
                                     ('S1', '#77dd77', 'dash'),
                                     ('S2', '#77dd77', 'dot')]:
            if level in pivots:
                fig.add_hline(y=pivots[level], line=dict(color=color, width=1, dash=dash),
                                annotation_text=f"{level}: ${pivots[level]:.2f}",
                                annotation_position="right",
                                annotation_font_size=10,
                                row=1, col=1)

    # ORB
    cfg = get_tf_config(primary_tf)
    if cfg.supports_orb:
        orb = orb_levels(df_main, minutes=30, tf_minutes=cfg.minutes_per_bar)
        if orb.get('or_high'):
            fig.add_hline(y=orb['or_high'],
                            line=dict(color='#66ffff', width=1.5, dash='dashdot'),
                            annotation_text=f"ORB H ${orb['or_high']:.2f}",
                            annotation_position='right',
                            annotation_font_size=10, row=1, col=1)
            fig.add_hline(y=orb['or_low'],
                            line=dict(color='#66ffff', width=1.5, dash='dashdot'),
                            annotation_text=f"ORB L ${orb['or_low']:.2f}",
                            annotation_position='right',
                            annotation_font_size=10, row=1, col=1)

    # Volume
    vol_colors = ['#3dbb6a' if c >= o else '#ff5555'
                   for c, o in zip(df_plot['Close'], df_plot['Open'])]
    fig.add_trace(go.Bar(
        x=df_plot.index, y=df_plot['Volume'],
        name='Volume', marker=dict(color=vol_colors), showlegend=False,
    ), row=2, col=1)

    # RSI
    if 'rsi' in df_plot.columns:
        fig.add_trace(go.Scatter(
            x=df_plot.index, y=df_plot['rsi'],
            name='RSI', line=dict(width=1.3, color='#ffaa66'),
            showlegend=False,
        ), row=3, col=1)
        fig.add_hline(y=70, line=dict(color='#ff5555', width=0.6, dash='dash'),
                        row=3, col=1)
        fig.add_hline(y=30, line=dict(color='#3dbb6a', width=0.6, dash='dash'),
                        row=3, col=1)

    fig.update_layout(
        height=720,
        xaxis_rangeslider_visible=False,
        template='plotly_dark',
        margin=dict(l=10, r=80, t=30, b=10),
        legend=dict(orientation='h', y=1.02, x=0),
        hovermode='x unified',
    )
    fig.update_yaxes(title_text=None, row=1, col=1)
    fig.update_yaxes(title_text="Vol", row=2, col=1, showgrid=False)
    fig.update_yaxes(title_text="RSI", row=3, col=1,
                      range=[0, 100], showgrid=False)

    st.plotly_chart(fig, use_container_width=True)
except ImportError:
    st.warning("⚠️ plotly 未安裝（pip install plotly），改用簡易表格顯示")
    st.dataframe(df_plot[['Open', 'High', 'Low', 'Close', 'Volume',
                            'rsi', 'adx', 'vwap']].tail(50),
                  use_container_width=True)


# ─── 指標摘要 ──
st.divider()
st.subheader("📊 當前指標快照")

last = df_main.iloc[-1]
cols = st.columns(6)
metrics = [
    ('Close', f"${last['Close']:.2f}"),
    ('EMA20', f"${last.get('e20', 0):.2f}" if 'e20' in df_main else '-'),
    ('EMA60', f"${last.get('e60', 0):.2f}" if 'e60' in df_main else '-'),
    ('RSI', f"{last.get('rsi', 0):.1f}" if 'rsi' in df_main else '-'),
    ('ADX', f"{last.get('adx', 0):.1f}" if 'adx' in df_main else '-'),
    ('ATR', f"{last.get('atr', 0):.2f}" if 'atr' in df_main else '-'),
]
for col, (label, value) in zip(cols, metrics):
    col.metric(label, value)


# ─── ORB / Gap / Relative Volume ──
c1, c2, c3 = st.columns(3)
cfg = get_tf_config(primary_tf)
if cfg.supports_orb:
    orb = orb_levels(df_main, minutes=30, tf_minutes=cfg.minutes_per_bar)
    with c1:
        st.markdown("#### 📐 Opening Range (30min)")
        if orb.get('or_high'):
            pos = orb.get('current_position', '?')
            pos_label = {'above_high': '🚀 上軌突破',
                          'inside': '⏳ 區間內',
                          'below_low': '⛔ 下軌跌破'}.get(pos, '?')
            st.markdown(f"**{pos_label}**")
            st.markdown(f"- 上軌: `${orb['or_high']:.2f}`")
            st.markdown(f"- 下軌: `${orb['or_low']:.2f}`")
            st.markdown(f"- 持續: {orb['bars_used']} bars")
        else:
            st.markdown("_今日尚未產生 ORB_")

if cfg.minutes_per_bar < 390:
    gap = gap_metrics(df_main)
    with c2:
        st.markdown("#### 📈 跳空缺口")
        if gap.get('gap_type') != 'none':
            sign = '🟢' if gap['gap_type'] == 'up' else '🔴'
            fill = '✅ 已回補' if gap.get('is_filled') else '❌ 未回補'
            st.markdown(f"**{sign} {gap['gap_type'].upper()} {gap['gap_pct']:+.2f}%**")
            st.markdown(f"- 前日收: `${gap['prev_close']:.2f}`")
            st.markdown(f"- 今日開: `${gap['today_open']:.2f}`")
            st.markdown(f"- 狀態: {fill}")
        else:
            st.markdown("_無明顯跳空（< 0.3%）_")

    rv = relative_volume(df_main, lookback_sessions=20)
    with c3:
        st.markdown("#### 🔊 相對量（同時段）")
        if rv is not None:
            if rv >= 2.0:
                st.markdown(f"**🔥 {rv:.2f}x — 爆量**")
            elif rv >= 1.3:
                st.markdown(f"**⚡ {rv:.2f}x — 放量**")
            elif rv < 0.7:
                st.markdown(f"**💧 {rv:.2f}x — 量縮**")
            else:
                st.markdown(f"**⚪ {rv:.2f}x — 正常**")
            st.caption("vs 近 20 個交易日同時段平均")
        else:
            st.markdown("_資料不足_")


# ─── Pivots 表格 ──
st.divider()
pivots = floor_pivots_from_df(df_main)
if pivots:
    st.markdown("#### 🎯 Floor Pivot Points（依前一日 HLC）")
    pivot_df = pd.DataFrame({
        'Level': ['R3', 'R2', 'R1', 'P', 'S1', 'S2', 'S3'],
        'Price': [pivots.get(k) for k in ['R3', 'R2', 'R1', 'P', 'S1', 'S2', 'S3']],
        'vs Close': [
            f"{(pivots.get(k) / last['Close'] - 1) * 100:+.2f}%" if pivots.get(k) else '-'
            for k in ['R3', 'R2', 'R1', 'P', 'S1', 'S2', 'S3']
        ],
    })
    st.dataframe(pivot_df, hide_index=True, use_container_width=False)


st.caption(
    f"Stock001 v9.29 Intraday Module ｜ Last bar: `{df_main.index[-1]}` "
    f"｜ Total bars in cache: {len(df_main)}"
)
