"""即時波段盯盤 — Stock001 v9.26

特性：
  - 30 秒自動刷新（用 streamlit-autorefresh）
  - 多 ticker dashboard，每檔 2 pane（Daily + 5m intraday）
  - VCP / W底 / M頂 即時偵測 + Pivot/Stop/Target 標註
  - VWAP / SMA20 / SMA50 overlay
  - 進場/出場 signal lights
  - 資料源：yfinance 1m/5m（free），預留 Alpaca hook

操作建議：
  - watchlist 控制在 3-9 檔（chart 渲染密集）
  - 美股盤前 21:00 / 盤中 22:30 ~ 04:00 台北時間 開啟最有效
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
from pathlib import Path
import json


st.set_page_config(
    page_title='即時盯盤 — Stock001',
    page_icon='📊',
    layout='wide',
)


# ─────────── Auto-refresh ───────────
try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except Exception:
    HAS_AUTOREFRESH = False


# ─────────── Header ───────────
st.markdown(
    '<div style="background:linear-gradient(90deg,#1a2a4a,#0a1a3a);'
    'padding:10px 18px;border-left:4px solid #4488ff;border-radius:6px">'
    '<div style="font-size:1.3rem;font-weight:800;color:#7accff">📊 即時波段盯盤</div>'
    '<div style="font-size:.78rem;color:#9bb">'
    '即時 VCP / W底 / 進出場訊號 — 為波段交易進場與出場時機提供秒級確認'
    '</div></div>',
    unsafe_allow_html=True,
)


# ─────────── Sidebar 設定 ───────────
with st.sidebar:
    st.subheader('⚙️ 設定')

    # Watchlist
    default_wl = 'NVDA,AAPL,TSLA,MU,AMD'
    wl_text = st.text_input(
        '盯盤清單（逗號分隔，3-9 檔最佳）',
        value=st.session_state.get('rtswing_wl', default_wl),
        help='美股代碼，例如：NVDA,TSLA,MU'
    )
    st.session_state['rtswing_wl'] = wl_text

    # 從 watchlists.json 載入快捷
    _wl_path = Path(__file__).parent.parent / 'watchlists.json'
    if _wl_path.exists():
        try:
            with open(_wl_path, encoding='utf-8') as f:
                wl_dict = json.load(f)
            wl_options = ['(無 — 用上方輸入)'] + sorted(wl_dict.keys())
            picked = st.selectbox('或選自選清單', wl_options, index=0)
            if picked != '(無 — 用上方輸入)':
                wl_text = wl_dict[picked]
                if isinstance(wl_text, str):
                    wl_text = ','.join(t.strip() for t in wl_text.replace('\n', ',').split(',') if t.strip())
                st.session_state['rtswing_wl'] = wl_text
        except Exception:
            pass

    # Refresh rate
    refresh_sec = st.select_slider('自動刷新秒數',
                                    options=[0, 30, 60, 120, 300],
                                    value=30,
                                    format_func=lambda x: '🛑 手動' if x == 0 else f'{x}s')

    # Intraday timeframe
    intraday_tf = st.selectbox('Intraday 時間框架',
                                 ['1m', '5m', '15m', '1h'],
                                 index=1)

    # Daily lookback
    daily_days = st.slider('Daily 視窗（天）', 30, 200, 80, step=10)

    # Manual refresh
    if st.button('🔄 立即刷新', use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─────────── Auto refresh ───────────
if refresh_sec > 0 and HAS_AUTOREFRESH:
    st_autorefresh(interval=refresh_sec * 1000, key='rtswing_autorefresh')


# ─────────── Tickers ───────────
tickers = [t.strip().upper() for t in wl_text.split(',') if t.strip()]
if not tickers:
    st.info('請在左側設定盯盤清單')
    st.stop()


# ─────────── Cached loaders ───────────
@st.cache_data(ttl=30, show_spinner=False)
def _load_daily(ticker, days):
    from realtime.intraday_loader import load_daily
    return load_daily(ticker, days=days)


@st.cache_data(ttl=30, show_spinner=False)
def _load_intraday(ticker, interval):
    from realtime.intraday_loader import load_intraday
    return load_intraday(ticker, interval=interval, days=2)


@st.cache_data(ttl=30, show_spinner=False)
def _compute_signal(ticker, _daily_hash, _intraday_hash, daily_records, intraday_records):
    """signal 計算（不直接吃 DataFrame，否則 cache key 太大）"""
    from realtime.signals import compute_full_signal
    daily_df = pd.DataFrame(daily_records) if daily_records else None
    if daily_df is not None and len(daily_df) > 0:
        if 'date' in daily_df.columns:
            daily_df['date'] = pd.to_datetime(daily_df['date'])
            daily_df = daily_df.set_index('date')
    intraday_df = pd.DataFrame(intraday_records) if intraday_records else None
    if intraday_df is not None and len(intraday_df) > 0:
        if 'date' in intraday_df.columns:
            intraday_df['date'] = pd.to_datetime(intraday_df['date'])
            intraday_df = intraday_df.set_index('date')
    return compute_full_signal(daily_df, intraday_df, ticker=ticker)


# ─────────── Render each ticker ───────────
SIGNAL_COLORS = {
    'BUY':       ('#3dbb6a', '#0a2a14', '🟢 BUY'),
    'WAIT':      ('#e8a020', '#1a1408', '🟡 WAIT'),
    'OVERHEAT':  ('#ff5555', '#1a0808', '🔴 OVERHEAT'),
    'NO_SIGNAL': ('#7a8899', '#0a1422', '⚪ NO SIG'),
    'HOLD':      ('#3dbb6a', '#0a2a14', '🟢 HOLD'),
    'PARTIAL':   ('#e8a020', '#1a1408', '🟡 PARTIAL'),
    'EXIT_ALL':  ('#ff5555', '#1a0808', '🔴 EXIT'),
}


for ticker in tickers:
    with st.container():
        # Load data
        daily_df = _load_daily(ticker, daily_days)
        intraday_df = _load_intraday(ticker, intraday_tf)

        if daily_df is None or len(daily_df) == 0:
            st.warning(f'❌ {ticker} — daily 資料無法載入')
            continue

        # Compute signal
        from realtime.signals import compute_full_signal
        signal = compute_full_signal(daily_df, intraday_df, ticker=ticker)
        if 'error' in signal:
            st.warning(f'❌ {ticker} — {signal["error"]}')
            continue

        # Header row
        entry = signal['entry_signal']
        exit_s = signal['exit_signal']
        e_color, e_bg, e_label = SIGNAL_COLORS.get(entry, SIGNAL_COLORS['NO_SIGNAL'])
        x_color, x_bg, x_label = SIGNAL_COLORS.get(exit_s, SIGNAL_COLORS['HOLD'])

        change_color = '#3dbb6a' if signal['change_pct_today'] >= 0 else '#ff5555'

        header_html = (
            f'<div style="background:#0a1a2a;border-left:4px solid {e_color};'
            f'padding:8px 12px;border-radius:6px;margin-top:14px">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px">'
            f'<div>'
            f'<span style="font-size:1.2rem;font-weight:800;color:#eef">{ticker}</span>'
            f'<span style="margin-left:14px;font-size:1.1rem;font-weight:700;color:#eef">'
            f'${signal["price"]:.2f}'
            f'</span>'
            f'<span style="margin-left:6px;font-size:.95rem;color:{change_color}">'
            f'{signal["change_pct_today"]:+.2f}%</span>'
            f'</div>'
            f'<div>'
            f'<span style="background:{e_bg};color:{e_color};padding:2px 8px;border-radius:4px;'
            f'font-weight:700;font-size:.85rem;margin-right:6px">{e_label}</span>'
            f'<span style="background:{x_bg};color:{x_color};padding:2px 8px;border-radius:4px;'
            f'font-weight:700;font-size:.85rem">{x_label}</span>'
            f'</div>'
            f'</div>'
            f'</div>'
        )
        st.markdown(header_html, unsafe_allow_html=True)

        # Key levels row
        cols = st.columns(5)
        with cols[0]:
            st.metric('Pivot',
                       f'${signal["pivot_price"]:.2f}' if signal.get('pivot_price') else '—')
        with cols[1]:
            st.metric('Stop',
                       f'${signal["stop_loss"]:.2f}' if signal.get('stop_loss') else '—',
                       delta=f'{((signal["stop_loss"]-signal["price"])/signal["price"]*100):.1f}%' if signal.get('stop_loss') else None)
        with cols[2]:
            st.metric('Target',
                       f'${signal["target_price"]:.2f}' if signal.get('target_price') else '—',
                       delta=f'{((signal["target_price"]-signal["price"])/signal["price"]*100):.1f}%' if signal.get('target_price') else None)
        with cols[3]:
            st.metric('VWAP',
                       f'${signal["vwap"]:.2f}' if signal.get('vwap') else '—')
        with cols[4]:
            st.metric('RSI',
                       f'{signal["rsi"]:.0f}' if signal.get('rsi') else '—')

        # Reasons
        if signal.get('reasons'):
            st.markdown(
                '<div style="font-size:.78rem;color:#9cf;margin:2px 0 6px 0">'
                + '<br>'.join(signal['reasons']) +
                '</div>',
                unsafe_allow_html=True
            )

        # Chart
        try:
            from realtime.charts import make_chart_data_uri
            uri = make_chart_data_uri(daily_df, intraday_df, signal, ticker=ticker)
            if uri:
                st.markdown(
                    f'<img src="{uri}" style="width:100%;border-radius:6px"/>',
                    unsafe_allow_html=True
                )
            else:
                st.warning(f'{ticker} 圖表生成失敗')
        except Exception as e:
            st.warning(f'{ticker} chart err: {type(e).__name__}: {e}')

        # Pattern summary
        vcp_info = signal.get('vcp_info') or {}
        db_info = signal.get('db_info') or {}
        dt_info = signal.get('dt_info') or {}
        pat_lines = []
        if vcp_info.get('is_vcp'):
            pat_lines.append(
                f"📐 VCP-{vcp_info.get('vcp_grade','?')} "
                f"({vcp_info.get('num_contractions',0)} 收口, "
                f"{vcp_info.get('breakout_status','?')})"
            )
        if db_info.get('is_double_bottom'):
            pat_lines.append(
                f"🟢 W底-{db_info.get('quality_grade','?')} "
                f"({db_info.get('status','?')})"
            )
        if dt_info.get('is_double_top'):
            pat_lines.append(
                f"🔴 M頂-{dt_info.get('quality_grade','?')} "
                f"({dt_info.get('status','?')})"
            )
        if pat_lines:
            st.markdown(
                '<div style="font-size:.78rem;color:#aabbcc;margin-top:4px">'
                + ' ｜ '.join(pat_lines) +
                f'｜ last update {signal["last_update"]}'
                '</div>',
                unsafe_allow_html=True
            )


# ─────────── Footer ───────────
st.markdown(
    '<div style="margin-top:30px;padding:10px;background:#0a1422;'
    'border-radius:6px;font-size:.72rem;color:#7a8899">'
    '⚠️ <b>免責提醒</b>：本系統僅作技術訊號參考，不構成投資建議。<br>'
    '資料來源 yfinance（15-20 分鐘延遲，免費）。'
    '若需真即時請設定 Alpaca API key（ALPACA_API_KEY / ALPACA_SECRET_KEY）。<br>'
    '建議：盤中專注 3-5 檔；訊號出現後仍需配合大盤環境、量價、新聞綜合判斷。'
    '</div>',
    unsafe_allow_html=True
)
