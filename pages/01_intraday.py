"""Intraday Analysis Page — Stock001 v9.30
=============================================

每個 timeframe 一個 tab，內容**完全等同 tv_app 個股詳細卡**（用同一套
detail_card_render module 渲染），唯一差別是底層資料是該 TF 的 bar。

啟動：
  streamlit run tv_app.py
  → 側邊欄選 "intraday"
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from intraday.config import TIMEFRAMES, get_tf_config
from intraday.data import get_intraday, market_info
from intraday.builder import build_d_from_intraday
from detail_card_render import (
    GROUP_NAMES, GROUP_WEIGHTS, GROUP_COLORS,
    TREND_W, POSITION_W, MOMENTUM_W, AUX_W,
    judge_trend, judge_position, judge_momentum, judge_aux,
    calc_summary, _calc_aux_summary, compute_momentum_grade,
    _rec, apply_cap, badge, get_rec_label, render_detail,
)


st.set_page_config(page_title="Intraday 個股詳細 | Stock001",
                    page_icon="⏱️", layout="wide")


# 共享 session state
if 'selected_ticker_intraday' not in st.session_state:
    inherited = st.session_state.get('current_ticker') or 'AAPL'
    st.session_state['selected_ticker_intraday'] = inherited


# ── tv_app CSS（讓 ind-item/ind-grid/badge 樣式生效）──
st.markdown("""
<style>
/* — ind-grid（與 tv_app render_detail 同樣 class）— */
.ind-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 6px;
  margin-bottom: 8px;
}
.ind-item {
  background: #0a1628;
  border: 1px solid #1a2f48;
  border-radius: 6px;
  padding: 7px 10px;
}
.ind-item.ind-buy   { border-color: #1a4a80; background: #08152a; }
.ind-item.ind-sell  { border-color: #6a1a1a; background: #1a0808; }
.ind-item.ind-neu   { border-color: #1a2f48; }
.ind-label {
  display: block;
  font-size: .68rem;
  color: #7ab0d0;
  margin-bottom: 3px;
}
.ind-val {
  font-size: .9rem;
  color: #e8f4fd;
  font-family: 'IBM Plex Mono', 'SF Mono', Consolas, monospace;
  font-weight: 600;
}
/* — badge — */
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: .7rem;
  font-weight: 700;
  margin: 0 4px;
}
.badge-strong-buy    { background:#0D47A1; color:#60CFFF; }
.badge-buy           { background:#0D2E50; color:#60B3FF; }
.badge-buy-limit     { background:#3A2A00; color:#F0C030; }
.badge-strong-sell   { background:#4A0A0A; color:#FF6B6B; }
.badge-sell          { background:#3B0D0D; color:#FF8080; }
.badge-overheat      { background:#3A1800; color:#FF8830; }
.badge-bearish       { background:#3A0808; color:#FF5555; }
.badge-neutral       { background:#1A2030; color:#9AAABB; }
</style>
""", unsafe_allow_html=True)


# ── 頂部標題 + 控制 ──
st.title("⏱️ Intraday 個股詳細指標（多時間框架）")
st.caption("與 tv_app 主頁同樣的 4 群指標 + 操作建議邏輯，唯一差別是底層 bar 為該 TF（1m/5m/15m/30m/1h/1d）")

c1, c2, c3 = st.columns([3, 2, 1])
with c1:
    ticker_input = st.text_input(
        "股票代號",
        value=st.session_state['selected_ticker_intraday'],
        help="US: AAPL / TW: 2330（不需 .TW）",
    )
    st.session_state['selected_ticker_intraday'] = ticker_input.strip().upper()
ticker = st.session_state['selected_ticker_intraday']

with c2:
    timeframes_selected = st.multiselect(
        "Timeframes",
        options=['1m', '5m', '15m', '30m', '1h', '1d'],
        default=['5m', '15m', '1h', '1d'],
    )

with c3:
    if st.button("🔄 重抓所有 TF"):
        for tf in timeframes_selected:
            get_intraday(ticker, tf, refresh=True)
        st.success("已重抓")
        st.rerun()

if not timeframes_selected:
    st.warning("至少選一個 timeframe")
    st.stop()


# ── 市場 metadata ──
info = market_info(ticker)
st.markdown(
    f"**{info['ticker']}** · {('🇺🇸 US' if info['market']=='us' else '🇹🇼 TW')}"
    f" · session: `{info['session_hours']}` · yf_symbol: `{info['yf_symbol']}`"
)

st.divider()


# ── 每個 TF 一個 tab ──
tabs = st.tabs([f"⏱️ {tf}" for tf in timeframes_selected])

for tab, tf in zip(tabs, timeframes_selected):
    with tab:
        with st.spinner(f"計算 {ticker} @ {tf}..."):
            df = get_intraday(ticker, tf, market=info['market'])
            if df is None or len(df) < 30:
                st.error(f"⚠️ {tf}: 資料不足或抓取失敗（bars={len(df) if df is not None else 0}）")
                continue

            d = build_d_from_intraday(df, tf=tf, ticker=ticker, market=info['market'])

            if d.get('_error'):
                st.error(f"⚠️ {tf}: {d['_error']}")
                continue

            # 顯示 TF metadata
            cfg = get_tf_config(tf)
            st.caption(
                f"📊 **{tf}** ｜ {len(df)} bars ｜ "
                f"每根 {cfg.minutes_per_bar} 分鐘 ｜ "
                f"last bar: `{d.get('_intraday_last_ts', '?')}` "
                f"｜ 週線 resample: {len(df)} bars / {cfg.bars_per_day*5:.0f} bar/week"
            )

            # 警告短期 TF 不適用某些指標
            warnings_list = []
            if not cfg.supports_stage:
                warnings_list.append("Stage 分析 (30W SMA) 對 " + tf + " 無意義 → 顯示但不要過度解讀")
            if not cfg.supports_sepa:
                warnings_list.append("SEPA Template (52w 高低) 對 " + tf + " 無意義")
            if d.get('w_close') is None:
                warnings_list.append("週線資料不足（< 20 週）→ 週線結構欄位會顯示 N/A")
            if warnings_list:
                st.warning("⚠️ TF 適用性提示：" + " ｜ ".join(warnings_list))

        # 跑完整 tv_app 詳細卡渲染流程
        try:
            gt = judge_trend(d)
            gp = judge_position(d)
            gm = judge_momentum(d)
            ga = judge_aux(d)
            ts = calc_summary(gt, TREND_W)
            ps = calc_summary(gp, POSITION_W)
            ms_b, ms_s, ms_n, _ = calc_summary(gm, MOMENTUM_W)
            mg = compute_momentum_grade(d)
            ms = (ms_b, ms_s, ms_n, mg)
            xs = _calc_aux_summary(ga, AUX_W)
            tb = round(ts[0] + ps[0] + ms[0] + xs[0], 1)
            ts_ = round(ts[1] + ps[1] + ms[1] + xs[1], 1)
            tn_ = round(ts[2] + ps[2] + ms[2] + xs[2], 1)
            verdict_raw = _rec(tb, ts_)
            verdict, cap = apply_cap(verdict_raw, d, mg)
            groups = (gt, gp, gm, ga)
            summs = (ts, ps, ms, xs)
            tsumm = (tb, ts_, tn_, verdict)

            # ── ④ 推薦策略 label（tv_app 表格欄那個小 badge）──
            rec_label, rec_style = get_rec_label(d, ticker=ticker)
            st.markdown(
                f'<div style="display:inline-block;{rec_style};'
                f'padding:5px 12px;border-radius:5px;font-size:.85rem;'
                f'font-weight:700;margin-bottom:8px">'
                f'④ 推薦策略：{rec_label}'
                f'</div>',
                unsafe_allow_html=True)

            # ── 整體 verdict badge ──
            cap_html = (
                f'<span style="color:#aa6655;font-size:.72rem;margin-left:6px">{cap}</span>'
                if cap else ''
            )
            st.markdown(
                f'<div style="margin:6px 0;font-size:.95rem">'
                f'<span style="color:#7ab0d0">{tf} 綜合判讀：</span>'
                f'{badge(verdict)}'
                f'<span style="color:#7a8899;font-size:.7rem;margin-left:8px">'
                f'(買 {tb} ｜ 賣 {ts_} ｜ 中 {tn_})</span>'
                f'{cap_html}'
                f'</div>',
                unsafe_allow_html=True)

            # ── 完整 detail card（concepts/advice/news 都 disable，因為這些跨 TF 無意義）──
            # 直接呼叫 detail_card_render 版本，不走 tv_app wrapper（沒帶 advice/news callbacks）
            html = render_detail(
                ticker, d, groups, summs, tsumm, cap,
                market=info['market'],
                advice_fn=None,        # intraday 不顯示操作建議（暫定）
                news_fn=None,          # 新聞不分 TF
                concepts_fn=None,      # 概念股不分 TF
            )
            st.markdown(html, unsafe_allow_html=True)

        except Exception as e:
            import traceback
            st.error(f"渲染失敗：{type(e).__name__}: {e}")
            st.code(traceback.format_exc())


st.divider()
st.caption(
    f"Stock001 v9.30 ｜ {ticker} ｜ TFs: {', '.join(timeframes_selected)} ｜ "
    f"使用 detail_card_render module（與 tv_app 主頁 100% 相同邏輯）"
)
