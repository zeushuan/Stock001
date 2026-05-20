"""Day-Trade 工具台 — Stock001 v9.50
=====================================
五合一當沖輔助工具，把「無情緒交易」落地成可操作流程：

  A · 戰法訊號面板   掃描當沖標的 v9.38 戰法進出場狀態 ＋ 即時 Zigzag 圖
  B · 交易卡產生器   輸入進場價/停損，自動算部位大小與 R 倍數目標
  C · 日內熔斷監控   交易日誌 ＋ 連虧 / 次數 / 虧損 三重熔斷
  D · SOXS 5/19 案例 訊號 vs 情緒 視覺化教學
  E · 多週期總覽     兩檔股票 × 全週期 ZigZag 網格，一眼掃方向

資料源：Twelve Data Free（抓不到時自動 fallback Alpaca IEX）；其他頁仍用 Alpaca。
啟動：streamlit run tv_app.py → 側邊欄選 "daytrade"
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from intraday.data import get_intraday
from intraday.strategy import detect_swing_signal
from intraday.charts import build_zigzag_chart_plotly

ET = ZoneInfo('America/New_York')
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="Day-Trade 工具台 | Stock001",
                   page_icon="⚡", layout="wide")


# ════════════════════════════════════════════════════════════════
# 共用 helper
# ════════════════════════════════════════════════════════════════

def _fmt_price(p):
    """價格格式化：< $10 顯示 4 位小數，否則 2 位。"""
    if p is None:
        return '—'
    try:
        p = float(p)
    except Exception:
        return '—'
    return f'${p:.4f}' if p < 10 else f'${p:.2f}'


def get_session_phase():
    """依當前 ET 時間回傳美股盤別與操作建議。"""
    now = datetime.now(ET)
    if now.weekday() >= 5:        # 5=Sat 6=Sun
        return {'emoji': '🛌', 'name': '週末休市',
                'advice': '美股週末不交易，週一 09:30 ET 再開盤。可先擬定觀察名單。',
                'tone': 'off', 'now': now}
    t = now.time()
    if t < dtime(9, 30):
        return {'emoji': '🌙', 'name': '盤前',
                'advice': '美股尚未開盤，等待 09:30 ET。先把觀察名單與交易計畫準備好。',
                'tone': 'off', 'now': now}
    if t < dtime(10, 0):
        return {'emoji': '⚡', 'name': '開盤波動區 09:30–10:00',
                'advice': '波動最劇烈、假突破最多。建議只觀察、不搶開盤第一根。',
                'tone': 'caution', 'now': now}
    if t < dtime(11, 30):
        return {'emoji': '🟢', 'name': '早盤趨勢區 10:00–11:30',
                'advice': '趨勢方向漸明朗 — 戰法進場訊號可信度最高的黃金時段。',
                'tone': 'good', 'now': now}
    if t < dtime(14, 0):
        return {'emoji': '😴', 'name': '午盤盤整區 11:30–14:00',
                'advice': '量縮盤整、假訊號偏多。建議減少交易、嚴設停損。',
                'tone': 'caution', 'now': now}
    if t < dtime(15, 30):
        return {'emoji': '🔵', 'name': '尾盤趨勢區 14:00–15:30',
                'advice': '趨勢常延續或反轉 — 可順勢操作，但隨時準備收盤。',
                'tone': 'good', 'now': now}
    if t < dtime(16, 0):
        return {'emoji': '🔔', 'name': '收盤清倉區 15:30–16:00',
                'advice': '準備平倉，避免留倉跳空風險。日內單不過夜。',
                'tone': 'caution', 'now': now}
    return {'emoji': '🌃', 'name': '盤後',
            'advice': '美股已收盤（16:00 ET）。是覆盤、整理交易日誌的時間。',
            'tone': 'off', 'now': now}


def is_rth() -> bool:
    """現在是否為美股常規交易時段（09:30–16:00 ET，週一至週五）。"""
    now = datetime.now(ET)
    return now.weekday() < 5 and dtime(9, 30) <= now.time() < dtime(16, 0)


def _last_utc(idx) -> pd.Timestamp:
    """intraday df 的最後一根 bar 時間 → tz-aware UTC Timestamp。"""
    t = pd.Timestamp(idx[-1])
    return t.tz_localize('UTC') if t.tz is None else t.tz_convert('UTC')


def _index_to_et(idx):
    """intraday df 的 index（naive UTC）→ naive ET，供圖表 x 軸顯示。"""
    if idx.tz is None:
        return idx.tz_localize('UTC').tz_convert(ET).tz_localize(None)
    return idx.tz_convert(ET).tz_localize(None)


_ENTRY_VIEW = {
    'ENTER':      ('🟢', '進場訊號成立'),
    'WAIT_BB_P1': ('⏳', '待 BB+1σ 觸發'),
    'NO_SETUP':   ('⚪', '無進場 setup'),
}
_EXIT_VIEW = {
    'EXIT':       ('🔴', '出場訊號成立'),
    'WARN_PRICE': ('⚠️', '跌破中軌警戒'),
    'WARN_EMA':   ('⚠️', 'EMA 下行警戒'),
    'HOLD':       ('⚪', '持有 / 觀望'),
}

# 多週期總覽卡片配色 — 依戰法訊號（進場綠 / 出場紅 / 其餘原色）
_GRID_SIG_STYLE = {
    ('enter', 'dark'):  {'paper': '#0c3a1e', 'plot': '#0d2e1a',
                         'capbg': '#0d3a1e', 'captx': '#d6f0df', 'tag': '🟢 進場'},
    ('enter', 'light'): {'paper': '#d9f2e1', 'plot': '#e8f7ee',
                         'capbg': '#c4ead0', 'captx': '#0a5a28', 'tag': '🟢 進場'},
    ('exit', 'dark'):   {'paper': '#3a1212', 'plot': '#2e1010',
                         'capbg': '#3a1212', 'captx': '#ffd5d5', 'tag': '🔴 出場'},
    ('exit', 'light'):  {'paper': '#ffd9d9', 'plot': '#ffecec',
                         'capbg': '#ffc6c6', 'captx': '#7a1212', 'tag': '🔴 出場'},
}


def _dt_fetch(ticker: str, tf: str):
    """day-trade 頁專用取資料 — Twelve Data 優先，失敗自動 fallback Alpaca IEX。"""
    df = None
    try:
        from intraday.twelvedata_src import fetch_td, has_twelvedata
        if has_twelvedata():
            df = fetch_td(ticker, tf)
    except Exception:
        df = None
    if df is None or len(df) < 30:
        try:
            df = get_intraday(ticker, tf=tf, market='us')
        except Exception:
            df = None
    return df


@st.cache_data(ttl=20, show_spinner=False)
def _scan_one(ticker: str, tf: str) -> dict:
    """抓單一標的 intraday 並跑 v9.38 戰法訊號。回傳精簡（可快取）dict。"""
    out = {'ticker': ticker, 'ok': False, 'err': None}
    df = _dt_fetch(ticker, tf)
    if df is None or len(df) < 30:
        out['err'] = f'資料不足（{0 if df is None else len(df)} bars）'
        return out
    try:
        sig = detect_swing_signal(df, market='us', tf=tf)
    except Exception as e:
        out['err'] = f'訊號計算失敗：{e}'
        return out
    if sig.get('error'):
        out['err'] = sig['error']
        return out
    entry = sig.get('entry', {}) or {}
    exit_ = sig.get('exit', {}) or {}
    sepa = sig.get('sepa', {}) or {}
    reentry = sig.get('reentry', {}) or {}
    ts = sig.get('ts')
    out.update({
        'ok': True,
        'close': sig.get('close'),
        'bb_mid': sig.get('bb_mid'),
        'bb_p1': sig.get('bb_p1sigma'),
        'entry_state': entry.get('state'),
        'entry_dist': entry.get('dist_pct'),
        'exit_state': exit_.get('state'),
        'sepa_score': sepa.get('score'),
        'sepa_total': sepa.get('total', 7),
        'reentry_count': reentry.get('count', 0) if isinstance(reentry, dict) else 0,
        'bar_ts': ts.strftime('%m-%d %H:%M') if hasattr(ts, 'strftime') else '—',
    })
    return out


@st.cache_data(ttl=60, show_spinner=False)
def _grid_fetch(ticker: str, tf: str):
    """抓單一 (ticker, tf) 的 OHLCV df，供多週期總覽網格使用（可快取 60s）。"""
    df = _dt_fetch(ticker, tf)
    if df is None or len(df) < 30:
        return None
    return df


def _render_live_zigzag(ticker, tf, theme_lbl, rth, idx):
    """在目前欄位內渲染單一標的的即時 Zigzag（新鮮度＋戰法訊號＋圖）。"""
    try:
        zdf = _dt_fetch(ticker, tf)
    except Exception as e:
        st.error(f"❌ {ticker}：抓資料失敗 {type(e).__name__}")
        return
    if zdf is None:
        st.error(f"❌ {ticker}：查無即時資料（代號錯誤或 API 無回應）")
        return
    if len(zdf) < 20:
        st.warning(f"⚠️ {ticker} 資料不足（{len(zdf)} bars）")
        return
    # 新鮮度（index 為 naive UTC）
    last_utc = _last_utc(zdf.index)
    last_et = last_utc.tz_convert(ET)
    lag_min = (pd.Timestamp.now(tz='UTC') - last_utc).total_seconds() / 60
    price = float(zdf['Close'].iloc[-1])
    bar_min = {'1m': 1, '5m': 5, '15m': 15, '30m': 30}.get(tf, 15)
    _fresh = (f"最新 bar {last_et.strftime('%m-%d %H:%M')} ET"
              f"　·　現價 {_fmt_price(price)}")
    if not rth:
        st.info(f"⏸️ 美股未開盤 — {_fresh}")
    elif lag_min <= bar_min * 2.5:
        st.success(f"🟢 即時（落後 {lag_min:.0f} 分）— {_fresh}")
    else:
        st.warning(f"⏰ 落後約 {lag_min:.0f} 分 — {_fresh}")
    # index 轉 ET 供圖表 x 軸
    zdf_et = zdf.copy()
    zdf_et.index = _index_to_et(zdf.index)
    # 目前戰法訊號 ＋ 撤退警示（對抗「等一下會漲回來」的執念）
    try:
        _sig = detect_swing_signal(zdf_et, market='us', tf=tf)
    except Exception:
        _sig = None
    if _sig and not _sig.get('error'):
        _estate = (_sig.get('entry', {}) or {}).get('state')
        _xstate = (_sig.get('exit', {}) or {}).get('state')
        try:
            _rhigh = float(zdf['High'].tail(40).max())
            _ddown = abs((price - _rhigh) / _rhigh * 100) if _rhigh > 0 else 0.0
        except Exception:
            _ddown = 0.0
        if _xstate == 'EXIT':
            st.error(
                f"🚨 **撤退訊號成立** — Close 跌破 BB 中軌 ＋ EMA5/EMA20 雙雙下彎，"
                f"距近期高點已回落 **{_ddown:.1f}%**。\n\n"
                f"這是**趨勢反轉、不是回檔**。別騙自己「等一下會漲回來」—— "
                f"SOXS 5/19 就是這樣 hold 到收盤 **−9.47%**（見 Tool D 案例）。\n\n"
                f"👉 **照紀律出場，不要凹單。**")
        elif _xstate in ('WARN_PRICE', 'WARN_EMA'):
            _wr = ('Close 已跌破 BB 中軌（EMA 尚未轉空）'
                   if _xstate == 'WARN_PRICE'
                   else 'EMA5/EMA20 已下彎（中軌尚未跌破）')
            st.warning(
                f"⚠️ **撤退預警** — {_wr}。\n\n"
                f"把停損掛好、手放在賣出鍵上。訊號一轉「撤退」就立刻走，"
                f"**不要等反彈**。")
        elif _estate == 'ENTER':
            st.success("🟢 **進場訊號成立** — 趨勢站穩 BB+1σ，可依紀律進場。")
        else:
            _, _etx = _ENTRY_VIEW.get(_estate, ('⚪', '—'))
            _, _xtx = _EXIT_VIEW.get(_xstate, ('⚪', '—'))
            st.info(f"⚪ 戰法訊號正常　·　進場：{_etx}　｜　出場：{_xtx}")
    # 戰法歷史 entry/exit/加碼 marker
    _swing = _reentry = None
    try:
        from intraday.strategy import (scan_with_exit_rule,
                                       scan_historical_reentry)
        _swing = scan_with_exit_rule(
            zdf_et, market='us', lookback_bars=120, tf=tf,
            exit_rule='mid_ema_down', entry_mode='bb_p1sig')
        _reentry = scan_historical_reentry(
            zdf_et, market='us', lookback_bars=120, tf=tf)
    except Exception:
        _swing = _reentry = None
    try:
        from intraday.settings import get_zigzag_atr_mult
        _atr_m = get_zigzag_atr_mult()
    except Exception:
        _atr_m = 1.3
    # 渲染 Plotly Zigzag
    try:
        fig = build_zigzag_chart_plotly(
            zdf_et, atr_mult=_atr_m, title='',
            max_bars=120, show_bb=True,
            show_emas=[5, 20, 60, 150, 200], show_macd=False,
            theme=('dark' if theme_lbl == '深色' else 'light'),
            swing_trades=_swing, reentry_events=_reentry)
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True,
                            key=f"dt_zz_plotly_{idx}_{ticker}")
        else:
            st.warning("⚠️ 圖表無法產生")
    except Exception as e:
        st.warning(f"⚠️ Plotly 渲染失敗：{str(e)[:100]}")


def _render_scan_cards(results):
    """把掃描結果排成 3 欄卡片。"""
    cols = st.columns(3)
    for idx, r in enumerate(results):
        with cols[idx % 3]:
            with st.container(border=True):
                if not r['ok']:
                    st.markdown(f"#### {r['ticker']}")
                    st.error(r.get('err') or '未知錯誤')
                    continue
                st.markdown(f"#### {r['ticker']}　{_fmt_price(r['close'])}")
                e_emoji, e_txt = _ENTRY_VIEW.get(
                    r['entry_state'], ('⚪', r['entry_state'] or '—'))
                x_emoji, x_txt = _EXIT_VIEW.get(
                    r['exit_state'], ('⚪', r['exit_state'] or '—'))
                dist = r.get('entry_dist')
                dist_txt = f"（距 +1σ {dist:+.2f}%）" if dist is not None else ""
                st.markdown(f"{e_emoji} **進場**：{e_txt}　{dist_txt}")
                st.markdown(f"{x_emoji} **出場**：{x_txt}")
                if r.get('reentry_count'):
                    st.markdown("🟡 **加碼**：EMA5 反轉訊號成立")
                sepa_s = r.get('sepa_score')
                sepa_txt = (f"SEPA {sepa_s}/{r.get('sepa_total', 7)}"
                            if sepa_s is not None else "SEPA —")
                st.caption(f"{sepa_txt}　·　中軌 {_fmt_price(r['bb_mid'])}"
                           f"　·　bar {r['bar_ts']}")


# ════════════════════════════════════════════════════════════════
# Header
# ════════════════════════════════════════════════════════════════

st.title("⚡ Day-Trade 工具台")
st.caption("Stock001 v9.50 ｜ 波段戰法 v9.38（bb_p1sig 進場 · mid_ema_down 出場）"
           " — 把「無情緒交易」變成可執行流程")

phase = get_session_phase()
_banner = (f"{phase['emoji']} **{phase['name']}**　·　{phase['advice']}"
           f"　·　🕐 {phase['now'].strftime('%Y-%m-%d %H:%M:%S')} ET")
if phase['tone'] == 'good':
    st.success(_banner)
elif phase['tone'] == 'caution':
    st.warning(_banner)
else:
    st.info(_banner)

# 資料源指示（day-trade 頁專用 Twelve Data；失敗自動 fallback Alpaca）
try:
    from intraday.twelvedata_src import has_twelvedata as _has_td, td_call_count
    if _has_td():
        st.caption(f"📡 資料源 **Twelve Data**（Free：8 calls/min · 800 credits/day）"
                   f"　·　本次連線真實 API 呼叫 {td_call_count()} 次"
                   f"　·　抓不到時自動 fallback Alpaca IEX")
    else:
        st.caption("📡 資料源 Alpaca IEX（未偵測到 Twelve Data key）")
except Exception:
    pass

tab_a, tab_b, tab_c, tab_d, tab_e = st.tabs([
    "A · 戰法訊號面板", "B · 交易卡產生器",
    "C · 日內熔斷監控", "D · SOXS 5/19 案例", "E · 多週期總覽"])


# ════════════════════════════════════════════════════════════════
# Tool A — 戰法訊號面板
# ════════════════════════════════════════════════════════════════

with tab_a:
    st.subheader("A · 戰法訊號面板")
    st.markdown("一次掃描多檔當沖標的的 **v9.38 戰法**進出場狀態 — "
                "綠色＝進場訊號成立、紅色＝出場訊號成立。")

    col1, col2 = st.columns([3, 1])
    with col1:
        wl_text = st.text_area(
            "觀察名單（逗號或換行分隔）",
            value="SOXL, SOXS, NVDL, AMDL, SPXL, TECL, ERX",
            height=80, key="dt_watchlist")
    with col2:
        scan_tf = st.selectbox("K 線週期", ['1m', '5m', '15m', '30m'],
                               index=2, key="dt_scan_tf")
        st.caption("v9.38 於 15m 回測驗證最佳；1m 適合當沖即時觀察")

    if st.button("🔍 掃描訊號", type="primary", key="dt_scan_btn"):
        tickers = [x.strip().upper() for x in
                   wl_text.replace('\n', ',').split(',') if x.strip()]
        if not tickers:
            st.warning("請至少輸入一個股票代號。")
        else:
            results = []
            prog = st.progress(0.0, text="掃描中…")
            for idx, tk in enumerate(tickers):
                prog.progress((idx + 1) / len(tickers),
                              text=f"掃描 {tk}　({idx + 1}/{len(tickers)})")
                results.append(_scan_one(tk, scan_tf))
            prog.empty()
            st.session_state['dt_scan_results'] = results
            st.session_state['dt_scan_meta'] = {
                'tf': scan_tf,
                'ts': datetime.now(ET).strftime('%H:%M:%S')}

    results = st.session_state.get('dt_scan_results')
    if results:
        meta = st.session_state.get('dt_scan_meta', {})
        st.caption(f"最後掃描：{meta.get('ts', '—')} ET ｜ "
                   f"週期 {meta.get('tf', '—')} ｜ 共 {len(results)} 檔")
        _render_scan_cards(results)

        with st.expander("📖 訊號說明"):
            st.markdown("""
**進場狀態**
- 🟢 **進場訊號成立** — EMA5 > EMA20 ＋ 5 條 EMA 全上揚 ＋ Close 接近/突破 BB +1σ
- ⏳ **待 BB+1σ** — 趨勢條件已成立，等股價推進到 BB +1σ 才觸發
- ⚪ **無進場 setup** — 趨勢條件尚未滿足

**出場狀態**
- 🔴 **出場訊號成立** — Close 跌破 BB 中軌 ＋ EMA5/EMA20 同步下行
- ⚠️ **跌破中軌警戒** — Close 已破中軌，但 EMA 尚未轉空
- ⚠️ **EMA 下行警戒** — EMA5/EMA20 下行，但中軌未破
- ⚪ **持有 / 觀望** — 無出場訊號

**🟡 加碼** — EMA5 向下後反轉、且回檔過程從未跌破 BB 中軌

> ⏰ **時段提醒**：戰法訊號給你的是「方向」，實際進出仍要對照上方盤別 —
> 早盤趨勢區（10:00–11:30 ET）訊號可信度最高；午盤盤整區假訊號偏多。
> 倍數 ETF 趨勢多在 13:30 ET 前後才真正抵定。
""")
    else:
        st.info("輸入觀察名單後按「🔍 掃描訊號」開始。")

    # ────────────────────────────────────────────────────────────
    # 即時 Zigzag 圖
    # ────────────────────────────────────────────────────────────
    st.divider()
    zz_on = st.toggle("📈 即時 Zigzag 圖", value=False, key="dt_zz_on",
                      help="盤中開啟可即時繪製 Zigzag；代號欄輸入多檔可並排比較")
    if not zz_on:
        st.caption("開啟可即時繪製 Zigzag 走勢圖；代號欄輸入多檔"
                   "（逗號分隔，最多 4 檔）即左右並排。")
    else:
        zc1, zc2, zc3, zc4 = st.columns([2, 1, 1, 1.4])
        zz_raw = zc1.text_input("代號（可多檔，逗號分隔）", value="SOXL, SOXS",
                                key="dt_zz_ticker")
        zz_tickers = [t.strip().upper() for t in
                      zz_raw.replace('\n', ',').split(',') if t.strip()][:4]
        zz_tf = zc2.selectbox("週期", ['1m', '5m', '15m', '30m'],
                              index=0, key="dt_zz_tf")
        zz_theme_lbl = zc3.selectbox("圖表主題", ['深色', '淺色'],
                                     index=0, key="dt_zz_theme")
        zz_refresh = zc4.radio("自動刷新", ['關閉', '30s', '60s'],
                               horizontal=True, key="dt_zz_refresh")

        _rsec = {'關閉': 0, '30s': 30, '60s': 60}[zz_refresh]
        _rth = is_rth()
        if _rsec > 0 and _rth:
            try:
                from streamlit_autorefresh import st_autorefresh
                st_autorefresh(interval=_rsec * 1000, key="dt_zz_autorefresh")
                st.caption(f"🔄 自動刷新每 {_rsec}s — Twelve Data 盤中真即時")
            except ImportError:
                st.caption("⚠️ 自動刷新需 `pip install streamlit-autorefresh`")
        elif _rsec > 0:
            st.caption("⏸️ 美股未開盤 — 自動刷新暫停，"
                       "盤中（09:30–16:00 ET）自動生效")

        if not zz_tickers:
            st.info("請輸入至少一個股票代號。")
        else:
            _zzcols = st.columns(len(zz_tickers))
            for _zi, _zt in enumerate(zz_tickers):
                with _zzcols[_zi]:
                    st.markdown(f"### 📈 {_zt}")
                    _render_live_zigzag(_zt, zz_tf, zz_theme_lbl, _rth, _zi)
            st.caption("🟢▲ 戰法進場　🔴✕ 戰法出場　🟡★ 加碼（EMA5 反轉）"
                       "　·　資料源 Twelve Data（盤中真即時）")


# ════════════════════════════════════════════════════════════════
# Tool B — 交易卡產生器
# ════════════════════════════════════════════════════════════════

with tab_b:
    st.subheader("B · 交易卡產生器")
    st.markdown("下單前先算清楚 **部位大小**、**最大虧損** 與 **R 倍數目標價** — "
                "把風險寫死在紙上，就不會被盤中情緒牽著走。")

    c1, c2, c3 = st.columns(3)
    with c1:
        tc_ticker = st.text_input("股票代號", value="SOXL", key="tc_ticker")
        tc_entry = st.number_input("進場價 $", min_value=0.0, value=20.0,
                                   step=0.01, format="%.4f", key="tc_entry")
    with c2:
        tc_stop_mode = st.radio("停損設定方式", ['百分比 %', '固定停損價'],
                                horizontal=True, key="tc_stop_mode")
        if tc_stop_mode == '百分比 %':
            tc_stop_pct = st.number_input("停損 %", min_value=0.1,
                                          max_value=50.0, value=2.0,
                                          step=0.1, key="tc_stop_pct")
            tc_stop_price = tc_entry * (1 - tc_stop_pct / 100)
            st.caption(f"→ 停損價 {_fmt_price(tc_stop_price)}")
        else:
            tc_stop_price = st.number_input("停損價 $", min_value=0.0,
                                            value=19.6, step=0.01,
                                            format="%.4f", key="tc_stop_price")
    with c3:
        tc_account = st.number_input("帳戶資金 $", min_value=0.0, value=10000.0,
                                     step=100.0, key="tc_account")
        tc_risk_pct = st.number_input("單筆風險 %", min_value=0.1, max_value=20.0,
                                      value=1.0, step=0.1, key="tc_risk_pct")

    risk_per_share = tc_entry - tc_stop_price
    if tc_entry <= 0:
        st.info("請輸入進場價。")
    elif risk_per_share <= 0:
        st.error("⚠️ 停損價必須**低於**進場價（做多）。請檢查停損設定。")
    else:
        risk_amount = tc_account * tc_risk_pct / 100
        shares = int(risk_amount // risk_per_share)
        position_value = shares * tc_entry
        position_pct = (position_value / tc_account * 100) if tc_account > 0 else 0
        stop_pct_real = risk_per_share / tc_entry * 100
        actual_risk = shares * risk_per_share
        t1 = tc_entry + risk_per_share
        t2 = tc_entry + 2 * risk_per_share
        t3 = tc_entry + 3 * risk_per_share

        if shares <= 0:
            st.error("⚠️ 依此風險設定可買股數為 **0** — 風險金額不足一股的停損距離。"
                     "請調高帳戶資金 / 單筆風險 %，或縮小停損距離。")
        else:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("可買股數", f"{shares:,} 股")
            m2.metric("部位金額", f"${position_value:,.0f}",
                      f"{position_pct:.1f}% 帳戶")
            m3.metric("停損價", _fmt_price(tc_stop_price),
                      f"-{stop_pct_real:.2f}%", delta_color="inverse")
            m4.metric("最大虧損", f"-${actual_risk:,.0f}",
                      f"-{tc_risk_pct:.1f}% 帳戶", delta_color="inverse")

            st.markdown(f"**🎯 R 倍數目標價**（1R＝每股風險 ${risk_per_share:.4f}）")
            g1, g2, g3 = st.columns(3)
            g1.metric("1R　（風報 1:1）", _fmt_price(t1),
                      f"+${shares * risk_per_share:,.0f}")
            g2.metric("2R　（風報 1:2）", _fmt_price(t2),
                      f"+${shares * 2 * risk_per_share:,.0f}")
            g3.metric("3R　（風報 1:3）", _fmt_price(t3),
                      f"+${shares * 3 * risk_per_share:,.0f}")

            if position_value > tc_account:
                st.warning(f"⚠️ 部位金額 ${position_value:,.0f} 超過帳戶資金 "
                           f"${tc_account:,.0f} — 需融資，槓桿交易請格外謹慎。")
            elif position_pct > 40:
                st.warning(f"⚠️ 單一部位佔帳戶 {position_pct:.0f}% — "
                           f"集中度偏高，留意風險。")

            card_txt = (
                f"【交易卡】{tc_ticker.upper()}\n"
                f"進場價　 ${tc_entry:.4f}\n"
                f"停損價　 ${tc_stop_price:.4f}　(-{stop_pct_real:.2f}%)\n"
                f"股　數　 {shares:,} 股\n"
                f"部位金額 ${position_value:,.0f}　({position_pct:.1f}% 帳戶)\n"
                f"最大虧損 -${actual_risk:,.0f}　(-{tc_risk_pct:.1f}% 帳戶)\n"
                f"目標　   1R ${t1:.4f} ｜ 2R ${t2:.4f} ｜ 3R ${t3:.4f}\n"
                f"紀律　   觸及停損價必出；到 2R 至少出一半，剩餘移動停利。")
            st.markdown("**📋 交易卡（可複製）**")
            st.code(card_txt, language=None)


# ════════════════════════════════════════════════════════════════
# Tool C — 日內熔斷監控
# ════════════════════════════════════════════════════════════════

with tab_c:
    st.subheader("C · 日內熔斷監控")
    st.markdown("記錄今日每一筆交易，**三重熔斷**機制防止連續虧損後的情緒化過度交易。")

    if 'dt_log' not in st.session_state:
        st.session_state['dt_log'] = []
    if 'dt_log_date' not in st.session_state:
        st.session_state['dt_log_date'] = datetime.now(ET).strftime('%Y-%m-%d')

    # 跨日自動清空
    today = datetime.now(ET).strftime('%Y-%m-%d')
    if st.session_state['dt_log_date'] != today:
        st.session_state['dt_log'] = []
        st.session_state['dt_log_date'] = today

    with st.expander("⚙️ 熔斷規則設定"):
        s1, s2, s3 = st.columns(3)
        max_trades = s1.number_input("每日最大交易次數", 1, 20, 3,
                                     key="dt_max_trades")
        max_consec = s2.number_input("連續虧損上限（筆）", 1, 10, 2,
                                     key="dt_max_consec")
        loss_circuit = s3.number_input("當日虧損熔斷 %", 0.5, 50.0, 5.0,
                                       step=0.5, key="dt_loss_circuit")

    with st.form("dt_add_trade", clear_on_submit=True):
        f1, f2 = st.columns([2, 2])
        add_ticker = f1.text_input("代號", value="", key="dt_add_ticker")
        add_pnl = f2.number_input("本筆損益 %", value=0.0, step=0.1,
                                  format="%.2f", key="dt_add_pnl")
        if st.form_submit_button("➕ 記錄這筆交易", use_container_width=True):
            st.session_state['dt_log'].append({
                'ticker': (add_ticker or '—').upper().strip() or '—',
                'pnl': float(add_pnl),
                'time': datetime.now(ET).strftime('%H:%M:%S'),
            })

    log = st.session_state['dt_log']
    n = len(log)
    total_pnl = sum(t['pnl'] for t in log)
    wins = sum(1 for t in log if t['pnl'] > 0)
    losses = sum(1 for t in log if t['pnl'] < 0)
    consec_loss = 0
    for t in reversed(log):
        if t['pnl'] < 0:
            consec_loss += 1
        else:
            break

    halts = []
    if n >= max_trades:
        halts.append(f"已達每日最大交易次數 {int(max_trades)} 筆")
    if total_pnl <= -loss_circuit:
        halts.append(f"當日累積虧損 {total_pnl:.2f}% 觸發熔斷"
                     f"（門檻 -{loss_circuit:.1f}%）")
    warn = (f"連續虧損 {consec_loss} 筆 — 強烈建議離場休息，檢討後再戰"
            if consec_loss >= max_consec else None)

    if halts:
        st.error("🔴 **今日停止交易** — " + "；".join(halts))
    elif warn:
        st.warning("🟡 **熔斷警告** — " + warn)
    elif n == 0:
        st.info("尚未記錄交易。每完成一筆就記錄損益 %，系統會自動監控過度交易。")
    else:
        st.success("🟢 **狀態正常** — 仍可依紀律交易，務必守住單筆風險與停損。")

    if n > 0:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("交易筆數", f"{n} / {int(max_trades)}")
        k2.metric("當日損益", f"{total_pnl:+.2f}%")
        k3.metric("勝 / 負", f"{wins} / {losses}")
        k4.metric("連續虧損", f"{consec_loss} 筆")

        df_log = pd.DataFrame([
            {'#': i + 1, '時間': t['time'], '代號': t['ticker'],
             '損益 %': f"{t['pnl']:+.2f}",
             '結果': ('✅ 獲利' if t['pnl'] > 0 else
                      ('❌ 虧損' if t['pnl'] < 0 else '➖ 平手'))}
            for i, t in enumerate(log)])
        st.dataframe(df_log, hide_index=True, use_container_width=True)

        if st.button("🗑️ 清空今日日誌", key="dt_clear_log"):
            st.session_state['dt_log'] = []
            st.rerun()

    st.markdown("""
---
**三重熔斷機制**
1. **次數熔斷** — 達每日最大交易次數即停手，避免「再凹一把」越陷越深。
2. **連虧熔斷** — 連續虧損達上限 → 當下狀態不對，先離場休息再檢討。
3. **虧損熔斷** — 當日累積虧損達門檻 → 立即收工，保住本金明天再來。

> 日誌只存在本次瀏覽器 session，**跨交易日（ET）自動清空**。
""")


# ════════════════════════════════════════════════════════════════
# Tool D — SOXS 5/19 案例
# ════════════════════════════════════════════════════════════════

with tab_d:
    st.subheader("D · SOXS 2026-05-19 案例 — 訊號 vs 情緒")
    st.markdown("""
這天 SOXS 開高走低、收黑 **−7%**，是一份「**紀律 vs 情緒**」的經典教材：

- 🟢 **戰法紀律**：09:50 進場 $10.38 → 10:29 出場 $10.85，**+4.53%** 落袋。
- 🤑 **情緒陷阱 1（追高）**：10:18 看它衝到 $11.06 才 FOMO 追進 →
  hold 到收盤 **−9.47%**。
- 😱 **情緒陷阱 2（恐慌）**：13:23 跌到全日最低 $9.38 時恐慌砍倉 →
  正好賣在最低點，之後還反彈。

**心法**：訊號告訴你進出場的「**時機**」；情緒只會讓你**追在最高、砍在最低**。
看圖中綠色三角（進）與紅色叉（出）就是戰法的紀律點。

📊 圖表**含盤前時段**（🌅 04:00 起）—— 藍色虛線 ▲ 標示 09:30 RTH 開盤，
可對照盤前鋪陳與開盤後走勢。
""")
    chart_path = os.path.join(ROOT, 'soxs_0519_chart.html')
    if os.path.exists(chart_path):
        with open(chart_path, encoding='utf-8') as f:
            components.html(f.read(), height=1180, scrolling=True)
    else:
        st.warning("找不到 `soxs_0519_chart.html` — 請先在專案根目錄執行："
                   "`python generate_soxs_chart.py` 產生圖表。")

    # ── 我的實單血淚 ──
    st.divider()
    st.markdown("### 🩸 我的實單血淚 — 同一天的真實操作")
    st.caption("把上面的教訓對照到自己 SOXS 5/19 的 7 筆實際成交"
               "（依券商畫面順序，最新在上）。")
    _my_trades = pd.DataFrame([
        {'動作': '賣出', '股數': 1500, '成交價': 9.74,  '當日區間位置': '21% 地板區'},
        {'動作': '賣出', '股數': 100,  '成交價': 10.23, '當日區間位置': '51% 中段'},
        {'動作': '買進', '股數': 500,  '成交價': 10.52, '當日區間位置': '68% 中上'},
        {'動作': '買進', '股數': 1000, '成交價': 10.13, '當日區間位置': '45% 中段'},
        {'動作': '買進', '股數': 1000, '成交價': 9.89,  '當日區間位置': '30% 中下'},
        {'動作': '賣出', '股數': 1000, '成交價': 10.21, '當日區間位置': '49% 中段'},
        {'動作': '買進', '股數': 100,  '成交價': 10.51, '當日區間位置': '67% 中上'},
    ])
    st.dataframe(_my_trades, hide_index=True, use_container_width=True)

    _b1, _b2, _b3 = st.columns(3)
    _b1.metric("買進均價", "$10.13", "2600 股", delta_color="off")
    _b2.metric("賣出均價", "$9.94", "2600 股", delta_color="off")
    _b3.metric("可見 7 筆淨損益", "-$488", "-1.85%")

    st.markdown("""
**🩸 血淚① — 問題不在「買」，在「沒賣」**
買進均價 $10.13 其實比戰法進場 $10.375 還漂亮。戰法 **10:29 喊出場 $10.845**，
卻沒走 —— 把 SOXS 一路抱過 $10.8 → $10.2 → $9.7。

**🩸 血淚② — 越跌越買（攤平）**
SOXS 崩盤途中還「買進 1000 @ $9.89」—— 接刀、攤平、賭「會彈回來」，
就是「執迷不悟」。

**🩸 血淚③ — 恐慌砍在地板**
最新一筆「賣出 1500 @ $9.74」是 58% 的量，砍在離全日最低 $9.38 只差
**3.8%** 的地方 —— 把上面的「情緒陷阱 2」親身演了一遍。
""")
    st.error("🧮 **紀律 vs 情緒的代價** — 2600 股若全照戰法 $10.845 出場 → "
             "**+$1,866**；實際 **-$488**。情緒來回吐掉 **≈ $2,350**。")
    st.success("✅ **下次鐵律**：看到 🚨 撤退警示就走 · 絕不對虧損部位加碼 · "
               "下單前先用交易卡算好停損。")
    st.caption("完整逐筆計算見 `analyze_my_soxs_trades.py`。")


# ════════════════════════════════════════════════════════════════
# Tool E — 多週期總覽（多檔 × 多週期 ZigZag 網格）
# ════════════════════════════════════════════════════════════════

with tab_e:
    st.subheader("E · 多週期總覽")
    st.markdown("兩檔股票左右並排成兩欄，所有週期由上往下排 —— "
                "往下掃看一檔的所有週期、往右比較兩檔同週期。")

    ge1, ge2, ge3 = st.columns(3)
    grid_a = ge1.text_input("股票 A", value="SOXS",
                            key="grid_a").strip().upper()
    grid_b = ge2.text_input("股票 B", value="SOXL",
                            key="grid_b").strip().upper()
    grid_theme = ge3.selectbox("圖表主題", ['深色', '淺色'], index=0,
                               key="grid_theme")

    _TF_ORDER = ['1m', '5m', '15m', '30m', '1h', '4h', '1d']
    _grid_tf_pick = st.multiselect(
        "顯示週期（由短到長往下排）", _TF_ORDER, default=_TF_ORDER,
        key="grid_tfs")
    grid_tfs = [tf for tf in _TF_ORDER if tf in _grid_tf_pick]

    gh1, gh2 = st.columns([1, 2])
    grid_bars = gh1.slider("每張圖顯示 bars", 30, 90, 45, step=5,
                           key="grid_bars")
    grid_refresh = gh2.radio("自動刷新", ['關閉', '60s', '120s'],
                             horizontal=True, key="grid_refresh")

    grid_on = st.toggle("📊 載入多週期網格", value=False, key="grid_on",
                        help="一次抓 2 檔 × N 週期，首次載入需數秒；"
                             "關閉以加快頁面載入")

    if not grid_on:
        st.info("👆 打開上方「📊 載入多週期網格」開關 —— 即可繪製兩檔股票"
                "左右並排成兩欄、7 個週期由上往下的 ZigZag 網格。")
    elif not grid_tfs:
        st.warning("請至少選一個時間週期。")
    elif not (grid_a or grid_b):
        st.warning("請至少輸入一檔股票代號。")
    else:
        _gsec = {'關閉': 0, '60s': 60, '120s': 120}[grid_refresh]
        if _gsec > 0 and is_rth():
            try:
                from streamlit_autorefresh import st_autorefresh
                st_autorefresh(interval=_gsec * 1000, key="grid_autorefresh")
                st.caption(f"🔄 自動刷新每 {_gsec}s — Alpaca IEX 盤中真即時")
            except ImportError:
                st.caption("⚠️ 自動刷新需 `pip install streamlit-autorefresh`")
        elif _gsec > 0:
            st.caption("⏸️ 美股未開盤 — 自動刷新暫停，"
                       "盤中（09:30–16:00 ET）自動生效")

        _gtheme = 'dark' if grid_theme == '深色' else 'light'
        try:
            from intraday.settings import get_zigzag_atr_mult
            _gatr = get_zigzag_atr_mult()
        except Exception:
            _gatr = 1.3

        _gstocks = [s for s in (grid_a, grid_b) if s]
        _gncol = len(_gstocks)

        # 表頭：股票名稱（左右並排成欄）
        _ghead = st.columns(_gncol)
        for _gi, _gs in enumerate(_gstocks):
            _ghead[_gi].markdown(f"### 📈 {_gs}")

        _gtotal = max(1, _gncol * len(grid_tfs))
        _gprog = st.progress(0.0, text="載入網格…")
        _gdone = 0
        for _gtf in grid_tfs:                        # 週期 = 列（由上往下）
            _grow = st.columns(_gncol)
            for _gi, _gs in enumerate(_gstocks):     # 股票 = 欄（左右並排）
                _gdone += 1
                _gprog.progress(_gdone / _gtotal,
                                text=f"載入 {_gs} {_gtf}（{_gdone}/{_gtotal}）")
                with _grow[_gi]:
                    _gdf = _grid_fetch(_gs, _gtf)
                    if _gdf is None or len(_gdf) < 30:
                        st.markdown(f"**{_gs} · {_gtf}**")
                        st.info("資料不足")
                        continue
                    _gwin = _gdf.tail(grid_bars)
                    _gc0 = float(_gwin['Close'].iloc[0])
                    _gc1 = float(_gwin['Close'].iloc[-1])
                    _gchg = (_gc1 - _gc0) / _gc0 * 100 if _gc0 else 0.0
                    _garrow = ('🔺' if _gchg > 0.05 else
                               '🔻' if _gchg < -0.05 else '▪️')
                    # 戰法訊號 → 卡片底色（出場紅 / 進場綠 / 其餘原色）＋ 加碼
                    _gstate = None
                    _greentry = 0
                    try:
                        _gsig = detect_swing_signal(_gdf, market='us', tf=_gtf)
                        if _gsig and not _gsig.get('error'):
                            _sx = (_gsig.get('exit', {}) or {}).get('state')
                            _se = (_gsig.get('entry', {}) or {}).get('state')
                            if _se == 'ENTER':
                                _gstate = 'enter'
                            elif _sx == 'EXIT':
                                _gstate = 'exit'
                            _greentry = ((_gsig.get('reentry') or {})
                                         .get('count', 0))
                    except Exception:
                        _gstate = None
                    _gstyle = (_GRID_SIG_STYLE.get((_gstate, _gtheme))
                               if _gstate else None)
                    _gtag2 = '　🟡 加碼' if _greentry else ''
                    # 標題列（有訊號時整條上色；有加碼補 🟡 標記）
                    if _gstyle:
                        st.markdown(
                            f"<div style='background:{_gstyle['capbg']};"
                            f"color:{_gstyle['captx']};padding:5px 9px;"
                            f"border-radius:5px;font-size:.9rem'>"
                            f"<b>{_gstyle['tag']}｜{_gs} · {_gtf}</b>　"
                            f"{_fmt_price(_gc1)}　{_garrow} {_gchg:+.1f}%"
                            f"{_gtag2}</div>",
                            unsafe_allow_html=True)
                    else:
                        st.markdown(f"**{_gs} · {_gtf}**　{_fmt_price(_gc1)}　"
                                    f"{_garrow} {_gchg:+.1f}%{_gtag2}")
                    # 戰法歷史標註（進場▲ / 出場✕ / 加碼★）
                    _gsw = _gre = None
                    try:
                        from intraday.strategy import (scan_with_exit_rule,
                                                       scan_historical_reentry)
                        _gsw = scan_with_exit_rule(
                            _gdf, market='us', lookback_bars=grid_bars,
                            tf=_gtf, exit_rule='mid_ema_down',
                            entry_mode='bb_p1sig')
                        _gre = scan_historical_reentry(
                            _gdf, market='us', lookback_bars=grid_bars,
                            tf=_gtf)
                    except Exception:
                        _gsw = _gre = None
                    try:
                        _gfig = build_zigzag_chart_plotly(
                            _gdf, atr_mult=_gatr, title='',
                            max_bars=grid_bars, show_bb=True,
                            show_emas=[5, 20], show_macd=False,
                            theme=_gtheme, swing_trades=_gsw,
                            reentry_events=_gre)
                        if _gfig is not None:
                            _gfig.update_layout(
                                height=340, showlegend=False,
                                margin=dict(l=4, r=4, t=6, b=4),
                                xaxis_rangeslider_visible=False)
                            if _gstyle:
                                _gfig.update_layout(
                                    paper_bgcolor=_gstyle['paper'],
                                    plot_bgcolor=_gstyle['plot'])
                            st.plotly_chart(
                                _gfig, use_container_width=True,
                                key=f"grid_{_gi}_{_gs}_{_gtf}",
                                config={'displayModeBar': False})
                        else:
                            st.info("無法繪圖")
                    except Exception as _gerr:
                        st.warning(f"繪圖失敗：{str(_gerr)[:60]}")
        _gprog.empty()
        st.caption("每格＝ZigZag＋BB＋EMA5/20，圖上 🟢▲進場 🔴✕出場 🟡★加碼　·　"
                   "🟢 綠底＝目前進場訊號　🔴 紅底＝目前出場訊號　·　"
                   "資料源 Twelve Data")
