"""Day-Trade 工具台 — Stock001 v9.47
=====================================
四合一當沖輔助工具，把「無情緒交易」落地成可操作流程：

  A · 戰法訊號面板   即時掃描當沖標的的 v9.38 戰法進出場狀態
  B · 交易卡產生器   輸入進場價/停損，自動算部位大小與 R 倍數目標
  C · 日內熔斷監控   交易日誌 ＋ 連虧 / 次數 / 虧損 三重熔斷
  D · SOXS 5/19 案例 訊號 vs 情緒 視覺化教學

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


@st.cache_data(ttl=20, show_spinner=False)
def _scan_one(ticker: str, tf: str) -> dict:
    """抓單一標的 intraday 並跑 v9.38 戰法訊號。回傳精簡（可快取）dict。"""
    out = {'ticker': ticker, 'ok': False, 'err': None}
    try:
        df = get_intraday(ticker, tf=tf, market='us')
    except Exception as e:
        out['err'] = f'抓資料失敗：{e}'
        return out
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
st.caption("Stock001 v9.47 ｜ 波段戰法 v9.38（bb_p1sig 進場 · mid_ema_down 出場）"
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

tab_a, tab_b, tab_c, tab_d = st.tabs([
    "A · 戰法訊號面板", "B · 交易卡產生器",
    "C · 日內熔斷監控", "D · SOXS 5/19 案例"])


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
        scan_tf = st.selectbox("K 線週期", ['15m', '5m', '30m'],
                               index=0, key="dt_scan_tf")
        st.caption("v9.38 於 15m 回測驗證最佳")

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
""")
    chart_path = os.path.join(ROOT, 'soxs_0519_chart.html')
    if os.path.exists(chart_path):
        with open(chart_path, encoding='utf-8') as f:
            components.html(f.read(), height=1080, scrolling=True)
    else:
        st.warning("找不到 `soxs_0519_chart.html` — 請先在專案根目錄執行："
                   "`python generate_soxs_chart.py` 產生圖表。")
