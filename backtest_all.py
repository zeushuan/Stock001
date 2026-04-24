"""
多股票六策略回測腳本  2025-10-24 ~ 2026-04-23
自動辨別台股 / 美股 / 指數，輸出逐筆明細 + 最終排行榜
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import warnings; warnings.filterwarnings("ignore")

import yfinance as yf
import pandas as pd
import numpy as np
from ta.trend import EMAIndicator, ADXIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
from datetime import timedelta

# ─────────────────────────────────────────────────────────────
START  = "2025-01-02"
END    = "2026-04-24"
INVEST = 100_000

SYMBOL_ALIASES = {
    "DJI":"^DJI","DJIA":"^DJI",
    "SPX":"^GSPC","SP500":"^GSPC",
    "NDX":"^NDX","NASDAQ":"^IXIC",
}

STOCK_LIST = [
    "DJI","SPX",
    "0050","2330","00922","00981A","00632R","00737",
    "BOTZ",
    "1711","8021","3167","8064",
]

# 反向ETF：使用專屬策略（無T4、ATR×1.5、快速出場）
INVERSE_ETF = {"00632R", "00633L", "00648U"}

# ─────────────────────────────────────────────────────────────
def is_tw(t):
    import re
    return bool(re.match(r'^\d+[A-Z]?$', t))

def yf_sym(t):
    if t in SYMBOL_ALIASES:
        return SYMBOL_ALIASES[t]
    return (t + ".TW") if is_tw(t) else t

def currency(t):
    return "TWD" if is_tw(t) else "USD"

def download(ticker):
    sym = yf_sym(ticker)
    s   = (pd.Timestamp(START) - timedelta(days=280)).strftime("%Y-%m-%d")
    e   = (pd.Timestamp(END)   + timedelta(days=2)).strftime("%Y-%m-%d")
    df  = yf.download(sym, start=s, end=e, auto_adjust=True, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    # 失敗→嘗試 .TWO
    if df.empty and sym.endswith(".TW"):
        sym2 = ticker + ".TWO"
        df   = yf.download(sym2, start=s, end=e, auto_adjust=True, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
    return df

def calc_ind(df):
    c, h, l = df["Close"], df["High"], df["Low"]
    df["e10"]  = EMAIndicator(c, 10).ema_indicator()
    df["e20"]  = EMAIndicator(c, 20).ema_indicator()
    df["e60"]  = EMAIndicator(c, 60).ema_indicator()
    df["rsi"]  = RSIIndicator(c, 14).rsi()
    adx        = ADXIndicator(h, l, c, 14)
    df["adx"]  = adx.adx()
    m          = MACD(c, 26, 12, 9)
    df["mh"]   = m.macd_diff()
    df["atr"]  = AverageTrueRange(h, l, c, 14).average_true_range()
    return df

# ─────────────────────────────────────────────────────────────
# 通用回測引擎（收盤價成交；stop_pct 為小數，如 0.05 = 5%）
# ─────────────────────────────────────────────────────────────
def run_bt(dates, prices, entry_fn, exit_fn, stop_pct=None):
    trades, in_mkt, ep, ed = [], False, None, None
    n = len(prices)
    for i in range(1, n):
        if not in_mkt:
            if entry_fn(i):
                in_mkt, ep, ed = True, prices[i], dates[i]
        else:
            hit_stop = stop_pct and (prices[i] < ep * (1 - stop_pct))
            if hit_stop or exit_fn(i):
                r = (prices[i] - ep) / ep
                trades.append(dict(ed=ed, xd=dates[i], ep=ep, xp=prices[i],
                                   ret=r, pnl=INVEST*r,
                                   days=(dates[i]-ed).days,
                                   stop=bool(hit_stop), open=False))
                in_mkt = False
    if in_mkt:
        r = (prices[-1] - ep) / ep
        trades.append(dict(ed=ed, xd=dates[-1], ep=ep, xp=prices[-1],
                           ret=r, pnl=INVEST*r,
                           days=(dates[-1]-ed).days,
                           stop=False, open=True))
    return trades


def run_bt_atr(dates, prices, atr_arr, entry_fn, exit_fn, atr_mult=2.5):
    """ATR 動態停損版：進場時以 ATR×倍數設初始停損，不隨價格調整。"""
    trades, in_mkt, ep, ed, stop_p = [], False, None, None, None
    n = len(prices)
    for i in range(1, n):
        if not in_mkt:
            if entry_fn(i):
                in_mkt, ep, ed = True, prices[i], dates[i]
                atr_now = atr_arr[i] if not np.isnan(atr_arr[i]) else prices[i] * 0.03
                stop_p  = prices[i] - atr_now * atr_mult
        else:
            hit_stop = (stop_p is not None) and (prices[i] < stop_p)
            if hit_stop or exit_fn(i):
                r = (prices[i] - ep) / ep
                trades.append(dict(ed=ed, xd=dates[i], ep=ep, xp=prices[i],
                                   ret=r, pnl=INVEST*r,
                                   days=(dates[i]-ed).days,
                                   stop=bool(hit_stop), open=False))
                in_mkt = False
    if in_mkt:
        r = (prices[-1] - ep) / ep
        trades.append(dict(ed=ed, xd=dates[-1], ep=ep, xp=prices[-1],
                           ret=r, pnl=INVEST*r,
                           days=(dates[-1]-ed).days,
                           stop=False, open=True))
    return trades

def pbar(pnl, scale=500):
    if pnl is None or (isinstance(pnl, float) and np.isnan(pnl)):
        return "N/A"
    n = min(int(abs(pnl)/scale), 16)
    return ("+" + "█"*n) if pnl >= 0 else ("-" + "▒"*n)

# ─────────────────────────────────────────────────────────────
def analyze(ticker):
    df = download(ticker)
    if df.empty or "Close" not in df.columns:
        return None
    df = calc_ind(df)

    mask  = (df.index >= START) & (df.index <= END)
    sub   = df[mask].copy()
    # 移除收盤價為 NaN 的列（可能是最新未完成交易日）
    sub   = sub[sub["Close"].notna()]
    if len(sub) < 10:
        return None

    n     = len(sub)
    dates = list(sub.index)
    pr    = sub["Close"].values.astype(float)
    e10   = sub["e10"].values.astype(float)
    e20   = sub["e20"].values.astype(float)
    e60   = sub["e60"].values.astype(float)
    rsi   = sub["rsi"].values.astype(float)
    adx_v = sub["adx"].values.astype(float)
    mh    = sub["mh"].values.astype(float)
    atr_v = sub["atr"].values.astype(float)

    def safe(arr, i): return arr[i] if i >= 0 and not np.isnan(arr[i]) else None

    # ── ① 買入持有 ────────────────────────────────────────────
    bh_ret = (pr[-1] - pr[0]) / pr[0]
    bh_pnl = INVEST * bh_ret

    # ── ② 趨勢策略：EMA20/60 交叉（原版）────────────────────
    t2 = run_bt(dates, pr,
        entry_fn=lambda i: (not any(np.isnan([e20[i],e60[i],e20[i-1],e60[i-1]]))
                            and e20[i-1] <= e60[i-1] and e20[i] > e60[i]),
        exit_fn =lambda i: (not any(np.isnan([e20[i],e60[i]]))
                            and e20[i] < e60[i]))

    # ── ③ RSI 策略：<32進/>55出（原版）──────────────────────
    t3 = run_bt(dates, pr,
        entry_fn=lambda i: (not np.isnan(rsi[i]) and rsi[i] < 32),
        exit_fn =lambda i: (not np.isnan(rsi[i]) and rsi[i] > 55))

    # ── ④ EMA 快線：EMA10/20叉 + EMA60同向過濾 + 停損5% ─────
    def e4_en(i):
        if i<1: return False
        if any(np.isnan([e10[i],e10[i-1],e20[i],e20[i-1],e60[i]])): return False
        return (e10[i-1] <= e20[i-1] and e10[i] > e20[i]   # 黃金交叉
                and e20[i] >= e60[i] * 0.99)                # 主趨勢配合

    def e4_ex(i):
        if i<1: return False
        if any(np.isnan([e10[i],e10[i-1],e20[i],e20[i-1]])): return False
        return e10[i-1] >= e20[i-1] and e10[i] < e20[i]

    t4 = run_bt(dates, pr, e4_en, e4_ex, stop_pct=0.05)

    # ── ⑤ 動態 RSI：多頭<45/>72 / 空頭<32反彈/>55 + 停損4% ─
    def e5_en(i):
        if i<1: return False
        if any(np.isnan([e20[i],e60[i],rsi[i],rsi[i-1]])): return False
        if e20[i] > e60[i]:
            return rsi[i] < 45
        else:
            return rsi[i] < 32 and rsi[i] > rsi[i-1]

    def e5_ex(i):
        if any(np.isnan([e20[i],e60[i],rsi[i]])): return False
        return rsi[i] > (72 if e20[i] > e60[i] else 55)

    t5 = run_bt(dates, pr, e5_en, e5_ex, stop_pct=0.04)

    # ── ⑥ 組合確認：RSI<42 + MACD柱改善 / RSI>65或EMA快死叉 + 停損5%
    def e6_en(i):
        if i<1: return False
        if any(np.isnan([rsi[i],mh[i],mh[i-1]])): return False
        return rsi[i] < 42 and mh[i-1] < 0 and mh[i] > mh[i-1]

    def e6_ex(i):
        if i<1: return False
        rsi_hi = not np.isnan(rsi[i]) and rsi[i] > 65
        ema_fl = (not any(np.isnan([e10[i],e10[i-1],e20[i],e20[i-1]]))
                  and e10[i-1] >= e20[i-1] and e10[i] < e20[i])
        return rsi_hi or ema_fl

    t6 = run_bt(dates, pr, e6_en, e6_ex, stop_pct=0.05)

    # ── ⑦ 自適應趨勢（完整版 v2）────────────────────────────────
    # 多頭主策略（T1/T2/T3）
    #   共同前提：EMA20 > EMA60 + ADX ≥ 18（過濾假多頭）
    #   T1 黃金交叉 | T2 期初多頭RSI<65 | T3 多頭拉回RSI<50
    # 【改良①】出場：EMA死亡交叉 + ADX<25時RSI>70出場（防震盪損耗）
    #   → 強趨勢(ADX≥25)持到死叉，弱趨勢(ADX<25)RSI超買即出
    # 停損：ATR×2.5
    #
    # 空頭反彈補充策略（T4）—— 與多頭主策略互補，不重疊
    # 【改良②】空頭 + RSI<35 且止跌回升 → 進場，RSI>55 或黃金交叉出場
    #   → 讓 00632R/BOTZ 在空頭期間也能捕到反彈
    # 停損：ATR×2.0（空頭更緊）

    def e7_en(i):
        if any(np.isnan([e20[i], e60[i], adx_v[i]])): return False
        if not (e20[i] > e60[i] and adx_v[i] >= 22):  return False  # 18→22 防假多頭
        if i < 1: return False
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                return True
        if i == 1 and not np.isnan(rsi[i]) and rsi[i] < 65:
            return True
        if not np.isnan(rsi[i]) and rsi[i] < 50:
            return True
        return False

    def e7_ex(i):
        """改良①：死亡交叉 + ADX<25時RSI>70 自適應出場"""
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i]])): return False
        if e20[i] < e60[i]: return True                          # 死亡交叉
        if not np.isnan(adx_v[i]) and adx_v[i] < 25:            # 弱趨勢
            if not np.isnan(rsi[i]) and rsi[i] > 70:
                return True                                       # RSI超買出場
        return False

    t7 = run_bt_atr(dates, pr, atr_v, e7_en, e7_ex, atr_mult=2.5)

    # 空頭反彈補充（T4）— 改良② + 改良③（連續2天RSI上升確認止跌）
    def e7b_en(i):
        if i < 2: return False
        if any(np.isnan([e20[i], e60[i], rsi[i], rsi[i-1], rsi[i-2]])): return False
        if e20[i] > e60[i]: return False                         # 只在空頭
        # RSI<35 且連續2天上升（改良③：更嚴格止跌確認，避免抓刀）
        return rsi[i] < 35 and rsi[i] > rsi[i-1] and rsi[i-1] > rsi[i-2]

    def e7b_ex(i):
        if any(np.isnan([rsi[i], e20[i], e60[i]])): return False
        if rsi[i] > 55: return True                              # 反彈目標達成
        if i > 0 and not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] < e60[i-1] and e20[i] >= e60[i]:
                return True                                       # EMA黃金交叉
        return False

    t7b = run_bt_atr(dates, pr, atr_v, e7b_en, e7b_ex, atr_mult=2.0)

    # 合計（多頭主策略 + 空頭反彈補充，兩者不重疊）
    t7_all = t7 + t7b

    # ── ⑦反向ETF專屬策略（反向ETF才執行）──────────────────────────
    # 核心洞察：
    #   反向ETF（如00632R）的EMA黃金交叉 = 大盤開始下跌，此時正確進場
    #   策略與正常股票相同（T1/T2/T3 based on own chart），但：
    #   1. 無T4（不在EMA空頭時抓反彈，空頭=大盤多頭，不宜持有反向ETF）
    #   2. ATR×1.5（更緊停損，因反向ETF波動大且有衰減）
    #   3. 弱趨勢出場：ADX<25時RSI>65即出（比一般的70更快出場）
    t7_inv = []
    if ticker in INVERSE_ETF:
        def e7inv_ex(i):
            """反向ETF出場：死亡交叉 + ADX<25時RSI>65（更快出場）"""
            if i < 1: return False
            if any(np.isnan([e20[i], e60[i]])): return False
            if e20[i] < e60[i]: return True
            if not np.isnan(adx_v[i]) and adx_v[i] < 25:
                if not np.isnan(rsi[i]) and rsi[i] > 65:
                    return True
            return False
        t7_inv = run_bt_atr(dates, pr, atr_v, e7_en, e7inv_ex, atr_mult=1.5)

    def tot(tt): return sum(t["pnl"] for t in tt)
    def days(tt): return sum(t["days"] for t in tt)

    result = dict(
        ticker=ticker,
        n=n, p0=pr[0], p1=pr[-1],
        bh_pnl=bh_pnl, bh_ret=bh_ret,
        t2=t2, t3=t3, t4=t4, t5=t5, t6=t6, t7=t7_all, t7b=t7b,
        t7_inv=t7_inv,
        pnl2=tot(t2), pnl3=tot(t3), pnl4=tot(t4), pnl5=tot(t5), pnl6=tot(t6), pnl7=tot(t7_all),
        pnl7_inv=tot(t7_inv),
        days2=days(t2), days3=days(t3), days4=days(t4), days5=days(t5), days6=days(t6), days7=days(t7_all),
        rsi_arr=rsi, e10=e10, e20=e20, e60=e60, dates=dates,
        rsi_min=float(pd.Series(rsi).dropna().min()) if not pd.Series(rsi).dropna().empty else np.nan,
        rsi_max=float(pd.Series(rsi).dropna().max()) if not pd.Series(rsi).dropna().empty else np.nan,
        bull_days=int((pd.Series(e20) > pd.Series(e60)).sum()),
        cur=currency(ticker),
    )
    return result

# ─────────────────────────────────────────────────────────────
def print_detail(r):
    ticker = r["ticker"]
    n, p0, p1 = r["n"], r["p0"], r["p1"]
    cur = r["cur"]
    rsi_arr = r["rsi_arr"]
    dates   = r["dates"]
    idx_map = {d: i for i, d in enumerate(dates)}

    W = 70
    print(f"\n{'='*W}")
    print(f"  {ticker:<10} {START} ~ {END}  ({n}交易日) | {cur}")
    print(f"  首日:{p0:.2f}  末日:{p1:.2f}  漲跌:{r['bh_ret']*100:+.2f}%  "
          f"RSI均值:{float(np.nanmean(rsi_arr)):.1f}  "
          f"EMA多頭:{r['bull_days']}天/空頭:{n-r['bull_days']}天")
    print(f"{'='*W}")

    t7_inv = r.get("t7_inv", [])
    is_inv = len(t7_inv) > 0  # 反向ETF標誌

    strats = [
        ("① 買入持有",       1,      n,          r["bh_pnl"],  r["bh_ret"]*100,  []),
        ("② 趨勢EMA20/60",   len(r["t2"]), r["days2"], r["pnl2"], r["pnl2"]/INVEST*100, r["t2"]),
        ("③ RSI<32/>55",     len(r["t3"]), r["days3"], r["pnl3"], r["pnl3"]/INVEST*100, r["t3"]),
        ("④ EMA快線",        len(r["t4"]), r["days4"], r["pnl4"], r["pnl4"]/INVEST*100, r["t4"]),
        ("⑤ 動態RSI",        len(r["t5"]), r["days5"], r["pnl5"], r["pnl5"]/INVEST*100, r["t5"]),
        ("⑥ 組合確認",       len(r["t6"]), r["days6"], r["pnl6"], r["pnl6"]/INVEST*100, r["t6"]),
        ("⑦ 自適應趨勢[新]", len(r["t7"]), r["days7"], r["pnl7"], r["pnl7"]/INVEST*100, r["t7"]),
    ]
    if is_inv:
        inv_days = sum(t["days"] for t in t7_inv)
        strats.append(
            ("⑦反向ETF專屬", len(t7_inv), inv_days,
             r["pnl7_inv"], r["pnl7_inv"]/INVEST*100, t7_inv)
        )

    best_pnl = max(s[3] for s in strats)
    print(f"  {'策略':<20} {'筆':>3} {'在市天':>6} {'損益':>10} {'報酬%':>8}")
    print(f"  {'-'*52}")
    for name, cnt, dy, pnl, ret, _ in strats:
        mark = " ◀最佳" if pnl == best_pnl else ""
        flag = " ★" if "反向ETF" in name else ""
        print(f"  {name:<20} {cnt:>3} {dy:>6} {pnl:>+10.0f} {ret:>+7.2f}%{mark}{flag}")

    # 逐筆明細：只印 ⑦ 自適應趨勢（標示 T4 空頭反彈筆）+ 反向ETF策略
    detail_strats = strats[6:]
    if is_inv:
        detail_strats = strats[6:]  # 含最後一個 ⑦反向ETF
    for name, cnt, dy, pnl, ret, trades in detail_strats:
        if not trades: continue
        print(f"\n  [{name}]")
        t7b_set = set(id(t) for t in r.get("t7b", []))
        cum = 0.0
        for t in trades:
            cum += t["pnl"]
            ei = idx_map.get(t["ed"])
            xi = idx_map.get(t["xd"])
            re_ = f"{rsi_arr[ei]:.0f}" if ei is not None and not np.isnan(rsi_arr[ei]) else "--"
            rx_ = f"{rsi_arr[xi]:.0f}" if xi is not None and not np.isnan(rsi_arr[xi]) else "--"
            suf = "[持]" if t["open"] else ("[停損]" if t["stop"] else "")
            tag = "[T4反彈]" if id(t) in t7b_set else ""
            print(f"    {t['ed'].strftime('%m/%d')}→{t['xd'].strftime('%m/%d')}"
                  f"  進:{t['ep']:.2f} 出:{t['xp']:.2f}"
                  f"  RSI:{re_}→{rx_}"
                  f"  {t['ret']*100:>+6.2f}%  {t['pnl']:>+8.0f}  {pbar(t['pnl'])} {suf}{tag}")
        print(f"    {'累計':>44} {cum:>+8.0f}")

# ─────────────────────────────────────────────────────────────
def print_leaderboard(all_results):
    """所有股票策略橫向比較總表"""
    print(f"\n\n{'▓'*80}")
    print(f"  最終排行榜  {START} ~ {END}  本金 {INVEST:,} / 各標的幣別")
    print(f"{'▓'*80}")

    # 標題
    print(f"\n  {'代號':<8} {'幣':>4} {'漲跌%':>7} "
          f"{'①持有':>8} {'②趨勢':>8} {'③RSI':>8} "
          f"{'④快線':>8} {'⑤動RSI':>8} {'⑥組合':>8} {'⑦自適應[新]':>11}  最佳策略")
    print(f"  {'-'*98}")

    rows = []
    for r in all_results:
        pnls = [r["bh_pnl"], r["pnl2"], r["pnl3"], r["pnl4"], r["pnl5"], r["pnl6"], r["pnl7"]]
        best_i = int(np.argmax(pnls))
        best_name = ["①持有","②趨勢","③RSI","④快線","⑤動RSI","⑥組合","⑦自適應"][best_i]
        rows.append((r, pnls, best_i, best_name))

    # 按①買入持有報酬排序
    rows.sort(key=lambda x: x[1][0], reverse=True)

    for r, pnls, best_i, best_name in rows:
        vals = "  ".join(f"{p/INVEST*100:>+7.2f}%" for p in pnls)
        mark = " ★" if best_name == "⑦自適應" else ""
        print(f"  {r['ticker']:<8} {r['cur']:>4} {r['bh_ret']*100:>+7.2f}%  {vals}  {best_name}{mark}")

    print(f"\n  ── 最佳策略勝出次數 ──")
    from collections import Counter
    cnt = Counter(row[3] for row in rows)
    for name, c in cnt.most_common():
        print(f"  {name}：{c} 次")

    print(f"\n  ── 各策略平均報酬（{len(all_results)}檔）──")
    keys  = ["bh_pnl","pnl2","pnl3","pnl4","pnl5","pnl6","pnl7"]
    names = ["①持有","②趨勢EMA20/60","③RSI<32/>55","④EMA快線","⑤動態RSI","⑥組合確認","⑦自適應趨勢[新]"]
    for k, nm in zip(keys, names):
        avg = np.mean([r[k]/INVEST*100 for r in all_results])
        mx  = max(r[k]/INVEST*100 for r in all_results)
        mn  = min(r[k]/INVEST*100 for r in all_results)
        tag = " ◀" if nm == "⑦自適應趨勢[新]" else ""
        print(f"  {nm:<22}  均值:{avg:>+7.2f}%  最高:{mx:>+7.2f}%  最低:{mn:>+7.2f}%{tag}")

# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{'▓'*70}")
    print(f"  多股票六策略回測  {START} ~ {END}  共 {len(STOCK_LIST)} 檔")
    print(f"{'▓'*70}")

    all_results = []
    failed      = []

    for ticker in STOCK_LIST:
        print(f"[下載] {ticker:<10}", end=" ", flush=True)
        try:
            r = analyze(ticker)
            if r is None:
                print("無資料")
                failed.append(ticker)
            else:
                print(f"完成  ({r['n']}天  {r['bh_ret']*100:+.2f}%)")
                all_results.append(r)
        except Exception as ex:
            print(f"ERROR: {ex}")
            failed.append(ticker)

    # 逐檔詳細輸出
    for r in all_results:
        print_detail(r)

    # 最終排行榜
    if all_results:
        print_leaderboard(all_results)

    if failed:
        print(f"\n  下載失敗：{', '.join(failed)}")

    print(f"\n{'▓'*70}")
    print(f"  回測完成  (未含手續費 / 停損以收盤價計)")
    print(f"{'▓'*70}\n")
