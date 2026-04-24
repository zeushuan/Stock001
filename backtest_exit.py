"""
出場條件比較回測  2020-01-02 ~ 2026-04-25
測試 8 種出場策略 × 13 檔，找最佳出場條件組合
進場邏輯固定：⑦自適應趨勢 T1/T2/T3（ADX≥22）
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import warnings; warnings.filterwarnings("ignore")

import yfinance as yf
import pandas as pd
import numpy as np
from ta.trend import EMAIndicator, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
from datetime import timedelta

START  = "2020-01-02"
END    = "2026-04-25"
INVEST = 100_000

SYMBOL_ALIASES = {
    "DJI":"^DJI","SPX":"^GSPC","NDX":"^NDX",
}
STOCK_LIST = [
    "DJI","SPX",
    "0050","2330","00922","00981A","00737",
    "BOTZ",
    "1711","8021","3167","8064",
    "00632R",
]

def is_tw(t):
    import re
    return bool(re.match(r'^\d+[A-Z]?$', t))

def yf_sym(t):
    if t in SYMBOL_ALIASES: return SYMBOL_ALIASES[t]
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
    if df.empty and sym.endswith(".TW"):
        sym2 = ticker + ".TWO"
        df   = yf.download(sym2, start=s, end=e, auto_adjust=True, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
    return df

def calc_ind(df):
    c, h, l = df["Close"], df["High"], df["Low"]
    df["e20"]  = EMAIndicator(c, 20).ema_indicator()
    df["e60"]  = EMAIndicator(c, 60).ema_indicator()
    df["rsi"]  = RSIIndicator(c, 14).rsi()
    df["adx"]  = ADXIndicator(h, l, c, 14).adx()
    df["atr"]  = AverageTrueRange(h, l, c, 14).average_true_range()
    return df

# ─────────────────────────────────────────────────────────────
# 回測引擎（支援追蹤停損、獲利目標）
# ─────────────────────────────────────────────────────────────
def run_bt_flex(dates, prices, atr_arr, entry_fn, exit_fn,
                atr_mult=2.5,         # 初始ATR停損倍數
                trail_mult=None,      # 追蹤停損倍數（從最高點回落）；None=不用
                profit_target=None):  # 固定獲利目標（如0.20=+20%）；None=不用
    """
    彈性回測引擎：
    - atr_mult:     進場時設初始停損 = 進場價 - ATR×atr_mult
    - trail_mult:   持倉期間追蹤最高收盤，當 close < peak - ATR×trail_mult 即出場
    - profit_target:達到 +profit_target% 立即出場獲利了結
    三種停損/出場以「最先觸發」為準。
    """
    trades = []
    in_mkt = False
    ep = ed = stop_p = peak = None
    n = len(prices)

    for i in range(1, n):
        if not in_mkt:
            if entry_fn(i):
                in_mkt = True
                ep, ed = prices[i], dates[i]
                atr_now = atr_arr[i] if not np.isnan(atr_arr[i]) else prices[i] * 0.03
                stop_p  = ep - atr_now * atr_mult
                peak    = ep
        else:
            # 更新追蹤最高點
            if prices[i] > peak:
                peak = prices[i]

            # 判斷出場觸發
            hit_init_stop  = (stop_p is not None) and (prices[i] < stop_p)

            hit_trail_stop = False
            if trail_mult is not None and not np.isnan(atr_arr[i]):
                trail_stop_p = peak - atr_arr[i] * trail_mult
                hit_trail_stop = prices[i] < trail_stop_p

            hit_profit = False
            if profit_target is not None and ep > 0:
                hit_profit = (prices[i] / ep - 1) >= profit_target

            hit_exit_fn = exit_fn(i)

            if hit_init_stop or hit_trail_stop or hit_profit or hit_exit_fn:
                reason = ("停損" if hit_init_stop else
                          "追蹤停損" if hit_trail_stop else
                          "獲利了結" if hit_profit else "出場訊號")
                r = (prices[i] - ep) / ep
                trades.append(dict(
                    ed=ed, xd=dates[i], ep=ep, xp=prices[i],
                    ret=r, pnl=INVEST*r,
                    days=(dates[i]-ed).days,
                    stop=bool(hit_init_stop),
                    trail=bool(hit_trail_stop),
                    profit=bool(hit_profit),
                    open=False,
                    reason=reason,
                ))
                in_mkt = False

    if in_mkt:
        r = (prices[-1] - ep) / ep
        trades.append(dict(
            ed=ed, xd=dates[-1], ep=ep, xp=prices[-1],
            ret=r, pnl=INVEST*r,
            days=(dates[-1]-ed).days,
            stop=False, trail=False, profit=False, open=True, reason="持倉中",
        ))
    return trades

def pbar(pnl, scale=500):
    if pnl is None or (isinstance(pnl, float) and np.isnan(pnl)):
        return "N/A"
    n = min(int(abs(pnl)/scale), 16)
    return ("+" + "█"*n) if pnl >= 0 else ("-" + "▒"*n)

# ─────────────────────────────────────────────────────────────
# 8種出場策略定義
# ─────────────────────────────────────────────────────────────
EXIT_VARIANTS = [
    # (代號,              名稱,                         atr_mult, trail_mult, profit_target, rsi_exit_th, adx_rsi_mode)
    ("V0_現況",   "V0 死叉+ADX<25→RSI>70+ATR2.5",      2.5, None, None, None,  "adx_rsi"),  # 現況
    ("V1_RSI75",  "V1 死叉+RSI>75+ATR2.5",              2.5, None, None, 75,    "rsi_any"),  # RSI>75出場
    ("V2_RSI70",  "V2 死叉+RSI>70+ATR2.5",              2.5, None, None, 70,    "rsi_any"),  # RSI>70出場
    ("V3_RSI65",  "V3 死叉+RSI>65+ATR2.5",              2.5, None, None, 65,    "rsi_any"),  # RSI>65出場
    ("V4_TP20",   "V4 死叉+獲利≥20%+ATR2.5",            2.5, None, 0.20, None,  "adx_rsi"),  # 固定+20%獲利
    ("V5_TP30",   "V5 死叉+獲利≥30%+ATR2.5",            2.5, None, 0.30, None,  "adx_rsi"),  # 固定+30%獲利
    ("V6_Trail2", "V6 死叉+追蹤ATR×2+初始ATR2.5",        2.5, 2.0,  None, None,  "adx_rsi"),  # 追蹤停損
    ("V7_NoStop", "V7 死叉出場(無ATR停損)",               99,  None, None, None,  "adx_rsi"),  # 無停損，純死叉
]

# ─────────────────────────────────────────────────────────────
def analyze(ticker):
    df = download(ticker)
    if df.empty or "Close" not in df.columns: return None
    df = calc_ind(df)
    mask = (df.index >= START) & (df.index <= END)
    sub  = df[mask].copy()
    sub  = sub[sub["Close"].notna()]
    if len(sub) < 10: return None

    dates = list(sub.index)
    pr    = sub["Close"].values.astype(float)
    e20   = sub["e20"].values.astype(float)
    e60   = sub["e60"].values.astype(float)
    rsi   = sub["rsi"].values.astype(float)
    adx_v = sub["adx"].values.astype(float)
    atr_v = sub["atr"].values.astype(float)

    # ── 進場：⑦ T1/T2/T3（ADX≥22）────────────────────────────
    def e7_en(i):
        if any(np.isnan([e20[i], e60[i], adx_v[i]])): return False
        if not (e20[i] > e60[i] and adx_v[i] >= 22):  return False
        if i < 1: return False
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]: return True  # T1 黃金交叉
        if i == 1 and not np.isnan(rsi[i]) and rsi[i] < 65: return True
        if not np.isnan(rsi[i]) and rsi[i] < 50: return True           # T3 拉回
        return False

    # ── 出場：依 rsi_exit_th / adx_rsi_mode 建立不同版本 ─────
    def make_exit_fn(rsi_th, adx_rsi_mode):
        def _ex(i):
            if i < 1: return False
            if any(np.isnan([e20[i], e60[i]])): return False
            if e20[i] < e60[i]: return True                           # 死亡交叉
            if rsi_th is not None and not np.isnan(rsi[i]):
                if adx_rsi_mode == "rsi_any":
                    if rsi[i] > rsi_th: return True                   # RSI任意超過門檻
                elif adx_rsi_mode == "adx_rsi":
                    if not np.isnan(adx_v[i]) and adx_v[i] < 25:
                        if rsi[i] > rsi_th: return True               # 弱趨勢RSI超買
            return False
        return _ex

    results = {}
    bh_ret  = (pr[-1] - pr[0]) / pr[0]
    results["bh"] = {"pnl": INVEST*bh_ret, "ret": bh_ret, "trades": []}

    for code, name, atr_m, trail_m, tp, rsi_th, mode in EXIT_VARIANTS:
        exit_fn = make_exit_fn(rsi_th, mode)
        trades  = run_bt_flex(dates, pr, atr_v, e7_en, exit_fn,
                              atr_mult=atr_m, trail_mult=trail_m, profit_target=tp)
        pnl = sum(t["pnl"] for t in trades)
        results[code] = {
            "pnl": pnl, "ret": pnl/INVEST,
            "trades": trades, "n": len(trades),
            "days": sum(t["days"] for t in trades),
            "name": name,
        }

    return dict(
        ticker=ticker, cur=currency(ticker),
        n=len(pr), p0=pr[0], p1=pr[-1],
        bh_ret=bh_ret, bh_pnl=INVEST*bh_ret,
        results=results,
        rsi=rsi, dates=dates,
        bull_days=int((pd.Series(e20)>pd.Series(e60)).sum()),
    )

# ─────────────────────────────────────────────────────────────
def print_detail(r):
    ticker = r["ticker"]
    W = 76
    print(f"\n{'='*W}")
    print(f"  {ticker:<10} {START}~{END}  ({r['n']}交易日) | {r['cur']}")
    print(f"  首日:{r['p0']:.2f}  末日:{r['p1']:.2f}  "
          f"漲跌:{r['bh_ret']*100:+.2f}%  EMA多頭:{r['bull_days']}天")
    print(f"{'='*W}")
    print(f"  {'策略':<32} {'筆':>3}  {'在市天':>6}  {'損益':>10}  {'報酬%':>8}")
    print(f"  {'-'*60}")

    res   = r["results"]
    # 計算最佳（排除持有，只看主動策略）
    best_pnl = max(v["pnl"] for k, v in res.items() if k != "bh")

    # 買入持有
    bh = res["bh"]
    print(f"  {'① 買入持有':<32} {'1':>3}  {r['n']:>6}  {bh['pnl']:>+10.0f}  {r['bh_ret']*100:>+7.2f}%")

    for code, name, *_ in EXIT_VARIANTS:
        v = res[code]
        mark = "  ◀最佳" if abs(v["pnl"] - best_pnl) < 0.01 else ""
        print(f"  {name:<32} {v['n']:>3}  {v['days']:>6}  {v['pnl']:>+10.0f}  "
              f"{v['ret']*100:>+7.2f}%{mark}")

    # 逐筆明細：只印最佳主動策略
    best_code = max(
        [(code, res[code]["pnl"]) for code, *_ in EXIT_VARIANTS],
        key=lambda x: x[1]
    )[0]
    bv = res[best_code]
    print(f"\n  [最佳: {bv['name']}]")
    rsi_arr = r["rsi"]
    idx_map = {d: i for i, d in enumerate(r["dates"])}
    cum = 0.0
    for t in bv["trades"]:
        cum += t["pnl"]
        ei = idx_map.get(t["ed"])
        xi = idx_map.get(t["xd"])
        re_ = f"{rsi_arr[ei]:.0f}" if ei is not None and not np.isnan(rsi_arr[ei]) else "--"
        rx_ = f"{rsi_arr[xi]:.0f}" if xi is not None and not np.isnan(rsi_arr[xi]) else "--"
        suf = "[持]" if t["open"] else f"[{t['reason']}]" if not t["open"] and t["reason"] != "出場訊號" else ""
        print(f"    {t['ed'].strftime('%m/%d')}→{t['xd'].strftime('%m/%d')}"
              f"  進:{t['ep']:.2f} 出:{t['xp']:.2f}"
              f"  RSI:{re_}→{rx_}"
              f"  {t['ret']*100:>+6.2f}%  {t['pnl']:>+8.0f}  {pbar(t['pnl'])} {suf}")
    print(f"    {'累計':>44} {cum:>+8.0f}")

# ─────────────────────────────────────────────────────────────
def print_leaderboard(all_results):
    codes  = [c for c, *_ in EXIT_VARIANTS]
    names  = [c for c, *_ in EXIT_VARIANTS]
    W = 120

    print(f"\n\n{'▓'*W}")
    print(f"  出場條件比較排行榜  {START}~{END}  本金 {INVEST:,}")
    print(f"{'▓'*W}")

    # 標題行
    hdr = f"  {'代號':<8} {'幣':>4} {'漲跌%':>7}  {'①持有':>8}"
    for code, *_ in EXIT_VARIANTS:
        hdr += f"  {code:>10}"
    hdr += "  最佳主動"
    print(hdr)
    print(f"  {'-'*(W-2)}")

    # 資料行
    rows = []
    for r in all_results:
        res = r["results"]
        pnls = {code: res[code]["pnl"] for code, *_ in EXIT_VARIANTS}
        best_code = max(pnls, key=pnls.get)
        rows.append((r, pnls, best_code))
    rows.sort(key=lambda x: x[0]["bh_ret"], reverse=True)

    for r, pnls, best_code in rows:
        line = (f"  {r['ticker']:<8} {r['cur']:>4} {r['bh_ret']*100:>+7.2f}%"
                f"  {r['bh_pnl']/INVEST*100:>+7.2f}%")
        for code, *_ in EXIT_VARIANTS:
            v = pnls[code]
            line += f"  {v/INVEST*100:>+9.2f}%"
        line += f"  {best_code}"
        print(line)

    # 各策略平均
    print(f"\n  ── 各出場策略平均報酬（{len(all_results)}檔）──")
    avgs = []
    for code, name_long, *_ in EXIT_VARIANTS:
        vals = [r["results"][code]["pnl"]/INVEST*100 for r in all_results]
        avg  = np.mean(vals)
        mx   = max(vals)
        mn   = min(vals)
        avgs.append((code, name_long, avg, mx, mn))

    # 按平均報酬排序
    avgs.sort(key=lambda x: x[2], reverse=True)
    print(f"  {'代號':<12} {'均值':>8}  {'最高':>8}  {'最低':>8}  說明")
    print(f"  {'-'*70}")
    for i, (code, name_long, avg, mx, mn) in enumerate(avgs):
        mark = " ◀" if i == 0 else ""
        print(f"  {code:<12} {avg:>+7.2f}%  {mx:>+7.2f}%  {mn:>+7.2f}%  {name_long}{mark}")

    # 勝出次數
    print(f"\n  ── 各策略最佳次數 ──")
    from collections import Counter
    cnt = Counter(best_code for _, _, best_code in rows)
    for code, c in cnt.most_common():
        print(f"  {code}：{c} 次")

# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{'▓'*70}")
    print(f"  出場條件回測  {START}~{END}  進場：⑦T1/T2/T3 ADX≥22")
    print(f"  測試 {len(EXIT_VARIANTS)} 種出場條件 × {len(STOCK_LIST)} 檔股票")
    print(f"{'▓'*70}\n")

    all_results = []
    for ticker in STOCK_LIST:
        print(f"[下載] {ticker:<10}", end=" ", flush=True)
        try:
            r = analyze(ticker)
            if r is None:
                print("無資料")
            else:
                print(f"完成  ({r['n']}天  {r['bh_ret']*100:+.2f}%)")
                all_results.append(r)
        except Exception as ex:
            print(f"ERROR: {ex}")

    for r in all_results:
        print_detail(r)

    print_leaderboard(all_results)

    print(f"\n{'▓'*70}")
    print(f"  回測完成  (未含手續費 / 停損以收盤價計)")
    print(f"{'▓'*70}\n")
