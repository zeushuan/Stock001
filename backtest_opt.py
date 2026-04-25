"""
⑦ 自適應趨勢[新] 優化回測  2020-01-02 ~ 2026-04-25
測試 10 種參數組合，含 V7 自動分類版（依進場時 ATR/Price 動態切換規則）

V0  現況基準   ATR×2.5 固定 / RSI>70出場(ADX<25) / T4 RSI<35 連2天
V1  RSI→75    出場門檻 70→75，減少過早出場
V2  動態ATR    ADX≥30時 ATR×3.0，強趨勢給更多空間
V3  T4嚴格    T4進場條件 RSI<28+連3天上升，提高勝率
V4  組合優化   V1+V2+V3 全合一
V5  無RSI出場  移除ADX<25→RSI出場，只留EMA死叉
V6  無ATR停損  移除ATR停損，只用EMA死叉出場

V7a 自動ATR3%  進場時 ATR/Price>3.0% → V5規則；其餘→V4規則
V7b 自動ATR35  進場時 ATR/Price>3.5% → V6規則(無停損)；其餘→V4規則
V7c 自動三階   ATR>3% AND EMA乖離>8% → V6；ATR>3% → V5；其餘→V4
"""
import warnings; warnings.filterwarnings("ignore")
import backtest_all as bt
import numpy as np
import pandas as pd

bt.START = "2020-01-02"
bt.END   = "2026-04-25"
INVEST   = bt.INVEST

# ── 全部標的：原清單 + 隨機清單（去重）────────────────────────
_BASE = list(bt.STOCK_LIST)
_RAND = ["2317","2454","2382","2303","00878","6669",
         "NVDA","TSLA","AAPL","META","AMD","PLTR"]
_seen = set()
ALL_STOCKS = []
for s in _BASE + _RAND:
    if s not in _seen:
        _seen.add(s)
        ALL_STOCKS.append(s)

# ── 優化版本定義（dict格式，支援classify_fn）──────────────────
# classify_fn(i, pr, e20, e60, rsi, adx_v, atr_v, mom60)
#   -> None（用全域參數）或 (rsi_th, mult, no_stop)
#
# rsi_exit_thresh : None = 不用RSI出場
# atr_mult_fn     : function(adx) -> float
# no_atr_stop     : True = 完全不設ATR停損

def _v4_params():  return (75, 3.0 if False else 2.5, False)   # placeholder；實際在classify中用
def _v5_params():  return (None, 2.5, False)
def _v6_params():  return (70,  2.5, True)

def _make_clf_v7a(thresh=3.0):
    """ATR/Price > thresh% → V5規則；否則 V4規則"""
    def clf(i, pr, e20, e60, rsi, adx_v, atr_v, mom60):
        rel = atr_v[i] / pr[i] * 100 if not np.isnan(atr_v[i]) and pr[i] > 0 else 0
        if rel > thresh:
            return (None, 2.5, False)          # V5：無RSI出場
        else:
            adx = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
            return (75, 3.0 if adx >= 30 else 2.5, False)   # V4
    return clf

def _make_clf_v7b(thresh=3.5):
    """ATR/Price > thresh% → V6規則(無停損)；否則 V4規則"""
    def clf(i, pr, e20, e60, rsi, adx_v, atr_v, mom60):
        rel = atr_v[i] / pr[i] * 100 if not np.isnan(atr_v[i]) and pr[i] > 0 else 0
        if rel > thresh:
            return (None, 2.5, True)           # V6：無停損無RSI出場
        else:
            adx = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
            return (75, 3.0 if adx >= 30 else 2.5, False)   # V4
    return clf

def _make_clf_v7c(atr_thresh=3.0, ema_thresh=8.0):
    """三階分類：
      ATR>3% AND EMA乖離>8%  → V6（飆股強趨勢，只守EMA死叉）
      ATR>3%                 → V5（高波動，移除RSI出場）
      其餘                   → V4（穩健股，RSI>75+動態ATR）
    """
    def clf(i, pr, e20, e60, rsi, adx_v, atr_v, mom60):
        rel = atr_v[i] / pr[i] * 100 if not np.isnan(atr_v[i]) and pr[i] > 0 else 0
        ema_gap = (e20[i] - e60[i]) / e60[i] * 100 if (
            not np.isnan(e20[i]) and not np.isnan(e60[i]) and e60[i] > 0) else 0
        if rel > atr_thresh and ema_gap > ema_thresh:
            return (None, 2.5, True)           # V6
        elif rel > atr_thresh:
            return (None, 2.5, False)          # V5
        else:
            adx = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
            return (75, 3.0 if adx >= 30 else 2.5, False)   # V4
    return clf

# ── V8 雙重門檻（ATR/P + 60日動能）─────────────────────────────
def _make_clf_v8a(atr_thresh=3.5, mom_thresh=20.0):
    """ATR/P > 3.5% AND 60日漲幅 > 20% → V6（無停損無RSI）；否則 V4
    目標：過濾掉 1711/6669 這類穩定大漲股（動能不夠快）
    """
    def clf(i, pr, e20, e60, rsi, adx_v, atr_v, mom60):
        rel = atr_v[i] / pr[i] * 100 if not np.isnan(atr_v[i]) and pr[i] > 0 else 0
        m60 = mom60[i] if not np.isnan(mom60[i]) else 0
        if rel > atr_thresh and m60 > mom_thresh:
            return (None, 2.5, True)           # V6：飆股，純EMA死叉
        else:
            adx = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
            return (75, 3.0 if adx >= 30 else 2.5, False)   # V4
    return clf

def _make_clf_v8b(atr_thresh=3.5, mom_thresh=20.0):
    """三階 + 動能：
      ATR>3.5% AND mom60>20%  → V6（無停損無RSI，飆股）
      ATR>3.5%               → V5（高波動但動能弱，移除RSI出場保護）
      其餘                   → V4（穩健，RSI>75+動態ATR）
    """
    def clf(i, pr, e20, e60, rsi, adx_v, atr_v, mom60):
        rel = atr_v[i] / pr[i] * 100 if not np.isnan(atr_v[i]) and pr[i] > 0 else 0
        m60 = mom60[i] if not np.isnan(mom60[i]) else 0
        if rel > atr_thresh and m60 > mom_thresh:
            return (None, 2.5, True)           # V6
        elif rel > atr_thresh:
            return (None, 2.5, False)          # V5
        else:
            adx = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
            return (75, 3.0 if adx >= 30 else 2.5, False)   # V4
    return clf

def _make_clf_v8c(atr_thresh=3.5, mom_thresh=30.0):
    """更嚴格門檻：60日動能 > 30% 才觸發V6，否則V4
    過濾力更強，只留真正爆發型飆股
    """
    def clf(i, pr, e20, e60, rsi, adx_v, atr_v, mom60):
        rel = atr_v[i] / pr[i] * 100 if not np.isnan(atr_v[i]) and pr[i] > 0 else 0
        m60 = mom60[i] if not np.isnan(mom60[i]) else 0
        if rel > atr_thresh and m60 > mom_thresh:
            return (None, 2.5, True)           # V6
        else:
            adx = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
            return (75, 3.0 if adx >= 30 else 2.5, False)   # V4
    return clf

VARIANTS = [
    # name,          rsi_th, mult_fn,                              t4_rsi, t4_con, no_stop, classify_fn
    ("V0 現況",       70,   lambda a: 2.5,                          35, 2, False, None),
    ("V1 RSI→75",    75,   lambda a: 2.5,                          35, 2, False, None),
    ("V2 動態ATR",    70,   lambda a: 3.0 if a >= 30 else 2.5,     35, 2, False, None),
    ("V3 T4嚴格",    70,   lambda a: 2.5,                          28, 3, False, None),
    ("V4 組合優化",   75,   lambda a: 3.0 if a >= 30 else 2.5,     28, 3, False, None),
    ("V5 無RSI出場", None,  lambda a: 2.5,                          35, 2, False, None),
    ("V6 無ATR停損",  70,   lambda a: 2.5,                          35, 2, True,  None),
    # ── V7 自動分類（ATR/P 單一門檻）──────────────────────────────
    ("V7a ATR3%",    70,   lambda a: 2.5,                          28, 3, False, _make_clf_v7a(3.0)),
    ("V7b ATR3.5%",  70,   lambda a: 2.5,                          28, 3, False, _make_clf_v7b(3.5)),
    ("V7c 三階",      70,   lambda a: 2.5,                          28, 3, False, _make_clf_v7c(3.0, 8.0)),
    # ── V8 雙重門檻（ATR/P + 60日動能）────────────────────────────
    ("V8a 雙重20%",  70,   lambda a: 2.5,                          28, 3, False, _make_clf_v8a(3.5, 20.0)),
    ("V8b 三階20%",  70,   lambda a: 2.5,                          28, 3, False, _make_clf_v8b(3.5, 20.0)),
    ("V8c 嚴格30%",  70,   lambda a: 2.5,                          28, 3, False, _make_clf_v8c(3.5, 30.0)),
]

# ── 彈性版 ⑦ 回測引擎（支援 classify_fn）──────────────────────
def run_v7_opt(dates, pr, e20, e60, rsi, adx_v, atr_v, mom60,
               rsi_exit_thresh=70,
               atr_mult_fn=None,
               t4_rsi_thresh=35,
               t4_consec=2,
               no_atr_stop=False,
               classify_fn=None):
    n = len(pr)

    # ── 多頭主策略進場 ──
    def en_main(i):
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i], adx_v[i]])): return False
        if not (e20[i] > e60[i] and adx_v[i] >= 22): return False
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]: return True  # T1
        if i == 1 and not np.isnan(rsi[i]) and rsi[i] < 65: return True  # T2
        if not np.isnan(rsi[i]) and rsi[i] < 50: return True             # T3
        return False

    def make_ex_main(rsi_th, no_stop_flag):
        def ex(i):
            if i < 1: return False
            if any(np.isnan([e20[i], e60[i]])): return False
            if e20[i] < e60[i]: return True                    # EMA死叉
            if rsi_th is not None:
                if not np.isnan(adx_v[i]) and adx_v[i] < 25:
                    if not np.isnan(rsi[i]) and rsi[i] > rsi_th:
                        return True
            return False
        return ex

    # ── T4 空頭反彈進場 ──
    def en_t4(i):
        if i < t4_consec: return False
        for k in range(t4_consec + 1):
            if np.isnan(rsi[i - k]): return False
        if any(np.isnan([e20[i], e60[i]])): return False
        if e20[i] > e60[i]: return False
        if rsi[i] >= t4_rsi_thresh: return False
        for k in range(t4_consec):
            if rsi[i - k] <= rsi[i - k - 1]: return False
        return True

    def ex_t4(i):
        if any(np.isnan([rsi[i], e20[i], e60[i]])): return False
        if rsi[i] > 55: return True
        if i > 0 and not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] < e60[i-1] and e20[i] >= e60[i]: return True
        return False

    # ── 回測核心（動態classify_fn）──
    def _run_main():
        trades = []
        in_mkt, ep, ed, stop_p, ex_fn = False, None, None, None, None
        for i in range(1, n):
            if not in_mkt:
                if en_main(i):
                    in_mkt, ep, ed = True, pr[i], dates[i]
                    # 決定本筆交易的規則
                    if classify_fn is not None:
                        params = classify_fn(i, pr, e20, e60, rsi, adx_v, atr_v, mom60)
                    else:
                        params = None
                    if params is not None:
                        _rsi_th, _mult, _no_stop = params
                    else:
                        _rsi_th  = rsi_exit_thresh
                        _adx_now = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
                        _mult    = atr_mult_fn(_adx_now) if atr_mult_fn else 2.5
                        _no_stop = no_atr_stop

                    ex_fn = make_ex_main(_rsi_th, _no_stop)
                    if _no_stop:
                        stop_p = None
                    else:
                        _atr = atr_v[i] if not np.isnan(atr_v[i]) else pr[i] * 0.03
                        stop_p = pr[i] - _atr * _mult
            else:
                hit_stop = (stop_p is not None) and (pr[i] < stop_p)
                if hit_stop or ex_fn(i):
                    r = (pr[i] - ep) / ep
                    trades.append(dict(ed=ed, xd=dates[i], ep=ep, xp=pr[i],
                                       ret=r, pnl=INVEST*r,
                                       days=(dates[i]-ed).days,
                                       stop=bool(hit_stop), open=False))
                    in_mkt = False
        if in_mkt:
            r = (pr[-1] - ep) / ep
            trades.append(dict(ed=ed, xd=dates[-1], ep=ep, xp=pr[-1],
                               ret=r, pnl=INVEST*r,
                               days=(dates[-1]-ed).days,
                               stop=False, open=True))
        return trades

    def _run_t4():
        trades = []
        in_mkt, ep, ed, stop_p = False, None, None, None
        for i in range(1, n):
            if not in_mkt:
                if en_t4(i):
                    in_mkt, ep, ed = True, pr[i], dates[i]
                    _atr = atr_v[i] if not np.isnan(atr_v[i]) else pr[i] * 0.03
                    stop_p = pr[i] - _atr * 2.0
            else:
                hit_stop = (stop_p is not None) and (pr[i] < stop_p)
                if hit_stop or ex_t4(i):
                    r = (pr[i] - ep) / ep
                    trades.append(dict(ed=ed, xd=dates[i], ep=ep, xp=pr[i],
                                       ret=r, pnl=INVEST*r,
                                       days=(dates[i]-ed).days,
                                       stop=bool(hit_stop), open=False))
                    in_mkt = False
        if in_mkt:
            r = (pr[-1] - ep) / ep
            trades.append(dict(ed=ed, xd=dates[-1], ep=ep, xp=pr[-1],
                               ret=r, pnl=INVEST*r,
                               days=(dates[-1]-ed).days,
                               stop=False, open=True))
        return trades

    return _run_main(), _run_t4()


# ── 主程式 ────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{'▓'*90}")
    print(f"  ⑦自適應趨勢[新] 優化回測  {bt.START} ~ {bt.END}  共 {len(ALL_STOCKS)} 檔")
    print(f"{'▓'*90}")

    results  = {}   # ticker -> [ret_v0, ..., ret_v9]
    all_data = {}   # ticker -> {n, bh_ret, cur, avg_rel_atr}

    for ticker in ALL_STOCKS:
        print(f"[下載] {ticker:<10}", end=" ", flush=True)
        try:
            df = bt.download(ticker)
            if df.empty or "Close" not in df.columns:
                print("無資料"); continue
            df = bt.calc_ind(df)
            mask = (df.index >= bt.START) & (df.index <= bt.END)
            sub  = df[mask].copy()
            sub  = sub[sub["Close"].notna()]
            if len(sub) < 20:
                print("資料不足"); continue

            dates  = list(sub.index)
            pr     = sub["Close"].values.astype(float)
            e20    = sub["e20"].values.astype(float)
            e60    = sub["e60"].values.astype(float)
            rsi    = sub["rsi"].values.astype(float)
            adx_v  = sub["adx"].values.astype(float)
            atr_v  = sub["atr"].values.astype(float)

            # 60日動能（滾動報酬）
            mom60  = np.full(len(pr), np.nan)
            for i in range(60, len(pr)):
                if pr[i-60] > 0:
                    mom60[i] = (pr[i] - pr[i-60]) / pr[i-60] * 100

            # 平均相對ATR（識別股性）
            rel_atr_arr = np.where(pr > 0, atr_v / pr * 100, np.nan)
            avg_rel_atr = float(np.nanmean(rel_atr_arr))

            bh_ret = (pr[-1] - pr[0]) / pr[0] * 100

            var_rets = []
            for (vname, rsi_th, mult_fn, t4_rsi, t4_con, no_stop, clf) in VARIANTS:
                t_main, t_t4 = run_v7_opt(
                    dates, pr, e20, e60, rsi, adx_v, atr_v, mom60,
                    rsi_exit_thresh=rsi_th,
                    atr_mult_fn=mult_fn,
                    t4_rsi_thresh=t4_rsi,
                    t4_consec=t4_con,
                    no_atr_stop=no_stop,
                    classify_fn=clf,
                )
                total_pnl = sum(t["pnl"] for t in t_main + t_t4)
                var_rets.append(total_pnl / INVEST * 100)

            results[ticker]  = var_rets
            all_data[ticker] = dict(n=len(sub), p0=pr[0], p1=pr[-1],
                                    bh_ret=bh_ret, cur=bt.currency(ticker),
                                    avg_rel_atr=avg_rel_atr)

            best_i = int(np.argmax(var_rets))
            vs_v0  = var_rets[best_i] - var_rets[0]
            v8a, v8b, v8c = var_rets[10], var_rets[11], var_rets[12]
            print(f"BH:{bh_ret:>+7.1f}%  ATR/P:{avg_rel_atr:.1f}%  "
                  f"V0:{var_rets[0]:>+6.1f}%  "
                  f"V8a:{v8a:>+6.1f}%  V8b:{v8b:>+6.1f}%  V8c:{v8c:>+6.1f}%  "
                  f"│最佳:{VARIANTS[best_i][0]}({vs_v0:>+.1f}%)")

        except Exception as ex:
            print(f"ERROR: {ex}")
            import traceback; traceback.print_exc()

    if not results:
        print("無任何結果"); exit()

    # ── 逐檔對比表（聚焦關鍵版本）──────────────────────────────────
    SHOW_VI = [0, 4, 5, 8, 10, 11, 12]  # V0, V4, V5, V7b, V8a, V8b, V8c
    show_vars = [VARIANTS[i] for i in SHOW_VI]

    print(f"\n\n{'='*120}")
    print(f"  逐檔比較（聚焦版）  ATR/P = 進場時ATR佔股價比率")
    print(f"{'='*120}")
    hdr  = f"  {'代號':<8} {'BH%':>8} {'ATR/P':>6} "
    hdr += "".join(f" {v[0]:>12}" for v in show_vars) + "  最佳版本"
    print(hdr)
    print(f"  {'-'*115}")

    sorted_t = sorted(results.keys(), key=lambda t: all_data[t]["bh_ret"], reverse=True)
    for ticker in sorted_t:
        d    = all_data[ticker]
        vrs  = results[ticker]
        best_i = int(np.argmax(vrs))
        vs_v0  = vrs[best_i] - vrs[0]
        show_v = [vrs[i] for i in SHOW_VI]
        row  = (f"  {ticker:<8} {d['bh_ret']:>+7.1f}% {d['avg_rel_atr']:>5.1f}% "
                + "".join(f" {v:>+11.1f}%" for v in show_v)
                + f"  {VARIANTS[best_i][0]}({vs_v0:>+.1f}%)")
        print(row)

    # ── 各版本平均報酬 ──────────────────────────────────────────
    print(f"\n{'─'*120}")
    print(f"  所有版本平均報酬（{len(results)} 檔）")
    print(f"{'─'*120}")
    all_bh = [all_data[t]["bh_ret"] for t in results]
    print(f"  {'①買入持有':<15}  均值:{np.mean(all_bh):>+7.1f}%  "
          f"最高:{max(all_bh):>+7.1f}%  最低:{min(all_bh):>+7.1f}%")
    avgs_all = [np.mean([results[t][vi] for t in results]) for vi in range(len(VARIANTS))]
    best_vi  = int(np.argmax(avgs_all))
    for vi, (vname, *_) in enumerate(VARIANTS):
        vals = [results[t][vi] for t in results]
        avg, hi, lo = np.mean(vals), max(vals), min(vals)
        vs0  = f"  Δ{avg - avgs_all[0]:>+.1f}%" if vi > 0 else ""
        star = "  ◀ 最佳" if vi == best_vi else ""
        print(f"  {vname:<15}  均值:{avg:>+7.1f}%  最高:{hi:>+7.1f}%  最低:{lo:>+7.1f}%{vs0}{star}")

    # ── V7 正確分類率分析 ───────────────────────────────────────
    print(f"\n{'─'*120}")
    print(f"  V7 自動分類效果分析")
    print(f"  （正確 = V7x 報酬 ≥ max(V4,V5) × 95%）")
    print(f"{'─'*120}")
    for vi_vx, vname_vx in [(7,"V7a ATR3%"),(8,"V7b ATR3.5%"),(9,"V7c 三階"),
                             (10,"V8a 雙重20%"),(11,"V8b 三階20%"),(12,"V8c 嚴格30%")]:
        correct = sum(
            1 for t in results
            if results[t][vi_vx] >= max(results[t][4], results[t][5]) * 0.95
        )
        print(f"  {vname_vx:<15}  {correct}/{len(results)} 檔達標"
              f"  ({correct/len(results)*100:.0f}%)")

    # ── 勝出次數 ────────────────────────────────────────────────
    print(f"\n{'─'*120}")
    from collections import Counter
    best_cnt = Counter(VARIANTS[int(np.argmax(results[t]))][0] for t in results)
    print("  各版本「最佳」勝出次數：")
    for vname, cnt in best_cnt.most_common():
        print(f"  {vname:<15}  {cnt:>3} 次  {'█'*cnt}")

    # ── 依 ATR/Price 分組 ───────────────────────────────────────
    print(f"\n{'─'*120}")
    print(f"  依平均 ATR/Price 分組比較")
    print(f"{'─'*120}")
    hi_vol  = [t for t in results if all_data[t]["avg_rel_atr"] >= 3.0]
    lo_vol  = [t for t in results if all_data[t]["avg_rel_atr"] <  3.0]
    for label, group in [("高波動(ATR/P≥3%)", hi_vol), ("低波動(ATR/P<3%)", lo_vol)]:
        if not group: continue
        avgs = [np.mean([results[t][vi] for t in group]) for vi in range(len(VARIANTS))]
        bv   = int(np.argmax(avgs))
        print(f"  [{label}({len(group)}檔)]")
        for vi in [0, 4, 5, 8, 10, 11, 12]:
            tag = " ◀" if vi == bv else ""
            print(f"    {VARIANTS[vi][0]:<15} {avgs[vi]:>+7.1f}%{tag}")

    print(f"\n{'▓'*90}")
    print(f"  優化回測完成  (未含手續費 / 停損以收盤價計)")
    print(f"{'▓'*90}\n")
