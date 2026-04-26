"""
v8 候選方案：T1 黃金交叉的趨勢強度過濾

問題：M4 用 EMA120 過濾 T1 → 元晶 +480→-36（誤殺真趨勢轉變）
洞察：T1 是「趨勢轉變」信號，不該被歷史 EMA120 否定
       但死貓反彈的 T1 通常 ADX 仍弱（趨勢未確立）

方案測試：
  base    ── v7 原版（T1 ADX≥22）
  ADX25   ── T1 要求 ADX≥25（稍嚴）
  ADX28   ── T1 要求 ADX≥28（再嚴）
  ADX30   ── T1 要求 ADX≥30（嚴格）
  ADX25R  ── T1 要求 ADX≥25 且 RSI<70（避免搶高）
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import numpy as np, csv

bt.START = '2020-01-02'
bt.END   = '2026-04-25'
INVEST   = 100_000

v7_map = {}
try:
    with open('tw_all_results_20260426.csv', encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            v7_map[r['ticker']] = float(r['ret_t7'])
except: pass


def run_variant(ticker, adx_th=22, rsi_max=999):
    df = bt.download(ticker)
    if df is None or df.empty: return None
    df = bt.calc_ind(df)
    df = df[df.index >= bt.START].copy()
    if len(df) < 60: return None

    dates = df.index.tolist()
    pr    = df['Close'].values
    e20   = df['e20'].values
    e60   = df['e60'].values
    e120  = df['e120'].values
    adx_v = df['adx'].values
    rsi   = df['rsi'].values
    atr_v = df['atr'].values
    n     = len(pr)

    def e7_en(i):
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i], adx_v[i]])): return False
        # 基本門檻 ADX≥22 用於 T3，但 T1 另有更嚴格門檻
        if not (e20[i] > e60[i] and adx_v[i] >= 22): return False
        # T1
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                # T1 專屬過濾
                if adx_v[i] < adx_th: return False
                if not np.isnan(rsi[i]) and rsi[i] >= rsi_max: return False
                return True
        # T3 保留 v7 原樣
        if i < 60: return False
        if np.isnan(e120[i]) or np.isnan(e120[i-60]) or e120[i-60] == 0: return False
        if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 < -2.0: return False
        if not np.isnan(rsi[i]) and rsi[i] < 50:
            return True
        return False

    def _ex_highvol(i):
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i]])): return False
        return e20[i] < e60[i]
    def _ex_lock2(i):
        if i < 1: return False
        if any(np.isnan([e60[i], e120[i]])): return False
        return e60[i] < e120[i]
    def _ex_stable(i):
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i]])): return False
        if e20[i] < e60[i]: return True
        if not np.isnan(adx_v[i]) and adx_v[i] < 25:
            if not np.isnan(rsi[i]) and rsi[i] > 75:
                return True
        return False

    trades = []
    in_mkt = False
    ep = ed = stop_p = ex_fn = None
    is_hv = False

    for i in range(1, n):
        if not in_mkt:
            if e7_en(i):
                in_mkt = True
                ep, ed = pr[i], dates[i]
                _atr = atr_v[i] if not np.isnan(atr_v[i]) else pr[i]*0.03
                rel = _atr / pr[i] * 100 if pr[i] > 0 else 0
                if rel > 3.5:
                    is_hv = True; stop_p = None; ex_fn = _ex_highvol
                else:
                    is_hv = False
                    _adx  = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
                    stop_p = pr[i] - _atr * (3.0 if _adx >= 30 else 2.5)
                    ex_fn = _ex_stable
        else:
            if not is_hv:
                td = (dates[i] - ed).days
                fp = (pr[i] - ep) / ep * 100
                if i >= 120 and td > 200 and fp > 50:
                    _en = e120[i]; _ep = e120[i-120]
                    cur_ex = _ex_lock2 if (not any(np.isnan([_en,_ep])) and _en > _ep) else ex_fn
                else:
                    cur_ex = ex_fn
            else:
                cur_ex = ex_fn

            hit_stop = (stop_p is not None) and (pr[i] < stop_p)
            if hit_stop or cur_ex(i):
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

    try:
        full   = bt.analyze(ticker)
        t4_pnl = sum(t['pnl'] for t in full.get('t7b', []))
    except:
        t4_pnl = 0
    total = sum(t['pnl'] for t in trades) + t4_pnl
    return total / INVEST * 100, len(trades)


# 模式: (label, adx_th, rsi_max)
MODES = [
    ('base',   22, 999),
    ('ADX25',  25, 999),
    ('ADX28',  28, 999),
    ('ADX30',  30, 999),
    ('ADX25R', 25, 70),
]

test_groups = [
    ("【死亡迴圈（目標改善）】",
     ['1732','4133','2939','5203','4943','6598','6657','3041','2642']),
    ("【好股票（必須不變差）】",
     ['6139','4961','2609','3017','6442','2317','2330','2454','6443','2485','3035','2404','3661','2368']),
]

hdr = f"{'代號':<7} {'v7%':>7}  " + "  ".join(f"{m[0]:>8}" for m in MODES)
print(hdr); print('-'*70)

grand = {m[0]: [] for m in MODES}

for label, tickers in test_groups:
    print(f'\n{label}')
    grp = {m[0]: [] for m in MODES}
    for ticker in tickers:
        v7ref = v7_map.get(ticker, float('nan'))
        row = f"  {ticker:<7} {v7ref:>+7.1f}%"
        for mname, adx_th, rsi_max in MODES:
            res = run_variant(ticker, adx_th=adx_th, rsi_max=rsi_max)
            if res:
                pct, _ = res
                diff = pct - v7ref if v7ref==v7ref else float('nan')
                flag = '↑' if diff > 5 else ('↓' if diff < -5 else ' ')
                row += f"  {pct:>+7.1f}{flag}"
                grp[mname].append(pct); grand[mname].append(pct)
            else:
                row += "       N/A"
        print(row)
    avg_row = f"  {'平均':<7} {'':>7} "
    for mname, *_ in MODES:
        vals = grp[mname]
        avg_row += f"  {np.mean(vals):>+7.1f} " if vals else "       N/A"
    print(avg_row)

print(f'\n{"全體平均":<7} {"":>7} ' +
      "  ".join(f"  {np.mean(grand[m[0]]):>+7.1f} " for m in MODES))
