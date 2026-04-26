"""
v8 候選方案：智能化過濾（依 EMA120 趨勢）

問題：簡單冷卻誤殺好股票（4961 天鈺 -666%）
原因：好股票停損後常因 EMA120 上升 → 真正的恢復再進場（不該冷卻）

方案：
  M1 — EMA120 條件冷卻：停損後若 EMA120 仍下跌 → 60天冷卻；上升 → 不冷卻
  M2 — T3 EMA120 嚴格化：原本 60日跌幅<2% → 改為 60日不跌（≥0%）
  M3 — M1 + M2 組合
  M4 — T3 EMA120 嚴格化 + T1 也要 EMA120 不跌（真正的趨勢轉換）
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


def run_variant(ticker, mode='base'):
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

    use_M1 = mode in ('M1', 'M3')      # 條件冷卻
    t3_strict = 0.0  if mode in ('M2', 'M3', 'M4') else -2.0   # T3 EMA120 門檻
    t1_filter = (mode == 'M4')          # T1 也要 EMA120 不跌

    def ema120_ok_for_t1(i):
        """T1 過濾：EMA120 60日不跌"""
        if i < 60: return True   # 歷史不足 → 放行
        if np.isnan(e120[i]) or np.isnan(e120[i-60]) or e120[i-60] == 0:
            return True
        pct = (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100
        return pct >= 0

    def e7_en(i):
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i], adx_v[i]])): return False
        if not (e20[i] > e60[i] and adx_v[i] >= 22): return False
        # T1
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                if t1_filter and not ema120_ok_for_t1(i):
                    return False
                return True
        # T3
        if i < 60: return False
        if np.isnan(e120[i]) or np.isnan(e120[i-60]) or e120[i-60] == 0: return False
        pct = (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100
        if pct < t3_strict: return False
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
    cooldown_until = -1

    for i in range(1, n):
        if not in_mkt:
            if i <= cooldown_until:
                continue
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

                # M1 條件冷卻：停損 + EMA120 仍下跌 → 60天冷卻
                if use_M1 and hit_stop and i >= 60:
                    if not np.isnan(e120[i]) and not np.isnan(e120[i-60]) and e120[i-60] != 0:
                        pct120 = (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100
                        if pct120 < 0:    # EMA120 仍下跌 → 冷卻
                            cooldown_until = i + 60

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


MODES = ['base', 'M1', 'M2', 'M3', 'M4']

test_groups = [
    ("【死亡迴圈（目標改善）】",
     ['1732','4133','2939','5203','4943','6598','6657','3041','2642']),
    ("【好股票（必須不變差）】",
     ['6139','4961','2609','3017','6442','2317','2330','2454','6443','2485','3035','2404','3661','2368']),
]

hdr = f"{'代號':<7} {'v7%':>7}  " + "  ".join(f"{m:>8}" for m in MODES)
print(hdr); print('-'*70)

grand = {m: [] for m in MODES}

for label, tickers in test_groups:
    print(f'\n{label}')
    grp = {m: [] for m in MODES}
    for ticker in tickers:
        v7ref = v7_map.get(ticker, float('nan'))
        row = f"  {ticker:<7} {v7ref:>+7.1f}%"
        for m in MODES:
            res = run_variant(ticker, mode=m)
            if res:
                pct, _ = res
                diff = pct - v7ref if v7ref==v7ref else float('nan')
                flag = '↑' if diff > 5 else ('↓' if diff < -5 else ' ')
                row += f"  {pct:>+7.1f}{flag}"
                grp[m].append(pct); grand[m].append(pct)
            else:
                row += "       N/A"
        print(row)
    avg_row = f"  {'平均':<7} {'':>7} "
    for m in MODES:
        vals = grp[m]
        avg_row += f"  {np.mean(vals):>+7.1f} " if vals else "       N/A"
    print(avg_row)

print(f'\n{"全體平均":<7} {"":>7} ' +
      "  ".join(f"  {np.mean(grand[m]):>+7.1f} " for m in MODES))
print('\n說明：M1=條件冷卻  M2=T3嚴格(0%)  M3=M1+M2  M4=T3嚴格+T1過濾')
