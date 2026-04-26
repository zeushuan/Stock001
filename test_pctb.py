"""
%b 方案測試（含精化版 A）

方案說明：
  base ── 現行策略（T3 EMA120 2% 過濾）
  A原  ── T1 %b > 0.65 → 全擋（粗版）
  A精  ── T1 %b > 0.65 AND EMA120 60日跌 > 2% → 阻擋（精化版）
           強趨勢（EMA120 上升）高%b T1 → 放行
  B    ── %b > 0.90 且 EMA20 下彎 → 獲利了結出場
  B2   ── %b > 0.85（更敏感）且 EMA20 下彎 → 獲利了結出場
  A精+B2 ── 精化A + B2 組合
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import numpy as np, csv

bt.START = '2020-01-02'
bt.END   = '2026-04-25'
INVEST   = 100_000

A_PCTB_T1    = 0.65
B_PCTB_EXIT  = 0.90
B2_PCTB_EXIT = 0.85

# 載入 v7 基線
v7_map = {}
try:
    with open('tw_all_results_20260425.csv', encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            v7_map[r['ticker']] = float(r['ret_t7'])
except: pass

# ── 核心策略（mode 控制方案）───────────────────────────────────
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
    pctb  = df['pctb'].values
    n     = len(pr)

    use_A_raw    = mode in ('A原', 'A原+B', 'A原+B2')
    use_A_refine = mode in ('A精', 'A精+B', 'A精+B2')
    use_B        = mode in ('B',)
    use_B2       = mode in ('B2', 'A原+B2', 'A精+B2')

    def e7_en(i):
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i], adx_v[i]])): return False
        if not (e20[i] > e60[i] and adx_v[i] >= 22): return False

        # T1 黃金交叉
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                pb = pctb[i]
                if (use_A_raw or use_A_refine) and not np.isnan(pb) and pb > A_PCTB_T1:
                    if use_A_raw:
                        return False   # 粗版：高%b 全擋
                    # 精化版：高%b + EMA120 下跌 > 2% → 才擋
                    if i >= 60 and not np.isnan(e120[i]) and not np.isnan(e120[i-60]) and e120[i-60] != 0:
                        pct120 = (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100
                        if pct120 < -2.0:
                            return False   # 死貓：阻擋
                    # EMA120 上升或歷史不足 → 放行
                return True

        # T3 RSI 拉回（保留 EMA120 2% 過濾）
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
    is_hv  = False

    for i in range(1, n):
        if not in_mkt:
            if e7_en(i):
                in_mkt = True
                ep, ed = pr[i], dates[i]
                _atr = atr_v[i] if not np.isnan(atr_v[i]) else pr[i]*0.03
                rel  = _atr / pr[i] * 100 if pr[i] > 0 else 0
                if rel > 3.5:
                    is_hv  = True; stop_p = None; ex_fn = _ex_highvol
                else:
                    is_hv  = False
                    _adx   = adx_v[i] if not np.isnan(adx_v[i]) else 22.0
                    stop_p = pr[i] - _atr * (3.0 if _adx >= 30 else 2.5)
                    ex_fn  = _ex_stable
        else:
            # v5 長持鎖定
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

            # 方案B/B2：%b 高位且 EMA20 下彎 → 獲利了結
            exit_b = False
            thresh = B2_PCTB_EXIT if use_B2 else (B_PCTB_EXIT if use_B else 9999)
            if not np.isnan(pctb[i]) and pctb[i] > thresh:
                if i >= 1 and not np.isnan(e20[i]) and not np.isnan(e20[i-1]):
                    if e20[i] < e20[i-1]:
                        exit_b = True

            hit_stop = (stop_p is not None) and (pr[i] < stop_p)
            if hit_stop or cur_ex(i) or exit_b:
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

    # T4 補充
    try:
        full   = bt.analyze(ticker)
        t4_pnl = sum(t['pnl'] for t in full.get('t7b', []))
    except:
        t4_pnl = 0

    total = sum(t['pnl'] for t in trades) + t4_pnl
    return total / INVEST * 100, len(trades)


# ── 測試 ──────────────────────────────────────────────────────
MODES = ['base', 'A原', 'A精', 'B', 'B2', 'A精+B2']

test_groups = [
    ("【死亡迴圈（目標改善）】",   ['2939','2498','5203','1732','4133']),
    ("【好股票（大趨勢）】",      ['6139','4961','2609','3017','6442','2404']),
    ("【好股票（穩健）】",        ['2317','2330','2454','2382','3035','6443']),
]

hdr = f"{'代號':<7} {'v7%':>7}  " + "  ".join(f"{m:>7}" for m in MODES)
print(hdr)
print('-' * (7 + 9 + len(MODES)*9))

grand = {m: [] for m in MODES}

for label, tickers in test_groups:
    print(f'\n{label}')
    grp = {m: [] for m in MODES}
    for ticker in tickers:
        v7ref = v7_map.get(ticker, float('nan'))
        row   = f"  {ticker:<7} {v7ref:>+7.1f}%"
        for m in MODES:
            res = run_variant(ticker, mode=m)
            if res:
                pct, _ = res
                diff   = pct - v7ref if v7ref == v7ref else float('nan')
                # 標記顯著變化
                flag = '↑' if diff > 10 else ('↓' if diff < -10 else ' ')
                row += f"  {pct:>+7.1f}{flag}"
                grp[m].append(pct)
                grand[m].append(pct)
            else:
                row += "      N/A"
        print(row)

    avg_row = f"  {'平均':<7} {'':>7} "
    for m in MODES:
        vals = grp[m]
        avg_row += f"  {np.mean(vals):>+7.1f} " if vals else "      N/A"
    print(avg_row)

print(f'\n{"全體平均":<7} {"":>7} ' +
      "  ".join(f"  {np.mean(grand[m]):>+7.1f} " for m in MODES))
print('\n說明：↑=比v7改善>10%  ↓=比v7退步>10%')
