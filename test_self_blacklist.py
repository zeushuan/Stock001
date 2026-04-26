"""
v8 候選方案：自我淘汰機制

洞察：所有「進場過濾」都有相同問題 —— 過濾掉壞股的同時也擋掉好股
策略：使用策略自身的「歷史表現回饋」來自動停止對該股票交易
       這比任何指標都更精準（用實證 vs. 理論）

方案：
  base    ── v7 原版
  BL3     ── 連續 3 次虧損 → 永久停止對該股票交易
  BL4     ── 連續 4 次虧損 → 永久停止
  BL5     ── 連續 5 次虧損 → 永久停止
  L4S60   ── 連續 4 次虧損 → 60 天暫停（暫時黑名單）
  L4S120  ── 連續 4 次虧損 → 120 天暫停

注意：贏一筆會重設「連續虧損計數」
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


def run_variant(ticker, max_consec_loss=999, suspend_days=999999):
    """
    max_consec_loss: 連續虧損達此次數即觸發暫停
    suspend_days: 暫停天數（999999 = 永久）
    """
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
        if not (e20[i] > e60[i] and adx_v[i] >= 22): return False
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                return True
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
    consec_loss = 0           # 連續虧損計數
    suspended_until = -1      # 暫停結束的索引

    for i in range(1, n):
        if not in_mkt:
            if i <= suspended_until:
                continue   # 在暫停期間
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

                # 更新連續虧損計數
                if r < 0:
                    consec_loss += 1
                    if consec_loss >= max_consec_loss:
                        suspended_until = i + suspend_days
                else:
                    consec_loss = 0   # 贏一次重設

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


# 模式: (label, max_consec_loss, suspend_days)
MODES = [
    ('base',    999, 999999),
    ('BL3',     3,   999999),
    ('BL4',     4,   999999),
    ('BL5',     5,   999999),
    ('L4S60',   4,   60),
    ('L4S120',  4,   120),
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
        for mname, mcl, sd in MODES:
            res = run_variant(ticker, max_consec_loss=mcl, suspend_days=sd)
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
