"""
方向 A：出場優化測試（v8 候選方案）

A1 — 時間停損 T30 / T45 / T60：
   持倉 N 日仍負 → 強制出場
   邏輯：搞錯方向了，越早脫身越好；對贏家無影響（30天內已轉正）

A2 — 動態 ATR 停損 AA：
   每次連續虧損 → ATR 倍數 ×0.8（最低到 1.5）
   贏一次 → 重設為預設（2.5 或 3.0）
   邏輯：策略連敗代表此股難搞，後續更謹慎；勝後恢復正常

A3 — EMA20 跟蹤停損 E20：
   收盤 < EMA20 → 出場（比 EMA20<EMA60 早數天）
   僅對穩健股啟用（高波動股保留原本 EMA20/60 死叉）
   變體：E20a 持倉 10 日後啟用；E20b 浮動獲利 >5% 後啟用

注意：所有出場規則都僅對「穩健股」（ATR/P ≤ 3.5%）啟用
       高波動股保留原本的 EMA20/60 死叉，不影響飆股主升段
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
    """
    模式：
      base   - v7 原版
      T30    - 時間停損 30 日
      T45    - 時間停損 45 日
      AA     - 動態 ATR（連敗緊縮）
      E20a   - 持倉 10 日後啟用 EMA20 跟蹤
      E20b   - 浮動獲利 >5% 後啟用 EMA20 跟蹤
      T30AA  - T30 + AA 組合
      T30E20b - T30 + E20b 組合
      ALL    - T30 + AA + E20b 全部
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

    use_T   = mode in ('T30', 'T45', 'T30AA', 'T30E20b', 'ALL')
    t_days  = 45 if mode == 'T45' else 30
    use_AA  = mode in ('AA', 'T30AA', 'ALL')
    use_E20a = mode == 'E20a'
    use_E20b = mode in ('E20b', 'T30E20b', 'ALL')

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
    consec_loss = 0   # AA 用：連敗次數

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
                    base_mult = 3.0 if _adx >= 30 else 2.5
                    # AA：連敗緊縮
                    if use_AA and consec_loss > 0:
                        mult = max(1.5, base_mult * (0.8 ** consec_loss))
                    else:
                        mult = base_mult
                    stop_p = pr[i] - _atr * mult
                    ex_fn = _ex_stable
        else:
            # 標準出場路徑
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
            do_exit_std = hit_stop or cur_ex(i)

            # ── A1 時間停損（穩健股） ──
            do_exit_time = False
            if use_T and not is_hv:
                trade_d = (dates[i] - ed).days
                fp_pct  = (pr[i] - ep) / ep * 100
                if trade_d >= t_days and fp_pct < 0:
                    do_exit_time = True

            # ── A3 EMA20 跟蹤停損（穩健股） ──
            do_exit_e20 = False
            if not is_hv and not np.isnan(e20[i]):
                fp_pct  = (pr[i] - ep) / ep * 100
                trade_d = (dates[i] - ed).days
                if use_E20a and trade_d >= 10:
                    if pr[i] < e20[i]:
                        do_exit_e20 = True
                elif use_E20b and fp_pct >= 5.0:
                    if pr[i] < e20[i]:
                        do_exit_e20 = True

            if do_exit_std or do_exit_time or do_exit_e20:
                r = (pr[i] - ep) / ep
                trades.append(dict(ed=ed, xd=dates[i], ep=ep, xp=pr[i],
                                   ret=r, pnl=INVEST*r,
                                   days=(dates[i]-ed).days,
                                   stop=bool(hit_stop), open=False))
                in_mkt = False
                # 連敗計數
                if r < 0:
                    consec_loss += 1
                else:
                    consec_loss = 0

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


MODES = ['base', 'T30', 'T45', 'AA', 'E20a', 'E20b', 'T30AA', 'T30E20b', 'ALL']

test_groups = [
    ("【死亡迴圈（目標改善）】",
     ['1732','4133','2939','5203','4943','6598','6657','3041','2642']),
    ("【好股票（必須不變差）】",
     ['6139','4961','2609','3017','6442','2317','2330','2454','6443','2485','3035','2404','3661','2368']),
]

hdr = f"{'代號':<6} {'v7%':>7}  " + "  ".join(f"{m:>7}" for m in MODES)
print(hdr); print('-'*70)

grand = {m: [] for m in MODES}

for label, tickers in test_groups:
    print(f'\n{label}')
    grp = {m: [] for m in MODES}
    for ticker in tickers:
        v7ref = v7_map.get(ticker, float('nan'))
        row = f"  {ticker:<6} {v7ref:>+7.1f}%"
        for m in MODES:
            res = run_variant(ticker, mode=m)
            if res:
                pct, _ = res
                diff = pct - v7ref if v7ref==v7ref else float('nan')
                flag = '↑' if diff > 5 else ('↓' if diff < -5 else ' ')
                row += f"  {pct:>+6.0f}{flag}"
                grp[m].append(pct); grand[m].append(pct)
            else:
                row += "      N/A"
        print(row)
    avg_row = f"  {'平均':<6} {'':>7} "
    for m in MODES:
        vals = grp[m]
        avg_row += f"  {np.mean(vals):>+6.0f} " if vals else "      N/A"
    print(avg_row)

print(f'\n{"全體":<6} {"":>7} ' +
      "  ".join(f"  {np.mean(grand[m]):>+6.0f} " for m in MODES))
print('\n說明：T30/T45=時間停損  AA=動態ATR  E20a=10日後跟蹤  E20b=獲利5%後跟蹤')
