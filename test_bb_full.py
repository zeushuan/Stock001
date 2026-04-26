"""
布林通道完整理論測試 —— 三大主軸

主軸 W ：W-Bottom 形態進場（升級 lo10 規則）
   T3 進場條件：
     1. 過去 15 天 %b 最低點 < 0.15（曾跌入下軌區，真正的左低點）
     2. 當前 %b > 左低點 + 0.05（形成更高的右低點，正向背離）
     3. 當前 %b < 0.40（仍在下半部，沒漲太多）
     4. RSI < 50

主軸 S ：Bandwidth Squeeze 過濾 T1
   T1 黃金交叉條件：
     - BW 在過去 60 天的相對位置 < 70%
     - （即還處於擠壓或剛突破階段，非已伸張過頭）

主軸 V ：Volume 確認
   T1：成交量 > 過去 20 日平均（真突破有量）
   T3：成交量 < 過去 20 日平均 × 1.5（拉回不應放量恐慌）

測試組合：base / W / S / V / W+S / W+V / S+V / W+S+V
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import numpy as np, csv
from ta.volatility import BollingerBands

bt.START = '2020-01-02'
bt.END   = '2026-04-25'
INVEST   = 100_000

# 載入 v7 基線
v7_map = {}
try:
    with open('tw_all_results_20260425.csv', encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            v7_map[r['ticker']] = float(r['ret_t7'])
except: pass


def run_variant(ticker, use_W=False, use_S=False, use_V=False):
    df = bt.download(ticker)
    if df is None or df.empty: return None
    df = bt.calc_ind(df)
    df = df[df.index >= bt.START].copy()
    if len(df) < 60: return None

    # 補算 BW
    bb = BollingerBands(close=df['Close'], window=20, window_dev=2)
    bb_h = bb.bollinger_hband().values
    bb_l = bb.bollinger_lband().values
    bb_m = bb.bollinger_mavg().values
    with np.errstate(divide='ignore', invalid='ignore'):
        bw = np.where(bb_m != 0, (bb_h - bb_l) / bb_m, np.nan)

    dates = df.index.tolist()
    pr    = df['Close'].values
    vol   = df['Volume'].values if 'Volume' in df.columns else np.full(len(df), np.nan)
    e20   = df['e20'].values
    e60   = df['e60'].values
    e120  = df['e120'].values
    adx_v = df['adx'].values
    rsi   = df['rsi'].values
    atr_v = df['atr'].values
    pctb  = df['pctb'].values
    n     = len(pr)

    def e7_en(i):
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i], adx_v[i]])): return False
        if not (e20[i] > e60[i] and adx_v[i] >= 22): return False

        # T1 黃金交叉
        is_t1 = False
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                is_t1 = True

        if is_t1:
            # 主軸 S：Squeeze 過濾
            if use_S and i >= 60:
                bw_recent = bw[max(0, i-60):i]
                valid = bw_recent[~np.isnan(bw_recent)]
                if len(valid) >= 30 and not np.isnan(bw[i]):
                    bw_min = np.min(valid)
                    bw_max = np.max(valid)
                    if bw_max > bw_min:
                        bw_pos = (bw[i] - bw_min) / (bw_max - bw_min)
                        if bw_pos > 0.70:    # 已過度伸張
                            return False
            # 主軸 V：T1 需有量
            if use_V and i >= 20:
                vma = np.nanmean(vol[max(0, i-20):i])
                if not np.isnan(vma) and vma > 0 and not np.isnan(vol[i]):
                    if vol[i] < 1.0 * vma:   # 量不足，可能假突破
                        return False
            return True

        # ── T3 ─────────────────────────────
        # EMA120 60日跌幅 < 2%（保留）
        if i < 60: return False
        if np.isnan(e120[i]) or np.isnan(e120[i-60]) or e120[i-60] == 0: return False
        if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 < -2.0: return False

        if np.isnan(rsi[i]) or rsi[i] >= 50: return False

        # 主軸 W：W-Bottom 形態
        if use_W:
            if i < 15: return False
            w15 = pctb[max(0, i-15):i]
            valid = w15[~np.isnan(w15)]
            if len(valid) < 5: return False
            lo15 = np.min(valid)
            if lo15 > 0.15: return False                # 沒有真正的低點
            if np.isnan(pctb[i]): return False
            if pctb[i] <= lo15 + 0.05: return False     # 沒形成更高的低點
            if pctb[i] > 0.40: return False              # 已反彈太多

        # 主軸 V：T3 不應大量
        if use_V and i >= 20:
            vma = np.nanmean(vol[max(0, i-20):i])
            if not np.isnan(vma) and vma > 0 and not np.isnan(vol[i]):
                if vol[i] > 1.5 * vma:    # 拉回放大量 = 賣壓沉重
                    return False

        return True

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


# ── 測試 ──────────────────────────────────────────────────────
MODES = [
    ('base',   False, False, False),
    ('W',      True,  False, False),
    ('S',      False, True,  False),
    ('V',      False, False, True),
    ('W+S',    True,  True,  False),
    ('W+V',    True,  False, True),
    ('S+V',    False, True,  True),
    ('W+S+V',  True,  True,  True),
]

test_groups = [
    ("【死亡迴圈（目標改善）】",  ['2939','2498','5203','1732','4133']),
    ("【好股票（大趨勢）】",     ['6139','4961','2609','3017','6442','2404']),
    ("【好股票（穩健）】",       ['2317','2330','2454','2382','3035','6443']),
]

hdr = f"{'代號':<7} {'v7%':>7}  " + "  ".join(f"{m[0]:>8}" for m in MODES)
print(hdr)
print('-' * (7 + 9 + len(MODES)*10))

grand = {m[0]: [] for m in MODES}

for label, tickers in test_groups:
    print(f'\n{label}')
    grp = {m[0]: [] for m in MODES}
    for ticker in tickers:
        v7ref = v7_map.get(ticker, float('nan'))
        row   = f"  {ticker:<7} {v7ref:>+7.1f}%"
        for mname, uW, uS, uV in MODES:
            res = run_variant(ticker, uW, uS, uV)
            if res:
                pct, _ = res
                diff = pct - v7ref if v7ref == v7ref else float('nan')
                flag = '↑' if diff > 10 else ('↓' if diff < -10 else ' ')
                row += f"  {pct:>+7.1f}{flag}"
                grp[mname].append(pct)
                grand[mname].append(pct)
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
print('\n說明：↑=比v7改善>10%  ↓=比v7退步>10%')
