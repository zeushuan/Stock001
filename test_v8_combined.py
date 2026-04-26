"""
v8 候選方案：方向 A（出場優化）+ 方向 C（信號分級）統一測試框架

【核心優化：快取機制】
  原本：每測 1 個方案 × 1 支股票 = 1 次 download + 1 次 calc_ind
  改為：每支股票只 download + calc_ind 一次，所有方案共用快取資料
  預期加速：5-10 倍

【方向 A：出場優化】
  T30/T45  ── 持倉 N 日仍負 → 強制出場
  AA       ── 連續虧損後 ATR 倍數×0.8（最低1.5），贏一次重設
  E20b     ── 浮動獲利 >5% 後啟用 EMA20 跟蹤停損
  ALL_A    ── T30 + AA + E20b 全部組合

【方向 C：信號強度分級】
  C        ── 依信號強度決定部位大小
    Score 計算（0-5 分）：
      T1 黃金交叉    : +1
      ADX ≥ 30      : +1
      EMA120 60日上升: +1
      RSI < 40 (深拉回): +1
      EMA20/60 差距>3%: +1
    部位倍率：
      0-1 分 → 0.5x（弱信號）
      2-3 分 → 1.0x（標準）
      4-5 分 → 1.5x（強信號）

【組合】
  C+ALL    ── 全部優化組合（v8 完整候選）
"""
import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import numpy as np, csv
from concurrent.futures import ThreadPoolExecutor

bt.START = '2020-01-02'
bt.END   = '2026-04-25'
INVEST   = 100_000

v7_map = {}
try:
    with open('tw_all_results_20260426.csv', encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            v7_map[r['ticker']] = float(r['ret_t7'])
except: pass


# ── 快取資料載入 ────────────────────────────────────────────────
def prepare(ticker):
    """單次下載 + calc_ind + T4，回傳 dict 供所有方案共用"""
    df = bt.download(ticker)
    if df is None or df.empty: return None
    df = bt.calc_ind(df)
    df = df[df.index >= bt.START].copy()
    if len(df) < 60: return None

    # 預先抓 T4 PnL（不受我們的方案影響）
    try:
        full = bt.analyze(ticker)
        t4_pnl = sum(t['pnl'] for t in full.get('t7b', []))
    except:
        t4_pnl = 0

    return dict(
        dates = df.index.tolist(),
        pr    = df['Close'].values,
        e20   = df['e20'].values,
        e60   = df['e60'].values,
        e120  = df['e120'].values,
        adx   = df['adx'].values,
        rsi   = df['rsi'].values,
        atr   = df['atr'].values,
        t4_pnl = t4_pnl,
    )


# ── 信號強度評分（C 用） ─────────────────────────────────────────
def signal_score(i, is_t1, e20, e60, e120, adx, rsi, n):
    score = 0
    if is_t1: score += 1
    if not np.isnan(adx[i]) and adx[i] >= 30: score += 1
    # EMA120 60日上升
    if i >= 60 and not np.isnan(e120[i]) and not np.isnan(e120[i-60]) and e120[i-60] != 0:
        if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 > 0:
            score += 1
    # RSI<40 (深拉回)
    if not np.isnan(rsi[i]) and rsi[i] < 40: score += 1
    # EMA20/60 差距>3%
    if not np.isnan(e20[i]) and not np.isnan(e60[i]) and e60[i] > 0:
        if (e20[i] - e60[i]) / e60[i] * 100 > 3.0:
            score += 1
    return score


def score_to_mult(score, use_C):
    if not use_C: return 1.0
    if score <= 1: return 0.5
    if score <= 3: return 1.0
    return 1.5


# ── 核心策略迴圈（接受快取資料 + 方案參數） ──────────────────────
def run_strategy(cache, mode='base'):
    if cache is None: return None
    dates = cache['dates']; pr = cache['pr']
    e20   = cache['e20']; e60 = cache['e60']; e120 = cache['e120']
    adx   = cache['adx']; rsi = cache['rsi']; atr = cache['atr']
    t4_pnl = cache['t4_pnl']
    n = len(pr)

    # 方案旗標
    use_T   = mode in ('T30','T45','ALL_A','C+ALL')
    t_days  = 45 if mode == 'T45' else 30
    use_AA  = mode in ('AA','ALL_A','C+ALL')
    use_E20a = mode == 'E20a'
    use_E20b = mode in ('E20b','ALL_A','C+ALL')
    use_C   = mode in ('C','C+ALL')

    def e7_en(i):
        if i < 1: return False, False
        if any(np.isnan([e20[i], e60[i], adx[i]])): return False, False
        if not (e20[i] > e60[i] and adx[i] >= 22): return False, False
        # T1
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                return True, True
        # T3
        if i < 60: return False, False
        if np.isnan(e120[i]) or np.isnan(e120[i-60]) or e120[i-60] == 0: return False, False
        if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 < -2.0: return False, False
        if not np.isnan(rsi[i]) and rsi[i] < 50:
            return True, False
        return False, False

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
        if not np.isnan(adx[i]) and adx[i] < 25:
            if not np.isnan(rsi[i]) and rsi[i] > 75:
                return True
        return False

    total_pnl = 0.0
    n_trades = 0
    in_mkt = False
    ep = ed = stop_p = ex_fn = None
    is_hv = False
    consec_loss = 0
    cur_mult = 1.0

    for i in range(1, n):
        if not in_mkt:
            ok, is_t1 = e7_en(i)
            if ok:
                in_mkt = True
                ep, ed = pr[i], dates[i]
                # 信號評分 + 部位倍率
                sc = signal_score(i, is_t1, e20, e60, e120, adx, rsi, n)
                cur_mult = score_to_mult(sc, use_C)

                _atr = atr[i] if not np.isnan(atr[i]) else pr[i]*0.03
                rel = _atr / pr[i] * 100 if pr[i] > 0 else 0
                if rel > 3.5:
                    is_hv = True; stop_p = None; ex_fn = _ex_highvol
                else:
                    is_hv = False
                    _adx = adx[i] if not np.isnan(adx[i]) else 22.0
                    base_mult = 3.0 if _adx >= 30 else 2.5
                    if use_AA and consec_loss > 0:
                        atr_mult = max(1.5, base_mult * (0.8 ** consec_loss))
                    else:
                        atr_mult = base_mult
                    stop_p = pr[i] - _atr * atr_mult
                    ex_fn = _ex_stable
        else:
            # v5 lock2
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

            # A1 時間停損
            do_exit_time = False
            if use_T and not is_hv:
                td = (dates[i] - ed).days
                fp = (pr[i] - ep) / ep * 100
                if td >= t_days and fp < 0:
                    do_exit_time = True

            # A3 EMA20 跟蹤
            do_exit_e20 = False
            if not is_hv and not np.isnan(e20[i]):
                fp = (pr[i] - ep) / ep * 100
                td = (dates[i] - ed).days
                if use_E20a and td >= 10:
                    if pr[i] < e20[i]: do_exit_e20 = True
                elif use_E20b and fp >= 5.0:
                    if pr[i] < e20[i]: do_exit_e20 = True

            if do_exit_std or do_exit_time or do_exit_e20:
                r = (pr[i] - ep) / ep
                # 套用部位倍率：PnL = INVEST × mult × return
                trade_pnl = INVEST * cur_mult * r
                total_pnl += trade_pnl
                n_trades += 1
                in_mkt = False
                if r < 0: consec_loss += 1
                else: consec_loss = 0

    if in_mkt:
        r = (pr[-1] - ep) / ep
        total_pnl += INVEST * cur_mult * r
        n_trades += 1

    total_pnl += t4_pnl
    return total_pnl / INVEST * 100, n_trades


# ── 並行執行所有股票 × 所有方案 ──────────────────────────────────
MODES = ['base', 'T30', 'T45', 'AA', 'E20a', 'E20b', 'ALL_A', 'C', 'C+ALL']

test_groups = [
    ("【死亡迴圈】",
     ['1732','4133','2939','5203','4943','6598','6657','3041','2642']),
    ("【好股票】",
     ['6139','4961','2609','3017','6442','2317','2330','2454','6443','2485','3035','2404','3661','2368']),
]

# 建立全部待處理股票清單
all_tickers = []
for _, ts in test_groups:
    all_tickers.extend(ts)

# 平行下載 + 預處理
print(f"預處理 {len(all_tickers)} 支股票（並行下載/計算指標）...")
import time
t0 = time.time()
cache_map = {}
with ThreadPoolExecutor(max_workers=10) as ex:
    futures = {ex.submit(prepare, tk): tk for tk in all_tickers}
    for fut in futures:
        tk = futures[fut]
        cache_map[tk] = fut.result()
print(f"預處理完成：{time.time()-t0:.1f}s\n")

hdr = f"{'代號':<6} {'v7%':>7}  " + "  ".join(f"{m:>7}" for m in MODES)
print(hdr); print('-'*70)

grand = {m: [] for m in MODES}

for label, tickers in test_groups:
    print(f'\n{label}')
    grp = {m: [] for m in MODES}
    for ticker in tickers:
        cache = cache_map.get(ticker)
        v7ref = v7_map.get(ticker, float('nan'))
        row = f"  {ticker:<6} {v7ref:>+7.1f}%"
        for m in MODES:
            if cache is None:
                row += "      N/A"; continue
            pct, _ = run_strategy(cache, mode=m)
            diff = pct - v7ref if v7ref==v7ref else float('nan')
            flag = '↑' if diff > 5 else ('↓' if diff < -5 else ' ')
            row += f"  {pct:>+6.0f}{flag}"
            grp[m].append(pct); grand[m].append(pct)
        print(row)
    avg_row = f"  {'平均':<6} {'':>7} "
    for m in MODES:
        vals = grp[m]
        avg_row += f"  {np.mean(vals):>+6.0f} " if vals else "      N/A"
    print(avg_row)

print(f'\n{"全體":<6} {"":>7} ' +
      "  ".join(f"  {np.mean(grand[m]):>+6.0f} " for m in MODES))

print(f'\n總執行時間：{time.time()-t0:.1f}s')
print('\n說明：')
print('  T30/T45 = 時間停損   AA = 動態ATR   E20a = 10日後EMA20跟蹤   E20b = 獲利5%後跟蹤')
print('  ALL_A = T30+AA+E20b   C = 信號分級部位   C+ALL = 全部組合')
