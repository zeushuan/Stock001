"""
策略變體執行器（v8 優化基礎建設）

設計理念：
  抽取 v7 主策略邏輯（_run_v7b_main + T4），參數化所有變體選項
  讓多變體測試可以共用「同一份 calc_ind 結果」+「不同的策略參數」

接受：
  ticker (str), df (DataFrame, 預先 calc_ind), mode (str)
回傳：
  dict 含 ticker, pnl, pnl_pct, n_trades, win_rate

支援的模式（mode 參數）：
  base    -- v7 原版（基線）
  T30     -- 時間停損 30 日
  T45     -- 時間停損 45 日
  AA      -- 動態 ATR（連敗緊縮）
  E20b    -- 浮動獲利 >5% 後啟用 EMA20 跟蹤
  W       -- W-Bottom 形態進場
  C       -- 信號分級部位（0.5/1.0/1.5x）
  T30+AA  -- 組合
  ALL     -- T30 + AA + E20b

新變體可在 _decode_mode() 中加入。
"""
import numpy as np
import pandas as pd

# 引用 backtest_all 的 START/END 設定（必須與 _analyze_core 一致）
import backtest_all as bt

INVEST = 100_000


def _filter_period(df):
    """套用 START/END 日期過濾，與 bt._analyze_core 一致"""
    if df is None or df.empty:
        return None
    mask = (df.index >= bt.START) & (df.index <= bt.END)
    sub = df[mask].copy()
    sub = sub[sub['Close'].notna()]
    return sub


# ─── 模式解碼器 ────────────────────────────────────────────────
def _decode_mode(mode: str) -> dict:
    """把模式字串拆成個別旗標"""
    parts = set(mode.split('+')) if mode != 'ALL' else {'T30', 'AA', 'E20b'}
    return dict(
        use_T   = ('T30' in parts) or ('T45' in parts) or ('T60' in parts),
        t_days  = 60 if 'T60' in parts else (45 if 'T45' in parts else 30),
        use_AA  = 'AA' in parts,
        use_E20a = 'E20a' in parts,
        use_E20b = 'E20b' in parts,
        use_W   = 'W' in parts,
        use_C   = 'C' in parts,
    )


# ─── 信號評分（C 用） ────────────────────────────────────────────
def _signal_score(i, is_t1, e20, e60, e120, adx, rsi):
    score = 0
    if is_t1: score += 1
    if not np.isnan(adx[i]) and adx[i] >= 30: score += 1
    if i >= 60 and not np.isnan(e120[i]) and not np.isnan(e120[i-60]) and e120[i-60] != 0:
        if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 > 0:
            score += 1
    if not np.isnan(rsi[i]) and rsi[i] < 40: score += 1
    if not np.isnan(e20[i]) and not np.isnan(e60[i]) and e60[i] > 0:
        if (e20[i] - e60[i]) / e60[i] * 100 > 3.0:
            score += 1
    return score


def _score_to_mult(score: int, use_C: bool) -> float:
    if not use_C: return 1.0
    if score <= 1: return 0.5
    if score <= 3: return 1.0
    return 1.5


# ─── 主策略迴圈（v7 + 變體） ────────────────────────────────────
def _run_v7_strategy(df, flags):
    """
    純策略邏輯：接受預處理 DataFrame + 旗標 dict，回傳 trades 列表
    """
    dates = df.index.tolist()
    pr    = df['Close'].values
    e20   = df['e20'].values
    e60   = df['e60'].values
    e120  = df['e120'].values
    adx   = df['adx'].values
    rsi   = df['rsi'].values
    atr   = df['atr'].values
    pctb  = df['pctb'].values if 'pctb' in df.columns else np.full(len(df), np.nan)
    n     = len(pr)

    use_T   = flags['use_T']
    t_days  = flags['t_days']
    use_AA  = flags['use_AA']
    use_E20a = flags['use_E20a']
    use_E20b = flags['use_E20b']
    use_W   = flags['use_W']
    use_C   = flags['use_C']

    def e7_en(i):
        if i < 1: return False, False
        if any(np.isnan([e20[i], e60[i], adx[i]])): return False, False
        if not (e20[i] > e60[i] and adx[i] >= 22): return False, False
        # T1 黃金交叉
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                return True, True
        # T3 RSI 拉回
        if i < 60: return False, False
        if np.isnan(e120[i]) or np.isnan(e120[i-60]) or e120[i-60] == 0: return False, False
        if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 < -2.0: return False, False
        if np.isnan(rsi[i]) or rsi[i] >= 50: return False, False
        # W-Bottom 過濾（變體）
        if use_W:
            if i < 15: return False, False
            w15 = pctb[max(0, i-15):i]
            valid = w15[~np.isnan(w15)]
            if len(valid) < 5: return False, False
            lo15 = float(np.min(valid))
            if lo15 > 0.15: return False, False
            if np.isnan(pctb[i]): return False, False
            if pctb[i] <= lo15 + 0.05: return False, False
            if pctb[i] > 0.40: return False, False
        return True, False

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

    trades = []
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
                # 信號評分（C 變體）
                if use_C:
                    sc = _signal_score(i, is_t1, e20, e60, e120, adx, rsi)
                    cur_mult = _score_to_mult(sc, True)
                else:
                    cur_mult = 1.0

                _atr = atr[i] if not np.isnan(atr[i]) else pr[i] * 0.03
                rel = _atr / pr[i] * 100 if pr[i] > 0 else 0
                if rel > 3.5:
                    is_hv = True; stop_p = None; ex_fn = _ex_highvol
                else:
                    is_hv = False
                    _adx = adx[i] if not np.isnan(adx[i]) else 22.0
                    base_mult = 3.0 if _adx >= 30 else 2.5
                    if use_AA and consec_loss > 0:
                        atr_m = max(1.5, base_mult * (0.8 ** consec_loss))
                    else:
                        atr_m = base_mult
                    stop_p = pr[i] - _atr * atr_m
                    ex_fn = _ex_stable
        else:
            # v5 長持鎖定
            if not is_hv:
                td = (dates[i] - ed).days
                fp = (pr[i] - ep) / ep * 100
                if i >= 120 and td > 200 and fp > 50:
                    _en = e120[i]; _ep = e120[i-120]
                    if not any(np.isnan([_en, _ep])) and _en > _ep:
                        cur_ex = _ex_lock2
                    else:
                        cur_ex = ex_fn
                else:
                    cur_ex = ex_fn
            else:
                cur_ex = ex_fn

            hit_stop = (stop_p is not None) and (pr[i] < stop_p)
            do_exit_std = hit_stop or cur_ex(i)

            # 時間停損
            do_exit_t = False
            if use_T and not is_hv:
                td = (dates[i] - ed).days
                if td >= t_days and pr[i] < ep:
                    do_exit_t = True

            # EMA20 跟蹤
            do_exit_e20 = False
            if not is_hv and not np.isnan(e20[i]):
                fp = (pr[i] - ep) / ep * 100
                td = (dates[i] - ed).days
                if use_E20a and td >= 10:
                    if pr[i] < e20[i]: do_exit_e20 = True
                elif use_E20b and fp >= 5.0:
                    if pr[i] < e20[i]: do_exit_e20 = True

            if do_exit_std or do_exit_t or do_exit_e20:
                r = (pr[i] - ep) / ep
                trades.append((r, cur_mult, hit_stop or do_exit_t))
                in_mkt = False
                if r < 0: consec_loss += 1
                else: consec_loss = 0

    if in_mkt:
        r = (pr[-1] - ep) / ep
        trades.append((r, cur_mult, False))

    return trades


def _run_t4_bear_bounce(df):
    """T4 空頭反彈（必須與 bt._analyze_core 內的 e7b_en/e7b_ex + run_bt_atr 完全一致）"""
    pr   = df['Close'].values
    e20  = df['e20'].values
    e60  = df['e60'].values
    rsi  = df['rsi'].values
    atr  = df['atr'].values
    n = len(pr)

    def e7b_en(i):
        # 與 bt._analyze_core 內 e7b_en 完全一致：
        #   rsi[i] < 35（今日RSI下軌）+ 連續2日上升
        if i < 2: return False
        if any(np.isnan([e20[i], e60[i], rsi[i], rsi[i-1], rsi[i-2]])): return False
        if e20[i] > e60[i]: return False
        return rsi[i] < 35 and rsi[i] > rsi[i-1] and rsi[i-1] > rsi[i-2]

    def e7b_ex(i):
        # 與 bt.e7b_ex 完全一致：rsi/e20/e60 任一 NaN → 直接 False
        if any(np.isnan([rsi[i], e20[i], e60[i]])): return False
        if rsi[i] > 55: return True
        if i > 0 and not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] < e60[i-1] and e20[i] >= e60[i]:   # 嚴格小於 + 等於或大於
                return True
        return False

    # 完全照 bt.run_bt_atr 邏輯：i 從 1 開始
    trades = []
    in_mkt = False
    ep = stop_p = None

    for i in range(1, n):
        if not in_mkt:
            if e7b_en(i):
                in_mkt = True
                ep = pr[i]
                _atr = atr[i] if not np.isnan(atr[i]) else pr[i] * 0.03
                stop_p = pr[i] - _atr * 2.0
        else:
            hit_stop = (stop_p is not None) and (pr[i] < stop_p)
            if hit_stop or e7b_ex(i):
                r = (pr[i] - ep) / ep
                trades.append((r, 1.0, hit_stop))
                in_mkt = False
    return trades


# ─── 對外主介面 ──────────────────────────────────────────────────
def run_v7_variant(ticker: str, df, mode: str = 'base') -> dict:
    """
    對單支股票跑 v7 + 變體策略。

    df: 已 calc_ind 過的 DataFrame（從快取讀取，含 280 日暖機期）
    mode: 模式字串，見 _decode_mode()

    回傳：dict（含 ticker, pnl, pnl_pct, n_trades, win_rate, mode）
    """
    if df is None or df.empty: return None
    if 'Close' not in df.columns: return None

    # 套用 START/END 過濾（必須先做，確保策略只跑回測期內）
    df = _filter_period(df)
    if df is None or df.empty: return None

    flags = _decode_mode(mode)
    main_trades = _run_v7_strategy(df, flags)
    t4_trades   = _run_t4_bear_bounce(df)
    all_trades  = main_trades + t4_trades

    if not all_trades:
        return dict(ticker=ticker, mode=mode, pnl=0.0, pnl_pct=0.0,
                    n_trades=0, n_t4=0, win_rate=0.0)

    pnl_list = [r * INVEST * mult for r, mult, _ in all_trades]
    total_pnl = sum(pnl_list)
    n_wins = sum(1 for r, _, _ in all_trades if r > 0)

    # BH 報酬
    pr = df['Close'].values
    bh_pct = (pr[-1] - pr[0]) / pr[0] * 100 if pr[0] > 0 else 0.0

    return dict(
        ticker=ticker, mode=mode,
        pnl=total_pnl,
        pnl_pct=total_pnl / INVEST * 100,
        n_trades=len(all_trades),
        n_t4=len(t4_trades),
        win_rate=n_wins / len(all_trades) * 100,
        bh_pct=bh_pct,
    )
