"""
策略變體執行器（v8 優化基礎建設）

★ 推薦模式 ★
  最大獲利：P0_T1T3            +197.48%（最差 -290%）
  ★ 平衡：P0_T1T3+CB30          +134.07%（最差 -166%）⭐ 生產推薦
  最保守：P0_T1T3+PS            +110.26%（最差 -149%）

Walk-forward 驗證：EARLY (2020-2022) +54.6 vs LATE (2023-2026) +54.4 = 結構穩定
扣 0.4275% 台股交易成本：P0_T1T3 仍 +184.33%

支援模式（mode 參數）：

【基礎變體（單倉位）】
  base    -- v7 原版（基線，+72.99%）
  T30/T45 -- 時間停損 N 日（中性或微傷）
  AA      -- 動態 ATR（連敗緊縮，幾乎無效）
  E20a/E20b -- EMA20 跟蹤停損（飆股殺手，禁用）
  W       -- W-Bottom 形態進場（理論最佳，全市場負面）
  C       -- 信號分級部位（強趨勢誤殺）

【金字塔加碼（多倉位，v8 主軸）】
  P{N}_{signals}  -- 同股可有多個並行倉位，所有現有倉位均≥N%獲利才開新倉
                      signals: T1 / T3 / T1T3
  範例（已測試）：
    P0_T1T3   -- 最大獲利 +197.48%
    P5_T3     -- 平衡 +115.30%
    P10_T1T3  -- 保守 +104.51%

【A 類：尾部風險控制】
  +PD       -- A1 倉位遞減（第N倉 0.8^(N-1)倍）
  +CB{N}    -- A2 累積虧損熔斷：超過 N% 已實現虧損則停止加碼  ★
  +VA       -- A3 動態 ATR（測試無效）
  +TS       -- A4 加碼後緊縮停損（測試微效）

【B 類：進場品質】
  +VC       -- B3 量能確認（進場量 > 20MA，過濾過嚴）
  +DP       -- B4 拉回深度（≥5%，輕微傷獲利）

【C 類：出場優化】（全部測試無顯著改善獲利）
  +TR       -- C1 跟蹤 ATR
  +PT       -- C2 階段獲利目標
  +ED       -- C3 EMA20 連 5 天下彎
  +RH       -- C4 RSI>75 連 5 天 + 下彎

【D 類：金字塔精化】
  +PS       -- D1 階梯式門檻（每多一倉 +5%）★ 控險最強
  +PG{N}    -- D3 加碼間距 N 天
  +PSL      -- D4 軟上限（倉位多時門檻倍增）

【F 類：架構擴充】
  +WK       -- F1 週線多頭確認（週 EMA20 > EMA60）
  +MK       -- F2 TWII 大盤多頭過濾（需先 cache ^TWII）

  反向ETF（00632R/00633L/00648U）：自動使用 ATR×1.5 + RSI>70 出場（無 T4）
  正股 + ETF：T1/T3 主策略 + T4 空頭反彈

組合：模式以 + 連接，例如 "P0_T1T3+CB30"

【設計理念】
  v7 對大趨勢股捕獲率僅 45%（+72.99% vs BH +163.94%）
  原因：v7 一次只能持 1 倉，主升段多次回檔信號被「已在倉中」浪費
  v8：同股不限倉位，每次 T3 拉回信號都能再加碼累積部位
  → 死亡迴圈股：永不觸發加碼（首倉永遠虧損）→ 損失不擴大
  → 大趨勢股：6139 亞翔 v7 +2441% → v8 +10298%（4 倍放大）

新變體可在 _decode_mode() 中加入。
"""
import numpy as np
import pandas as pd

# 引用 backtest_all 的 START/END 設定（必須與 _analyze_core 一致）
import backtest_all as bt

INVEST = 100_000


def _filter_period(df, start=None, end=None):
    """
    套用日期過濾，與 bt._analyze_core 一致。
    start/end 為 None 時預設使用 bt.START / bt.END。
    支援自訂範圍以做 walk-forward 驗證。
    """
    if df is None or df.empty:
        return None
    s = start or bt.START
    e = end or bt.END
    mask = (df.index >= s) & (df.index <= e)
    sub = df[mask].copy()
    sub = sub[sub['Close'].notna()]
    return sub


# ─── 反向 ETF 清單（與 bt.INVERSE_ETF 同步） ──────────────────────
INVERSE_ETF = {"00632R", "00633L", "00648U"}


# ─── 模式解碼器 ────────────────────────────────────────────────
def _decode_mode(mode: str) -> dict:
    """把模式字串拆成個別旗標"""
    parts = set(mode.split('+')) if mode != 'ALL' else {'T30', 'AA', 'E20b'}

    flags = dict(
        use_T   = ('T30' in parts) or ('T45' in parts) or ('T60' in parts),
        t_days  = 60 if 'T60' in parts else (45 if 'T45' in parts else 30),
        use_AA  = 'AA' in parts,
        use_E20a = 'E20a' in parts,
        use_E20b = 'E20b' in parts,
        use_W   = 'W' in parts,
        use_C   = 'C' in parts,
        # 金字塔
        pyramid_th = None,        # None = 不加碼；數值 = 加碼門檻 %
        pyramid_signals = set(),  # {'T1'}, {'T3'}, {'T1','T3'}
        # A 類：尾部風險控制
        use_PD  = 'PD' in parts,    # A1 倉位遞減：第N倉 mult = 0.8^(N-1)
        cb_th   = None,             # A2 累積虧損熔斷
        use_VA  = 'VA' in parts,    # A3 動態 ATR
        use_TS  = 'TS' in parts,    # A4 加碼後緊縮停損
        # C 類：出場優化
        use_TR  = 'TR' in parts,    # C1 跟蹤 ATR 停損
        use_PT  = 'PT' in parts,    # C2 階段獲利目標
        use_ED  = 'ED' in parts,    # C3 EMA20 連 5 天下彎
        use_RH  = 'RH' in parts,    # C4 RSI>75 連 5 天 + 下彎
        # B 類：進場品質
        use_VC  = 'VC' in parts,    # B3 量能確認
        use_DP  = 'DP' in parts,    # B4 拉回深度
        # D 類：金字塔精化
        use_PS   = 'PS' in parts,    # D1 階梯式門檻
        pg_days  = None,             # D3 加碼間距
        use_PSL  = 'PSL' in parts,   # D4 軟上限
        # F 類：架構擴充
        use_WK   = 'WK' in parts,    # F1 週線多頭確認
        use_MK   = 'MK' in parts,    # F2 大盤多頭過濾
        # E2 Monte Carlo：可調核心參數（None 用預設值）
        adx_th       = None,    # 預設 22
        e120_filter  = None,    # 預設 -2.0
        rsi_t3_th    = None,    # 預設 50
        atr_mult_lo  = None,    # 預設 2.5（ADX<30 時）
        atr_mult_hi  = None,    # 預設 3.0（ADX≥30 時）
    )

    # 解析金字塔模式 P{th}_{signals}
    for part in parts:
        if not part.startswith('P') or len(part) < 2:
            continue
        if not part[1].isdigit():
            continue
        try:
            body = part[1:]
            if '_' in body:
                th_str, sig_str = body.split('_', 1)
            else:
                th_str = body
                sig_str = 'T1T3'   # 預設兩種信號都允許
            flags['pyramid_th'] = float(th_str)
            sig_str_upper = sig_str.upper()
            if 'T1' in sig_str_upper:
                flags['pyramid_signals'].add('T1')
            if 'T3' in sig_str_upper:
                flags['pyramid_signals'].add('T3')
        except ValueError:
            continue

    # 解析累積虧損熔斷 CB{th}（A2）
    for part in parts:
        if part.startswith('CB') and len(part) > 2:
            try:
                flags['cb_th'] = float(part[2:])
            except ValueError:
                pass

    # 解析加碼間距 PG{days}（D3）
    for part in parts:
        if part.startswith('PG') and len(part) > 2:
            try:
                flags['pg_days'] = int(part[2:])
            except ValueError:
                pass

    # E2 Monte Carlo 可調參數
    # ADX{N}    -- ADX 進場門檻
    # E120{N}   -- EMA120 60日過濾門檻（負數，如 E120-3 表示 -3%）
    # RSI{N}    -- T3 RSI 上限
    # ATL{F}    -- 低 ADX ATR 倍數（ADX<30 時）
    # ATH{F}    -- 高 ADX ATR 倍數（ADX≥30 時）
    for part in parts:
        if part.startswith('ADX') and len(part) > 3 and part != 'ADX':
            try: flags['adx_th'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('E120') and len(part) > 4:
            try: flags['e120_filter'] = float(part[4:])
            except ValueError: pass
        elif part.startswith('RSI') and len(part) > 3:
            try: flags['rsi_t3_th'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('ATL') and len(part) > 3:
            try: flags['atr_mult_lo'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('ATH') and len(part) > 3:
            try: flags['atr_mult_hi'] = float(part[3:])
            except ValueError: pass

    return flags


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


# ─── 主策略迴圈（v7 + 變體 + 金字塔加碼） ────────────────────────
def _run_v7_strategy(df, flags, is_inverse_etf=False):
    """
    純策略邏輯：接受預處理 DataFrame + 旗標 dict，回傳 trades 列表

    支援金字塔：positions 列表，每筆獨立 ep/stop_p/ex_fn
    is_inverse_etf=True：使用反向ETF 出場（RSI>70）+ ATR×1.5
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

    use_T    = flags['use_T']
    t_days   = flags['t_days']
    use_AA   = flags['use_AA']
    use_E20a = flags['use_E20a']
    use_E20b = flags['use_E20b']
    use_W    = flags['use_W']
    use_C    = flags['use_C']
    pyramid_th       = flags['pyramid_th']
    pyramid_signals  = flags['pyramid_signals']
    use_PD   = flags['use_PD']    # A1 倉位遞減
    cb_th    = flags['cb_th']     # A2 累積虧損熔斷
    use_VA   = flags['use_VA']    # A3 動態 ATR
    use_TS   = flags['use_TS']    # A4 加碼後緊縮停損
    use_TR   = flags['use_TR']    # C1 跟蹤 ATR 停損
    use_PT   = flags['use_PT']    # C2 階段獲利目標
    use_ED   = flags['use_ED']    # C3 EMA20 連 5 天下彎
    use_RH   = flags['use_RH']    # C4 RSI 高位下彎
    use_VC   = flags['use_VC']    # B3 量能確認
    use_DP   = flags['use_DP']    # B4 拉回深度
    use_PS   = flags['use_PS']    # D1 階梯式門檻
    pg_days  = flags['pg_days']   # D3 加碼間距
    use_PSL  = flags['use_PSL']   # D4 軟上限
    use_WK   = flags['use_WK']    # F1 週線多頭確認
    use_MK   = flags['use_MK']    # F2 大盤多頭過濾
    # E2 可調參數（None 則用預設值）
    adx_th       = flags['adx_th']       if flags['adx_th'] is not None else 22.0
    e120_filter  = flags['e120_filter']  if flags['e120_filter'] is not None else -2.0
    rsi_t3_th    = flags['rsi_t3_th']    if flags['rsi_t3_th'] is not None else 50.0
    atr_mult_lo  = flags['atr_mult_lo']  if flags['atr_mult_lo'] is not None else 2.5
    atr_mult_hi  = flags['atr_mult_hi']  if flags['atr_mult_hi'] is not None else 3.0

    # F1 週線 EMA 預計算（從日線 resample）
    if use_WK:
        try:
            weekly_close = df['Close'].resample('W-FRI').last()
            from ta.trend import EMAIndicator
            we20_w = EMAIndicator(weekly_close, 20).ema_indicator()
            we60_w = EMAIndicator(weekly_close, 60).ema_indicator()
            # 對齊回日線索引（ffill 把週值往前填）
            we20_daily = we20_w.reindex(df.index, method='ffill').values
            we60_daily = we60_w.reindex(df.index, method='ffill').values
        except Exception:
            we20_daily = we60_daily = None
            use_WK = False
    else:
        we20_daily = we60_daily = None

    # F2 大盤 TWII 過濾預載
    twii_bull_arr = None
    if use_MK:
        try:
            import data_loader as _dl
            twii_df = _dl.load_from_cache('^TWII')
            if twii_df is not None and not twii_df.empty:
                # 對齊到 df 的日期
                aligned = twii_df.reindex(df.index, method='ffill')
                if 'e20' in aligned.columns and 'e60' in aligned.columns:
                    twii_bull_arr = (aligned['e20'].values > aligned['e60'].values)
        except Exception:
            twii_bull_arr = None
    # B3 預備：成交量陣列
    if use_VC and 'Volume' in df.columns:
        vol = df['Volume'].values
    else:
        vol = None

    # A3 預計算：ATR/Price 歷史中位數（用於波動正常化）
    if use_VA:
        atr_pct_arr = np.where(pr > 0, atr / pr * 100, np.nan)
        atr_pct_median = np.nanmedian(atr_pct_arr) if len(atr_pct_arr) else 3.0
    else:
        atr_pct_median = None

    def e7_en(i):
        """回傳 (ok, is_t1)"""
        if i < 1: return False, False
        if any(np.isnan([e20[i], e60[i], adx[i]])): return False, False
        if not (e20[i] > e60[i] and adx[i] >= adx_th): return False, False

        # F1 週線多頭確認
        if use_WK and we20_daily is not None and we60_daily is not None:
            if not np.isnan(we20_daily[i]) and not np.isnan(we60_daily[i]):
                if we20_daily[i] <= we60_daily[i]:
                    return False, False

        # F2 大盤 TWII 多頭過濾
        if use_MK and twii_bull_arr is not None:
            if not twii_bull_arr[i]:
                return False, False

        # B3 量能確認（共用過濾，T1/T3 都需通過）
        if use_VC and vol is not None and i >= 20:
            vol_ma = np.nanmean(vol[max(0, i-20):i])
            if not np.isnan(vol_ma) and vol_ma > 0 and not np.isnan(vol[i]):
                if vol[i] < vol_ma:    # 進場量必須 ≥ 20日均量
                    return False, False

        # T1 黃金交叉
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                return True, True
        # T3 RSI 拉回（套用 E2 可調參數）
        if i < 60: return False, False
        if np.isnan(e120[i]) or np.isnan(e120[i-60]) or e120[i-60] == 0: return False, False
        if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 < e120_filter: return False, False
        if np.isnan(rsi[i]) or rsi[i] >= rsi_t3_th: return False, False

        # B4 T3 拉回深度確認：當前價需從 30 日高點下跌 ≥ 5%
        if use_DP and i >= 30:
            hi30 = float(np.nanmax(pr[max(0, i-30):i]))
            if hi30 > 0:
                pullback_pct = (hi30 - pr[i]) / hi30 * 100
                if pullback_pct < 5.0:    # 拉回不夠深 → 假回檔
                    return False, False

        # W-Bottom 過濾
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

    def _ex_inverse(i):
        """反向 ETF 出場：死叉 OR RSI>70"""
        if i < 1: return False
        if any(np.isnan([e20[i], e60[i]])): return False
        if e20[i] < e60[i]: return True
        if not np.isnan(rsi[i]) and rsi[i] > 70: return True
        return False

    def _open_position(i, is_t1, n_existing=0):
        """
        建立新倉位 dict
        n_existing: 現有同股倉位數（A1 倉位遞減用）
        """
        _atr = atr[i] if not np.isnan(atr[i]) else pr[i] * 0.03
        rel  = _atr / pr[i] * 100 if pr[i] > 0 else 0

        # A3 動態 ATR：當前波動 vs 歷史中位數
        va_factor = 1.0
        if use_VA and atr_pct_median and atr_pct_median > 0:
            cur_atr_pct = rel
            ratio = cur_atr_pct / atr_pct_median
            if ratio > 1.5:
                va_factor = 1.3      # 高波動 → 放寬停損 1.3 倍
            elif ratio < 0.7:
                va_factor = 0.85     # 低波動 → 緊縮停損 0.85 倍

        if is_inverse_etf:
            is_hv  = False
            _adx   = adx[i] if not np.isnan(adx[i]) else 22.0
            base_mult = 1.5 * va_factor
            if use_AA and consec_loss > 0:
                atr_m = max(1.0, base_mult * (0.8 ** consec_loss))
            else:
                atr_m = base_mult
            stop_p = pr[i] - _atr * atr_m
            ex_fn  = _ex_inverse
        elif rel > 3.5:
            is_hv  = True
            stop_p = None
            ex_fn  = _ex_highvol
            base_mult = 0.0    # 高波動股無 ATR 停損，base_mult 僅供 dict 紀錄
        else:
            is_hv = False
            _adx  = adx[i] if not np.isnan(adx[i]) else adx_th
            base_mult = (atr_mult_hi if _adx >= 30 else atr_mult_lo) * va_factor
            if use_AA and consec_loss > 0:
                atr_m = max(1.5, base_mult * (0.8 ** consec_loss))
            else:
                atr_m = base_mult
            stop_p = pr[i] - _atr * atr_m
            ex_fn  = _ex_stable

        # 信號評分（C 變體）
        if use_C:
            sc = _signal_score(i, is_t1, e20, e60, e120, adx, rsi)
            mult = _score_to_mult(sc, True)
        else:
            mult = 1.0

        # A1 倉位遞減：第 N+1 倉位用 0.8^N 倍數
        if use_PD and n_existing > 0:
            mult *= (0.8 ** n_existing)

        return dict(
            ed=dates[i], ei=i, ep=pr[i],
            is_hv=is_hv, stop_p=stop_p, ex_fn=ex_fn,
            is_t1=is_t1, mult=mult,
            # C 類用：每倉位獨立追蹤狀態
            atr_at_entry=_atr,            # 進場時 ATR（C1 用）
            atr_mult_used=base_mult,      # 進場時 ATR 倍數
            high_since_entry=pr[i],       # 進場後最高價（C1 跟蹤用）
            pt_stage=0,                   # C2 已觸發的階段（0/1/2/3）
        )

    trades = []           # 已平倉交易：(ret, mult, stopped)
    positions = []        # 開倉中的所有倉位
    consec_loss = 0       # AA 用：全域連敗計數
    realized_loss_pct = 0.0   # A2 用：已實現虧損累積（%）

    for i in range(1, n):
        # ── Step 1：先檢查所有現有倉位的出場條件 ───────────────
        had_exit = False
        for p in list(positions):
            ed = p['ed']; ep = p['ep']
            is_hv = p['is_hv']
            ex_fn  = p['ex_fn']

            # C1 跟蹤 ATR 停損：依進場後最高價往上追停損
            if pr[i] > p['high_since_entry']:
                p['high_since_entry'] = pr[i]
            if use_TR and not is_hv and p['atr_at_entry'] > 0:
                trail_stop = p['high_since_entry'] - p['atr_at_entry'] * p['atr_mult_used']
                if p['stop_p'] is None or trail_stop > p['stop_p']:
                    p['stop_p'] = trail_stop

            # C2 階段獲利目標：+50% → 移到入場價；+100% → 移到 +30%；+200% → 移到 +80%
            if use_PT and not is_hv:
                fp = (pr[i] - ep) / ep * 100
                if fp >= 200 and p['pt_stage'] < 3:
                    p['stop_p'] = ep * 1.80
                    p['pt_stage'] = 3
                elif fp >= 100 and p['pt_stage'] < 2:
                    p['stop_p'] = ep * 1.30
                    p['pt_stage'] = 2
                elif fp >= 50 and p['pt_stage'] < 1:
                    p['stop_p'] = ep                  # break-even
                    p['pt_stage'] = 1

            stop_p = p['stop_p']

            # v5 長持鎖定（per-position）
            if not is_hv and not is_inverse_etf:
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

            # C3 EMA20 連 5 天下彎 → 出場（限非高波動 + 已浮動獲利）
            do_exit_ed = False
            if use_ED and not is_hv and i >= 5:
                fp = (pr[i] - ep) / ep * 100
                if fp > 5:   # 已有獲利才考慮
                    declining_days = 0
                    for k in range(1, 6):
                        if i - k >= 0 and not np.isnan(e20[i-k]) and not np.isnan(e20[i-k-1]):
                            if e20[i-k] < e20[i-k-1]:
                                declining_days += 1
                    if declining_days >= 5:
                        do_exit_ed = True

            # C4 RSI 高位下彎 → 出場（>75 連 5 天 + 開始下行）
            do_exit_rh = False
            if use_RH and not is_hv and i >= 5:
                rsi_hi_days = sum(
                    1 for k in range(0, 5)
                    if i - k >= 0 and not np.isnan(rsi[i-k]) and rsi[i-k] > 75
                )
                if rsi_hi_days >= 5 and not np.isnan(rsi[i]) and not np.isnan(rsi[i-1]):
                    if rsi[i] < rsi[i-1]:    # 開始下行
                        do_exit_rh = True

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

            if do_exit_std or do_exit_t or do_exit_e20 or do_exit_ed or do_exit_rh:
                r = (pr[i] - ep) / ep
                trades.append((r, p['mult'], hit_stop or do_exit_t))
                positions.remove(p)
                had_exit = True
                if r < 0:
                    consec_loss += 1
                    realized_loss_pct += abs(r * p['mult']) * 100
                else:
                    consec_loss = 0

        # ── Step 2：進場檢查（同日有出場則跳過，與 bt 原語意一致）──
        skip_entry = had_exit and not positions
        if not skip_entry:
            ok, is_t1 = e7_en(i)
            if ok:
                should_open = False
                if not positions:
                    should_open = True   # 首倉一律允許
                elif pyramid_th is not None:
                    signal_type = 'T1' if is_t1 else 'T3'
                    if signal_type in pyramid_signals:
                        n_pos = len(positions)

                        # D1 階梯式門檻：每多一倉，門檻 +5%
                        eff_th = pyramid_th
                        if use_PS:
                            eff_th = pyramid_th + 5.0 * n_pos

                        # D4 軟上限：倉位多時門檻倍增
                        if use_PSL:
                            if n_pos >= 6:
                                eff_th = max(eff_th, pyramid_th + 10.0)
                            elif n_pos >= 4:
                                eff_th = max(eff_th, pyramid_th + 5.0)

                        # D3 加碼間距：距上次加碼 < N 天則禁止
                        gap_ok = True
                        if pg_days is not None and positions:
                            last_entry = max(p['ed'] for p in positions)
                            gap = (dates[i] - last_entry).days
                            if gap < pg_days:
                                gap_ok = False

                        all_above_th = all(
                            ((pr[i] - p['ep']) / p['ep'] * 100) >= eff_th
                            for p in positions
                        )
                        if all_above_th and gap_ok:
                            should_open = True
                            # A2 累積虧損熔斷
                            if cb_th is not None and realized_loss_pct >= cb_th:
                                should_open = False
                if should_open:
                    n_existing = len(positions)
                    positions.append(_open_position(i, is_t1, n_existing=n_existing))

                    # A4 加碼後緊縮停損：所有現有倉位（含新倉）停損點上移至破發保護
                    if use_TS and n_existing > 0:
                        # 將舊倉位停損上移到至少進場價（break-even），或當前 ATR 距離
                        for p in positions[:-1]:  # 不含新加的倉
                            if p['stop_p'] is not None and not p['is_hv']:
                                # 移到 max(目前停損, 進場價 - 0.5×ATR)
                                _atr_now = atr[i] if not np.isnan(atr[i]) else pr[i]*0.03
                                new_stop = max(p['stop_p'], p['ep'] - _atr_now * 0.5)
                                p['stop_p'] = new_stop

    # 期末未平倉的倉位以最後價結算
    for p in positions:
        r = (pr[-1] - p['ep']) / p['ep']
        trades.append((r, p['mult'], False))

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
def run_v7_variant(ticker: str, df, mode: str = 'base',
                   start: str = None, end: str = None,
                   tx_cost_pct: float = 0.0) -> dict:
    """
    對單支股票跑 v7 + 變體策略。

    df: 已 calc_ind 過的 DataFrame（從快取讀取，含 280 日暖機期）
    mode: 模式字串，見 _decode_mode()
    start/end: 自訂回測區間（walk-forward 用），None 則用 bt.START/bt.END
    tx_cost_pct: 雙邊交易成本（%），預設 0；台股實際約 0.4275%

    反向ETF：使用反向專屬出場（RSI>70 + ATR×1.5），不執行 T4
    其他股票：T1/T3 主策略 + T4 空頭反彈

    回傳：dict（含 ticker, pnl, pnl_pct, n_trades, win_rate, mode）
    """
    if df is None or df.empty: return None
    if 'Close' not in df.columns: return None

    # 套用日期過濾（必須先做，確保策略只跑回測期內）
    df = _filter_period(df, start=start, end=end)
    if df is None or df.empty: return None

    flags = _decode_mode(mode)
    is_inv = ticker in INVERSE_ETF
    main_trades = _run_v7_strategy(df, flags, is_inverse_etf=is_inv)
    # 反向ETF 不啟用 T4（在空頭時不抓反彈，否則邏輯衝突）
    t4_trades   = [] if is_inv else _run_t4_bear_bounce(df)
    all_trades  = main_trades + t4_trades

    if not all_trades:
        return dict(ticker=ticker, mode=mode, pnl=0.0, pnl_pct=0.0,
                    n_trades=0, n_t4=0, win_rate=0.0, pnl_pct_net=0.0)

    pnl_list = [r * INVEST * mult for r, mult, _ in all_trades]
    total_pnl = sum(pnl_list)
    n_wins = sum(1 for r, _, _ in all_trades if r > 0)

    # 計入交易成本（每筆雙邊扣除 tx_cost_pct%）
    if tx_cost_pct > 0:
        # 每筆交易來回成本 = INVEST × mult × tx_cost_pct%
        cost_total = sum(INVEST * mult * (tx_cost_pct / 100.0)
                         for _, mult, _ in all_trades)
        net_pnl = total_pnl - cost_total
    else:
        net_pnl = total_pnl

    # BH 報酬
    pr = df['Close'].values
    bh_pct = (pr[-1] - pr[0]) / pr[0] * 100 if pr[0] > 0 else 0.0

    return dict(
        ticker=ticker, mode=mode,
        pnl=total_pnl,
        pnl_pct=total_pnl / INVEST * 100,
        pnl_pct_net=net_pnl / INVEST * 100,    # 扣除交易成本後
        n_trades=len(all_trades),
        n_t4=len(t4_trades),
        win_rate=n_wins / len(all_trades) * 100,
        bh_pct=bh_pct,
    )
