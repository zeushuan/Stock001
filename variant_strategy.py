"""
策略變體執行器（v8 優化基礎建設）

★ 推薦模式 ★
  最大獲利：P0_T1T3            +197.48% / -290% / 風報比 0.68
  風控版：  P0_T1T3+CB30        +134.07% / -166% / 0.81
  自適應：  P0_T1T3+POS         +141.68% / -166% / 0.85
  ★ 跨市場：P0_T1T3+POS+DXY    +120.51% / -122% / 0.99 ⭐ 最高風報比
  尾部最佳：P0_T1T3+POS+VIX30+DXY +115% / -120% / 0.96

【新發現：跨市場過濾】
  DXY  美元下行才進場（弱美元利新興市場）→ 風報比 0.99
  VIX30 VIX>30 不進場（避恐慌期）        → 風報比 0.91
  ER   財報季 (5/8/11/3) 避險             → 太粗糙，無效
  LIQ  流動性過濾                         → 大樣本無感
  SPX  美股多頭過濾                       → 略效

Walk-forward 驗證：EARLY (2020-2022) +54.6 vs LATE (2023-2026) +54.4 穩定
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


# ─── 產業 specific 跨市場映射 ─────────────────────────────────────
# 每個產業匹配最相關的跨市場指標（基於前期 industry_pos_analysis 發現）
_INDUSTRY_FILTERS = {
    # 半導體相關 → SOX 多頭
    '半導體業': 'SOX',
    '電子零組件業': 'SOX',
    '電腦及週邊設備業': 'SOX',
    '其他電子業': 'SOX',
    '通信網路業': 'SOX',
    '光電業': 'SOX',
    # 景氣循環/原物料 → HG 銅多頭
    '航運業': 'HG',
    '鋼鐵工業': 'HG',
    '化學工業': 'HG',
    '塑膠工業': 'HG',
    '紡織纖維': 'HG',
    '橡膠工業': 'HG',
    '玻璃陶瓷': 'HG',
    '水泥工業': 'HG',
    # 內需/防禦 → DXY 弱美元（全球流動性）
    '食品工業': 'DXY',
    '觀光餐旅': 'DXY',
    '貿易百貨業': 'DXY',
    '居家生活': 'DXY',
    '運動休閒': 'DXY',
    # 金融/服務 → DXY
    '金融保險業': 'DXY',
    '資訊服務業': 'DXY',
    '數位雲端': 'DXY',
    # 工業 → DXY
    '電機機械': 'DXY',
    '電器電纜': 'DXY',
    '電子通路業': 'DXY',
    '汽車工業': 'DXY',
    '建材營造業': 'DXY',
    # 其他 → 不加過濾（保留 POS 純策略）
}

# ─── 強化學習 Q-table 載入 ──────────────────────────────────
_Q_TABLE_CACHE = None
def _load_q_table():
    """載入訓練好的 Q-table 並轉成 dict[(state, action)] -> q_val"""
    global _Q_TABLE_CACHE
    if _Q_TABLE_CACHE is not None:
        return _Q_TABLE_CACHE
    import json
    from pathlib import Path
    p = Path(__file__).parent / 'q_table.json'
    if not p.exists():
        _Q_TABLE_CACHE = {}
        return _Q_TABLE_CACHE
    try:
        with open(p, encoding='utf-8') as f:
            data = json.load(f)
        # key 格式：(s_pnl, s_dxy, s_vix, s_spx, s_pos, s_rsi, s_bd)_action
        result = {}
        for k, v in data.items():
            # 拆解 "(0, 1, 2, 1, 3, 2, 1)_1"
            try:
                state_str, action_str = k.rsplit('_', 1)
                state_tuple = eval(state_str)
                result[(state_tuple, int(action_str))] = float(v)
            except: pass
        _Q_TABLE_CACHE = result
    except Exception:
        _Q_TABLE_CACHE = {}
    return _Q_TABLE_CACHE


def _rl_discretize_state(pos_pnl_pct, dxy_bear, vix, spx_bull, n_pos, rsi, bull_days_pct):
    """與 rl_trainer.py 的 discretize_state 完全一致"""
    if pos_pnl_pct < 0: s_pnl = 0
    elif pos_pnl_pct < 10: s_pnl = 1
    elif pos_pnl_pct < 50: s_pnl = 2
    else: s_pnl = 3
    s_dxy = 1 if dxy_bear else 0
    if vix is None or (isinstance(vix, float) and np.isnan(vix)): s_vix = 1
    elif vix < 20: s_vix = 0
    elif vix < 30: s_vix = 1
    else: s_vix = 2
    s_spx = 1 if spx_bull else 0
    if n_pos == 0: s_pos = 0
    elif n_pos < 4: s_pos = 1
    elif n_pos < 8: s_pos = 2
    else: s_pos = 3
    if rsi is None or (isinstance(rsi, float) and np.isnan(rsi)): s_rsi = 1
    elif rsi < 30: s_rsi = 0
    elif rsi < 50: s_rsi = 1
    elif rsi < 70: s_rsi = 2
    else: s_rsi = 3
    if bull_days_pct is None or np.isnan(bull_days_pct): s_bd = 1
    elif bull_days_pct < 40: s_bd = 0
    elif bull_days_pct < 60: s_bd = 1
    else: s_bd = 2
    return (s_pnl, s_dxy, s_vix, s_spx, s_pos, s_rsi, s_bd)


_INDUSTRY_MAP_CACHE = None
def _load_industry_map():
    """載入並快取 ticker→industry 映射"""
    global _INDUSTRY_MAP_CACHE
    if _INDUSTRY_MAP_CACHE is None:
        import json
        from pathlib import Path
        p = Path(__file__).parent / 'tw_stock_list.json'
        if p.exists():
            with open(p, encoding='utf-8') as f:
                data = json.load(f)
            _INDUSTRY_MAP_CACHE = {tk: meta.get('industry', '')
                                    for tk, meta in data.items()}
        else:
            _INDUSTRY_MAP_CACHE = {}
    return _INDUSTRY_MAP_CACHE


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
        # 自適應規則（基於分群分析發現）
        bull_days_th = None,    # 加碼前要求過去 250 天 bull_days >= N%
        v7_pos_only  = 'POS' in parts,        # POS：累積為正才加碼
        pos_min_pct  = None,                  # POSN：累積需達 N% 才加碼
        use_PR       = 'PR' in parts,         # PR：停損後 POS 重置
        # 全新方向：執行優化
        liq_min_vol  = None,    # LIQ{N}：進場前要求 60 日均量 ≥ N 張（預設 None）
        slippage_pct = None,    # SLP{F}：進場/出場滑價%
        # 全新方向：跨市場
        vix_max_th   = None,    # VIX{N}：VIX 超過 N 時禁止新進場
        use_SPX      = 'SPX' in parts,    # SPX 多頭時才進場
        use_DXY      = 'DXY' in parts,    # 美元下行時才進場（基本版）
        dxy_strength_th = None,           # DXYS{N}：DXY EMA20 須比 EMA60 低 N% 以上
        tnx_max_th   = None,    # TNX{N}：10年美債殖利率上限
        # 全新方向：基本面（財報季避險）
        use_ER       = 'ER' in parts,    # 財報季避險
        # 深化跨市場
        use_GLD      = 'GLD' in parts,    # 黃金多頭時不進場（避險情緒高）
        use_HG       = 'HG' in parts,     # 銅多頭時才進場（景氣強）
        use_SOX      = 'SOX' in parts,    # 費半多頭時才進場
        use_VIXTR    = 'VIXTR' in parts,  # VIX 下行時才進場（恐慌平息）
        use_DXYROC   = 'DXYROC' in parts, # DXY 5 日變化率 > 0 不進場（美元快速強勢）
        cuau_min     = None,              # CUAU{N}：銅金比 > N% 才進場
        # 產業 specific 跨市場
        use_IND      = 'IND' in parts,    # 依產業套用不同跨市場過濾
        # 事件驅動避險（即時可計算）
        avol_max_th  = None,    # AVOL{N}：當前 ATR / 60日均 > N 倍時禁進場
        shk_pct_th   = None,    # SHK{N}：單日 |%change| > N% 後 5天冷卻
        # 多時間框架（週線）
        use_WRSI     = 'WRSI' in parts,    # 週 RSI<50 才允許加碼
        use_WADX     = 'WADX' in parts,    # 週 ADX≥22 才進場
        # 全新方法：Ensemble Voting
        ens_min_votes = None,    # EV{K}
        # 全新方法：Volatility Regime
        vol_regime_th = None,    # VR{N}
        # 全新方法：強化學習加碼決策
        use_RL       = 'RL' in parts,    # RL：用訓練好的 Q-table 決定加碼
        # 全新方法：Online Adaptive Threshold（線上自適應）
        use_AT       = 'AT' in parts,    # AT：贏放寬、輸收緊（自適應 POS 門檻）
        # 全新方法：Anomaly Detection（簡易統計版）
        anom_atr_th  = None,             # ANOM{N}：當前 ATR > N 倍 60 日中位數時暫停
        # 全新方法：三大法人籌碼整合
        use_INST     = 'INST' in parts,  # 三大法人合計買超才加碼
        use_FOR      = 'FOR' in parts,   # 外資 N 日累積買超才加碼
        for_days     = None,             # FORN{N}：外資 N 日累積期
        # 基本面：月營收 YoY 過濾
        revup_n      = None,             # REVUP{N}：連續 N 月 YoY > 0 才進場
        # 基本面：毛利率 YoY 過濾
        use_MARGUP   = 'MARGUP' in parts,    # 毛利率 vs 一年前同季 上升才進場
        # 盤中執行：VWAP 進出場（需 vwap_cache）
        use_VWAP_ENTRY = 'VWAPENTRY' in parts,    # 只在 close < 當日 VWAP 才進場
        use_VWAP_EXEC  = 'VWAPEXEC' in parts,     # 進場價用 min(close, VWAP)、出場價用 max
        use_VWAP_EXIT  = 'VWAPEXIT' in parts,     # 只在 close > 當日 VWAP 才出場
        # VWAPEXEC 隔離驗證：停損出場用市價（不套 VWAP），其他出場仍用 max(close, VWAP)
        use_VWAP_NOSTOP = 'VWAPNOSTOP' in parts,  # 與 VWAPEXEC 並用：hit_stop 時跳過 VWAP
        # 🆕 VWAP 進階變體（2026-04-28）
        # VWAPDEV{N}：偏離度 ≥ N% 才進場（更嚴格的 VWAPENTRY）
        # VWAPBAND{N}：close ≤ VWAP - N × (day_range/4) 才進場（VWAP-Nσ 通道）
        # STRONGCL：前日 close 位於日內高點 70% 以上才進場（強勢追勢）
        # WEAKCL：前日 close 位於日內低點 30% 以下才進場（逢低）
        vwap_dev_pct  = None,                     # VWAPDEV{N}：偏離 N% 才進場
        vwap_band_n   = None,                     # VWAPBAND{N}：N σ 通道
        use_STRONGCL  = 'STRONGCL' in parts,
        use_WEAKCL    = 'WEAKCL' in parts,
        # 🆕 EPS / P/E 估值過濾（2026-04-28，需要 per_cache）
        use_PEPOS    = 'PEPOS' in parts,         # PER > 0（過濾虧損公司）
        pe_max       = None,                      # PEMAX{N}：PER ≤ N
        pe_min       = None,                      # PEMIN{N}：PER ≥ N
        use_PEMID    = 'PEMID' in parts,         # 10 < PER < 30
        div_min      = None,                      # DIV{N}：殖利率 ≥ N%
        pbr_max      = None,                      # PBR{N}：PBR ≤ N
        # PER 進階（動態 / 動量 / 相對）
        pe_mom_pct   = None,                      # PEMOM{N}：PER 60 日內降 N% 以上（盈餘上修信號）
        pe_rel_pct   = None,                      # PEREL{N}：PER 比 90 日中位數低 N%（個股相對便宜）
        use_PEAVG    = 'PEAVG' in parts,         # PER < 自己 90 日平均（簡化版相對）
        # 黑天鵝防護（讀 black_swans.json 的 danger_dates）
        use_BSGUARD    = 'BSGUARD' in parts,      # 危險窗暫停 T1/T3 進場
        use_BSEXIT     = 'BSEXIT' in parts,       # 危險窗加速出場（ATR×1.5）
        use_BSPOSHALF  = 'BSPOSHALF' in parts,    # 危險窗部位減半
        # 🆕 動態 ATR 停損（保護獲利）
        use_DYNSTOP    = 'DYNSTOP' in parts,      # 持倉 > 30d 且獲利 > 20% → ATR×1.5（trailing）
        # 🆕 券資比過濾（margin_cache 必要）
        ms_min        = None,                     # MSRATIO{N}：券資比 ≥ N%（軋空潛力）
        ms_max        = None,                     # MSCAP{N}：券資比 ≤ N%（避開過熱）
        ms_mom        = None,                     # MSMOM{N}：券資比 60d 內升 N% 以上
    )

    # 🆕 解析 VWAPDEV{N} / VWAPBAND{N} / PEMAX{N} / PEMIN{N} / DIV{N} / PBR{N}
    for part in parts:
        if part.startswith('VWAPDEV') and len(part) > 7:
            try: flags['vwap_dev_pct'] = float(part[7:])
            except ValueError: pass
        elif part.startswith('VWAPBAND') and len(part) > 8:
            try: flags['vwap_band_n'] = float(part[8:])
            except ValueError: pass
        elif part.startswith('PEMAX') and len(part) > 5:
            try: flags['pe_max'] = float(part[5:])
            except ValueError: pass
        elif part.startswith('PEMIN') and len(part) > 5:
            try: flags['pe_min'] = float(part[5:])
            except ValueError: pass
        elif part.startswith('DIV') and len(part) > 3 and part[3].isdigit():
            try: flags['div_min'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('PBR') and len(part) > 3 and part[3].isdigit():
            try: flags['pbr_max'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('PEMOM') and len(part) > 5:
            try: flags['pe_mom_pct'] = float(part[5:])
            except ValueError: pass
        elif part.startswith('PEREL') and len(part) > 5:
            try: flags['pe_rel_pct'] = float(part[5:])
            except ValueError: pass
        elif part.startswith('MSRATIO') and len(part) > 7:
            try: flags['ms_min'] = float(part[7:])
            except ValueError: pass
        elif part.startswith('MSCAP') and len(part) > 5:
            try: flags['ms_max'] = float(part[5:])
            except ValueError: pass
        elif part.startswith('MSMOM') and len(part) > 5:
            try: flags['ms_mom'] = float(part[5:])
            except ValueError: pass

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
    # BD{N}     -- 加碼前要求過去 250 天 bull_days >= N%（自適應）
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
        elif part.startswith('BD') and len(part) > 2:
            try: flags['bull_days_th'] = float(part[2:])
            except ValueError: pass
        elif part.startswith('POS') and len(part) > 3:
            try:
                flags['pos_min_pct'] = float(part[3:])
                flags['v7_pos_only'] = True
            except ValueError: pass
        elif part.startswith('LIQ') and len(part) > 3:
            try: flags['liq_min_vol'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('SLP') and len(part) > 3:
            try: flags['slippage_pct'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('VIX') and len(part) > 3:
            try: flags['vix_max_th'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('TNX') and len(part) > 3:
            try: flags['tnx_max_th'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('CUAU') and len(part) > 4:
            try: flags['cuau_min'] = float(part[4:])
            except ValueError: pass
        elif part.startswith('DXYS') and len(part) > 4:
            try: flags['dxy_strength_th'] = float(part[4:])
            except ValueError: pass
        elif part.startswith('AVOL') and len(part) > 4:
            try: flags['avol_max_th'] = float(part[4:])
            except ValueError: pass
        elif part.startswith('SHK') and len(part) > 3:
            try: flags['shk_pct_th'] = float(part[3:])
            except ValueError: pass
        elif part.startswith('EV') and len(part) > 2:
            try: flags['ens_min_votes'] = int(part[2:])
            except ValueError: pass
        elif part.startswith('VR') and len(part) > 2:
            try: flags['vol_regime_th'] = float(part[2:])
            except ValueError: pass
        elif part.startswith('ANOM') and len(part) > 4:
            try: flags['anom_atr_th'] = float(part[4:])
            except ValueError: pass
        elif part.startswith('FORN') and len(part) > 4:
            try:
                flags['for_days'] = int(part[4:])
                flags['use_FOR'] = True
            except ValueError: pass
        elif part.startswith('REVUP') and len(part) > 5:
            # REVUP3 = 連續 3 月 YoY > 0% 才進場
            try: flags['revup_n'] = int(part[5:])
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
def _run_v7_strategy(df, flags, is_inverse_etf=False, ticker=None):
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
    bull_days_th = flags['bull_days_th']
    v7_pos_only  = flags['v7_pos_only']
    pos_min_pct  = flags['pos_min_pct']
    use_PR       = flags['use_PR']
    liq_min_vol  = flags['liq_min_vol']    # LIQ{N}：60日均量門檻
    slippage_pct = flags['slippage_pct']   # SLP{F}：滑價%
    vix_max_th   = flags['vix_max_th']     # VIX{N}：VIX 上限
    use_SPX      = flags['use_SPX']        # SPX 多頭過濾
    use_DXY      = flags['use_DXY']        # 美元下行過濾（基本版）
    dxy_strength_th = flags['dxy_strength_th']  # DXY 強度門檻（動態版）
    tnx_max_th   = flags['tnx_max_th']     # TNX{N}：美債殖利率上限
    use_ER       = flags['use_ER']         # 財報季避險
    use_GLD      = flags['use_GLD']        # 黃金多頭時不進場
    use_HG       = flags['use_HG']         # 銅多頭時才進場
    use_SOX      = flags['use_SOX']        # 費半多頭時才進場
    use_VIXTR    = flags['use_VIXTR']      # VIX 下行才進場
    use_DXYROC   = flags['use_DXYROC']     # DXY 變化率過濾
    cuau_min     = flags['cuau_min']       # 銅金比門檻
    avol_max_th  = flags['avol_max_th']    # ATR 異常放大門檻
    shk_pct_th   = flags['shk_pct_th']     # 衝擊事件門檻
    use_WRSI     = flags['use_WRSI']       # 週 RSI 過濾
    use_WADX     = flags['use_WADX']       # 週 ADX 過濾
    ens_min_votes = flags.get('ens_min_votes')  # Ensemble 票數門檻
    vol_regime_th = flags.get('vol_regime_th')  # 波動率 regime
    use_RL       = flags.get('use_RL', False)   # 強化學習 Q-table 決策
    use_AT       = flags.get('use_AT', False)   # 線上自適應門檻
    anom_atr_th  = flags.get('anom_atr_th')     # 異常波動偵測
    use_INST     = flags.get('use_INST', False) # 三大法人合計買超
    use_FOR      = flags.get('use_FOR', False)  # 外資 N 日累積買超
    for_days     = flags.get('for_days') or 5
    revup_n      = flags.get('revup_n')         # 月營收連續 N 月 YoY > 0
    use_MARGUP   = flags.get('use_MARGUP', False)  # 毛利率 YoY 上升
    use_VWAP_ENTRY = flags.get('use_VWAP_ENTRY', False)
    use_VWAP_EXEC  = flags.get('use_VWAP_EXEC', False)
    use_VWAP_EXIT  = flags.get('use_VWAP_EXIT', False)
    use_VWAP_NOSTOP = flags.get('use_VWAP_NOSTOP', False)
    vwap_dev_pct   = flags.get('vwap_dev_pct')
    vwap_band_n    = flags.get('vwap_band_n')
    use_STRONGCL   = flags.get('use_STRONGCL', False)
    use_WEAKCL     = flags.get('use_WEAKCL', False)
    use_PEPOS      = flags.get('use_PEPOS', False)
    pe_max         = flags.get('pe_max')
    pe_min         = flags.get('pe_min')
    use_PEMID      = flags.get('use_PEMID', False)
    div_min        = flags.get('div_min')
    pbr_max        = flags.get('pbr_max')
    pe_mom_pct     = flags.get('pe_mom_pct')
    pe_rel_pct     = flags.get('pe_rel_pct')
    use_PEAVG      = flags.get('use_PEAVG', False)

    # 🆕 PER / PBR 預載（per_cache）
    pe_arr = pbr_arr = div_arr = None
    needs_pe = (use_PEPOS or pe_max is not None or pe_min is not None
                or use_PEMID or div_min is not None or pbr_max is not None
                or pe_mom_pct is not None or pe_rel_pct is not None or use_PEAVG)
    if needs_pe and ticker:
        try:
            from pathlib import Path as _P
            pe_path = _P(__file__).parent / 'per_cache' / f'{ticker}.parquet'
            if pe_path.exists():
                pe_df = pd.read_parquet(pe_path)
                pe_arr = pe_df['PER'].reindex(df.index, method='nearest',
                                               tolerance=pd.Timedelta('5D')).values
                pbr_arr = pe_df['PBR'].reindex(df.index, method='nearest',
                                                tolerance=pd.Timedelta('5D')).values
                div_arr = pe_df['dividend_yield'].reindex(df.index, method='nearest',
                                                           tolerance=pd.Timedelta('5D')).values
        except Exception:
            pass
    use_BSGUARD    = flags.get('use_BSGUARD', False)
    use_BSEXIT     = flags.get('use_BSEXIT', False)
    use_BSPOSHALF  = flags.get('use_BSPOSHALF', False)
    use_DYNSTOP    = flags.get('use_DYNSTOP', False)
    ms_min         = flags.get('ms_min')
    ms_max         = flags.get('ms_max')
    ms_mom         = flags.get('ms_mom')

    # 🆕 券資比 cache 預載（margin_cache）
    ms_arr = None
    needs_ms = (ms_min is not None or ms_max is not None or ms_mom is not None)
    if needs_ms and ticker:
        try:
            from pathlib import Path as _P
            ms_path = _P(__file__).parent / 'margin_cache' / f'{ticker}.parquet'
            if ms_path.exists():
                ms_df = pd.read_parquet(ms_path)
                ms_arr = ms_df['msratio'].reindex(df.index, method='nearest',
                                                   tolerance=pd.Timedelta('5D')).values
        except Exception:
            pass

    # 黑天鵝危險日清單（一次性載入）
    bs_danger_set = None
    if use_BSGUARD or use_BSEXIT or use_BSPOSHALF:
        try:
            from pathlib import Path as _P
            import json as _json
            bs_path = _P(__file__).parent / 'black_swans.json'
            if bs_path.exists():
                with open(bs_path, encoding='utf-8') as _f:
                    bs_data = _json.load(_f)
                bs_danger_set = set(bs_data.get('danger_dates', []))
        except Exception:
            bs_danger_set = None

    # 🆕 載入此股的毛利率 YoY 訊號（vs 一年前同季）
    margup_ok_arr = None
    if use_MARGUP and ticker:
        from pathlib import Path as _P
        m_file = _P(__file__).parent / 'margin_quarterly.parquet'
        if m_file.exists():
            try:
                m_all = pd.read_parquet(m_file)
                m_t = m_all[m_all['ticker'] == ticker].copy()
                if len(m_t) >= 5:
                    m_t = m_t.sort_values('date').set_index('date')
                    # 同季 YoY：本季毛利率 vs 4 季前
                    m_t['margin_yoy'] = m_t['margin'] - m_t['margin'].shift(4)
                    m_t['signal'] = (m_t['margin_yoy'] > 0)
                    # 對齊到日線：財報公布日（季底 +45 天保守）
                    m_t.index = m_t.index + pd.Timedelta(days=45)
                    m_aligned = m_t['signal'].reindex(
                        df.index, method='ffill').fillna(False)
                    margup_ok_arr = m_aligned.values
            except Exception:
                pass

    # 🆕 載入此股的月營收 YoY 訊號（連續 N 月正成長）
    revup_ok_arr = None
    if revup_n is not None and ticker:
        from pathlib import Path as _P
        rev_file = _P(__file__).parent / 'monthly_revenue.parquet'
        if rev_file.exists():
            try:
                rev_all = pd.read_parquet(rev_file)
                rev = rev_all[rev_all['ticker'] == ticker].copy()
                if not rev.empty:
                    rev = rev.sort_values('date').set_index('date')
                    # YoY = 本月 / 12 個月前同月 - 1
                    rev['rev_yoy'] = rev['revenue'].pct_change(12)
                    rev['yoy_pos'] = (rev['rev_yoy'] > 0).astype(int)
                    # 連續 N 月正成長：用 rolling sum 確認
                    rev['cons_pos'] = rev['yoy_pos'].rolling(revup_n).sum()
                    rev['signal'] = (rev['cons_pos'] >= revup_n)
                    # 對齊到日線：每月公布日（date 是公布月首日）後生效
                    # 加 10 天保守緩衝（10 號才公布）
                    rev_aligned = rev['signal'].reindex(
                        df.index, method='ffill').fillna(False)
                    revup_ok_arr = rev_aligned.values
            except Exception:
                pass

    # 載入此股的三大法人歷史
    inst_total_arr = None
    inst_foreign_arr = None
    if (use_INST or use_FOR) and ticker:
        from pathlib import Path
        inst_path = Path(__file__).parent / 'inst_per_ticker' / f'{ticker}.parquet'
        if inst_path.exists():
            try:
                inst_df = pd.read_parquet(inst_path)
                # 對齊到日線
                inst_aligned = inst_df.reindex(df.index, method='ffill')
                if '三大法人買賣超股數' in inst_aligned.columns:
                    inst_total_arr = inst_aligned['三大法人買賣超股數'].values
                if '外陸資買賣超股數(不含外資自營商)' in inst_aligned.columns:
                    inst_foreign_arr = inst_aligned['外陸資買賣超股數(不含外資自營商)'].values
            except Exception:
                pass

    # 載入 RL Q-table
    rl_q_table = _load_q_table() if use_RL else {}

    # 預備：60 日 ATR 中位數（ANOM 用）
    atr_med60_arr = None
    if anom_atr_th is not None:
        atr_med60_arr = pd.Series(atr).rolling(60).median().values

    # 預備：60日波動率百分位（VR 用）
    vol_regime_arr = None
    if vol_regime_th is not None and len(pr) > 60:
        try:
            log_ret = np.diff(np.log(pr + 1e-9))
            vol_60 = pd.Series(log_ret).rolling(60).std().values
            # 計算每天的「波動率歷史百分位」
            vol_regime_arr = np.full(len(pr), 50.0)
            for i in range(120, len(pr) - 1):
                window = vol_60[max(0, i-250):i]
                valid = window[~np.isnan(window)]
                if len(valid) > 50 and not np.isnan(vol_60[i]):
                    pct = np.sum(valid <= vol_60[i]) / len(valid) * 100
                    vol_regime_arr[i+1] = pct
        except Exception:
            vol_regime_arr = None

    # 預備：ATR 60 日均線（AVOL 用）
    if avol_max_th is not None:
        atr_ma60_arr = pd.Series(atr).rolling(60).mean().values
    else:
        atr_ma60_arr = None

    # 預備：日報酬率（SHK 用）
    if shk_pct_th is not None:
        daily_chg_arr = np.zeros(len(pr))
        if len(pr) > 1:
            daily_chg_arr[1:] = np.abs((pr[1:] - pr[:-1]) / pr[:-1] * 100)
    else:
        daily_chg_arr = None

    # 預備：週線 RSI / ADX
    week_rsi_daily = None
    week_adx_daily = None
    if use_WRSI or use_WADX:
        try:
            from ta.momentum import RSIIndicator
            from ta.trend import ADXIndicator
            weekly_close = df['Close'].resample('W-FRI').last()
            weekly_high  = df['High'].resample('W-FRI').max() if 'High' in df.columns else weekly_close
            weekly_low   = df['Low'].resample('W-FRI').min() if 'Low' in df.columns else weekly_close
            if use_WRSI and len(weekly_close) > 14:
                wrsi_w = RSIIndicator(weekly_close, 14).rsi()
                week_rsi_daily = wrsi_w.reindex(df.index, method='ffill').values
            if use_WADX and len(weekly_close) > 14:
                wadx_w = ADXIndicator(weekly_high, weekly_low, weekly_close, 14).adx()
                week_adx_daily = wadx_w.reindex(df.index, method='ffill').values
        except Exception:
            pass

    # 載入跨市場序列
    def _load_cross_market(ticker):
        try:
            import data_loader as _dl
            xdf = _dl.load_from_cache(ticker)
            if xdf is None or xdf.empty:
                return None
            return xdf
        except: return None

    vix_series = None
    if vix_max_th is not None:
        xdf = _load_cross_market('^VIX')
        if xdf is not None:
            vix_series = xdf['Close'].reindex(df.index, method='ffill').values

    spx_bull_arr = None
    if use_SPX:
        xdf = _load_cross_market('^GSPC')
        if xdf is not None:
            aligned = xdf.reindex(df.index, method='ffill')
            if 'e20' in aligned.columns and 'e60' in aligned.columns:
                spx_bull_arr = (aligned['e20'].values > aligned['e60'].values)

    dxy_bear_arr = None
    dxy_strong_bear_arr = None
    if use_DXY or dxy_strength_th is not None:
        xdf = _load_cross_market('DX-Y.NYB')
        if xdf is None:
            xdf = _load_cross_market('DX=F')
        if xdf is not None:
            aligned = xdf.reindex(df.index, method='ffill')
            if 'e20' in aligned.columns and 'e60' in aligned.columns:
                dxy_e20 = aligned['e20'].values
                dxy_e60 = aligned['e60'].values
                dxy_bear_arr = (dxy_e20 < dxy_e60)
                if dxy_strength_th is not None:
                    # DXY EMA20 比 EMA60 低 N% 以上才算強勢弱美元
                    with np.errstate(divide='ignore', invalid='ignore'):
                        dxy_diff_pct = np.where(
                            dxy_e60 > 0,
                            (dxy_e20 - dxy_e60) / dxy_e60 * 100,
                            np.nan
                        )
                    dxy_strong_bear_arr = (dxy_diff_pct < -dxy_strength_th)

    tnx_series = None
    if tnx_max_th is not None:
        xdf = _load_cross_market('^TNX')
        if xdf is not None:
            tnx_series = xdf['Close'].reindex(df.index, method='ffill').values

    # 深化跨市場：黃金多頭、銅多頭、費半多頭
    gld_bull_arr = None
    if use_GLD:
        for tk in ['GC=F', 'GLD']:
            xdf = _load_cross_market(tk)
            if xdf is not None and 'e20' in xdf.columns:
                aligned = xdf.reindex(df.index, method='ffill')
                gld_bull_arr = (aligned['e20'].values > aligned['e60'].values)
                break

    hg_bull_arr = None
    if use_HG:
        xdf = _load_cross_market('HG=F')
        if xdf is not None and 'e20' in xdf.columns:
            aligned = xdf.reindex(df.index, method='ffill')
            hg_bull_arr = (aligned['e20'].values > aligned['e60'].values)

    sox_bull_arr = None
    if use_SOX:
        xdf = _load_cross_market('^SOX')
        if xdf is not None and 'e20' in xdf.columns:
            aligned = xdf.reindex(df.index, method='ffill')
            sox_bull_arr = (aligned['e20'].values > aligned['e60'].values)

    # VIX 下行（5日均線下行）
    vix_falling_arr = None
    if use_VIXTR and vix_series is None:
        xdf = _load_cross_market('^VIX')
        if xdf is not None:
            vix_series_loc = xdf['Close'].reindex(df.index, method='ffill').values
            # 5日均下行
            vix_ma5 = pd.Series(vix_series_loc).rolling(5).mean().values
            vix_ma5_prev = np.roll(vix_ma5, 1)
            vix_falling_arr = (vix_ma5 < vix_ma5_prev)

    # DXY 5日變化率（DXY 上升太快不進場）
    dxy_roc_ok_arr = None
    if use_DXYROC:
        xdf = _load_cross_market('DX-Y.NYB') or _load_cross_market('DX=F')
        if xdf is not None:
            dxy_close = xdf['Close'].reindex(df.index, method='ffill').values
            dxy_roc = (dxy_close - np.roll(dxy_close, 5)) / np.roll(dxy_close, 5) * 100
            dxy_roc_ok_arr = (dxy_roc <= 0)   # DXY 5 日下降才進場

    # 銅金比 (Copper/Gold ratio)
    cuau_arr = None
    if cuau_min is not None:
        cu_df = _load_cross_market('HG=F')
        au_df = _load_cross_market('GC=F')
        if cu_df is not None and au_df is not None:
            cu = cu_df['Close'].reindex(df.index, method='ffill').values
            au = au_df['Close'].reindex(df.index, method='ffill').values
            ratio = np.where(au > 0, cu / au * 1000, np.nan)  # 乘1000讓數值更直觀
            # 計算相對位置（過去 250 天百分位）
            cuau_arr = np.full(len(df), np.nan)
            for i in range(250, len(df)):
                window = ratio[i-250:i]
                valid = window[~np.isnan(window)]
                if len(valid) > 100:
                    pct = (np.sum(valid <= ratio[i]) / len(valid)) * 100
                    cuau_arr[i] = pct

    # 成交量序列（流動性過濾用）
    vol_arr = df['Volume'].values if 'Volume' in df.columns else None

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

    # 🆕 VWAP 預載（盤中執行優化 + 進階變體）
    vwap_arr = None
    vwap_high_arr = None  # HighOfDay
    vwap_low_arr  = None  # LowOfDay
    vwap_close_arr = None  # 日內 close
    needs_vwap = (use_VWAP_ENTRY or use_VWAP_EXEC or use_VWAP_EXIT
                  or vwap_dev_pct is not None or vwap_band_n is not None
                  or use_STRONGCL or use_WEAKCL)
    if needs_vwap and ticker:
        try:
            from pathlib import Path as _P
            vwap_path = _P(__file__).parent / 'vwap_cache' / f'{ticker}.parquet'
            if vwap_path.exists():
                vw_df = pd.read_parquet(vwap_path)
                # 對齊到日線（vw_df index 是日期 datetime）
                vw_aligned = vw_df['VWAP'].reindex(df.index, method='nearest',
                                                    tolerance=pd.Timedelta('1D'))
                vwap_arr = vw_aligned.values
                if 'HighOfDay' in vw_df.columns:
                    vwap_high_arr = vw_df['HighOfDay'].reindex(df.index, method='nearest',
                                                               tolerance=pd.Timedelta('1D')).values
                if 'LowOfDay' in vw_df.columns:
                    vwap_low_arr = vw_df['LowOfDay'].reindex(df.index, method='nearest',
                                                              tolerance=pd.Timedelta('1D')).values
                if 'Close' in vw_df.columns:
                    vwap_close_arr = vw_df['Close'].reindex(df.index, method='nearest',
                                                             tolerance=pd.Timedelta('1D')).values
        except Exception:
            vwap_arr = None

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

        # 執行優化：流動性過濾（LIQ）
        if liq_min_vol is not None and vol_arr is not None and i >= 60:
            recent_vol = vol_arr[max(0, i-60):i]
            valid_vol = recent_vol[~np.isnan(recent_vol)]
            if len(valid_vol) > 30:
                avg_vol = np.mean(valid_vol)
                if avg_vol < liq_min_vol:
                    return False, False

        # 跨市場：VIX 過高時禁止進場（VIX）
        if vix_max_th is not None and vix_series is not None:
            if not np.isnan(vix_series[i]) and vix_series[i] > vix_max_th:
                return False, False

        # 跨市場：S&P500 必須多頭（SPX）
        if use_SPX and spx_bull_arr is not None:
            if not spx_bull_arr[i]:
                return False, False

        # ─── Ensemble Voting：N 個保護條件中至少 K 個同意 ───
        if ens_min_votes is not None:
            votes = 0
            total_protectors = 0

            # 投票項 1：DXY 弱美元
            if dxy_bear_arr is not None:
                total_protectors += 1
                if dxy_bear_arr[i]: votes += 1

            # 投票項 2：VIX 在合理範圍
            if vix_series is not None:
                total_protectors += 1
                if not np.isnan(vix_series[i]) and vix_series[i] < 25:
                    votes += 1

            # 投票項 3：SPX 多頭
            if spx_bull_arr is not None:
                total_protectors += 1
                if spx_bull_arr[i]: votes += 1

            # 投票項 4：週 ADX 強勢
            if week_adx_daily is not None:
                total_protectors += 1
                if not np.isnan(week_adx_daily[i]) and week_adx_daily[i] >= 20:
                    votes += 1

            # 投票項 5：產業 specific 過濾
            if use_IND:
                total_protectors += 1
                ind_map = _load_industry_map()
                industry = ind_map.get(ticker, '')
                ind_filter = _INDUSTRY_FILTERS.get(industry)
                ind_passes = False
                if ind_filter == 'SOX' and sox_bull_arr is not None:
                    ind_passes = sox_bull_arr[i]
                elif ind_filter == 'HG' and hg_bull_arr is not None:
                    ind_passes = hg_bull_arr[i]
                elif ind_filter == 'DXY' and dxy_bear_arr is not None:
                    ind_passes = dxy_bear_arr[i]
                else:
                    ind_passes = True   # 沒對應產業則自動通過
                if ind_passes: votes += 1

            # 投票項 6：波動率 regime（低波動加分）
            if vol_regime_arr is not None:
                total_protectors += 1
                if vol_regime_arr[i] < 70:   # 非極端高波動
                    votes += 1

            # 動態門檻：根據實際載入的保護器數量調整
            if total_protectors >= ens_min_votes:
                if votes < ens_min_votes:
                    return False, False

        # 跨市場：美元下行才進場（DXY，弱美元利新興市場）
        if use_DXY and dxy_bear_arr is not None:
            if not dxy_bear_arr[i]:
                return False, False

        # 跨市場：DXY 強度過濾（動態，DXYS{N}）
        if dxy_strength_th is not None and dxy_strong_bear_arr is not None:
            if not dxy_strong_bear_arr[i]:
                return False, False

        # 跨市場：10年美債殖利率上限（TNX）
        if tnx_max_th is not None and tnx_series is not None:
            if not np.isnan(tnx_series[i]) and tnx_series[i] > tnx_max_th:
                return False, False

        # 基本面：財報季避險（5/8/11/3 月禁止新倉）
        if use_ER:
            month = dates[i].month
            if month in (3, 5, 8, 11):
                return False, False

        # 事件驅動：ATR 異常放大過濾
        if avol_max_th is not None and atr_ma60_arr is not None and i >= 60:
            if not np.isnan(atr_ma60_arr[i]) and atr_ma60_arr[i] > 0 and not np.isnan(atr[i]):
                if atr[i] / atr_ma60_arr[i] > avol_max_th:
                    return False, False

        # 事件驅動：衝擊事件後 5 天冷卻
        if shk_pct_th is not None and daily_chg_arr is not None and i >= 5:
            recent_max_chg = np.max(daily_chg_arr[max(0, i-5):i+1])
            if recent_max_chg > shk_pct_th:
                return False, False

        # 多時間框架：週 RSI 過濾（週深度回檔才允許 T3 拉回加碼）
        if use_WRSI and week_rsi_daily is not None:
            if not np.isnan(week_rsi_daily[i]):
                # T1 黃金交叉不過濾，僅 T3 須週 RSI<60（避免週線過熱時加碼）
                pass  # 在 T3 區塊獨立處理

        # 多時間框架：週 ADX 過濾
        if use_WADX and week_adx_daily is not None:
            if not np.isnan(week_adx_daily[i]) and week_adx_daily[i] < 22:
                return False, False

        # 波動率 regime 過濾（VR{N}：當前波動率百分位 > N 時不進場）
        if vol_regime_th is not None and vol_regime_arr is not None:
            if vol_regime_arr[i] > vol_regime_th:
                return False, False

        # 🆕 月營收 YoY 連續 N 月正成長才進場（REVUP{N}）
        if revup_n is not None and revup_ok_arr is not None:
            if not bool(revup_ok_arr[i]):
                return False, False

        # 🆕 VWAPENTRY：只在 close < 當日 VWAP 才進場（買在均價以下）
        if use_VWAP_ENTRY and vwap_arr is not None:
            cur_vwap = vwap_arr[i] if i < len(vwap_arr) else None
            if cur_vwap is not None and not np.isnan(cur_vwap):
                if pr[i] >= cur_vwap:
                    return False, False

        # 🆕 VWAPDEV{N}：偏離 ≥ N% 才進場（更嚴格的 VWAPENTRY）
        if vwap_dev_pct is not None and vwap_arr is not None:
            cur_vwap = vwap_arr[i] if i < len(vwap_arr) else None
            if cur_vwap is not None and not np.isnan(cur_vwap) and cur_vwap > 0:
                dev = (cur_vwap - pr[i]) / cur_vwap * 100  # close 比 VWAP 低多少 %
                if dev < vwap_dev_pct:
                    return False, False

        # 🆕 VWAPBAND{N}：close ≤ VWAP - N × (day_range/4) 才進場
        # （day_range/4 ≈ daily σ；N=1 表示 1σ 通道）
        if vwap_band_n is not None and vwap_arr is not None and vwap_high_arr is not None and vwap_low_arr is not None:
            if i < len(vwap_arr):
                cur_vwap = vwap_arr[i]
                cur_h = vwap_high_arr[i]
                cur_l = vwap_low_arr[i]
                if (not np.isnan(cur_vwap) and not np.isnan(cur_h)
                        and not np.isnan(cur_l) and cur_h > cur_l):
                    sigma = (cur_h - cur_l) / 4.0
                    threshold = cur_vwap - vwap_band_n * sigma
                    if pr[i] > threshold:
                        return False, False

        # 🆕 STRONGCL：前日 close 位於日內 70% 以上才進場（強勢追勢）
        if use_STRONGCL and vwap_high_arr is not None and vwap_low_arr is not None and vwap_close_arr is not None:
            if i >= 1 and i-1 < len(vwap_arr):
                ph = vwap_high_arr[i-1]
                pl = vwap_low_arr[i-1]
                pc = vwap_close_arr[i-1]
                if not (np.isnan(ph) or np.isnan(pl) or np.isnan(pc)) and ph > pl:
                    strength = (pc - pl) / (ph - pl)
                    if strength < 0.70:
                        return False, False

        # 🆕 WEAKCL：前日 close 位於日內 30% 以下才進場（逢低）
        if use_WEAKCL and vwap_high_arr is not None and vwap_low_arr is not None and vwap_close_arr is not None:
            if i >= 1 and i-1 < len(vwap_arr):
                ph = vwap_high_arr[i-1]
                pl = vwap_low_arr[i-1]
                pc = vwap_close_arr[i-1]
                if not (np.isnan(ph) or np.isnan(pl) or np.isnan(pc)) and ph > pl:
                    strength = (pc - pl) / (ph - pl)
                    if strength > 0.30:
                        return False, False

        # 🆕 PER / PBR / 殖利率 過濾（基本面估值）
        if pe_arr is not None and i < len(pe_arr):
            cur_pe = pe_arr[i]
            cur_pbr = pbr_arr[i] if pbr_arr is not None else None
            cur_div = div_arr[i] if div_arr is not None else None
            # PEPOS：PER > 0（過濾虧損）— FinMind 對虧損公司 PER 為 NaN 或極大
            if use_PEPOS:
                if cur_pe is None or np.isnan(cur_pe) or cur_pe <= 0 or cur_pe > 200:
                    return False, False
            if pe_max is not None:
                if cur_pe is None or np.isnan(cur_pe) or cur_pe > pe_max:
                    return False, False
            if pe_min is not None:
                if cur_pe is None or np.isnan(cur_pe) or cur_pe < pe_min:
                    return False, False
            if use_PEMID:
                if cur_pe is None or np.isnan(cur_pe) or cur_pe < 10 or cur_pe > 30:
                    return False, False
            if div_min is not None and cur_div is not None:
                if np.isnan(cur_div) or cur_div < div_min:
                    return False, False
            if pbr_max is not None and cur_pbr is not None:
                if np.isnan(cur_pbr) or cur_pbr > pbr_max:
                    return False, False
            # 進階：PER 動量（盈餘上修偵測）— 比 60 日前下降 N%
            if pe_mom_pct is not None and i >= 60:
                pe_60d_ago = pe_arr[i-60]
                if (cur_pe is not None and not np.isnan(cur_pe)
                        and pe_60d_ago is not None and not np.isnan(pe_60d_ago)
                        and pe_60d_ago > 0):
                    drop_pct = (pe_60d_ago - cur_pe) / pe_60d_ago * 100
                    if drop_pct < pe_mom_pct:
                        return False, False
                else:
                    return False, False
            # 進階：PER 相對自己 90 日中位數
            if pe_rel_pct is not None and i >= 90:
                window = pe_arr[i-90:i]
                valid = window[~np.isnan(window) & (window > 0)]
                if len(valid) >= 30:
                    median90 = np.median(valid)
                    if cur_pe is None or np.isnan(cur_pe) or cur_pe > median90 * (1 - pe_rel_pct/100):
                        return False, False
                else:
                    return False, False
            # 進階：PER < 自己 90 日平均（簡化版）
            if use_PEAVG and i >= 90:
                window = pe_arr[i-90:i]
                valid = window[~np.isnan(window) & (window > 0)]
                if len(valid) >= 30:
                    avg90 = np.mean(valid)
                    if cur_pe is None or np.isnan(cur_pe) or cur_pe >= avg90:
                        return False, False
                else:
                    return False, False

        # 🆕 券資比過濾（軋空潛力 / 避開過熱）
        if ms_arr is not None and i < len(ms_arr):
            cur_ms = ms_arr[i]
            if ms_min is not None:
                if cur_ms is None or np.isnan(cur_ms) or cur_ms < ms_min:
                    return False, False
            if ms_max is not None:
                if cur_ms is None or np.isnan(cur_ms) or cur_ms > ms_max:
                    return False, False
            if ms_mom is not None and i >= 60:
                ms_60d = ms_arr[i-60]
                if (cur_ms is not None and not np.isnan(cur_ms)
                        and ms_60d is not None and not np.isnan(ms_60d)
                        and ms_60d > 0):
                    rise_pct = (cur_ms - ms_60d) / ms_60d * 100
                    if rise_pct < ms_mom:
                        return False, False
                else:
                    return False, False

        # 🆕 毛利率 YoY 上升才進場（MARGUP）
        if use_MARGUP and margup_ok_arr is not None:
            if not bool(margup_ok_arr[i]):
                return False, False

        # 三大法人合計買超才進場（INST）
        if use_INST and inst_total_arr is not None:
            if not np.isnan(inst_total_arr[i]) and inst_total_arr[i] <= 0:
                return False, False

        # 外資 N 日累積買超才進場（FOR / FORN{N}）
        if use_FOR and inst_foreign_arr is not None and i >= for_days:
            recent_for = inst_foreign_arr[i-for_days:i+1]
            valid = recent_for[~np.isnan(recent_for)]
            if len(valid) >= for_days // 2:
                cum_for = np.sum(valid)
                if cum_for <= 0:
                    return False, False

        # 深化跨市場：黃金多頭時不進場（避險情緒高）
        if use_GLD and gld_bull_arr is not None:
            if gld_bull_arr[i]:
                return False, False

        # 深化跨市場：銅多頭時才進場（景氣強）
        if use_HG and hg_bull_arr is not None:
            if not hg_bull_arr[i]:
                return False, False

        # 深化跨市場：費半多頭才進場
        if use_SOX and sox_bull_arr is not None:
            if not sox_bull_arr[i]:
                return False, False

        # 深化跨市場：VIX 下行才進場（恐慌平息）
        if use_VIXTR and vix_falling_arr is not None:
            if i >= 5 and not vix_falling_arr[i]:
                return False, False

        # 深化跨市場：DXY 變化率過濾
        if use_DXYROC and dxy_roc_ok_arr is not None:
            if i >= 5 and not dxy_roc_ok_arr[i]:
                return False, False

        # 深化跨市場：銅金比 > N 百分位才進場
        if cuau_min is not None and cuau_arr is not None:
            if i >= 250 and not np.isnan(cuau_arr[i]):
                if cuau_arr[i] < cuau_min:
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

        # 多時間框架：週 RSI < 60 才允許 T3（避免週線過熱時加碼）
        if use_WRSI and week_rsi_daily is not None:
            if not np.isnan(week_rsi_daily[i]) and week_rsi_daily[i] >= 60:
                return False, False

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

        # 🆕 VWAP_EXEC：進場價調整（用 min(close, VWAP) 模擬盤中等待）
        ep_adjusted = pr[i]
        if use_VWAP_EXEC and vwap_arr is not None and i < len(vwap_arr):
            cur_vwap = vwap_arr[i]
            if not np.isnan(cur_vwap):
                ep_adjusted = min(pr[i], cur_vwap)

        return dict(
            ed=dates[i], ei=i, ep=ep_adjusted,
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
    pos_cum_pct = 0.0     # POS 用：累積已實現收益率%（可被 PR 重置）
    at_dynamic_th = 0.0   # AT 用：線上自適應 POS 門檻（贏放寬、輸收緊）

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

            # 🆕 DYNSTOP：動態 ATR 停損（持倉 > 30d 且獲利 > 20% → ATR×1.5 trailing）
            eff_stop_p = stop_p
            if use_DYNSTOP and stop_p is not None:
                holding_days = (dates[i] - p['ed']).days
                cur_profit_pct = (pr[i] - ep) / ep * 100 if ep > 0 else 0
                if holding_days > 30 and cur_profit_pct > 20:
                    cur_atr = atr[i] if not np.isnan(atr[i]) else p.get('atr_at_entry', 0)
                    if cur_atr > 0:
                        tight_stop = pr[i] - cur_atr * 1.5
                        # Trail only (don't lower the stop)
                        eff_stop_p = max(stop_p, tight_stop)

            hit_stop = (eff_stop_p is not None) and (pr[i] < eff_stop_p)
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
                # 執行優化：滑價（進出場各扣 SLP%）
                eff_ep = ep
                eff_xp = pr[i]
                # 🆕 VWAP_EXEC：出場價用 max(close, VWAP)（賣在均價以上）
                # NOSTOP 隔離驗證：停損觸發時跳過 VWAP（用市價，反映真實風險）
                _apply_vwap = use_VWAP_EXEC and not (use_VWAP_NOSTOP and hit_stop)
                if _apply_vwap and vwap_arr is not None and i < len(vwap_arr):
                    cur_vwap = vwap_arr[i]
                    if not np.isnan(cur_vwap):
                        eff_xp = max(pr[i], cur_vwap)
                if slippage_pct is not None:
                    slp = slippage_pct / 100.0
                    eff_ep = ep * (1 + slp)
                    eff_xp = eff_xp * (1 - slp)
                r = (eff_xp - eff_ep) / eff_ep
                trades.append((r, p['mult'], hit_stop or do_exit_t))
                positions.remove(p)
                had_exit = True
                # 更新 POS 累積指標
                pos_cum_pct += r * p['mult'] * 100
                # AT 線上自適應：贏 → 門檻降低 0.5；輸 → 門檻升高 1
                if use_AT:
                    if r > 0:
                        at_dynamic_th = max(0.0, at_dynamic_th - 0.5)
                    else:
                        at_dynamic_th = min(15.0, at_dynamic_th + 1.0)
                if r < 0:
                    consec_loss += 1
                    realized_loss_pct += abs(r * p['mult']) * 100
                    # PR 規則：停損時重置 POS 累積（清零）
                    if use_PR and (hit_stop or do_exit_t):
                        pos_cum_pct = 0.0
                else:
                    consec_loss = 0

        # ── Step 2：進場檢查（同日有出場則跳過，與 bt 原語意一致）──
        skip_entry = had_exit and not positions
        # 黑天鵝防護：危險窗（trigger 起 21 日內）暫停 T1/T3 進場
        if use_BSGUARD and bs_danger_set is not None:
            cur_date_str = pd.Timestamp(dates[i]).strftime('%Y-%m-%d')
            if cur_date_str in bs_danger_set:
                skip_entry = True
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

                        # D1 階梯式門檻
                        eff_th = pyramid_th
                        if use_PS:
                            eff_th = pyramid_th + 5.0 * n_pos
                        # D4 軟上限
                        if use_PSL:
                            if n_pos >= 6:
                                eff_th = max(eff_th, pyramid_th + 10.0)
                            elif n_pos >= 4:
                                eff_th = max(eff_th, pyramid_th + 5.0)

                        # D3 加碼間距
                        gap_ok = True
                        if pg_days is not None and positions:
                            last_entry = max(p['ed'] for p in positions)
                            gap = (dates[i] - last_entry).days
                            if gap < pg_days:
                                gap_ok = False

                        # 自適應 BD：過去 250 天 bull_days 比例需 >= 門檻
                        bd_ok = True
                        if bull_days_th is not None and i >= 250:
                            past_e20 = e20[i-250:i]
                            past_e60 = e60[i-250:i]
                            valid = ~np.isnan(past_e20) & ~np.isnan(past_e60)
                            if np.sum(valid) > 100:
                                bull_pct = np.sum((past_e20 > past_e60) & valid) / np.sum(valid) * 100
                                if bull_pct < bull_days_th:
                                    bd_ok = False

                        # 自適應 POS：累積已實現損益必須達門檻才允許加碼
                        pos_ok = True
                        if v7_pos_only:
                            min_th = pos_min_pct if pos_min_pct is not None else 0.0
                            # AT 動態門檻覆蓋：使用線上學習的門檻
                            if use_AT:
                                min_th = max(min_th, at_dynamic_th)
                            if pos_cum_pct < min_th:
                                pos_ok = False

                        # 異常波動偵測（ANOM）
                        anom_ok = True
                        if anom_atr_th is not None and atr_med60_arr is not None:
                            if not np.isnan(atr_med60_arr[i]) and atr_med60_arr[i] > 0:
                                if atr[i] / atr_med60_arr[i] > anom_atr_th:
                                    anom_ok = False

                        # 強化學習決策：用 Q-table 查最佳 action
                        rl_ok = True
                        if use_RL and rl_q_table:
                            # 構造當下 state
                            _vix = vix_series[i] if vix_series is not None else None
                            _dxy_bear = dxy_bear_arr[i] if dxy_bear_arr is not None else False
                            _spx_bull = spx_bull_arr[i] if spx_bull_arr is not None else True
                            # bull_days_pct 過去 250 天
                            if i >= 250:
                                past = (e20[i-250:i] > e60[i-250:i])
                                _bd_pct = float(np.sum(past) / 250 * 100)
                            else:
                                _bd_pct = 50.0
                            state = _rl_discretize_state(
                                pos_pnl_pct = pos_cum_pct,
                                dxy_bear = _dxy_bear,
                                vix = _vix,
                                spx_bull = _spx_bull,
                                n_pos = len(positions),
                                rsi = rsi[i],
                                bull_days_pct = _bd_pct,
                            )
                            q_hold = rl_q_table.get((state, 0), 0.0)
                            q_pyramid = rl_q_table.get((state, 1), 0.0)
                            if q_pyramid <= q_hold:
                                rl_ok = False

                        all_above_th = all(
                            ((pr[i] - p['ep']) / p['ep'] * 100) >= eff_th
                            for p in positions
                        )
                        if all_above_th and gap_ok and bd_ok and pos_ok and rl_ok and anom_ok:
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

    # IND 模式：依產業動態啟用對應跨市場過濾
    if flags.get('use_IND'):
        ind_map = _load_industry_map()
        industry = ind_map.get(ticker, '')
        ind_filter = _INDUSTRY_FILTERS.get(industry)
        if ind_filter == 'SOX':
            flags['use_SOX'] = True
        elif ind_filter == 'HG':
            flags['use_HG'] = True
        elif ind_filter == 'DXY':
            flags['use_DXY'] = True
        # 其他產業不加過濾，保留純 POS

    is_inv = ticker in INVERSE_ETF
    main_trades = _run_v7_strategy(df, flags, is_inverse_etf=is_inv, ticker=ticker)
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
