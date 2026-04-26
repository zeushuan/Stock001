"""
強化學習：Tabular Q-Learning 學「該不該加碼」決策

State 設計（離散化）：
  pos_pnl_state    累積已實現收益：<0 / 0~10% / 10~50% / >50%   (4 levels)
  dxy_state        DXY 方向：bear / bull                         (2 levels)
  vix_state        VIX 等級：<20 / 20~30 / >30                   (3 levels)
  spx_state        SPX 多頭：bull / bear                         (2 levels)
  n_pos_state      現有倉位數：0 / 1-3 / 4-7 / 8+               (4 levels)
  rsi_state        當前 RSI：<30 / 30-50 / 50-70 / >70           (4 levels)
  bull_days_state  過去 250 天多頭比例：<40 / 40-60 / >60       (3 levels)
共 4×2×3×2×4×4×3 = 2304 states

Action 空間：
  0 = 不加碼（保持）
  1 = 加碼

Reward 設計：
  進入動作後 60 天的「持倉收益率」 - 滑價成本
  懲罰：若 60 天內被停損，給負獎勵

Algorithm：Tabular Q-Learning with ε-greedy
  α (學習率) = 0.1
  γ (折扣) = 0.95
  ε (探索率) = 0.2 → 0.05 (decay)
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import json
import numpy as np
import pandas as pd
import random
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

import data_loader as dl
import variant_strategy as vs


# ─── State 離散化 ─────────────────────────────────────────────
def discretize_state(pos_pnl_pct, dxy_bear, vix, spx_bull, n_pos, rsi, bull_days_pct):
    """把連續特徵轉成離散 state tuple"""
    # pos_pnl
    if pos_pnl_pct < 0: s_pnl = 0
    elif pos_pnl_pct < 10: s_pnl = 1
    elif pos_pnl_pct < 50: s_pnl = 2
    else: s_pnl = 3

    s_dxy = 1 if dxy_bear else 0

    if vix is None or np.isnan(vix): s_vix = 1
    elif vix < 20: s_vix = 0
    elif vix < 30: s_vix = 1
    else: s_vix = 2

    s_spx = 1 if spx_bull else 0

    if n_pos == 0: s_pos = 0
    elif n_pos < 4: s_pos = 1
    elif n_pos < 8: s_pos = 2
    else: s_pos = 3

    if rsi is None or np.isnan(rsi): s_rsi = 1
    elif rsi < 30: s_rsi = 0
    elif rsi < 50: s_rsi = 1
    elif rsi < 70: s_rsi = 2
    else: s_rsi = 3

    if bull_days_pct is None or np.isnan(bull_days_pct): s_bd = 1
    elif bull_days_pct < 40: s_bd = 0
    elif bull_days_pct < 60: s_bd = 1
    else: s_bd = 2

    return (s_pnl, s_dxy, s_vix, s_spx, s_pos, s_rsi, s_bd)


# ─── 訓練樣本生成 ────────────────────────────────────────────
def generate_samples(ticker, file_path):
    """
    對單支股票，每個 T1/T3 進場機會產生 (state, action, reward) 樣本
    用 P0_T1T3 模式跑，每個進場機會評估「加碼 vs 不加碼」的 60 天後表現
    """
    try:
        df_raw = pd.read_parquet(file_path)
    except: return []

    df_f = vs._filter_period(df_raw)
    if df_f is None or df_f.empty or len(df_f) < 100:
        return []

    pr = df_f['Close'].values
    e20 = df_f['e20'].values
    e60 = df_f['e60'].values
    e120 = df_f['e120'].values
    rsi = df_f['rsi'].values
    adx = df_f['adx'].values
    n = len(pr)

    # 載入跨市場
    try:
        vix_df = dl.load_from_cache('^VIX')
        vix_arr = vix_df['Close'].reindex(df_f.index, method='ffill').values if vix_df is not None else None
    except: vix_arr = None
    try:
        spx_df = dl.load_from_cache('^GSPC')
        spx_bull_arr = (spx_df['e20'].reindex(df_f.index, method='ffill').values >
                        spx_df['e60'].reindex(df_f.index, method='ffill').values) if spx_df is not None else None
    except: spx_bull_arr = None
    try:
        dxy_df = dl.load_from_cache('DX-Y.NYB')
        dxy_bear_arr = (dxy_df['e20'].reindex(df_f.index, method='ffill').values <
                        dxy_df['e60'].reindex(df_f.index, method='ffill').values) if dxy_df is not None else None
    except: dxy_bear_arr = None

    samples = []
    cum_pnl = 0.0     # 該股累積已實現收益（POS 機制）
    n_pos = 0         # 模擬倉位數

    for i in range(60, n - 60):  # 留 60 天給 reward 計算
        # 進場條件檢查
        if any(np.isnan([e20[i], e60[i], adx[i]])): continue
        if not (e20[i] > e60[i] and adx[i] >= 22): continue

        is_t1 = False
        if not any(np.isnan([e20[i-1], e60[i-1]])):
            if e20[i-1] <= e60[i-1] and e20[i] > e60[i]:
                is_t1 = True

        is_t3 = False
        if not is_t1:
            if not np.isnan(e120[i]) and not np.isnan(e120[i-60]) and e120[i-60] != 0:
                if (e120[i] - e120[i-60]) / abs(e120[i-60]) * 100 >= -2.0:
                    if not np.isnan(rsi[i]) and rsi[i] < 50:
                        is_t3 = True

        if not (is_t1 or is_t3): continue

        # 計算 bull_days_pct（過去 250 天）
        if i >= 250:
            past = (e20[i-250:i] > e60[i-250:i])
            bd_pct = float(np.sum(past) / 250 * 100)
        else:
            bd_pct = 50.0

        # 構造 state
        state = discretize_state(
            pos_pnl_pct = cum_pnl * 100,
            dxy_bear = dxy_bear_arr[i] if dxy_bear_arr is not None else False,
            vix = vix_arr[i] if vix_arr is not None else None,
            spx_bull = spx_bull_arr[i] if spx_bull_arr is not None else True,
            n_pos = n_pos,
            rsi = rsi[i],
            bull_days_pct = bd_pct,
        )

        # Reward：60 天後的收益率
        future_ret = (pr[i+60] - pr[i]) / pr[i]
        # 限制 reward 範圍（避免極端值）
        future_ret = max(-0.5, min(1.0, future_ret))

        # 兩個 action 都記錄樣本（離線學習）
        # action 0 = 不加碼 → reward = 0（等同無變化）
        # action 1 = 加碼 → reward = future_ret
        samples.append((state, 0, 0.0))
        samples.append((state, 1, future_ret))

        # 更新模擬狀態（為了 cum_pnl 與 n_pos 隨進場推進）
        if cum_pnl >= 0:    # POS 條件：累積為正才加碼
            n_pos += 1
            # 60 天後結算
            cum_pnl += future_ret * 0.5    # 簡化：假設半倉模擬

    return samples


# ─── Q-Learning 訓練 ───────────────────────────────────────
def train_q_table(samples, alpha=0.1, gamma=0.0, epochs=3):
    """
    Tabular Q-Learning（這裡用 supervised 風格簡化版）：
      Q(s, a) ← (1-α) Q(s, a) + α * reward
    γ=0 因為樣本獨立（每 60 天 reward 不延伸）
    """
    q_table = {}
    print(f"訓練樣本數：{len(samples):,}")

    for epoch in range(epochs):
        random.shuffle(samples)
        for state, action, reward in samples:
            key = (state, action)
            old_q = q_table.get(key, 0.0)
            new_q = old_q + alpha * (reward - old_q)
            q_table[key] = new_q
        print(f"  Epoch {epoch+1}/{epochs}: Q-table size = {len(q_table)}")

    return q_table


# ─── 主程式 ───────────────────────────────────────────────
def main():
    print("━━━━━━ 強化學習：Q-Learning 訓練加碼決策 ━━━━━━\n")

    from v8_runner import load_tickers
    tickers = load_tickers()
    print(f"股票數：{len(tickers)}")

    print("\n[1/3] 並行產生訓練樣本（12 workers）...")
    t0 = time.time()
    all_samples = []
    with ProcessPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(generate_samples, tk, str(dl.cache_path(tk))): tk
                   for tk in tickers}
        for fut in as_completed(futures):
            try:
                samples = fut.result(timeout=30)
                all_samples.extend(samples)
            except: pass
    print(f"完成：{len(all_samples):,} 個樣本 / {time.time()-t0:.1f}s\n")

    if len(all_samples) < 1000:
        print("樣本太少，無法訓練")
        return

    print("[2/3] Q-Learning 訓練...")
    t1 = time.time()
    q_table = train_q_table(all_samples, alpha=0.1, epochs=3)
    print(f"完成：{time.time()-t1:.1f}s\n")

    # ─── 分析學到的策略 ─────────────────────────────────
    print("━━━ Q-table 學習結果分析 ━━━")
    # 對每個 state，比較 a=0 vs a=1 的 Q 值
    states_seen = set(s for (s, _) in q_table.keys())
    print(f"探索過的 state：{len(states_seen)}")

    # 分類 state：哪些 state 偏好 action=1（加碼）？
    prefer_pyramid = []
    prefer_hold = []
    no_diff = []
    for state in states_seen:
        q0 = q_table.get((state, 0), 0)
        q1 = q_table.get((state, 1), 0)
        diff = q1 - q0
        if diff > 0.02:
            prefer_pyramid.append((state, diff, q0, q1))
        elif diff < -0.02:
            prefer_hold.append((state, diff, q0, q1))
        else:
            no_diff.append(state)

    print(f"  偏好加碼：{len(prefer_pyramid)} states")
    print(f"  偏好不加碼：{len(prefer_hold)} states")
    print(f"  無差異：{len(no_diff)} states")

    # 印 TOP 偏好加碼 states
    print("\n━━━ TOP 10 偏好加碼的 state（學到的進攻時機）━━━")
    state_names = ['累積', 'DXY', 'VIX', 'SPX', '倉位', 'RSI', '多頭天']
    state_labels = {
        'pos_pnl': ['<0', '0~10', '10~50', '>50'],
        'dxy':     ['bull', 'bear'],
        'vix':     ['<20', '20-30', '>30'],
        'spx':     ['bear', 'bull'],
        'n_pos':   ['0', '1-3', '4-7', '8+'],
        'rsi':     ['<30', '30-50', '50-70', '>70'],
        'bd':      ['<40%', '40-60%', '>60%'],
    }
    keys = ['pos_pnl', 'dxy', 'vix', 'spx', 'n_pos', 'rsi', 'bd']

    prefer_pyramid.sort(key=lambda x: -x[1])
    for state, diff, q0, q1 in prefer_pyramid[:10]:
        labels = [state_labels[k][state[i]] for i, k in enumerate(keys)]
        print(f"  {' / '.join(labels):<50}  Q1-Q0={diff:+.3f}  Q1={q1:+.3f}")

    print("\n━━━ TOP 10 偏好不加碼的 state（學到的避險時機）━━━")
    prefer_hold.sort(key=lambda x: x[1])
    for state, diff, q0, q1 in prefer_hold[:10]:
        labels = [state_labels[k][state[i]] for i, k in enumerate(keys)]
        print(f"  {' / '.join(labels):<50}  Q1-Q0={diff:+.3f}  Q1={q1:+.3f}")

    # 儲存 Q-table
    print("\n[3/3] 儲存 Q-table...")
    q_serializable = {}
    for (state, action), val in q_table.items():
        q_serializable[f"{state}_{action}"] = val
    with open('q_table.json', 'w', encoding='utf-8') as f:
        json.dump(q_serializable, f, indent=2)
    print(f"已儲存 q_table.json（{len(q_serializable)} 個項目）")


if __name__ == '__main__':
    main()
