"""VCP（Volatility Contraction Pattern）從 ZigZag pivots 偵測

VCP 的核心是「逐次收口」：
  - 一連串 H-L 對，每次回檔（contraction）幅度 < 前次
  - 量逐次衰減（最後一次最乾）
  - 收口完成後在頂部附近橫盤等突破

從 ZigZag pivots 序列來看：
  - 從最近的 H 往回，每對相鄰 H-L 就是一次「收口」
  - 收口寬度 = (H_price - L_price) / H_price
  - 連續 3 次以上收口逐次變小 = 真 VCP
"""
import numpy as np
import pandas as pd


def detect_vcp_from_pivots(df, pivots, min_contractions=3,
                              max_contractions=8,
                              vol_dryup_threshold=0.75):
    """從 ZigZag pivots 偵測 VCP

    回傳：
      {
        'is_vcp': bool,
        'contractions': [
            {'H_idx', 'H_date', 'H_price', 'L_idx', 'L_date', 'L_price',
             'width_pct', 'vol_avg', 'vol_ratio_to_first'}
            ...  # 從最舊到最新
        ],
        'num_contractions': int,
        'is_strictly_decreasing': bool,
        'final_contraction_pct': float,
        'volume_dry_up': bool,
        'breakout_pivot': pivot dict or None,  # 最後一個 pivot 是 H 且接近前 H
        'consolidation_top': float,  # VCP 的頂部水平線
        'pivot_buy_price': float,  # 突破買點（建議）
      }
    """
    if len(pivots) < 4:
        return {'is_vcp': False, 'reason': 'pivots 不足'}

    v = df['Volume'].values
    n = len(df)

    # 從最新往回，找 H-L-H-L-H-L ... 序列
    # 每個 H 後面接一個 L = 一次「contraction」
    contractions = []

    # 從尾巴開始往前找
    # 但要先確認最後是不是 H（橫盤頂）或 L（在底）
    # VCP 的標準是當下處於最後一個 H 之後的回檔末端 OR 在突破前
    # 所以我們從最近一個 H 開始找回

    # 找出所有 H 索引和 L 索引
    pivot_seq = [(p['type'], p) for p in pivots]

    # 從末端往前掃，找 H-L 對序列
    last_H = None
    pair_indices = []
    for i in range(len(pivots) - 1, -1, -1):
        if pivots[i]['type'] == 'H':
            last_H = i
            break

    if last_H is None:
        return {'is_vcp': False, 'reason': '無 H pivot'}

    # 從 last_H 往前抓 H-L-H-L 對
    # 每對 = 一次收口（H 之後緊鄰的 L）
    # 走法：last_H, 然後找它前面的 L, 然後找該 L 之前的 H, 依此類推
    contractions_raw = []
    cursor = last_H
    while cursor > 0:
        cur_pivot = pivots[cursor]
        if cur_pivot['type'] != 'H': break
        # 找前一個 L
        prev_L_idx = None
        for k in range(cursor - 1, -1, -1):
            if pivots[k]['type'] == 'L':
                prev_L_idx = k; break
        if prev_L_idx is None: break

        # 該 L 之後到 cur_H 之間的 contraction = cur_H - prev_L
        H_p = cur_pivot['price']
        L_p = pivots[prev_L_idx]['price']
        width_pct = (H_p - L_p) / H_p * 100

        # vol：H 到 L 區間的平均量
        L_idx_df = pivots[prev_L_idx]['idx']
        H_idx_df = cur_pivot['idx']
        seg_vol = v[min(L_idx_df, H_idx_df):max(L_idx_df, H_idx_df)+1]
        vol_avg = float(np.nanmean(seg_vol)) if len(seg_vol) > 0 else 0

        contractions_raw.append({
            'H_idx': cur_pivot['idx'], 'H_date': cur_pivot['date'], 'H_price': H_p,
            'L_idx': pivots[prev_L_idx]['idx'], 'L_date': pivots[prev_L_idx]['date'],
            'L_price': L_p,
            'width_pct': round(width_pct, 2),
            'vol_avg': vol_avg,
        })

        # 下一輪：找 prev_L 之前的 H
        next_H_idx = None
        for k in range(prev_L_idx - 1, -1, -1):
            if pivots[k]['type'] == 'H':
                next_H_idx = k; break
        if next_H_idx is None: break
        cursor = next_H_idx

    # 反轉：最舊在前，最新在後
    contractions = list(reversed(contractions_raw))

    if len(contractions) < min_contractions:
        return {'is_vcp': False,
                'reason': f'收口次數 {len(contractions)} < {min_contractions}',
                'contractions': contractions,
                'num_contractions': len(contractions)}

    if len(contractions) > max_contractions:
        # 只取最近 N 次
        contractions = contractions[-max_contractions:]

    # ─── 檢查條件 ───
    widths = [c['width_pct'] for c in contractions]
    vols = [c['vol_avg'] for c in contractions]

    # 1. 嚴格遞減（每次收口都比前次小）
    strictly_dec = all(widths[i+1] < widths[i] for i in range(len(widths)-1))
    # 2. 大致遞減（允許 1 次反彈，整體趨勢向下）
    # 計算「總體下降比例」
    avg_first_half = float(np.mean(widths[:len(widths)//2 or 1]))
    avg_second_half = float(np.mean(widths[len(widths)//2:]))
    overall_dec = avg_second_half < avg_first_half * 0.9

    # 3. 最後一次收口 < 10%
    final_pct = widths[-1]

    # 4. 量縮：最後一次 vol < 第一次 × threshold
    first_vol = vols[0] if vols[0] > 0 else 1
    last_vol = vols[-1]
    vol_ratio = last_vol / first_vol if first_vol > 0 else 1.0
    vol_dry_up = vol_ratio < vol_dryup_threshold

    # 計算每個 contraction 的 vol_ratio_to_first
    for c in contractions:
        c['vol_ratio_to_first'] = round(c['vol_avg'] / first_vol, 2) if first_vol > 0 else 1.0

    # 5. 整體收口 score（多項條件加總）
    score = 0
    if strictly_dec: score += 2
    elif overall_dec: score += 1
    if final_pct <= 5: score += 2
    elif final_pct <= 10: score += 1
    if vol_dry_up: score += 1
    if len(contractions) >= 4: score += 1
    if len(contractions) >= 5: score += 1   # 強化加分

    # is_vcp：score >= 3 且至少大致遞減
    is_vcp = (score >= 3) and (strictly_dec or overall_dec)

    # 6. 突破狀態
    cur_close = float(df['Close'].iloc[-1])
    consolidation_top = max(c['H_price'] for c in contractions)
    last_H_price = contractions[-1]['H_price']
    breakout_status = (
        'breakout' if cur_close > last_H_price * 1.005 else
        'pre_breakout' if cur_close > last_H_price * 0.95 else
        'still_contracting'
    )

    # 7. 進場建議
    pivot_buy_price = last_H_price * 1.005  # 突破最後一個 H 略上方
    stop_loss = contractions[-1]['L_price'] * 0.98

    return {
        'is_vcp': is_vcp,
        'num_contractions': len(contractions),
        'contractions': contractions,
        'widths_pct': widths,
        'strictly_decreasing': strictly_dec,
        'overall_decreasing': overall_dec,
        'final_contraction_pct': final_pct,
        'vol_ratio_last_to_first': round(vol_ratio, 2),
        'volume_dry_up': vol_dry_up,
        'consolidation_top': consolidation_top,
        'pivot_buy_price': pivot_buy_price,
        'breakout_status': breakout_status,
        'current_price': cur_close,
        'stop_loss': stop_loss,
        'vcp_score': score,
        'vcp_grade': 'A' if score >= 6 else ('B' if score >= 4 else ('C' if score >= 3 else 'D')),
    }
