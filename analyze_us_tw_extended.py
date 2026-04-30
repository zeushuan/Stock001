"""美股對台股 / 個股 延伸研究
==================================
A. 跨年度穩定性（年度 lag-1 相關）
B. 多頭 vs 空頭差異（市況分層）
C. Walk-forward 預測準確度

回測期：2020-01 ~ 2026-04（6 年 4 個月，1,476 對齊樣本）
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl


def load_returns(ticker):
    df = dl.load_from_cache(ticker)
    if df is None: return None
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)
    df = df[df.index >= pd.Timestamp('2020-01-01')]
    if len(df) < 100: return None
    return df['Close'].pct_change() * 100


def lag1_corr(us_ret, tw_ret):
    df = pd.concat([us_ret.shift(1), tw_ret], axis=1, join='inner').dropna()
    df.columns = ['us', 'tw']
    if len(df) < 30: return None, None, None
    corr = df['us'].corr(df['tw'])
    if np.var(df['us']) > 0:
        beta = np.cov(df['us'], df['tw'])[0, 1] / np.var(df['us'])
    else:
        beta = 0
    return float(corr), float(beta), len(df)


def main():
    twii = load_returns('^TWII')
    spx = load_returns('^GSPC')
    sox = load_returns('^SOX')
    vix = load_returns('^VIX')
    tsmc = load_returns('2330')
    tsm = load_returns('TSM')

    if any(x is None for x in [twii, spx, sox]):
        print("❌ 必要資料缺失")
        return

    # ============ A. 跨年度穩定性 ============
    print("=" * 100)
    print("A. 跨年度穩定性（每年 lag-1 相關係數）")
    print("=" * 100)

    pairs = [
        ('SPX → TWII', spx, twii),
        ('SOX → TWII', sox, twii),
        ('VIX → TWII', vix, twii) if vix is not None else None,
        ('SOX → 2330', sox, tsmc) if tsmc is not None else None,
        ('SPX → 2330', spx, tsmc) if tsmc is not None else None,
        ('TSM → 2330', tsm, tsmc) if tsm is not None and tsmc is not None else None,
    ]
    pairs = [p for p in pairs if p is not None]

    years = list(range(2020, 2027))
    print(f"\n{'指標 →':<25} {' '.join(f'{y:>7}' for y in years)} {'整體':>8}")
    print("-" * 100)

    annual_data = {}
    for name, x_ret, y_ret in pairs:
        cells = []
        for y in years:
            ystart = pd.Timestamp(f'{y}-01-01')
            yend = pd.Timestamp(f'{y}-12-31')
            x_y = x_ret[(x_ret.index >= ystart) & (x_ret.index <= yend)]
            y_y = y_ret[(y_ret.index >= ystart) & (y_ret.index <= yend)]
            corr, beta, n = lag1_corr(x_y, y_y)
            cells.append(f"{corr:+.2f}" if corr is not None else "  —  ")
        # 整體
        corr_all, _, _ = lag1_corr(x_ret, y_ret)
        cells_str = ' '.join(f"{c:>7}" for c in cells)
        print(f"{name:<25} {cells_str} {corr_all:>+8.3f}")
        annual_data[name] = cells

    # 穩定度評估（年度標準差）
    print(f"\n📊 跨年穩定度（標準差越小越穩）")
    print("-" * 100)
    for name, x_ret, y_ret in pairs:
        annual_corrs = []
        for y in years:
            ystart = pd.Timestamp(f'{y}-01-01')
            yend = pd.Timestamp(f'{y}-12-31')
            x_y = x_ret[(x_ret.index >= ystart) & (x_ret.index <= yend)]
            y_y = y_ret[(y_ret.index >= ystart) & (y_ret.index <= yend)]
            corr, _, n = lag1_corr(x_y, y_y)
            if corr is not None and n >= 100:
                annual_corrs.append(corr)
        if annual_corrs:
            arr = np.array(annual_corrs)
            print(f"  {name:<25} 均 {arr.mean():+.3f} | "
                  f"σ {arr.std():.3f} | "
                  f"max {arr.max():+.3f} | min {arr.min():+.3f}")

    # ============ B. 多頭 vs 空頭差異 ============
    print("\n" + "=" * 100)
    print("B. 多頭 vs 空頭差異（市況分層 lag-1 相關）")
    print("=" * 100)

    # 用 TWII 60 日報酬判斷市況
    twii_df = dl.load_from_cache('^TWII')
    if hasattr(twii_df.index, 'tz') and twii_df.index.tz is not None:
        twii_df = twii_df.copy()
        twii_df.index = twii_df.index.tz_localize(None)
    twii_df = twii_df[twii_df.index >= pd.Timestamp('2020-01-01')]
    twii_df['ret_60d'] = twii_df['Close'].pct_change(60) * 100

    bull_mask = twii_df['ret_60d'] > 5    # 60d > +5% 為多頭
    bear_mask = twii_df['ret_60d'] < -5   # 60d < -5% 為空頭
    side_mask = ~(bull_mask | bear_mask)  # 中間為盤整

    bull_dates = twii_df.index[bull_mask]
    bear_dates = twii_df.index[bear_mask]
    side_dates = twii_df.index[side_mask]
    print(f"\n  多頭日數 (TWII 60d > +5%): {len(bull_dates)}")
    print(f"  空頭日數 (TWII 60d < -5%): {len(bear_dates)}")
    print(f"  盤整日數: {len(side_dates)}")

    print(f"\n{'指標':<25} {'多頭 corr':>11} {'盤整 corr':>11} {'空頭 corr':>11}")
    print("-" * 100)
    for name, x_ret, y_ret in pairs:
        # 對齊各 mask
        df = pd.concat([x_ret.shift(1), y_ret], axis=1, join='inner').dropna()
        df.columns = ['us', 'tw']
        bull_df = df.loc[df.index.isin(bull_dates)]
        bear_df = df.loc[df.index.isin(bear_dates)]
        side_df = df.loc[df.index.isin(side_dates)]
        bull_corr = bull_df['us'].corr(bull_df['tw']) if len(bull_df) > 30 else None
        bear_corr = bear_df['us'].corr(bear_df['tw']) if len(bear_df) > 30 else None
        side_corr = side_df['us'].corr(side_df['tw']) if len(side_df) > 30 else None
        b_str = f"{bull_corr:+.3f}" if bull_corr is not None else "—"
        s_str = f"{side_corr:+.3f}" if side_corr is not None else "—"
        x_str = f"{bear_corr:+.3f}" if bear_corr is not None else "—"
        print(f"{name:<25} {b_str:>11} {s_str:>11} {x_str:>11}")

    # ============ C. Walk-Forward 預測準確度 ============
    print("\n" + "=" * 100)
    print("C. Walk-Forward 預測準確度（2020-2023 訓練 / 2024-2026 預測）")
    print("=" * 100)

    train_end = pd.Timestamp('2024-01-01')
    print(f"\n{'指標':<25} {'TRAIN β':>10} {'TEST β':>10} "
          f"{'β 變化':>10} {'TRAIN R²':>10} {'TEST R²':>10}")
    print("-" * 100)

    for name, x_ret, y_ret in pairs:
        df = pd.concat([x_ret.shift(1), y_ret], axis=1, join='inner').dropna()
        df.columns = ['us', 'tw']
        train = df[df.index < train_end]
        test = df[df.index >= train_end]
        if len(train) < 100 or len(test) < 100: continue
        # train β
        if np.var(train['us']) > 0:
            train_beta = np.cov(train['us'], train['tw'])[0, 1] / np.var(train['us'])
            train_corr = train['us'].corr(train['tw'])
            train_r2 = train_corr ** 2
        else: continue
        # test β
        if np.var(test['us']) > 0:
            test_beta = np.cov(test['us'], test['tw'])[0, 1] / np.var(test['us'])
            test_corr = test['us'].corr(test['tw'])
            test_r2 = test_corr ** 2
        else: continue
        beta_change = (test_beta - train_beta) / train_beta * 100 if train_beta else 0
        print(f"{name:<25} {train_beta:>+10.3f} {test_beta:>+10.3f} "
              f"{beta_change:>+9.0f}% {train_r2:>10.3f} {test_r2:>10.3f}")

    # ── 預測誤差量化 ──
    print(f"\n📊 預測誤差量化（用 train β + 昨夜美股報酬 → 預測今日台股報酬）")
    print(f"  指標：MAE（平均絕對誤差）/ 比 baseline（用 0%）改善 %")
    print("-" * 100)
    print(f"{'指標':<25} {'baseline MAE':>13} {'model MAE':>11} {'改善%':>9}")
    print("-" * 100)
    for name, x_ret, y_ret in pairs:
        df = pd.concat([x_ret.shift(1), y_ret], axis=1, join='inner').dropna()
        df.columns = ['us', 'tw']
        train = df[df.index < train_end]
        test = df[df.index >= train_end]
        if len(train) < 100 or len(test) < 100: continue
        if np.var(train['us']) <= 0: continue
        # train α, β
        beta = np.cov(train['us'], train['tw'])[0, 1] / np.var(train['us'])
        alpha = train['tw'].mean() - beta * train['us'].mean()
        # test 預測
        pred = alpha + beta * test['us']
        actual = test['tw']
        # MAE
        baseline_mae = np.abs(actual).mean()  # 預測都是 0
        model_mae = np.abs(actual - pred).mean()
        improve = (baseline_mae - model_mae) / baseline_mae * 100
        print(f"{name:<25} {baseline_mae:>+13.3f} {model_mae:>+11.3f} "
              f"{improve:>+8.1f}%")

    # 寫 JSON
    out = {
        'period': '2020-01 ~ 2026-04 (6.4 years)',
        'annual_corr': annual_data,
    }
    with open('us_tw_extended.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n💾 寫入 us_tw_extended.json")


if __name__ == '__main__':
    main()
