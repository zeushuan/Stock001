"""台股 ↔ 美股連動分析（含時差校正）
=========================================
時區邏輯：
  美東 EST = UTC-5（冬）/ UTC-4（夏）
  台北 TST = UTC+8
  美股收盤（台北次日 04:00-05:00）→ 影響台股次日開盤
  台股收盤（美東當日 01:30）→ 美股當日盤前可參考

對齊方式：
  US_close[t] → TW_open/close[t+1]（lag-0 前一日美股影響台股當日）
  即：分析 TW(t) 變化 vs US(t-1) 變化

研究問題：
  Q1: 大盤連動 — SP500 t-1 → TWII t 相關係數
  Q2: NASDAQ → TWII（科技股影響）
  Q3: 費半 SOX → 台積電/類股
  Q4: DXY 升 → TW 跌？
  Q5: VIX 升 → TW 跌幅放大？
  Q6: 跳空 — US 漲 N% → TW 隔日 gap-up 機率
  Q7: 個股級別 — TSM ADR 與 2330 隔日連動
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl


def daily_returns(df, col='Close'):
    """日報酬 (close-to-close %)"""
    s = df[col].copy()
    return s.pct_change() * 100


def gap_up_pct(df):
    """跳空高開幅度 = (today_open - prev_close) / prev_close × 100
    若無 Open 欄位用 Close 替代"""
    if 'Open' in df.columns:
        return (df['Open'] / df['Close'].shift(1) - 1) * 100
    return df['Close'].pct_change() * 100


def align_lag(us_series, tw_series, lag=1):
    """對齊：tw[t] vs us[t-lag]
    lag=1 = 前一日美股影響台股當日"""
    us_lagged = us_series.shift(lag)
    df = pd.DataFrame({'us': us_lagged, 'tw': tw_series}).dropna()
    return df


def correlation_analysis(name_us, name_tw, us_df, tw_df, lags=[0, 1, 2]):
    """跨市場相關係數（不同 lag）"""
    us_ret = daily_returns(us_df)
    tw_ret = daily_returns(tw_df)
    print(f"\n📊 {name_us} ↔ {name_tw} 相關係數")
    print(f"  {'Lag':<10} {'相關係數':>10} {'樣本':>8}")
    print(f"  {'-'*40}")
    results = {}
    for lag in lags:
        df = align_lag(us_ret, tw_ret, lag=lag)
        if len(df) < 30:
            print(f"  Lag {lag:<6} 樣本不足")
            continue
        corr = df['us'].corr(df['tw'])
        n = len(df)
        results[f'lag_{lag}'] = {'corr': float(corr), 'n': int(n)}
        meaning = ('US 同日影響' if lag == 0 else
                   f'US t-{lag} 影響 TW t' if lag > 0 else
                   f'TW 領先 {-lag} 日')
        print(f"  Lag {lag} ({meaning}): {corr:>+.3f}  n={n}")
    return results


def spillover_analysis(us_name, tw_name, us_df, tw_df):
    """漲跌幅分群 spillover：US t-1 漲 N% → TW t 統計"""
    us_ret = daily_returns(us_df)
    tw_ret = daily_returns(tw_df)
    df = align_lag(us_ret, tw_ret, lag=1)

    # 5 個分群
    bins = [(-100, -2), (-2, -0.5), (-0.5, 0.5), (0.5, 2), (2, 100)]
    labels = ['<-2%', '-2~-0.5%', '-0.5~0.5%', '0.5~2%', '>2%']
    print(f"\n📊 {us_name} 漲跌 → {tw_name} 隔日反應")
    print(f"  {'US 區間':<14} {'樣本':>5} {'TW 均報%':>9} {'TW 中位%':>9} {'勝率%':>7}")
    print(f"  {'-'*50}")
    out = {}
    for (lo, hi), lbl in zip(bins, labels):
        mask = (df['us'] >= lo) & (df['us'] < hi)
        sub = df[mask]
        if len(sub) < 5:
            continue
        mean = sub['tw'].mean()
        median = sub['tw'].median()
        win = (sub['tw'] > 0).mean() * 100
        out[lbl] = {'n': int(len(sub)), 'mean': float(mean),
                    'median': float(median), 'win': float(win)}
        print(f"  {lbl:<14} {len(sub):>5} {mean:>+9.2f} {median:>+9.2f} "
              f"{win:>+7.1f}")
    return out


def gap_analysis(us_name, tw_name, us_df, tw_df):
    """跳空分析：US t-1 漲跌 → TW t gap-up 機率"""
    us_ret = daily_returns(us_df)
    tw_gap = gap_up_pct(tw_df)
    us_lagged = us_ret.shift(1)
    df = pd.DataFrame({'us': us_lagged, 'gap': tw_gap}).dropna()
    if len(df) < 30: return None

    print(f"\n📊 {us_name} 漲跌 → {tw_name} 隔日跳空 gap")
    print(f"  {'US 區間':<14} {'樣本':>5} {'TW gap 均%':>10} {'gap-up 機率%':>11}")
    print(f"  {'-'*55}")
    bins = [(-100, -2), (-2, -0.5), (-0.5, 0.5), (0.5, 2), (2, 100)]
    labels = ['<-2%', '-2~-0.5%', '-0.5~0.5%', '0.5~2%', '>2%']
    out = {}
    for (lo, hi), lbl in zip(bins, labels):
        mask = (df['us'] >= lo) & (df['us'] < hi)
        sub = df[mask]
        if len(sub) < 5: continue
        mean = sub['gap'].mean()
        gap_up_prob = (sub['gap'] > 0).mean() * 100
        out[lbl] = {'n': int(len(sub)), 'mean_gap': float(mean),
                    'gap_up_prob': float(gap_up_prob)}
        print(f"  {lbl:<14} {len(sub):>5} {mean:>+10.2f} {gap_up_prob:>+11.1f}")
    return out


def regression_strength(us_name, tw_name, us_df, tw_df):
    """回歸係數（彈性）：TW = β × US + α"""
    us_ret = daily_returns(us_df)
    tw_ret = daily_returns(tw_df)
    df = align_lag(us_ret, tw_ret, lag=1)
    if len(df) < 30: return None

    # OLS
    x = df['us'].values
    y = df['tw'].values
    beta = np.cov(x, y)[0, 1] / np.var(x)
    alpha = y.mean() - beta * x.mean()
    r2 = (np.corrcoef(x, y)[0, 1]) ** 2
    return {'beta': float(beta), 'alpha': float(alpha), 'r2': float(r2),
            'n': int(len(df))}


def main():
    print("=" * 100)
    print("🌏 台股 ↔ 美股連動分析（時差校正：US t-1 → TW t）")
    print("=" * 100)
    print("時區邏輯：美股當日收盤 = 台北次日 04-05 點 → 影響台股次日開盤")
    print("分析期間：2020-01-01 ~ 2026-04-30")
    print()

    # 載入大盤資料
    indices = {}
    for name in ['^GSPC', '^IXIC', '^SOX', '^TWII', '^DXY', '^VIX']:
        try:
            df = dl.load_from_cache(name)
            if df is None: continue
            # 統一 tz
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df = df.copy()
                df.index = df.index.tz_localize(None)
            df = df[df.index >= pd.Timestamp('2020-01-01')]
            indices[name] = df
            print(f"✓ {name}: {len(df)} 日 [{df.index[0].date()} ~ {df.index[-1].date()}]")
        except Exception as e:
            print(f"✗ {name}: {e}")
    print()

    twii = indices.get('^TWII')
    spx = indices.get('^GSPC')
    nasdaq = indices.get('^IXIC')
    sox = indices.get('^SOX')
    dxy = indices.get('^DXY')
    vix = indices.get('^VIX')

    if twii is None or spx is None:
        print("❌ 必要資料缺失")
        return

    all_results = {}

    # ── Q1: 大盤對台股的相關性（多 lag）
    print("=" * 100)
    print("Q1: 大盤連動相關係數 (多 lag 分析)")
    print("=" * 100)
    if spx is not None:
        all_results['SPX_TWII'] = correlation_analysis(
            'S&P 500 (^GSPC)', '台股加權 (^TWII)', spx, twii,
            lags=[0, 1, 2])
    if nasdaq is not None:
        all_results['NASDAQ_TWII'] = correlation_analysis(
            'NASDAQ (^IXIC)', '台股加權 (^TWII)', nasdaq, twii,
            lags=[0, 1, 2])
    if sox is not None:
        all_results['SOX_TWII'] = correlation_analysis(
            '費半 (^SOX)', '台股加權 (^TWII)', sox, twii,
            lags=[0, 1, 2])
    if dxy is not None:
        all_results['DXY_TWII'] = correlation_analysis(
            '美元指數 (^DXY)', '台股加權 (^TWII)', dxy, twii,
            lags=[0, 1, 2])
    if vix is not None:
        all_results['VIX_TWII'] = correlation_analysis(
            '恐慌指數 (^VIX)', '台股加權 (^TWII)', vix, twii,
            lags=[0, 1, 2])

    # ── Q2: Spillover 分群分析 ──
    print("\n" + "=" * 100)
    print("Q2: 美股漲跌幅 → 台股次日反應（Spillover）")
    print("=" * 100)
    all_results['SPX_spillover'] = spillover_analysis(
        'S&P 500', '台股 TWII', spx, twii)
    if nasdaq is not None:
        all_results['NASDAQ_spillover'] = spillover_analysis(
            'NASDAQ', '台股 TWII', nasdaq, twii)
    if sox is not None:
        all_results['SOX_spillover'] = spillover_analysis(
            '費半 SOX', '台股 TWII', sox, twii)

    # ── Q3: 跳空分析 ──
    print("\n" + "=" * 100)
    print("Q3: 跳空分析 (US 漲跌 → TW 開盤 gap)")
    print("=" * 100)
    all_results['SPX_gap'] = gap_analysis(
        'S&P 500', '台股 TWII', spx, twii)
    if sox is not None:
        all_results['SOX_gap'] = gap_analysis(
            '費半 SOX', '台股 TWII', sox, twii)

    # ── Q4: 個股級別 — 台積電（2330）vs TSM ADR
    print("\n" + "=" * 100)
    print("Q4: 個股級別 — 2330 (台積電) vs 美股相關")
    print("=" * 100)
    try:
        tsmc = dl.load_from_cache('2330')
        if hasattr(tsmc.index, 'tz') and tsmc.index.tz is not None:
            tsmc = tsmc.copy()
            tsmc.index = tsmc.index.tz_localize(None)
        tsmc = tsmc[tsmc.index >= pd.Timestamp('2020-01-01')]
        print(f"  2330 樣本: {len(tsmc)} 日")
        all_results['SPX_2330'] = correlation_analysis(
            'S&P 500', '2330 台積電', spx, tsmc, lags=[0, 1, 2])
        if sox is not None:
            all_results['SOX_2330'] = correlation_analysis(
                '費半 SOX', '2330 台積電', sox, tsmc, lags=[0, 1, 2])
        if nasdaq is not None:
            all_results['NASDAQ_2330'] = correlation_analysis(
                'NASDAQ', '2330 台積電', nasdaq, tsmc, lags=[0, 1, 2])

        # TSM ADR
        try:
            tsm = dl.load_from_cache('TSM')
            if hasattr(tsm.index, 'tz') and tsm.index.tz is not None:
                tsm = tsm.copy()
                tsm.index = tsm.index.tz_localize(None)
            tsm = tsm[tsm.index >= pd.Timestamp('2020-01-01')]
            print(f"  TSM ADR: {len(tsm)} 日")
            all_results['TSM_2330'] = correlation_analysis(
                'TSM ADR', '2330 台積電', tsm, tsmc, lags=[0, 1, 2])
        except: pass
    except Exception as e:
        print(f"  2330 載入失敗: {e}")

    # ── Q5: 回歸彈性（β）──
    print("\n" + "=" * 100)
    print("Q5: 回歸彈性 — 美股漲 1% 帶動台股漲多少")
    print("=" * 100)
    print(f"  {'美股指數':<25} {'β (彈性)':>10} {'R²':>8} {'n':>6}")
    print(f"  {'-'*55}")
    for name, df_us in [('S&P 500', spx), ('NASDAQ', nasdaq),
                         ('費半 SOX', sox)]:
        if df_us is None: continue
        r = regression_strength(name, '台股 TWII', df_us, twii)
        if r:
            print(f"  {name:<25} {r['beta']:>+10.3f} {r['r2']:>8.3f} "
                  f"{r['n']:>6}")
            all_results[f'beta_{name}'] = r

    # ── Q6: 半導體類股（2330/2454/2303）vs SOX
    print("\n" + "=" * 100)
    print("Q6: 半導體類股 vs 費半 SOX 連動")
    print("=" * 100)
    if sox is not None:
        for tk in ['2330', '2454', '2303', '2379']:
            try:
                stk = dl.load_from_cache(tk)
                if stk is None: continue
                if hasattr(stk.index, 'tz') and stk.index.tz is not None:
                    stk = stk.copy()
                    stk.index = stk.index.tz_localize(None)
                stk = stk[stk.index >= pd.Timestamp('2020-01-01')]
                r = regression_strength(f'SOX→{tk}', tk, sox, stk)
                if r:
                    name_map = {'2330': '台積電', '2454': '聯發科',
                                '2303': '聯電', '2379': '瑞昱'}
                    print(f"  SOX → {tk} {name_map.get(tk, ''):<8} "
                          f"β={r['beta']:+.3f}  R²={r['r2']:.3f}  n={r['n']}")
                    all_results[f'SOX_{tk}'] = r
            except: pass

    # 寫 JSON
    with open('tw_us_linkage.json', 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n💾 寫入 tw_us_linkage.json")

    # 結論摘要
    print("\n" + "=" * 100)
    print("💡 結論摘要")
    print("=" * 100)
    for k in ['SPX_TWII', 'SOX_TWII', 'NASDAQ_TWII']:
        if k in all_results:
            data = all_results[k]
            lag1 = data.get('lag_1', {})
            print(f"  {k}: lag-1 相關 {lag1.get('corr', 0):+.3f}")


if __name__ == '__main__':
    main()
