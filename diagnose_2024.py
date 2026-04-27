"""
A. 2024 黑盒診斷

問題：v8 各風格在 2024 年風報比都接近 0，但 2024 是 AI 主升段
直覺上應該大賺。為什麼？

實證假設：
  H1. 2024 加碼次數 vs 進場次數 比例改變 → 主升段太流暢、回調少
  H2. 2024 持倉天數縮短 → 早出場錯過主升段
  H3. 2024 大贏家集中度高 → 少數股暴漲、多數平庸
  H4. 2024 假突破/假死叉增加 → trend-following 被切碎
  H5. 2024 ATR/Price 異常（飆股波動大）→ 停損過嚴

每個假設驗證後給出明確的「2024 失效機制」。
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import data_loader as dl
import variant_strategy as vs


YEARS = [
    ('2020', '2020-01-02', '2020-12-31'),
    ('2021', '2021-01-01', '2021-12-31'),
    ('2022', '2022-01-01', '2022-12-31'),
    ('2023', '2023-01-01', '2023-12-31'),
    ('2024', '2024-01-01', '2024-12-31'),
    ('2025+', '2025-01-01', '2026-04-25'),
]


def run_one(args):
    ticker, mode, start, end = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r['n_trades'] == 0: return None
        return r
    except Exception:
        return None


def collect_stats(start, end, mode, tickers):
    args_list = [(t, mode, start, end) for t in tickers]
    rows = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for r in ex.map(run_one, args_list, chunksize=80):
            if r is not None: rows.append(r)
    if not rows: return None
    df = pd.DataFrame(rows)
    pct = df['pnl_pct'].values
    n_trades = df['n_trades'].values
    n_t4 = df['n_t4'].values
    main_trades = n_trades - n_t4

    return dict(
        n_stocks=len(df),
        n_trades_total=int(n_trades.sum()),
        n_t4_total=int(n_t4.sum()),
        n_main_total=int(main_trades.sum()),
        n_trades_per_stock=n_trades.mean(),
        n_main_per_stock=main_trades.mean(),
        mean_pnl=pct.mean(),
        median_pnl=np.median(pct),
        worst=pct.min(),
        best=pct.max(),
        win_rate=(pct > 0).mean() * 100,
        big_win_pct=(pct > 100).mean() * 100,
        bad_loss_pct=(pct < -30).mean() * 100,
        rr=pct.mean() / abs(pct.min()) if pct.min() < 0 else 0,
    )


def per_stock_metrics(df_year):
    """從 cache 計算每股各年度的飆股程度"""
    out = {}
    for yr, start, end in YEARS:
        df_slice = df_year[(df_year.index >= start) & (df_year.index <= end)]
        if len(df_slice) < 60: continue
        c = df_slice['Close'].values
        max_c = c.max(); min_c = c.min()
        ret = (c[-1] - c[0]) / c[0] * 100 if c[0] else 0
        max_dd = (max_c - min_c) / max_c * 100 if max_c else 0
        atr = df_slice['atr'].values
        atr_p_mean = (atr / c).mean() * 100
        out[yr] = dict(ret=ret, max_dd=max_dd, atr_p=atr_p_mean)
    return out


def main():
    files = sorted(Path('data_cache').glob('*.parquet'))
    tickers = [f.stem for f in files]
    print(f"載入 {len(tickers)} 檔資料")

    # ── 各年度 baseline 統計（用極致風控當代表）──
    mode = 'P0_T1T3+POS+IND+DXY'
    print(f"\n基準策略：{mode}\n")
    print("=" * 130)
    print(f"{'年':>6s} {'股數':>6s} {'總交易':>7s} {'主交易':>7s} {'T4交易':>7s} "
          f"{'每股交易':>9s} {'每股主':>8s} {'均值%':>8s} {'最差%':>8s} "
          f"{'勝率':>7s} {'>100%':>7s} {'<-30%':>7s} {'RR':>6s}")
    print("=" * 130)

    yearly_metrics = {}
    for yr, start, end in YEARS:
        m = collect_stats(start, end, mode, tickers)
        if not m: continue
        yearly_metrics[yr] = m
        print(f"{yr:>6s} {m['n_stocks']:>6d} {m['n_trades_total']:>7d} "
              f"{m['n_main_total']:>7d} {m['n_t4_total']:>7d} "
              f"{m['n_trades_per_stock']:>8.1f} {m['n_main_per_stock']:>7.1f} "
              f"{m['mean_pnl']:>+7.1f}% {m['worst']:>+7.1f}% "
              f"{m['win_rate']:>6.1f}% {m['big_win_pct']:>6.1f}% {m['bad_loss_pct']:>6.1f}% "
              f"{m['rr']:>5.2f}")

    # ── 計算 2024 vs 其他年的差異 ──
    print("\n" + "=" * 80)
    print("📊 2024 vs 其他年度 對比")
    print("=" * 80)

    if '2024' in yearly_metrics:
        m24 = yearly_metrics['2024']
        others = {y: m for y, m in yearly_metrics.items() if y != '2024'}
        if others:
            avg_others = {k: np.mean([m[k] for m in others.values()])
                          for k in m24 if isinstance(m24[k], (int, float))}
            print(f"{'指標':22s} {'2024':>10s} {'他年平均':>10s} {'差異':>10s}")
            for k in ['n_trades_per_stock', 'n_main_per_stock', 'mean_pnl',
                      'win_rate', 'big_win_pct', 'bad_loss_pct', 'rr']:
                if k in m24 and k in avg_others:
                    print(f"{k:22s} {m24[k]:>10.2f} {avg_others[k]:>10.2f} "
                          f"{m24[k]-avg_others[k]:>+10.2f}")

    # ── 大盤每年表現對比 ──
    print("\n" + "=" * 80)
    print("📈 大盤 (^TWII) 每年表現對比")
    print("=" * 80)
    twii = dl.load_from_cache('^TWII')
    if twii is not None:
        for yr, start, end in YEARS:
            sub = twii[(twii.index >= start) & (twii.index <= end)]
            if len(sub) < 5: continue
            ret = (sub['Close'].iloc[-1] - sub['Close'].iloc[0]) / sub['Close'].iloc[0] * 100
            atr_p = (sub['atr'] / sub['Close']).mean() * 100
            print(f"  {yr}: 大盤報酬 {ret:+6.1f}%, 平均 ATR/P {atr_p:.1f}%")


if __name__ == '__main__':
    main()
