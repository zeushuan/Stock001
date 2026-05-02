"""大盤買持回測（TWII / SPY）— 對比我們策略
"""
import sys
sys.path.insert(0, '.')
from backtest_strategy import START_DATE
import pandas as pd
import numpy as np

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl


def backtest_buy_hold(ticker, label, start_date=START_DATE):
    """純買入持有回測"""
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 100:
            return None
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(start_date)]
        if len(df) < 100:
            return None

        c = df['Close'].values
        first = c[0]; last = c[-1]
        years = (df.index[-1] - df.index[0]).days / 365.25
        total_ret = (last - first) / first * 100
        cagr = ((last / first) ** (1/years) - 1) * 100

        # Daily returns for Sharpe
        daily_ret = pd.Series(c).pct_change().dropna()
        sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() > 0 else 0

        # MDD
        peak = pd.Series(c).cummax()
        dd = (pd.Series(c) - peak) / peak
        mdd = dd.min() * 100

        return {
            'label': label,
            'ticker': ticker,
            'period': f'{df.index[0].strftime("%Y-%m-%d")} → {df.index[-1].strftime("%Y-%m-%d")}',
            'years': round(years, 2),
            'first': round(first, 2),
            'last': round(last, 2),
            'total_return_pct': round(total_ret, 2),
            'cagr_pct': round(cagr, 2),
            'sharpe': round(sharpe, 2),
            'mdd_pct': round(mdd, 2),
        }
    except Exception as e:
        return {'error': str(e)}


def main():
    print("📊 大盤買持回測 vs 我們策略")
    print(f"  期間: {START_DATE} → 現在 (~6 年)")
    print()

    indices = [
        ('^TWII', '🇹🇼 TWII (台股加權)'),
        ('^GSPC', '🇺🇸 SPY (S&P 500)'),
        ('^SOX',  '🇺🇸 SOX (費城半導體)'),
    ]

    print("=" * 90)
    print(f"{'指數':>30}{'年數':>7}{'起始':>10}{'結束':>10}{'總報酬':>10}{'CAGR':>9}{'Sharpe':>8}{'MDD':>9}")
    print("=" * 90)
    for ticker, label in indices:
        r = backtest_buy_hold(ticker, label)
        if r and 'error' not in r:
            print(f"{r['label']:>30}{r['years']:>7.2f}{r['first']:>10.2f}{r['last']:>10.2f}"
                  f"{r['total_return_pct']:>+9.2f}%{r['cagr_pct']:>+8.2f}%{r['sharpe']:>8.2f}{r['mdd_pct']:>+8.2f}%")
        elif r:
            print(f"{label}: error {r['error']}")
        else:
            print(f"{label}: not in cache")

    print()
    print("=" * 90)
    print("📊 對比：我們策略 OOS 數據（從 walk-forward / sweep 結果）")
    print("=" * 90)
    strategies = [
        ('🇹🇼 倒鎚 h30 + pos=50 + drop_deep (OOS)',  7.30, 1.74,  -4.64),
        ('🇹🇼 T1_V7 h30 + pos=10 + FIFO (OOS)',     14.78, 0.81, -25.55),
        ('🇺🇸 US TOP T1_V7 h60 + pos=10',           16.40, 2.74,  -5.64),
        ('🇺🇸 US TOP T1_V7 h30 + pos=10',           14.35, 2.41, -19.02),
    ]
    print(f"{'策略':>50}{'CAGR':>9}{'Sharpe':>8}{'MDD':>9}")
    print("-" * 90)
    for label, cagr, sharpe, mdd in strategies:
        print(f"{label:>50}{cagr:>+8.2f}%{sharpe:>8.2f}{mdd:>+8.2f}%")

    print()
    print("=" * 90)
    print("📋 結論")
    print("=" * 90)
    print("  TWII 6 年表現超強（含 AI 大行情）→ 個股策略要超越大盤不容易")
    print("  S&P 500 6 年穩健（CAGR ~10%）→ 我們 US TOP T1_V7 hold=60 大幅超越（+16.40%）")
    print("  策略 vs 大盤要看：CAGR + Sharpe（風險調整後）+ MDD（下行保護）")


if __name__ == '__main__':
    main()
