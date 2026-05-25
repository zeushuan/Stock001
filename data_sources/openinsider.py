"""openinsider — 全市場 Form 4 集群買入掃描
======================================================

對應規格《Smart Money》§2.2 / §8.3 Phase 3。
OpenInsider 提供免費 HTML 篩選器，pandas.read_html 可直接解析。

核心用法（規格周末工作流）:
  1. 掃近 N 天「P=Purchase, Value ≥ $X」的全市場集群買入
  2. 依 ticker group by → 找出 cluster_size ≥ 2 的候選池
  3. 餵給 smart_money_signals.compute_sms() 算完整 SMS

公開 API:
  - scan_cluster_buys(days=30, min_value=100_000, min_cluster=2) → pd.DataFrame
  - top_clusters_by_value(df, n=20) → pd.DataFrame
  - candidate_tickers(df, min_cluster=2) → list[str]

CLI:
  python -m data_sources.openinsider                  # 預設掃 30d, ≥$100K, ≥2 人
  python -m data_sources.openinsider --days 14 --min-cluster 3
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from typing import Optional

import pandas as pd

log = logging.getLogger("stock001.data_sources.openinsider")


# OpenInsider 篩選器基礎 URL
_URL_BASE = "http://openinsider.com/screener"


def _parse_dollar(s) -> float:
    """'$1,234,567' → 1234567.0；負值 '-$1,234' → -1234"""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return 0.0
    text = str(s).replace(',', '').replace('$', '').strip()
    try:
        return float(text)
    except Exception:
        return 0.0


def _parse_pct(s) -> float:
    """'+368%' → 368.0；'NaN' → 0"""
    if s is None or pd.isna(s):
        return 0.0
    try:
        return float(re.sub(r'[+%]', '', str(s)))
    except Exception:
        return 0.0


def scan_cluster_buys(
    days: int = 30,
    min_value: float = 100_000,
    min_cluster: int = 2,
) -> pd.DataFrame:
    """掃 OpenInsider 全市場「公開市場買入」(P)，回傳清洗後 DataFrame。

    Args:
        days: 回看天數
        min_value: 單筆買入金額下限（USD）
        min_cluster: ticker 至少幾位 insider 才視為集群

    Returns:
        DataFrame columns:
          Ticker / FilingDate / TradeDate / Insider / Title /
          TradeType / Price / Qty / Value / ClusterSize / TotalValue
        已過濾出 Trade Type 為 P，且該 ticker cluster_size ≥ min_cluster
    """
    # OpenInsider 篩選器參數:
    # xp=1 公開市場買入 / fd=days 近 N 天 / vl=min 金額下限 / cnt=500 取多列
    url = (f"{_URL_BASE}?xp=1&xs=1&vl={int(min_value/1000)}"
           f"&fd={days}&cnt=500")
    try:
        tables = pd.read_html(url)
    except Exception as exc:
        log.warning("[openinsider] read_html failed: %s", exc)
        return pd.DataFrame()

    # 找主表（最大的）
    main = None
    for t in tables:
        if (isinstance(t, pd.DataFrame) and t.shape[0] > 10
                and 'Ticker' in t.columns):
            main = t
            break
    if main is None or main.empty:
        return pd.DataFrame()

    # 清理欄位（'Filing\xa0Date' → 'FilingDate' 等）
    rename_map = {c: c.replace('\xa0', '').replace(' ', '') for c in main.columns}
    df = main.rename(columns=rename_map).copy()

    # 只留 P (Purchase) — Trade Type 可能是 'P - Purchase'
    if 'TradeType' in df.columns:
        df = df[df['TradeType'].astype(str).str.startswith('P')]
    if df.empty:
        return pd.DataFrame()

    # 清 Value / Qty / Price
    df['Value'] = df['Value'].apply(_parse_dollar)
    df['Qty']   = df['Qty'].apply(_parse_dollar)
    df['Price'] = df['Price'].apply(_parse_dollar)
    # ΔOwn % 數值化
    if 'ΔOwn' in df.columns:
        df['DeltaOwnPct'] = df['ΔOwn'].apply(_parse_pct)
    # FilingDate / TradeDate 轉日期
    for col in ('FilingDate', 'TradeDate'):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # 過濾 ≥ min_value（OpenInsider 篩選器有最小單位，這裡再嚴格過一次）
    df = df[df['Value'] >= min_value]

    # 計算每 ticker 的 cluster size + total value
    if 'Insider' not in df.columns and 'InsiderName' in df.columns:
        df['Insider'] = df['InsiderName']
    grp = df.groupby('Ticker').agg(
        ClusterSize=('Insider', lambda s: s.nunique()),
        TotalValue=('Value', 'sum'),
        MaxValue=('Value', 'max'),
        FirstBuy=('TradeDate', 'min'),
        LastBuy=('TradeDate', 'max'),
    )
    df = df.merge(grp, on='Ticker', how='left')

    # 過濾集群數
    df = df[df['ClusterSize'] >= min_cluster]
    return df.sort_values(
        ['TotalValue', 'TradeDate'], ascending=[False, False]
    ).reset_index(drop=True)


def top_clusters_by_value(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """從 scan_cluster_buys 輸出取 top N ticker（依 TotalValue 排）。

    每 ticker 一列彙總（不是每筆交易）。
    """
    if df is None or df.empty:
        return pd.DataFrame()
    agg = df.groupby('Ticker').agg(
        CompanyName=('CompanyName', 'first'),
        ClusterSize=('ClusterSize', 'first'),
        TotalValue=('TotalValue', 'first'),
        MaxValue=('MaxValue', 'first'),
        FirstBuy=('FirstBuy', 'first'),
        LastBuy=('LastBuy', 'first'),
        Insiders=('Insider', lambda s: ', '.join(sorted(set(s)))[:80]),
        Titles=('Title', lambda s: ', '.join(sorted(set(s)))[:80]),
    ).sort_values('TotalValue', ascending=False)
    return agg.head(n).reset_index()


def candidate_tickers(df: pd.DataFrame, min_cluster: int = 3) -> list[str]:
    """產出候選 ticker 清單，給 smart_money_signals 後續評分用。"""
    if df is None or df.empty:
        return []
    flt = df[df['ClusterSize'] >= min_cluster]
    return sorted(flt['Ticker'].unique().tolist())


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog='data_sources.openinsider',
        description='Scan OpenInsider for cluster buying (P transactions)',
    )
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--min-value', type=float, default=100_000,
                        help='min single transaction USD (default $100K)')
    parser.add_argument('--min-cluster', type=int, default=2,
                        help='min distinct insiders per ticker (default 2)')
    parser.add_argument('--top', type=int, default=20,
                        help='show top N tickers by TotalValue')
    args = parser.parse_args(argv)

    print(f'\n=== OpenInsider Cluster Scan ===')
    print(f'  days={args.days}  min_value=${args.min_value:,.0f}  '
          f'min_cluster={args.min_cluster}')
    df = scan_cluster_buys(
        days=args.days,
        min_value=args.min_value,
        min_cluster=args.min_cluster,
    )
    if df.empty:
        print('  (no matches)')
        return 0
    top = top_clusters_by_value(df, n=args.top)
    print(f'\n  found {len(top)} candidate tickers '
          f'(cluster_size ≥ {args.min_cluster})')
    print(f'\n  {"Ticker":7s} {"Company":30s}  {"Cluster":>7s}  '
          f'{"TotalValue":>13s}  {"MaxBuy":>13s}  Insiders / Titles')
    print('  ' + '-' * 130)
    for _, row in top.iterrows():
        co = (str(row['CompanyName'])[:30] if pd.notna(row['CompanyName']) else '')
        print(f'  {row["Ticker"]:7s} {co:30s}  '
              f'{int(row["ClusterSize"]):>7d}  '
              f'${row["TotalValue"]:>11,.0f}  '
              f'${row["MaxValue"]:>11,.0f}  '
              f'[{row["Titles"][:50]}]')
    return 0


if __name__ == '__main__':
    sys.exit(main())
