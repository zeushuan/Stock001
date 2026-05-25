"""edgar_form4 — Form 4 內部人交易抓取 (SMS Phase 1 MVP)
======================================================

對應規格《Stock001_SmartMoney_Module_Spec.md》§2.1 / §4：
  - 抓取公司的 Form 4 申報（內部人交易）
  - 解析每筆交易 (Code / Shares / Price / Value / Date)
  - 按 Code 過濾 (P = 公開市場買入；S = 公開市場賣出；其他濾掉)
  - 集群偵測 (cluster buying: 30 日內多位 insider 同步買入)

公開 API:
  - fetch_form4_transactions(ticker, days=90, only_p=False) -> pd.DataFrame
        近 N 天的 Form 4 交易，合併成單一 DataFrame；
        only_p=True 只留 Code='P' (公開市場買入)
  - detect_cluster_buying(df, days=30, min_amount=100_000) -> dict
        判斷是否符合「集群買入」訊號（規格 §4.1.3）

Form 4 交易代碼（規格 附錄 B）:
  P = Open Market Purchase    ← 唯一看漲訊號
  S = Open Market Sale        (用於偵測集群賣出紅旗)
  M = Option Exercise         過濾
  A = Award/Grant             過濾
  G = Gift                    過濾
  F = Tax Withholding         過濾

CLI:
  python -m data_sources.edgar_form4 TSLA           # 近 90 日全部
  python -m data_sources.edgar_form4 TSLA --only-p  # 只看 P 買入
  python -m data_sources.edgar_form4 TSLA --cluster # 集群偵測
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from . import ensure_sec_identity


# 規格附錄 B：採用的交易代碼
SIGNAL_CODES = {'P': 'Open Market Purchase',  # 唯一看漲
                'S': 'Open Market Sale'}      # 集群賣出紅旗


def fetch_form4_transactions(
    ticker: str,
    days: int = 90,
    only_p: bool = False,
    max_filings: int = 100,
) -> pd.DataFrame:
    """抓近 N 天的 Form 4 交易，合併成單一 DataFrame。

    Args:
        ticker: 'TSLA' / 'NVDA' / ...
        days: 回看天數（預設 90，配合規格「近 90 日」前置閘門）
        only_p: 是否只留 Code='P' (公開市場買入)
        max_filings: 為避免歷史拉得太多，限制最多檢視幾份 filing

    Returns:
        DataFrame，columns 來自 Form4.to_dataframe():
          Transaction Type / Code / Description / Shares / Price / Value /
          Date / Form / Issuer / Ticker / Insider / Position / Remaining Shares
    """
    ensure_sec_identity()
    from edgar import Company

    co = Company(ticker)
    cutoff = datetime.now() - timedelta(days=days)
    filings = co.get_filings(form='4')
    take = filings.head(max_filings) if len(filings) > max_filings else filings

    rows: list[pd.DataFrame] = []
    for f in take:
        # 早於 cutoff 就停（filings 已按日期降序）
        try:
            f_date = pd.to_datetime(f.filing_date)
            if f_date < cutoff:
                break
        except Exception:
            pass
        try:
            obj = f.obj()
            df = obj.to_dataframe()
            if isinstance(df, pd.DataFrame) and not df.empty:
                rows.append(df)
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    # 過濾日期（to_dataframe 內 Date 是 datetime）
    if 'Date' in out.columns:
        out['Date'] = pd.to_datetime(out['Date'], errors='coerce')
        out = out[out['Date'] >= pd.Timestamp(cutoff)]
    if only_p:
        out = out[out['Code'] == 'P']
    return out.reset_index(drop=True)


def detect_cluster_buying(
    df: pd.DataFrame,
    days: int = 30,
    min_amount: float = 100_000,
) -> dict:
    """偵測「集群買入」訊號（規格 §4.1.3）。

    定義：
      - 過去 N 天內，多位不同 insider 都做了 P (買入)
      - 單筆或累計金額 ≥ min_amount

    Args:
        df: fetch_form4_transactions 的輸出
        days: 集群時間窗（預設 30）
        min_amount: 個別買入金額下限（USD）

    Returns:
        dict:
          - cluster_size: int (不同 insider 人數，only P)
          - has_cfo: bool (是否含 CFO — 規格「王炸」)
          - total_value: float (P 交易累計金額)
          - max_value: float (單筆最大金額)
          - insiders: list[dict]  (含 name / position / total_amount)
          - cluster_sells: int (集群賣出紅旗，S 交易的 insider 人數)
          - period_start / period_end: datetime
    """
    if df is None or df.empty:
        return {'cluster_size': 0, 'has_cfo': False, 'total_value': 0,
                'max_value': 0, 'insiders': [], 'cluster_sells': 0,
                'period_start': None, 'period_end': None}
    if 'Date' not in df.columns:
        return {'cluster_size': 0, 'has_cfo': False, 'total_value': 0,
                'max_value': 0, 'insiders': [], 'cluster_sells': 0,
                'period_start': None, 'period_end': None}

    end = pd.to_datetime(df['Date']).max()
    start = end - pd.Timedelta(days=days)
    window = df[(df['Date'] >= start) & (df['Date'] <= end)].copy()

    # P 買入分析
    p_df = window[window['Code'] == 'P'].copy()
    p_df['Value'] = pd.to_numeric(p_df.get('Value'), errors='coerce').fillna(0)
    big_p = p_df[p_df['Value'] >= min_amount]

    # 按 insider 聚合
    insiders = []
    has_cfo = False
    if not big_p.empty and 'Insider' in big_p.columns:
        grp = big_p.groupby(['Insider', 'Position'], as_index=False).agg(
            n_transactions=('Code', 'count'),
            total_amount=('Value', 'sum'),
            max_amount=('Value', 'max'),
        )
        for _, row in grp.iterrows():
            pos = str(row['Position'])
            if 'CFO' in pos.upper() or 'Chief Financial' in pos:
                has_cfo = True
            insiders.append({
                'name': str(row['Insider']),
                'position': pos,
                'n_transactions': int(row['n_transactions']),
                'total_amount': float(row['total_amount']),
                'max_amount': float(row['max_amount']),
            })

    # S 集群賣出（紅旗）
    s_df = window[window['Code'] == 'S']
    cluster_sells = (s_df['Insider'].nunique()
                     if not s_df.empty and 'Insider' in s_df.columns else 0)

    return {
        'cluster_size': len(insiders),
        'has_cfo': has_cfo,
        'total_value': float(big_p['Value'].sum()) if not big_p.empty else 0,
        'max_value': float(big_p['Value'].max()) if not big_p.empty else 0,
        'insiders': insiders,
        'cluster_sells': int(cluster_sells),
        'period_start': start,
        'period_end': end,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog='data_sources.edgar_form4',
        description='Fetch Form 4 insider transactions via edgartools',
    )
    parser.add_argument('ticker', help='Stock ticker (e.g. TSLA)')
    parser.add_argument('--days', type=int, default=90,
                        help='Days to look back (default 90, per spec §4.3 gate)')
    parser.add_argument('--only-p', action='store_true',
                        help='Only show Code=P (Open Market Purchase)')
    parser.add_argument('--cluster', action='store_true',
                        help='Run cluster buying detection (per spec §4.1.3)')
    parser.add_argument('--min-amount', type=float, default=100_000,
                        help='Min purchase amount for cluster (default $100K)')
    args = parser.parse_args(argv)

    print(f'\n=== Form 4: {args.ticker} (last {args.days} days) ===')
    df = fetch_form4_transactions(args.ticker, days=args.days, only_p=args.only_p)
    if df is None or df.empty:
        msg = ('no Form 4 transactions with Code=P' if args.only_p
               else 'no Form 4 transactions')
        print(f'  ({msg})')
        return 0

    # 摘要
    if 'Code' in df.columns:
        code_counts = df['Code'].value_counts().to_dict()
        codes_str = ' '.join(f'{c}={n}' for c, n in code_counts.items())
        print(f'  {len(df)} transactions, codes: {codes_str}')

    print('\n--- transactions ---')
    cols = [c for c in ['Date', 'Code', 'Insider', 'Position',
                          'Shares', 'Price', 'Value', 'Description']
            if c in df.columns]
    print(df[cols].to_string(index=False))

    if args.cluster:
        print('\n--- cluster buying detection ---')
        c = detect_cluster_buying(df, min_amount=args.min_amount)
        print(f'  period: {c["period_start"]} ~ {c["period_end"]}')
        print(f'  cluster_size (distinct insiders with P ≥ ${args.min_amount:,.0f}): '
              f'{c["cluster_size"]}')
        print(f'  has CFO ("王炸"): {c["has_cfo"]}')
        print(f'  total P value: ${c["total_value"]:,.0f}')
        print(f'  max single P:  ${c["max_value"]:,.0f}')
        if c['insiders']:
            print('  insiders:')
            for ins in c['insiders']:
                print(f'    - {ins["name"]:30s}  {ins["position"][:40]:40s}  '
                      f'${ins["total_amount"]:>12,.0f}  '
                      f'({ins["n_transactions"]} tx)')
        if c['cluster_sells'] >= 3:
            print(f'  🚨 RED FLAG: cluster sells = {c["cluster_sells"]} insiders')
    return 0


if __name__ == '__main__':
    sys.exit(main())
