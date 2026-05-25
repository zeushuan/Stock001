"""edgar_13f — 13F-HR 機構持倉抓取 (SMS Phase 1 MVP)
======================================================

對應規格《Stock001_SmartMoney_Module_Spec.md》§2.1 / §3：
  - 抓取機構的 13F-HR 申報
  - 取出 holdings DataFrame（含 Ticker / Cusip / Value / Shares）
  - 季對季比對（NEW / CLOSED / INCREASED / DECREASED）

公開 API:
  - fetch_13f_holdings(ticker_or_cik, n=1) -> list[dict]
        取最近 n 個 13F 申報，每個 dict 含 filing_date / accession / holdings
  - fetch_13f_compare(ticker_or_cik) -> dict
        最新 vs 前一季比對（NEW/INCREASED/DECREASED/CLOSED）
  - fetch_13f_history(ticker_or_cik, periods=4) -> list[dict]
        近 N 季趨勢

CLI:
  python -m data_sources.edgar_13f BRK-A          # 最新 holdings
  python -m data_sources.edgar_13f BRK-A --compare  # 季對季比對
  python -m data_sources.edgar_13f BRK-A --periods 4  # 4 季趨勢

注意：
  - 13F 為「機構申報」(管理 > $1 億美元的 fund advisor)。
  - 用 ticker 找的是「公司股票本身」；找 fund advisor 要用其管理公司名 / CIK。
    例：BRK-A → 找到 Berkshire 自己的 13F (它同時也是 fund advisor)。
        想抓 BlackRock 13F → Company('BLK') 或對應 CIK。
  - 13F 規格 45 天延遲，所以「最新」可能落後現在約 1-2 個月。
"""
from __future__ import annotations

import argparse
import sys
from typing import Optional

import pandas as pd

from . import ensure_sec_identity


def fetch_13f_holdings(ticker_or_cik: str, n: int = 1) -> list[dict]:
    """抓最近 n 個 13F-HR 申報，每個含完整持倉 DataFrame。

    Args:
        ticker_or_cik: 'BRK-A' / 'BLK' / CIK 數字
        n: 取最近幾季

    Returns:
        list[dict]，每個 dict:
          - filing_date: str 'YYYY-MM-DD'
          - accession: str (SEC accession number)
          - reporting_period: str 'YYYY-MM-DD' (季末)
          - manager_name: str
          - holdings: pd.DataFrame (cols: Issuer/Cusip/Value/Shares/Ticker/...)
          - n_holdings: int (列數)
          - total_value: int (Value 總和)
    """
    ensure_sec_identity()
    from edgar import Company

    co = Company(ticker_or_cik)
    filings = co.get_filings(form='13F-HR')
    if len(filings) == 0:
        return []
    # 取前 n 個
    take = filings.head(n) if n > 1 else [filings[0]]
    out: list[dict] = []
    for f in take:
        try:
            obj = f.obj()
        except Exception as exc:
            out.append({
                'filing_date': str(f.filing_date),
                'accession': f.accession_no,
                'error': f'parse failed: {type(exc).__name__}: {exc}',
            })
            continue
        df = getattr(obj, 'infotable', None)
        rec = {
            'filing_date': str(f.filing_date),
            'accession': f.accession_no,
            'reporting_period': str(getattr(obj, 'reporting_period', '') or ''),
            'manager_name': str(getattr(obj, 'filer_name', '') or co.name),
            'holdings': df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
            'n_holdings': int(len(df)) if isinstance(df, pd.DataFrame) else 0,
            'total_value': int(df['Value'].sum()) if (
                isinstance(df, pd.DataFrame) and 'Value' in df.columns) else 0,
        }
        out.append(rec)
    return out


def fetch_13f_compare(ticker_or_cik: str) -> dict:
    """最新一季 vs 前一季比對（NEW/INCREASED/DECREASED/CLOSED）。

    對應規格 §3.1「只看新建倉 + 大幅加倉」的核心 API。

    Returns:
        dict:
          - current_period: str  (最新季)
          - previous_period: str (前一季)
          - manager_name: str
          - delta: pd.DataFrame   (各項 holdings 的變動)
          - n_total: int
        或 {'error': str} 若資料不足
    """
    ensure_sec_identity()
    from edgar import Company

    co = Company(ticker_or_cik)
    filings = co.get_filings(form='13F-HR')
    if len(filings) < 2:
        return {'error': f'需 ≥ 2 份 13F 才能比對，目前只有 {len(filings)} 份'}
    try:
        latest = filings[0].obj()
        prev   = filings[1].obj()
        cmp = latest.compare_holdings(prev)
    except Exception as exc:
        return {'error': f'compare 失敗: {type(exc).__name__}: {exc}'}
    return {
        'current_period':  getattr(cmp, 'current_period', ''),
        'previous_period': getattr(cmp, 'previous_period', ''),
        'manager_name':    getattr(cmp, 'manager_name', co.name),
        'delta':           getattr(cmp, 'data', pd.DataFrame()),
        'n_total':         int(len(getattr(cmp, 'data', []))),
    }


def fetch_13f_history(ticker_or_cik: str, periods: int = 4) -> list[dict]:
    """近 N 季 13F 趨勢（每季一個 snapshot）。"""
    return fetch_13f_holdings(ticker_or_cik, n=periods)


def _format_summary(rec: dict) -> str:
    if 'error' in rec:
        return f"  ERROR: {rec['error']}"
    df = rec.get('holdings')
    lines = [
        f"  date={rec['filing_date']}  period={rec.get('reporting_period','?')}",
        f"  manager={rec['manager_name'][:50]}",
        f"  accession={rec['accession']}",
        f"  n_holdings={rec['n_holdings']}  total_value=${rec['total_value']:,}",
    ]
    if isinstance(df, pd.DataFrame) and not df.empty:
        # Top 5 by Value
        cols = [c for c in ['Issuer', 'Ticker', 'Cusip', 'Value', 'SharesPrnAmount']
                if c in df.columns]
        if 'Value' in df.columns:
            top = df.nlargest(5, 'Value')[cols]
            lines.append('  Top 5 by Value:')
            for _, row in top.iterrows():
                lines.append(
                    f"    {row.get('Issuer','?')[:30]:30s}  "
                    f"{row.get('Ticker','-'):6s}  "
                    f"${int(row.get('Value', 0)):>15,}")
    return '\n'.join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog='data_sources.edgar_13f',
        description='Fetch 13F-HR institutional holdings via edgartools',
    )
    parser.add_argument('ticker', help='Ticker (e.g. BRK-A) or CIK')
    parser.add_argument('--periods', type=int, default=1,
                        help='Fetch N most-recent 13F filings (default 1)')
    parser.add_argument('--compare', action='store_true',
                        help='Show latest vs previous quarter comparison')
    args = parser.parse_args(argv)

    if args.compare:
        print(f'\n=== 13F COMPARE: {args.ticker} ===')
        cmp = fetch_13f_compare(args.ticker)
        if 'error' in cmp:
            print(f'  ERROR: {cmp["error"]}')
            return 1
        print(f'  current={cmp["current_period"]}  '
              f'previous={cmp["previous_period"]}')
        print(f'  manager={cmp["manager_name"][:60]}')
        print(f'  delta rows: {cmp["n_total"]}')
        df = cmp.get('delta')
        if isinstance(df, pd.DataFrame) and not df.empty:
            print(f'  delta columns: {list(df.columns)}')
            print('\n  --- delta (head 15) ---')
            print(df.head(15).to_string(index=False))
        return 0

    print(f'\n=== 13F HOLDINGS: {args.ticker} (last {args.periods}) ===')
    recs = fetch_13f_holdings(args.ticker, n=args.periods)
    if not recs:
        print('  (no 13F filings found)')
        return 1
    for i, rec in enumerate(recs):
        print(f'\n--- filing #{i+1} ---')
        print(_format_summary(rec))
    return 0


if __name__ == '__main__':
    sys.exit(main())
