"""
行業內輪動（Top-3 sectors）

每月排序 21 個 TWSE 產業強度（用各產業內個股 60 日均值平均報酬代表），
只允許在前 3 強產業內進場。

設計：
  1. 從 tw_universe.txt 讀產業分類
  2. 按月計算每產業 60 日報酬（平均所有該產業個股）
  3. 排名取 top 3
  4. 進場時檢查該股產業是否在當月 top 3
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor


def load_industry_map():
    """從 tw_universe.txt 取每股產業分類"""
    out = {}
    fp = Path('tw_universe.txt')
    if not fp.exists(): return out
    for line in fp.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 5:
            ticker, _name, _typ, _mkt, industry = parts[:5]
            if industry and _typ == '股票':
                out[ticker] = industry
    return out


def compute_monthly_industry_strength():
    """每月計算每產業強度（產業內個股 60 日平均報酬）。
    回傳 {date: {industry: rank}}（排名 1=最強）"""
    import data_loader as dl
    industry_map = load_industry_map()
    files = sorted(Path('data_cache').glob('*.parquet'))

    # 收集每股每日 60 日 ROC
    all_roc = {}
    for fp in files:
        df = dl.load_from_cache(fp.stem)
        if df is None or len(df) < 65: continue
        ticker = fp.stem
        if ticker not in industry_map: continue
        c = df['Close'].values
        roc60 = np.full(len(c), np.nan)
        for i in range(60, len(c)):
            roc60[i] = (c[i] / c[i-60] - 1) * 100
        all_roc[ticker] = pd.Series(roc60, index=df.index)

    if not all_roc: return {}, industry_map

    # 共同月份
    sample_idx = next(iter(all_roc.values())).index
    monthly_dates = sample_idx.to_period('M').drop_duplicates()

    # 每月排名
    monthly_rank = {}
    for ym in monthly_dates:
        # 抓該月最後一天的 ROC
        month_end = sample_idx[sample_idx.to_period('M') == ym]
        if len(month_end) == 0: continue
        d = month_end[-1]
        # 收集每產業所有個股 ROC
        industry_rocs = defaultdict(list)
        for ticker, roc_series in all_roc.items():
            ind = industry_map.get(ticker)
            if not ind: continue
            try:
                v = roc_series.loc[d]
                if not np.isnan(v): industry_rocs[ind].append(v)
            except KeyError: continue
        # 平均
        ind_strength = {ind: np.mean(rocs) for ind, rocs in industry_rocs.items() if len(rocs) >= 3}
        # 排名
        ranked = sorted(ind_strength.items(), key=lambda x: -x[1])
        rank_map = {ind: r+1 for r, (ind, _) in enumerate(ranked)}
        monthly_rank[ym] = rank_map

    return monthly_rank, industry_map


def main():
    print("[1/2] 計算月度產業強度...")
    monthly_rank, industry_map = compute_monthly_industry_strength()
    print(f"  共 {len(monthly_rank)} 個月、{len(industry_map)} 檔股票")

    # 匯出 top 3 industries 各月
    print("\n[2/2] 各月 top 3 強勢產業：")
    for ym in sorted(monthly_rank.keys())[-12:]:  # 最近 12 個月
        ranks = monthly_rank[ym]
        top3 = sorted(ranks.items(), key=lambda x: x[1])[:3]
        print(f"  {ym}: {' / '.join(ind for ind, _ in top3)}")

    # 寫成 JSON 給 variant_strategy.py 用
    import json
    out_path = Path('sector_rank.json')
    serializable = {str(ym): {ind: rank for ind, rank in ranks.items()}
                    for ym, ranks in monthly_rank.items()}
    out_path.write_text(json.dumps(serializable, ensure_ascii=False, indent=1),
                        encoding='utf-8')
    print(f"\n已輸出 {out_path}（{out_path.stat().st_size/1024:.0f} KB）")


if __name__ == '__main__':
    main()
