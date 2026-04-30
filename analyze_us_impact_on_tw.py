"""美股對台股個股影響深度分析（個股級 Lag-1）
==================================================
延伸 analyze_tw_us_linkage.py（指數級）→ 個股級

對 1058 檔台股計算：
  - Lag-1 相關係數 vs 5 個美股指數（^GSPC/^IXIC/^SOX/^DXY/^VIX）
  - 回歸 β（彈性）
  - 行業分組

輸出：
  - Top 20 最受美股影響的台股
  - Top 20 最獨立（最少受美股影響）
  - 行業級平均相關
  - 哪個美股指數最適合預測哪類台股
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16

# 美股指數
US_INDICES = ['^GSPC', '^IXIC', '^SOX', '^DXY', '^VIX']
US_LABEL = {
    '^GSPC':  'SPX',
    '^IXIC':  'NASDAQ',
    '^SOX':   'SOX',
    '^DXY':   'DXY',
    '^VIX':   'VIX',
}


def load_idx_returns(ticker):
    """載入指數並計算日報酬"""
    df = dl.load_from_cache(ticker)
    if df is None: return None
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)
    df = df[df.index >= pd.Timestamp('2020-01-01')]
    if len(df) < 100: return None
    return df['Close'].pct_change() * 100


# 全域美股 returns（worker 共享）
_US_RETURNS = None


def init_worker():
    global _US_RETURNS
    _US_RETURNS = {idx: load_idx_returns(idx) for idx in US_INDICES}


def analyze_one(ticker):
    """對單檔 TW，計算與各美股指數 Lag-1 相關係數 + β"""
    global _US_RETURNS
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp('2020-01-01')]
        if len(df) < 280: return (ticker, None)

        tw_ret = df['Close'].pct_change() * 100

        result = {}
        for idx_name in US_INDICES:
            us_ret = _US_RETURNS.get(idx_name)
            if us_ret is None: continue
            # Lag-1: us[t-1] vs tw[t]
            us_lagged = us_ret.shift(1)
            joined = pd.concat([us_lagged, tw_ret], axis=1, join='inner').dropna()
            joined.columns = ['us', 'tw']
            if len(joined) < 100: continue
            corr = joined['us'].corr(joined['tw'])
            x = joined['us'].values
            y = joined['tw'].values
            beta = np.cov(x, y)[0, 1] / np.var(x) if np.var(x) > 0 else 0
            r2 = corr ** 2
            result[US_LABEL[idx_name]] = {
                'corr': float(corr), 'beta': float(beta),
                'r2': float(r2), 'n': int(len(joined)),
            }
        return (ticker, result)
    except Exception:
        return (ticker, None)


def load_industry_map():
    """從 tw_universe.txt 讀取行業對應"""
    p = Path('tw_universe.txt')
    out = {}
    if not p.exists(): return out
    for line in p.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 5 and parts[4]:
            out[parts[0].strip()] = parts[4].strip()
        elif len(parts) >= 3:
            out[parts[0].strip()] = parts[2].strip() if parts[2] else '其他'
    return out


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = [t for t in universe if t in vwap_set]
    print(f"🇹🇼 TW universe: {len(universe)} 檔")

    industry_map = load_industry_map()
    print(f"  行業對應: {len(industry_map)} 檔有資料\n")

    # 跑分析
    print("📊 計算每檔 vs 5 個美股指數 Lag-1 相關...")
    t0 = time.time()
    all_results = {}
    with ProcessPoolExecutor(max_workers=WORKERS,
                              initializer=init_worker) as ex:
        for ticker, r in ex.map(analyze_one, universe, chunksize=50):
            if r is not None:
                all_results[ticker] = r
    print(f"  完成 {time.time()-t0:.1f}s（成功 {len(all_results)}/{len(universe)}）\n")

    # ── 1. Top 20 最受美股影響（用 SOX 相關度排序，因為對台股科技權重最敏感）──
    print("=" * 100)
    print("🏆 Top 20 最受美股 SOX 影響的台股（Lag-1 相關係數）")
    print("=" * 100)
    by_sox = sorted(
        [(t, r['SOX']['corr'], r['SOX']['beta'], r['SOX']['r2'])
         for t, r in all_results.items() if 'SOX' in r],
        key=lambda x: -x[1]
    )[:20]
    print(f"  {'#':<3} {'Ticker':<8} {'相關':>8} {'β':>8} {'R²':>8}  行業")
    print("-" * 100)
    for i, (t, c, b, r2) in enumerate(by_sox, 1):
        ind = industry_map.get(t, '—')
        print(f"  {i:<3} {t:<8} {c:>+8.3f} {b:>+8.3f} {r2:>8.3f}  {ind}")

    # ── 2. Top 20 最獨立（最不受美股影響）──
    print(f"\n🏆 Top 20 最獨立（SOX 相關 |corr| 最低）")
    print("=" * 100)
    by_indep = sorted(
        [(t, r['SOX']['corr'])
         for t, r in all_results.items() if 'SOX' in r],
        key=lambda x: abs(x[1])
    )[:20]
    print(f"  {'#':<3} {'Ticker':<8} {'相關':>8}  行業")
    print("-" * 100)
    for i, (t, c) in enumerate(by_indep, 1):
        ind = industry_map.get(t, '—')
        print(f"  {i:<3} {t:<8} {c:>+8.3f}  {ind}")

    # ── 3. 行業級平均 ──
    print(f"\n📊 行業級平均 Lag-1 相關（vs 各美股指數）")
    print("=" * 100)
    industry_data = {}  # {industry: {idx: [corrs]}}
    for t, r in all_results.items():
        ind = industry_map.get(t, '其他')
        for idx_name, d in r.items():
            industry_data.setdefault(ind, {}).setdefault(idx_name, []).append(d['corr'])

    print(f"  {'行業':<20} {'n':>4} "
          f"{'SPX':>8} {'NASDAQ':>8} {'SOX':>8} {'DXY':>8} {'VIX':>8}  最強連動")
    print("-" * 100)
    industry_summary = {}
    industries = sorted(industry_data.keys(),
                         key=lambda k: -np.mean(industry_data[k].get('SOX', [0])))
    for ind in industries:
        idxs = industry_data[ind]
        n = len(next(iter(idxs.values())))
        if n < 5: continue
        avg = {idx_name: np.mean(corrs) if corrs else 0
               for idx_name, corrs in idxs.items()}
        # 找絕對值最大（最強連動）
        strongest = max([(k, v) for k, v in avg.items() if k != 'VIX'],
                         key=lambda x: abs(x[1])) if avg else ('—', 0)
        print(f"  {ind:<20} {n:>4} "
              f"{avg.get('SPX', 0):>+8.3f} {avg.get('NASDAQ', 0):>+8.3f} "
              f"{avg.get('SOX', 0):>+8.3f} {avg.get('DXY', 0):>+8.3f} "
              f"{avg.get('VIX', 0):>+8.3f}  {strongest[0]} {strongest[1]:+.3f}")
        industry_summary[ind] = {**avg, 'n': int(n)}

    # ── 4. 跨指數對比（哪個美股指數最強）──
    print(f"\n📊 各美股指數對台股整體影響")
    print("=" * 100)
    overall = {idx: [] for idx in [US_LABEL[i] for i in US_INDICES]}
    for t, r in all_results.items():
        for idx_name, d in r.items():
            overall[idx_name].append(d['corr'])

    print(f"  {'美股指數':<10} {'樣本':>5} {'平均相關':>10} {'中位':>10} "
          f"{'95% 分位':>10} {'5% 分位':>10}")
    print("-" * 100)
    for idx_name, corrs in overall.items():
        if not corrs: continue
        arr = np.array(corrs)
        print(f"  {idx_name:<10} {len(arr):>5} {arr.mean():>+10.3f} "
              f"{np.median(arr):>+10.3f} {np.quantile(arr, 0.95):>+10.3f} "
              f"{np.quantile(arr, 0.05):>+10.3f}")

    # ── 5. 重要個股 ──
    print(f"\n📊 重要個股 vs 各美股指數")
    print("=" * 100)
    key_tickers = [
        ('2330', '台積電'), ('2454', '聯發科'), ('2317', '鴻海'),
        ('2412', '中華電'), ('2882', '國泰金'), ('1301', '台塑'),
        ('2308', '台達電'), ('1216', '統一'), ('2207', '和泰車'),
        ('2603', '長榮'), ('2891', '中信金'), ('1101', '台泥'),
    ]
    print(f"  {'Ticker':<8} {'名稱':<10} "
          f"{'SPX':>8} {'NASDAQ':>8} {'SOX':>8} {'DXY':>8} {'VIX':>8}")
    print("-" * 100)
    for tk, name in key_tickers:
        r = all_results.get(tk)
        if r is None:
            print(f"  {tk:<8} {name:<10}  (無資料)")
            continue
        line = f"  {tk:<8} {name:<10}"
        for idx_name in ['SPX', 'NASDAQ', 'SOX', 'DXY', 'VIX']:
            d = r.get(idx_name, {})
            corr = d.get('corr', 0)
            line += f" {corr:>+8.3f}"
        print(line)

    # 寫入
    out = {
        'per_ticker': {t: r for t, r in all_results.items()},
        'industry_summary': industry_summary,
        'overall_stats': {
            idx: {
                'mean': float(np.mean(corrs)),
                'median': float(np.median(corrs)),
                'q05': float(np.quantile(corrs, 0.05)),
                'q95': float(np.quantile(corrs, 0.95)),
                'n': int(len(corrs)),
            } for idx, corrs in overall.items() if corrs
        },
        'top_us_sensitive': [(t, c, b, r2) for t, c, b, r2 in by_sox[:30]],
        'top_independent':  [(t, c) for t, c in by_indep[:30]],
    }
    with open('us_impact_on_tw.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n💾 寫入 us_impact_on_tw.json")


if __name__ == '__main__':
    main()
