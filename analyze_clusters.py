"""個股 Cluster 分群（行為相近的群組）
=========================================
目的：找出「跨產業同向群組」(behavioral clusters)
方法：用每股 6 年日報酬向量做相關性矩陣 → K-means 分群

期望發現：
  - AI 概念群（半導體 + 設備 + 散熱 + 雲端）
  - 內需穩定群（電信 + 食品 + 醫療）
  - 景氣循環群（鋼鐵 + 塑膠 + 航運）
  - 高 β 投機群（飆股題材）
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
    if len(df) < 280: return None
    return df['Close'].pct_change()


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = [t for t in universe if t in vwap_set]

    # 取流動性 top 200（避免雜訊）
    print(f"📊 計算 ADV 篩選 top 200...")
    advs = []
    for t in universe:
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
            advs.append((t, adv))
        except: pass
    advs.sort(key=lambda x: -x[1])
    top_universe = [t for t, _ in advs[:200]]
    print(f"  Top 200 流動股確定\n")

    # 取得每股報酬序列
    print("📊 載入報酬序列...")
    rets = {}
    for t in top_universe:
        r = load_returns(t)
        if r is not None and len(r) > 1000:
            rets[t] = r
    print(f"  {len(rets)} 檔有完整資料\n")

    # 建 DataFrame（對齊日期）
    df_all = pd.DataFrame(rets).dropna(how='all')
    df_all = df_all.dropna(axis=1, thresh=int(len(df_all) * 0.7))  # 至少 70% 有資料
    df_all = df_all.fillna(0)
    print(f"  對齊後: {df_all.shape[0]} 日 × {df_all.shape[1]} 檔\n")

    # K-means 用 sklearn（如果沒有就用簡化版）
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler
        # Standardize (transpose: rows = stocks)
        X = df_all.T.values  # shape: n_stocks × n_days
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        # K=6
        K = 6
        km = KMeans(n_clusters=K, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        tickers = list(df_all.columns)
    except ImportError:
        print("  sklearn 未安裝，改用相關性 hierarchical")
        # Fallback：用相關性矩陣 + 簡單分群
        corr = df_all.corr()
        from scipy.cluster.hierarchy import linkage, fcluster
        Z = linkage(corr.values, method='ward')
        labels = fcluster(Z, t=6, criterion='maxclust') - 1
        tickers = list(corr.columns)
        K = 6

    # 行業
    industry_map = {}
    p = Path('tw_universe.txt')
    if p.exists():
        for line in p.read_text(encoding='utf-8').splitlines():
            if not line or line.startswith('#'): continue
            parts = line.split('|')
            if len(parts) >= 5 and parts[4]:
                industry_map[parts[0].strip()] = parts[4].strip()
    name_map = {}
    sj = Path('tw_stock_list.json')
    if sj.exists():
        data = json.loads(sj.read_text(encoding='utf-8'))
        if isinstance(data, dict):
            if 'tickers' in data: data = data['tickers']
            for k, v in data.items():
                if isinstance(v, dict): name_map[k] = v.get('name', '')

    # 列出各 cluster 成員
    clusters = {i: [] for i in range(K)}
    for t, lbl in zip(tickers, labels):
        clusters[int(lbl)].append(t)

    print("=" * 100)
    print(f"📊 K-means {K} 群結果（top 200 流動股）")
    print("=" * 100)
    for cid in range(K):
        members = clusters[cid]
        # 行業分布
        ind_count = {}
        for t in members:
            ind = industry_map.get(t, '其他')
            ind_count[ind] = ind_count.get(ind, 0) + 1
        # 主要行業
        sorted_inds = sorted(ind_count.items(), key=lambda x: -x[1])
        top_inds = sorted_inds[:3]

        # 計算群內平均報酬與 σ
        if len(members) > 0:
            avg_ret = df_all[members].mean(axis=1).mean() * 252 * 100
            avg_sigma = df_all[members].mean(axis=1).std() * np.sqrt(252) * 100
        else:
            avg_ret, avg_sigma = 0, 0

        print(f"\n🎯 Cluster {cid}: {len(members)} 檔")
        print(f"  主要行業: {', '.join(f'{i}({n})' for i, n in top_inds)}")
        print(f"  6 年化報酬: {avg_ret:+.1f}% / σ {avg_sigma:.1f}%")
        print(f"  代表個股 (前 15):")
        # 顯示 top 15
        for t in members[:15]:
            ind = industry_map.get(t, '—')[:8]
            nm = name_map.get(t, '')[:10]
            print(f"    {t} {nm:<10} {ind}")

    # 寫入
    out = {
        'k': K,
        'clusters': {
            cid: {
                'members': clusters[cid],
                'top_industries': dict(sorted({
                    industry_map.get(t, '其他'): 1 for t in clusters[cid]
                }.items())),
            } for cid in range(K)
        },
    }
    with open('clusters.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n💾 寫入 clusters.json")


if __name__ == '__main__':
    main()
