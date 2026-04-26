"""
產業別 POS 效果深度分析

問題：POS 在不同產業是否效果差異大？某些產業是否更適合 P0/CB30/base？

方法：
  對每個產業，計算該產業內：
    - P0 平均
    - POS 平均
    - CB30 平均
    - base 平均
    - 推薦哪個策略（取最佳 4 者）
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import json
import numpy as np


def load_results(path):
    d = {}
    for r in csv.DictReader(open(path, encoding='utf-8-sig')):
        try: d[r['ticker']] = float(r['pnl_pct'])
        except: pass
    return d


def main():
    base = load_results('results_base.csv')
    p0   = load_results('results_P0_T1T3.csv')
    cb30 = load_results('results_P0_T1T3+CB30.csv')
    pos  = load_results('results_P0_T1T3+POS.csv')

    with open('tw_stock_list.json', encoding='utf-8') as f:
        meta = json.load(f)

    # 按產業分組
    by_industry = {}
    for tk in base.keys() & p0.keys() & cb30.keys() & pos.keys():
        ind = meta.get(tk, {}).get('industry', '其他')
        by_industry.setdefault(ind, []).append(tk)

    print("━━━━━━ 產業別四模式表現對比 ━━━━━━\n")
    print(f"{'產業':<14} {'樣本':>5}  {'base':>9} {'P0':>9} {'CB30':>9} {'POS':>9}  {'最佳':<8}  {'POS vs CB30':>12}")
    print('-' * 95)

    rows = []
    for ind, tickers in by_industry.items():
        if len(tickers) < 10: continue
        b_avg = np.mean([base[t]  for t in tickers])
        p_avg = np.mean([p0[t]    for t in tickers])
        c_avg = np.mean([cb30[t]  for t in tickers])
        s_avg = np.mean([pos[t]   for t in tickers])
        best  = max([('base', b_avg), ('P0', p_avg), ('CB30', c_avg), ('POS', s_avg)],
                    key=lambda x: x[1])
        rows.append({
            'ind': ind, 'n': len(tickers),
            'base': b_avg, 'p0': p_avg, 'cb30': c_avg, 'pos': s_avg,
            'best': best[0], 'best_val': best[1],
            'pos_vs_cb30': s_avg - c_avg,
        })

    rows.sort(key=lambda r: -r['pos'])

    for r in rows:
        marker = ' ⭐' if r['best'] == 'POS' else ''
        print(f"  {r['ind']:<14} {r['n']:>4}  {r['base']:>+8.0f}% {r['p0']:>+8.0f}% "
              f"{r['cb30']:>+8.0f}% {r['pos']:>+8.0f}%  {r['best']:<7}{marker}  "
              f"{r['pos_vs_cb30']:>+11.1f}")

    print()
    print("━━━━━━ 各策略勝出產業數 ━━━━━━")
    counts = {}
    for r in rows:
        counts[r['best']] = counts.get(r['best'], 0) + 1
    for s in ['base', 'P0', 'CB30', 'POS']:
        n = counts.get(s, 0)
        print(f"  {s:<6}: {n} 個產業最佳")

    # 計算 POS 在哪些產業最強
    print("\n━━━ POS 大幅領先 CB30 的產業 (POS - CB30 > +50%) ━━━")
    pos_strong = [r for r in rows if r['pos_vs_cb30'] > 50]
    pos_strong.sort(key=lambda r: -r['pos_vs_cb30'])
    for r in pos_strong:
        print(f"  {r['ind']:<14} POS {r['pos']:>+7.0f}% vs CB30 {r['cb30']:>+7.0f}%"
              f"  領先 {r['pos_vs_cb30']:>+6.0f}%")

    print("\n━━━ CB30 大幅領先 POS 的產業 (CB30 - POS > +50%) ━━━")
    cb_strong = [r for r in rows if -r['pos_vs_cb30'] > 50]
    cb_strong.sort(key=lambda r: r['pos_vs_cb30'])
    for r in cb_strong:
        print(f"  {r['ind']:<14} CB30 {r['cb30']:>+7.0f}% vs POS {r['pos']:>+7.0f}%"
              f"  領先 {-r['pos_vs_cb30']:>+6.0f}%")

    # 估算「產業最佳路由」總均值
    print("\n━━━ 產業最佳路由均值（理論最佳）━━━")
    total = 0; total_n = 0
    for r in rows:
        total += r['best_val'] * r['n']
        total_n += r['n']
    avg_routed = total / total_n if total_n else 0
    print(f"  各產業選最佳策略後加權均值：{avg_routed:+.2f}%")
    print(f"  vs 全部用 POS：           {np.mean([pos[t] for t in pos.keys() & meta.keys()]):+.2f}%")
    print(f"  vs 全部用 CB30：          {np.mean([cb30[t] for t in cb30.keys() & meta.keys()]):+.2f}%")
    print(f"  vs 全部用 P0：            {np.mean([p0[t] for t in p0.keys() & meta.keys()]):+.2f}%")


if __name__ == '__main__':
    main()
