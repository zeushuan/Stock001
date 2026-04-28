"""Portfolio Simulation：模擬「按推薦清單組合」的實際績效
==============================================================
從 full_market_results.json 讀取每股 TEST 期報酬，模擬：

A. Equal-weight portfolio：選 Top N 檔，等比例分配
B. Tier-based：TOP 200 / OK 675 / NA 153 各別分析
C. Random baseline：隨機抽 N 檔對照（同樣本大小）

輸出指標：
  - mean / median / worst / best return
  - win rate (% 正報酬)
  - return std (波動)
  - Sharpe-like (mean / std)
  - VaR-95% (5% 機率最差個股)
  - Top 10%, Bottom 10%
"""
import sys, json
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
import random


def portfolio_metrics(returns):
    """單一投資組合指標"""
    if not returns: return None
    arr = np.array(returns)
    mean = arr.mean()
    median = np.median(arr)
    std = arr.std()
    return {
        'n': len(arr),
        'mean': mean,
        'median': median,
        'worst': arr.min(),
        'best': arr.max(),
        'win_rate': (arr > 0).mean() * 100,
        'std': std,
        'sharpe_like': mean / std if std > 0 else 0,
        'var95': np.percentile(arr, 5),     # 5% 機率最差
        'top10pct': np.percentile(arr, 90),  # Top 10% 平均之上
        'bot10pct': np.percentile(arr, 10),  # Bottom 10%
    }


def main():
    with open('full_market_results.json', encoding='utf-8') as f:
        data = json.load(f)
    with open('vwap_applicable.json', encoding='utf-8') as f:
        tier_data = json.load(f)

    # 取 TEST 期 VWAPEXEC 報酬
    vwap_test = data.get('B VWAPEXEC|TEST', {})
    base_test = data.get('A baseline|TEST', {})
    pnl = dict(zip(vwap_test['tickers'], vwap_test['pnl_pcts']))
    base_pnl = dict(zip(base_test['tickers'], base_test['pnl_pcts']))

    # 按 tier 分組
    by_tier = {'TOP': [], 'OK': [], 'NA': []}
    for t, info in tier_data.items():
        if t in pnl:
            by_tier[info['tier']].append((t, pnl[t]))

    print(f"全市場樣本：{len(pnl)} 檔（VWAPEXEC TEST 期報酬）\n")
    for tier, lst in by_tier.items():
        print(f"  {tier}: {len(lst)} 檔")

    # ─── A. 全市場 vs 各 tier ─────────────────────────
    print("\n" + "=" * 90)
    print("A. 全市場 vs 各 tier 報酬分布（TEST 期，VWAPEXEC）")
    print("=" * 90)
    print(f"{'組合':<25} {'n':>5} {'均值%':>9} {'中位%':>9} {'最差%':>9} "
          f"{'最佳%':>10} {'勝率%':>7} {'std':>8} {'Sharpe~':>8}")
    print("-" * 90)

    all_returns = list(pnl.values())
    m = portfolio_metrics(all_returns)
    print(f"{'全市場 1028 檔':<25} {m['n']:>5} {m['mean']:>+9.1f} "
          f"{m['median']:>+9.1f} {m['worst']:>+9.1f} {m['best']:>+10.1f} "
          f"{m['win_rate']:>7.1f} {m['std']:>8.1f} {m['sharpe_like']:>8.3f}")

    for tier in ['TOP', 'OK', 'NA']:
        rs = [r for _, r in by_tier[tier]]
        m = portfolio_metrics(rs)
        if m:
            print(f"{f'Tier {tier}':<25} {m['n']:>5} {m['mean']:>+9.1f} "
                  f"{m['median']:>+9.1f} {m['worst']:>+9.1f} {m['best']:>+10.1f} "
                  f"{m['win_rate']:>7.1f} {m['std']:>8.1f} {m['sharpe_like']:>8.3f}")

    # ─── B. Top N（按 VWAPEXEC Δ 從高到低）─────────────
    print("\n" + "=" * 90)
    print("B. Top N 投資組合（按 VWAPEXEC Δ vs baseline 從高到低排序）")
    print("=" * 90)
    print(f"{'組合':<20} {'n':>5} {'均值%':>9} {'最差%':>9} "
          f"{'最佳%':>10} {'勝率%':>7} {'VaR-95':>9} {'Top10%':>9}")
    print("-" * 90)
    # 計算 delta 並排序
    deltas = []
    for t in pnl:
        if t in base_pnl:
            deltas.append((t, pnl[t] - base_pnl[t], pnl[t]))
    deltas.sort(key=lambda x: -x[1])

    for n in [10, 30, 50, 100, 200, 500]:
        top_n = [d[2] for d in deltas[:n]]
        m = portfolio_metrics(top_n)
        if m:
            print(f"{f'Top {n}':<20} {m['n']:>5} {m['mean']:>+9.1f} "
                  f"{m['worst']:>+9.1f} {m['best']:>+10.1f} "
                  f"{m['win_rate']:>7.1f} {m['var95']:>+9.1f} {m['top10pct']:>+9.1f}")

    # ─── C. 隨機 Baseline 對照 ─────────────────────────
    print("\n" + "=" * 90)
    print("C. 隨機抽樣對照（相同 n，從全市場隨機抽 1000 次）")
    print("=" * 90)
    random.seed(42)
    print(f"{'組合':<20} {'n':>5} {'均值平均%':>11} {'中位數%':>10} {'95%區間':>20}")
    print("-" * 90)
    universe_returns = list(pnl.values())
    for n in [10, 30, 50, 100, 200]:
        if n > len(universe_returns): continue
        means = []
        for _ in range(1000):
            sample = random.sample(universe_returns, n)
            means.append(np.mean(sample))
        means = np.array(means)
        ci_low, ci_hi = np.percentile(means, 2.5), np.percentile(means, 97.5)
        print(f"{f'隨機 {n}':<20} {n:>5} {means.mean():>+11.1f} "
              f"{np.median(means):>+10.1f}  [{ci_low:>+8.1f}, {ci_hi:>+8.1f}]")

    # ─── D. Top N vs 隨機 Baseline 比較（核心判斷）────
    print("\n" + "=" * 90)
    print("D. Top N 是否顯著優於隨機？（核心驗證）")
    print("=" * 90)
    print(f"{'組合':<15} {'Top N 均值':>11} {'隨機均值':>11} {'差距':>9} "
          f"{'隨機 95% 上限':>14} {'是否顯著':>10}")
    print("-" * 90)
    random.seed(42)
    for n in [10, 30, 50, 100, 200]:
        top_n = [d[2] for d in deltas[:n]]
        if len(top_n) < n: continue
        top_mean = np.mean(top_n)
        rand_means = []
        for _ in range(1000):
            sample = random.sample(universe_returns, n)
            rand_means.append(np.mean(sample))
        rand_means = np.array(rand_means)
        rand_mean = rand_means.mean()
        ci_hi = np.percentile(rand_means, 97.5)
        sig = '⭐ 顯著' if top_mean > ci_hi else '無'
        print(f"{f'Top {n}':<15} {top_mean:>+11.1f} {rand_mean:>+11.1f} "
              f"{top_mean-rand_mean:>+9.1f} {ci_hi:>+14.1f} {sig:>10}")

    # ─── E. 同等資金分配的累積報酬 ─────────────────────
    print("\n" + "=" * 90)
    print("E. 等比例資金分配 — 假設 100 萬投資 N 檔等權各 1/N")
    print("=" * 90)
    INITIAL = 1_000_000
    print(f"{'組合':<20} {'初始':>10} {'結束':>12} {'總報酬':>10} {'年化':>8}")
    print("-" * 70)
    # TEST 期約 22 個月
    months = 22
    years = months / 12
    for label, picks in [
        ('全市場 1028 檔', list(pnl.values())),
        ('Tier TOP 200', [r for _, r in by_tier['TOP']]),
        ('Top 50', [d[2] for d in deltas[:50]]),
        ('Top 100', [d[2] for d in deltas[:100]]),
    ]:
        if not picks: continue
        avg_ret = np.mean(picks) / 100
        end_val = INITIAL * (1 + avg_ret)
        ann = ((end_val/INITIAL)**(1/years) - 1) * 100 if years > 0 else 0
        print(f"{label:<20} {INITIAL:>10,} {end_val:>12,.0f} "
              f"{avg_ret*100:>+9.1f}% {ann:>+7.1f}%")


if __name__ == '__main__':
    main()
