"""
全市場每支股票的獲利/虧損模式深度分析

不修正、不優化，純粹探索：
  1. v7 base / v8 P0_T1T3 / v8 P0_T1T3+CB30 三種模式的全市場分布
  2. 哪些股票在不同模式下表現極端？
  3. 行為模式分類：飆股、震盪股、持續下跌、橫盤、反向 ETF
  4. v7→v8 改善 / 退步的原因推論
  5. 產業別表現分布
  6. 與 BH 的相關性
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import json
from pathlib import Path

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────
def load(path: str) -> dict:
    """讀 v8_runner CSV → {ticker: {bh, pnl, trades, wr, n_t4}}"""
    d = {}
    if not Path(path).exists():
        return d
    for r in csv.DictReader(open(path, encoding='utf-8-sig')):
        try:
            d[r['ticker']] = dict(
                bh    = float(r['bh_pct'] or 0),
                pnl   = float(r['pnl_pct']),
                pnl_net = float(r.get('pnl_pct_net') or r['pnl_pct']),
                trades= int(r['n_trades']),
                t4    = int(r.get('n_t4', 0)),
                wr    = float(r['win_rate'] or 0),
            )
        except: pass
    return d


def load_meta() -> dict:
    """股票名稱與產業"""
    p = Path('tw_stock_list.json')
    if not p.exists(): return {}
    with open(p, encoding='utf-8') as f:
        return json.load(f)


def fmt(x, d=1, sign=True):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return '   N/A'
    fs = f'{{:>+{d+5}.{d}f}}%' if sign else f'{{:>{d+5}.{d}f}}%'
    return fs.format(x)


# ─────────────────────────────────────────────────────────────────
def main():
    print("━━━━━━━━━━ 全市場每支股票績效深度分析 ━━━━━━━━━━\n")

    base = load('results_base.csv')
    p0   = load('results_P0_T1T3.csv')
    cb30 = load('results_P0_T1T3+CB30.csv')
    meta = load_meta()

    if not (base and p0 and cb30):
        print("缺少必要 CSV 檔案，請先跑：")
        print("  python v8_runner.py --mode base")
        print("  python v8_runner.py --mode P0_T1T3")
        print("  python v8_runner.py --mode P0_T1T3+CB30")
        return

    # 取共同集
    common = sorted(set(base.keys()) & set(p0.keys()) & set(cb30.keys()))
    print(f"分析共同樣本：{len(common)} 檔\n")

    # 建表
    table = []
    for tk in common:
        b = base[tk]; p = p0[tk]; c = cb30[tk]
        m = meta.get(tk, {})
        table.append({
            'ticker': tk,
            'name': m.get('name', '')[:8],
            'industry': m.get('industry', '')[:8],
            'bh': b['bh'],
            'v7': b['pnl'],
            'p0': p['pnl'],
            'cb30': c['pnl'],
            'd_v7_p0': p['pnl'] - b['pnl'],
            'd_p0_cb30': c['pnl'] - p['pnl'],
            'trades_v7': b['trades'],
            'trades_p0': p['trades'],
        })

    # ─── 1. 三模式全市場統計 ─────────────────────────────────
    print("━━━ 1. 三模式統計總覽 ━━━")
    for col, label in [('bh', 'BH 持有'), ('v7', 'v7 base'),
                        ('p0', 'v8 P0_T1T3'), ('cb30', 'v8 P0+CB30')]:
        vals = [r[col] for r in table]
        a = np.array(vals)
        print(f"  {label:<14} 均值 {fmt(np.mean(a))} ｜ 中位 {fmt(np.median(a))}"
              f" ｜ 標差 {np.std(a):.0f}"
              f" ｜ 最高 {fmt(np.max(a))} ｜ 最低 {fmt(np.min(a))}")

    # ─── 2. 分位數分布 ────────────────────────────────────────
    print("\n━━━ 2. 分位數分布（看尾部） ━━━")
    for col, label in [('v7', 'v7 base'), ('p0', 'P0_T1T3'), ('cb30', 'P0+CB30')]:
        a = np.array([r[col] for r in table])
        print(f"  {label:<10} P5={fmt(np.percentile(a, 5))} ｜ P25={fmt(np.percentile(a, 25))}"
              f" ｜ P50={fmt(np.percentile(a, 50))} ｜ P75={fmt(np.percentile(a, 75))}"
              f" ｜ P95={fmt(np.percentile(a, 95))}")

    # ─── 3. 按 BH 帶分類後的策略表現 ───────────────────────
    print("\n━━━ 3. 按 BH 表現分組（看不同型態的股票如何被策略捕捉） ━━━")
    bands = [
        ('Mega 飆股 (BH > 1000%)', lambda r: r['bh'] > 1000),
        ('強勢股 (BH 500~1000%)',   lambda r: 500 < r['bh'] <= 1000),
        ('正常股 (BH 100~500%)',    lambda r: 100 < r['bh'] <= 500),
        ('微正報酬 (BH 0~100%)',    lambda r: 0 <= r['bh'] <= 100),
        ('輕跌股 (BH -50~0%)',      lambda r: -50 < r['bh'] < 0),
        ('重跌股 (BH < -50%)',      lambda r: r['bh'] <= -50),
    ]
    print(f"  {'群組':<22} {'樣本':>5} {'BH均值':>9} {'v7均值':>9} {'P0均值':>9}"
          f" {'CB30均值':>9} {'P0vsBH':>9}")
    print('  ' + '-' * 80)
    for name, cond in bands:
        sub = [r for r in table if cond(r)]
        if not sub: continue
        bh_m  = np.mean([r['bh'] for r in sub])
        v7_m  = np.mean([r['v7'] for r in sub])
        p0_m  = np.mean([r['p0'] for r in sub])
        cb_m  = np.mean([r['cb30'] for r in sub])
        ratio = (p0_m / bh_m * 100) if bh_m > 0 else float('nan')
        print(f"  {name:<22} {len(sub):>5} {fmt(bh_m, 0)} {fmt(v7_m, 0)}"
              f" {fmt(p0_m, 0)} {fmt(cb_m, 0)} {ratio:>7.1f}%")

    # ─── 4. v8 改善最大 TOP 30 ──────────────────────────────
    print("\n━━━ 4. v7 → v8 P0_T1T3 改善最大 TOP 30 ━━━")
    table.sort(key=lambda r: -r['d_v7_p0'])
    print(f"  {'代號':<7} {'名稱':<10} {'產業':<10} {'BH%':>8} {'v7%':>8}"
          f" {'P0%':>8} {'改善':>8} {'CB30%':>8} {'v7→CB30':>9}")
    print('  ' + '-' * 90)
    for r in table[:30]:
        d_cb = r['cb30'] - r['v7']
        print(f"  {r['ticker']:<7} {r['name']:<10} {r['industry']:<10}"
              f" {r['bh']:>+7.0f}% {r['v7']:>+7.0f}% {r['p0']:>+7.0f}%"
              f" {r['d_v7_p0']:>+7.0f}% {r['cb30']:>+7.0f}% {d_cb:>+8.0f}%")

    # ─── 5. v8 退步最大 BOTTOM 30 ──────────────────────────
    print("\n━━━ 5. v7 → v8 P0_T1T3 退步最大（金字塔加碼變死亡迴圈）━━━")
    table.sort(key=lambda r: r['d_v7_p0'])
    print(f"  {'代號':<7} {'名稱':<10} {'產業':<10} {'BH%':>8} {'v7%':>8}"
          f" {'P0%':>8} {'退步':>8} {'CB30 救回':>10}")
    print('  ' + '-' * 90)
    for r in table[:30]:
        cb30_save = r['cb30'] - r['p0']    # CB30 vs P0 的救回幅度
        print(f"  {r['ticker']:<7} {r['name']:<10} {r['industry']:<10}"
              f" {r['bh']:>+7.0f}% {r['v7']:>+7.0f}% {r['p0']:>+7.0f}%"
              f" {r['d_v7_p0']:>+7.0f}% {cb30_save:>+9.0f}%")

    # ─── 6. CB30 救回最大（risk control 效果驗證）────────
    print("\n━━━ 6. CB30 風險控制救回最大 TOP 20 ━━━")
    table.sort(key=lambda r: r['d_p0_cb30'])
    print(f"  {'代號':<7} {'名稱':<10} {'BH%':>8} {'v7%':>8} {'P0%':>8}"
          f" {'CB30%':>8} {'救回':>8}")
    print('  ' + '-' * 70)
    for r in table[:20]:
        print(f"  {r['ticker']:<7} {r['name']:<10} {r['bh']:>+7.0f}%"
              f" {r['v7']:>+7.0f}% {r['p0']:>+7.0f}% {r['cb30']:>+7.0f}%"
              f" {r['d_p0_cb30']:>+7.0f}%")

    # ─── 7. 三模式都賠錢的「死亡股」TOP 30 ──────────────
    print("\n━━━ 7. v7/P0/CB30 三模式都嚴重賠錢的股票 TOP 30 ━━━")
    losers = [r for r in table if r['v7'] < -50 and r['p0'] < -50 and r['cb30'] < -50]
    losers.sort(key=lambda r: r['cb30'])
    print(f"  共 {len(losers)} 檔三模式都賠 -50%+")
    print(f"  {'代號':<7} {'名稱':<10} {'BH%':>8} {'v7%':>8} {'P0%':>8} {'CB30%':>8} 進場數")
    for r in losers[:30]:
        print(f"  {r['ticker']:<7} {r['name']:<10} {r['bh']:>+7.0f}%"
              f" {r['v7']:>+7.0f}% {r['p0']:>+7.0f}% {r['cb30']:>+7.0f}% {r['trades_p0']:>5}")

    # ─── 8. 跑贏 BH 的股票（策略表現超越持有） ──────────
    print("\n━━━ 8. v8 P0_T1T3 表現超越 BH 的股票 ━━━")
    beat_bh = [r for r in table if r['p0'] > r['bh'] and r['p0'] > 0]
    beat_bh.sort(key=lambda r: -(r['p0'] - r['bh']))
    print(f"  共 {len(beat_bh)} 檔 v8 P0 超越 BH（{len(beat_bh)/len(table)*100:.1f}%）")
    print(f"  {'代號':<7} {'名稱':<10} {'BH%':>8} {'P0%':>9} {'超越':>9}")
    for r in beat_bh[:20]:
        diff = r['p0'] - r['bh']
        print(f"  {r['ticker']:<7} {r['name']:<10} {r['bh']:>+7.0f}%"
              f" {r['p0']:>+8.0f}% {diff:>+8.0f}%")

    # ─── 9. 產業別績效（前 15 大）──────────────────────────
    print("\n━━━ 9. 產業別三模式平均表現（≥10 檔）━━━")
    by_industry = {}
    for r in table:
        ind = r['industry']
        by_industry.setdefault(ind, []).append(r)
    rows = []
    for ind, items in by_industry.items():
        if len(items) < 10: continue
        rows.append({
            'ind': ind,
            'n': len(items),
            'bh_avg': np.mean([r['bh'] for r in items]),
            'v7_avg': np.mean([r['v7'] for r in items]),
            'p0_avg': np.mean([r['p0'] for r in items]),
            'cb_avg': np.mean([r['cb30'] for r in items]),
        })
    rows.sort(key=lambda r: -r['p0_avg'])
    print(f"  {'產業':<14} {'檔數':>4} {'BH%':>8} {'v7%':>8} {'P0%':>8}"
          f" {'CB30%':>8} {'P0改善%':>9}")
    for r in rows[:20]:
        improve = r['p0_avg'] - r['v7_avg']
        print(f"  {r['ind']:<14} {r['n']:>4} {fmt(r['bh_avg'], 0)}"
              f" {fmt(r['v7_avg'], 0)} {fmt(r['p0_avg'], 0)}"
              f" {fmt(r['cb_avg'], 0)} {improve:>+8.0f}%")

    # ─── 10. 加碼倉位數分布（v7 → P0 增加多少筆）────────
    print("\n━━━ 10. P0 加碼次數分布（vs v7 增加的交易數）━━━")
    diffs = [(r['trades_p0'] - r['trades_v7']) for r in table]
    arr = np.array(diffs)
    print(f"  增加 0 筆（無加碼）：    {np.sum(arr <= 0):>5} 檔 ({np.sum(arr<=0)/len(arr)*100:.1f}%)")
    print(f"  增加 1-5 筆：           {np.sum((arr > 0) & (arr <= 5)):>5} 檔")
    print(f"  增加 6-15 筆：          {np.sum((arr > 5) & (arr <= 15)):>5} 檔")
    print(f"  增加 16-30 筆：         {np.sum((arr > 15) & (arr <= 30)):>5} 檔")
    print(f"  增加 30+ 筆（重度加碼）：{np.sum(arr > 30):>5} 檔")

    # 加碼次數 vs v7→p0 改善的相關性
    print(f"\n  加碼次數 vs 改善幅度的關聯：")
    for thresh, label in [(0, '無加碼'), (5, '1-5次'), (15, '6-15次'),
                           (30, '16-30次'), (999, '30+次')]:
        if thresh == 0:
            sub = [r for r in table if r['trades_p0'] - r['trades_v7'] <= 0]
        else:
            sub = [r for r in table
                   if (thresh-15 if thresh<=15 else 30) < (r['trades_p0'] - r['trades_v7']) <= thresh]
        if not sub: continue
        improve = np.mean([r['d_v7_p0'] for r in sub])
        print(f"    {label:<10} n={len(sub):>4}  v7→P0 平均改善 {fmt(improve, 0)}")

    # ─── 11. 異常檢測：v7 大贏但 P0 反而虧損 ────────────
    print("\n━━━ 11. 異常股票：v7 大贏但 P0 卻變大虧 ━━━")
    anomalies = [r for r in table if r['v7'] > 100 and r['p0'] < -50]
    anomalies.sort(key=lambda r: r['p0'])
    print(f"  共 {len(anomalies)} 檔（v7 +100%+ 但 P0 -50%-）")
    print(f"  {'代號':<7} {'名稱':<10} {'BH%':>8} {'v7%':>8} {'P0%':>8} {'CB30%':>8}")
    for r in anomalies[:15]:
        print(f"  {r['ticker']:<7} {r['name']:<10} {r['bh']:>+7.0f}%"
              f" {r['v7']:>+7.0f}% {r['p0']:>+7.0f}% {r['cb30']:>+7.0f}%")

    # ─── 12. 結論摘要 ────────────────────────────────────
    print("\n━━━ 12. 統計摘要 ━━━")
    n = len(table)
    p0_better = sum(1 for r in table if r['d_v7_p0'] > 5)
    p0_worse  = sum(1 for r in table if r['d_v7_p0'] < -5)
    p0_same   = n - p0_better - p0_worse
    cb_better = sum(1 for r in table if r['cb30'] > r['p0'] + 5)
    cb_worse  = sum(1 for r in table if r['cb30'] < r['p0'] - 5)

    print(f"  v7 → v8 P0 比較：")
    print(f"    改善 >+5%：{p0_better:>4} 檔 ({p0_better/n*100:.0f}%)")
    print(f"    持平：    {p0_same:>4} 檔 ({p0_same/n*100:.0f}%)")
    print(f"    退步 >-5%：{p0_worse:>4} 檔 ({p0_worse/n*100:.0f}%)")
    print(f"  P0 → P0+CB30 比較：")
    print(f"    救回 >+5%：{cb_better:>4} 檔 ({cb_better/n*100:.0f}%)")
    print(f"    傷害 >-5%：{cb_worse:>4} 檔 ({cb_worse/n*100:.0f}%)")

    # 寫總表 CSV
    output = 'per_stock_full.csv'
    with open(output, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(table[0].keys()))
        w.writeheader()
        w.writerows(sorted(table, key=lambda r: -r['cb30']))
    print(f"\n  完整表已存：{output}")


if __name__ == '__main__':
    main()
