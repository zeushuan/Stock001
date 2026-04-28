"""個別 Winner / Loser 挖掘
==============================
從 full_market_results.json 挖出：
  1. VWAPEXEC 相對 baseline 改善最多的股票（最該用 VWAPEXEC）
  2. VWAPEXEC 相對 baseline 改善最少 / 變差的股票（VWAPEXEC 不適用）
  3. 整體最佳 / 最差個股（不論變體）
  4. 跨 segment 對比

只看 TEST 期（out-of-sample），最有實務意義。
"""
import sys, json
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def load_industry_map():
    """從 tw_stock_list.json 取股票名稱+產業"""
    p = Path('tw_stock_list.json')
    if not p.exists(): return {}
    try:
        with open(p, encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            if 'tickers' in data:
                # backtest_tw_all 格式
                tickers = data['tickers']
                if isinstance(tickers, dict):
                    return {k: v.get('name', '') for k, v in tickers.items()}
            return {k: v.get('name', '') if isinstance(v, dict) else ''
                    for k, v in data.items()}
        return {}
    except Exception:
        return {}


def main():
    with open('full_market_results.json', encoding='utf-8') as f:
        data = json.load(f)

    name_map = load_industry_map()

    base_test = data.get('A baseline|TEST', {})
    vwap_test = data.get('B VWAPEXEC|TEST', {})

    # 建 ticker -> pnl 對照
    base_pnl = dict(zip(base_test['tickers'], base_test['pnl_pcts']))
    vwap_pnl = dict(zip(vwap_test['tickers'], vwap_test['pnl_pcts']))

    common = sorted(set(base_pnl) & set(vwap_pnl))
    print(f"共同樣本（兩變體都有交易）: {len(common)} 檔\n")

    # 計算每股 VWAPEXEC 改善量
    deltas = []
    for t in common:
        b = base_pnl[t]
        v = vwap_pnl[t]
        deltas.append({
            'ticker': t,
            'name': name_map.get(t, ''),
            'baseline': b,
            'vwapexec': v,
            'delta': v - b,
            'segment': '1-2xxx' if t[0] in '12' else
                       '3-5xxx' if t[0] in '345' else
                       '6xxx+'  if t[0] in '6789' else 'other',
        })

    # 排序
    deltas.sort(key=lambda x: -x['delta'])

    # ── 1. VWAPEXEC 改善最多的 Top 20 ─────────────────────
    print("=" * 88)
    print("🏆 VWAPEXEC 改善最大的 Top 20（最該用 VWAPEXEC）")
    print("=" * 88)
    print(f"{'#':<4} {'代號':<6} {'名稱':<10} {'Seg':<8} "
          f"{'baseline%':>10} {'+VWAPEXEC%':>12} {'Δ%':>10}")
    print("-" * 88)
    for i, d in enumerate(deltas[:20], 1):
        print(f"{i:<4} {d['ticker']:<6} {d['name'][:8]:<10} {d['segment']:<8} "
              f"{d['baseline']:>+10.1f} {d['vwapexec']:>+12.1f} "
              f"{d['delta']:>+10.1f}")

    # ── 2. VWAPEXEC 變差最多的 Bottom 20 ─────────────────
    print("\n" + "=" * 88)
    print("⚠️ VWAPEXEC 改善最少 / 反而變差的 Bottom 20（不適用）")
    print("=" * 88)
    print(f"{'#':<4} {'代號':<6} {'名稱':<10} {'Seg':<8} "
          f"{'baseline%':>10} {'+VWAPEXEC%':>12} {'Δ%':>10}")
    print("-" * 88)
    for i, d in enumerate(deltas[-20:][::-1], 1):
        print(f"{i:<4} {d['ticker']:<6} {d['name'][:8]:<10} {d['segment']:<8} "
              f"{d['baseline']:>+10.1f} {d['vwapexec']:>+12.1f} "
              f"{d['delta']:>+10.1f}")

    # ── 3. baseline 最差 + VWAPEXEC 是否救得回？─────────────
    print("\n" + "=" * 88)
    print("🆘 baseline 最慘 Top 20 — 看 VWAPEXEC 能救多少")
    print("=" * 88)
    by_baseline = sorted(deltas, key=lambda x: x['baseline'])[:20]
    print(f"{'#':<4} {'代號':<6} {'名稱':<10} {'Seg':<8} "
          f"{'baseline%':>10} {'+VWAPEXEC%':>12} {'Δ%':>10} 備註")
    print("-" * 88)
    saved = 0
    for i, d in enumerate(by_baseline, 1):
        rescued = d['vwapexec'] > 0 and d['baseline'] < 0
        if rescued: saved += 1
        note = '✅ 由虧轉盈' if rescued else ('🔻 仍虧損' if d['vwapexec'] < 0 else '')
        print(f"{i:<4} {d['ticker']:<6} {d['name'][:8]:<10} {d['segment']:<8} "
              f"{d['baseline']:>+10.1f} {d['vwapexec']:>+12.1f} "
              f"{d['delta']:>+10.1f} {note}")
    print(f"\n  → {saved}/20 由虧轉盈（{saved*5}%）")

    # ── 4. 整體最強 Top 20（baseline + VWAPEXEC）─────────
    print("\n" + "=" * 88)
    print("🚀 整體最強 Top 20（按 +VWAPEXEC）")
    print("=" * 88)
    by_vwap = sorted(deltas, key=lambda x: -x['vwapexec'])[:20]
    print(f"{'#':<4} {'代號':<6} {'名稱':<10} {'Seg':<8} "
          f"{'baseline%':>10} {'+VWAPEXEC%':>12} {'Δ%':>10}")
    print("-" * 88)
    for i, d in enumerate(by_vwap, 1):
        print(f"{i:<4} {d['ticker']:<6} {d['name'][:8]:<10} {d['segment']:<8} "
              f"{d['baseline']:>+10.1f} {d['vwapexec']:>+12.1f} "
              f"{d['delta']:>+10.1f}")

    # ── 5. Segment 統計 ───────────────────────────
    print("\n" + "=" * 88)
    print("📊 各 Segment 統計（TEST 期）")
    print("=" * 88)
    print(f"{'Segment':<10} {'n':>5} {'avg base':>10} {'avg vwap':>10} "
          f"{'avg Δ':>9} {'>0 ratio':>10} {'救回 ratio':>10}")
    print("-" * 88)
    for seg in ['1-2xxx', '3-5xxx', '6xxx+']:
        seg_data = [d for d in deltas if d['segment'] == seg]
        if not seg_data: continue
        n = len(seg_data)
        avg_b = sum(d['baseline'] for d in seg_data) / n
        avg_v = sum(d['vwapexec'] for d in seg_data) / n
        avg_d = sum(d['delta'] for d in seg_data) / n
        positive = sum(1 for d in seg_data if d['delta'] > 0)
        rescued = sum(1 for d in seg_data
                      if d['baseline'] < 0 and d['vwapexec'] > 0)
        loss_n = sum(1 for d in seg_data if d['baseline'] < 0)
        print(f"{seg:<10} {n:>5} {avg_b:>+10.1f} {avg_v:>+10.1f} "
              f"{avg_d:>+9.1f} {positive/n*100:>9.0f}% "
              f"{(rescued/loss_n*100 if loss_n else 0):>9.0f}%")


if __name__ == '__main__':
    main()
