"""建立 VWAPEXEC 適用清單
================================
從 full_market_results.json 計算每股 Δ RR (VWAPEXEC - baseline)：
  Top 200 = ⭐ 適用（VWAPEXEC 帶來大幅提升）
  > 0      = ✅ 一般適用
  ≤ 0      = ⚠️ 不適用（VWAPEXEC 反而變差）

輸出：vwap_applicable.json — { ticker: { delta, tier } }
tier: 'TOP' / 'OK' / 'NA'
"""
import sys, json
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def main():
    with open('full_market_results.json', encoding='utf-8') as f:
        data = json.load(f)

    base = data.get('A baseline|TEST', {})
    vwap = data.get('B VWAPEXEC|TEST', {})

    base_pnl = dict(zip(base['tickers'], base['pnl_pcts']))
    vwap_pnl = dict(zip(vwap['tickers'], vwap['pnl_pcts']))

    common = set(base_pnl) & set(vwap_pnl)
    deltas = []
    for t in common:
        d = vwap_pnl[t] - base_pnl[t]
        deltas.append({'ticker': t, 'baseline': base_pnl[t],
                       'vwapexec': vwap_pnl[t], 'delta': d})
    deltas.sort(key=lambda x: -x['delta'])

    n = len(deltas)
    n_top = min(200, n // 5)
    out = {}
    for i, e in enumerate(deltas):
        if i < n_top:
            tier = 'TOP'
        elif e['delta'] > 0:
            tier = 'OK'
        else:
            tier = 'NA'
        out[e['ticker']] = {
            'baseline': round(e['baseline'], 1),
            'vwapexec': round(e['vwapexec'], 1),
            'delta': round(e['delta'], 1),
            'tier': tier,
        }

    # 統計
    n_top_actual = sum(1 for v in out.values() if v['tier'] == 'TOP')
    n_ok = sum(1 for v in out.values() if v['tier'] == 'OK')
    n_na = sum(1 for v in out.values() if v['tier'] == 'NA')
    print(f"分級結果：")
    print(f"  ⭐ TOP (前 {n_top_actual} 檔): VWAPEXEC 大幅提升")
    print(f"  ✅ OK  ({n_ok} 檔): 一般適用")
    print(f"  ⚠️ NA  ({n_na} 檔): VWAPEXEC 不適用（Δ ≤ 0）")
    print()

    print(f"⭐ TOP 20 預覽：")
    for i, e in enumerate(deltas[:20], 1):
        print(f"  {i:>2}. {e['ticker']}  Δ {e['delta']:>+.1f}  "
              f"(base {e['baseline']:>+.1f} → vwap {e['vwapexec']:>+.1f})")

    print(f"\n⚠️ NA 最差 10 檔：")
    for i, e in enumerate(deltas[-10:], 1):
        print(f"  {i:>2}. {e['ticker']}  Δ {e['delta']:>+.1f}")

    with open('vwap_applicable.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 vwap_applicable.json ({len(out)} 檔)")


if __name__ == '__main__':
    main()
