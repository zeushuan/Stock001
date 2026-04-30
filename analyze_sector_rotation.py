"""產業輪動策略研究
==========================
研究問題：每月找最強產業 → 集中投資，是否優於全市場等權？

策略邏輯：
  每月 1 日 rebalance
  計算過去 N 月各產業平均報酬
  選 top K 產業集中持有
  N = 1 / 3 / 6 月（觀察窗）
  K = 1 / 3 / 5 個產業
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = [t for t in universe if t in vwap_set]

    # 讀行業
    industry_map = {}
    p = Path('tw_universe.txt')
    if p.exists():
        for line in p.read_text(encoding='utf-8').splitlines():
            if not line or line.startswith('#'): continue
            parts = line.split('|')
            if len(parts) >= 5 and parts[4]:
                industry_map[parts[0].strip()] = parts[4].strip()

    # 載入收盤
    print("📊 載入收盤序列...")
    closes = {}
    for t in universe:
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 280: continue
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df = df.copy()
                df.index = df.index.tz_localize(None)
            df = df[df.index >= pd.Timestamp('2020-01-01')]
            if len(df) < 280: continue
            closes[t] = df['Close']
        except: pass
    print(f"  {len(closes)} 檔載入\n")

    # 月度收盤
    df_close = pd.DataFrame(closes).resample('ME').last()
    df_ret = df_close.pct_change()  # 月報酬

    print(f"  月度資料: {df_ret.shape[0]} 個月 × {df_ret.shape[1]} 檔")

    # 對每個月計算產業平均報酬
    industries = set(industry_map.values())
    sector_rets = pd.DataFrame(index=df_ret.index)
    for ind in industries:
        members = [t for t, i in industry_map.items() if i == ind and t in df_ret.columns]
        if len(members) < 5: continue
        sector_rets[ind] = df_ret[members].mean(axis=1)

    print(f"  產業數: {sector_rets.shape[1]}")

    # 策略模擬
    def rotation_strategy(lookback_n, top_k):
        """每月 rebalance：選過去 N 月平均最強的 K 個產業
        持有下個月，計算報酬"""
        portfolio_rets = []
        for i in range(lookback_n, len(sector_rets) - 1):
            past = sector_rets.iloc[i-lookback_n:i].mean()
            top_inds = past.dropna().sort_values(ascending=False).head(top_k).index.tolist()
            if not top_inds: continue
            next_ret = sector_rets.iloc[i+1][top_inds].mean()
            portfolio_rets.append(next_ret)
        if not portfolio_rets: return None
        ret_arr = np.array(portfolio_rets)
        ret_arr = ret_arr[~np.isnan(ret_arr)]
        total = (1 + ret_arr).prod() - 1
        avg = ret_arr.mean()
        sigma = ret_arr.std()
        sharpe = avg / sigma * np.sqrt(12) if sigma > 0 else 0
        return {
            'lookback': lookback_n, 'top_k': top_k,
            'total_return': float(total * 100),
            'monthly_avg': float(avg * 100),
            'monthly_sigma': float(sigma * 100),
            'sharpe': float(sharpe),
            'n_months': len(ret_arr),
        }

    # baseline: 全市場等權
    baseline = sector_rets.mean(axis=1).dropna()
    base_total = (1 + baseline).prod() - 1
    base_avg = baseline.mean()
    base_sigma = baseline.std()
    base_sharpe = base_avg / base_sigma * np.sqrt(12) if base_sigma > 0 else 0
    print(f"\n📊 Baseline 全產業等權:")
    print(f"  總報酬 {base_total*100:+.1f}% / 月均 {base_avg*100:+.2f}% / "
          f"σ {base_sigma*100:.2f}% / Sharpe {base_sharpe:.2f}")

    # 跑各參數組合
    print(f"\n📊 產業輪動策略矩陣（lookback × top_k）")
    print(f"  {'參數':<25} {'總報酬%':>9} {'月均%':>8} {'σ%':>8} {'Sharpe':>8} {'vs base%':>9}")
    print("-" * 80)
    results = {}
    for lb in [1, 3, 6, 12]:
        for k in [1, 3, 5]:
            r = rotation_strategy(lb, k)
            if r is None: continue
            label = f'lookback={lb}m, top {k}'
            vs_base = r['total_return'] - base_total * 100
            print(f"  {label:<25} {r['total_return']:>+9.1f} "
                  f"{r['monthly_avg']:>+8.2f} {r['monthly_sigma']:>+8.2f} "
                  f"{r['sharpe']:>+8.2f} {vs_base:>+9.1f}")
            results[label] = r

    # 找最佳
    best = max(results.values(), key=lambda x: x['sharpe'])
    print(f"\n⭐ 最佳組合: lookback={best['lookback']}m / top_k={best['top_k']}")
    print(f"  Sharpe {best['sharpe']:.2f} (vs baseline {base_sharpe:.2f})")

    out = {
        'baseline': {
            'total_return': float(base_total * 100),
            'monthly_avg': float(base_avg * 100),
            'sharpe': float(base_sharpe),
        },
        'strategies': results,
        'best': best,
    }
    with open('sector_rotation.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("\n💾 寫入 sector_rotation.json")


if __name__ == '__main__':
    main()
