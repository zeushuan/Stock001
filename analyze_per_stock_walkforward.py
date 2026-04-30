"""個股級 Walk-Forward β 穩定性
====================================
對 1058 檔台股每檔跑：
  Train: 2020-2023 計算 β + R²
  Test:  2024-2026 驗證 β + R² + MAE 改善

找出：
  - β 最穩定（變化 < 20%）
  - R² 兩段都 > 0.15（有預測力）
  - MAE 改善 > 10%（actionable）
  Top N actionable predictive stocks
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


def load_returns(ticker):
    df = dl.load_from_cache(ticker)
    if df is None: return None
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_localize(None)
    df = df[df.index >= pd.Timestamp('2020-01-01')]
    if len(df) < 100: return None
    return df['Close'].pct_change() * 100


_SOX_RET = None


def init_worker():
    global _SOX_RET
    _SOX_RET = load_returns('^SOX')


def analyze_one(ticker):
    """對單檔，跑 SOX → ticker walk-forward"""
    global _SOX_RET
    try:
        tw_ret = load_returns(ticker)
        if tw_ret is None: return (ticker, None)
        sox_lagged = _SOX_RET.shift(1)
        df = pd.concat([sox_lagged, tw_ret], axis=1, join='inner').dropna()
        df.columns = ['us', 'tw']

        train_end = pd.Timestamp('2024-01-01')
        train = df[df.index < train_end]
        test = df[df.index >= train_end]
        if len(train) < 200 or len(test) < 100: return (ticker, None)

        if np.var(train['us']) <= 0: return (ticker, None)
        train_beta = np.cov(train['us'], train['tw'])[0, 1] / np.var(train['us'])
        train_alpha = train['tw'].mean() - train_beta * train['us'].mean()
        train_corr = train['us'].corr(train['tw'])
        train_r2 = train_corr ** 2

        if np.var(test['us']) <= 0: return (ticker, None)
        test_beta = np.cov(test['us'], test['tw'])[0, 1] / np.var(test['us'])
        test_corr = test['us'].corr(test['tw'])
        test_r2 = test_corr ** 2

        # MAE 改善
        pred = train_alpha + train_beta * test['us']
        actual = test['tw']
        baseline_mae = float(np.abs(actual).mean())
        model_mae = float(np.abs(actual - pred).mean())
        improve = (baseline_mae - model_mae) / baseline_mae * 100 \
                  if baseline_mae > 0 else 0

        beta_change_pct = abs(test_beta - train_beta) / abs(train_beta) * 100 \
                          if abs(train_beta) > 0.01 else 999

        return (ticker, {
            'train_beta': float(train_beta),
            'test_beta': float(test_beta),
            'train_r2': float(train_r2),
            'test_r2': float(test_r2),
            'beta_change_pct': float(beta_change_pct),
            'baseline_mae': baseline_mae,
            'model_mae': model_mae,
            'mae_improve_pct': float(improve),
            'n_train': int(len(train)),
            'n_test': int(len(test)),
        })
    except Exception:
        return (ticker, None)


def load_industry_map():
    p = Path('tw_universe.txt')
    out = {}
    if not p.exists(): return out
    for line in p.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 5 and parts[4]:
            out[parts[0].strip()] = parts[4].strip()
    return out


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = [t for t in universe if t in vwap_set]
    print(f"🇹🇼 TW universe: {len(universe)} 檔\n")

    industry_map = load_industry_map()

    print("📊 跑每檔 SOX → TW walk-forward...")
    t0 = time.time()
    results = {}
    with ProcessPoolExecutor(max_workers=WORKERS,
                              initializer=init_worker) as ex:
        for ticker, r in ex.map(analyze_one, universe, chunksize=50):
            if r is not None:
                results[ticker] = r
    print(f"  完成 {time.time()-t0:.1f}s（{len(results)}/{len(universe)}）\n")

    # ── 篩選「actionable」標準 ──
    # β 穩定 (< 30%) + Train R² > 0.10 + Test R² > 0.10 + MAE 改善 > 5%
    actionable = []
    for tk, r in results.items():
        if (r['beta_change_pct'] < 30 and
                r['train_r2'] > 0.10 and r['test_r2'] > 0.10 and
                r['mae_improve_pct'] > 5):
            actionable.append((tk, r))

    actionable.sort(key=lambda x: -x[1]['mae_improve_pct'])

    print("=" * 110)
    print(f"🏆 Top 30 個股級 actionable 預測（β穩+R²>0.10+MAE改善>5%）")
    print("=" * 110)
    print(f"  {'#':<3} {'Ticker':<7} {'行業':<14} "
          f"{'TrainR²':>8} {'TestR²':>8} {'TrainBeta':>10} {'TestBeta':>10} "
          f"{'β變化%':>8} {'MAE改善%':>9}")
    print("-" * 110)
    for i, (tk, r) in enumerate(actionable[:30], 1):
        ind = industry_map.get(tk, '—')[:12]
        print(f"  {i:<3} {tk:<7} {ind:<14} "
              f"{r['train_r2']:>8.3f} {r['test_r2']:>8.3f} "
              f"{r['train_beta']:>+10.3f} {r['test_beta']:>+10.3f} "
              f"{r['beta_change_pct']:>+8.0f} {r['mae_improve_pct']:>+9.1f}")

    print(f"\n  總 actionable: {len(actionable)} 檔（{len(actionable)/len(results)*100:.1f}% of universe）")

    # 行業統計
    print("\n" + "=" * 110)
    print(f"📊 各行業 actionable 比例")
    print("=" * 110)
    industry_total = {}
    industry_actionable = {}
    for tk, r in results.items():
        ind = industry_map.get(tk, '其他')
        industry_total[ind] = industry_total.get(ind, 0) + 1
    for tk, r in actionable:
        ind = industry_map.get(tk, '其他')
        industry_actionable[ind] = industry_actionable.get(ind, 0) + 1

    rows = []
    for ind, total in industry_total.items():
        if total < 5: continue
        n_act = industry_actionable.get(ind, 0)
        ratio = n_act / total * 100
        # 平均 MAE 改善
        avg_mae = np.mean([r['mae_improve_pct'] for tk, r in results.items()
                            if industry_map.get(tk, '') == ind])
        rows.append((ind, total, n_act, ratio, avg_mae))
    rows.sort(key=lambda x: -x[3])

    print(f"  {'行業':<14} {'總':>4} {'actionable':>11} {'比例%':>7} {'avg MAE改善%':>13}")
    for ind, total, n_act, ratio, avg_mae in rows[:15]:
        print(f"  {ind:<14} {total:>4} {n_act:>11} {ratio:>+7.1f} {avg_mae:>+13.2f}")

    # 個股 detail (重要股)
    print("\n" + "=" * 110)
    print("📊 重要個股 walk-forward 結果")
    print("=" * 110)
    key = [
        ('2330', '台積電'), ('2454', '聯發科'), ('2317', '鴻海'),
        ('2308', '台達電'), ('3711', '日月光'), ('2382', '廣達'),
        ('6505', '台塑化'), ('2412', '中華電'), ('1216', '統一'),
        ('2207', '和泰車'), ('2882', '國泰金'), ('2891', '中信金'),
    ]
    print(f"  {'Ticker':<7} {'名稱':<10} "
          f"{'TrainR²':>8} {'TestR²':>8} {'β變化%':>8} {'MAE改善%':>9}")
    for tk, name in key:
        r = results.get(tk)
        if r is None:
            print(f"  {tk:<7} {name:<10} (無資料)")
            continue
        print(f"  {tk:<7} {name:<10} "
              f"{r['train_r2']:>8.3f} {r['test_r2']:>8.3f} "
              f"{r['beta_change_pct']:>+8.0f} {r['mae_improve_pct']:>+9.1f}")

    # 寫 JSON
    out = {
        'all_results': results,
        'actionable_top30': [(tk, r) for tk, r in actionable[:30]],
        'config': {
            'us_index': 'SOX',
            'lag': 1,
            'train_period': '2020-01 ~ 2023-12',
            'test_period': '2024-01 ~ 2026-04',
            'criteria': 'beta_change<30% + train_r2>0.10 + test_r2>0.10 + mae_improve>5%',
        }
    }
    with open('per_stock_walkforward.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n💾 寫入 per_stock_walkforward.json")


if __name__ == '__main__':
    main()
