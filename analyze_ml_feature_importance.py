"""ML Feature Importance — 哪個技術指標真的重要？
================================================
用 Random Forest / XGBoost 預測「下月 30 天報酬」
分析特徵重要性 (feature importance + permutation importance)

Features (對每個 entry day 計算):
  - rsi, adx, atr_pct
  - ema20_gap_pct, ema60_atr_dist
  - cross_days
  - drawdown_60d, distance_high60
  - rsi_5d_change, adx_5d_change
  - vol_60d_avg, price_60d_change
  - pct_b (Bollinger Band position)

Target: 30 天後報酬 (cont. value)

樣本：TW 全市場 vs US 高流動 各跑一次
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl


def extract_features(args):
    """對單檔股票，每個交易日提取 features + 30天後報酬"""
    ticker, market = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp('2020-01-01')]
        if len(df) < 280: return (ticker, None)

        e20 = df['e20'].values
        e60 = df['e60'].values
        rsi = df['rsi'].values if 'rsi' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        atr = df['atr'].values if 'atr' in df.columns else None
        close = df['Close'].values
        high = df['High'].values if 'High' in df.columns else close
        low = df['Low'].values if 'Low' in df.columns else close
        vol = df['Volume'].values if 'Volume' in df.columns else np.zeros_like(close)
        n = len(df)

        if rsi is None or adx is None or atr is None: return (ticker, None)

        adx_th = 18 if market == 'us' else 22
        HOLD = 30

        # 60d high
        h60 = np.array([close[max(0, i-60):i].max() if i >= 1 else close[i]
                        for i in range(n)])
        drawdown = (h60 - close) / h60 * 100
        drawdown[h60 == 0] = 0

        rows = []
        for i in range(60, n - HOLD):
            if any(np.isnan(x) for x in
                   [e20[i], e60[i], rsi[i], adx[i], atr[i]]): continue
            if e20[i] <= e60[i]: continue  # 多頭
            if adx[i] < adx_th: continue   # ADX 達標

            cd = None
            for k in range(1, min(60, i)):
                if np.isnan(e20[i-k]) or np.isnan(e60[i-k]): continue
                if e20[i-k] <= e60[i-k]:
                    cd = k; break
            if cd is None: cd = 60

            if cd > 30: continue  # 只看新趨勢

            # Features
            rsi_5d = rsi[i-5] if i >= 5 and not np.isnan(rsi[i-5]) else rsi[i]
            adx_5d = adx[i-5] if i >= 5 and not np.isnan(adx[i-5]) else adx[i]
            e20_5d = e20[i-5] if i >= 5 and not np.isnan(e20[i-5]) else e20[i]

            atr_pct = atr[i] / close[i] * 100 if close[i] > 0 else 0
            ema20_gap_pct = (e20[i] - e60[i]) / e60[i] * 100 if e60[i] > 0 else 0
            ema60_atr_dist = (close[i] - e60[i]) / atr[i] if atr[i] > 0 else 0
            rsi_chg = rsi[i] - rsi_5d
            adx_chg = adx[i] - adx_5d
            ema20_chg = (e20[i] - e20_5d) / e20_5d * 100 if e20_5d > 0 else 0
            vol_60d = np.mean(vol[i-60:i]) if i >= 60 else vol[i]
            vol_ratio = vol[i] / vol_60d if vol_60d > 0 else 1
            price_60d_chg = (close[i] - close[i-60]) / close[i-60] * 100 if i >= 60 and close[i-60] > 0 else 0

            target = (close[i + HOLD] - close[i]) / close[i] * 100

            rows.append({
                'rsi': rsi[i], 'adx': adx[i], 'atr_pct': atr_pct,
                'cross_days': cd,
                'ema20_gap_pct': ema20_gap_pct,
                'ema60_atr_dist': ema60_atr_dist,
                'drawdown_60d': drawdown[i],
                'rsi_chg_5d': rsi_chg,
                'adx_chg_5d': adx_chg,
                'ema20_chg_5d': ema20_chg,
                'vol_ratio': vol_ratio,
                'price_60d_chg': price_60d_chg,
                'target': target,
            })

        return (ticker, rows)
    except Exception:
        return (ticker, None)


def main():
    DATA = Path('data_cache')
    tw_universe = sorted([p.stem for p in DATA.glob('*.parquet')
                          if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                          and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    tw_universe = [t for t in tw_universe if t in vwap_set]
    print(f"🇹🇼 TW: {len(tw_universe)} 檔\n")

    print("📊 提取 features...")
    t0 = time.time()
    all_rows = []
    tasks = [(t, 'tw') for t in tw_universe]
    with ProcessPoolExecutor(max_workers=16) as ex:
        for ticker, rows in ex.map(extract_features, tasks, chunksize=80):
            if rows is not None:
                all_rows.extend(rows)
    print(f"  {time.time()-t0:.1f}s 完成，{len(all_rows)} 樣本\n")

    df = pd.DataFrame(all_rows)
    print(f"  Features: {[c for c in df.columns if c != 'target']}")

    X = df.drop('target', axis=1).values
    y = df['target'].values
    feature_names = [c for c in df.columns if c != 'target']

    # ── Random Forest Regressor ──
    try:
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42)
        rf = RandomForestRegressor(n_estimators=100, max_depth=8,
                                    n_jobs=-1, random_state=42)
        print("\n📊 訓練 RandomForest...")
        rf.fit(X_train, y_train)
        train_score = rf.score(X_train, y_train)
        test_score = rf.score(X_test, y_test)
        print(f"  Train R² {train_score:.3f} / Test R² {test_score:.3f}")

        # Feature importance (基於 Gini/MSE reduction)
        imps = list(zip(feature_names, rf.feature_importances_))
        imps.sort(key=lambda x: -x[1])

        print("\n📊 Feature Importance（重要性）")
        print(f"  {'Feature':<22} {'重要性':>10}")
        print("-" * 50)
        for fn, imp in imps:
            bar = '█' * int(imp * 100)
            print(f"  {fn:<22} {imp:>10.4f}  {bar}")

        # 預測誤差
        y_pred = rf.predict(X_test)
        mae = np.mean(np.abs(y_test - y_pred))
        baseline_mae = np.mean(np.abs(y_test))
        improve = (baseline_mae - mae) / baseline_mae * 100
        print(f"\n📊 預測準確度（30 天後報酬）")
        print(f"  baseline MAE: {baseline_mae:.2f}%")
        print(f"  RF MAE:       {mae:.2f}%")
        print(f"  改善:         {improve:+.1f}%")

        out = {
            'feature_importance': dict(imps),
            'train_r2': float(train_score),
            'test_r2': float(test_score),
            'mae': float(mae),
            'baseline_mae': float(baseline_mae),
            'mae_improve_pct': float(improve),
            'n_train': len(X_train),
            'n_test': len(X_test),
        }
        with open('ml_feature_importance.json', 'w', encoding='utf-8') as f:
            json.dump(out, f, indent=2, default=str)
        print("\n💾 寫入 ml_feature_importance.json")

    except ImportError:
        print("❌ sklearn 未安裝")


if __name__ == '__main__':
    main()
