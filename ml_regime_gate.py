"""
ML Regime Gate（方案 1：LR + RF）

訓練 binary classifier「下個月 v8 全市場 RR 是否 < 0.3」
特徵：TWII 60d ROC、廣度、VIX、外資累計、DXY ROC
標籤：用 baseline POS+IND+DXY 各月 RR 計算

輸出：regime_predictions.json {YYYY-MM: predict_yes (block trading)}
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import json
import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import data_loader as dl
import variant_strategy as vs


YEAR_MONTHS = pd.date_range('2020-01-01', '2026-04-30', freq='ME')


# ─────────────────────────────────────────────────────────────────
# Step 1: 計算每月 v8 baseline RR（label 來源）
# ─────────────────────────────────────────────────────────────────
def run_one(args):
    ticker, mode, start, end = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return None
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r['n_trades'] == 0: return None
        return r['pnl_pct']
    except Exception:
        return None


def compute_monthly_rr(mode, tickers):
    """每月計算 v8 全市場 RR"""
    out = {}
    for me in YEAR_MONTHS:
        ms = me.replace(day=1)
        start = ms.strftime('%Y-%m-%d')
        end = me.strftime('%Y-%m-%d')
        args = [(t, mode, start, end) for t in tickers]
        rets = []
        with ProcessPoolExecutor(max_workers=8) as ex:
            for r in ex.map(run_one, args, chunksize=80):
                if r is not None: rets.append(r)
        if not rets:
            out[ms.strftime('%Y-%m')] = None
            continue
        arr = np.array(rets)
        mean = arr.mean(); worst = arr.min()
        rr = mean / abs(worst) if worst < 0 else 0
        out[ms.strftime('%Y-%m')] = float(rr)
    return out


# ─────────────────────────────────────────────────────────────────
# Step 2: 計算每月特徵
# ─────────────────────────────────────────────────────────────────
def compute_monthly_features():
    """回傳 DataFrame index=YYYY-MM"""
    feat_rows = []

    # TWII 載入
    twii = dl.load_from_cache('^TWII')
    if twii is None:
        print('警告：無 TWII 快取')
        return None

    # VIX 載入
    vix_df = None
    try:
        import yfinance as yf
        vix = yf.Ticker('^VIX').history(period='6y', auto_adjust=True)
        vix.index = pd.to_datetime(vix.index)
        if hasattr(vix.index, 'tz') and vix.index.tz is not None:
            vix.index = vix.index.tz_localize(None)
        vix_df = vix
    except Exception as e:
        print(f'VIX 抓取失敗：{e}')

    # DXY
    dxy_df = dl.load_from_cache('DX-Y.NYB')
    if dxy_df is None:
        try:
            import yfinance as yf
            dxy = yf.Ticker('DX-Y.NYB').history(period='6y', auto_adjust=True)
            dxy.index = pd.to_datetime(dxy.index)
            if hasattr(dxy.index, 'tz') and dxy.index.tz is not None:
                dxy.index = dxy.index.tz_localize(None)
            dxy_df = dxy
        except Exception:
            pass

    # 30 檔權值股廣度（最近端 EMA60）
    proxy_tickers = ['2330','2317','2454','2412','2882','2891','2308','2382',
                     '2884','2603','2885','2886','3008','2880','2207','1101',
                     '1216','1303','2002','2615','2609','2610','3711','2474',
                     '6505','3034','2618','3037','2890','2887']
    proxy_dfs = {}
    for t in proxy_tickers:
        d = dl.load_from_cache(t)
        if d is not None:
            proxy_dfs[t] = d

    for me in YEAR_MONTHS:
        ms = me.replace(day=1)
        ym = ms.strftime('%Y-%m')

        twii_60d = np.nan
        try:
            sub = twii[twii.index <= me]
            if len(sub) >= 60:
                cur = sub['Close'].iloc[-1]; c60 = sub['Close'].iloc[-60]
                twii_60d = (cur - c60) / c60 * 100
        except Exception: pass

        breadth = np.nan
        try:
            above = 0; total = 0
            for t, d in proxy_dfs.items():
                sub = d[d.index <= me]
                if len(sub) < 60: continue
                ema60 = sub['Close'].ewm(span=60, adjust=False).mean().iloc[-1]
                cur = sub['Close'].iloc[-1]
                if cur > ema60: above += 1
                total += 1
            if total >= 10: breadth = above / total * 100
        except Exception: pass

        vix_avg = np.nan
        if vix_df is not None:
            try:
                m_start = ms; m_end = me
                sub = vix_df[(vix_df.index >= m_start) & (vix_df.index <= m_end)]
                if len(sub) >= 5:
                    vix_avg = float(sub['Close'].mean())
            except Exception: pass

        dxy_roc5 = np.nan
        if dxy_df is not None:
            try:
                sub = dxy_df[dxy_df.index <= me]
                if len(sub) >= 5:
                    cur = sub['Close'].iloc[-1]; c5 = sub['Close'].iloc[-5]
                    dxy_roc5 = (cur - c5) / c5 * 100
            except Exception: pass

        feat_rows.append({
            'ym': ym,
            'twii_60d': twii_60d,
            'breadth': breadth,
            'vix_avg': vix_avg,
            'dxy_roc5': dxy_roc5,
        })

    return pd.DataFrame(feat_rows).set_index('ym')


# ─────────────────────────────────────────────────────────────────
# Step 3: Train/Test
# ─────────────────────────────────────────────────────────────────
def main():
    print("[1/4] 計算每月特徵...")
    feats = compute_monthly_features()
    if feats is None: return
    print(f"  shape: {feats.shape}")
    print(feats.tail(6))

    print("\n[2/4] 計算每月 v8 全市場 RR (label 來源)...")
    files = sorted(Path('data_cache').glob('*.parquet'))
    tickers = [f.stem for f in files]
    rr_by_month = compute_monthly_rr('P0_T1T3+POS+IND+DXY', tickers)
    rr_series = pd.Series(rr_by_month)
    feats['rr_thismonth'] = rr_series
    # label: 下月 RR < 0（虧損月）
    feats['rr_nextmonth'] = feats['rr_thismonth'].shift(-1)
    feats['label_block'] = (feats['rr_nextmonth'] < 0.0).astype(int)

    print(f"  Label 分布:\n{feats['label_block'].value_counts()}")

    # 過濾有效資料
    valid = feats.dropna(subset=['twii_60d','breadth','vix_avg','dxy_roc5','label_block'])
    print(f"\n  有效樣本: {len(valid)}")

    # Train/Test split
    train = valid.loc[:'2023-12']
    test = valid.loc['2024-01':]
    print(f"  Train: {len(train)} 月, Test: {len(test)} 月")

    if len(train) < 12:
        print("樣本太少，無法訓練"); return

    print("\n[3/4] 訓練 LR + RF...")
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score, precision_score, recall_score

    X_cols = ['twii_60d','breadth','vix_avg','dxy_roc5']
    X_train = train[X_cols].values
    y_train = train['label_block'].values
    X_test = test[X_cols].values
    y_test = test['label_block'].values

    models = {}
    for name, mdl in [
        ('LR', LogisticRegression(max_iter=200)),
        ('RF', RandomForestClassifier(n_estimators=50, max_depth=4, random_state=42)),
    ]:
        mdl.fit(X_train, y_train)
        pred_train = mdl.predict(X_train)
        pred_test = mdl.predict(X_test)
        acc_tr = accuracy_score(y_train, pred_train) * 100
        acc_te = accuracy_score(y_test, pred_test) * 100
        prec_te = precision_score(y_test, pred_test, zero_division=0) * 100
        rec_te = recall_score(y_test, pred_test, zero_division=0) * 100
        print(f"  {name}: Train acc {acc_tr:.0f}% | Test acc {acc_te:.0f}% | "
              f"Test precision {prec_te:.0f}% | Test recall {rec_te:.0f}%")
        models[name] = (mdl, pred_test)

    # 寫出 LR + RF 預測
    print("\n[4/4] 輸出預測 JSON...")
    out = {}
    for ym, lr_p, rf_p in zip(test.index, models['LR'][1], models['RF'][1]):
        out[ym] = {
            'lr_block': int(lr_p),
            'rf_block': int(rf_p),
        }
    Path('regime_predictions.json').write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding='utf-8')
    print(f"  輸出 {len(out)} 個月預測")

    # 另存 Train 期間的也用
    out_train = {}
    for name, (mdl, _) in models.items():
        pred_all = mdl.predict(valid[X_cols].values)
        for ym, p in zip(valid.index, pred_all):
            if ym not in out_train: out_train[ym] = {}
            out_train[ym][f'{name.lower()}_block'] = int(p)
    Path('regime_predictions_full.json').write_text(
        json.dumps(out_train, ensure_ascii=False, indent=1), encoding='utf-8')


if __name__ == '__main__':
    main()
