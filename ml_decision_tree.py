"""
ML 簡易版：用決策樹預測 v7→P0 是否會改善

樣本：1243 檔已計算特徵
特徵：bull_days_pct / v7_pct / trend_score / rsi_avg / volatility 等 14 個
標籤：improved_p0 > 5 (改善) / improved_p0 < -5 (退步) / 持平

目的：
  1. 找出最重要的特徵與分割點
  2. 看是否能用簡單規則替代或增強 POS
  3. 評估 ML 對策略的貢獻
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
import pandas as pd

try:
    from sklearn.tree import DecisionTreeClassifier, export_text
    from sklearn.model_selection import cross_val_score
    from sklearn.ensemble import RandomForestClassifier
    HAS_SK = True
except ImportError:
    HAS_SK = False
    print("⚠️ sklearn 未安裝。嘗試: pip install scikit-learn")
    sys.exit(1)


def main():
    print("━━━━━━ ML 決策樹：預測 v7→P0 改善 ━━━━━━\n")
    df = pd.read_csv('features.csv', encoding='utf-8-sig')

    # 移除無效樣本
    df = df.dropna(subset=['v7_pct', 'p0_pct', 'improved_p0'])
    print(f"有效樣本：{len(df)}\n")

    # 二元標籤：改善 = 1, 其他 = 0
    df['target'] = (df['improved_p0'] > 5).astype(int)

    # 特徵欄位
    features = [
        'bh_pct', 'v7_pct', 'volatility_d', 'max_dd_pct',
        'bull_days_pct', 'trend_score', 'rsi_avg', 'rsi_volatility',
        'adx_avg', 'avg_atr_pct', 'positive_year_ratio',
        'worst_year_ret', 'pyramids_added',
    ]
    X = df[features].fillna(0)
    y = df['target']

    print(f"標籤分布：")
    print(f"  改善 (1)：{y.sum()} 檔 ({y.mean()*100:.1f}%)")
    print(f"  其他 (0)：{len(y) - y.sum()} 檔 ({(1-y.mean())*100:.1f}%)")
    print()

    # ─── 1. 簡單決策樹（深度 3）────────────────────────
    print("━━━ 1. 決策樹 (max_depth=3)：可解釋規則 ━━━")
    tree = DecisionTreeClassifier(max_depth=3, random_state=42, min_samples_leaf=30)
    tree.fit(X, y)

    # 5-fold CV
    scores = cross_val_score(tree, X, y, cv=5, scoring='accuracy')
    print(f"5-fold CV 準確率：{scores.mean()*100:.1f}% (±{scores.std()*100:.1f}%)")
    print(f"訓練集準確率：    {tree.score(X, y)*100:.1f}%")

    # 印出規則
    print("\n決策樹規則：")
    rules = export_text(tree, feature_names=features, max_depth=3)
    for line in rules.split('\n'):
        print(f"  {line}")

    # ─── 2. 隨機森林（更精確但較難解釋）──────────────
    print("\n━━━ 2. 隨機森林（特徵重要性）━━━")
    rf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42,
                                 min_samples_leaf=10)
    rf.fit(X, y)
    rf_scores = cross_val_score(rf, X, y, cv=5, scoring='accuracy')
    print(f"5-fold CV 準確率：{rf_scores.mean()*100:.1f}% (±{rf_scores.std()*100:.1f}%)")

    # 特徵重要性
    importances = sorted(zip(features, rf.feature_importances_), key=lambda x: -x[1])
    print("\n特徵重要性（隨機森林）：")
    for feat, imp in importances:
        bar = '█' * int(imp * 200)
        print(f"  {feat:<22} {imp*100:>5.1f}%  {bar}")

    # ─── 3. 各特徵單獨閾值最佳分割 ─────────────────
    print("\n━━━ 3. 各特徵獨立分析（最強分割點）━━━")
    print(f"  {'特徵':<22} {'最佳閾值':>10} {'方向':>6} {'+5% 改善率':>12}")
    print('  ' + '-' * 65)
    for feat in features:
        vals = df[feat].values
        if np.isnan(vals).all(): continue
        targets = df['target'].values

        # 嘗試多個閾值
        thresholds = np.percentile(vals[~np.isnan(vals)], [10, 25, 50, 75, 90])
        best_th = None; best_acc = 0; best_dir = ''
        for th in thresholds:
            for op in ['>', '<']:
                if op == '>':
                    pred = (vals > th).astype(int)
                else:
                    pred = (vals < th).astype(int)
                # 兩種對應：1=改善 / 1=不改善
                acc = max((pred == targets).mean(), ((1-pred) == targets).mean())
                if acc > best_acc:
                    best_acc = acc
                    best_th = th
                    best_dir = op
        print(f"  {feat:<22} {best_th:>+9.2f} {best_dir:>6} {best_acc*100:>11.1f}%")

    # ─── 4. 如果用 v7_pct 為主規則 ──────────────────
    print("\n━━━ 4. 簡單規則：v7_pct > 0 是否就足夠？ ━━━")
    rule = (df['v7_pct'] > 0).astype(int)
    rule_acc = (rule == df['target']).mean()
    print(f"  規則「v7_pct > 0 → 預測改善」準確率：{rule_acc*100:.1f}%")
    print(f"  - 預測改善的樣本中真改善比例（precision）：")
    pred_improve = df[rule == 1]
    print(f"      {pred_improve['target'].mean()*100:.1f}%（{len(pred_improve)} 檔）")
    print(f"  - 預測不改善的樣本中真不改善比例：")
    pred_not = df[rule == 0]
    print(f"      {(1-pred_not['target'].mean())*100:.1f}%（{len(pred_not)} 檔）")

    # ─── 5. 結論：ML 是否能超越 POS ────────────────
    print("\n━━━ 5. ML vs POS 比較 ━━━")
    print(f"  POS 規則本質：'累積為正才加碼' (即時版)")
    print(f"  v7_pct 規則：'全期 v7 為正才加碼' (事後版)")
    print(f"  決策樹 (max_depth=3) CV 準確率：{scores.mean()*100:.1f}%")
    print(f"  簡單 v7>0 規則準確率：{rule_acc*100:.1f}%")
    print()
    print("  ⚠️ 注意：決策樹用了「事後特徵」(v7_pct, bh_pct)，無法即時用")
    print("  → POS 已是「即時 v7 為正」的最簡規則")
    print("  → ML 額外特徵（bull_days/trend_score）若不能即時計算就沒實用價值")


if __name__ == '__main__':
    main()
