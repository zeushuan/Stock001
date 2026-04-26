"""
分群分析：用 features.csv 找出 v7→P0 改善/退步的特徵差異

問題：能否從股票特徵預判它在 P0_T1T3 模式下會改善還是退步？

方法：
  1. 改善 (improved_p0 > +5%) vs 退步 (improved_p0 < -5%) 兩組特徵均值對比
  2. 雙樣本 t-test 找出最有區辨力的特徵
  3. 簡單規則式分類器：用一兩個特徵能否準確分類？
  4. 設計適配規則：哪些特徵組合預示「不該加碼」？
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import numpy as np
import pandas as pd


def load_features():
    return pd.read_csv('features.csv', encoding='utf-8-sig')


def main():
    print("━━━━━━ 分群分析：v7→P0 改善 vs 退步的特徵差異 ━━━━━━\n")
    df = load_features()
    print(f"樣本數：{len(df)}\n")

    # 三類分組
    improved = df[df['cat'] == 'improved']
    regressed = df[df['cat'] == 'regressed']
    unchanged = df[df['cat'] == 'unchanged']
    print(f"改善 (improved): {len(improved)} 檔")
    print(f"持平 (unchanged): {len(unchanged)} 檔")
    print(f"退步 (regressed): {len(regressed)} 檔\n")

    # ─── 1. 各特徵的兩組均值對比 ────────────────────────
    feature_cols = ['volatility_d', 'max_dd_pct', 'bull_days_pct',
                    'trend_score', 'rsi_avg', 'rsi_volatility', 'adx_avg',
                    'avg_atr_pct', 'positive_year_ratio', 'worst_year_ret',
                    'bh_pct', 'v7_pct', 'pyramids_added']

    print("━━━ 1. 改善 vs 退步：特徵均值對比 ━━━")
    print(f"  {'特徵':<22} {'改善均值':>10} {'退步均值':>10} {'差距':>10} {'判定':<10}")
    print('  ' + '-' * 75)

    discriminative = []
    for col in feature_cols:
        if col not in df.columns:
            continue
        imp_vals = improved[col].dropna().values
        reg_vals = regressed[col].dropna().values
        if len(imp_vals) == 0 or len(reg_vals) == 0:
            continue
        imp_mean = np.mean(imp_vals)
        reg_mean = np.mean(reg_vals)
        diff = imp_mean - reg_mean

        # 簡單 effect size：差距 / 合併標準差
        pooled_sd = np.sqrt((np.var(imp_vals) + np.var(reg_vals)) / 2)
        cohen_d = diff / pooled_sd if pooled_sd > 0 else 0

        if abs(cohen_d) > 0.5:
            verdict = "⭐ 強區辨"
        elif abs(cohen_d) > 0.3:
            verdict = "🟡 中度"
        else:
            verdict = "微弱"

        print(f"  {col:<22} {imp_mean:>+9.2f}  {reg_mean:>+9.2f}  {diff:>+9.2f}"
              f"  {verdict} (d={cohen_d:+.2f})")
        if abs(cohen_d) > 0.3:
            discriminative.append((col, imp_mean, reg_mean, cohen_d))

    # ─── 2. 找出最強區辨特徵 ────────────────────────
    print("\n━━━ 2. 最強區辨特徵（按 Cohen's d 排序）━━━")
    discriminative.sort(key=lambda x: -abs(x[3]))
    for col, imp, reg, d in discriminative[:5]:
        direction = "改善 > 退步" if d > 0 else "退步 > 改善"
        print(f"  {col:<22} d={d:+.2f}   {direction}")
        print(f"    改善均值 {imp:+.2f}    退步均值 {reg:+.2f}")

    # ─── 3. 簡單規則式分類器 ────────────────────────
    print("\n━━━ 3. 規則式分類器測試 ━━━")
    # 用 v7_pct 試：v7 已虧損 → 退步機率高？
    print("\n  規則 A：v7 已虧損 (v7_pct < 0) → 預期 P0 也退步")
    rule_a_imp = (improved['v7_pct'] < 0).sum()
    rule_a_reg = (regressed['v7_pct'] < 0).sum()
    print(f"    改善組中 v7<0 比例：{rule_a_imp/len(improved)*100:.1f}%")
    print(f"    退步組中 v7<0 比例：{rule_a_reg/len(regressed)*100:.1f}%")

    print("\n  規則 B：BH 微正報酬 (0 < BH < 100) → 退步機率高？")
    rule_b_imp = ((improved['bh_pct'] > 0) & (improved['bh_pct'] < 100)).sum()
    rule_b_reg = ((regressed['bh_pct'] > 0) & (regressed['bh_pct'] < 100)).sum()
    print(f"    改善組 BH 0~100 比例：{rule_b_imp/len(improved)*100:.1f}%")
    print(f"    退步組 BH 0~100 比例：{rule_b_reg/len(regressed)*100:.1f}%")

    print("\n  規則 C：高波動 (volatility_d > 50) → ？")
    rule_c_imp = (improved['volatility_d'] > 50).sum()
    rule_c_reg = (regressed['volatility_d'] > 50).sum()
    print(f"    改善組 vol>50 比例：{rule_c_imp/len(improved)*100:.1f}%")
    print(f"    退步組 vol>50 比例：{rule_c_reg/len(regressed)*100:.1f}%")

    print("\n  規則 D：低多頭天數 (bull_days < 50%) → ？")
    rule_d_imp = (improved['bull_days_pct'] < 50).sum()
    rule_d_reg = (regressed['bull_days_pct'] < 50).sum()
    print(f"    改善組 bull<50% 比例：{rule_d_imp/len(improved)*100:.1f}%")
    print(f"    退步組 bull<50% 比例：{rule_d_reg/len(regressed)*100:.1f}%")

    print("\n  規則 E：年度正報酬比例 < 0.5 → ？")
    rule_e_imp = (improved['positive_year_ratio'] < 0.5).sum()
    rule_e_reg = (regressed['positive_year_ratio'] < 0.5).sum()
    print(f"    改善組 +年比 <50% 比例：{rule_e_imp/len(improved)*100:.1f}%")
    print(f"    退步組 +年比 <50% 比例：{rule_e_reg/len(regressed)*100:.1f}%")

    # ─── 4. 退步股的關鍵特徵組合 ────────────────────
    print("\n━━━ 4. 退步股集中特徵組合 ━━━")
    # 退步且嚴重的（improved < -50）
    severe_reg = df[df['improved_p0'] < -50]
    print(f"  嚴重退步股（v7→P0 退步 >50%）：{len(severe_reg)} 檔")
    if len(severe_reg) > 0:
        print(f"  - bull_days_pct 均值：{severe_reg['bull_days_pct'].mean():.1f}%")
        print(f"  - max_dd_pct 均值：  {severe_reg['max_dd_pct'].mean():.1f}%")
        print(f"  - volatility 均值：  {severe_reg['volatility_d'].mean():.1f}")
        print(f"  - trend_score 均值： {severe_reg['trend_score'].mean():.1f}")
        print(f"  - v7_pct 均值：      {severe_reg['v7_pct'].mean():.1f}%")
        print(f"  - bh_pct 均值：      {severe_reg['bh_pct'].mean():.1f}%")

    # 大幅改善的
    big_imp = df[df['improved_p0'] > 500]
    print(f"\n  大幅改善股（v7→P0 改善 >500%）：{len(big_imp)} 檔")
    if len(big_imp) > 0:
        print(f"  - bull_days_pct 均值：{big_imp['bull_days_pct'].mean():.1f}%")
        print(f"  - max_dd_pct 均值：  {big_imp['max_dd_pct'].mean():.1f}%")
        print(f"  - volatility 均值：  {big_imp['volatility_d'].mean():.1f}")
        print(f"  - trend_score 均值： {big_imp['trend_score'].mean():.1f}")
        print(f"  - v7_pct 均值：      {big_imp['v7_pct'].mean():.1f}%")
        print(f"  - bh_pct 均值：      {big_imp['bh_pct'].mean():.1f}%")

    # ─── 5. 產業類別的退步比例 ────────────────────────
    # 用 ticker 對應 stock_list.json 的 industry
    import json
    try:
        with open('tw_stock_list.json', encoding='utf-8') as f:
            meta = json.load(f)
    except: meta = {}

    df['industry'] = df['ticker'].map(lambda t: meta.get(t, {}).get('industry', '其他'))

    print("\n━━━ 5. 產業別 v7→P0 退步比例 ━━━")
    industry_stats = []
    for ind, sub in df.groupby('industry'):
        if len(sub) < 10: continue
        n_total = len(sub)
        n_imp   = (sub['cat'] == 'improved').sum()
        n_reg   = (sub['cat'] == 'regressed').sum()
        industry_stats.append({
            'industry': ind,
            'total': n_total,
            'imp_pct': n_imp / n_total * 100,
            'reg_pct': n_reg / n_total * 100,
            'avg_imp': sub['improved_p0'].mean(),
        })

    industry_stats.sort(key=lambda x: -x['avg_imp'])
    print(f"  {'產業':<14} {'樣本':>5} {'改善%':>7} {'退步%':>7} {'平均改善':>10}")
    for r in industry_stats[:20]:
        print(f"  {r['industry']:<14} {r['total']:>5} {r['imp_pct']:>6.1f}%"
              f"  {r['reg_pct']:>6.1f}%  {r['avg_imp']:>+9.1f}%")

    # ─── 6. 自適應變體推論 ─────────────────────────
    print("\n━━━ 6. 自適應變體可能規則 ━━━")
    print("  基於上述發現，可考慮的規則：")
    print()
    print("  規則 R1：依 BH 帶決定加碼門檻")
    print("    BH >= 200%  → P0（不限門檻，最大化加碼）")
    print("    100 ≤ BH < 200 → P5_T1T3（適度門檻）")
    print("    BH < 100  → 不加碼（用 v7 base）")
    print()
    print("  規則 R2：依 v7 base 結果決定")
    print("    v7 > +50%  → 啟用 P0 加碼")
    print("    v7 -20~+50 → 啟用 P5+CB30")
    print("    v7 < -20%  → 不加碼（避免死亡迴圈惡化）")
    print()
    print("  規則 R3：依多頭天數比例")
    print("    bull_days_pct >= 60% → 強趨勢股，加碼")
    print("    bull_days_pct < 40% → 震盪股，不加碼")


if __name__ == '__main__':
    main()
