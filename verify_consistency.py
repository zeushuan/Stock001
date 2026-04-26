"""
驗證 variant_strategy.py 的 base 模式與 backtest_all.py 的 v7 結果一致
"""
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception: pass

import warnings; warnings.filterwarnings('ignore')
import backtest_all as bt
import variant_strategy as vs
import data_loader as dl

bt.START = '2020-01-02'
bt.END   = '2026-04-25'

tickers = ['2317', '2330', '6139', '4961', '2939', '5203', '1732', '4133']

print(f"{'股票':<6}  {'bt main%':>9} {'bt T4%':>8} {'bt 全%':>8}  "
      f"{'vs main%':>9} {'vs T4%':>8} {'vs 全%':>8}  "
      f"{'main diff':>10} {'T4 diff':>9}")
print('-' * 100)

for tk in tickers:
    df = dl.load_from_cache(tk)
    if df is None: continue

    bt_result = bt.analyze_with_indicators(df, tk)
    bt_t7   = bt_result['t7']     # 全部
    bt_t7b  = bt_result['t7b']    # 僅 T4
    bt_t7b_set = {(t['ed'], t['ep']) for t in bt_t7b}
    bt_main = [t for t in bt_t7 if (t['ed'], t['ep']) not in bt_t7b_set]
    bt_main_pct = sum(t['pnl'] for t in bt_main) / 100_000 * 100
    bt_t4_pct   = sum(t['pnl'] for t in bt_t7b) / 100_000 * 100
    bt_total    = bt_main_pct + bt_t4_pct

    # variant 內部分開計算（先套用日期過濾，再呼叫內部函式）
    df_filtered = vs._filter_period(df)
    flags = vs._decode_mode('base')
    main_trades = vs._run_v7_strategy(df_filtered, flags)
    t4_trades   = vs._run_t4_bear_bounce(df_filtered)
    vs_main_pct = sum(r * 100_000 * mult for r, mult, _ in main_trades) / 100_000 * 100
    vs_t4_pct   = sum(r * 100_000 * mult for r, mult, _ in t4_trades) / 100_000 * 100
    vs_total    = vs_main_pct + vs_t4_pct

    print(f"  {tk:<5}   {bt_main_pct:>+8.1f}% {bt_t4_pct:>+7.1f}% {bt_total:>+7.1f}%  "
          f"{vs_main_pct:>+8.1f}% {vs_t4_pct:>+7.1f}% {vs_total:>+7.1f}%  "
          f"{vs_main_pct-bt_main_pct:>+9.1f} {vs_t4_pct-bt_t4_pct:>+8.1f}")
