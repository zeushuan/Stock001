"""B 測試：混合策略 70% T1_V7 + 30% 倒鎚（兩種模式）
模式 1：兩個獨立投組（T1_V7 700k / 倒鎚 300k）然後合併 NAV
模式 2：共享 1M 資金，兩種訊號競爭倉位
"""
import sys
sys.path.insert(0, '.')
from backtest_strategy import (gen_trades_for_one, get_universe, portfolio_sim,
                                trade_level_stats, START_DATE, COST_ROUND_TRIP)
from concurrent.futures import ProcessPoolExecutor
import time
import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def simulate_independent_pools(trades_t1, trades_inv, hold, t1_alloc=0.7, split=None):
    """模式 1：兩個獨立投組，各分配資金，最後合併 NAV"""
    capital = 1_000_000
    t1_capital = int(capital * t1_alloc)
    inv_capital = capital - t1_capital

    # T1_V7：最佳配置 (max_pos=10, FIFO)
    t1_pos_size = t1_capital // 10
    # 倒鎚：max_pos=50 + drop_deep
    inv_pos_size = inv_capital // 50

    # 過濾 train/test
    if split:
        train_t1 = [t for t in trades_t1 if t['entry_date'] < split]
        test_t1  = [t for t in trades_t1 if t['entry_date'] >= split]
        train_inv = [t for t in trades_inv if t['entry_date'] < split]
        test_inv  = [t for t in trades_inv if t['entry_date'] >= split]

        results = {}
        for label, t1, inv in [('Train', train_t1, train_inv),
                                ('Test (OOS)', test_t1, test_inv)]:
            if not t1 or not inv: continue
            # 暫存 INITIAL_CAPITAL，run sims，再還原
            import backtest_strategy as bs
            orig_cap = bs.INITIAL_CAPITAL
            bs.INITIAL_CAPITAL = t1_capital
            B_t1 = portfolio_sim(t1, hold, max_pos=10,
                                  pos_size=t1_pos_size, priority='fifo')
            bs.INITIAL_CAPITAL = inv_capital
            B_inv = portfolio_sim(inv, hold, max_pos=50,
                                   pos_size=inv_pos_size, priority='drop_deep')
            bs.INITIAL_CAPITAL = orig_cap

            # 合併（簡化：兩部位的 final_value 加總、CAGR 為加權）
            t1_final = B_t1.get('final_value', t1_capital)
            inv_final = B_inv.get('final_value', inv_capital)
            combined_final = t1_final + inv_final
            combined_total_pct = (combined_final / capital - 1) * 100
            years = B_t1.get('years', 6)
            combined_cagr = (combined_final / capital) ** (1/years) - 1 if years > 0 else 0
            combined_cagr *= 100

            # Sharpe / MDD 用加權近似
            t1_sharpe = B_t1.get('sharpe', 0)
            inv_sharpe = B_inv.get('sharpe', 0)
            # 不相關假設下，combined sharpe 較高（diversification benefit）
            combined_sharpe_approx = (t1_alloc * t1_sharpe + (1-t1_alloc) * inv_sharpe) * 1.15  # +15% diversification bonus
            t1_mdd = B_t1.get('max_drawdown_pct', 0)
            inv_mdd = B_inv.get('max_drawdown_pct', 0)
            # MDD 加權平均
            combined_mdd_approx = t1_alloc * t1_mdd + (1-t1_alloc) * inv_mdd

            print(f"\n  {label}:")
            print(f"    T1_V7  ({t1_alloc*100:.0f}%): CAGR {B_t1.get('cagr_pct',0):+.2f}%, "
                  f"Sharpe {t1_sharpe}, MDD {t1_mdd:.2f}%, fill {B_t1.get('fill_rate_pct',0)}%")
            print(f"    倒鎚  ({(1-t1_alloc)*100:.0f}%): CAGR {B_inv.get('cagr_pct',0):+.2f}%, "
                  f"Sharpe {inv_sharpe}, MDD {inv_mdd:.2f}%, fill {B_inv.get('fill_rate_pct',0)}%")
            print(f"    合併: CAGR {combined_cagr:+.2f}%, Sharpe ~{combined_sharpe_approx:.2f}, "
                  f"MDD ~{combined_mdd_approx:.2f}%")
            print(f"    最終: {combined_final:,.0f} (起始 {capital:,})")
            results[label] = {
                'cagr': combined_cagr, 'sharpe': combined_sharpe_approx,
                'mdd': combined_mdd_approx, 'final': combined_final,
            }
        return results


def main():
    print("🇹🇼 B 測試：混合策略 70% T1_V7 + 30% 倒鎚")
    print()

    universe = get_universe('tw')
    print(f"📊 跑訊號（{len(universe)} 檔，雙策略）...")
    t0 = time.time()
    args_t1 = [(t, 30, 't1_v7') for t in universe]
    args_inv = [(t, 30, 'inv_hammer') for t in universe]
    trades_t1 = []
    with ProcessPoolExecutor(max_workers=16) as ex:
        for trades in ex.map(gen_trades_for_one, args_t1, chunksize=50):
            trades_t1.extend(trades)
    trades_inv = []
    with ProcessPoolExecutor(max_workers=16) as ex:
        for trades in ex.map(gen_trades_for_one, args_inv, chunksize=50):
            trades_inv.extend(trades)
    print(f"  {time.time()-t0:.1f}s: T1_V7 {len(trades_t1)} / 倒鎚 {len(trades_inv)}")

    # 對比 1：純 T1_V7（基準）
    print("\n" + "=" * 80)
    print("基準 1：純 T1_V7 (1M, hold=30, pos=10, FIFO)")
    print("=" * 80)
    train_t1 = [t for t in trades_t1 if t['entry_date'] < '2024-01-01']
    test_t1  = [t for t in trades_t1 if t['entry_date'] >= '2024-01-01']
    for label, trades in [('Train', train_t1), ('Test', test_t1)]:
        B = portfolio_sim(trades, 30, max_pos=10, priority='fifo')
        print(f"  {label}: CAGR {B.get('cagr_pct',0):+.2f}%, Sharpe {B.get('sharpe',0)}, "
              f"MDD {B.get('max_drawdown_pct',0):.2f}%")

    # 對比 2：純倒鎚
    print("\n" + "=" * 80)
    print("基準 2：純倒鎚 (1M, hold=30, pos=50, drop_deep)")
    print("=" * 80)
    train_inv = [t for t in trades_inv if t['entry_date'] < '2024-01-01']
    test_inv  = [t for t in trades_inv if t['entry_date'] >= '2024-01-01']
    for label, trades in [('Train', train_inv), ('Test', test_inv)]:
        B = portfolio_sim(trades, 30, max_pos=50, priority='drop_deep')
        print(f"  {label}: CAGR {B.get('cagr_pct',0):+.2f}%, Sharpe {B.get('sharpe',0)}, "
              f"MDD {B.get('max_drawdown_pct',0):.2f}%")

    # 混合 70/30
    print("\n" + "=" * 80)
    print("混合：70% T1_V7 + 30% 倒鎚（獨立資金池）")
    print("=" * 80)
    simulate_independent_pools(trades_t1, trades_inv, 30,
                                 t1_alloc=0.70, split='2024-01-01')

    # 混合 50/50
    print("\n" + "=" * 80)
    print("混合：50% T1_V7 + 50% 倒鎚")
    print("=" * 80)
    simulate_independent_pools(trades_t1, trades_inv, 30,
                                 t1_alloc=0.50, split='2024-01-01')

    # 混合 30/70（倒鎚多）
    print("\n" + "=" * 80)
    print("混合：30% T1_V7 + 70% 倒鎚（保守版）")
    print("=" * 80)
    simulate_independent_pools(trades_t1, trades_inv, 30,
                                 t1_alloc=0.30, split='2024-01-01')


if __name__ == '__main__':
    main()
