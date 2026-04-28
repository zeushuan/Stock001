"""重建 VWAPEXEC 適用清單 v2（多因子綜合分數）
=================================================
原 v1：純 Δ% 排名 → 偏向高波動 1 檔飆股的誤導
v2 多因子：

  score = 30 × log10(TEST_PnL + 100)      # 報酬規模（log 抑制極端值）
        + 20 × (TEST_PnL > 0)              # 測試期賺錢
        + 20 × (TRAIN_PnL > 0)             # 訓練期賺錢（穩定）
        + 20 × log10(avg_daily_value)      # 流動性
        + 10 × min(test_RR_proxy, 1.0)     # 個股風險調整

最高 200 → TOP / 中段 → OK / 倒數 100 → NA

輸出：
  vwap_applicable.json (覆寫原檔)
  applicable_v2_diff.json（與 v1 差異）
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def main():
    with open('full_market_results.json', encoding='utf-8') as f:
        data = json.load(f)

    # 取 VWAPEXEC 的 FULL/TRAIN/TEST PnL
    vwap_full = data.get('B VWAPEXEC|FULL', {})
    vwap_train = data.get('B VWAPEXEC|TRAIN', {})
    vwap_test = data.get('B VWAPEXEC|TEST', {})
    base_test = data.get('A baseline|TEST', {})

    pnl_full = dict(zip(vwap_full['tickers'], vwap_full['pnl_pcts']))
    pnl_train = dict(zip(vwap_train['tickers'], vwap_train['pnl_pcts']))
    pnl_test = dict(zip(vwap_test['tickers'], vwap_test['pnl_pcts']))
    base_pnl = dict(zip(base_test['tickers'], base_test['pnl_pcts']))

    # 流動性：從 data_cache 算近 60 日平均成交額
    print("計算流動性...")
    liquidity = {}
    DATA = Path('data_cache')
    for t in pnl_test:
        p = DATA / f'{t}.parquet'
        if not p.exists(): continue
        try:
            df = pd.read_parquet(p)
            if len(df) < 60: continue
            recent = df.iloc[-60:]
            avg_value = float((recent['Close'] * recent['Volume']).mean())
            liquidity[t] = avg_value
        except Exception:
            continue

    print(f"流動性樣本: {len(liquidity)}\n")

    # ─── 計算每股綜合分數 ────────────────────────────────
    print("計算 v2 綜合分數...")
    rows = []
    for t in pnl_test:
        if t not in liquidity: continue
        pnl_t = pnl_test[t]
        pnl_tr = pnl_train.get(t, 0)
        pnl_f = pnl_full.get(t, 0)
        bp = base_pnl.get(t, 0)
        liq = liquidity[t]

        # 1. 報酬規模 (log10) — 報酬越高分越高，但壓縮飆股
        return_score = max(0, np.log10(pnl_t + 100) * 30) if pnl_t > -100 else 0

        # 2. TEST 期賺錢
        test_win = 20 if pnl_t > 0 else 0

        # 3. TRAIN 期賺錢（穩定性）
        train_win = 20 if pnl_tr > 0 else 0

        # 4. 流動性 log10（值範圍 ~3-10）
        liq_score = np.log10(liq) * 2 if liq > 1 else 0  # 約 6-20 分
        liq_score = min(liq_score, 20)

        # 5. 風險調整 RR（個股 PnL / |最差|）
        # 個股無 worst case，用 baseline 比較作 proxy
        delta = pnl_t - bp
        rr_proxy = (pnl_t / 100) / max(abs(bp), 10) if bp < 0 else min(pnl_t / 100, 1.0)
        rr_score = min(max(rr_proxy * 10, 0), 10)

        total = return_score + test_win + train_win + liq_score + rr_score

        rows.append({
            'ticker': t,
            'pnl_test': round(pnl_t, 1),
            'pnl_train': round(pnl_tr, 1),
            'pnl_full': round(pnl_f, 1),
            'baseline_test': round(bp, 1),
            'delta': round(delta, 1),
            'liquidity': int(liq),
            'score': round(total, 2),
            'breakdown': {
                'return': round(return_score, 1),
                'test_win': test_win,
                'train_win': train_win,
                'liq': round(liq_score, 1),
                'rr': round(rr_score, 1),
            },
        })

    rows.sort(key=lambda x: -x['score'])
    n = len(rows)
    print(f"總共 {n} 檔有完整資料\n")

    # 分級
    out = {}
    n_top = 200
    n_na = 100
    top_tickers = set()
    for i, r in enumerate(rows):
        if i < n_top: tier = 'TOP'
        elif i >= n - n_na and r['pnl_test'] <= 0: tier = 'NA'
        else: tier = 'OK'
        if tier == 'TOP': top_tickers.add(r['ticker'])
        out[r['ticker']] = {
            'baseline': r['baseline_test'],
            'vwapexec': r['pnl_test'],
            'delta': r['delta'],
            'tier': tier,
            'score': r['score'],
        }

    # 與 v1 比較
    print("=" * 60)
    print("v1 vs v2 重疊度")
    print("=" * 60)
    if Path('vwap_applicable.json').exists():
        with open('vwap_applicable.json', encoding='utf-8') as f:
            v1 = json.load(f)
        v1_top = set(t for t, info in v1.items() if info.get('tier') == 'TOP')
        overlap = len(top_tickers & v1_top)
        print(f"v1 TOP 200: {len(v1_top)} 檔")
        print(f"v2 TOP 200: {len(top_tickers)} 檔")
        print(f"重疊: {overlap} ({overlap/200*100:.1f}%)")
        print(f"v1 有但 v2 沒: {len(v1_top - top_tickers)}")
        print(f"v2 有但 v1 沒: {len(top_tickers - v1_top)}")

        # 看哪些股票被加入/移除
        added = sorted(top_tickers - v1_top)
        removed = sorted(v1_top - top_tickers)
        print(f"\nv2 新加入 TOP（v1 不在內）前 10:")
        for t in added[:10]:
            r = next(rr for rr in rows if rr['ticker'] == t)
            print(f"  {t}  score={r['score']}  pnl_test={r['pnl_test']:+.1f}%  "
                  f"liq={r['liquidity']:>10,}")
        print(f"\nv2 移除（v1 是 TOP，v2 不是）前 10:")
        for t in removed[:10]:
            r_old = v1.get(t, {})
            r_new = out.get(t, {})
            print(f"  {t}  v1_delta={r_old.get('delta',0):+.0f}%  "
                  f"v2_score={r_new.get('score',0):.0f}  v2_tier={r_new.get('tier','')}")

    # 印 v2 TOP 20
    print(f"\n{'='*60}")
    print(f"v2 TOP 20 預覽")
    print(f"{'='*60}")
    print(f"{'排':<3} {'代號':<6} {'分數':>7} {'報酬%':>9} "
          f"{'baseline':>10} {'流動':>13}")
    for i, r in enumerate(rows[:20], 1):
        print(f"{i:<3} {r['ticker']:<6} {r['score']:>7.1f} "
              f"{r['pnl_test']:>+9.1f} {r['baseline_test']:>+10.1f} "
              f"{r['liquidity']:>13,}")

    # 寫檔
    with open('vwap_applicable.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 vwap_applicable.json (v2)")
    print(f"   TOP: {sum(1 for v in out.values() if v['tier']=='TOP')}")
    print(f"   OK : {sum(1 for v in out.values() if v['tier']=='OK')}")
    print(f"   NA : {sum(1 for v in out.values() if v['tier']=='NA')}")


if __name__ == '__main__':
    main()
