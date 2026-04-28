"""50+ 變體綜合驗證（v9.9e 新版）
==================================
基準：A baseline / B VWAPEXEC / C P5+VWAPEXEC（v9.7 新最佳）
測試所有歷史變體 + 新 NLP 變體（NLP 待資料就緒）

每個變體都疊加在 P5+VWAPEXEC 之上，看是否能再 +0.05 RR。

執行：~10-15 分鐘（全市場 1050 × 30+ variants × 2 windows）
"""
import sys, time
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import numpy as np
from concurrent.futures import ProcessPoolExecutor

import data_loader as dl
import variant_strategy as vs

WORKERS = 16

VARIANTS = [
    # ─── 基準（reference）────────────────────────────────
    ('A baseline',           'P0_T1T3+POS+IND+DXY'),
    ('B VWAPEXEC',           'P0_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('C P5+VWAPEXEC',        'P5_T1T3+POS+IND+DXY+VWAPEXEC'),
    # ─── 加碼門檻 ──────────────────────────────────────
    ('P10+VWAPEXEC',         'P10_T1T3+POS+IND+DXY+VWAPEXEC'),
    ('P15+VWAPEXEC',         'P15_T1T3+POS+IND+DXY+VWAPEXEC'),
    # ─── EPS / PE 估值（已知失敗）────────────────────────
    ('+PEPOS',               'P5_T1T3+POS+IND+DXY+VWAPEXEC+PEPOS'),
    ('+PEMID',               'P5_T1T3+POS+IND+DXY+VWAPEXEC+PEMID'),
    ('+PEMAX30',             'P5_T1T3+POS+IND+DXY+VWAPEXEC+PEMAX30'),
    ('+DIV3',                'P5_T1T3+POS+IND+DXY+VWAPEXEC+DIV3'),
    ('+PBR2',                'P5_T1T3+POS+IND+DXY+VWAPEXEC+PBR2'),
    ('+PEMOM10',             'P5_T1T3+POS+IND+DXY+VWAPEXEC+PEMOM10'),
    ('+PEAVG',               'P5_T1T3+POS+IND+DXY+VWAPEXEC+PEAVG'),
    # ─── 券資比（已知失敗）─────────────────────────────
    ('+MSRATIO5',            'P5_T1T3+POS+IND+DXY+VWAPEXEC+MSRATIO5'),
    ('+MSCAP30',             'P5_T1T3+POS+IND+DXY+VWAPEXEC+MSCAP30'),
    ('+MSMOM50',             'P5_T1T3+POS+IND+DXY+VWAPEXEC+MSMOM50'),
    # ─── 黑天鵝防護（已知失敗）──────────────────────────
    ('+BSGUARD',             'P5_T1T3+POS+IND+DXY+VWAPEXEC+BSGUARD'),
    # ─── VWAP 進階（已知失敗）─────────────────────────
    ('+VWAPDEV1',            'P5_T1T3+POS+IND+DXY+VWAPEXEC+VWAPDEV1'),
    ('+VWAPDEV2',            'P5_T1T3+POS+IND+DXY+VWAPEXEC+VWAPDEV2'),
    ('+VWAPBAND1',           'P5_T1T3+POS+IND+DXY+VWAPEXEC+VWAPBAND1'),
    ('+STRONGCL',            'P5_T1T3+POS+IND+DXY+VWAPEXEC+STRONGCL'),
    ('+WEAKCL',              'P5_T1T3+POS+IND+DXY+VWAPEXEC+WEAKCL'),
    # ─── 動態停損（已知無效）────────────────────────
    ('+DYNSTOP',             'P5_T1T3+POS+IND+DXY+VWAPEXEC+DYNSTOP'),
    # ─── 多時間框架 ─────────────────────────────────
    ('+WRSI',                'P5_T1T3+POS+IND+DXY+VWAPEXEC+WRSI'),
    ('+WADX',                'P5_T1T3+POS+IND+DXY+VWAPEXEC+WADX'),
    # ─── 跨市場過濾 ─────────────────────────────────
    ('+VIX30',               'P5_T1T3+POS+IND+DXY+VWAPEXEC+VIX30'),
    ('+SOX',                 'P5_T1T3+POS+IND+DXY+VWAPEXEC+SOX'),
    ('+HG',                  'P5_T1T3+POS+IND+DXY+VWAPEXEC+HG'),
    # ─── NEWS（待 news_cache 就緒後加）────────────────
    # ('+NEWSPOS3',            'P5_T1T3+POS+IND+DXY+VWAPEXEC+NEWSPOS3'),
    # ('+NEWSNEG3',            'P5_T1T3+POS+IND+DXY+VWAPEXEC+NEWSNEG3'),
    # ('+NEWSMOM',             'P5_T1T3+POS+IND+DXY+VWAPEXEC+NEWSMOM'),
]
WINDOWS = [
    ('FULL',  '2020-01-02', '2026-04-25'),
    ('TEST',  '2024-06-01', '2026-04-25'),
]


def run_one(args):
    ticker, mode, start, end, label = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (label, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0:
            return (label, None)
        return (label, r['pnl_pct'])
    except Exception:
        return (label, None)


def metrics(returns):
    if not returns: return None
    arr = np.array(returns)
    return dict(
        n=len(arr), mean=float(arr.mean()), worst=float(arr.min()),
        win=float((arr > 0).mean() * 100),
        rr=float((arr.mean() / abs(arr.min())) if arr.min() < 0 else 0),
    )


def main():
    data_cache = set(p.stem for p in Path('data_cache').glob('*.parquet'))
    vwap_cache = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    universe = sorted(t for t in (data_cache & vwap_cache)
                      if t and t[0].isdigit() and len(t) == 4
                      and not t.startswith('00'))
    print(f"workers = {WORKERS}, universe = {len(universe)} 檔\n")
    print(f"變體數：{len(VARIANTS)} × 2 窗 = {len(VARIANTS)*2} slices\n")

    all_tasks = []
    for win_name, start, end in WINDOWS:
        for var_name, mode in VARIANTS:
            for t in universe:
                all_tasks.append((t, mode, start, end, (var_name, win_name)))
    print(f"總任務：{len(all_tasks)}\n")

    t0 = time.time()
    bucket = {}
    n_done = 0
    milestone = max(1, len(all_tasks) // 25)
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for label, pnl in ex.map(run_one, all_tasks, chunksize=80):
            n_done += 1
            if pnl is not None:
                bucket.setdefault(label, []).append(pnl)
            if n_done % milestone == 0:
                pct = n_done / len(all_tasks) * 100
                rate = n_done / max(time.time()-t0, 0.1)
                eta = (len(all_tasks) - n_done) / max(rate, 0.1) / 60
                print(f"  {pct:.0f}% ({n_done}/{len(all_tasks)})  "
                      f"{rate:.0f}/s  ETA {eta:.1f} min", flush=True)

    elapsed = time.time() - t0
    print(f"\n完成 {elapsed:.1f}s\n")

    # ─── 結果排序 ─────────────────────────────────────
    print("=" * 100)
    print(f"50+ 變體綜合排行榜（{len(universe)} 檔，TEST 期排序）")
    print("=" * 100)

    # 按 TEST RR 排序
    test_results = []
    for var_name, _ in VARIANTS:
        m = metrics(bucket.get((var_name, 'TEST'), []))
        if m: test_results.append((var_name, m))
    test_results.sort(key=lambda x: -x[1]['rr'])

    base_rr = next((m['rr'] for v, m in test_results if v == 'C P5+VWAPEXEC'), 0)
    print(f"{'排名':<5} {'變體':<22} {'n':>5} {'均值%':>9} {'最差%':>9} "
          f"{'勝率%':>7} {'RR':>7}  {'Δ vs C':>9}")
    print("-" * 100)
    for i, (v, m) in enumerate(test_results, 1):
        delta = m['rr'] - base_rr
        marker = ''
        if v == 'C P5+VWAPEXEC':
            marker = '⭐ baseline'
        elif delta > 0.05:
            marker = '⭐ 超越'
        elif delta < -0.05:
            marker = '❌ 不如'
        else:
            marker = '➖ 持平'
        print(f"{i:<5} {v:<22} {m['n']:>5} {m['mean']:>+9.1f} "
              f"{m['worst']:>+9.1f} {m['win']:>7.1f} {m['rr']:>7.3f}  "
              f"{delta:>+9.3f}  {marker}")

    # FULL 期同步顯示
    print("\n" + "=" * 100)
    print("FULL 期對照")
    print("=" * 100)
    full_results = []
    for var_name, _ in VARIANTS:
        m = metrics(bucket.get((var_name, 'FULL'), []))
        if m: full_results.append((var_name, m))
    full_results.sort(key=lambda x: -x[1]['rr'])

    base_full = next((m['rr'] for v, m in full_results if v == 'C P5+VWAPEXEC'), 0)
    for i, (v, m) in enumerate(full_results[:10], 1):
        delta = m['rr'] - base_full
        print(f"  {i}. {v}: RR {m['rr']:.3f}  Δ {delta:+.3f}  (mean {m['mean']:+.1f})")

    # 結論
    print("\n" + "=" * 100)
    print("結論")
    print("=" * 100)
    superior = [v for v, m in test_results if m['rr'] - base_rr > 0.05 and v != 'C P5+VWAPEXEC']
    if superior:
        print(f"  ⭐ 超越 P5+VWAPEXEC 的變體 ({len(superior)} 個):")
        for v in superior:
            m = next(mm for vv, mm in test_results if vv == v)
            print(f"     - {v}: RR {m['rr']:.3f} (Δ +{m['rr']-base_rr:.3f})")
    else:
        print("  ❌ 沒有變體超越 P5+VWAPEXEC")
        print("  → 確認 P5+VWAPEXEC 仍是局部最佳")


if __name__ == '__main__':
    main()
