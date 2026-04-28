"""黑天鵝事件 EDA — 不防護，先看實況
======================================
目的：理解 v8 在黑天鵝事件不同階段的實際表現，再決定該不該/怎麼調整防護。

把 2020-2026 每個進場日按與黑天鵝事件的相對位置分群：

  ① 危險窗內    : trigger 起 ≤ 10 個交易日（v8 信號很少，但若觸發呢？）
  ② 剛結束 +10D : trigger 結束後 1-10 個交易日（黃金反彈期？）
  ③ 剛結束 +30D : trigger 結束後 11-30 個交易日（延伸反彈期）
  ④ 正常期      : 距離任何事件 ≥ 30 個交易日（baseline）

對每群分別計算：
  - T1 (穩健金叉) / 飆股 (T1+ADX≥30)
  - T3 拉回
  - T4 反彈
各別的勝率 / 均報 / RR (30 天持有)

✅ 如果 ②③ 的 T4 / T3 RR 顯著高於 ④ → 應該「BSREBOUND 加碼」而非「BSGUARD 阻擋」
❌ 如果 ① 的 T1/T3 RR 顯著低 → BSGUARD 確實該阻 T1/T3
✅ 如果 ① 的 T4 RR 與 ④ 接近 → BSGUARD 應該保留 T4（之前砍光了）
"""
import sys, json
from pathlib import Path
import numpy as np
import pandas as pd
import ta
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

DATA = Path('data_cache')
UNIVERSE = sorted([p.stem for p in DATA.glob('*.parquet')
                   if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                   and not p.stem.startswith('00')])
HOLD = 30


def load_bs_phases():
    """讀 black_swans.json，把每天打上四個階段標籤"""
    with open('black_swans.json', encoding='utf-8') as f:
        d = json.load(f)
    events = d['events']

    # 每個事件的 trigger end 日（pd.Timestamp）
    ev_periods = []  # list of (start, end) Timestamp
    for e in events:
        ev_periods.append((pd.Timestamp(e['start']), pd.Timestamp(e['end'])))

    def phase_for(ts):
        """傳入 Timestamp，回傳 'IN' / 'POST10' / 'POST30' / 'NORMAL'"""
        # 統一去除 tz（避免 tz-aware vs naive 比較）
        if hasattr(ts, 'tz') and ts.tz is not None:
            ts = ts.tz_localize(None)
        for s, e in ev_periods:
            # 在事件中 (s ~ e + 10 BD)
            in_end = e + pd.tseries.offsets.BDay(10)
            post10_end = e + pd.tseries.offsets.BDay(20)
            post30_end = e + pd.tseries.offsets.BDay(40)
            if s <= ts <= in_end:
                return 'IN'
            if in_end < ts <= post10_end:
                return 'POST10'
            if post10_end < ts <= post30_end:
                return 'POST30'
        return 'NORMAL'
    return phase_for


def analyze_one(ticker):
    df = dl.load_from_cache(ticker)
    if df is None or len(df) < 280: return None
    test_df = df[df.index >= '2020-01-01'].copy()
    if len(test_df) < 50: return None

    test_df['e5'] = ta.trend.ema_indicator(test_df['Close'], window=5)
    e5 = test_df['e5'].values
    e20 = test_df['e20'].values
    e60 = test_df['e60'].values
    rsi = test_df['rsi'].values
    adx = test_df['adx'].values
    close = test_df['Close'].values
    n = len(test_df)
    dates = test_df.index

    phase_for = load_bs_phases()

    # 計算每天 cross_days
    cross_days = np.full(n, np.nan)
    for i in range(1, n):
        if not np.isnan(e20[i]) and not np.isnan(e60[i]):
            cur_bull = e20[i] > e60[i]
            for k in range(1, min(60, i)):
                if np.isnan(e20[i-k]) or np.isnan(e60[i-k]): continue
                prev_bull = e20[i-k] > e60[i-k]
                if prev_bull != cur_bull:
                    cross_days[i] = k if cur_bull else -k
                    break

    # 訊號 → 階段 → 報酬
    # bucket: { (signal, phase): [ret, ...] }
    bucket = {}

    for i in range(5, n - HOLD):
        if any(np.isnan(x) for x in [e5[i], e20[i], e60[i], rsi[i], adx[i]]): continue

        is_bull = e20[i] > e60[i]
        cd = cross_days[i]
        ret = (close[i + HOLD] - close[i]) / close[i] * 100
        phase = phase_for(dates[i])

        # T4 反彈：空頭 + RSI<32 + 連 2 天上升
        if not is_bull:
            if i >= 2 and rsi[i] < 32 and rsi[i] > rsi[i-1] > rsi[i-2]:
                bucket.setdefault(('T4', phase), []).append(ret)
            continue

        # 多頭以下
        if adx[i] < 22: continue
        is_strong = adx[i] >= 30
        is_fresh = not np.isnan(cd) and 0 < cd <= 10
        is_pullback = 35 <= rsi[i] < 50

        if is_fresh and is_strong:
            bucket.setdefault(('飆股', phase), []).append(ret)
        elif is_fresh:
            bucket.setdefault(('T1', phase), []).append(ret)

        if is_pullback:
            bucket.setdefault(('T3', phase), []).append(ret)

    return bucket


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    a = a[~np.isnan(a)]
    if len(a) == 0: return None
    return {'n': len(a), 'mean': a.mean(),
            'win': (a > 0).mean() * 100, 'worst': a.min(),
            'rr': a.mean()/abs(a.min()) if a.min() < 0 else 0}


def main():
    print(f"全市場 {len(UNIVERSE)} 檔 — 黑天鵝四階段 × 4 訊號 EDA\n")
    with ProcessPoolExecutor(max_workers=16) as ex:
        all_r = [r for r in ex.map(analyze_one, UNIVERSE) if r is not None]
    print(f"成功 {len(all_r)}/{len(UNIVERSE)}\n")

    # 合併
    merged = {}
    for r in all_r:
        for k, v in r.items():
            merged.setdefault(k, []).extend(v)

    PHASES = [
        ('IN',     '① 危險窗內 (trigger ~ +10D)'),
        ('POST10', '② 剛結束 +10D (黃金反彈?)'),
        ('POST30', '③ 剛結束 +30D (延伸反彈?)'),
        ('NORMAL', '④ 正常期 (距事件 ≥30 BD)'),
    ]
    SIGNALS = ['飆股', 'T1', 'T3', 'T4']

    print("=" * 100)
    print("📊 各訊號 × 各黑天鵝階段（30 天持有報酬）")
    print("=" * 100)
    print(f"{'階段':<32} {'訊號':<7} {'樣本':>7} {'勝率%':>8} {'均報%':>9} {'最差%':>9} {'RR':>7}")
    print("-" * 100)

    for ph, ph_label in PHASES:
        for sig in SIGNALS:
            m = metrics(merged.get((sig, ph), []))
            if m:
                print(f"{ph_label:<32} {sig:<7} {m['n']:>7} {m['win']:>+8.1f} "
                      f"{m['mean']:>+9.2f} {m['worst']:>+9.1f} {m['rr']:>7.3f}")
        print()

    # 每訊號跨階段對比
    print("\n" + "=" * 100)
    print("🔬 每訊號跨 4 階段 RR 對比")
    print("=" * 100)
    print(f"{'訊號':<7} {'IN(危險)':>10} {'POST10':>10} {'POST30':>10} {'NORMAL':>10} "
          f"  {'最佳階段':<12}")
    print("-" * 100)
    for sig in SIGNALS:
        rr = {}
        for ph, _ in PHASES:
            m = metrics(merged.get((sig, ph), []))
            rr[ph] = m['rr'] if m else None
        best_ph = max((p for p in rr if rr[p] is not None), key=lambda p: rr[p],
                      default=None)
        row = [sig]
        for ph, _ in PHASES:
            v = rr[ph]
            row.append(f"{v:>10.3f}" if v is not None else f"{'—':>10}")
        row.append(f"  {best_ph}" if best_ph else "")
        print(' '.join(str(x) for x in row))

    # 結論觸發
    print("\n" + "=" * 100)
    print("💡 結論與下一步")
    print("=" * 100)
    for sig in SIGNALS:
        in_m  = metrics(merged.get((sig, 'IN'), []))
        p10_m = metrics(merged.get((sig, 'POST10'), []))
        p30_m = metrics(merged.get((sig, 'POST30'), []))
        nm_m  = metrics(merged.get((sig, 'NORMAL'), []))
        if not nm_m: continue
        nm_rr = nm_m['rr']

        msgs = []
        if in_m and in_m['rr'] < nm_rr - 0.05:
            msgs.append(f"① 危險窗內 RR {in_m['rr']:.3f} 顯著低於 NORMAL {nm_rr:.3f} → 可能該阻擋")
        elif in_m:
            msgs.append(f"① 危險窗內 RR {in_m['rr']:.3f} ≈ NORMAL → 不該阻擋")

        if p10_m and p10_m['rr'] > nm_rr + 0.05:
            msgs.append(f"② POST10 RR {p10_m['rr']:.3f} 顯著高於 NORMAL → 可能該加碼")
        elif p30_m and p30_m['rr'] > nm_rr + 0.05:
            msgs.append(f"③ POST30 RR {p30_m['rr']:.3f} 顯著高於 NORMAL → 可能該加碼")

        if msgs:
            print(f"\n  【{sig}】")
            for m in msgs: print(f"    {m}")

    # 寫入 JSON 給後續分析用
    out = {}
    for (sig, ph), arr in merged.items():
        m = metrics(arr)
        if m:
            out[f'{sig}_{ph}'] = m
    with open('blackswan_eda.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 blackswan_eda.json")


if __name__ == '__main__':
    main()
