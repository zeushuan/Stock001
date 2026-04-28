"""T3 進場 RSI 門檻比較
=========================
比較不同 RSI 拉回深度的績效：
  RSI < 35   深度拉回（更保守）
  RSI < 40   中深拉回
  RSI < 45   中淺拉回
  RSI < 50   淺拉回（v8 標準）
"""
import sys, json
from pathlib import Path
import numpy as np
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

TOP200 = sorted([t for t, info in
                 json.load(open('vwap_applicable.json', encoding='utf-8')).items()
                 if info.get('tier') == 'TOP'])

THRESHOLDS = [35, 40, 45, 50]
HOLD_DAYS = [10, 20, 30, 60]


def analyze_one(ticker):
    df = dl.load_from_cache(ticker)
    if df is None or len(df) < 280: return None
    test_df = df[df.index >= '2024-06-01']
    if len(test_df) < 30: return None

    e20 = test_df['e20'].values
    e60 = test_df['e60'].values
    rsi = test_df['rsi'].values
    adx = test_df['adx'].values
    close = test_df['Close'].values
    n = len(test_df)

    # 不同 RSI 上限門檻的進場結果
    results = {}
    for th in THRESHOLDS:
        results[th] = {h: [] for h in HOLD_DAYS}

    for i in range(n - max(HOLD_DAYS)):
        if any(np.isnan(x) for x in [e20[i], e60[i], rsi[i], adx[i]]): continue
        is_bull = e20[i] > e60[i]
        if not is_bull: continue
        if adx[i] < 22: continue

        # RSI 必須 ≥ 25（避免空頭過深）
        if rsi[i] < 25: continue

        entry_price = close[i]
        for th in THRESHOLDS:
            # 「進場條件」：RSI < th 且 ≥ 25
            if rsi[i] < th and rsi[i] >= 25:
                for h in HOLD_DAYS:
                    if i + h < n:
                        ret = (close[i+h] - entry_price) / entry_price * 100
                        results[th][h].append(ret)

    return results


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    return {
        'n': len(a),
        'mean': a.mean(),
        'median': np.median(a),
        'win': (a > 0).mean() * 100,
        'worst': a.min(),
        'best': a.max(),
        'rr': a.mean() / abs(a.min()) if a.min() < 0 else 0,
    }


def main():
    print(f"分析 TOP 200 在 TEST 期 (2024.6-2026.4) 不同 RSI 進場門檻...\n")

    with ProcessPoolExecutor(max_workers=12) as ex:
        all_results = [r for r in ex.map(analyze_one, TOP200) if r is not None]

    print(f"成功 {len(all_results)}/{len(TOP200)}\n")

    # 彙總
    print("=" * 100)
    print("📊 T3 進場 RSI 門檻比較（多頭+ADX≥22）")
    print("=" * 100)
    print(f"{'RSI 門檻':<12} {'持有':<6} {'樣本':>7} {'勝率%':>8} {'均值%':>9} "
          f"{'中位%':>9} {'最差%':>9} {'最佳%':>9} {'RR':>7}")
    print("-" * 100)

    for th in THRESHOLDS:
        for h in HOLD_DAYS:
            all_data = []
            for r in all_results:
                all_data.extend(r[th][h])
            m = metrics(all_data)
            if m:
                label = f"RSI < {th}"
                print(f"{label:<12} {f'{h} 天':<6} {m['n']:>7} "
                      f"{m['win']:>+8.1f} {m['mean']:>+9.2f} {m['median']:>+9.2f} "
                      f"{m['worst']:>+9.1f} {m['best']:>+9.1f} {m['rr']:>7.3f}")
        print()

    # 重點對比
    print("=" * 100)
    print("📌 重點對比 — 30 天持有")
    print("=" * 100)
    h = 30
    rows = []
    for th in THRESHOLDS:
        all_data = [x for r in all_results for x in r[th][h]]
        m = metrics(all_data)
        if m:
            rows.append((th, m))
    print(f"{'門檻':<10} {'樣本':>7} {'勝率%':>8} {'均值%':>9} {'最差%':>9} {'RR':>7}  趨勢")
    print("-" * 80)
    for i, (th, m) in enumerate(rows):
        if i == 0:
            trend = "（深度拉回，少見）"
        elif i == len(rows) - 1:
            trend = "（淺拉回，最常見）"
        else:
            trend = ""
        print(f"RSI < {th:<6} {m['n']:>7} {m['win']:>+8.1f} {m['mean']:>+9.2f} "
              f"{m['worst']:>+9.1f} {m['rr']:>7.3f}  {trend}")

    # 結論
    print("\n" + "=" * 100)
    print("結論：勝率 vs 風險的取捨")
    print("=" * 100)
    if len(rows) >= 2:
        rsi35 = next((m for th, m in rows if th == 35), None)
        rsi50 = next((m for th, m in rows if th == 50), None)
        if rsi35 and rsi50:
            print(f"\n30 天持有對比：")
            print(f"  RSI < 35 (深度拉回): 勝率 {rsi35['win']:.1f}%, 均報 "
                  f"{rsi35['mean']:+.2f}%, 樣本 {rsi35['n']}, RR {rsi35['rr']:.3f}")
            print(f"  RSI < 50 (淺拉回 v8 標準): 勝率 {rsi50['win']:.1f}%, 均報 "
                  f"{rsi50['mean']:+.2f}%, 樣本 {rsi50['n']}, RR {rsi50['rr']:.3f}")
            print(f"\n  差距：勝率 {rsi35['win']-rsi50['win']:+.1f}pp, "
                  f"報酬 {rsi35['mean']-rsi50['mean']:+.2f}pp, "
                  f"RR {rsi35['rr']-rsi50['rr']:+.3f}")


if __name__ == '__main__':
    main()
