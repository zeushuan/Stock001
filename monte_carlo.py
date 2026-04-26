"""
E2 Monte Carlo 參數敏感度測試

目的：
  測試核心參數小幅變動對 P0_T1T3+CB30 績效的影響
  健壯策略 = 參數小變動，績效維持穩定
  脆弱策略 = 績效隨參數變動大幅起伏

測試的參數：
  ADX 進場門檻       18 / 20 / 22(default) / 24 / 26
  EMA120 過濾門檻   -1 / -2(default) / -3 / -5 (%)
  RSI T3 上限       45 / 48 / 50(default) / 52 / 55
  ATR 倍數 (低ADX)  2.0 / 2.5(default) / 3.0
  ATR 倍數 (高ADX)  2.5 / 3.0(default) / 3.5

判定：
  若各參數變動下 P0_T1T3+CB30 均值偏離 base 在 ±15% 以內 → 健壯
  若有參數讓 P0_T1T3 均值嚴重偏離（>±30%）→ 該參數脆弱，避免該值
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import subprocess
import time
from pathlib import Path

import numpy as np


BASE_MODE = 'P0_T1T3+CB30'  # 推薦的生產配置作為基準

# 參數測試矩陣
TESTS = [
    # (參數名, 參數附加, 描述)
    ('ADX',     [(20, 'ADX20'), (24, 'ADX24'), (26, 'ADX26')],   'ADX 進場門檻'),
    ('E120',    [(-1, 'E120-1'), (-3, 'E120-3'), (-5, 'E120-5')], 'EMA120 過濾門檻'),
    ('RSI',     [(45, 'RSI45'), (48, 'RSI48'), (52, 'RSI52'), (55, 'RSI55')],  'RSI T3 上限'),
    ('ATR_lo',  [(2.0, 'ATL2.0'), (3.0, 'ATL3.0')],              'ATR 倍數 (低ADX)'),
    ('ATR_hi',  [(2.5, 'ATH2.5'), (3.5, 'ATH3.5')],              'ATR 倍數 (高ADX)'),
]


def run_one(mode_suffix: str, label: str, workers: int = 4) -> str:
    """跑一個變體並回傳 CSV 路徑"""
    mode = f"{BASE_MODE}+{mode_suffix}" if mode_suffix else BASE_MODE
    output = f"mc_{label}.csv"
    cmd = [sys.executable, 'v8_runner.py',
           '--mode', mode,
           '--workers', str(workers),
           '--quiet',
           '--output', output]
    subprocess.run(cmd, check=True)
    return output


def load_mean(path: str) -> float:
    vals = []
    for r in csv.DictReader(open(path, encoding='utf-8-sig')):
        try:
            vals.append(float(r['pnl_pct']))
        except:
            pass
    return float(np.mean(vals)) if vals else float('nan')


def main():
    print("━━━━━━━━━━ E2 Monte Carlo 參數敏感度測試 ━━━━━━━━━━\n")
    print(f"基準配置：{BASE_MODE}")
    print(f"參數預設：ADX≥22, EMA120 60日跌≤2%, RSI<50, ATR×2.5/3.0\n")

    t0 = time.time()

    # 跑基準
    print(f"  [基準] {BASE_MODE} ...", flush=True)
    base_path = run_one('', 'baseline')
    base_mean = load_mean(base_path)
    print(f"  [基準] 完成：{base_mean:+.2f}%\n", flush=True)

    # 一一測試各參數
    all_results = []
    for param_name, variants, description in TESTS:
        print(f"━━━ {description} ━━━")
        results = [(0, 'default', base_mean)]  # 預設值
        for value, suffix in variants:
            label = f"{param_name}_{value}".replace('-', 'm').replace('.', '_')
            print(f"  [{suffix}] running...", flush=True)
            path = run_one(suffix, label)
            mean = load_mean(path)
            diff = mean - base_mean
            print(f"  [{suffix}] {mean:+.2f}%  vs base {diff:+.2f}", flush=True)
            results.append((value, suffix, mean))

        # 統計：均值範圍 / 標準差
        means = [m for _, _, m in results]
        sd = np.std(means)
        rng = max(means) - min(means)
        print(f"  → 範圍：{min(means):+.2f}% ~ {max(means):+.2f}%（差距 {rng:.2f}）  σ={sd:.2f}\n")
        all_results.append((description, results, sd, rng))

    # 整體判定
    print("━━━━━━━━━━ 健壯性報告 ━━━━━━━━━━")
    print(f"  基準（P0_T1T3+CB30 預設）：{base_mean:+.2f}%\n")
    print(f"  {'參數':<25} {'σ':>8} {'最大偏差':>10} {'判定':<15}")
    print(f"  {'-'*60}")
    fragile_count = 0
    for desc, results, sd, rng in all_results:
        max_dev = max(abs(m - base_mean) for _, _, m in results)
        if max_dev < 15:
            verdict = "✅ 健壯"
        elif max_dev < 30:
            verdict = "🟡 中度敏感"
        else:
            verdict = "❌ 脆弱"
            fragile_count += 1
        print(f"  {desc:<25} {sd:>+8.2f} {max_dev:>+9.2f}%  {verdict}")

    print()
    if fragile_count == 0:
        print("  ◆ 整體判定：✅ 策略對核心參數健壯，可信度高")
    elif fragile_count == 1:
        print(f"  ◆ 整體判定：🟡 1 個參數敏感，需謹慎使用該參數的特定值")
    else:
        print(f"  ◆ 整體判定：⚠️ {fragile_count} 個參數脆弱，可能存在過擬合風險")

    print(f"\n總執行時間：{time.time()-t0:.1f}s")


if __name__ == '__main__':
    main()
