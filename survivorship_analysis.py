"""
E3 Survivorship Bias 影響估計

問題：
  tw_stock_list.json 是「2026-04 仍在上市」的股票清單
  2020-2025 期間下市/被收購的股票完全沒納入
  這些往往是表現最差的，把它們排除會高估策略均值

策略：
  1. 嘗試取得部分已知下市股的歷史資料（透過 yfinance）
  2. 同時用「BH 表現最差的存活股」當代理（proxy）
  3. 估算若加入下市股，全市場均值會如何變化

已知下市/合併台股範例（2020-2025）：
  2402 毅嘉（2024 終止上市）
  6164 業旺（2021 下市）
  4137 麗豐 KY（2023 終止）
  4147 開立（2022 下市）
  ... 等
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import warnings; warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import yfinance as yf

import data_loader as dl
import variant_strategy as vs


# 已知 2020-2025 部分下市/合併的台股（需逐一確認 yfinance 是否仍有資料）
DELISTED_CANDIDATES = [
    '2402',   # 毅嘉
    '6164',   # 業旺
    '4137',   # 麗豐-KY
    '4147',   # 開立
    '5485',   # 治盛 (2023)
    '8479',   # 富堡（2024 終止）
    '6446',   # 藥華藥（仍上市，跳過）
    '1611',   # 中電
    '5439',   # 高技
    '8401',   # 白蘭氏（已下市）
]


def try_fetch_delisted(ticker):
    """嘗試取得下市股歷史資料"""
    try:
        df = dl.fetch_one(ticker)
        if df is None or df.empty:
            return None
        # 只保留 2020-2026 期間
        df = df[df.index >= '2019-04-01']
        if len(df) < 60:
            return None
        return df
    except Exception:
        return None


def estimate_proxy_effect():
    """用「存活股中表現最差」估算下市股的影響"""
    print("━━━━━━━━━━ E3 Survivorship Bias 影響估計 ━━━━━━━━━━\n")

    # 載入 P0_T1T3+CB30 結果（推薦配置）
    try:
        with open('mc_baseline.csv', encoding='utf-8-sig') as f:
            rows = list(csv.DictReader(f))
    except FileNotFoundError:
        # 回退到 results_base.csv
        try:
            with open('results_base.csv', encoding='utf-8-sig') as f:
                rows = list(csv.DictReader(f))
        except:
            print("找不到結果 CSV，請先跑 v8_runner")
            return

    valid = []
    for r in rows:
        try:
            valid.append((r['ticker'], float(r['pnl_pct']), float(r['bh_pct'] or 0)))
        except: pass

    pnls = np.array([v[1] for v in valid])
    bhs  = np.array([v[2] for v in valid])

    print(f"當前樣本：{len(valid)} 檔（2026-04 仍上市的存活股）")
    print(f"  策略均值：{np.mean(pnls):+.2f}%")
    print(f"  BH 均值：  {np.mean(bhs):+.2f}%\n")

    # 場景估計：假設有 X% 額外下市股，每檔策略 PnL 為 -150% (悲觀) 至 -50% (溫和)
    print("━━━━━━━━━━ 假設場景 ━━━━━━━━━━")
    print(f"{'下市股佔比':<12} {'下市股PnL':<15} {'均值偏差':<10} {'校正後均值':<12}")
    print('-' * 55)
    for delisted_pct in [5, 10, 15, 20]:
        for delisted_pnl in [-50, -100, -150]:
            # 模擬：增加 N 檔下市股
            n_delisted = int(len(valid) * delisted_pct / 100)
            extended = list(pnls) + [delisted_pnl] * n_delisted
            adj_mean = np.mean(extended)
            bias = adj_mean - np.mean(pnls)
            print(f"  {delisted_pct:>2}%        {delisted_pnl:>+5}%          "
                  f"{bias:>+8.2f}%  {adj_mean:>+9.2f}%")

    print()

    # 用存活股中最差的 N 檔估計
    sorted_pnls = sorted(pnls)
    print("━━━━━━━━━━ 存活股最差 50 檔（下市股代理）━━━━━━━━━━")
    worst50 = sorted_pnls[:50]
    print(f"  最差 50 檔均值：{np.mean(worst50):+.2f}%")
    print(f"  最差 50 檔範圍：{worst50[0]:+.0f}% ~ {worst50[49]:+.0f}%")
    print()

    # 假設 10% 下市股，績效類似「最差 N 檔的均值」
    proxy_mean = np.mean(worst50)
    for delisted_pct in [5, 10, 15]:
        n_delisted = int(len(valid) * delisted_pct / 100)
        extended = list(pnls) + [proxy_mean] * n_delisted
        adj_mean = np.mean(extended)
        bias = adj_mean - np.mean(pnls)
        print(f"  若有 {delisted_pct}% 下市股，PnL 同最差50均值 ({proxy_mean:+.0f}%) → "
              f"校正後 {adj_mean:+.2f}% (偏差 {bias:+.2f})")


def try_real_delisted():
    """嘗試實際取得下市股歷史資料"""
    print("\n━━━━━━━━━━ 實際下市股資料取得測試 ━━━━━━━━━━")
    print(f"嘗試 {len(DELISTED_CANDIDATES)} 個候選代號...\n")

    success = []
    for tk in DELISTED_CANDIDATES:
        df = try_fetch_delisted(tk)
        if df is not None:
            last_date = df.index[-1].strftime('%Y-%m-%d')
            print(f"  ✅ {tk}: {len(df)} 個交易日，最後日期 {last_date}")
            success.append((tk, df))
        else:
            print(f"  ❌ {tk}: 無資料")

    if not success:
        print("\n  yfinance 對下市股普遍無歷史資料，使用代理估計法")
        return

    print(f"\n成功取得 {len(success)} 檔下市股，跑 P0_T1T3+CB30 策略...")
    pnls = []
    for tk, df in success:
        df_ind = df  # 已包含指標（fetch_one 內已 calc_ind）
        result = vs.run_v7_variant(tk, df_ind, mode='P0_T1T3+CB30')
        if result:
            pnls.append((tk, result['pnl_pct'], result['bh_pct']))
            print(f"  {tk}: BH={result['bh_pct']:+.0f}%  策略={result['pnl_pct']:+.0f}%")

    if pnls:
        avg = np.mean([p[1] for p in pnls])
        print(f"\n  下市股策略平均：{avg:+.2f}%")


if __name__ == '__main__':
    estimate_proxy_effect()
    try_real_delisted()
