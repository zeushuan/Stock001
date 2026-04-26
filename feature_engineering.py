"""
特徵工程：計算每支股票的行為特徵，供分群/預測分析使用

特徵清單：
  bh_pct          BH 報酬
  v7_pct          v7 base 報酬
  p0_pct          P0_T1T3 報酬
  cb30_pct        P0+CB30 報酬
  improved_p0     v7→P0 改善幅度
  saved_cb30      P0→CB30 救回幅度

  volatility_d    日線收益率年化波動率
  max_dd_pct      最大回撤 %
  bull_days_pct   EMA20>EMA60 天數佔比
  trend_score     簡單趨勢分（線性回歸斜率歸一化）
  avg_atr_pct     ATR/Price 平均
  rsi_avg         RSI 平均
  rsi_volatility  RSI 標準差
  adx_avg         ADX 平均

  positive_year_ratio  年度正報酬年數 / 總年數
  worst_drawdown_yr    單年最大回撤年份
  pyramids_added       P0 比 v7 多的交易筆數

輸出：
  features.csv     所有股票完整特徵表（供後續分析使用）
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import csv
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

import data_loader as dl


def compute_features(ticker: str, file_path: str) -> dict:
    """單支股票特徵計算"""
    try:
        df = pd.read_parquet(file_path)
    except: return None

    # 套用回測期間
    df = df[(df.index >= '2020-01-02') & (df.index <= '2026-04-25')]
    df = df[df['Close'].notna()]
    if len(df) < 100: return None

    pr   = df['Close'].values
    e20  = df['e20'].values
    e60  = df['e60'].values
    rsi  = df['rsi'].values
    adx  = df['adx'].values
    atr  = df['atr'].values

    # 每日收益率
    daily_ret = np.diff(np.log(pr + 1e-9))
    volatility_d = float(np.std(daily_ret) * np.sqrt(252) * 100)  # 年化波動率%

    # 最大回撤
    cum_max = np.maximum.accumulate(pr)
    dd = (pr - cum_max) / cum_max
    max_dd_pct = float(np.min(dd) * 100)

    # 多頭天數比例
    bull_mask = (e20 > e60) & ~np.isnan(e20) & ~np.isnan(e60)
    bull_days_pct = float(np.sum(bull_mask) / len(pr) * 100)

    # 趨勢分（log price 線性斜率歸一化）
    n = len(pr)
    log_pr = np.log(pr + 1e-9)
    x = np.arange(n)
    if np.std(log_pr) > 0:
        slope = np.polyfit(x, log_pr, 1)[0]
        # 歸一化：每年增長率
        trend_score = float(slope * 252 * 100)  # 年化%
    else:
        trend_score = 0.0

    # 各指標平均
    rsi_avg = float(np.nanmean(rsi))
    rsi_volatility = float(np.nanstd(rsi))
    adx_avg = float(np.nanmean(adx))
    avg_atr_pct = float(np.nanmean(np.where(pr > 0, atr / pr * 100, np.nan)))

    # 年度報酬統計
    df_yearly = df['Close'].resample('YE').last()
    yearly_ret = df_yearly.pct_change().dropna()
    if len(yearly_ret) > 0:
        positive_year_ratio = float((yearly_ret > 0).sum() / len(yearly_ret))
        worst_year_ret = float(yearly_ret.min())
    else:
        positive_year_ratio = 0.5
        worst_year_ret = 0.0

    return dict(
        ticker=ticker,
        n_days=len(pr),
        first_date=df.index[0].strftime('%Y-%m-%d'),
        last_date=df.index[-1].strftime('%Y-%m-%d'),
        volatility_d=round(volatility_d, 2),
        max_dd_pct=round(max_dd_pct, 2),
        bull_days_pct=round(bull_days_pct, 2),
        trend_score=round(trend_score, 2),
        rsi_avg=round(rsi_avg, 2),
        rsi_volatility=round(rsi_volatility, 2),
        adx_avg=round(adx_avg, 2),
        avg_atr_pct=round(avg_atr_pct, 2),
        positive_year_ratio=round(positive_year_ratio, 2),
        worst_year_ret=round(worst_year_ret * 100, 2),
    )


def main():
    print("━━━━━━ 特徵工程：1263 檔股票行為特徵計算 ━━━━━━\n")

    # 載入股票清單
    from v8_runner import load_tickers
    tickers = load_tickers()
    print(f"總股票數：{len(tickers)}")

    # 平行計算
    print("\n[並行計算] 12 workers...")
    import time
    t0 = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(compute_features, tk, str(dl.cache_path(tk))): tk
                   for tk in tickers}
        for fut in as_completed(futures):
            try:
                r = fut.result(timeout=10)
                if r is not None:
                    results.append(r)
            except: pass
    print(f"完成：{len(results)} 檔 / {time.time()-t0:.1f}s\n")

    # 載入策略結果合併
    print("[合併] 載入 base / P0_T1T3 / CB30 結果...")
    def load(p):
        d = {}
        for r in csv.DictReader(open(p, encoding='utf-8-sig')):
            try:
                d[r['ticker']] = (float(r['pnl_pct']), int(r['n_trades']),
                                  float(r['bh_pct'] or 0))
            except: pass
        return d

    base = load('results_base.csv')
    p0   = load('results_P0_T1T3.csv')
    cb30 = load('results_P0_T1T3+CB30.csv')

    for r in results:
        tk = r['ticker']
        if tk in base:
            r['bh_pct'] = base[tk][2]
            r['v7_pct'] = base[tk][0]
            r['v7_trades'] = base[tk][1]
        if tk in p0:
            r['p0_pct'] = p0[tk][0]
            r['p0_trades'] = p0[tk][1]
            r['pyramids_added'] = p0[tk][1] - base.get(tk, (0, 0, 0))[1]
        if tk in cb30:
            r['cb30_pct'] = cb30[tk][0]
        r['improved_p0']  = r.get('p0_pct', 0) - r.get('v7_pct', 0)
        r['saved_cb30']   = r.get('cb30_pct', 0) - r.get('p0_pct', 0)
        # 改善類別
        if r['improved_p0'] > 5:    r['cat'] = 'improved'
        elif r['improved_p0'] < -5: r['cat'] = 'regressed'
        else:                       r['cat'] = 'unchanged'

    # 寫出
    output = 'features.csv'
    fields = list(results[0].keys())
    # 確保所有 dict 有相同 key
    all_keys = set()
    for r in results: all_keys.update(r.keys())
    fields = sorted(all_keys, key=lambda k: (k != 'ticker', k))

    with open(output, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        for r in results:
            # 補空缺欄位
            for k in fields:
                r.setdefault(k, '')
            w.writerow(r)
    print(f"✅ 特徵已寫入：{output}（{len(results)} 檔，{len(fields)} 個特徵）")

    # 簡易摘要
    print("\n━━━ 特徵分布簡述 ━━━")
    valid = [r for r in results if 'p0_pct' in r]
    for col in ['volatility_d', 'max_dd_pct', 'bull_days_pct', 'trend_score',
                'rsi_avg', 'adx_avg', 'avg_atr_pct', 'positive_year_ratio']:
        vals = [r[col] for r in valid if col in r]
        if vals:
            arr = np.array(vals)
            print(f"  {col:<22} 均值={np.mean(arr):.2f}  中位={np.median(arr):.2f}"
                  f"  範圍=[{np.min(arr):.2f}, {np.max(arr):.2f}]")


if __name__ == '__main__':
    main()
