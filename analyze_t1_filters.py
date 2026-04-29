"""T1 + 附加條件 × 勝率分析
==================================
基準：T1 = cross_days 1-10 + ADX 達標 + 多頭

加入 12 個附加條件，看哪個組合勝率/RR 最佳：
  ADX 強度     ADX≥25 / ≥30 (飆股)
  RSI 區間     不熱 (50-65) / 拉回 (30-50) / 偏熱 (60-70)
  EMA5 確認    close>EMA5 / EMA5>EMA20 / EMA20上升
  距離 EMA60   <1.5 ATR (不延伸) / >0.5 ATR (有 buffer)
  波動率       ATR/P <5% (低波動)
  大盤過濾     大盤多頭 (TWII/SPX 自身多頭)
  不接刀       從 60d 高點未跌 >15%
  RSI 動量     RSI 上升中
  時機         Day 1-7 (前期) / Day 5-10 (後期)
  ADX 動量     ADX 上升中
"""
import sys, time, json
from pathlib import Path
import numpy as np
import pandas as pd
import ta
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16
HOLD = 30
MIN_ADV = 104_000_000

US_ETF_EXCLUDE = {  # 簡化版
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
}


def analyze_one(args):
    """對單檔股票，找所有 T1 進場（cross_days 1-10），按各種條件分層"""
    ticker, market, market_close = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        idx = df.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx = idx.tz_localize(None)
        df = df[idx >= pd.Timestamp('2020-01-01')]
        if len(df) < 280: return (ticker, None)

        e20 = df['e20'].values
        e60 = df['e60'].values
        rsi = df['rsi'].values if 'rsi' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        atr = df['atr'].values if 'atr' in df.columns else None
        close = df['Close'].values
        n = len(df)

        # 計算 EMA5
        e5 = ta.trend.ema_indicator(df['Close'], window=5).values

        # ADX 門檻
        adx_th = 18 if market == 'us' else 22

        # 大盤趨勢資料對齊
        market_bull = None
        if market_close is not None:
            mc = market_close.reindex(df.index, method='ffill').values
            mc_ma = pd.Series(mc).rolling(20).mean().values
            mc_ma60 = pd.Series(mc).rolling(60).mean().values
            market_bull = (mc_ma > mc_ma60)

        # 條件組
        results = {}

        for i in range(60, n - HOLD):
            if rsi is None or adx is None or atr is None: continue
            if any(np.isnan(x) for x in [e20[i], e60[i], rsi[i], adx[i], atr[i]]): continue
            if e20[i] <= e60[i]: continue
            if adx[i] < adx_th: continue

            # T1: 找最近黃金交叉 1-10 天內
            cd = None
            for k in range(1, min(15, i)):
                if np.isnan(e20[i-k]) or np.isnan(e60[i-k]): continue
                if e20[i-k] <= e60[i-k]:
                    cd = k; break
            if cd is None or cd > 10: continue   # T1 = 1-10 天

            ret = (close[i + HOLD] - close[i]) / close[i] * 100

            # 附加條件
            close_v = close[i]
            adx_v = adx[i]
            rsi_v = rsi[i]
            atr_v = atr[i]
            e5_v = e5[i] if not np.isnan(e5[i]) else None
            e20_v = e20[i]
            e60_v = e60[i]

            # 距 EMA60 ATR 倍數
            ema60_atr_dist = (close_v - e60_v) / atr_v if atr_v > 0 else 0
            # ATR/P 波動率
            atr_pct = atr_v / close_v * 100 if close_v > 0 else 0
            # EMA20 5d 上升
            e20_5d = e20[i-5] if i >= 5 and not np.isnan(e20[i-5]) else None
            e20_up = (e20_5d is not None and e20_v > e20_5d)
            # ADX 上升
            adx_up = (i >= 5 and not np.isnan(adx[i-5]) and adx_v > adx[i-5])
            # RSI 上升
            rsi_up = (i >= 1 and not np.isnan(rsi[i-1]) and rsi_v > rsi[i-1])
            # EMA5 條件
            close_above_e5 = (e5_v is not None and close_v > e5_v)
            e5_above_e20 = (e5_v is not None and e5_v > e20_v)
            # 60d 高點
            high60 = close[max(0, i-60):i].max() if i >= 60 else close_v
            from_high = (high60 - close_v) / high60 * 100 if high60 > 0 else 0

            # ── 各條件分層 ──
            results.setdefault('A0_baseline_T1', []).append(ret)

            # ADX 強度
            if adx_v >= 25: results.setdefault('B1_ADX25+', []).append(ret)
            if adx_v >= 30: results.setdefault('B2_ADX30+_飆股', []).append(ret)
            if adx_v < 25: results.setdefault('B3_ADX<25', []).append(ret)

            # RSI 區間
            if 30 <= rsi_v < 50: results.setdefault('C1_RSI拉回(30-50)', []).append(ret)
            if 50 <= rsi_v < 60: results.setdefault('C2_RSI中性(50-60)', []).append(ret)
            if 60 <= rsi_v < 70: results.setdefault('C3_RSI偏熱(60-70)', []).append(ret)
            if rsi_v >= 70: results.setdefault('C4_RSI過熱(≥70)', []).append(ret)

            # EMA 結構
            if close_above_e5: results.setdefault('D1_close>EMA5', []).append(ret)
            if e5_above_e20: results.setdefault('D2_EMA5>EMA20', []).append(ret)
            if e20_up: results.setdefault('D3_EMA20上升', []).append(ret)

            # 距 EMA60
            if 0 < ema60_atr_dist < 1.5:
                results.setdefault('E1_距EMA60<1.5ATR(不延伸)', []).append(ret)
            if ema60_atr_dist > 0.5:
                results.setdefault('E2_距EMA60>0.5ATR(有buffer)', []).append(ret)
            if ema60_atr_dist > 3.0:
                results.setdefault('E3_距EMA60>3ATR(過度延伸)', []).append(ret)

            # 波動率
            if atr_pct < 3:
                results.setdefault('F1_低波動(ATR<3%)', []).append(ret)
            if 3 <= atr_pct < 5:
                results.setdefault('F2_中波動(ATR3-5%)', []).append(ret)
            if atr_pct >= 5:
                results.setdefault('F3_高波動(ATR≥5%)', []).append(ret)

            # 大盤
            if market_bull is not None and i < len(market_bull):
                if market_bull[i]:
                    results.setdefault('G1_大盤多頭', []).append(ret)
                else:
                    results.setdefault('G2_大盤空頭', []).append(ret)

            # 不接刀
            if from_high < 5:
                results.setdefault('H1_接近60d高點(<5%)', []).append(ret)
            if from_high >= 15:
                results.setdefault('H2_從高點跌≥15%(疑接刀)', []).append(ret)

            # 動量
            if rsi_up: results.setdefault('I1_RSI上升中', []).append(ret)
            if adx_up: results.setdefault('I2_ADX上升中', []).append(ret)

            # 時機（Day 1-7 / 5-10）
            if 1 <= cd <= 5: results.setdefault('J1_Day1-5(早期)', []).append(ret)
            if 6 <= cd <= 10: results.setdefault('J2_Day6-10(後期)', []).append(ret)

            # ★ 組合：高勝率候選
            if adx_v >= 25 and rsi_v < 60 and e5_above_e20:
                results.setdefault('★_ADX25+RSI<60+EMA5>20', []).append(ret)
            if adx_v >= 30 and 50 <= rsi_v < 70 and 0 < ema60_atr_dist < 2:
                results.setdefault('★_ADX30+RSI50-70+不延伸', []).append(ret)
            if 1 <= cd <= 5 and adx_v >= 25 and e20_up:
                results.setdefault('★_早鳥+ADX25+EMA20升', []).append(ret)
            if rsi_v < 60 and from_high < 5:
                results.setdefault('★_RSI<60+接近高點', []).append(ret)

        return (ticker, results)
    except Exception:
        return (ticker, None)


def metrics(arr):
    if not arr: return None
    a = np.array(arr)
    a = a[~np.isnan(a)]
    if len(a) == 0: return None
    return {
        'n': len(a), 'mean': float(a.mean()),
        'median': float(np.median(a)),
        'win': float((a > 0).mean() * 100),
        'worst': float(a.min()),
        'rr': float(a.mean() / abs(a.min())) if a.min() < 0 else 0,
    }


def main():
    DATA = Path('data_cache')
    tw_universe = sorted([p.stem for p in DATA.glob('*.parquet')
                          if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                          and not p.stem.startswith('00')])
    vwap_set = set(p.stem for p in Path('vwap_cache').glob('*.parquet'))
    tw_universe = [t for t in tw_universe if t in vwap_set]

    us_full = json.loads(Path('us_full_tickers.json').read_text(encoding='utf-8'))
    us_high_liquid = []
    for t in sorted(us_full['tickers']):
        if t in US_ETF_EXCLUDE: continue
        if not (DATA / f'{t}.parquet').exists(): continue
        try:
            df = dl.load_from_cache(t)
            if df is None or len(df) < 60: continue
            adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
            if adv >= MIN_ADV: us_high_liquid.append(t)
        except: pass

    print(f"🇹🇼 TW universe: {len(tw_universe)} 檔")
    print(f"🇺🇸 US 高流動: {len(us_high_liquid)} 檔\n")

    # 大盤資料
    twii_df = dl.load_from_cache('^TWII')
    twii_close = twii_df['Close'] if twii_df is not None else None
    spx_df = dl.load_from_cache('^GSPC')
    spx_close = spx_df['Close'] if spx_df is not None else None

    def run_market(universe, market, market_close):
        print(f"\n📊 跑 {market.upper()} T1 + 附加條件分析...")
        t0 = time.time()
        agg = {}
        tasks = [(t, market, market_close) for t in universe]
        with ProcessPoolExecutor(max_workers=WORKERS) as ex:
            for ticker, r in ex.map(analyze_one, tasks, chunksize=80):
                if r is not None:
                    for k, v in r.items():
                        agg.setdefault(k, []).extend(v)
        print(f"  完成 {time.time()-t0:.1f}s")
        return agg

    tw_agg = run_market(tw_universe, 'tw', twii_close)
    us_agg = run_market(us_high_liquid, 'us', spx_close)

    # 報告（每市場）
    for market_name, agg in [('🇹🇼 TW', tw_agg), ('🇺🇸 US 高流動', us_agg)]:
        print(f"\n" + "=" * 110)
        print(f"📊 {market_name} — T1 + 附加條件 × 30 天勝率")
        print("=" * 110)
        # 計算 metrics
        rows = []
        baseline_rr = None
        baseline_win = None
        for k in sorted(agg.keys()):
            m = metrics(agg[k])
            if m:
                rows.append((k, m))
                if k == 'A0_baseline_T1':
                    baseline_rr = m['rr']
                    baseline_win = m['win']

        # 按 RR 排序
        rows.sort(key=lambda x: -x[1]['rr'])

        print(f"{'條件':<32} {'樣本':>8} {'勝率%':>8} {'均報%':>9} "
              f"{'中位%':>8} {'RR':>7}  {'Δ_RR':>7} 視覺化")
        print("-" * 110)
        for k, m in rows:
            d_rr = m['rr'] - baseline_rr if baseline_rr is not None else 0
            d_win = m['win'] - baseline_win if baseline_win is not None else 0
            bar = '█' * max(0, int(m['rr'] * 30))
            marker = ''
            if k == 'A0_baseline_T1': marker = ' ⭐ 基準'
            elif d_rr > 0.02: marker = ' 🔥'
            elif d_rr > 0.01: marker = ' ✓'
            elif d_rr < -0.02: marker = ' ✗'
            print(f"{k:<32} {m['n']:>8} {m['win']:>+8.1f} "
                  f"{m['mean']:>+9.2f} {m['median']:>+8.2f} {m['rr']:>7.3f} "
                  f"{d_rr:>+7.3f} {bar[:20]}{marker}")

    # 寫 JSON
    out = {
        'tw_metrics': {k: metrics(v) for k, v in tw_agg.items()},
        'us_metrics': {k: metrics(v) for k, v in us_agg.items()},
    }
    with open('t1_filters_winrate.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("\n💾 寫入 t1_filters_winrate.json")


if __name__ == '__main__':
    main()
