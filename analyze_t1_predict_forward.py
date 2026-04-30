"""T1 預測 forward 分析（v9.11）
==================================
之前 analyze_t1_preentry.py 的偏誤：從「已發生 T1」回看，會漏算 P 觸發但 T1 沒發生的失敗例子。

本腳本做嚴格 forward 分析：
  1. 對每一天 t，檢查 P 條件是否成立
  2. 若成立，記錄訊號 + 計算 t+1 進場、t+1+30 出場的真實報酬
  3. 同時記錄 t+1/t+2/t+3 是否真的有 T1 cross 發生（precision）

研究問題：
  Q1: 各種 P 變體的真實命中率是多少？（forward）
  Q2: 哪個變體 + alpha 高 + 樣本夠？
  Q3: 加大盤多頭過濾後改善多少？

測試 12 個變體：
  距離閾值 × 連漲天數 × 額外條件
"""
import sys, time, json
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16
HOLD = 30
START_DATE = '2020-01-01'
COST_RT = 0.00671

# 大盤資料
TWII_TICKER = '^TWII'

# 所有變體
VARIANTS = {
    # 純距離 + 連漲（baseline P1 + 變體）
    'V1_dist10_rise2':       {'dist_max': 1.0, 'rise_days': 2},
    'V2_dist05_rise2':       {'dist_max': 0.5, 'rise_days': 2},
    'V3_dist10_rise3':       {'dist_max': 1.0, 'rise_days': 3},
    'V4_dist05_rise3':       {'dist_max': 0.5, 'rise_days': 3},
    'V5_dist03_rise2':       {'dist_max': 0.3, 'rise_days': 2},

    # + ADX 過濾
    'V6_dist10_rise2_adx18': {'dist_max': 1.0, 'rise_days': 2, 'adx_min': 18},
    'V7_dist10_rise2_adx22': {'dist_max': 1.0, 'rise_days': 2, 'adx_min': 22},

    # + 大盤多頭
    'V8_dist10_rise2_market': {'dist_max': 1.0, 'rise_days': 2, 'market_bull': True},

    # + 距離 EMA20 上方確認也算（即使已 cross 但才 1 天內）
    'V9_dist10_rise2_strict': {'dist_max': 1.0, 'rise_days': 2, 'strict_below': True},

    # + close 加速（acceleration: today rise > yesterday rise）
    'V10_dist10_accel':      {'dist_max': 1.0, 'rise_days': 2, 'accel': True},

    # 寬鬆基準（沒任何過濾）
    'V0_baseline_below_ema': {'dist_max': 999, 'rise_days': 0},
}


def load_market():
    """載入大盤資料，回傳 (date_idx → is_market_bull) dict"""
    try:
        df = dl.load_from_cache(TWII_TICKER)
        if df is None or len(df) < 60: return {}
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        e20 = df['e20'].values
        e60 = df['e60'].values
        result = {}
        for i, dt in enumerate(df.index):
            if i < 60: continue
            if np.isnan(e20[i]) or np.isnan(e60[i]): continue
            result[dt.strftime('%Y-%m-%d')] = bool(e20[i] > e60[i])
        return result
    except Exception:
        return {}


# 在 worker 裡讀大盤太慢，改用 module-level cache
_MARKET_CACHE = None
def _get_market_cache():
    global _MARKET_CACHE
    if _MARKET_CACHE is None:
        _MARKET_CACHE = load_market()
    return _MARKET_CACHE


def analyze_one(ticker):
    """每天每檔檢查所有變體：
       - 條件成立 → 記錄 30d ret + 是否 1-3 天內真的 cross"""
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return None
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy(); df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp(START_DATE)]
        if len(df) < 60: return None

        c = df['Close'].values
        o = df['Open'].values
        e20 = df['e20'].values if 'e20' in df.columns else None
        e60 = df['e60'].values if 'e60' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        if e20 is None or e60 is None or adx is None:
            return None
        n = len(df)
        idx_dates = [d.strftime('%Y-%m-%d') for d in df.index]
        market = _get_market_cache()

        result = {}
        for vname in VARIANTS:
            result[vname] = {
                'rets': [],
                'crossed_3d': 0,    # 訊號後 1-3 天內真的 cross
                'crossed_5d': 0,
                'crossed_10d': 0,
                'no_cross': 0,
            }

        for t in range(60, n - HOLD - 1):
            if any(np.isnan(x) for x in [e20[t], e60[t], adx[t]]):
                continue
            # 多頭排列（共同前提）
            if e20[t] <= e60[t]:
                continue
            close_t = c[t]
            e20_t = e20[t]
            dist_pct = (e20_t - close_t) / e20_t * 100  # 距 EMA20，正值=還在下方

            # 連漲天數
            rise_days_count = 0
            for k in range(1, 6):
                if t - k < 0: break
                if c[t - k + 1] > c[t - k]:
                    rise_days_count = k
                else:
                    break

            adx_5d = adx[t-5] if t >= 5 and not np.isnan(adx[t-5]) else adx[t]
            adx_rising = adx[t] > adx_5d

            # 是否在 t+1, t+2, t+3 真的 cross 上 EMA20？
            cross_3d = False
            cross_5d = False
            cross_10d = False
            for k in range(1, 11):
                if t + k >= n: break
                if c[t + k] > e20[t + k]:
                    if k <= 3: cross_3d = True
                    if k <= 5: cross_5d = True
                    cross_10d = True
                    break

            # 計算 30d 報酬
            ent = o[t + 1]
            exi = o[t + 1 + HOLD] if t + 1 + HOLD < n else None
            if ent is None or exi is None: continue
            if np.isnan(ent) or np.isnan(exi): continue
            if ent <= 0 or exi <= 0: continue
            ret = (exi - ent) / ent - COST_RT

            # 大盤多頭？
            is_market_bull = market.get(idx_dates[t], False)

            # 對每個 variant 檢查條件
            for vname, cfg in VARIANTS.items():
                # V0 baseline: 任何 close 在 EMA20 下方 + 多頭排列（已通過）
                if cfg.get('dist_max', 999) >= 999 and cfg.get('rise_days', 0) == 0:
                    if close_t < e20_t:  # 還在 EMA20 下方
                        result[vname]['rets'].append(ret)
                        if cross_3d: result[vname]['crossed_3d'] += 1
                        elif cross_5d: result[vname]['crossed_5d'] += 1
                        elif cross_10d: result[vname]['crossed_10d'] += 1
                        else: result[vname]['no_cross'] += 1
                    continue

                # 一般 P 變體
                if close_t >= e20_t:  # 已上穿 → 不算 pre-entry
                    if not cfg.get('strict_below'):
                        # 寬鬆：剛 cross 1 天內也算
                        if cfg.get('strict_below') is False or cfg.get('strict_below') is None:
                            pass
                    continue
                if dist_pct > cfg['dist_max']: continue
                if rise_days_count < cfg['rise_days']: continue
                if cfg.get('adx_min') is not None and adx[t] < cfg['adx_min']: continue
                if cfg.get('market_bull') and not is_market_bull: continue
                if cfg.get('accel'):
                    if t < 2: continue
                    rise_today = c[t] - c[t-1]
                    rise_yesterday = c[t-1] - c[t-2]
                    if rise_today <= rise_yesterday: continue

                result[vname]['rets'].append(ret)
                if cross_3d: result[vname]['crossed_3d'] += 1
                elif cross_5d: result[vname]['crossed_5d'] += 1
                elif cross_10d: result[vname]['crossed_10d'] += 1
                else: result[vname]['no_cross'] += 1

        return result
    except Exception:
        return None


def main():
    DATA = Path('data_cache')
    universe = sorted([
        p.stem for p in DATA.glob('*.parquet')
        if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
        and not p.stem.startswith('00')
    ])
    print(f"🇹🇼 Universe: {len(universe)} 檔")
    print(f"  期間: {START_DATE} → 現在  Hold: {HOLD} 天  Cost: {COST_RT*100:.2f}%")
    print(f"  測試 {len(VARIANTS)} 個變體\n")

    print(f"📊 跑 forward 預測分析（{WORKERS} workers）...")
    t0 = time.time()
    aggregated = {v: {'rets': [], 'crossed_3d': 0, 'crossed_5d': 0,
                      'crossed_10d': 0, 'no_cross': 0} for v in VARIANTS}
    n_processed = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for r in ex.map(analyze_one, universe, chunksize=50):
            if r is None: continue
            n_processed += 1
            for vname, dat in r.items():
                aggregated[vname]['rets'].extend(dat['rets'])
                aggregated[vname]['crossed_3d'] += dat['crossed_3d']
                aggregated[vname]['crossed_5d'] += dat['crossed_5d']
                aggregated[vname]['crossed_10d'] += dat['crossed_10d']
                aggregated[vname]['no_cross'] += dat['no_cross']
    print(f"  完成 {time.time()-t0:.1f}s ({n_processed}/{len(universe)} 檔)")
    print()

    # 計算每個 variant 的指標
    rows = []
    for vname, dat in aggregated.items():
        rets = dat['rets']
        if len(rets) < 30:
            continue
        a = np.array(rets)
        win = (a > 0).mean() * 100
        mean = a.mean() * 100
        median = np.median(a) * 100
        std = a.std() * 100
        n = len(rets)

        # Cross precision: 訊號觸發後，多少 % 真的在 N 天內 cross 上 EMA20
        cross_3d = dat['crossed_3d'] / n * 100
        cross_5d = (dat['crossed_3d'] + dat['crossed_5d']) / n * 100
        cross_10d = (dat['crossed_3d'] + dat['crossed_5d'] + dat['crossed_10d']) / n * 100

        # PF
        pos_sum = a[a > 0].sum()
        neg_sum = -a[a < 0].sum() if (a < 0).any() else 0.001
        pf = pos_sum / neg_sum if neg_sum > 0 else 999

        rows.append({
            'variant': vname,
            'n': n,
            'win_pct': round(win, 1),
            'mean_pct': round(mean, 2),
            'median_pct': round(median, 2),
            'std_pct': round(std, 2),
            'pf': round(pf, 2),
            'cross_3d_pct': round(cross_3d, 1),
            'cross_5d_pct': round(cross_5d, 1),
            'cross_10d_pct': round(cross_10d, 1),
        })

    # 排序：mean ret 降序
    rows.sort(key=lambda r: r['mean_pct'], reverse=True)

    print("=" * 110)
    print(f"{'Variant':<28}{'n':>8}{'Win%':>7}{'Mean':>8}{'Med':>8}{'Std':>7}{'PF':>6}{'X3d':>7}{'X5d':>7}{'X10d':>7}")
    print("=" * 110)
    for r in rows:
        marker = ''
        if r['mean_pct'] >= 4 and r['win_pct'] >= 53:
            marker = ' ★'
        if r['mean_pct'] >= 5:
            marker = ' 🚀'
        print(f"  {r['variant']:<28}{r['n']:>8}{r['win_pct']:>6.1f}%{r['mean_pct']:>+7.2f}%"
              f"{r['median_pct']:>+7.2f}%{r['std_pct']:>6.2f}%{r['pf']:>6.2f}"
              f"{r['cross_3d_pct']:>6.1f}%{r['cross_5d_pct']:>6.1f}%{r['cross_10d_pct']:>6.1f}%{marker}")

    print()
    print("📋 解讀:")
    print("  X3d = 訊號後 3 天內真的 cross 上 EMA20 的 %（precision）")
    print("  X10d = 訊號後 10 天內 cross 的 %")
    print("  Win% = 30 天後實際淨報酬 > 0 的比例")
    print("  Mean = 30 天平均淨報酬")
    print()
    base = next((r for r in rows if r['variant'] == 'V0_baseline_below_ema'), None)
    if base:
        print(f"基準 V0 (任何 close < EMA20 + 多頭排列): n={base['n']}, win={base['win_pct']}%, mean={base['mean_pct']:+.2f}%")
        print()
        print("與基準比較（提前進場 alpha）:")
        for r in rows:
            if r['variant'] == 'V0_baseline_below_ema': continue
            d_mean = r['mean_pct'] - base['mean_pct']
            d_win = r['win_pct'] - base['win_pct']
            star = '🚀' if d_mean > 2 else '★' if d_mean > 1 else '⚪'
            print(f"  {star} {r['variant']:<28} Δmean={d_mean:+.2f}%  Δwin={d_win:+.1f}%  precision X3d={r['cross_3d_pct']:.0f}%")

    out = {'config': {'hold': HOLD, 'start': START_DATE, 'cost_rt': COST_RT,
                      'universe': len(universe), 'variants': VARIANTS},
           'results': rows}
    with open('analyze_t1_predict_forward_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n✅ 寫入 analyze_t1_predict_forward_results.json")


if __name__ == '__main__':
    main()
