"""跌深反彈 × 天數深度研究
=================================
延伸 v9.10l 發現：US 跌深反彈 (drawdown ≥15%) RR 0.171

研究問題：
  Q1: drawdown 達 15% 後 Day 1, 2, 3...N 進場勝率/RR 曲線
  Q2: 不同跌幅級距（15-20% / 20-30% / 30-50% / >50%）哪個 RR 最高？
  Q3: 跌深 + 其他組合（T1 / RSI<30 / ADX↑ / 大盤多頭）
  Q4: TW 跌深無效，但細分後是否有 sub-window 有效？

樣本：TW 1058 + US 555 高流動 × 6 年
持有：30 天固定持有（與 cross_days 研究一致）
"""
import sys, time, json
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16
HOLD = 30
MIN_ADV = 104_000_000
DRAWDOWN_TH = 15  # 進入跌深區的門檻（%）

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
}


def analyze_one(args):
    """對單檔股票，找所有「進入跌深 → N 天後進場」事件，按條件分群"""
    ticker, market = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp('2020-01-01')]
        if len(df) < 280: return (ticker, None)

        e20 = df['e20'].values
        e60 = df['e60'].values
        rsi = df['rsi'].values if 'rsi' in df.columns else None
        adx = df['adx'].values if 'adx' in df.columns else None
        atr = df['atr'].values if 'atr' in df.columns else None
        close = df['Close'].values
        high = df['High'].values if 'High' in df.columns else close
        n = len(df)
        if rsi is None or adx is None or atr is None: return (ticker, None)

        adx_th = 18 if market == 'us' else 22

        # 60 日 rolling high
        high60 = np.array([
            close[max(0, i-60):i].max() if i >= 1 else close[i]
            for i in range(n)
        ])
        # drawdown %
        drawdown = (high60 - close) / high60 * 100
        drawdown[high60 == 0] = 0

        results = {
            # Day 0-30 跌深後天數曲線
            **{f'day{d}': [] for d in range(0, 31)},
            # 跌幅級距
            'dd_15_20':  [],
            'dd_20_30':  [],
            'dd_30_50':  [],
            'dd_50plus': [],
            # 條件組合
            'dd_T1':       [],   # 跌深 + T1 黃金交叉
            'dd_T1_bull':  [],   # 跌深 + T1 + 多頭
            'dd_RSI_30':   [],   # 跌深 + RSI<30 超賣
            'dd_RSI_50':   [],   # 跌深 + RSI<50 拉回
            'dd_ADX_up':   [],   # 跌深 + ADX 上升中
            'dd_bull':     [],   # 跌深 + 多頭
            'dd_bear':     [],   # 跌深 + 空頭
        }

        # 找「進入跌深」的觸發日（drawdown 從 < 15% 變成 ≥ 15%）
        in_dd = False
        dd_start_i = None
        for i in range(60, n - HOLD):
            if any(np.isnan(x) for x in [e20[i], e60[i], rsi[i], adx[i], atr[i]]):
                continue

            cur_dd = drawdown[i]

            if not in_dd and cur_dd >= DRAWDOWN_TH:
                in_dd = True
                dd_start_i = i
            elif in_dd and cur_dd < DRAWDOWN_TH * 0.5:  # 跌幅縮回 < 7.5% 視為脫離
                in_dd = False
                dd_start_i = None

            if not in_dd or dd_start_i is None: continue

            # 跌深後第 d 天（從 dd_start_i 算起）
            d_after = i - dd_start_i
            if d_after > 30: continue

            # 30 日報酬
            ret = (close[i + HOLD] - close[i]) / close[i] * 100

            # Day 曲線
            results[f'day{d_after}'].append(ret)

            # 跌幅級距（用觸發當下的跌幅）
            init_dd = drawdown[dd_start_i]
            if 15 <= init_dd < 20: results['dd_15_20'].append(ret)
            elif 20 <= init_dd < 30: results['dd_20_30'].append(ret)
            elif 30 <= init_dd < 50: results['dd_30_50'].append(ret)
            elif init_dd >= 50: results['dd_50plus'].append(ret)

            # 條件組合（在跌深窗內任何一天進場）
            is_bull = e20[i] > e60[i]

            # T1: 找最近黃金交叉 1-10 天
            cd = None
            for k in range(1, min(15, i)):
                if np.isnan(e20[i-k]) or np.isnan(e60[i-k]): continue
                if e20[i-k] <= e60[i-k]:
                    cd = k; break
            t1 = (cd is not None and 0 < cd <= 10)

            if t1:
                results['dd_T1'].append(ret)
                if is_bull and adx[i] >= adx_th:
                    results['dd_T1_bull'].append(ret)

            if rsi[i] < 30: results['dd_RSI_30'].append(ret)
            if rsi[i] < 50: results['dd_RSI_50'].append(ret)

            adx_up = (i >= 5 and not np.isnan(adx[i-5])
                      and adx[i] > adx[i-5])
            if adx_up: results['dd_ADX_up'].append(ret)

            if is_bull: results['dd_bull'].append(ret)
            else: results['dd_bear'].append(ret)

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

    print(f"🇹🇼 TW: {len(tw_universe)}  🇺🇸 US: {len(us_high_liquid)}\n")

    def run(universe, market):
        agg = {}
        tasks = [(t, market) for t in universe]
        t0 = time.time()
        with ProcessPoolExecutor(max_workers=WORKERS) as ex:
            for ticker, r in ex.map(analyze_one, tasks, chunksize=80):
                if r is not None:
                    for k, v in r.items():
                        agg.setdefault(k, []).extend(v)
        print(f"  {market.upper()} 完成 {time.time()-t0:.1f}s")
        return agg

    print("📊 跑 TW 跌深反彈分析...")
    tw_agg = run(tw_universe, 'tw')
    print("📊 跑 US 跌深反彈分析...")
    us_agg = run(us_high_liquid, 'us')

    for market_name, agg in [('🇹🇼 TW', tw_agg), ('🇺🇸 US 高流動', us_agg)]:
        # ── Day 0-30 曲線 ──
        print(f"\n{'='*100}")
        print(f"📊 {market_name} — 跌深 ({DRAWDOWN_TH}%) 後第 N 天進場 × 30 天勝率")
        print("="*100)
        print(f"{'Day':<6} {'樣本':>7} {'勝率%':>8} {'均報%':>9} "
              f"{'中位%':>8} {'最差%':>9} {'RR':>7}  視覺化")
        print("-"*100)
        best_day = None
        best_rr = -999
        for d in range(0, 31):
            m = metrics(agg.get(f'day{d}', []))
            if m and m['n'] >= 100:
                bar = '█' * max(0, int(m['rr'] * 30))
                marker = ''
                if m['rr'] > best_rr:
                    best_rr = m['rr']; best_day = d
                if d in [0, 1, 2, 3, 5, 7, 10, 14, 20, 30]:
                    print(f"Day {d:<3} {m['n']:>7} {m['win']:>+8.1f} "
                          f"{m['mean']:>+9.2f} {m['median']:>+8.2f} "
                          f"{m['worst']:>+9.1f} {m['rr']:>7.3f}  {bar[:25]}")
        if best_day is not None:
            print(f"\n  ⭐ 最佳 Day {best_day}: RR {best_rr:.3f}")

        # ── 跌幅級距 ──
        print(f"\n📊 跌幅級距 × RR")
        print("-"*100)
        print(f"{'級距':<20} {'樣本':>7} {'勝率%':>8} {'均報%':>9} {'RR':>7}")
        for k, label in [('dd_15_20', '15-20% (淺跌)'),
                          ('dd_20_30', '20-30% (中跌)'),
                          ('dd_30_50', '30-50% (深跌)'),
                          ('dd_50plus', '>50% (重挫)')]:
            m = metrics(agg.get(k, []))
            if m:
                print(f"{label:<20} {m['n']:>7} {m['win']:>+8.1f} "
                      f"{m['mean']:>+9.2f} {m['rr']:>7.3f}")

        # ── 條件組合 ──
        print(f"\n📊 跌深 + 其他條件組合")
        print("-"*100)
        print(f"{'條件':<30} {'樣本':>7} {'勝率%':>8} {'均報%':>9} {'RR':>7}")
        for k, label in [
            ('dd_T1',      '+ T1 黃金交叉'),
            ('dd_T1_bull', '+ T1 + 多頭+ADX達標'),
            ('dd_RSI_30',  '+ RSI<30 超賣'),
            ('dd_RSI_50',  '+ RSI<50 拉回'),
            ('dd_ADX_up',  '+ ADX 上升中'),
            ('dd_bull',    '+ 多頭'),
            ('dd_bear',    '+ 空頭'),
        ]:
            m = metrics(agg.get(k, []))
            if m:
                print(f"{label:<30} {m['n']:>7} {m['win']:>+8.1f} "
                      f"{m['mean']:>+9.2f} {m['rr']:>7.3f}")

    # 寫 JSON
    out = {
        'tw_metrics': {k: metrics(v) for k, v in tw_agg.items()},
        'us_metrics': {k: metrics(v) for k, v in us_agg.items()},
        'config': {
            'drawdown_threshold': DRAWDOWN_TH,
            'hold_days': HOLD,
            'tw_universe_size': len(tw_universe),
            'us_universe_size': len(us_high_liquid),
        }
    }
    with open('drawdown_days_results.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print("\n💾 寫入 drawdown_days_results.json")


if __name__ == '__main__':
    main()
