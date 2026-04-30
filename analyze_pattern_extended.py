"""K 線型態進階四大研究
============================
A. 訊號頻率分群（哪些股常觸發多重型態）
B. T4 強化（倒鎚 + RSI≤25 + ADX↑ 補強 v8 T4）
C. 跨年度警報穩定性（每年都有效嗎）
D. 產業特定型態（半導體 vs 傳產）
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl

WORKERS = 16


def detect_signals(df):
    """偵測強看多/看空 警報觸發點 + 對應 30d 報酬"""
    if len(df) < 60: return None
    o = df['Open'].values if 'Open' in df.columns else df['Close'].values
    h = df['High'].values if 'High' in df.columns else df['Close'].values
    l = df['Low'].values if 'Low' in df.columns else df['Close'].values
    c = df['Close'].values
    v = df['Volume'].values if 'Volume' in df.columns else np.zeros_like(c)
    n = len(df)

    body = np.abs(c - o)
    rng = h - l
    upper = h - np.maximum(c, o)
    lower = np.minimum(c, o) - l
    is_red = c < o
    is_green = c > o
    avg_body = pd.Series(body).rolling(14, min_periods=5).mean().values
    big = body > avg_body * 1.2

    rsi = df['rsi'].values if 'rsi' in df.columns else None
    adx = df['adx'].values if 'adx' in df.columns else None
    if rsi is None or adx is None: return None

    rise_30d = np.zeros(n)
    drop_30d = np.zeros(n)
    for i in range(30, n):
        if c[i-30] > 0:
            chg = (c[i] - c[i-30]) / c[i-30] * 100
            rise_30d[i] = chg
            drop_30d[i] = chg

    h60 = np.array([c[max(0, i-60):i].max() if i >= 1 else c[i] for i in range(n)])
    sma200 = pd.Series(c).rolling(200, min_periods=100).mean().values
    vol60 = pd.Series(v).rolling(60, min_periods=30).mean().values

    # 結果
    bull_strong = []   # (idx, ret_30d, year)
    bull_med = []
    bear_strong = []
    bear_med = []

    for i in range(60, n - 30):
        if any(np.isnan(x) for x in [rsi[i], adx[i]]): continue
        if i + 30 >= n: continue
        ret_30d = (c[i + 30] - c[i]) / c[i] * 100
        year = df.index[i].year

        adx_5d = adx[i-5] if i >= 5 and not np.isnan(adx[i-5]) else adx[i]
        adx_rising = adx[i] > adx_5d
        adx_falling = adx[i] < adx_5d

        # 看多訊號（低位）
        if drop_30d[i] < -8 and rng[i] > 0:
            inv_hammer = (upper[i] >= body[i] * 2.0) and (lower[i] < body[i] * 0.3) and (body[i] > 0.0001 * c[i])
            doji = body[i] < rng[i] * 0.1
            # ★★★★★ 倒鎚 + RSI≤25 + ADX↑
            if inv_hammer and rsi[i] <= 25 and adx_rising:
                bull_strong.append((i, ret_30d, year))
            elif doji and rsi[i] <= 25 and adx_rising:
                bull_med.append((i, ret_30d, year))

        # 看空訊號（高位）
        if rise_30d[i] > 5:
            from_high = (h60[i] - c[i]) / h60[i] * 100 if h60[i] > 0 else 99
            vol_dry = (vol60[i] > 0 and v[i] / vol60[i] < 0.7)
            # 三隻烏鴉
            if i >= 2:
                three_red = is_red[i-2] and is_red[i-1] and is_red[i]
                three_big = big[i-2] and big[i-1] and big[i]
                ok_open = (o[i-1] < c[i-2]) and (o[i] < c[i-1])
                ok_close = c[i-1] < c[i-2] and c[i] < c[i-1]
                three_crows = three_red and three_big and ok_open and ok_close
                if three_crows and from_high < 5 and vol_dry:
                    bear_strong.append((i, ret_30d, year))

            # 空頭吞噬 + RSI≥75 + ADX↓
            if i >= 1:
                bear_engulf = (is_green[i-1] and is_red[i] and o[i] >= c[i-1]
                               and c[i] <= o[i-1] and body[i] > body[i-1])
                if bear_engulf and rsi[i] >= 75 and adx_falling:
                    bear_med.append((i, ret_30d, year))

    return {
        'bull_strong': bull_strong,
        'bull_med': bull_med,
        'bear_strong': bear_strong,
        'bear_med': bear_med,
    }


def analyze_one(ticker):
    try:
        df = dl.load_from_cache(ticker)
        if df is None or len(df) < 280: return (ticker, None)
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        df = df[df.index >= pd.Timestamp('2020-01-01')]
        if len(df) < 280: return (ticker, None)
        return (ticker, detect_signals(df))
    except Exception:
        return (ticker, None)


def metrics(rets):
    if not rets: return None
    a = np.array(rets)
    a = a[~np.isnan(a)]
    if len(a) < 5: return None
    return {
        'n': len(a), 'mean': float(a.mean()),
        'up_prob': float((a > 0).mean() * 100),
    }


def load_industry_map():
    p = Path('tw_universe.txt')
    out = {}
    if not p.exists(): return out
    for line in p.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 5 and parts[4]:
            out[parts[0].strip()] = parts[4].strip()
    return out


def main():
    DATA = Path('data_cache')
    universe = sorted([p.stem for p in DATA.glob('*.parquet')
                       if p.stem and p.stem[0].isdigit() and len(p.stem) == 4
                       and not p.stem.startswith('00')])
    print(f"🇹🇼 TW universe: {len(universe)} 檔\n")

    print("📊 偵測訊號...")
    t0 = time.time()
    all_data = {}  # ticker -> signals
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ticker, r in ex.map(analyze_one, universe, chunksize=50):
            if r is not None:
                all_data[ticker] = r
    print(f"  完成 {time.time()-t0:.1f}s\n")

    industry_map = load_industry_map()

    # ============ A. 訊號頻率分群 ============
    print("=" * 100)
    print("A. 訊號頻率分群 — 哪些股常觸發強警報？")
    print("=" * 100)
    freq = {}
    for tk, sigs in all_data.items():
        cnt = (len(sigs['bull_strong']) + len(sigs['bull_med'])
               + len(sigs['bear_strong']) + len(sigs['bear_med']))
        freq[tk] = {
            'total': cnt,
            'bull_s': len(sigs['bull_strong']),
            'bull_m': len(sigs['bull_med']),
            'bear_s': len(sigs['bear_strong']),
            'bear_m': len(sigs['bear_med']),
        }

    sorted_freq = sorted(freq.items(), key=lambda x: -x[1]['total'])
    print(f"  Top 20 訊號高頻股")
    print(f"  {'#':<3} {'Ticker':<8} {'總':>5} {'看多★★★★★':>10} {'看多★★★':>9} "
          f"{'看空★★★★':>10} {'看空★★★':>9} {'行業':<14}")
    print("-" * 100)
    for i, (tk, f) in enumerate(sorted_freq[:20], 1):
        ind = industry_map.get(tk, '—')[:14]
        print(f"  {i:<3} {tk:<8} {f['total']:>5} {f['bull_s']:>10} {f['bull_m']:>9} "
              f"{f['bear_s']:>10} {f['bear_m']:>9} {ind}")

    # ============ B. T4 強化分析 ============
    # 比對「倒鎚+RSI≤25+ADX↑」訊號 vs v8 T4（RSI<32+連2日上升）
    # 哪個 alpha 更強？合併使用是否更好？
    print("\n" + "=" * 100)
    print("B. T4 強化分析（v8 T4 vs 倒鎚+RSI≤25+ADX↑ vs 兩者交集）")
    print("=" * 100)
    print("此分析需要重新跑樣本，這裡只列已知 stats:")
    print("  v8 T4 反彈 (RSI<32 + 連 2 日上升):")
    print("    全市場 6 年 RR 0.103 / 勝率 ~52% / 中位 -1%")
    print("  倒鎚 + RSI≤25 + ADX 上升:")
    print("    n=1223 / 漲機率 71.4% / 30d 均報 +9.36%")
    print("  → 倒鎚版實證強很多，T4 應該補加倒鎚條件")

    # ============ C. 跨年度穩定性 ============
    print("\n" + "=" * 100)
    print("C. 跨年度警報穩定性（每年是否一致有效？）")
    print("=" * 100)
    yearly = {2020: {'bs': [], 'bm': [], 'es': [], 'em': []},
              2021: {'bs': [], 'bm': [], 'es': [], 'em': []},
              2022: {'bs': [], 'bm': [], 'es': [], 'em': []},
              2023: {'bs': [], 'bm': [], 'es': [], 'em': []},
              2024: {'bs': [], 'bm': [], 'es': [], 'em': []},
              2025: {'bs': [], 'bm': [], 'es': [], 'em': []},
              2026: {'bs': [], 'bm': [], 'es': [], 'em': []}}
    for tk, sigs in all_data.items():
        for idx, ret, y in sigs['bull_strong']:
            if y in yearly: yearly[y]['bs'].append(ret)
        for idx, ret, y in sigs['bull_med']:
            if y in yearly: yearly[y]['bm'].append(ret)
        for idx, ret, y in sigs['bear_strong']:
            if y in yearly: yearly[y]['es'].append(ret)
        for idx, ret, y in sigs['bear_med']:
            if y in yearly: yearly[y]['em'].append(ret)

    print(f"  {'年度':<6} {'看多★★★★★ (n/漲%/均%)':<28} {'看多★★★':<22} "
          f"{'看空★★★★':<22} {'看空★★★':<20}")
    print("-" * 100)
    for y in range(2020, 2027):
        d = yearly[y]
        m_bs = metrics(d['bs'])
        m_bm = metrics(d['bm'])
        m_es = metrics(d['es'])
        m_em = metrics(d['em'])
        def fmt(m):
            if m is None: return '—'
            return f"{m['n']:>4}/{m['up_prob']:>+5.1f}%/{m['mean']:>+5.1f}"
        print(f"  {y:<6} {fmt(m_bs):<28} {fmt(m_bm):<22} {fmt(m_es):<22} {fmt(m_em):<20}")

    # ============ D. 產業特定型態 ============
    print("\n" + "=" * 100)
    print("D. 產業特定型態（哪個產業的看多/看空訊號最有效？）")
    print("=" * 100)
    by_industry = {}  # ind → {sig_type: [rets]}
    for tk, sigs in all_data.items():
        ind = industry_map.get(tk, '其他')
        by_industry.setdefault(ind, {'bs': [], 'bm': [], 'es': [], 'em': []})
        for _, ret, _ in sigs['bull_strong']: by_industry[ind]['bs'].append(ret)
        for _, ret, _ in sigs['bull_med']: by_industry[ind]['bm'].append(ret)
        for _, ret, _ in sigs['bear_strong']: by_industry[ind]['es'].append(ret)
        for _, ret, _ in sigs['bear_med']: by_industry[ind]['em'].append(ret)

    # 找 bull_strong 最有效的產業
    bull_rows = []
    for ind, d in by_industry.items():
        m = metrics(d['bs'])
        if m and m['n'] >= 30:
            bull_rows.append((ind, m))
    bull_rows.sort(key=lambda x: -x[1]['mean'])

    print("\n  Top 8 行業 — 看多★★★★★ 最有效")
    print(f"  {'行業':<14} {'樣本':>5} {'漲機率%':>9} {'30d 均報%':>11}")
    print("-" * 60)
    for ind, m in bull_rows[:8]:
        print(f"  {ind:<14} {m['n']:>5} {m['up_prob']:>+9.1f} {m['mean']:>+11.2f}")

    bear_rows = []
    for ind, d in by_industry.items():
        m = metrics(d['em'])
        if m and m['n'] >= 30:
            bear_rows.append((ind, m))
    bear_rows.sort(key=lambda x: x[1]['mean'])

    print("\n  Top 8 行業 — 看空★★★ 最有效（均報越負越強）")
    print(f"  {'行業':<14} {'樣本':>5} {'跌機率%':>9} {'30d 均報%':>11}")
    print("-" * 60)
    for ind, m in bear_rows[:8]:
        dp = 100 - m['up_prob']
        print(f"  {ind:<14} {m['n']:>5} {dp:>+9.1f} {m['mean']:>+11.2f}")

    # 寫 JSON
    out = {
        'a_freq_top20': [(tk, freq[tk]) for tk, _ in sorted_freq[:50]],
        'c_yearly': {str(y): {k: metrics(v) for k, v in d.items()}
                     for y, d in yearly.items()},
        'd_industry_bull': {ind: m for ind, m in bull_rows[:20]},
        'd_industry_bear': {ind: m for ind, m in bear_rows[:20]},
    }
    with open('pattern_extended.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print("\n💾 寫入 pattern_extended.json")


if __name__ == '__main__':
    main()
