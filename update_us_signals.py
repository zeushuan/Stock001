"""US TOP 200 完整流程（v9.10 更新）
======================================
策略：v8 P10+POS+ADX18（2026-04-29 美股研究最佳，TEST RR 0.496 / 勝率 55.3%）

1. 跑 P10_T1T3+POS+ADX18（高流動 tier 局部最佳）
2. 計算多因子分數（同 v2）
3. 輸出 us_top200_signals.json + us_applicable.json
4. 計算今日訊號（ENTRY/EXIT/HOLD）

universe：us_full_tickers.json（NYSE+NASDAQ 全 5,629 檔）∩ data_cache，
          篩選 ADV ≥ $104M 高流動 tier
"""
import sys, json, time
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import data_loader as dl
import variant_strategy as vs
from fetch_us_universe import US_TOP

DATA = Path('data_cache')

# v9.10：美股最佳變體（高流動 tier TEST RR 0.496，比 P5+POS 0.348 改善 +43%）
# US 沒 IND/DXY/VWAPEXEC 資料，但加 ADX18 寬鬆趨勢過濾與 P10 加碼門檻
MODE = 'P10_T1T3+POS+ADX18'

# 🆕 排除 ETF（TOP 200 只應該是真實公司股票）
US_ETF_EXCLUDE = {
    # 大盤 / 寬基
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT',
    'EFA','AGG','LQD','HYG','IEF','SHY','BIL',
    # 商品 / 貴金屬
    'GLD','SLV','USO','UNG','UCO','SCO','BOIL','KOLD','UNL',
    'IAU','PALL','PPLT','DBA','DBC','GSG','DBO','DBE',
    # 區域 / 國家
    'EEM','EWJ','EWZ','EWY','FXI','MCHI','INDA','EWG','EWU','EWC',
    'EWA','EWT','EWS','EWH','EWP','EWQ','EWI','EWN','EWL','EWO',
    # 產業 SPDR
    'XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLRE','XLC',
    'XOP','XBI','XME','XHB','XRT','XPH','XAR','XSD','XSW','XTL',
    # 半導體 / 科技 / 生技
    'SMH','SOXX','IBB','XHE','SCHB','VGT','VHT','VFH','VIS','VDE',
    'VNQ','VOX','VPU','VAW','VCR','VDC','VYM',
    # ARK 系列
    'ARKK','ARKQ','ARKW','ARKG','ARKF','ARKX',
    # 槓桿 / 反向
    'TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','SVXY','UVXY','VXX','VIXY',
    'NUGT','DUST','JNUG','JDST','GUSH','DRIP','LABU','LABD','TMF','TMV',
    'TNA','TZA','UDOW','SDOW','SPXL','SPXS','UWM','TWM','URTY','SRTY',
    'YINN','YANG','EDC','EDZ','BOND','RWM','SH','SDS','SSO','QID','QLD',
    # 債券
    'AGGY','SCHO','SCHR','SCHZ','VCIT','VCSH','VCLT','MBB','MUB','HYS',
    # 主題
    'JETS','MOON','JEPI','JEPQ','SCHD','DIVO','VOOV','VOOG','SPLG','SPLV',
}

WINDOWS = [
    ('FULL',  '2020-01-02', '2026-04-25'),
    ('TRAIN', '2020-01-02', '2024-05-31'),
    ('TEST',  '2024-06-01', '2026-04-25'),
]


def _run_one(args):
    ticker, mode, start, end = args
    try:
        df = dl.load_from_cache(ticker)
        if df is None: return (ticker, None)
        r = vs.run_v7_variant(ticker, df, mode=mode, start=start, end=end)
        if r is None or r.get('n_trades', 0) == 0: return (ticker, None)
        return (ticker, r['pnl_pct'])
    except Exception:
        return (ticker, None)


def classify(d):
    """🆕 v9.10：對齊回測 MODE 'P10_T1T3+POS+ADX18' 的 ADX18 寬鬆趨勢過濾"""
    e20, e60 = d.get('ema20'), d.get('ema60')
    if e20 is None or e60 is None: return 'WAIT'
    is_bull = e20 > e60
    rsi = d.get('rsi'); rsi_p = d.get('rsi_prev'); rsi_p2 = d.get('rsi_prev2')
    if not is_bull:
        t4 = (rsi and rsi < 32 and rsi_p and rsi > rsi_p and rsi_p2 and rsi_p > rsi_p2)
        return 'ENTRY' if t4 else 'WAIT'
    adx = d.get('adx')
    # 🆕 美股用 ADX18 寬鬆閾值（對應 P10+POS+ADX18 局部最佳）
    if not (adx and adx >= 18): return 'WAIT'
    atr14, close = d.get('atr14'), d.get('close')
    rel_atr = atr14/close*100 if (atr14 and close) else 0
    gap = (e20-e60)/e60*100 if e60 else None
    if gap and gap < 1.0: return 'EXIT'
    if not (rel_atr > 3.5):
        if adx and adx < 25 and rsi and rsi > 75: return 'EXIT'
    cd = d.get('ema20_cross_days')
    t1 = cd and 0 < cd <= 10
    t3 = rsi and rsi < 50
    return 'ENTRY' if (t1 or t3) else 'HOLD'


def main():
    # v9.10：universe 改用 us_full_tickers.json（NYSE+NASDAQ 全 5,629 檔）
    full_path = Path('us_full_tickers.json')
    if full_path.exists():
        meta = json.loads(full_path.read_text(encoding='utf-8'))
        full_tickers = set(meta['tickers'])
        # 高流動 tier：ADV ≥ $104M（從 us_full_results 得 p75）
        MIN_ADV = 104_000_000
        candidates = [t for t in sorted(full_tickers)
                      if (DATA / f'{t}.parquet').exists()
                      and t not in US_ETF_EXCLUDE]
        universe = []
        for t in candidates:
            try:
                df = pd.read_parquet(DATA / f'{t}.parquet')
                if len(df) < 60: continue
                adv = (df['Close'].tail(60) * df['Volume'].tail(60)).mean()
                if adv >= MIN_ADV:
                    universe.append(t)
            except: continue
        print(f"US universe (us_full_tickers ∩ data_cache, 高流動 ADV≥${MIN_ADV/1e6:.0f}M): "
              f"{len(universe)} 檔\n")
    else:
        # Fallback：US_TOP 列表
        universe = sorted([t for t in US_TOP
                           if (DATA / f'{t}.parquet').exists()
                           and t not in US_ETF_EXCLUDE])
        print(f"US universe (US_TOP fallback, 排除 ETF): {len(universe)} 檔\n")

    # ─── Step 1: 回測 3 期 ─────────────────────────────
    print(f"Step 1: 跑 {MODE} 回測（FULL/TRAIN/TEST）...")
    pnl = {win: {} for win, _, _ in WINDOWS}
    t0 = time.time()
    for win_name, start, end in WINDOWS:
        args = [(t, MODE, start, end) for t in universe]
        with ProcessPoolExecutor(max_workers=12) as ex:
            for ticker, p in ex.map(_run_one, args):
                if p is not None:
                    pnl[win_name][ticker] = p
        print(f"  [{win_name}] n={len(pnl[win_name])}")
    print(f"  耗時 {time.time()-t0:.1f}s\n")

    # ─── Step 2: 計算流動性 ────────────────────────────
    print("Step 2: 計算流動性...")
    liquidity = {}
    for t in universe:
        try:
            df = pd.read_parquet(DATA / f'{t}.parquet')
            recent = df.iloc[-60:]
            liquidity[t] = float((recent['Close'] * recent['Volume']).mean())
        except Exception:
            continue

    # ─── Step 3: 多因子評分 ────────────────────────────
    print("\nStep 3: 多因子綜合分數")
    rows = []
    for t in universe:
        pt = pnl['TEST'].get(t)
        ptr = pnl['TRAIN'].get(t)
        pf = pnl['FULL'].get(t)
        if pt is None: continue
        liq = liquidity.get(t, 0)

        return_score = max(0, np.log10(pt + 100) * 30) if pt > -100 else 0
        test_win = 20 if pt > 0 else 0
        train_win = 20 if (ptr is not None and ptr > 0) else 0
        liq_score = min(np.log10(max(liq, 1)) * 2, 20)
        rr_proxy = pt / 100 if pt > 0 else 0
        rr_score = min(rr_proxy * 10, 10)

        total = return_score + test_win + train_win + liq_score + rr_score
        rows.append({
            'ticker': t,
            'pnl_test': round(pt, 1),
            'pnl_train': round(ptr, 1) if ptr is not None else None,
            'pnl_full': round(pf, 1) if pf is not None else None,
            'liquidity': int(liq),
            'score': round(total, 2),
        })
    rows.sort(key=lambda x: -x['score'])

    n = len(rows)
    print(f"  總共 {n} 檔有完整回測資料")
    print(f"\nUS TOP 20 預覽:")
    print(f"{'排':<3} {'代號':<8} {'分數':>7} {'TEST%':>9} {'流動性':>14}")
    for i, r in enumerate(rows[:20], 1):
        print(f"{i:<3} {r['ticker']:<8} {r['score']:>7.1f} "
              f"{r['pnl_test']:>+9.1f} {r['liquidity']:>14,}")

    # ─── Step 4: 分級 ──────────────────────────────────
    n_top = min(200, n // 2)  # US TOP 200（>= 400 檔可選）
    n_na = max(40, n // 10)
    tier_dict = {}
    for i, r in enumerate(rows):
        if i < n_top: tier = 'TOP'
        elif i >= n - n_na and r['pnl_test'] <= 0: tier = 'NA'
        else: tier = 'OK'
        tier_dict[r['ticker']] = {
            'vwapexec': r['pnl_test'],   # US 沒 vwap，用 baseline pnl 作 proxy
            'tier': tier,
            'score': r['score'],
        }

    # ─── Step 5: 計算今日訊號 ───────────────────────────
    print("\nStep 5: 計算今日 entry/exit/hold ...")
    # 🆕 v9.10h：載入 US 公司名稱 map
    us_name_map = {}
    if Path('us_full_tickers.json').exists():
        try:
            full = json.load(open('us_full_tickers.json', encoding='utf-8'))
            for x in full.get('detail', []):
                sym = x.get('symbol', '')
                nm = x.get('name', '')
                for sep in [' - ', ' Common ', ' Class ', ' Ordinary ']:
                    if sep in nm:
                        nm = nm.split(sep)[0]
                        break
                if sym: us_name_map[sym] = nm[:40]
        except Exception: pass

    entry, exit_, hold, wait = [], [], [], []
    last_dates = []
    for t in universe:
        # 只看 TOP tier
        if tier_dict.get(t, {}).get('tier') != 'TOP': continue
        try:
            df = pd.read_parquet(DATA / f'{t}.parquet')
            if len(df) < 30: continue
            last = -1
            e20s, e60s = df['e20'].values, df['e60'].values
            cd = None
            if not (np.isnan(e20s[last]) or np.isnan(e60s[last])):
                cur_bull = e20s[last] > e60s[last]
                for k in range(1, min(60, len(df))):
                    if np.isnan(e20s[last-k]) or np.isnan(e60s[last-k]): continue
                    if (e20s[last-k] > e60s[last-k]) != cur_bull:
                        cd = k if cur_bull else -k
                        break

            d = {
                'close': float(df['Close'].iloc[last]),
                'ema20': float(e20s[last]) if not np.isnan(e20s[last]) else None,
                'ema60': float(e60s[last]) if not np.isnan(e60s[last]) else None,
                'rsi': float(df['rsi'].iloc[last]) if 'rsi' in df.columns and not np.isnan(df['rsi'].iloc[last]) else None,
                'rsi_prev': float(df['rsi'].iloc[last-1]) if len(df)>=2 and 'rsi' in df.columns and not np.isnan(df['rsi'].iloc[last-1]) else None,
                'rsi_prev2': float(df['rsi'].iloc[last-2]) if len(df)>=3 and 'rsi' in df.columns and not np.isnan(df['rsi'].iloc[last-2]) else None,
                'adx': float(df['adx'].iloc[last]) if 'adx' in df.columns and not np.isnan(df['adx'].iloc[last]) else None,
                'atr14': float(df['atr'].iloc[last]) if 'atr' in df.columns and not np.isnan(df['atr'].iloc[last]) else None,
                'ema20_cross_days': cd,
            }
            action = classify(d)
            score = tier_dict[t]['score']
            pnl_t = tier_dict[t]['vwapexec']
            rsi_v = d.get('rsi')
            if cd and 0 < cd <= 10: sig = f'T1 {cd}d'
            elif rsi_v and rsi_v < 50: sig = f'T3 RSI{rsi_v:.0f}'
            elif action == 'EXIT': sig = 'RSI>75/EMA死叉'
            else: sig = '—'

            row = {
                'ticker': t,
                'name': us_name_map.get(t, t),  # 🆕 改用公司名稱
                'close': round(d['close'], 2),
                'rsi': round(rsi_v, 1) if rsi_v else None,
                'ema20_cross_days': cd,
                'delta': round(pnl_t, 1),  # 用 TEST PnL 作 delta proxy
                'sig': sig,
                'pe': None,  # US 暫不抓 PE
                'pbr': None,
                'div': None,
            }
            if action == 'ENTRY': entry.append(row)
            elif action == 'EXIT': exit_.append(row)
            elif action == 'HOLD': hold.append(row)
            else: wait.append(row)
            last_dates.append(df.index[last].strftime('%Y-%m-%d'))
        except Exception:
            continue

    entry.sort(key=lambda x: -x['delta'])
    exit_.sort(key=lambda x: -x['delta'])
    hold.sort(key=lambda x: -x['delta'])

    # 🆕 fail-safe：雲端沒 data_cache 時不覆蓋已存在的 JSON
    if not last_dates:
        print("\n❌ 沒有可處理的 US ticker（data_cache 不存在或所有 ticker 都跳過）")
        print("   雲端環境請在本機跑後 commit JSON 至 repo")
        print("   保留現有 us_top200_signals.json 不覆蓋")
        return

    out = {
        'updated_at': max(last_dates) if last_dates else 'unknown',
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'top_total': len([t for t in tier_dict if tier_dict[t]['tier'] == 'TOP']),
        'entry': entry,
        'exit': exit_,
        'hold': hold,
        'wait_count': len(wait),
    }
    with open('us_top200_signals.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    with open('us_applicable.json', 'w', encoding='utf-8') as f:
        json.dump(tier_dict, f, indent=2, ensure_ascii=False)

    print(f"\n📊 US TOP 100 即時掃描:")
    print(f"  📅 資料截至：{out['updated_at']}")
    print(f"  🚀 進場：{len(entry)}")
    print(f"  🚪 出倉：{len(exit_)}")
    print(f"  📌 持倉：{len(hold)}")
    print(f"  ⏸  觀望：{len(wait)}")
    print(f"\n✅ 寫入 us_top200_signals.json + us_applicable.json")


if __name__ == '__main__':
    main()
