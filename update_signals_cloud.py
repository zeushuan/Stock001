"""☁️ 雲端版每日訊號更新（不需 data_cache）
=================================================
直接用 yfinance 批量抓最新 280 日 OHLCV → 計算指標 → 分類 → 寫 JSON

支援：
  TW TOP 200（從 vwap_applicable.json 讀清單）
  US TOP 100（從 us_applicable.json 讀清單）

雲端可用：
  python update_signals_cloud.py        # 跑兩個市場
  python update_signals_cloud.py --tw   # 只跑 TW
  python update_signals_cloud.py --us   # 只跑 US

預估時間：
  TW 200 檔 yfinance batch ~30 秒
  US 200 檔 yfinance batch ~30 秒
  指標計算 ~30 秒
  總計 ~1.5 分鐘
"""
import sys, json, time, argparse
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
import ta
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def _calc_ind(df):
    """計算 v8 指標（最低需求）"""
    if df is None or len(df) < 30: return None
    df = df.copy()
    df['e5']   = ta.trend.ema_indicator(df['Close'], window=5)
    df['e20']  = ta.trend.ema_indicator(df['Close'], window=20)
    df['e60']  = ta.trend.ema_indicator(df['Close'], window=60)
    df['rsi']  = ta.momentum.rsi(df['Close'], window=14)
    df['adx']  = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    df['atr']  = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], 14)
    return df


def _fetch_batch(tickers, period='1y'):
    """批量 yfinance 抓取（每次 ~50 檔以避免 rate limit）"""
    out = {}
    BATCH = 50
    for i in range(0, len(tickers), BATCH):
        batch = tickers[i:i+BATCH]
        try:
            df = yf.download(batch, period=period, interval='1d',
                             auto_adjust=False, progress=False,
                             group_by='ticker', threads=True)
            if df is None or df.empty: continue
            for t in batch:
                try:
                    if len(batch) == 1:
                        sub = df
                    else:
                        sub = df[t]
                    sub = sub.dropna(how='all')
                    if len(sub) < 30: continue
                    out[t] = sub
                except Exception:
                    continue
        except Exception as e:
            print(f"  ❌ batch {i//BATCH+1} 失敗: {str(e)[:80]}")
        print(f"  📦 batch {i//BATCH+1}/{(len(tickers)+BATCH-1)//BATCH}: "
              f"成功 {len(out)} 檔")
    return out


def _classify_tw(d):
    """台股分類（對應 P5+VWAPEXEC，ADX≥22）"""
    e20, e60 = d.get('ema20'), d.get('ema60')
    if e20 is None or e60 is None: return 'WAIT'
    is_bull = e20 > e60
    rsi, rsi_p, rsi_p2 = d.get('rsi'), d.get('rsi_prev'), d.get('rsi_prev2')
    if not is_bull:
        t4 = (rsi and rsi < 32 and rsi_p and rsi > rsi_p
              and rsi_p2 and rsi_p > rsi_p2)
        return 'ENTRY' if t4 else 'WAIT'
    adx = d.get('adx')
    if not (adx and adx >= 22): return 'WAIT'
    cd = d.get('ema20_cross_days')
    t1 = cd and 0 < cd <= 10
    t3 = rsi and rsi < 50
    return 'ENTRY' if (t1 or t3) else 'HOLD'


def _classify_us(d):
    """美股分類（對應 P10+POS+ADX18，ADX≥18）"""
    e20, e60 = d.get('ema20'), d.get('ema60')
    if e20 is None or e60 is None: return 'WAIT'
    is_bull = e20 > e60
    rsi, rsi_p, rsi_p2 = d.get('rsi'), d.get('rsi_prev'), d.get('rsi_prev2')
    if not is_bull:
        t4 = (rsi and rsi < 32 and rsi_p and rsi > rsi_p
              and rsi_p2 and rsi_p > rsi_p2)
        return 'ENTRY' if t4 else 'WAIT'
    adx = d.get('adx')
    if not (adx and adx >= 18): return 'WAIT'  # 美股寬鬆閾值
    cd = d.get('ema20_cross_days')
    t1 = cd and 0 < cd <= 10
    t3 = rsi and rsi < 50
    return 'ENTRY' if (t1 or t3) else 'HOLD'


def _t3_confidence(close, e5, e20, e5_5d, e20_5d):
    """T3 信心度 0-5"""
    score = 0
    hits = []
    if e20 is not None and close > e20:
        score += 1; hits.append('close>EMA20')
    e20_up = e20 is not None and e20_5d is not None and e20 > e20_5d
    e5_up = e5 is not None and e5_5d is not None and e5 > e5_5d
    if e20_up: score += 1; hits.append('EMA20上升')
    if e5_up: score += 1; hits.append('EMA5上升')
    if e5 is not None and e20 is not None and e5 > e20:
        score += 1; hits.append('EMA5>EMA20')
    if e5_up and e20_up: score += 1; hits.append('雙均線都升')
    return score, hits


def _process(df_dict, classify_fn, name_map=None):
    """從每股 DataFrame → 計算指標 → 分類 → 產出 row"""
    name_map = name_map or {}
    entry, exit_, hold, wait = [], [], [], []
    last_dates = []

    for ticker, df in df_dict.items():
        df = _calc_ind(df)
        if df is None or len(df) < 30: continue
        last = -1

        e20s, e60s = df['e20'].values, df['e60'].values
        e5s = df['e5'].values
        rsi_arr = df['rsi'].values
        adx_arr = df['adx'].values
        atr_arr = df['atr'].values
        close_arr = df['Close'].values

        # cross_days
        cd = None
        if not (np.isnan(e20s[last]) or np.isnan(e60s[last])):
            cur_bull = e20s[last] > e60s[last]
            for k in range(1, min(60, len(df))):
                if np.isnan(e20s[last-k]) or np.isnan(e60s[last-k]): continue
                if (e20s[last-k] > e60s[last-k]) != cur_bull:
                    cd = k if cur_bull else -k
                    break

        # T3 信心度
        t3_score = 0
        t3_hits = []
        if len(df) >= 6:
            close_v = float(close_arr[last])
            e5_now = e5s[last] if not np.isnan(e5s[last]) else None
            e20_now = e20s[last] if not np.isnan(e20s[last]) else None
            e5_5d = e5s[last-5] if not np.isnan(e5s[last-5]) else None
            e20_5d = e20s[last-5] if not np.isnan(e20s[last-5]) else None
            t3_score, t3_hits = _t3_confidence(close_v, e5_now, e20_now,
                                                e5_5d, e20_5d)

        d = {
            'close': float(close_arr[last]),
            'ema20': float(e20s[last]) if not np.isnan(e20s[last]) else None,
            'ema60': float(e60s[last]) if not np.isnan(e60s[last]) else None,
            'rsi': float(rsi_arr[last]) if not np.isnan(rsi_arr[last]) else None,
            'rsi_prev': float(rsi_arr[last-1]) if len(rsi_arr) >= 2
                         and not np.isnan(rsi_arr[last-1]) else None,
            'rsi_prev2': float(rsi_arr[last-2]) if len(rsi_arr) >= 3
                          and not np.isnan(rsi_arr[last-2]) else None,
            'adx': float(adx_arr[last]) if not np.isnan(adx_arr[last]) else None,
            'atr14': float(atr_arr[last]) if not np.isnan(atr_arr[last]) else None,
            'ema20_cross_days': cd,
        }

        action = classify_fn(d)
        cd_v = d.get('ema20_cross_days')
        rsi_v = d.get('rsi')
        if cd_v and 0 < cd_v <= 10: sig = f'T1 {cd_v}d'
        elif rsi_v and rsi_v < 50: sig = f'T3 RSI{rsi_v:.0f}'
        elif action == 'EXIT': sig = 'RSI>75/EMA死叉'
        else: sig = '—'

        # ticker 顯示去 .TW/.TWO
        display_ticker = ticker.replace('.TW', '').replace('.TWO', '')

        row = {
            'ticker': display_ticker,
            'name': name_map.get(display_ticker, display_ticker),
            'close': round(d['close'], 2),
            'rsi': round(rsi_v, 1) if rsi_v else None,
            'ema20_cross_days': cd,
            'delta': 0,  # 雲端版沒回測 delta，給 0
            'sig': sig,
            't3_confidence': t3_score,
            't3_confidence_hits': t3_hits,
        }
        if action == 'ENTRY': entry.append(row)
        elif action == 'EXIT': exit_.append(row)
        elif action == 'HOLD': hold.append(row)
        else: wait.append(row)
        last_dates.append(df.index[last].strftime('%Y-%m-%d'))

    return entry, exit_, hold, wait, last_dates


def update_tw():
    """更新台股 TOP 200 訊號"""
    print("🇹🇼 更新 TW TOP 200 訊號（雲端版）")
    print("=" * 60)

    # 讀 TOP 200 清單
    if not Path('vwap_applicable.json').exists():
        print("❌ vwap_applicable.json 不存在")
        return False
    tier_data = json.load(open('vwap_applicable.json', encoding='utf-8'))
    top200 = sorted([t for t, info in tier_data.items()
                     if info.get('tier') == 'TOP'])
    print(f"  TOP 200 清單: {len(top200)} 檔")

    # 名稱
    name_map = {}
    if Path('tw_stock_list.json').exists():
        data = json.load(open('tw_stock_list.json', encoding='utf-8'))
        if isinstance(data, dict):
            if 'tickers' in data: data = data['tickers']
            for k, v in data.items():
                if isinstance(v, dict):
                    name_map[k] = v.get('name', '')
    try:
        import twstock
        for k, info in twstock.codes.items():
            if not name_map.get(k) and hasattr(info, 'name'):
                name_map[k] = info.name
    except Exception:
        pass

    # yfinance 抓取（加 .TW 後綴）
    yf_tickers = [f'{t}.TW' for t in top200]
    print(f"\n📥 yfinance 抓取 {len(yf_tickers)} 檔（period=1y）...")
    t0 = time.time()
    df_dict = _fetch_batch(yf_tickers, period='1y')
    print(f"  完成 {time.time()-t0:.1f}s，成功 {len(df_dict)}/{len(yf_tickers)}")

    # 處理
    entry, exit_, hold, wait, last_dates = _process(df_dict, _classify_tw, name_map)

    if not last_dates:
        print("❌ 處理失敗，保留現有 JSON")
        return False

    # 排序（依 cross_days/rsi 簡單排序）
    entry.sort(key=lambda x: -(x.get('ema20_cross_days') or 0))
    exit_.sort(key=lambda x: x.get('rsi') or 0, reverse=True)
    hold.sort(key=lambda x: -(x.get('rsi') or 0))

    out = {
        'updated_at': max(last_dates),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'top200_total': len(top200),
        'entry': entry,
        'exit': exit_,
        'hold': hold,
        'wait_count': len(wait),
        'source': 'cloud (yfinance live)',
    }
    with open('top200_signals.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"\n📊 TW TOP 200 即時掃描:")
    print(f"  📅 資料截至：{out['updated_at']}")
    print(f"  🚀 進場：{len(entry)}")
    print(f"  🚪 出倉：{len(exit_)}")
    print(f"  📌 持倉：{len(hold)}")
    print(f"  ⏸  觀望：{len(wait)}")
    print(f"\n✅ 寫入 top200_signals.json")
    return True


def update_us():
    """更新美股 TOP 100 訊號"""
    print("\n🇺🇸 更新 US TOP 100 訊號（雲端版）")
    print("=" * 60)

    # 讀 TOP 清單（us_applicable.json）
    if not Path('us_applicable.json').exists():
        print("❌ us_applicable.json 不存在 — 請先在本機跑 update_us_signals.py 一次")
        return False
    us_tier = json.load(open('us_applicable.json', encoding='utf-8'))
    us_top = sorted([t for t, info in us_tier.items()
                     if info.get('tier') == 'TOP'])
    print(f"  US TOP 清單: {len(us_top)} 檔")

    # yfinance 抓取
    print(f"\n📥 yfinance 抓取 {len(us_top)} 檔...")
    t0 = time.time()
    df_dict = _fetch_batch(us_top, period='1y')
    print(f"  完成 {time.time()-t0:.1f}s，成功 {len(df_dict)}/{len(us_top)}")

    # 處理（US 用 _classify_us）
    entry, exit_, hold, wait, last_dates = _process(df_dict, _classify_us)

    if not last_dates:
        print("❌ 處理失敗，保留現有 JSON")
        return False

    entry.sort(key=lambda x: -(x.get('ema20_cross_days') or 0))
    exit_.sort(key=lambda x: x.get('rsi') or 0, reverse=True)
    hold.sort(key=lambda x: -(x.get('rsi') or 0))

    out = {
        'updated_at': max(last_dates),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'top_total': len(us_top),
        'entry': entry,
        'exit': exit_,
        'hold': hold,
        'wait_count': len(wait),
        'source': 'cloud (yfinance live)',
    }
    with open('us_top200_signals.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"\n📊 US TOP 100 即時掃描:")
    print(f"  📅 資料截至：{out['updated_at']}")
    print(f"  🚀 進場：{len(entry)}")
    print(f"  🚪 出倉：{len(exit_)}")
    print(f"  📌 持倉：{len(hold)}")
    print(f"  ⏸  觀望：{len(wait)}")
    print(f"\n✅ 寫入 us_top200_signals.json")
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tw', action='store_true', help='只更新台股')
    p.add_argument('--us', action='store_true', help='只更新美股')
    args = p.parse_args()

    do_tw = args.tw or not (args.tw or args.us)  # 預設都跑
    do_us = args.us or not (args.tw or args.us)

    t0 = time.time()
    if do_tw: update_tw()
    if do_us: update_us()
    print(f"\n⏱  總耗時 {time.time()-t0:.1f}s")


if __name__ == '__main__':
    main()
