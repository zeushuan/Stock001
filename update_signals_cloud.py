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


def _detect_alerts(df, last_idx, ticker_market='tw'):
    """🆕 v9.10y：偵測強警報 + 即將觸發
    回傳 list[(level, side, tag, desc)]"""
    try:
        from kline_patterns import detect_recent
    except ImportError:
        return []
    if df is None or len(df) < 60: return []
    alerts = []
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    c = df['Close'].values
    v = df['Volume'].values
    n = len(df)
    rsi = df['rsi'].values
    adx = df['adx'].values

    if last_idx < 0: last_idx = n + last_idx
    rsi_v = float(rsi[last_idx]) if not np.isnan(rsi[last_idx]) else None
    adx_v = float(adx[last_idx]) if not np.isnan(adx[last_idx]) else None
    close_v = float(c[last_idx])
    if rsi_v is None or adx_v is None: return []

    adx_5d = adx[last_idx-5] if last_idx >= 5 and not np.isnan(adx[last_idx-5]) else adx_v
    adx_rising = adx_v > adx_5d
    adx_falling = adx_v < adx_5d

    # 60d high/low + SMA200 + vol_avg
    h60 = float(h[max(0, last_idx-60):last_idx+1].max()) if last_idx >= 1 else close_v
    l60 = float(l[max(0, last_idx-60):last_idx+1].min()) if last_idx >= 1 else close_v
    from_high = (h60 - close_v) / h60 * 100 if h60 > 0 else 99
    from_low = (close_v - l60) / l60 * 100 if l60 > 0 else 99
    sma200 = float(np.mean(c[max(0, last_idx-199):last_idx+1])) if last_idx >= 100 else close_v
    sma200_pct = (close_v / sma200 - 1) * 100 if sma200 > 0 else 0
    extended_down = sma200_pct < -25
    vol_avg = float(np.mean(v[max(0, last_idx-59):last_idx+1])) if last_idx >= 30 else 0
    vol_dry = (vol_avg > 0 and v[last_idx] / vol_avg < 0.7)

    # K 線型態
    patterns = detect_recent(df.iloc[:last_idx+1], lookback=3)
    recent = {p['name']: p['days_ago'] for p in patterns if p['days_ago'] <= 1}
    recent_3d = {p['name']: p['days_ago'] for p in patterns if p['days_ago'] <= 3}

    # 計算當天 drop_30d（用於 quality_score）
    drop_30d_v = ((close_v - c[max(0, last_idx-30)]) / c[max(0, last_idx-30)] * 100
                   if last_idx >= 30 and c[last_idx-30] > 0 else 0)

    # ── 看多警報 ──
    # ★★★★★ 倒鎚 + RSI≤25 + ADX↑
    # OOS 2024+ 驗證：71.8% 漲 / +9.35% 30d / PF 5.5（訊號級 alpha 反而強化，無過擬合）
    # 注意：投組級在 1M/10倉 下 OOS 已 CAGR 0%（容量瓶頸）— 適合大資金或多倉位
    # quality_score = -drop_30d_v（跌越深越優先，priority research 證實 +84% CAGR）
    if 'INV_HAMMER' in recent and rsi_v <= 25 and adx_rising:
        alerts.append({'level': 5, 'side': 'bull',
            'tag': f'倒鎚 + RSI {rsi_v:.0f}≤25 + ADX↑',
            'expect': '+9.35% 30d (71.8% 漲, OOS 驗證)',
            'quality_score': float(-drop_30d_v)})  # 跌越深 → score 越高
    # 即將觸發：倒鎚 + RSI 26-30 + ADX↑
    elif 'INV_HAMMER' in recent and 25 < rsi_v <= 30 and adx_rising:
        alerts.append({'level': 'imm_bull', 'side': 'bull',
            'tag': f'即將: 倒鎚 + RSI {rsi_v:.0f}（差 {rsi_v-25:.1f} 點到極強）',
            'expect': 'RSI 再降到≤25 即達 ★★★★★',
            'quality_score': float(-drop_30d_v)})
    elif 'INV_HAMMER' in recent and rsi_v <= 25:  # ADX 沒升
        alerts.append({'level': 'imm_bull', 'side': 'bull',
            'tag': f'即將: 倒鎚 + RSI≤25（ADX 未轉強）',
            'expect': 'ADX 上升即達 ★★★★★',
            'quality_score': float(-drop_30d_v)})
    # ★★★★ 倒鎚 + 跌深
    elif 'INV_HAMMER' in recent and extended_down:
        alerts.append({'level': 4, 'side': 'bull',
            'tag': f'倒鎚 + 距 SMA200 {sma200_pct:+.0f}%',
            'expect': '+7.85% 30d (64.5% 漲)',
            'quality_score': float(-sma200_pct)})  # 距 SMA200 越負越優先
    # ★★★ 底部十字星 + RSI≤25 + ADX↑
    elif 'DOJI' in recent and rsi_v <= 25 and adx_rising and from_low < 10:
        alerts.append({'level': 3, 'side': 'bull',
            'tag': f'底部十字星 + RSI {rsi_v:.0f}≤25 + ADX↑',
            'expect': '+7.02% 30d (67.4% 漲)',
            'quality_score': float(50 - rsi_v)})  # RSI 越低越優先

    # 🆕 v9.11：★★ T1 即將上穿（V7: P1 + ADX≥22）— Walk-forward OOS 主力策略
    # In-sample 2020-2023: 51% 漲 / +2.61% 30d
    # OOS 2024+: 45% 漲 / +1.36% 30d（訊號級輕微 decay）
    # 投組 1M/10倉 hold 30d OOS CAGR +14.78%（**OOS 唯一贏家**：訊號密集容量友善）
    # 注意：T1_V7 hold 60d 是過擬合 trap（Sharpe 1.92→0.17）— 切勿用 60d
    # quality_score = (50 - RSI)（priority research：T1_V7 用 rsi_low 最佳）
    e20_arr = df['e20'].values if 'e20' in df.columns else None
    e60_arr = df['e60'].values if 'e60' in df.columns else None
    if e20_arr is not None and e60_arr is not None and last_idx >= 2:
        e20_now = float(e20_arr[last_idx]) if not np.isnan(e20_arr[last_idx]) else None
        e60_now = float(e60_arr[last_idx]) if not np.isnan(e60_arr[last_idx]) else None
        if (e20_now and e60_now
            and e20_now > e60_now              # 多頭排列
            and close_v < e20_now               # 還沒上穿
            and (e20_now - close_v) / e20_now * 100 <= 1.0  # 距 EMA20 ≤ 1%
            and c[last_idx] > c[last_idx-1]    # 連 2 天上漲
            and c[last_idx-1] > c[last_idx-2]
            and adx_v >= 22):                   # ADX 趨勢強（V7 額外條件）
            dist_pct = (e20_now - close_v) / e20_now * 100
            alerts.append({'level': 2, 'side': 'bull',
                'tag': f'T1 即將上穿（距 EMA20 {dist_pct:.2f}% + 連2漲 + ADX≥22）',
                'expect': '投組 OOS CAGR +14.78% / 訊號級 45% 漲 / +1.36% 30d',
                'quality_score': float(50 - rsi_v)})  # RSI 越低越優先

    # ── 看空警報 ──
    # ★★★★ 三隻烏鴉 + 距高<5% + 量縮
    # quality_score = -from_high（越接近高點越優先）
    if 'THREE_CROWS' in recent and from_high < 5 and vol_dry:
        alerts.append({'level': 4, 'side': 'bear',
            'tag': f'三隻烏鴉 + 距高 {from_high:.1f}% + 量縮',
            'expect': '-1.26% 30d (71% 跌)',
            'quality_score': float(-from_high)})
    # 即將：三隻烏鴉 + 距高 5-10% + 量縮
    elif 'THREE_CROWS' in recent and 5 <= from_high < 10 and vol_dry:
        alerts.append({'level': 'imm_bear', 'side': 'bear',
            'tag': f'即將: 三隻烏鴉 + 距高 {from_high:.1f}%（差到 <5%）',
            'expect': '價再升即達 ★★★★',
            'quality_score': float(-from_high)})
    # ★★★ 空頭吞噬 + RSI≥75 + ADX↓
    # quality_score = RSI - 50（RSI 越高越優先）
    if 'BEAR_ENGULF' in recent and rsi_v >= 75 and adx_falling:
        alerts.append({'level': 3, 'side': 'bear',
            'tag': f'空頭吞噬 + RSI {rsi_v:.0f}≥75 + ADX↓',
            'expect': '-0.24% 30d (60% 跌)',
            'quality_score': float(rsi_v - 50)})
    # 即將：空頭吞噬 + RSI 70-74 + ADX↓
    elif 'BEAR_ENGULF' in recent and 70 <= rsi_v < 75 and adx_falling:
        alerts.append({'level': 'imm_bear', 'side': 'bear',
            'tag': f'即將: 空頭吞噬 + RSI {rsi_v:.0f}（差 {75-rsi_v:.1f} 點到 75）',
            'expect': 'RSI 再升即達 ★★★',
            'quality_score': float(rsi_v - 50)})
    # ★★ 黃昏之星 + RSI≥75
    if 'EVENING_STAR' in recent and rsi_v >= 75:
        alerts.append({'level': 2, 'side': 'bear',
            'tag': f'黃昏之星 + RSI {rsi_v:.0f}≥75',
            'expect': '-0.54% 30d (70% 跌)',
            'quality_score': float(rsi_v - 50)})

    return alerts


def _process(df_dict, classify_fn, name_map=None):
    """從每股 DataFrame → 計算指標 → 分類 → 產出 row"""
    name_map = name_map or {}
    entry, exit_, hold, wait = [], [], [], []
    last_dates = []
    all_alerts = []  # 🆕 v9.10y

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

        # 🆕 v9.10m：用「60 日價格動量」當 delta（取代 0）
        # 雲端版沒 backtest，但 60d 動量是有意義的近期表現指標
        if len(close_arr) >= 60:
            close_60d_ago = close_arr[-60]
            if close_60d_ago > 0:
                mom_60d = (close_arr[-1] - close_60d_ago) / close_60d_ago * 100
            else:
                mom_60d = 0
        else:
            mom_60d = 0

        # 🆕 v9.10y：偵測警報
        stock_alerts = _detect_alerts(df, last)
        if stock_alerts:
            for al in stock_alerts:
                all_alerts.append({
                    'ticker': display_ticker,
                    'name': name_map.get(display_ticker, display_ticker),
                    'close': round(d['close'], 2),
                    **al,
                })

        row = {
            'ticker': display_ticker,
            'name': name_map.get(display_ticker, display_ticker),
            'close': round(d['close'], 2),
            'rsi': round(rsi_v, 1) if rsi_v else None,
            'ema20_cross_days': cd,
            'delta': round(mom_60d, 1),  # 60 日價格動量 (%)
            'sig': sig,
            't3_confidence': t3_score,
            't3_confidence_hits': t3_hits,
            'alerts': stock_alerts,  # 🆕 警報資料
        }
        if action == 'ENTRY': entry.append(row)
        elif action == 'EXIT': exit_.append(row)
        elif action == 'HOLD': hold.append(row)
        else: wait.append(row)
        last_dates.append(df.index[last].strftime('%Y-%m-%d'))

    return entry, exit_, hold, wait, last_dates, all_alerts


def _append_alert_history(market, alerts, alert_date, hist_path='alert_history.json'):
    """🆕 v9.11：把當天的 alerts 加進歷史檔，後續由 update_alert_outcomes.py
    在 5/15/30 天後回算實際漲跌幅，用來計算 live 命中率。

    重複日期 + ticker + level + side 不會重加（idempotent）。
    """
    if not alerts:
        return 0

    if Path(hist_path).exists():
        try:
            hist = json.load(open(hist_path, encoding='utf-8'))
            if not isinstance(hist, dict) or 'alerts' not in hist:
                hist = {'alerts': []}
        except Exception:
            hist = {'alerts': []}
    else:
        hist = {'alerts': []}

    # 既存的 (date, ticker, level, side) 集合 → 避免重複加
    existing = set()
    for a in hist['alerts']:
        existing.add((a['alert_date'], a['ticker'],
                      str(a.get('level')), a.get('side')))

    added = 0
    for al in alerts:
        key = (alert_date, al['ticker'], str(al.get('level')), al.get('side'))
        if key in existing:
            continue
        existing.add(key)  # 同一輪內也要 dedup
        hist['alerts'].append({
            'alert_date': alert_date,
            'ticker': al['ticker'],
            'name': al.get('name', ''),
            'market': market,
            'level': al.get('level'),
            'side': al.get('side'),
            'tag': al.get('tag', ''),
            'expect': al.get('expect', ''),
            'entry_price': al.get('close'),
            'outcomes': {
                '5d':  {'close': None, 'ret_pct': None, 'checked_at': None},
                '15d': {'close': None, 'ret_pct': None, 'checked_at': None},
                '30d': {'close': None, 'ret_pct': None, 'checked_at': None},
            },
        })
        added += 1

    hist['last_updated'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    hist['total_alerts'] = len(hist['alerts'])

    with open(hist_path, 'w', encoding='utf-8') as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)

    return added


def update_tw():
    """更新台股 TOP 200 訊號"""
    print("🇹🇼 更新 TW TOP 200 訊號（雲端版）")
    print("=" * 60)
    # 🆕 診斷資訊
    import os as _os
    print(f"  cwd = {_os.getcwd()}")
    print(f"  __file__ = {__file__}")
    for f in ['vwap_applicable.json', 'tw_stock_list.json', 'top200_signals.json']:
        p = Path(f)
        if p.exists():
            print(f"  ✓ {f}: {p.stat().st_size} bytes")
        else:
            print(f"  ✗ {f}: NOT FOUND")

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
    entry, exit_, hold, wait, last_dates, all_alerts = _process(df_dict, _classify_tw, name_map)

    if not last_dates:
        print("❌ 處理失敗，保留現有 JSON")
        return False

    # 排序（依 cross_days/rsi 簡單排序）
    entry.sort(key=lambda x: -(x.get('ema20_cross_days') or 0))
    exit_.sort(key=lambda x: x.get('rsi') or 0, reverse=True)
    hold.sort(key=lambda x: -(x.get('rsi') or 0))

    # 🆕 v9.10y：警報排序（強警報優先）+ v9.11：同 level 內按 quality_score 降序
    # priority sweep 證實：drop_deep / rsi_low / from_high 排序能 +84% CAGR
    def _sort_key(a):
        lv = a['level']
        if lv == 5: lv_rank = 0
        elif lv == 4: lv_rank = 1
        elif lv == 3: lv_rank = 2
        elif lv == 2: lv_rank = 3
        elif lv == 'imm_bull': lv_rank = 4
        elif lv == 'imm_bear': lv_rank = 5
        else: lv_rank = 6
        # 同 level 內按 quality_score 降序（高品質優先）
        return (lv_rank, -(a.get('quality_score') or 0))
    all_alerts.sort(key=_sort_key)

    out = {
        'updated_at': max(last_dates),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'top200_total': len(top200),
        'entry': entry,
        'exit': exit_,
        'hold': hold,
        'wait_count': len(wait),
        'alerts': all_alerts,  # 🆕 警報列表
        'source': 'cloud (yfinance live)',
    }
    with open('top200_signals.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    # 🆕 v9.11：append 進歷史檔（後續由 update_alert_outcomes.py 回算 5/15/30d 命中率）
    n_added = _append_alert_history('tw', all_alerts, out['updated_at'])

    print(f"\n📊 TW TOP 200 即時掃描:")
    print(f"  📅 資料截至：{out['updated_at']}")
    print(f"  🚀 進場：{len(entry)}")
    print(f"  🚪 出倉：{len(exit_)}")
    print(f"  📌 持倉：{len(hold)}")
    print(f"  ⏸  觀望：{len(wait)}")
    print(f"  📝 alert_history: 新增 {n_added} 筆")
    print(f"\n✅ 寫入 top200_signals.json")
    return True


def update_us():
    """更新美股 TOP 100 訊號"""
    print("\n🇺🇸 更新 US TOP 100 訊號（雲端版）")
    print("=" * 60)
    # 🆕 診斷資訊
    import os as _os
    print(f"  cwd = {_os.getcwd()}")
    print(f"  __file__ = {__file__}")
    for f in ['us_applicable.json', 'us_top200_signals.json']:
        p = Path(f)
        if p.exists():
            print(f"  ✓ {f}: {p.stat().st_size} bytes")
        else:
            print(f"  ✗ {f}: NOT FOUND")

    # 讀 TOP 清單（us_applicable.json）
    if not Path('us_applicable.json').exists():
        print("❌ us_applicable.json 不存在 — 請先在本機跑 update_us_signals.py 一次")
        return False
    us_tier = json.load(open('us_applicable.json', encoding='utf-8'))
    us_top = sorted([t for t, info in us_tier.items()
                     if info.get('tier') == 'TOP'])
    print(f"  US TOP 清單: {len(us_top)} 檔")

    # 🆕 v9.10h：載入 US 公司名稱 map（從 us_full_tickers.json 的 detail）
    name_map = {}
    if Path('us_full_tickers.json').exists():
        try:
            full = json.load(open('us_full_tickers.json', encoding='utf-8'))
            for x in full.get('detail', []):
                sym = x.get('symbol', '')
                nm = x.get('name', '')
                # 簡化名稱：取 " - " 或 " Common" 之前
                for sep in [' - ', ' Common ', ' Class ', ' Ordinary ']:
                    if sep in nm:
                        nm = nm.split(sep)[0]
                        break
                if sym: name_map[sym] = nm[:40]  # 截斷至 40 字
        except Exception as e:
            print(f"  ⚠️ name_map 載入失敗: {e}")
    print(f"  名稱 map: {len(name_map)} 檔")

    # yfinance 抓取
    print(f"\n📥 yfinance 抓取 {len(us_top)} 檔...")
    t0 = time.time()
    df_dict = _fetch_batch(us_top, period='1y')
    print(f"  完成 {time.time()-t0:.1f}s，成功 {len(df_dict)}/{len(us_top)}")

    # 處理（US 用 _classify_us）
    entry, exit_, hold, wait, last_dates, all_alerts = _process(df_dict, _classify_us, name_map)

    if not last_dates:
        print("❌ 處理失敗，保留現有 JSON")
        return False

    entry.sort(key=lambda x: -(x.get('ema20_cross_days') or 0))
    exit_.sort(key=lambda x: x.get('rsi') or 0, reverse=True)
    hold.sort(key=lambda x: -(x.get('rsi') or 0))

    # 🆕 v9.10y：警報排序 + v9.11：同 level 內按 quality_score 降序
    def _sort_key_us(a):
        lv = a['level']
        if lv == 5: lv_rank = 0
        elif lv == 4: lv_rank = 1
        elif lv == 3: lv_rank = 2
        elif lv == 2: lv_rank = 3
        elif lv == 'imm_bull': lv_rank = 4
        elif lv == 'imm_bear': lv_rank = 5
        else: lv_rank = 6
        return (lv_rank, -(a.get('quality_score') or 0))
    all_alerts.sort(key=_sort_key_us)

    out = {
        'updated_at': max(last_dates),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'top_total': len(us_top),
        'entry': entry,
        'exit': exit_,
        'hold': hold,
        'wait_count': len(wait),
        'alerts': all_alerts,  # 🆕 警報列表
        'source': 'cloud (yfinance live)',
    }
    with open('us_top200_signals.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    # 🆕 v9.11：append 進歷史檔
    n_added = _append_alert_history('us', all_alerts, out['updated_at'])

    print(f"\n📊 US TOP 100 即時掃描:")
    print(f"  📅 資料截至：{out['updated_at']}")
    print(f"  🚀 進場：{len(entry)}")
    print(f"  🚪 出倉：{len(exit_)}")
    print(f"  📌 持倉：{len(hold)}")
    print(f"  ⏸  觀望：{len(wait)}")
    print(f"  📝 alert_history: 新增 {n_added} 筆")
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
