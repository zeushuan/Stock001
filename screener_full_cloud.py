"""☁️ 全市場篩選器預計算（cloud 版，v9.13）
=================================================
不依賴 data_cache，用 yfinance batch 抓資料 → 跑全部 34 filter →
輸出 screener_results.json（tv_app cloud 環境直接讀）

執行：
  python screener_full_cloud.py        # 兩市場
  python screener_full_cloud.py --tw
  python screener_full_cloud.py --us

預估時間：35-40 分鐘（同 weekly_full_scan）
"""
import sys, json, time, argparse
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
import ta

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

US_ETF_EXCLUDE = {
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT','EFA','AGG',
    'LQD','HYG','GLD','SLV','USO','UNG','UCO','SCO','EEM','EWJ','EWZ','EWY',
    'FXI','MCHI','XLK','XLF','XLV','XLE','XLY','XLP','XLI','XLU','XLB','XLC',
    'SMH','SOXX','IBB','TQQQ','SQQQ','SOXL','SOXS','UPRO','SPXU','VXX','UVXY',
    'ARKK','ARKG','ARKF','ARKW','ARKQ',
}


def _calc_ind(df):
    if df is None or len(df) < 60: return None
    df = df.copy()
    df['e10'] = ta.trend.ema_indicator(df['Close'], window=10)
    df['e20'] = ta.trend.ema_indicator(df['Close'], window=20)
    df['e60'] = ta.trend.ema_indicator(df['Close'], window=60)
    df['rsi'] = ta.momentum.rsi(df['Close'], window=14)
    df['adx'] = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    df['atr'] = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], 14)
    return df


def _fetch_batch(tickers, period='14mo', is_tw=False):
    """🆕 v9.13：TW 用較小 batch (25) + 失敗時 retry，避免 GitHub Actions 環境下大批 .TW 抓不到"""
    out = {}
    BATCH = 25 if is_tw else 50  # TW 用較小 batch
    failed_tickers = []
    for i in range(0, len(tickers), BATCH):
        batch = tickers[i:i+BATCH]
        success = False
        for retry in range(2):  # 最多 2 次嘗試
            try:
                df = yf.download(batch, period=period, interval='1d',
                                 auto_adjust=False, progress=False,
                                 group_by='ticker', threads=True)
                if df is None or df.empty:
                    if retry == 0:
                        time.sleep(2); continue
                    break
                batch_count = 0
                for t in batch:
                    try:
                        if len(batch) == 1: sub = df
                        else: sub = df[t]
                        sub = sub.dropna(how='all')
                        if len(sub) >= 60:
                            out[t] = sub
                            batch_count += 1
                    except Exception: continue
                success = True
                break
            except Exception as e:
                if retry == 0:
                    print(f"  ⚠️ batch {i//BATCH+1} 第 1 次失敗 (retry): {str(e)[:60]}")
                    time.sleep(3)
                else:
                    print(f"  ❌ batch {i//BATCH+1} 兩次失敗: {str(e)[:80]}")
        if not success:
            failed_tickers.extend(batch)
        if (i // BATCH + 1) % 5 == 0:
            print(f"  📦 進度 {i//BATCH+1}/{(len(tickers)+BATCH-1)//BATCH}: 累計 {len(out)} 檔")

    # 失敗的 ticker 個別重試
    if failed_tickers:
        print(f"  🔁 個別重試 {len(failed_tickers)} 檔失敗...")
        for t in failed_tickers[:30]:  # 限 30 個避免太慢
            try:
                df = yf.download(t, period=period, interval='1d',
                                 auto_adjust=False, progress=False)
                if df is not None and len(df) >= 60:
                    out[t] = df.dropna(how='all')
            except Exception: continue
    return out


def get_full_universe(market):
    """🆕 v9.20.11：universe 擴大涵蓋
    - TW: 含 ETF 00* + 5-6 位數 active ETF + 4 位數個股
    - US: 1-5 字元字母（主流 leveraged ETF 排除）"""
    if market == 'tw':
        if Path('tw_stock_list.json').exists():
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            # 包含 4 位數個股 + ETF（含 00 開頭、5-6 位數 active ETF）
            return sorted([t for t, info in d.items()
                           if isinstance(info, dict)
                           and t and t[0].isdigit() and 4 <= len(t) <= 7])
    elif market == 'us':
        # 🆕 v9.14：用 us_full_tickers.json (5629 檔全市場，原本 us_applicable 只 555)
        if Path('us_full_tickers.json').exists():
            d = json.load(open('us_full_tickers.json', encoding='utf-8'))
            tickers = []
            for x in d.get('detail', []):
                t = x.get('symbol', '').strip()
                if t.isalpha() and t.isupper() and 1 <= len(t) <= 5:
                    if t not in US_ETF_EXCLUDE:
                        tickers.append(t)
            return sorted(tickers)
        # fallback: us_applicable.json
        if Path('us_applicable.json').exists():
            d = json.load(open('us_applicable.json', encoding='utf-8'))
            return sorted([t for t, info in d.items()
                           if isinstance(info, dict)
                           and info.get('tier') != 'NA'
                           and t.isalpha() and t.isupper()
                           and 1 <= len(t) <= 5
                           and t not in US_ETF_EXCLUDE])
    return []


def _fetch_tw_via_official(tickers):
    """🆕 v9.14：直接用 TWSE/TPEX 官方 API 抓全市場單日資料（200 個交易日）
    不依賴 twstock 套件，純 HTTP 請求 → 免被 GitHub runner 阻擋
    速度：~6 分鐘抓全 TW 200 天歷史"""
    # 🆕 v9.20.2：TW SEPA 需 252+ 天（52w 高低 + sma200_30d_ago）
    from fetch_tw_official import fetch_all_tw_history
    print(f"  📥 TWSE/TPEX 官方 API（不依賴 twstock，200 個交易日）...")
    all_data = fetch_all_tw_history(days=280)
    # 過濾只回傳被請求的 ticker（保護 universe 一致性）
    requested = set(tickers)
    out = {t: df for t, df in all_data.items() if t in requested}
    print(f"  TW 全市場抓到 {len(all_data)} 檔，universe 命中 {len(out)} 檔")
    return out


def fetch_market_data(market, tickers):
    """🆕 v9.25.5：抽出 fetch 邏輯，供 unified_cron_scan 共用 df_dict"""
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    print(f"\n{flag} {market.upper()} fetch: {len(tickers)} 檔")
    t0 = time.time()
    if market == 'tw':
        df_dict = _fetch_tw_via_official(tickers)
    else:
        print(f"  📥 yfinance batch（period=14mo, batch=50）...")
        df_dict = _fetch_batch(tickers, period='14mo', is_tw=False)
    print(f"  完成 {time.time()-t0:.1f}s，成功 {len(df_dict)}/{len(tickers)}")

    # 抓索引一次（給 Pass 2.5 用）
    idx_yf = '^GSPC' if market == 'us' else '^TWII'
    if idx_yf not in df_dict:
        try:
            import yfinance as yf, io, contextlib, logging
            logging.getLogger('yfinance').setLevel(logging.CRITICAL)
            with contextlib.redirect_stderr(io.StringIO()), \
                 contextlib.redirect_stdout(io.StringIO()):
                _idx = yf.download(idx_yf, period='14mo', interval='1d',
                                     progress=False, auto_adjust=True, timeout=30)
            if _idx is not None and len(_idx) > 100:
                if isinstance(_idx.columns, pd.MultiIndex):
                    _idx.columns = _idx.columns.get_level_values(0)
                df_dict[idx_yf] = _idx
                print(f"  抓索引 {idx_yf} OK ({len(_idx)} bars)")
        except Exception as e:
            print(f"  抓索引 {idx_yf} 失敗: {type(e).__name__}")
    return df_dict


def scan_market(market, tickers, name_map, df_dict=None):
    """掃描某市場，跑全部 filter

    🆕 v9.25.5：df_dict 參數 — 若已有預抓資料就直接用，省 fetch 時間
    """
    flag = '🇹🇼' if market == 'tw' else '🇺🇸'
    if df_dict is None:
        df_dict = fetch_market_data(market, tickers)
    else:
        print(f"\n{flag} {market.upper()} 使用預抓 df_dict ({len(df_dict)} 檔)")

    # 算指標 + 跑所有 filter
    print(f"  🧮 計算指標 + 跑 filter...")
    t0 = time.time()
    from screener_filters import _get_state, FILTERS
    # 🆕 v9.19：RS Rating 需要 universe-wide 計算 → 2-pass
    from sepa_vcp import compute_rs_ratings

    min_vol = 500_000 if market == 'tw' else 1_000_000
    min_price = 5.0

    # filter_name → list of matching tickers
    by_filter = {fname: [] for fname in FILTERS}

    # ── Pass 1：算 state + 收集 returns（給 RS 用）─────────
    print(f"  Pass 1: state + returns ...")
    ticker_states = {}     # ticker → state dict
    ticker_returns = {}    # ticker → {'13w', '26w', '39w', '52w'}

    for yf_t, df in df_dict.items():
        ticker = yf_t.replace('.TW', '') if (market == 'tw' and '.TW' in yf_t) else yf_t
        df_ind = _calc_ind(df)
        if df_ind is None: continue
        try:
            v_arr = df_ind['Volume'].values
            close = float(df_ind['Close'].iloc[-1])
            if close < min_price: continue
            if len(v_arr) >= 60:
                avg_vol = float(np.mean(v_arr[-60:]))
                if avg_vol < min_vol: continue
        except Exception:
            continue

        state = _get_state(df_ind, market)
        if state is None: continue
        ticker_states[ticker] = state
        ticker_returns[ticker] = {
            '13w': state.get('returns_13w', 0),
            '26w': state.get('returns_26w', 0),
            '39w': state.get('returns_39w', 0),
            '52w': state.get('returns_52w', 0),
        }

    # ── Pass 2：算 RS Rating ───────────────────────
    print(f"  Pass 2: 計算 RS Ratings ({len(ticker_returns)} tickers)...")
    rs_ratings = compute_rs_ratings(ticker_returns)
    # 注入到 state
    for ticker, rs in rs_ratings.items():
        if ticker in ticker_states:
            ticker_states[ticker]['rs_rating'] = rs

    # ── 🆕 v9.24 Pass 2.5：RS Leading High（紫色點訊號）──
    # 🐛 v9.25.4：取消 yfinance fallback — 在 cron 中容易觸發 rate limit 卡住整個 screener
    try:
        from scanners.rs_leading_high import (detect_rs_leading_high,
                                                apply_quality_filters, score_signal)
        idx_yf = '^GSPC' if market == 'us' else '^TWII'
        # 1. 先試 df_dict
        idx_df = df_dict.get(idx_yf) or df_dict.get('SPY')
        # 2. 試 data_cache（推薦 — 不會觸發 rate limit）
        if idx_df is None:
            try:
                import data_loader as _dl
                idx_df = _dl.load_from_cache(idx_yf)
                if idx_df is None and market == 'us':
                    idx_df = _dl.load_from_cache('SPY')
            except Exception:
                idx_df = None
        # 3. 仍然沒有 → 跳過 Pass 2.5（不再 yfinance fallback）
        if idx_df is None:
            print(f"  Pass 2.5 skipped: 找不到 {idx_yf} 資料（df_dict + data_cache 都無）")
            raise StopIteration  # 用 exception 跳出 try block

        if idx_df is not None and len(idx_df) > 100:
            idx_close = idx_df['Close'].astype(float)

            # 🆕 v9.27 Pass 2.6：Beta 計算（共用同一份 idx_close）
            try:
                from beta_helpers import compute_beta, classify_beta
                _beta_n = 0
                for yf_t, df in df_dict.items():
                    ticker = yf_t.replace('.TW', '') if (market == 'tw' and '.TW' in yf_t) else yf_t
                    if ticker not in ticker_states: continue
                    if df is None or len(df) < 70: continue
                    try:
                        b = compute_beta(df['Close'], idx_close, lookback=60)
                        if b is not None:
                            ticker_states[ticker]['beta_60d'] = b
                            ticker_states[ticker]['beta_class'] = classify_beta(b)
                            _beta_n += 1
                    except Exception: continue
                print(f"  Pass 2.6 Beta: {_beta_n} tickers")
            except Exception as e:
                print(f"  Pass 2.6 Beta skipped: {type(e).__name__}: {e}")

            raw_sigs = []
            for yf_t, df in df_dict.items():
                ticker = yf_t.replace('.TW', '') if (market == 'tw' and '.TW' in yf_t) else yf_t
                if ticker not in ticker_states: continue
                if df is None or len(df) < 200: continue
                try:
                    sig = detect_rs_leading_high(
                        stock_prices=df['Close'].astype(float),
                        index_prices=idx_close,
                        stock_volumes=df['Volume'].astype(float),
                        ticker=ticker,
                        as_of_date=df.index[-1],
                    )
                    if sig is None: continue
                    if apply_quality_filters(sig, df['Close'], df['Volume'],
                                              market=('US' if market == 'us' else 'TW')):
                        raw_sigs.append((ticker, sig, df['Close']))
                except Exception: continue

            if raw_sigs:
                rs_slopes = [s.rs_slope_50d for _, s, _ in raw_sigs]
                ctx = {'rs_slopes': rs_slopes}
                for tk, sig, sp in raw_sigs:
                    recent = sp.tail(30)
                    if len(recent) >= 10 and recent.mean() > 0:
                        ctx[f'cv_{tk}'] = float(recent.std() / recent.mean())
                for tk, sig, _ in raw_sigs:
                    score_signal(sig, ctx)
                raw_sigs.sort(key=lambda x: -(x[1].quality_score or 0))
                for r, (tk, sig, _) in enumerate(raw_sigs):
                    ticker_states[tk]['rs_leading_high_passed'] = True
                    ticker_states[tk]['rs_leading_high_score'] = sig.quality_score
                    ticker_states[tk]['rs_leading_high_purple_dots'] = sig.purple_dot_count_recent
                    ticker_states[tk]['rs_leading_high_distance'] = round(sig.stock_distance_from_high_pct * 100, 2)
                    ticker_states[tk]['rs_leading_high_rank'] = r + 1
                    ticker_states[tk]['rs_leading_high_theme'] = sig.theme
            print(f"  Pass 2.5 RS Leading High: {len(raw_sigs)} signals")
    except StopIteration:
        pass   # 預期的跳出（idx_df 找不到）
    except Exception as e:
        print(f"  RS Leading High skipped: {type(e).__name__}: {e}")

    # ── Pass 3：跑 filter ─────────────────────────
    print(f"  Pass 3: 跑 filter ...")
    for ticker, state in ticker_states.items():
        for fname, fn in FILTERS.items():
            try:
                if fn(state):
                    by_filter[fname].append({
                        'ticker': ticker,
                        'name': name_map.get(ticker, ''),
                        'market': market,
                        'close': round(state['close'], 2),
                        'rsi': round(state['rsi'], 1) if state.get('rsi') else None,
                        'adx': round(state['adx'], 1) if state.get('adx') else None,
                        'is_bull': state['is_bull'],
                        'cross_days': state.get('cross_days'),
                        'pct_b': round(state['bb_pct_b'], 2) if state.get('bb_pct_b') is not None else None,
                        'from_high': round(state['from_high'], 1),
                        'from_low': round(state['from_low'], 1),
                        'imminent_dc': state.get('imminent_dc', False),
                        'date': state['date'],
                        # 🆕 v9.19 SEPA/VCP/RS
                        'rs_rating': state.get('rs_rating'),
                        'sepa_n_met': state.get('sepa_n_met', 0),
                        'vcp_is_vcp': state.get('vcp_is_vcp', False),
                        'vcp_near_pivot_pct': state.get('vcp_near_pivot_pct', 0),
                        # 🆕 v9.24 RS Leading High
                        'rs_leading_high_score': state.get('rs_leading_high_score'),
                        'rs_leading_high_purple_dots': state.get('rs_leading_high_purple_dots'),
                        'rs_leading_high_distance': state.get('rs_leading_high_distance'),
                        'rs_leading_high_rank': state.get('rs_leading_high_rank'),
                        'rs_leading_high_theme': state.get('rs_leading_high_theme'),
                        # 🆕 v9.27 Beta
                        'beta_60d': state.get('beta_60d'),
                        'beta_class': state.get('beta_class'),
                    })
            except Exception:
                continue

    print(f"  完成 {time.time()-t0:.1f}s")
    # 🆕 v9.20.5：回傳所有 tickers 的 RS Rating（不分是否在 filter 內）
    market_rs = {tk: round(s.get('rs_rating'), 1) for tk, s in ticker_states.items()
                 if s.get('rs_rating') is not None}
    return by_filter, market_rs


def load_name_maps(market):
    name_map = {}
    if market == 'tw' and Path('tw_stock_list.json').exists():
        try:
            d = json.load(open('tw_stock_list.json', encoding='utf-8'))
            for t, info in d.items():
                if isinstance(info, dict): name_map[t] = info.get('name', '')
        except Exception: pass
    elif market == 'us' and Path('us_full_tickers.json').exists():
        try:
            full = json.load(open('us_full_tickers.json', encoding='utf-8'))
            for x in full.get('detail', []):
                sym = x.get('symbol', '')
                nm = x.get('name', '')
                for sep in [' - ', ' Common ', ' Class ', ' Ordinary ']:
                    if sep in nm:
                        nm = nm.split(sep)[0]; break
                if sym: name_map[sym] = nm[:40]
        except Exception: pass
    return name_map


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tw', action='store_true')
    p.add_argument('--us', action='store_true')
    args = p.parse_args()
    do_tw = args.tw or not (args.tw or args.us)
    do_us = args.us or not (args.tw or args.us)

    output = {
        'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d'),
        'computed_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': '全市場篩選器預計算（34 filter × 全 universe）',
        'by_filter': {},  # filter_name → list of stocks
        'rs_ratings': {},  # 🆕 v9.20.5：所有 ticker 的 RS Rating
    }

    t_start = time.time()
    all_combined = {}
    all_rs = {}

    if do_tw:
        tw_uni = get_full_universe('tw')
        if tw_uni:
            tw_name = load_name_maps('tw')
            tw_results, tw_rs = scan_market('tw', tw_uni, tw_name)
            for fname, items in tw_results.items():
                all_combined.setdefault(fname, []).extend(items)
            all_rs.update(tw_rs)
    if do_us:
        us_uni = get_full_universe('us')
        if us_uni:
            us_name = load_name_maps('us')
            us_results, us_rs = scan_market('us', us_uni, us_name)
            for fname, items in us_results.items():
                all_combined.setdefault(fname, []).extend(items)
            all_rs.update(us_rs)

    output['by_filter'] = all_combined
    output['rs_ratings'] = all_rs

    out_file = 'screener_results.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    elapsed = time.time() - t_start
    print(f"\n✅ 寫入 {out_file} (總耗時 {elapsed:.1f}s)")

    # 印各 filter 統計
    print(f"\n📊 各 filter 結果統計:")
    for fname, items in sorted(all_combined.items(), key=lambda x: -len(x[1])):
        if items:
            tw_n = sum(1 for i in items if i.get('market') == 'tw')
            us_n = sum(1 for i in items if i.get('market') == 'us')
            print(f"  {fname}: 共 {len(items)} 檔 (TW {tw_n} / US {us_n})")


if __name__ == '__main__':
    main()
