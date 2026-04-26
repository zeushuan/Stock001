"""
台股全市場回測  backtest_tw_all.py
TWSE 上市普通股 + ETF  2020-01-02 ~ 2026-04-25
全部 7 策略（①~⑦v3）、斷點續跑、輸出 CSV 報告 + 排行榜

用法：
  python backtest_tw_all.py            # 完整跑，自動斷點續跑
  python backtest_tw_all.py --refresh  # 清除快取，重新取得股票清單
  python backtest_tw_all.py --report   # 只印報告（已有結果CSV）
"""
import warnings; warnings.filterwarnings("ignore")
import backtest_all as bt   # 負責 sys.stdout UTF-8 + download + calc_ind

import os, sys, json, time, csv, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import StringIO
import numpy as np
import pandas as pd
import requests
import urllib3
import yfinance as yf
urllib3.disable_warnings()


# ── 設定 ─────────────────────────────────────────────────────
ANALYZE_TIMEOUT = 45    # 每支股票 CPU 分析最多等幾秒
MAX_WORKERS     = 10    # 並行 CPU 分析線程數
BATCH_SIZE      = 50    # 每批下載支數（一次 API call）


# ── 批次下載 ─────────────────────────────────────────────────
def batch_download(codes):
    """
    批次下載多支台股，一次 API call，回傳 {code: df} dict。
    先試 .TW，空資料再補 .TWO。
    """
    syms = [bt.yf_sym(c) for c in codes]
    s = (pd.Timestamp(bt.START) - timedelta(days=280)).strftime("%Y-%m-%d")
    e = (pd.Timestamp(bt.END)   + timedelta(days=2)).strftime("%Y-%m-%d")
    result = {}

    def _extract(raw, code, sym):
        """從批次下載結果萃取單支 df"""
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                lvl0 = raw.columns.get_level_values(0).unique()
                if sym in lvl0:
                    df = raw[sym].copy()
                else:
                    return None
            else:
                # 只有一支時 yf.download 回傳扁平 df
                df = raw.copy()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.dropna(how="all")
            if df.empty or "Close" not in df.columns:
                return None
            return df
        except Exception:
            return None

    # ① 主要批次下載（.TW）
    try:
        raw = yf.download(
            syms, start=s, end=e,
            auto_adjust=True, progress=False,
            group_by="ticker", threads=True,
        )
        retry_codes, retry_syms = [], []
        for code, sym in zip(codes, syms):
            df = _extract(raw, code, sym)
            if df is not None:
                result[code] = df
            elif sym.endswith(".TW"):
                retry_codes.append(code)
                retry_syms.append(code + ".TWO")
    except Exception as ex:
        print(f"  [批次下載] .TW 失敗: {ex}", flush=True)
        retry_codes = [c for c in codes if bt.yf_sym(c).endswith(".TW")]
        retry_syms  = [c + ".TWO" for c in retry_codes]

    # ② .TWO 補充下載（空資料的 .TW 股票）
    if retry_codes:
        try:
            if len(retry_syms) == 1:
                raw2 = yf.download(
                    retry_syms[0], start=s, end=e,
                    auto_adjust=True, progress=False,
                )
                df2 = _extract(raw2, retry_codes[0], retry_syms[0])
                if df2 is not None:
                    result[retry_codes[0]] = df2
            else:
                raw2 = yf.download(
                    retry_syms, start=s, end=e,
                    auto_adjust=True, progress=False,
                    group_by="ticker", threads=True,
                )
                for code, sym2 in zip(retry_codes, retry_syms):
                    df2 = _extract(raw2, code, sym2)
                    if df2 is not None:
                        result[code] = df2
        except Exception as ex:
            print(f"  [批次下載] .TWO 失敗: {ex}", flush=True)

    return result


# ── 有超時保護的 analyze_from_df（CPU only） ─────────────────
def analyze_from_df_with_timeout(code, df):
    """執行 bt.analyze_from_df()，超過 ANALYZE_TIMEOUT 秒回傳 None"""
    result = [None]
    exc    = [None]

    def _run():
        try:
            result[0] = bt.analyze_from_df(df, code)
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(ANALYZE_TIMEOUT)
    if t.is_alive():
        return None, TimeoutError(f"超過 {ANALYZE_TIMEOUT}s")
    if exc[0] is not None:
        return None, exc[0]
    return result[0], None


# ── 舊版（單支）有超時保護的 analyze（保留供相容） ────────────
def analyze_with_timeout(code):
    """執行 bt.analyze()，超過 ANALYZE_TIMEOUT 秒回傳 None"""
    result = [None]
    exc    = [None]

    def _run():
        try:
            result[0] = bt.analyze(code)
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(ANALYZE_TIMEOUT)
    if t.is_alive():
        return None, TimeoutError(f"超過 {ANALYZE_TIMEOUT}s")
    if exc[0] is not None:
        return None, exc[0]
    return result[0], None

# ── 回測參數 ─────────────────────────────────────────────────
bt.START = "2020-01-02"
bt.END   = "2026-04-25"
INVEST   = bt.INVEST

LIST_CACHE   = "tw_stock_list.json"          # 股票清單快取
CKPT_CSV     = "tw_backtest_checkpoint.csv"  # 斷點進度檔
OUTPUT_CSV   = f"tw_all_results_{datetime.now().strftime('%Y%m%d')}.csv"
SAVE_EVERY   = 50    # 每幾檔存一次進度
TOP_N        = 30    # 排行榜顯示筆數

STRATEGY_KEYS  = ["bh_pnl","pnl2","pnl3","pnl4","pnl5","pnl6","pnl7"]
STRATEGY_NAMES = ["①持有","②趨勢","③RSI","④快線","⑤動RSI","⑥組合","⑦自適應v3"]

# ── 取得 TWSE 上市股票清單 ─────────────────────────────────────
def fetch_twse_list(force_refresh=False):
    """
    從 TWSE ISIN 頁面取得上市清單（普通股 + ETF）。
    一次抓全部（不帶 issuetype），再依 CFICode 過濾：
      ES → 普通股（約1048檔）
      CE / EF → ETF / 受益憑證（約245檔）
      RW 等其餘（權證、轉換債…）→ 跳過
    結果快取至 LIST_CACHE，有效期 7 天。
    回傳 dict: {code: {'name': str, 'type': str, 'industry': str}}
    """
    if not force_refresh and os.path.exists(LIST_CACHE):
        age_days = (time.time() - os.path.getmtime(LIST_CACHE)) / 86400
        if age_days < 7:
            with open(LIST_CACHE, encoding="utf-8") as f:
                data = json.load(f)
            print(f"[清單] 使用快取：共 {len(data)} 檔（{LIST_CACHE}，"
                  f"{age_days:.1f} 天前）")
            return data

    print("[清單] 從 TWSE ISIN 下載完整清單（一次全抓）...")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        )
    }

    # 不帶 issuetype，取得全部約33000列（含權證）
    # 欄位結構：
    #   col[0]=序號  col[1]=ISIN  col[2]=代號  col[3]=名稱
    #   col[4]=市場別  col[5]=種類  col[6]=產業別  col[7]=上市日
    #   col[8]=CFICode  col[9]=備註
    url = (
        "https://isin.twse.com.tw/isin/class_main.jsp?"
        "owncode=&stockname=&isincode=&market=1&issuetype="
        "&industrycode=&Page=1&chklike=Y"
    )

    # CFI prefix → 種類標籤
    CFI_KEEP = {
        "ES": "普通股",
        "CE": "ETF",
        "EF": "ETF",
    }

    all_stocks = {}
    cfi_count  = {}   # 統計用

    try:
        resp = requests.get(url, headers=headers, timeout=30, verify=False)
        text = resp.content.decode("ms950", errors="replace")
        tables = pd.read_html(StringIO(text))
        if not tables:
            print("[清單] ⚠️  TWSE 回傳無表格，請稍後重試")
            return {}

        df = tables[0]
        print(f"[清單] 原始資料共 {len(df)-1} 列")

        for i in range(1, len(df)):   # 第0列是標頭
            row  = df.iloc[i]
            code = str(row.iloc[2]).strip()
            name = str(row.iloc[3]).strip()
            ind  = str(row.iloc[6]).strip() if len(row) > 6 else ""
            cfi  = str(row.iloc[8]).strip() if len(row) > 8 else ""

            # CFI prefix 計數（偵錯用）
            pfx = cfi[:2] if len(cfi) >= 2 else cfi
            cfi_count[pfx] = cfi_count.get(pfx, 0) + 1

            # 只保留普通股和 ETF
            label = CFI_KEEP.get(pfx)
            if label is None:
                continue

            # 有效代號：4~6 碼，前4碼為數字（如 2330、00878、00921B）
            if not (len(code) >= 4 and code[:4].isdigit()):
                continue
            if name in ("nan", "None", ""):
                continue
            if ind in ("nan", "None"):
                ind = ""

            if code not in all_stocks:
                all_stocks[code] = {
                    "name":     name,
                    "type":     label,
                    "industry": ind,
                    "cfi":      pfx,
                }

        # 統計輸出
        n_stock = sum(1 for v in all_stocks.values() if v["type"] == "普通股")
        n_etf   = sum(1 for v in all_stocks.values() if v["type"] == "ETF")
        print(f"[清單] 過濾後：普通股 {n_stock} 檔  ETF {n_etf} 檔  "
              f"合計 {len(all_stocks)} 檔")
        print(f"[清單] CFI 分佈（前幾名）："
              f"{dict(sorted(cfi_count.items(), key=lambda x:-x[1])[:8])}")

    except Exception as ex:
        print(f"[清單] ⚠️  下載/解析失敗: {ex}")
        return {}

    if not all_stocks:
        print("[清單] ⚠️  過濾後無有效股票，請檢查 CFI 邏輯")
        return {}

    # 依代號排序後存快取
    all_stocks = dict(sorted(all_stocks.items()))
    with open(LIST_CACHE, "w", encoding="utf-8") as f:
        json.dump(all_stocks, f, ensure_ascii=False, indent=2)
    print(f"[清單] 共 {len(all_stocks)} 檔，已快取至 {LIST_CACHE}")
    return all_stocks


# ── 載入斷點進度 ──────────────────────────────────────────────
def load_checkpoint():
    """回傳已完成的 {ticker: row_dict}"""
    done = {}
    if not os.path.exists(CKPT_CSV):
        return done
    with open(CKPT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            done[row["ticker"]] = row
    print(f"[續跑] 已載入斷點進度：{len(done)} 檔完成")
    return done


def save_checkpoint(done_rows):
    """將已完成的結果存入進度 CSV"""
    if not done_rows:
        return
    fieldnames = list(next(iter(done_rows.values())).keys())
    with open(CKPT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(done_rows.values())


# ── 從 analyze() 結果萃取精簡資訊 ────────────────────────────
def extract_row(r, meta):
    """從 bt.analyze() 回傳的 dict 萃取存 CSV 用的精簡 dict"""
    atr_arr = r.get("rsi_arr")   # 借用字段（實際是 rsi）
    pr      = r.get("p1", 0)     # last price
    p0      = r.get("p0", 1)
    # 計算平均 ATR/Price（從 sub DataFrame 算，若有的話）
    avg_rel_atr = 0.0

    pnls = [r[k] for k in STRATEGY_KEYS]
    best_i    = int(np.argmax(pnls))
    best_name = STRATEGY_NAMES[best_i]

    row = {
        "ticker":       r["ticker"],
        "name":         meta.get("name", ""),
        "type":         meta.get("type", ""),
        "industry":     meta.get("industry", ""),
        "currency":     r["cur"],
        "n_days":       r["n"],
        "price_start":  f"{p0:.2f}",
        "price_end":    f"{pr:.2f}",
        "bh_ret_pct":   f"{r['bh_ret']*100:.2f}",
        "bull_days":    r.get("bull_days", 0),
        "pnl_bh":       f"{r['bh_pnl']:.0f}",
        "pnl_t2":       f"{r['pnl2']:.0f}",
        "pnl_t3":       f"{r['pnl3']:.0f}",
        "pnl_t4":       f"{r['pnl4']:.0f}",
        "pnl_t5":       f"{r['pnl5']:.0f}",
        "pnl_t6":       f"{r['pnl6']:.0f}",
        "pnl_t7":       f"{r['pnl7']:.0f}",
        "ret_bh":       f"{r['bh_pnl']/INVEST*100:.2f}",
        "ret_t2":       f"{r['pnl2']/INVEST*100:.2f}",
        "ret_t3":       f"{r['pnl3']/INVEST*100:.2f}",
        "ret_t4":       f"{r['pnl4']/INVEST*100:.2f}",
        "ret_t5":       f"{r['pnl5']/INVEST*100:.2f}",
        "ret_t6":       f"{r['pnl6']/INVEST*100:.2f}",
        "ret_t7":       f"{r['pnl7']/INVEST*100:.2f}",
        "n_trades_t7":  len(r.get("t7", [])),
        "best_strategy":best_name,
    }
    return row


# ── 最終報告 ──────────────────────────────────────────────────
def print_report(done_rows):
    if not done_rows:
        print("無結果"); return

    rows = list(done_rows.values())

    def fv(row, key):
        try: return float(row[key])
        except: return 0.0

    # 依 ⑦v3 報酬排序
    rows_sorted_t7 = sorted(rows, key=lambda r: fv(r,"ret_t7"), reverse=True)
    rows_sorted_bh = sorted(rows, key=lambda r: fv(r,"bh_ret_pct"), reverse=True)

    W = 100
    print(f"\n{'▓'*W}")
    print(f"  台股全市場回測報告  {bt.START} ~ {bt.END}  共 {len(rows)} 檔有效")
    print(f"{'▓'*W}")

    # ── Top N：⑦自適應v3 ──
    print(f"\n  ┌{'─'*96}┐")
    print(f"  │  TOP {TOP_N}  ⑦自適應趨勢 v3 報酬排行{' '*64}│")
    print(f"  ├{'─'*96}┤")
    print(f"  │  {'代號':<8}{'名稱':<14}{'產業':<14}  {'BH%':>8}  {'⑦v3%':>8}  {'②趨勢%':>8}  "
          f"{'最佳策略':<12}  ATR/P  │")
    print(f"  ├{'─'*96}┤")
    for row in rows_sorted_t7[:TOP_N]:
        print(f"  │  {row['ticker']:<8}{row['name']:<14}{row['industry'][:12]:<14}"
              f"  {fv(row,'bh_ret_pct'):>+7.1f}%"
              f"  {fv(row,'ret_t7'):>+7.1f}%"
              f"  {fv(row,'ret_t2'):>+7.1f}%"
              f"  {row['best_strategy']:<12}"
              f"  {'─':>5}  │")
    print(f"  └{'─'*96}┘")

    # ── Top N：買入持有 ──
    print(f"\n  ┌{'─'*96}┐")
    print(f"  │  TOP {TOP_N}  買入持有 報酬排行{' '*72}│")
    print(f"  ├{'─'*96}┤")
    print(f"  │  {'代號':<8}{'名稱':<14}{'產業':<14}  {'BH%':>8}  {'⑦v3%':>8}  "
          f"{'②趨勢%':>8}  {'最佳策略':<12}  │")
    print(f"  ├{'─'*96}┤")
    for row in rows_sorted_bh[:TOP_N]:
        print(f"  │  {row['ticker']:<8}{row['name']:<14}{row['industry'][:12]:<14}"
              f"  {fv(row,'bh_ret_pct'):>+7.1f}%"
              f"  {fv(row,'ret_t7'):>+7.1f}%"
              f"  {fv(row,'ret_t2'):>+7.1f}%"
              f"  {row['best_strategy']:<12}  │")
    print(f"  └{'─'*96}┘")

    # ── Bottom 10：⑦v3 最差 ──
    print(f"\n  ── BOTTOM 10  ⑦自適應v3 最差（停損次數偏多）──")
    for row in rows_sorted_t7[-10:]:
        print(f"  {row['ticker']:<8}{row['name']:<14}  BH:{fv(row,'bh_ret_pct'):>+7.1f}%"
              f"  ⑦:{fv(row,'ret_t7'):>+7.1f}%")

    # ── 各策略平均 ──
    print(f"\n{'─'*W}")
    print(f"  各策略平均報酬（{len(rows)} 檔）")
    print(f"{'─'*W}")
    ret_keys = ["ret_bh","ret_t2","ret_t3","ret_t4","ret_t5","ret_t6","ret_t7"]
    avgs = [np.mean([fv(r, k) for r in rows]) for k in ret_keys]
    maxs = [max(fv(r, k) for r in rows) for k in ret_keys]
    mins = [min(fv(r, k) for r in rows) for k in ret_keys]
    best_avg_i = int(np.argmax(avgs))
    for i, (nm, avg, mx, mn) in enumerate(zip(STRATEGY_NAMES, avgs, maxs, mins)):
        tag = "  ◀ 均值最佳" if i == best_avg_i else ""
        print(f"  {nm:<16}  均值:{avg:>+8.1f}%  最高:{mx:>+8.1f}%  最低:{mn:>+8.1f}%{tag}")

    # ── 最佳策略勝出次數 ──
    from collections import Counter
    best_cnt = Counter(r["best_strategy"] for r in rows)
    print(f"\n{'─'*W}")
    print(f"  各策略「最佳」勝出次數")
    print(f"{'─'*W}")
    for name, cnt in best_cnt.most_common():
        bar = "█" * min(cnt // 5, 40)
        print(f"  {name:<16}  {cnt:>4} 次  {bar}")

    # ── 產業別統計（Top 產業 by ⑦v3 均值）──
    from collections import defaultdict
    ind_rows = defaultdict(list)
    for r in rows:
        ind = r.get("industry", "") or "—"
        if ind and ind != "—":
            ind_rows[ind].append(fv(r, "ret_t7"))
    ind_stats = {
        ind: (np.mean(vs), len(vs))
        for ind, vs in ind_rows.items() if len(vs) >= 3
    }
    ind_sorted = sorted(ind_stats.items(), key=lambda x: x[1][0], reverse=True)
    print(f"\n{'─'*W}")
    print(f"  產業別 ⑦v3 平均報酬（≥3檔）Top 20")
    print(f"{'─'*W}")
    for ind, (avg, cnt) in ind_sorted[:20]:
        bar = "█" * min(int(avg / 10), 30) if avg > 0 else "▒" * min(int(-avg/10), 10)
        print(f"  {ind[:16]:<16}  {cnt:>4}檔  {avg:>+7.1f}%  {bar}")

    # ── 產業別 Bottom ──
    print(f"\n  ... Bottom 10 產業 ...")
    for ind, (avg, cnt) in ind_sorted[-10:]:
        print(f"  {ind[:16]:<16}  {cnt:>4}檔  {avg:>+7.1f}%")

    print(f"\n{'▓'*W}")
    print(f"  結果已儲存至：{OUTPUT_CSV}")
    print(f"{'▓'*W}\n")


# ── 主程式 ────────────────────────────────────────────────────
if __name__ == "__main__":
    force_refresh = "--refresh" in sys.argv
    report_only   = "--report"  in sys.argv

    # 取得股票清單
    stock_list = fetch_twse_list(force_refresh=force_refresh)
    if not stock_list:
        print("無法取得股票清單，中止。"); sys.exit(1)

    print(f"\n[股票清單] 共 {len(stock_list)} 檔（普通股 + ETF）")

    # 只輸出報告模式
    if report_only:
        done = load_checkpoint()
        print_report(done)
        sys.exit(0)

    # 載入斷點進度
    done = load_checkpoint()
    remaining = [c for c in stock_list if c not in done]
    print(f"[進度] 待處理：{len(remaining)} 檔"
          f"（已完成：{len(done)}，共：{len(stock_list)}）")
    # 批次下載估算：每批~10s 下載 + BATCH_SIZE/MAX_WORKERS*2s CPU 分析
    n_batches_est = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
    best  = n_batches_est * (10 + BATCH_SIZE / MAX_WORKERS * 2) / 60
    worst = n_batches_est * (30 + BATCH_SIZE / MAX_WORKERS * ANALYZE_TIMEOUT) / 60
    print(f"[預估] {n_batches_est} 批 × (下載+分析)：正常約 {best:.0f} 分鐘，"
          f"最差 {worst:.0f} 分鐘\n")

    failed   = []
    saved_at = [len(done)]   # 上次存檔時的筆數
    t_start  = time.time()
    total    = len(remaining)
    n_done   = [0]           # 全域完成計數

    # 分批：每批 BATCH_SIZE 支，一次 API call 下載，再並行 CPU 分析
    batches  = [remaining[i:i+BATCH_SIZE]
                for i in range(0, len(remaining), BATCH_SIZE)]
    n_batches = len(batches)
    print(f"[批次] BATCH_SIZE={BATCH_SIZE}  MAX_WORKERS={MAX_WORKERS}"
          f"  共 {n_batches} 批\n")

    for b_idx, batch in enumerate(batches, 1):
        # ── 批次下載（單一執行緒，一次 API call）────────────────
        t_dl = time.time()
        print(f"[批次 {b_idx:3d}/{n_batches}] 下載 {len(batch)} 支...",
              end=" ", flush=True)
        df_map = batch_download(batch)
        print(f"取得 {len(df_map)}/{len(batch)} 支"
              f"  ({time.time()-t_dl:.1f}s)", flush=True)

        # ── 並行 CPU 分析（MAX_WORKERS 線程，無 IO） ────────────
        done_lock = threading.Lock()

        def process_from_df(args):
            _seq, code = args
            meta = stock_list.get(code, {})
            name = meta.get("name", "")
            df   = df_map.get(code)
            if df is None:
                return _seq, code, name, meta, None, ValueError("無下載資料")
            r, err = analyze_from_df_with_timeout(code, df)
            return _seq, code, name, meta, r, err

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_from_df, (i, code)): code
                for i, code in enumerate(batch)
            }

            for future in as_completed(futures):
                try:
                    _seq, code, name, meta, r, err = future.result()
                except Exception as ex:
                    code = futures[future]
                    print(f"  [FATAL] {code}: {ex}", flush=True)
                    with done_lock:
                        failed.append(code)
                    continue

                with done_lock:
                    n_done[0] += 1
                    nd = n_done[0]
                    elapsed = time.time() - t_start
                    rate = nd / elapsed
                    eta  = (total - nd) / rate if rate > 0 else 0

                    if err is not None:
                        tag = "TIMEOUT" if isinstance(err, TimeoutError) else "ERROR"
                        print(f"  [{nd:4d}/{total}] {code:<8} {name[:10]:<10}  "
                              f"{tag}  ETA:{eta/60:.0f}m", flush=True)
                        failed.append(code)
                    elif r is None:
                        print(f"  [{nd:4d}/{total}] {code:<8} {name[:10]:<10}  "
                              f"無資料  ETA:{eta/60:.0f}m", flush=True)
                        failed.append(code)
                    else:
                        row = extract_row(r, meta)
                        done[code] = row
                        bh_s = f"BH:{float(row['bh_ret_pct']):>+7.1f}%"
                        t7_s = f"⑦:{float(row['ret_t7']):>+7.1f}%"
                        print(f"  [{nd:4d}/{total}] {code:<8} {name[:10]:<10}  "
                              f"{bh_s}  {t7_s}  ETA:{eta/60:.0f}m", flush=True)

                        # 每 SAVE_EVERY 筆存一次斷點
                        if len(done) - saved_at[0] >= SAVE_EVERY:
                            save_checkpoint(done)
                            print(f"  ── [存檔] {len(done)} 檔進度已儲存 ──",
                                  flush=True)
                            saved_at[0] = len(done)

    # 最終存檔
    save_checkpoint(done)

    # 輸出最終 CSV
    if done:
        fieldnames = list(next(iter(done.values())).keys())
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            # 依 ⑦v3 報酬由高到低排序
            sorted_rows = sorted(done.values(),
                                 key=lambda r: float(r.get("ret_t7", 0)),
                                 reverse=True)
            writer.writerows(sorted_rows)

    total_sec = time.time() - t_start
    print(f"\n[完成] 成功：{len(done)} 檔  失敗/跳過：{len(failed)} 檔"
          f"  耗時：{total_sec/60:.1f} 分鐘")
    if failed:
        print(f"  失敗清單：{', '.join(failed[:30])}"
              f"{'...' if len(failed)>30 else ''}")

    # 印報告
    print_report(done)
