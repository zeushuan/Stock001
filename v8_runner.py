"""
單變體執行器（v8 優化基礎建設）

跑全市場某個策略變體，輸出 CSV 報告。
利用 ProcessPoolExecutor 突破 GIL，CPU-bound 分析真平行。

用法：
  python v8_runner.py --mode base --workers 12
  python v8_runner.py --mode T30  --workers 4 --output results_T30.csv
  python v8_runner.py --mode W    --workers 4 --refresh   # 強制重新下載

規格：
  --mode      策略模式（見 variant_strategy.py）
  --workers   ProcessPool 工作數（預設 12，物理核心數）
  --output    結果 CSV 路徑（預設 results_{mode}.csv）
  --refresh   忽略快取重新下載
  --tickers   指定股票清單檔（預設用 backtest_tw_all.py 的清單）
"""
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

import argparse
import csv
import json
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import data_loader
import variant_strategy as vs


def load_tickers():
    """從 tw_stock_list.json 載入股票清單"""
    p = Path(__file__).parent / 'tw_stock_list.json'
    if p.exists():
        with open(p, encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                # backtest_tw_all 格式：{ticker: {name:..., industry:...}}
                if 'tickers' in data:
                    return data['tickers']
                return list(data.keys())
            if isinstance(data, list):
                return [it['ticker'] if isinstance(it, dict) else it for it in data]
    print("[警告] tw_stock_list.json 不存在，請先跑一次 backtest_tw_all.py 產生清單")
    sys.exit(1)


def _worker_task(args):
    """ProcessPool 工作單元（必須是 module-level 才能 pickle）"""
    ticker, df_bytes, mode = args
    # df_bytes 是 parquet bytes，在子進程中還原
    import io, pandas as pd
    df = pd.read_parquet(io.BytesIO(df_bytes))
    return vs.run_v7_variant(ticker, df, mode=mode)


def _worker_task_path(args):
    """直接從 parquet 檔讀取（避免大量 IPC 資料傳輸）"""
    ticker, file_path, mode = args
    import pandas as pd
    try:
        df = pd.read_parquet(file_path)
        return vs.run_v7_variant(ticker, df, mode=mode)
    except Exception as e:
        return dict(ticker=ticker, mode=mode, error=str(e))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', default='base', help='策略模式')
    ap.add_argument('--workers', type=int, default=12, help='ProcessPool worker 數')
    ap.add_argument('--output', default=None)
    ap.add_argument('--refresh', action='store_true', help='強制重新下載')
    ap.add_argument('--tickers-file', default=None, help='自訂股票清單檔（每行一支）')
    ap.add_argument('--quiet', action='store_true')
    args = ap.parse_args()

    if args.output is None:
        args.output = f"results_{args.mode}.csv"

    # 載入清單
    if args.tickers_file:
        with open(args.tickers_file) as f:
            tickers = [ln.strip() for ln in f if ln.strip()]
    else:
        tickers = load_tickers()

    if not args.quiet:
        print(f"[{args.mode}] 開始全市場回測，{len(tickers)} 檔，{args.workers} workers")

    t_total = time.time()

    # ─── 階段 1：取得快取資料 ──────────────────────────────────
    t0 = time.time()
    if not args.quiet:
        print(f"[{args.mode}] 階段1：載入快取資料...")
    data_map = data_loader.batch_get_all(
        tickers, force_refresh=args.refresh,
        verbose=not args.quiet
    )
    t_load = time.time() - t0
    if not args.quiet:
        print(f"[{args.mode}] 階段1完成：{t_load:.1f}s（{len(data_map)}/{len(tickers)} 檔有資料）")

    # ─── 階段 2：ProcessPool 平行分析 ──────────────────────────
    t1 = time.time()
    if not args.quiet:
        print(f"[{args.mode}] 階段2：ProcessPool 平行分析（{args.workers} workers）...")

    # 用檔案路徑傳遞，避免 IPC 大量資料 pickle 開銷
    tasks = [(tk, str(data_loader.cache_path(tk)), args.mode)
             for tk in data_map.keys()]

    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(_worker_task_path, t): t[0] for t in tasks}
        for fut in as_completed(futures):
            try:
                r = fut.result(timeout=60)
                if r is not None:
                    results.append(r)
            except Exception as e:
                tk = futures[fut]
                if not args.quiet:
                    print(f"  [{tk}] error: {e}")

    t_analyze = time.time() - t1
    if not args.quiet:
        print(f"[{args.mode}] 階段2完成：{t_analyze:.1f}s ({len(results)} 檔)")

    # ─── 階段 3：寫 CSV ───────────────────────────────────────
    if results:
        # 排序：按 pnl_pct 倒序
        results.sort(key=lambda r: r.get('pnl_pct', -1e9), reverse=True)
        with open(args.output, 'w', encoding='utf-8-sig', newline='') as f:
            fields = ['ticker', 'mode', 'bh_pct', 'pnl_pct', 'n_trades',
                      'n_t4', 'win_rate', 'pnl']
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            for r in results:
                writer.writerow(r)

    # ─── 統計摘要 ──────────────────────────────────────────────
    valid = [r for r in results if 'pnl_pct' in r]
    if valid and not args.quiet:
        import numpy as np
        pcts = [r['pnl_pct'] for r in valid]
        bh_pcts = [r.get('bh_pct', 0) for r in valid]
        try:
            print()
            print(f"━━━━━━━━ [{args.mode}] 結果摘要 ━━━━━━━━")
            print(f"  有效樣本：     {len(valid)} 檔")
            print(f"  ⑦ 變體均值：   {np.mean(pcts):+.2f}%")
            print(f"  ① BH 均值：    {np.mean(bh_pcts):+.2f}%")
            print(f"  最高 / 最低：  {max(pcts):+.1f}% / {min(pcts):+.1f}%")
            print(f"  載入耗時：     {t_load:.1f}s")
            print(f"  分析耗時：     {t_analyze:.1f}s")
            print(f"  總耗時：       {time.time()-t_total:.1f}s")
            print(f"  輸出 CSV：     {args.output}")
        except (ValueError, OSError):
            pass
    elif not valid and not args.quiet:
        try: print(f"[{args.mode}] 無有效結果")
        except (ValueError, OSError): pass


if __name__ == '__main__':
    main()
