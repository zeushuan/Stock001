"""全市場 TW 股票 VWAP 學習（~1078 檔，預估 5 小時）
================================================
從 data_cache 取得所有 4位數 TWSE / 5位數 OTC 股票，
跳過 vwap_cache 已有的，剩下全部抓 Fugle 5-min bar。

策略：
- 失敗 ticker 跳過（已下櫃 / 新興板 / Fugle 不收錄）不重試，繼續下一檔
- 每 50 檔印一次累計進度 + 寫 checkpoint 日誌
- 中斷後重啟可從 checkpoint 接續（vwap_cache 自動跳過）
"""
import sys, time, os
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

from vwap_loader import fetch_and_cache_vwap

CACHE_DIR = Path(__file__).parent / 'vwap_cache'
DATA_DIR  = Path(__file__).parent / 'data_cache'
LOG_PATH  = Path(__file__).parent / 'fetch_all_tw.log'


def get_universe():
    """從 data_cache 取所有純股票 ticker（數字開頭）"""
    all_files = sorted(DATA_DIR.glob('*.parquet'))
    tickers = []
    for p in all_files:
        t = p.stem
        if t and t[0].isdigit():
            tickers.append(t)
    return tickers


def main():
    universe = get_universe()
    have_vwap = set(p.stem for p in CACHE_DIR.glob('*.parquet'))
    todo = [t for t in universe if t not in have_vwap]

    print(f"全市場 universe: {len(universe)} 檔")
    print(f"已有 VWAP: {len(have_vwap & set(universe))} 檔")
    print(f"待抓: {len(todo)} 檔")
    print(f"預估時間: {len(todo)*16/3600:.1f} 小時")
    print(flush=True)

    t0 = time.time()
    ok = 0; fail = 0
    fail_list = []

    with open(LOG_PATH, 'a', encoding='utf-8') as logf:
        logf.write(f"\n{'='*60}\n")
        logf.write(f"開始 fetch_all_tw  {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        logf.write(f"待抓 {len(todo)} 檔\n")
        logf.flush()

        for i, t in enumerate(todo, 1):
            try:
                df = fetch_and_cache_vwap(t, start='2020-01-01',
                                          end='2026-04-27', freq='5m')
                if df is not None and len(df) > 0:
                    ok += 1
                else:
                    fail += 1; fail_list.append(t)
            except Exception as e:
                fail += 1; fail_list.append(t)
                print(f"  [{t}] err: {str(e)[:80]}", flush=True)

            # 每 25 檔 summary
            if i % 25 == 0:
                elapsed = (time.time() - t0) / 60
                remain = (len(todo) - i) * elapsed / i
                msg = (f"[{i:4d}/{len(todo)}] ok={ok}  fail={fail}  "
                       f"已 {elapsed:.1f} min, 剩 {remain:.0f} min")
                print(msg, flush=True)
                logf.write(msg + "\n"); logf.flush()

            # 適度間隔（vwap_loader 內已有 1.5s/chunk）
            time.sleep(0.3)

        elapsed = (time.time() - t0) / 60
        msg = f"\n總耗時 {elapsed:.1f} min  成功 {ok}/{len(todo)}  失敗 {fail}"
        print(msg, flush=True)
        logf.write(msg + "\n")

        if fail_list:
            print(f"失敗清單前 30 檔: {fail_list[:30]}")
            logf.write(f"失敗清單: {fail_list}\n")


if __name__ == '__main__':
    main()
