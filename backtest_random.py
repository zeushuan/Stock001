"""
清單外隨機標的回測  2020-01-02 ~ 2026-04-25
台股：2317 鴻海、2454 聯發科、2382 廣達、2303 聯電、00878 國泰高股息、6669 緯穎
美股：NVDA、TSLA、AAPL、META、AMD、PLTR
"""
import warnings; warnings.filterwarnings("ignore")
import backtest_all as bt  # backtest_all 已在模組層級重設 sys.stdout UTF-8

bt.START = "2020-01-02"
bt.END   = "2026-04-25"
bt.STOCK_LIST = [
    "2317", "2454", "2382", "2303", "00878", "6669",
    "NVDA", "TSLA", "AAPL", "META", "AMD", "PLTR",
]

if __name__ == "__main__":
    print(f"\n{'▓'*70}")
    print(f"  清單外隨機標的回測  {bt.START} ~ {bt.END}  共 {len(bt.STOCK_LIST)} 檔")
    print(f"  台股：2317 2454 2382 2303 00878 6669")
    print(f"  美股：NVDA TSLA AAPL META AMD PLTR")
    print(f"{'▓'*70}")

    all_results = []
    for ticker in bt.STOCK_LIST:
        print(f"[下載] {ticker:<10}", end=" ", flush=True)
        try:
            r = bt.analyze(ticker)
            if r is None:
                print("無資料")
            else:
                print(f"完成  ({r['n']}天  {r['bh_ret']*100:+.2f}%)")
                all_results.append(r)
        except Exception as ex:
            print(f"ERROR: {ex}")

    for r in all_results:
        bt.print_detail(r)

    bt.print_leaderboard(all_results)

    print(f"\n{'▓'*70}")
    print(f"  回測完成  (未含手續費 / 停損以收盤價計)")
    print(f"{'▓'*70}\n")
