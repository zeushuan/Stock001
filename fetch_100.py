"""抓 100 檔 VWAP（背景執行用）— 跳過已抓的"""
import sys, time
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

from vwap_loader import fetch_and_cache_vwap

TICKERS_100 = [
    # 半導體 + 大型 (1-30)
    '2330','2317','2454','2412','2308','2882','2891','2886','2884','2603',
    '2610','3008','6505','2885','2880','2883','3034','2474','2382','6669',
    '8081','2207','2002','1216','1303','1101','2615','2618','2890','2887',
    # 金融 + 半導體 (31-60)
    '5871','2379','3711','6488','3037','2356','2376','2353','2354','4904',
    '6770','5483','3045','5347','8046','6526','3017','3661','5269','8454',
    '8299','2357','6515','2912','9921','1102','1402','1301','1326','2609',
    # 中型 (61-90)
    '6116','2812','2823','2888','6271','3231','2059','3653','1227','9904',
    '2105','2360','2371','2377','2347','6285','4958','2392','6239','3406',
    '2049','1605','2027','2014','2542','5876','1722','1707','9914','3035',
    # 中小型 + 概念 (91-100)
    '2393','2301','6147','6024','2645','4938','3596','4763','9933','2540',
]

CACHE_DIR = Path(__file__).parent / 'vwap_cache'
CACHE_DIR.mkdir(exist_ok=True)

# 跳過已快取
todo = [t for t in TICKERS_100 if not (CACHE_DIR / f'{t}.parquet').exists()]
print(f"[抓 VWAP] 全部 {len(TICKERS_100)} 檔，待抓 {len(todo)} 檔（已快取 {len(TICKERS_100)-len(todo)}）", flush=True)

t0 = time.time()
ok = 0; fail = 0
fail_list = []
for i, t in enumerate(todo, 1):
    print(f"[{i:3d}/{len(todo)}]", end=' ', flush=True)
    try:
        df = fetch_and_cache_vwap(t, start='2020-01-01', end='2026-04-27', freq='5m')
        if df is not None and len(df) > 0:
            ok += 1
        else:
            fail += 1; fail_list.append(t)
    except Exception as e:
        fail += 1; fail_list.append(t)
        print(f"  [{t}] 例外：{str(e)[:60]}", flush=True)
    if i % 5 == 0:
        elapsed = (time.time() - t0) / 60
        print(f"  ── 進度 {i}/{len(todo)}  ok={ok}  fail={fail}  累計 {elapsed:.1f} min", flush=True)
    # 每檔多 sleep 一下，怕 rate limit
    time.sleep(0.5)

print(f"\n總耗時 {(time.time()-t0)/60:.1f} min  成功 {ok}/{len(todo)}  失敗 {fail}", flush=True)
if fail_list:
    print(f"失敗清單：{fail_list}", flush=True)
