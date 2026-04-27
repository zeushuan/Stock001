"""抓 100 檔中小型 TWSE 股票 VWAP（避開現有 100 大型權值股）
擴大樣本驗證 VWAPEXEC 在小流動股的表現
"""
import sys, time
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

from vwap_loader import fetch_and_cache_vwap
from fetch_100 import TICKERS_100 as EXISTING

# 100 檔中小型 TWSE（避開現有大型權值）
SMALLMID_100 = [
    # 食品 (10)
    '1210','1213','1215','1217','1218','1219','1229','1232','1233','1234',
    # 化工/塑膠 (10)
    '1304','1305','1307','1308','1310','1312','1313','1314','1319','1907',
    # 紡織/成衣 (10)
    '1409','1410','1413','1417','1418','1419','1444','1455','1466','1476',
    # 電機/電子 (10)
    '1503','1504','1513','1514','1517','1519','1530','1531','1536','1537',
    # 鋼鐵/金屬 (5)
    '2020','2022','2023','2030','2031',
    # 航運/觀光 (8)
    '2606','2607','2608','2611','2630','2702','2705','2727',
    # 中型半導體 (10)
    '2329','2337','2342','2344','2351','2363','2401','2436','2455','2458',
    # 電子零組件 (10)
    '2313','2324','2327','2328','2331','2349','2421','2492','2497','2498',
    # 通信/網通 (5)
    '2419','3380','6168','6166','6248',
    # 中型金融 (5)
    '2832','2834','2836','2845','2855',
    # 中型其他 (17)
    '2547','2548','2597','2915','5522','3041','3056','3060','3563','4123',
    '4174','4961','6230','6451','9907','9911','9938',
]

# 確認無重複、不在現有清單
SMALLMID_100 = list(dict.fromkeys(SMALLMID_100))
SMALLMID_100 = [t for t in SMALLMID_100 if t not in EXISTING]
print(f"待抓 {len(SMALLMID_100)} 檔（去重 + 排除現有）\n")

CACHE_DIR = Path(__file__).parent / 'vwap_cache'
todo = [t for t in SMALLMID_100 if not (CACHE_DIR / f'{t}.parquet').exists()]
print(f"  - 已快取：{len(SMALLMID_100)-len(todo)}")
print(f"  - 待抓：{len(todo)}\n")

t0 = time.time()
ok = 0; fail = 0; fail_list = []
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
        print(f"  [{t}] err: {str(e)[:60]}", flush=True)
    if i % 5 == 0:
        elapsed = (time.time() - t0) / 60
        print(f"  ── 進度 {i}/{len(todo)}  ok={ok}  fail={fail}  累計 {elapsed:.1f} min", flush=True)
    time.sleep(0.5)

print(f"\n總耗時 {(time.time()-t0)/60:.1f} min  成功 {ok}/{len(todo)}  失敗 {fail}")
if fail_list:
    print(f"失敗清單：{fail_list}")
