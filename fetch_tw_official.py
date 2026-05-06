"""TWSE / TPEX 官方 API 抓 TW 全市場資料（v9.14）
=========================================================
不依賴 twstock / yfinance，直接 HTTP 請求 TWSE/TPEX 官方端點。

策略：用「全市場單日」端點，N 天 → N 次 HTTP（vs 每股每月一次）
  TWSE 上市：https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=YYYYMMDD&type=ALLBUT0999
  TPEX 上櫃：https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?d=YYY/MM/DD&se=AL

預估：200 個交易日 × 2 (TWSE + TPEX) = 400 個 HTTP 請求 → 3-5 分鐘抓全 TW 一年資料
"""
import sys, json, time
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass


def _safe_float(s):
    """安全轉 float（去除逗號、處理空值）"""
    try:
        s = str(s).replace(',', '').replace('--', '').strip()
        if not s or s == '0' or s == 'X' or s == '0.00':
            return None
        return float(s)
    except Exception:
        return None


def fetch_twse_day(date_str):
    """TWSE 上市個股單日 OHLCV
    date_str: YYYYMMDD
    回傳：dict[ticker] = (open, high, low, close, volume)"""
    url = ("https://www.twse.com.tw/exchangeReport/MI_INDEX"
           f"?response=json&date={date_str}&type=ALLBUT0999")
    try:
        r = requests.get(url, timeout=30, verify=False, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
        })
        if r.status_code != 200: return {}
        d = r.json()
        if d.get('stat') != 'OK': return {}
        out = {}
        # tables[8] 是「每日收盤行情(全部)」(index 可能變動，依 fields 判斷)
        for table in d.get('tables', []):
            fields = table.get('fields', [])
            if not fields: continue
            # 找含「證券代號」+「開盤價」「最高價」「最低價」「收盤價」「成交股數」的 table
            try:
                idx_code = fields.index('證券代號')
                idx_open = fields.index('開盤價')
                idx_high = fields.index('最高價')
                idx_low  = fields.index('最低價')
                idx_close = fields.index('收盤價')
                idx_vol = fields.index('成交股數')
            except ValueError:
                continue
            for row in table.get('data', []):
                code = row[idx_code].strip()
                if not code or not code.isdigit(): continue
                if len(code) != 4: continue
                if code.startswith('00'): continue  # ETF
                ohlcv = (
                    _safe_float(row[idx_open]),
                    _safe_float(row[idx_high]),
                    _safe_float(row[idx_low]),
                    _safe_float(row[idx_close]),
                    _safe_float(row[idx_vol]),
                )
                if all(x is not None for x in ohlcv):
                    out[code] = ohlcv
            break  # 找到 main table 就 break
        return out
    except Exception as e:
        return {}


def fetch_tpex_day(date_str):
    """TPEX 上櫃個股單日 OHLCV
    date_str: YYYYMMDD（會轉成民國年 YYY/MM/DD）"""
    y = int(date_str[:4]) - 1911
    m = date_str[4:6]; d = date_str[6:8]
    url = ("https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/"
           f"stk_quote_result.php?d={y}/{m}/{d}&se=AL&_=" + str(int(time.time()*1000)))
    try:
        r = requests.get(url, timeout=30, verify=False, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
            'Referer': 'https://www.tpex.org.tw/',
        })
        if r.status_code != 200: return {}
        d_json = r.json()
        out = {}
        # TPEX response: {'aaData': [[code, name, close, change, open, high, low, ...], ...]}
        for row in d_json.get('aaData', []):
            try:
                code = row[0].strip()
                if not code.isdigit() or len(code) != 4: continue
                if code.startswith('00'): continue
                close = _safe_float(row[2])
                opn   = _safe_float(row[4])
                high  = _safe_float(row[5])
                low   = _safe_float(row[6])
                vol   = _safe_float(row[8])  # 成交股數
                ohlcv = (opn, high, low, close, vol)
                if all(x is not None for x in ohlcv):
                    out[code] = ohlcv
            except Exception: continue
        return out
    except Exception:
        return {}


def fetch_all_tw_history(days=300):
    """抓全市場 N 個交易日的 OHLCV → dict[ticker] = DataFrame"""
    print(f"📥 TWSE/TPEX 官方 API：抓最近 {days} 個交易日...")
    today = datetime.now()
    # 抓 days 個交易日（往前推約 days * 1.5 個自然日，避開假日）
    history = defaultdict(dict)  # ticker → {date: ohlcv}

    fetched_dates = 0
    skipped = 0
    cur = today
    max_calendar_days = int(days * 1.5)
    for i in range(max_calendar_days):
        if fetched_dates >= days: break
        # 跳過週末
        if cur.weekday() >= 5:
            cur -= timedelta(days=1); continue

        date_str = cur.strftime('%Y%m%d')
        twse = fetch_twse_day(date_str)
        tpex = fetch_tpex_day(date_str)
        merged = {**twse, **tpex}
        if merged:
            for code, ohlcv in merged.items():
                history[code][cur.date()] = ohlcv
            fetched_dates += 1
            if fetched_dates % 20 == 0:
                print(f"  📦 進度 {fetched_dates}/{days}: {date_str} TWSE+TPEX={len(merged)} 檔")
        else:
            skipped += 1
        cur -= timedelta(days=1)
        time.sleep(0.3)  # 限速避免被擋

    print(f"  完成 {fetched_dates} 天，跳過 {skipped} 天（假日 / 抓不到）")
    print(f"  累計 {len(history)} 個 ticker")

    # 組成 DataFrame
    out = {}
    for code, day_dict in history.items():
        if len(day_dict) < 60: continue
        rows = []
        for d, ohlcv in sorted(day_dict.items()):
            rows.append({
                'Date': d, 'Open': ohlcv[0], 'High': ohlcv[1],
                'Low': ohlcv[2], 'Close': ohlcv[3], 'Volume': ohlcv[4],
            })
        df = pd.DataFrame(rows)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date').sort_index()
        out[code] = df
    return out


if __name__ == '__main__':
    # 測試
    out = fetch_all_tw_history(days=10)
    print(f'\n抓到 {len(out)} 檔 TW 股票')
    if '2330' in out:
        df = out['2330']
        print(f'2330 (台積電): {len(df)} 行')
        print(df.tail())
