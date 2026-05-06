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


_DEBUG_PRINTED = {'twse_first': False, 'tpex_first': False}


def fetch_twse_day(date_str, debug=False):
    """TWSE 上市個股單日 OHLCV — 解析 tables[8] 每日收盤行情
    fields: ['證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額',
             '開盤價', '最高價', '最低價', '收盤價', ...]"""
    url = ("https://www.twse.com.tw/exchangeReport/MI_INDEX"
           f"?response=json&date={date_str}&type=ALLBUT0999")
    try:
        r = requests.get(url, timeout=30, verify=False, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
        })
        if r.status_code != 200: return {}
        d = r.json()
        if d.get('stat') and d.get('stat') != 'OK': return {}
        out = {}
        for table in d.get('tables', []):
            fields = table.get('fields', [])
            if '證券代號' not in fields: continue
            # 找 indices
            idx_code = fields.index('證券代號')
            idx_open = fields.index('開盤價') if '開盤價' in fields else -1
            idx_high = fields.index('最高價') if '最高價' in fields else -1
            idx_low  = fields.index('最低價') if '最低價' in fields else -1
            idx_close = fields.index('收盤價') if '收盤價' in fields else -1
            idx_vol = fields.index('成交股數') if '成交股數' in fields else -1
            if -1 in (idx_open, idx_high, idx_low, idx_close, idx_vol): continue
            for row in table.get('data', []):
                code = row[idx_code].strip()
                if not code.isdigit() or len(code) != 4: continue
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
            break
        if not _DEBUG_PRINTED['twse_first']:
            print(f"  [DEBUG] TWSE {date_str}: 解析到 {len(out)} 檔")
            _DEBUG_PRINTED['twse_first'] = True
        return out
    except Exception as e:
        if not _DEBUG_PRINTED.get('twse_err'):
            print(f"  [ERROR] TWSE {date_str}: {type(e).__name__}: {e}")
            _DEBUG_PRINTED['twse_err'] = True
        return {}


def fetch_tpex_day(date_str, debug=False):
    """TPEX 上櫃個股單日 OHLCV — 解析 tables[0] 上櫃股票行情
    fields: ['代號', '名稱', '收盤', '漲跌', '開盤', '最高', '最低', '均價', '成交股數', ...]"""
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
        for table in d_json.get('tables', []):
            fields = table.get('fields', [])
            if '代號' not in fields: continue
            idx_code = fields.index('代號')
            idx_open = fields.index('開盤') if '開盤' in fields else -1
            idx_high = fields.index('最高') if '最高' in fields else -1
            idx_low  = fields.index('最低') if '最低' in fields else -1
            idx_close = fields.index('收盤') if '收盤' in fields else -1
            idx_vol = fields.index('成交股數') if '成交股數' in fields else -1
            if -1 in (idx_open, idx_high, idx_low, idx_close, idx_vol): continue
            for row in table.get('data', []):
                code = str(row[idx_code]).strip()
                if not code.isdigit() or len(code) != 4: continue
                if code.startswith('00'): continue
                ohlcv = (
                    _safe_float(row[idx_open]),
                    _safe_float(row[idx_high]),
                    _safe_float(row[idx_low]),
                    _safe_float(row[idx_close]),
                    _safe_float(row[idx_vol]),
                )
                if all(x is not None for x in ohlcv):
                    out[code] = ohlcv
            break
        if not _DEBUG_PRINTED['tpex_first']:
            print(f"  [DEBUG] TPEX {date_str}: 解析到 {len(out)} 檔")
            _DEBUG_PRINTED['tpex_first'] = True
        return out
    except Exception as e:
        if not _DEBUG_PRINTED.get('tpex_err'):
            print(f"  [ERROR] TPEX {date_str}: {type(e).__name__}: {e}")
            _DEBUG_PRINTED['tpex_err'] = True
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
