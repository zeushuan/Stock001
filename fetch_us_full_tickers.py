"""抓取完整 NYSE + NASDAQ ticker 清單
======================================
來源：NASDAQ Trader FTP（公開免費）
  - nasdaqlisted.txt    NASDAQ 上市
  - otherlisted.txt     NYSE / AMEX / IEX

過濾：
  - 排除 ETF / Test Issue / Round Lot Size 異常
  - 排除權證 / Units / 優先股（單純股票才回測）
  - 排除非英文純大寫 ticker（避免特殊符號）

輸出：
  us_full_tickers.json
    {
      'fetched_at': ISO date,
      'tickers': ['AAPL', 'MSFT', ...],
      'total': N,
      'breakdown': {NASDAQ: N1, NYSE: N2, AMEX: N3}
    }
"""
import sys, json, time
from pathlib import Path
from datetime import datetime
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import urllib.request
import io

NASDAQ_URL = 'https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'
OTHER_URL  = 'https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'


def fetch_text(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode('utf-8', errors='replace')


def parse_pipe(text, exchange_label='NASDAQ'):
    """解析 pipe-delimited 檔案，回傳 [(symbol, name, etf_flag), ...]"""
    rows = []
    lines = text.strip().split('\n')
    if not lines: return rows
    header = lines[0].split('|')
    sym_idx = 0
    for i, h in enumerate(header):
        if 'Symbol' in h or 'ACT Symbol' in h:
            sym_idx = i
            break
    name_idx = next((i for i, h in enumerate(header)
                     if 'Security Name' in h), 1)
    etf_idx = next((i for i, h in enumerate(header) if h.strip() == 'ETF'), -1)
    test_idx = next((i for i, h in enumerate(header)
                     if 'Test Issue' in h), -1)

    for line in lines[1:]:
        if not line or line.startswith('File Creation'): continue
        parts = line.split('|')
        if len(parts) <= sym_idx: continue
        sym = parts[sym_idx].strip()
        if not sym: continue
        name = parts[name_idx].strip() if name_idx < len(parts) else ''
        is_etf = (etf_idx >= 0 and etf_idx < len(parts) and
                  parts[etf_idx].strip() == 'Y')
        is_test = (test_idx >= 0 and test_idx < len(parts) and
                   parts[test_idx].strip() == 'Y')
        rows.append((sym, name, is_etf, is_test, exchange_label))
    return rows


def is_clean_stock(sym, name, is_etf, is_test):
    """過濾條件：純股票、非 ETF、非測試、非權證/Units/優先股"""
    if is_etf or is_test: return False
    if not sym or not sym.isalpha() or not sym.isupper(): return False
    # 過濾名稱中的非單純股票
    name_low = name.lower()
    bad_keywords = [
        'warrant', 'units', 'preferred', 'depositary', 'right',
        'note', 'debenture', 'series ', 'subordinated',
        'when issued', 'when-issued', 'when distributed',
    ]
    for kw in bad_keywords:
        if kw in name_low: return False
    # 過濾 ticker 太短（< 1）或太長（> 5，通常是特殊類別）
    if len(sym) > 5: return False
    return True


def main():
    print("📥 抓取 NASDAQ ticker 清單...")
    nasdaq_txt = fetch_text(NASDAQ_URL)
    nasdaq_rows = parse_pipe(nasdaq_txt, 'NASDAQ')
    print(f"  原始 {len(nasdaq_rows)} 行")

    print("📥 抓取 NYSE/AMEX/IEX ticker 清單...")
    other_txt = fetch_text(OTHER_URL)
    other_rows = parse_pipe(other_txt, 'NYSE')
    print(f"  原始 {len(other_rows)} 行")

    all_rows = nasdaq_rows + other_rows
    print(f"\n📊 合併原始：{len(all_rows)} 行")

    # 過濾
    clean = []
    breakdown = {'NASDAQ': 0, 'NYSE': 0, 'AMEX': 0, 'IEX': 0}
    seen = set()
    for sym, name, is_etf, is_test, ex in all_rows:
        if not is_clean_stock(sym, name, is_etf, is_test): continue
        if sym in seen: continue
        seen.add(sym)
        clean.append({'symbol': sym, 'name': name, 'exchange': ex})
        breakdown[ex] = breakdown.get(ex, 0) + 1

    print(f"\n✅ 過濾後純股票：{len(clean)} 檔")
    print(f"   分布：{breakdown}")

    out = {
        'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total': len(clean),
        'breakdown': breakdown,
        'tickers': [s['symbol'] for s in clean],
        'detail': clean,
    }
    Path('us_full_tickers.json').write_text(
        json.dumps(out, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f"\n💾 寫入 us_full_tickers.json")

    # 抽樣
    print(f"\n抽樣前 10 檔：")
    for s in clean[:10]:
        print(f"  {s['symbol']:>6} {s['exchange']:<8} {s['name'][:60]}")
    print(f"\n抽樣末 10 檔：")
    for s in clean[-10:]:
        print(f"  {s['symbol']:>6} {s['exchange']:<8} {s['name'][:60]}")


if __name__ == '__main__':
    main()
