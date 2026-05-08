"""波段持倉出場通知腳本（v9.17）
==========================================================
讀取 watchlists_user.json，對每檔持倉檢查 4 種主動出場 recipe，
若任一觸發就 LINE 推送提醒。

設計
-----
1. 讀 watchlists_user.json（從 GitHub repo 取得，本地也 fallback）
2. 對每檔 ticker：
   - 抓 yfinance 最新資料
   - 用 state_classifier.evaluate_recipes_live() 評估 4 recipe
   - 任一觸發 → 加進通知清單
3. 把通知整合成 LINE 訊息，推送

使用情境
---------
- 在 GitHub Actions cron 每天 13:35 / 05:05 跑（盤後即時）
- 與既有 weekly_full_scan.yml 一起使用

執行
-----
  python notify_swing_exit.py                # 跑所有 watchlists
  python notify_swing_exit.py --watchlist "我的美股"  # 只看單一清單
  python notify_swing_exit.py --dry-run      # 計算但不推 LINE
"""
import sys, os, json, argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import yfinance as yf
from state_classifier import classify_market_state, evaluate_recipes_live

# 直接重用既有的 indicator 計算（從 backtest_all.py）
import backtest_all as bt


def parse_tickers_text(text: str) -> list:
    """把 watchlist 的多行文字解析成 (ticker, market) tuples"""
    out = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue
        parts = [p.strip() for p in line.split(',')]
        ticker = parts[0].upper()
        # 判斷 market（同 tv_app）
        if ticker.isdigit() or (any(c.isdigit() for c in ticker) and len(ticker) <= 6):
            out.append((ticker, 'tw'))
        else:
            out.append((ticker, 'us'))
    return out


def load_watchlists():
    """從 GitHub 拉 watchlists_user.json，本地檔案 fallback"""
    # 先試 GitHub
    try:
        import urllib.request
        url = 'https://raw.githubusercontent.com/zeushuan/Stock001/main/watchlists_user.json'
        with urllib.request.urlopen(url, timeout=10) as r:
            if r.status == 200:
                d = json.loads(r.read().decode('utf-8'))
                print(f'✅ 從 GitHub 拉到 watchlists_user.json ({len(d)} 個清單)')
                return d
    except Exception as e:
        print(f'⚠️ GitHub 拉取失敗：{e}')

    # Fallback: 本地檔案
    p = Path(__file__).parent / 'watchlists_user.json'
    if p.exists():
        try:
            d = json.loads(p.read_text(encoding='utf-8'))
            print(f'✅ 讀本地 watchlists_user.json ({len(d)} 個清單)')
            return d
        except Exception:
            pass
    print('❌ 找不到 watchlists_user.json')
    return {}


def fetch_and_evaluate(ticker: str, market: str):
    """抓最新資料 + 評估 4 recipe + 三狀態。回傳 dict 或 None"""
    try:
        # 用 yfinance 抓
        yf_t = f'{ticker}.TW' if market == 'tw' else ticker
        df_raw = yf.download(yf_t, period='400d', interval='1d',
                              progress=False, auto_adjust=False)
        if df_raw is None or df_raw.empty or len(df_raw) < 80:
            return None
        if isinstance(df_raw.columns, pd.MultiIndex):
            df_raw.columns = df_raw.columns.get_level_values(0)

        # 算指標
        df = bt.calc_ind(df_raw)
        if df is None or df.empty:
            return None

        # 評估
        state = classify_market_state(df)
        recipes = evaluate_recipes_live(df)

        triggered = [r for r in recipes if r['triggered']]
        return {
            'ticker': ticker,
            'market': market,
            'close': float(df['Close'].iloc[-1]),
            'state_label': state['state_label'],
            'state_days': state['days_in_state'],
            'recipes': recipes,
            'triggered': triggered,
            'n_triggered': len(triggered),
        }
    except Exception as e:
        return {'ticker': ticker, 'market': market, 'error': str(e)}


def push_line(msg: str, dry_run: bool = False):
    """透過 LINE Bot 推送訊息（用 LINE_CHANNEL_TOKEN + LINE_USER_ID env）"""
    token = os.environ.get('LINE_CHANNEL_TOKEN', '')
    user_id = os.environ.get('LINE_USER_ID', '')
    if not token or not user_id:
        print('⚠️ LINE_CHANNEL_TOKEN / LINE_USER_ID 未設定，跳過推送')
        print(msg)
        return False
    if dry_run:
        print('[DRY RUN] 訊息內容：')
        print(msg)
        return False

    import urllib.request
    body = json.dumps({
        'to': user_id,
        'messages': [{'type': 'text', 'text': msg[:4900]}],
    }).encode('utf-8')
    req = urllib.request.Request(
        'https://api.line.me/v2/bot/message/push',
        data=body,
        headers={'Content-Type': 'application/json',
                 'Authorization': f'Bearer {token}'})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            print(f'✅ LINE 推送成功 (status {r.status})')
            return True
    except Exception as e:
        print(f'❌ LINE 推送失敗：{e}')
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--watchlist', type=str, default=None,
                   help='只看單一清單（不指定就跑全部）')
    p.add_argument('--dry-run', action='store_true', help='不真的推 LINE')
    args = p.parse_args()

    wls = load_watchlists()
    if not wls:
        print('沒有 watchlists 可以檢查')
        return

    # 篩選清單
    if args.watchlist:
        wls = {k: v for k, v in wls.items() if k == args.watchlist}
        if not wls:
            print(f'找不到清單「{args.watchlist}」')
            return

    print(f'📋 檢查 {len(wls)} 個清單：{list(wls.keys())}\n')

    # 收集所有 unique tickers
    all_tickers = set()
    by_list = {}
    for name, text in wls.items():
        ts = parse_tickers_text(text)
        by_list[name] = ts
        for tk, mkt in ts:
            all_tickers.add((tk, mkt))

    print(f'📊 總 unique tickers: {len(all_tickers)}\n')

    # 並行 fetch + evaluate
    print(f'抓資料 + 評估 recipes...')
    results = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        for r in ex.map(lambda x: fetch_and_evaluate(*x), all_tickers):
            if r and 'error' not in r:
                results[(r['ticker'], r['market'])] = r

    # 整理通知訊息
    triggered_by_list = {}
    for name, ts in by_list.items():
        triggered = []
        for tk, mkt in ts:
            r = results.get((tk, mkt))
            if r and r.get('n_triggered', 0) > 0:
                triggered.append(r)
        if triggered:
            triggered_by_list[name] = triggered

    if not triggered_by_list:
        print('✅ 所有持倉均無出場觸發，市場平靜')
        return

    # 組訊息
    lines = ['🚪 波段持倉出場提醒']
    lines.append(f'時間：{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append('')

    for name, triggered in triggered_by_list.items():
        lines.append(f'📋 [{name}] ({len(triggered)} 檔觸發)')
        for r in triggered:
            flag = '🇹🇼' if r['market'] == 'tw' else '🇺🇸'
            triggered_names = ', '.join(t['name'] for t in r['triggered'])
            lines.append(
                f'  {flag} {r["ticker"]} ${r["close"]:.2f} '
                f'{r["state_label"]} ({r["state_days"]}d)'
            )
            lines.append(f'    └ 觸發：{triggered_names}')
        lines.append('')

    # 加圖例提示
    lines.append('---')
    lines.append('🎯 D ATR 動態 = OOS 最高效率（推薦）')
    lines.append('⚖️ B 平衡 = E1+E3 雙重確認')
    lines.append('🛡️ A 保守快出 = 太敏感（OOS 不推薦）')
    lines.append('🚀 C 飆股 = 太緊（OOS 不推薦）')

    msg = '\n'.join(lines)
    print(msg)
    print()
    push_line(msg, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
