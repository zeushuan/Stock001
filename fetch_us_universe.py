"""抓 US TOP 200 大型股 6 年日線資料
====================================
S&P 500 中最大市值 + 熱門 NASDAQ 100 + 流動 ETF
"""
import sys, time
from pathlib import Path
import yfinance as yf
import pandas as pd
import ta
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

CACHE = Path(__file__).parent / 'data_cache'
CACHE.mkdir(exist_ok=True)

# Top 200 US stocks（S&P 500 大型股 + NASDAQ-100 + 熱門 ETF）
US_TOP = [
    # 大型科技 (Mega Cap)
    'AAPL','MSFT','GOOGL','GOOG','AMZN','META','NVDA','TSLA','AVGO','TSM',
    'ORCL','ASML','NFLX','AMD','ADBE','CRM','INTC','QCOM','CSCO','TXN',
    # 半導體
    'AMAT','MU','LRCX','KLAC','MRVL','SNPS','CDNS','NXPI','ADI','MCHP',
    'ON','MPWR','SWKS','TER','MKSI','ENTG','ENPH','SLAB','LSCC','RMBS',
    # 軟體 / SaaS
    'NOW','SHOP','PANW','SNOW','PLTR','CRWD','MDB','TEAM','WDAY','DDOG',
    'NET','OKTA','HUBS','VEEV','ZS','ANET','SQ','SOFI','PYPL','HOOD',
    # AI / 數據
    'INTU','IBM','CDW','GLOB','ESTC','AI','PATH',
    # 金融
    'JPM','BAC','WFC','C','GS','MS','BLK','AXP','SCHW','SPGI',
    'V','MA','BX','KKR','CB','PGR','ICE','CME','MMC','TRV',
    # 消費
    'WMT','COST','HD','MCD','SBUX','NKE','LULU','TGT','LOW','BKNG',
    'CMG','RCL','MAR','HLT','TJX','ROST','DLTR','DG','YUM','UBER',
    # 健康
    'UNH','JNJ','LLY','ABBV','MRK','PFE','TMO','ABT','DHR','BMY',
    'AMGN','GILD','ISRG','REGN','VRTX','CI','HUM','CVS','HCA','BSX',
    # 工業
    'CAT','HON','UPS','RTX','BA','LMT','GE','DE','MMM','UNP',
    'ETN','EMR','ITW','PCAR','GD','NOC','LHX','TDG','PH','ROP',
    # 能源
    'XOM','CVX','COP','SLB','OXY','EOG','PSX','HES','MPC','VLO',
    # 通訊
    'DIS','CMCSA','T','VZ','TMUS','CHTR','EA','TTWO','SPOT','RBLX',
    # 公用事業 / 房地產
    'NEE','SO','DUK','PLD','AMT','CCI','EQIX','PSA','SPG',
    # 流動 ETF
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT',
    'GLD','SLV','USO','EEM','XLK','XLF','XLV','XLE','XLY','XLP',
    'XLI','XLU','XLB','XLRE','XOP','XBI','SMH','SOXX','IBB','ARKK',
    # 中小型熱門
    'COIN','MSTR','RIVN','LCID','RKLB','DASH','ABNB','CART','RDDT',
]
# 去重
US_TOP = sorted(set(US_TOP))
print(f"US universe: {len(US_TOP)} tickers")


def calc_ind(df):
    """計算 v8 指標（同 backtest_tw_all）"""
    if df is None or len(df) < 280: return None
    df = df.copy()
    df['e10']  = ta.trend.ema_indicator(df['Close'], window=10)
    df['e20']  = ta.trend.ema_indicator(df['Close'], window=20)
    df['e60']  = ta.trend.ema_indicator(df['Close'], window=60)
    df['e120'] = ta.trend.ema_indicator(df['Close'], window=120)
    df['rsi']  = ta.momentum.rsi(df['Close'], window=14)
    df['adx']  = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    macd = ta.trend.MACD(df['Close'])
    df['mh']   = macd.macd_diff()
    df['atr']  = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], 14)
    bb = ta.volatility.BollingerBands(df['Close'], window=20)
    df['pctb'] = bb.bollinger_pband()
    return df


def fetch_one(ticker):
    out = CACHE / f'{ticker}.parquet'
    if out.exists():
        try:
            df = pd.read_parquet(out)
            if len(df) >= 280: return True
        except: pass
    try:
        df = yf.Ticker(ticker).history(period='6y', interval='1d', auto_adjust=False)
        if df is None or df.empty or len(df) < 280:
            return False
        df = df[['Open','High','Low','Close','Volume']]
        df = calc_ind(df)
        if df is None: return False
        df.to_parquet(out)
        return True
    except Exception as e:
        print(f"  [{ticker}] err: {str(e)[:60]}")
        return False


def main():
    print(f"\n抓 {len(US_TOP)} 檔 US 股票 6 年日線（yfinance）...\n")
    todo = [t for t in US_TOP if not (CACHE / f'{t}.parquet').exists()]
    print(f"已快取：{len(US_TOP) - len(todo)}")
    print(f"待抓：{len(todo)}\n")

    if not todo:
        print("✅ 全部已快取")
        return

    t0 = time.time()
    ok = 0; fail = 0
    for i, t in enumerate(todo, 1):
        if fetch_one(t):
            ok += 1
        else:
            fail += 1
        if i % 20 == 0:
            elapsed = (time.time() - t0) / 60
            print(f"[{i:3d}/{len(todo)}] ok={ok} fail={fail}  已 {elapsed:.1f} min")
        time.sleep(0.3)

    elapsed = (time.time() - t0) / 60
    print(f"\n總耗時 {elapsed:.1f} min  成功 {ok}/{len(todo)}  失敗 {fail}")


if __name__ == '__main__':
    main()
