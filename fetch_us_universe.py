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

# 完整 S&P 500 + NASDAQ 100 + 熱門 ETF + 中小型熱門 ≈ 530 unique
US_TOP = [
    # ─── 大型科技 ─────────────────────────────────
    'AAPL','MSFT','GOOGL','GOOG','AMZN','META','NVDA','TSLA','AVGO','TSM',
    'ORCL','ASML','NFLX','AMD','ADBE','CRM','INTC','QCOM','CSCO','TXN',
    'INTU','IBM','CDW','SAP','SNOW','NOW','SHOP','PANW','PLTR','CRWD',
    'MDB','TEAM','WDAY','DDOG','NET','OKTA','HUBS','VEEV','ZS','ANET',
    'SQ','SOFI','PYPL','HOOD','COIN','MSTR','RBLX','DOCU','ZM','TWLO',
    # ─── 半導體 ───────────────────────────────────
    'AMAT','MU','LRCX','KLAC','MRVL','SNPS','CDNS','NXPI','ADI','MCHP',
    'ON','MPWR','SWKS','TER','MKSI','ENTG','ENPH','SLAB','LSCC','RMBS',
    'AMKR','UCTT','ICHR','CRUS','QRVO','SMCI','ALGM','ASRT',
    # ─── 金融 ─────────────────────────────────────
    'JPM','BAC','WFC','C','GS','MS','BLK','AXP','SCHW','SPGI',
    'V','MA','BX','KKR','CB','PGR','ICE','CME','TRV','APO',
    'AFL','ALL','MTB','USB','PNC','TFC','BK','STT','NTRS','ACGL',
    'BRK-B','TROW','RJF','LPLA','RY','TD','BMO','HSBC','UBS','BSX',
    # ─── 消費 ─────────────────────────────────────
    'WMT','COST','HD','MCD','SBUX','NKE','LULU','TGT','LOW','BKNG',
    'CMG','RCL','MAR','HLT','TJX','ROST','DLTR','DG','YUM','UBER',
    'EXPE','ABNB','DASH','CART','LYFT','GRMN','NCLH','CCL','POOL','CASY',
    'KO','PEP','PG','CL','KMB','MO','PM','CHD','EL','MNST',
    'KHC','GIS','K','HSY','SJM','CPB','TSN','ADM','TAP','BG',
    # ─── 健康 ─────────────────────────────────────
    'UNH','JNJ','LLY','ABBV','MRK','PFE','TMO','ABT','DHR','BMY',
    'AMGN','GILD','ISRG','REGN','VRTX','CI','HUM','CVS','HCA','ELV',
    'BIIB','MCK','COR','SYK','MDT','EW','ZTS','IDXX','DXCM','ALGN',
    'CTLT','HOLX','RMD','VTRS','BAX','BDX','GEHC','WST','MTD','LH',
    # ─── 工業 ─────────────────────────────────────
    'CAT','HON','UPS','RTX','BA','LMT','GE','DE','MMM','UNP',
    'ETN','EMR','ITW','PCAR','GD','NOC','LHX','TDG','PH','ROP',
    'CSX','NSC','FDX','WM','RSG','XYL','PNR','AME','FAST','DOV',
    'SWK','TT','CMI','PWR','OTIS','PAYX','ROK','ROL','VRSK','GWW',
    # ─── 能源 ─────────────────────────────────────
    'XOM','CVX','COP','SLB','OXY','EOG','PSX','MPC','VLO','PXD',
    'WMB','KMI','OKE','LNG','TRP','ENB','BKR','HAL','DVN','APA',
    'FANG','MRO','CTRA','EQT','NFG','HP','RIG','PARR',
    # ─── 通訊 ─────────────────────────────────────
    'DIS','CMCSA','T','VZ','TMUS','CHTR','EA','TTWO','SPOT','PARA',
    'WBD','FOX','FOXA','LYV','OMC','IPG','TKO',
    # ─── 公用事業 / 房地產 ────────────────────────
    'NEE','SO','DUK','PLD','AMT','CCI','EQIX','PSA','SPG','SRE',
    'AEP','D','XEL','EXC','PEG','ED','PPL','AEE','WEC','EIX',
    'O','DLR','EQR','AVB','UDR','ESS','MAA','CPT','BXP','VTR',
    'WELL','VICI','SBAC','EXR','MPW','CTRE','HST',
    # ─── 原物料 ───────────────────────────────────
    'LIN','SHW','APD','FCX','NEM','ECL','DOW','DD','PPG','VMC',
    'MLM','NUE','STLD','RS','CLF','X','MOS','CF','LYB','BLL',
    # ─── 流動 ETF ─────────────────────────────────
    'SPY','QQQ','IWM','DIA','VOO','VTI','VEA','VWO','BND','TLT',
    'GLD','SLV','USO','UNG','EEM','XLK','XLF','XLV','XLE','XLY',
    'XLP','XLI','XLU','XLB','XLRE','XLC','XOP','XBI','SMH','SOXX',
    'IBB','ARKK','ARKQ','ARKW','ARKG','ARKF','ARKX','TQQQ','SQQQ','SOXL',
    'SOXS','TMF','TMV','UPRO','SPXU','SVXY','UVXY','VXX','UCO','SCO',
    'NUGT','DUST','JNUG','JDST','BOIL','KOLD','GUSH','DRIP','LABU','LABD',
    'EWJ','EWZ','EWY','FXI','MCHI','INDA','EWG','EWU','EWC','EWA',
    # ─── 中小型熱門 / 主題 ───────────────────────
    'RIVN','LCID','RKLB','RDDT','SOFI','UPST','AFRM','OPEN','REKR',
    'DKNG','PENN','BYD','MGM','LVS','WYNN','CZR','SIRI','VRSN','AKAM',
    'SNAP','PINS','MTCH','EBAY','ETSY','W','CHWY','PETS','CART','GOEV',
    'NIO','XPEV','LI','BIDU','BABA','JD','PDD','TCEHY','HKEX',
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
