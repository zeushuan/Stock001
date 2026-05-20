"""產生 SOXS 5/19 逐分鐘 chart — 標出訊號點 + 情緒陷阱
========================================================
"""
import sys, io, os, json
try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
except Exception:
    pass

from datetime import datetime, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo
import pandas as pd
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from intraday.data import ALPACA_API_KEY, ALPACA_API_SECRET

ET = ZoneInfo('America/New_York')

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
end = datetime.now(timezone.utc)
start = end - timedelta(days=6)
req = StockBarsRequest(symbol_or_symbols='SOXS', timeframe=TimeFrame.Minute,
                        start=start, end=end, feed=DataFeed.IEX)
df = client.get_stock_bars(req).df
if isinstance(df.index, pd.MultiIndex):
    df = df.droplevel('symbol')
df = df.rename(columns={'open':'Open','high':'High','low':'Low',
                          'close':'Close','volume':'Volume'})
df.index = df.index.tz_convert(ET)
df['date'] = df.index.date
df['t'] = df.index.time

target = pd.Timestamp('2026-05-19').date()
day = df[df['date'] == target].copy()
rth = day[(day['t'] >= dtime(9,30)) & (day['t'] < dtime(16,0))].copy()

times = [t.strftime('%H:%M') for t in rth.index]
o = rth['Open'].tolist()
h = rth['High'].tolist()
l = rth['Low'].tolist()
c = rth['Close'].tolist()

day_open = float(rth.iloc[0]['Open'])

# 指標
close = rth['Close']
ema5 = close.ewm(span=5, adjust=False).mean().tolist()
ema20 = close.ewm(span=20, adjust=False).mean().tolist()
sma20 = close.rolling(20).mean()
std20 = close.rolling(20).std()
bb_p1 = (sma20 + std20).tolist()
bb_mid = sma20.tolist()

# 找關鍵點 index
def idx_of(time_str):
    for i, t in enumerate(times):
        if t == time_str:
            return i
    return None

i_entry = idx_of('09:50')
i_exit  = idx_of('10:29')
i_high  = c.index(max(c)) if False else int(rth['High'].values.argmax())
i_low   = int(rth['Low'].values.argmin())

data = {
    'times': times, 'open': o, 'high': h, 'low': l, 'close': c,
    'ema5': ema5, 'ema20': ema20, 'bb_p1': bb_p1, 'bb_mid': bb_mid,
    'day_open': day_open,
    'i_entry': i_entry, 'i_exit': i_exit, 'i_high': i_high, 'i_low': i_low,
    'entry_price': c[i_entry], 'exit_price': c[i_exit],
    'high_price': h[i_high], 'low_price': l[i_low],
}

html = '''<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SOXS 2026-05-19 逐分鐘 — 訊號 vs 情緒</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
body{font-family:-apple-system,'Microsoft JhengHei',sans-serif;background:#0a1020;
     color:#e8f4fd;margin:0;padding:24px;}
.container{max-width:1100px;margin:0 auto;}
h1{color:#5dccdd;border-bottom:3px solid #1a3055;padding-bottom:12px;}
.box{background:#0a1828;border:1px solid #1a3055;border-radius:8px;
     padding:14px 18px;margin:14px 0;}
.green{color:#3dbb6a;font-weight:700;}
.red{color:#ff5555;font-weight:700;}
.gold{color:#f0c030;font-weight:700;}
.cyan{color:#5dccdd;}
table{width:100%;border-collapse:collapse;font-size:.86rem;margin:10px 0;}
th{background:#1a3055;color:#5dccdd;padding:8px;text-align:left;}
td{padding:7px 8px;border-bottom:1px solid #1a2f48;font-family:Consolas,monospace;}
.warn{background:#2a0a0a;border-left:4px solid #ff5555;padding:10px 14px;
      border-radius:6px;margin:10px 0;}
.ok{background:#0a2014;border-left:4px solid #3dbb6a;padding:10px 14px;
    border-radius:6px;margin:10px 0;}
</style></head><body><div class="container">
<h1>📉 SOXS 2026-05-19 逐分鐘 — 訊號 vs 情緒</h1>
<div class="box">
Open <b>$''' + f'{day_open:.4f}' + '''</b> →
Close <b class="red">$''' + f'{c[-1]:.4f}' + '''</b>
(<b class="red">''' + f'{(c[-1]-day_open)/day_open*100:+.2f}%' + '''</b>) ·
High <b>$''' + f'{data["high_price"]:.4f}' + '''</b> @ ''' + times[i_high] + ''' ·
Low <b>$''' + f'{data["low_price"]:.4f}' + '''</b> @ ''' + times[i_low] + '''
</div>

<div id="chart" style="height:560px"></div>

<div class="ok">
🟢 <b>戰法進場</b> 09:50 @ $''' + f'{data["entry_price"]:.4f}' + ''' &nbsp;|&nbsp;
🔴 <b>戰法出場</b> 10:29 @ $''' + f'{data["exit_price"]:.4f}' + ''' &nbsp;|&nbsp;
<b class="green">P&L +''' + f'{(data["exit_price"]-data["entry_price"])/data["entry_price"]*100:.2f}%' + '''</b>
</div>

<div class="warn">
🤑 <b>情緒陷阱 1</b>：10:18 高點 $''' + f'{data["high_price"]:.4f}' + '''
— 此時追高 hold 到收盤 = <b class="red">-9.47%</b><br>
😱 <b>情緒陷阱 2</b>：13:23 低點 $''' + f'{data["low_price"]:.4f}' + '''
— 此時恐慌砍倉 = 賣在全日最低，之後彈回 $''' + f'{c[-1]:.4f}' + '''
</div>

<table>
<thead><tr><th>角色</th><th>進場</th><th>出場</th><th>結果</th></tr></thead>
<tbody>
<tr><td class="green">紀律 (戰法)</td><td>09:50 $''' + f'{data["entry_price"]:.2f}' + '''</td>
<td>10:29 $''' + f'{data["exit_price"]:.2f}' + '''</td>
<td class="green">+''' + f'{(data["exit_price"]-data["entry_price"])/data["entry_price"]*100:.2f}%' + ''' ✅</td></tr>
<tr><td class="red">情緒 A (追高)</td><td>10:18 $''' + f'{data["high_price"]:.2f}' + ''' (FOMO)</td>
<td>15:59 $''' + f'{c[-1]:.2f}' + '''</td><td class="red">-9.47% ☠️</td></tr>
<tr><td class="red">情緒 B (恐慌砍)</td><td>10:18 $''' + f'{data["high_price"]:.2f}' + '''</td>
<td>13:23 $''' + f'{data["low_price"]:.2f}' + ''' (panic)</td>
<td class="red">-15.1% ☠️☠️</td></tr>
</tbody></table>

<script>
const D = ''' + json.dumps(data) + ''';
const traces = [
  {x:D.times,open:D.open,high:D.high,low:D.low,close:D.close,
   type:'candlestick',name:'SOXS 1m',
   increasing:{line:{color:'#3dbb6a'}},decreasing:{line:{color:'#ff5555'}}},
  {x:D.times,y:D.ema5,type:'scatter',mode:'lines',name:'EMA5',
   line:{color:'#f0c030',width:1.3}},
  {x:D.times,y:D.ema20,type:'scatter',mode:'lines',name:'EMA20',
   line:{color:'#5dccdd',width:1.3}},
  {x:D.times,y:D.bb_p1,type:'scatter',mode:'lines',name:'BB+1σ',
   line:{color:'#a87acc',width:1,dash:'dot'}},
  {x:D.times,y:D.bb_mid,type:'scatter',mode:'lines',name:'BB Mid',
   line:{color:'#7a8899',width:1,dash:'dash'}},
  // 進場 marker
  {x:[D.times[D.i_entry]],y:[D.entry_price*0.985],type:'scatter',mode:'markers+text',
   name:'戰法進場',marker:{symbol:'triangle-up',size:18,color:'#3dbb6a',
   line:{color:'#0a4a14',width:2}},text:['🟢進'],textposition:'bottom center',
   textfont:{color:'#3dbb6a',size:13}},
  // 出場 marker
  {x:[D.times[D.i_exit]],y:[D.exit_price*1.015],type:'scatter',mode:'markers+text',
   name:'戰法出場',marker:{symbol:'x',size:16,color:'#ff5555',
   line:{color:'#000',width:1.5}},text:['🔴出'],textposition:'top center',
   textfont:{color:'#ff5555',size:13}},
];
const layout = {
  paper_bgcolor:'#0a1828',plot_bgcolor:'#050e1a',
  font:{color:'#c8dff0',family:'Consolas,monospace',size:10},
  margin:{t:40,b:60,l:55,r:20},
  title:{text:'SOXS 2026-05-19 1m K線 + 戰法訊號',font:{color:'#5dccdd',size:14}},
  xaxis:{gridcolor:'#1a2f48',rangeslider:{visible:false},tickangle:-45,
         nticks:26,title:'時間 (ET)'},
  yaxis:{gridcolor:'#1a2f48',title:'價格 ($)'},
  legend:{bgcolor:'#0a1020',bordercolor:'#1a3055',borderwidth:1,
          orientation:'h',y:1.06},
  hovermode:'x unified',
  annotations:[
    {x:D.times[D.i_high],y:D.high_price,text:'🤑 追高陷阱 $'+D.high_price.toFixed(2),
     showarrow:true,arrowcolor:'#ff5555',arrowhead:2,ax:0,ay:-40,
     font:{color:'#ff5555',size:11},bgcolor:'#2a0a0a',bordercolor:'#ff5555'},
    {x:D.times[D.i_low],y:D.low_price,text:'😱 恐慌砍底 $'+D.low_price.toFixed(2),
     showarrow:true,arrowcolor:'#ff5555',arrowhead:2,ax:0,ay:40,
     font:{color:'#ff5555',size:11},bgcolor:'#2a0a0a',bordercolor:'#ff5555'},
  ],
};
Plotly.newPlot('chart',traces,layout,{responsive:true,displaylogo:false});
</script>
<div style="text-align:center;color:#5a7090;font-size:.82rem;margin-top:24px;
     border-top:1px solid #1a3055;padding-top:14px">
Stock001 ｜ SOXS 5/19 訊號 vs 情緒教學案例
</div>
</div></body></html>'''

Path('soxs_0519_chart.html').write_text(html, encoding='utf-8')
print(f'✅ soxs_0519_chart.html 已生成 ({len(html):,} bytes)')
print(f'   進場 09:50 ${data["entry_price"]:.4f} → 出場 10:29 ${data["exit_price"]:.4f}')
print(f'   高點 {times[i_high]} ${data["high_price"]:.4f} / 低點 {times[i_low]} ${data["low_price"]:.4f}')
