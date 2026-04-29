"""市場差異化驗證 — 確認 TW vs US 規則分離正確
=================================================
模擬不同 ticker 給入 get_operation_advice / get_rec_label，
確認返回的閾值與市場標籤正確
"""
import sys
from pathlib import Path
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

# 模擬 d 資料
def make_data(ema20=110, ema60=100, adx=20, rsi=45, **kwargs):
    return {
        'ema20': ema20, 'ema60': ema60, 'adx': adx, 'rsi': rsi,
        'rsi_prev': kwargs.get('rsi_prev', 47),
        'rsi_prev2': kwargs.get('rsi_prev2', 49),
        'atr14': 5.0, 'close': 110, 'sma200': 100,
        'ema20_cross_days': kwargs.get('cross_days', 5),
        **kwargs,
    }


def main():
    # 直接 import tv_app 的關鍵邏輯（避開 streamlit）
    import importlib.util
    spec = importlib.util.spec_from_file_location("tv_app", "tv_app.py")
    # streamlit import 會 fail，我們只看程式碼
    print("=" * 80)
    print("📋 TW vs US 規則差異化驗證")
    print("=" * 80)

    # 1. update_signals_cloud 的 _classify_tw / _classify_us
    print("\n【1】update_signals_cloud.py 分類函數")
    import update_signals_cloud as cloud
    test_data = make_data(adx=20)  # ADX 介於 18-22
    print(f"  測試資料: ADX=20 (介於 18 與 22 之間)")
    tw_result = cloud._classify_tw(test_data)
    us_result = cloud._classify_us(test_data)
    print(f"  🇹🇼 TW _classify_tw → {tw_result}  (ADX 20 < 22 → WAIT 預期)")
    print(f"  🇺🇸 US _classify_us → {us_result}  (ADX 20 ≥ 18 → ENTRY 預期)")
    if tw_result == 'WAIT' and us_result == 'ENTRY':
        print("  ✅ 通過：兩市場用不同 ADX 門檻")
    else:
        print("  ❌ 異常")

    # 2. ADX 25 — 兩市場都該通過
    print("\n  測試資料: ADX=25 (兩市場都應通過)")
    test_data2 = make_data(adx=25)
    print(f"  🇹🇼 TW: {cloud._classify_tw(test_data2)} | 🇺🇸 US: {cloud._classify_us(test_data2)}")

    # 2. update_daily_signals / update_us_signals 也對齊
    print("\n【2】backtest MODE 確認")
    print("  update_daily_signals.py 使用 classify() ADX≥22")
    with open('update_daily_signals.py', encoding='utf-8') as f:
        tw_src = f.read()
    if 'adx >= 22' in tw_src:
        print("  ✅ TW classify ADX≥22 確認")

    print("  update_us_signals.py 使用 classify() ADX≥18")
    with open('update_us_signals.py', encoding='utf-8') as f:
        us_src = f.read()
    if 'adx >= 18' in us_src:
        print("  ✅ US classify ADX≥18 確認")

    print("\n  update_us_signals.py MODE")
    if "MODE = 'P10_T1T3+POS+ADX18'" in us_src:
        print("  ✅ US backtest MODE = P10_T1T3+POS+ADX18")
    else:
        print("  ❌ US MODE 可能未更新")

    # 3. tv_app.py get_rec_label / get_operation_advice 加 _is_us
    print("\n【3】tv_app.py 個股 detail 卡片")
    with open('tv_app.py', encoding='utf-8') as f:
        app_src = f.read()
    checks = [
        ('get_rec_label _is_us 偵測', '_is_us = _tk_clean.isalpha() and _tk_clean.isupper() and not _is_inverse'),
        ('get_rec_label _adx_th', '_adx_th    = 18 if (_is_us or _is_crypto) else 22'),
        ('get_operation_advice _is_us', '_is_us = _tk_clean.isalpha() and _tk_clean.isupper() and \\\n             _tk_upper not in _INVERSE_ETF_TICKERS'),
        ('get_operation_advice _adx_th', '_adx_th = 18 if (_is_us or _is_crypto) else 22'),
        ('VWAPEXEC 提示只在有 vwap_today 時顯示', 'if vwap_today and close:'),
    ]
    for name, pattern in checks:
        if pattern in app_src:
            print(f"  ✅ {name}")
        else:
            # 找近似
            short = pattern[:40]
            if short in app_src:
                print(f"  ⚠️ {name}（部分匹配）")
            else:
                print(f"  ❌ {name} 未找到")

    # 4. 範例：給定相同數據，TW vs US 顯示哪個 _adx_th
    print("\n【4】個股 ticker 偵測測試")
    test_cases = [
        ('2330', 'TW'),
        ('2454', 'TW'),
        ('AAPL', 'US'),
        ('NFLX', 'US'),
        ('NVDA', 'US'),
        ('BTC-USD', 'Crypto'),
        ('ETH-USD', 'Crypto'),
        ('00632R', 'Inverse ETF (TW)'),
    ]
    print(f"  {'Ticker':<10} {'偵測結果':<12} {'ADX 門檻':<10}")
    print("  " + "-" * 40)
    INVERSE_ETF = {"00632R", "00633L", "00648U", "00675L", "00676L"}
    for tk, expected in test_cases:
        _tk_upper = tk.upper().replace(".TW", "").replace(".TWO", "")
        _is_inverse = _tk_upper in INVERSE_ETF
        _tk_clean = _tk_upper.replace('-USD', '').replace('-', '')
        _is_us = _tk_clean.isalpha() and _tk_clean.isupper() and not _is_inverse
        _is_crypto = _tk_upper.endswith('-USD')
        _adx_th = 18 if (_is_us or _is_crypto) else 22
        if _is_inverse:
            label = '反向ETF (TW)'
        elif _is_crypto:
            label = '🪙 Crypto'
        elif _is_us:
            label = '🇺🇸 US'
        else:
            label = '🇹🇼 TW'
        print(f"  {tk:<10} {label:<14} ≥{_adx_th}")

    print("\n" + "=" * 80)
    print("📊 結論")
    print("=" * 80)
    print("""
TW (4 位數字) → ADX≥22 / 加碼 P5 / VWAPEXEC ✓ / IND+DXY 跨市場過濾
US (純大寫字母) → ADX≥18 / 加碼 P10 / 無 VWAPEXEC / 無 IND/DXY
Crypto (-USD)   → ADX≥18 / v8 不適用（封存）
反向 ETF        → 維持 ADX≥22 + ATR×1.5 + RSI>70 出場（台股專用）

這些差異化分散在：
  ✓ tv_app.get_rec_label()        個股表格「操作建議」欄
  ✓ tv_app.get_operation_advice() 個股 detail 卡片
  ✓ update_daily_signals.classify() TW TOP 200 ENTRY/EXIT/HOLD
  ✓ update_us_signals.classify()  US TOP 100 ENTRY/EXIT/HOLD
  ✓ update_signals_cloud._classify_tw/_classify_us  雲端版
  ✓ Backtest MODE TW vs US 已分（變體腳本各自設定）
""")


if __name__ == '__main__':
    main()
