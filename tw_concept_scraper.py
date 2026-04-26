"""
台股每檔概念股 + 公司簡介自動爬蟲

來源：
  - yfinance .info（longBusinessSummary, industry, sector）
  - 規則分類器：英文 summary + 產業 → 推斷中文概念標籤

輸出：
  tw_stock_meta.json  {ticker: {desc, short_name, industry_zh, industry_en,
                                tags: [...]}}

用法：
  python tw_concept_scraper.py                # 全 universe
  python tw_concept_scraper.py --max 100
  python tw_concept_scraper.py --info
"""
import sys
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except: pass

import argparse
import json
import re
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

ROOT = Path(__file__).parent
META_FILE = ROOT / 'tw_stock_meta.json'
UNIVERSE_FILE = ROOT / 'tw_universe.txt'

WRITE_LOCK = threading.Lock()

# ─────────────────────────────────────────────────────────────────
# 關鍵字 → 概念標籤對照（英文/中文 keyword → Chinese tag）
# 越往下越具體；同一檔可被打多個標籤
# ─────────────────────────────────────────────────────────────────
KEYWORD_RULES = [
    # 半導體類
    (r'\bnor\s*flash\b', "NOR Flash"),
    (r'\bnand\s*flash\b', "NAND Flash"),
    (r'\bdram\b|dynamic random', "DRAM"),
    (r'\b(memory|flash)\s*(chip|product|module)?', "記憶體"),
    (r'\bcowos\b|chip[\s\-]on[\s\-]wafer', "CoWoS先進封裝"),
    (r'\bfan[\s\-]?out\b|advanced packag', "先進封裝"),
    (r'\b(package|packaging|assembly)\s*(and|&)?\s*(test|testing)', "封測"),
    (r'\bsilicon\s*photonic|optical\s*intercon', "矽光子"),
    (r'\bcpo\b|co[\s\-]?packaged\s*optic', "CPO"),
    (r'\bmcu|microcontroller', "MCU"),
    (r'\bsoc\b|system[\s\-]on[\s\-]chip', "SoC"),
    (r'\bdriver\s*ic|tddi|display\s*driver', "面板驅動IC"),
    (r'\btouch\s*ic|touch\s*controller', "觸控IC"),
    (r'\bpower\s*management|pmic', "PMIC"),
    (r'\bmosfet|igbt|power\s*semiconductor', "功率半導體"),
    (r'\bsic\b|silicon\s*carbide|gan\b|gallium\s*nitr', "第三代半導體"),
    (r'\bdiode|rectifier|分離式|discrete', "分離式元件"),
    (r'\bfoundry|wafer\s*manufactur', "晶圓代工"),
    (r'\bsilicon\s*wafer|矽晶圓', "矽晶圓"),
    (r'\bfabless|ic\s*design|integrated\s*circuit\s*design', "IC設計"),
    (r'\bsemiconductor|integrated\s*circuit', "半導體"),

    # PCB / 載板
    (r'\babf\s*substrate|abf\s*載板', "ABF載板"),
    (r'\bccl\b|copper[\s\-]?clad', "CCL/銅箔基板"),
    (r'\bflexible\s*pcb|fpc\b', "軟板FPC"),
    (r'\bpcb\b|printed\s*circuit\s*board', "PCB"),

    # AI / 伺服器
    (r'\bai\s*server|artificial\s*intelligence\s*server', "AI伺服器"),
    (r'\bartificial\s*intelligence|machine\s*learning|deep\s*learning', "AI"),
    (r'\bserver\b|cloud\s*server|data\s*center', "伺服器"),
    (r'\bnvidia|nvda', "Nvidia供應鏈"),
    (r'\bchassis|滑軌|server\s*case|機構件|rail\s*kit', "伺服器機殼"),
    (r'\bthermal\s*module|liquid\s*cool|cooling\s*solution|heat\s*sink|散熱', "散熱"),

    # 通訊 / 5G / 衛星
    (r'\bsatellite|leo\s*sat|低軌衛星', "低軌衛星"),
    (r'\b5g\b|millimeter\s*wave', "5G"),
    (r'\bwireless|rf\s*module|antenna|天線', "通訊"),
    (r'\boptical\s*communic|fiber\s*optic|fttx|ftth', "光通訊"),

    # 蘋果 / 手機
    (r'\bapple\s*inc|iphone|airpod|apple\s*watch|macbook', "蘋果概念"),
    (r'\bsmart\s*phone|smartphone|mobile\s*phone|平板|tablet', "手機/平板"),

    # 車用 / 電動車
    (r'\bautomotive|vehicle|car\b|automobile|車用', "車用電子"),
    (r'\bev\b|electric\s*vehicle|電動車', "電動車"),
    (r'\bautonomous\s*driv|adas|self[\s\-]drive', "自駕"),
    (r'\bcharging\s*station|充電樁', "充電樁"),
    (r'\blithium|li[\s\-]?ion|battery|電池', "電池"),
    (r'\benergy\s*storage|儲能', "儲能"),

    # 機器人 / 自動化
    (r'\brobot|robotic|機器人', "機器人"),
    (r'\bdrone|uav|無人機|無人飛行', "無人機"),
    (r'\bautomation|工業自動化', "工業自動化"),
    (r'\bindustrial\s*pc|工業電腦', "工業電腦"),

    # 軍工 / 國防
    (r'\bdefense|military|missile|武器|飛彈|軍用', "軍工/國防"),
    (r'\baerospace|航太|aircraft', "航太"),
    (r'\bship|shipbuilding|船舶|造船|submarine', "造船"),

    # 顯示器 / 光電
    (r'\bpanel|display|liquid\s*crystal|lcd|oled', "面板/光電"),
    (r'\bled\b', "LED"),
    (r'\boptical|lens|camera\s*module|optical\s*component', "光學/光電"),
    (r'\bvr\b|virtual\s*reality|ar\b|augmented|metavers|元宇宙', "AR/VR/元宇宙"),
    (r'\bhead[\s\-]?mount|hmd', "頭戴顯示"),

    # 電力 / 能源
    (r'\btransformer|substation|switchgear|gis|重電', "重電"),
    (r'\bsmart\s*grid|grid\s*resilien|電網', "電網"),
    (r'\bpower\s*cable|電線電纜', "電線電纜"),
    (r'\bups\s|不斷電|uninterrupt', "UPS"),
    (r'\bbbu|battery\s*backup', "BBU備援電池"),
    (r'\bsolar|photovoltaic|太陽能|pv\s*module', "太陽能"),
    (r'\bwind\s*power|wind\s*turbine|離岸風電|風電', "風電"),
    (r'\bnuclear|核能|核電', "核能"),
    (r'\bhydrogen|fuel\s*cell|氫能', "氫能"),

    # 化工 / 材料
    (r'\bplastic|塑膠|塑化', "塑化"),
    (r'\bchemical|化工|化學', "化工"),
    (r'\bglass\s*fiber|fiberglass|玻纖', "玻纖布/材料"),
    (r'\bglass\b|玻璃', "玻璃"),
    (r'\bsteel|鋼鐵|stainless\s*steel', "鋼鐵"),
    (r'\bcement|水泥', "水泥"),
    (r'\bnon[\s\-]?ferrous|copper|aluminum|alumin|金屬', "金屬"),
    (r'\bresin|樹脂', "樹脂"),

    # 紡織 / 食品 / 傳產
    (r'\btextile|garment|fabric|紡織', "紡織"),
    (r'\bfunctional\s*fabric|outdoor\s*fabric|戶外', "戶外/機能布"),
    (r'\bfood|beverage|食品|飲料|餐飲', "食品/餐飲"),
    (r'\bagricult|農業', "農業"),
    (r'\bpaper|造紙', "造紙"),
    (r'\brubber|tire|橡膠', "橡膠"),

    # 金融
    (r'\binsuran|保險', "保險"),
    (r'\bbank|銀行', "銀行"),
    (r'\bsecurit(?:y|ies)|brokerage|證券', "證券"),
    (r'\bfinancial\s*holding|金控', "金控"),

    # 其他
    (r'\bairline|aviation|airport|航空', "航空"),
    (r'\bshipping|container\s*ship|貨櫃|航運', "航運"),
    (r'\breal\s*estate|property|construction|營建|建設', "營建"),
    (r'\btourism|hotel|觀光|飯店', "觀光餐旅"),
    (r'\bdepartment\s*store|百貨', "百貨"),
    (r'\bmedical\s*device|醫材', "醫材"),
    (r'\bpharmaceut|biotech|醫藥|生技', "生技"),
    (r'\bhealth\s*care|醫療', "醫療"),
    (r'\bblockchain|cryptocurrency|區塊鏈|比特幣', "區塊鏈"),
    (r'\bcybersecurity|資安', "資安"),
    (r'\bnetwork(?:\s*equipment)?|switch|router|網通', "網通"),
    (r'\bdistribution|distributor|通路', "電子通路"),
    (r'\bgaming|game\s*develop|遊戲', "遊戲"),
    (r'\bcomponent|connector|connector\s*assembly|連接器', "連接器"),
    (r'\bspecial\s*chemical|specialty\s*chemical', "特化"),
]


def classify_tags(summary_en: str, industry_en: str, name: str) -> list:
    """根據英文 summary + industry + 名稱推斷概念標籤"""
    text = ((summary_en or "") + " " + (industry_en or "") + " " + (name or "")).lower()
    tags = []
    seen = set()
    for pat, tag in KEYWORD_RULES:
        if re.search(pat, text):
            if tag not in seen:
                tags.append(tag); seen.add(tag)
    return tags


def load_universe() -> list:
    """從 tw_universe.txt 取得 [(ticker, name_zh, industry_zh)]"""
    out = []
    if not UNIVERSE_FILE.exists(): return out
    for line in UNIVERSE_FILE.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split('|')
        if len(parts) >= 5:
            out.append((parts[0], parts[1], parts[4]))
        elif len(parts) >= 2:
            out.append((parts[0], parts[1], ''))
    return out


def load_existing() -> dict:
    if META_FILE.exists():
        try:
            return json.loads(META_FILE.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}


def save_meta(d: dict):
    tmp = META_FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2),
                   encoding='utf-8')
    tmp.replace(META_FILE)


def fetch_one(ticker: str, name_zh: str, industry_zh: str) -> tuple:
    sym = f"{ticker}.TW"
    try:
        info = yf.Ticker(sym).get_info()
        if not info or not isinstance(info, dict):
            return ticker, None
        summary = (info.get('longBusinessSummary') or '').strip()
        industry_en = (info.get('industry') or '').strip()
        sector_en = (info.get('sector') or '').strip()
        short_name = (info.get('shortName') or info.get('longName') or '').strip()
        # 標籤
        tags = classify_tags(summary, industry_en, short_name)
        # desc：取 summary 第一句，限制 80 字
        desc = ''
        if summary:
            first_dot = summary.find('. ')
            desc = summary[:first_dot+1] if first_dot > 0 else summary[:200]
        return ticker, dict(
            name_zh=name_zh,
            short_name_en=short_name,
            industry_zh=industry_zh,
            industry_en=industry_en,
            sector_en=sector_en,
            summary_en=summary[:600],
            desc_en=desc,
            tags=tags,
        )
    except Exception as e:
        return ticker, None


def scrape(max_workers: int = 4, limit: int = None,
           retry_failed: bool = False):
    universe = load_universe()
    existing = load_existing()

    todo = []
    for ticker, name_zh, industry_zh in universe:
        if ticker in existing and existing[ticker] is not None:
            if not retry_failed: continue
        todo.append((ticker, name_zh, industry_zh))

    if limit: todo = todo[:limit]
    print(f"[掃描] universe {len(universe)}，已紀錄 {len(existing)}，"
          f"本次處理 {len(todo)} 檔（workers={max_workers}）")
    if not todo: return

    t0 = time.time()
    done = 0
    new_data = dict(existing)
    success = 0

    def _task(item):
        ticker, name_zh, industry_zh = item
        time.sleep(0.3)
        return fetch_one(ticker, name_zh, industry_zh)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_task, x) for x in todo]
        for fut in as_completed(futures):
            try:
                ticker, meta = fut.result(timeout=30)
            except Exception:
                continue
            done += 1
            if meta is not None:
                new_data[ticker] = meta
                success += 1
            if done % 25 == 0 or done == len(todo):
                with WRITE_LOCK:
                    save_meta(new_data)
                eta = (time.time()-t0) / done * (len(todo)-done)
                print(f"  [{done}/{len(todo)}] {ticker} "
                      f"(success {success})  ETA {eta/60:.1f}min", flush=True)

    print(f"\n總耗時 {(time.time()-t0)/60:.1f} min  成功 {success}/{len(todo)}")


def info():
    universe = load_universe()
    existing = load_existing()
    print(f"universe: {len(universe)}")
    print(f"已紀錄:   {len(existing)}")
    have_tags = sum(1 for v in existing.values() if v and v.get('tags'))
    print(f"  有標籤: {have_tags}")
    if existing:
        from collections import Counter
        tag_counts = Counter()
        for v in existing.values():
            if not v: continue
            for t in v.get('tags', []):
                tag_counts[t] += 1
        print(f"\n前 20 大標籤分布：")
        for tag, n in tag_counts.most_common(20):
            print(f"  {n:5d}  {tag}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--max', type=int, default=None)
    ap.add_argument('--retry-failed', action='store_true')
    ap.add_argument('--info', action='store_true')
    args = ap.parse_args()
    if args.info: info(); return
    scrape(max_workers=args.workers, limit=args.max,
           retry_failed=args.retry_failed)


if __name__ == '__main__':
    main()
