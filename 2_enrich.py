"""
STEP 2 — Enricher
=================
Reads companies_scraped.csv (output of 1_scrape.py) and for each company:
  - Fetches text from the company's website
  - Searches Wikipedia for the company and saves the article text
  - Classifies the industry using keyword matching
  - Records the number of keyword hits for the winning industry

Output columns in companies_final.csv:
  name, website, website_text, wikipedia_text, industry, corresponding_keyword_hits

Failsafe: progress is saved to enrich_progress.json after every company.
If the script crashes, re-running it will skip already-enriched companies.

Requirements:
    pip install requests beautifulsoup4

Usage:
    python 2_enrich.py
"""

import os
import csv
import json
import time
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup


_SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV     = os.path.join(_SCRIPT_DIR, "data/companies_scraped.csv")
OUTPUT_CSV    = os.path.join(_SCRIPT_DIR, "data/companies_final.csv")
PROGRESS_FILE = os.path.join(_SCRIPT_DIR, "data/enrich_progress.json")


# ─────────────────────────────────────────────
# INDUSTRY KEYWORDS
# ─────────────────────────────────────────────

INDUSTRY_KEYWORDS = {
    "Agritech": [
        "farm", "crop", "agri", "fertilizer", "seed", "livestock", "aquaculture", "fishery",
        "农业", "农场", "种植", "农作物", "化肥", "种子", "畜牧", "水产养殖", "渔业", "农林",
    ],
    "Air Transport": [
        "airline", "aviation", "airport", "aerospace", "aircraft", "air cargo", "air freight",
        "航空", "机场", "航天", "飞机", "空运", "航空货运",
    ],
    "Built Environment": [
        "construction", "real estate", "property", "building", "architect", "infra",
        "civil engineering", "housing", "developer", "contractor", "renovation", "interior design",
        "建筑", "房地产", "地产", "楼宇", "基础设施", "土木工程", "住宅", "开发商", "承包商", "装修", "室内设计",
    ],
    "Business Services": [
        "consult", "advisory", "accounting", "legal", "law firm", "recruit", "audit",
        "insurance", "invest", "outsourc",
        "咨询", "顾问", "会计", "法律", "律师", "人力资源", "招聘", "审计", "保险", "投资", "外包",
    ],
    "Electronics": [
        "electron", "semiconductor", "chip", "circuit", "pcb", "display", "battery",
        "sensor", "component", "led",
        "电子", "半导体", "芯片", "电路", "印刷电路板", "显示屏", "电池", "传感器", "元器件", "发光二极管",
    ],
    "Energy & Chemicals": [
        "energy", "oil", "gas", "chemical", "petroleum", "solar", "wind power", "coal",
        "refinery", "power plant", "nuclear", "petrochemi",
        "能源", "石油", "天然气", "化工", "石化", "太阳能", "风能", "煤炭", "炼油", "发电", "核能", "新能源",
    ],
    "Food Manufacturing": [
        "food manufactur", "food produc", "beverage", "snack", "dairy", "meat process",
        "packaged food", "food process", "condiment", "flour", "edible oil",
        "食品制造", "食品生产", "饮料", "零食", "乳制品", "肉类加工", "包装食品", "食品加工", "调味品", "面粉", "食用油",
    ],
    "Food Services": [
        "restaurant", "cater", "cafe", "dining", "food service", "canteen", "fast food", "bakery",
        "餐厅", "餐饮", "咖啡", "食堂", "快餐", "烘焙", "酒吧",
    ],
    "Healthcare & Biomedical": [
        "health", "medical", "hospital", "pharma", "biotech", "clinic", "drug",
        "diagnostic", "life science", "medical device", "genomic",
        "医疗", "医学", "医院", "制药", "药业", "生物技术", "诊所", "药物", "诊断", "生命科学", "医疗器械", "基因",
    ],
    "ICT & Media": [
        "tech", "software", "internet", "media", "telecom", "digital", "cloud",
        "data", "artificial intel", "platform", "saas", "cybersecur", "broadcast", "publish",
        "科技", "软件", "互联网", "传媒", "电信", "数字", "云计算", "数据", "人工智能", "平台", "网络安全", "广播", "出版",
    ],
    "Land Transport": [
        "truck", "bus", "rail", "road transport", "ground transport", "coach", "metro", "tram",
        "货车", "巴士", "铁路", "公路运输", "陆路运输", "地铁", "有轨电车", "公路货运",
    ],
    "Logistics": [
        "logistic", "supply chain", "warehouse", "distribution", "cargo", "courier",
        "fulfillment", "3pl", "freight forward", "cold chain",
        "物流", "供应链", "仓储", "配送", "货运", "快递", "货运代理", "冷链",
    ],
    "Marine & Offshore Energy": [
        "marine", "offshore", "vessel", "port", "maritime", "subsea", "shipyard",
        "oil rig", "naval", "dredg",
        "海洋", "海上", "船舶", "港口", "航运", "海底", "造船", "钻台", "疏浚",
    ],
    "Precision Engineering": [
        "precision", "machin", "tooling", "mold", "cnc", "automat", "robot",
        "equipment manufactur", "industrial machin", "stamping", "casting",
        "精密", "机械加工", "模具", "数控", "自动化", "机器人", "设备制造", "工业机械", "冲压", "铸造",
    ],
    "Retail": [
        "retail", "store", "shop", "e-commerce", "supermarket", "mall",
        "consumer goods", "brand", "fashion", "luxury", "convenience store",
        "零售", "门店", "商店", "电商", "超市", "商场", "消费品", "品牌", "时尚", "奢侈品", "便利店",
    ],
    "Sea Transport": [
        "shipping line", "sea freight", "container ship", "ocean freight", "liner",
        "bulk carrier", "tanker", "ferry", "ship manag",
        "班轮", "海运", "集装箱船", "远洋货运", "散货船", "油轮", "渡轮", "船舶管理",
    ],
    "Urban Solutions": [
        "smart city", "urban", "waste manag", "water treat", "environ", "sustainab",
        "green energy", "sanitat", "utilities", "urban plan", "sewage", "recycl",
        "智慧城市", "城市", "废物管理", "水处理", "环保", "可持续", "绿色能源", "环卫", "公用事业", "城市规划", "污水", "循环",
    ],
    "Wholesale Trade": [
        "wholesale", "trading", "import", "export", "distribut", "commodit",
        "sourcing", "procure", "merchant",
        "批发", "贸易", "进口", "出口", "经销", "大宗商品", "采购", "商行", "跨境",
    ],
}


# ─────────────────────────────────────────────
# FETCH HELPERS
# ─────────────────────────────────────────────

def fetch_website_text(url, timeout=12):
    """Fetch and return cleaned visible text from a URL. Returns empty string on failure."""
    if not url:
        return ""
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"},
                         timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "head"]):
            tag.decompose()
        return soup.get_text(" ", strip=True)[:8000]
    except Exception:
        return ""


def fetch_wikipedia_text(company_name, timeout=10):
    """
    Search Wikipedia for the company name and return the plain text of the
    first matching article. Returns empty string if nothing is found.
    Uses the Wikipedia API — no scraping needed.
    """
    try:
        # Step 1: search for the company name
        search_url = "https://en.wikipedia.org/w/api.php"
        search_params = {
            "action": "query",
            "list": "search",
            "srsearch": company_name,
            "srlimit": 1,
            "format": "json",
        }
        r = requests.get(search_url, params=search_params,
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        r.raise_for_status()
        results = r.json().get("query", {}).get("search", [])
        if not results:
            return ""

        page_title = results[0]["title"]

        # Step 2: fetch the article extract (plain text summary)
        extract_params = {
            "action": "query",
            "titles": page_title,
            "prop": "extracts",
            "explaintext": True,   # plain text, no HTML
            "exintro": False,      # full article, not just intro
            "format": "json",
        }
        r = requests.get(search_url, params=extract_params,
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
        page  = next(iter(pages.values()))
        text  = page.get("extract", "")
        return text[:8000]  # cap at 8000 chars

    except Exception:
        return ""


# ─────────────────────────────────────────────
# INDUSTRY CLASSIFICATION
# ─────────────────────────────────────────────

def classify_industry(text):
    """
    Returns (industry_label, keyword_hit_count).
    If there are ties, all tied industries are joined with ' / '.
    """
    tl     = text.lower()
    scores = {ind: sum(1 for kw in kws if kw in tl)
              for ind, kws in INDUSTRY_KEYWORDS.items()}
    top    = max(scores.values())
    if top == 0:
        return "Unknown", 0
    tied   = [ind for ind, score in scores.items() if score == top]
    return " / ".join(tied), top


# ─────────────────────────────────────────────
# CHECKPOINT HELPERS
# ─────────────────────────────────────────────

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            done = json.load(f)
        print(f"  Resuming: {len(done)} companies already enriched.")
        return {row["name"]: row for row in done}
    return {}


def save_progress(done_dict):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(done_dict.values()), f, ensure_ascii=False, indent=2)


def clear_progress():
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


# ─────────────────────────────────────────────
# CSV HELPERS
# ─────────────────────────────────────────────

def read_input_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"\n  Input file not found: {path}\n"
            f"  Run 1_scrape.py first.\n"
        )
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def save_output_csv(rows, path):
    fieldnames = [
        "name", "website", "website_text", "wikipedia_text",
        "industry", "corresponding_keyword_hits",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n✅  {len(rows)} companies saved → {path}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=== Step 2: Enriching ===\n")

    companies = read_input_csv(INPUT_CSV)
    done      = load_progress()
    results   = list(done.values())
    remaining = [c for c in companies if c["name"] not in done]

    print(f"  Total companies : {len(companies)}")
    print(f"  Already enriched: {len(done)}")
    print(f"  Remaining       : {len(remaining)}\n")

    for i, co in enumerate(remaining):
        name    = co["name"]
        website = co.get("website", "").strip()
        print(f"  [{i+1}/{len(remaining)}] {name}")

        # Fetch texts
        print(f"    Fetching website text ...")
        website_text = fetch_website_text(website)

        print(f"    Fetching Wikipedia text ...")
        wikipedia_text = fetch_wikipedia_text(name)

        # Classify industry using whichever text we have
        combined_text = " ".join(filter(None, [website_text, wikipedia_text]))
        industry, hits = classify_industry(combined_text)

        print(f"    Industry: {industry}  (hits: {hits})")
        if not website_text:
            print(f"    ⚠  No website text retrieved")
        if not wikipedia_text:
            print(f"    ⚠  No Wikipedia article found")

        row = {
            "name":                       name,
            "website":                    website,
            "website_text":               website_text,
            "wikipedia_text":             wikipedia_text,
            "industry":                   industry,
            "corresponding_keyword_hits": hits,
        }
        results.append(row)
        done[name] = row

        save_progress(done)
        time.sleep(0.8)

    save_output_csv(results, OUTPUT_CSV)
    clear_progress()
    print(f"\nDone! Open {OUTPUT_CSV} to review the results.")


if __name__ == "__main__":
    main()
