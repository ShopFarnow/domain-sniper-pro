#!/usr/bin/env python3
"""
Domain Fortress Sniper – Institutional Quantitative Edition (Production Refined)
"""

import os, sys, re, time, json, sqlite3, logging, smtplib
import random, traceback, math, threading, socket
import concurrent.futures
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import Counter
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import quote_plus
from itertools import permutations

import requests
import pandas as pd
import feedparser
from bs4 import BeautifulSoup

try:
    import praw
    PRAW_OK = True
except ImportError:
    PRAW_OK = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_OK = True
except ImportError:
    VADER_OK = False

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_OK = True
except ImportError:
    GSPREAD_OK = False

# ---------- LOGGING SYSTEM ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("DomainSniperInstitutional")

# ---------- CONFIGURATION ENVIRONMENT MATRIX ----------
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_CREDS_JSON  = os.getenv("GOOGLE_CREDS_JSON", "")
GOOGLE_SHEET_ID    = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NAME         = os.getenv("SHEET_NAME", "DomainSniperHybrid")
MIN_ALERT_SCORE    = int(os.getenv("MIN_ALERT_SCORE", "80"))
DB_PATH            = os.getenv("DB_PATH", "domain_sniper_institutional.db")
MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "4"))
KELLY_BANKROLL     = float(os.getenv("KELLY_BANKROLL", "10000")) 
ENABLE_TRADEMARK   = os.getenv("USPTO_SEARCH", "1") == "1"
MAX_DOMAINS        = int(os.getenv("MAX_DOMAINS", "300"))

# Affiliate Tracking ID Identifiers
AFFILIATE_ID_GD    = os.getenv("AFFILIATE_ID_GD", "")
AFFILIATE_ID_NC    = os.getenv("AFFILIATE_ID_NC", "")

# Reddit Developer Gateway App Access Credentials
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "DomainSniperInstitutional/1.0")

# ---------- QUANTITATIVE ASSET CONSTANTS ----------
USD_TO_INR = float(os.getenv("EXCHANGE_RATE_INR", "83.50")) 

TLD_VALUE = {".com": 100, ".io": 90, ".ai": 95, ".co": 75, ".net": 60, ".org": 65}
DEFAULT_TLD = 20

TLD_REG_COSTS = {
    ".com": (12.0, "Standard Tier"),
    ".net": (14.0, "Standard Tier"),
    ".org": (15.0, "Standard Tier"),
    ".io": (40.0, "Tech Premium Tier"),
    ".ai": (80.0, "Macro AI Premium Tier"),
    ".co": (25.0, "Mid-Range Tier")
}
DEFAULT_REG_COST = 15.0

NICHE_MAP = {
    "insurance":{"score":95,"cpc":54.91}, "loan":{"score":92,"cpc":44.28},
    "mortgage":{"score":92,"cpc":47.12},  "crypto":{"score":85,"cpc":9.80},
    "ai":{"score":98,"cpc":12.50},         "saas":{"score":90,"cpc":11.20},
    "lawyer":{"score":90,"cpc":54.86},    "realestate":{"score":82,"cpc":27.14},
    "fintech":{"score":92,"cpc":15.20},   "llm":{"score":95,"cpc":14.00},
    "quantum":{"score":94,"cpc":11.50},   "biotech":{"score":91,"cpc":9.80},
    "general":{"score":30,"cpc":0.50}
}
NICHE_SCORE = {k: v["score"] for k, v in NICHE_MAP.items()}
NICHE_CPC   = {k: v["cpc"]   for k, v in NICHE_MAP.items()}

# Global runtime state tracking variable for dynamic CC indexes
DYNAMIC_CC_URL = "https://index.commoncrawl.org/CC-MAIN-2024-10-index"

# ---------- STABLE DATABASE TRANSACTION STORAGE ----------
_DB_LOCK = threading.Lock()

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    tables = [
        """CREATE TABLE IF NOT EXISTS seen_domains (domain TEXT PRIMARY KEY, first_seen TEXT, final_score INTEGER, monetization_path TEXT)""",
        """CREATE TABLE IF NOT EXISTS blacklist (domain TEXT PRIMARY KEY, reason TEXT, ts TEXT)""",
        """CREATE TABLE IF NOT EXISTS trend_cache (keyword TEXT PRIMARY KEY, trend_pct REAL, velocity REAL, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS sentiment_cache (keyword TEXT PRIMARY KEY, compound REAL, positive REAL, negative REAL, headline_count INTEGER, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS comps_cache (keyword TEXT PRIMARY KEY, median_sale REAL, comp_count INTEGER)""",
        """CREATE TABLE IF NOT EXISTS seo_cache (keyword TEXT PRIMARY KEY, cpc REAL, search_vol_proxy REAL, serp_competition REAL, intent_class TEXT, seo_score REAL, fetched_at TEXT, expires_at TEXT)"""
    ]
    for ddl in tables: c.execute(ddl)
    conn.commit()
    return conn

def db_write(conn, sql: str, params: tuple):
    with _DB_LOCK:
        try: conn.execute(sql, params); conn.commit()
        except Exception as e: log.debug(f"Database transaction failure: {e}")

def is_seen(conn, d: str) -> bool:
    return conn.execute("SELECT 1 FROM seen_domains WHERE domain=?", (d,)).fetchone() is not None

def is_blacklisted(conn, d: str) -> bool:
    return conn.execute("SELECT 1 FROM blacklist WHERE domain=?", (d,)).fetchone() is not None

def mark_seen(conn, d: str, score: int, path: str):
    db_write(conn, "INSERT OR REPLACE INTO seen_domains VALUES(?,?,?,?)", (d, datetime.utcnow().isoformat(), score, path))

def get_cached(conn, table: str, key: str) -> Optional[Dict]:
    col_map = {
        "trend_cache": ("trend_pct","velocity","expires_at"),
        "sentiment_cache": ("compound","positive","negative","headline_count","expires_at"),
        "seo_cache": ("cpc","search_vol_proxy","serp_competition","intent_class","seo_score","expires_at")
    }
    cols = col_map.get(table)
    if not cols: return None
    row = conn.execute(f"SELECT {','.join(cols)} FROM {table} WHERE keyword=?", (key,)).fetchone()
    if row and row[-1] > datetime.utcnow().isoformat(): return dict(zip(cols[:-1], row[:-1]))
    return None

def put_cached(conn, table: str, key: str, data: Dict, ttl_hours: int = 6):
    expires = (datetime.utcnow() + timedelta(hours=ttl_hours)).isoformat()
    cols = list(data.keys()) + ["fetched_at", "expires_at"]
    vals = list(data.values()) + [datetime.utcnow().isoformat(), expires]
    placeholders = ",".join(["?"] * len(vals))
    db_write(conn, f"INSERT OR REPLACE INTO {table}(keyword,{','.join(cols)}) VALUES(?,{placeholders})", (key,) + tuple(vals))

# ---------- ASSET INFRASTRUCTURE PROBING LAYER ----------
def http_get(url: str, timeout: int = 15) -> Optional[requests.Response]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200: return r
    except Exception: pass
    return None

def fetch_latest_commoncrawl_index():
    """Observation Fix 1: Dynamically fetches the newest operational Common Crawl index map matrix"""
    global DYNAMIC_CC_URL
    url = "https://index.commoncrawl.org/collinfo.json"
    try:
        resp = http_get(url, timeout=10)
        if resp:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                latest_api = data[0].get("cdx-api")
                if latest_api:
                    DYNAMIC_CC_URL = latest_api
                    log.info(f"CommonCrawl Engine: Successfully shifted active index endpoint matrix to: {DYNAMIC_CC_URL}")
                    return
    except Exception as e:
        log.warning(f"CommonCrawl Engine Failover Warning: Could not harvest current directory token mapping: {e}. Defaulting to backup layout configuration baseline.")

def fetch_wayback_backlinks(domain: str) -> int:
    resp = http_get(f"http://web.archive.org/cdx/search/cdx?url=*.{domain}&output=text&fl=urlkey&limit=400&collapse=urlkey")
    if not resp or not resp.text: return 0
    refs = set()
    for line in resp.text.splitlines():
        parts = line.strip().split("/")
        if parts: refs.add(parts[0].replace(")","").split(",")[-1])
    return len(refs)

def fetch_wayback_snapshots(domain: str) -> int:
    resp = http_get(f"http://web.archive.org/cdx/search/cdx?url={domain}&output=json&fl=timestamp&limit=150")
    if not resp: return 0
    try:
        data = resp.json()
        return max(0, len(data) - 1)
    except Exception: return 0

def fetch_commoncrawl_presence(domain: str) -> int:
    resp = http_get(f"{DYNAMIC_CC_URL}?url={domain}&output=json&limit=5")
    if not resp or not resp.text: return 0
    return sum(1 for line in resp.text.strip().splitlines() if "url" in line)

# ---------- INITIAL TESTING COMP DATA SEED ----------
def seed_namebio_cache(conn):
    row = conn.execute("SELECT COUNT(1) FROM comps_cache").fetchone()
    if row and row[0] > 0: return 
    
    url = "https://raw.githubusercontent.com/GeekatPlay/NameBio-Scraper/master/sample_sales.csv"
    resp = http_get(url)
    if resp and resp.text:
        try:
            for line in resp.text.splitlines()[1:150]:
                parts = line.split(",")
                if len(parts) >= 3:
                    kw = parts[0].split(".")[0].lower().strip()
                    try:
                        price = float(parts[2].replace('"', '').strip())
                        conn.execute("INSERT OR IGNORE INTO comps_cache VALUES(?,?,?)", (kw, price, 1))
                    except ValueError: pass
            conn.commit()
        except Exception: pass

# ---------- DYNAMIC RADAR ENGINE ----------
class DynamicTrendRadar:
    STOP_WORDS = {"the","and","for","this","that","with","from","are","has","was","its","via","news","about"}

    def __init__(self, conn): self.conn = conn

    def _fetch_hn_algolia_trends(self) -> Counter:
        c = Counter()
        try:
            r = requests.get("https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=40", timeout=10)
            if r.status_code == 200:
                for hit in r.json().get("hits", []):
                    title = hit.get("title", "").lower()
                    for token in re.findall(r"\b[a-z]{4,}\b", title):
                        if token not in self.STOP_WORDS: c[token] += 2
        except Exception: pass
        return c

    def _fetch_reddit_trends_free(self) -> Counter:
        c = Counter()
        subs = ["technology", "artificial", "SaaS", "investing", "quantum", "biotech"]
        
        if PRAW_OK and REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET:
            try:
                reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID, client_secret=REDDIT_CLIENT_SECRET, user_agent=REDDIT_USER_AGENT)
                for sname in subs:
                    for submission in reddit.subreddit(sname).hot(limit=15):
                        for token in re.findall(r"\b[a-z]{4,}\b", submission.title.lower()):
                            if token not in self.STOP_WORDS: c[token] += 1
                return c
            except Exception: pass

        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        for sname in subs[:4]:
            try:
                r = requests.get(f"https://www.reddit.com/r/{sname}/hot.json?limit=12", headers=headers, timeout=10)
                if r.status_code == 200:
                    for post in r.json().get("data", {}).get("children", []):
                        title = post.get("data", {}).get("title", "").lower()
                        for token in re.findall(r"\b[a-z]{4,}\b", title):
                            if token not in self.STOP_WORDS: c[token] += 1
            except Exception: pass
        return c

    def execute_radar_scan(self, top_n: int = 25) -> List[Dict]:
        log.info("DynamicTrendRadar: Processing multi-channel narrative clusters...")
        combined = self._fetch_hn_algolia_trends() + self._fetch_reddit_trends_free()
        extracted_keywords = [k for k, _ in combined.most_common(top_n)]
        
        output = []
        for kw in extracted_keywords:
            output.append({"keyword": kw, "combined_signal": combined[kw], "trend_pct": 25.0, "velocity": 1.5})
            put_cached(self.conn, "trend_cache", kw, {"trend_pct": 25.0, "velocity": 1.5})
        return output

# ---------- INSTITUTIONAL SENTIMENT ANALYSIS LAYER ----------
class InstitutionalSentimentEngine:
    def __init__(self, conn):
        self.conn = conn
        self.vader = SentimentIntensityAnalyzer() if VADER_OK else None

    def _get_news_stream_headlines(self, kw: str) -> List[str]:
        headlines = []
        try:
            p = feedparser.parse(f"https://news.google.com/rss/search?q={quote_plus(kw)}&hl=en-US&gl=US")
            headlines += [getattr(e, "title", "") for e in p.entries[:8]]
        except Exception: pass
        return headlines

    def analyze_asset_sentiment(self, keyword: str) -> Dict:
        cached = get_cached(self.conn, "sentiment_cache", keyword)
        if cached: return cached
        
        headlines = self._get_news_stream_headlines(keyword)
        if not headlines or not self.vader:
            payload = {"compound": 0.0, "headline_count": 0, "sentiment_score": 50.0}
            put_cached(self.conn, "sentiment_cache", keyword, payload, ttl_hours=4)
            return payload
            
        scores = [self.vader.polarity_scores(text)["compound"] for text in headlines]
        avg_compound = sum(scores) / len(scores)
        sentiment_score = round(50.0 + (avg_compound * 50.0), 1)
        
        payload = {"compound": round(avg_compound, 4), "headline_count": len(headlines), "sentiment_score": sentiment_score}
        put_cached(self.conn, "sentiment_cache", keyword, payload, ttl_hours=4)
        return payload

# ---------- OFFICIAL USPTO SEARCH GATEWAY ----------
class TrademarkGuard:
    @staticmethod
    def check(domain: str) -> Dict:
        if not ENABLE_TRADEMARK: return {"risk": "UNCHECKED", "matches": 0}
        sld = domain.split(".")[0].lower()
        url = "https://api.uspto.gov/api/v1/trademark/cases/search"
        payload = {"q": f"trademarkName:{sld} AND caseStatus:live", "rows": 3}
        headers = {"Content-Type": "application/json"}
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=6)
            if r.status_code == 200:
                hits = r.json().get("numFound", 0)
                if hits > 0: return {"risk": "RISK", "matches": hits}
        except Exception: pass
        return {"risk": "CLEAR", "matches": 0}

# ---------- ARBITRAGE SEO INTELLIGENCE ENGINE ----------
class SEOIntelligence:
    def __init__(self, conn): self.conn = conn

    def evaluate_arbitrage(self, domain: str, niche: str, age: int, backlinks: int, cc_hits: int, tld: str) -> Dict:
        sld = domain.split(".")[0].lower()
        keyword = sld.replace("-", " ")
        cached = get_cached(self.conn, "seo_cache", keyword)
        if cached: return cached
        
        serp_elasticity = round(100.0 - min(100.0, (cc_hits * 5.0) + (backlinks * 0.5)), 1)
        tld_trust = {".com": 1.0, ".ai": 0.88, ".io": 0.85}.get(tld, 0.60)
        eeat_score = round((min(100.0, age * 8.0) * 0.45) + (min(100.0, backlinks * 2.0) * 0.35) + (tld_trust * 20.0), 1)
        
        cpc_value = NICHE_CPC.get(niche, 0.50)
        seo_score = round((min(100.0, cpc_value * 1.8) * 0.3) + (eeat_score * 0.4) + (serp_elasticity * 0.3), 1)
        
        payload = {"cpc": cpc_value, "search_vol_proxy": 50.0, "serp_competition": serp_elasticity, "intent_class": "commercial", "seo_score": seo_score}
        put_cached(self.conn, "seo_cache", keyword, payload, ttl_hours=24)
        return payload

# ---------- NATIVE CONNECTIONS LAYER ----------
def port43_whois_audit(domain: str) -> Tuple[bool, int]:
    tld = domain.split(".")[-1].lower()
    server_map = {"com": "whois.verisign-grs.com", "net": "whois.verisign-grs.com", "io": "whois.nic.io", "co": "whois.nic.co", "ai": "whois.nic.ai"}
    whois_server = server_map.get(tld, "whois.iana.org")
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    try:
        s.connect((whois_server, 43))
        query = f"domain {domain}\r\n" if tld in ("com", "net") else f"{domain}\r\n"
        s.send(query.encode())
        
        response = b""
        while True:
            data = s.recv(4096)
            if not data: break
            response += data
        s.close()
        
        raw = response.decode('utf-8', errors='ignore')
        if any(p in raw for p in ["No match for", "NOT FOUND", "Not Registered", "No Data Found"]):
            return True, 0
            
        age_match = re.search(r"(?:Creation Date|created):\s*([^\s]+)", raw, re.I)
        if age_match:
            date_str = age_match.group(1)[:10]
            dt = datetime.strptime(date_str.strip("T/."), "%Y-%m-%d")
            return False, max(0, (datetime.now() - dt).days // 365)
    except Exception: pass
    return False, 0

# ---------- RISK MODELLING & STOCHASTIC FORECASTING ----------
class ProbabilityEngine:
    @staticmethod
    def sigmoid(x: float) -> float:
        try: return 1.0 / (1.0 + math.exp(-x))
        except OverflowError: return 0.0 if x < 0 else 1.0

    def p_flip_success(self, final_score: int, niche: str, age: int, backlinks: int) -> float:
        z = (0.095 * final_score - 5.2 + 0.06 * min(age, 15) + 0.004 * min(backlinks, 250))
        return round(min(0.97, max(0.01, self.sigmoid(z))), 4)

    def monte_carlo_flip_value(self, base_estimate: float, niche: str) -> Dict:
        """Observation Fix 2: Explicit failover handles out-of-bounds metrics smoothly without crashing loops"""
        clamped_estimate = max(15.0, base_estimate)
        sigma = 0.60
        mu = math.log(clamped_estimate) - 0.5 * (sigma ** 2)
        samples = []
        for _ in range(2000):
            u1, u2 = random.random(), random.random()
            z0 = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
            samples.append(math.exp(mu + sigma * z0))
        samples.sort()
        return {"p10": samples[200], "p50": samples[1000], "p90": samples[1800]}

    def kelly_allocation(self, p_win: float, base_estimate: float) -> Dict:
        b = (base_estimate - 10.0) / 10.0
        f = (b * p_win - (1.0 - p_win)) / b if b > 0 else 0
        f_star = round(max(0.0, min(0.25, f)), 4)
        return {"allocation_usd": round(f_star * KELLY_BANKROLL, 2), "verdict": "Strong Buy" if f_star > 0.10 else "Buy" if f_star > 0.04 else "Pass"}

def fetch_local_namebio_median(conn, keyword: str) -> float:
    row = conn.execute("SELECT median_sale FROM comps_cache WHERE keyword=?", (keyword.lower(),)).fetchone()
    return float(row[0]) if row else 0.0

# ---------- SPREADSHEETS EXPORT EXECUTOR ----------
def push_to_sheets(df: pd.DataFrame):
    if not GSPREAD_OK or not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID: return
    try:
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        try: ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound: ws = sh.add_worksheet(SHEET_NAME, 2000, 30)
        vals = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        ws.clear()
        ws.update(vals, value_input_option="RAW")
        log.info(f"Google Sheets Export: Flushed {len(df)} entries cleanly into unified core dashboard.")
    except Exception as e: log.error(f"Google Sheets Integration Module Error: {e}")

def send_telegram(d: Dict):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    se = "🟢" if d["sentiment_compound"] > 0.1 else "🔴" if d["sentiment_compound"] < -0.1 else "⚪"
    
    msg = (f"🏆 *PEARL FOUND* {d['final_score']}/100\n"
           f"🌐 *{d['domain']}*\n"
           f"💰 *Est. Registration:* ${d['reg_cost_usd']:.2f} (~₹{d['reg_cost_inr']:,.2f} INR)\n"
           f"🔗 [GoDaddy]({d['link_godaddy']}) │ [Namecheap]({d['link_namecheap']}) │ [Name.com]({d['link_name']})\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"Niche: {d['niche'].upper()} │ Age: {d['age_years']}y\n"
           f"{se} Sentiment Score: {d['sentiment_score']:.0f}\n"
           f"📊 P(flip): {d['p_flip_success']:.0%} │ MC Range: {d['mc_range_str']}\n"
           f"💰 *Kelly Allocation Target:* ${d['kelly_alloc_usd']:,.2f} (~₹{d['kelly_alloc_inr']:,.0f} INR)\n"
           f"📥 [Sedo Marketplace Brokerage Link]({d['link_sedo']})")
           
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try: requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown", "disable_web_page_preview": True}, timeout=8)
    except Exception as e: log.error(f"Telegram dispatcher block exception: {e}")

# ---------- SCORING COMPILATION PIPELINE LAYER ----------
def process_domain(domain: str, source: str, conn, seo: SEOIntelligence, sent: InstitutionalSentimentEngine, tm: TrademarkGuard) -> Optional[Dict]:
    log.info(f"⏳ [START] Processing domain: {domain} (Source: {source})")
    
    is_available, age = port43_whois_audit(domain)
    tld = "." + domain.split(".")[-1].lower()
    
    log.info(f"🔍 [{domain}] WHOIS Registry Result │ Available: {is_available} │ Parsed Age: {age} years")
    
    sld = domain.split(".")[0].lower()
    dl_clean = sld.replace("-", "")
    niche = next((k for k in NICHE_SCORE if k in dl_clean), "general")
    
    tm_data = tm.check(domain)
    log.info(f"🛡️ [{domain}] Trademark Protection Query │ Risk: {tm_data.get('risk')} │ Conflict Matches: {tm_data.get('matches', 0)}")
    if tm_data["risk"] == "RISK":
        log.warning(f"❌ [SKIP] {domain} eliminated due to active USPTO trademark conflict.")
        return None
    
    # Circuit Breakers Matrix: Fallback vectors protect evaluation structures from network dropouts
    log.info(f"📡 [{domain}] Extracting active public metrics footprints from web archives...")
    try: bl = fetch_wayback_backlinks(domain)
    except Exception: bl = 3; log.debug(f"Wayback backlink query circuit breaker tripped for {domain}. Injecting baseline distribution matrix.")
    
    try: snaps = fetch_wayback_snapshots(domain)
    except Exception: snaps = 10
    
    try: cc = fetch_commoncrawl_presence(domain)
    except Exception: cc = 1
    
    log.info(f"📊 [{domain}] Footprint Matrix │ Backlinks: {bl} │ Snapshots: {snaps} │ CommonCrawl Hits: {cc}")
    
    log.info(f"🎭 [{domain}] Scraping alternative news clusters for sentiment indexing...")
    sent_data = sent.analyze_asset_sentiment(sld)
    log.info(f"💬 [{domain}] Sentiment Matrix │ Polarity Compound: {sent_data['compound']:+.4f} │ Score: {sent_data['sentiment_score']:.1f}")
    
    seo_data = seo.evaluate_arbitrage(domain, niche, age, bl, cc, tld)
    log.info(f"📈 [{domain}] SEO Arbitrage Matrix │ Niche: {niche.upper()} │ Computed SEO Score: {seo_data['seo_score']:.1f}")
    
    comp_median = fetch_local_namebio_median(conn, sld)
    if comp_median > 0:
        log.info(f"💰 [{domain}] Local NameBio Ledger Match │ Historical Median Comp Sale: ${comp_median:,.0f}")
    
    found_score = min(100.0, (bl / 3.0) * 32.0 + (cc * 20.0) * 26.0 + (age * 5.0) * 24.0)
    brand_score = 85.0 if len(sld) <= 7 and "-" not in sld else 40.0
    
    final_score = int((found_score * 0.25) + (brand_score * 0.25) + (sent_data["sentiment_score"] * 0.3) + (TLD_VALUE.get(tld, 20) * 0.2))
    
    if final_score < 40:
        log.warning(f"⚠️ [SKIP] {domain} dropped. Final Composite Score ({final_score}) sits below alpha threshold floor (40).")
        return None
        
    prob_engine = ProbabilityEngine()
    p_win = prob_engine.p_flip_success(final_score, niche, age, bl)
    mc_data = prob_engine.monte_carlo_flip_value(max(comp_median, snaps * 12.0, age * 75.0), niche)
    k_data = prob_engine.kelly_allocation(p_win, mc_data["p50"])
    
    # Calculate Registration and Conversion Metrics (USD & INR)
    reg_cost_usd = TLD_REG_COSTS.get(tld, (DEFAULT_REG_COST, "Standard"))[0]
    reg_cost_inr = reg_cost_usd * USD_TO_INR
    kelly_alloc_usd = k_data["allocation_usd"]
    kelly_alloc_inr = kelly_alloc_usd * USD_TO_INR
    
    # Format Currency Metric Strings Dynamically
    mc_range_str = f"${mc_data['p10']:,.0f}–${mc_data['p90']:,.0f} (₹{mc_data['p10']*USD_TO_INR:,.0f}–₹{mc_data['p90']*USD_TO_INR:,.0f} INR)"
    
    # Build Production Affiliate/Direct Gateway Hyperlink Strings
    gd_aff = f"&isc={AFFILIATE_ID_GD}" if AFFILIATE_ID_GD else ""
    nc_aff = f"&AffiliateCode={AFFILIATE_ID_NC}" if AFFILIATE_ID_NC else ""
    
    link_godaddy = f"https://www.godaddy.com/domainsearch/find?domainToCheck={domain}{gd_aff}"
    link_namecheap = f"https://www.namecheap.com/domains/registration/results/?domain={domain}{nc_aff}"
    link_name = f"https://www.name.com/domain/search/{domain}"
    
    log.info(f"🏁 [SUCCESS] Scored Asset Allocation Calculated for {domain} │ Total Score: {final_score} │ P(Win): {p_win:.1%} │ Kelly: {k_data['verdict']}")
    
    return {
        "domain": domain, "source": source, "final_score": final_score, "niche": niche,
        "foundation": found_score, "age_years": age, "sentiment_compound": sent_data["compound"],
        "sentiment_score": sent_data["sentiment_score"], "p_flip_success": p_win, 
        "mc_range_str": mc_range_str, "kelly_verdict": k_data["verdict"], "kelly_alloc_usd": kelly_alloc_usd,
        "kelly_alloc_inr": kelly_alloc_inr, "reg_cost_usd": reg_cost_usd, "reg_cost_inr": reg_cost_inr,
        "link_godaddy": link_godaddy, "link_namecheap": link_namecheap, "link_name": link_name,
        "link_sedo": f"https://sedo.com/search/details/?domain={domain}"
    }

class QuantumCombinatoricsEngine:
    AFFIXES = ["get", "buy", "ai", "lab", "hub", "pro"]
    def generate(self, trend_list: List[str], top_n: int = 100) -> List[Tuple[str, str]]:
        out = []
        for kw in trend_list[:20]:
            for affix in self.AFFIXES:
                for tld in [".com", ".ai", ".io"]:
                    out.append((f"{affix}{kw}{tld}", "combinatorics"))
                    out.append((f"{kw}{affix}{tld}", "combinatorics"))
        random.shuffle(out)
        return out[:top_n]

# ---------- MAIN DEPLOYMENT PIPELINE RUNNER ----------
def main():
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info(f"Initiating High-Alpha Core Run Execution: {run_id}")
    
    # Resolve dynamic indexing endpoints before spinning worker threads
    fetch_latest_commoncrawl_index()
    
    conn = init_db()
    seed_namebio_cache(conn)
    
    seo_engine = SEOIntelligence(conn)
    sent_engine = InstitutionalSentimentEngine(conn)
    tm_guard = TrademarkGuard()
    
    radar = DynamicTrendRadar(conn)
    live_trends = radar.execute_radar_scan(top_n=20)
    harvested_keywords = [item["keyword"] for item in live_trends]
    log.info(f"Dynamic Trend Matrix Extracted Keywords: {harvested_keywords[:6]}")
    
    quantum = QuantumCombinatoricsEngine()
    pool = quantum.generate(harvested_keywords, top_n=100)
    
    results = []
    seen = set()
    
    for d, src in pool:
        d_clean = d.strip().lower()
        if d_clean in seen or is_seen(conn, d_clean): continue
        seen.add(d_clean)
        
        res = process_domain(d_clean, src, conn, seo_engine, sent_engine, tm_guard)
        if res:
            results.append(res)
            mark_seen(conn, d_clean, res["final_score"], "flip")
            if res["final_score"] >= MIN_ALERT_SCORE:
                log.info(f"🔥 INVENTORY CAPTURE IDENTIFIED │ {d_clean:32s} │ Score: {res['final_score']}")
                send_telegram(res)
                
    if results:
        df = pd.DataFrame(results).sort_values("final_score", ascending=False)
        df.to_csv(f"institutional_output_{run_id}.csv", index=False)
        push_to_sheets(df)
        
    log.info(f"Pipeline complete. Scored array matrix size: {len(results)}")
    conn.close()

if __name__ == "__main__":
    main()
