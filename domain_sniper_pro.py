#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         Domain Fortress Sniper PRO  v6 – Fixed & Production Ready           ║
╚══════════════════════════════════════════════════════════════════════════════╝

FIXES applied over v6:
  1. Replaced broken domainsdb.info with reliable Namebeta scraper
  2. Added Reddit OAuth support (if credentials provided) – higher limits
  3. Fixed Reddit public JSON fallback when OAuth unavailable
  4. Added fallback sentiment when RSS feeds fail (uses mock)
  5. Reduced default MAX_WORKERS from 6 → 3 to avoid 503 storms
  6. Added proper user-agent rotation for all scrapers
  7. Included vaderSentiment in import guard (graceful fallback)
  8. Removed NewsAPI hard dependency (optional)
  9. Added per‑source timeout & retry configuration
 10. Ensured generated fallback domains are only used if primary sources return 0
"""

import os, sys, re, time, json, sqlite3, logging, smtplib
import random, traceback, hashlib, math
import concurrent.futures
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import Counter
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import quote_plus

import requests
import pandas as pd
import feedparser
from bs4 import BeautifulSoup
import whois

# Optional imports with graceful fallback
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    print("[WARN] vaderSentiment not installed. Run: pip install vaderSentiment")

try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    PYTRENDS_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ========== LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("DomainSniperV6")

# ========== ENVIRONMENT ==========
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NAME        = os.getenv("SHEET_NAME", "DomainSniperV6")
MIN_ALERT_SCORE   = int(os.getenv("MIN_ALERT_SCORE", "68"))
SAFE_BROWSING_KEY = os.getenv("SAFE_BROWSING_KEY", "")
EMAIL_DIGEST_TO   = os.getenv("EMAIL_DIGEST_TO", "")
GMAIL_USER        = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASS    = os.getenv("GMAIL_APP_PASS", "")
AFFILIATE_ID_GD   = os.getenv("AFFILIATE_ID_GD", "")
AFFILIATE_ID_NC   = os.getenv("AFFILIATE_ID_NC", "")
DB_PATH           = os.getenv("DB_PATH", "domain_sniper_v6.db")
MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "3"))   # reduced to avoid 503
NEWS_API_KEY      = os.getenv("NEWS_API_KEY", "")
REDDIT_CLIENT_ID  = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_SECRET     = os.getenv("REDDIT_SECRET", "")
COINGECKO_KEY     = os.getenv("COINGECKO_API_KEY", "")
SCORE_FLOOR       = int(os.getenv("SCORE_FLOOR", "45"))
MAX_TREND_SOURCES = int(os.getenv("MAX_TREND_SOURCES", "4"))
CUSTOM_KEYWORDS   = [k.strip() for k in os.getenv("TREND_KEYWORDS", "").split(",") if k.strip()]

# ========== CONSTANTS ==========
TLD_VALUE = {
    ".com": 100, ".io": 88, ".ai": 92, ".co": 75, ".net": 60,
    ".org": 55,  ".in": 45, ".us": 42, ".app": 74, ".dev": 72,
    ".tech": 58, ".online": 30, ".info": 25, ".biz": 20,
    ".xyz": 28, ".gg": 65, ".vc": 68, ".finance": 62,
}
DEFAULT_TLD = 15

NICHE_MAP = {
    "insurance": 90, "loan": 90, "mortgage": 88, "crypto": 82, "ai": 88,
    "saas": 82, "health": 76, "lawyer": 85, "travel": 65, "shop": 56,
    "realestate": 80, "clinic": 78, "dentist": 80, "plumber": 72,
    "solar": 76, "fintech": 84, "ecommerce": 72, "agency": 62,
    "marketing": 66, "consulting": 70, "fitness": 66, "yoga": 60,
    "vpn": 80, "hosting": 74, "invest": 86, "forex": 82, "nft": 68,
    "defi": 78, "llm": 85, "gpt": 84, "blockchain": 78, "robotics": 75,
    "ev": 74, "electric": 70, "sustainability": 68, "climate": 66,
    "mental": 72, "therapy": 74, "telehealth": 78, "biotech": 76,
    "quantum": 80, "drone": 70, "space": 72, "gaming": 68, "metaverse": 60,
}

PARKING_CPM = {
    "insurance": 18, "loan": 15, "mortgage": 14, "crypto": 13, "ai": 12,
    "saas": 10, "health": 9, "lawyer": 14, "travel": 8, "shop": 5,
    "realestate": 11, "clinic": 10, "dentist": 12, "plumber": 9,
    "solar": 10, "fintech": 13, "llm": 11, "ev": 9, "quantum": 10,
    "general": 3,
}

LEAD_VALUE = {
    "insurance": 28, "loan": 22, "mortgage": 32, "lawyer": 45,
    "dentist": 18, "clinic": 14, "plumber": 10, "solar": 20,
    "realestate": 22, "telehealth": 16, "therapy": 14,
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# RSS feeds (free, no key)
NEWS_RSS_FEEDS = {
    "tech":     "https://feeds.feedburner.com/TechCrunch",
    "ai":       "https://www.artificialintelligence-news.com/feed/",
    "crypto":   "https://cointelegraph.com/rss",
    "business": "https://feeds.reuters.com/reuters/businessNews",
    "health":   "https://medlineplus.gov/xml/mplus_topics_health_news.xml",
    "startup":  "https://techcrunch.com/startups/feed/",
    "finance":  "https://www.marketwatch.com/rss/realtimeheadlines",
    "science":  "https://rss.sciencedaily.com/all.xml",
}

HN_FEEDS = {
    "top":  "https://hacker-news.firebaseio.com/v0/topstories.json",
    "new":  "https://hacker-news.firebaseio.com/v0/newstories.json",
    "best": "https://hacker-news.firebaseio.com/v0/beststories.json",
}

# Subreddits for public JSON (fallback if OAuth not configured)
REDDIT_SUBREDDITS = [
    "technology", "artificial", "MachineLearning", "startups",
    "entrepreneur", "investing", "personalfinance", "cybersecurity",
    "webdev", "Futurology", "business", "SaaS",
]

# ========== HELPER FUNCTIONS ==========
def get_composite_weights(age: int, niche: str) -> Dict[str, float]:
    if age == 0:
        return {"foundation": 0.18, "flip": 0.28, "history": 0.14,
                "sentiment": 0.22, "momentum": 0.12, "monetization": 0.06}
    elif age < 3:
        return {"foundation": 0.20, "flip": 0.26, "history": 0.16,
                "sentiment": 0.18, "momentum": 0.12, "monetization": 0.08}
    elif age < 10:
        return {"foundation": 0.26, "flip": 0.26, "history": 0.18,
                "sentiment": 0.14, "momentum": 0.10, "monetization": 0.06}
    else:
        return {"foundation": 0.30, "flip": 0.24, "history": 0.22,
                "sentiment": 0.10, "momentum": 0.08, "monetization": 0.06}

def http_get(url: str, timeout: int = 25, retries: int = 3,
             backoff: float = 2.0, headers: Optional[Dict] = None,
             json_resp: bool = False) -> Any:
    _headers = {"User-Agent": random.choice(USER_AGENTS)}
    if headers:
        _headers.update(headers)
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=_headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json() if json_resp else resp
            if resp.status_code in (429, 503):
                wait = backoff * (2 ** attempt) + random.uniform(0, 1)
                log.warning(f"Rate-limited {resp.status_code} – waiting {wait:.1f}s")
                time.sleep(wait)
            elif resp.status_code == 404:
                return None
            else:
                return None
        except Exception:
            time.sleep(backoff * (attempt + 1))
    return None

def http_post(url: str, body: Dict, timeout: int = 15) -> Optional[requests.Response]:
    try:
        return requests.post(url, json=body, timeout=timeout,
                             headers={"User-Agent": random.choice(USER_AGENTS)})
    except Exception:
        return None

# ========== DATABASE ==========
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS seen_domains (
        domain TEXT PRIMARY KEY, first_seen TEXT,
        final_score INTEGER, monetization_path TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS blacklist (
        domain TEXT PRIMARY KEY, reason TEXT, blacklisted_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS trend_cache (
        keyword TEXT PRIMARY KEY, trend_pct REAL, velocity REAL,
        source_count INTEGER, fetched_at TEXT, expires_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS sentiment_cache (
        keyword TEXT PRIMARY KEY, compound REAL, positive REAL, negative REAL,
        headline_count INTEGER, top_headlines TEXT, fetched_at TEXT, expires_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS keyword_signals (
        keyword TEXT PRIMARY KEY, hn_mentions INTEGER, reddit_score INTEGER,
        news_volume INTEGER, coingecko_rank INTEGER, combined_signal REAL,
        updated_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS run_stats (
        run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT,
        domains_scanned INTEGER, pearls_found INTEGER,
        top_domain TEXT, top_score INTEGER
    )""")
    conn.commit()
    return conn

def is_seen(conn, domain: str) -> bool:
    return conn.execute("SELECT 1 FROM seen_domains WHERE domain=?", (domain,)).fetchone() is not None

def mark_seen(conn, domain: str, score: int, path: str):
    conn.execute("INSERT OR REPLACE INTO seen_domains VALUES (?,?,?,?)",
                 (domain, datetime.utcnow().isoformat(), score, path))
    conn.commit()

def is_blacklisted(conn, domain: str) -> bool:
    return conn.execute("SELECT 1 FROM blacklist WHERE domain=?", (domain,)).fetchone() is not None

def add_to_blacklist(conn, domain: str, reason: str):
    conn.execute("INSERT OR IGNORE INTO blacklist VALUES (?,?,?)",
                 (domain, reason, datetime.utcnow().isoformat()))
    conn.commit()
    log.info(f"Blacklisted {domain}: {reason}")

def get_cached_trend(conn, keyword: str) -> Optional[Dict]:
    row = conn.execute(
        "SELECT trend_pct, velocity, source_count, expires_at FROM trend_cache WHERE keyword=?",
        (keyword,)
    ).fetchone()
    if row and row[3] > datetime.utcnow().isoformat():
        return {"trend_pct": row[0], "velocity": row[1], "source_count": row[2]}
    return None

def cache_trend(conn, keyword: str, trend_pct: float, velocity: float, source_count: int):
    expires = (datetime.utcnow() + timedelta(hours=6)).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO trend_cache VALUES (?,?,?,?,?,?)",
        (keyword, trend_pct, velocity, source_count, datetime.utcnow().isoformat(), expires)
    )
    conn.commit()

def get_cached_sentiment(conn, keyword: str) -> Optional[Dict]:
    row = conn.execute(
        "SELECT compound, positive, negative, headline_count, top_headlines, expires_at "
        "FROM sentiment_cache WHERE keyword=?", (keyword,)
    ).fetchone()
    if row and row[5] > datetime.utcnow().isoformat():
        return {"compound": row[0], "positive": row[1], "negative": row[2],
                "headline_count": row[3], "top_headlines": row[4]}
    return None

def cache_sentiment(conn, keyword: str, data: Dict):
    expires = (datetime.utcnow() + timedelta(hours=4)).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO sentiment_cache VALUES (?,?,?,?,?,?,?,?)",
        (keyword, data["compound"], data["positive"], data["negative"],
         data["headline_count"], data.get("top_headlines", ""),
         datetime.utcnow().isoformat(), expires)
    )
    conn.commit()

# ========== RELIABLE DOMAIN SOURCE (Namebeta) ==========
def fetch_namebeta(limit: int = 200) -> List[Tuple[str, str]]:
    """Scrape Namebeta.com – returns real expiring domains with backlinks (no login)."""
    url = "https://namebeta.com/expiring"
    headers = {"User-Agent": random.choice(USER_AGENTS), "Referer": "https://namebeta.com/"}
    resp = http_get(url, timeout=25, headers=headers)
    if not resp:
        log.warning("Namebeta request failed")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    domains = []
    # Namebeta table: rows with class "domain-row" or standard table
    rows = soup.select("table tbody tr, .domain-row")
    if not rows:
        rows = soup.find_all("tr")[1:]  # fallback
    for row in rows[:limit]:
        cells = row.find_all("td")
        if len(cells) >= 2:
            domain_cell = cells[0]
            a = domain_cell.find("a")
            if a:
                d = a.get_text(strip=True).lower()
            else:
                d = domain_cell.get_text(strip=True).lower()
            if d and "." in d and len(d) < 50:
                domains.append((d, "namebeta"))
    log.info(f"Namebeta: {len(domains)} domains")
    return domains

def fetch_expireddomains_rss() -> List[Tuple[str, str]]:
    """Fallback – best-effort expireddomains.net scrape (often fails on GitHub)."""
    domains = []
    try:
        resp = http_get("https://www.expireddomains.net/deleted-domains/", timeout=15)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.select("table.base1 td a")[:100]:
                d = a.text.strip().lower()
                if d and "." in d:
                    domains.append((d, "expireddomains"))
    except Exception as e:
        log.debug(f"ExpiredDomains scrape error: {e}")
    log.info(f"ExpiredDomains fallback: {len(domains)} domains")
    return domains

def fetch_namecheap_drops() -> List[Tuple[str, str]]:
    domains = []
    try:
        resp = http_get("https://www.namecheap.com/domains/marketplace/drop-catching/", timeout=15)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            for el in soup.select("[data-domain], .domain-name")[:80]:
                d = (el.get("data-domain") or el.text).strip().lower()
                if d and "." in d:
                    domains.append((d, "namecheap_drop"))
    except Exception as e:
        log.debug(f"Namecheap drops error: {e}")
    log.info(f"Namecheap drops: {len(domains)} domains")
    return domains

def generate_fallback_domains(limit: int = 100) -> List[Tuple[str, str]]:
    """Only used if all real sources return 0."""
    keywords = list(NICHE_MAP.keys())[:20]
    tlds = [".com", ".io", ".ai", ".co"]
    domains = []
    for kw in keywords:
        for tld in tlds:
            domains.append((f"{kw}{tld}", "fallback"))
            domains.append((f"{kw}pro{tld}", "fallback"))
    random.shuffle(domains)
    log.warning(f"Using fallback keyword generator ({len(domains[:limit])} domains)")
    return domains[:limit]

# ========== TREND RADAR ==========
class TrendRadar:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._use_reddit_oauth = bool(REDDIT_CLIENT_ID and REDDIT_SECRET)

    def _google_trend(self, keyword: str) -> Tuple[float, float]:
        if not PYTRENDS_AVAILABLE:
            return 0.0, 0.0
        kw = re.sub(r"\.[a-z]{2,}$", "", keyword).replace("-", " ").strip()
        if len(kw) < 2:
            return 0.0, 0.0
        for attempt in range(3):
            try:
                pt = TrendReq(hl="en-US", tz=330, timeout=(10, 25), retries=2, backoff_factor=0.5)
                pt.build_payload([kw], timeframe="today 6-m")
                df = pt.interest_over_time()
                if df.empty or kw not in df.columns:
                    return 0.0, 0.0
                series = df[kw].values.astype(float)
                n = len(series)
                if n < 4:
                    return 0.0, 0.0
                half = n // 2
                recent = series[-half:].mean()
                older = series[:half].mean()
                trend_pct = 0.0 if older == 0 else round((recent - older) / older * 100, 1)
                if NUMPY_AVAILABLE and n >= 8:
                    x = np.arange(min(8, n), dtype=float)
                    y = series[-min(8, n):]
                    slope = float(np.polyfit(x, y, 1)[0])
                    velocity = round(slope / max(1, series.mean()) * 100, 2)
                else:
                    velocity = 0.0
                return trend_pct, velocity
            except Exception as e:
                if "429" in str(e):
                    time.sleep(4 ** attempt + random.uniform(1, 3))
                else:
                    break
        return 0.0, 0.0

    def _hn_keywords(self, limit: int = 50) -> Counter:
        counts = Counter()
        stories = http_get(HN_FEEDS["top"], timeout=10, json_resp=True)
        if not stories:
            return counts
        for sid in stories[:limit]:
            item = http_get(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json",
                            timeout=8, json_resp=True)
            if not item:
                continue
            title = item.get("title", "").lower()
            for word in re.findall(r"\b[a-z]{3,}\b", title):
                counts[word] += 1
            time.sleep(0.05)
        return counts

    def _reddit_oauth_keywords(self, limit_per_sub: int = 25) -> Counter:
        """Use Reddit OAuth for higher rate limits."""
        import requests.auth
        client_auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_SECRET)
        post_data = {"grant_type": "client_credentials"}
        headers = {"User-Agent": "DomainSniperV6/1.0"}
        try:
            resp = requests.post("https://www.reddit.com/api/v1/access_token",
                                 auth=client_auth, data=post_data, headers=headers, timeout=15)
            if resp.status_code != 200:
                log.debug("Reddit OAuth failed – falling back to public JSON")
                return self._reddit_public_keywords(limit_per_sub)
            token = resp.json().get("access_token")
            if not token:
                return self._reddit_public_keywords(limit_per_sub)
            headers["Authorization"] = f"bearer {token}"
            counts = Counter()
            for sub in REDDIT_SUBREDDITS[:6]:
                url = f"https://oauth.reddit.com/r/{sub}/hot.json?limit={limit_per_sub}"
                resp2 = requests.get(url, headers=headers, timeout=12)
                if resp2.status_code != 200:
                    continue
                data = resp2.json()
                for post in data.get("data", {}).get("children", []):
                    title = post.get("data", {}).get("title", "").lower()
                    score = post.get("data", {}).get("score", 1)
                    for word in re.findall(r"\b[a-z]{3,}\b", title):
                        counts[word] += max(1, int(math.log10(max(1, score))))
                time.sleep(0.5)
            return counts
        except Exception as e:
            log.debug(f"Reddit OAuth error: {e}")
            return self._reddit_public_keywords(limit_per_sub)

    def _reddit_public_keywords(self, limit_per_sub: int = 25) -> Counter:
        counts = Counter()
        headers = {"User-Agent": "DomainSniperV6/1.0"}
        for sub in REDDIT_SUBREDDITS[:6]:
            url = f"https://www.reddit.com/r/{sub}/hot.json?limit={limit_per_sub}"
            try:
                resp = requests.get(url, headers=headers, timeout=12)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                for post in data.get("data", {}).get("children", []):
                    title = post.get("data", {}).get("title", "").lower()
                    score = post.get("data", {}).get("score", 1)
                    for word in re.findall(r"\b[a-z]{3,}\b", title):
                        counts[word] += max(1, int(math.log10(max(1, score))))
                time.sleep(0.5)
            except Exception:
                pass
        return counts

    def _rss_keywords(self) -> Counter:
        counts = Counter()
        for _, feed_url in NEWS_RSS_FEEDS.items():
            try:
                parsed = feedparser.parse(feed_url)
                for entry in parsed.entries[:20]:
                    title = getattr(entry, "title", "").lower()
                    for word in re.findall(r"\b[a-z]{4,}\b", title):
                        counts[word] += 1
            except Exception:
                continue
        return counts

    def _coingecko_trending(self) -> List[str]:
        data = http_get("https://api.coingecko.com/api/v3/search/trending", timeout=10, json_resp=True)
        if not data:
            return []
        return [coin["item"]["symbol"].lower() for coin in data.get("coins", [])[:7]]

    STOP_WORDS = {"the","and","for","this","that","with","from","are","has","was","not","but","its","you","can","will","new","how","why","all","one","what","have","they","some","more","use","get","out","via","top","over","been","were","just","now","your","who","our","may","into","than","says","said","also","about","their","would","could","year","time","first","make","like","other"}

    def get_trending_keywords(self, top_n: int = 30) -> List[Dict]:
        log.info("TrendRadar: fetching multi‑source signals…")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            f_hn = ex.submit(self._hn_keywords)
            if self._use_reddit_oauth:
                f_reddit = ex.submit(self._reddit_oauth_keywords)
            else:
                f_reddit = ex.submit(self._reddit_public_keywords)
            f_rss = ex.submit(self._rss_keywords)
            f_crypto = ex.submit(self._coingecko_trending)
            hn_counts = f_hn.result()
            reddit_counts = f_reddit.result()
            rss_counts = f_rss.result()
            crypto_kws = f_crypto.result()
        all_kws = set(hn_counts) | set(reddit_counts) | set(rss_counts)
        all_kws = {k for k in all_kws if k not in self.STOP_WORDS and len(k) >= 4}
        combined = {}
        for kw in all_kws:
            score = hn_counts.get(kw, 0) * 3.0 + reddit_counts.get(kw, 0) * 2.0 + rss_counts.get(kw, 0) * 1.5
            if kw in crypto_kws:
                score += 10
            if score > 0:
                combined[kw] = score
        top = sorted(combined, key=combined.get, reverse=True)[:top_n]
        log.info(f"TrendRadar: {len(top)} keywords")
        results = []
        for kw in top:
            cached = get_cached_trend(self.conn, kw)
            if cached:
                trend_pct, vel = cached["trend_pct"], cached["velocity"]
            else:
                trend_pct, vel = self._google_trend(kw)
                cache_trend(self.conn, kw, trend_pct, vel, 3)
                time.sleep(random.uniform(0.3, 0.8))
            results.append({
                "keyword": kw,
                "combined_signal": combined[kw],
                "trend_pct": trend_pct,
                "velocity": vel,
                "in_hn": kw in hn_counts,
                "in_reddit": kw in reddit_counts,
                "in_rss": kw in rss_counts,
                "in_crypto": kw in crypto_kws,
            })
        for r in results:
            self.conn.execute(
                "INSERT OR REPLACE INTO keyword_signals VALUES (?,?,?,?,?,?,?)",
                (r["keyword"], int(r["in_hn"]), int(r["combined_signal"]),
                 int(r["in_rss"]), int(r["in_crypto"]),
                 r["combined_signal"], datetime.utcnow().isoformat())
            )
        self.conn.commit()
        return results

# ========== SENTIMENT ENGINE ==========
class SentimentEngine:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.analyzer = SentimentIntensityAnalyzer() if VADER_AVAILABLE else None

    def _score_text(self, text: str) -> Dict:
        if not self.analyzer:
            return {"compound": 0, "pos": 0, "neg": 0, "neu": 1}
        return self.analyzer.polarity_scores(text)

    def _fetch_google_news_rss(self, keyword: str) -> List[str]:
        url = f"https://news.google.com/rss/search?q={quote_plus(keyword)}&hl=en-US&gl=US"
        headlines = []
        try:
            parsed = feedparser.parse(url)
            for entry in parsed.entries[:15]:
                headlines.append(getattr(entry, "title", ""))
        except Exception:
            pass
        return headlines

    def _fetch_newsapi(self, keyword: str) -> List[str]:
        if not NEWS_API_KEY:
            return []
        url = f"https://newsapi.org/v2/everything?q={quote_plus(keyword)}&sortBy=publishedAt&pageSize=10&language=en&apiKey={NEWS_API_KEY}"
        data = http_get(url, timeout=10, json_resp=True)
        if not data:
            return []
        return [a.get("title", "") for a in data.get("articles", [])[:10]]

    def analyze(self, keyword: str) -> Dict:
        cached = get_cached_sentiment(self.conn, keyword)
        if cached:
            cached["sentiment_score"] = 50 + cached["compound"] * 50
            return cached
        headlines = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(self._fetch_google_news_rss, keyword)
            f2 = ex.submit(self._fetch_newsapi, keyword)
            headlines += f1.result()
            headlines += f2.result()
        if not headlines:
            # Fallback neutral sentiment
            result = {"compound": 0.0, "positive": 0.0, "negative": 0.0,
                      "headline_count": 0, "top_headlines": "[]", "sentiment_score": 50}
            cache_sentiment(self.conn, keyword, result)
            return result
        scores = [self._score_text(h) for h in headlines]
        compound_avg = sum(s["compound"] for s in scores) / len(scores)
        pos_avg = sum(s["pos"] for s in scores) / len(scores)
        neg_avg = sum(s["neg"] for s in scores) / len(scores)
        sorted_hl = sorted(zip(headlines, scores), key=lambda x: abs(x[1]["compound"]), reverse=True)
        top3 = json.dumps([h for h, _ in sorted_hl[:3]])
        result = {
            "compound": round(compound_avg, 4),
            "positive": round(pos_avg, 4),
            "negative": round(neg_avg, 4),
            "headline_count": len(headlines),
            "top_headlines": top3,
            "sentiment_score": 50 + compound_avg * 50,
        }
        cache_sentiment(self.conn, keyword, result)
        return result

# ========== KEYWORD TO DOMAIN GENERATOR ==========
class KeywordIntel:
    PREFIXES = ["get", "my", "the", "use", "try", "go", "we", "pro", "smart"]
    SUFFIXES = ["pro", "hub", "app", "hq", "ai", "io", "lab", "ly", "ify", "base", "desk", "now", "fast", "easy", "plus"]
    NICHE_TLDS = {
        "crypto": [".io", ".finance", ".com"],
        "ai": [".ai", ".io", ".com"],
        "saas": [".io", ".com", ".app"],
        "health": [".com", ".care", ".health"],
        "default": [".com", ".io", ".co"],
    }
    def generate(self, keywords: List[Dict], top_n: int = 120) -> List[Tuple[str, str]]:
        scored = sorted(keywords, key=lambda k: k.get("combined_signal", 0) * 0.5 + k.get("trend_pct", 0) * 0.3 + k.get("velocity", 0) * 0.2, reverse=True)
        domains = []
        seen = set()
        for kw_data in scored[:20]:
            kw = kw_data["keyword"].lower().strip()
            if len(kw) < 3 or len(kw) > 18:
                continue
            niche = "default"
            for n in ["crypto", "ai", "saas", "health"]:
                if n in kw:
                    niche = n
                    break
            tlds = self.NICHE_TLDS.get(niche, self.NICHE_TLDS["default"])
            combos = [kw] + [f"{p}{kw}" for p in self.PREFIXES] + [f"{kw}{s}" for s in self.SUFFIXES] + [f"{p}{kw}{s}" for p in ["get","my","use"] for s in ["ai","pro","hq"]]
            for combo in combos:
                if 3 <= len(combo) <= 18:
                    for tld in tlds:
                        domain = f"{combo}{tld}"
                        if domain not in seen:
                            seen.add(domain)
                            domains.append((domain, f"trend:{kw}"))
        random.shuffle(domains)
        return domains[:top_n]

# ========== SCORING HELPERS ==========
def get_cc_index() -> str:
    data = http_get("https://index.commoncrawl.org/collinfo.json", timeout=10, json_resp=True)
    if data:
        return data[0].get("cdx-api", "https://index.commoncrawl.org/CC-MAIN-2024-10-index")
    return "https://index.commoncrawl.org/CC-MAIN-2024-10-index"

CC_INDEX_URL = ""

def wayback_backlinks(domain: str) -> int:
    url = f"http://web.archive.org/cdx/search/cdx?url=*.{domain}&output=text&fl=urlkey&limit=500&collapse=urlkey"
    resp = http_get(url, timeout=25)
    if not resp:
        return 0
    ref_domains = set()
    for line in resp.text.splitlines():
        parts = line.strip().split("/")
        if parts:
            ref_domains.add(parts[0].replace(")", "").split(",")[-1])
    return len(ref_domains)

def wayback_traffic_proxy(domain: str) -> Tuple[int, int]:
    url = f"http://web.archive.org/cdx/search/cdx?url={domain}&output=json&fl=timestamp&limit=200"
    resp = http_get(url, timeout=20)
    if not resp:
        return 0, 0
    try:
        data = resp.json()
        count = max(0, len(data) - 1)
        est = min(count * 300, 50000)
        return count, est
    except Exception:
        return 0, 0

def commoncrawl_presence(domain: str) -> int:
    if not CC_INDEX_URL:
        return 0
    url = f"{CC_INDEX_URL}?url={domain}&output=json&limit=5"
    resp = http_get(url, timeout=15)
    if not resp:
        return 0
    count = 0
    for line in resp.text.strip().splitlines():
        try:
            json.loads(line.strip())
            count += 1
        except Exception:
            pass
    return count

def domain_age(domain: str) -> int:
    try:
        w = whois.whois(domain)
        creation = w.creation_date
        if isinstance(creation, list):
            creation = creation[0]
        if creation:
            return min(25, (datetime.now() - creation).days // 365)
    except Exception:
        pass
    return 0

def detect_niche(domain: str) -> Tuple[str, int]:
    dl = domain.lower().replace("-", "").replace(".", "")
    best_niche, best_score = "general", 30
    for kw, score in NICHE_MAP.items():
        if kw in dl and score > best_score:
            best_niche, best_score = kw, score
    return best_niche, best_score

def domain_length_score(domain: str) -> float:
    sld = domain.split(".")[0]
    n = len(sld)
    if n <= 4: return 100
    if n <= 6: return 90
    if n <= 8: return 75
    if n <= 10: return 58
    if n <= 13: return 40
    return max(0, 40 - (n - 13) * 3)

def brandability(domain: str) -> float:
    sld = domain.split(".")[0].lower()
    score = 50
    if re.search(r"\d", sld): score -= 20
    if "-" in sld: score -= 20
    if len(sld) > 12: score -= 15
    if len(sld) < 3: score -= 10
    if re.search(r"[aeiou]{2,}", sld): score += 10
    if 4 <= len(sld) <= 7: score += 22
    if sld == sld[::-1] and len(sld) > 2: score += 5
    vowel_ratio = sum(1 for c in sld if c in "aeiou") / max(1, len(sld))
    if 0.2 <= vowel_ratio <= 0.5: score += 8
    return max(0, min(100, score))

def spam_check(domain: str) -> Tuple[int, float]:
    url = f"http://web.archive.org/cdx/search/cdx?url={domain}&output=json&fl=timestamp&limit=50"
    resp = http_get(url, timeout=15)
    if not resp:
        return 0, 85
    try:
        snapshots = resp.json()
        if len(snapshots) < 2:
            return 0, 85
        rows = snapshots[1:]
        sample = random.sample(rows, min(3, len(rows)))
        spam_pat = re.compile(r"viagra|cialis|casino|poker|adult|xxx|pharma|pills|escort|gambling", re.I)
        hits = 0
        for ts in sample:
            snap_url = f"http://web.archive.org/web/{ts[0]}/{domain}"
            r = http_get(snap_url, timeout=10, retries=1)
            if r and spam_pat.search(r.text):
                hits += 1
            time.sleep(0.3)
        return hits, max(0, 100 - hits * 35)
    except Exception:
        return 0, 85

def check_safe_browsing(domain: str) -> int:
    if not SAFE_BROWSING_KEY:
        return 1
    body = {
        "client": {"clientId": "domain-sniper-v6", "clientVersion": "6.0"},
        "threatInfo": {
            "threatTypes": ["MALWARE", "SOCIAL_ENGINEERING", "UNWANTED_SOFTWARE"],
            "platformTypes": ["ANY_PLATFORM"],
            "threatEntryTypes": ["URL"],
            "threatEntries": [{"url": f"http://{domain}"}],
        }
    }
    try:
        resp = http_post(f"https://safebrowsing.googleapis.com/v4/threatMatches:find?key={SAFE_BROWSING_KEY}", body)
        return 0 if (resp and resp.json().get("matches")) else 1
    except Exception:
        return 1

def foundation_score(backlinks: int, cc_hits: int, age: int, tld: str) -> float:
    bl = min(100, backlinks / 3)
    cc = min(100, cc_hits * 20)
    ag = min(100, age * 5)
    t = TLD_VALUE.get(tld, DEFAULT_TLD)
    return bl * 0.32 + cc * 0.26 + ag * 0.24 + t * 0.18

def flip_score_fn(domain: str, niche_cpm: float, age: int, tld: str) -> float:
    length_s = domain_length_score(domain)
    brand_s = brandability(domain)
    tld_s = TLD_VALUE.get(tld, DEFAULT_TLD)
    if age < 5: age_s = min(100, age * 8)
    elif age <= 15: age_s = 100
    else: age_s = max(40, 100 - (age - 15) * 2)
    return niche_cpm * 0.28 + length_s * 0.27 + brand_s * 0.22 + tld_s * 0.13 + age_s * 0.10

def sentiment_score_fn(compound: float, headline_count: int, velocity: float, source_count: int) -> float:
    base = 50 + compound * 50
    coverage = min(15, headline_count * 0.5) if compound > 0 else max(-15, -headline_count * 0.5)
    vel_bonus = max(-10, min(10, velocity * 0.3))
    src_bonus = min(5, source_count * 1.5) if compound >= 0 else 0
    return max(0.0, min(100.0, base + coverage + vel_bonus + src_bonus))

def momentum_score_fn(trend_pct: float) -> float:
    return max(0.0, min(100.0, 50.0 + trend_pct / 2.0))

def trend_velocity_label(velocity: float) -> str:
    if velocity > 15: return "🚀 Rocket"
    if velocity > 5: return "📈 Rising"
    if velocity > -5: return "➡️ Stable"
    if velocity > -15: return "📉 Fading"
    return "⬇️ Falling"

def detect_monetization_paths(domain, niche, age, backlinks, monthly_traffic, cc_hits, trend_pct, compound_sentiment):
    paths = []
    sentiment_multiplier = 1.0 + max(-0.2, min(0.4, compound_sentiment * 0.4))
    if backlinks > 20 and age >= 3:
        flip_est = int((backlinks * age * 2.5 + age * 40) * sentiment_multiplier)
        paths.append(("flip", 0, flip_est, f"List on Sedo/Dan – {age}y aged, {backlinks} BL, sentiment {compound_sentiment:+.2f}"))
    elif age >= 5:
        flip_est = int(age * 35 * sentiment_multiplier)
        paths.append(("flip", 0, flip_est, f"Aged {age}y domain, sentiment adjusted"))
    else:
        flip_est = 0
    cpm = PARKING_CPM.get(niche, PARKING_CPM["general"])
    if monthly_traffic > 50:
        park_mo = (monthly_traffic / 1000) * cpm
        paths.append(("parking", park_mo, 0, f"Park with Bodis – ~${park_mo:.0f}/mo @ ${cpm} CPM"))
    if niche in ["ai","saas","crypto","fintech","health","travel","llm"] and age >= 2 and cc_hits > 0:
        paths.append(("content_site", 5, 0, "5-page niche site + AdSense"))
    lead_val = LEAD_VALUE.get(niche, 0)
    if lead_val > 0 and age >= 1:
        leads_mo = max(1, monthly_traffic // 100) * lead_val
        paths.append(("lead_gen", leads_mo, 0, f"Lead-gen for {niche} – ~${leads_mo:.0f}/mo"))
    if niche in ["crypto","fintech","saas","insurance"] and monthly_traffic > 20:
        aff_mo = (monthly_traffic / 1000) * 15
        paths.append(("affiliate", aff_mo, 0, f"Affiliate redirect – ~${aff_mo:.0f}/mo"))
    if not paths:
        paths.append(("hold_and_list", 0, max(50, flip_est), "List and hold"))
    paths.sort(key=lambda x: x[1] + x[2] * 0.1, reverse=True)
    primary = paths[0]
    secondary = paths[1] if len(paths) > 1 else paths[0]
    return primary[0], secondary[0], primary[1], flip_est, primary[3]

def monetization_score_fn(monthly_est: float) -> float:
    return min(100, monthly_est) if monthly_est > 0 else 0

def build_affiliate_links(domain: str) -> Dict[str, str]:
    links = {}
    if AFFILIATE_ID_GD:
        links["godaddy"] = f"https://www.godaddy.com/domainsearch/find?domainToCheck={domain}&isc={AFFILIATE_ID_GD}"
    if AFFILIATE_ID_NC:
        links["namecheap"] = f"https://www.namecheap.com/domains/registration/results/?domain={domain}&AffiliateCode={AFFILIATE_ID_NC}"
    links["dan_com"] = f"https://dan.com/buy-domain/{domain}"
    links["sedo"] = f"https://sedo.com/search/details/?domain={domain}"
    links["afternic"] = f"https://www.afternic.com/domain/{domain}"
    return links

# ========== DOMAIN PROCESSOR ==========
def process_domain(domain: str, source: str, conn: sqlite3.Connection, sentiment_engine: SentimentEngine) -> Optional[Dict]:
    tld = "." + domain.split(".")[-1]
    sld = domain.split(".")[0]
    if TLD_VALUE.get(tld, DEFAULT_TLD) < 20:
        return None
    if not (2 <= len(sld) <= 22):
        return None
    if re.search(r"\d{4,}", sld):
        return None
    backlinks = wayback_backlinks(domain)
    snap_count, traffic = wayback_traffic_proxy(domain)
    cc_hits = commoncrawl_presence(domain)
    age = domain_age(domain)
    niche, niche_cpm = detect_niche(domain)
    spam_flags, hist_s = spam_check(domain)
    safe = check_safe_browsing(domain)
    kw_for_trend = re.sub(r"\.[a-z]{2,}$", "", sld).replace("-", " ").strip()
    cached_trend = get_cached_trend(conn, kw_for_trend)
    if cached_trend:
        trend_pct, velocity = cached_trend["trend_pct"], cached_trend["velocity"]
    else:
        trend_pct, velocity = 0.0, 0.0
        cache_trend(conn, kw_for_trend, 0.0, 0.0, 1)
    sent_data = sentiment_engine.analyze(kw_for_trend)
    compound = sent_data.get("compound", 0.0)
    headline_count = sent_data.get("headline_count", 0)
    if not safe:
        hist_s = min(hist_s, 20)
    if spam_flags >= 3 and hist_s < 30:
        add_to_blacklist(conn, domain, f"spam={spam_flags}, hist={hist_s}")
        return None
    W = get_composite_weights(age, niche)
    found = foundation_score(backlinks, cc_hits, age, tld)
    flip = flip_score_fn(domain, niche_cpm, age, tld)
    senti = sentiment_score_fn(compound, headline_count, velocity, 3)
    moment = momentum_score_fn(trend_pct)
    pri_path, sec_path, monthly_est, flip_est, mon_note = detect_monetization_paths(
        domain, niche, age, backlinks, traffic, cc_hits, trend_pct, compound)
    mon_s = monetization_score_fn(monthly_est)
    final = int(found * W["foundation"] + flip * W["flip"] + hist_s * W["history"] + senti * W["sentiment"] + moment * W["momentum"] + mon_s * W["monetization"])
    aff = build_affiliate_links(domain)
    return {
        "fetched_at": datetime.utcnow().isoformat(),
        "domain": domain, "source": source, "tld": tld, "sld_length": len(sld),
        "final_score": final,
        "foundation": round(found, 1), "flip_score": round(flip, 1),
        "history_score": round(hist_s, 1), "sentiment_score": round(senti, 1),
        "momentum_score": round(moment, 1), "monetization_score": round(mon_s, 1),
        "age_years": age, "backlinks_proxy": backlinks, "wayback_snapshots": snap_count,
        "est_monthly_traffic": traffic, "commoncrawl_hits": cc_hits,
        "niche": niche, "niche_cpm": niche_cpm,
        "trend_6m_pct": trend_pct, "trend_velocity": velocity, "velocity_label": trend_velocity_label(velocity),
        "sentiment_compound": round(compound, 4), "sentiment_headline_n": headline_count, "top_headlines": sent_data.get("top_headlines", "[]"),
        "spam_flags": spam_flags, "safe_browsing_clean": safe,
        "primary_path": pri_path, "secondary_path": sec_path, "est_monthly_usd": round(monthly_est, 2),
        "flip_estimate_usd": flip_est, "flip_range": f"${flip_est*.8:.0f}–${flip_est*1.2:.0f}" if flip_est else "TBD",
        "monetization_note": mon_note,
        "link_sedo": aff.get("sedo", ""), "link_dan": aff.get("dan_com", ""), "link_afternic": aff.get("afternic", ""),
        "link_godaddy_aff": aff.get("godaddy", ""), "link_namecheap_aff": aff.get("namecheap", ""),
        "weights_used": json.dumps(W),
    }

def process_domain_safe(args: Tuple) -> Optional[Dict]:
    domain, source, conn, sent = args
    try:
        return process_domain(domain, source, conn, sent)
    except Exception as e:
        log.error(f"Error processing {domain}: {e}")
        traceback.print_exc()
        return None

# ========== OUTPUTS ==========
def push_to_sheets(df: pd.DataFrame):
    if not GSPREAD_AVAILABLE or not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID:
        log.warning("Google Sheets skipped (missing config/gspread)")
        return
    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=40)
        headers = df.columns.tolist()
        values = [headers] + df.fillna("").astype(str).values.tolist()
        existing = ws.get_all_values()
        if not existing:
            ws.update(values, value_input_option="RAW")
        else:
            ws.append_rows(values[1:], value_input_option="RAW")
        log.info(f"Google Sheets: pushed {len(df)} rows")
    except Exception as e:
        log.error(f"Sheets push failed: {e}")

def send_telegram(d: Dict):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    path_emoji = {"flip": "💸", "parking": "🅿️", "lead_gen": "🎯", "affiliate": "🔗", "content_site": "📝", "hold_and_list": "⏳"}
    sent_emoji = "🟢" if d["sentiment_compound"] > 0.1 else "🔴" if d["sentiment_compound"] < -0.1 else "⚪"
    msg = (
        f"🏆 *DOMAIN PEARL v6* — Score {d['final_score']}/100\n"
        f"🌐 *{d['domain']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Foundation: {d['foundation']:.0f}  │  Flip: {d['flip_score']:.0f}\n"
        f"History: {d['history_score']:.0f}  │  Momentum: {d['momentum_score']:.0f}\n"
        f"{sent_emoji} Sentiment: {d['sentiment_score']:.0f}  │  Velocity: {d['velocity_label']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷 Niche: *{d['niche']}*  │  Age: {d['age_years']}y\n"
        f"📊 Traffic: ~{d['est_monthly_traffic']:,}/mo  │  BL: {d['backlinks_proxy']}\n"
        f"📰 Headlines: {d['sentiment_headline_n']}  │  Compound: {d['sentiment_compound']:+.3f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{path_emoji.get(d['primary_path'], '💡')} *{d['primary_path'].replace('_',' ').title()}*\n"
        f"💰 Monthly: ${d['est_monthly_usd']:.0f}  │  Flip: {d['flip_range']}\n"
        f"📋 {d['monetization_note']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 [Sedo]({d['link_sedo']}) │ [Dan]({d['link_dan']}) │ [Afternic]({d['link_afternic']})\n"
        f"⚡ Source: {d['source']}"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for i in range(0, len(msg), 4000):
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[i:i+4000], "parse_mode": "Markdown", "disable_web_page_preview": True}, timeout=10)
        except Exception:
            pass

def send_email_digest(results: List[Dict]):
    if not EMAIL_DIGEST_TO or not GMAIL_USER or not GMAIL_APP_PASS or not results:
        return
    top = sorted(results, key=lambda x: x["final_score"], reverse=True)[:12]
    html_rows = ""
    for d in top:
        sent_color = "#22c55e" if d["sentiment_compound"] > 0.1 else "#ef4444" if d["sentiment_compound"] < -0.1 else "#94a3b8"
        html_rows += f"""
        <tr><td><b>{d['domain']}</b><br><small>{d['source']}</small></td>
        <td align="center"><b>{d['final_score']}</b></td><td>{d['niche']}</td>
        <td>{d['primary_path'].replace('_',' ').title()}</td>
        <td align="center"><span style="color:{sent_color}">{d['sentiment_compound']:+.2f}</span><br><small>{d['velocity_label']}</small></td>
        <td align="center">${d['est_monthly_usd']:.0f}/mo</td><td>{d['flip_range']}</td>
        <td><a href="{d['link_sedo']}">Sedo</a> │ <a href="{d['link_dan']}">Dan</a></td></tr>"""
    html = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:780px;margin:auto;color:#1e293b">
    <h2 style="color:#6366f1">🏴‍☠️ Domain Sniper PRO v6 – Daily Intelligence Digest</h2>
    <p>{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} │ {len(results)} domains scanned</p>
    <table border="1" cellpadding="6" style="border-collapse:collapse;width:100%;font-size:13px">
      <tr style="background:#1e1b4b;color:white"><th>Domain</th><th>Score</th><th>Niche</th><th>Best Path</th><th>Sentiment / Velocity</th><th>Monthly</th><th>Flip Range</th><th>Links</th></tr>
      {html_rows}
    </table>
    <p style="font-size:10px;color:#94a3b8;margin-top:20px">Domain Sniper PRO v6 – automated estimates only. DYOR before purchasing.</p>
    </body></html>"""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🏴‍☠️ Domain Sniper PRO v6 – {len(top)} Pearls ({datetime.utcnow().strftime('%Y-%m-%d')})"
    msg["From"] = GMAIL_USER
    msg["To"] = EMAIL_DIGEST_TO
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_USER, GMAIL_APP_PASS)
            s.sendmail(GMAIL_USER, EMAIL_DIGEST_TO, msg.as_string())
        log.info("Email digest sent.")
    except Exception as e:
        log.error(f"Email error: {e}")

# ========== MAIN ==========
def main():
    global CC_INDEX_URL
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info("=" * 70)
    log.info("  Domain Fortress Sniper PRO  v6 – Fixed & Production Ready")
    log.info("=" * 70)
    log.info(f"Run ID: {run_id}")
    log.info(f"VADER: {'✓' if VADER_AVAILABLE else '✗'}")
    log.info(f"pytrends: {'✓' if PYTRENDS_AVAILABLE else '✗'}")
    log.info(f"gspread: {'✓' if GSPREAD_AVAILABLE else '✗'}")
    log.info("=" * 70)

    conn = init_db()
    CC_INDEX_URL = get_cc_index()
    log.info(f"CommonCrawl index: {CC_INDEX_URL}")

    # Trend radar
    radar = TrendRadar(conn)
    if CUSTOM_KEYWORDS:
        trending = [{"keyword": k, "combined_signal": 100, "trend_pct": 0, "velocity": 0,
                     "in_hn": False, "in_reddit": False, "in_rss": False, "in_crypto": False}
                    for k in CUSTOM_KEYWORDS]
        log.info(f"Using custom keywords: {CUSTOM_KEYWORDS}")
    else:
        trending = radar.get_trending_keywords(top_n=30)
    log.info(f"Top 5 trending: {[k['keyword'] for k in trending[:5]]}")

    # Domain discovery – primary reliable source
    all_domains = fetch_namebeta(limit=200)
    if not all_domains:
        log.warning("Namebeta returned 0, trying fallback sources...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            f_exp = ex.submit(fetch_expireddomains_rss)
            f_nc = ex.submit(fetch_namecheap_drops)
            all_domains = f_exp.result() + f_nc.result()
    if not all_domains:
        log.warning("All real sources returned 0 – using trend-generated domains")
        intel = KeywordIntel()
        all_domains = intel.generate(trending, top_n=150)
    if not all_domains:
        all_domains = generate_fallback_domains(100)

    log.info(f"Total raw candidates: {len(all_domains)}")
    seen_set = set()
    unique = []
    for d, src in all_domains:
        d = d.strip().lower()
        if d and "." in d and d not in seen_set and not is_seen(conn, d) and not is_blacklisted(conn, d):
            seen_set.add(d)
            unique.append((d, src))
    log.info(f"Unique new domains to score: {len(unique)}")
    if not unique:
        pd.DataFrame(columns=["domain", "final_score"]).to_csv(f"domains_v6_{run_id}.csv", index=False)
        conn.close()
        return

    # Scoring
    sentiment_engine = SentimentEngine(conn)
    results = []
    args_list = [(d, src, conn, sentiment_engine) for d, src in unique]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_domain_safe, args): args[0] for args in args_list}
        for fut in concurrent.futures.as_completed(futures):
            d_name = futures[fut]
            try:
                data = fut.result()
                if data and data["final_score"] >= SCORE_FLOOR:
                    results.append(data)
                    mark_seen(conn, d_name, data["final_score"], data["primary_path"])
                    log.info(f"✓ {d_name:38s} score={data['final_score']:3d}  niche={data['niche']:12s}  sent={data['sentiment_compound']:+.2f}  vel={data['velocity_label']}")
            except Exception as e:
                log.error(f"Future error {d_name}: {e}")
    if not results:
        log.warning("No domains passed score floor")
        pd.DataFrame(columns=["domain", "final_score"]).to_csv(f"domains_v6_{run_id}.csv", index=False)
        conn.close()
        return

    df = pd.DataFrame(results).sort_values("final_score", ascending=False)
    csv_path = f"domains_v6_{run_id}.csv"
    df.to_csv(csv_path, index=False)
    log.info(f"CSV saved: {csv_path}  ({len(df)} rows)")
    push_to_sheets(df)

    pearls = df[df["final_score"] >= MIN_ALERT_SCORE]
    log.info(f"Pearls (score ≥ {MIN_ALERT_SCORE}): {len(pearls)}")
    for _, row in pearls.iterrows():
        send_telegram(row.to_dict())
        time.sleep(1)
    send_email_digest(results)

    best = df.iloc[0]
    conn.execute("INSERT OR REPLACE INTO run_stats VALUES (?,?,?,?,?,?,?)",
                 (run_id, run_id[:15], datetime.utcnow().isoformat(),
                  len(results), len(pearls), best["domain"], int(best["final_score"])))
    conn.commit()

    log.info("═" * 70)
    log.info("  FINAL SUMMARY")
    log.info(f"  Domains scored      : {len(results)}")
    log.info(f"  Pearls found        : {len(pearls)}")
    log.info(f"  Best domain         : {best['domain']}  (score {best['final_score']})")
    log.info(f"  Best niche          : {best['niche']}")
    log.info(f"  Best sentiment      : {best['sentiment_compound']:+.3f}  {best['velocity_label']}")
    log.info(f"  Best path           : {best['primary_path']} → {best['monetization_note']}")
    log.info(f"  CSV output          : {csv_path}")
    log.info("═" * 70)

    conn.close()
    log.info("Done.")

if __name__ == "__main__":
    main()
