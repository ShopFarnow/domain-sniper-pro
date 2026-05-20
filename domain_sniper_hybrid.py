#!/usr/bin/env python3
"""
Domain Fortress Sniper – Hybrid Edition (with detailed progress tracking)
"""

import os, sys, re, time, json, sqlite3, logging, smtplib
import random, traceback, math, threading
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
import whois

# Optional imports
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_OK = True
except ImportError:
    VADER_OK = False
try:
    from pytrends.request import TrendReq
    PYTRENDS_OK = True
except ImportError:
    PYTRENDS_OK = False
try:
    import numpy as np
    NUMPY_OK = True
except ImportError:
    NUMPY_OK = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_OK = True
except ImportError:
    GSPREAD_OK = False

# ---------- LOGGING (with timestamps) ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("DomainSniperHybrid")

# ---------- ENVIRONMENT (unchanged) ----------
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NAME        = os.getenv("SHEET_NAME", "DomainSniperHybrid")
MIN_ALERT_SCORE   = int(os.getenv("MIN_ALERT_SCORE", "68"))
SAFE_BROWSING_KEY = os.getenv("SAFE_BROWSING_KEY", "")
EMAIL_DIGEST_TO   = os.getenv("EMAIL_DIGEST_TO", "")
GMAIL_USER        = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASS    = os.getenv("GMAIL_APP_PASS", "")
AFFILIATE_ID_GD   = os.getenv("AFFILIATE_ID_GD", "")
AFFILIATE_ID_NC   = os.getenv("AFFILIATE_ID_NC", "")
DB_PATH           = os.getenv("DB_PATH", "domain_sniper_hybrid.db")
MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "2"))
NEWS_API_KEY      = os.getenv("NEWS_API_KEY", "")
KELLY_BANKROLL    = float(os.getenv("KELLY_BANKROLL", "10000"))
ENABLE_TRADEMARK  = os.getenv("USPTO_SEARCH", "0") == "1"
MAX_DOMAINS       = int(os.getenv("MAX_DOMAINS", "300"))
CUSTOM_KEYWORDS   = [k.strip() for k in os.getenv("TREND_KEYWORDS", "").split(",") if k.strip()]

# ---------- CONSTANTS (unchanged) ----------
TLD_VALUE = {
    ".com":100, ".io":88, ".ai":92, ".co":75, ".net":60, ".org":55,
    ".in":45, ".us":42, ".app":74, ".dev":72, ".tech":58,
    ".online":30, ".info":25, ".biz":20, ".xyz":28, ".gg":65,
    ".vc":68, ".finance":62,
}
DEFAULT_TLD = 15

NICHE_MAP = {
    "insurance":{"score":90,"cpc":54.91}, "loan":{"score":90,"cpc":44.28},
    "mortgage":{"score":88,"cpc":47.12},  "crypto":{"score":82,"cpc":9.80},
    "ai":{"score":88,"cpc":8.50},         "saas":{"score":82,"cpc":11.20},
    "health":{"score":76,"cpc":6.10},     "lawyer":{"score":85,"cpc":54.86},
    "realestate":{"score":80,"cpc":27.14},"clinic":{"score":78,"cpc":16.40},
    "dentist":{"score":80,"cpc":31.90},   "plumber":{"score":72,"cpc":22.50},
    "solar":{"score":76,"cpc":12.40},     "fintech":{"score":84,"cpc":13.20},
    "ecommerce":{"score":72,"cpc":7.80},  "vpn":{"score":80,"cpc":6.90},
    "hosting":{"score":74,"cpc":8.60},    "invest":{"score":86,"cpc":36.40},
    "forex":{"score":82,"cpc":21.00},     "nft":{"score":68,"cpc":4.20},
    "defi":{"score":78,"cpc":8.90},       "llm":{"score":85,"cpc":9.00},
    "gpt":{"score":84,"cpc":7.50},        "blockchain":{"score":78,"cpc":5.60},
    "robotics":{"score":75,"cpc":3.20},   "ev":{"score":74,"cpc":4.10},
    "therapy":{"score":74,"cpc":18.90},   "telehealth":{"score":78,"cpc":12.30},
    "biotech":{"score":76,"cpc":7.80},    "quantum":{"score":80,"cpc":4.50},
    "gaming":{"score":68,"cpc":2.90},     "travel":{"score":65,"cpc":1.80},
    "shop":{"score":56,"cpc":1.20},       "general":{"score":30,"cpc":0.50},
}
NICHE_SCORE = {k: v["score"] for k, v in NICHE_MAP.items()}
NICHE_CPC   = {k: v["cpc"]   for k, v in NICHE_MAP.items()}

PARKING_CPM = {
    "insurance":18,"loan":15,"mortgage":14,"crypto":13,"ai":12,
    "saas":10,"health":9,"lawyer":14,"travel":8,"shop":5,
    "realestate":11,"clinic":10,"dentist":12,"plumber":9,
    "solar":10,"fintech":13,"llm":11,"ev":9,"general":3,
}
LEAD_VALUE = {
    "insurance":28,"loan":22,"mortgage":32,"lawyer":45,
    "dentist":18,"clinic":14,"plumber":10,"solar":20,
    "realestate":22,"telehealth":16,"therapy":14,
}

FLIP_MULTIPLES = {
    "insurance":{"mean":4.2,"std":1.8},  "lawyer":{"mean":5.1,"std":2.1},
    "ai":{"mean":6.8,"std":3.2},         "llm":{"mean":7.5,"std":3.8},
    "fintech":{"mean":4.8,"std":2.0},    "saas":{"mean":5.5,"std":2.5},
    "crypto":{"mean":3.9,"std":2.4},     "health":{"mean":3.7,"std":1.6},
    "realestate":{"mean":4.0,"std":1.9}, "general":{"mean":2.5,"std":1.2},
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

NEWS_RSS_FEEDS = {
    "tech":    "https://feeds.feedburner.com/TechCrunch",
    "ai":      "https://www.artificialintelligence-news.com/feed/",
    "crypto":  "https://cointelegraph.com/rss",
    "business":"https://feeds.reuters.com/reuters/businessNews",
    "startup": "https://techcrunch.com/startups/feed/",
    "finance": "https://www.marketwatch.com/rss/realtimeheadlines",
    "science": "https://rss.sciencedaily.com/all.xml",
}
HN_TOP = "https://hacker-news.firebaseio.com/v0/topstories.json"
REDDIT_SUBS = [
    "technology","artificial","MachineLearning","startups",
    "entrepreneur","investing","personalfinance","SaaS",
]

# ---------- HTTP LAYER with timeout and retry (unchanged) ----------
def http_get(url: str, timeout: int = 20, retries: int = 3,
             backoff: float = 2.0, json_resp: bool = False,
             extra_headers: Optional[Dict] = None) -> Any:
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json() if json_resp else r
            if r.status_code in (429, 503):
                wait = backoff * (2 ** attempt) + random.uniform(0, 1)
                log.warning(f"Rate-limited {r.status_code} – waiting {wait:.1f}s")
                time.sleep(wait)
            else:
                return None
        except Exception:
            pass
        time.sleep(backoff * (attempt + 1))
    return None

def http_post(url: str, body: Dict, timeout: int = 15) -> Optional[requests.Response]:
    try:
        return requests.post(url, json=body, timeout=timeout,
                             headers={"User-Agent": random.choice(USER_AGENTS)})
    except Exception:
        return None

# ---------- SQLITE (unchanged) ----------
_DB_LOCK = threading.Lock()

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    tables = [
        """CREATE TABLE IF NOT EXISTS seen_domains (
            domain TEXT PRIMARY KEY, first_seen TEXT,
            final_score INTEGER, monetization_path TEXT)""",
        """CREATE TABLE IF NOT EXISTS blacklist (
            domain TEXT PRIMARY KEY, reason TEXT, ts TEXT)""",
        """CREATE TABLE IF NOT EXISTS trend_cache (
            keyword TEXT PRIMARY KEY, trend_pct REAL, velocity REAL,
            source_count INTEGER, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS sentiment_cache (
            keyword TEXT PRIMARY KEY, compound REAL, positive REAL,
            negative REAL, headline_count INTEGER, top_headlines TEXT,
            fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS keyword_signals (
            keyword TEXT PRIMARY KEY, hn_mentions INTEGER,
            reddit_score INTEGER, news_volume INTEGER,
            coingecko_rank INTEGER, combined_signal REAL, updated_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS comps_cache (
            keyword TEXT PRIMARY KEY, sales_json TEXT,
            median_sale REAL, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS run_stats (
            run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT,
            domains_scanned INTEGER, pearls_found INTEGER,
            top_domain TEXT, top_score INTEGER)""",
        """CREATE TABLE IF NOT EXISTS seo_cache (
            keyword TEXT PRIMARY KEY, cpc REAL, search_vol_proxy REAL,
            serp_competition REAL, intent_class TEXT,
            seo_score REAL, fetched_at TEXT, expires_at TEXT)""",
    ]
    for ddl in tables:
        c.execute(ddl)
    conn.commit()
    return conn

def db_write(conn, sql: str, params: tuple):
    with _DB_LOCK:
        try:
            conn.execute(sql, params)
            conn.commit()
        except Exception as e:
            log.debug(f"DB write error: {e}")

def is_seen(conn, d: str) -> bool:
    return conn.execute("SELECT 1 FROM seen_domains WHERE domain=?", (d,)).fetchone() is not None

def is_blacklisted(conn, d: str) -> bool:
    return conn.execute("SELECT 1 FROM blacklist WHERE domain=?", (d,)).fetchone() is not None

def mark_seen(conn, d: str, score: int, path: str):
    db_write(conn, "INSERT OR REPLACE INTO seen_domains VALUES(?,?,?,?)",
             (d, datetime.utcnow().isoformat(), score, path))

def add_blacklist(conn, d: str, reason: str):
    db_write(conn, "INSERT OR IGNORE INTO blacklist VALUES(?,?,?)",
             (d, reason, datetime.utcnow().isoformat()))

def get_cached(conn, table: str, key: str) -> Optional[Dict]:
    col_map = {
        "trend_cache": ("trend_pct","velocity","source_count","expires_at"),
        "sentiment_cache": ("compound","positive","negative","headline_count","top_headlines","expires_at"),
        "comps_cache": ("sales_json","median_sale","expires_at"),
        "seo_cache": ("cpc","search_vol_proxy","serp_competition","intent_class","seo_score","expires_at"),
    }
    cols = col_map.get(table)
    if not cols:
        return None
    row = conn.execute(f"SELECT {','.join(cols)} FROM {table} WHERE keyword=?", (key,)).fetchone()
    if row and row[-1] > datetime.utcnow().isoformat():
        return dict(zip(cols[:-1], row[:-1]))
    return None

def put_cached(conn, table: str, key: str, data: Dict, ttl_hours: int = 6):
    expires = (datetime.utcnow() + timedelta(hours=ttl_hours)).isoformat()
    cols = list(data.keys()) + ["fetched_at", "expires_at"]
    vals = list(data.values()) + [datetime.utcnow().isoformat(), expires]
    placeholders = ",".join(["?"] * len(vals))
    db_write(conn, f"INSERT OR REPLACE INTO {table}(keyword,{','.join(cols)}) VALUES(?,{placeholders})",
             (key,) + tuple(vals))

# ---------- PROBABILITY ENGINE (unchanged) ----------
class ProbabilityEngine:
    LOGISTIC_A = 0.08
    LOGISTIC_B = -4.5
    NICHE_VOL = {"ai":0.65,"llm":0.72,"crypto":0.85,"fintech":0.58,
                 "insurance":0.42,"lawyer":0.45,"general":0.60}

    @staticmethod
    def sigmoid(x): return 1.0 / (1.0 + math.exp(-x))

    def p_flip_success(self, final_score: int, niche: str, age: int, backlinks: int) -> float:
        z = (self.LOGISTIC_A * final_score + self.LOGISTIC_B
             + 0.05 * min(age, 15) + 0.003 * min(backlinks, 200))
        base_p = self.sigmoid(z)
        boost = {"ai":0.06,"llm":0.07,"fintech":0.04,"insurance":0.05,"lawyer":0.05}.get(niche,0.0)
        return round(min(0.95, max(0.02, base_p + boost)), 4)

    def monte_carlo_flip_value(self, base_estimate: float, niche: str,
                               n_sims: int = 10000) -> Dict:
        if base_estimate <= 0:
            return {"mean":0,"p10":0,"p50":0,"p90":0,"p95":0,"std":0,"ci95":"$0–$0"}
        sigma = self.NICHE_VOL.get(niche, 0.60)
        mu = math.log(base_estimate) - 0.5 * sigma**2
        if NUMPY_OK:
            samples = np.random.lognormal(mean=mu, sigma=sigma, size=n_sims)
            p10, p50, p90, p95 = np.percentile(samples, [10,50,90,95])
            mean, std = np.mean(samples), np.std(samples)
        else:
            # Box-Müller fallback
            samples = []
            for _ in range(n_sims):
                u1,u2 = random.random(), random.random()
                z0 = math.sqrt(-2*math.log(u1))*math.cos(2*math.pi*u2)
                samples.append(math.exp(mu + sigma * z0))
            samples.sort()
            p10 = samples[int(0.10*n_sims)]
            p50 = samples[int(0.50*n_sims)]
            p90 = samples[int(0.90*n_sims)]
            p95 = samples[int(0.95*n_sims)]
            mean = sum(samples)/n_sims
            std = math.sqrt(sum((s-mean)**2 for s in samples)/n_sims)
        return {
            "mean": round(mean,0), "p10": round(p10,0), "p50": round(p50,0),
            "p90": round(p90,0), "p95": round(p95,0), "std": round(std,0),
            "ci95": f"${p10:,.0f}–${p95:,.0f}",
        }

    def kelly_fraction(self, p_win: float, b: float) -> float:
        if b <= 0 or p_win <= 0: return 0.0
        f = (b * p_win - (1-p_win)) / b
        return round(max(0.0, min(0.25, f)), 4)

    def kelly_allocation(self, p_win: float, base_estimate: float,
                         cost: float = 10.0) -> Dict:
        if cost <= 0 or base_estimate <= cost:
            return {"f_star":0.0,"allocation_usd":0.0,"payoff_ratio":0.0,
                    "expected_value":0.0,"verdict":"Skip"}
        b = (base_estimate - cost) / cost
        f = self.kelly_fraction(p_win, b)
        alloc = round(f * KELLY_BANKROLL, 2)
        ev = round(p_win * (base_estimate - cost) - (1-p_win) * cost, 2)
        verdict = "Strong Buy" if f > 0.10 else "Buy" if f > 0.04 else "Small" if f > 0.01 else "Pass"
        return {"f_star": f, "allocation_usd": alloc, "payoff_ratio": round(b,2),
                "expected_value": ev, "verdict": verdict}

# ---------- COMBINATORICS ENGINE (unchanged) ----------
class CombinatoricsEngine:
    POWER_PREFIXES = {"get":1.3,"buy":1.5,"hire":1.4,"find":1.2,"top":1.3,
                      "best":1.35,"pro":1.2,"my":1.1,"fast":1.2,"smart":1.15}
    POWER_SUFFIXES = {"pro":1.3,"hub":1.2,"ai":1.5,"io":1.2,"lab":1.15,"hq":1.1,
                      "app":1.2,"ly":1.05,"base":1.1,"now":1.2,"fast":1.2,"easy":1.15}
    NICHE_TLDS = {"crypto":[".io",".finance",".com"], "ai":[".ai",".io",".com"],
                  "fintech":[".finance",".io",".com"], "saas":[".io",".app",".com"],
                  "health":[".com",".io"], "default":[".com",".io",".co"]}

    def _niche_tlds(self, niche: str) -> List[str]:
        for k in self.NICHE_TLDS:
            if k in niche:
                return self.NICHE_TLDS[k]
        return self.NICHE_TLDS["default"]

    def score_combination(self, parts: List[str], tld: str) -> float:
        sld = "".join(parts)
        if len(sld) < 3 or len(sld) > 20: return 0.0
        niche_kw = None
        niche_s = NICHE_SCORE.get("general",30)
        cpc_val = NICHE_CPC.get("general",0.5)
        for p in parts:
            if p in NICHE_SCORE:
                niche_kw = p
                niche_s = NICHE_SCORE[p]
                cpc_val = NICHE_CPC.get(p,0.5)
                break
        if niche_kw is None: return 0.0
        prefix_mult = 1.0
        suffix_mult = 1.0
        for p in parts:
            if p != niche_kw:
                if p in self.POWER_PREFIXES: prefix_mult = max(prefix_mult, self.POWER_PREFIXES[p])
                if p in self.POWER_SUFFIXES: suffix_mult = max(suffix_mult, self.POWER_SUFFIXES[p])
        tld_v = TLD_VALUE.get(tld, DEFAULT_TLD)
        tld_mult = tld_v / 100.0
        n = len(sld)
        length_factor = max(0.3, 1.0 - max(0, n-8) * 0.05)
        vowels = sum(1 for c in sld if c in "aeiou")
        pronounce = 1.1 if 0.25 <= vowels/max(1,n) <= 0.5 else 1.0
        score = (niche_s * 0.40 + cpc_val * 0.40 + tld_v * 0.20) * prefix_mult * suffix_mult * tld_mult * length_factor * pronounce
        return round(score, 3)

    def generate_candidates(self, keywords: List[Dict], top_n: int = 200) -> List[Tuple[str, str, float]]:
        log.info("CombinatoricsEngine: generating candidate domains from trending keywords...")
        results = []
        seen = set()
        for kw_data in sorted(keywords, key=lambda x: x.get("combined_signal",0)+x.get("trend_pct",0)*0.3, reverse=True)[:25]:
            kw = kw_data["keyword"].lower().strip()
            if len(kw) < 3 or len(kw) > 16: continue
            niche = next((n for n in self.NICHE_TLDS if n in kw), "default")
            tlds = self._niche_tlds(niche)
            for pref in self.POWER_PREFIXES:
                for suf in self.POWER_SUFFIXES:
                    for tld in tlds:
                        for parts in [[pref,kw,suf], [pref,kw], [kw,suf], [kw]]:
                            sld = "".join(parts)
                            if 4 <= len(sld) <= 18:
                                dom = f"{sld}{tld}"
                                if dom not in seen:
                                    seen.add(dom)
                                    cps = self.score_combination(parts, tld)
                                    if cps > 0:
                                        results.append((dom, f"combo:{kw}", cps))
        results.sort(key=lambda x: x[2], reverse=True)
        log.info(f"CombinatoricsEngine: generated {len(results)} candidates")
        return results[:top_n]

# ---------- SEO INTELLIGENCE (unchanged) ----------
class SEOIntelligence:
    COMMERCIAL = {"buy","hire","get","find","service","agency","pro","company","firm","local"}
    INFORMATIONAL = {"how","what","why","guide","tips","best","top","review"}

    def __init__(self, conn):
        self.conn = conn

    def _search_vol_proxy(self, keyword: str) -> float:
        cached = get_cached(self.conn, "trend_cache", keyword)
        if cached:
            return min(100, max(0, 50 + cached.get("trend_pct",0)*0.5))
        return 50.0

    def _serp_competition(self, cc_hits: int, backlinks: int) -> float:
        comp = min(100, cc_hits * 5 + backlinks * 0.5)
        return round(100 - comp, 1)

    def _commercial_intent(self, domain: str) -> Tuple[float, str]:
        sld = domain.split(".")[0].lower()
        tokens = re.findall(r"[a-z]{3,}", sld)
        comm = sum(1 for t in tokens if t in self.COMMERCIAL)
        info = sum(1 for t in tokens if t in self.INFORMATIONAL)
        niche_match = any(t in NICHE_SCORE for t in tokens)
        if comm >= 2 or (comm >=1 and niche_match): return 90.0, "transactional"
        if comm >=1: return 72.0, "commercial"
        if info >=1 and niche_match: return 55.0, "informational"
        if niche_match: return 50.0, "commercial"
        return 30.0, "navigational"

    def _eeat_proxy(self, age: int, backlinks: int, tld: str) -> float:
        tld_trust = {".com":1.0,".org":1.0,".gov":1.2,".edu":1.2,".io":0.85,".ai":0.88}.get(tld,0.6)
        age_score = min(100, age * 8)
        link_score = min(100, backlinks * 2)
        return round((age_score * 0.4 + link_score * 0.4 + tld_trust * 20), 1)

    def _topical_authority(self, domain: str, niche: str) -> float:
        sld = domain.split(".")[0].lower()
        if niche != "general" and niche in sld:
            extra = len(re.findall(r"[a-z]{3,}", sld)) - 1
            return max(60, 100 - extra * 10)
        return 45.0 if niche != "general" else 25.0

    def _featured_snippet_opp(self, domain: str, cc_hits: int, intent: str) -> float:
        if intent == "informational" and cc_hits < 3: return 80.0
        if intent in ("commercial","transactional") and cc_hits < 5: return 60.0
        return 30.0

    def analyze(self, domain: str, niche: str, age: int, backlinks: int, cc_hits: int, tld: str) -> Dict:
        keyword = domain.split(".")[0].lower()
        cached = get_cached(self.conn, "seo_cache", keyword)
        if cached:
            return cached
        vol = self._search_vol_proxy(keyword)
        serp = self._serp_competition(cc_hits, backlinks)
        intent_s, ic = self._commercial_intent(domain)
        eeat = self._eeat_proxy(age, backlinks, tld)
        topical = self._topical_authority(domain, niche)
        fs = self._featured_snippet_opp(domain, cc_hits, ic)
        cpc = NICHE_CPC.get(niche, NICHE_CPC.get("general",0.5))
        cpc_score = min(100, cpc * 1.8)
        seo_score = round(
            cpc_score   * 0.22 +
            intent_s    * 0.20 +
            vol         * 0.16 +
            eeat        * 0.16 +
            topical     * 0.12 +
            serp        * 0.08 +
            fs          * 0.06, 1)
        result = {
            "cpc": round(cpc,2), "search_vol_proxy": round(vol,1),
            "serp_competition": round(serp,1), "intent_class": ic,
            "seo_score": seo_score, "cpc_score": round(cpc_score,1),
            "intent_score": round(intent_s,1), "eeat_score": round(eeat,1),
            "topical_score": round(topical,1), "fs_opportunity": round(fs,1),
        }
        put_cached(self.conn, "seo_cache", keyword, result, ttl_hours=12)
        return result

# ---------- TREND RADAR (unchanged, but we added progress logs) ----------
class TrendRadar:
    STOP = {"the","and","for","this","that","with","from","are","has","was","not","but","its","you","can","will","new","how","why","all","one","what","have","they","some","more","use","get","out","via","top","over","been","were","just","now","your","who","our","may","into","than","says","said","also","about","their","would","could","year","time","first","make","like","other"}
    def __init__(self, conn): self.conn = conn

    def _google_trend(self, kw: str) -> Tuple[float, float]:
        if not PYTRENDS_OK: return 0.0,0.0
        kw = re.sub(r"\.[a-z]{2,}$","",kw).replace("-"," ").strip()
        if len(kw)<2: return 0.0,0.0
        for attempt in range(3):
            try:
                pt = TrendReq(hl="en-US", tz=330, timeout=(10,25), retries=2, backoff_factor=0.5)
                pt.build_payload([kw], timeframe="today 6-m")
                df = pt.interest_over_time()
                if df.empty or kw not in df.columns: return 0.0,0.0
                s = df[kw].values.astype(float)
                half = len(s)//2
                old = s[:half].mean()
                new = s[-half:].mean()
                pct = 0.0 if old==0 else round((new-old)/old*100,1)
                vel = 0.0
                if NUMPY_OK and len(s)>=8:
                    x = np.arange(8,dtype=float)
                    vel = round(float(np.polyfit(x,s[-8:],1)[0]) / max(1,s.mean())*100,2)
                return pct, vel
            except Exception:
                time.sleep(2**attempt + random.uniform(0,1))
        return 0.0,0.0

    def _hn_keywords(self) -> Counter:
        log.info("TrendRadar: fetching HackerNews...")
        cnt = Counter()
        ids = http_get(HN_TOP, timeout=10, json_resp=True)
        if not ids: return cnt
        for sid in ids[:50]:
            item = http_get(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json", timeout=8, json_resp=True)
            if item:
                title = item.get("title","").lower()
                for w in re.findall(r"\b[a-z]{3,}\b", title): cnt[w] += 1
            time.sleep(0.05)
        return cnt

    def _reddit_keywords(self) -> Counter:
        log.info("TrendRadar: fetching Reddit...")
        cnt = Counter()
        headers = {"User-Agent": "DomainSniperHybrid/1.0"}
        for sub in REDDIT_SUBS[:6]:
            try:
                r = requests.get(f"https://www.reddit.com/r/{sub}/hot.json?limit=25", headers=headers, timeout=12)
                if r.status_code != 200: continue
                for post in r.json().get("data",{}).get("children",[]):
                    title = post.get("data",{}).get("title","").lower()
                    score = max(1, post.get("data",{}).get("score",1))
                    w = max(1, int(math.log10(score)))
                    for word in re.findall(r"\b[a-z]{3,}\b", title): cnt[word] += w
                time.sleep(0.5)
            except Exception:
                pass
        return cnt

    def _rss_keywords(self) -> Counter:
        log.info("TrendRadar: fetching RSS feeds...")
        cnt = Counter()
        for _, url in NEWS_RSS_FEEDS.items():
            try:
                parsed = feedparser.parse(url)
                for e in parsed.entries[:20]:
                    title = getattr(e,"title","").lower()
                    for w in re.findall(r"\b[a-z]{4,}\b", title): cnt[w] += 1
            except Exception:
                pass
        return cnt

    def _crypto_trending(self) -> List[str]:
        log.info("TrendRadar: fetching CoinGecko trending...")
        data = http_get("https://api.coingecko.com/api/v3/search/trending", timeout=10, json_resp=True)
        if not data: return []
        return [c["item"]["symbol"].lower() for c in data.get("coins",[])[:7]]

    def get_trending_keywords(self, top_n: int = 30) -> List[Dict]:
        log.info("TrendRadar: starting multi-source scan...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            hn_fut = ex.submit(self._hn_keywords)
            reddit_fut = ex.submit(self._reddit_keywords)
            rss_fut = ex.submit(self._rss_keywords)
            crypto_fut = ex.submit(self._crypto_trending)
            hn = hn_fut.result()
            rd = reddit_fut.result()
            rss = rss_fut.result()
            crypto = crypto_fut.result()
        all_kws = {k for k in set(hn) | set(rd) | set(rss) if k not in self.STOP and len(k)>=4}
        combined = {}
        for kw in all_kws:
            s = hn.get(kw,0)*3.0 + rd.get(kw,0)*2.0 + rss.get(kw,0)*1.5
            if kw in crypto: s += 10
            if s > 0: combined[kw] = s
        top = sorted(combined, key=combined.get, reverse=True)[:top_n]
        log.info(f"TrendRadar: {len(top)} keywords extracted")
        results = []
        for kw in top:
            cached = get_cached(self.conn, "trend_cache", kw)
            if cached:
                tp, vel = cached["trend_pct"], cached["velocity"]
            else:
                tp, vel = self._google_trend(kw)
                put_cached(self.conn,"trend_cache",kw,{"trend_pct":tp,"velocity":vel,"source_count":3})
                time.sleep(random.uniform(0.3,0.8))
            results.append({"keyword":kw,"combined_signal":combined[kw],
                            "trend_pct":tp,"velocity":vel,
                            "in_hn":kw in hn,"in_reddit":kw in rd,
                            "in_rss":kw in rss,"in_crypto":kw in crypto})
        for r in results:
            db_write(self.conn,
                     "INSERT OR REPLACE INTO keyword_signals VALUES(?,?,?,?,?,?,?)",
                     (r["keyword"], int(r["in_hn"]), int(r["combined_signal"]),
                      int(r["in_rss"]), int(r["in_crypto"]),
                      r["combined_signal"], datetime.utcnow().isoformat()))
        return results

# ---------- SENTIMENT ENGINE (unchanged) ----------
class SentimentEngine:
    def __init__(self, conn):
        self.conn = conn
        self.vader = SentimentIntensityAnalyzer() if VADER_OK else None

    def _score(self, text):
        if not self.vader: return {"compound":0,"pos":0,"neg":0}
        return self.vader.polarity_scores(text)

    def _gnews(self, kw):
        try:
            p = feedparser.parse(f"https://news.google.com/rss/search?q={quote_plus(kw)}&hl=en-US&gl=US")
            return [getattr(e,"title","") for e in p.entries[:15]]
        except Exception:
            return []

    def _newsapi(self, kw):
        if not NEWS_API_KEY: return []
        data = http_get(f"https://newsapi.org/v2/everything?q={quote_plus(kw)}&sortBy=publishedAt&pageSize=10&language=en&apiKey={NEWS_API_KEY}",
                        timeout=10, json_resp=True)
        return [a.get("title","") for a in (data or {}).get("articles",[])[:10]]

    def analyze(self, keyword: str) -> Dict:
        cached = get_cached(self.conn, "sentiment_cache", keyword)
        if cached:
            cached["sentiment_score"] = round(50 + cached["compound"]*50,1)
            return cached
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            headlines = ex.submit(self._gnews, keyword).result() + ex.submit(self._newsapi, keyword).result()
        if not headlines:
            r = {"compound":0.0,"positive":0.0,"negative":0.0,
                 "headline_count":0,"top_headlines":"[]","sentiment_score":50.0}
            put_cached(self.conn,"sentiment_cache",keyword,r,ttl_hours=4)
            return r
        scores = [self._score(h) for h in headlines]
        c_avg = sum(s["compound"] for s in scores)/len(scores)
        p_avg = sum(s["pos"] for s in scores)/len(scores)
        n_avg = sum(s["neg"] for s in scores)/len(scores)
        top3 = json.dumps([h for h,_ in sorted(zip(headlines,scores), key=lambda x:abs(x[1]["compound"]), reverse=True)[:3]])
        r = {"compound":round(c_avg,4),"positive":round(p_avg,4),"negative":round(n_avg,4),
             "headline_count":len(headlines),"top_headlines":top3,"sentiment_score":round(50+c_avg*50,1)}
        put_cached(self.conn,"sentiment_cache",keyword,r,ttl_hours=4)
        return r

# ---------- COMPARABLE SALES (unchanged) ----------
class NameBioComps:
    def __init__(self, conn): self.conn = conn
    def fetch(self, keyword: str, niche: str) -> Dict:
        cached = get_cached(self.conn, "comps_cache", keyword)
        if cached: return cached
        sales = self._scrape_namebio(keyword)
        if not sales:
            mult = FLIP_MULTIPLES.get(niche, FLIP_MULTIPLES["general"])
            return {"sales_json":"[]","median_sale":0,"comp_count":0,
                    "niche_mult_mean":mult["mean"],"niche_mult_std":mult["std"]}
        median = sorted(sales)[len(sales)//2]
        result = {"sales_json":json.dumps(sales[:10]), "median_sale":float(median),
                  "comp_count":len(sales), "niche_mult_mean":FLIP_MULTIPLES.get(niche,FLIP_MULTIPLES["general"])["mean"],
                  "niche_mult_std":FLIP_MULTIPLES.get(niche,FLIP_MULTIPLES["general"])["std"]}
        put_cached(self.conn,"comps_cache",keyword,result,ttl_hours=24)
        return result
    def _scrape_namebio(self, keyword: str) -> List[float]:
        url = f"https://namebio.com/?s={quote_plus(keyword)}&tld=com&sort=price"
        resp = http_get(url, timeout=15)
        if not resp: return []
        prices = []
        soup = BeautifulSoup(resp.text, "html.parser")
        for el in soup.select(".price, [data-price], td.amount")[:20]:
            raw = re.sub(r"[^\d.]", "", el.text.strip())
            try:
                p = float(raw)
                if 100 < p < 1_000_000: prices.append(p)
            except Exception:
                pass
        return prices

# ---------- TRADEMARK GUARD (unchanged) ----------
class TrademarkGuard:
    @staticmethod
    def check(domain: str) -> Dict:
        if not ENABLE_TRADEMARK: return {"risk":"UNCHECKED","matches":0,"detail":"disabled"}
        sld = domain.split(".")[0].lower()
        url = f"https://tmsearch.uspto.gov/search/search?lang=en&query={quote_plus(sld)}&searchType=statusSearch&status=live&dateRangeField=regDate"
        resp = http_get(url, timeout=12)
        if not resp: return {"risk":"UNKNOWN","matches":0,"detail":"USPTO unreachable"}
        hits = len(re.findall(r'class="result"', resp.text, re.I))
        if hits == 0: return {"risk":"CLEAR","matches":0,"detail":"No USPTO match"}
        if hits <= 3: return {"risk":"CAUTION","matches":hits,"detail":"Check manually"}
        return {"risk":"RISK","matches":hits,"detail":"Active TM found"}

# ---------- DOMAIN DISCOVERY (with timeouts and logging) ----------
def fetch_domainsdb(limit=200) -> List[Tuple[str,str]]:
    log.info("Fetching DomainsDB...")
    data = http_get("https://api.domainsdb.info/v1/domains/search?domain=*.com&limit=200", timeout=20, json_resp=True)
    if not data: return []
    out = [(i.get("domain","").lower().strip(), "domainsdb") for i in data.get("domains",[])[:limit] if i.get("domain")]
    log.info(f"DomainsDB: {len(out)} domains")
    return out

def fetch_expireddomains_page1() -> List[Tuple[str,str]]:
    log.info("Fetching ExpiredDomains.net (page 1)...")
    domains = []
    try:
        resp = http_get("https://www.expireddomains.net/deleted-domains/?ftlds[]=com&fwhois=22", timeout=15)
        if resp:
            soup = BeautifulSoup(resp.text,"html.parser")
            for a in soup.select("td.field_domain a[href*='domain-name-search']")[:100]:
                d = a.text.strip().lower()
                if d and "." in d: domains.append((d,"expireddomains"))
    except Exception as e: log.debug(f"ExpiredDomains: {e}")
    log.info(f"ExpiredDomains: {len(domains)} domains")
    return domains

def fetch_dropcatch_feed() -> List[Tuple[str,str]]:
    log.info("Fetching DropCatch RSS feed...")
    domains = []
    try:
        p = feedparser.parse("https://www.dropcatch.com/rss/auctions")
        for e in p.entries[:100]:
            m = re.search(r"[a-z0-9\-]+\.[a-z]{2,}", getattr(e,"title","").lower())
            if m: domains.append((m.group(),"dropcatch"))
    except Exception as e: log.debug(f"DropCatch RSS: {e}")
    log.info(f"DropCatch: {len(domains)} domains")
    return domains

def generate_fallback_domains(limit=100) -> List[Tuple[str,str]]:
    log.info("Generating fallback domains (static keywords + TLDs)...")
    kws = list(NICHE_SCORE.keys())[:20]
    tlds = [".com",".io",".ai",".co"]
    out = [(f"{k}{t}","fallback") for k in kws for t in tlds] + [(f"{k}pro{t}","fallback") for k in kws for t in tlds]
    random.shuffle(out)
    log.info(f"Fallback domains generated: {len(out[:limit])}")
    return out[:limit]

# ---------- SCORING HELPERS (unchanged) ----------
def get_cc_index() -> str:
    data = http_get("https://index.commoncrawl.org/collinfo.json", timeout=10, json_resp=True)
    if data: return data[0].get("cdx-api","https://index.commoncrawl.org/CC-MAIN-2024-10-index")
    return "https://index.commoncrawl.org/CC-MAIN-2024-10-index"
CC_INDEX_URL = ""

def wayback_backlinks(domain):
    r = http_get(f"http://web.archive.org/cdx/search/cdx?url=*.{domain}&output=text&fl=urlkey&limit=500&collapse=urlkey", timeout=25)
    if not r: return 0
    refs = set()
    for line in r.text.splitlines():
        parts = line.strip().split("/")
        if parts: refs.add(parts[0].replace(")","").split(",")[-1])
    return len(refs)

def wayback_traffic_proxy(domain):
    r = http_get(f"http://web.archive.org/cdx/search/cdx?url={domain}&output=json&fl=timestamp&limit=200", timeout=20)
    if not r: return 0,0
    try:
        d = r.json()
        c = max(0, len(d)-1)
        return c, min(c*300, 50000)
    except: return 0,0

def commoncrawl_presence(domain):
    if not CC_INDEX_URL: return 0
    r = http_get(f"{CC_INDEX_URL}?url={domain}&output=json&limit=5", timeout=15)
    if not r: return 0
    cnt = 0
    for line in r.text.strip().splitlines():
        try: json.loads(line); cnt+=1
        except: pass
    return cnt

def domain_age(domain):
    try:
        w = whois.whois(domain)
        c = w.creation_date
        if isinstance(c,list): c = c[0]
        if c: return min(25, (datetime.now()-c).days//365)
    except: pass
    return 0

def detect_niche(domain):
    dl = domain.lower().replace("-","").replace(".","")
    best,bs = "general",30
    for kw,s in NICHE_SCORE.items():
        if kw in dl and s>bs: best,bs = kw,s
    return best,bs

def domain_length_score(domain):
    n = len(domain.split(".")[0])
    if n<=4: return 100
    if n<=6: return 90
    if n<=8: return 75
    if n<=10: return 58
    if n<=13: return 40
    return max(0,40-(n-13)*3)

def brandability(domain):
    sld = domain.split(".")[0].lower()
    s = 50
    if re.search(r"\d", sld): s -= 20
    if "-" in sld: s -= 20
    if len(sld)>12: s -= 15
    if len(sld)<3: s -= 10
    if re.search(r"[aeiou]{2,}", sld): s += 10
    if 4<=len(sld)<=7: s += 22
    if sld==sld[::-1] and len(sld)>2: s += 5
    vr = sum(1 for c in sld if c in "aeiou")/max(1,len(sld))
    if 0.2<=vr<=0.5: s += 8
    return max(0,min(100,s))

def spam_check(domain):
    r = http_get(f"http://web.archive.org/cdx/search/cdx?url={domain}&output=json&fl=timestamp&limit=50", timeout=15)
    if not r: return 0,85
    try:
        snaps = r.json()
        if len(snaps)<2: return 0,85
        sample = random.sample(snaps[1:], min(3,len(snaps)-1))
        pat = re.compile(r"viagra|cialis|casino|poker|adult|xxx|pharma|pills|escort|gambling", re.I)
        hits = 0
        for ts in sample:
            rr = http_get(f"http://web.archive.org/web/{ts[0]}/{domain}", timeout=10, retries=1)
            if rr and pat.search(rr.text): hits += 1
            time.sleep(0.3)
        return hits, max(0,100-hits*35)
    except: return 0,85

def check_safe_browsing(domain):
    if not SAFE_BROWSING_KEY: return 1
    body = {"client":{"clientId":"dsHybrid","clientVersion":"1.0"},
            "threatInfo":{"threatTypes":["MALWARE","SOCIAL_ENGINEERING","UNWANTED_SOFTWARE"],
                          "platformTypes":["ANY_PLATFORM"],"threatEntryTypes":["URL"],
                          "threatEntries":[{"url":f"http://{domain}"}]}}
    try:
        r = http_post(f"https://safebrowsing.googleapis.com/v4/threatMatches:find?key={SAFE_BROWSING_KEY}", body)
        return 0 if (r and r.json().get("matches")) else 1
    except: return 1

def foundation_score(bl,cc,age,tld):
    return min(100,bl/3)*0.32 + min(100,cc*20)*0.26 + min(100,age*5)*0.24 + TLD_VALUE.get(tld,DEFAULT_TLD)*0.18

def flip_score_fn(domain,cpm,age,tld):
    ls = domain_length_score(domain)
    bs = brandability(domain)
    ts = TLD_VALUE.get(tld,DEFAULT_TLD)
    a_s = min(100,age*8) if age<5 else (100 if age<=15 else max(40,100-(age-15)*2))
    return cpm*0.28 + ls*0.27 + bs*0.22 + ts*0.13 + a_s*0.10

def sentiment_score_fn(compound, headline_count, velocity, sources):
    base = 50 + compound*50
    cov = min(15, headline_count*0.5) if compound>0 else max(-15, -headline_count*0.5)
    vel = max(-10, min(10, velocity*0.3))
    src = min(5, sources*1.5) if compound>=0 else 0
    return max(0, min(100, base+cov+vel+src))

def momentum_score_fn(tp): return max(0, min(100, 50+tp/2))

def trend_velocity_label(v):
    if v>15: return "🚀 Rocket"
    if v>5: return "📈 Rising"
    if v>-5: return "➡️ Stable"
    if v>-15: return "📉 Fading"
    return "⬇️ Falling"

def get_weights(age, niche):
    if age==0: return {"foundation":0.16,"flip":0.22,"history":0.12,"seo":0.18,"sentiment":0.14,"momentum":0.12,"monetization":0.06}
    if age<3:  return {"foundation":0.18,"flip":0.22,"history":0.14,"seo":0.16,"sentiment":0.14,"momentum":0.10,"monetization":0.06}
    if age<10: return {"foundation":0.20,"flip":0.20,"history":0.14,"seo":0.16,"sentiment":0.12,"momentum":0.10,"monetization":0.08}
    return {"foundation":0.24,"flip":0.20,"history":0.18,"seo":0.16,"sentiment":0.08,"momentum":0.08,"monetization":0.06}

def detect_monetization_paths(domain,niche,age,bl,traffic,cc,trend_pct,compound,comps_median):
    paths = []
    sm = 1.0 + max(-0.2, min(0.4, compound*0.4))
    if comps_median > 0:
        flip_est = int(comps_median * sm)
    elif bl > 20 and age >= 3:
        flip_est = int((bl * age * 2.5 + age * 40) * sm)
    elif age >= 5:
        flip_est = int(age * 35 * sm)
    else: flip_est = 0
    if flip_est > 0: paths.append(("flip",0,flip_est,f"Sedo/Dan/Afternic – comps ${comps_median:,.0f}"))
    cpm = PARKING_CPM.get(niche, PARKING_CPM.get("general",3))
    if traffic>50: paths.append(("parking", (traffic/1000)*cpm, 0, f"Park Bodis ~${(traffic/1000)*cpm:.0f}/mo"))
    if niche in ["ai","saas","crypto","fintech","health","llm"] and age>=2 and cc>0: paths.append(("content_site",5,0,"5-page niche site + AdSense"))
    lv = LEAD_VALUE.get(niche,0)
    if lv>0 and age>=1: paths.append(("lead_gen", max(1,traffic//100)*lv, 0, f"Lead-gen {niche}"))
    if niche in ["crypto","fintech","saas","insurance"] and traffic>20: paths.append(("affiliate", (traffic/1000)*15, 0, f"Affiliate redirect"))
    if not paths: paths.append(("hold_and_list",0,max(50,flip_est),"List and hold"))
    paths.sort(key=lambda x: x[1]+x[2]*0.1, reverse=True)
    return paths[0][0], paths[1][0] if len(paths)>1 else paths[0][0], paths[0][1], flip_est, paths[0][3]

def monetization_score_fn(me): return min(100,me) if me>0 else 0

def build_affiliate_links(domain):
    links = {}
    if AFFILIATE_ID_GD: links["godaddy"] = f"https://www.godaddy.com/domainsearch/find?domainToCheck={domain}&isc={AFFILIATE_ID_GD}"
    if AFFILIATE_ID_NC: links["namecheap"] = f"https://www.namecheap.com/domains/registration/results/?domain={domain}&AffiliateCode={AFFILIATE_ID_NC}"
    links["dan"] = f"https://dan.com/buy-domain/{domain}"
    links["sedo"] = f"https://sedo.com/search/details/?domain={domain}"
    links["afternic"] = f"https://www.afternic.com/domain/{domain}"
    return links

_prob_engine = ProbabilityEngine()
_combo_engine = CombinatoricsEngine()

def process_domain(domain:str, source:str, conn, seo:SEOIntelligence,
                   sent:SentimentEngine, comps:NameBioComps,
                   tm:TrademarkGuard) -> Optional[Dict]:
    tld = "." + domain.split(".")[-1]
    sld = domain.split(".")[0]
    if TLD_VALUE.get(tld,DEFAULT_TLD) < 20: return None
    if not (2<=len(sld)<=22): return None
    if re.search(r"\d{4,}",sld): return None

    bl = wayback_backlinks(domain)
    snaps, traffic = wayback_traffic_proxy(domain)
    cc = commoncrawl_presence(domain)
    age = domain_age(domain)
    niche, _ = detect_niche(domain)
    spam_flags, hist_s = spam_check(domain)
    safe = check_safe_browsing(domain)

    kw = re.sub(r"\.[a-z]{2,}$","",sld).replace("-"," ").strip()
    cached_t = get_cached(conn,"trend_cache",kw)
    trend_pct = cached_t["trend_pct"] if cached_t else 0.0
    velocity = cached_t["velocity"] if cached_t else 0.0

    sent_data = sent.analyze(kw)
    compound = sent_data.get("compound",0.0)
    hn_count = sent_data.get("headline_count",0)

    seo_data = seo.analyze(domain, niche, age, bl, cc, tld)
    seo_score = seo_data.get("seo_score",50.0)

    comp_data = comps.fetch(kw, niche)
    comp_med = comp_data.get("median_sale",0)

    tm_risk = tm.check(domain)

    if not safe: hist_s = min(hist_s,20)
    if spam_flags>=3 and hist_s<30:
        add_blacklist(conn, domain, f"spam={spam_flags},hist={hist_s}")
        return None
    if tm_risk["risk"] == "RISK":
        add_blacklist(conn, domain, f"trademark={tm_risk['matches']}")
        return None

    W = get_weights(age,niche)
    found = foundation_score(bl,cc,age,tld)
    flip = flip_score_fn(domain, NICHE_SCORE.get(niche,30), age, tld)
    senti = sentiment_score_fn(compound, hn_count, velocity, 3)
    mom = momentum_score_fn(trend_pct)
    pri,sec,me,flip_est,mon_note = detect_monetization_paths(
        domain,niche,age,bl,traffic,cc,trend_pct,compound,comp_med)
    mon_s = monetization_score_fn(me)

    final = int(
        found*W["foundation"] + flip*W["flip"] + hist_s*W["history"] +
        seo_score*W["seo"] + senti*W["sentiment"] + mom*W["momentum"] + mon_s*W["monetization"]
    )

    p_win = _prob_engine.p_flip_success(final, niche, age, bl)
    mc = _prob_engine.monte_carlo_flip_value(max(flip_est,me), niche)
    kelly = _prob_engine.kelly_allocation(p_win, mc["p50"])
    aff = build_affiliate_links(domain)
    cps = _combo_engine.score_combination([sld], tld)
best_combo = ""    # not used, but keep the variable

    return {
        "fetched_at": datetime.utcnow().isoformat(),
        "domain": domain, "source": source, "tld": tld, "sld_length": len(sld),
        "final_score": final,
        "foundation": round(found,1), "flip_score": round(flip,1),
        "history_score": round(hist_s,1), "seo_score": round(seo_score,1),
        "sentiment_score": round(senti,1), "momentum_score": round(mom,1),
        "monetization_score": round(mon_s,1),
        "seo_intent_class": seo_data.get("intent_class","?"),
        "seo_cpc_usd": seo_data.get("cpc",0),
        "age_years": age, "backlinks_proxy": bl,
        "wayback_snapshots": snaps, "est_monthly_traffic": traffic,
        "commoncrawl_hits": cc,
        "niche": niche, "niche_cpc": NICHE_CPC.get(niche,0.5),
        "trend_6m_pct": trend_pct, "trend_velocity": velocity,
        "velocity_label": trend_velocity_label(velocity),
        "sentiment_compound": round(compound,4),
        "sentiment_headline_n": hn_count,
        "top_headlines": sent_data.get("top_headlines","[]"),
        "spam_flags": spam_flags, "safe_browsing_clean": safe,
        "trademark_risk": tm_risk["risk"],
        "combinatoric_cps": round(cps,2), "best_combo": best_combo,
        "comp_median_usd": comp_med, "comp_count": comp_data.get("comp_count",0),
        "p_flip_success": p_win,
        "mc_mean": mc["mean"], "mc_p10": mc["p10"], "mc_p50": mc["p50"],
        "mc_p90": mc["p90"], "mc_ci95": mc["ci95"],
        "kelly_fraction": kelly["f_star"], "kelly_alloc_usd": kelly["allocation_usd"],
        "kelly_verdict": kelly["verdict"], "payoff_ratio": kelly["payoff_ratio"],
        "expected_value": kelly["expected_value"],
        "primary_path": pri, "secondary_path": sec,
        "est_monthly_usd": round(me,2), "flip_estimate_usd": flip_est,
        "flip_range": f"${flip_est*.8:,.0f}–${flip_est*1.2:,.0f}" if flip_est else "TBD",
        "monetization_note": mon_note,
        "link_sedo": aff.get("sedo",""), "link_dan": aff.get("dan",""),
        "link_afternic": aff.get("afternic",""), "link_godaddy_aff": aff.get("godaddy",""),
        "link_namecheap_aff": aff.get("namecheap",""),
        "weights_used": json.dumps(W),
    }

def process_domain_safe(args):
    domain,src,conn,seo,sent,comps,tm = args
    try: return process_domain(domain,src,conn,seo,sent,comps,tm)
    except Exception as e: log.error(f"Error {domain}: {e}"); return None

def generate_html_report(df: pd.DataFrame, run_id: str) -> str:
    top = df.head(20)
    rows = ""
    for _,r in top.iterrows():
        sc = r["final_score"]
        bg = "#dcfce7" if sc>=75 else "#fef9c3" if sc>=60 else "#fee2e2"
        sent_col = "#16a34a" if r["sentiment_compound"]>0.1 else "#dc2626" if r["sentiment_compound"]<-0.1 else "#64748b"
        kelly_col = "#16a34a" if r["kelly_verdict"]=="Strong Buy" else "#ca8a04" if r["kelly_verdict"]=="Buy" else "#64748b"
        rows += f"""
        <tr style="background:{bg}">
          <td><b><a href="https://{r['domain']}" target="_blank">{r['domain']}</a></b><br><small>{r['source']}</small></td>
          <td align="center"><b>{sc}</b></td>
          <td>{r['niche']} <br> <small>{r['seo_intent_class']}</small></td>
          <td align="center"><span style="color:{sent_col}">{r['sentiment_compound']:+.2f}</span><br>{r['velocity_label']}</td>
          <td align="center">{r['p_flip_success']:.0%} </td>
          <td align="center">{r['mc_ci95']}</td>
          <td align="center"><b style="color:{kelly_col}">{r['kelly_verdict']}</b><br>${r['kelly_alloc_usd']:,.0f}</td>
          <td>{r['primary_path'].replace('_',' ').title()}</td>
          <td><a href="{r['link_sedo']}">Sedo</a> <a href="{r['link_dan']}">Dan</a> <a href="{r['link_afternic']}">Afternic</a></td>
        </tr>"""
    html = f"""
    <html><head><meta charset="utf-8"><title>Domain Sniper Hybrid – {run_id}</title>
    <style>body{{font-family:Arial;max-width:1200px;margin:auto;padding:20px;color:#1e293b}}
    table{{border-collapse:collapse;width:100%;font-size:12px}}
    th{{background:#1e1b4b;color:white;padding:6px}} td{{padding:5px;border:1px solid #e2e8f0}}
    </style></head><body>
    <h1>🏴‍☠️ Domain Sniper Hybrid – Institutional Report</h1>
    <p><b>Run:</b> {run_id} | <b>Date:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} | <b>Scored:</b> {len(df)}</p>
    <tr><tr><th>Domain</th><th>Score</th><th>Niche/Intent</th><th>Sentiment/Vel</th><th>P(Flip)</th><th>MC 95% CI</th><th>Kelly/$</th><th>Path</th><th>Links</th></tr>
    {rows}
    </table><p style="font-size:10px;color:#94a3b8">Not financial advice. DYOR.</p></body></html>"""
    path = f"report_hybrid_{run_id}.html"
    with open(path,"w",encoding="utf-8") as f: f.write(html)
    log.info(f"HTML report: {path}")
    return path

def push_to_sheets(df):
    if not GSPREAD_OK or not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID: return
    try:
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON),
                      scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        try: ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound: ws = sh.add_worksheet(SHEET_NAME,2000,40)
        vals = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        ws.update(vals, value_input_option="RAW") if not ws.get_all_values() else ws.append_rows(vals[1:])
        log.info(f"Sheets: {len(df)} rows")
    except Exception as e: log.error(f"Sheets error: {e}")

def send_telegram(d):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    emoji={"flip":"💸","parking":"🅿️","lead_gen":"🎯","affiliate":"🔗","content_site":"📝","hold_and_list":"⏳"}
    se="🟢" if d["sentiment_compound"]>0.1 else "🔴" if d["sentiment_compound"]<-0.1 else "⚪"
    ke={"Strong Buy":"🔥","Buy":"✅","Small":"🔹"}.get(d["kelly_verdict"],"⬜")
    msg = (f"🏆 *PEARL* {d['final_score']}/100\n🌐 *{d['domain']}*\n"
           f"Foundation:{d['foundation']:.0f} Flip:{d['flip_score']:.0f} SEO:{d['seo_score']:.0f}\n"
           f"{se} Sentiment:{d['sentiment_score']:.0f} Vel:{d['velocity_label']}\n"
           f"Niche:{d['niche']} Age:{d['age_years']}y CPC:${d['seo_cpc_usd']:.2f}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"📊 P(flip): {d['p_flip_success']:.0%} MC 95% CI: {d['mc_ci95']}\n"
           f"{ke} Kelly: {d['kelly_verdict']} — ${d['kelly_alloc_usd']:,.0f}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"{emoji.get(d['primary_path'],'💡')} {d['primary_path'].replace('_',' ').title()}\n"
           f"Monthly:${d['est_monthly_usd']:.0f} Flip:{d['flip_range']}\n"
           f"[Sedo]({d['link_sedo']}) [Dan]({d['link_dan']}) [Afternic]({d['link_afternic']})")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for i in range(0,len(msg),4000):
        try: requests.post(url, json={"chat_id":TELEGRAM_CHAT_ID,"text":msg[i:i+4000],"parse_mode":"Markdown","disable_web_page_preview":True}, timeout=10)
        except: pass

def send_email_digest(results):
    if not EMAIL_DIGEST_TO or not GMAIL_USER or not GMAIL_APP_PASS or not results: return
    top = sorted(results, key=lambda x: x["final_score"], reverse=True)[:12]
    rows = ""
    for d in top:
        kc = "#16a34a" if d["kelly_verdict"]=="Strong Buy" else "#ca8a04" if d["kelly_verdict"]=="Buy" else "#64748b"
        rows += f"<tr><td><b>{d['domain']}</b><br><small>{d['source']}</small></td><td align='center'><b>{d['final_score']}</b></td><td>{d['niche']}</td><td align='center'>{d['p_flip_success']:.0%}</td><td align='center'>{d['mc_ci95']}</td><td align='center'><b style='color:{kc}'>{d['kelly_verdict']}</b><br>${d['kelly_alloc_usd']:,.0f}</td><td align='center'><a href='{d['link_sedo']}'>Sedo</a></td></tr>"
    html = (f"<html><body><h2>Domain Sniper Hybrid – Daily Digest</h2><p>{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} | {len(results)} scored</p>"
            f"<table border='1' cellpadding='5'><tr><th>Domain</th><th>Score</th><th>Niche</th><th>P(Flip)</th><th>MC CI95</th><th>Kelly/$</th><th>Buy</th></tr>{rows}</table>"
            f"<p>Not advice. DYOR.</p></body></html>")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Domain Sniper Hybrid – {len(top)} Pearls ({datetime.utcnow().strftime('%Y-%m-%d')})"
    msg["From"] = GMAIL_USER
    msg["To"] = EMAIL_DIGEST_TO
    msg.attach(MIMEText(html,"html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(GMAIL_USER, GMAIL_APP_PASS)
            s.sendmail(GMAIL_USER, EMAIL_DIGEST_TO, msg.as_string())
        log.info("Email digest sent")
    except Exception as e: log.error(f"Email error: {e}")

def main():
    global CC_INDEX_URL
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info("═"*70)
    log.info("  Domain Fortress Sniper – Hybrid Edition (with progress logs)")
    log.info("═"*70)
    log.info(f"Run: {run_id}")
    log.info(f"VADER: {'✓' if VADER_OK else '✗'}")
    log.info(f"pytrends: {'✓' if PYTRENDS_OK else '✗'}")
    log.info(f"numpy: {'✓' if NUMPY_OK else '✗'}")
    log.info(f"gspread: {'✓' if GSPREAD_OK else '✗'}")
    log.info(f"Trademark: {'ON' if ENABLE_TRADEMARK else 'OFF'}")
    log.info(f"Bankroll: ${KELLY_BANKROLL:,.0f}")
    log.info("═"*70)

    conn = init_db()
    CC_INDEX_URL = get_cc_index()
    log.info(f"CommonCrawl index: {CC_INDEX_URL}")

    seo_engine = SEOIntelligence(conn)
    sent_engine = SentimentEngine(conn)
    comps_engine = NameBioComps(conn)
    tm_guard = TrademarkGuard()

    radar = TrendRadar(conn)
    if CUSTOM_KEYWORDS:
        trending = [{"keyword":k,"combined_signal":100,"trend_pct":0,"velocity":0,
                     "in_hn":False,"in_reddit":False,"in_rss":False,"in_crypto":False}
                    for k in CUSTOM_KEYWORDS]
        log.info(f"Custom keywords: {CUSTOM_KEYWORDS}")
    else:
        trending = radar.get_trending_keywords(top_n=30)
    log.info(f"Top 5 trending: {[k['keyword'] for k in trending[:5]]}")

    # Combinatorics engine
    log.info("Step: Generating combinatorics candidates...")
    combo_candidates = _combo_engine.generate_candidates(trending, top_n=150)
    log.info(f"  {len(combo_candidates)} combo candidates generated")

    # Domain discovery
    log.info("Step: Fetching domains from external sources...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        f_db = ex.submit(fetch_domainsdb, 200)
        f_exp = ex.submit(fetch_expireddomains_page1)
        f_dc = ex.submit(fetch_dropcatch_feed)
        all_domains = f_db.result() + f_exp.result() + f_dc.result()
    all_domains += [(d,s) for d,s,_ in combo_candidates]
    if not all_domains:
        log.warning("No domains from any source – using fallback generator")
        all_domains = generate_fallback_domains(150)

    log.info(f"Total raw candidates: {len(all_domains)}")
    seen_set = set()
    unique = []
    for d,src in all_domains:
        d = d.strip().lower()
        if d and "." in d and len(d)<60 and d not in seen_set and not is_seen(conn,d) and not is_blacklisted(conn,d):
            seen_set.add(d)
            unique.append((d,src))
    unique = unique[:MAX_DOMAINS]
    log.info(f"Unique new domains to score: {len(unique)}")
    if not unique:
        pd.DataFrame(columns=["domain","final_score"]).to_csv(f"domains_hybrid_{run_id}.csv", index=False)
        conn.close()
        return

    # Parallel scoring
    log.info(f"Step: Scoring {len(unique)} domains (MAX_WORKERS={MAX_WORKERS})...")
    results = []
    args = [(d,s,conn,seo_engine,sent_engine,comps_engine,tm_guard) for d,s in unique]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_domain_safe, a):a[0] for a in args}
        for fut in concurrent.futures.as_completed(futures):
            dname = futures[fut]
            try:
                data = fut.result()
                if data and data["final_score"] >= SCORE_FLOOR:
                    results.append(data)
                    mark_seen(conn, dname, data["final_score"], data["primary_path"])
                    log.info(f"✓ {dname:38s} score={data['final_score']:3d}  SEO={data['seo_score']:.0f}  P(flip)={data['p_flip_success']:.0%}  Kelly={data['kelly_verdict']}")
            except Exception as e: log.error(f"Future error {dname}: {e}")

    if not results:
        log.warning("No domains passed score floor")
        pd.DataFrame(columns=["domain","final_score"]).to_csv(f"domains_hybrid_{run_id}.csv", index=False)
        conn.close()
        return

    # Outputs
    df = pd.DataFrame(results).sort_values("final_score", ascending=False)
    csv_path = f"domains_hybrid_{run_id}.csv"
    df.to_csv(csv_path, index=False)
    log.info(f"CSV: {csv_path} ({len(df)} rows)")
    html_path = generate_html_report(df, run_id)
    push_to_sheets(df)

    pearls = df[df["final_score"] >= MIN_ALERT_SCORE]
    log.info(f"Pearls (score≥{MIN_ALERT_SCORE}): {len(pearls)}")
    for _,row in pearls.iterrows(): send_telegram(row.to_dict()); time.sleep(1)
    send_email_digest(results)

    best = df.iloc[0]
    db_write(conn,"INSERT OR REPLACE INTO run_stats VALUES(?,?,?,?,?,?,?)",
             (run_id, run_id[:15], datetime.utcnow().isoformat(), len(results), len(pearls), best["domain"], int(best["final_score"])))

    log.info("═"*70)
    log.info("  FINAL SUMMARY")
    log.info(f"  Domains scored     : {len(results)}")
    log.info(f"  Pearls found       : {len(pearls)}")
    log.info(f"  Best domain        : {best['domain']}  (score {best['final_score']})")
    log.info(f"  P(flip success)    : {best['p_flip_success']:.0%}")
    log.info(f"  Kelly verdict      : {best['kelly_verdict']}  (alloc ${best['kelly_alloc_usd']:,.0f})")
    log.info(f"  CSV                : {csv_path}")
    log.info(f"  HTML report        : {html_path}")
    log.info("═"*70)
    conn.close()
    log.info("Done.")

if __name__ == "__main__":
    main()
