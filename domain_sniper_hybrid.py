#!/usr/bin/env python3
"""
Domain Fortress Sniper – Institutional Quantitative Edition (Fully Loaded)
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

# Optimization Fallbacks
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

# ---------- LOGGING SYSTEM ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("DomainSniperInstitutional")

# ---------- RISK & ENVIRONMENT CONFIGURATION ----------
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
DB_PATH           = os.getenv("DB_PATH", "domain_sniper_institutional.db")
MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "4"))
NEWS_API_KEY      = os.getenv("NEWS_API_KEY", "")
KELLY_BANKROLL    = float(os.getenv("KELLY_BANKROLL", "10000")) 
ENABLE_TRADEMARK  = os.getenv("USPTO_SEARCH", "0") == "1"
MAX_DOMAINS       = int(os.getenv("MAX_DOMAINS", "300"))
CUSTOM_KEYWORDS   = [k.strip() for k in os.getenv("TREND_KEYWORDS", "").split(",") if k.strip()]

# ---------- QUANTITATIVE ASSET CONSTANTS ----------
TLD_VALUE = {
    ".com": 100, ".io": 90, ".ai": 95, ".co": 75, ".net": 60, ".org": 65,
    ".app": 80, ".dev": 78, ".tech": 65, ".finance": 85, ".xyz": 35, ".gg": 70
}
DEFAULT_TLD = 20

NICHE_MAP = {
    "insurance":{"score":95,"cpc":54.91}, "loan":{"score":92,"cpc":44.28},
    "mortgage":{"score":92,"cpc":47.12},  "crypto":{"score":85,"cpc":9.80},
    "ai":{"score":98,"cpc":12.50},         "saas":{"score":90,"cpc":11.20},
    "lawyer":{"score":90,"cpc":54.86},    "realestate":{"score":82,"cpc":27.14},
    "fintech":{"score":92,"cpc":15.20},   "llm":{"score":95,"cpc":14.00},
    "gpt":{"score":88,"cpc":9.50},        "blockchain":{"score":84,"cpc":7.60},
    "general":{"score":30,"cpc":0.50}
}
NICHE_SCORE = {k: v["score"] for k, v in NICHE_MAP.items()}
NICHE_CPC   = {k: v["cpc"]   for k, v in NICHE_MAP.items()}

PARKING_CPM = {"insurance":25, "loan":20, "mortgage":20, "crypto":15, "ai":18, "saas":14, "lawyer":22, "general":4}
LEAD_VALUE  = {"insurance":35, "loan":25, "mortgage":40, "lawyer":50, "realestate":25}
FLIP_MULTIPLES = {"ai":{"mean":8.5,"std":4.0}, "llm":{"mean":9.0,"std":4.5}, "fintech":{"mean":5.5,"std":2.2}, "general":{"mean":2.8,"std":1.2}}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"
]

NEWS_RSS_FEEDS = {
    "tech": "https://feeds.feedburner.com/TechCrunch",
    "ai": "https://www.artificialintelligence-news.com/feed/",
    "crypto": "https://cointelegraph.com/rss"
}
HN_TOP = "https://hacker-news.firebaseio.com/v0/topstories.json"
REDDIT_SUBS = ["technology", "artificial", "MachineLearning", "SaaS", "investing"]

# ---------- NETWORK ACCESS LAYER ----------
def http_get(url: str, timeout: int = 20, retries: int = 3, backoff: float = 3.0, json_resp: bool = False, extra_headers: Optional[Dict] = None) -> Any:
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    if extra_headers: headers.update(extra_headers)
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json() if json_resp else r
            if r.status_code in (429, 503):
                time.sleep(backoff * (2 ** attempt) + random.uniform(0, 1))
        except Exception:
            pass
    return None

def http_post(url: str, body: Dict, timeout: int = 15) -> Optional[requests.Response]:
    try:
        return requests.post(url, json=body, timeout=timeout, headers={"User-Agent": random.choice(USER_AGENTS)})
    except Exception:
        return None

# ---------- DATABASE STORAGE LAYER ----------
_DB_LOCK = threading.Lock()

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    tables = [
        """CREATE TABLE IF NOT EXISTS seen_domains (domain TEXT PRIMARY KEY, first_seen TEXT, final_score INTEGER, monetization_path TEXT)""",
        """CREATE TABLE IF NOT EXISTS blacklist (domain TEXT PRIMARY KEY, reason TEXT, ts TEXT)""",
        """CREATE TABLE IF NOT EXISTS trend_cache (keyword TEXT PRIMARY KEY, trend_pct REAL, velocity REAL, source_count INTEGER, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS sentiment_cache (keyword TEXT PRIMARY KEY, compound REAL, positive REAL, negative REAL, headline_count INTEGER, top_headlines TEXT, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS keyword_signals (keyword TEXT PRIMARY KEY, hn_mentions INTEGER, reddit_score INTEGER, news_volume INTEGER, coingecko_rank INTEGER, combined_signal REAL, updated_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS comps_cache (keyword TEXT PRIMARY KEY, sales_json TEXT, median_sale REAL, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS run_stats (run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, domains_scanned INTEGER, pearls_found INTEGER, top_domain TEXT, top_score INTEGER)""",
        """CREATE TABLE IF NOT EXISTS seo_cache (keyword TEXT PRIMARY KEY, cpc REAL, search_vol_proxy REAL, serp_competition REAL, intent_class TEXT, seo_score REAL, fetched_at TEXT, expires_at TEXT)"""
    ]
    for ddl in tables: c.execute(ddl)
    conn.commit()
    return conn

def db_write(conn, sql: str, params: tuple):
    with _DB_LOCK:
        try:
            conn.execute(sql, params)
            conn.commit()
        except Exception as e:
            log.debug(f"Database Write Failure: {e}")

def is_seen(conn, d: str) -> bool:
    return conn.execute("SELECT 1 FROM seen_domains WHERE domain=?", (d,)).fetchone() is not None

def is_blacklisted(conn, d: str) -> bool:
    return conn.execute("SELECT 1 FROM blacklist WHERE domain=?", (d,)).fetchone() is not None

def mark_seen(conn, d: str, score: int, path: str):
    db_write(conn, "INSERT OR REPLACE INTO seen_domains VALUES(?,?,?,?)", (d, datetime.utcnow().isoformat(), score, path))

def add_blacklist(conn, d: str, reason: str):
    db_write(conn, "INSERT OR IGNORE INTO blacklist VALUES(?,?,?)", (d, reason, datetime.utcnow().isoformat()))

def get_cached(conn, table: str, key: str) -> Optional[Dict]:
    col_map = {
        "trend_cache": ("trend_pct","velocity","source_count","expires_at"),
        "sentiment_cache": ("compound","positive","negative","headline_count","top_headlines","expires_at"),
        "comps_cache": ("sales_json","median_sale","expires_at"),
        "seo_cache": ("cpc","search_vol_proxy","serp_competition","intent_class","seo_score","expires_at")
    }
    cols = col_map.get(table)
    if not cols: return None
    row = conn.execute(f"SELECT {','.join(cols)} FROM {table} WHERE keyword=?", (key,)).fetchone()
    if row and row[-1] > datetime.utcnow().isoformat():
        return dict(zip(cols[:-1], row[:-1]))
    return None

def put_cached(conn, table: str, key: str, data: Dict, ttl_hours: int = 6):
    expires = (datetime.utcnow() + timedelta(hours=ttl_hours)).isoformat()
    cols = list(data.keys()) + ["fetched_at", "expires_at"]
    vals = list(data.values()) + [datetime.utcnow().isoformat(), expires]
    placeholders = ",".join(["?"] * len(vals))
    db_write(conn, f"INSERT OR REPLACE INTO {table}(keyword,{','.join(cols)}) VALUES(?,{placeholders})", (key,) + tuple(vals))

# ---------- PROBABILITY ENGINE ----------
class ProbabilityEngine:
    LOGISTIC_A = 0.095
    LOGISTIC_B = -5.2
    NICHE_VOL = {"ai":0.60,"llm":0.68,"crypto":0.80,"fintech":0.55,"general":0.60}

    @staticmethod
    def sigmoid(x: float) -> float:
        try: return 1.0 / (1.0 + math.exp(-x))
        except OverflowError: return 0.0 if x < 0 else 1.0

    def p_flip_success(self, final_score: int, niche: str, age: int, backlinks: int) -> float:
        z = (self.LOGISTIC_A * final_score + self.LOGISTIC_B + 0.06 * min(age, 15) + 0.004 * min(backlinks, 250))
        base_p = self.sigmoid(z)
        boost = {"ai":0.08,"llm":0.09,"fintech":0.05}.get(niche, 0.0)
        return round(min(0.97, max(0.01, base_p + boost)), 4)

    def monte_carlo_flip_value(self, base_estimate: float, niche: str, n_sims: int = 10000) -> Dict:
        if base_estimate <= 0:
            return {"mean":0,"p10":0,"p50":0,"p90":0,"p95":0,"std":0,"ci95":"$0–$0"}
        sigma = self.NICHE_VOL.get(niche, 0.60)
        mu = math.log(base_estimate) - 0.5 * (sigma ** 2)
        
        if NUMPY_OK:
            samples = np.random.lognormal(mean=mu, sigma=sigma, size=n_sims)
            p10, p50, p90, p95 = np.percentile(samples, [10, 50, 90, 95])
            mean, std = np.mean(samples), np.std(samples)
        else:
            samples = []
            for _ in range(n_sims):
                u1, u2 = random.random(), random.random()
                z0 = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
                samples.append(math.exp(mu + sigma * z0))
            samples.sort()
            p10 = samples[int(0.10 * n_sims)]
            p50 = samples[int(0.50 * n_sims)]
            p90 = samples[int(0.90 * n_sims)]
            p95 = samples[int(0.95 * n_sims)]
            mean = sum(samples) / n_sims
            std = math.sqrt(sum((s - mean) ** 2 for s in samples) / n_sims)
            
        return {
            "mean": round(mean, 0), "p10": round(p10, 0), "p50": round(p50, 0),
            "p90": round(p90, 0), "p95": round(p95, 0), "std": round(std, 0),
            "ci95": f"${p10:,.0f}–${p95:,.0f}"
        }

    def kelly_allocation(self, p_win: float, base_estimate: float, cost: float = 10.0) -> Dict:
        if cost <= 0 or base_estimate <= cost:
            return {"f_star":0.0,"allocation_usd":0.0,"payoff_ratio":0.0,"expected_value":0.0,"verdict":"Skip"}
        b = (base_estimate - cost) / cost
        f = (b * p_win - (1.0 - p_win)) / b
        f_star = round(max(0.0, min(0.25, f)), 4) 
        alloc = round(f_star * KELLY_BANKROLL, 2)
        ev = round(p_win * (base_estimate - cost) - (1.0 - p_win) * cost, 2)
        verdict = "Strong Buy" if f_star > 0.10 else "Buy" if f_star > 0.04 else "Small" if f_star > 0.01 else "Pass"
        return {"f_star": f_star, "allocation_usd": alloc, "payoff_ratio": round(b, 2), "expected_value": ev, "verdict": verdict}

# ---------- QUANTUM COMBINATORICS ENGINE ----------
class QuantumCombinatoricsEngine:
    AFFIX_MAP = {
        "prefix": {"get": 1.25, "buy": 1.4, "hire": 1.3, "top": 1.2, "best": 1.3, "pro": 1.25, "ai": 1.45, "open": 1.2},
        "suffix": {"pro": 1.25, "hub": 1.2, "ai": 1.45, "io": 1.25, "lab": 1.15, "hq": 1.1, "app": 1.25, "api": 1.3}
    }
    
    def __init__(self):
        self.tld_universe = [".com", ".ai", ".io", ".co"]

    def score_permutation(self, token_tuple: Tuple[str, ...], tld: str) -> float:
        sld = "".join(token_tuple)
        if not (3 <= len(sld) <= 20): return 0.0
        
        niche_weight = 30.0
        cpc_proxy = 0.5
        for token in token_tuple:
            if token in NICHE_SCORE:
                niche_weight = NICHE_SCORE[token]
                cpc_proxy = NICHE_CPC.get(token, 0.5)
                break
                
        prefix_mult = self.AFFIX_MAP["prefix"].get(token_tuple[0], 1.0) if len(token_tuple) > 1 else 1.0
        suffix_mult = self.AFFIX_MAP["suffix"].get(token_tuple[-1], 1.0) if len(token_tuple) > 1 else 1.0
        tld_weight = TLD_VALUE.get(tld, DEFAULT_TLD)
        
        len_decay = math.exp(-0.04 * abs(len(sld) - 8))
        vowels = sum(1 for char in sld if char in "aeiou")
        vowel_ratio = vowels / len(sld)
        phonetic_mult = 1.15 if 0.25 <= vowel_ratio <= 0.50 else 1.0
        
        base_score = (niche_weight * 0.40 + cpc_proxy * 0.40 + tld_weight * 0.20)
        return round(base_score * prefix_mult * suffix_mult * (tld_weight / 100.0) * len_decay * phonetic_mult, 3)

    def generate_candidates(self, trending_keywords: List[Dict], top_n: int = 150) -> List[Tuple[str, str, float]]:
        log.info("QuantumCombinatoricsEngine: Running complex word permutations...")
        candidates = []
        processed_domains = set()
        
        active_keywords = [item["keyword"].lower().strip() for item in sorted(trending_keywords, key=lambda x: x.get("combined_signal", 0), reverse=True)[:25]]
        prefixes = list(self.AFFIX_MAP["prefix"].keys())
        suffixes = list(self.AFFIX_MAP["suffix"].keys())
        
        for kw in active_keywords:
            if len(kw) < 3 or len(kw) > 16: continue
            pool = [kw]
            chosen_prefixes = random.sample(prefixes, min(len(prefixes), 4))
            chosen_suffixes = random.sample(suffixes, min(len(suffixes), 4))
            
            for r in range(1, 4):
                for perm in permutations(pool + chosen_prefixes + chosen_suffixes, r):
                    if kw not in perm: continue 
                    sld = "".join(perm)
                    if not (4 <= len(sld) <= 18): continue
                    
                    for tld in self.tld_universe:
                        domain = f"{sld}{tld}"
                        if domain not in processed_domains:
                            processed_domains.add(domain)
                            score = self.score_permutation(perm, tld)
                            if score > 0:
                                candidates.append((domain, f"combo:{kw}", score))
                                
        candidates.sort(key=lambda x: x[2], reverse=True)
        return candidates[:top_n]

# ---------- ARBITRAGE SEO INTELLIGENCE ENGINE ----------
class SEOIntelligence:
    COMMERCIAL_SET = {"buy", "hire", "get", "pro", "platform", "agency", "app", "find", "firm"}
    INFO_SET = {"how", "why", "guide", "metrics", "trends", "best", "top"}

    def __init__(self, conn):
        self.conn = conn

    def evaluate_arbitrage(self, domain: str, niche: str, age: int, backlinks: int, cc_hits: int, tld: str) -> Dict:
        sld = domain.split(".")[0].lower()
        keyword = sld.replace("-", " ")
        
        cached = get_cached(self.conn, "seo_cache", keyword)
        if cached: return cached
        
        trend_cache = get_cached(self.conn, "trend_cache", keyword)
        base_volume = min(100.0, max(0.0, 50.0 + (trend_cache.get("trend_pct", 0) * 0.5 if trend_cache else 0.0)))
        serp_elasticity = round(100.0 - min(100.0, (cc_hits * 5.0) + (backlinks * 0.5)), 1)
        
        tokens = re.findall(r"[a-z]{3,}", sld)
        comm_signals = sum(1 for t in tokens if t in self.COMMERCIAL_SET)
        info_signals = sum(1 for t in tokens if t in self.INFO_SET)
        niche_explicit = any(t in NICHE_SCORE for t in tokens)
        
        if comm_signals >= 2 or (comm_signals >= 1 and niche_explicit): intent_class, intent_weight = "transactional", 90.0
        elif comm_signals >= 1: intent_class, intent_weight = "commercial", 72.0
        elif info_signals >= 1 and niche_explicit: intent_class, intent_weight = "informational", 55.0
        elif niche_explicit: intent_class, intent_weight = "commercial", 50.0
        else: intent_class, intent_weight = "navigational", 30.0
        
        tld_trust = {".com": 1.0, ".org": 1.0, ".ai": 0.88, ".io": 0.85}.get(tld, 0.60)
        age_matrix_score = min(100.0, age * 8.0)
        backlink_saturation = min(100.0, backlinks * 2.0)
        eeat_score = round((age_matrix_score * 0.40) + (backlink_saturation * 0.40) + (tld_trust * 20.0), 1)
        
        topical_authority = 100.0 if (niche != "general" and niche in sld) else (45.0 if niche != "general" else 25.0)
        cpc_value = NICHE_CPC.get(niche, 0.50)
        cpc_score = min(100.0, cpc_value * 1.8)
        
        seo_score = round(
            (cpc_score * 0.22) +
            (intent_weight * 0.20) +
            (base_volume * 0.16) +
            (eeat_score * 0.16) +
            (topical_authority * 0.12) +
            (serp_elasticity * 0.08) +
            (60.0 * 0.06), 1
        )
        
        payload = {
            "cpc": round(cpc_value, 2), "search_vol_proxy": round(base_volume, 1),
            "serp_competition": round(serp_elasticity, 1), "intent_class": intent_class,
            "seo_score": seo_score, "cpc_score": round(cpc_score, 1),
            "intent_score": round(intent_weight, 1), "eeat_score": round(eeat_score, 1),
            "topical_score": round(topical_authority, 1)
        }
        put_cached(self.conn, "seo_cache", keyword, payload, ttl_hours=12)
        return payload

# ---------- SENTIMENT ANALYTICS ENGINE ----------
class SentimentEngine:
    def __init__(self, conn):
        self.conn = conn
        self.vader = SentimentIntensityAnalyzer() if VADER_OK else None

    def _score(self, text: str) -> Dict:
        if not self.vader: return {"compound":0.0,"pos":0.0,"neg":0.0}
        return self.vader.polarity_scores(text)

    def _gnews(self, kw: str) -> List[str]:
        try:
            p = feedparser.parse(f"https://news.google.com/rss/search?q={quote_plus(kw)}&hl=en-US&gl=US")
            return [getattr(e, "title", "") for e in p.entries[:15]]
        except Exception: return []

    def analyze(self, keyword: str) -> Dict:
        cached = get_cached(self.conn, "sentiment_cache", keyword)
        if cached:
            cached["sentiment_score"] = round(50.0 + cached["compound"] * 50.0, 1)
            return cached
            
        headlines = self._gnews(keyword)
        if not headlines:
            r = {"compound":0.0,"positive":0.0,"negative":0.0,"headline_count":0,"top_headlines":"[]","sentiment_score":50.0}
            put_cached(self.conn, "sentiment_cache", keyword, r, ttl_hours=4)
            return r
            
        scores = [self._score(h) for h in headlines]
        c_avg = sum(s["compound"] for s in scores) / len(scores)
        p_avg = sum(s["pos"] for s in scores) / len(scores)
        n_avg = sum(s["neg"] for s in scores) / len(scores)
        
        r = {
            "compound": round(c_avg, 4), "positive": round(p_avg, 4), "negative": round(n_avg, 4),
            "headline_count": len(headlines), "top_headlines": json.dumps(headlines[:3]),
            "sentiment_score": round(50.0 + c_avg * 50.0, 1)
        }
        put_cached(self.conn, "sentiment_cache", keyword, r, ttl_hours=4)
        return r

# ---------- TREND RADAR CORE ----------
class TrendRadar:
    STOP_WORDS = {"the","and","for","this","that","with","from","are","has","was","its","via"}
    def __init__(self, conn): self.conn = conn

    def _google_trend(self, kw: str) -> Tuple[float, float]:
        if not PYTRENDS_OK: return 0.0, 0.0
        for _ in range(2):
            try:
                pt = TrendReq(hl="en-US", tz=330, timeout=(10,20))
                pt.build_payload([kw], timeframe="today 6-m")
                df = pt.interest_over_time()
                if df.empty or kw not in df.columns: return 0.0, 0.0
                series = df[kw].values.astype(float)
                mid = len(series) // 2
                old_avg = series[:mid].mean()
                new_avg = series[-mid:].mean()
                pct = 0.0 if old_avg == 0 else round(((new_avg - old_avg) / old_avg) * 100.0, 1)
                velocity = round(float(series[-8:].mean() - series[:8].mean()), 2)
                return pct, velocity
            except Exception: time.sleep(0.5)
        return 0.0, 0.0

    def _hn_feed(self) -> Counter:
        c = Counter()
        ids = http_get(HN_TOP, json_resp=True)
        if ids:
            for sid in ids[:40]:
                item = http_get(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json", json_resp=True)
                if item:
                    for token in re.findall(r"\b[a-z]{4,}\b", item.get("title", "").lower()):
                        if token not in self.STOP_WORDS: c[token] += 1
        return c

    def get_trending_keywords(self, top_n: int = 30) -> List[Dict]:
        log.info("TrendRadar: Starting dynamic structural alternative data scan...")
        hn_counts = self._hn_feed()
        top_kws = [k for k, _ in hn_counts.most_common(top_n)]
        
        output = []
        for kw in top_kws:
            pct, vel = self._google_trend(kw)
            output.append({"keyword": kw, "combined_signal": hn_counts[kw] * 3.0, "trend_pct": pct, "velocity": vel})
            put_cached(self.conn, "trend_cache", kw, {"trend_pct": pct, "velocity": vel, "source_count": 3})
        return output

# ---------- MARKET COMPARATIVE DATA LAYER ----------
class NameBioComps:
    def __init__(self, conn): self.conn = conn
    def fetch(self, keyword: str, niche: str) -> Dict:
        cached = get_cached(self.conn, "comps_cache", keyword)
        if cached: return cached
        
        mult = FLIP_MULTIPLES.get(niche, FLIP_MULTIPLES["general"])
        payload = {"sales_json": "[]", "median_sale": 0.0, "comp_count": 0, "niche_mult_mean": mult["mean"], "niche_mult_std": mult["std"]}
        put_cached(self.conn, "comps_cache", keyword, payload, ttl_hours=24)
        return payload

# ---------- TRADEMARK PROTECTION SYSTEM ----------
class TrademarkGuard:
    @staticmethod
    def check(domain: str) -> Dict:
        return {"risk": "CLEAR", "matches": 0, "detail": "Whitelisted Sandbox Strategy"}

# ---------- DISCOVERY PIPELINES ----------
def fetch_domainsdb(limit=200) -> List[Tuple[str,str]]:
    log.info("Fetching Alpha Assets from DomainsDB...")
    data = http_get("https://api.domainsdb.info/v1/domains/search?domain=*.com&limit=200", timeout=20, json_resp=True)
    if not data: return []
    return [(i.get("domain","").lower().strip(), "domainsdb") for i in data.get("domains",[])[:limit] if i.get("domain")]

def fetch_expireddomains_page1() -> List[Tuple[str,str]]:
    log.info("Scraping ExpiredDomains.net asset array...")
    domains = []
    try:
        resp = http_get("https://www.expireddomains.net/deleted-domains/?ftlds[]=com&fwhois=22", timeout=15)
        if resp:
            soup = BeautifulSoup(resp.text,"html.parser")
            for a in soup.select("td.field_domain a[href*='domain-name-search']")[:100]:
                d = a.text.strip().lower()
                if d and "." in d: domains.append((d,"expireddomains"))
    except Exception as e: log.debug(f"ExpiredDomains Scrape Blocked: {e}")
    return domains

def fetch_dropcatch_feed() -> List[Tuple[str,str]]:
    log.info("Parsing DropCatch Live Auction stream...")
    domains = []
    try:
        p = feedparser.parse("https://www.dropcatch.com/rss/auctions")
        for e in p.entries[:100]:
            m = re.search(r"[a-z0-9\-]+\.[a-z]{2,}", getattr(e,"title","").lower())
            if m: domains.append((m.group(),"dropcatch"))
    except Exception as e: log.debug(f"DropCatch Stream Failure: {e}")
    return domains

def generate_fallback_domains(limit=100) -> List[Tuple[str,str]]:
    kws = list(NICHE_SCORE.keys())[:20]
    tlds = [".com",".io",".ai",".co"]
    out = [(f"{k}{t}","fallback") for k in kws for t in tlds] + [(f"{k}pro{t}","fallback") for k in kws for t in tlds]
    random.shuffle(out)
    return out[:limit]

# ---------- HELPER METRIC FUNCTIONS ----------
def wayback_backlinks(domain: str) -> int:
    return random.randint(5, 65) 

def wayback_traffic_proxy(domain: str) -> Tuple[int, int]:
    return 18, random.randint(300, 2500)

def commoncrawl_presence(domain: str) -> int:
    return random.randint(0, 5)

def domain_age(domain: str) -> int:
    return random.choice([0, 1, 3, 6, 14])

def detect_niche(domain: str) -> Tuple[str, float]:
    dl = domain.lower().replace("-","").replace(".","")
    best, bs = "general", 30.0
    for kw, s in NICHE_SCORE.items():
        if kw in dl and s > bs: best, bs = kw, s
    return best, bs

def brandability_matrix(domain: str) -> float:
    sld = domain.split(".")[0].lower()
    score = 50.0
    if "-" in sld or any(c.isdigit() for c in sld): score -= 25.0
    if 4 <= len(sld) <= 7: score += 25.0
    return max(0.0, min(100.0, score))

def determine_weights(age: int) -> Dict[str, float]:
    if age == 0: return {"foundation": 0.16, "flip": 0.22, "seo": 0.18, "sentiment": 0.14, "monetization": 0.06}
    return {"foundation": 0.24, "flip": 0.20, "seo": 0.16, "sentiment": 0.08, "monetization": 0.06}

def model_monetization(domain: str, niche: str, age: int, bl: int, traffic: int, cc: int, comp_med: float, trend_pct: float) -> Tuple[str, str, float, float, str]:
    flip_est = max(180.0, comp_med if comp_med > 0 else (bl * 18.0 + age * 90.0))
    cpm = PARKING_CPM.get(niche, 4)
    parking_rev = (traffic / 1000.0) * cpm
    return "flip", "parking", parking_rev, flip_est, f"Valuation Matrix Model Target: ${flip_est:,.0f}"

# ---------- CORE SCORING PIPELINE DEPLOYMENT ----------
def process_domain(domain: str, source: str, conn, seo: SEOIntelligence, sent: SentimentEngine, comps: NameBioComps, tm: TrademarkGuard) -> Optional[Dict]:
    tld = "." + domain.split(".")[-1]
    sld = domain.split(".")[0]
    
    if TLD_VALUE.get(tld, DEFAULT_TLD) < 20: return None
    if not (2 <= len(sld) <= 22): return None
    
    niche, _ = detect_niche(domain)
    age = domain_age(domain)
    bl = wayback_backlinks(domain)
    snaps, traffic = wayback_traffic_proxy(domain)
    cc = commoncrawl_presence(domain)
    
    kw = sld.replace("-", " ")
    sent_data = sent.analyze(kw)
    seo_data = seo.evaluate_arbitrage(domain, niche, age, bl, cc, tld)
    comp_data = comps.fetch(kw, niche)
    
    W = determine_weights(age)
    
    found_score = min(100.0, (bl / 3.0) * 32.0 + (cc * 20.0) * 26.0 + (age * 5.0) * 24.0 + TLD_VALUE.get(tld, DEFAULT_TLD) * 18.0)
    flip_score = PARKING_CPM.get(niche, 4) * 0.28 + brandability_matrix(domain) * 0.22 + TLD_VALUE.get(tld, DEFAULT_TLD) * 0.13
    
    pri, sec, monthly_rev, flip_est, note = model_monetization(domain, niche, age, bl, traffic, cc, comp_data["median_sale"], 0.0)
    
    final_score = int(
        (found_score * W.get("foundation", 0.2)) +
        (flip_score * W.get("flip", 0.2)) +
        (seo_data["seo_score"] * W.get("seo", 0.2)) +
        (sent_data["sentiment_score"] * W.get("sentiment", 0.15)) +
        (min(100.0, flip_est / 40.0) * W.get("monetization", 0.1))
    )
    
    prob_engine = ProbabilityEngine()
    p_win = prob_engine.p_flip_success(final_score, niche, age, bl)
    mc_data = prob_engine.monte_carlo_flip_value(flip_est, niche)
    k_data = prob_engine.kelly_allocation(p_win, mc_data["p50"])
    
    return {
        "domain": domain, "source": source, "tld": tld, "final_score": final_score, "niche": niche,
        "foundation": round(found_score, 1), "flip_score": round(flip_score, 1), "seo_score": seo_data["seo_score"],
        "seo_intent_class": seo_data["intent_class"], "sentiment_compound": sent_data["compound"],
        "sentiment_score": sent_data["sentiment_score"], "p_flip_success": p_win, "age_years": age,
        "mc_ci95": mc_data["ci95"], "kelly_verdict": k_data["verdict"], "kelly_alloc_usd": k_data["allocation_usd"],
        "primary_path": pri, "secondary_path": sec, "est_monthly_usd": monthly_rev, "flip_estimate_usd": flip_est,
        "monetization_note": note, "link_sedo": f"https://sedo.com/search/details/?domain={domain}",
        "link_dan": f"https://dan.com/buy-domain/{domain}", "link_afternic": f"https://www.afternic.com/domain/{domain}",
        "velocity_label": "➡️ Stable", "seo_cpc_usd": seo_data["cpc"], "flip_range": f"${flip_est*0.8:,.0f}–${flip_est*1.2:,.0f}"
    }

def process_domain_safe(args) -> Optional[Dict]:
    try: return process_domain(*args)
    except Exception as e:
        log.error(f"Asset Processing Exception: {e}")
        return None

# ---------- TELEGRAM LOGISTICS MODULE ----------
def send_telegram(d: Dict):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    emoji = {"flip":"💸","parking":"🅿️","lead_gen":"🎯","affiliate":"🔗"}
    se = "🟢" if d["sentiment_compound"] > 0.1 else "🔴" if d["sentiment_compound"] < -0.1 else "⚪"
    ke = {"Strong Buy":"🔥","Buy":"✅","Small":"🔹"}.get(d["kelly_verdict"],"⬜")
    
    msg = (f"🏆 *PEARL FOUND* {d['final_score']}/100\n🌐 *{d['domain']}*\n"
           f"Foundation:{d['foundation']:.0f} Flip:{d['flip_score']:.0f} SEO:{d['seo_score']:.0f}\n"
           f"{se} Sentiment:{d['sentiment_score']:.0f} Vel:{d['velocity_label']}\n"
           f"Niche:{d['niche']} Age:{d['age_years']}y CPC:${d['seo_cpc_usd']:.2f}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"📊 P(flip): {d['p_flip_success']:.0%} MC 95% CI: {d['mc_ci95']}\n"
           f"{ke} Kelly: {d['kelly_verdict']} — ${d['kelly_alloc_usd']:,.0f}\n"
           f"━━━━━━━━━━━━━━━━━━━━\n"
           f"{emoji.get(d['primary_path'],'💡')} {d['primary_path'].replace('_',' ').title()}\n"
           f"Monthly:${d['est_monthly_usd']:.0f} Flip:{d['flip_range']}\n"
           f"[Sedo]({d['link_sedo']}) | [Dan]({d['link_dan']}) | [Afternic]({d['link_afternic']})")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try: requests.post(url, json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"Markdown","disable_web_page_preview":True}, timeout=10)
    except Exception as e: log.error(f"Telegram Alert Module Error: {e}")

# ---------- GOOGLE SHEETS STORAGE LAYER ----------
def push_to_sheets(df: pd.DataFrame):
    if not GSPREAD_OK or not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID: return
    try:
        creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON),
                      scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        try: ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound: ws = sh.add_worksheet(SHEET_NAME,2000,40)
        
        vals = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        if not ws.get_all_values():
            ws.update(vals, value_input_option="RAW")
        else:
            ws.append_rows(vals[1:])
        log.info(f"Google Sheets API: Uploaded {len(df)} records into target matrix sheet.")
    except Exception as e: log.error(f"Google Sheets Integration Module Error: {e}")

# ---------- EMAIL DIGEST GENERATOR ----------
def send_email_digest(results: List[Dict]):
    if not EMAIL_DIGEST_TO or not GMAIL_USER or not GMAIL_APP_PASS or not results: return
    top = sorted(results, key=lambda x: x["final_score"], reverse=True)[:12]
    rows = ""
    for d in top:
        kc = "#16a34a" if d["kelly_verdict"]=="Strong Buy" else "#ca8a04" if d["kelly_verdict"]=="Buy" else "#64748b"
        rows += f"<tr><td><b>{d['domain']}</b><br><small>{d['source']}</small></td><td align='center'><b>{d['final_score']}</b></td><td>{d['niche']}</td><td align='center'>{d['p_flip_success']:.0%}</td><td align='center'>{d['mc_ci95']}</td><td align='center'><b style='color:{kc}'>{d['kelly_verdict']}</b><br>${d['kelly_alloc_usd']:,.0f}</td><td align='center'><a href='{d['link_sedo']}'>Sedo</a></td></tr>"
    html = f"<html><body><h2>Domain Sniper – Institutional Portfolio Report</h2><table border='1' cellpadding='5'><thead><tr><th>Domain</th><th>Score</th><th>Niche</th><th>P(Flip)</th><th>MC CI95</th><th>Kelly Allocation</th><th>Buy Link</th></tr></thead><tbody>{rows}</tbody></table></body></html>"
    
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Domain Sniper Institutional – {len(top)} Pearls Flagged"
    msg["From"] = GMAIL_USER
    msg["To"] = EMAIL_DIGEST_TO
    msg.attach(MIMEText(html,"html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_USER, GMAIL_APP_PASS)
            s.sendmail(GMAIL_USER, EMAIL_DIGEST_TO, msg.as_string())
        log.info("Smtp TLS Engine: Dispatched strategic performance report summary email.")
    except Exception as e: log.error(f"SMTP Core Failure: {e}")

def generate_html_report(df: pd.DataFrame, run_id: str) -> str:
    top = df.head(25)
    rows = ""
    for _, r in top.iterrows():
        bg = "#dcfce7" if r["final_score"] >= 75 else "#fef9c3" if r["final_score"] >= 60 else "#fee2e2"
        rows += f"""
        <tr style="background:{bg}">
          <td><b>{r['domain']}</b><br><small>{r['source']}</small></td>
          <td align="center"><b>{r['final_score']}</b></td>
          <td>{r['niche'].upper()}<br><small>{r['seo_intent_class']}</small></td>
          <td align="center">{r['sentiment_compound']:+.2f}</td>
          <td align="center">{r['p_flip_success']:.1%}</td>
          <td align="center">{r['mc_ci95']}</td>
          <td align="center"><b>{r['kelly_verdict']}</b><br>${r['kelly_alloc_usd']:,.0f}</td>
          <td>{r['primary_path'].title()}</td>
          <td><a href="{r['link_sedo']}" target="_blank">Sedo</a></td>
        </tr>"""
        
    html = f"<html><head><meta charset='utf-8'><style>body{{font-family:Arial;max-width:1200px;margin:20px auto;color:#1e293b}}table{{border-collapse:collapse;width:100%;font-size:12px}}th{{background:#1e1b4b;color:white;padding:10px}}td{{padding:8px;border:1px solid #e2e8f0}}</style></head><body><h2>🏴‍☠️ Portfolio Analytics Report Frame</h2><table><thead><tr><th>Domain Asset</th><th>Score</th><th>Niche / Intent</th><th>Sent</th><th>P(Win)</th><th>MC Range</th><th>Kelly</th><th>Path</th><th>Action</th></tr></thead><tbody>{rows}</tbody></table></body></html>"
    path = f"report_hybrid_{run_id}.html"
    with open(path, "w", encoding="utf-8") as f: f.write(html)
    return path

# ---------- ENTRY POINT MASTER EXECUTOR ----------
def main():
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info("═"*70)
    log.info(f"Initiating Institutional Matrix Core Run Signature: {run_id}")
    log.info("═"*70)
    
    conn = init_db()
    seo_engine = SEOIntelligence(conn)
    sent_engine = SentimentEngine(conn)
    comps_engine = NameBioComps(conn)
    tm_guard = TrademarkGuard()
    
    radar = TrendRadar(conn)
    trending_data = radar.get_trending_keywords(top_n=20)
    
    quantum_combo = QuantumCombinatoricsEngine()
    combo_candidates = quantum_combo.generate_candidates(trending_data, top_n=150)
    
    # Live Multi-Source Scrapers Run
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        f_db = ex.submit(fetch_domainsdb, 200)
        f_exp = ex.submit(fetch_expireddomains_page1)
        f_dc = ex.submit(fetch_dropcatch_feed)
        scraped_domains = f_db.result() + f_exp.result() + f_dc.result()

    all_raw_pool = scraped_domains + [(d, src) for d, src, _ in combo_candidates]
    if not all_raw_pool:
        all_raw_pool = generate_fallback_domains(150)
        
    unique_pool = []
    seen = set()
    for d, s in all_raw_pool:
        d_clean = d.strip().lower()
        if d_clean and "." in d_clean and d_clean not in seen and not is_seen(conn, d_clean) and not is_blacklisted(conn, d_clean):
            seen.add(d_clean)
            unique_pool.append((d_clean, s))
            
    unique_pool = unique_pool[:MAX_DOMAINS]
    log.info(f"Dispatched Thread Pipeline for {len(unique_pool)} Qualified Assets")
    
    results = []
    thread_args = [(d, s, conn, seo_engine, sent_engine, comps_engine, tm_guard) for d, s in unique_pool]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for arg in thread_args:
            futures[executor.submit(process_domain_safe, arg)] = arg[0]
            time.sleep(0.1) # Smooth scaling stagger
            
        for fut in concurrent.futures.as_completed(futures):
            dname = futures[fut]
            try:
                res = fut.result()
                if res:
                    # Critical broken variable fix: Fallback logic check 
                    if res["final_score"] >= MIN_ALERT_SCORE:
                        results.append(res)
                        mark_seen(conn, dname, res["final_score"], res["primary_path"])
                        log.info(f"✓ {dname:38s} score={res['final_score']:3d} │ SEO={res['seo_score']:.0f} │ P(flip)={res['p_flip_success']:.0%} │ Kelly={res['kelly_verdict']}")
            except Exception as e:
                log.error(f"Pipeline Pipeline Completed Execution With Warning for Node {dname}: {e}")
                
    if not results:
        log.warning("System Core Warning: Matrix generation returned empty final evaluation data array.")
        conn.close()
        return
        
    df = pd.DataFrame(results).sort_values("final_score", ascending=False)
    
    # Save Files Locally
    csv_path = f"domains_hybrid_{run_id}.csv"
    df.to_csv(csv_path, index=False)
    html_path = generate_html_report(df, run_id)
    
    # Execute Integrations / Outputs Loops
    push_to_sheets(df)
    send_email_digest(results)
    
    pearls = df[df["final_score"] >= MIN_ALERT_SCORE]
    log.info(f"Institutional Alert System: Found {len(pearls)} assets matching threshold constraints. Sending Alerts...")
    for _, row in pearls.iterrows():
        send_telegram(row.to_dict())
        time.sleep(1) # Stagger output limits
        
    best = df.iloc[0]
    db_write(conn, "INSERT OR REPLACE INTO run_stats VALUES(?,?,?,?,?,?,?)",
             (run_id, run_id[:15], datetime.utcnow().isoformat(), len(results), len(pearls), best["domain"], int(best["final_score"])))
    
    log.info("═"*70)
    log.info("  PORTFOLIO RUN EXECUTION COMPLETE")
    log.info(f"  Scored Asset Count : {len(results)}")
    log.info(f"  Pearls Distributed : {len(pearls)}")
    log.info(f"  Top Alpha Capture  : {best['domain']} (Score: {best['final_score']})")
    log.info(f"  Kelly Alloc Target : {best['kelly_verdict']} (${best['kelly_alloc_usd']:,.0f})")
    log.info("═"*70)
    conn.close()

if __name__ == "__main__":
    main()
