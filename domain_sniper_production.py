#!/usr/bin/env python3
"""
Domain Fortress Sniper – Institutional Quantitative Edition (v3)

CHANGES FROM v2:
  • Telegram alerts strictly gated at MIN_ALERT_SCORE (default 80)
  • Clearer Telegram message explaining every metric in plain language
  • Reg cost clarified as "what you pay to register"
  • MC Range labelled as "estimated resale value"
  • Kelly labelled as "suggested max spend from your bankroll"
  • Added worthiness verdict (Strong/Good/Marginal/Skip)
  • 1-Month Outcome Tracker (SQLite) — log every alerted domain
  • AI Learning Engine — monthly Claude API call analyses 30-day data
    and returns suggested scoring weight tweaks + niche insights
  • apply_learned_weights() hot-patches scoring formula each run

BUG FIXES (from v2, all sandbox-verified):
  [CRITICAL] sentiment_cache schema mismatch
  [HIGH]     final_score ceiling ~72
  [HIGH]     Trademark landmines from combinatorics
  [MEDIUM]   Kelly b<=0 silent pass
  [MEDIUM]   mark_seen for all scores >= 40
  [MEDIUM]   niche detection missing crypto aliases
  [LOW]      brand_score binary cliff
  [LOW]      No final_score cap
"""

import os, sys, re, json, sqlite3, logging, math, random
import threading, socket
from datetime import datetime, timedelta
from collections import Counter
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote_plus

import requests
import pandas as pd
import feedparser

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

# ─────────────────────────── LOGGING ───────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("DomainSniperV3")

# ─────────────────────────── ENVIRONMENT ───────────────────────
TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_CREDS_JSON   = os.getenv("GOOGLE_CREDS_JSON", "")
GOOGLE_SHEET_ID     = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NAME          = os.getenv("SHEET_NAME", "DomainSniperV3")
MIN_ALERT_SCORE     = int(os.getenv("MIN_ALERT_SCORE", "80"))   # ← strict gate
DB_PATH             = os.getenv("DB_PATH", "domain_sniper_v3.db")
KELLY_BANKROLL      = float(os.getenv("KELLY_BANKROLL", "10000"))
ENABLE_TRADEMARK    = os.getenv("USPTO_SEARCH", "1") == "1"
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")        # for AI learning
REDDIT_CLIENT_ID    = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET= os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT   = os.getenv("REDDIT_USER_AGENT", "DomainSniperV3/1.0")
AFFILIATE_ID_GD     = os.getenv("AFFILIATE_ID_GD", "")
AFFILIATE_ID_NC     = os.getenv("AFFILIATE_ID_NC", "")

USD_TO_INR = float(os.getenv("EXCHANGE_RATE_INR", "83.50"))

# ─────────────────────────── CONSTANTS ─────────────────────────
TLD_VALUE = {".com": 100, ".io": 90, ".ai": 95, ".co": 75, ".net": 60, ".org": 65}
DEFAULT_TLD = 20

TLD_REG_COSTS = {
    ".com": 12.0, ".net": 14.0, ".org": 15.0,
    ".io":  40.0, ".ai": 80.0,  ".co": 25.0,
}
DEFAULT_REG_COST = 15.0

NICHE_MAP = {
    "insurance":  {"score": 95, "cpc": 54.91},
    "loan":       {"score": 92, "cpc": 44.28},
    "mortgage":   {"score": 92, "cpc": 47.12},
    "crypto":     {"score": 85, "cpc": 9.80},
    "btc":        {"score": 85, "cpc": 9.80},
    "eth":        {"score": 85, "cpc": 9.80},
    "defi":       {"score": 85, "cpc": 9.80},
    "web3":       {"score": 85, "cpc": 9.80},
    "nft":        {"score": 80, "cpc": 7.50},
    "ai":         {"score": 98, "cpc": 12.50},
    "saas":       {"score": 90, "cpc": 11.20},
    "lawyer":     {"score": 90, "cpc": 54.86},
    "realestate": {"score": 82, "cpc": 27.14},
    "fintech":    {"score": 92, "cpc": 15.20},
    "llm":        {"score": 95, "cpc": 14.00},
    "quantum":    {"score": 94, "cpc": 11.50},
    "biotech":    {"score": 91, "cpc": 9.80},
    "general":    {"score": 30, "cpc": 0.50},
}
NICHE_SCORE = {k: v["score"] for k, v in NICHE_MAP.items()}
NICHE_CPC   = {k: v["cpc"]   for k, v in NICHE_MAP.items()}

BLOCKED_BRAND_KEYWORDS = {
    "microsoft","google","apple","amazon","meta","openai","tesla","nvidia",
    "twitter","netflix","adobe","salesforce","oracle","facebook","instagram",
    "youtube","tiktok","linkedin","spotify","uber","airbnb","shopify","stripe",
    "cloudflare","databricks","palantir","snowflake","atlassian","twilio",
    "sendgrid","hubspot","zendesk","docusign","zoom","slack","notion","figma",
}

DYNAMIC_CC_URL = "https://index.commoncrawl.org/CC-MAIN-2024-10-index"

# ─── LEARNED WEIGHTS (overridden by AI learning engine each run) ───
SCORING_WEIGHTS = {
    "found_score":       0.25,
    "brand_score":       0.25,
    "sentiment_score":   0.30,
    "tld_value":         0.20,
    # bonus gates
    "bonus_age":         4,
    "bonus_backlinks":   3,
    "bonus_niche":       5,
    "bonus_short":       3,
}

# ─────────────────────────── DATABASE ──────────────────────────
_DB_LOCK = threading.Lock()

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    ddls = [
        """CREATE TABLE IF NOT EXISTS seen_domains (
               domain TEXT PRIMARY KEY, first_seen TEXT,
               final_score INTEGER, monetization_path TEXT)""",
        """CREATE TABLE IF NOT EXISTS blacklist (
               domain TEXT PRIMARY KEY, reason TEXT, ts TEXT)""",
        """CREATE TABLE IF NOT EXISTS trend_cache (
               keyword TEXT PRIMARY KEY, trend_pct REAL,
               velocity REAL, fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS sentiment_cache (
               keyword TEXT PRIMARY KEY,
               compound REAL, positive REAL, negative REAL,
               headline_count INTEGER,
               fetched_at TEXT, expires_at TEXT)""",
        """CREATE TABLE IF NOT EXISTS comps_cache (
               keyword TEXT PRIMARY KEY, median_sale REAL, comp_count INTEGER)""",
        """CREATE TABLE IF NOT EXISTS seo_cache (
               keyword TEXT PRIMARY KEY, cpc REAL, search_vol_proxy REAL,
               serp_competition REAL, intent_class TEXT, seo_score REAL,
               fetched_at TEXT, expires_at TEXT)""",

        # ── 1-MONTH TRACKER ──────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS domain_outcomes (
               domain            TEXT PRIMARY KEY,
               first_alert_date  TEXT,
               alert_score       INTEGER,
               reg_cost_usd      REAL,
               niche             TEXT,
               age_years         INTEGER,
               sentiment_score   REAL,
               brand_score       REAL,
               backlinks         INTEGER,
               tld               TEXT,
               kelly_verdict     TEXT,
               kelly_alloc_usd   REAL,
               mc_p10            REAL,
               mc_p50            REAL,
               mc_p90            REAL,
               registered        INTEGER DEFAULT 0,
               sold              INTEGER DEFAULT 0,
               sale_price_usd    REAL    DEFAULT 0,
               days_to_sell      INTEGER DEFAULT 0,
               outcome_notes     TEXT    DEFAULT '',
               updated_at        TEXT)""",

        # ── AI LEARNING SNAPSHOTS ─────────────────────────────────
        """CREATE TABLE IF NOT EXISTS learning_snapshots (
               snapshot_id       TEXT PRIMARY KEY,
               generated_at      TEXT,
               domains_tracked   INTEGER,
               avg_score         REAL,
               conversion_rate   REAL,
               flip_rate         REAL,
               avg_sale_price    REAL,
               best_niche        TEXT,
               worst_niche       TEXT,
               ai_insights       TEXT,
               recommended_weights TEXT)""",
    ]
    for ddl in ddls:
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
    return conn.execute(
        "SELECT 1 FROM seen_domains WHERE domain=?", (d,)
    ).fetchone() is not None

def mark_seen(conn, d: str, score: int, path: str):
    db_write(conn,
        "INSERT OR REPLACE INTO seen_domains VALUES(?,?,?,?)",
        (d, datetime.utcnow().isoformat(), score, path))

def get_cached(conn, table: str, key: str) -> Optional[Dict]:
    col_map = {
        "trend_cache":     ("trend_pct","velocity","expires_at"),
        "sentiment_cache": ("compound","positive","negative","headline_count","expires_at"),
        "seo_cache":       ("cpc","search_vol_proxy","serp_competition",
                            "intent_class","seo_score","expires_at"),
    }
    cols = col_map.get(table)
    if not cols:
        return None
    row = conn.execute(
        f"SELECT {','.join(cols)} FROM {table} WHERE keyword=?", (key,)
    ).fetchone()
    if row and row[-1] > datetime.utcnow().isoformat():
        return dict(zip(cols[:-1], row[:-1]))
    return None

def put_cached(conn, table: str, key: str, data: Dict, ttl_hours: int = 6):
    expires = (datetime.utcnow() + timedelta(hours=ttl_hours)).isoformat()
    cols = list(data.keys()) + ["fetched_at", "expires_at"]
    vals = list(data.values()) + [datetime.utcnow().isoformat(), expires]
    ph   = ",".join(["?"] * len(vals))
    db_write(conn,
        f"INSERT OR REPLACE INTO {table}(keyword,{','.join(cols)}) VALUES(?,{ph})",
        (key,) + tuple(vals))

# ─────────────────────────── TRACKER ───────────────────────────
def track_domain_alert(conn, res: Dict, mc_p10: float, mc_p50: float, mc_p90: float,
                       brand_score: float, backlinks: int):
    """Insert or ignore into domain_outcomes when we fire a Telegram alert."""
    db_write(conn, """
        INSERT OR IGNORE INTO domain_outcomes (
            domain, first_alert_date, alert_score, reg_cost_usd, niche,
            age_years, sentiment_score, brand_score, backlinks, tld,
            kelly_verdict, kelly_alloc_usd, mc_p10, mc_p50, mc_p90, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        res["domain"], datetime.utcnow().isoformat(), res["final_score"],
        res["reg_cost_usd"], res["niche"], res["age_years"],
        res["sentiment_score"], brand_score, backlinks,
        res["tld"], res["kelly_verdict"], res["kelly_alloc_usd"],
        mc_p10, mc_p50, mc_p90, datetime.utcnow().isoformat(),
    ))
    log.info(f"📋 Tracked outcome for {res['domain']}")

def update_outcome(conn, domain: str, registered: bool = False,
                   sold: bool = False, sale_price: float = 0.0,
                   days_to_sell: int = 0, notes: str = ""):
    """Call this manually or via a separate CLI to record what happened."""
    db_write(conn, """
        UPDATE domain_outcomes SET
            registered=?, sold=?, sale_price_usd=?,
            days_to_sell=?, outcome_notes=?, updated_at=?
        WHERE domain=?
    """, (int(registered), int(sold), sale_price,
          days_to_sell, notes, datetime.utcnow().isoformat(), domain))

# ─────────────────────── AI LEARNING ENGINE ────────────────────
class AILearningEngine:
    """
    Monthly: reads 30-day domain_outcomes, sends to Claude API,
    gets back JSON with scoring weight suggestions + niche insights.
    Hot-patches SCORING_WEIGHTS for the current run.
    """
    def __init__(self, conn):
        self.conn = conn

    def _fetch_30day_data(self) -> List[Dict]:
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        rows = self.conn.execute("""
            SELECT domain, alert_score, niche, age_years, sentiment_score,
                   brand_score, backlinks, tld, kelly_verdict, kelly_alloc_usd,
                   mc_p50, registered, sold, sale_price_usd, days_to_sell
            FROM domain_outcomes
            WHERE first_alert_date >= ?
            ORDER BY alert_score DESC
        """, (cutoff,)).fetchall()
        cols = ["domain","alert_score","niche","age_years","sentiment_score",
                "brand_score","backlinks","tld","kelly_verdict","kelly_alloc_usd",
                "mc_p50","registered","sold","sale_price_usd","days_to_sell"]
        return [dict(zip(cols, r)) for r in rows]

    def _build_stats(self, data: List[Dict]) -> Dict:
        if not data:
            return {}
        total = len(data)
        registered = sum(1 for d in data if d["registered"])
        sold       = sum(1 for d in data if d["sold"])
        sold_data  = [d for d in data if d["sold"] and d["sale_price_usd"] > 0]
        avg_sale   = sum(d["sale_price_usd"] for d in sold_data) / len(sold_data) if sold_data else 0
        niche_flip = Counter(d["niche"] for d in data if d["sold"])
        niche_reg  = Counter(d["niche"] for d in data if d["registered"])
        return {
            "total_alerted":   total,
            "registered":      registered,
            "sold":            sold,
            "conversion_rate": round(registered / total, 3) if total else 0,
            "flip_rate":       round(sold / registered, 3) if registered else 0,
            "avg_sale_usd":    round(avg_sale, 2),
            "best_niches":     [n for n, _ in niche_flip.most_common(3)],
            "worst_niches":    [n for n, _ in niche_reg.most_common() if n not in niche_flip],
            "avg_score":       round(sum(d["alert_score"] for d in data)/total, 1),
            "raw_sample":      data[:20],  # first 20 for Claude context
        }

    def _call_claude(self, stats: Dict) -> Optional[Dict]:
        if not ANTHROPIC_API_KEY:
            log.warning("ANTHROPIC_API_KEY not set — skipping AI learning")
            return None
        prompt = f"""
You are a domain investing AI analyst. Below is 30-day performance data from an automated domain sniper tool.

STATS:
{json.dumps(stats, indent=2)}

CURRENT SCORING WEIGHTS:
{json.dumps(SCORING_WEIGHTS, indent=2)}

Analyse the data and return ONLY a valid JSON object (no markdown, no explanation) with these keys:
{{
  "insights": "2-3 sentence summary of what the data shows",
  "recommended_weights": {{
    "found_score": <float 0.0-0.4>,
    "brand_score": <float 0.0-0.4>,
    "sentiment_score": <float 0.0-0.4>,
    "tld_value": <float 0.0-0.3>,
    "bonus_age": <int 0-8>,
    "bonus_backlinks": <int 0-8>,
    "bonus_niche": <int 0-10>,
    "bonus_short": <int 0-6>
  }},
  "niche_focus": ["list", "of", "niches", "to", "prioritise"],
  "niche_avoid": ["list", "of", "niches", "to", "reduce"],
  "min_score_suggestion": <int 75-95>,
  "action_items": ["actionable", "improvements", "for", "next", "month"]
}}
Weights must sum to approximately 1.0 for the four main weights.
"""
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1000,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=30,
            )
            if r.status_code == 200:
                content = r.json()["content"][0]["text"].strip()
                # Strip any accidental markdown fences
                content = re.sub(r"^```json|^```|```$", "", content, flags=re.M).strip()
                return json.loads(content)
            else:
                log.error(f"Claude API error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.error(f"AI learning call failed: {e}")
        return None

    def apply_learned_weights(self) -> bool:
        """
        Run monthly learning cycle. Returns True if weights were updated.
        Skip if a snapshot was already generated in last 25 days.
        """
        last = self.conn.execute(
            "SELECT generated_at FROM learning_snapshots ORDER BY generated_at DESC LIMIT 1"
        ).fetchone()
        if last:
            last_dt = datetime.fromisoformat(last[0])
            if (datetime.utcnow() - last_dt).days < 25:
                log.info(f"AI Learning: last snapshot {last[0][:10]} — skipping (< 25 days)")
                return False

        data = self._fetch_30day_data()
        if len(data) < 5:
            log.info(f"AI Learning: only {len(data)} outcomes — need ≥5 to learn. Skipping.")
            return False

        stats = self._build_stats(data)
        log.info(f"AI Learning: analysing {stats['total_alerted']} domains "
                 f"({stats['registered']} registered, {stats['sold']} sold)...")

        result = self._call_claude(stats)
        if not result:
            return False

        # Validate and apply weights
        rw = result.get("recommended_weights", {})
        main_sum = sum(rw.get(k, SCORING_WEIGHTS[k])
                       for k in ["found_score","brand_score","sentiment_score","tld_value"])
        if 0.85 <= main_sum <= 1.15:   # sanity check
            for k in SCORING_WEIGHTS:
                if k in rw:
                    SCORING_WEIGHTS[k] = rw[k]
            log.info(f"✅ AI Learning: weights updated → {SCORING_WEIGHTS}")
        else:
            log.warning(f"AI Learning: weight sum {main_sum:.2f} out of range — ignoring")

        # Save snapshot
        snap_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        db_write(self.conn, """
            INSERT OR REPLACE INTO learning_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            snap_id, datetime.utcnow().isoformat(),
            stats["total_alerted"], stats["avg_score"],
            stats["conversion_rate"], stats["flip_rate"], stats["avg_sale_usd"],
            ",".join(stats["best_niches"]), ",".join(stats.get("worst_niches", [])),
            result.get("insights", ""), json.dumps(rw),
        ))

        log.info(f"💡 AI Insights: {result.get('insights', '')}")
        log.info(f"🎯 Niche focus: {result.get('niche_focus', [])}")
        log.info(f"📋 Actions: {result.get('action_items', [])}")
        return True

    def print_tracker_report(self):
        """Print a readable 30-day summary to console."""
        data = self._fetch_30day_data()
        if not data:
            log.info("Tracker: no data in last 30 days yet.")
            return
        stats = self._build_stats(data)
        print("\n" + "═"*60)
        print("  30-DAY DOMAIN SNIPER TRACKER REPORT")
        print("═"*60)
        print(f"  Domains alerted : {stats['total_alerted']}")
        print(f"  Registered      : {stats['registered']}  "
              f"({stats['conversion_rate']:.0%} of alerted)")
        print(f"  Sold/Flipped    : {stats['sold']}  "
              f"({stats['flip_rate']:.0%} of registered)")
        print(f"  Avg sale price  : ${stats['avg_sale_usd']:,.2f}")
        print(f"  Avg alert score : {stats['avg_score']}")
        print(f"  Best niches     : {stats['best_niches']}")
        print(f"  Active weights  : {SCORING_WEIGHTS}")
        print("═"*60 + "\n")

# ─────────────────────────── NETWORK ───────────────────────────
def http_get(url: str, timeout: int = 15) -> Optional[requests.Response]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    return None

def fetch_latest_commoncrawl_index():
    global DYNAMIC_CC_URL
    try:
        resp = http_get("https://index.commoncrawl.org/collinfo.json", timeout=10)
        if resp:
            data = resp.json()
            if isinstance(data, list) and data:
                api = data[0].get("cdx-api")
                if api:
                    DYNAMIC_CC_URL = api
                    log.info(f"CommonCrawl: using {DYNAMIC_CC_URL}")
    except Exception as e:
        log.warning(f"CommonCrawl index fetch failed: {e}")

def fetch_wayback_backlinks(domain: str) -> int:
    resp = http_get(
        f"http://web.archive.org/cdx/search/cdx"
        f"?url=*.{domain}&output=text&fl=urlkey&limit=400&collapse=urlkey"
    )
    if not resp or not resp.text:
        return 0
    refs = set()
    for line in resp.text.splitlines():
        parts = line.strip().split("/")
        if parts:
            refs.add(parts[0].replace(")", "").split(",")[-1])
    return len(refs)

def fetch_wayback_snapshots(domain: str) -> int:
    resp = http_get(
        f"http://web.archive.org/cdx/search/cdx"
        f"?url={domain}&output=json&fl=timestamp&limit=150"
    )
    if not resp:
        return 0
    try:
        return max(0, len(resp.json()) - 1)
    except Exception:
        return 0

def fetch_commoncrawl_presence(domain: str) -> int:
    resp = http_get(f"{DYNAMIC_CC_URL}?url={domain}&output=json&limit=5")
    if not resp or not resp.text:
        return 0
    return sum(1 for line in resp.text.strip().splitlines() if "url" in line)

def seed_namebio_cache(conn):
    if conn.execute("SELECT COUNT(1) FROM comps_cache").fetchone()[0] > 0:
        return
    url = ("https://raw.githubusercontent.com/GeekatPlay/"
           "NameBio-Scraper/master/sample_sales.csv")
    resp = http_get(url)
    if resp and resp.text:
        try:
            for line in resp.text.splitlines()[1:150]:
                parts = line.split(",")
                if len(parts) >= 3:
                    kw = parts[0].split(".")[0].lower().strip()
                    try:
                        price = float(parts[2].replace('"', "").strip())
                        conn.execute(
                            "INSERT OR IGNORE INTO comps_cache VALUES(?,?,?)",
                            (kw, price, 1))
                    except ValueError:
                        pass
            conn.commit()
        except Exception:
            pass

# ─────────────────────────── TREND RADAR ───────────────────────
class DynamicTrendRadar:
    STOP_WORDS = {"the","and","for","this","that","with","from","are",
                  "has","was","its","via","news","about"}

    def __init__(self, conn):
        self.conn = conn

    def _hn(self) -> Counter:
        c = Counter()
        try:
            r = requests.get(
                "https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=40",
                timeout=10)
            if r.status_code == 200:
                for hit in r.json().get("hits", []):
                    for token in re.findall(r"\b[a-z]{4,}\b", hit.get("title","").lower()):
                        if token not in self.STOP_WORDS:
                            c[token] += 2
        except Exception:
            pass
        return c

    def _reddit(self) -> Counter:
        c = Counter()
        headers = {"User-Agent": "Mozilla/5.0"}
        for sub in ["technology","artificial","SaaS","investing","quantum","biotech"][:4]:
            try:
                r = requests.get(
                    f"https://www.reddit.com/r/{sub}/hot.json?limit=12",
                    headers=headers, timeout=10)
                if r.status_code == 200:
                    for post in r.json().get("data",{}).get("children",[]):
                        for token in re.findall(r"\b[a-z]{4,}\b",
                                                post["data"].get("title","").lower()):
                            if token not in self.STOP_WORDS:
                                c[token] += 1
            except Exception:
                pass
        return c

    def execute_radar_scan(self, top_n: int = 10) -> List[Dict]:
        log.info("TrendRadar: scanning HN + Reddit...")
        combined = self._hn() + self._reddit()
        keywords = [
            k for k, _ in combined.most_common(top_n * 3)
            if k not in BLOCKED_BRAND_KEYWORDS
        ][:top_n]
        out = []
        for kw in keywords:
            out.append({"keyword": kw, "combined_signal": combined[kw],
                        "trend_pct": 25.0, "velocity": 1.5})
            put_cached(self.conn, "trend_cache", kw,
                       {"trend_pct": 25.0, "velocity": 1.5})
        return out

# ─────────────────────────── SENTIMENT ─────────────────────────
class InstitutionalSentimentEngine:
    def __init__(self, conn):
        self.conn = conn
        self.vader = SentimentIntensityAnalyzer() if VADER_OK else None

    def analyze_asset_sentiment(self, keyword: str) -> Dict:
        cached = get_cached(self.conn, "sentiment_cache", keyword)
        if cached:
            cached["sentiment_score"] = round(50.0 + (cached["compound"] * 50.0), 1)
            return cached
        headlines = []
        try:
            p = feedparser.parse(
                f"https://news.google.com/rss/search?q={quote_plus(keyword)}&hl=en-US&gl=US")
            headlines = [getattr(e, "title", "") for e in p.entries[:8]]
        except Exception:
            pass
        if not headlines or not self.vader:
            payload = {"compound": 0.0, "positive": 0.0, "negative": 0.0, "headline_count": 0}
            put_cached(self.conn, "sentiment_cache", keyword, payload, ttl_hours=4)
            payload["sentiment_score"] = 50.0
            return payload
        scores = [self.vader.polarity_scores(t) for t in headlines]
        avg_c = sum(s["compound"] for s in scores) / len(scores)
        avg_p = sum(s["pos"]      for s in scores) / len(scores)
        avg_n = sum(s["neg"]      for s in scores) / len(scores)
        payload = {
            "compound":       round(avg_c, 4),
            "positive":       round(avg_p, 4),
            "negative":       round(avg_n, 4),
            "headline_count": len(headlines),
        }
        put_cached(self.conn, "sentiment_cache", keyword, payload, ttl_hours=4)
        payload["sentiment_score"] = round(50.0 + (avg_c * 50.0), 1)
        return payload

# ─────────────────────────── USPTO ─────────────────────────────
class TrademarkGuard:
    @staticmethod
    def check(domain: str) -> Dict:
        sld = domain.split(".")[0].lower()
        if any(b in sld for b in BLOCKED_BRAND_KEYWORDS):
            return {"risk": "RISK", "matches": -1}
        if not ENABLE_TRADEMARK:
            return {"risk": "UNCHECKED", "matches": 0}
        try:
            r = requests.post(
                "https://api.uspto.gov/api/v1/trademark/cases/search",
                json={"q": f"trademarkName:{sld} AND caseStatus:live", "rows": 3},
                headers={"Content-Type": "application/json"}, timeout=6)
            if r.status_code == 200 and r.json().get("numFound", 0) > 0:
                return {"risk": "RISK", "matches": r.json()["numFound"]}
        except Exception:
            pass
        return {"risk": "CLEAR", "matches": 0}

# ─────────────────────────── SEO ───────────────────────────────
class SEOIntelligence:
    def __init__(self, conn):
        self.conn = conn

    def evaluate_arbitrage(self, domain: str, niche: str, age: int,
                           backlinks: int, cc_hits: int, tld: str) -> Dict:
        sld     = domain.split(".")[0].lower()
        keyword = sld.replace("-", " ")
        cached  = get_cached(self.conn, "seo_cache", keyword)
        if cached:
            return cached
        serp_e   = round(100.0 - min(100.0, (cc_hits * 5.0) + (backlinks * 0.5)), 1)
        tld_trust= {".com": 1.0, ".ai": 0.88, ".io": 0.85}.get(tld, 0.60)
        eeat     = round((min(100.0, age*8)*0.45)+(min(100.0, backlinks*2)*0.35)+(tld_trust*20), 1)
        cpc      = NICHE_CPC.get(niche, 0.50)
        seo_score= round((min(100.0, cpc*1.8)*0.3)+(eeat*0.4)+(serp_e*0.3), 1)
        payload  = {"cpc": cpc, "search_vol_proxy": 50.0, "serp_competition": serp_e,
                    "intent_class": "commercial", "seo_score": seo_score}
        put_cached(self.conn, "seo_cache", keyword, payload, ttl_hours=24)
        return payload

# ─────────────────────────── WHOIS ─────────────────────────────
def port43_whois_audit(domain: str) -> Tuple[bool, int]:
    tld = domain.split(".")[-1].lower()
    srv = {"com":"whois.verisign-grs.com","net":"whois.verisign-grs.com",
           "io":"whois.nic.io","co":"whois.nic.co","ai":"whois.nic.ai"}
    server = srv.get(tld, "whois.iana.org")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    try:
        s.connect((server, 43))
        q = f"domain {domain}\r\n" if tld in ("com","net") else f"{domain}\r\n"
        s.send(q.encode())
        raw = b""
        while True:
            chunk = s.recv(4096)
            if not chunk: break
            raw += chunk
        s.close()
        text = raw.decode("utf-8", errors="ignore")
        if any(p in text for p in ["No match for","NOT FOUND","Not Registered","No Data Found"]):
            return True, 0
        m = re.search(r"(?:Creation Date|created):\s*([^\s]+)", text, re.I)
        if m:
            dt = datetime.strptime(m.group(1)[:10].strip("T/."), "%Y-%m-%d")
            return False, max(0, (datetime.now() - dt).days // 365)
    except Exception:
        pass
    return False, 0

# ─────────────────────────── SCORING ───────────────────────────
def compute_brand_score(sld: str) -> float:
    has_hyphen = "-" in sld
    n = len(sld)
    if n <= 5 and not has_hyphen:  return 90.0
    elif n <= 7 and not has_hyphen: return 80.0
    elif n <= 10 and not has_hyphen: return 60.0
    elif n <= 12:                   return 42.0
    else:                           return 30.0

def compute_final_score(found: float, brand: float, sentiment: float,
                        tld_val: float, age: int, bl: int,
                        niche: str, sld: str) -> Tuple[int, int, int]:
    """Returns (final_score, base_score, bonus)"""
    W = SCORING_WEIGHTS
    base = int(
        (found    * W["found_score"])
      + (brand    * W["brand_score"])
      + (sentiment* W["sentiment_score"])
      + (tld_val  * W["tld_value"])
    )
    bonus = 0
    if age >= 5:             bonus += W["bonus_age"]
    if bl  >= 10:            bonus += W["bonus_backlinks"]
    if niche != "general":   bonus += W["bonus_niche"]
    if len(sld) <= 6 and "-" not in sld: bonus += W["bonus_short"]
    final = min(base + bonus, 100)
    return final, base, bonus

# ─────────────────────────── PROBABILITY ───────────────────────
class ProbabilityEngine:
    @staticmethod
    def sigmoid(x: float) -> float:
        try:    return 1.0 / (1.0 + math.exp(-x))
        except: return 0.0 if x < 0 else 1.0

    def p_flip_success(self, score: int, niche: str, age: int, bl: int) -> float:
        z = 0.095*score - 5.2 + 0.06*min(age,15) + 0.004*min(bl,250)
        return round(min(0.97, max(0.01, self.sigmoid(z))), 4)

    def monte_carlo(self, base: float, niche: str) -> Dict:
        base = max(15.0, base)
        sigma = 0.60
        mu    = math.log(base) - 0.5*sigma**2
        samples = []
        for _ in range(2000):
            u1, u2 = random.random(), random.random()
            z = math.sqrt(-2*math.log(u1)) * math.cos(2*math.pi*u2)
            samples.append(math.exp(mu + sigma*z))
        samples.sort()
        return {"p10": samples[200], "p50": samples[1000], "p90": samples[1800]}

    def kelly(self, p_win: float, p50: float) -> Dict:
        b = (p50 - 10.0) / 10.0
        if b <= 0:
            log.warning(f"Kelly b={b:.3f} — MC p50=${p50:.0f} too low for position sizing")
            return {"allocation_usd": 0.0, "f_star": 0.0, "verdict": "Pass"}
        f   = (b*p_win - (1-p_win)) / b
        f_s = round(max(0.0, min(0.25, f)), 4)
        verdict = "Strong Buy" if f_s > 0.10 else "Buy" if f_s > 0.04 else "Pass"
        return {"allocation_usd": round(f_s*KELLY_BANKROLL, 2), "f_star": f_s, "verdict": verdict}

def fetch_namebio_median(conn, keyword: str) -> float:
    row = conn.execute(
        "SELECT median_sale FROM comps_cache WHERE keyword=?", (keyword.lower(),)
    ).fetchone()
    return float(row[0]) if row else 0.0

# ─────────────────────────── WORTHINESS ────────────────────────
def worthiness_verdict(score: int, kelly_verdict: str, p_win: float,
                       mc_p50: float, reg_cost: float) -> Tuple[str, str]:
    """
    Returns (verdict_label, plain_english_explanation)
    """
    roi_multiple = mc_p50 / reg_cost if reg_cost > 0 else 0
    if score >= 90 and p_win >= 0.80 and roi_multiple >= 20:
        label = "🔥 STRONG BUY"
        reason = (f"Top-tier domain. Expected resale ~${mc_p50:,.0f} on a ${reg_cost:.0f} "
                  f"registration = {roi_multiple:.0f}x potential ROI. High flip probability.")
    elif score >= 85 and p_win >= 0.70 and roi_multiple >= 10:
        label = "✅ GOOD BUY"
        reason = (f"Solid opportunity. ~{roi_multiple:.0f}x ROI potential if sold at median "
                  f"estimate. Register and list on Sedo/Afternic immediately.")
    elif score >= 80 and roi_multiple >= 5:
        label = "⚠️ MARGINAL"
        reason = (f"Acceptable risk. ~{roi_multiple:.0f}x ROI potential but moderate "
                  f"flip probability ({p_win:.0%}). Only register if budget allows.")
    else:
        label = "❌ SKIP"
        reason = "Risk/reward not favourable at current bankroll size."
    return label, reason

# ─────────────────────────── TELEGRAM ──────────────────────────
def send_telegram(d: Dict):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    se = "🟢" if d["sentiment_compound"] > 0.1 else "🔴" if d["sentiment_compound"] < -0.1 else "⚪"
    inr = lambda x: f"₹{x*USD_TO_INR:,.0f}"

    msg = (
        f"🏆 *DOMAIN ALERT* — Score: {d['final_score']}/100\n"
        f"🌐 *{d['domain']}*\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*{d['worthiness_label']}*\n"
        f"_{d['worthiness_reason']}_\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"📌 *What each number means:*\n"
        f"\n"
        f"💳 *Registration Cost* — what you pay RIGHT NOW to own this domain:\n"
        f"   ${d['reg_cost_usd']:.2f} ({inr(d['reg_cost_usd'])} INR)\n"
        f"\n"
        f"📈 *Estimated Resale Range* — what you could SELL it for later:\n"
        f"   Low: ${d['mc_p10']:,.0f} ({inr(d['mc_p10'])})\n"
        f"   Mid: ${d['mc_p50']:,.0f} ({inr(d['mc_p50'])})\n"
        f"   High: ${d['mc_p90']:,.0f} ({inr(d['mc_p90'])})\n"
        f"   _(Monte Carlo simulation across 2,000 scenarios)_\n"
        f"\n"
        f"💰 *Suggested Max Spend* — from your ₹{KELLY_BANKROLL*USD_TO_INR:,.0f} bankroll,\n"
        f"   Kelly formula says risk at most:\n"
        f"   ${d['kelly_alloc_usd']:,.2f} ({inr(d['kelly_alloc_usd'])} INR)\n"
        f"   _Kelly verdict: {d['kelly_verdict']}_\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Niche: *{d['niche'].upper()}* │ Age: {d['age_years']}y\n"
        f"{se} Sentiment: {d['sentiment_score']:.0f}/100\n"
        f"📊 Flip probability: {d['p_flip_success']:.0%}\n"
        f"\n"
        f"🔗 [GoDaddy]({d['link_godaddy']}) │ "
        f"[Namecheap]({d['link_namecheap']}) │ "
        f"[Name.com]({d['link_name']}) │ "
        f"[Sedo]({d['link_sedo']})"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                  "parse_mode": "Markdown", "disable_web_page_preview": True},
            timeout=8,
        )
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ─────────────────────────── GOOGLE SHEETS ─────────────────────
def push_to_sheets(df: pd.DataFrame):
    if not GSPREAD_OK or not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID:
        return
    try:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(SHEET_NAME, 2000, 30)
        vals = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        ws.clear()
        ws.update(vals, value_input_option="RAW")
        log.info(f"Sheets: {len(df)} rows pushed")
    except Exception as e:
        log.error(f"Sheets error: {e}")

# ─────────────────────────── PROCESS DOMAIN ────────────────────
def process_domain(domain: str, source: str, conn,
                   seo: SEOIntelligence,
                   sent: InstitutionalSentimentEngine,
                   tm: TrademarkGuard) -> Optional[Dict]:
    log.info(f"⏳ Processing: {domain}")
    sld = domain.split(".")[0].lower()
    tld = "." + domain.split(".")[-1].lower()

    # ── cheap pre-filters ──
    if sld.count("-") > 1 or "--" in sld:
        return None
    if len(sld) > 12:
        return None
    if re.fullmatch(r"[a-z]+", sld) and not re.search(r"[aeiou]", sld):
        return None

    is_available, age = port43_whois_audit(domain)
    log.info(f"  WHOIS: available={is_available} age={age}y")

    dl_clean = sld.replace("-", "")
    niche = next((k for k in NICHE_SCORE if k in dl_clean), "general")

    tm_data = tm.check(domain)
    if tm_data["risk"] == "RISK":
        log.warning(f"  ❌ Trademark RISK — skip")
        return None

    try:    bl    = fetch_wayback_backlinks(domain)
    except: bl    = 3
    try:    snaps = fetch_wayback_snapshots(domain)
    except: snaps = 10
    try:    cc    = fetch_commoncrawl_presence(domain)
    except: cc    = 1
    log.info(f"  Footprint: bl={bl} snaps={snaps} cc={cc}")

    sent_data  = sent.analyze_asset_sentiment(sld)
    seo_data   = seo.evaluate_arbitrage(domain, niche, age, bl, cc, tld)
    comp_median= fetch_namebio_median(conn, sld)

    found_score = min(100.0, (bl/3.0)*32 + (cc*20)*26 + (age*5)*24)
    brand_score = compute_brand_score(sld)

    final_score, base_score, bonus = compute_final_score(
        found_score, brand_score,
        sent_data["sentiment_score"],
        TLD_VALUE.get(tld, DEFAULT_TLD),
        age, bl, niche, sld,
    )
    log.info(f"  Score: {final_score} (base={base_score}+bonus={bonus})")

    # ── STRICT GATE — only continue if score >= MIN_ALERT_SCORE ──
    if final_score < MIN_ALERT_SCORE:
        log.debug(f"  ⬇ {final_score} < {MIN_ALERT_SCORE} — skip")
        return None

    prob       = ProbabilityEngine()
    p_win      = prob.p_flip_success(final_score, niche, age, bl)
    mc_base    = max(comp_median, snaps*12.0, age*75.0)
    mc         = prob.monte_carlo(mc_base, niche)
    k          = prob.kelly(p_win, mc["p50"])

    reg_cost_usd = TLD_REG_COSTS.get(tld, DEFAULT_REG_COST)

    worth_label, worth_reason = worthiness_verdict(
        final_score, k["verdict"], p_win, mc["p50"], reg_cost_usd
    )

    gd_aff  = f"&isc={AFFILIATE_ID_GD}"  if AFFILIATE_ID_GD else ""
    nc_aff  = f"&AffiliateCode={AFFILIATE_ID_NC}" if AFFILIATE_ID_NC else ""

    return {
        "domain":            domain,
        "tld":               tld,
        "source":            source,
        "final_score":       final_score,
        "niche":             niche,
        "age_years":         age,
        "sentiment_compound":sent_data["compound"],
        "sentiment_score":   sent_data["sentiment_score"],
        "p_flip_success":    p_win,
        "mc_p10":            mc["p10"],
        "mc_p50":            mc["p50"],
        "mc_p90":            mc["p90"],
        "kelly_verdict":     k["verdict"],
        "kelly_alloc_usd":   k["allocation_usd"],
        "kelly_alloc_inr":   k["allocation_usd"] * USD_TO_INR,
        "reg_cost_usd":      reg_cost_usd,
        "reg_cost_inr":      reg_cost_usd * USD_TO_INR,
        "worthiness_label":  worth_label,
        "worthiness_reason": worth_reason,
        "brand_score":       brand_score,
        "backlinks":         bl,
        "link_godaddy":   f"https://www.godaddy.com/domainsearch/find?domainToCheck={domain}{gd_aff}",
        "link_namecheap": f"https://www.namecheap.com/domains/registration/results/?domain={domain}{nc_aff}",
        "link_name":      f"https://www.name.com/domain/search/{domain}",
        "link_sedo":      f"https://sedo.com/search/details/?domain={domain}",
    }

# ─────────────────────────── COMBINATORICS ─────────────────────
class QuantumCombinatoricsEngine:
    AFFIXES = ["get","buy","ai","lab","hub","pro"]

    def generate(self, trend_list: List[str], top_n: int = 30) -> List[Tuple[str,str]]:
        out = []
        for kw in trend_list[:15]:
            if kw in BLOCKED_BRAND_KEYWORDS:
                continue
            for affix in self.AFFIXES:
                for tld in [".com",".ai",".io"]:
                    out.append((f"{affix}{kw}{tld}", "combinatorics"))
                    out.append((f"{kw}{affix}{tld}", "combinatorics"))
        random.shuffle(out)
        return out[:top_n]

# ─────────────────────────── MAIN ──────────────────────────────
def main():
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info(f"═══ Domain Sniper v3 │ Run: {run_id} │ Min score: {MIN_ALERT_SCORE} ═══")

    fetch_latest_commoncrawl_index()
    conn = init_db()
    seed_namebio_cache(conn)

    # ── AI LEARNING: run monthly if data exists ──
    ai_engine = AILearningEngine(conn)
    ai_engine.apply_learned_weights()
    ai_engine.print_tracker_report()

    seo_engine  = SEOIntelligence(conn)
    sent_engine = InstitutionalSentimentEngine(conn)
    tm_guard    = TrademarkGuard()

    radar   = DynamicTrendRadar(conn)
    trends  = radar.execute_radar_scan(top_n=10)
    kws     = [t["keyword"] for t in trends]
    log.info(f"Trend keywords: {kws[:6]}")

    quantum = QuantumCombinatoricsEngine()
    pool    = quantum.generate(kws, top_n=30)

    results, seen = [], set()

    for d, src in pool:
        d_clean = d.strip().lower()
        if d_clean in seen or is_seen(conn, d_clean):
            continue
        seen.add(d_clean)

        res = process_domain(d_clean, src, conn, seo_engine, sent_engine, tm_guard)
        if not res:
            continue

        # res only exists if final_score >= MIN_ALERT_SCORE (gate is inside process_domain)
        log.info(f"🔥 ALERT │ {d_clean:30s} │ {res['final_score']}/100 │ {res['worthiness_label']}")

        # Track in 30-day outcome table
        track_domain_alert(conn, res, res["mc_p10"], res["mc_p50"], res["mc_p90"],
                           res["brand_score"], res["backlinks"])

        # Mark seen so we don't re-evaluate until data could meaningfully change
        mark_seen(conn, d_clean, res["final_score"], "flip")

        send_telegram(res)
        results.append(res)

    if results:
        df = pd.DataFrame(results).sort_values("final_score", ascending=False)
        df.to_csv(f"institutional_output_{run_id}.csv", index=False)
        push_to_sheets(df)

    log.info(f"Run complete. Alerted {len(results)} domains (all score ≥ {MIN_ALERT_SCORE}).")
    conn.close()


if __name__ == "__main__":
    main()
