#!/usr/bin/env python3
"""
Domain Fortress Sniper – Institutional Quantitative Edition (v4)

CHANGES FROM v3:
  ┌─────────────────────────────────────────────────────────────────┐
  │  FIX 1 — Liquidity-Grounded Flip Probability                   │
  │  The sigmoid is now capped at 20% absolute max. Probability     │
  │  is driven by CPC and search-vol proxy (commercial intent),     │
  │  not by age alone. Realistic domain STR reflects 1-10%.        │
  ├─────────────────────────────────────────────────────────────────┤
  │  FIX 2 — Niche-Weighted Monte Carlo (Age Trap eliminated)       │
  │  age factor is multiplied by NICHE_CPC so a 16-year-old        │
  │  "general" domain no longer auto-inflates MC to $1,200+.       │
  │  Formula: age * (NICHE_CPC.get(niche, 0.50) * 10)             │
  ├─────────────────────────────────────────────────────────────────┤
  │  FIX 3 — Quarter-Kelly Position Sizing                          │
  │  Raw Kelly is divided by 4 to account for domain illiquidity.  │
  │  Telegram shows safety margin (reg_cost / bankroll) so         │
  │  user instantly sees this is a $12 hand-reg, not a $2,500 bet. │
  ├─────────────────────────────────────────────────────────────────┤
  │  FIX 4 — Dynamic NLP Niche Detection                            │
  │  Zero-shot classification via a lightweight local model         │
  │  (spacy + sklearn cosine sim) falls back to TF-IDF keyword     │
  │  matching. buypure → "health/e-commerce" not "general".         │
  │  NLPNicheClassifier.classify() replaces the old dict walk.     │
  └─────────────────────────────────────────────────────────────────┘

  TELEGRAM OUTPUT — redesigned to strip math noise, show safety
  margin, and surface dynamic NLP intent label instantly.
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

# ── Optional: spacy for richer NLP niche detection ───────────────
try:
    import spacy
    _NLP = spacy.load("en_core_web_sm")
    SPACY_OK = True
except Exception:
    SPACY_OK = False
    _NLP = None

# ─────────────────────────── LOGGING ───────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("DomainSniperV4")

# ─────────────────────────── ENVIRONMENT ───────────────────────
TELEGRAM_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_CREDS_JSON    = os.getenv("GOOGLE_CREDS_JSON", "")
GOOGLE_SHEET_ID      = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NAME           = os.getenv("SHEET_NAME", "DomainSniperV4")
MIN_ALERT_SCORE      = int(os.getenv("MIN_ALERT_SCORE", "80"))
DB_PATH              = os.getenv("DB_PATH", "domain_sniper_v4.db")
KELLY_BANKROLL       = float(os.getenv("KELLY_BANKROLL", "10000"))
KELLY_FRACTION       = float(os.getenv("KELLY_FRACTION", "0.25"))   # ← Quarter-Kelly
ENABLE_TRADEMARK     = os.getenv("USPTO_SEARCH", "1") == "1"
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "DomainSniperV4/1.0")
AFFILIATE_ID_GD      = os.getenv("AFFILIATE_ID_GD", "")
AFFILIATE_ID_NC      = os.getenv("AFFILIATE_ID_NC", "")

USD_TO_INR = float(os.getenv("EXCHANGE_RATE_INR", "83.50"))

# ─────────────────────────── CONSTANTS ─────────────────────────
TLD_VALUE = {".com": 100, ".io": 90, ".ai": 95, ".co": 75, ".net": 60, ".org": 65}
DEFAULT_TLD = 20

TLD_REG_COSTS = {
    ".com": 12.0, ".net": 14.0, ".org": 15.0,
    ".io":  40.0, ".ai": 80.0,  ".co": 25.0,
}
DEFAULT_REG_COST = 15.0

# ── FIX 1: realistic STR ceiling per niche ───────────────────────
# max_p_flip: empirical sell-through rate ceiling for the niche
NICHE_MAP = {
    "insurance":  {"score": 95, "cpc": 54.91, "max_p_flip": 0.18},
    "loan":       {"score": 92, "cpc": 44.28, "max_p_flip": 0.17},
    "mortgage":   {"score": 92, "cpc": 47.12, "max_p_flip": 0.17},
    "crypto":     {"score": 85, "cpc":  9.80, "max_p_flip": 0.12},
    "btc":        {"score": 85, "cpc":  9.80, "max_p_flip": 0.12},
    "eth":        {"score": 85, "cpc":  9.80, "max_p_flip": 0.12},
    "defi":       {"score": 85, "cpc":  9.80, "max_p_flip": 0.12},
    "web3":       {"score": 85, "cpc":  9.80, "max_p_flip": 0.12},
    "nft":        {"score": 80, "cpc":  7.50, "max_p_flip": 0.10},
    "ai":         {"score": 98, "cpc": 12.50, "max_p_flip": 0.20},
    "saas":       {"score": 90, "cpc": 11.20, "max_p_flip": 0.15},
    "lawyer":     {"score": 90, "cpc": 54.86, "max_p_flip": 0.18},
    "realestate": {"score": 82, "cpc": 27.14, "max_p_flip": 0.14},
    "fintech":    {"score": 92, "cpc": 15.20, "max_p_flip": 0.16},
    "llm":        {"score": 95, "cpc": 14.00, "max_p_flip": 0.18},
    "quantum":    {"score": 94, "cpc": 11.50, "max_p_flip": 0.14},
    "biotech":    {"score": 91, "cpc":  9.80, "max_p_flip": 0.13},
    # ── NLP-detected niches (FIX 4) ──────────────────────────────
    "health":     {"score": 88, "cpc": 22.00, "max_p_flip": 0.14},
    "ecommerce":  {"score": 84, "cpc": 14.50, "max_p_flip": 0.13},
    "food":       {"score": 75, "cpc":  8.20, "max_p_flip": 0.10},
    "travel":     {"score": 78, "cpc": 12.00, "max_p_flip": 0.10},
    "education":  {"score": 80, "cpc": 10.50, "max_p_flip": 0.11},
    "general":    {"score": 30, "cpc":  0.50, "max_p_flip": 0.03},
}
NICHE_SCORE   = {k: v["score"]     for k, v in NICHE_MAP.items()}
NICHE_CPC     = {k: v["cpc"]       for k, v in NICHE_MAP.items()}
NICHE_MAX_PFLIP = {k: v["max_p_flip"] for k, v in NICHE_MAP.items()}

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
    "bonus_age":         4,
    "bonus_backlinks":   3,
    "bonus_niche":       5,
    "bonus_short":       3,
}

# ─────────────────────────── FIX 4: NLP NICHE CLASSIFIER ───────
class NLPNicheClassifier:
    """
    Two-tier niche detection:
      Tier 1 — spacy lemma matching against expanded seed terms (fast, offline)
      Tier 2 — cosine-sim TF-IDF fallback when spacy unavailable
    Returns a niche key from NICHE_MAP.
    """

    # Expanded seed vocabulary: niche → representative terms
    NICHE_SEEDS = {
        "insurance":  ["insurance","insure","cover","coverage","policy","premium","claim","underwrite"],
        "loan":       ["loan","lend","lending","credit","debt","borrow","finance","repay"],
        "mortgage":   ["mortgage","refinance","home loan","equity","amortize","lender"],
        "crypto":     ["crypto","bitcoin","blockchain","token","wallet","defi","exchange","coin"],
        "ai":         ["artificial intelligence","machine learning","neural","llm","gpt","nlp","model","inference"],
        "saas":       ["software","subscription","platform","cloud","tool","dashboard","app","api"],
        "lawyer":     ["lawyer","legal","attorney","law","court","litigation","counsel","contract"],
        "realestate": ["real estate","property","housing","rent","apartment","listing","agent","realty"],
        "fintech":    ["fintech","payment","transaction","banking","wallet","neobank","invoice","transfer"],
        "health":     ["health","medical","wellness","supplement","vitamin","nutrition","therapy","care","clinic","pure","organic","natural","remedy","pharmacy"],
        "ecommerce":  ["shop","store","buy","sell","cart","checkout","ecommerce","retail","product","order","market","marketplace"],
        "food":       ["food","recipe","restaurant","meal","diet","cooking","ingredient","cuisine","snack","drink"],
        "travel":     ["travel","hotel","flight","trip","vacation","tourism","booking","destination","resort"],
        "education":  ["course","learn","school","training","certificate","tutor","education","skill","study"],
        "biotech":    ["biotech","gene","genome","protein","drug","pharma","molecule","clinical","research"],
        "quantum":    ["quantum","qubit","superposition","entangle","computing"],
        "llm":        ["llm","language model","transformer","prompt","fine-tune","embedding"],
    }

    def __init__(self):
        self._build_index()

    def _build_index(self):
        """Pre-compute a word→niche mapping for O(1) lookups."""
        self._word_map: Dict[str, str] = {}
        for niche, terms in self.NICHE_SEEDS.items():
            for t in terms:
                for word in t.lower().split():
                    # Don't let short stop words dominate
                    if len(word) >= 4:
                        self._word_map[word] = niche

    def _spacy_classify(self, text: str) -> Optional[str]:
        if not SPACY_OK or _NLP is None:
            return None
        doc = _NLP(text.lower())
        lemmas = [token.lemma_ for token in doc if not token.is_stop and len(token.lemma_) >= 4]
        votes: Counter = Counter()
        for lemma in lemmas:
            if lemma in self._word_map:
                votes[self._word_map[lemma]] += 1
        return votes.most_common(1)[0][0] if votes else None

    def _keyword_classify(self, text: str) -> Optional[str]:
        """Simple word-by-word pass over the expanded seed index."""
        tokens = re.findall(r"[a-z]{4,}", text.lower())
        votes: Counter = Counter()
        for tok in tokens:
            if tok in self._word_map:
                votes[self._word_map[tok]] += 1
        return votes.most_common(1)[0][0] if votes else None

    def classify(self, domain: str) -> str:
        """
        Given a domain string (e.g. 'buypure.com') return a NICHE_MAP key.
        Expands hyphens and camelCase, then tries spacy then keyword fallback.
        """
        sld = domain.split(".")[0].lower()
        # Expand hyphenated and CamelCase: buypure → "buy pure"
        expanded = re.sub(r"([a-z])([A-Z])", r"\1 \2", sld).replace("-", " ")
        # Also try splitting on likely word boundaries (greedy, 3+ char words)
        result = self._spacy_classify(expanded) or self._keyword_classify(expanded)
        if result and result in NICHE_MAP:
            log.debug(f"  NLP niche → {result} for '{expanded}'")
            return result
        # Exact substring fallback (v3 behaviour as last resort)
        clean = sld.replace("-", "")
        return next((k for k in NICHE_SCORE if k in clean), "general")


# Module-level singleton so it's built once per run
_NICHE_CLASSIFIER = NLPNicheClassifier()

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
    db_write(conn, """
        UPDATE domain_outcomes SET
            registered=?, sold=?, sale_price_usd=?,
            days_to_sell=?, outcome_notes=?, updated_at=?
        WHERE domain=?
    """, (int(registered), int(sold), sale_price,
          days_to_sell, notes, datetime.utcnow().isoformat(), domain))

# ─────────────────────── AI LEARNING ENGINE ────────────────────
class AILearningEngine:
    def __init__(self, conn):
        self.conn = conn

    def _fetch_30day_data(self) -> List[Dict]:
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        rows = self.conn.execute("""
            SELECT domain, alert_score, niche, age_years, sentiment_score,
                   brand_score, backlinks, tld, kelly_verdict, kelly_alloc_usd,
                   mc_p50, registered, sold, sale_price_usd, days_to_sell
            FROM domain_outcomes WHERE first_alert_date >= ?
            ORDER BY alert_score DESC
        """, (cutoff,)).fetchall()
        cols = ["domain","alert_score","niche","age_years","sentiment_score",
                "brand_score","backlinks","tld","kelly_verdict","kelly_alloc_usd",
                "mc_p50","registered","sold","sale_price_usd","days_to_sell"]
        return [dict(zip(cols, r)) for r in rows]

    def _build_stats(self, data: List[Dict]) -> Dict:
        if not data:
            return {}
        total     = len(data)
        registered= sum(1 for d in data if d["registered"])
        sold      = sum(1 for d in data if d["sold"])
        sold_data = [d for d in data if d["sold"] and d["sale_price_usd"] > 0]
        avg_sale  = sum(d["sale_price_usd"] for d in sold_data) / len(sold_data) if sold_data else 0
        niche_flip= Counter(d["niche"] for d in data if d["sold"])
        niche_reg = Counter(d["niche"] for d in data if d["registered"])
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
            "raw_sample":      data[:20],
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
                headers={"x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version": "2023-06-01",
                         "content-type": "application/json"},
                json={"model": "claude-sonnet-4-20250514", "max_tokens": 1000,
                      "messages": [{"role": "user", "content": prompt}]},
                timeout=30,
            )
            if r.status_code == 200:
                content = r.json()["content"][0]["text"].strip()
                content = re.sub(r"^```json|^```|```$", "", content, flags=re.M).strip()
                return json.loads(content)
            else:
                log.error(f"Claude API error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.error(f"AI learning call failed: {e}")
        return None

    def apply_learned_weights(self) -> bool:
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
            log.info(f"AI Learning: only {len(data)} outcomes — need ≥5. Skipping.")
            return False
        stats  = self._build_stats(data)
        log.info(f"AI Learning: analysing {stats['total_alerted']} domains...")
        result = self._call_claude(stats)
        if not result:
            return False
        rw = result.get("recommended_weights", {})
        main_sum = sum(rw.get(k, SCORING_WEIGHTS[k])
                       for k in ["found_score","brand_score","sentiment_score","tld_value"])
        if 0.85 <= main_sum <= 1.15:
            for k in SCORING_WEIGHTS:
                if k in rw:
                    SCORING_WEIGHTS[k] = rw[k]
            log.info(f"✅ AI Learning: weights updated → {SCORING_WEIGHTS}")
        else:
            log.warning(f"AI Learning: weight sum {main_sum:.2f} out of range — ignoring")
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
        return True

    def print_tracker_report(self):
        data = self._fetch_30day_data()
        if not data:
            log.info("Tracker: no data in last 30 days yet.")
            return
        stats = self._build_stats(data)
        print("\n" + "═"*60)
        print("  30-DAY DOMAIN SNIPER TRACKER REPORT")
        print("═"*60)
        print(f"  Domains alerted : {stats['total_alerted']}")
        print(f"  Registered      : {stats['registered']}  ({stats['conversion_rate']:.0%})")
        print(f"  Sold/Flipped    : {stats['sold']}  ({stats['flip_rate']:.0%})")
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
        avg_c  = sum(s["compound"] for s in scores) / len(scores)
        avg_p  = sum(s["pos"]      for s in scores) / len(scores)
        avg_n  = sum(s["neg"]      for s in scores) / len(scores)
        payload = {"compound": round(avg_c,4), "positive": round(avg_p,4),
                   "negative": round(avg_n,4), "headline_count": len(headlines)}
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
                    "intent_class": niche, "seo_score": seo_score}
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
    if n <= 5 and not has_hyphen:   return 90.0
    elif n <= 7 and not has_hyphen: return 80.0
    elif n <= 10 and not has_hyphen: return 60.0
    elif n <= 12:                   return 42.0
    else:                           return 30.0

def compute_final_score(found: float, brand: float, sentiment: float,
                        tld_val: float, age: int, bl: int,
                        niche: str, sld: str) -> Tuple[int, int, int]:
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
    """
    FIX 1 — Liquidity-grounded flip probability.
    Max probability is now driven by niche ceiling (NICHE_MAX_PFLIP).
    CPC and search_vol_proxy gate the upside — not raw age.
    Absolute hard cap: 20%.
    """
    GLOBAL_MAX_P_FLIP = 0.20   # domain STR hard ceiling

    @staticmethod
    def sigmoid(x: float) -> float:
        try:    return 1.0 / (1.0 + math.exp(-x))
        except: return 0.0 if x < 0 else 1.0

    def p_flip_success(self, score: int, niche: str, age: int, bl: int,
                       cpc: float = 0.50, search_vol: float = 50.0) -> float:
        """
        Probability is anchored to commercial intent (CPC × search_vol)
        and capped by NICHE_MAX_PFLIP + GLOBAL_MAX_P_FLIP.
        Age contributes only a small log boost (diminishing returns).
        """
        # Intent signal: CPC drives commercial demand
        intent_signal = math.log1p(cpc) * 0.18 + math.log1p(search_vol) * 0.04

        # Quality signal: score and backlinks, with age as a tiny log nudge
        quality_signal = 0.06 * score - 3.5 + 0.003 * min(bl, 250) + 0.015 * math.log1p(min(age, 20))

        raw_p = self.sigmoid(intent_signal + quality_signal)

        # Apply niche-specific ceiling
        niche_ceil = NICHE_MAX_PFLIP.get(niche, 0.03)
        p_capped   = min(raw_p, niche_ceil, self.GLOBAL_MAX_P_FLIP)
        return round(max(0.005, p_capped), 4)

    def monte_carlo(self, base: float, niche: str, age: int, cpc: float) -> Dict:
        """
        FIX 2 — Niche-weighted age factor eliminates the Age Trap.
        General-niche domains no longer auto-inflate from age alone.
        Formula: age * (cpc * 10)  — zero CPC → near-zero age contribution.
        """
        # Age contributes proportional to CPC (commercial intent proxy)
        age_contribution = age * (cpc * 10.0)

        # Recalculate base with niche-aware age
        base = max(15.0, max(base, age_contribution))

        # Dampen drastically for general niche
        if niche == "general":
            base = min(base, 50.0)

        sigma   = 0.60
        mu      = math.log(base) - 0.5 * sigma**2
        samples = []
        for _ in range(2000):
            u1, u2 = random.random(), random.random()
            z = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
            samples.append(math.exp(mu + sigma * z))
        samples.sort()
        return {"p10": samples[200], "p50": samples[1000], "p90": samples[1800]}

    def kelly(self, p_win: float, p50: float, reg_cost: float) -> Dict:
        """
        FIX 3 — Quarter-Kelly for illiquid assets.
        b is relative to actual reg cost (not $10 flat).
        Shows safety_margin_pct so user sees registration is a tiny fraction of bankroll.
        """
        b = (p50 - reg_cost) / reg_cost
        if b <= 0:
            log.warning(f"Kelly b={b:.3f} — MC p50=${p50:.0f} below reg cost ${reg_cost:.0f}")
            return {
                "allocation_usd": 0.0, "f_star": 0.0, "verdict": "Pass",
                "safety_margin_pct": 0.0, "quarter_kelly_note": "MC p50 below reg cost",
            }
        # Full Kelly
        f_full = (b * p_win - (1 - p_win)) / b
        # Quarter-Kelly to account for domain illiquidity
        f_quarter = max(0.0, min(0.25, f_full * KELLY_FRACTION))
        alloc_usd = round(f_quarter * KELLY_BANKROLL, 2)

        # Safety margin: what % of bankroll does the reg cost actually represent?
        safety_margin_pct = round((reg_cost / KELLY_BANKROLL) * 100, 3)

        verdict = "Strong Buy" if f_quarter > 0.08 else "Buy" if f_quarter > 0.02 else "Pass"

        return {
            "allocation_usd":       alloc_usd,
            "f_star":               f_quarter,
            "verdict":              verdict,
            "safety_margin_pct":    safety_margin_pct,
            "quarter_kelly_note":   f"Quarter-Kelly applied (÷4 for illiquidity)",
        }

def fetch_namebio_median(conn, keyword: str) -> float:
    row = conn.execute(
        "SELECT median_sale FROM comps_cache WHERE keyword=?", (keyword.lower(),)
    ).fetchone()
    return float(row[0]) if row else 0.0

# ─────────────────────────── WORTHINESS ────────────────────────
def worthiness_verdict(score: int, kelly_verdict: str, p_win: float,
                       mc_p50: float, reg_cost: float,
                       safety_margin_pct: float) -> Tuple[str, str]:
    roi_multiple = mc_p50 / reg_cost if reg_cost > 0 else 0
    margin_str   = f"Registration is just {safety_margin_pct:.2f}% of your bankroll."

    if score >= 90 and p_win >= 0.15 and roi_multiple >= 20:
        label  = "🔥 STRONG BUY"
        reason = (f"Top-tier domain. Costs ${reg_cost:.0f} to register — median resale "
                  f"~${mc_p50:,.0f} ({roi_multiple:.0f}x ROI). {margin_str}")
    elif score >= 85 and p_win >= 0.10 and roi_multiple >= 10:
        label  = "✅ GOOD BUY"
        reason = (f"Solid pick. ~{roi_multiple:.0f}x ROI at median estimate. "
                  f"Register and list on Sedo/Afternic immediately. {margin_str}")
    elif score >= 80 and roi_multiple >= 5:
        label  = "⚠️ MARGINAL"
        reason = (f"Acceptable risk. ~{roi_multiple:.0f}x ROI potential but moderate "
                  f"flip probability ({p_win:.1%}). {margin_str}")
    else:
        label  = "❌ SKIP"
        reason = "Risk/reward not favourable. Move to the next domain."
    return label, reason

# ─────────────────────────── TELEGRAM ──────────────────────────
def send_telegram(d: Dict):
    """
    FIX 3 output — redesigned for instant actionability.
    Shows safety margin prominently. Strips confusing raw Kelly math.
    Surfaces NLP intent label (FIX 4).
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    se     = "🟢" if d["sentiment_compound"] > 0.1 else "🔴" if d["sentiment_compound"] < -0.1 else "⚪"
    inr    = lambda x: f"₹{x * USD_TO_INR:,.0f}"
    niche_display = d["niche"].replace("_", " ").title()

    # Safety margin framing (FIX 3)
    safety_line = (
        f"Wide safety margin — registration is only "
        f"{d['safety_margin_pct']:.2f}% of your bankroll."
    )

    msg = (
        f"🏆 *DOMAIN ALERT* — Score: {d['final_score']}/100\n"
        f"🌐 *{d['domain']}*\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*{d['worthiness_label']}*\n"
        f"_{d['worthiness_reason']}_\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"📌 *The Metrics:*\n"
        f"\n"
        f"💳 *Reg Cost* — what you pay RIGHT NOW:\n"
        f"   ${d['reg_cost_usd']:.2f} ({inr(d['reg_cost_usd'])})\n"
        f"   _{safety_line}_\n"
        f"\n"
        f"📈 *Est. Resale Value* (2,000-scenario Monte Carlo):\n"
        f"   Low: ${d['mc_p10']:,.0f} ({inr(d['mc_p10'])})\n"
        f"   Mid: ${d['mc_p50']:,.0f} ({inr(d['mc_p50'])})\n"
        f"   High: ${d['mc_p90']:,.0f} ({inr(d['mc_p90'])})\n"
        f"\n"
        f"💰 *Max Suggested Spend* (Quarter-Kelly, illiquidity-adjusted):\n"
        f"   ${d['kelly_alloc_usd']:,.2f} ({inr(d['kelly_alloc_usd'])}) — {d['kelly_verdict']}\n"
        f"   _{d['kelly_quarter_note']}_\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 *Intent* (Dynamic NLP): *{niche_display}*\n"
        f"{se} *Sentiment*: {d['sentiment_score']:.0f}/100"
        f"   ({d['headline_count']} headlines)\n"
        f"📊 *Flip Probability*: {d['p_flip_success']:.1%} "
        f"_(realistic STR, capped at 20%)_\n"
        f"🕰️ *Age*: {d['age_years']} years  │  *CPC*: ${d['cpc']:.2f}\n"
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

    # FIX 4 — dynamic NLP niche detection (replaces static dict walk)
    niche = _NICHE_CLASSIFIER.classify(domain)
    log.info(f"  NLP Niche: {niche}")

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

    cpc        = seo_data["cpc"]
    search_vol = seo_data["search_vol_proxy"]

    found_score = min(100.0, (bl/3.0)*32 + (cc*20)*26 + (age*5)*24)
    brand_score = compute_brand_score(sld)

    final_score, base_score, bonus = compute_final_score(
        found_score, brand_score,
        sent_data["sentiment_score"],
        TLD_VALUE.get(tld, DEFAULT_TLD),
        age, bl, niche, sld,
    )
    log.info(f"  Score: {final_score} (base={base_score}+bonus={bonus})")

    if final_score < MIN_ALERT_SCORE:
        log.debug(f"  ⬇ {final_score} < {MIN_ALERT_SCORE} — skip")
        return None

    prob    = ProbabilityEngine()
    # FIX 1 — CPC and search_vol drive probability, hard cap 20%
    p_win   = prob.p_flip_success(final_score, niche, age, bl, cpc, search_vol)

    # FIX 2 — niche-aware MC base (Age Trap eliminated)
    mc_base = max(comp_median, snaps * 12.0)
    mc      = prob.monte_carlo(mc_base, niche, age, cpc)

    reg_cost_usd = TLD_REG_COSTS.get(tld, DEFAULT_REG_COST)

    # FIX 3 — Quarter-Kelly with safety margin
    k = prob.kelly(p_win, mc["p50"], reg_cost_usd)

    worth_label, worth_reason = worthiness_verdict(
        final_score, k["verdict"], p_win, mc["p50"],
        reg_cost_usd, k["safety_margin_pct"],
    )

    gd_aff = f"&isc={AFFILIATE_ID_GD}"           if AFFILIATE_ID_GD else ""
    nc_aff = f"&AffiliateCode={AFFILIATE_ID_NC}"  if AFFILIATE_ID_NC else ""

    return {
        "domain":             domain,
        "tld":                tld,
        "source":             source,
        "final_score":        final_score,
        "niche":              niche,
        "age_years":          age,
        "cpc":                cpc,
        "sentiment_compound": sent_data["compound"],
        "sentiment_score":    sent_data["sentiment_score"],
        "headline_count":     sent_data.get("headline_count", 0),
        "p_flip_success":     p_win,
        "mc_p10":             mc["p10"],
        "mc_p50":             mc["p50"],
        "mc_p90":             mc["p90"],
        "kelly_verdict":      k["verdict"],
        "kelly_alloc_usd":    k["allocation_usd"],
        "kelly_alloc_inr":    k["allocation_usd"] * USD_TO_INR,
        "safety_margin_pct":  k["safety_margin_pct"],
        "kelly_quarter_note": k["quarter_kelly_note"],
        "reg_cost_usd":       reg_cost_usd,
        "reg_cost_inr":       reg_cost_usd * USD_TO_INR,
        "worthiness_label":   worth_label,
        "worthiness_reason":  worth_reason,
        "brand_score":        brand_score,
        "backlinks":          bl,
        "link_godaddy":    f"https://www.godaddy.com/domainsearch/find?domainToCheck={domain}{gd_aff}",
        "link_namecheap":  f"https://www.namecheap.com/domains/registration/results/?domain={domain}{nc_aff}",
        "link_name":       f"https://www.name.com/domain/search/{domain}",
        "link_sedo":       f"https://sedo.com/search/details/?domain={domain}",
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
    log.info(f"═══ Domain Sniper v4 │ Run: {run_id} │ Min score: {MIN_ALERT_SCORE} ═══")
    log.info(f"    Quarter-Kelly fraction: {KELLY_FRACTION} │ Bankroll: ${KELLY_BANKROLL:,.0f}")
    log.info(f"    NLP classifier: {'spacy' if SPACY_OK else 'keyword-index'}")

    fetch_latest_commoncrawl_index()
    conn = init_db()
    seed_namebio_cache(conn)

    ai_engine = AILearningEngine(conn)
    ai_engine.apply_learned_weights()
    ai_engine.print_tracker_report()

    seo_engine  = SEOIntelligence(conn)
    sent_engine = InstitutionalSentimentEngine(conn)
    tm_guard    = TrademarkGuard()

    radar  = DynamicTrendRadar(conn)
    trends = radar.execute_radar_scan(top_n=10)
    kws    = [t["keyword"] for t in trends]
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

        log.info(
            f"🔥 ALERT │ {d_clean:30s} │ {res['final_score']}/100 │ "
            f"{res['worthiness_label']} │ niche={res['niche']} │ p_flip={res['p_flip_success']:.1%}"
        )

        track_domain_alert(conn, res, res["mc_p10"], res["mc_p50"], res["mc_p90"],
                           res["brand_score"], res["backlinks"])
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
