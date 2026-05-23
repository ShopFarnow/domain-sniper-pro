cat > /mnt/user-data/outputs/domain_sniper_v5.py << 'PYEOF'
#!/usr/bin/env python3
"""
Domain Fortress Sniper – Institutional Quantitative Edition (v5)

═══════════════════════════════════════════════════════════════════
CHANGES FROM v4
═══════════════════════════════════════════════════════════════════

  CHANGE 1 — SAVE EVERY SCAN, ALERT ONLY SCORE > 85
  ──────────────────────────────────────────────────
  • All domains evaluated (any score) are written to `daily_scans`
    table immediately after scoring. This table is the raw data
    warehouse — no score gate, no filtering.
  • Telegram alerts fire ONLY when final_score > 85 (strict >, not ≥).
  • domain_outcomes (the outcome tracker) is written only for alerted
    domains (score > 85), since those are the ones we might register.
  • The separation means: daily_scans feeds the monthly AI analysis
    with the full picture; domain_outcomes tracks our real decisions.

  CHANGE 2 — ENHANCED MONTHLY AI ANALYSIS (3 new pillars)
  ─────────────────────────────────────────────────────────
  Pillar A: Sale Price Analysis
    Claude now receives sold domain prices, average days-to-sell,
    price distribution by niche, and ROI multiples. It returns
    price benchmarks and niche-specific sell targets.

  Pillar B: Trend-Based Forward Domain Projections
    The radar scans the last 30 days of trending keywords from
    daily_scans. Claude cross-references those with macro news
    signals (e.g. "jio", "starlink", "gemini", "sora") to project
    which domain patterns are likely to spike in value in the next
    30-90 days and recommends specific domain names to watch/register
    proactively.

  Pillar C: System Improvement Recommendations
    Claude audits the scoring weights, niche miss-rate (domains that
    slipped through as "general"), flip-rate by TLD, and sentiment
    accuracy. It returns concrete code-level suggestions (e.g.
    "raise bonus_niche from 5 → 8", "add 'robotics' to NICHE_MAP").

  All three pillars are saved to `learning_snapshots` and pushed to
  Telegram as a monthly digest message (separate from domain alerts).
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
log = logging.getLogger("DomainSniperV5")

# ─────────────────────────── ENVIRONMENT ───────────────────────
TELEGRAM_TOKEN       = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_CREDS_JSON    = os.getenv("GOOGLE_CREDS_JSON", "")
GOOGLE_SHEET_ID      = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NAME           = os.getenv("SHEET_NAME", "DomainSniperV5")

# ── TWO separate score thresholds ──────────────────────────────
SAVE_ALL_SCORE       = int(os.getenv("SAVE_ALL_SCORE",   "0"))   # save everything ≥ 0
TELEGRAM_MIN_SCORE   = int(os.getenv("TELEGRAM_MIN_SCORE", "85")) # alert only when > 85

DB_PATH              = os.getenv("DB_PATH", "domain_sniper_v5.db")
KELLY_BANKROLL       = float(os.getenv("KELLY_BANKROLL", "10000"))
KELLY_FRACTION       = float(os.getenv("KELLY_FRACTION", "0.25"))
ENABLE_TRADEMARK     = os.getenv("USPTO_SEARCH", "1") == "1"
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
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
    "health":     {"score": 88, "cpc": 22.00, "max_p_flip": 0.14},
    "ecommerce":  {"score": 84, "cpc": 14.50, "max_p_flip": 0.13},
    "food":       {"score": 75, "cpc":  8.20, "max_p_flip": 0.10},
    "travel":     {"score": 78, "cpc": 12.00, "max_p_flip": 0.10},
    "education":  {"score": 80, "cpc": 10.50, "max_p_flip": 0.11},
    "general":    {"score": 30, "cpc":  0.50, "max_p_flip": 0.03},
}
NICHE_SCORE     = {k: v["score"]     for k, v in NICHE_MAP.items()}
NICHE_CPC       = {k: v["cpc"]       for k, v in NICHE_MAP.items()}
NICHE_MAX_PFLIP = {k: v["max_p_flip"] for k, v in NICHE_MAP.items()}

BLOCKED_BRAND_KEYWORDS = {
    "microsoft","google","apple","amazon","meta","openai","tesla","nvidia",
    "twitter","netflix","adobe","salesforce","oracle","facebook","instagram",
    "youtube","tiktok","linkedin","spotify","uber","airbnb","shopify","stripe",
    "cloudflare","databricks","palantir","snowflake","atlassian","twilio",
    "sendgrid","hubspot","zendesk","docusign","zoom","slack","notion","figma",
}

DYNAMIC_CC_URL = "https://index.commoncrawl.org/CC-MAIN-2024-10-index"

SCORING_WEIGHTS = {
    "found_score":     0.25,
    "brand_score":     0.25,
    "sentiment_score": 0.30,
    "tld_value":       0.20,
    "bonus_age":       4,
    "bonus_backlinks": 3,
    "bonus_niche":     5,
    "bonus_short":     3,
}

# ─────────────────────────── NLP NICHE CLASSIFIER ──────────────
class NLPNicheClassifier:
    NICHE_SEEDS = {
        "insurance":  ["insurance","insure","cover","coverage","policy","premium","claim"],
        "loan":       ["loan","lend","lending","credit","debt","borrow","finance","repay"],
        "mortgage":   ["mortgage","refinance","equity","amortize","lender"],
        "crypto":     ["crypto","bitcoin","blockchain","token","wallet","defi","coin"],
        "ai":         ["intelligence","machine","learning","neural","llm","gpt","inference","artificial"],
        "saas":       ["software","subscription","platform","cloud","dashboard","api"],
        "lawyer":     ["lawyer","legal","attorney","court","litigation","counsel"],
        "realestate": ["property","housing","rent","apartment","listing","realty","estate"],
        "fintech":    ["fintech","payment","banking","neobank","invoice","transfer"],
        "health":     ["health","medical","wellness","supplement","vitamin","nutrition",
                       "therapy","clinic","pure","organic","natural","remedy","pharmacy"],
        "ecommerce":  ["shop","store","buy","sell","cart","checkout","retail","market"],
        "food":       ["food","recipe","restaurant","meal","diet","cooking","snack"],
        "travel":     ["travel","hotel","flight","trip","vacation","tourism","resort"],
        "education":  ["course","learn","school","training","certificate","tutor","study"],
        "biotech":    ["biotech","gene","genome","protein","drug","pharma","clinical"],
        "quantum":    ["quantum","qubit","superposition","entangle"],
        "llm":        ["llm","language","transformer","prompt","embedding","finetune"],
        "fintech":    ["fintech","payment","banking","neobank","invoice"],
    }

    def __init__(self):
        self._word_map: Dict[str, str] = {}
        for niche, terms in self.NICHE_SEEDS.items():
            for t in terms:
                for word in t.lower().split():
                    if len(word) >= 4:
                        self._word_map[word] = niche

    def _spacy_classify(self, text: str) -> Optional[str]:
        if not SPACY_OK or _NLP is None:
            return None
        doc   = _NLP(text.lower())
        votes: Counter = Counter()
        for token in doc:
            if not token.is_stop and len(token.lemma_) >= 4:
                if token.lemma_ in self._word_map:
                    votes[self._word_map[token.lemma_]] += 1
        return votes.most_common(1)[0][0] if votes else None

    def _keyword_classify(self, text: str) -> Optional[str]:
        tokens = re.findall(r"[a-z]{4,}", text.lower())
        votes: Counter = Counter()
        for tok in tokens:
            if tok in self._word_map:
                votes[self._word_map[tok]] += 1
        return votes.most_common(1)[0][0] if votes else None

    def classify(self, domain: str) -> str:
        sld      = domain.split(".")[0].lower()
        expanded = re.sub(r"([a-z])([A-Z])", r"\1 \2", sld).replace("-", " ")
        result   = self._spacy_classify(expanded) or self._keyword_classify(expanded)
        if result and result in NICHE_MAP:
            return result
        clean = sld.replace("-", "")
        return next((k for k in NICHE_SCORE if k in clean), "general")

_NICHE_CLASSIFIER = NLPNicheClassifier()

# ─────────────────────────── DATABASE ──────────────────────────
_DB_LOCK = threading.Lock()

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    ddls = [
        # ── existing tables ─────────────────────────────────────────
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

        # ── CHANGE 1: daily_scans — every domain evaluated, no gate ─
        """CREATE TABLE IF NOT EXISTS daily_scans (
               id               INTEGER PRIMARY KEY AUTOINCREMENT,
               scan_date        TEXT,
               run_id           TEXT,
               domain           TEXT,
               tld              TEXT,
               niche            TEXT,
               final_score      INTEGER,
               base_score       INTEGER,
               bonus            INTEGER,
               age_years        INTEGER,
               brand_score      REAL,
               sentiment_score  REAL,
               sentiment_compound REAL,
               backlinks        INTEGER,
               cpc              REAL,
               mc_p50           REAL,
               reg_cost_usd     REAL,
               p_flip_success   REAL,
               kelly_verdict    TEXT,
               kelly_alloc_usd  REAL,
               worthiness_label TEXT,
               telegram_alerted INTEGER DEFAULT 0,
               source           TEXT)""",

        # ── outcome tracker (only for alerted domains) ───────────────
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

        # ── AI learning snapshots (enhanced in v5) ───────────────────
        """CREATE TABLE IF NOT EXISTS learning_snapshots (
               snapshot_id           TEXT PRIMARY KEY,
               generated_at          TEXT,
               domains_tracked       INTEGER,
               avg_score             REAL,
               conversion_rate       REAL,
               flip_rate             REAL,
               avg_sale_price        REAL,
               best_niche            TEXT,
               worst_niche           TEXT,
               ai_insights           TEXT,
               sale_price_analysis   TEXT,
               forward_projections   TEXT,
               system_recommendations TEXT,
               recommended_weights   TEXT)""",
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

# ─────────────────────────── DAILY SCAN LOGGER ─────────────────
def log_daily_scan(conn, run_id: str, res: Dict,
                   base_score: int, bonus: int, alerted: bool):
    """
    CHANGE 1 — Write every evaluated domain to daily_scans.
    Called for ALL domains that pass pre-filters, regardless of score.
    """
    db_write(conn, """
        INSERT INTO daily_scans (
            scan_date, run_id, domain, tld, niche, final_score,
            base_score, bonus, age_years, brand_score,
            sentiment_score, sentiment_compound, backlinks,
            cpc, mc_p50, reg_cost_usd, p_flip_success,
            kelly_verdict, kelly_alloc_usd, worthiness_label,
            telegram_alerted, source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        datetime.utcnow().date().isoformat(), run_id,
        res["domain"], res["tld"], res["niche"], res["final_score"],
        base_score, bonus, res["age_years"], res["brand_score"],
        res["sentiment_score"], res["sentiment_compound"], res["backlinks"],
        res["cpc"], res["mc_p50"], res["reg_cost_usd"],
        res["p_flip_success"], res["kelly_verdict"], res["kelly_alloc_usd"],
        res["worthiness_label"], int(alerted), res["source"],
    ))

# ─────────────────────────── OUTCOME TRACKER ───────────────────
def track_domain_alert(conn, res: Dict, mc_p10: float, mc_p50: float, mc_p90: float,
                       brand_score: float, backlinks: int):
    """Only called for domains that triggered a Telegram alert (score > 85)."""
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
    log.info(f"📋 Outcome tracked for {res['domain']}")

def update_outcome(conn, domain: str, registered: bool = False,
                   sold: bool = False, sale_price: float = 0.0,
                   days_to_sell: int = 0, notes: str = ""):
    """Call manually or via CLI to record what happened after registration."""
    db_write(conn, """
        UPDATE domain_outcomes SET
            registered=?, sold=?, sale_price_usd=?,
            days_to_sell=?, outcome_notes=?, updated_at=?
        WHERE domain=?
    """, (int(registered), int(sold), sale_price,
          days_to_sell, notes, datetime.utcnow().isoformat(), domain))

# ─────────────────────────── AI LEARNING ENGINE (v5) ───────────
class AILearningEngine:
    """
    Monthly analysis with 3 pillars:
      A. Sale Price Analysis
      B. Forward Domain Projections (trend-based, e.g. jioai-style)
      C. System Improvement Recommendations
    """

    def __init__(self, conn):
        self.conn = conn

    # ── data fetchers ────────────────────────────────────────────

    def _fetch_outcome_data(self) -> List[Dict]:
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

    def _fetch_daily_scan_data(self) -> List[Dict]:
        """Pull 30 days of ALL scanned domains for trend and miss-rate analysis."""
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        rows = self.conn.execute("""
            SELECT domain, niche, final_score, age_years, cpc, tld,
                   telegram_alerted, scan_date, worthiness_label
            FROM daily_scans WHERE scan_date >= ?
            ORDER BY final_score DESC
        """, (cutoff[:10],)).fetchall()
        cols = ["domain","niche","final_score","age_years","cpc","tld",
                "telegram_alerted","scan_date","worthiness_label"]
        return [dict(zip(cols, r)) for r in rows]

    def _build_sale_price_stats(self, outcomes: List[Dict]) -> Dict:
        """Pillar A: sale price breakdown."""
        sold   = [d for d in outcomes if d["sold"] and d["sale_price_usd"] > 0]
        reg    = [d for d in outcomes if d["registered"]]
        unsold = [d for d in reg if not d["sold"]]

        by_niche: Dict[str, List[float]] = {}
        by_tld:   Dict[str, List[float]] = {}
        roi_multiples = []
        days_list = []

        for d in sold:
            by_niche.setdefault(d["niche"], []).append(d["sale_price_usd"])
            by_tld.setdefault(d["tld"], []).append(d["sale_price_usd"])
            roi_multiples.append(d["sale_price_usd"] / max(1, d["kelly_alloc_usd"]))
            days_list.append(d["days_to_sell"])

        def _stats(lst):
            if not lst: return {}
            lst_s = sorted(lst)
            n = len(lst_s)
            return {
                "count": n,
                "min":   round(lst_s[0], 2),
                "p25":   round(lst_s[n//4], 2),
                "median":round(lst_s[n//2], 2),
                "p75":   round(lst_s[3*n//4], 2),
                "max":   round(lst_s[-1], 2),
                "mean":  round(sum(lst_s)/n, 2),
            }

        return {
            "total_sold":          len(sold),
            "total_registered":    len(reg),
            "total_unsold":        len(unsold),
            "avg_sale_price_usd":  round(sum(d["sale_price_usd"] for d in sold)/len(sold), 2) if sold else 0,
            "avg_days_to_sell":    round(sum(days_list)/len(days_list), 1) if days_list else 0,
            "sale_price_dist":     _stats([d["sale_price_usd"] for d in sold]),
            "roi_multiples":       _stats(roi_multiples),
            "by_niche":            {n: _stats(v) for n, v in by_niche.items()},
            "by_tld":              {t: _stats(v) for t, v in by_tld.items()},
            "sold_sample":         [{"domain": d["domain"], "niche": d["niche"],
                                     "sale_usd": d["sale_price_usd"],
                                     "days": d["days_to_sell"],
                                     "score": d["alert_score"]} for d in sold[:15]],
        }

    def _build_scan_stats(self, scans: List[Dict]) -> Dict:
        """Stats from daily_scans for Pillars B and C."""
        total = len(scans)
        alerted = sum(1 for s in scans if s["telegram_alerted"])
        niche_miss = Counter(s["niche"] for s in scans if s["niche"] == "general")
        niche_dist = Counter(s["niche"] for s in scans)
        tld_dist   = Counter(s["tld"]   for s in scans)
        # Top trend keywords that kept appearing
        kw_tokens: Counter = Counter()
        for s in scans:
            for tok in re.findall(r"[a-z]{4,}", s["domain"].split(".")[0]):
                kw_tokens[tok] += 1
        return {
            "total_scans":          total,
            "total_alerted":        alerted,
            "alert_rate_pct":       round(100 * alerted / total, 1) if total else 0,
            "general_niche_count":  niche_miss.total(),
            "general_niche_pct":    round(100 * niche_miss.total() / total, 1) if total else 0,
            "niche_distribution":   dict(niche_dist.most_common(10)),
            "tld_distribution":     dict(tld_dist.most_common()),
            "top_keyword_tokens":   dict(kw_tokens.most_common(20)),
            "score_distribution": {
                "90_100": sum(1 for s in scans if s["final_score"] >= 90),
                "85_89":  sum(1 for s in scans if 85 <= s["final_score"] < 90),
                "70_84":  sum(1 for s in scans if 70 <= s["final_score"] < 85),
                "below70": sum(1 for s in scans if s["final_score"] < 70),
            },
        }

    def _call_claude(self, outcomes: List[Dict], sale_stats: Dict,
                     scan_stats: Dict) -> Optional[Dict]:
        """
        Single Claude call covering all 3 pillars.
        Returns structured JSON with sale_price_analysis, forward_projections,
        system_recommendations, and scoring weights.
        """
        if not ANTHROPIC_API_KEY:
            log.warning("ANTHROPIC_API_KEY not set — skipping AI analysis")
            return None

        prompt = f"""
You are a domain investing AI analyst reviewing a month of automated domain sniper data.

══════════════════════════════════════════
PILLAR A — SALE PRICE ANALYSIS
══════════════════════════════════════════
{json.dumps(sale_stats, indent=2)}

══════════════════════════════════════════
PILLAR B — SCAN STATS + KEYWORD SIGNALS
(30-day snapshot of ALL evaluated domains)
══════════════════════════════════════════
{json.dumps(scan_stats, indent=2)}

══════════════════════════════════════════
CURRENT SCORING WEIGHTS
══════════════════════════════════════════
{json.dumps(SCORING_WEIGHTS, indent=2)}

══════════════════════════════════════════
TASK
══════════════════════════════════════════
Return ONLY a valid JSON object (no markdown, no explanation outside JSON) with exactly these keys:

{{
  "insights": "3-4 sentence executive summary of the month",

  "sale_price_analysis": {{
    "summary": "What do sale prices tell us about real market value?",
    "niche_benchmarks": {{
      "niche_name": {{"target_sell_price_usd": 0, "avg_days_to_sell": 0, "roi_multiple": 0}}
    }},
    "pricing_strategy": "Concrete advice: when to list at what price, which platform per niche",
    "red_flags": ["domains that sold below MC p50 and why"]
  }},

  "forward_projections": {{
    "summary": "Based on keyword signals and macro trends, what domain patterns will spike 30-90 days from now?",
    "projected_niches": ["niche1", "niche2"],
    "specific_domain_ideas": [
      {{
        "domain": "example.ai",
        "rationale": "why this specific name",
        "estimated_value_usd": 0,
        "register_by": "YYYY-MM-DD",
        "confidence": "high/medium/low"
      }}
    ],
    "trend_keywords_to_watch": ["keyword1", "keyword2"],
    "india_specific": "Domains tied to Indian market trends (e.g. jioai, upifast, bharatllm) worth watching"
  }},

  "system_recommendations": {{
    "summary": "Top 3 concrete improvements for next month",
    "scoring_fixes": [
      "e.g. raise bonus_niche from 5 to 8 because general-niche domains scored too high"
    ],
    "niche_map_additions": [
      {{"niche": "robotics", "suggested_cpc": 9.0, "seeds": ["robot","drone","autonomous"]}}
    ],
    "data_quality_issues": ["e.g. 32% of domains fell to general — expand NLP seed list for X"],
    "pipeline_improvements": ["specific code or logic changes"],
    "alert_threshold_advice": "Should TELEGRAM_MIN_SCORE stay at 85 or move?"
  }},

  "recommended_weights": {{
    "found_score": 0.25,
    "brand_score": 0.25,
    "sentiment_score": 0.30,
    "tld_value": 0.20,
    "bonus_age": 4,
    "bonus_backlinks": 3,
    "bonus_niche": 5,
    "bonus_short": 3
  }},

  "min_score_suggestion": 85,

  "action_items": ["concrete next steps prioritised by impact"]
}}

Important: forward_projections.specific_domain_ideas must include at least 5 real, registerable domain ideas
based on the keyword signals in the scan data. Think about compound keywords like
jioai, starlinkindia, upichain, bharatllm — combinations of trending brand/geo signals
with high-value niche suffixes. Base confidence on how many times related keywords appeared.
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
                    "max_tokens": 2500,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=45,
            )
            if r.status_code == 200:
                content = r.json()["content"][0]["text"].strip()
                content = re.sub(r"^```json|^```|```$", "", content, flags=re.M).strip()
                return json.loads(content)
            else:
                log.error(f"Claude API error {r.status_code}: {r.text[:300]}")
        except Exception as e:
            log.error(f"AI learning call failed: {e}")
        return None

    def apply_learned_weights(self) -> bool:
        """Run monthly cycle. Returns True if weights were updated."""
        last = self.conn.execute(
            "SELECT generated_at FROM learning_snapshots ORDER BY generated_at DESC LIMIT 1"
        ).fetchone()
        if last:
            last_dt = datetime.fromisoformat(last[0])
            if (datetime.utcnow() - last_dt).days < 25:
                log.info(f"AI Learning: last snapshot {last[0][:10]} — skipping (< 25 days)")
                return False

        outcomes  = self._fetch_outcome_data()
        scans     = self._fetch_daily_scan_data()

        if len(scans) < 10:
            log.info(f"AI Learning: only {len(scans)} scans — need ≥10. Skipping.")
            return False

        sale_stats = self._build_sale_price_stats(outcomes)
        scan_stats = self._build_scan_stats(scans)

        log.info(f"AI Learning: analysing {len(scans)} scans, "
                 f"{sale_stats['total_sold']} sold, "
                 f"{sale_stats['total_registered']} registered...")

        result = self._call_claude(outcomes, sale_stats, scan_stats)
        if not result:
            return False

        # Apply weight updates
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

        # Persist snapshot
        snap_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        db_write(self.conn, """
            INSERT OR REPLACE INTO learning_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            snap_id, datetime.utcnow().isoformat(),
            scan_stats["total_scans"],
            0.0,  # avg_score placeholder
            scan_stats["alert_rate_pct"] / 100,
            sale_stats["total_sold"] / max(1, sale_stats["total_registered"]),
            sale_stats["avg_sale_price_usd"],
            ",".join(result.get("forward_projections", {}).get("projected_niches", [])),
            ",".join(result.get("system_recommendations", {}).get("data_quality_issues", [])),
            result.get("insights", ""),
            json.dumps(result.get("sale_price_analysis", {})),
            json.dumps(result.get("forward_projections", {})),
            json.dumps(result.get("system_recommendations", {})),
            json.dumps(rw),
        ))

        log.info(f"💡 Insights: {result.get('insights', '')}")
        self._send_monthly_digest(result, sale_stats, scan_stats)
        return True

    def _send_monthly_digest(self, result: Dict, sale_stats: Dict, scan_stats: Dict):
        """Send the full monthly analysis as a Telegram message."""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return

        fp   = result.get("forward_projections", {})
        sr   = result.get("system_recommendations", {})
        spa  = result.get("sale_price_analysis", {})
        ideas = fp.get("specific_domain_ideas", [])[:5]

        ideas_text = ""
        for i, idea in enumerate(ideas, 1):
            ideas_text += (
                f"  {i}. `{idea.get('domain','?')}` — "
                f"${idea.get('estimated_value_usd',0):,} est. "
                f"[{idea.get('confidence','?')} confidence]\n"
                f"     _{idea.get('rationale','')}_\n"
            )

        actions = "\n".join(f"  • {a}" for a in result.get("action_items", [])[:5])
        fixes   = "\n".join(f"  • {f}" for f in sr.get("scoring_fixes", [])[:3])

        msg = (
            f"📊 *MONTHLY AI ANALYSIS REPORT*\n"
            f"_{datetime.utcnow().strftime('%B %Y')}_\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"*Executive Summary*\n"
            f"_{result.get('insights', 'No insights generated.')}_\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 *PILLAR A — SALE PRICE ANALYSIS*\n"
            f"Domains sold: *{sale_stats['total_sold']}*  │  "
            f"Avg price: *${sale_stats['avg_sale_price_usd']:,.0f}*  │  "
            f"Avg days: *{sale_stats['avg_days_to_sell']:.0f}d*\n"
            f"_{spa.get('pricing_strategy', '')}_\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔭 *PILLAR B — FORWARD PROJECTIONS (next 30-90 days)*\n"
            f"_{fp.get('summary', '')}_\n"
            f"\n"
            f"*Register these now:*\n"
            f"{ideas_text}"
            f"\n"
            f"🇮🇳 India signal: _{fp.get('india_specific', 'N/A')}_\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔧 *PILLAR C — SYSTEM RECOMMENDATIONS*\n"
            f"Scans this month: *{scan_stats['total_scans']}*  │  "
            f"Alert rate: *{scan_stats['alert_rate_pct']}%*  │  "
            f"General-niche miss: *{scan_stats['general_niche_pct']}%*\n"
            f"\n"
            f"*Scoring fixes:*\n{fixes}\n"
            f"\n"
            f"*Alert threshold advice:* _{sr.get('alert_threshold_advice', '')}_\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Top Action Items:*\n{actions}"
        )

        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                      "parse_mode": "Markdown", "disable_web_page_preview": True},
                timeout=10,
            )
            log.info("📬 Monthly digest sent to Telegram")
        except Exception as e:
            log.error(f"Monthly digest Telegram error: {e}")

    def print_tracker_report(self):
        scans = self._fetch_daily_scan_data()
        outcomes = self._fetch_outcome_data()
        if not scans:
            log.info("Tracker: no scans in last 30 days yet.")
            return
        ss = self._build_scan_stats(scans)
        sp = self._build_sale_price_stats(outcomes)
        print("\n" + "═"*60)
        print("  30-DAY DOMAIN SNIPER TRACKER REPORT (v5)")
        print("═"*60)
        print(f"  Total scans     : {ss['total_scans']} domains evaluated")
        print(f"  Alerts sent     : {ss['total_alerted']} (score > {TELEGRAM_MIN_SCORE})")
        print(f"  Alert rate      : {ss['alert_rate_pct']}%")
        print(f"  General-niche   : {ss['general_niche_pct']}% (NLP miss rate)")
        print(f"  Domains sold    : {sp['total_sold']}")
        print(f"  Avg sale price  : ${sp['avg_sale_price_usd']:,.2f}")
        print(f"  Avg days to sell: {sp['avg_days_to_sell']:.0f}")
        print(f"  Top niches      : {list(ss['niche_distribution'].keys())[:5]}")
        print(f"  Active weights  : {SCORING_WEIGHTS}")
        print("═"*60 + "\n")

# ─────────────────────────── NETWORK HELPERS ───────────────────
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
    if n <= 5 and not has_hyphen:    return 90.0
    elif n <= 7 and not has_hyphen:  return 80.0
    elif n <= 10 and not has_hyphen: return 60.0
    elif n <= 12:                    return 42.0
    else:                            return 30.0

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
    if age >= 5:                           bonus += W["bonus_age"]
    if bl  >= 10:                          bonus += W["bonus_backlinks"]
    if niche != "general":                 bonus += W["bonus_niche"]
    if len(sld) <= 6 and "-" not in sld:   bonus += W["bonus_short"]
    return min(base + bonus, 100), base, bonus

# ─────────────────────────── PROBABILITY ───────────────────────
class ProbabilityEngine:
    GLOBAL_MAX_P_FLIP = 0.20

    @staticmethod
    def sigmoid(x: float) -> float:
        try:    return 1.0 / (1.0 + math.exp(-x))
        except: return 0.0 if x < 0 else 1.0

    def p_flip_success(self, score: int, niche: str, age: int, bl: int,
                       cpc: float = 0.50, search_vol: float = 50.0) -> float:
        intent_signal  = math.log1p(cpc) * 0.18 + math.log1p(search_vol) * 0.04
        quality_signal = (0.06 * score - 3.5
                          + 0.003 * min(bl, 250)
                          + 0.015 * math.log1p(min(age, 20)))
        raw_p      = self.sigmoid(intent_signal + quality_signal)
        niche_ceil = NICHE_MAX_PFLIP.get(niche, 0.03)
        return round(max(0.005, min(raw_p, niche_ceil, self.GLOBAL_MAX_P_FLIP)), 4)

    def monte_carlo(self, base: float, niche: str, age: int, cpc: float) -> Dict:
        age_contribution = age * (cpc * 10.0)
        base = max(15.0, max(base, age_contribution))
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
        b = (p50 - reg_cost) / reg_cost
        if b <= 0:
            return {"allocation_usd": 0.0, "f_star": 0.0, "verdict": "Pass",
                    "safety_margin_pct": 0.0,
                    "quarter_kelly_note": "MC p50 below reg cost — no position"}
        f_full    = (b * p_win - (1 - p_win)) / b
        f_quarter = max(0.0, min(0.25, f_full * KELLY_FRACTION))
        alloc_usd = round(f_quarter * KELLY_BANKROLL, 2)
        safety    = round((reg_cost / KELLY_BANKROLL) * 100, 3)
        verdict   = "Strong Buy" if f_quarter > 0.08 else "Buy" if f_quarter > 0.02 else "Pass"
        return {"allocation_usd": alloc_usd, "f_star": f_quarter, "verdict": verdict,
                "safety_margin_pct": safety,
                "quarter_kelly_note": "Quarter-Kelly applied (÷4 for domain illiquidity)"}

def fetch_namebio_median(conn, keyword: str) -> float:
    row = conn.execute(
        "SELECT median_sale FROM comps_cache WHERE keyword=?", (keyword.lower(),)
    ).fetchone()
    return float(row[0]) if row else 0.0

# ─────────────────────────── WORTHINESS ────────────────────────
def worthiness_verdict(score: int, kelly_verdict: str, p_win: float,
                       mc_p50: float, reg_cost: float,
                       safety_margin_pct: float) -> Tuple[str, str]:
    roi = mc_p50 / reg_cost if reg_cost > 0 else 0
    m   = f"Registration is {safety_margin_pct:.2f}% of your bankroll."
    if score >= 90 and p_win >= 0.15 and roi >= 20:
        return ("🔥 STRONG BUY",
                f"Top domain. ${reg_cost:.0f} reg → ~${mc_p50:,.0f} median ({roi:.0f}x ROI). {m}")
    elif score >= 85 and p_win >= 0.10 and roi >= 10:
        return ("✅ GOOD BUY",
                f"Solid pick. ~{roi:.0f}x ROI at median estimate. List on Sedo/Afternic. {m}")
    elif score >= 80 and roi >= 5:
        return ("⚠️ MARGINAL",
                f"Acceptable risk. ~{roi:.0f}x ROI, flip prob {p_win:.1%}. {m}")
    else:
        return ("❌ SKIP", "Risk/reward not favourable.")

# ─────────────────────────── TELEGRAM ──────────────────────────
def send_telegram_alert(d: Dict):
    """CHANGE 1 — only called when final_score > TELEGRAM_MIN_SCORE (85)."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    se  = "🟢" if d["sentiment_compound"] > 0.1 else "🔴" if d["sentiment_compound"] < -0.1 else "⚪"
    inr = lambda x: f"₹{x * USD_TO_INR:,.0f}"
    nd  = d["niche"].replace("_", " ").title()

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
        f"💳 Reg Cost: *${d['reg_cost_usd']:.2f}* ({inr(d['reg_cost_usd'])})\n"
        f"   _{d['safety_margin_pct']:.2f}% of bankroll — wide safety margin_\n"
        f"\n"
        f"📈 Est. Resale (2,000 scenarios):\n"
        f"   Low  ${d['mc_p10']:,.0f} │ Mid ${d['mc_p50']:,.0f} │ High ${d['mc_p90']:,.0f}\n"
        f"\n"
        f"💰 Max Spend (Quarter-Kelly): *${d['kelly_alloc_usd']:,.2f}* — {d['kelly_verdict']}\n"
        f"   _{d['kelly_quarter_note']}_\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Intent (NLP): *{nd}*  │  CPC: ${d['cpc']:.2f}\n"
        f"{se} Sentiment: {d['sentiment_score']:.0f}/100  │  Age: {d['age_years']}y\n"
        f"📊 Flip prob: {d['p_flip_success']:.1%} _(capped at 20% STR)_\n"
        f"\n"
        f"🔗 [GoDaddy]({d['link_godaddy']}) │ "
        f"[Namecheap]({d['link_namecheap']}) │ "
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
def push_to_sheets(df: pd.DataFrame, tab: str = SHEET_NAME):
    if not GSPREAD_OK or not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID:
        return
    try:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
        try:
            ws = sh.worksheet(tab)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(tab, 5000, 30)
        vals = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        ws.clear()
        ws.update(vals, value_input_option="RAW")
        log.info(f"Sheets [{tab}]: {len(df)} rows pushed")
    except Exception as e:
        log.error(f"Sheets error: {e}")

# ─────────────────────────── PROCESS DOMAIN ────────────────────
def process_domain(domain: str, source: str, conn, run_id: str,
                   seo: SEOIntelligence,
                   sent: InstitutionalSentimentEngine,
                   tm: TrademarkGuard) -> Optional[Dict]:
    """
    CHANGE 1 — Returns a result dict for ALL domains that pass pre-filters.
    Scoring happens unconditionally. The caller decides what to do based on score.
    base_score and bonus are included in the dict for daily_scans logging.
    """
    log.info(f"⏳ Processing: {domain}")
    sld = domain.split(".")[0].lower()
    tld = "." + domain.split(".")[-1].lower()

    if sld.count("-") > 1 or "--" in sld:
        return None
    if len(sld) > 12:
        return None
    if re.fullmatch(r"[a-z]+", sld) and not re.search(r"[aeiou]", sld):
        return None

    is_available, age = port43_whois_audit(domain)
    log.info(f"  WHOIS: available={is_available} age={age}y")

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

    # ── Probability + MC + Kelly (computed for all, needed for daily_scans) ──
    prob    = ProbabilityEngine()
    p_win   = prob.p_flip_success(final_score, niche, age, bl, cpc, search_vol)
    mc_base = max(comp_median, snaps * 12.0)
    mc      = prob.monte_carlo(mc_base, niche, age, cpc)

    reg_cost_usd = TLD_REG_COSTS.get(tld, DEFAULT_REG_COST)
    k = prob.kelly(p_win, mc["p50"], reg_cost_usd)

    worth_label, worth_reason = worthiness_verdict(
        final_score, k["verdict"], p_win, mc["p50"],
        reg_cost_usd, k["safety_margin_pct"],
    )

    gd_aff = f"&isc={AFFILIATE_ID_GD}"          if AFFILIATE_ID_GD else ""
    nc_aff = f"&AffiliateCode={AFFILIATE_ID_NC}" if AFFILIATE_ID_NC else ""

    return {
        # identity
        "domain":             domain,
        "tld":                tld,
        "source":             source,
        # scoring (needed for daily_scans)
        "final_score":        final_score,
        "_base_score":        base_score,
        "_bonus":             bonus,
        # niche + age
        "niche":              niche,
        "age_years":          age,
        "cpc":                cpc,
        # sentiment
        "sentiment_compound": sent_data["compound"],
        "sentiment_score":    sent_data["sentiment_score"],
        "headline_count":     sent_data.get("headline_count", 0),
        # probability
        "p_flip_success":     p_win,
        # monte carlo
        "mc_p10":             mc["p10"],
        "mc_p50":             mc["p50"],
        "mc_p90":             mc["p90"],
        # kelly
        "kelly_verdict":      k["verdict"],
        "kelly_alloc_usd":    k["allocation_usd"],
        "kelly_alloc_inr":    k["allocation_usd"] * USD_TO_INR,
        "safety_margin_pct":  k["safety_margin_pct"],
        "kelly_quarter_note": k["quarter_kelly_note"],
        # cost
        "reg_cost_usd":       reg_cost_usd,
        "reg_cost_inr":       reg_cost_usd * USD_TO_INR,
        # verdict
        "worthiness_label":   worth_label,
        "worthiness_reason":  worth_reason,
        # raw signals
        "brand_score":        brand_score,
        "backlinks":          bl,
        # links
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
    log.info(f"═══ Domain Sniper v5 │ Run: {run_id} ═══")
    log.info(f"    Save all scans ≥ score {SAVE_ALL_SCORE}")
    log.info(f"    Telegram alert  > score {TELEGRAM_MIN_SCORE}")
    log.info(f"    Quarter-Kelly: {KELLY_FRACTION}  │  Bankroll: ${KELLY_BANKROLL:,.0f}")
    log.info(f"    NLP: {'spacy' if SPACY_OK else 'keyword-index'}")

    fetch_latest_commoncrawl_index()
    conn = init_db()
    seed_namebio_cache(conn)

    # Monthly AI analysis (runs once per 25 days)
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

    all_scanned, alerted, seen = [], [], set()

    for d, src in pool:
        d_clean = d.strip().lower()
        if d_clean in seen or is_seen(conn, d_clean):
            continue
        seen.add(d_clean)

        res = process_domain(d_clean, src, conn, run_id,
                             seo_engine, sent_engine, tm_guard)
        if not res:
            continue

        score         = res["final_score"]
        should_alert  = score > TELEGRAM_MIN_SCORE   # CHANGE 1: strict >

        # ── CHANGE 1: save every scan to daily_scans regardless of score ──
        log_daily_scan(conn, run_id, res,
                       base_score=res["_base_score"],
                       bonus=res["_bonus"],
                       alerted=should_alert)
        all_scanned.append(res)

        if should_alert:
            log.info(
                f"🔥 ALERT │ {d_clean:30s} │ {score}/100 │ "
                f"{res['worthiness_label']} │ niche={res['niche']}"
            )
            track_domain_alert(conn, res, res["mc_p10"], res["mc_p50"], res["mc_p90"],
                               res["brand_score"], res["backlinks"])
            mark_seen(conn, d_clean, score, "flip")
            send_telegram_alert(res)
            alerted.append(res)
        else:
            log.debug(f"  📁 Saved to daily_scans (score={score}, no alert)")

    # ── Export daily CSV (all scans) ─────────────────────────────
    if all_scanned:
        df_all = pd.DataFrame(all_scanned).sort_values("final_score", ascending=False)
        # Drop internal keys before export
        df_all = df_all.drop(columns=["_base_score","_bonus"], errors="ignore")
        df_all.to_csv(f"daily_scans_{run_id}.csv", index=False)
        log.info(f"📁 All {len(all_scanned)} scans saved to daily_scans_{run_id}.csv")

    # ── Export alerted CSV + push to Sheets ──────────────────────
    if alerted:
        df_alert = pd.DataFrame(alerted).sort_values("final_score", ascending=False)
        df_alert = df_alert.drop(columns=["_base_score","_bonus"], errors="ignore")
        df_alert.to_csv(f"alerts_{run_id}.csv", index=False)
        push_to_sheets(df_alert, tab=SHEET_NAME)
        log.info(f"🔔 {len(alerted)} alerts pushed to Sheets tab '{SHEET_NAME}'")

    log.info(
        f"Run complete. Scanned: {len(all_scanned)} │ "
        f"Alerted (score > {TELEGRAM_MIN_SCORE}): {len(alerted)}"
    )
    conn.close()


if __name__ == "__main__":
    main()
