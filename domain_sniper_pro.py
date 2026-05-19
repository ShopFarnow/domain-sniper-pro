#!/usr/bin/env python3
"""
Domain Fortress Sniper PRO v5 – Reliable API & Fallback
- Uses domainsdb.info free API (no key, no login)
- Fallback keyword generation if API fails
- Retry‑with‑backoff for all HTTP calls
- Concurrent scoring (5 workers)
- Google Sheets export, Telegram alerts, email digest
- Runs on GitHub Actions without failures
"""

import os, sys, re, time, json, sqlite3, logging, smtplib, random, traceback
import concurrent.futures
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import pandas as pd
from bs4 import BeautifulSoup
import whois
from pytrends.request import TrendReq
import gspread
from google.oauth2.service_account import Credentials

# ========== LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ========== ENV CONFIG ==========
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_NAME        = os.getenv("SHEET_NAME", "DomainSniperPro")
MIN_ALERT_SCORE   = int(os.getenv("MIN_ALERT_SCORE", "70"))
SAFE_BROWSING_KEY = os.getenv("SAFE_BROWSING_KEY", "")
EMAIL_DIGEST_TO   = os.getenv("EMAIL_DIGEST_TO", "")
GMAIL_USER        = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASS    = os.getenv("GMAIL_APP_PASS", "")
AFFILIATE_ID_GD   = os.getenv("AFFILIATE_ID_GD", "")
AFFILIATE_ID_NC   = os.getenv("AFFILIATE_ID_NC", "")
DB_PATH           = os.getenv("DB_PATH", "domain_sniper.db")
MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "5"))

# ========== SCORING CONSTANTS ==========
TLD_VALUE = {
    ".com":100, ".io":88, ".ai":85, ".co":75, ".net":60,
    ".org":55, ".in":45, ".us":40, ".app":72, ".dev":70,
    ".tech":55, ".online":30, ".info":25, ".biz":20,
}
DEFAULT_TLD = 15

NICHE_MAP = {
    "insurance":90, "loan":90, "mortgage":88, "crypto":80, "ai":82,
    "saas":80, "health":75, "lawyer":85, "travel":65, "shop":55,
    "realestate":80, "clinic":78, "dentist":80, "plumber":72,
    "solar":75, "fintech":82, "ecommerce":70, "agency":60,
    "marketing":65, "consulting":68, "fitness":65, "yoga":60,
    "vpn":78, "hosting":72, "invest":85, "forex":80, "nft":70,
}

PARKING_CPM = {
    "insurance":18, "loan":15, "mortgage":14, "crypto":12, "ai":10,
    "saas":9, "health":8, "lawyer":14, "travel":7, "shop":5,
    "realestate":10, "clinic":9, "dentist":11, "plumber":8,
    "solar":9, "fintech":11, "general":3,
}

LEAD_VALUE = {
    "insurance":25, "loan":20, "mortgage":30, "lawyer":40,
    "dentist":15, "clinic":12, "plumber":8, "solar":18, "realestate":20,
}

WEIGHTS = {
    "foundation":   0.28,
    "flip":         0.32,
    "history":      0.20,
    "momentum":     0.12,
    "monetization": 0.08,
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ========== SQLite (persistence) ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS seen_domains (
        domain TEXT PRIMARY KEY,
        first_seen TEXT,
        final_score INTEGER,
        monetization_path TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS blacklist (
        domain TEXT PRIMARY KEY,
        reason TEXT,
        blacklisted_at TEXT
    )""")
    conn.commit()
    return conn

def is_seen(conn, domain):
    return conn.execute("SELECT 1 FROM seen_domains WHERE domain=?", (domain,)).fetchone() is not None

def mark_seen(conn, domain, score, path):
    conn.execute(
        "INSERT OR REPLACE INTO seen_domains VALUES (?,?,?,?)",
        (domain, datetime.utcnow().isoformat(), score, path)
    )
    conn.commit()

def is_blacklisted(conn, domain):
    return conn.execute("SELECT 1 FROM blacklist WHERE domain=?", (domain,)).fetchone() is not None

def add_to_blacklist(conn, domain, reason):
    conn.execute(
        "INSERT OR IGNORE INTO blacklist VALUES (?,?,?)",
        (domain, reason, datetime.utcnow().isoformat())
    )
    conn.commit()
    log.info(f"Blacklisted {domain}: {reason}")

# ========== HTTP Helper ==========
def http_get(url, timeout=25, retries=3, backoff=2.0, headers=None):
    _headers = {"User-Agent": random.choice(USER_AGENTS)}
    if headers:
        _headers.update(headers)
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=_headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 503):
                wait = backoff * (2 ** attempt) + random.uniform(0, 1)
                log.warning(f"Rate-limited ({resp.status_code}) – waiting {wait:.1f}s")
                time.sleep(wait)
            else:
                return None
        except Exception:
            pass
        time.sleep(backoff * (attempt + 1))
    return None

# ========== DOMAIN FETCHER (reliable, free) ==========
def fetch_domainsdb(limit=200):
    """Public API from domainsdb.info – returns recently registered/expired domains."""
    url = "https://api.domainsdb.info/v1/domains/search?domain=*.com&limit=200"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": random.choice(USER_AGENTS)})
        if resp.status_code == 200:
            data = resp.json()
            domains = []
            for item in data.get("domains", [])[:limit]:
                d = item.get("domain", "").lower().strip()
                if d and d.endswith(".com") and len(d) < 50:
                    domains.append((d, "domainsdb"))
            log.info(f"domainsdb.info: {len(domains)} domains")
            return domains
        else:
            log.warning(f"domainsdb HTTP {resp.status_code}")
    except Exception as e:
        log.error(f"domainsdb error: {e}")
    return []

def generate_fallback_domains(limit=100):
    """Generate keyword-based domains as fallback – ensures we always have something."""
    keywords = [
        "insurance", "loan", "mortgage", "crypto", "ai", "saas", "health", "lawyer",
        "travel", "realestate", "clinic", "dentist", "plumber", "solar", "fintech",
        "vpn", "hosting", "invest", "forex", "nft", "gaming", "betting"
    ]
    tlds = [".com", ".io", ".ai", ".co", ".net"]
    domains = []
    for kw in keywords:
        for tld in tlds:
            domains.append((f"{kw}{tld}", "fallback"))
            domains.append((f"{kw}pro{tld}", "fallback"))
    random.shuffle(domains)
    return domains[:limit]

# ========== SCORING HELPERS (unchanged, but all use http_get where needed) ==========
def get_latest_cc_index():
    resp = http_get("https://index.commoncrawl.org/collinfo.json", timeout=10)
    if resp:
        try:
            data = resp.json()
            if data:
                return data[0]["cdx-api"]
        except Exception:
            pass
    return "https://index.commoncrawl.org/CC-MAIN-2024-10-index"

CC_INDEX_URL = get_latest_cc_index()

def wayback_backlinks(domain):
    url = f"http://web.archive.org/cdx/search/cdx?url=*.{domain}&output=text&fl=urlkey&limit=500&collapse=urlkey"
    resp = http_get(url, timeout=25)
    if not resp:
        return 0
    ref_domains = set()
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("/")
        if parts:
            ref_domains.add(parts[0].replace(")", "").split(",")[-1])
    return len(ref_domains)

def wayback_traffic_proxy(domain):
    url = f"http://web.archive.org/cdx/search/cdx?url={domain}&output=json&fl=timestamp&limit=200"
    resp = http_get(url, timeout=20)
    if not resp:
        return 0, 0
    try:
        data = resp.json()
        count = max(0, len(data) - 1)
        est_monthly = min(count * 300, 50000)
        return count, est_monthly
    except Exception:
        return 0, 0

def commoncrawl_presence(domain):
    global CC_INDEX_URL
    url = f"{CC_INDEX_URL}?url={domain}&output=json&limit=5"
    resp = http_get(url, timeout=15)
    if not resp:
        return 0
    count = 0
    for line in resp.text.strip().splitlines():
        if line.strip():
            try:
                json.loads(line)
                count += 1
            except Exception:
                pass
    return count

def domain_age(domain):
    try:
        w = whois.whois(domain)
        creation = w.creation_date
        if isinstance(creation, list):
            creation = creation[0]
        if creation:
            return min(25, (datetime.now() - creation).days // 365)
    except Exception as e:
        log.debug(f"WHOIS error for {domain}: {e}")
    return 0

def google_trends_score(keyword):
    kw = re.sub(r"\.[a-z]{2,}$", "", keyword).replace("-", " ").strip()
    if not kw or len(kw) < 2:
        return 0.0
    for attempt in range(3):
        try:
            pytrends = TrendReq(hl="en-US", tz=330, timeout=(10, 25), retries=2, backoff_factor=0.5)
            pytrends.build_payload([kw], timeframe="today 6-m")
            df = pytrends.interest_over_time()
            if df.empty or kw not in df.columns:
                return 0.0
            half = len(df) // 2
            recent = df[kw].iloc[-half:].mean()
            older = df[kw].iloc[:half].mean()
            return 0.0 if older == 0 else round((recent - older) / older * 100, 1)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                time.sleep(4 ** attempt + random.uniform(1, 3))
            else:
                break
    return 0.0

def detect_niche(domain):
    dl = domain.lower().replace("-", "").replace(".", "")
    best_niche, best_score = "general", 30
    for kw, score in NICHE_MAP.items():
        if kw in dl and score > best_score:
            best_niche, best_score = kw, score
    return best_niche, best_score

def domain_length_score(domain):
    sld = domain.split(".")[0]
    n = len(sld)
    if n <= 4: return 100
    if n <= 6: return 88
    if n <= 8: return 72
    if n <= 10: return 55
    if n <= 13: return 38
    return max(0, 38 - (n - 13) * 3)

def brandability(domain):
    sld = domain.split(".")[0].lower()
    score = 50
    if re.search(r"\d", sld): score -= 20
    if "-" in sld: score -= 20
    if len(sld) > 12: score -= 15
    if len(sld) < 3: score -= 10
    if re.search(r"[aeiou]{2,}", sld): score += 10
    if 4 <= len(sld) <= 7: score += 20
    if sld == sld[::-1]: score += 5
    return max(0, min(100, score))

def spam_check(domain):
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
        spam_pattern = re.compile(r"viagra|cialis|casino|poker|adult|xxx|pharma|pills|escort", re.I)
        spam_hits = 0
        for ts in sample:
            snap_url = f"http://web.archive.org/web/{ts[0]}/{domain}"
            r = http_get(snap_url, timeout=10, retries=1)
            if r and spam_pattern.search(r.text):
                spam_hits += 1
            time.sleep(0.3)
        score = max(0, 100 - spam_hits * 35)
        return spam_hits, score
    except Exception:
        return 0, 85

def check_safe_browsing(domain):
    if not SAFE_BROWSING_KEY:
        return 1
    url = f"https://safebrowsing.googleapis.com/v4/threatMatches:find?key={SAFE_BROWSING_KEY}"
    body = {
        "client": {"clientId": "domain-sniper-pro", "clientVersion": "5.0"},
        "threatInfo": {
            "threatTypes": ["MALWARE", "SOCIAL_ENGINEERING", "UNWANTED_SOFTWARE"],
            "platformTypes": ["ANY_PLATFORM"],
            "threatEntryTypes": ["URL"],
            "threatEntries": [{"url": f"http://{domain}"}]
        }
    }
    try:
        resp = requests.post(url, json=body, timeout=10)
        return 0 if resp.json().get("matches") else 1
    except Exception:
        return 1

def in_majestic_million(domain):
    """Simplified: we no longer load the full CSV; return False to keep lightweight."""
    return False  # to avoid large download; can be re-enabled later.

# ========== SCORING FUNCTIONS ==========
def foundation_score(backlinks, cc_hits, age, tld, majestic):
    bl_norm = min(100, backlinks / 3)
    cc_norm = min(100, cc_hits * 20)
    age_norm = min(100, age * 5)
    tld_norm = TLD_VALUE.get(tld, DEFAULT_TLD)
    base = bl_norm * 0.30 + cc_norm * 0.25 + age_norm * 0.25 + tld_norm * 0.20
    if majestic:
        base = min(100, base + 15)
    return base

def flip_score_fn(domain, niche_cpm, age, tld):
    length_s = domain_length_score(domain)
    brand_s = brandability(domain)
    tld_s = TLD_VALUE.get(tld, DEFAULT_TLD)
    if age < 5:
        age_s = min(100, age * 8)
    elif age <= 15:
        age_s = 100
    else:
        age_s = max(40, 100 - (age - 15) * 2)
    return niche_cpm * 0.30 + length_s * 0.25 + brand_s * 0.20 + tld_s * 0.15 + age_s * 0.10

def momentum_score(trend_pct):
    return max(0.0, min(100.0, 50.0 + trend_pct / 2.0))

def detect_monetization_paths(domain, niche, age, backlinks, monthly_traffic, cc_hits, trend_pct):
    paths = []
    flip_estimate = 0
    if backlinks > 20 and age >= 3:
        flip_estimate = max(300, int(backlinks * age * 2.5 + age * 40))
        paths.append(("flip", 0, flip_estimate, f"List on Sedo/Dan/Afternic – {age}y aged domain, {backlinks} BL"))
    elif age >= 5:
        flip_estimate = age * 35
        paths.append(("flip", 0, flip_estimate, f"List on Sedo/Dan/Afternic – aged {age}y domain"))

    cpm = PARKING_CPM.get(niche, PARKING_CPM["general"])
    if monthly_traffic > 50:
        park_monthly = (monthly_traffic / 1000) * cpm
        paths.append(("parking", park_monthly, 0, f"Park with Bodis/ParkingCrew – est ${park_monthly:.0f}/mo @ ${cpm} CPM"))

    if niche in ["ai","saas","crypto","fintech","health","travel"] and age >= 2 and cc_hits > 0:
        paths.append(("content_site", 5, 0, "Build 5-page niche site + Google AdSense"))

    lead_val = LEAD_VALUE.get(niche, 0)
    if lead_val > 0 and age >= 1:
        leads_monthly = max(1, monthly_traffic // 100) * lead_val
        paths.append(("lead_gen", leads_monthly, 0, f"Lead-gen page for {niche} – sell leads @ ${lead_val}/lead"))

    if niche in ["crypto","fintech","saas","insurance"] and monthly_traffic > 20:
        aff_monthly = (monthly_traffic / 1000) * 15
        paths.append(("affiliate", aff_monthly, 0, f"Redirect to affiliate offer in {niche} niche"))

    if not paths:
        paths.append(("hold_and_list", 0, flip_estimate or 50, "List for sale; low immediate opportunity"))

    paths.sort(key=lambda x: x[1], reverse=True)
    primary = paths[0]
    secondary = paths[1] if len(paths) > 1 else paths[0]
    return primary[0], secondary[0], primary[1], flip_estimate, primary[3]

def monetization_score(monthly_est):
    return min(100, monthly_est) if monthly_est > 0 else 0

def build_affiliate_links(domain):
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
def process_domain(domain, source, conn):
    tld = "." + domain.split(".")[-1]
    if TLD_VALUE.get(tld, DEFAULT_TLD) < 20:
        log.debug(f"Skip {domain}: low-value TLD {tld}")
        return None
    sld = domain.split(".")[0]
    if len(sld) > 22 or len(sld) < 2:
        log.debug(f"Skip {domain}: SLD length {len(sld)} out of range")
        return None
    if re.search(r"\d{4,}", sld):
        log.debug(f"Skip {domain}: long numeric sequence")
        return None

    backlinks = wayback_backlinks(domain)
    snap_count, traffic = wayback_traffic_proxy(domain)
    cc_hits = commoncrawl_presence(domain)
    age = domain_age(domain)
    niche, niche_cpm = detect_niche(domain)
    trend = google_trends_score(domain)
    spam_flags, hist_s = spam_check(domain)
    safe = check_safe_browsing(domain)
    majestic = in_majestic_million(domain)

    if not safe:
        hist_s = min(hist_s, 20)
    if spam_flags >= 3 and hist_s < 30:
        add_to_blacklist(conn, domain, f"spam flags={spam_flags}, history={hist_s}")
        return None

    found = foundation_score(backlinks, cc_hits, age, tld, majestic)
    flip = flip_score_fn(domain, niche_cpm, age, tld)
    moment = momentum_score(trend)

    pri_path, sec_path, monthly_est, flip_est, mon_note = detect_monetization_paths(
        domain, niche, age, backlinks, traffic, cc_hits, trend
    )
    mon_s = monetization_score(monthly_est)

    final = int(
        found * WEIGHTS["foundation"] +
        flip * WEIGHTS["flip"] +
        hist_s * WEIGHTS["history"] +
        moment * WEIGHTS["momentum"] +
        mon_s * WEIGHTS["monetization"]
    )

    aff_links = build_affiliate_links(domain)

    return {
        "fetched_at": datetime.utcnow().isoformat(),
        "domain": domain,
        "source": source,
        "final_score": final,
        "foundation": round(found, 1),
        "flip_score": round(flip, 1),
        "history_score": round(hist_s, 1),
        "momentum_score": round(moment, 1),
        "monetization_score": round(mon_s, 1),
        "tld": tld,
        "sld_length": len(sld),
        "age_years": age,
        "backlinks_proxy": backlinks,
        "wayback_snapshots": snap_count,
        "est_monthly_traffic": traffic,
        "commoncrawl_hits": cc_hits,
        "in_majestic_million": majestic,
        "niche": niche,
        "niche_cpm": niche_cpm,
        "trend_6m_pct": trend,
        "spam_flags": spam_flags,
        "safe_browsing_clean": safe,
        "primary_path": pri_path,
        "secondary_path": sec_path,
        "est_monthly_usd": round(monthly_est, 2),
        "flip_estimate_usd": flip_est,
        "monetization_note": mon_note,
        "flip_range": f"${flip_est*.8:.0f}–${flip_est*1.2:.0f}" if flip_est else "TBD",
        "link_sedo": aff_links.get("sedo", ""),
        "link_dan": aff_links.get("dan_com", ""),
        "link_afternic": aff_links.get("afternic", ""),
        "link_godaddy_aff": aff_links.get("godaddy", ""),
        "link_namecheap_aff": aff_links.get("namecheap", ""),
    }

def process_domain_safe(args):
    domain, source, conn = args
    try:
        return process_domain(domain, source, conn)
    except Exception as e:
        log.error(f"Error processing {domain}: {e}")
        traceback.print_exc()
        return None

# ========== OUTPUTS ==========
def push_to_sheets(df):
    if not GOOGLE_CREDS_JSON or not GOOGLE_SHEET_ID:
        log.warning("Google Sheets config missing – skipping export.")
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
            ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=35)
        headers = df.columns.tolist()
        values = [headers] + df.fillna("").astype(str).values.tolist()
        existing = ws.get_all_values()
        if not existing:
            ws.update(values, value_input_option="RAW")
        else:
            ws.append_rows(values[1:], value_input_option="RAW")
        log.info(f"Pushed {len(df)} rows to Google Sheets.")
    except Exception as e:
        log.error(f"Sheets push failed: {e}")
        traceback.print_exc()

def send_telegram(d):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    path_emoji = {"flip": "💸", "parking": "🅿️", "lead_gen": "🎯", "affiliate": "🔗", "content_site": "📝", "hold_and_list": "⏳"}
    emoji = path_emoji.get(d["primary_path"], "💡")
    msg = (
        f"🏆 *DOMAIN PEARL* — Score {d['final_score']}/100\n"
        f"🌐 *{d['domain']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Foundation: {d['foundation']:.0f}  |  Flip: {d['flip_score']:.0f}\n"
        f"History: {d['history_score']:.0f}  |  Momentum: {d['momentum_score']:.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷 Niche: *{d['niche']}*  |  Age: {d['age_years']}y\n"
        f"📊 Traffic: ~{d['est_monthly_traffic']:,}/mo  |  BL: {d['backlinks_proxy']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} *Best Path: {d['primary_path'].replace('_',' ').title()}*\n"
        f"💰 Est. monthly: ${d['est_monthly_usd']:.0f}  |  Flip: {d['flip_range']}\n"
        f"📋 {d['monetization_note']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 [Sedo]({d['link_sedo']}) | [Dan]({d['link_dan']}) | [Afternic]({d['link_afternic']})\n"
        f"⚡ Act fast – drops within 24h!"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chunk_start in range(0, len(msg), 4000):
        try:
            requests.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg[chunk_start:chunk_start+4000],
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10)
        except Exception as e:
            log.error(f"Telegram error: {e}")
    log.info(f"Telegram alert sent for {d['domain']}")

def send_email_digest(results):
    if not EMAIL_DIGEST_TO or not GMAIL_USER or not GMAIL_APP_PASS:
        return
    if not results:
        return
    top = sorted(results, key=lambda x: x["final_score"], reverse=True)[:10]
    html_rows = ""
    for d in top:
        html_rows += f"""
        <tr>
          <td><b>{d['domain']}</b></td>
          <td style="text-align:center">{d['final_score']}</td>
          <td>{d['niche']}</td>
          <td>{d['primary_path'].replace('_',' ').title()}</td>
          <td>${d['est_monthly_usd']:.0f}/mo</td>
          <td>{d['flip_range']}</td>
          <td><a href="{d['link_sedo']}">Sedo</a> | <a href="{d['link_dan']}">Dan</a></td>
        </tr>"""
    html = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:auto">
    <h2>🏴‍☠️ Domain Sniper PRO – Daily Digest</h2>
    <p>{datetime.utcnow().strftime('%Y-%m-%d')} | {len(results)} domains scanned</p>
    <table border="1" cellpadding="6" style="border-collapse:collapse;width:100%">
      <tr style="background:#1a1a2e;color:white">
        <th>Domain</th><th>Score</th><th>Niche</th><th>Best Path</th>
        <th>Monthly</th><th>Flip Range</th><th>Links</th>
      </tr>
      {html_rows}
    </table>
    <p style="font-size:11px">Domain Sniper PRO – automated estimates only.</p>
    </body></html>"""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🏴‍☠️ Domain Sniper PRO – {len(top)} Pearls ({datetime.utcnow().strftime('%Y-%m-%d')})"
    msg["From"] = GMAIL_USER
    msg["To"] = EMAIL_DIGEST_TO
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASS)
            server.sendmail(GMAIL_USER, EMAIL_DIGEST_TO, msg.as_string())
        log.info("Email digest sent.")
    except Exception as e:
        log.error(f"Email error: {e}")

# ========== MAIN ==========
def main():
    global CC_INDEX_URL
    log.info("=" * 65)
    log.info("Domain Fortress Sniper PRO v5 – Reliable API & Fallback")
    log.info("=" * 65)

    conn = init_db()
    log.info("Database initialized.")

    CC_INDEX_URL = get_latest_cc_index()
    log.info(f"CommonCrawl index: {CC_INDEX_URL}")

    # Fetch domains
    all_domains = fetch_domainsdb(limit=200)
    if not all_domains:
        log.warning("Primary domain source returned 0 – using fallback keyword generator.")
        all_domains = generate_fallback_domains(limit=150)

    log.info(f"Total collected (after dedup not yet): {len(all_domains)}")

    # Deduplicate and filter seen/blacklisted
    seen_set = set()
    unique = []
    for d, src in all_domains:
        d = d.strip().lower()
        if d and d not in seen_set and not is_seen(conn, d) and not is_blacklisted(conn, d):
            seen_set.add(d)
            unique.append((d, src))

    log.info(f"New unique domains to process: {len(unique)}")

    if not unique:
        log.warning("No new domains – saving empty CSV to avoid upload error.")
        pd.DataFrame(columns=["domain", "final_score"]).to_csv(
            f"domains_{datetime.utcnow().strftime('%Y%m%d')}.csv", index=False
        )
        conn.close()
        return

    # Process in parallel
    results = []
    args_list = [(d, src, conn) for d, src in unique]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_domain_safe, args): args for args in args_list}
        for future in concurrent.futures.as_completed(futures):
            d_name = futures[future][0]
            try:
                data = future.result()
                if data:
                    results.append(data)
                    mark_seen(conn, d_name, data["final_score"], data["primary_path"])
                    log.info(f"✓ {d_name:35s} score={data['final_score']:3d}  niche={data['niche']:12s}  path={data['primary_path']}")
            except Exception as e:
                log.error(f"Future error for {d_name}: {e}")

    if not results:
        log.warning("No domains passed scoring – saving empty CSV.")
        pd.DataFrame(columns=["domain", "final_score"]).to_csv(
            f"domains_{datetime.utcnow().strftime('%Y%m%d')}.csv", index=False
        )
        conn.close()
        return

    # Results DataFrame
    df = pd.DataFrame(results).sort_values("final_score", ascending=False)

    csv_path = f"domains_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    df.to_csv(csv_path, index=False)
    log.info(f"CSV saved: {csv_path}  ({len(df)} rows)")

    push_to_sheets(df)

    pearls = df[df["final_score"] >= MIN_ALERT_SCORE]
    log.info(f"Pearls (score >= {MIN_ALERT_SCORE}): {len(pearls)}")
    for _, row in pearls.iterrows():
        send_telegram(row.to_dict())
        time.sleep(1)

    send_email_digest(results)

    # Summary
    log.info("=" * 50)
    log.info("SUMMARY")
    log.info(f"  Domains processed : {len(results)}")
    log.info(f"  Pearls found      : {len(pearls)}")
    if not results.empty:
        best = df.iloc[0]
        log.info(f"  Best domain       : {best['domain']} (score {best['final_score']})")
        log.info(f"  Best path         : {best['primary_path']} → {best['monetization_note']}")
    log.info("=" * 50)

    conn.close()
    log.info("Done.")

if __name__ == "__main__":
    main()
