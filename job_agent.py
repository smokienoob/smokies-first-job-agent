"""
Job Posting Notifier Agent — Google Sheets edition (v1.2)
Reads target companies from a published Google Sheet, scrapes careers portals,
sends Telegram alerts for new matching roles.

Changelog:
- v1.2: Hardened fuzzy matching (token_set_ratio, threshold 90, length guard)
- v1.1: Match-only baseline, smarter title matching, HTML scraper de-dup
"""
import csv
import io
import json
import os
import re
import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

# ---------- CONFIG ----------
SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL")
SEEN_FILE = "seen_jobs.json"
TG_TOKEN = os.environ.get("TG_BOT_TOKEN")
TG_CHAT = os.environ.get("TG_CHAT_ID")

# Fuzzy matching tuning
FUZZY_THRESHOLD = 90         # how close 'close enough' is (0-100)
FUZZY_MIN_LENGTH = 8         # don't fuzzy-match keywords shorter than this

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


# ---------- SHEET LOADER ----------
def load_companies_from_sheet():
    if not SHEET_CSV_URL:
        raise RuntimeError("SHEET_CSV_URL env var not set")
    r = requests.get(SHEET_CSV_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    companies = []
    for row in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}
        if not row.get("company") or not row.get("careers url"):
            continue
        if row.get("active", "true").upper() == "FALSE":
            continue
        companies.append({
            "name": row["company"],
            "url": row["careers url"],
            "scraper_type": row.get("scraper type", "auto") or "auto",
            "target_titles": [t.strip() for t in row.get("target titles", "").split(",") if t.strip()],
            "country": row.get("country", ""),
        })
    return companies


# ---------- AUTO-DETECT ----------
def detect_scraper(url):
    host = urlparse(url).netloc.lower()
    path = urlparse(url).path.strip("/")

    if "greenhouse.io" in host:
        token = path.split("/")[0] if path else None
        if token:
            return "greenhouse", {"board_token": token}

    if "lever.co" in host:
        slug = path.split("/")[0] if path else None
        if slug:
            return "lever", {"company_slug": slug}

    if "ashbyhq.com" in host:
        slug = path.split("/")[0] if path else None
        if slug:
            return "ashby", {"slug": slug}

    return None, None


# ---------- SCRAPERS ----------
def scrape_greenhouse(board_token):
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return [
        {
            "title": j["title"],
            "location": j.get("location", {}).get("name", ""),
            "url": j["absolute_url"],
        }
        for j in r.json().get("jobs", [])
    ]


def scrape_lever(company_slug):
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return [
        {
            "title": j["text"],
            "location": j.get("categories", {}).get("location", ""),
            "url": j["hostedUrl"],
        }
        for j in r.json()
    ]


def scrape_ashby(slug):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    jobs = []
    for j in r.json().get("jobs", []):
        jobs.append({
            "title": j["title"],
            "location": j.get("location", ""),
            "url": j.get("jobUrl") or j.get("applyUrl", ""),
        })
    return jobs


def scrape_html(url, selector):
    """HTML scraper with URL-based de-duplication."""
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []
    seen_urls = set()
    for el in soup.select(selector):
        title = el.get_text(strip=True)
        if not title:
            continue
        link_el = el if el.name == "a" else el.find("a")
        href = link_el.get("href") if link_el else url
        if href and href.startswith("/"):
            href = urljoin(url, href)
        href = href or url
        dedup_key = (href, title.lower())
        if dedup_key in seen_urls:
            continue
        seen_urls.add(dedup_key)
        jobs.append({"title": title, "location": "", "url": href})
    return jobs


def fetch_jobs(company):
    stype = company["scraper_type"].lower()

    if stype == "auto":
        detected, args = detect_scraper(company["url"])
        if not detected:
            print(f"  [!] Could not auto-detect scraper for {company['name']}. "
                  f"Set 'Scraper Type' to 'html:<css-selector>' in the sheet.")
            return []
        stype = detected
    else:
        args = None

    if stype == "greenhouse":
        return scrape_greenhouse(**args)
    if stype == "lever":
        return scrape_lever(**args)
    if stype == "ashby":
        return scrape_ashby(**args)
    if stype.startswith("html:"):
        selector = stype[5:].strip()
        return scrape_html(company["url"], selector)

    print(f"  [!] Unknown scraper type '{stype}' for {company['name']}")
    return []


# ---------- MATCHING ----------
def normalize_title(text):
    """
    Lowercase + replace separators with spaces, collapse whitespace.
    'ML/AI-Engineer, Sr.' → 'ml ai engineer sr'
    """
    text = text.lower()
    text = re.sub(r"[\/\-_,.()\[\]]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def matches(job, target_titles, target_country):
    norm_title = normalize_title(job["title"])
    norm_loc = normalize_title(job["location"])

    title_match = False

    for t in target_titles:
        norm_target = normalize_title(t)
        if not norm_target:
            continue

        # Method 1: strict token matching (preferred — fast, precise).
        # All tokens in your keyword must be present as whole words in the title.
        target_tokens = norm_target.split()
        title_tokens = norm_title.split()
        if all(tok in title_tokens for tok in target_tokens):
            title_match = True
            break

        # Method 2: fuzzy fallback for typos/minor variations.
        # Only triggers for keywords long enough that fuzz won't false-positive
        # on tiny words. token_set_ratio is word-aware unlike partial_ratio.
        if len(norm_target) >= FUZZY_MIN_LENGTH:
            score = fuzz.token_set_ratio(norm_target, norm_title)
            if score >= FUZZY_THRESHOLD:
                title_match = True
                break

    country_match = (
        not target_country
        or not norm_loc
        or normalize_title(target_country) in norm_loc
    )

    return title_match and country_match


def job_id(company, job):
    raw = f"{company}|{job['url']}|{job['title']}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


# ---------- NOTIFICATIONS ----------
def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        print("[!] Telegram creds missing — printing instead:\n" + text)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={
                "chat_id": TG_CHAT,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=15,
        )
    except Exception as e:
        print(f"[!] Telegram send failed: {e}")


# ---------- MAIN ----------
def main():
    companies = load_companies_from_sheet()
    print(f"Loaded {len(companies)} active companies from sheet.")

    first_run = not Path(SEEN_FILE).exists()
    seen = set(json.loads(Path(SEEN_FILE).read_text())) if not first_run else set()
    new_seen = set(seen)
    alerts = []
    total_jobs_scanned = 0

    for c in companies:
        print(f"→ {c['name']}")
        try:
            jobs = fetch_jobs(c)
        except Exception as e:
            print(f"  [!] Failed: {e}")
            continue

        company_matches = 0
        for job in jobs:
            jid = job_id(c["name"], job)
            is_match = matches(job, c["target_titles"], c["country"])
            if is_match:
                company_matches += 1

            if jid in seen:
                continue

            if is_match:
                new_seen.add(jid)
                if not first_run:
                    alerts.append((c["name"], job))

        total_jobs_scanned += len(jobs)
        print(f"  {len(jobs)} jobs scanned, {company_matches} match criteria")

    if first_run:
        baseline_count = len(new_seen)
        print(f"First run: baselining {baseline_count} matching jobs "
              f"(out of {total_jobs_scanned} total scanned). No alerts sent.")
        send_telegram(
            f"✅ Job agent activated\n"
            f"Tracking {len(companies)} companies\n"
            f"Found {baseline_count} jobs matching your criteria right now "
            f"(out of {total_jobs_scanned} open roles)\n"
            f"You'll get alerts only for NEW matching postings going forward."
        )
    else:
        for company, job in alerts:
            msg = (
                f"🎯 <b>New role at {company}</b>\n"
                f"<b>{job['title']}</b>\n"
                f"📍 {job['location'] or 'See posting'}\n"
                f"🔗 {job['url']}"
            )
            send_telegram(msg)
        print(f"Sent {len(alerts)} alerts.")

    Path(SEEN_FILE).write_text(json.dumps(sorted(new_seen), indent=2))


if __name__ == "__main__":
    main()
