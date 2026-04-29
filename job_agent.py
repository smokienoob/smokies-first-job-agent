"""
Job Posting Notifier Agent — Google Sheets edition (v2.2)
- v2.2: Eightfold (Netflix), Microsoft scoped to search results,
        FORCE_ALERTS test mode
- v2.1: Microsoft selector hardened, smarter debug
- v2.0: Workday, SmartRecruiters, BambooHR, Taleo, SuccessFactors, Airbnb
- v1.x: foundations
"""
import csv
import io
import json
import os
import re
import hashlib
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, quote
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

# ---------- CONFIG ----------
SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL")
SEEN_FILE = "seen_jobs.json"
TG_TOKEN = os.environ.get("TG_BOT_TOKEN")
TG_CHAT = os.environ.get("TG_CHAT_ID")

# Set FORCE_ALERTS=1 in workflow env to re-alert on all current matches once.
# Useful for testing notifications without waiting for new jobs.
FORCE_ALERTS = os.environ.get("FORCE_ALERTS", "").lower() in ("1", "true", "yes")

FUZZY_THRESHOLD = 90
FUZZY_MIN_LENGTH = 8
DEBUG_PLAYWRIGHT = True

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


# ---------- SHEET LOADER ----------
"""
PATCH for job_agent.py — replace load_companies_from_sheet() with this version.

This reads the sheet CSV as before, but ALSO reads platforms.json (written by
sheet_updater.py) to get the correct scraper_key per company.

platforms.json takes priority over whatever is in the 'Scraper Type' column.
If a company isn't in platforms.json yet, it falls back to the sheet value.
"""

def load_companies_from_sheet():
    if not SHEET_CSV_URL:
        raise RuntimeError("SHEET_CSV_URL env var not set")

    # Load platforms.json if it exists
    platforms = {}
    platforms_file = Path("platforms.json")
    if platforms_file.exists():
        try:
            platforms = json.loads(platforms_file.read_text())
        except Exception as e:
            print(f"[!] Could not read platforms.json: {e}")

    r = requests.get(SHEET_CSV_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    companies = []

    for row in reader:
        row = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}
        if not row.get("company") or not row.get("careers url"):
            continue
        if row.get("active", "true").upper() in ("FALSE", "NO", "0"):
            continue

        company_name = row["company"]

        # Get scraper type: platforms.json takes priority over sheet column
        platform_entry = platforms.get(company_name, {})
        scraper_key = platform_entry.get("scraper_key", "")
        # Use corrected URL from platforms.json if available
        url = platform_entry.get("url") or row.get("careers url", "")

        # Fall back to sheet's Scraper Type column if platforms.json has nothing
        if not scraper_key:
            scraper_key = row.get("scraper type", "auto") or "auto"

        # If still empty (unsupported platform), set to auto so detect_scraper runs
        # (it will print a warning if it can't detect)
        if not scraper_key:
            scraper_key = "auto"

        companies.append({
            "name": company_name,
            "url": url,
            "scraper_type": scraper_key,
            "target_titles": [t.strip() for t in row.get("target titles", "").split(",") if t.strip()],
            "country": row.get("country", ""),
        })

    if platforms:
        print(f"  (platforms.json loaded — {len(platforms)} entries)")

    return companies

# ---------- AUTO-DETECT ----------
def detect_scraper(url):
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.strip("/")
    path_parts = path.split("/") if path else []

    if "greenhouse.io" in host:
        token = path_parts[0] if path_parts else None
        if token:
            return "greenhouse", {"board_token": token}

    if "lever.co" in host:
        slug = path_parts[0] if path_parts else None
        if slug:
            return "lever", {"company_slug": slug}

    if "ashbyhq.com" in host:
        slug = path_parts[0] if path_parts else None
        if slug:
            return "ashby", {"slug": slug}

    if "smartrecruiters.com" in host:
        slug = path_parts[0] if path_parts else None
        if slug:
            return "smartrecruiters", {"company_slug": slug}

    if "myworkdayjobs.com" in host:
        host_parts = host.split(".")
        if len(host_parts) >= 4:
            tenant = host_parts[0]
            data_center = host_parts[1]
            site_idx = 0
            if path_parts and re.match(r"^[a-z]{2}(-[A-Z]{2})?$", path_parts[0]):
                site_idx = 1
            site = path_parts[site_idx] if len(path_parts) > site_idx else "External"
            return "workday", {"tenant": tenant, "data_center": data_center, "site": site}

    if "myworkdaysite.com" in host:
        if len(path_parts) >= 3 and path_parts[0] == "recruiting":
            return "workday_alt", {"tenant": path_parts[1], "site": path_parts[2]}

    if "bamboohr.com" in host:
        sub = host.split(".")[0]
        if sub and sub != "www":
            return "bamboohr", {"company_slug": sub}

    # Eightfold: jobs.<tenant>.net or explore.jobs.<tenant>.net
    if "jobs.netflix.net" in host or ".eightfold.ai" in host:
        return "eightfold", {"raw_url": url}
    # Generic Eightfold detection: explore.jobs.<x>.net pattern
    if "explore.jobs" in host:
        return "eightfold", {"raw_url": url}

    if "successfactors" in host or "/careersection/" in path:
        return "successfactors", {"raw_url": url}

    if "taleo.net" in host:
        return "taleo", {"raw_url": url}

    if "revolut.com" in host and "career" in path:
        return "playwright_revolut", {}

    if "google.com" in host and "careers" in path:
        return "playwright_google", {}

    if ("careers.airbnb.com" in host or
            ("airbnb.com" in host and "career" in path)):
        return "playwright_airbnb", {}

    if "careers.microsoft.com" in host or "jobs.careers.microsoft.com" in host:
        return "playwright_microsoft", {}

    return None, None


# ---------- API SCRAPERS ----------
def scrape_greenhouse(board_token):
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return [
        {"title": j["title"], "location": j.get("location", {}).get("name", ""),
         "url": j["absolute_url"]}
        for j in r.json().get("jobs", [])
    ]


def scrape_lever(company_slug):
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return [
        {"title": j["text"], "location": j.get("categories", {}).get("location", ""),
         "url": j["hostedUrl"]}
        for j in r.json()
    ]


def scrape_ashby(slug):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return [
        {"title": j["title"], "location": j.get("location", ""),
         "url": j.get("jobUrl") or j.get("applyUrl", "")}
        for j in r.json().get("jobs", [])
    ]


def scrape_smartrecruiters(company_slug):
    base = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings"
    jobs = []
    offset = 0
    limit = 100
    for _ in range(20):
        r = requests.get(f"{base}?limit={limit}&offset={offset}",
                         headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        content = data.get("content", [])
        if not content:
            break
        for j in content:
            loc = j.get("location", {}) or {}
            loc_str = ", ".join(filter(None, [
                loc.get("city", ""), loc.get("region", ""), loc.get("country", "")
            ]))
            job_url = f"https://jobs.smartrecruiters.com/{company_slug}/{j.get('id', '')}"
            jobs.append({"title": j.get("name", ""), "location": loc_str, "url": job_url})
        if len(content) < limit:
            break
        offset += limit
    return jobs


def scrape_bamboohr(company_slug):
    url = f"https://{company_slug}.bamboohr.com/careers/list"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    jobs = []
    for j in data.get("result", []):
        loc_obj = j.get("location", {}) or {}
        loc_str = ", ".join(filter(None, [
            loc_obj.get("city", ""), loc_obj.get("state", ""),
            loc_obj.get("country", "")
        ]))
        title = (j.get("jobOpeningName") or "").strip()
        if not title:
            continue
        job_id_val = j.get("id")
        job_url = f"https://{company_slug}.bamboohr.com/careers/{job_id_val}" if job_id_val else url
        jobs.append({"title": title, "location": loc_str, "url": job_url})
    return jobs


def scrape_eightfold(raw_url):
    """Eightfold (Netflix, others). Paginates the public API.
 
    The page is served at e.g. https://explore.jobs.netflix.net/careers
    and the underlying API is at /api/apply/v2/jobs on the same host.
    """
    from urllib.parse import urlparse, urljoin
    parsed = urlparse(raw_url)
    host = parsed.netloc.lower()
 
    # The 'domain' parameter the API expects is the company's main domain
    # (e.g. netflix.com), derived from the explore.jobs.<x>.net subdomain.
    if "explore.jobs." in host:
        # explore.jobs.netflix.net → netflix.com
        domain_part = host.replace("explore.jobs.", "").rsplit(".", 1)[0]
        domain = f"{domain_part}.com"
    else:
        # Fallback: just strip jobs/careers prefixes
        domain = host.replace("jobs.", "").replace("careers.", "")
 
    api_base = f"https://{host}/api/apply/v2/jobs"
    jobs = []
    page_size = 100
    max_pages = 5  # 500 jobs total cap
 
    for page in range(max_pages):
        start = page * page_size
        params = {
            "domain": domain,
            "start": start,
            "num": page_size,
            "exact_phrase": "",
            "query": "",
            "Country": "",
            "Location": "",
        }
        try:
            r = requests.get(api_base, params=params, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                if page == 0:
                    print(f"  [Eightfold] API returned {r.status_code}, falling back to HTML")
                    return _scrape_eightfold_html(raw_url)
                else:
                    break
            data = r.json()
        except Exception as e:
            if page == 0:
                print(f"  [Eightfold] API call failed ({e}), falling back to HTML")
                return _scrape_eightfold_html(raw_url)
            else:
                break
 
        positions = data.get("positions", [])
        if not positions:
            break
 
        jobs.extend(_parse_eightfold_positions(positions))
 
        # Stop if we got fewer than requested (no more pages)
        if len(positions) < page_size:
            break
 
        time.sleep(0.3)  # be polite
 
    print(f"  [Eightfold] Retrieved {len(jobs)} jobs across {page + 1} page(s)")
    return jobs
 
 
def _scrape_eightfold_html(raw_url):
    """Fallback: extract embedded JSON from the careers page HTML."""
    try:
        r = requests.get(raw_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"  [Eightfold] HTML fetch failed: {e}")
        return []
 
    match = re.search(r'"positions"\s*:\s*(\[.*?\])\s*,\s*"', r.text, re.DOTALL)
    if not match:
        match = re.search(r'"positions"\s*:\s*(\[[^\]]*\])', r.text)
    if not match:
        print(f"  [Eightfold] Could not find positions JSON in HTML.")
        return []
 
    try:
        positions = json.loads(match.group(1))
    except Exception as e:
        print(f"  [Eightfold] JSON parse failed: {e}")
        return []
    return _parse_eightfold_positions(positions)
 
 
def _parse_eightfold_positions(positions):
    jobs = []
    for p in positions:
        title = p.get("name") or p.get("posting_name") or ""
        if not title:
            continue
        locs = p.get("locations") or [p.get("location", "")]
        if isinstance(locs, list):
            loc_str = " | ".join(filter(None, locs))
        else:
            loc_str = str(locs)
        url = p.get("canonicalPositionUrl") or ""
        jobs.append({"title": title, "location": loc_str, "url": url})
    return jobs
 







# ---------- WORKDAY ----------
def scrape_workday(tenant, data_center, site):
    api_url = f"https://{tenant}.{data_center}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
    base_host = f"{tenant}.{data_center}.myworkdayjobs.com"
    return _scrape_workday_endpoint(api_url, base_host)


def scrape_workday_alt(tenant, site):
    api_url = f"https://jobs.myworkdaysite.com/wday/cxs/{tenant}/{site}/jobs"
    return _scrape_workday_endpoint(api_url, "jobs.myworkdaysite.com")


def _scrape_workday_endpoint(api_url, base_host):
    jobs = []
    offset = 0
    limit = 20
    for _ in range(25):
        body = {"appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""}
        try:
            r = requests.post(api_url,
                              headers={**HEADERS, "Content-Type": "application/json"},
                              json=body, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"  [Workday] POST failed: {e}")
            break
        data = r.json()
        postings = data.get("jobPostings", [])
        if not postings:
            break
        for j in postings:
            ext = j.get("externalPath", "")
            job_url = f"https://{base_host}{ext}" if ext.startswith("/") else f"https://{base_host}/{ext}"
            jobs.append({
                "title": j.get("title", ""),
                "location": j.get("locationsText", "") or j.get("primaryLocation", ""),
                "url": job_url,
            })
        total = data.get("total", 0)
        if offset + limit >= total or len(postings) < limit:
            break
        offset += limit
        time.sleep(0.5)
    return jobs


def scrape_successfactors(raw_url):
    try:
        r = requests.get(raw_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"  [SuccessFactors] Fetch failed: {e}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []
    seen_urls = set()
    for el in soup.select("a[href*='jobReqId'], a[href*='jobsearch'], "
                          "a[href*='jobdetails'], a.jobTitle-link"):
        title = el.get_text(strip=True)
        href = el.get("href", "")
        if not title or len(title) < 3:
            continue
        if href.startswith("/"):
            href = urljoin(raw_url, href)
        key = (href, title.lower())
        if key in seen_urls:
            continue
        seen_urls.add(key)
        jobs.append({"title": title, "location": "", "url": href})
    return jobs


def scrape_taleo(raw_url):
    try:
        r = requests.get(raw_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"  [Taleo] Fetch failed: {e}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []
    seen_urls = set()
    for el in soup.select("a[href*='jobdetail'], a[href*='jobDetails'], "
                          "a[href*='requisition']"):
        title = el.get_text(strip=True)
        href = el.get("href", "")
        if not title or len(title) < 3:
            continue
        if href.startswith("/"):
            href = urljoin(raw_url, href)
        key = (href, title.lower())
        if key in seen_urls:
            continue
        seen_urls.add(key)
        jobs.append({"title": title, "location": "", "url": href})
    return jobs


def scrape_html(url, selector):
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
        key = (href, title.lower())
        if key in seen_urls:
            continue
        seen_urls.add(key)
        jobs.append({"title": title, "location": "", "url": href})
    return jobs


# ---------- PLAYWRIGHT INFRA ----------
_playwright_browser = None

def _get_browser():
    global _playwright_browser
    if _playwright_browser is None:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        _playwright_browser = pw.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
        ])
    return _playwright_browser


def _close_browser():
    global _playwright_browser
    if _playwright_browser is not None:
        try:
            _playwright_browser.close()
        except Exception:
            pass
        _playwright_browser = None


def _new_page():
    browser = _get_browser()
    ctx = browser.new_context(
        user_agent=HEADERS["User-Agent"],
        viewport={"width": 1366, "height": 800},
        locale="en-US",
    )
    return ctx, ctx.new_page()


def _debug_dump(page, label):
    if not DEBUG_PLAYWRIGHT:
        return
    try:
        title = page.title()
        url = page.url
        body_text = page.evaluate("() => document.body.innerText.slice(0, 1500)")
        listitems_info = page.evaluate("""() => {
            const items = document.querySelectorAll('[role="listitem"]');
            return Array.from(items).slice(0, 5).map((el, i) => ({
                idx: i,
                outerHTML_preview: el.outerHTML.slice(0, 400),
                text_preview: el.innerText.slice(0, 200),
            }));
        }""")
        print(f"  [DEBUG {label}] title='{title}' url='{url}'")
        print(f"  [DEBUG {label}] body_text_preview:\n{'-'*40}\n{body_text[:800]}\n{'-'*40}")
        print(f"  [DEBUG {label}] first {len(listitems_info)} role=listitem:")
        for item in listitems_info:
            print(f"    [{item['idx']}] text: {item['text_preview']}")
            print(f"        html: {item['outerHTML_preview']}")
    except Exception as e:
        print(f"  [DEBUG {label}] dump failed: {e}")


def _dedupe_jobs(jobs):
    seen = set()
    unique = []
    for j in jobs:
        key = (j["url"], j["title"].lower())
        if key in seen:
            continue
        seen.add(key)
        unique.append(j)
    return unique


# ---------- PLAYWRIGHT SCRAPERS ----------
def scrape_playwright_google(target_titles, target_country):
    query = " ".join(target_titles) if target_titles else ""
    base = "https://www.google.com/about/careers/applications/jobs/results/"
    params = []
    if query:
        params.append(f"q={quote(query)}")
    if target_country:
        params.append(f"location={quote(target_country.split(',')[0].strip())}")
    url = base + ("?" + "&".join(params) if params else "")
    print(f"  [Playwright] GET {url}")

    ctx, page = _new_page()
    jobs = []
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(5)
        cards = []
        for sel in ["li.lLd3Je", "[role=listitem]", "ul li:has(h3)"]:
            cards = page.query_selector_all(sel)
            if cards:
                print(f"  [Playwright] Selector '{sel}' matched {len(cards)} elements")
                break
        if not cards:
            _debug_dump(page, "google")
            return []

        for card in cards:
            try:
                title_el = card.query_selector("h3, h2")
                if not title_el:
                    continue
                title = title_el.inner_text().strip()
                if not title:
                    continue
                loc_text = ""
                for loc_sel in ["[aria-label*='Location']", ".pwO9Dc", ".r0wTof"]:
                    loc_el = card.query_selector(loc_sel)
                    if loc_el:
                        loc_text = loc_el.inner_text().strip()
                        break
                link_el = card.query_selector("a")
                href = link_el.get_attribute("href") if link_el else ""
                if href and href.startswith("/"):
                    href = urljoin("https://www.google.com", href)
                jobs.append({"title": title, "location": loc_text, "url": href or url})
            except Exception:
                continue
    finally:
        ctx.close()
    return _dedupe_jobs(jobs)


def scrape_playwright_microsoft(target_titles, target_country):
    """Microsoft Careers — scoped to results region only (avoid filter sidebar)."""
    query = " ".join(target_titles) if target_titles else ""
    base = "https://jobs.careers.microsoft.com/global/en/search"
    params = []
    if query:
        params.append(f"q={quote(query)}")
    if target_country:
        params.append(f"lc={quote(target_country.split(',')[0].strip())}")
    params.append("pgSz=20")
    url = base + "?" + "&".join(params)
    print(f"  [Playwright] GET {url}")

    ctx, page = _new_page()
    jobs = []
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(8)

        # Scope the search to the main results region. Microsoft's filter
        # sidebar also has role=listitem — we need to avoid it.
        # Try scoped selectors that target ONLY the results column.
        scoped_candidates = [
            # Anchor links to specific job pages — most reliable
            "a[href*='/global/en/job/']",
            "a[href*='/job/']",
            # Cards inside main content
            "main [role='listitem']",
            "[role='main'] [role='listitem']",
            "main div[class*='ms-List-cell']",
            "div[data-automation-id*='jobCard']",
        ]
        cards = []
        chosen_sel = None
        for sel in scoped_candidates:
            try:
                found = page.query_selector_all(sel)
                if found:
                    print(f"  [Playwright] Selector '{sel}' matched {len(found)} elements")
                    cards = found
                    chosen_sel = sel
                    break
            except Exception:
                continue

        if not cards:
            _debug_dump(page, "microsoft")
            return []

        for card in cards:
            try:
                # If the card itself is an anchor with aria-label, that's typically the title
                tag = card.evaluate("el => el.tagName")
                title = ""
                href = ""

                if tag == "A":
                    aria = card.get_attribute("aria-label") or ""
                    href = card.get_attribute("href") or ""
                    if aria:
                        title = aria.strip()
                    else:
                        # Take first line of inner text
                        raw = card.inner_text().strip()
                        title = raw.split("\n")[0][:120] if raw else ""
                else:
                    # Card-style element — look for title inside
                    for tsel in ["h2", "h3", "h4", "[role='heading']",
                                 "a[aria-label]", "[class*='jobTitle']"]:
                        title_el = card.query_selector(tsel)
                        if title_el:
                            aria = title_el.get_attribute("aria-label")
                            cand = aria or title_el.inner_text().strip()
                            if cand and len(cand) > 5:
                                title = cand.split("\n")[0]
                                break
                    a = card.query_selector("a")
                    if a:
                        href = a.get_attribute("href") or ""

                if not title or len(title) < 5:
                    continue

                # Filter out obvious non-titles (filter labels, button text)
                title_lower = title.lower()
                if title_lower in {"save", "apply", "share", "view", "details",
                                   "architecture", "engineering", "research",
                                   "computer science", "software engineering"}:
                    continue
                # If it's a single word with no spaces and short, likely a category
                if len(title.split()) == 1 and len(title) < 15:
                    continue

                if href and href.startswith("/"):
                    href = urljoin("https://jobs.careers.microsoft.com", href)

                loc_text = ""
                if tag != "A":
                    for lsel in ["[aria-label*='Location']", "[class*='location']"]:
                        loc_el = card.query_selector(lsel)
                        if loc_el:
                            loc_text = loc_el.inner_text().strip()
                            break

                jobs.append({"title": title, "location": loc_text, "url": href or url})
            except Exception:
                continue

        if not jobs:
            print(f"  [Playwright] Extracted 0 jobs from {len(cards)} '{chosen_sel}' "
                  f"elements. Dumping page.")
            _debug_dump(page, "microsoft")
    finally:
        ctx.close()
    return _dedupe_jobs(jobs)


def scrape_playwright_revolut(target_titles, target_country):
    query = " ".join(target_titles) if target_titles else ""
    base = "https://www.revolut.com/careers/search"
    params = []
    if query:
        params.append(f"text={quote(query)}")
    if target_country:
        params.append(f"country={quote(target_country.split(',')[0].strip())}")
    url = base + ("?" + "&".join(params) if params else "")
    print(f"  [Playwright] GET {url}")

    ctx, page = _new_page()
    jobs = []
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(6)
        cards = []
        for sel in ["a[href*='/careers/position/']", "div[class*='JobCard']",
                    "[data-testid*='job']"]:
            cards = page.query_selector_all(sel)
            if cards:
                print(f"  [Playwright] Selector '{sel}' matched {len(cards)} elements")
                break
        if not cards:
            return []

        for card in cards:
            try:
                title_el = card.query_selector("h2, h3, h4, [class*='title']")
                title = title_el.inner_text().strip() if title_el else \
                        card.inner_text().strip().split("\n")[0][:100]
                if not title:
                    continue
                loc_el = card.query_selector("[class*='location'], [class*='Location']")
                loc_text = loc_el.inner_text().strip() if loc_el else ""
                tag = card.evaluate("el => el.tagName")
                if tag == "A":
                    href = card.get_attribute("href")
                else:
                    a = card.query_selector("a")
                    href = a.get_attribute("href") if a else ""
                if href and href.startswith("/"):
                    href = urljoin("https://www.revolut.com", href)
                jobs.append({"title": title, "location": loc_text, "url": href or url})
            except Exception:
                continue
    finally:
        ctx.close()
    return _dedupe_jobs(jobs)


def scrape_playwright_airbnb(target_titles, target_country):
    base = "https://careers.airbnb.com/positions/"
    print(f"  [Playwright] GET {base}")

    ctx, page = _new_page()
    jobs = []
    try:
        page.goto(base, wait_until="domcontentloaded", timeout=45000)
        time.sleep(6)
        for _ in range(4):
            page.mouse.wheel(0, 2000)
            time.sleep(1)

        cards = []
        for sel in ["a[href*='/positions/']", "div[class*='position']"]:
            found = page.query_selector_all(sel)
            if found and len(found) > 1:
                print(f"  [Playwright] Selector '{sel}' matched {len(found)} elements")
                cards = found
                break
        if not cards:
            return []

        for card in cards:
            try:
                title_el = card.query_selector("h2, h3, h4, [class*='title']")
                title = title_el.inner_text().strip() if title_el else ""
                if not title:
                    raw = card.inner_text().strip()
                    title = raw.split("\n")[0][:120] if raw else ""
                if not title or len(title) < 3:
                    continue
                loc_el = card.query_selector("[class*='location'], [class*='Location']")
                loc_text = loc_el.inner_text().strip() if loc_el else ""
                tag = card.evaluate("el => el.tagName")
                if tag == "A":
                    href = card.get_attribute("href")
                else:
                    a = card.query_selector("a")
                    href = a.get_attribute("href") if a else ""
                if href and href.startswith("/"):
                    href = urljoin("https://careers.airbnb.com", href)
                jobs.append({"title": title, "location": loc_text, "url": href or base})
            except Exception:
                continue
    finally:
        ctx.close()
    return _dedupe_jobs(jobs)


# ---------- DISPATCHER ----------
def fetch_jobs(company):
    stype = company["scraper_type"].lower()
    if stype == "auto":
        detected, args = detect_scraper(company["url"])
        if not detected:
            print(f"  [!] Could not auto-detect scraper for {company['name']}.")
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
    if stype == "smartrecruiters":
        return scrape_smartrecruiters(**args)
    if stype == "bamboohr":
        return scrape_bamboohr(**args)
    if stype == "eightfold":
        return scrape_eightfold(**args)
    if stype == "workday":
        return scrape_workday(**args)
    if stype == "workday_alt":
        return scrape_workday_alt(**args)
    if stype == "successfactors":
        return scrape_successfactors(**args)
    if stype == "taleo":
        return scrape_taleo(**args)
    if stype == "playwright_google":
        return scrape_playwright_google(company["target_titles"], company["country"])
    if stype == "playwright_microsoft":
        return scrape_playwright_microsoft(company["target_titles"], company["country"])
    if stype == "playwright_revolut":
        return scrape_playwright_revolut(company["target_titles"], company["country"])
    if stype == "playwright_airbnb":
        return scrape_playwright_airbnb(company["target_titles"], company["country"])
    if stype.startswith("html:"):
        return scrape_html(company["url"], stype[5:].strip())

    print(f"  [!] Unknown scraper type '{stype}' for {company['name']}")
    return []


# ---------- MATCHING ----------
def normalize_title(text):
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
        target_tokens = norm_target.split()
        title_tokens = norm_title.split()
        if all(tok in title_tokens for tok in target_tokens):
            title_match = True
            break
        if len(norm_target) >= FUZZY_MIN_LENGTH:
            if fuzz.token_set_ratio(norm_target, norm_title) >= FUZZY_THRESHOLD:
                title_match = True
                break

    country_match = False
    if not target_country:
        country_match = True
    elif not norm_loc:
        country_match = True
    else:
        for c in target_country.split(","):
            c_norm = normalize_title(c)
            if c_norm and c_norm in norm_loc:
                country_match = True
                break
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
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": False},
            timeout=15,
        )
    except Exception as e:
        print(f"[!] Telegram send failed: {e}")


# ---------- MAIN ----------
def main():
    companies = load_companies_from_sheet()
    print(f"Loaded {len(companies)} active companies from sheet.")
    if FORCE_ALERTS:
        print("FORCE_ALERTS=1 set — will send alerts for ALL current matches.")

    first_run = not Path(SEEN_FILE).exists()
    seen = set(json.loads(Path(SEEN_FILE).read_text())) if not first_run else set()
    new_seen = set(seen)
    alerts = []
    total_jobs_scanned = 0

    try:
        for c in companies:
            print(f"→ {c['name']} (country='{c['country']}', titles={c['target_titles']})")
            try:
                jobs = fetch_jobs(c)
            except Exception as e:
                print(f"  [!] Failed: {e}")
                continue

            company_matches = 0
            sample_misses = []
            for job in jobs:
                jid = job_id(c["name"], job)
                is_match = matches(job, c["target_titles"], c["country"])
                if is_match:
                    company_matches += 1
                else:
                    if len(sample_misses) < 3:
                        sample_misses.append(job)

                if is_match:
                    new_seen.add(jid)
                    # In FORCE_ALERTS mode, alert on every match regardless of seen.
                    # Otherwise: only new matches not in seen.
                    if FORCE_ALERTS:
                        alerts.append((c["name"], job))
                    elif jid not in seen and not first_run:
                        alerts.append((c["name"], job))

            total_jobs_scanned += len(jobs)
            print(f"  {len(jobs)} jobs scanned, {company_matches} match criteria")
            if jobs and company_matches == 0 and sample_misses:
                print(f"  Sample non-matches:")
                for m in sample_misses:
                    print(f"    - title='{m['title']}' location='{m['location']}'")
    finally:
        _close_browser()

    if first_run and not FORCE_ALERTS:
        baseline_count = len(new_seen)
        print(f"First run: baselining {baseline_count} matching jobs "
              f"(out of {total_jobs_scanned} total scanned). No alerts sent.")
        send_telegram(
            f"✅ Job agent activated\n"
            f"Tracking {len(companies)} companies\n"
            f"Found {baseline_count} jobs matching your criteria right now "
            f"(out of {total_jobs_scanned} open roles)"
        )
    else:
        # Cap alerts to avoid Telegram rate-limit floods
        max_alerts = 25
        if len(alerts) > max_alerts:
            print(f"Capping alerts at {max_alerts} (had {len(alerts)} matches)")
            alerts = alerts[:max_alerts]
        for company, job in alerts:
            msg = (
                f"🎯 <b>New role at {company}</b>\n"
                f"<b>{job['title']}</b>\n"
                f"📍 {job['location'] or 'See posting'}\n"
                f"🔗 {job['url']}"
            )
            send_telegram(msg)
            time.sleep(0.3)  # gentle rate-limit
        print(f"Sent {len(alerts)} alerts.")

    Path(SEEN_FILE).write_text(json.dumps(sorted(new_seen), indent=2))


if __name__ == "__main__":
    main()
