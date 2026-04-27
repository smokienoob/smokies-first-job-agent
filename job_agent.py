"""
Job Posting Notifier Agent — Google Sheets edition (v2.1)
- v2.1: Microsoft selector hardened. Debug dump fires when extraction returns
        zero jobs even if a selector matched.
- v2.0: Workday, SmartRecruiters, BambooHR, Taleo, SuccessFactors, Airbnb.
- v1.4: Playwright debug dump
- v1.3: Playwright support
- v1.2: Hardened fuzzy matching
- v1.1: Match-only baseline, smarter title matching, HTML scraper de-dup
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
            jobs.append({
                "title": j.get("name", ""),
                "location": loc_str,
                "url": job_url,
            })
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


# ---------- SUCCESSFACTORS ----------
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
    if not jobs:
        print(f"  [SuccessFactors] No jobs extracted from HTML.")
    return jobs


# ---------- TALEO ----------
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


# ---------- HTML FALLBACK ----------
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
        # Show all elements with role=listitem and their inner structure
        listitems_info = page.evaluate("""() => {
            const items = document.querySelectorAll('[role="listitem"]');
            return Array.from(items).slice(0, 5).map((el, i) => ({
                idx: i,
                outerHTML_preview: el.outerHTML.slice(0, 400),
                text_preview: el.innerText.slice(0, 200),
            }));
        }""")
        headings = page.evaluate("""() => {
            const els = document.querySelectorAll('h1, h2, h3');
            return Array.from(els).slice(0, 15).map(e => ({
                tag: e.tagName.toLowerCase(),
                text: e.innerText.trim().slice(0, 100),
                classes: e.className.slice(0, 80)
            }));
        }""")
        print(f"  [DEBUG {label}] title='{title}' url='{url}'")
        print(f"  [DEBUG {label}] body_text_preview:\n{'-'*40}\n{body_text[:800]}\n{'-'*40}")
        print(f"  [DEBUG {label}] first {len(listitems_info)} role=listitem elements:")
        for item in listitems_info:
            print(f"    [{item['idx']}] text: {item['text_preview']}")
            print(f"        html: {item['outerHTML_preview']}")
        print(f"  [DEBUG {label}] first {len(headings)} headings:")
        for h in headings:
            print(f"    <{h['tag']} class='{h['classes']}'> {h['text']}")
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
        # Trigger debug dump if extraction got nothing
        if not jobs:
            print("  [Playwright] Extracted 0 jobs despite matched selector. Dumping page.")
            _debug_dump(page, "google")
    finally:
        ctx.close()
    return _dedupe_jobs(jobs)


def scrape_playwright_microsoft(target_titles, target_country):
    """Microsoft Careers — improved extraction with broader title hunting."""
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

        # Try multiple containers
        candidates = [
            "div[role='listitem']",
            "[data-automation-id*='jobCard']",
            "div[class*='ms-List-cell']",
            "div[class*='SearchResultCard']",
            "div[class*='jobCard']",
        ]
        cards = []
        chosen_sel = None
        for sel in candidates:
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

        # Extract — try MANY title sources per card
        for card in cards:
            try:
                title = ""
                # Strategy: get all text content of the card, take first non-trivial line
                # that looks like a job title (more than 5 chars, not a button label)
                full_text = card.inner_text() if card else ""
                # Try heading first
                for tsel in ["h2", "h3", "h4", "[role='heading']",
                             "[class*='jobTitle']", "[class*='Title']",
                             "a[aria-label]"]:
                    title_el = card.query_selector(tsel)
                    if title_el:
                        # Try aria-label first if it's an anchor — often holds full title
                        aria = title_el.get_attribute("aria-label") if tsel.startswith("a") else None
                        cand = aria or title_el.inner_text().strip()
                        if cand and len(cand) > 5:
                            title = cand.split("\n")[0].strip()
                            break

                # Fallback: parse first meaningful line of text
                if not title and full_text:
                    lines = [l.strip() for l in full_text.split("\n") if l.strip()]
                    # Skip lines that look like buttons / metadata
                    skip_keywords = {"save", "apply", "share", "view", "details",
                                     "remote", "hybrid", "onsite", "full-time", "part-time"}
                    for line in lines:
                        line_lower = line.lower()
                        if line and len(line) > 5 and line_lower not in skip_keywords \
                                and not any(line_lower == k for k in skip_keywords):
                            title = line[:120]
                            break

                if not title:
                    continue

                loc_text = ""
                for lsel in ["[aria-label*='Location']", "[class*='location']",
                             "[class*='Location']"]:
                    loc_el = card.query_selector(lsel)
                    if loc_el:
                        loc_text = loc_el.inner_text().strip()
                        break

                link_el = card.query_selector("a")
                href = link_el.get_attribute("href") if link_el else ""
                if href and href.startswith("/"):
                    href = urljoin("https://jobs.careers.microsoft.com", href)
                jobs.append({"title": title, "location": loc_text, "url": href or url})
            except Exception:
                continue

        # If extraction failed entirely, dump the page
        if not jobs:
            print(f"  [Playwright] Extracted 0 jobs from {len(cards)} '{chosen_sel}' "
                  f"elements. Dumping page for diagnosis.")
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
            _debug_dump(page, "revolut")
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
        if not jobs:
            _debug_dump(page, "revolut")
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
            _debug_dump(page, "airbnb")
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
        if not jobs:
            _debug_dump(page, "airbnb")
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

                if jid in seen:
                    continue
                if is_match:
                    new_seen.add(jid)
                    if not first_run:
                        alerts.append((c["name"], job))

            total_jobs_scanned += len(jobs)
            print(f"  {len(jobs)} jobs scanned, {company_matches} match criteria")
            if jobs and company_matches == 0 and sample_misses:
                print(f"  Sample non-matches:")
                for m in sample_misses:
                    print(f"    - title='{m['title']}' location='{m['location']}'")
    finally:
        _close_browser()

    if first_run:
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
