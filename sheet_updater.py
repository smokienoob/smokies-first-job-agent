"""
sheet_updater.py — Platform Auto-Detector with Playwright fallback
Reads your published Google Sheet CSV, detects the hiring platform
for each company URL, and writes platforms.json into the repo.

Detection cascade for each company:
  1. URL pattern match (instant, free)
  2. Plain HTTP fetch + redirect + regex on HTML (~2 sec)
  3. Playwright browser render + regex on rendered HTML (~15 sec)

Step 3 catches JavaScript-heavy SPAs like Stripe, Uber, Atlassian etc.
where the ATS links only appear after JS runs.

Run via: Actions tab → Update Sheet Platforms → Run workflow
"""

import csv
import io
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

# ---------- CONFIG ----------
SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL")
OUTPUT_FILE = "platforms.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


# ---------- PLATFORM RULES ----------
PLATFORM_RULES = [
    {
        "name": "Greenhouse",
        "scraper_key": "greenhouse",
        "detect": lambda h, p: "greenhouse.io" in h,
        "canonical": lambda h, p: f"https://boards.greenhouse.io/{p.split('/')[0]}" if p else None,
    },
    {
        "name": "Lever",
        "scraper_key": "lever",
        "detect": lambda h, p: "lever.co" in h,
        "canonical": lambda h, p: f"https://jobs.lever.co/{p.split('/')[0]}" if p else None,
    },
    {
        "name": "Ashby",
        "scraper_key": "ashby",
        "detect": lambda h, p: "ashbyhq.com" in h,
        "canonical": lambda h, p: f"https://jobs.ashbyhq.com/{p.split('/')[0]}" if p else None,
    },
    {
        "name": "Workday",
        "scraper_key": "workday",
        "detect": lambda h, p: "myworkdayjobs.com" in h or "myworkdaysite.com" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "SmartRecruiters",
        "scraper_key": "smartrecruiters",
        "detect": lambda h, p: "smartrecruiters.com" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "BambooHR",
        "scraper_key": "bamboohr",
        "detect": lambda h, p: "bamboohr.com" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "Eightfold",
        "scraper_key": "eightfold",
        "detect": lambda h, p: "eightfold.ai" in h or "explore.jobs" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "Workable",
        "scraper_key": "workable",
        "detect": lambda h, p: "workable.com" in h or "apply.workable.com" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "SuccessFactors",
        "scraper_key": "successfactors",
        "detect": lambda h, p: "successfactors" in h or "/careersection/" in p,
        "canonical": lambda h, p: None,
    },
    {
        "name": "Taleo",
        "scraper_key": "taleo",
        "detect": lambda h, p: "taleo.net" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "Google Careers",
        "scraper_key": "playwright_google",
        "detect": lambda h, p: "google.com" in h and "careers" in p,
        "canonical": lambda h, p: "https://www.google.com/about/careers/applications",
    },
    {
        "name": "Microsoft Careers",
        "scraper_key": "playwright_microsoft",
        "detect": lambda h, p: "careers.microsoft.com" in h or "jobs.careers.microsoft.com" in h,
        "canonical": lambda h, p: "https://jobs.careers.microsoft.com/global/en/search",
    },
    {
        "name": "Revolut",
        "scraper_key": "playwright_revolut",
        "detect": lambda h, p: "revolut.com" in h and "career" in p,
        "canonical": lambda h, p: "https://www.revolut.com/careers/search",
    },
    {
        "name": "Airbnb",
        "scraper_key": "playwright_airbnb",
        "detect": lambda h, p: "careers.airbnb.com" in h or ("airbnb.com" in h and "career" in p),
        "canonical": lambda h, p: "https://careers.airbnb.com/positions/",
    },
    {
        "name": "iCIMS",
        "scraper_key": None,
        "detect": lambda h, p: "icims.com" in h,
        "canonical": lambda h, p: None,
    },
]

# Patterns to search for inside page HTML (rendered or static)
HTML_FINGERPRINTS = [
    ("Greenhouse",     "greenhouse",       r'boards\.greenhouse\.io/(?:embed/job_board\?for=)?([a-z0-9_-]+)',  lambda m: f"https://boards.greenhouse.io/{m.group(1)}"),
    ("Lever",          "lever",            r'jobs\.lever\.co/([a-z0-9_-]+)',                                   lambda m: f"https://jobs.lever.co/{m.group(1)}"),
    ("Ashby",          "ashby",            r'jobs\.ashbyhq\.com/([a-z0-9_-]+)',                                lambda m: f"https://jobs.ashbyhq.com/{m.group(1)}"),
    ("Workday",        "workday",          r'([a-z0-9-]+\.[a-z0-9]+\.myworkdayjobs\.com/[a-zA-Z0-9_-]+)',      lambda m: f"https://{m.group(1)}"),
    ("SmartRecruiters","smartrecruiters",  r'(?:careers|jobs)\.smartrecruiters\.com/([a-z0-9_-]+)',            lambda m: f"https://careers.smartrecruiters.com/{m.group(1)}"),
    ("Eightfold",      "eightfold",        r'([\w.-]+\.eightfold\.ai)',                                        lambda m: f"https://{m.group(1)}/careers"),
    ("Workable",       "workable",         r'apply\.workable\.com/([a-z0-9_-]+)',                              lambda m: f"https://apply.workable.com/{m.group(1)}"),
    ("BambooHR",       "bamboohr",         r'([a-z0-9_-]+)\.bamboohr\.com',                                    lambda m: f"https://{m.group(1)}.bamboohr.com/careers"),
    ("Taleo",          "taleo",            r'([\w.-]+\.taleo\.net)',                                           lambda m: f"https://{m.group(1)}"),
    ("iCIMS",          None,               r'icims\.com',                                                      lambda m: None),
]


def parse_url(raw: str):
    raw = raw.strip()
    if not raw.startswith("http"):
        raw = "https://" + raw
    try:
        u = urlparse(raw)
        return u.netloc.lower(), u.path.strip("/").lower()
    except Exception:
        return raw.lower(), ""


def detect_from_url(url: str):
    """Try to detect platform purely from the URL string."""
    host, path = parse_url(url)
    for rule in PLATFORM_RULES:
        try:
            if rule["detect"](host, path):
                canonical = None
                try:
                    canonical = rule["canonical"](host, path)
                except Exception:
                    pass
                return rule["name"], rule["scraper_key"], canonical or url
        except Exception:
            continue
    return None, None, None


def search_html_for_platform(html: str, base_url: str):
    """Look through HTML for ATS fingerprints. Returns (name, key, url) or all None."""
    # Check final URL (post-redirect or rendered page URL) against our URL rules first
    host, path = parse_url(base_url)
    for rule in PLATFORM_RULES:
        try:
            if rule["detect"](host, path):
                canonical = None
                try:
                    canonical = rule["canonical"](host, path)
                except Exception:
                    pass
                return rule["name"], rule["scraper_key"], canonical or base_url
        except Exception:
            continue

    # Now search the HTML body for ATS markers
    for platform_name, scraper_key, pattern, canonical_fn in HTML_FINGERPRINTS:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            try:
                canonical = canonical_fn(m)
            except Exception:
                canonical = base_url
            return platform_name, scraper_key, canonical or base_url

    return None, None, None


def detect_from_http(url: str):
    """Plain HTTP fetch + check redirects + scan HTML."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        return search_html_for_platform(r.text, r.url)
    except Exception as e:
        print(f"    [HTTP fetch failed: {e}]")
        return None, None, None


# ---------- PLAYWRIGHT FALLBACK ----------
_pw_browser = None

def _get_pw_browser():
    global _pw_browser
    if _pw_browser is None:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        _pw_browser = pw.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
        ])
    return _pw_browser


def _close_pw_browser():
    global _pw_browser
    if _pw_browser is not None:
        try:
            _pw_browser.close()
        except Exception:
            pass
        _pw_browser = None


def detect_from_playwright(url: str):
    """Render the page in a real browser, then scan rendered HTML for ATS markers."""
    try:
        browser = _get_pw_browser()
        ctx = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1366, "height": 800},
            locale="en-US",
        )
        page = ctx.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(5)  # let JS settle and inject ATS iframes

            # Try clicking common "View jobs" or "Open positions" buttons (best-effort)
            for txt in ["View open positions", "View all jobs", "See open roles",
                        "Browse jobs", "Open positions"]:
                try:
                    btn = page.get_by_text(txt, exact=False).first
                    if btn.is_visible(timeout=1000):
                        btn.click(timeout=2000)
                        time.sleep(2)
                        break
                except Exception:
                    continue

            html = page.content()
            current_url = page.url

            # Also collect href attributes — sometimes ATS links are in <a> tags
            # but cluttered with extra HTML; just appending hrefs makes regex easier
            try:
                hrefs = page.evaluate(
                    "() => Array.from(document.querySelectorAll('a')).map(a => a.href).join(' ')"
                )
                html = html + "\n" + hrefs
            except Exception:
                pass

            # Also check iframe srcs
            try:
                iframes = page.evaluate(
                    "() => Array.from(document.querySelectorAll('iframe')).map(f => f.src).join(' ')"
                )
                html = html + "\n" + iframes
            except Exception:
                pass

            return search_html_for_platform(html, current_url)
        finally:
            ctx.close()
    except Exception as e:
        print(f"    [Playwright failed: {e}]")
        return None, None, None


def detect_platform(url: str, use_playwright: bool = True):
    """Full detection cascade: URL → HTTP → Playwright."""
    name, key, canonical = detect_from_url(url)
    if name:
        return name, key, canonical

    print(f"    → fetching page (HTTP) to detect ATS...")
    name, key, canonical = detect_from_http(url)
    if name:
        return name, key, canonical

    if use_playwright:
        print(f"    → rendering with browser (this is slow, ~15s) to detect ATS...")
        name, key, canonical = detect_from_playwright(url)
        if name:
            return name, key, canonical

    return "Unknown", "", url


def load_sheet():
    if not SHEET_CSV_URL:
        raise RuntimeError("SHEET_CSV_URL env var not set.")
    r = requests.get(SHEET_CSV_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return list(csv.DictReader(io.StringIO(r.text)))


def main():
    print("Reading sheet...")
    rows = load_sheet()

    existing = {}
    if Path(OUTPUT_FILE).exists():
        try:
            existing = json.loads(Path(OUTPUT_FILE).read_text())
            print(f"  Loaded {len(existing)} existing entries from {OUTPUT_FILE}")
        except Exception:
            pass

    results = {}
    changed = 0

    try:
        for row in rows:
            norm = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}
            company = norm.get("company", "").strip()
            url = norm.get("careers url", "").strip()
            active = norm.get("active", "true").upper()

            if not company or not url:
                continue

            # Cache: skip companies we already detected as a known platform
            old = existing.get(company, {})
            if old.get("platform") and old["platform"] != "Unknown":
                results[company] = old
                print(f"  ✓      {company:<22} {old['platform']:<25} (cached)")
                continue

            platform_name, scraper_key, canonical_url = detect_platform(url, use_playwright=True)

            entry = {
                "platform": platform_name,
                "scraper_key": scraper_key or "",
                "url": canonical_url or url,
                "active": active not in ("FALSE", "NO", "0"),
            }

            status = "→ NEW" if old.get("platform") != platform_name else "✓"
            if old.get("platform") != platform_name:
                changed += 1

            print(f"  {status:<6} {company:<22} {platform_name:<25} scraper={scraper_key or '(none)'}")
            results[company] = entry
    finally:
        _close_pw_browser()

    Path(OUTPUT_FILE).write_text(json.dumps(results, indent=2))
    print(f"\nWrote {len(results)} entries to {OUTPUT_FILE} ({changed} changed).")
    print("\nPlatform summary:")

    by_platform = {}
    for c, e in results.items():
        p = e["platform"]
        by_platform.setdefault(p, []).append(c)
    for p, companies in sorted(by_platform.items()):
        print(f"  {p:<25} → {', '.join(companies)}")


if __name__ == "__main__":
    main()
