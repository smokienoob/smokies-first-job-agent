"""
sheet_updater.py — Platform Auto-Detector (No Google API needed)
Reads your published Google Sheet CSV, detects the hiring platform
for each company, and writes platforms.json into the repo.

job_agent.py reads platforms.json to know which scraper to use.

Run manually whenever you add new companies:
  Actions tab → Update Sheet Platforms → Run workflow

No service account. No Google Cloud setup. Uses only SHEET_CSV_URL.
"""

import csv
import io
import json
import os
import re
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
        "canonical": lambda h, p: f"https://careers.smartrecruiters.com/{p.split('/')[0]}" if p else None,
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


def parse_url(raw: str):
    raw = raw.strip()
    if not raw.startswith("http"):
        raw = "https://" + raw
    try:
        u = urlparse(raw)
        return u.netloc.lower(), u.path.strip("/").lower()
    except Exception:
        return raw.lower(), ""


def detect_platform(url: str):
    host, path = parse_url(url)
    for rule in PLATFORM_RULES:
        try:
            if rule["detect"](host, path):
                return rule
        except Exception:
            continue
    return None


def best_url(rule, original_url: str) -> str:
    if rule is None:
        return original_url
    host, path = parse_url(original_url)
    try:
        c = rule["canonical"](host, path)
        return c if c else original_url
    except Exception:
        return original_url


def load_sheet():
    if not SHEET_CSV_URL:
        raise RuntimeError("SHEET_CSV_URL env var not set.")
    r = requests.get(SHEET_CSV_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    return list(reader)


def main():
    print("Reading sheet...")
    rows = load_sheet()

    # Load existing platforms.json so we don't overwrite manual overrides
    existing = {}
    if Path(OUTPUT_FILE).exists():
        try:
            existing = json.loads(Path(OUTPUT_FILE).read_text())
            print(f"  Loaded {len(existing)} existing entries from {OUTPUT_FILE}")
        except Exception:
            pass

    results = {}
    changed = 0

    for row in rows:
        # Normalise keys
        norm = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}
        company = norm.get("company", "").strip()
        url = norm.get("careers url", "").strip()
        active = norm.get("active", "true").upper()

        if not company or not url:
            continue

        rule = detect_platform(url)
        platform_name = rule["name"] if rule else "Unknown"
        scraper_key = rule["scraper_key"] if rule else ""
        if rule and rule["scraper_key"] is None:
            scraper_key = ""  # unsupported (e.g. iCIMS)

        corrected_url = best_url(rule, url)

        entry = {
            "platform": platform_name,
            "scraper_key": scraper_key,
            "url": corrected_url,
            "active": active not in ("FALSE", "NO", "0"),
        }

        old = existing.get(company, {})
        status = "✓" if old.get("platform") == platform_name else "→ NEW"
        if old.get("platform") != platform_name:
            changed += 1

        print(f"  {status:6} {company:<22} {platform_name:<25} scraper={scraper_key or '(none)'}")
        results[company] = entry

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
