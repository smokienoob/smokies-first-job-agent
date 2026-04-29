"""
sheet_updater.py — Platform Auto-Detector
Reads your Google Sheet, detects the hiring platform for each company
from its Careers URL, and writes the result back into the "Platform" column.

Run manually whenever you add new companies:
    python sheet_updater.py

Or trigger it via GitHub Actions (see updater_workflow.yml).

Requirements:
    pip install requests gspread google-auth

Environment variables required:
    SHEET_CSV_URL       — published CSV URL (same as job_agent.py uses)
    GOOGLE_CREDENTIALS  — service account JSON as a string (for write access)
    SHEET_ID            — the Google Sheet ID (from the URL)
"""

import json
import os
import re
from urllib.parse import urlparse

import gspread
import requests
from google.oauth2.service_account import Credentials

# ---------- CONFIG ----------
SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL")
SHEET_ID = os.environ.get("SHEET_ID")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS")

# Column header names (case-insensitive match)
COL_CAREERS_URL = "careers url"
COL_PLATFORM = "platform"
COL_SCRAPER_TYPE = "scraper type"   # this is what job_agent.py reads

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


# ---------- PLATFORM DETECTION ----------
# Each rule: (platform_name, scraper_key, reliability, detect_fn, canonical_url_fn)
# scraper_key maps directly to the if/elif chain in job_agent.py
# reliability: "api" = fast/stable, "playwright" = slow/fragile, "html" = medium

PLATFORM_RULES = [
    {
        "name": "Greenhouse",
        "scraper_key": "greenhouse",
        "reliability": "api",
        "detect": lambda h, p: "greenhouse.io" in h,
        "canonical": lambda h, p: f"https://boards.greenhouse.io/{p.split('/')[0]}" if p else None,
    },
    {
        "name": "Lever",
        "scraper_key": "lever",
        "reliability": "api",
        "detect": lambda h, p: "lever.co" in h,
        "canonical": lambda h, p: f"https://jobs.lever.co/{p.split('/')[0]}" if p else None,
    },
    {
        "name": "Ashby",
        "scraper_key": "ashby",
        "reliability": "api",
        "detect": lambda h, p: "ashbyhq.com" in h,
        "canonical": lambda h, p: f"https://jobs.ashbyhq.com/{p.split('/')[0]}" if p else None,
    },
    {
        "name": "Workday",
        "scraper_key": "workday",
        "reliability": "api",
        "detect": lambda h, p: "myworkdayjobs.com" in h or "myworkdaysite.com" in h,
        "canonical": lambda h, p: None,  # keep original — tenant/site vary
    },
    {
        "name": "SmartRecruiters",
        "scraper_key": "smartrecruiters",
        "reliability": "api",
        "detect": lambda h, p: "smartrecruiters.com" in h,
        "canonical": lambda h, p: f"https://careers.smartrecruiters.com/{p.split('/')[0]}" if p else None,
    },
    {
        "name": "BambooHR",
        "scraper_key": "bamboohr",
        "reliability": "api",
        "detect": lambda h, p: "bamboohr.com" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "Eightfold",
        "scraper_key": "eightfold",
        "reliability": "api",
        "detect": lambda h, p: "eightfold.ai" in h or "explore.jobs" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "SuccessFactors",
        "scraper_key": "successfactors",
        "reliability": "html",
        "detect": lambda h, p: "successfactors" in h or "/careersection/" in p,
        "canonical": lambda h, p: None,
    },
    {
        "name": "Taleo",
        "scraper_key": "taleo",
        "reliability": "html",
        "detect": lambda h, p: "taleo.net" in h,
        "canonical": lambda h, p: None,
    },
    {
        "name": "Google Careers",
        "scraper_key": "playwright_google",
        "reliability": "playwright",
        "detect": lambda h, p: "google.com" in h and "careers" in p,
        "canonical": lambda h, p: "https://www.google.com/about/careers/applications",
    },
    {
        "name": "Microsoft Careers",
        "scraper_key": "playwright_microsoft",
        "reliability": "playwright",
        "detect": lambda h, p: "careers.microsoft.com" in h or "jobs.careers.microsoft.com" in h,
        "canonical": lambda h, p: "https://jobs.careers.microsoft.com/global/en/search",
    },
    {
        "name": "Revolut",
        "scraper_key": "playwright_revolut",
        "reliability": "playwright",
        "detect": lambda h, p: "revolut.com" in h and "career" in p,
        "canonical": lambda h, p: "https://www.revolut.com/careers/search",
    },
    {
        "name": "Airbnb",
        "scraper_key": "playwright_airbnb",
        "reliability": "playwright",
        "detect": lambda h, p: "careers.airbnb.com" in h or ("airbnb.com" in h and "career" in p),
        "canonical": lambda h, p: "https://careers.airbnb.com/positions/",
    },
    {
        "name": "iCIMS",
        "scraper_key": None,           # not supported
        "reliability": "unsupported",
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
        host = u.netloc.lower()
        path = u.path.strip("/").lower()
        return host, path
    except Exception:
        return raw.lower(), ""


def detect_platform(url: str):
    """Return the matching platform rule dict, or None."""
    host, path = parse_url(url)
    for rule in PLATFORM_RULES:
        try:
            if rule["detect"](host, path):
                return rule
        except Exception:
            continue
    return None


def canonical_url(rule: dict, original_url: str) -> str:
    """Return the best URL to use in the sheet for this platform."""
    if rule is None:
        return original_url
    host, path = parse_url(original_url)
    try:
        c = rule["canonical"](host, path)
        return c if c else original_url
    except Exception:
        return original_url


# ---------- GOOGLE SHEETS AUTH ----------
def get_sheet_client():
    """Return an authenticated gspread client via service account."""
    if not GOOGLE_CREDENTIALS:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS env var not set. "
            "See README for how to create a service account."
        )
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


# ---------- SHEET HELPERS ----------
def get_col_index(headers: list, name: str) -> int:
    """Return 1-based column index for a header name (case-insensitive). -1 if missing."""
    name_lower = name.lower().strip()
    for i, h in enumerate(headers):
        if h.lower().strip() == name_lower:
            return i + 1  # gspread is 1-indexed
    return -1


def ensure_column(worksheet, headers: list, col_name: str) -> int:
    """Add column if it doesn't exist. Returns 1-based index."""
    idx = get_col_index(headers, col_name)
    if idx == -1:
        # Append a new column header
        new_col = len(headers) + 1
        worksheet.update_cell(1, new_col, col_name)
        headers.append(col_name)
        idx = new_col
        print(f"  Created new column '{col_name}' at position {idx}")
    return idx


# ---------- MAIN ----------
def main():
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID env var not set.")

    print("Connecting to Google Sheets...")
    client = get_sheet_client()
    sheet = client.open_by_key(SHEET_ID)
    ws = sheet.get_worksheet(0)          # first tab
    all_values = ws.get_all_values()

    if not all_values:
        print("Sheet is empty.")
        return

    headers = all_values[0]
    rows = all_values[1:]

    # Find or create required columns
    url_col = get_col_index(headers, COL_CAREERS_URL)
    if url_col == -1:
        print(f"ERROR: Could not find '{COL_CAREERS_URL}' column in sheet.")
        return

    platform_col = ensure_column(ws, headers, "Platform")
    scraper_col = ensure_column(ws, headers, COL_SCRAPER_TYPE)

    print(f"Found {len(rows)} data rows. Detecting platforms...\n")

    updates = []   # list of (row_idx, platform_col, scraper_col, platform_name, scraper_key)

    for i, row in enumerate(rows):
        row_num = i + 2   # 1-indexed, +1 for header

        # Pad row if shorter than expected
        while len(row) < max(url_col, platform_col, scraper_col):
            row.append("")

        url = row[url_col - 1].strip()
        existing_platform = row[platform_col - 1].strip() if len(row) >= platform_col else ""
        existing_scraper = row[scraper_col - 1].strip() if len(row) >= scraper_col else ""

        if not url:
            continue

        rule = detect_platform(url)
        platform_name = rule["name"] if rule else "Unknown"
        scraper_key = rule["scraper_key"] if rule else ""
        # For unsupported platforms, set scraper to empty so agent skips gracefully
        if rule and rule["reliability"] == "unsupported":
            scraper_key = ""

        # Only write if something changed
        changed = (existing_platform != platform_name) or (existing_scraper != (scraper_key or ""))
        status = "✓ unchanged" if not changed else "→ updating"
        print(f"  Row {row_num:3d}  {row[0]:<20}  {platform_name:<25}  scraper={scraper_key or '(none)'}  {status}")

        if changed:
            updates.append((row_num, platform_name, scraper_key or ""))

    if not updates:
        print("\nAll platforms already up-to-date. Nothing to write.")
        return

    print(f"\nWriting {len(updates)} update(s) to sheet...")
    for row_num, platform_name, scraper_key in updates:
        ws.update_cell(row_num, platform_col, platform_name)
        ws.update_cell(row_num, scraper_col, scraper_key)

    print(f"Done. {len(updates)} row(s) updated.")
    print("\nYour sheet now has:")
    print("  'Platform'     — human-readable platform name (for your reference)")
    print("  'Scraper Type' — value job_agent.py reads to pick the right scraper")


if __name__ == "__main__":
    main()
