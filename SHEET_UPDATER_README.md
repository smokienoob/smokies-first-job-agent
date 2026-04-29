# Sheet Updater — Setup Guide

`sheet_updater.py` reads your Google Sheet, detects each company's hiring
platform from the Careers URL, and writes two columns back:

- **Platform** — human-readable name (e.g. "Greenhouse", "Workday")
- **Scraper Type** — the key `job_agent.py` uses to pick the right scraper

Run it once when you add new companies. `job_agent.py` then reads
`Scraper Type` automatically — no more manual guessing.

---

## One-time setup: Google Service Account (10 min)

`sheet_updater.py` needs *write* access to your sheet. The published CSV URL
only gives read access, so we use a Google Service Account for writing.

### Step 1 — Create a Service Account

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a new project (or use an existing one) — top-left dropdown → New Project
3. Enable the **Google Sheets API**:
   - Left sidebar → APIs & Services → Library
   - Search "Google Sheets API" → Enable
4. Enable the **Google Drive API** the same way
5. Left sidebar → APIs & Services → Credentials → Create Credentials → Service Account
6. Give it any name (e.g. `sheet-writer`) → Create → Done
7. Click the service account you just created → Keys tab → Add Key → JSON
8. A `.json` file downloads — **this is your `GOOGLE_CREDENTIALS`**

### Step 2 — Share your sheet with the service account

1. Open the JSON file, find the `"client_email"` field
   (looks like `sheet-writer@your-project.iam.gserviceaccount.com`)
2. Open your Google Sheet → Share → paste that email → Editor → Send

### Step 3 — Add secrets to GitHub

Repo → Settings → Secrets → Actions → New repository secret:

| Secret name | Value |
|---|---|
| `SHEET_ID` | The long ID in your sheet URL: `docs.google.com/spreadsheets/d/**THIS_PART**/edit` |
| `GOOGLE_CREDENTIALS` | The entire contents of the downloaded JSON file (paste as-is) |

`SHEET_CSV_URL`, `TG_BOT_TOKEN`, `TG_CHAT_ID` — you already have these.

### Step 4 — Add the workflow file

Copy `updater_workflow.yml` to `.github/workflows/` in your repo
(same folder as `agent.yml`).

---

## Running it

### Manually (recommended — run when you add companies)

GitHub repo → Actions tab → **Update Sheet Platforms** → Run workflow

### What happens

The script prints a log like:

```
Row  2  Google               Google Careers            scraper=playwright_google  → updating
Row  3  Microsoft            Microsoft Careers         scraper=playwright_microsoft  → updating
Row  4  Notion               Ashby                     scraper=ashby  ✓ unchanged
Row  5  GSK                  Workday                   scraper=workday  ✓ unchanged
Row 10  Apple                Unknown                   scraper=(none)  → updating
Row 17  AMD                  iCIMS                     scraper=(none)  → updating
```

After it runs, open your sheet — you'll see two new columns filled in.
For "Unknown" platforms (Apple, custom sites), the Platform column shows
`Unknown` and Scraper Type is blank — set those rows to `Active = FALSE`
until you add a custom scraper.

---

## How job_agent.py uses this

`job_agent.py` already reads the `Scraper Type` column from your sheet.
The dispatcher in `fetch_jobs()` matches it:

```python
if stype == "greenhouse":   → scrape_greenhouse()
if stype == "workday":      → scrape_workday()
if stype == "ashby":        → scrape_ashby()
# etc.
```

So the loop is:
1. Add a company row to the sheet with just Company + Careers URL
2. Run **Update Sheet Platforms** → Scraper Type gets filled in automatically
3. Next scheduled `job_agent.py` run picks it up and scrapes correctly

No code changes needed for any company on a supported platform.

---

## Supported platforms

| Platform | Scraper Type value | Speed |
|---|---|---|
| Greenhouse | `greenhouse` | Fast (API) |
| Lever | `lever` | Fast (API) |
| Ashby | `ashby` | Fast (API) |
| Workday | `workday` | Fast (API) |
| SmartRecruiters | `smartrecruiters` | Fast (API) |
| BambooHR | `bamboohr` | Fast (API) |
| Eightfold | `eightfold` | Fast (API) |
| SuccessFactors | `successfactors` | Medium (HTML) |
| Taleo | `taleo` | Medium (HTML) |
| Google Careers | `playwright_google` | Slow (Playwright) |
| Microsoft Careers | `playwright_microsoft` | Slow (Playwright) |
| Revolut | `playwright_revolut` | Slow (Playwright) |
| Airbnb | `playwright_airbnb` | Slow (Playwright) |
| iCIMS | *(blank — unsupported)* | — |
