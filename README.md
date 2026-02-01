# PistonHeads Fleet Scraper

Scrapes PistonHeads members fleet table and car detail pages, storing current cars into Supabase.

## Setup (local)
1. Create a Python venv
2. `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in Supabase vars
4. Run:
   - Initial scrape: `python -m scraper.main --mode initial`
   - Nightly: `python -m scraper.main --mode nightly`

## Render
### Option A (recommended): Render Cron Job
- Build command: `pip install -r requirements.txt`
- Start command:
  - nightly: `python -m scraper.main --mode nightly`
- Schedule: `0 0 * * *` (midnight GMT)
- Add env vars:
  - SUPABASE_URL
  - SUPABASE_SERVICE_ROLE_KEY
  - USER_AGENT (optional)
  - TIMEZONE=Europe/London (optional; default is London)

### Initial scrape
Create a one-off Render Job / manual deploy that runs:
`python -m scraper.main --mode initial`

