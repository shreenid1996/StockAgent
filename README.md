# StockAgent

A local Python application that tracks a configurable watchlist of tech stocks, fetches the last 24 hours of company news daily, filters and scores articles for relevance, deduplicates and clusters related stories, and delivers a daily investor-style digest to your email via Gmail.

**Runs entirely locally — no cloud services, no web UI, no trading features.**

---

## Features

- Tracks 10 configurable tech stocks (default: AAPL, MSFT, NVDA, GOOGL, AMZN, META, TSLA, AMD, AVGO, ORCL)
- Fetches last-24h company news from [Finnhub](https://finnhub.io)
- Rule-based relevance and importance scoring (earnings, mergers, regulation, product, executive, legal, macro)
- Deduplication and clustering of related stories
- Extractive summarization — no hallucinated content
- HTML + plain-text email digest via Gmail API (OAuth2)
- Full CLI with dry-run, force, and fetch/compose/send modes
- SQLite persistence — all data stored locally
- Rotating log files

---

## Requirements

- Python 3.11+
- A [Finnhub](https://finnhub.io) account (free tier works)
- A Google account with Gmail API enabled

---

## Setup

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd stock_news_agent
pip install -r requirements.txt
```

### 2. Configure secrets

Copy the example env file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```env
FINNHUB_API_KEY=your_finnhub_api_key_here
GMAIL_SENDER=your_gmail_address@gmail.com
GMAIL_RECIPIENT=recipient_email@example.com
GMAIL_CREDENTIALS_PATH=credentials.json
GMAIL_TOKEN_PATH=token.json
```

### 3. Configure the watchlist and settings

Edit `config.yaml` to customise your watchlist, scoring thresholds, and scheduler time:

```yaml
watchlist:
  - AAPL
  - MSFT
  - NVDA
  # add or remove tickers here

scheduler:
  run_time: "07:00"   # 24h local time
```

### 4. Initialise the database

```bash
python main.py init-db
```

---

## Finnhub API Setup

1. Go to [https://finnhub.io](https://finnhub.io) and create a free account.
2. From your dashboard, copy your **API Key**.
3. Paste it into `.env` as `FINNHUB_API_KEY`.

The free tier provides access to the `/company-news` endpoint used by StockAgent.

---

## Gmail API Setup (OAuth2)

StockAgent uses Gmail's OAuth2 flow to send emails on your behalf. You only need to do this once.

### Step 1 — Enable the Gmail API

1. Go to [https://console.cloud.google.com](https://console.cloud.google.com).
2. Create a new project (or select an existing one).
3. Navigate to **APIs & Services → Library**.
4. Search for **Gmail API** and click **Enable**.

### Step 2 — Create OAuth2 credentials

1. Go to **APIs & Services → Credentials**.
2. Click **Create Credentials → OAuth client ID**.
3. Choose **Desktop app** as the application type.
4. Download the credentials JSON file.
5. Save it as `credentials.json` in the `stock_news_agent/` directory (or the path set in `GMAIL_CREDENTIALS_PATH`).

### Step 3 — Configure the OAuth consent screen

1. Go to **APIs & Services → OAuth consent screen**.
2. Set the app to **External** (or Internal if using Google Workspace).
3. Add your Gmail address as a **Test user**.
4. Add the scope: `https://www.googleapis.com/auth/gmail.send`.

### Step 4 — Authenticate (first run only)

Run any command that sends email (e.g. `run` or `send-last`). A browser window will open asking you to authorise the app. After authorising, a `token.json` file is saved locally and reused for all future runs.

```bash
python main.py run --dry-run   # test without sending
python main.py run             # full run including send
```

> **Note:** Keep `credentials.json` and `token.json` private. Add them to `.gitignore`.

---

## CLI Usage

All commands are run from the `stock_news_agent/` directory:

```bash
# Run the full pipeline (fetch → score → cluster → summarize → compose → send)
python main.py run

# Full pipeline but skip the email send (useful for testing)
python main.py run --dry-run

# Full pipeline, send even if a digest was already sent today
python main.py run --force

# Fetch and store articles only (no scoring, clustering, or sending)
python main.py fetch-only

# Compose a digest from already-stored articles (no fetch, no send)
python main.py compose-only

# Send the most recently composed digest
python main.py send-last

# Send the most recently composed digest even if already sent today
python main.py send-last --force

# Initialise (or re-initialise) the SQLite database schema
python main.py init-db
```

---

## Scheduler

### Option A — Built-in Python scheduler (simplest)

Run the scheduler as a background process. It checks every 60 seconds and triggers the pipeline at the time set in `config.yaml → scheduler.run_time`.

```bash
python -m app.scheduler
```

Keep this running in a terminal, or use a process manager like `nohup`, `screen`, or `pm2`.

### Option B — Windows Task Scheduler

1. Open **Task Scheduler** → **Create Basic Task**.
2. Set the trigger to **Daily** at your preferred time.
3. Set the action to:
   - **Program:** `C:\path\to\python.exe`
   - **Arguments:** `main.py run`
   - **Start in:** `C:\path\to\stock_news_agent\`

### Option C — Linux/macOS cron

Add a cron job to run the pipeline daily at 7:00 AM:

```bash
crontab -e
```

Add:

```cron
0 7 * * * cd /path/to/stock_news_agent && python main.py run >> app/logs/cron.log 2>&1
```

---

## Project Structure

```
stock_news_agent/
├── main.py                  # CLI entry point
├── cli.py                   # argparse command definitions
├── config.yaml              # Non-secret settings (watchlist, thresholds, etc.)
├── .env                     # Secret credentials (not committed)
├── .env.example             # Template for .env
├── requirements.txt         # Pinned Python dependencies
├── pytest.ini               # Test configuration
├── app/
│   ├── settings.py          # Config + secrets loader
│   ├── logger.py            # Rotating file logger
│   ├── db.py                # SQLite connection and schema
│   ├── models.py            # Dataclasses for all domain objects
│   ├── utils.py             # Pure utility functions
│   ├── fetch_news.py        # Finnhub API client
│   ├── normalize.py         # Article normalisation and storage
│   ├── filter_score.py      # Rule-based relevance scoring
│   ├── dedupe_cluster.py    # Article deduplication and clustering
│   ├── summarize.py         # Extractive summarization
│   ├── compose_digest.py    # Jinja2 digest rendering
│   ├── send_email.py        # Gmail API email sender
│   ├── pipeline.py          # Full pipeline orchestration
│   ├── scheduler.py         # Daily schedule runner
│   ├── templates/
│   │   ├── digest.html      # HTML email template
│   │   └── digest.txt       # Plain-text email template
│   ├── data/                # Raw API responses + SQLite DB
│   └── logs/                # Rotating log files
└── tests/                   # pytest + hypothesis test suite
```

---

## Running Tests

```bash
python -m pytest
```

The test suite uses `pytest` and `hypothesis` for property-based testing. All tests run without live API credentials.

---

## Database

StockAgent uses a local SQLite database at `app/data/stock_news.db` (configurable in `config.yaml`).

Tables:

| Table | Description |
|---|---|
| `watchlist` | Active ticker symbols |
| `articles` | Normalised news articles |
| `article_scores` | Relevance and importance scores |
| `event_clusters` | Deduplicated article clusters |
| `digest_runs` | Digest history with rendered content |
| `run_logs` | Per-step pipeline execution logs |

---

## Disclaimer

StockAgent is for informational purposes only. It does not provide financial advice. Always consult a qualified financial advisor before making investment decisions.
