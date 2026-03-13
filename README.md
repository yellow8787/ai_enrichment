# Merchant Enrichment Pipeline

Automated pipeline to enrich merchant/seller data with **domain discovery**, **Instagram handles**, **IG follower counts**, and **Store Leads GMV (estimated revenue)** — then filter for high-performing sellers.

## What It Does

Given a CSV of seller names, the pipeline fills in missing data through multiple methods:

| Data | Method | Speed |
|------|--------|-------|
| Domain | Serper Google Search + Gemini AI validation | ~2s/row |
| IG Handle | Website scraping + Serper `site:instagram.com` | ~2s/row |
| IG Followers | Serper snippet parsing, screenshot+OCR fallback | ~2s or ~8s/row |
| GMV (Revenue) | Store Leads Chrome Extension screenshot+OCR | ~10s/row |

## Pipeline Flow

```
Step 1: --discover       Find domain + IG handle (API, background)
Step 2: (manual)         Fill gaps via Google AI Studio
Step 3: --phase1         Store Leads GMV (screenshot, overnight)
Step 4: --phase3a        IG followers via Serper (API, background)
Step 5: --phase3b        IG followers screenshot fallback (overnight)
Step 6: --phase5         Filter -> high_performing_sellers.csv
```

## Requirements

### Python Packages

```bash
pip install -r requirements.txt
```

### External Tools

- **Tesseract OCR** (for screenshot text recognition)
  - Download: https://github.com/UB-Mannheim/tesseract/wiki
  - Install to: `C:\Program Files\Tesseract-OCR\`

- **Store Leads Chrome Extension** (for GMV data)
  - Install from Chrome Web Store
  - Free account required

### API Keys

| Service | Purpose | Free Tier | Sign Up |
|---------|---------|-----------|---------|
| Serper.dev | Google Search API | 2,500 queries | https://serper.dev |
| Google Gemini | Domain validation AI | Free (gemini-2.0-flash) | https://aistudio.google.com |

Set your API keys in `pipeline.py` CONFIG section, or via environment variables:

```bash
set SERPER_API_KEY=your_key_here
set GEMINI_API_KEY=your_key_here
```

## Input CSV Format

```csv
seller_name,domain,instagram_handle,ig_followers,storeleads_gmv_usd,register_country_code,tiktok_account_name
Fresh 'n Rebel,freshnrebel.com,@freshnrebel,,,NL,freshnrebel
SomeStore,,,,,,
```

- `seller_name` — required
- All other columns can be empty; the pipeline fills them in
- `domain`, `instagram_handle` — filled by `--discover`
- `ig_followers` — filled by `--phase3a` / `--phase3b`
- `storeleads_gmv_usd` — filled by `--phase1`

## Usage

### First Time Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Install Tesseract OCR
3. Set API keys in `pipeline.py`
4. Calibrate screen coordinates:

```bash
python pipeline.py --find-position
```

This captures 5 screen positions for Store Leads extension and IG profile areas. Fill the coordinates into the CONFIG section.

### Running the Pipeline

**Step 1: Discover domain + IG handle (pure API, can use PC normally)**

```bash
python pipeline.py --discover --input data.csv
```

**Step 2: GMV via Store Leads (screenshot, don't touch mouse)**

```bash
python pipeline.py --phase1 --input data.csv
```

**Step 3: IG followers via Serper (pure API)**

```bash
python pipeline.py --phase3a --input data.csv
```

**Step 4: IG followers screenshot fallback (don't touch mouse)**

```bash
python pipeline.py --phase3b --input data.csv
```

**Step 5: Filter high performers**

```bash
python pipeline.py --phase5 --input data.csv
```

Outputs `high_performing_sellers.csv` (GMV >= $400K/year OR IG >= 10K followers).

### Testing with Limited Rows

Add `--limit N` to any command to test with only N rows:

```bash
python pipeline.py --discover --input data.csv --limit 5
```

### Debug Commands

```bash
python pipeline.py --debug-gmv --input data.csv      # Test Store Leads OCR on 1 domain
python pipeline.py --debug-ig --input data.csv        # Test IG OCR on 1 handle
python pipeline.py --debug-serper --input data.csv    # Test Serper API response
```

## All Commands

| Command | What it does | Needs mouse? | ~Time |
|---------|-------------|:------------:|-------|
| `--discover` | Find domain + IG handle | No | ~2s/row |
| `--phase1` | Store Leads GMV | **Yes** | ~10s/row |
| `--phase2` | Find IG handles only | No | ~2s/row |
| `--phase3a` | IG followers (Serper) | No | ~2s/row |
| `--phase3b` | IG followers (screenshot) | **Yes** | ~8s/row |
| `--phase4` | Find domains only | No | ~2s/row |
| `--phase5` | Filter results | No | instant |
| `--find-position` | Calibrate coordinates | **Yes** | 1 min |
| `--debug-gmv` | Test GMV on 1 row | **Yes** | 10s |
| `--debug-ig` | Test IG on 1 row | **Yes** | 8s |
| `--debug-serper` | Test Serper API | No | 2s |

## Configuration

Edit the CONFIG section in `pipeline.py`:

```python
# Screen coordinates (run --find-position to get these)
EXTENSION_X = 2584      # Store Leads icon X
EXTENSION_Y = 128       # Store Leads icon Y
SL_LEFT     = 951       # Panel screenshot area
SL_TOP      = 170
SL_RIGHT    = 2500
SL_BOTTOM   = 973
CLOSE_X     = 1450      # Error popup Close button
CLOSE_Y     = 970
IG_LEFT     = 1085      # IG followers area
IG_TOP      = 455
IG_RIGHT    = 1648
IG_BOTTOM   = 509

# Timing (adjust if pages load slowly)
PAGE_WAIT = 6           # Website load wait (seconds)
EXT_WAIT  = 3           # Store Leads panel load wait
IG_WAIT   = 4           # Instagram page load wait
AUTOSAVE  = 20          # Auto-save every N rows

# Filter thresholds
FILTER_GMV = 400_000    # Annual GMV in USD
FILTER_IG  = 10_000     # IG follower count
```

## Features

- **Auto-save**: Saves progress every 20 rows — safe to interrupt
- **Skip completed**: Re-running any phase skips already-filled rows
- **Error handling**: Store Leads rate limit popup auto-detected and dismissed
- **Emergency stop**: Move mouse to top-left corner (0,0) during screenshot phases
- **Multi-encoding**: Handles CSV files with special characters (UTF-8, Latin-1, CP1252)

## License

MIT
