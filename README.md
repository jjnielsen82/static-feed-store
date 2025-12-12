# Elite Agent Data

Automated data pipeline for Elite Agent statistics.

## How It Works

1. **Local scraper** runs daily on your machine, outputs CSV files
2. **Sync script** pushes new records to this GitHub repo (checking for duplicates by MLS#)
3. **GitHub Actions** automatically processes the data and generates JSON files
4. **Website** fetches the pre-computed JSON from GitHub (via jsDelivr CDN)

## Data Files

### Input (data/)
- `phoenix_closed.csv` - Raw Phoenix transaction data
- `tucson_closed.csv` - Raw Tucson transaction data

### Output (output/)
- `phoenix_agents.json` - Processed Phoenix agent statistics
- `phoenix_companies.json` - Processed Phoenix company statistics
- `tucson_agents.json` - Processed Tucson agent statistics
- `tucson_companies.json` - Processed Tucson company statistics

## JSON URLs (for website)

Use jsDelivr CDN for fast, cached delivery:

```
https://cdn.jsdelivr.net/gh/YOUR_USERNAME/elite-agent-data@main/output/phoenix_agents.json
https://cdn.jsdelivr.net/gh/YOUR_USERNAME/elite-agent-data@main/output/tucson_agents.json
```

Or use raw GitHub (slower, but always fresh):

```
https://raw.githubusercontent.com/YOUR_USERNAME/elite-agent-data/main/output/phoenix_agents.json
```

## Local Setup

### 1. Clone this repo

```bash
git clone https://github.com/YOUR_USERNAME/elite-agent-data.git
```

### 2. Configure sync script

Edit `sync_to_github.py` on your local machine:

```python
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = "YOUR_USERNAME/elite-agent-data"  # Change this
```

### 3. Set GitHub Token

Create a Personal Access Token:
1. Go to GitHub → Settings → Developer Settings → Personal Access Tokens
2. Generate new token (classic) with `repo` scope
3. Save the token

Set as environment variable:
```bash
export GITHUB_TOKEN="your_token_here"
```

Or create a `.env` file:
```
GITHUB_TOKEN=your_token_here
```

### 4. Run Daily

After your scraper finishes, run:
```bash
python3 sync_to_github.py
```

This will:
- Find the latest CSV files
- Download existing data from GitHub
- Append only new records
- Push updates to GitHub
- Trigger GitHub Actions to process the data

## Manual Trigger

You can manually trigger data processing:
1. Go to Actions tab in this repo
2. Select "Process Elite Agent Data"
3. Click "Run workflow"
