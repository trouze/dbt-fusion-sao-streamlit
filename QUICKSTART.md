# Quick Start Guide

## Installation (One-time setup)

```bash
cd /path/to/dbt-fusion-sao-streamlit
pip install -r requirements.txt
```

## Running the App

**Option 1: Use the start script (easiest)**
```bash
./start.sh
```

**Option 2: Run manually**
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m streamlit run app.py
```

## Initial Setup in the App

1. **Open the app** (it will launch in your browser at http://localhost:8501)

2. **Go to "⚙️ Configuration" tab**

3. **Enter your credentials:**
   ```
   dbt Cloud URL: https://cloud.getdbt.com (or your instance URL)
   API Key: [your dbt Cloud API token - supports REST & GraphQL]
   Account ID: [your account ID]
   Job ID: [job you want to analyze]
   Project ID: [optional]
   Environment ID: [optional - for Model Reuse & SLO Analysis]
   ```

4. **Click "💾 Save Configuration"**

5. **Start analyzing!**

## Your First Analysis

### Option A: Quick Health Check (30 seconds)

1. Go to **"📊 Summary Statistics"** tab
2. Click **"📊 Generate Summary"**
3. See your metrics vs. goals:
   - Freshness Configuration Coverage (Goal: 80%)
   - Model Reuse Rate (Goal: 30%)

### Option B: Detailed Analysis (2-3 minutes)

1. Go to **"📋 Freshness Details"** tab
2. Click **"🔍 Analyze Freshness"**
3. Browse detailed model-by-model breakdown
4. Filter and export as needed

### Option C: Run Trends (1-2 minutes)

1. Go to **"📈 Run Status Details"** tab
2. Select date range (default: last 7 days)
3. Set max runs to analyze (start with 10)
4. Click **"📊 Analyze Run Statuses"**
5. View interactive charts and tables

### Option D: Environment-Wide SLO Analysis (1-2 minutes) 🆕

1. Go to **"🔄 Model Reuse & SLO Analysis"** tab
2. Enter your Environment ID
3. Click **"🔍 Analyze Environment"**
4. View:
   - Total reuse percentage
   - SLO compliance status
   - Build after distribution charts
   - Automatic insights & recommendations

## Common Tasks

### Analyze a Different Job

1. Click **"🔄 Reconfigure"** in the sidebar
2. Change Job ID
3. Save configuration
4. Run your analysis

### Export Data

Each analysis tab has a **"📥 Download"** button to export results as CSV or JSON.

### Stop the App

Press `Ctrl+C` in the terminal where the app is running.

## Troubleshooting

**App won't start?**
- Make sure you installed dependencies: `pip install -r requirements.txt`
- Use `python3 -m streamlit` not just `streamlit`

**Can't connect to dbt Cloud?**
- Verify your API key is valid
- Check Account ID is correct
- Ensure API key has proper permissions

**Run analysis is slow?**
- Start with 5-10 runs
- Reduce date range to 7 days
- Each run requires ~3 API calls

## File Structure

```
dbt-fusion-sao-streamlit/
├── README.md                       # Full documentation
├── QUICKSTART.md                   # This file
├── CHANGELOG.md                    # Version history
├── MODEL_REUSE_GUIDE.md           # Guide for new SLO Analysis tab
├── requirements.txt                # Python dependencies
├── app.py                          # Main Streamlit app
├── log_freshness.py               # Core analysis logic
├── scripts/
│   ├── log_freshness_job.py       # CLI: job fetching helper
│   └── graphql_reuse.py           # CLI: standalone GraphQL script
├── start.sh                        # Launch script
└── .gitignore                     # Prevent committing secrets
```

## Next Steps

1. ✅ Install dependencies
2. ✅ Run the app
3. ✅ Configure credentials
4. ✅ Run your first analysis
5. 📖 Read the full [README.md](README.md) for advanced features

---

**Need help?** Check the full [README.md](README.md) for detailed documentation.



