# 📈 ATH Scanner — All Indian Stocks (NSE + BSE)

Scans **every listed Indian stock** (NSE + BSE, including SME, smallcap, midcap)
for all-time highs. Runs automatically at **3:31 PM IST** every trading day.

---

## 🏗️ How It Works

1. **scanner.py** — Downloads full NSE + BSE symbol lists, fetches 5 years of price history for each stock via Yahoo Finance, and identifies stocks at all-time highs
2. **app.py** — Flask web server that serves the dashboard and triggers scans
3. **Scheduler** — Automatically runs the scan at 3:31 PM IST every weekday
4. **Dashboard** — Beautiful web UI showing all ATH stocks with NSE/BSE filter and search

---

## 🚀 Deploy to Render (Free — Recommended)

Render is a free cloud platform. Your scanner will run 24/7 at no cost.

### Step 1 — Create GitHub Account
Go to **https://github.com** and sign up (free).

### Step 2 — Upload the project to GitHub
1. Go to **https://github.com/new** → create a repo called `ath-scanner`
2. Click **"uploading an existing file"**
3. Upload ALL files from this folder:
   - `scanner.py`
   - `app.py`
   - `requirements.txt`
   - `Procfile`
   - `render.yaml`
   - `templates/index.html`
4. Click **"Commit changes"**

### Step 3 — Deploy on Render
1. Go to **https://render.com** → Sign up with your GitHub account
2. Click **"New +"** → **"Web Service"**
3. Connect your `ath-scanner` GitHub repo
4. Render auto-detects settings from `render.yaml`
5. Click **"Create Web Service"**
6. Wait 2-3 minutes for deployment
7. Your dashboard is live at `https://ath-scanner-xxxx.onrender.com`

### Step 4 — Run Your First Scan
1. Open your Render URL in browser
2. Click **"⬡ Run Scan"**
3. It will scan all ~5000 NSE+BSE stocks (takes 15-30 minutes)
4. After that, it auto-runs every day at 3:31 PM IST

---

## 💻 Run Locally (Optional)

```bash
# Install Python 3.11+
# Then:
pip install -r requirements.txt
mkdir data
python app.py
# Open http://localhost:5000
```

To run just the scanner:
```bash
python scanner.py
# Force refresh symbol list:
python scanner.py --refresh-symbols
```

---

## 📁 File Structure

```
ath-scanner/
├── scanner.py          # Core scanning engine
├── app.py              # Flask web server + scheduler
├── requirements.txt    # Python dependencies
├── Procfile            # For Render deployment
├── render.yaml         # Render config
├── templates/
│   └── index.html      # Dashboard UI
└── data/               # Auto-created
    ├── all_symbols.json # Cached symbol list (refreshed weekly)
    ├── ath_results.json # Latest scan results
    └── scanner.log      # Agent activity log
```

---

## ⚙️ How ATH Is Detected

For each stock:
1. Fetches **5 years** of daily closing prices from Yahoo Finance
2. Finds the **highest closing price** in that period
3. If today's price is **≥ 99.5%** of that high → marked as **All-Time High**

The 99.5% threshold accounts for minor rounding differences between exchanges.

---

## 📊 Coverage

| Source | Stocks |
|--------|--------|
| NSE Equity (EQ series) | ~1,800 |
| NSE SME (SM/ST series) | ~600 |
| BSE Equity | ~3,500 |
| **Total** | **~5,500+** |

---

## ⏰ Schedule

- Auto-scan: **3:31 PM IST, Monday–Friday**
- Symbol list refresh: **Weekly**
- Dashboard refresh: **Every 30 seconds**

---

## 🆓 Cost

**Completely free** on Render's free tier.
- 750 free hours/month (enough for 24/7)
- No credit card required
