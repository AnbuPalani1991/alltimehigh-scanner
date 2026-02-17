"""
ATH Scanner — All NSE + BSE Stocks
Fetches all listed symbols from NSE/BSE, checks each for all-time high,
saves results to JSON. Designed to run daily at 3:31 PM IST.
"""

import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ─────────────────────────────────────────────────────────────
OUTPUT_FILE   = Path("data/ath_results.json")
SYMBOLS_FILE  = Path("data/all_symbols.json")
LOG_FILE      = Path("data/scanner.log")
MAX_WORKERS   = 8       # parallel fetch threads
BATCH_DELAY   = 0.05    # seconds between batches to avoid rate limiting
ATH_THRESHOLD = 0.995   # 99.5% of historical high counts as ATH
YEARS_HISTORY = 5       # years of history to compare against

IST = timezone(timedelta(hours=5, minutes=30))

Path("data").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ── FETCH ALL SYMBOLS ──────────────────────────────────────────────────
def fetch_nse_symbols() -> list[dict]:
    """Fetch all NSE equity symbols from NSE India API."""
    symbols = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com"
        }
        session = requests.Session()
        # Establish session cookie
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(1)

        # Fetch all equity symbols
        url = "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        # Use the full market watch endpoint instead
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        resp = session.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        # Parse CSV
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        for _, row in df.iterrows():
            sym = str(row.get('SYMBOL', '')).strip()
            name = str(row.get('NAME OF COMPANY', '')).strip()
            if sym and sym != 'SYMBOL':
                symbols.append({
                    'symbol': f"{sym}.NS",
                    'name': name,
                    'exchange': 'NSE',
                    'series': str(row.get('SERIES', '')).strip()
                })
        log.info(f"NSE: fetched {len(symbols)} symbols")
    except Exception as e:
        log.error(f"NSE symbol fetch error: {e}")
    return symbols


def fetch_bse_symbols() -> list[dict]:
    """Fetch all BSE equity symbols."""
    symbols = []
    try:
        # BSE equity listing CSV
        url = "https://www.bseindia.com/corporates/List_Scrips.aspx"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        # Use BSE API
        url = "https://api.bseindia.com/BseIndiaAPI/api/getScripData/w?strCat=-1&strPrevClose=&strSector=&strIndex=0&strstart=0&strEnd=&strstock="
        resp = requests.get(url, headers=headers, timeout=30)
        data = resp.json()
        for item in data.get('Table', []):
            scrip = str(item.get('short_name', '')).strip()
            name  = str(item.get('LONGNAME', '')).strip()
            code  = str(item.get('SCRIP_CD', '')).strip()
            if scrip:
                symbols.append({
                    'symbol': f"{scrip}.BO",
                    'name': name,
                    'exchange': 'BSE',
                    'code': code
                })
        log.info(f"BSE: fetched {len(symbols)} symbols")
    except Exception as e:
        log.error(f"BSE symbol fetch error: {e}")
    return symbols


def load_or_fetch_symbols(force_refresh=False) -> list[dict]:
    """Load symbols from cache or fetch fresh."""
    if SYMBOLS_FILE.exists() and not force_refresh:
        age = time.time() - SYMBOLS_FILE.stat().st_mtime
        if age < 86400 * 7:  # refresh weekly
            with open(SYMBOLS_FILE) as f:
                symbols = json.load(f)
            log.info(f"Loaded {len(symbols)} symbols from cache")
            return symbols

    log.info("Fetching fresh symbol lists from NSE + BSE...")
    nse = fetch_nse_symbols()
    bse = fetch_bse_symbols()

    # Deduplicate — prefer NSE for cross-listed stocks
    all_syms = nse + bse
    log.info(f"Total symbols: {len(all_syms)} (NSE: {len(nse)}, BSE: {len(bse)})")

    with open(SYMBOLS_FILE, 'w') as f:
        json.dump(all_syms, f, indent=2)

    return all_syms


# ── FETCH INDIVIDUAL STOCK ─────────────────────────────────────────────
def check_ath(stock: dict) -> dict | None:
    """
    Fetch 5y price history for a stock and check if today's price is at ATH.
    Returns stock dict if ATH, else None.
    """
    symbol = stock['symbol']
    try:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{requests.utils.quote(symbol)}"
            f"?interval=1d&range={YEARS_HISTORY}y"
        )
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code == 404:
            return None  # Symbol not on Yahoo Finance
        resp.raise_for_status()

        data = resp.json()
        result = data.get('chart', {}).get('result', [])
        if not result:
            return None

        meta   = result[0].get('meta', {})
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])

        current = meta.get('regularMarketPrice') or meta.get('previousClose')
        if not current:
            return None

        valid_closes = [c for c in closes if c and c > 0]
        if len(valid_closes) < 30:  # Need at least 30 days of history
            return None

        historical_high = max(valid_closes)

        if current >= historical_high * ATH_THRESHOLD:
            return {
                'symbol':   symbol,
                'name':     stock.get('name') or meta.get('longName') or meta.get('shortName') or symbol,
                'price':    round(current, 2),
                'ath':      round(historical_high, 2),
                'exchange': stock.get('exchange', 'NSE' if symbol.endswith('.NS') else 'BSE'),
                'series':   stock.get('series', ''),
            }
        return None

    except requests.exceptions.Timeout:
        log.debug(f"Timeout: {symbol}")
        return None
    except Exception as e:
        log.debug(f"Error {symbol}: {e}")
        return None


# ── MAIN SCAN ──────────────────────────────────────────────────────────
def run_scan(symbols: list[dict]) -> list[dict]:
    """Scan all symbols in parallel, return list of ATH stocks."""
    log.info(f"Starting ATH scan for {len(symbols)} symbols...")
    start = time.time()
    ath_stocks = []
    done = 0
    total = len(symbols)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_ath, s): s for s in symbols}
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                ath_stocks.append(result)
                log.info(f"★ ATH: {result['symbol']} — {result['name']}")

            if done % 100 == 0:
                elapsed = time.time() - start
                rate = done / elapsed
                eta = (total - done) / rate
                log.info(f"Progress: {done}/{total} ({done*100//total}%) | "
                         f"ATH found: {len(ath_stocks)} | ETA: {eta:.0f}s")
            time.sleep(BATCH_DELAY / MAX_WORKERS)

    elapsed = time.time() - start
    log.info(f"Scan complete in {elapsed:.0f}s | ATH stocks: {len(ath_stocks)}")
    return ath_stocks


# ── SAVE RESULTS ───────────────────────────────────────────────────────
def save_results(ath_stocks: list[dict], total_scanned: int):
    """Save scan results to JSON file."""
    now_ist = datetime.now(IST)
    payload = {
        "scan_date":     now_ist.strftime("%d %b %Y"),
        "scan_time":     now_ist.strftime("%I:%M %p IST"),
        "scan_datetime": now_ist.isoformat(),
        "total_scanned": total_scanned,
        "ath_count":     len(ath_stocks),
        "stocks":        sorted(ath_stocks, key=lambda x: x['exchange'] + x['symbol']),
    }
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(payload, f, indent=2)
    log.info(f"Results saved → {OUTPUT_FILE}")
    return payload


# ── ENTRY POINT ────────────────────────────────────────────────────────
def main(force_symbols=False):
    log.info("=" * 60)
    log.info("ATH SCANNER STARTED")
    log.info("=" * 60)

    symbols = load_or_fetch_symbols(force_refresh=force_symbols)
    if not symbols:
        log.error("No symbols loaded. Exiting.")
        return

    # Filter: only include equity series (EQ, BE, SM, ST for NSE)
    equity_series = {'EQ', 'BE', 'BZ', 'SM', 'ST', 'N', 'W', 'M', ''}
    filtered = [
        s for s in symbols
        if s.get('series', 'EQ') in equity_series or s.get('exchange') == 'BSE'
    ]
    log.info(f"Filtered to {len(filtered)} equity symbols")

    ath_stocks = run_scan(filtered)
    save_results(ath_stocks, len(filtered))

    log.info(f"DONE — {len(ath_stocks)} stocks at all-time high out of {len(filtered)} scanned")


if __name__ == "__main__":
    import sys
    force = '--refresh-symbols' in sys.argv
    main(force_symbols=force)
