"""
ATH Scanner — All NSE + BSE Stocks
Fetches all listed symbols from NSE/BSE, checks each for all-time high,
saves results to JSON. Designed to run daily at 3:31 PM IST.

ATH Logic:
- Fetches max available history (10y) for each stock
- Stock is at ATH if today's close >= highest close ever recorded
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
MAX_WORKERS   = 6
BATCH_DELAY   = 0.1
ATH_THRESHOLD = 0.98   # within 2% of all-time high counts
HISTORY_RANGE = "10y"  # max history

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


# ── FETCH ALL NSE SYMBOLS ──────────────────────────────────────────────
def fetch_nse_symbols() -> list[dict]:
    symbols = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
        }
        # NSE official equity list CSV
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        for _, row in df.iterrows():
            sym    = str(row.get('SYMBOL', '')).strip()
            name   = str(row.get('NAME OF COMPANY', '')).strip()
            series = str(row.get('SERIES', '')).strip()
            if sym and sym != 'SYMBOL':
                symbols.append({
                    'symbol':   f"{sym}.NS",
                    'name':     name,
                    'exchange': 'NSE',
                    'series':   series,
                })
        log.info(f"NSE: {len(symbols)} symbols fetched")
    except Exception as e:
        log.error(f"NSE fetch error: {e}")
    return symbols


def fetch_bse_symbols() -> list[dict]:
    symbols = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        # BSE equity list CSV
        url = "https://www.bseindia.com/corporates/List_Scrips.aspx"
        # Use BSE direct CSV download
        url = "https://api.bseindia.com/BseIndiaAPI/api/getScripData/w?strCat=-1&strPrevClose=&strSector=&strIndex=0&strstart=0&strEnd=&strstock="
        resp = requests.get(url, headers=headers, timeout=30)
        data = resp.json()
        for item in data.get('Table', []):
            scrip = str(item.get('short_name', '')).strip()
            name  = str(item.get('LONGNAME', '')).strip()
            if scrip:
                symbols.append({
                    'symbol':   f"{scrip}.BO",
                    'name':     name,
                    'exchange': 'BSE',
                    'series':   'EQ',
                })
        log.info(f"BSE: {len(symbols)} symbols fetched")
    except Exception as e:
        log.error(f"BSE fetch error: {e}")
    return symbols


def load_or_fetch_symbols(force_refresh=False) -> list[dict]:
    if SYMBOLS_FILE.exists() and not force_refresh:
        age = time.time() - SYMBOLS_FILE.stat().st_mtime
        if age < 86400 * 7:
            with open(SYMBOLS_FILE) as f:
                syms = json.load(f)
            log.info(f"Loaded {len(syms)} symbols from cache")
            return syms

    log.info("Fetching symbol lists from NSE + BSE...")
    nse = fetch_nse_symbols()
    bse = fetch_bse_symbols()
    all_syms = nse + bse
    log.info(f"Total: {len(all_syms)} symbols (NSE: {len(nse)}, BSE: {len(bse)})")

    with open(SYMBOLS_FILE, 'w') as f:
        json.dump(all_syms, f, indent=2)
    return all_syms


# ── CHECK ATH ──────────────────────────────────────────────────────────
def check_ath(stock: dict) -> dict | None:
    """
    Fetch max available price history and check if stock is at all-time high.
    
    Key fix: We fetch ALL historical closes, find the max, then check if
    the MOST RECENT close (last entry in the array) equals that max.
    This correctly identifies stocks where the latest trading day IS the ATH.
    """
    symbol = stock['symbol']
    try:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{requests.utils.quote(symbol)}"
            f"?interval=1d&range={HISTORY_RANGE}"
        )
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = requests.get(url, headers=headers, timeout=20)

        if resp.status_code in (404, 422):
            return None
        resp.raise_for_status()

        data = resp.json()
        result = data.get('chart', {}).get('result', [])
        if not result:
            return None

        meta   = result[0].get('meta', {})
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])

        # Filter out None/zero values
        valid  = [c for c in closes if c and c > 0]
        if len(valid) < 20:
            return None

        # All-time high = max of ALL historical closes
        all_time_high = max(valid)

        # Today's price = last close in the array (most recent trading day)
        latest_close  = valid[-1]

        # Current market price from meta (may be intraday)
        current_price = meta.get('regularMarketPrice') or latest_close

        # Stock is at ATH if current price is within ATH_THRESHOLD of the all-time high
        is_ath = current_price >= all_time_high * ATH_THRESHOLD

        if is_ath:
            return {
                'symbol':   symbol,
                'name':     stock.get('name') or meta.get('longName') or meta.get('shortName') or symbol,
                'price':    round(current_price, 2),
                'ath':      round(all_time_high, 2),
                'exchange': stock.get('exchange', 'NSE' if '.NS' in symbol else 'BSE'),
                'series':   stock.get('series', ''),
            }
        return None

    except requests.exceptions.Timeout:
        log.debug(f"Timeout: {symbol}")
        return None
    except Exception as e:
        log.debug(f"Skip {symbol}: {e}")
        return None


# ── MAIN SCAN ──────────────────────────────────────────────────────────
def run_scan(symbols: list[dict]) -> list[dict]:
    log.info(f"Starting ATH scan for {len(symbols)} symbols...")
    start     = time.time()
    ath_stocks = []
    done      = 0
    total     = len(symbols)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_ath, s): s for s in symbols}
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                ath_stocks.append(result)
                log.info(f"★ ATH: {result['symbol']} — {result['name']} @ ₹{result['price']}")

            if done % 100 == 0:
                elapsed = time.time() - start
                rate    = done / elapsed if elapsed > 0 else 1
                eta     = (total - done) / rate
                log.info(
                    f"Progress: {done}/{total} ({done*100//total}%) | "
                    f"ATH found: {len(ath_stocks)} | ETA: {eta:.0f}s"
                )

    elapsed = time.time() - start
    log.info(f"Scan complete in {elapsed:.0f}s | ATH stocks: {len(ath_stocks)}")
    return ath_stocks


# ── SAVE RESULTS ───────────────────────────────────────────────────────
def save_results(ath_stocks: list[dict], total_scanned: int):
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

    # Only equity series
    equity = {'EQ', 'BE', 'BZ', 'SM', 'ST', 'N', 'W', 'M', ''}
    filtered = [
        s for s in symbols
        if s.get('series', 'EQ') in equity or s.get('exchange') == 'BSE'
    ]
    log.info(f"Scanning {len(filtered)} equity symbols")

    ath_stocks = run_scan(filtered)
    save_results(ath_stocks, len(filtered))
    log.info(f"DONE — {len(ath_stocks)} ATH stocks out of {len(filtered)} scanned")


if __name__ == "__main__":
    import sys
    force = '--refresh-symbols' in sys.argv
    main(force_symbols=force)
