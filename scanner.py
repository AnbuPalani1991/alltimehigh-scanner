"""
ATH Scanner — Uses NSE India's official API directly.
NSE provides free end-of-day data including 52-week highs.
We use this to identify stocks at or near their all-time highs.
"""

import json
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

OUTPUT_FILE = Path("data/ath_results.json")
LOG_FILE    = Path("data/scanner.log")
IST         = timezone(timedelta(hours=5, minutes=30))

Path("data").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive",
}

# All NSE indices to scan
NSE_INDICES = [
    "NIFTY 50",
    "NIFTY NEXT 50",
    "NIFTY 100",
    "NIFTY 200",
    "NIFTY 500",
    "NIFTY MIDCAP 50",
    "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 50",
    "NIFTY SMALLCAP 100",
    "NIFTY SMALLCAP 250",
    "NIFTY MICROCAP250",
    "NIFTY TOTAL MARKET",
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY ENERGY",
    "NIFTY FINANCIAL SERVICES",
    "NIFTY FMCG",
    "NIFTY IT",
    "NIFTY MEDIA",
    "NIFTY METAL",
    "NIFTY PHARMA",
    "NIFTY PSU BANK",
    "NIFTY REALTY",
    "NIFTY INDIA CONSUMPTION",
    "NIFTY CPSE",
    "NIFTY INFRASTRUCTURE",
    "NIFTY MNC",
    "NIFTY SERVICES SECTOR",
    "NIFTY SME EMERGE",
]


def nse_session() -> requests.Session:
    """Create an NSE session with proper cookies."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        # Must hit the main page first to get cookies
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(1)
        session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15)
        time.sleep(0.5)
    except Exception as e:
        log.warning(f"NSE session setup: {e}")
    return session


def fetch_index_stocks(session: requests.Session, index_name: str) -> list[dict]:
    """Fetch all stocks in an NSE index with their 52-week high data."""
    try:
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={requests.utils.quote(index_name)}"
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            log.warning(f"Index {index_name}: HTTP {resp.status_code}")
            return []

        data = resp.json()
        stocks = data.get('data', [])
        log.info(f"Index '{index_name}': {len(stocks)} stocks")
        return stocks
    except Exception as e:
        log.warning(f"Index {index_name} error: {e}")
        return []


def is_at_52w_high(stock: dict) -> bool:
    """
    Check if stock is at or near its 52-week high.
    NSE provides: lastPrice, yearHigh (52-week high)
    If lastPrice >= yearHigh * 0.98, it's at/near 52-week high.
    """
    try:
        last  = float(stock.get('lastPrice', 0))
        high  = float(stock.get('yearHigh', 0))
        if last <= 0 or high <= 0:
            return False
        return last >= high * 0.98
    except:
        return False


def run_scan() -> list[dict]:
    """
    Scan all NSE indices for stocks at 52-week highs.
    52-week high is the best proxy for ATH available from free APIs.
    """
    log.info("Starting NSE scan...")
    session   = nse_session()
    seen      = set()
    ath_stocks = []
    all_stocks = []

    # Fetch all stocks across all indices
    for index_name in NSE_INDICES:
        stocks = fetch_index_stocks(session, index_name)
        for s in stocks:
            sym = s.get('symbol', '')
            if sym and sym not in seen and sym != '-':
                seen.add(sym)
                all_stocks.append(s)
        time.sleep(0.3)  # Be polite to NSE API

    log.info(f"Total unique stocks fetched: {len(all_stocks)}")

    # Also fetch all securities list
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        resp = session.get(url, timeout=20)
        if resp.status_code == 200:
            for s in resp.json().get('data', []):
                sym = s.get('symbol', '')
                if sym and sym not in seen:
                    seen.add(sym)
                    all_stocks.append(s)
    except Exception as e:
        log.warning(f"F&O list error: {e}")

    log.info(f"Total stocks to check: {len(all_stocks)}")

    # Check each for 52-week high
    for s in all_stocks:
        if is_at_52w_high(s):
            sym  = s.get('symbol', '')
            name = s.get('meta', {}).get('companyName', '') or sym
            last = float(s.get('lastPrice', 0))
            high = float(s.get('yearHigh', 0))
            chg  = float(s.get('pChange', 0))

            ath_stocks.append({
                'symbol':   f"{sym}.NS",
                'name':     name,
                'price':    round(last, 2),
                'ath':      round(high, 2),
                'change':   round(chg, 2),
                'exchange': 'NSE',
                'series':   'EQ',
            })
            log.info(f"★ 52W HIGH: {sym} — {name} @ ₹{last}")

    log.info(f"Stocks at 52-week high: {len(ath_stocks)}")
    return ath_stocks, len(all_stocks)


def save_results(ath_stocks: list, total_scanned: int):
    now = datetime.now(IST)
    payload = {
        "scan_date":     now.strftime("%d %b %Y"),
        "scan_time":     now.strftime("%I:%M %p IST"),
        "scan_datetime": now.isoformat(),
        "total_scanned": total_scanned,
        "ath_count":     len(ath_stocks),
        "label":         "52-Week High",
        "stocks":        sorted(ath_stocks, key=lambda x: x['symbol']),
    }
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(payload, f, indent=2)
    log.info(f"Saved {len(ath_stocks)} results → {OUTPUT_FILE}")
    return payload


def main():
    log.info("=" * 60)
    log.info("ATH SCANNER STARTED (NSE Official API)")
    log.info("=" * 60)
    try:
        ath_stocks, total = run_scan()
        save_results(ath_stocks, total)
        log.info(f"DONE — {len(ath_stocks)} stocks at 52-week high from {total} scanned")
    except Exception as e:
        log.error(f"Scan failed: {e}")
        raise


if __name__ == "__main__":
    main()
