"""
ATH Scanner — All NSE + BSE Stocks
Uses yfinance library which handles Yahoo Finance authentication properly.
"""

import json
import time
import logging
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

OUTPUT_FILE  = Path("data/ath_results.json")
SYMBOLS_FILE = Path("data/all_symbols.json")
LOG_FILE     = Path("data/scanner.log")
MAX_WORKERS  = 4
ATH_THRESHOLD = 0.98  # within 2% of ATH counts
IST = timezone(timedelta(hours=5, minutes=30))

Path("data").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger(__name__)


# ── FETCH SYMBOLS ──────────────────────────────────────────────────────
def fetch_nse_symbols() -> list[dict]:
    symbols = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        for _, row in df.iterrows():
            sym    = str(row.get('SYMBOL', '')).strip()
            name   = str(row.get('NAME OF COMPANY', '')).strip()
            series = str(row.get('SERIES', '')).strip()
            if sym and sym != 'SYMBOL' and series in ('EQ', 'BE', 'BZ', 'SM', 'ST'):
                symbols.append({'symbol': f"{sym}.NS", 'name': name, 'exchange': 'NSE', 'series': series})
        log.info(f"NSE: {len(symbols)} symbols")
    except Exception as e:
        log.error(f"NSE fetch error: {e}")
        # Fallback: Nifty 500 hardcoded list
        symbols = get_fallback_nse()
    return symbols


def fetch_bse_symbols() -> list[dict]:
    symbols = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        url = "https://api.bseindia.com/BseIndiaAPI/api/getScripData/w?strCat=-1&strPrevClose=&strSector=&strIndex=0&strstart=0&strEnd=&strstock="
        resp = requests.get(url, headers=headers, timeout=30)
        data = resp.json()
        for item in data.get('Table', []):
            scrip = str(item.get('short_name', '')).strip()
            name  = str(item.get('LONGNAME', '')).strip()
            if scrip:
                symbols.append({'symbol': f"{scrip}.BO", 'name': name, 'exchange': 'BSE', 'series': 'EQ'})
        log.info(f"BSE: {len(symbols)} symbols")
    except Exception as e:
        log.error(f"BSE fetch error: {e}")
    return symbols


def get_fallback_nse() -> list[dict]:
    """Fallback: comprehensive NSE stock list if API fails."""
    stocks = [
        ("RELIANCE","Reliance Industries"),("TCS","Tata Consultancy Services"),
        ("HDFCBANK","HDFC Bank"),("INFY","Infosys"),("ICICIBANK","ICICI Bank"),
        ("HINDUNILVR","Hindustan Unilever"),("ITC","ITC"),("SBIN","State Bank of India"),
        ("BHARTIARTL","Bharti Airtel"),("KOTAKBANK","Kotak Mahindra Bank"),
        ("WIPRO","Wipro"),("ASIANPAINT","Asian Paints"),("AXISBANK","Axis Bank"),
        ("MARUTI","Maruti Suzuki"),("TATAMOTORS","Tata Motors"),
        ("HCLTECH","HCL Technologies"),("SUNPHARMA","Sun Pharma"),
        ("ULTRACEMCO","UltraTech Cement"),("TITAN","Titan"),("BAJFINANCE","Bajaj Finance"),
        ("BAJAJFINSV","Bajaj Finserv"),("NTPC","NTPC"),("POWERGRID","Power Grid"),
        ("ONGC","ONGC"),("COALINDIA","Coal India"),("JSWSTEEL","JSW Steel"),
        ("TATASTEEL","Tata Steel"),("ADANIENT","Adani Enterprises"),
        ("ADANIPORTS","Adani Ports"),("LT","L&T"),("M&M","Mahindra & Mahindra"),
        ("TECHM","Tech Mahindra"),("DIVISLAB","Divi's Labs"),("DRREDDY","Dr Reddy's"),
        ("CIPLA","Cipla"),("APOLLOHOSP","Apollo Hospitals"),("GRASIM","Grasim"),
        ("BPCL","BPCL"),("INDUSINDBK","IndusInd Bank"),("BRITANNIA","Britannia"),
        ("NESTLEIND","Nestle India"),("EICHERMOT","Eicher Motors"),
        ("HEROMOTOCO","Hero MotoCorp"),("BAJAJ-AUTO","Bajaj Auto"),
        ("TATACONSUM","Tata Consumer"),("HINDALCO","Hindalco"),("VEDL","Vedanta"),
        ("PIDILITIND","Pidilite"),("HAVELLS","Havells"),("TITAN","Titan"),
        ("VOLTAS","Voltas"),("PAGEIND","Page Industries"),("MUTHOOTFIN","Muthoot Finance"),
        ("CHOLAFIN","Cholamandalam"),("AUROPHARMA","Aurobindo Pharma"),
        ("TORNTPHARM","Torrent Pharma"),("LUPIN","Lupin"),("BIOCON","Biocon"),
        ("SIEMENS","Siemens India"),("ABB","ABB India"),("PERSISTENT","Persistent Systems"),
        ("COFORGE","Coforge"),("LTIM","LTIMindtree"),("MPHASIS","Mphasis"),
        ("NAUKRI","Info Edge"),("ZOMATO","Zomato"),("DMART","DMart"),
        ("TRENT","Trent"),("DIXON","Dixon Technologies"),("ASTRAL","Astral"),
        ("SBILIFE","SBI Life"),("HDFCLIFE","HDFC Life"),("LICI","LIC"),
        ("IRCTC","IRCTC"),("HAL","HAL"),("BEL","BEL"),("BHEL","BHEL"),
        ("SAIL","SAIL"),("NMDC","NMDC"),("DEEPAKNTR","Deepak Nitrite"),
        ("AARTIIND","Aarti Industries"),("BALKRISIND","Balkrishna Ind"),
        ("TVSMOTOR","TVS Motor"),("MOTHERSON","Samvardhana Motherson"),
        ("CUMMINSIND","Cummins India"),("KAJARIACER","Kajaria Ceramics"),
        ("SUPREMEIND","Supreme Industries"),("WHIRLPOOL","Whirlpool India"),
        ("BATAINDIA","Bata India"),("METROBRAND","Metro Brands"),
        ("POLYCAB","Polycab India"),("ANGELONE","Angel One"),
        ("HFCL","HFCL"),("RAILTEL","RailTel"),("IRFC","IRFC"),
        ("RVNL","RVNL"),("NBCC","NBCC"),("RECLTD","REC"),("PFC","PFC"),
        ("ADANIPOWER","Adani Power"),("ADANIGREEN","Adani Green"),
        ("ADANITRANS","Adani Transmission"),("ADANIWILMAR","Adani Wilmar"),
        ("GODREJCP","Godrej Consumer"),("GODREJPROP","Godrej Properties"),
        ("MCDOWELL-N","United Spirits"),("UBL","United Breweries"),
        ("COLPAL","Colgate Palmolive"),("MARICO","Marico"),
        ("DABUR","Dabur India"),("EMAMILTD","Emami"),
        ("BERGEPAINT","Berger Paints"),("KANSAINER","Kansai Nerolac"),
        ("INDIGO","IndiGo (InterGlobe)"),("SPICEJET","SpiceJet"),
        ("CONCOR","Container Corp"),("GMRINFRA","GMR Infra"),
        ("MINDTREE","Mindtree"),("HEXAWARE","Hexaware"),
        ("RBLBANK","RBL Bank"),("FEDERALBNK","Federal Bank"),
        ("BANDHANBNK","Bandhan Bank"),("IDFCFIRSTB","IDFC First Bank"),
        ("YESBANK","Yes Bank"),("PNB","Punjab National Bank"),
        ("BANKBARODA","Bank of Baroda"),("CANBK","Canara Bank"),
        ("UNIONBANK","Union Bank"),("IOB","Indian Overseas Bank"),
        ("MAHABANK","Bank of Maharashtra"),("CENTRALBK","Central Bank"),
        ("IDBI","IDBI Bank"),("LICHSGFIN","LIC Housing Finance"),
        ("M&MFIN","M&M Financial"),("MANAPPURAM","Manappuram Finance"),
        ("SHRIRAMFIN","Shriram Finance"),("SUNDARMFIN","Sundaram Finance"),
        ("LALPATHLAB","Dr Lal PathLabs"),("METROPOLIS","Metropolis Healthcare"),
        ("FORTIS","Fortis Healthcare"),("MAXHEALTH","Max Healthcare"),
        ("NH","Narayana Hrudayalaya"),("AARTIDRUGS","Aarti Drugs"),
        ("ALKEM","Alkem Labs"),("IPCALAB","IPCA Labs"),("NATCOPHARM","Natco Pharma"),
        ("LAURUSLABS","Laurus Labs"),("GRANULES","Granules India"),
        ("SUDARSCHEM","Sudarshan Chemical"),("VINATIORGA","Vinati Organics"),
        ("NAVINFLUOR","Navin Fluorine"),("SRF","SRF"),("FLUOROCHEM","Gujarat Fluorochem"),
        ("CLEAN","Clean Science"),("FINEORG","Fine Organic"),
        ("TATACHEM","Tata Chemicals"),("GHCL","GHCL"),
        ("LINDEINDIA","Linde India"),("GUJGASLTD","Gujarat Gas"),
        ("IGL","Indraprastha Gas"),("MGL","Mahanagar Gas"),
        ("PETRONET","Petronet LNG"),("GAIL","GAIL"),("IOC","Indian Oil"),
        ("HPCL","HPCL"),("MRPL","MRPL"),("CPCL","CPCL"),
        ("TATAPOWER","Tata Power"),("TORNTPOWER","Torrent Power"),
        ("CESC","CESC"),("JPPOWER","Jaiprakash Power"),
        ("INOXWIND","Inox Wind"),("SUZLON","Suzlon Energy"),
        ("SWSOLAR","Sterling Wilson Solar"),("KPIGREEN","KPI Green"),
        ("PREMIER","Premier Energies"),("WAAREEENER","Waaree Energies"),
        ("ZYDUSLIFE","Zydus Lifesciences"),("ABBOTINDIA","Abbott India"),
        ("PFIZER","Pfizer India"),("SANOFI","Sanofi India"),
        ("GLAXO","GSK Pharma"),("JBCHEPHARM","JB Chemicals"),
        ("ERIS","Eris Lifesciences"),("AJANTPHARM","Ajanta Pharma"),
        ("MANKIND","Mankind Pharma"),("JYOTHYLAB","Jyothy Labs"),
        ("PIDILITIND","Pidilite"),("ITDCEM","ITD Cementation"),
        ("NCC","NCC"),("KNR","KNR Constructions"),("AHLUCONT","Ahlu Container"),
        ("PNCINFRA","PNC Infratech"),("GPPL","Gujarat Pipavav Port"),
        ("ESABINDIA","ESAB India"),("GRINDWELL","Grindwell Norton"),
        ("TIMKEN","Timken India"),("SKF","SKF India"),
        ("SCHAEFFLER","Schaeffler India"),("BHARATFORG","Bharat Forge"),
        ("SUPRAJIT","Suprajit Engineering"),("SUNDRMFAST","Sundram Fasteners"),
        ("AMARAJABAT","Amara Raja"),("EXIDEIND","Exide Industries"),
        ("BOSCHLTD","Bosch India"),("MINDAIND","Minda Industries"),
        ("MOTHERSON","Samvardhana Motherson"),("TIINDIA","Tube Investments"),
        ("CRAFTSMAN","Craftsman Auto"),("ENDURANCE","Endurance Tech"),
    ]
    return [{'symbol': f"{s}.NS", 'name': n, 'exchange': 'NSE', 'series': 'EQ'} for s, n in stocks]


def load_or_fetch_symbols(force_refresh=False) -> list[dict]:
    if SYMBOLS_FILE.exists() and not force_refresh:
        age = time.time() - SYMBOLS_FILE.stat().st_mtime
        if age < 86400 * 7:
            with open(SYMBOLS_FILE) as f:
                syms = json.load(f)
            log.info(f"Loaded {len(syms)} symbols from cache")
            return syms

    log.info("Fetching symbol lists...")
    nse = fetch_nse_symbols()
    bse = fetch_bse_symbols()
    all_syms = nse + bse
    log.info(f"Total: {len(all_syms)} symbols")
    with open(SYMBOLS_FILE, 'w') as f:
        json.dump(all_syms, f, indent=2)
    return all_syms


# ── CHECK ATH USING YFINANCE ───────────────────────────────────────────
def check_ath(stock: dict) -> dict | None:
    """
    Uses yfinance to fetch full price history.
    Stock is at ATH if latest close >= all historical closes * ATH_THRESHOLD.
    """
    symbol = stock['symbol']
    try:
        ticker = yf.Ticker(symbol)
        # Fetch max available history
        hist = ticker.history(period="max", auto_adjust=True)

        if hist.empty or len(hist) < 20:
            return None

        closes = hist['Close'].dropna().values
        if len(closes) == 0:
            return None

        all_time_high = float(closes.max())
        latest_close  = float(closes[-1])

        if latest_close >= all_time_high * ATH_THRESHOLD:
            return {
                'symbol':   symbol,
                'name':     stock.get('name', symbol),
                'price':    round(latest_close, 2),
                'ath':      round(all_time_high, 2),
                'exchange': stock.get('exchange', 'NSE'),
                'series':   stock.get('series', 'EQ'),
            }
        return None

    except Exception as e:
        log.debug(f"Skip {symbol}: {e}")
        return None


# ── MAIN SCAN ──────────────────────────────────────────────────────────
def run_scan(symbols: list[dict]) -> list[dict]:
    log.info(f"Scanning {len(symbols)} symbols with yfinance...")
    start      = time.time()
    ath_stocks = []
    done       = 0
    total      = len(symbols)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_ath, s): s for s in symbols}
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                ath_stocks.append(result)
                log.info(f"★ ATH: {result['symbol']} — {result['name']} @ ₹{result['price']}")

            if done % 50 == 0:
                elapsed = time.time() - start
                rate    = done / elapsed if elapsed > 0 else 1
                eta     = (total - done) / rate
                log.info(f"Progress: {done}/{total} ({done*100//total}%) | ATH: {len(ath_stocks)} | ETA: {eta:.0f}s")

    log.info(f"Done in {time.time()-start:.0f}s | ATH stocks: {len(ath_stocks)}")
    return ath_stocks


def save_results(ath_stocks, total_scanned):
    now = datetime.now(IST)
    payload = {
        "scan_date":     now.strftime("%d %b %Y"),
        "scan_time":     now.strftime("%I:%M %p IST"),
        "scan_datetime": now.isoformat(),
        "total_scanned": total_scanned,
        "ath_count":     len(ath_stocks),
        "stocks":        sorted(ath_stocks, key=lambda x: x['exchange'] + x['symbol']),
    }
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(payload, f, indent=2)
    log.info(f"Saved → {OUTPUT_FILE}")


def main(force_symbols=False):
    log.info("=" * 60)
    log.info("ATH SCANNER STARTED")
    log.info("=" * 60)
    symbols = load_or_fetch_symbols(force_refresh=force_symbols)
    if not symbols:
        log.error("No symbols. Exiting.")
        return
    ath_stocks = run_scan(symbols)
    save_results(ath_stocks, len(symbols))
    log.info(f"DONE — {len(ath_stocks)} ATH stocks from {len(symbols)} scanned")


if __name__ == "__main__":
    import sys
    main(force_symbols='--refresh-symbols' in sys.argv)
