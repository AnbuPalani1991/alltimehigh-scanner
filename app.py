"""
ATH Scanner — Web Server
Serves the dashboard and provides API endpoints for scan results and status.
"""

import json
import subprocess
import threading
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from flask import Flask, jsonify, render_template, send_from_directory
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
log = logging.getLogger(__name__)

DATA_FILE    = Path("data/ath_results.json")
SYMBOLS_FILE = Path("data/all_symbols.json")
LOG_FILE     = Path("data/scanner.log")
IST          = timezone(timedelta(hours=5, minutes=30))

scan_status = {
    "running": False,
    "progress": 0,
    "total": 0,
    "found": 0,
    "message": "Idle",
    "started_at": None,
}


# ── ROUTES ─────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/results")
def api_results():
    """Return the latest scan results."""
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return jsonify(json.load(f))
    return jsonify({
        "scan_date": None,
        "scan_time": None,
        "total_scanned": 0,
        "ath_count": 0,
        "stocks": []
    })


@app.route("/api/status")
def api_status():
    """Return current scan status."""
    return jsonify(scan_status)


@app.route("/api/scan", methods=["POST"])
def api_trigger_scan():
    """Manually trigger a scan."""
    if scan_status["running"]:
        return jsonify({"error": "Scan already running"}), 409
    thread = threading.Thread(target=run_scan_background)
    thread.daemon = True
    thread.start()
    return jsonify({"message": "Scan started"})


@app.route("/api/log")
def api_log():
    """Return last 100 lines of scan log."""
    if LOG_FILE.exists():
        with open(LOG_FILE) as f:
            lines = f.readlines()
        return jsonify({"lines": lines[-100:]})
    return jsonify({"lines": []})


@app.route("/api/symbols/count")
def api_symbols_count():
    """Return how many symbols are cached."""
    if SYMBOLS_FILE.exists():
        with open(SYMBOLS_FILE) as f:
            data = json.load(f)
        return jsonify({"count": len(data), "cached": True})
    return jsonify({"count": 0, "cached": False})


# ── BACKGROUND SCAN ────────────────────────────────────────────────────
def run_scan_background():
    """Run scanner.py as subprocess and stream progress."""
    global scan_status
    scan_status.update({
        "running": True,
        "progress": 0,
        "found": 0,
        "message": "Starting scan...",
        "started_at": datetime.now(IST).isoformat(),
    })

    try:
        import subprocess, sys, re
        proc = subprocess.Popen(
            [sys.executable, "scanner.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue

            # Parse progress
            m = re.search(r'Progress: (\d+)/(\d+).*ATH found: (\d+)', line)
            if m:
                done, total, found = int(m.group(1)), int(m.group(2)), int(m.group(3))
                scan_status.update({
                    "progress": done,
                    "total": total,
                    "found": found,
                    "message": f"Scanning... {done}/{total} stocks",
                })

            # ATH found
            elif '★ ATH:' in line:
                scan_status["found"] = scan_status.get("found", 0) + 1
                scan_status["message"] = line.split('★ ATH:')[-1].strip()

            # Completion
            elif 'DONE —' in line:
                scan_status["message"] = line

        proc.wait()
        scan_status.update({
            "running": False,
            "progress": scan_status.get("total", 0),
            "message": f"Scan complete! Found {scan_status['found']} ATH stocks.",
        })
        log.info("Background scan finished")

    except Exception as e:
        log.error(f"Background scan error: {e}")
        scan_status.update({"running": False, "message": f"Error: {e}"})


# ── SCHEDULER ──────────────────────────────────────────────────────────
def schedule_daily_scan():
    """Schedule scan to run at 3:31 PM IST every weekday."""
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_scan_background,
        trigger="cron",
        day_of_week="mon-fri",
        hour=15,
        minute=31,
        id="daily_ath_scan",
    )
    scheduler.start()
    log.info("Scheduler started — scan will run Mon-Fri at 3:31 PM IST")
    return scheduler


# ── MAIN ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    Path("data").mkdir(exist_ok=True)
    scheduler = schedule_daily_scan()
    log.info("Starting ATH Scanner web server on port 5000...")
    app.run(host="0.0.0.0", port=5000, debug=False)
