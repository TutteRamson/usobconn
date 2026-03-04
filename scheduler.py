"""Daily scheduler — runs one JSON retrieval per day at a random time.

The scheduler picks a random hour/minute for today (or tomorrow if the
chosen time has already passed) and sleeps until then.  After the
retrieval finishes it schedules the next one for a random time tomorrow.

It re-uses the same progress-queue mechanism as the manual retrieval so
the UI can show live progress if someone opens it while the batch is
running.
"""

import logging
import random
import threading
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)


def _random_time_today_or_tomorrow():
    """Return a datetime for a random HH:MM today, or tomorrow if already past."""
    now = datetime.now(timezone.utc)
    hour = random.randint(6, 22)  # between 06:00 and 22:59 UTC
    minute = random.randint(0, 59)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def start_scheduler(app, start_scrape_fn):
    """Launch the scheduler daemon thread.

    Args:
        app: Flask application instance (for logging/context).
        start_scrape_fn: callable() that kicks off a retrieval and returns
                         the session_id.  Expected to handle its own
                         app context and progress queue setup.
    """

    def _loop():
        while True:
            next_run = _random_time_today_or_tomorrow()
            now = datetime.now(timezone.utc)
            wait_seconds = (next_run - now).total_seconds()

            log.info(
                "Next scheduled retrieval at %s UTC (in %.0f minutes)",
                next_run.strftime("%Y-%m-%d %H:%M"),
                wait_seconds / 60,
            )

            # Store next-run time so the UI can display it
            with app.app_context():
                app.config["NEXT_SCHEDULED_SCRAPE"] = next_run.isoformat()

            time.sleep(max(wait_seconds, 0))

            log.info("Scheduled retrieval starting now.")
            try:
                start_scrape_fn()
            except Exception:
                log.exception("Scheduled retrieval failed")

    t = threading.Thread(target=_loop, daemon=True, name="retrieval-scheduler")
    t.start()
    log.info("Retrieval scheduler started.")
