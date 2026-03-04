"""Scraper module for Monarch connection status page using Playwright.

The page uses a CSS grid layout (not a <table>). Each institution row is a div
with class 'sc-gahYZc cGmkTm' containing 5 child cells:
  Cell 0: Institution name + logo
  Cell 1: Data provider (Plaid / Finicity / MX) + additional providers
  Cell 2: Connection success    (4 colored blocks)
  Cell 3: Connection longevity  (4 colored blocks)
  Cell 4: Average update time   (4 colored blocks)

Metric blocks use background-color to indicate level:
  rgb(43, 154, 102)   = dark green   (filled)
  rgb(141, 182, 84)   = light green  (filled)
  rgb(255, 197, 61)   = yellow       (filled)
  rgb(220, 62, 66)    = red          (filled)
  rgb(235, 232, 229)  = gray         (empty)

Score mapping (filled blocks out of 4):
  4/4 -> 100%   3/4 -> 75%   2/4 -> 50%   1/4 -> 25%   0/4 -> 0%
"""

import base64
import hashlib
import json
import os
import random
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright

from models import Connection, ScrapeSession, db

TARGET_URL = "https://www.monarch.com/connection-status"

# Gray is the only "empty" color; anything else counts as filled
GRAY_COLOR = "rgb(235, 232, 229)"

# 0 filled blocks = not rated (null), not 0%.
# On Monarch's scale, all-gray means "no data" for that metric.
SCORE_MAP = {0: None, 1: 25.0, 2: 50.0, 3: 75.0, 4: 100.0}

LEVEL_LABELS = {
    25.0: "Low",
    50.0: "Medium",
    75.0: "Good",
    100.0: "Excellent",
}


def scrape_connections(app, progress_callback=None, session_id=None):
    """
    Scrape the Monarch connection status page.

    Args:
        app: Flask app instance (for db context)
        progress_callback: callable(event_type, data_dict) for SSE updates
        session_id: existing ScrapeSession id to update (if None, creates new)

    Returns:
        scrape_session_id
    """

    def emit(event_type, data):
        if progress_callback:
            progress_callback(event_type, data)

    with app.app_context():
        if session_id:
            session = db.session.get(ScrapeSession, session_id)
            session.started_at = datetime.now(timezone.utc)
            session.status = "running"
            db.session.commit()
        else:
            session = ScrapeSession(
                started_at=datetime.now(timezone.utc), status="running"
            )
            db.session.add(session)
            db.session.commit()
            session_id = session.id

    emit("status", {"message": "Starting browser...", "session_id": session_id})

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/New_York",
            )

            # Remove automation indicators
            page = context.new_page()
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)

            emit(
                "status",
                {"message": "Navigating to Monarch connection status page..."},
            )
            # Use domcontentloaded instead of networkidle — the page has
            # long-running analytics/tracking requests that prevent networkidle
            # from ever firing within the timeout.
            page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=90000)
            # Extra settle time to let JS hydrate and render
            page.wait_for_timeout(random.randint(2000, 3000))

            # Dismiss cookie banner if present
            try:
                reject_btn = page.query_selector(
                    "button:has-text('Reject Non-Essential')"
                )
                if reject_btn:
                    reject_btn.click()
                    page.wait_for_timeout(1000)
            except Exception:
                pass

            emit(
                "status",
                {"message": "Page loaded. Scrolling to load all rows..."},
            )

            # Wait for data rows to appear — use the actual styled-component class
            page.wait_for_selector(".sc-gahYZc.cGmkTm", timeout=30000)
            page.wait_for_timeout(random.randint(800, 1200))

            # JavaScript to extract visible rows.
            # getComputedStyle only returns accurate colors for elements that
            # are on-screen (painted). Off-screen blocks return the default
            # gray, which would incorrectly score every metric as 0%.
            # We extract visible rows at each scroll position during the
            # single scroll pass that also triggers lazy loading.
            JS_EXTRACT_VISIBLE = """() => {
                const GRAY = 'rgb(235, 232, 229)';
                // 0 filled blocks = not rated (null), not 0%
                const SCORE_MAP = {0: null, 1: 25, 2: 50, 3: 75, 4: 100};
                const results = [];
                const vTop = window.scrollY;
                const vBot = vTop + window.innerHeight;

                const rows = document.querySelectorAll('.sc-gahYZc.cGmkTm');
                rows.forEach((row, idx) => {
                    const rect = row.getBoundingClientRect();
                    const absTop = rect.top + window.scrollY;
                    const absBot = rect.bottom + window.scrollY;
                    // Only process rows at least partially in the viewport
                    if (absBot < vTop || absTop > vBot) return;

                    try {
                        const cells = row.children;
                        if (cells.length < 5) return;

                        const nameEl = cells[0];
                        const name = nameEl.innerText.trim().split('\\n')[0].trim();
                        if (!name || name.length < 2) return;

                        // Extract logo data URI from the img in the name cell
                        const logoEl = nameEl.querySelector('img');
                        const logo = logoEl ? logoEl.src : null;

                        // Use textContent to capture ALL provider names,
                        // including those hidden behind "+N more" links.
                        // textContent returns text from hidden elements too.
                        const provCell = cells[1];
                        const fullProvText = provCell.textContent || '';

                        let allProviders = [];
                        const knownProviders = ['Plaid', 'Finicity', 'MX'];
                        for (const kp of knownProviders) {
                            if (fullProvText.includes(kp)) {
                                allProviders.push(kp);
                            }
                        }
                        if (allProviders.length === 0) allProviders.push('');

                        const provider = allProviders[0];

                        // Cross-check with "+N more" count and metric
                        // block count (4 blocks per provider).
                        const moreMatch = fullProvText.match(/\\+(\\d+)\\s*more/i);
                        // "+N more" means N beyond the first visible one
                        const expectedFromMore = moreMatch ? 1 + parseInt(moreMatch[1], 10) : allProviders.length;
                        const totalFromText = Math.max(allProviders.length, expectedFromMore);
                        let blockCount = 0;
                        for (let ci = 2; ci <= 4; ci++) {
                            const b = cells[ci] ? cells[ci].querySelectorAll('.sc-dYwGCk').length : 0;
                            if (b > blockCount) blockCount = b;
                        }
                        const fromBlocks = blockCount > 0 ? Math.floor(blockCount / 4) : 0;
                        const numProviders = Math.max(totalFromText, fromBlocks, 1);

                        // Pad allProviders with placeholders for the
                        // additional providers we know exist but can't name.
                        while (allProviders.length < numProviders) {
                            allProviders.push('Additional Provider');
                        }

                        // Read metric blocks grouped per provider.
                        // Each provider gets 4 blocks (0-4 filled) within a metric cell.
                        function readMetricGrouped(cell) {
                            const blocks = cell.querySelectorAll('.sc-dYwGCk');
                            if (blocks.length === 0) return Array(numProviders).fill(null);
                            const blocksPerProv = 4;
                            const scores = [];
                            for (let p = 0; p < numProviders; p++) {
                                const start = p * blocksPerProv;
                                const end = start + blocksPerProv;
                                if (start >= blocks.length) {
                                    scores.push(null);
                                    continue;
                                }
                                let filled = 0;
                                for (let i = start; i < Math.min(end, blocks.length); i++) {
                                    const bg = window.getComputedStyle(blocks[i]).backgroundColor;
                                    if (bg !== GRAY) filled++;
                                }
                                scores.push(SCORE_MAP[filled] !== undefined ? SCORE_MAP[filled] : null);
                            }
                            return scores;
                        }

                        const successScores = readMetricGrouped(cells[2]);
                        const longevityScores = readMetricGrouped(cells[3]);
                        const updateScores = readMetricGrouped(cells[4]);

                        // Primary provider gets the first set of metrics
                        const success_pct = successScores[0];
                        const longevity_pct = longevityScores[0];
                        const update_pct = updateScores[0];

                        // Build per-provider detail for additional providers
                        let providerDetails = [];
                        for (let p = 0; p < numProviders; p++) {
                            providerDetails.push({
                                name: allProviders[p],
                                success_pct: successScores[p] !== undefined ? successScores[p] : null,
                                longevity_pct: longevityScores[p] !== undefined ? longevityScores[p] : null,
                                update_pct: updateScores[p] !== undefined ? updateScores[p] : null
                            });
                        }

                        let status = 'OK';
                        const rowText = row.innerText;
                        if (rowText.includes('Unavailable')) status = 'Unavailable';
                        else if (rowText.includes('Issues reported')) status = 'Issues reported';

                        results.push({
                            idx: idx,
                            name: name,
                            provider: provider,
                            logo: logo,
                            additional_providers: allProviders.slice(1),
                            provider_details: providerDetails,
                            success_pct: success_pct,
                            longevity_pct: longevity_pct,
                            update_pct: update_pct,
                            status: status
                        });
                    } catch(e) {}
                });
                return results;
            }"""

            def _merge_batch(batch, all_extracted):
                """Merge a batch of extracted rows into the accumulated dict."""
                for inst in batch:
                    key = (inst["name"], inst["provider"])
                    existing = all_extracted.get(key)
                    if existing is None:
                        all_extracted[key] = inst
                    else:
                        # Update if this batch has better (non-null) metrics
                        for field in ("success_pct", "longevity_pct", "update_pct"):
                            new_val = inst.get(field)
                            old_val = existing.get(field)
                            if new_val is not None and (old_val is None or old_val == 0) and new_val > 0:
                                existing[field] = new_val
                        # Merge provider_details: prefer non-null metrics
                        new_details = inst.get("provider_details", [])
                        old_details = existing.get("provider_details", [])
                        if new_details and (not old_details or len(new_details) >= len(old_details)):
                            merged = []
                            for i, nd in enumerate(new_details):
                                od = old_details[i] if i < len(old_details) else {}
                                m = dict(nd)
                                for mf in ("success_pct", "longevity_pct", "update_pct"):
                                    if m.get(mf) is None and od.get(mf) is not None:
                                        m[mf] = od[mf]
                                merged.append(m)
                            existing["provider_details"] = merged
                        # Always update idx to reflect latest DOM position
                        if inst.get("idx") is not None:
                            existing["idx"] = inst["idx"]

            # Single combined scroll pass: triggers lazy loading AND extracts
            # visible row data at each position, so we only traverse once.
            prev_count = 0
            stall_streak = 0
            STALL_LIMIT = 15  # require 15 consecutive stalls to stop
            max_scroll_attempts = 500
            all_extracted = {}  # keyed by (name, provider) to deduplicate

            def _extract_visible():
                """Extract data from rows currently in the viewport."""
                return page.evaluate(JS_EXTRACT_VISIBLE)

            # Capture the initially visible rows BEFORE scrolling begins.
            # Without this, the first scroll moves past the top rows and
            # they may never be extracted if the cleanup sweep is disrupted.
            initial_batch = _extract_visible()
            _merge_batch(initial_batch, all_extracted)

            for scroll_attempt in range(max_scroll_attempts):
                # Scroll incrementally (viewport-height steps) instead of
                # jumping straight to the bottom, which looks more natural
                # and gives the lazy loader time to fetch batches.
                page.evaluate(
                    "window.scrollBy(0, window.innerHeight * (0.8 + Math.random() * 0.4))"
                )
                page.wait_for_timeout(random.randint(300, 600))

                current_count = page.evaluate(
                    "document.querySelectorAll('.sc-gahYZc.cGmkTm').length"
                )

                # Extract visible rows at this scroll position
                batch = _extract_visible()
                _merge_batch(batch, all_extracted)

                if current_count > prev_count:
                    stall_streak = 0
                    prev_count = current_count
                else:
                    stall_streak += 1
                    # Give the lazy loader a bit more time on stalls
                    page.wait_for_timeout(
                        random.randint(300, 600) * min(stall_streak, 3)
                    )

                    if stall_streak == 3:
                        # Jump to bottom to trigger pending lazy loads
                        page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        page.wait_for_timeout(random.randint(1000, 2000))
                        batch = _extract_visible()
                        _merge_batch(batch, all_extracted)
                    elif stall_streak == 6:
                        # Scroll up significantly and back down
                        page.evaluate(
                            "window.scrollBy(0, -window.innerHeight * 5)"
                        )
                        page.wait_for_timeout(random.randint(1000, 1500))
                        batch = _extract_visible()
                        _merge_batch(batch, all_extracted)
                        page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        page.wait_for_timeout(random.randint(1500, 2500))
                        batch = _extract_visible()
                        _merge_batch(batch, all_extracted)
                    elif stall_streak == 10:
                        # Scroll all the way to top, let page settle,
                        # then back to bottom
                        page.evaluate("window.scrollTo(0, 0)")
                        page.wait_for_timeout(random.randint(1500, 2500))
                        page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        page.wait_for_timeout(random.randint(2000, 3000))
                        batch = _extract_visible()
                        _merge_batch(batch, all_extracted)
                    elif stall_streak == 13:
                        # Final attempt: scroll back up and slowly descend
                        page.evaluate(
                            "window.scrollBy(0, -window.innerHeight * 10)"
                        )
                        page.wait_for_timeout(random.randint(1000, 2000))
                        for _ in range(5):
                            page.evaluate(
                                "window.scrollBy(0, window.innerHeight)"
                            )
                            page.wait_for_timeout(random.randint(500, 800))
                        page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        page.wait_for_timeout(random.randint(1500, 2500))
                        batch = _extract_visible()
                        _merge_batch(batch, all_extracted)

                    # After recovery, recheck row count and reset if new
                    # rows appeared
                    if stall_streak in (3, 6, 10, 13):
                        new_count = page.evaluate(
                            "document.querySelectorAll('.sc-gahYZc.cGmkTm').length"
                        )
                        if new_count > prev_count:
                            stall_streak = 0
                            prev_count = new_count

                if stall_streak >= STALL_LIMIT:
                    break

                # Emit progress every 3rd scroll attempt
                if current_count > 0 and scroll_attempt % 3 == 0:
                    emit(
                        "progress",
                        {
                            "message": f"Scrolling... {len(all_extracted)} / ~{current_count} institutions captured",
                            "phase": "scrolling",
                            "count": len(all_extracted),
                        },
                    )

            # Quick cleanup sweep: scroll top-to-bottom to catch any rows
            # that were still loading during the first pass.
            emit(
                "status",
                {
                    "message": f"Verifying {len(all_extracted)} institutions..."
                },
            )
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(random.randint(400, 600))

            for _ in range(600):
                page.wait_for_timeout(random.randint(80, 150))
                batch = _extract_visible()
                _merge_batch(batch, all_extracted)

                at_bottom = page.evaluate(
                    """() => {
                    window.scrollBy(0, window.innerHeight * 0.8);
                    return window.scrollY + window.innerHeight >= document.body.scrollHeight - 50;
                }"""
                )
                if at_bottom:
                    page.wait_for_timeout(random.randint(80, 150))
                    batch = _extract_visible()
                    _merge_batch(batch, all_extracted)
                    break

            # Sort by DOM index so rank reflects the actual page order.
            institutions_data = sorted(
                all_extracted.values(),
                key=lambda x: x.get("idx", 9999),
            )

            total = len(institutions_data)
            emit(
                "progress",
                {
                    "message": f"Extracted {total} institutions. Saving to database...",
                    "phase": "saving",
                    "current": 0,
                    "total": total,
                    "count": total,
                },
            )

            # Save to database
            with app.app_context():
                session = db.session.get(ScrapeSession, session_id)
                session.total_institutions = total

                for idx, inst in enumerate(institutions_data):
                    success = inst.get("success_pct")
                    longevity = inst.get("longevity_pct")
                    update = inst.get("update_pct")

                    # Build structured provider data for storage.
                    # provider_details includes per-provider metrics with labels.
                    provider_details = inst.get("provider_details", [])
                    for pd in provider_details:
                        for mf in ("success_pct", "longevity_pct", "update_pct"):
                            pval = pd.get(mf)
                            label_key = {"success_pct": "success_rate",
                                         "longevity_pct": "longevity",
                                         "update_pct": "update_frequency"}[mf]
                            pd[label_key] = LEVEL_LABELS.get(pval)

                    # Save logo to disk (instance/logos/<hash>.png)
                    logo_data = inst.get("logo")
                    if logo_data and logo_data.startswith("data:image"):
                        inst_name = inst.get("name", "Unknown")
                        logo_hash = hashlib.md5(inst_name.encode()).hexdigest()
                        logo_dir = os.path.join(app.instance_path, "logos")
                        os.makedirs(logo_dir, exist_ok=True)
                        logo_path = os.path.join(logo_dir, f"{logo_hash}.png")
                        try:
                            _, b64data = logo_data.split(",", 1)
                            with open(logo_path, "wb") as lf:
                                lf.write(base64.b64decode(b64data))
                        except Exception:
                            pass  # skip logo if decode fails

                    conn = Connection(
                        scrape_session_id=session_id,
                        rank=idx + 1,
                        institution_name=inst.get("name", "Unknown"),
                        data_provider=inst.get("provider", ""),
                        additional_providers=json.dumps(provider_details),
                        success_pct=success,
                        success_rate=LEVEL_LABELS.get(success),
                        longevity_pct=longevity,
                        longevity=LEVEL_LABELS.get(longevity),
                        update_pct=update,
                        update_frequency=LEVEL_LABELS.get(update),
                        connection_status=inst.get("status", "OK"),
                    )
                    db.session.add(conn)

                    if (idx + 1) % 25 == 0 or idx == total - 1:
                        emit(
                            "progress",
                            {
                                "message": f"Saving {idx + 1} / {total} institutions",
                                "phase": "saving",
                                "current": idx + 1,
                                "total": total,
                                "count": total,
                            },
                        )

                session.status = "completed"
                session.finished_at = datetime.now(timezone.utc)
                db.session.commit()

            browser.close()

        emit(
            "complete",
            {
                "message": f"Scraping complete! {total} institutions saved.",
                "session_id": session_id,
                "total": total,
            },
        )
        return session_id

    except Exception as e:
        with app.app_context():
            session = db.session.get(ScrapeSession, session_id)
            if session:
                session.status = "failed"
                session.error_message = str(e)
                session.finished_at = datetime.now(timezone.utc)
                db.session.commit()

        emit("error", {"message": f"Scraping failed: {str(e)}"})
        raise
