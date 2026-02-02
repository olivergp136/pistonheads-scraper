# scraper/main.py

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Optional, Dict, Any

import pytz

from .settings import get_settings
from .supabase_db import SupabaseDB
from .pistonheads import (
    PistonHeadsClient,
    london_now,
    extract_known_makes_from_fleet_html,
    parse_fleet_page,
    parse_car_details_page,
    make_model_year_fields,
)
from .parsing import detect_soft_error
from .known_makes import FALLBACK_MAKES  # fallback full makes list

STOP_BEFORE_LONDON = pytz.timezone("Europe/London").localize(datetime(2023, 1, 1, 0, 0, 0))


def iso(dt) -> Optional[str]:
    if dt is None:
        return None
    return dt.isoformat()


def _get_initial_resume_page(state: Dict[str, Any]) -> int:
    """
    Resume initial scrape from the next page after the last completed page.

    We only resume automatically if the last recorded mode was 'initial'.
    That prevents a nightly run from overwriting last_completed_page and breaking the resume logic.

    If the last run timed out mid-page, last_completed_page will still refer to the last *finished* page,
    so restarting will re-run the partially processed page (safe).
    """
    last_mode = (state.get("last_mode") or "").strip().lower()
    last_completed = state.get("last_completed_page")

    if last_mode == "initial" and isinstance(last_completed, int) and last_completed >= 1:
        return last_completed + 1

    return 1


def run(mode: str) -> None:
    settings = get_settings()
    db = SupabaseDB(settings.supabase_url, settings.supabase_service_role_key)

    state = db.get_scrape_state()
    stop_signature = state.get("last_fleet_signature") if mode == "nightly" else None

    ph = PistonHeadsClient(
        user_agent=settings.user_agent,
        min_delay=settings.min_delay_seconds,
        max_delay=settings.max_delay_seconds,
        cooldown_seconds=settings.cooldown_seconds,
        max_retries=settings.max_retries,
    )

    now_ldn = london_now(settings.timezone)

    try:
        # ✅ Resume logic for long initial scrapes (Render cron timeouts)
        if mode == "initial":
            p = _get_initial_resume_page(state)
            if p > 1:
                print(f"[resume] initial scrape resuming from page {p} (last_completed_page={p-1})")
            else:
                print("[start] initial scrape starting from page 1")
        else:
            p = 1
            print("[start] nightly scrape starting from page 1")

        first_row_signature_this_run: Optional[str] = None
        known_makes: Optional[list[str]] = None

        while True:
            fleet_url = settings.start_url_template.format(p=p)
            r = ph.get(fleet_url)

            # Genuine 403 handling is in the client; here handle other failures
            if r.status_code >= 500:
                raise RuntimeError(f"Server error {r.status_code} on fleet page {p}")

            fleet_html = r.text

            # Build known makes from first fleet page we successfully fetch.
            # If extraction fails, fall back to the full list you provided.
            if known_makes is None:
                extracted = extract_known_makes_from_fleet_html(fleet_html)
                known_makes = extracted if extracted else FALLBACK_MAKES
                print(f"[fleet] known makes loaded: {len(known_makes)} (extracted={len(extracted)})")

            # Parse rows
            rows = parse_fleet_page(fleet_html, now_london=now_ldn, known_makes=known_makes)

            if not rows:
                # no table / no results => end
                print(f"[end] no rows found on page {p}")
                break

            if first_row_signature_this_run is None:
                first_row_signature_this_run = rows[0].signature

            # nightly stop: once we hit the previous scrape signature, we're done
            if mode == "nightly" and stop_signature:
                for row in rows:
                    if row.signature == stop_signature:
                        db.update_scrape_state(
                            last_fleet_signature=first_row_signature_this_run,
                            last_run_at=iso(now_ldn),
                            last_mode=mode,
                            last_completed_page=p,
                        )
                        print(f"[stop] reached previous signature on page {p}.")
                        return

            # process each row sequentially
            for row in rows:
                # initial stop condition: stop once items are earlier than 01/01/2023 London time
                if mode == "initial" and row.updated_at_london is not None and row.updated_at_london < STOP_BEFORE_LONDON:
                    db.update_scrape_state(
                        last_fleet_signature=first_row_signature_this_run,
                        last_run_at=iso(now_ldn),
                        last_mode=mode,
                        last_completed_page=p,
                    )
                    print(f"[stop] reached pre-2023 item on page {p}: {row.updated_at_london.isoformat()}")
                    return

                car_url = settings.car_url_template.format(car_id=row.car_id)
                car_res = ph.get(car_url)

                # If 403, client already cooled down + retried. If still 403 here, treat as blocked and stop hard.
                if car_res.status_code == 403:
                    raise RuntimeError(f"403 persisted after retries on carId={row.car_id}")

                if car_res.status_code == 404:
                    # soft skip
                    continue

                car_html = car_res.text

                # soft error pages: skip immediately, no cooldown
                if detect_soft_error(car_html):
                    continue

                details = parse_car_details_page(car_html)

                ownership = (details.ownership or "").strip()
                if ownership not in ("Current Car", "Previously Owned"):
                    # unknown format => skip to be safe
                    continue

                existing = db.get_car(row.car_id)

                # Extract make/model/year from the display string using known_makes
                make, model_name, model_year = make_model_year_fields(row.display_name, known_makes)

                if ownership == "Previously Owned":
                    if existing:
                        # mark as sold if not already
                        if existing.get("status") != "Sold":
                            db.update_car(row.car_id, {
                                "status": "Sold",
                                "sold_at": iso(now_ldn),
                                "last_seen_at": iso(now_ldn),
                                "last_scraped_at": iso(now_ldn),
                                "last_updated_at": iso(row.updated_at_london),
                                "last_updated_raw": row.updated_raw,
                            })
                        else:
                            db.update_car(row.car_id, {
                                "last_seen_at": iso(now_ldn),
                                "last_scraped_at": iso(now_ldn),
                                "last_updated_at": iso(row.updated_at_london),
                                "last_updated_raw": row.updated_raw,
                            })
                    # If it’s previously owned and we don’t have it, do nothing (per your spec).
                    continue

                # ownership == Current Car
                if not existing:
                    notes_text = details.notes_text.strip() if details.notes_text else None
                    row_to_insert: Dict[str, Any] = {
                        "car_id": row.car_id,
                        "owner_username": row.owner,
                        "display_name": row.display_name,
                        "make": make,
                        "model": model_name,
                        "model_year": model_year,
                        "status": "Current",
                        "sold_at": None,
                        "last_updated_at": iso(row.updated_at_london),
                        "last_updated_raw": row.updated_raw,
                        "notes_current": notes_text,
                        "notes_history": None,
                        "first_seen_at": iso(now_ldn),
                        "last_seen_at": iso(now_ldn),
                        "last_scraped_at": iso(now_ldn),
                    }
                    db.upsert_car(row_to_insert)
                else:
                    patch: Dict[str, Any] = {
                        "owner_username": row.owner,  # in case username display changes (rare)
                        "last_updated_at": iso(row.updated_at_london),
                        "last_updated_raw": row.updated_raw,
                        "last_seen_at": iso(now_ldn),
                        "last_scraped_at": iso(now_ldn),
                        "status": "Current",
                        "sold_at": None,
                    }

                    # display_name / make / model / year changes
                    if (existing.get("display_name") or "") != row.display_name:
                        patch["display_name"] = row.display_name
                        patch["make"] = make
                        patch["model"] = model_name
                        patch["model_year"] = model_year

                    # notes append logic
                    new_notes = (details.notes_text or "").strip()
                    old_notes = (existing.get("notes_current") or "").strip()
                    if new_notes and new_notes != old_notes:
                        history = existing.get("notes_history")
                        if not isinstance(history, list):
                            history = []

                        history.append({
                            "captured_at": iso(now_ldn),
                            "notes": new_notes,
                        })
                        patch["notes_current"] = new_notes
                        patch["notes_history"] = history

                    db.update_car(row.car_id, patch)

            # finished page: ✅ persist progress so a timed-out run can resume
            db.update_scrape_state(
                last_run_at=iso(now_ldn),
                last_mode=mode,
                last_completed_page=p,
                # only set last_fleet_signature on completion/stop for nightly,
                # but it doesn't hurt to keep the first-row signature up to date
                last_fleet_signature=first_row_signature_this_run if first_row_signature_this_run else None,
            )

            print(f"[page] completed page {p} (rows={len(rows)})")
            p += 1

    finally:
        ph.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["initial", "nightly"], required=True)
    args = parser.parse_args()
    run(args.mode)


if __name__ == "__main__":
    main()
