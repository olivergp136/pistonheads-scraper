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
from .known_makes import FALLBACK_MAKES

STOP_BEFORE_LONDON = pytz.timezone("Europe/London").localize(datetime(2023, 1, 1, 0, 0, 0))


def iso(dt) -> Optional[str]:
    return None if dt is None else dt.isoformat()


def _get_initial_resume_page(state: Dict[str, Any]) -> int:
    last_completed = state.get("last_completed_page")
    if isinstance(last_completed, int) and last_completed >= 1:
        return last_completed + 1
    return 1


def run(mode: str) -> None:
    settings = get_settings()
    db = SupabaseDB(settings.supabase_url, settings.supabase_service_role_key)

    state = db.get_scrape_state()

    # ✅ Nightly uses last_fleet_signature as "previous nightly head signature"
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
        if mode == "initial":
            p = _get_initial_resume_page(state)
            print(f"[start] initial scrape from page {p}")
        else:
            p = 1
            print("[start] nightly scrape from page 1")

        nightly_head_signature_this_run: Optional[str] = None
        known_makes: Optional[list[str]] = None

        while True:
            fleet_url = settings.start_url_template.format(p=p)
            r = ph.get(fleet_url)

            if r.status_code >= 500:
                raise RuntimeError(f"Server error {r.status_code} on fleet page {p}")

            fleet_html = r.text

            if known_makes is None:
                extracted = extract_known_makes_from_fleet_html(fleet_html)
                known_makes = extracted if extracted else FALLBACK_MAKES
                print(f"[fleet] known makes loaded: {len(known_makes)} (extracted={len(extracted)})")

            rows = parse_fleet_page(fleet_html, now_london=now_ldn, known_makes=known_makes)

            if not rows:
                print(f"[end] no rows found on page {p}")
                break

            # ✅ Save the "head signature" for THIS nightly run (page 1, first row)
            if mode == "nightly" and nightly_head_signature_this_run is None:
                nightly_head_signature_this_run = rows[0].signature
                print(f"[nightly] head signature this run: {nightly_head_signature_this_run}")
                print(f"[nightly] stop signature from previous run: {stop_signature}")

            # ✅ Stop as soon as we encounter yesterday's head signature
            if mode == "nightly" and stop_signature:
                for row in rows:
                    if row.signature == stop_signature:
                        db.update_scrape_state(
                            last_fleet_signature=nightly_head_signature_this_run,
                            last_run_at=iso(now_ldn),
                            last_mode="nightly",
                            # IMPORTANT: nightly does NOT touch last_completed_page (reserved for initial resume)
                        )
                        print(f"[stop] reached previous nightly signature on page {p}. Done.")
                        return

            for row in rows:
                if mode == "initial" and row.updated_at_london is not None and row.updated_at_london < STOP_BEFORE_LONDON:
                    db.update_scrape_state(
                        last_run_at=iso(now_ldn),
                        last_mode="initial",
                        last_completed_page=p,
                    )
                    print(f"[stop] reached pre-2023 item on page {p}: {row.updated_at_london.isoformat()}")
                    return

                car_url = settings.car_url_template.format(car_id=row.car_id)
                car_res = ph.get(car_url)

                if car_res.status_code == 403:
                    raise RuntimeError(f"403 persisted after retries on carId={row.car_id}")

                if car_res.status_code == 404:
                    continue

                car_html = car_res.text
                if detect_soft_error(car_html):
                    continue

                details = parse_car_details_page(car_html)
                ownership = (details.ownership or "").strip()
                if ownership not in ("Current Car", "Previously Owned"):
                    continue

                existing = db.get_car(row.car_id)
                make, model_name, model_year = make_model_year_fields(row.display_name, known_makes)

                if ownership == "Previously Owned":
                    if existing:
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
                    continue

                # Current Car
                if not existing:
                    notes_text = details.notes_text.strip() if details.notes_text else None
                    db.upsert_car({
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
                    })
                else:
                    patch: Dict[str, Any] = {
                        "owner_username": row.owner,
                        "last_updated_at": iso(row.updated_at_london),
                        "last_updated_raw": row.updated_raw,
                        "last_seen_at": iso(now_ldn),
                        "last_scraped_at": iso(now_ldn),
                        "status": "Current",
                        "sold_at": None,
                    }

                    if (existing.get("display_name") or "") != row.display_name:
                        patch["display_name"] = row.display_name
                        patch["make"] = make
                        patch["model"] = model_name
                        patch["model_year"] = model_year

                    new_notes = (details.notes_text or "").strip()
                    old_notes = (existing.get("notes_current") or "").strip()
                    if new_notes and new_notes != old_notes:
                        history = existing.get("notes_history")
                        if not isinstance(history, list):
                            history = []
                        history.append({"captured_at": iso(now_ldn), "notes": new_notes})
                        patch["notes_current"] = new_notes
                        patch["notes_history"] = history

                    db.update_car(row.car_id, patch)

            # ✅ Progress bookkeeping
            if mode == "initial":
                db.update_scrape_state(
                    last_run_at=iso(now_ldn),
                    last_mode="initial",
                    last_completed_page=p,
                )
            else:
                # Nightly: do NOT touch last_completed_page
                db.update_scrape_state(
                    last_run_at=iso(now_ldn),
                    last_mode="nightly",
                )

            print(f"[page] completed page {p} (rows={len(rows)})")
            p += 1

        # If nightly finishes without hitting stop_signature (rare on first ever nightly),
        # still set the head signature so tomorrow stops properly.
        if mode == "nightly" and nightly_head_signature_this_run:
            db.update_scrape_state(
                last_fleet_signature=nightly_head_signature_this_run,
                last_run_at=iso(now_ldn),
                last_mode="nightly",
            )

    finally:
        ph.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["initial", "nightly"], required=True)
    args = parser.parse_args()
    run(args.mode)


if __name__ == "__main__":
    main()
