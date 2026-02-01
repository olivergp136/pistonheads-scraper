# scraper/parsing.py

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

import pytz
from bs4 import BeautifulSoup

# Soft error phrases (skip immediately, no 403 cooldown)
SOFT_ERROR_PHRASES = [
    "This user's profile is not available",
    "This member limits who may view their full profile",
    "Oops",
    "can't find",
    "cannot find",
    "not available",
]

YEAR_RE = re.compile(r"\((19|20)\d{2}\)\s*$")

# e.g. "Monday 26th January"  (assume current year)
# e.g. "Tuesday 23rd September 2025"
# (Optional time isn't shown in your examples, but we allow it if it appears later.)
LONG_DATE_RE = re.compile(
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
    r"(\d{1,2})(st|nd|rd|th)\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"(?:\s+(\d{4}))?"
    r"(?:\s*\((\d{1,2}):(\d{2})\))?$",
    re.IGNORECASE,
)

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass
class FleetRow:
    owner: str
    display_name: str
    car_id: int
    updated_raw: str
    updated_at_london: Optional[datetime]
    signature: str


def clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def html_notes_to_text(notes_html: str) -> str:
    if not notes_html:
        return ""
    soup = BeautifulSoup(notes_html, "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text()
    text = html.unescape(text)
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def parse_make_model_year(display_name: str, known_makes: List[str]) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    - year: pull (YYYY) at end if present
    - make: longest prefix match against known_makes
    - model: remainder (minus make and year), stripped
    """
    dn = display_name.strip()

    year = None
    m = YEAR_RE.search(dn)
    if m:
        year = int(m.group(0).strip("() "))
        dn_wo_year = YEAR_RE.sub("", dn).strip()
    else:
        dn_wo_year = dn

    dn_norm = re.sub(r"\s+", " ", dn_wo_year).strip().lower()

    makes_sorted = sorted(known_makes, key=lambda x: len(x), reverse=True)
    make_found = None
    for make in makes_sorted:
        mk_norm = make.lower()
        if dn_norm == mk_norm or dn_norm.startswith(mk_norm + " "):
            make_found = make
            break

    model = dn_wo_year
    if make_found:
        pattern = re.compile(rf"^{re.escape(make_found)}\s+", re.IGNORECASE)
        model = pattern.sub("", dn_wo_year).strip()

    if model == "":
        model = None

    return make_found, model, year


def detect_soft_error(page_text: str) -> bool:
    t = (page_text or "").lower()
    return any(p.lower() in t for p in SOFT_ERROR_PHRASES)


def parse_fleet_updated(updated_raw: str, now_london: datetime) -> Optional[datetime]:
    """
    Convert fleet Updated cell text into a tz-aware datetime in Europe/London.

    Handles:
      - "09:49" -> today at 09:49
      - "Yesterday (22:19)" -> yesterday at 22:19
      - "Friday (10:01)" -> most recent Friday at 10:01 (within past 7 days)
      - "Friday" -> most recent Friday at 00:00
      - "Monday 26th January" -> assume current year, time 00:00
      - "Tuesday 23rd September 2025" -> explicit year, time 00:00
        (Also supports optional "(HH:MM)" on the end if it ever appears.)

    If unparseable, returns None.
    """
    raw = clean_text(updated_raw)

    if now_london.tzinfo is None:
        raise RuntimeError("now_london must be tz-aware (Europe/London).")

    # 1) Long-form dates: "Monday 26th January" OR "Tuesday 23rd September 2025"
    m = LONG_DATE_RE.fullmatch(raw)
    if m:
        day_num = int(m.group(2))
        month_name = (m.group(4) or "").lower()
        year_str = m.group(5)
        hh_str = m.group(6)
        mm_str = m.group(7)

        month_num = MONTHS.get(month_name)
        if not month_num:
            return None

        year_num = int(year_str) if year_str else now_london.year
        hh = int(hh_str) if hh_str is not None else 0
        mm = int(mm_str) if mm_str is not None else 0

        try:
            dt = now_london.replace(year=year_num, month=month_num, day=day_num, hour=hh, minute=mm, second=0, microsecond=0)
            return dt
        except ValueError:
            # invalid calendar date
            return None

    # 2) Yesterday (HH:MM)
    m = re.search(r"Yesterday\s*\((\d{1,2}):(\d{2})\)", raw, re.IGNORECASE)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        dt = (now_london - timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)
        return dt

    # 3) Weekday (HH:MM)
    m = re.search(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*\((\d{1,2}):(\d{2})\)",
        raw,
        re.IGNORECASE,
    )
    if m:
        wd_name = m.group(1).lower()
        hh, mm = int(m.group(2)), int(m.group(3))
        target = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"].index(wd_name)
        days_back = (now_london.weekday() - target) % 7
        candidate = (now_london - timedelta(days=days_back)).replace(hour=hh, minute=mm, second=0, microsecond=0)
        if candidate > now_london:
            candidate = candidate - timedelta(days=7)
        return candidate

    # 4) Just time "HH:MM"
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        dt = now_london.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if dt > now_london + timedelta(minutes=1):
            dt = dt - timedelta(days=1)
        return dt

    # 5) Weekday only
    m = re.fullmatch(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)", raw, re.IGNORECASE)
    if m:
        wd_name = m.group(1).lower()
        target = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"].index(wd_name)
        days_back = (now_london.weekday() - target) % 7
        candidate = (now_london - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
        if candidate > now_london:
            candidate = candidate - timedelta(days=7)
        return candidate

    return None
