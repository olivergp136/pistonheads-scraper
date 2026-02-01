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

@dataclass
class FleetRow:
    owner: str
    display_name: str
    car_id: int
    updated_raw: str
    updated_at_london: Optional[datetime]
    signature: str

def _tz(tz_name: str):
    return pytz.timezone(tz_name)

def clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def html_notes_to_text(notes_html: str) -> str:
    if not notes_html:
        return ""
    # notes content includes <br/> etc. Use BeautifulSoup to convert to text lines.
    soup = BeautifulSoup(notes_html, "html.parser")
    # replace <br> with newlines
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text()
    text = html.unescape(text)
    # normalize whitespace but preserve newlines reasonably
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

    # normalize for prefix matching
    dn_norm = re.sub(r"\s+", " ", dn_wo_year).strip().lower()

    # longest make first
    makes_sorted = sorted(known_makes, key=lambda x: len(x), reverse=True)
    make_found = None
    for make in makes_sorted:
        mk_norm = make.lower()
        # allow exact prefix + space or exact string
        if dn_norm == mk_norm or dn_norm.startswith(mk_norm + " "):
            make_found = make
            break

    model = dn_wo_year
    if make_found:
        # remove the make prefix in original-casing space-normalized way
        # do a case-insensitive prefix strip
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
      - "Friday" -> most recent Friday at 00:00 (fallback)
    If unparseable, returns None.
    """
    raw = clean_text(updated_raw)

    tz = now_london.tzinfo
    if tz is None:
        raise RuntimeError("now_london must be tz-aware")

    # Yesterday (HH:MM)
    m = re.search(r"Yesterday\s*\((\d{1,2}):(\d{2})\)", raw, re.IGNORECASE)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        dt = (now_london - timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)
        return dt

    # Weekday (HH:MM)
    m = re.search(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*\((\d{1,2}):(\d{2})\)", raw, re.IGNORECASE)
    if m:
        wd_name = m.group(1).lower()
        hh, mm = int(m.group(2)), int(m.group(3))
        target = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"].index(wd_name)
        # compute most recent target weekday (including today if matches and time <= now)
        days_back = (now_london.weekday() - target) % 7
        candidate = (now_london - timedelta(days=days_back)).replace(hour=hh, minute=mm, second=0, microsecond=0)
        if candidate > now_london:
            candidate = candidate - timedelta(days=7)
        return candidate

    # Just time "HH:MM"
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        dt = now_london.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if dt > now_london + timedelta(minutes=1):
            # in case of weird clock drift; keep safe
            dt = dt - timedelta(days=1)
        return dt

    # Weekday only
    m = re.fullmatch(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)", raw, re.IGNORECASE)
    if m:
        wd_name = m.group(1).lower()
        target = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"].index(wd_name)
        days_back = (now_london.weekday() - target) % 7
        candidate = (now_london - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
        if candidate > now_london:
            candidate = candidate - timedelta(days=7)
        return candidate

    return None

