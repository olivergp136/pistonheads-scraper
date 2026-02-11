# scraper/pistonheads.py

from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

import httpx
import pytz
from bs4 import BeautifulSoup

from .parsing import (
    FleetRow,
    clean_text,
    normalize_updated_raw,
    parse_fleet_updated,
    detect_soft_error,
    html_notes_to_text,
    parse_make_model_year,
)

@dataclass
class CarDetails:
    ownership: Optional[str]       # "Current Car" / "Previously Owned"
    notes_text: str

def london_now(tz_name: str) -> datetime:
    tz = pytz.timezone(tz_name)
    return datetime.now(tz)

def jitter_sleep(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))

class PistonHeadsClient:
    def __init__(self, *, user_agent: str, min_delay: float, max_delay: float, cooldown_seconds: int, max_retries: int):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.cooldown_seconds = cooldown_seconds
        self.max_retries = max_retries
        self.client = httpx.Client(
            timeout=httpx.Timeout(30.0),
            headers={
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
                "Connection": "keep-alive",
            },
            follow_redirects=True,
        )

    def close(self):
        self.client.close()

    def get(self, url: str) -> httpx.Response:
        for attempt in range(1, self.max_retries + 1):
            jitter_sleep(self.min_delay, self.max_delay)
            r = self.client.get(url)
            if r.status_code == 403:
                if attempt < self.max_retries:
                    time.sleep(self.cooldown_seconds)
                    continue
            return r
        return r

def extract_known_makes_from_fleet_html(fleet_html: str) -> List[str]:
    soup = BeautifulSoup(fleet_html, "html.parser")
    sel = soup.find("select", {"id": "marque"})
    makes: List[str] = []
    if not sel:
        return makes
    for opt in sel.find_all("option"):
        val = clean_text(opt.get_text())
        if not val or val.lower() in ("all marques", "all"):
            continue
        makes.append(val)
    seen = set()
    out = []
    for m in makes:
        if m.lower() not in seen:
            out.append(m)
            seen.add(m.lower())
    return out

def parse_fleet_page(
    fleet_html: str,
    *,
    now_london: datetime,
    known_makes: List[str],
) -> List[FleetRow]:
    soup = BeautifulSoup(fleet_html, "html.parser")
    table = soup.find("table", {"class": "data"})
    if not table:
        return []

    rows = []
    tbody = table.find("tbody")
    tr_list = (tbody.find_all("tr") if tbody else table.find_all("tr"))

    for tr in tr_list:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        owner = clean_text(tds[0].get_text())

        model_td = tds[2]
        a = model_td.find("a", href=True)
        if not a:
            continue

        display_name = clean_text(a.get_text())
        href = a["href"]

        m = re.search(r"carId=(\d+)", href, re.IGNORECASE)
        if not m:
            continue
        car_id = int(m.group(1))

        updated_td = tds[4]
        updated_raw = normalize_updated_raw(updated_td.get_text())

        updated_at = parse_fleet_updated(updated_raw, now_london)
        signature = f"{car_id}|{owner}|{updated_raw}"

        rows.append(FleetRow(
            owner=owner,
            display_name=display_name,
            car_id=car_id,
            updated_raw=updated_raw,
            updated_at_london=updated_at,
            signature=signature,
        ))

    return rows

def parse_car_details_page(car_html: str) -> CarDetails:
    soup = BeautifulSoup(car_html, "html.parser")

    ownership_div = soup.find("div", {"id": "ownership"})
    ownership = clean_text(ownership_div.get_text()) if ownership_div else None

    notes_div = soup.find("div", {"id": "notes"})
    notes_text = ""
    if notes_div:
        notes_text = html_notes_to_text(notes_div.decode_contents())
    return CarDetails(ownership=ownership, notes_text=notes_text)

def make_model_year_fields(display_name: str, known_makes: List[str]) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    return parse_make_model_year(display_name, known_makes)
