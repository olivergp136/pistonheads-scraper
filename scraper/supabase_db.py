from __future__ import annotations

import random
import time
from typing import Any, Dict, Optional, Callable, TypeVar

from supabase import create_client, Client

T = TypeVar("T")


class SupabaseDB:
    """
    Wrapper around supabase-py with retries for transient Cloudflare/Supabase hiccups.

    Your logs show Supabase returned an HTML Cloudflare 500 page, which causes a JSON decode
    failure inside the client. That is transient; we retry those instead of crashing the run.
    """

    def __init__(self, url: str, service_role_key: str):
        self.client: Client = create_client(url, service_role_key)

    # -----------------------
    # Retry helpers
    # -----------------------
    @staticmethod
    def _is_retryable_error(err: Exception) -> bool:
        """
        Decide if an exception is likely transient and safe to retry.
        We keep this conservative: only gateway/Cloudflare/JSON-decode/network-ish issues.
        """
        msg = str(err).lower()

        # JSON decoding issues (often caused by HTML response / empty response)
        if "jsondecodeerror" in msg or "expecting value" in msg:
            return True

        # Cloudflare / gateway-ish
        if "cloudflare" in msg or "internal server error" in msg:
            return True

        # Supabase/PostgREST: when non-JSON comes back, it often says this
        if "json could not be generated" in msg:
            return True

        # Common transient HTTP codes bubbling up in exception strings
        for code in ("500", "502", "503", "504"):
            if f"code': {code}" in msg or f"code={code}" in msg:
                return True

        # Network-ish transient signals
        if "timeout" in msg or "timed out" in msg:
            return True
        if "connection reset" in msg or "connecterror" in msg or "readerror" in msg:
            return True

        return False

    @staticmethod
    def _backoff_sleep(attempt: int) -> None:
        """
        Exponential backoff with jitter. attempt starts at 1.
        """
        base = min(60.0, 2.0 ** attempt)  # cap growth
        jitter = random.uniform(0.0, 1.5)
        time.sleep(base + jitter)

    def _with_retries(self, fn: Callable[[], T], *, op_name: str, max_attempts: int = 6) -> T:
        last_err: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                return fn()
            except Exception as err:
                last_err = err
                if self._is_retryable_error(err) and attempt < max_attempts:
                    print(
                        f"[supabase] transient error during {op_name} "
                        f"(attempt {attempt}/{max_attempts}) -> retrying. err={err}"
                    )
                    self._backoff_sleep(attempt)
                    continue
                raise
        # should never hit
        raise last_err if last_err else RuntimeError(f"Unknown failure in {op_name}")

    # ---- scrape_state ----
    def get_scrape_state(self) -> Dict[str, Any]:
        def _op():
            res = self.client.table("scrape_state").select("*").eq("id", 1).single().execute()
            return res.data

        return self._with_retries(_op, op_name="get_scrape_state")

    def update_scrape_state(
        self,
        *,
        last_fleet_signature: Optional[str] = None,
        last_run_at: Optional[str] = None,
        last_mode: Optional[str] = None,
        last_completed_page: Optional[int] = None,
    ) -> None:
        payload: Dict[str, Any] = {}
        if last_fleet_signature is not None:
            payload["last_fleet_signature"] = last_fleet_signature
        if last_run_at is not None:
            payload["last_run_at"] = last_run_at
        if last_mode is not None:
            payload["last_mode"] = last_mode
        if last_completed_page is not None:
            payload["last_completed_page"] = last_completed_page

        if not payload:
            return

        def _op():
            self.client.table("scrape_state").update(payload).eq("id", 1).execute()
            return None

        self._with_retries(_op, op_name="update_scrape_state")

    # ---- member_cars ----
    def get_car(self, car_id: int) -> Optional[Dict[str, Any]]:
        def _op():
            res = self.client.table("member_cars").select("*").eq("car_id", car_id).limit(1).execute()
            if res.data:
                return res.data[0]
            return None

        return self._with_retries(_op, op_name="get_car")

    def upsert_car(self, row: Dict[str, Any]) -> None:
        def _op():
            self.client.table("member_cars").upsert(row).execute()
            return None

        self._with_retries(_op, op_name="upsert_car")

    def update_car(self, car_id: int, patch: Dict[str, Any]) -> None:
        def _op():
            self.client.table("member_cars").update(patch).eq("car_id", car_id).execute()
            return None

        self._with_retries(_op, op_name="update_car")
