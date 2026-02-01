from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
from supabase import create_client, Client

class SupabaseDB:
    def __init__(self, url: str, service_role_key: str):
        self.client: Client = create_client(url, service_role_key)

    # ---- scrape_state ----
    def get_scrape_state(self) -> Dict[str, Any]:
        res = self.client.table("scrape_state").select("*").eq("id", 1).single().execute()
        return res.data

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

        self.client.table("scrape_state").update(payload).eq("id", 1).execute()

    # ---- member_cars ----
    def get_car(self, car_id: int) -> Optional[Dict[str, Any]]:
        res = self.client.table("member_cars").select("*").eq("car_id", car_id).limit(1).execute()
        if res.data:
            return res.data[0]
        return None

    def upsert_car(self, row: Dict[str, Any]) -> None:
        self.client.table("member_cars").upsert(row).execute()

    def update_car(self, car_id: int, patch: Dict[str, Any]) -> None:
        self.client.table("member_cars").update(patch).eq("car_id", car_id).execute()

