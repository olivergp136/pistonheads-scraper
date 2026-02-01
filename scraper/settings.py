import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_service_role_key: str
    user_agent: str
    timezone: str

    # rate limits
    min_delay_seconds: float = 6.0
    max_delay_seconds: float = 8.0

    # 403 cooldown
    cooldown_seconds: int = 30 * 60
    max_retries: int = 3

    # scraping
    start_url_template: str = "https://www.pistonheads.com/members/fleet.asp?p={p}&s=&m=&marque=&o=&model="
    base_url: str = "https://www.pistonheads.com/members/"
    car_url_template: str = "https://www.pistonheads.com/members/showCar.asp?carId={car_id}"

def get_settings() -> Settings:
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.")

    return Settings(
        supabase_url=url,
        supabase_service_role_key=key,
        user_agent=os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; PistonHeadsScraper/1.0)"),
        timezone=os.getenv("TIMEZONE", "Europe/London"),
    )

