import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from dotenv import load_dotenv

_BACKEND_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=_BACKEND_ENV_PATH, override=False)


def _to_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _split_csv(value: str | None, default: List[str]) -> List[str]:
    if not value:
        return default
    parts = [item.strip() for item in value.split(",") if item.strip()]
    return parts or default


def _default_app_db_path() -> str:
    backend_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(backend_root, "data", "app.db")


@dataclass(frozen=True)
class Settings:
    db_server: str
    db_port: int
    db_name: str
    db_user: str
    db_pass: str

    admin_username: str
    admin_password: str
    admin_email: str

    app_db_path: str
    app_encryption_key: str | None

    jwt_secret: str
    jwt_algorithm: str
    jwt_expires_minutes: int
    password_reset_expiry_minutes: int

    allow_origins: List[str]
    rate_limit_window_sec: int
    rate_limit_max_requests: int
    shift_out_cutoff_hours: int
    inout_swap: bool
    cookie_domain: str | None
    cookie_secure: bool

    frontend_base_url: str
    smtp_timeout_seconds: int


def get_settings() -> Settings:
    return Settings(
        db_server=(os.getenv("DB_SERVER") or "").strip(),
        db_port=_to_int(os.getenv("DB_PORT"), 1433),
        db_name=os.getenv("DB_NAME", "AXData"),
        db_user=os.getenv("DB_USER", ""),
        db_pass=os.getenv("DB_PASS", ""),
        admin_username=os.getenv("ADMIN_USERNAME", "admin"),
        admin_password=os.getenv("ADMIN_PASSWORD", "change-me"),
        admin_email=os.getenv("ADMIN_EMAIL", "admin@local"),
        app_db_path=os.getenv("APP_DB_PATH", _default_app_db_path()),
        app_encryption_key=(os.getenv("APP_ENCRYPTION_KEY") or os.getenv("JWT_SECRET") or None),
        jwt_secret=os.getenv("JWT_SECRET", "change-this-secret"),
        jwt_algorithm=os.getenv("JWT_ALGORITHM", "HS256"),
        jwt_expires_minutes=_to_int(os.getenv("JWT_EXPIRES_MINUTES"), 480),
        password_reset_expiry_minutes=max(5, _to_int(os.getenv("PASSWORD_RESET_EXPIRY_MINUTES"), 60)),
        allow_origins=_split_csv(
            os.getenv("ALLOW_ORIGIN"),
            [
                "https://dash.hse-oilchem.com",
                "https://admin.hse-oilchem.com",
                "http://localhost:3000",
            ],
        ),
        rate_limit_window_sec=_to_int(os.getenv("RATE_LIMIT_WINDOW_SEC"), 60),
        rate_limit_max_requests=_to_int(os.getenv("RATE_LIMIT_MAX_REQUESTS"), 120),
        shift_out_cutoff_hours=max(0, min(_to_int(os.getenv("SHIFT_OUT_CUTOFF_HOURS"), 12), 23)),
        inout_swap=_to_bool(os.getenv("INOUT_SWAP"), False),
        cookie_domain=os.getenv("COOKIE_DOMAIN") or None,
        cookie_secure=_to_bool(os.getenv("COOKIE_SECURE"), False),
        frontend_base_url=(os.getenv("FRONTEND_BASE_URL", "http://localhost:3000").strip().rstrip("/")),
        smtp_timeout_seconds=max(5, _to_int(os.getenv("SMTP_TIMEOUT_SECONDS"), 20)),
    )


settings = get_settings()
