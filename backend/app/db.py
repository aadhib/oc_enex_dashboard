from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import pyodbc
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_BACKEND_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
_DOTENV_LOADED = False
_DB_TARGET_LOGGED = False


def ensure_backend_env_loaded() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    load_dotenv(dotenv_path=_BACKEND_ENV_PATH, override=False)
    _DOTENV_LOADED = True


def _to_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    text = value.strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def _default_nix_driver_path() -> str:
    if sys.platform == "darwin":
        return "/opt/homebrew/lib/libtdsodbc.so"
    return "/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so"


def get_db_settings() -> dict[str, Any]:
    ensure_backend_env_loaded()
    return {
        "server": (os.getenv("DB_SERVER") or "").strip(),
        "port": _to_int(os.getenv("DB_PORT"), 1433),
        "name": (os.getenv("DB_NAME") or "AXData").strip() or "AXData",
        "user": (os.getenv("DB_USER") or "").strip(),
        "pass": os.getenv("DB_PASS") or "",
        "win_driver": (os.getenv("WIN_DRIVER") or "SQL Server").strip() or "SQL Server",
        "tds_version": (os.getenv("TDS_VERSION") or "7.0").strip() or "7.0",
        "nix_driver_path": (
            os.getenv("NIX_DRIVER_PATH") or _default_nix_driver_path()
        ).strip()
        or _default_nix_driver_path(),
    }


def validate_db_server_for_startup() -> None:
    db = get_db_settings()
    server = str(db["server"]).strip().lower()
    if not server:
        raise RuntimeError("DB_SERVER is empty; set DB_SERVER in backend/.env")
    if server in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError(
            "DB_SERVER is localhost; set DB_SERVER to the Windows Server IP in backend/.env"
        )


def log_db_connection_target_once() -> None:
    global _DB_TARGET_LOGGED
    if _DB_TARGET_LOGGED:
        return
    db = get_db_settings()
    logger.info(
        "DB: connecting to %s:%s / %s as %s",
        db["server"],
        db["port"],
        db["name"],
        db["user"] or "<empty>",
    )
    _DB_TARGET_LOGGED = True


def get_db_connection_error_payload() -> dict[str, Any]:
    db = get_db_settings()
    return {
        "error": "DB connection failed",
        "hint": "Check VPN/tunnel and backend/.env DB_SERVER/DB_PORT",
        "server": db["server"],
        "port": db["port"],
    }


def _clean_driver(driver_value: str) -> str:
    return driver_value.strip().strip("{}")


def build_connection_string() -> str:
    db = get_db_settings()
    server = db["server"]
    port = int(db["port"])
    database = db["name"]
    user = db["user"]
    password = db["pass"]

    if sys.platform == "win32":
        win_driver = _clean_driver(str(db["win_driver"]))
        return (
            f"DRIVER={{{win_driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "Trusted_Connection=no;"
        )

    nix_driver = _clean_driver(str(db["nix_driver_path"]))
    tds_version = str(db["tds_version"])
    return (
        f"DRIVER={{{nix_driver}}};"
        f"SERVER={server};"
        f"PORT={port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TDS_Version={tds_version};"
        "Encrypt=no;"
    )


def _normalize_sql_placeholders(sql: str) -> str:
    # Legacy query fragments still use `%s` placeholders. pyodbc expects `?`.
    return sql.replace("%s", "?")


def _normalize_params(params: Any | None) -> tuple[Any, ...]:
    if params is None:
        return ()
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    if isinstance(params, set):
        return tuple(params)
    if isinstance(params, dict):
        raise TypeError("Named parameters are not supported for this DB cursor.")
    if isinstance(params, str):
        return (params,)
    if isinstance(params, bytes):
        return (params,)
    if isinstance(params, Iterable):
        return tuple(params)
    return (params,)


def _row_to_dict(description: Sequence[Any] | None, row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)

    columns = [str(column[0]) for column in (description or [])]
    if not columns:
        return {}
    return {columns[index]: row[index] for index in range(len(columns))}


def rows_to_dicts(cursor: Any, rows: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    if rows is None:
        rows = cursor.fetchall()
    description = getattr(cursor, "description", None)
    return [_row_to_dict(description, row) for row in rows]


class DictCursor:
    def __init__(self, cursor: pyodbc.Cursor):
        self._cursor = cursor

    @property
    def description(self) -> Any:
        return self._cursor.description

    def execute(self, sql: str, params: Any | None = None) -> "DictCursor":
        normalized_sql = _normalize_sql_placeholders(sql)
        normalized_params = _normalize_params(params)
        if normalized_params:
            self._cursor.execute(normalized_sql, *normalized_params)
        else:
            self._cursor.execute(normalized_sql)
        return self

    def executemany(self, sql: str, params_seq: Sequence[Sequence[Any]]) -> "DictCursor":
        normalized_sql = _normalize_sql_placeholders(sql)
        self._cursor.executemany(normalized_sql, params_seq)
        return self

    def fetchone(self) -> dict[str, Any] | None:
        row = self._cursor.fetchone()
        if row is None:
            return None
        return _row_to_dict(self._cursor.description, row)

    def fetchmany(self, size: int | None = None) -> list[dict[str, Any]]:
        if size is None:
            rows = self._cursor.fetchmany()
        else:
            rows = self._cursor.fetchmany(size)
        return rows_to_dicts(self._cursor, rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return rows_to_dicts(self._cursor, self._cursor.fetchall())

    def close(self) -> None:
        self._cursor.close()

    def __iter__(self) -> Iterator[dict[str, Any]]:
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row

    def __getattr__(self, item: str) -> Any:
        return getattr(self._cursor, item)


DBOperationalError = pyodbc.Error


@contextmanager
def get_connection() -> Iterator[pyodbc.Connection]:
    validate_db_server_for_startup()
    conn = pyodbc.connect(build_connection_string(), timeout=8)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_cursor() -> Iterator[DictCursor]:
    with get_connection() as connection:
        cursor = connection.cursor()
        wrapped = DictCursor(cursor)
        try:
            yield wrapped
        finally:
            wrapped.close()


ensure_backend_env_loaded()
