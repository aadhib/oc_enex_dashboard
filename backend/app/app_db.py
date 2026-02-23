from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

from .config import settings
from .security import (
    decrypt_text,
    encrypt_text,
    generate_token,
    hash_password,
    hash_token,
    utc_now_iso,
)

logger = logging.getLogger(__name__)
_warned_plain_smtp = False
VALID_USER_ROLES = {"admin", "inspector"}


def _normalize_role(value: str | None) -> str:
    role = str(value or "").strip().lower()
    if role == "hr":
        return "inspector"
    if role in VALID_USER_ROLES:
        return role
    raise ValueError("role must be one of: admin, inspector")


def _normalize_user_payload(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    raw_role = str(normalized.get("role") or "").strip().lower()
    normalized["role"] = "inspector" if raw_role == "hr" else raw_role
    normalized["is_active"] = bool(normalized.get("is_active"))
    return normalized


def _dict_from_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


@contextmanager
def get_app_db() -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(settings.app_db_path, timeout=30, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def init_app_db() -> None:
    directory = os.path.dirname(settings.app_db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with get_app_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL CHECK(role IN ('admin', 'inspector')),
                password_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_login_at TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS password_resets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used_at TEXT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS smtp_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                host TEXT,
                port INTEGER,
                username TEXT,
                password_encrypted TEXT,
                from_email TEXT,
                from_name TEXT,
                use_tls INTEGER NOT NULL DEFAULT 1,
                use_ssl INTEGER NOT NULL DEFAULT 0,
                cc_list TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS employee_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_no TEXT NOT NULL UNIQUE,
                emp_id TEXT NULL,
                employee_name_cache TEXT NULL,
                employee_email TEXT,
                work_start_time TEXT,
                work_end_time TEXT,
                late_grace_minutes INTEGER NOT NULL DEFAULT 0,
                early_grace_minutes INTEGER NOT NULL DEFAULT 0,
                notify_employee INTEGER NOT NULL DEFAULT 0,
                notify_cc_override TEXT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notifications_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_no TEXT NOT NULL,
                date TEXT NOT NULL,
                type TEXT NOT NULL,
                to_email TEXT,
                cc TEXT,
                sent_at TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
            CREATE INDEX IF NOT EXISTS idx_password_resets_hash ON password_resets(token_hash);
            CREATE INDEX IF NOT EXISTS idx_employee_settings_card ON employee_settings(card_no);
            CREATE INDEX IF NOT EXISTS idx_notifications_log_date ON notifications_log(date);
            """
        )
        _migrate_users_role_schema(conn)
        _normalize_legacy_roles(conn)

    ensure_default_admin_user()


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.lower().split())


def _migrate_users_role_schema(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'users'"
    ).fetchone()
    if not row:
        return

    table_sql = str(row["sql"] or "")
    normalized_sql = _normalize_sql(table_sql)
    if "check(role in ('admin', 'inspector'))" in normalized_sql:
        return

    # Recreate users table to update role check constraint and normalize legacy roles.
    conn.executescript(
        """
        PRAGMA foreign_keys = OFF;

        CREATE TABLE IF NOT EXISTS users_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL CHECK(role IN ('admin', 'inspector')),
            password_hash TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_login_at TEXT NULL
        );

        INSERT INTO users_new (id, email, username, role, password_hash, is_active, created_at, updated_at, last_login_at)
        SELECT
            id,
            email,
            username,
            CASE
                WHEN lower(role) = 'admin' THEN 'admin'
                WHEN lower(role) = 'hr' THEN 'inspector'
                WHEN lower(role) = 'inspector' THEN 'inspector'
                ELSE 'inspector'
            END AS role,
            password_hash,
            is_active,
            created_at,
            updated_at,
            last_login_at
        FROM users;

        DROP TABLE users;
        ALTER TABLE users_new RENAME TO users;
        CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

        PRAGMA foreign_keys = ON;
        """
    )


def _normalize_legacy_roles(conn: sqlite3.Connection) -> None:
    now = utc_now_iso()
    conn.execute(
        "UPDATE users SET role = 'inspector', updated_at = ? WHERE lower(role) = 'hr'",
        (now,),
    )


def ensure_default_admin_user() -> None:
    with get_app_db() as conn:
        row = conn.execute(
            "SELECT id FROM users WHERE role = 'admin' LIMIT 1"
        ).fetchone()

        if row:
            return

        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO users (email, username, role, password_hash, is_active, created_at, updated_at)
            VALUES (?, ?, 'admin', ?, 1, ?, ?)
            """,
            (
                settings.admin_email.strip().lower(),
                settings.admin_username.strip(),
                hash_password(settings.admin_password),
                now,
                now,
            ),
        )
        logger.info("Created default admin user '%s'", settings.admin_username)


def get_user_by_login(login: str) -> dict[str, Any] | None:
    normalized = login.strip()
    if not normalized:
        return None

    with get_app_db() as conn:
        row = conn.execute(
            """
            SELECT id, email, username, role, password_hash, is_active, created_at, updated_at, last_login_at
            FROM users
            WHERE lower(username) = lower(?) OR lower(email) = lower(?)
            LIMIT 1
            """,
            (normalized, normalized),
        ).fetchone()

    payload = _dict_from_row(row)
    if not payload:
        return None
    return _normalize_user_payload(payload)


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with get_app_db() as conn:
        row = conn.execute(
            """
            SELECT id, email, username, role, password_hash, is_active, created_at, updated_at, last_login_at
            FROM users
            WHERE id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()

    payload = _dict_from_row(row)
    if not payload:
        return None
    return _normalize_user_payload(payload)


def touch_user_login(user_id: int) -> None:
    now = utc_now_iso()
    with get_app_db() as conn:
        conn.execute(
            "UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?",
            (now, now, user_id),
        )


def list_users() -> list[dict[str, Any]]:
    with get_app_db() as conn:
        rows = conn.execute(
            """
            SELECT id, email, username, role, is_active, created_at, updated_at, last_login_at
            FROM users
            ORDER BY
                CASE WHEN role = 'admin' THEN 0 ELSE 1 END,
                username COLLATE NOCASE ASC
            """
        ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        payload = _dict_from_row(row)
        if payload is None:
            continue
        result.append(_normalize_user_payload(payload))
    return result


def create_user(*, email: str, username: str, password: str, role: str) -> dict[str, Any]:
    normalized_email = email.strip().lower()
    normalized_username = username.strip()
    normalized_role = _normalize_role(role)
    if not normalized_email or not normalized_username:
        raise ValueError("email and username are required")

    now = utc_now_iso()
    with get_app_db() as conn:
        exists = conn.execute(
            "SELECT id FROM users WHERE lower(email) = lower(?) OR lower(username) = lower(?) LIMIT 1",
            (normalized_email, normalized_username),
        ).fetchone()
        if exists:
            raise ValueError("user with same email or username already exists")

        cursor = conn.execute(
            """
            INSERT INTO users (email, username, role, password_hash, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (normalized_email, normalized_username, normalized_role, hash_password(password), now, now),
        )
        user_id = int(cursor.lastrowid)

    user = get_user_by_id(user_id)
    if not user:
        raise ValueError("failed to create user")
    return user


def update_user(
    *,
    user_id: int,
    email: str | None = None,
    password: str | None = None,
    role: str | None = None,
    is_active: bool | None = None,
) -> dict[str, Any]:
    existing = get_user_by_id(user_id)
    if not existing:
        raise ValueError("user not found")

    normalized_email = str(existing.get("email") or "").strip().lower()
    if email is not None:
        normalized_email = email.strip().lower()
        if not normalized_email:
            raise ValueError("email is required")

    normalized_role = str(existing.get("role") or "inspector")
    if role is not None:
        normalized_role = _normalize_role(role)

    active_value = 1 if bool(existing.get("is_active")) else 0
    if is_active is not None:
        active_value = 1 if is_active else 0

    now = utc_now_iso()
    with get_app_db() as conn:
        duplicate = conn.execute(
            "SELECT id FROM users WHERE lower(email) = lower(?) AND id <> ? LIMIT 1",
            (normalized_email, user_id),
        ).fetchone()
        if duplicate:
            raise ValueError("user with same email already exists")

        params: list[Any] = [normalized_email, normalized_role, active_value, now]
        sql = "UPDATE users SET email = ?, role = ?, is_active = ?, updated_at = ?"
        if password is not None:
            sql += ", password_hash = ?"
            params.append(hash_password(password))
        sql += " WHERE id = ?"
        params.append(user_id)
        conn.execute(sql, tuple(params))

    user = get_user_by_id(user_id)
    if not user:
        raise ValueError("user not found")
    return user


def delete_user(user_id: int) -> None:
    with get_app_db() as conn:
        conn.execute("DELETE FROM password_resets WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))


def count_active_admin_users() -> int:
    with get_app_db() as conn:
        row = conn.execute(
            "SELECT COUNT(1) AS cnt FROM users WHERE role = 'admin' AND is_active = 1"
        ).fetchone()
    if not row:
        return 0
    return int(row["cnt"] or 0)


def list_hr_users() -> list[dict[str, Any]]:
    # Backward-compatible helper for legacy endpoints.
    return [user for user in list_users() if user.get("role") == "inspector"]


def create_hr_user(email: str, username: str, password: str) -> dict[str, Any]:
    # Backward-compatible helper for legacy endpoints.
    return create_user(email=email, username=username, password=password, role="inspector")


def set_user_active(user_id: int, is_active: bool) -> None:
    now = utc_now_iso()
    with get_app_db() as conn:
        conn.execute(
            "UPDATE users SET is_active = ?, updated_at = ? WHERE id = ?",
            (1 if is_active else 0, now, user_id),
        )


def set_user_password(user_id: int, new_password: str) -> None:
    now = utc_now_iso()
    with get_app_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
            (hash_password(new_password), now, user_id),
        )


def create_password_reset(user_id: int) -> str:
    raw_token = generate_token(32)
    token_digest = hash_token(raw_token)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=settings.password_reset_expiry_minutes)

    with get_app_db() as conn:
        conn.execute(
            """
            INSERT INTO password_resets (user_id, token_hash, expires_at, used_at, created_at)
            VALUES (?, ?, ?, NULL, ?)
            """,
            (
                user_id,
                token_digest,
                expires.replace(microsecond=0).isoformat(),
                now.replace(microsecond=0).isoformat(),
            ),
        )

    return raw_token


def redeem_password_reset(token: str, new_password: str) -> bool:
    token_digest = hash_token(token.strip())
    now = datetime.now(timezone.utc).replace(microsecond=0)

    with get_app_db() as conn:
        row = conn.execute(
            """
            SELECT pr.id AS reset_id, pr.user_id AS user_id
            FROM password_resets pr
            JOIN users u ON u.id = pr.user_id
            WHERE pr.token_hash = ?
              AND pr.used_at IS NULL
              AND pr.expires_at >= ?
              AND u.is_active = 1
            ORDER BY pr.id DESC
            LIMIT 1
            """,
            (token_digest, now.isoformat()),
        ).fetchone()

        if not row:
            return False

        reset_id = int(row["reset_id"])
        user_id = int(row["user_id"])
        updated_at = utc_now_iso()

        conn.execute(
            "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
            (hash_password(new_password), updated_at, user_id),
        )
        conn.execute(
            "UPDATE password_resets SET used_at = ? WHERE id = ?",
            (updated_at, reset_id),
        )

    return True


def get_smtp_settings(include_password: bool = False) -> dict[str, Any]:
    with get_app_db() as conn:
        row = conn.execute(
            """
            SELECT id, host, port, username, password_encrypted, from_email, from_name,
                   use_tls, use_ssl, cc_list, updated_at
            FROM smtp_settings
            WHERE id = 1
            LIMIT 1
            """
        ).fetchone()

    if not row:
        return {
            "host": "",
            "port": 587,
            "username": "",
            "password": "" if include_password else None,
            "from_email": "",
            "from_name": "Oilchem Entry/Exit Admin",
            "use_tls": True,
            "use_ssl": False,
            "cc_list": "",
            "updated_at": None,
        }

    payload = _dict_from_row(row) or {}
    decrypted_password = decrypt_text(payload.get("password_encrypted") or "", settings.app_encryption_key)

    return {
        "host": (payload.get("host") or "").strip(),
        "port": int(payload.get("port") or 587),
        "username": (payload.get("username") or "").strip(),
        "password": decrypted_password if include_password else None,
        "from_email": (payload.get("from_email") or "").strip(),
        "from_name": (payload.get("from_name") or "Oilchem Entry/Exit Admin").strip() or "Oilchem Entry/Exit Admin",
        "use_tls": bool(payload.get("use_tls")),
        "use_ssl": bool(payload.get("use_ssl")),
        "cc_list": (payload.get("cc_list") or "").strip(),
        "updated_at": payload.get("updated_at"),
    }


def upsert_smtp_settings(data: dict[str, Any]) -> None:
    global _warned_plain_smtp

    host = str(data.get("host") or "").strip()
    port = int(data.get("port") or 0)
    username = str(data.get("username") or "").strip()
    password = str(data.get("password") or "")
    from_email = str(data.get("from_email") or "").strip()
    from_name = str(data.get("from_name") or "Oilchem Entry/Exit Admin").strip() or "Oilchem Entry/Exit Admin"
    use_tls = 1 if data.get("use_tls") else 0
    use_ssl = 1 if data.get("use_ssl") else 0
    cc_list = str(data.get("cc_list") or "").strip()
    updated_at = utc_now_iso()

    if password and not settings.app_encryption_key and not _warned_plain_smtp:
        logger.warning(
            "APP_ENCRYPTION_KEY is not configured. SMTP password will be stored in plaintext."
        )
        _warned_plain_smtp = True

    encrypted_password = encrypt_text(password, settings.app_encryption_key)

    with get_app_db() as conn:
        existing_row = conn.execute(
            "SELECT id, password_encrypted FROM smtp_settings WHERE id = 1"
        ).fetchone()

        if existing_row:
            if not password:
                encrypted_password = str(existing_row["password_encrypted"] or "")
            conn.execute(
                """
                UPDATE smtp_settings
                SET host = ?, port = ?, username = ?, password_encrypted = ?,
                    from_email = ?, from_name = ?, use_tls = ?, use_ssl = ?, cc_list = ?, updated_at = ?
                WHERE id = 1
                """,
                (
                    host,
                    port,
                    username,
                    encrypted_password,
                    from_email,
                    from_name,
                    use_tls,
                    use_ssl,
                    cc_list,
                    updated_at,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO smtp_settings (
                    id, host, port, username, password_encrypted,
                    from_email, from_name, use_tls, use_ssl, cc_list, updated_at
                )
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    host,
                    port,
                    username,
                    encrypted_password,
                    from_email,
                    from_name,
                    use_tls,
                    use_ssl,
                    cc_list,
                    updated_at,
                ),
            )


def get_employee_settings_map(card_nos: Sequence[str]) -> dict[str, dict[str, Any]]:
    normalized = [card.strip() for card in card_nos if card and card.strip()]
    if not normalized:
        return {}

    placeholders = ",".join(["?"] * len(normalized))
    sql = (
        "SELECT id, card_no, emp_id, employee_name_cache, employee_email, work_start_time, work_end_time, "
        "late_grace_minutes, early_grace_minutes, notify_employee, notify_cc_override, updated_at "
        f"FROM employee_settings WHERE card_no IN ({placeholders})"
    )

    with get_app_db() as conn:
        rows = conn.execute(sql, tuple(normalized)).fetchall()

    data: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = _dict_from_row(row)
        if not payload:
            continue
        card_no = str(payload.get("card_no") or "").strip()
        if not card_no:
            continue
        data[card_no] = payload

    return data


def get_employee_setting(card_no: str) -> dict[str, Any] | None:
    with get_app_db() as conn:
        row = conn.execute(
            """
            SELECT id, card_no, emp_id, employee_name_cache, employee_email, work_start_time, work_end_time,
                   late_grace_minutes, early_grace_minutes, notify_employee, notify_cc_override, updated_at
            FROM employee_settings
            WHERE card_no = ?
            LIMIT 1
            """,
            (card_no.strip(),),
        ).fetchone()

    return _dict_from_row(row)


def upsert_employee_setting(data: dict[str, Any]) -> dict[str, Any]:
    card_no = str(data.get("card_no") or "").strip()
    if not card_no:
        raise ValueError("card_no is required")

    emp_id = data.get("emp_id")
    employee_name_cache = str(data.get("employee_name_cache") or "").strip() or None
    employee_email = str(data.get("employee_email") or "").strip() or None
    work_start_time = str(data.get("work_start_time") or "09:00").strip() or "09:00"
    work_end_time = str(data.get("work_end_time") or "18:00").strip() or "18:00"
    late_grace_minutes = max(0, int(data.get("late_grace_minutes") or 0))
    early_grace_minutes = max(0, int(data.get("early_grace_minutes") or 0))
    notify_employee = 1 if data.get("notify_employee") else 0
    notify_cc_override = str(data.get("notify_cc_override") or "").strip() or None
    updated_at = utc_now_iso()

    with get_app_db() as conn:
        existing = conn.execute(
            "SELECT id FROM employee_settings WHERE card_no = ? LIMIT 1",
            (card_no,),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE employee_settings
                SET emp_id = ?, employee_name_cache = ?, employee_email = ?,
                    work_start_time = ?, work_end_time = ?,
                    late_grace_minutes = ?, early_grace_minutes = ?,
                    notify_employee = ?, notify_cc_override = ?, updated_at = ?
                WHERE card_no = ?
                """,
                (
                    str(emp_id).strip() if emp_id is not None and str(emp_id).strip() else None,
                    employee_name_cache,
                    employee_email,
                    work_start_time,
                    work_end_time,
                    late_grace_minutes,
                    early_grace_minutes,
                    notify_employee,
                    notify_cc_override,
                    updated_at,
                    card_no,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO employee_settings (
                    card_no, emp_id, employee_name_cache, employee_email,
                    work_start_time, work_end_time,
                    late_grace_minutes, early_grace_minutes,
                    notify_employee, notify_cc_override, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    card_no,
                    str(emp_id).strip() if emp_id is not None and str(emp_id).strip() else None,
                    employee_name_cache,
                    employee_email,
                    work_start_time,
                    work_end_time,
                    late_grace_minutes,
                    early_grace_minutes,
                    notify_employee,
                    notify_cc_override,
                    updated_at,
                ),
            )

    setting = get_employee_setting(card_no)
    if not setting:
        raise ValueError("failed to save employee settings")
    return setting


def list_notification_targets(card_no: str | None = None) -> list[dict[str, Any]]:
    with get_app_db() as conn:
        if card_no:
            rows = conn.execute(
                """
                SELECT id, card_no, emp_id, employee_name_cache, employee_email, work_start_time, work_end_time,
                       late_grace_minutes, early_grace_minutes, notify_employee, notify_cc_override, updated_at
                FROM employee_settings
                WHERE card_no = ?
                LIMIT 1
                """,
                (card_no.strip(),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, card_no, emp_id, employee_name_cache, employee_email, work_start_time, work_end_time,
                       late_grace_minutes, early_grace_minutes, notify_employee, notify_cc_override, updated_at
                FROM employee_settings
                WHERE notify_employee = 1
                  AND employee_email IS NOT NULL
                  AND trim(employee_email) <> ''
                ORDER BY card_no
                """
            ).fetchall()

    return [_dict_from_row(row) for row in rows if row is not None]


def insert_notification_log(
    *,
    card_no: str,
    date_value: str,
    notice_type: str,
    to_email: str,
    cc: str,
    status: str,
    error: str | None,
) -> None:
    with get_app_db() as conn:
        conn.execute(
            """
            INSERT INTO notifications_log (card_no, date, type, to_email, cc, sent_at, status, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card_no,
                date_value,
                notice_type,
                to_email,
                cc,
                utc_now_iso(),
                status,
                error,
            ),
        )


def list_notification_logs(limit: int = 100) -> list[dict[str, Any]]:
    with get_app_db() as conn:
        rows = conn.execute(
            """
            SELECT id, card_no, date, type, to_email, cc, sent_at, status, error
            FROM notifications_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, min(limit, 500)),),
        ).fetchall()

    return [_dict_from_row(row) for row in rows if row is not None]
