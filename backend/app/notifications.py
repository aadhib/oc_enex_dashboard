from __future__ import annotations

import smtplib
from datetime import date, datetime, time, timedelta
from email.message import EmailMessage
from typing import Any

from fastapi import HTTPException, status

from .app_db import (
    get_employee_setting,
    get_smtp_settings,
    insert_notification_log,
    list_notification_targets,
)
from .config import settings
from .reports import fetch_daily_report


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="date must be in YYYY-MM-DD format",
        ) from exc


def _parse_hhmm(value: str | None, fallback: str) -> time:
    raw = (value or fallback).strip()
    try:
        return datetime.strptime(raw, "%H:%M").time()
    except ValueError:
        return datetime.strptime(fallback, "%H:%M").time()


def _parse_report_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None

    patterns = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    )
    for pattern in patterns:
        try:
            return datetime.strptime(raw, pattern)
        except ValueError:
            continue
    return None


def _format_dt_12h(value: datetime | None) -> str:
    if not value:
        return "N/A"
    return value.strftime("%Y-%m-%d %I:%M:%S %p")


def _format_time_12h(value: time) -> str:
    sample = datetime.combine(datetime(2000, 1, 1).date(), value)
    return sample.strftime("%I:%M %p")


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [item.strip() for item in value.split(",") if item.strip()]
    # Deduplicate while preserving order
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        lowered = part.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(part)
    return ordered


def _build_shift_window(
    base_date: date,
    start_hhmm: str | None,
    end_hhmm: str | None,
) -> tuple[datetime, datetime]:
    start_time = _parse_hhmm(start_hhmm, "09:00")
    end_time = _parse_hhmm(end_hhmm, "18:00")

    shift_start = datetime.combine(base_date, start_time)
    shift_end = datetime.combine(base_date, end_time)
    if shift_end <= shift_start:
        shift_end = shift_end + timedelta(days=1)

    return shift_start, shift_end


def _build_status(
    first_in: datetime | None,
    last_out: datetime | None,
    shift_start: datetime,
    shift_end: datetime,
    late_grace_minutes: int,
    early_grace_minutes: int,
) -> tuple[str | None, str]:
    if not first_in or not last_out:
        return "MISSING_PUNCH", "MISSING PUNCH"

    late_limit = shift_start + timedelta(minutes=max(0, late_grace_minutes))
    early_limit = shift_end - timedelta(minutes=max(0, early_grace_minutes))

    is_late = first_in > late_limit
    is_early = last_out < early_limit

    if is_late and is_early:
        return "LATE_EARLY", "LATE / EARLY"
    if is_late:
        return "LATE", "LATE"
    if is_early:
        return "EARLY", "EARLY"

    return None, "OK"


def _subject_for_type(notice_type: str, date_value: str) -> str:
    if notice_type == "LATE":
        return f"Entry/Exit Notice: Late Check-in - {date_value}"
    if notice_type == "EARLY":
        return f"Entry/Exit Notice: Early Check-out - {date_value}"
    if notice_type == "LATE_EARLY":
        return f"Entry/Exit Notice: Late Check-in / Early Check-out - {date_value}"
    return f"Entry/Exit Notice: Missing Punch - {date_value}"


def _send_email(
    *,
    smtp_config: dict[str, Any],
    to_email: str,
    cc_list: list[str],
    subject: str,
    body: str,
) -> None:
    host = str(smtp_config.get("host") or "").strip()
    port = int(smtp_config.get("port") or 0)
    username = str(smtp_config.get("username") or "").strip()
    password = str(smtp_config.get("password") or "")
    from_email = str(smtp_config.get("from_email") or "").strip()
    from_name = str(smtp_config.get("from_name") or "Oilchem Entry/Exit Admin").strip() or "Oilchem Entry/Exit Admin"
    use_tls = bool(smtp_config.get("use_tls"))
    use_ssl = bool(smtp_config.get("use_ssl"))

    if not host or not port or not from_email:
        raise RuntimeError("SMTP settings are incomplete")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = f"{from_name} <{from_email}>"
    message["To"] = to_email
    if cc_list:
        message["Cc"] = ", ".join(cc_list)
    message.set_content(body)

    recipients = [to_email] + cc_list

    if use_ssl:
        server = smtplib.SMTP_SSL(host, port, timeout=settings.smtp_timeout_seconds)
    else:
        server = smtplib.SMTP(host, port, timeout=settings.smtp_timeout_seconds)

    try:
        if not use_ssl and use_tls:
            server.starttls()
        if username:
            server.login(username, password)
        server.send_message(message, to_addrs=recipients)
    finally:
        server.quit()


def run_notifications(date_value: str, card_no: str | None = None) -> dict[str, Any]:
    target_date = _parse_date(date_value)
    smtp_config = get_smtp_settings(include_password=True)
    default_cc = _split_csv(smtp_config.get("cc_list"))

    if card_no:
        setting = get_employee_setting(card_no)
        targets = [setting] if setting else []
    else:
        targets = list_notification_targets(None)

    results: list[dict[str, Any]] = []
    sent_count = 0
    skipped_count = 0
    failed_count = 0

    if card_no and not targets:
        targets = [
            {
                "card_no": card_no,
                "employee_email": "",
                "work_start_time": "09:00",
                "work_end_time": "18:00",
                "late_grace_minutes": 0,
                "early_grace_minutes": 0,
                "notify_cc_override": "",
            }
        ]

    for target in targets:
        card = str(target.get("card_no") or "").strip()
        if not card:
            continue

        report = fetch_daily_report(card_no=card, date_value=date_value)
        employee_name = str(report.get("employee_name") or target.get("employee_name_cache") or card)
        to_email = str(target.get("employee_email") or "").strip()

        first_in = _parse_report_datetime(report.get("first_in"))
        last_out = _parse_report_datetime(report.get("last_out"))

        shift_start, shift_end = _build_shift_window(
            base_date=target_date,
            start_hhmm=str(target.get("work_start_time") or "09:00"),
            end_hhmm=str(target.get("work_end_time") or "18:00"),
        )

        notice_type, status_label = _build_status(
            first_in=first_in,
            last_out=last_out,
            shift_start=shift_start,
            shift_end=shift_end,
            late_grace_minutes=int(target.get("late_grace_minutes") or 0),
            early_grace_minutes=int(target.get("early_grace_minutes") or 0),
        )

        if not notice_type:
            skipped_count += 1
            results.append(
                {
                    "card_no": card,
                    "employee_name": employee_name,
                    "status": "SKIPPED",
                    "notice_type": None,
                    "to_email": to_email or None,
                    "error": None,
                }
            )
            continue

        if not to_email:
            failed_count += 1
            error = "Missing employee email in settings"
            insert_notification_log(
                card_no=card,
                date_value=date_value,
                notice_type=notice_type,
                to_email="",
                cc=", ".join(default_cc),
                status="FAILED",
                error=error,
            )
            results.append(
                {
                    "card_no": card,
                    "employee_name": employee_name,
                    "status": "FAILED",
                    "notice_type": notice_type,
                    "to_email": None,
                    "error": error,
                }
            )
            continue

        override_cc = _split_csv(str(target.get("notify_cc_override") or ""))
        cc_list = _split_csv(",".join(default_cc + override_cc))

        subject = _subject_for_type(notice_type, date_value)
        body = "\n".join(
            [
                "Oilchem Entry/Exit Notice",
                "",
                f"Employee: {employee_name}",
                f"CardNo: {card}",
                f"Date: {date_value}",
                f"First IN: {_format_dt_12h(first_in)}",
                f"Last OUT: {_format_dt_12h(last_out)}",
                f"Scheduled Start: {_format_time_12h(shift_start.time())}",
                f"Scheduled End: {_format_time_12h(shift_end.time())}",
                f"Status: {status_label}",
            ]
        )

        try:
            _send_email(
                smtp_config=smtp_config,
                to_email=to_email,
                cc_list=cc_list,
                subject=subject,
                body=body,
            )
            sent_count += 1
            insert_notification_log(
                card_no=card,
                date_value=date_value,
                notice_type=notice_type,
                to_email=to_email,
                cc=", ".join(cc_list),
                status="SENT",
                error=None,
            )
            results.append(
                {
                    "card_no": card,
                    "employee_name": employee_name,
                    "status": "SENT",
                    "notice_type": notice_type,
                    "to_email": to_email,
                    "error": None,
                }
            )
        except Exception as exc:
            failed_count += 1
            insert_notification_log(
                card_no=card,
                date_value=date_value,
                notice_type=notice_type,
                to_email=to_email,
                cc=", ".join(cc_list),
                status="FAILED",
                error=str(exc),
            )
            results.append(
                {
                    "card_no": card,
                    "employee_name": employee_name,
                    "status": "FAILED",
                    "notice_type": notice_type,
                    "to_email": to_email,
                    "error": str(exc),
                }
            )

    total_targets = len(targets)
    return {
        "date": date_value,
        "total_targets": total_targets,
        "sent_count": sent_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
        "results": results,
    }
