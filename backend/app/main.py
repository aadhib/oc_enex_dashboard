from __future__ import annotations

import re
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from .app_db import (
    create_hr_user,
    create_password_reset,
    get_employee_settings_map,
    get_smtp_settings,
    get_user_by_id,
    get_user_by_login,
    init_app_db,
    list_hr_users,
    list_notification_logs,
    set_user_active,
    set_user_password,
    touch_user_login,
    upsert_employee_setting,
    upsert_smtp_settings,
    redeem_password_reset,
)
from .auth import (
    TOKEN_COOKIE_NAME,
    AuthUser,
    create_access_token,
    get_current_user,
    require_admin,
    require_hr_or_admin,
)
from .config import settings
from .db import (
    DBOperationalError,
    get_db_connection_error_payload,
    get_cursor,
    log_db_connection_target_once,
    validate_db_server_for_startup,
)
from .notifications import run_notifications
from .pdf_exports import build_daily_pdf, build_monthly_pdf, build_yearly_pdf
from .rate_limit import build_rate_limit_middleware
from .reports import (
    fetch_dashboard_summary,
    fetch_daily_report,
    fetch_employees,
    fetch_monthly_report,
    fetch_yearly_report,
)
from .schemas import (
    AuthMeResponse,
    AuthResponse,
    CreateHRUserRequest,
    DashboardSummaryResponse,
    DailyReport,
    EmployeeSettingItem,
    EmployeeSettingsResponse,
    EmployeeSettingUpsertRequest,
    EmployeesResponse,
    HRUserItem,
    HRUsersResponse,
    LoginRequest,
    MonthlyReport,
    NotificationLogsResponse,
    NotificationRunResponse,
    ResetLinkResponse,
    ResetPasswordRequest,
    SMTPSettingsRequest,
    SMTPSettingsResponse,
    SetTempPasswordRequest,
    UpdateUserActiveRequest,
    YearlyReport,
)
from .security import verify_password

_BACKEND_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

app = FastAPI(title="Oilchem HR Admin API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "OPTIONS"],
    allow_headers=["*"],
)

app.middleware("http")(
    build_rate_limit_middleware(
        window_seconds=settings.rate_limit_window_sec,
        max_requests=settings.rate_limit_max_requests,
    )
)


@app.on_event("startup")
def startup_event() -> None:
    load_dotenv(dotenv_path=_BACKEND_ENV_PATH, override=False)
    validate_db_server_for_startup()
    log_db_connection_target_once()
    init_app_db()


def _db_connection_failed_response() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=get_db_connection_error_payload(),
    )


def _build_inline_pdf_response(filename: str, payload: bytes) -> Response:
    response = Response(content=payload, media_type="application/pdf")
    response.headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return response


def _sanitize_filename_part(value: str | None) -> str:
    if value is None:
        return "Unknown"

    text = value.strip().replace(" ", "-")
    text = re.sub(r"[^A-Za-z0-9\-_]", "", text)
    text = re.sub(r"-{2,}", "-", text)
    text = text.strip("-_")
    if not text:
        text = "Unknown"
    return text[:40]


def _build_pdf_filename(prefix: str, employee_name: str, card_no: str, period: str) -> str:
    safe_name = _sanitize_filename_part(employee_name)
    safe_card = _sanitize_filename_part(card_no)
    safe_period = _sanitize_filename_part(period)
    return f"{prefix}_{safe_name}_{safe_card}_{safe_period}.pdf"


def _serialize_hr_user(row: dict[str, Any]) -> HRUserItem:
    return HRUserItem(
        id=int(row["id"]),
        email=str(row.get("email") or ""),
        username=str(row.get("username") or ""),
        role=str(row.get("role") or "hr"),
        is_active=bool(row.get("is_active")),
        created_at=str(row.get("created_at") or ""),
        updated_at=str(row.get("updated_at") or ""),
        last_login_at=(str(row.get("last_login_at")) if row.get("last_login_at") else None),
    )


def _smtp_is_ready(config: dict[str, Any]) -> bool:
    return bool(str(config.get("host") or "").strip() and str(config.get("from_email") or "").strip())


def _send_reset_email(to_email: str, username: str, reset_url: str) -> None:
    smtp_config = get_smtp_settings(include_password=True)
    if not _smtp_is_ready(smtp_config):
        return

    host = str(smtp_config.get("host") or "").strip()
    port = int(smtp_config.get("port") or 0)
    from_email = str(smtp_config.get("from_email") or "").strip()
    from_name = str(smtp_config.get("from_name") or "Oilchem HR Admin").strip() or "Oilchem HR Admin"
    smtp_username = str(smtp_config.get("username") or "").strip()
    smtp_password = str(smtp_config.get("password") or "")
    use_tls = bool(smtp_config.get("use_tls"))
    use_ssl = bool(smtp_config.get("use_ssl"))

    message = EmailMessage()
    message["Subject"] = "Oilchem HR Admin: Password Reset Link"
    message["From"] = f"{from_name} <{from_email}>"
    message["To"] = to_email
    message.set_content(
        "\n".join(
            [
                "Hello,",
                "",
                f"A password reset was requested for your HR user ({username}).",
                f"Reset link: {reset_url}",
                f"This link expires in {settings.password_reset_expiry_minutes} minutes.",
            ]
        )
    )

    if use_ssl:
        server = smtplib.SMTP_SSL(host, port, timeout=settings.smtp_timeout_seconds)
    else:
        server = smtplib.SMTP(host, port, timeout=settings.smtp_timeout_seconds)

    try:
        if not use_ssl and use_tls:
            server.starttls()
        if smtp_username:
            server.login(smtp_username, smtp_password)
        server.send_message(message)
    finally:
        server.quit()


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/health/db")
def db_healthcheck() -> dict[str, bool]:
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT 1 AS ok")
            row = cursor.fetchone() or {}
    except DBOperationalError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                **get_db_connection_error_payload(),
                "reason": str(exc),
            },
        ) from exc

    ok_value = row.get("ok")
    if ok_value is None:
        ok_value = row.get("OK")

    if isinstance(ok_value, bool):
        return {"ok": ok_value}
    if isinstance(ok_value, int):
        return {"ok": ok_value == 1}
    if ok_value is None:
        return {"ok": False}
    return {"ok": str(ok_value).strip() in {"1", "true", "TRUE", "True"}}


@app.post("/api/auth/login", response_model=AuthResponse)
def login(request: LoginRequest, response: Response) -> AuthResponse:
    login_id = request.username.strip()
    user = get_user_by_login(login_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username/email or password",
        )

    if not bool(user.get("is_active")):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is disabled",
        )

    if not verify_password(request.password, str(user.get("password_hash") or "")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username/email or password",
        )

    role = str(user.get("role") or "")
    if role not in {"admin", "hr"}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Role is not allowed",
        )

    auth_user: AuthUser = {
        "id": int(user["id"]),
        "email": str(user.get("email") or ""),
        "username": str(user.get("username") or ""),
        "role": role,
    }

    token, expires_in = create_access_token(auth_user)
    touch_user_login(auth_user["id"])

    response.set_cookie(
        key=TOKEN_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        domain=settings.cookie_domain,
        max_age=expires_in,
        path="/",
    )

    return AuthResponse(
        access_token=token,
        expires_in=expires_in,
        role=auth_user["role"],
        username=auth_user["username"],
        email=auth_user["email"],
    )


@app.post("/api/auth/logout")
def logout(response: Response) -> dict[str, str]:
    response.delete_cookie(
        key=TOKEN_COOKIE_NAME,
        domain=settings.cookie_domain,
        path="/",
    )
    return {"message": "Logged out"}


@app.get("/api/auth/me", response_model=AuthMeResponse)
def auth_me(user: AuthUser = Depends(get_current_user)) -> AuthMeResponse:
    return AuthMeResponse(
        id=user["id"],
        role=user["role"],
        username=user["username"],
        email=user["email"],
    )


@app.post("/api/auth/reset-password")
def redeem_reset_password(payload: ResetPasswordRequest) -> dict[str, str]:
    ok = redeem_password_reset(payload.token, payload.new_password)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token",
        )

    return {"message": "Password updated"}


@app.get("/api/admin/smtp-settings", response_model=SMTPSettingsResponse)
def get_admin_smtp_settings(_user: AuthUser = Depends(require_admin)) -> SMTPSettingsResponse:
    data = get_smtp_settings(include_password=False)
    return SMTPSettingsResponse(**data)


@app.put("/api/admin/smtp-settings", response_model=SMTPSettingsResponse)
def save_admin_smtp_settings(
    request: SMTPSettingsRequest,
    _user: AuthUser = Depends(require_admin),
) -> SMTPSettingsResponse:
    upsert_smtp_settings(request.model_dump())
    data = get_smtp_settings(include_password=False)
    return SMTPSettingsResponse(**data)


@app.get("/api/admin/hr-users", response_model=HRUsersResponse)
def get_hr_users(_user: AuthUser = Depends(require_admin)) -> HRUsersResponse:
    users = [_serialize_hr_user(row) for row in list_hr_users()]
    return HRUsersResponse(users=users)


@app.post("/api/admin/hr-users", response_model=HRUserItem)
def add_hr_user(
    payload: CreateHRUserRequest,
    _user: AuthUser = Depends(require_admin),
) -> HRUserItem:
    if "@" not in payload.email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="email is invalid",
        )

    try:
        user = create_hr_user(
            email=payload.email,
            username=payload.username,
            password=payload.temp_password,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return _serialize_hr_user(user)


@app.patch("/api/admin/hr-users/{user_id}/active", response_model=HRUserItem)
def set_hr_user_active(
    user_id: int,
    payload: UpdateUserActiveRequest,
    _user: AuthUser = Depends(require_admin),
) -> HRUserItem:
    target = get_user_by_id(user_id)
    if not target or str(target.get("role")) != "hr":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="HR user not found")

    set_user_active(user_id=user_id, is_active=payload.is_active)
    updated = get_user_by_id(user_id)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="HR user not found")

    return _serialize_hr_user(updated)


@app.post("/api/admin/hr-users/{user_id}/set-password")
def admin_set_hr_password(
    user_id: int,
    payload: SetTempPasswordRequest,
    _user: AuthUser = Depends(require_admin),
) -> dict[str, str]:
    target = get_user_by_id(user_id)
    if not target or str(target.get("role")) != "hr":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="HR user not found")

    set_user_password(user_id, payload.temp_password)
    return {"message": "Password updated"}


@app.post("/api/admin/hr-users/{user_id}/reset-link", response_model=ResetLinkResponse)
def generate_hr_reset_link(
    user_id: int,
    _user: AuthUser = Depends(require_admin),
) -> ResetLinkResponse:
    target = get_user_by_id(user_id)
    if not target or str(target.get("role")) != "hr":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="HR user not found")

    token = create_password_reset(user_id)
    reset_url = f"{settings.frontend_base_url}/reset-password?token={token}"

    email = str(target.get("email") or "").strip()
    username = str(target.get("username") or "hr")
    if email:
        try:
            _send_reset_email(email, username, reset_url)
        except Exception:
            # Do not fail reset token generation if SMTP delivery fails.
            pass

    return ResetLinkResponse(
        reset_token=token,
        reset_url=reset_url,
        expires_in_minutes=settings.password_reset_expiry_minutes,
    )


@app.get("/api/employees", response_model=EmployeesResponse)
def list_employees(
    search: str = Query(default="", max_length=64),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> EmployeesResponse:
    try:
        employees = fetch_employees(search.strip())
    except DBOperationalError:
        return _db_connection_failed_response()
    return EmployeesResponse(employees=employees)


@app.get("/api/dashboard/summary", response_model=DashboardSummaryResponse)
def dashboard_summary(_user: AuthUser = Depends(require_hr_or_admin)) -> DashboardSummaryResponse:
    try:
        payload = fetch_dashboard_summary()
    except DBOperationalError:
        return _db_connection_failed_response()
    return DashboardSummaryResponse(**payload)


@app.get("/api/employee-settings", response_model=EmployeeSettingsResponse)
def list_employee_settings(
    search: str = Query(default="", max_length=64),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> EmployeeSettingsResponse:
    try:
        employees = fetch_employees(search.strip())
    except DBOperationalError:
        return _db_connection_failed_response()
    cards = [str(employee.get("card_no") or "").strip() for employee in employees]
    settings_map = get_employee_settings_map(cards)

    payload: list[EmployeeSettingItem] = []
    for employee in employees:
        card_no = str(employee.get("card_no") or "").strip()
        db_setting = settings_map.get(card_no, {})

        payload.append(
            EmployeeSettingItem(
                emp_id=employee.get("emp_id") or card_no,
                card_no=card_no,
                employee_name=str(employee.get("employee_name") or card_no),
                employee_email=str(db_setting.get("employee_email") or ""),
                work_start_time=str(db_setting.get("work_start_time") or "09:00"),
                work_end_time=str(db_setting.get("work_end_time") or "18:00"),
                late_grace_minutes=int(db_setting.get("late_grace_minutes") or 0),
                early_grace_minutes=int(db_setting.get("early_grace_minutes") or 0),
                notify_employee=bool(db_setting.get("notify_employee")),
                notify_cc_override=str(db_setting.get("notify_cc_override") or ""),
                updated_at=(str(db_setting.get("updated_at")) if db_setting.get("updated_at") else None),
            )
        )

    return EmployeeSettingsResponse(employees=payload)


@app.put("/api/employee-settings/{card_no}", response_model=EmployeeSettingItem)
def save_employee_setting(
    card_no: str,
    payload: EmployeeSettingUpsertRequest,
    _user: AuthUser = Depends(require_hr_or_admin),
) -> EmployeeSettingItem:
    card = card_no.strip()
    if not card:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="card_no is required")

    upsert_payload = payload.model_dump()
    upsert_payload["card_no"] = card
    upsert_payload["employee_name_cache"] = (payload.employee_name or "").strip() or None

    try:
        saved = upsert_employee_setting(upsert_payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return EmployeeSettingItem(
        emp_id=saved.get("emp_id") or card,
        card_no=card,
        employee_name=str(payload.employee_name or saved.get("employee_name_cache") or card),
        employee_email=str(saved.get("employee_email") or ""),
        work_start_time=str(saved.get("work_start_time") or "09:00"),
        work_end_time=str(saved.get("work_end_time") or "18:00"),
        late_grace_minutes=int(saved.get("late_grace_minutes") or 0),
        early_grace_minutes=int(saved.get("early_grace_minutes") or 0),
        notify_employee=bool(saved.get("notify_employee")),
        notify_cc_override=str(saved.get("notify_cc_override") or ""),
        updated_at=(str(saved.get("updated_at")) if saved.get("updated_at") else None),
    )


@app.post("/api/notifications/run", response_model=NotificationRunResponse)
def trigger_notifications(
    date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    card_no: str | None = Query(default=None, max_length=64),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> NotificationRunResponse:
    payload = run_notifications(date_value=date, card_no=(card_no.strip() if card_no else None))
    return NotificationRunResponse(**payload)


@app.get("/api/notifications/logs", response_model=NotificationLogsResponse)
def get_notification_logs(
    limit: int = Query(default=100, ge=1, le=500),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> NotificationLogsResponse:
    logs = list_notification_logs(limit=limit)
    return NotificationLogsResponse(logs=logs)


@app.get("/api/reports/daily", response_model=DailyReport)
def get_daily_report(
    card_no: str = Query(..., min_length=1, max_length=64),
    date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> DailyReport:
    try:
        payload = fetch_daily_report(card_no=card_no.strip(), date_value=date)
    except DBOperationalError:
        return _db_connection_failed_response()
    return DailyReport(**payload)


@app.get("/api/reports/monthly", response_model=MonthlyReport)
def get_monthly_report(
    card_no: str = Query(..., min_length=1, max_length=64),
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> MonthlyReport:
    try:
        payload = fetch_monthly_report(card_no=card_no.strip(), month_value=month)
    except DBOperationalError:
        return _db_connection_failed_response()
    return MonthlyReport(**payload)


@app.get("/api/reports/yearly", response_model=YearlyReport)
def get_yearly_report(
    card_no: str = Query(..., min_length=1, max_length=64),
    year: str = Query(..., pattern=r"^\d{4}$"),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> YearlyReport:
    try:
        payload = fetch_yearly_report(card_no=card_no.strip(), year_value=year)
    except DBOperationalError:
        return _db_connection_failed_response()
    return YearlyReport(**payload)


@app.get("/api/export/daily.pdf")
def export_daily_pdf(
    card_no: str = Query(..., min_length=1, max_length=64),
    date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> Response:
    try:
        report = fetch_daily_report(card_no=card_no.strip(), date_value=date)
    except DBOperationalError:
        return _db_connection_failed_response()
    payload = build_daily_pdf(report)
    filename = _build_pdf_filename(
        prefix="OC_Att_D",
        employee_name=str(report.get("employee_name") or ""),
        card_no=str(report.get("card_no") or ""),
        period=str(report.get("date") or date),
    )
    return _build_inline_pdf_response(filename=filename, payload=payload)


@app.get("/api/export/monthly.pdf")
def export_monthly_pdf(
    card_no: str = Query(..., min_length=1, max_length=64),
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> Response:
    try:
        report = fetch_monthly_report(card_no=card_no.strip(), month_value=month)
    except DBOperationalError:
        return _db_connection_failed_response()
    payload = build_monthly_pdf(report)
    filename = _build_pdf_filename(
        prefix="OC_Att_M",
        employee_name=str(report.get("employee_name") or ""),
        card_no=str(report.get("card_no") or ""),
        period=str(report.get("month") or month),
    )
    return _build_inline_pdf_response(filename=filename, payload=payload)


@app.get("/api/export/yearly.pdf")
def export_yearly_pdf(
    card_no: str = Query(..., min_length=1, max_length=64),
    year: str = Query(..., pattern=r"^\d{4}$"),
    _user: AuthUser = Depends(require_hr_or_admin),
) -> Response:
    try:
        report = fetch_yearly_report(card_no=card_no.strip(), year_value=year)
    except DBOperationalError:
        return _db_connection_failed_response()
    payload = build_yearly_pdf(report)
    filename = _build_pdf_filename(
        prefix="OC_Att_Y",
        employee_name=str(report.get("employee_name") or ""),
        card_no=str(report.get("card_no") or ""),
        period=str(report.get("year") or year),
    )
    return _build_inline_pdf_response(filename=filename, payload=payload)
