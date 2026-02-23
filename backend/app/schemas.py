from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=256)


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    role: str
    username: str
    email: str


class AuthMeResponse(BaseModel):
    id: int
    role: str
    username: str
    email: str


class EmployeeItem(BaseModel):
    emp_id: int | str
    card_no: str
    employee_name: str


class EmployeesResponse(BaseModel):
    employees: List[EmployeeItem]


class DashboardSummaryResponse(BaseModel):
    totalEmployees: int
    inCount: int
    outCount: int
    unknownCount: Optional[int] = None
    generatedAt: str


class DailyReport(BaseModel):
    employee_name: str
    card_no: str
    department: Optional[str]
    date: str
    first_in: Optional[str]
    last_out: Optional[str]
    duration_minutes: Optional[int]
    duration_hhmm: Optional[str]
    duration: Optional[str] = None
    missing_punch: bool = False
    rows: List[dict[str, Any]] = Field(default_factory=list)
    transactions: List[dict[str, Any]] = Field(default_factory=list)
    intervals: List[dict[str, Any]] = Field(default_factory=list)
    total_in_minutes: Optional[int] = None
    total_out_minutes: Optional[int] = None
    total_in: Optional[str] = None
    total_out: Optional[str] = None
    totalInMinutes: Optional[int] = None
    totalOutMinutes: Optional[int] = None
    totalInHHMM: Optional[str] = None
    totalOutHHMM: Optional[str] = None
    notes: List[str] = Field(default_factory=list)
    total_work_minutes: Optional[int] = None
    mappingVariant: str
    swapApplied: bool


class MonthlyDailyRecord(BaseModel):
    date: str
    first_in: Optional[str]
    last_out: Optional[str]
    duration_minutes: Optional[int]
    duration_hhmm: Optional[str]
    missing_punch: bool = False


class MonthlyReport(BaseModel):
    employee_name: str
    card_no: str
    department: Optional[str]
    month: str
    records: List[MonthlyDailyRecord]
    total_days: int
    missing_punch_days: int = 0
    total_minutes: int
    total_duration_hhmm: Optional[str]
    total_duration_readable: Optional[str]
    totalInMinutes: Optional[int] = None
    totalOutMinutes: Optional[int] = None
    totalInHHMM: Optional[str] = None
    totalOutHHMM: Optional[str] = None
    total_work_minutes: Optional[int] = None
    mappingVariant: str
    swapApplied: bool


class YearlyMonthRecord(BaseModel):
    month: str
    worked_days: int
    missing_punch_days: int = 0
    total_minutes: int
    average_minutes_per_day: Optional[int]
    average_duration_hhmm: Optional[str]
    total_duration_hhmm: Optional[str]
    total_duration_readable: Optional[str]


class YearlyReport(BaseModel):
    employee_name: str
    card_no: str
    department: Optional[str]
    year: str
    months: List[YearlyMonthRecord]
    total_worked_days: int
    missing_punch_days: int = 0
    total_minutes: int
    total_duration_hhmm: Optional[str]
    total_duration_readable: Optional[str]
    totalInMinutes: Optional[int] = None
    totalOutMinutes: Optional[int] = None
    totalInHHMM: Optional[str] = None
    totalOutHHMM: Optional[str] = None
    total_work_minutes: Optional[int] = None
    mappingVariant: str
    swapApplied: bool


class SMTPSettingsRequest(BaseModel):
    host: str = Field(default="", max_length=255)
    port: int = Field(default=587, ge=1, le=65535)
    username: str = Field(default="", max_length=255)
    password: str = Field(default="", max_length=512)
    from_email: str = Field(default="", max_length=255)
    from_name: str = Field(default="Oilchem Attendance Admin", max_length=255)
    use_tls: bool = True
    use_ssl: bool = False
    cc_list: str = Field(default="", max_length=2000)


class SMTPSettingsResponse(BaseModel):
    host: str
    port: int
    username: str
    from_email: str
    from_name: str
    use_tls: bool
    use_ssl: bool
    cc_list: str
    updated_at: Optional[str]


class HRUserItem(BaseModel):
    id: int
    email: str
    username: str
    role: str
    is_active: bool
    created_at: str
    updated_at: str
    last_login_at: Optional[str]


class HRUsersResponse(BaseModel):
    users: List[HRUserItem]


class CreateHRUserRequest(BaseModel):
    email: str = Field(min_length=5, max_length=255)
    username: str = Field(min_length=1, max_length=128)
    temp_password: str = Field(min_length=8, max_length=256)


class UpdateUserActiveRequest(BaseModel):
    is_active: bool


class SetTempPasswordRequest(BaseModel):
    temp_password: str = Field(min_length=8, max_length=256)


class ResetLinkResponse(BaseModel):
    reset_token: str
    reset_url: str
    expires_in_minutes: int


class ResetPasswordRequest(BaseModel):
    token: str = Field(min_length=8, max_length=512)
    new_password: str = Field(min_length=8, max_length=256)


class EmployeeSettingItem(BaseModel):
    emp_id: int | str
    card_no: str
    employee_name: str
    employee_email: str
    work_start_time: str
    work_end_time: str
    late_grace_minutes: int
    early_grace_minutes: int
    notify_employee: bool
    notify_cc_override: str
    updated_at: Optional[str]


class EmployeeSettingsResponse(BaseModel):
    employees: List[EmployeeSettingItem]


class EmployeeSettingUpsertRequest(BaseModel):
    emp_id: Optional[int | str] = None
    employee_name: Optional[str] = None
    employee_email: str = Field(default="", max_length=255)
    work_start_time: str = Field(default="09:00", pattern=r"^\d{2}:\d{2}$")
    work_end_time: str = Field(default="18:00", pattern=r"^\d{2}:\d{2}$")
    late_grace_minutes: int = Field(default=0, ge=0, le=360)
    early_grace_minutes: int = Field(default=0, ge=0, le=360)
    notify_employee: bool = False
    notify_cc_override: str = Field(default="", max_length=2000)


class NotificationRunItem(BaseModel):
    card_no: str
    employee_name: str
    status: str
    notice_type: Optional[str]
    to_email: Optional[str]
    error: Optional[str]


class NotificationRunResponse(BaseModel):
    date: str
    total_targets: int
    sent_count: int
    skipped_count: int
    failed_count: int
    results: List[NotificationRunItem]


class NotificationLogItem(BaseModel):
    id: int
    card_no: str
    date: str
    type: str
    to_email: Optional[str]
    cc: Optional[str]
    sent_at: str
    status: str
    error: Optional[str]


class NotificationLogsResponse(BaseModel):
    logs: List[NotificationLogItem]
