from collections import defaultdict
from datetime import date, datetime, timedelta
import logging
from threading import Lock
import time
from typing import Any, Dict, List, Sequence

from fastapi import HTTPException, status

from .config import settings
from .db import get_cursor

_SCHEMA_CACHE: dict[str, Any] | None = None
_SCHEMA_LOCK = Lock()

_MAPPING_CACHE: dict[str, Any] | None = None
_MAPPING_LOCK = Lock()
_MAPPING_CACHE_TTL_SECONDS = 300
_EMPLOYEE_SAMPLE_LOGGED = False
logger = logging.getLogger(__name__)

_NAME_COLUMN_CANDIDATES = (
    "EmployeeName",
    "EnglishName",
    "Name",
    "EmpName",
    "EName",
    "UserName",
    "User",
)
_EMPLOYEE_ID_PRIMARY_CANDIDATES = (
    "EmployeeCode",
    "EMPLOYEECODE",
)
_EMPLOYEE_ID_USERNO_CANDIDATES = (
    "UserNo",
    "User_No",
    "USERNO",
)
_EMPLOYEE_ID_USERID_CANDIDATES = (
    "UserId",
    "USERID",
    "User_ID",
)
_EMPLOYEE_ID_FALLBACK_CANDIDATES = (
    "EmployeeID",
    "EmpNo",
    "EmpID",
    "UserNum",
    "EmployeeNo",
    "EmpCode",
)
_DEPARTMENT_COLUMN_CANDIDATES = (
    "Department",
    "DepartmentName",
    "DeptName",
    "Dept",
    "DepName",
)
_DEPARTMENT_REF_COLUMN_CANDIDATES = (
    "DepartmentID",
    "DeptID",
    "DepID",
    "Department",
    "Dept",
    "DepartmentNo",
    "DeptNo",
    "DepNo",
)
_DEPARTMENT_TABLE_NAME_CANDIDATES = (
    "TDepartment",
    "Department",
    "TDept",
    "Dept",
    "TBDepartment",
)
_NUMERIC_SQL_TYPES = {
    "bigint",
    "int",
    "smallint",
    "tinyint",
    "decimal",
    "numeric",
    "float",
    "real",
    "money",
    "smallmoney",
}


def _format_dt(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else None


def _format_time_only(value: datetime | None) -> str | None:
    return value.strftime("%H:%M:%S") if value else None


def _format_time_12h(value: datetime | None) -> str | None:
    return value.strftime("%I:%M:%S %p") if value else None


def _minutes_to_hhmm(value: int | None) -> str | None:
    if value is None or value < 0:
        return None
    hours = value // 60
    minutes = value % 60
    return f"{hours:02d}:{minutes:02d}"


def format_duration_readable(minutes: int | None) -> str | None:
    if minutes is None:
        return None
    hrs = minutes // 60
    mins = minutes % 60
    if hrs <= 0:
        return f"{mins:02d} Mins"
    return f"{hrs} Hrs {mins:02d} Mins"


def _duration_minutes(first_in: datetime | None, last_out: datetime | None) -> int | None:
    if not first_in or not last_out or last_out < first_in:
        return None
    delta = last_out - first_in
    return int(delta.total_seconds() // 60)


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _normalize_inout_flag(value: Any) -> int | None:
    parsed = _to_int(value)
    if parsed == 1:
        return 1
    if parsed == 0:
        return 0
    return None


def _parse_date(date_value: str) -> date:
    try:
        return datetime.strptime(date_value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="date must be in YYYY-MM-DD format",
        ) from exc


def _month_bounds(month_value: str) -> tuple[datetime, datetime, str]:
    try:
        start = datetime.strptime(month_value, "%Y-%m")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="month must be in YYYY-MM format",
        ) from exc

    if start.month == 12:
        end = datetime(start.year + 1, 1, 1)
    else:
        end = datetime(start.year, start.month + 1, 1)

    return start, end, start.strftime("%Y-%m")


def _year_bounds(year_value: str) -> tuple[datetime, datetime, str]:
    try:
        year_int = int(year_value)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="year must be numeric YYYY",
        ) from exc

    if year_int < 1900 or year_int > 2100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="year is out of accepted range",
        )

    start = datetime(year_int, 1, 1)
    end = datetime(year_int + 1, 1, 1)
    return start, end, str(year_int)


def _pick_first(existing: set[str], candidates: Sequence[str]) -> str | None:
    lowered = {column.lower(): column for column in existing}
    for candidate in candidates:
        found = lowered.get(candidate.lower())
        if found:
            return found
    return None


def _columns_of_with_cursor(cursor: Any, table_name: str) -> set[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME AS ColumnName
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        """,
        (table_name,),
    )
    rows = cursor.fetchall()
    columns: set[str] = set()
    for row in rows:
        value = row.get("ColumnName")
        if value:
            columns.add(str(value))
    return columns


def _column_info_of_with_cursor(cursor: Any, table_name: str) -> dict[str, dict[str, str]]:
    cursor.execute(
        """
        SELECT COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        """,
        (table_name,),
    )
    rows = cursor.fetchall()
    info: dict[str, dict[str, str]] = {}
    for row in rows:
        column_name = str(row.get("ColumnName") or "").strip()
        if not column_name:
            continue
        info[column_name.lower()] = {
            "name": column_name,
            "data_type": str(row.get("DataType") or "").strip().lower(),
        }
    return info


def _tables_of_with_cursor(cursor: Any) -> list[str]:
    cursor.execute(
        """
        SELECT TABLE_NAME AS TableName
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """
    )
    rows = cursor.fetchall()
    tables: list[str] = []
    for row in rows:
        table_name = str(row.get("TableName") or "").strip()
        if table_name:
            tables.append(table_name)
    return tables


def _sample_columns_match(
    cursor: Any,
    *,
    table_name: str,
    left_col: str,
    right_col: str,
    sample_size: int = 50,
) -> bool:
    if not left_col or not right_col:
        return False

    sql = f"""
        SELECT TOP {int(sample_size)}
            CONVERT(VARCHAR(255), [{left_col}]) AS LeftValue,
            CONVERT(VARCHAR(255), [{right_col}]) AS RightValue
        FROM [{table_name}]
        WHERE [{left_col}] IS NOT NULL
          AND [{right_col}] IS NOT NULL
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    if len(rows) < 5:
        return False

    for row in rows:
        left_val = str(row.get("LeftValue") or "").strip()
        right_val = str(row.get("RightValue") or "").strip()
        if not left_val or not right_val or left_val != right_val:
            return False
    return True


def _detect_employee_id_column(
    cursor: Any,
    *,
    employee_columns: set[str],
) -> tuple[str | None, str]:
    employee_code_col = _pick_first(employee_columns, _EMPLOYEE_ID_PRIMARY_CANDIDATES)
    if employee_code_col:
        return employee_code_col, "EMPLOYEECODE"

    return None, "MISSING_EMPLOYEECODE"


def _detect_department_lookup(
    cursor: Any,
    *,
    employee_columns: set[str],
    employee_column_info: dict[str, dict[str, str]],
) -> dict[str, str] | None:
    employee_dept_col = _pick_first(employee_columns, _DEPARTMENT_REF_COLUMN_CANDIDATES)
    if not employee_dept_col:
        return None

    dept_meta = employee_column_info.get(employee_dept_col.lower(), {})
    dept_type = str(dept_meta.get("data_type") or "").lower()
    if dept_type not in _NUMERIC_SQL_TYPES:
        # Already a likely textual department name in TEmployee; no lookup required.
        return None

    all_tables = _tables_of_with_cursor(cursor)
    lower_to_actual = {table.lower(): table for table in all_tables}

    ordered_candidates: list[str] = []
    for candidate in _DEPARTMENT_TABLE_NAME_CANDIDATES:
        actual = lower_to_actual.get(candidate.lower())
        if actual and actual not in ordered_candidates:
            ordered_candidates.append(actual)

    for table_name in all_tables:
        if table_name in ordered_candidates:
            continue
        lowered = table_name.lower()
        if "dept" in lowered or "department" in lowered:
            ordered_candidates.append(table_name)

    for table_name in ordered_candidates:
        if table_name.lower() == "temployee":
            continue

        table_columns = _columns_of_with_cursor(cursor, table_name)
        if not table_columns:
            continue

        key_col = _pick_first(
            table_columns,
            (
                employee_dept_col,
                "DepartmentID",
                "DeptID",
                "DepID",
                "DepartmentNo",
                "DeptNo",
                "DepNo",
                "Department",
                "Dept",
            ),
        )
        name_col = _pick_first(
            table_columns,
            (
                "DepartmentName",
                "DeptName",
                "DepName",
                "Department",
                "Dept",
                "Name",
                "Description",
            ),
        )
        if key_col and name_col and key_col.lower() != name_col.lower():
            return {
                "table": table_name,
                "key_col": key_col,
                "name_col": name_col,
                "employee_col": employee_dept_col,
            }

    return None


def _columns_of(table_name: str) -> set[str]:
    with get_cursor() as cursor:
        return _columns_of_with_cursor(cursor, table_name)


def _resolve_schema(cursor: Any) -> dict[str, Any]:
    employee_columns = _columns_of_with_cursor(cursor, "TEmployee")
    employee_column_info = _column_info_of_with_cursor(cursor, "TEmployee")
    event_columns = _columns_of_with_cursor(cursor, "TEvent")
    event_type_columns = _columns_of_with_cursor(cursor, "TEventType")
    employee_id_col, employee_id_source = _detect_employee_id_column(
        cursor,
        employee_columns=employee_columns,
    )
    employee_dept_col = _pick_first(employee_columns, _DEPARTMENT_REF_COLUMN_CANDIDATES) or _pick_first(
        employee_columns,
        _DEPARTMENT_COLUMN_CANDIDATES,
    )
    department_lookup = _detect_department_lookup(
        cursor,
        employee_columns=employee_columns,
        employee_column_info=employee_column_info,
    )

    return {
        "employee_columns": employee_columns,
        "employee_column_info": employee_column_info,
        "event_columns": event_columns,
        "event_type_columns": event_type_columns,
        "employee_name_col": _pick_first(employee_columns, _NAME_COLUMN_CANDIDATES),
        "employee_department_col": employee_dept_col,
        "employee_department_lookup": department_lookup,
        "employee_id_col": employee_id_col,
        "employee_id_source": employee_id_source,
        "employee_emp_id_col": _pick_first(employee_columns, ("EmpID",)) or employee_id_col or "EmpID",
        "employee_card_col": _pick_first(employee_columns, ("CardNo",)) or "CardNo",
        "employee_emp_enable_col": _pick_first(employee_columns, ("EmpEnable",)) or "EmpEnable",
        "employee_deleted_col": _pick_first(employee_columns, ("Deleted",)) or "Deleted",
        "employee_leave_col": _pick_first(employee_columns, ("Leave",)) or "Leave",
        "employee_is_visitor_col": _pick_first(employee_columns, ("isVisitor", "IsVisitor"))
        or "isVisitor",
        "event_emp_id_col": _pick_first(event_columns, ("EmpID",)),
        "event_card_col": _pick_first(event_columns, ("CardNo",)),
        "event_time_col": _pick_first(event_columns, ("EventTime",)),
    }


def _get_schema() -> dict[str, Any]:
    global _SCHEMA_CACHE

    with _SCHEMA_LOCK:
        if _SCHEMA_CACHE is not None:
            return _SCHEMA_CACHE

    with get_cursor() as cursor:
        resolved = _resolve_schema(cursor)

    with _SCHEMA_LOCK:
        if _SCHEMA_CACHE is None:
            _SCHEMA_CACHE = resolved
        return _SCHEMA_CACHE


def _active_employee_where(alias: str, schema: dict[str, Any]) -> str:
    emp_enable_col = schema["employee_emp_enable_col"] or "EmpEnable"
    deleted_col = schema["employee_deleted_col"] or "Deleted"
    leave_col = schema["employee_leave_col"] or "Leave"
    visitor_col = schema["employee_is_visitor_col"] or "isVisitor"
    card_col = schema["employee_card_col"] or "CardNo"

    return (
        f"{alias}.[{emp_enable_col}] = 1 "
        f"AND ({alias}.[{deleted_col}] = 0 OR {alias}.[{deleted_col}] IS NULL) "
        f"AND ({alias}.[{leave_col}] = 0 OR {alias}.[{leave_col}] IS NULL) "
        f"AND ({alias}.[{visitor_col}] = 0 OR {alias}.[{visitor_col}] IS NULL) "
        f"AND ({alias}.[{card_col}] IS NOT NULL AND {alias}.[{card_col}] <> 0)"
    )


def _employee_name_expr_for_alias(alias: str, schema: dict[str, Any]) -> str:
    name_col = schema.get("employee_name_col")
    if not name_col:
        return "''"
    return f"LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(255), {alias}.[{name_col}]), '')))"


def _employee_id_col(schema: dict[str, Any]) -> str:
    value = schema.get("employee_id_col")
    if not value:
        return ""
    return str(value)


def _employee_id_expr_for_alias(alias: str, schema: dict[str, Any]) -> str:
    employee_id_col = _employee_id_col(schema)
    if not employee_id_col:
        return "''"
    return f"LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(64), {alias}.[{employee_id_col}]), '')))"


def _employee_department_select_components(
    *,
    employee_alias: str,
    schema: dict[str, Any],
    department_alias: str = "dept",
) -> tuple[str, str]:
    dept_col = schema.get("employee_department_col")
    if not dept_col:
        return "''", ""

    lookup = schema.get("employee_department_lookup")
    if isinstance(lookup, dict):
        table_name = str(lookup.get("table") or "").strip()
        key_col = str(lookup.get("key_col") or "").strip()
        name_col = str(lookup.get("name_col") or "").strip()
        employee_col = str(lookup.get("employee_col") or dept_col).strip()
        if table_name and key_col and name_col and employee_col:
            dept_expr = (
                f"LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(255), {department_alias}.[{name_col}]), '')))"
            )
            join_sql = (
                f"LEFT JOIN [{table_name}] {department_alias} "
                f"ON {employee_alias}.[{employee_col}] = {department_alias}.[{key_col}]"
            )
            return dept_expr, join_sql

    dept_expr = f"LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(255), {employee_alias}.[{dept_col}]), '')))"
    return dept_expr, ""


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _employee_name_or_card(row_name: Any, fallback_card_no: str) -> str:
    name = _clean_text(row_name)
    return name or fallback_card_no


def _normalize_emp_id(value: Any) -> int | str:
    if value is None:
        return ""
    if isinstance(value, int):
        return value

    raw = _clean_text(value)
    if not raw:
        return ""

    try:
        return int(raw)
    except ValueError:
        return raw


def _log_employee_sample_once(employees: Sequence[dict[str, Any]]) -> None:
    global _EMPLOYEE_SAMPLE_LOGGED
    if _EMPLOYEE_SAMPLE_LOGGED or not employees:
        return

    sample = dict(employees[0])
    schema = _get_schema()
    logger.info(
        "Employee sample fields resolved: card_no=%s employee_id=%s department=%s source=%s",
        _clean_text(sample.get("card_no")) or "-",
        _clean_text(sample.get("employee_id") if sample.get("employee_id") is not None else sample.get("emp_id")) or "-",
        _clean_text(sample.get("department")) or "-",
        _clean_text(schema.get("employee_id_source")) or "-",
    )
    _EMPLOYEE_SAMPLE_LOGGED = True


def _entry_exit_case(expr: str) -> str:
    return (
        "CASE "
        f"WHEN CONVERT(VARCHAR(255), {expr}) LIKE 'Entry%' THEN 1 "
        f"WHEN CONVERT(VARCHAR(255), {expr}) LIKE 'Exit%' THEN 0 "
        "ELSE NULL END"
    )


def _normalized_inout_case(raw_expr: str, fallback_text_expr: str | None = None) -> str:
    parts = [
        "CASE",
        f"WHEN {raw_expr} = 1 THEN 1",
        f"WHEN {raw_expr} = 0 THEN 0",
        f"WHEN {raw_expr} = 2 THEN 0",
        f"WHEN {raw_expr} = -1 THEN 0",
        f"WHEN UPPER(LTRIM(RTRIM(CONVERT(VARCHAR(255), {raw_expr})))) IN ('IN', 'I', 'ENTRY', 'ENTER') THEN 1",
        f"WHEN UPPER(LTRIM(RTRIM(CONVERT(VARCHAR(255), {raw_expr})))) IN ('OUT', 'O', 'EXIT', 'LEAVE') THEN 0",
        f"WHEN CONVERT(VARCHAR(255), {raw_expr}) LIKE 'Entry%' THEN 1",
        f"WHEN CONVERT(VARCHAR(255), {raw_expr}) LIKE 'Exit%' THEN 0",
    ]

    if fallback_text_expr:
        parts.append(
            f"WHEN CONVERT(VARCHAR(255), {fallback_text_expr}) LIKE 'Entry%' THEN 1"
        )
        parts.append(
            f"WHEN CONVERT(VARCHAR(255), {fallback_text_expr}) LIKE 'Exit%' THEN 0"
        )

    parts.append("ELSE NULL END")
    return " ".join(parts)


def _detect_event_variant(
    event_alias: str = "e",
    event_type_alias: str = "et",
) -> dict[str, str]:
    schema = _get_schema()
    event_columns: set[str] = schema.get("event_columns", set())
    event_type_columns: set[str] = schema.get("event_type_columns", set())

    tevent_event_type_col = _pick_first(event_columns, ("EventType",))
    tevent_event_id_col = _pick_first(event_columns, ("EventID",))
    tevent_inout_col = _pick_first(event_columns, ("InOut",))
    tevent_event_text_col = _pick_first(event_columns, ("Event",))

    tetype_event_id_col = _pick_first(event_type_columns, ("EventID",))
    tetype_inout_col = _pick_first(event_type_columns, ("InOut",))
    tetype_event_text_col = _pick_first(event_type_columns, ("Event",))

    if tevent_event_type_col and tetype_event_id_col:
        join_sql = (
            f"LEFT JOIN [TEventType] {event_type_alias} "
            f"ON {event_alias}.[{tevent_event_type_col}] = {event_type_alias}.[{tetype_event_id_col}]"
        )

        if tetype_inout_col:
            inout_expr = _normalized_inout_case(
                f"{event_type_alias}.[{tetype_inout_col}]",
                f"{event_type_alias}.[{tetype_event_text_col}]" if tetype_event_text_col else None,
            )
        elif tetype_event_text_col:
            inout_expr = _entry_exit_case(f"{event_type_alias}.[{tetype_event_text_col}]")
        else:
            inout_expr = "NULL"

        return {
            "variant": "EVENTTYPE_to_EventID",
            "join_sql": join_sql,
            "inout_expr": inout_expr,
        }

    if tevent_event_id_col and tetype_event_id_col:
        join_sql = (
            f"LEFT JOIN [TEventType] {event_type_alias} "
            f"ON {event_alias}.[{tevent_event_id_col}] = {event_type_alias}.[{tetype_event_id_col}]"
        )

        if tetype_inout_col:
            inout_expr = _normalized_inout_case(
                f"{event_type_alias}.[{tetype_inout_col}]",
                f"{event_type_alias}.[{tetype_event_text_col}]" if tetype_event_text_col else None,
            )
        elif tetype_event_text_col:
            inout_expr = _entry_exit_case(f"{event_type_alias}.[{tetype_event_text_col}]")
        else:
            inout_expr = "NULL"

        return {
            "variant": "EVENTID_to_EventID",
            "join_sql": join_sql,
            "inout_expr": inout_expr,
        }

    if tevent_inout_col:
        return {
            "variant": "TEVENT_InOut_only",
            "join_sql": "",
            "inout_expr": _normalized_inout_case(f"{event_alias}.[{tevent_inout_col}]"),
        }

    if tevent_event_text_col:
        return {
            "variant": "TEVENT_Event_text_only",
            "join_sql": "",
            "inout_expr": _entry_exit_case(f"{event_alias}.[{tevent_event_text_col}]"),
        }

    return {
        "variant": "UNSUPPORTED",
        "join_sql": "",
        "inout_expr": "NULL",
    }


def _fetch_employee_identity(card_no: str) -> Dict[str, Any]:
    schema = _get_schema()
    employee_id_expr = _employee_id_expr_for_alias("emp", schema)
    employee_card_col = schema["employee_card_col"] or "CardNo"
    active_where = _active_employee_where("emp", schema)
    employee_name_expr = _employee_name_expr_for_alias("emp", schema)
    department_expr, department_join_sql = _employee_department_select_components(
        employee_alias="emp",
        schema=schema,
        department_alias="dept",
    )

    sql = f"""
        SELECT TOP 1
            {employee_id_expr} AS EmployeeID,
            CONVERT(VARCHAR(64), emp.[{employee_card_col}]) AS CardNo,
            {employee_name_expr} AS EmployeeName,
            {department_expr} AS Department
        FROM [dbo].[TEmployee] emp
        {department_join_sql}
        WHERE {active_where}
          AND CONVERT(VARCHAR(64), emp.[{employee_card_col}]) = %s
        ORDER BY EmployeeName, CardNo
    """

    with get_cursor() as cursor:
        cursor.execute(sql, (card_no,))
        row = cursor.fetchone() or {}

    normalized_card_no = _clean_text(row.get("CardNo")) or card_no
    employee_name = _employee_name_or_card(row.get("EmployeeName"), normalized_card_no)
    employee_id = _normalize_emp_id(row.get("EmployeeID"))
    department = _clean_text(row.get("Department")) or None

    return {
        "emp_id": employee_id,
        "employee_id": employee_id,
        "card_no": normalized_card_no,
        "employee_name": employee_name,
        "department": department,
    }


def fetch_employees(search: str) -> List[Dict[str, Any]]:
    schema = _get_schema()
    employee_id_expr = _employee_id_expr_for_alias("emp", schema)
    employee_card_col = schema["employee_card_col"] or "CardNo"
    active_where = _active_employee_where("emp", schema)
    employee_name_expr = _employee_name_expr_for_alias("emp", schema)
    department_expr, department_join_sql = _employee_department_select_components(
        employee_alias="emp",
        schema=schema,
        department_alias="dept",
    )

    sql = f"""
        SELECT TOP 200
            {employee_id_expr} AS EmployeeID,
            CONVERT(VARCHAR(64), emp.[{employee_card_col}]) AS CardNo,
            {employee_name_expr} AS EmployeeName,
            {department_expr} AS Department
        FROM [dbo].[TEmployee] emp
        {department_join_sql}
        WHERE {active_where}
    """

    params: tuple[str, ...] = ()
    if search:
        wildcard = f"%{search}%"
        sql += f"""
          AND (
              {employee_name_expr} LIKE %s
              OR CONVERT(VARCHAR(64), emp.[{employee_card_col}]) LIKE %s
              OR {employee_id_expr} LIKE %s
          )
        """
        params = (wildcard, wildcard, wildcard)

    sql += """
        ORDER BY EmployeeName, CardNo
    """

    with get_cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    employees: List[Dict[str, Any]] = []
    for row in rows:
        card_no = _clean_text(row.get("CardNo"))
        if not card_no:
            continue
        employee_name = _employee_name_or_card(row.get("EmployeeName"), card_no)
        employee_id = _normalize_emp_id(row.get("EmployeeID"))
        department = _clean_text(row.get("Department")) or None
        employees.append(
            {
                "emp_id": employee_id,
                "employee_id": employee_id,
                "card_no": card_no,
                "employee_name": employee_name,
                "department": department,
            }
        )

    _log_employee_sample_once(employees)
    return employees


def _fetch_all_active_employees() -> list[dict[str, Any]]:
    schema = _get_schema()
    employee_id_expr = _employee_id_expr_for_alias("emp", schema)
    employee_card_col = schema["employee_card_col"] or "CardNo"
    active_where = _active_employee_where("emp", schema)
    employee_name_expr = _employee_name_expr_for_alias("emp", schema)
    department_expr, department_join_sql = _employee_department_select_components(
        employee_alias="emp",
        schema=schema,
        department_alias="dept",
    )

    sql = f"""
        SELECT
            {employee_id_expr} AS EmployeeID,
            CONVERT(VARCHAR(64), emp.[{employee_card_col}]) AS CardNo,
            {employee_name_expr} AS EmployeeName,
            {department_expr} AS Department
        FROM [dbo].[TEmployee] emp
        {department_join_sql}
        WHERE {active_where}
        ORDER BY EmployeeName, CardNo
    """

    with get_cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    employees: list[dict[str, Any]] = []
    for row in rows:
        card_no = _clean_text(row.get("CardNo"))
        if not card_no:
            continue
        employee_name = _employee_name_or_card(row.get("EmployeeName"), card_no)
        employee_id = _normalize_emp_id(row.get("EmployeeID"))
        department = _clean_text(row.get("Department")) or None
        employees.append(
            {
                "emp_id": employee_id,
                "employee_id": employee_id,
                "card_no": card_no,
                "employee_name": employee_name,
                "department": department,
            }
        )
    return employees


def _fetch_all_active_events(
    *,
    start: datetime,
    end: datetime,
    detector: dict[str, str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    schema = _get_schema()
    event_card_col = schema.get("event_card_col")
    event_time_col = schema.get("event_time_col")
    employee_card_col = schema.get("employee_card_col") or "CardNo"
    if not event_card_col or not event_time_col:
        return {}

    resolved_detector = detector or _detect_event_variant(event_alias="e", event_type_alias="et")
    if resolved_detector["variant"] == "UNSUPPORTED":
        return {}

    active_where = _active_employee_where("emp", schema)
    sql = f"""
        SELECT
            CONVERT(VARCHAR(64), emp.[{employee_card_col}]) AS CardNo,
            e.[{event_time_col}] AS EventTime,
            {resolved_detector['inout_expr']} AS InOutFlag
        FROM [TEvent] e
        {resolved_detector['join_sql']}
        INNER JOIN [dbo].[TEmployee] emp
            ON CONVERT(VARCHAR(64), e.[{event_card_col}]) = CONVERT(VARCHAR(64), emp.[{employee_card_col}])
        WHERE {active_where}
          AND e.[{event_time_col}] >= %s
          AND e.[{event_time_col}] < %s
        ORDER BY CardNo ASC, e.[{event_time_col}] ASC
    """

    with get_cursor() as cursor:
        cursor.execute(sql, (start, end))
        rows = cursor.fetchall()

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        card_no = _clean_text(row.get("CardNo"))
        event_time = row.get("EventTime")
        if not card_no or not isinstance(event_time, datetime):
            continue
        grouped[card_no].append(
            {
                "event_time": event_time,
                "inout_flag": _normalize_inout_flag(row.get("InOutFlag")),
            }
        )
    return grouped


def _build_daily_records_for_period_from_events(
    *,
    events: Sequence[dict[str, Any]],
    start: datetime,
    end: datetime,
    swap_applied: bool,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    total_events = len(events)
    start_idx = 0
    end_idx = 0

    day_cursor = start
    while day_cursor < end:
        day_end = day_cursor + timedelta(days=1)
        overnight_end = day_end + timedelta(hours=settings.shift_out_cutoff_hours)

        while start_idx < total_events and events[start_idx]["event_time"] < day_cursor:
            start_idx += 1

        if end_idx < start_idx:
            end_idx = start_idx

        while end_idx < total_events and events[end_idx]["event_time"] < overnight_end:
            end_idx += 1

        day_events = events[start_idx:end_idx]
        day_record = _compute_day_attendance(
            day_start=day_cursor,
            day_events=day_events,
            swap_applied=swap_applied,
        )

        if day_record["has_relevant_events"]:
            records.append(_serialize_day_record(day_record))

        day_cursor = day_end

    return records


def _compute_period_segment_totals_from_events(
    *,
    events: Sequence[dict[str, Any]],
    start: datetime,
    end: datetime,
    swap_applied: bool,
) -> dict[str, Any]:
    if end <= start or not events:
        return {
            "totalInMinutes": 0,
            "totalOutMinutes": 0,
            "totalInHHMM": _minutes_to_hhmm(0),
            "totalOutHHMM": _minutes_to_hhmm(0),
            "per_day": {},
        }

    day_totals: dict[str, dict[str, int]] = {}
    state: int | None = None
    segment_start: datetime | None = None

    for event in events:
        event_time = event.get("event_time")
        if not isinstance(event_time, datetime):
            continue

        next_state = _event_state(event, swap_applied)
        if next_state is None:
            continue

        if state is None:
            state = next_state
            segment_start = event_time
            continue

        if next_state == state:
            continue

        if segment_start is not None:
            _accumulate_segment_minutes(
                day_totals,
                state=state,
                segment_start=segment_start,
                segment_end=event_time,
                window_start=start,
                window_end=end,
            )

        state = next_state
        segment_start = event_time

    total_in_minutes = sum(bucket["in_minutes"] for bucket in day_totals.values())
    total_out_minutes = sum(bucket["out_minutes"] for bucket in day_totals.values())

    return {
        "totalInMinutes": total_in_minutes,
        "totalOutMinutes": total_out_minutes,
        "totalInHHMM": _minutes_to_hhmm(total_in_minutes),
        "totalOutHHMM": _minutes_to_hhmm(total_out_minutes),
        "per_day": day_totals,
    }


def _build_daily_transactions_and_intervals_from_events(
    *,
    selected_date: date,
    raw_window_events: Sequence[dict[str, Any]],
    swap_applied: bool,
) -> dict[str, Any]:
    day_start = datetime.combine(selected_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    window_end = day_end + timedelta(hours=settings.shift_out_cutoff_hours)

    if not raw_window_events:
        return {
            "date": day_start.strftime("%Y-%m-%d"),
            "first_in": None,
            "last_out": None,
            "duration_minutes": None,
            "duration_hhmm": None,
            "missing_punch": False,
            "rows": [],
            "transactions": [],
            "intervals": [],
            "total_in_minutes": 0,
            "total_out_minutes": 0,
            "total_in": _minutes_to_hhmm(0),
            "total_out": _minutes_to_hhmm(0),
            "totalInMinutes": 0,
            "totalOutMinutes": 0,
            "totalInHHMM": _minutes_to_hhmm(0),
            "totalOutHHMM": _minutes_to_hhmm(0),
            "notes": [],
        }

    in_scope_events = [
        event for event in raw_window_events if day_start <= event["event_time"] < window_end
    ]
    normalized_events = _normalize_event_sequence(events=in_scope_events, swap_applied=swap_applied)

    if not normalized_events:
        return {
            "date": day_start.strftime("%Y-%m-%d"),
            "first_in": None,
            "last_out": None,
            "duration_minutes": None,
            "duration_hhmm": None,
            "missing_punch": False,
            "rows": [],
            "transactions": [],
            "intervals": [],
            "total_in_minutes": 0,
            "total_out_minutes": 0,
            "total_in": _minutes_to_hhmm(0),
            "total_out": _minutes_to_hhmm(0),
            "totalInMinutes": 0,
            "totalOutMinutes": 0,
            "totalInHHMM": _minutes_to_hhmm(0),
            "totalOutHHMM": _minutes_to_hhmm(0),
            "notes": [],
        }

    notes: list[str] = []

    first_in_index: int | None = None
    for index, event in enumerate(normalized_events):
        event_time = event["event_time"]
        if day_start <= event_time < day_end and event["state"] == 1:
            first_in_index = index
            break

    out_on_day = [
        event
        for event in normalized_events
        if day_start <= event["event_time"] < day_end and event["state"] == 0
    ]

    if first_in_index is None:
        if out_on_day:
            notes.append("No IN punch found on selected date; OUT-only transactions were ignored.")
        return {
            "date": day_start.strftime("%Y-%m-%d"),
            "first_in": None,
            "last_out": _format_dt(out_on_day[-1]["event_time"]) if out_on_day else None,
            "duration_minutes": None,
            "duration_hhmm": None,
            "missing_punch": bool(out_on_day),
            "rows": [],
            "transactions": [],
            "intervals": [],
            "total_in_minutes": 0,
            "total_out_minutes": 0,
            "total_in": _minutes_to_hhmm(0),
            "total_out": _minutes_to_hhmm(0),
            "totalInMinutes": 0,
            "totalOutMinutes": 0,
            "totalInHHMM": _minutes_to_hhmm(0),
            "totalOutHHMM": _minutes_to_hhmm(0),
            "notes": notes,
        }

    first_in_dt = normalized_events[first_in_index]["event_time"]
    last_out_index: int | None = None
    for index in range(len(normalized_events) - 1, first_in_index - 1, -1):
        event = normalized_events[index]
        if event["event_time"] >= first_in_dt and event["state"] == 0:
            last_out_index = index
            break

    sequence_end_index = last_out_index if last_out_index is not None else len(normalized_events) - 1
    sequence_events = normalized_events[first_in_index : sequence_end_index + 1]

    transactions: list[dict[str, Any]] = []
    intervals: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    total_in_minutes = 0
    total_out_minutes = 0
    open_in: datetime | None = None
    last_out_for_break: datetime | None = None

    for event in sequence_events:
        event_time = event["event_time"]
        state = int(event["state"])
        inferred = bool(event["inferred"])
        event_label = _event_type_label(state)

        transactions.append(
            {
                "type": event_label,
                "time": _format_time_only(event_time),
                "timestamp": _format_dt(event_time),
                "inferred": inferred,
            }
        )

        if state == 1:
            if open_in is not None:
                notes.append(
                    f"Ignored consecutive IN at {_format_dt(event_time)} while previous IN remained open."
                )
                continue

            if last_out_for_break is not None and event_time > last_out_for_break:
                total_out_minutes += int((event_time - last_out_for_break).total_seconds() // 60)
                last_out_for_break = None

            open_in = event_time
            continue

        if open_in is None:
            notes.append(f"Ignored OUT at {_format_dt(event_time)} without a matching prior IN.")
            last_out_for_break = event_time
            continue

        in_minutes = int((event_time - open_in).total_seconds() // 60)
        if in_minutes >= 0:
            total_in_minutes += in_minutes
            intervals.append(
                {
                    "date": open_in.strftime("%Y-%m-%d"),
                    "in": _format_dt(open_in),
                    "out": _format_dt(event_time),
                    "in_time": _format_time_only(open_in),
                    "out_time": _format_time_only(event_time),
                    "in_duration_minutes": in_minutes,
                    "in_duration_hhmm": _minutes_to_hhmm(in_minutes),
                }
            )
            rows.append(
                {
                    "date": open_in.strftime("%Y-%m-%d"),
                    "in": _format_time_12h(open_in),
                    "out": _format_time_12h(event_time),
                    "duration": _minutes_to_hhmm(in_minutes),
                    "in_raw": _format_dt(open_in),
                    "out_raw": _format_dt(event_time),
                    "duration_minutes": in_minutes,
                }
            )
        else:
            notes.append(
                f"Skipped negative IN interval from {_format_dt(open_in)} to {_format_dt(event_time)}."
            )

        open_in = None
        last_out_for_break = event_time

    if open_in is not None:
        notes.append(f"Missing OUT after last IN at {_format_dt(open_in)}; open interval excluded from totals.")

    last_out_dt = normalized_events[last_out_index]["event_time"] if last_out_index is not None else None
    duration_minutes = _duration_minutes(first_in_dt, last_out_dt)
    if duration_minutes is None and first_in_dt is not None and last_out_dt is None:
        notes.append("Missing OUT punch in selected work window.")

    return {
        "date": day_start.strftime("%Y-%m-%d"),
        "first_in": _format_dt(first_in_dt),
        "last_out": _format_dt(last_out_dt),
        "duration_minutes": duration_minutes,
        "duration_hhmm": _minutes_to_hhmm(duration_minutes),
        "missing_punch": (first_in_dt is None) ^ (last_out_dt is None),
        "rows": rows,
        "transactions": transactions,
        "intervals": intervals,
        "total_in_minutes": total_in_minutes,
        "total_out_minutes": total_out_minutes,
        "total_in": _minutes_to_hhmm(total_in_minutes),
        "total_out": _minutes_to_hhmm(total_out_minutes),
        "totalInMinutes": total_in_minutes,
        "totalOutMinutes": total_out_minutes,
        "totalInHHMM": _minutes_to_hhmm(total_in_minutes),
        "totalOutHHMM": _minutes_to_hhmm(total_out_minutes),
        "notes": notes,
    }


def _count_sessions_from_events(
    *,
    events: Sequence[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
    swap_applied: bool,
) -> int:
    if not events:
        return 0

    normalized = _normalize_event_sequence(events=events, swap_applied=swap_applied)
    sessions = 0
    open_in: datetime | None = None

    for event in normalized:
        event_time = event["event_time"]
        state = int(event["state"])
        if event_time >= window_end:
            break

        if state == 1:
            if open_in is None:
                open_in = event_time
            continue

        if open_in is None:
            continue

        if event_time >= open_in and event_time > window_start and open_in < window_end:
            sessions += 1
        open_in = None

    return sessions
def fetch_dashboard_summary() -> Dict[str, Any]:
    schema = _get_schema()
    employee_card_col = schema.get("employee_card_col") or "CardNo"
    event_card_col = schema.get("event_card_col")
    event_time_col = schema.get("event_time_col")
    active_where = _active_employee_where("emp", schema)

    total_sql = f"""
        SELECT COUNT(1) AS TotalEmployees
        FROM [dbo].[TEmployee] emp
        WHERE {active_where}
    """

    with get_cursor() as cursor:
        cursor.execute(total_sql)
        total_row = cursor.fetchone() or {}

    total = int(total_row.get("TotalEmployees") or 0)

    if not event_card_col or not event_time_col:
        return {
            "totalEmployees": total,
            "inCount": 0,
            "outCount": 0,
            "unknownCount": total,
            "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    detector = _detect_event_variant(event_alias="e2", event_type_alias="et2")
    if detector["variant"] == "UNSUPPORTED":
        return {
            "totalEmployees": total,
            "inCount": 0,
            "outCount": 0,
            "unknownCount": total,
            "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    sql = f"""
        SELECT
            COUNT(1) AS TotalEmployees,
            SUM(CASE WHEN summary.LastInOut = 1 THEN 1 ELSE 0 END) AS InCount,
            SUM(CASE WHEN summary.LastInOut = 0 THEN 1 ELSE 0 END) AS OutCount,
            SUM(CASE WHEN summary.LastInOut IS NULL THEN 1 ELSE 0 END) AS UnknownCount
        FROM (
            SELECT
                CONVERT(VARCHAR(64), emp.[{employee_card_col}]) AS CardNo,
                (
                    SELECT TOP 1 {detector['inout_expr']}
                    FROM [TEvent] e2
                    {detector['join_sql']}
                    WHERE CONVERT(VARCHAR(64), e2.[{event_card_col}]) = CONVERT(VARCHAR(64), emp.[{employee_card_col}])
                    ORDER BY e2.[{event_time_col}] DESC
                ) AS LastInOut
            FROM [dbo].[TEmployee] emp
            WHERE {active_where}
        ) summary
    """

    with get_cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone() or {}

    in_count = int(row.get("InCount") or 0)
    out_count = int(row.get("OutCount") or 0)
    unknown_count = int(row.get("UnknownCount") or max(total - in_count - out_count, 0))

    return {
        "totalEmployees": total,
        "inCount": in_count,
        "outCount": out_count,
        "unknownCount": unknown_count,
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _fetch_recent_mapping_sample(
    limit: int = 200,
    detector: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    schema = _get_schema()
    event_time_col = schema.get("event_time_col")
    if not event_time_col:
        return []

    resolved_detector = detector or _detect_event_variant(event_alias="e", event_type_alias="et")
    if resolved_detector["variant"] == "UNSUPPORTED":
        return []

    sql = f"""
        SELECT TOP {int(limit)}
            e.[{event_time_col}] AS EventTime,
            {resolved_detector['inout_expr']} AS InOutFlag
        FROM [TEvent] e
        {resolved_detector['join_sql']}
        WHERE e.[{event_time_col}] IS NOT NULL
        ORDER BY e.[{event_time_col}] DESC
    """

    with get_cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    sample: list[dict[str, Any]] = []
    for row in rows:
        event_time = row.get("EventTime")
        if not isinstance(event_time, datetime):
            continue
        sample.append(
            {
                "event_time": event_time,
                "inout_flag": _normalize_inout_flag(row.get("InOutFlag")),
            }
        )

    return sample


def _should_auto_swap(sample: Sequence[dict[str, Any]]) -> bool:
    valid = [row for row in sample if row.get("inout_flag") in {0, 1}]
    if len(valid) < 50:
        # Not enough usable IN/OUT rows => disable heuristic swap detection.
        return False

    in_total = 0
    in_early = 0
    out_total = 0
    out_afternoon_evening = 0

    for row in valid:
        event_time = row.get("event_time")
        flag = row.get("inout_flag")
        if not isinstance(event_time, datetime):
            continue

        hour = event_time.hour
        if flag == 1:
            in_total += 1
            if 0 <= hour <= 6:
                in_early += 1
        elif flag == 0:
            out_total += 1
            if 12 <= hour <= 23:
                out_afternoon_evening += 1

    if in_total == 0 or out_total == 0:
        return False

    in_ratio = in_early / in_total
    out_ratio = out_afternoon_evening / out_total
    return in_ratio > 0.60 and out_ratio > 0.60


def _get_mapping_state(detector: dict[str, str] | None = None) -> dict[str, Any]:
    global _MAPPING_CACHE

    resolved_detector = detector or _detect_event_variant(event_alias="e", event_type_alias="et")
    detector_variant = resolved_detector["variant"]

    if detector_variant == "UNSUPPORTED":
        return {
            "mappingVariant": "unsupported",
            "swapApplied": False,
            "detectorVariant": detector_variant,
            "autoDetected": False,
            "manualOverride": False,
        }

    if bool(settings.inout_swap):
        return {
            "mappingVariant": "swapped",
            "swapApplied": True,
            "detectorVariant": detector_variant,
            "autoDetected": False,
            "manualOverride": True,
        }

    now = time.time()
    with _MAPPING_LOCK:
        cached = _MAPPING_CACHE
        if cached and (now - float(cached.get("ts") or 0)) < _MAPPING_CACHE_TTL_SECONDS:
            if cached.get("detectorVariant") == detector_variant:
                return {
                    "mappingVariant": str(cached.get("mappingVariant") or "normal"),
                    "swapApplied": bool(cached.get("swapApplied")),
                    "detectorVariant": detector_variant,
                    "autoDetected": bool(cached.get("autoDetected")),
                    "manualOverride": False,
                }

    sample = _fetch_recent_mapping_sample(limit=200, detector=resolved_detector)
    auto_swapped = _should_auto_swap(sample)

    state = {
        "mappingVariant": "swapped" if auto_swapped else "normal",
        "swapApplied": auto_swapped,
        "detectorVariant": detector_variant,
        "autoDetected": auto_swapped,
        "manualOverride": False,
    }

    with _MAPPING_LOCK:
        _MAPPING_CACHE = {
            "ts": now,
            "mappingVariant": state["mappingVariant"],
            "swapApplied": state["swapApplied"],
            "detectorVariant": detector_variant,
            "autoDetected": state["autoDetected"],
        }

    return state


def _fetch_events_for_card(
    card_no: str,
    start: datetime,
    end: datetime,
    detector: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    schema = _get_schema()
    event_card_col = schema.get("event_card_col")
    event_time_col = schema.get("event_time_col")
    employee_card_col = schema.get("employee_card_col") or "CardNo"
    if not event_card_col or not event_time_col:
        return []

    resolved_detector = detector or _detect_event_variant(event_alias="e", event_type_alias="et")
    if resolved_detector["variant"] == "UNSUPPORTED":
        return []

    sql = f"""
        SELECT
            e.[{event_time_col}] AS EventTime,
            {resolved_detector['inout_expr']} AS InOutFlag
        FROM [TEvent] e
        {resolved_detector['join_sql']}
        INNER JOIN [dbo].[TEmployee] emp
            ON CONVERT(VARCHAR(64), e.[{event_card_col}]) = CONVERT(VARCHAR(64), emp.[{employee_card_col}])
        WHERE CONVERT(VARCHAR(64), emp.[{employee_card_col}]) = %s
          AND e.[{event_time_col}] >= %s
          AND e.[{event_time_col}] < %s
        ORDER BY e.[{event_time_col}] ASC
    """

    with get_cursor() as cursor:
        cursor.execute(sql, (card_no, start, end))
        rows = cursor.fetchall()

    events: list[dict[str, Any]] = []
    for row in rows:
        event_time = row.get("EventTime")
        if not isinstance(event_time, datetime):
            continue
        events.append(
            {
                "event_time": event_time,
                "inout_flag": _normalize_inout_flag(row.get("InOutFlag")),
            }
        )

    return events


def _fetch_last_event_before(
    card_no: str,
    boundary: datetime,
    detector: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    schema = _get_schema()
    event_card_col = schema.get("event_card_col")
    event_time_col = schema.get("event_time_col")
    employee_card_col = schema.get("employee_card_col") or "CardNo"
    if not event_card_col or not event_time_col:
        return None

    resolved_detector = detector or _detect_event_variant(event_alias="e", event_type_alias="et")
    if resolved_detector["variant"] == "UNSUPPORTED":
        return None

    sql = f"""
        SELECT TOP 1
            e.[{event_time_col}] AS EventTime,
            {resolved_detector['inout_expr']} AS InOutFlag
        FROM [TEvent] e
        {resolved_detector['join_sql']}
        INNER JOIN [dbo].[TEmployee] emp
            ON CONVERT(VARCHAR(64), e.[{event_card_col}]) = CONVERT(VARCHAR(64), emp.[{employee_card_col}])
        WHERE CONVERT(VARCHAR(64), emp.[{employee_card_col}]) = %s
          AND e.[{event_time_col}] < %s
        ORDER BY e.[{event_time_col}] DESC
    """

    with get_cursor() as cursor:
        cursor.execute(sql, (card_no, boundary))
        row = cursor.fetchone() or {}

    event_time = row.get("EventTime")
    if not isinstance(event_time, datetime):
        return None

    return {
        "event_time": event_time,
        "inout_flag": _normalize_inout_flag(row.get("InOutFlag")),
    }


def _event_state(event: dict[str, Any], swap_applied: bool) -> int | None:
    flag = event.get("inout_flag")
    if flag not in {0, 1}:
        return None
    if not swap_applied:
        return int(flag)
    return 0 if flag == 1 else 1


def _normalize_event_sequence(
    *,
    events: Sequence[dict[str, Any]],
    swap_applied: bool,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    previous_state: int | None = None

    for event in sorted(events, key=lambda item: item["event_time"]):
        event_time = event.get("event_time")
        if not isinstance(event_time, datetime):
            continue

        state = _event_state(event, swap_applied)
        inferred = False

        if state is None:
            inferred = True
            if previous_state is None:
                state = 1
            else:
                state = 0 if previous_state == 1 else 1

        normalized.append(
            {
                "event_time": event_time,
                "state": state,
                "inferred": inferred,
            }
        )
        previous_state = state

    return normalized


def _event_type_label(state: int) -> str:
    return "IN" if state == 1 else "OUT"


def _build_daily_transactions_and_intervals(
    *,
    card_no: str,
    selected_date: date,
    swap_applied: bool,
    detector: dict[str, str] | None = None,
) -> dict[str, Any]:
    day_start = datetime.combine(selected_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    window_start = day_start - timedelta(hours=12)
    window_end = day_end + timedelta(hours=12)

    raw_window_events = _fetch_events_for_card(
        card_no=card_no,
        start=window_start,
        end=window_end,
        detector=detector,
    )
    return _build_daily_transactions_and_intervals_from_events(
        selected_date=selected_date,
        raw_window_events=raw_window_events,
        swap_applied=swap_applied,
    )


def _accumulate_segment_minutes(
    day_totals: dict[str, dict[str, int]],
    *,
    state: int,
    segment_start: datetime,
    segment_end: datetime,
    window_start: datetime,
    window_end: datetime,
) -> None:
    clipped_start = max(segment_start, window_start)
    clipped_end = min(segment_end, window_end)
    if clipped_end <= clipped_start:
        return

    cursor = clipped_start
    while cursor < clipped_end:
        next_day = datetime(cursor.year, cursor.month, cursor.day) + timedelta(days=1)
        chunk_end = min(clipped_end, next_day)
        minutes = int((chunk_end - cursor).total_seconds() // 60)
        if minutes > 0:
            day_key = cursor.strftime("%Y-%m-%d")
            bucket = day_totals.setdefault(day_key, {"in_minutes": 0, "out_minutes": 0})
            if state == 1:
                bucket["in_minutes"] += minutes
            elif state == 0:
                bucket["out_minutes"] += minutes
        cursor = chunk_end


def _compute_period_segment_totals(
    *,
    card_no: str,
    start: datetime,
    end: datetime,
    swap_applied: bool,
    detector: dict[str, str] | None = None,
) -> dict[str, Any]:
    if end <= start:
        return {
            "totalInMinutes": 0,
            "totalOutMinutes": 0,
            "totalInHHMM": _minutes_to_hhmm(0),
            "totalOutHHMM": _minutes_to_hhmm(0),
            "per_day": {},
        }

    resolved_detector = detector or _detect_event_variant(event_alias="e", event_type_alias="et")
    if resolved_detector["variant"] == "UNSUPPORTED":
        return {
            "totalInMinutes": 0,
            "totalOutMinutes": 0,
            "totalInHHMM": _minutes_to_hhmm(0),
            "totalOutHHMM": _minutes_to_hhmm(0),
            "per_day": {},
        }

    timeline = _fetch_events_for_card(
        card_no=card_no,
        start=start,
        end=end,
        detector=resolved_detector,
    )
    anchor = _fetch_last_event_before(
        card_no=card_no,
        boundary=start,
        detector=resolved_detector,
    )
    if anchor is not None:
        timeline.insert(0, anchor)

    if not timeline:
        return {
            "totalInMinutes": 0,
            "totalOutMinutes": 0,
            "totalInHHMM": _minutes_to_hhmm(0),
            "totalOutHHMM": _minutes_to_hhmm(0),
            "per_day": {},
        }

    timeline.sort(key=lambda item: item["event_time"])

    day_totals: dict[str, dict[str, int]] = {}
    state: int | None = None
    segment_start: datetime | None = None

    for event in timeline:
        event_time = event.get("event_time")
        if not isinstance(event_time, datetime):
            continue

        next_state = _event_state(event, swap_applied)
        if next_state is None:
            continue

        if state is None:
            state = next_state
            segment_start = event_time
            continue

        if next_state == state:
            continue

        if segment_start is not None:
            _accumulate_segment_minutes(
                day_totals,
                state=state,
                segment_start=segment_start,
                segment_end=event_time,
                window_start=start,
                window_end=end,
            )

        state = next_state
        segment_start = event_time

    total_in_minutes = sum(bucket["in_minutes"] for bucket in day_totals.values())
    total_out_minutes = sum(bucket["out_minutes"] for bucket in day_totals.values())

    return {
        "totalInMinutes": total_in_minutes,
        "totalOutMinutes": total_out_minutes,
        "totalInHHMM": _minutes_to_hhmm(total_in_minutes),
        "totalOutHHMM": _minutes_to_hhmm(total_out_minutes),
        "per_day": day_totals,
    }


def _is_in_event(event: dict[str, Any], swap_applied: bool) -> bool:
    flag = event.get("inout_flag")
    if flag not in {0, 1}:
        return False
    effective = 0 if (swap_applied and flag == 1) else 1 if (swap_applied and flag == 0) else flag
    return effective == 1


def _is_out_event(event: dict[str, Any], swap_applied: bool) -> bool:
    flag = event.get("inout_flag")
    if flag not in {0, 1}:
        return False
    effective = 0 if (swap_applied and flag == 1) else 1 if (swap_applied and flag == 0) else flag
    return effective == 0


def _compute_day_attendance(
    *,
    day_start: datetime,
    day_events: Sequence[dict[str, Any]],
    swap_applied: bool,
) -> dict[str, Any]:
    cutoff_hours = settings.shift_out_cutoff_hours
    day_end = day_start + timedelta(days=1)
    overnight_end = day_end + timedelta(hours=cutoff_hours)

    in_primary: list[datetime] = []
    out_primary: list[datetime] = []

    for event in day_events:
        event_time = event["event_time"]
        if event_time < day_start or event_time >= day_end:
            continue

        if _is_in_event(event, swap_applied):
            in_primary.append(event_time)
        elif _is_out_event(event, swap_applied):
            out_primary.append(event_time)

    first_in = min(in_primary) if in_primary else None
    last_out: datetime | None = None

    if first_in is not None:
        outs_after_in = [
            event["event_time"]
            for event in day_events
            if first_in <= event["event_time"] < overnight_end and _is_out_event(event, swap_applied)
        ]
        if outs_after_in:
            last_out = max(outs_after_in)

        # Sanity checks: never keep negative duration results.
        if last_out is not None and last_out < first_in:
            next_out: datetime | None = None
            last_out_after_in: datetime | None = None
            for event in day_events:
                event_time = event["event_time"]
                if event_time < first_in or event_time >= overnight_end:
                    continue
                if not _is_out_event(event, swap_applied):
                    continue
                if next_out is None:
                    next_out = event_time
                last_out_after_in = event_time
            last_out = last_out_after_in or next_out
    else:
        # OUT-only day => missing punch.
        last_out = max(out_primary) if out_primary else None

    duration_minutes = _duration_minutes(first_in, last_out)
    if first_in is not None and last_out is not None and duration_minutes is None:
        # Never return negative durations; treat as missing pairing.
        last_out = None

    missing_punch = (first_in is None) ^ (last_out is None)
    has_relevant_events = bool(in_primary or out_primary)

    return {
        "date": day_start.strftime("%Y-%m-%d"),
        "first_in_dt": first_in,
        "last_out_dt": last_out,
        "duration_minutes": duration_minutes,
        "duration_hhmm": _minutes_to_hhmm(duration_minutes),
        "missing_punch": missing_punch,
        "has_relevant_events": has_relevant_events,
    }


def _serialize_day_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": record["date"],
        "first_in": _format_dt(record.get("first_in_dt")),
        "last_out": _format_dt(record.get("last_out_dt")),
        "duration_minutes": record.get("duration_minutes"),
        "duration_hhmm": record.get("duration_hhmm"),
        "missing_punch": bool(record.get("missing_punch")),
    }


def _build_daily_records_for_period(
    *,
    card_no: str,
    start: datetime,
    end: datetime,
    swap_applied: bool,
    detector: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    extended_end = end + timedelta(days=1, hours=settings.shift_out_cutoff_hours)
    events = _fetch_events_for_card(
        card_no=card_no,
        start=start,
        end=extended_end,
        detector=detector,
    )

    records: list[dict[str, Any]] = []
    total_events = len(events)
    start_idx = 0
    end_idx = 0

    day_cursor = start
    while day_cursor < end:
        day_end = day_cursor + timedelta(days=1)
        overnight_end = day_end + timedelta(hours=settings.shift_out_cutoff_hours)

        while start_idx < total_events and events[start_idx]["event_time"] < day_cursor:
            start_idx += 1

        if end_idx < start_idx:
            end_idx = start_idx

        while end_idx < total_events and events[end_idx]["event_time"] < overnight_end:
            end_idx += 1

        day_events = events[start_idx:end_idx]
        day_record = _compute_day_attendance(
            day_start=day_cursor,
            day_events=day_events,
            swap_applied=swap_applied,
        )

        if day_record["has_relevant_events"]:
            records.append(_serialize_day_record(day_record))

        day_cursor = day_end

    return records


def _build_single_day_record(
    *,
    card_no: str,
    selected_date: date,
    swap_applied: bool,
    detector: dict[str, str] | None = None,
) -> dict[str, Any]:
    day_start = datetime.combine(selected_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    overnight_end = day_end + timedelta(hours=settings.shift_out_cutoff_hours)

    events = _fetch_events_for_card(
        card_no=card_no,
        start=day_start,
        end=overnight_end,
        detector=detector,
    )
    day_record = _compute_day_attendance(
        day_start=day_start,
        day_events=events,
        swap_applied=swap_applied,
    )
    return _serialize_day_record(day_record)


def fetch_daily_report(card_no: str, date_value: str) -> Dict[str, Any]:
    selected_date = _parse_date(date_value)
    detector = _detect_event_variant(event_alias="e", event_type_alias="et")
    mapping = _get_mapping_state(detector=detector)
    identity = _fetch_employee_identity(card_no)

    day_record = _build_daily_transactions_and_intervals(
        card_no=card_no,
        selected_date=selected_date,
        swap_applied=bool(mapping["swapApplied"]),
        detector=detector,
    )

    return {
        "employee_name": identity["employee_name"],
        "card_no": identity["card_no"],
        "department": identity["department"],
        "date": day_record["date"],
        "first_in": day_record["first_in"],
        "last_out": day_record["last_out"],
        "duration_minutes": day_record["duration_minutes"],
        "duration_hhmm": day_record["duration_hhmm"],
        "duration": day_record["duration_hhmm"],
        "missing_punch": day_record["missing_punch"],
        "rows": day_record["rows"],
        "transactions": day_record["transactions"],
        "intervals": day_record["intervals"],
        "total_in_minutes": day_record["total_in_minutes"],
        "total_out_minutes": day_record["total_out_minutes"],
        "total_in": day_record["total_in"],
        "total_out": day_record["total_out"],
        "totalInMinutes": day_record["totalInMinutes"],
        "totalOutMinutes": day_record["totalOutMinutes"],
        "totalInHHMM": day_record["totalInHHMM"],
        "totalOutHHMM": day_record["totalOutHHMM"],
        "notes": day_record["notes"],
        "total_work_minutes": day_record["duration_minutes"],
        "mappingVariant": mapping["mappingVariant"],
        "swapApplied": mapping["swapApplied"],
    }


def fetch_monthly_report(card_no: str, month_value: str) -> Dict[str, Any]:
    start, end, normalized_month = _month_bounds(month_value)
    detector = _detect_event_variant(event_alias="e", event_type_alias="et")
    mapping = _get_mapping_state(detector=detector)
    identity = _fetch_employee_identity(card_no)

    records = _build_daily_records_for_period(
        card_no=card_no,
        start=start,
        end=end,
        swap_applied=bool(mapping["swapApplied"]),
        detector=detector,
    )
    period_totals = _compute_period_segment_totals(
        card_no=card_no,
        start=start,
        end=end,
        swap_applied=bool(mapping["swapApplied"]),
        detector=detector,
    )

    total_minutes = sum(item["duration_minutes"] or 0 for item in records)
    total_days = sum(1 for item in records if item.get("first_in"))
    missing_punch_days = sum(1 for item in records if bool(item.get("missing_punch")))

    return {
        "employee_name": identity["employee_name"],
        "card_no": identity["card_no"],
        "department": identity["department"],
        "month": normalized_month,
        "records": records,
        "total_days": total_days,
        "missing_punch_days": missing_punch_days,
        "total_minutes": total_minutes,
        "total_duration_hhmm": _minutes_to_hhmm(total_minutes),
        "total_duration_readable": format_duration_readable(total_minutes),
        "totalInMinutes": period_totals["totalInMinutes"],
        "totalOutMinutes": period_totals["totalOutMinutes"],
        "totalInHHMM": period_totals["totalInHHMM"],
        "totalOutHHMM": period_totals["totalOutHHMM"],
        "total_work_minutes": total_minutes,
        "mappingVariant": mapping["mappingVariant"],
        "swapApplied": mapping["swapApplied"],
    }


def fetch_yearly_report(card_no: str, year_value: str) -> Dict[str, Any]:
    start, end, normalized_year = _year_bounds(year_value)
    detector = _detect_event_variant(event_alias="e", event_type_alias="et")
    mapping = _get_mapping_state(detector=detector)
    identity = _fetch_employee_identity(card_no)

    daily_records = _build_daily_records_for_period(
        card_no=card_no,
        start=start,
        end=end,
        swap_applied=bool(mapping["swapApplied"]),
        detector=detector,
    )
    period_totals = _compute_period_segment_totals(
        card_no=card_no,
        start=start,
        end=end,
        swap_applied=bool(mapping["swapApplied"]),
        detector=detector,
    )

    month_map: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "worked_days": 0,
            "duration_days": 0,
            "missing_punch_days": 0,
            "total_minutes": 0,
        }
    )

    for record in daily_records:
        month_key = record["date"][:7]

        if record.get("first_in"):
            month_map[month_key]["worked_days"] += 1

        if record.get("missing_punch"):
            month_map[month_key]["missing_punch_days"] += 1

        duration = record["duration_minutes"]
        if duration is None:
            continue

        month_map[month_key]["duration_days"] += 1
        month_map[month_key]["total_minutes"] += duration

    months: List[Dict[str, Any]] = []
    total_worked_days = 0
    total_missing_punch_days = 0
    total_minutes = 0

    for month_key in sorted(month_map.keys()):
        worked_days = month_map[month_key]["worked_days"]
        duration_days = month_map[month_key]["duration_days"]
        missing_punch_days = month_map[month_key]["missing_punch_days"]
        month_minutes = month_map[month_key]["total_minutes"]

        total_worked_days += worked_days
        total_missing_punch_days += missing_punch_days
        total_minutes += month_minutes

        average = int(month_minutes / duration_days) if duration_days else None

        months.append(
            {
                "month": month_key,
                "worked_days": worked_days,
                "missing_punch_days": missing_punch_days,
                "total_minutes": month_minutes,
                "average_minutes_per_day": average,
                "average_duration_hhmm": _minutes_to_hhmm(average),
                "total_duration_hhmm": _minutes_to_hhmm(month_minutes),
                "total_duration_readable": format_duration_readable(month_minutes),
            }
        )

    return {
        "employee_name": identity["employee_name"],
        "card_no": identity["card_no"],
        "department": identity["department"],
        "year": normalized_year,
        "months": months,
        "total_worked_days": total_worked_days,
        "missing_punch_days": total_missing_punch_days,
        "total_minutes": total_minutes,
        "total_duration_hhmm": _minutes_to_hhmm(total_minutes),
        "total_duration_readable": format_duration_readable(total_minutes),
        "totalInMinutes": period_totals["totalInMinutes"],
        "totalOutMinutes": period_totals["totalOutMinutes"],
        "totalInHHMM": period_totals["totalInHHMM"],
        "totalOutHHMM": period_totals["totalOutHHMM"],
        "total_work_minutes": total_minutes,
        "mappingVariant": mapping["mappingVariant"],
        "swapApplied": mapping["swapApplied"],
    }


def _sorted_employee_rows_for_all(employees: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [dict(item) for item in employees],
        key=lambda item: (
            _clean_text(item.get("employee_name")).lower(),
            _clean_text(item.get("card_no")),
        ),
    )


def fetch_daily_report_all_employees(date_value: str) -> Dict[str, Any]:
    selected_date = _parse_date(date_value)
    detector = _detect_event_variant(event_alias="e", event_type_alias="et")
    mapping = _get_mapping_state(detector=detector)
    swap_applied = bool(mapping["swapApplied"])

    employees = _sorted_employee_rows_for_all(_fetch_all_active_employees())

    day_start = datetime.combine(selected_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    window_start = day_start - timedelta(hours=12)
    window_end = day_end + timedelta(hours=12)
    events_by_card = _fetch_all_active_events(start=window_start, end=window_end, detector=detector)

    rows: list[dict[str, Any]] = []
    total_in_minutes = 0
    total_out_minutes = 0
    total_duration_minutes = 0
    total_sessions = 0
    working_rows = 0
    missing_punch_count = 0

    for employee in employees:
        card_no = str(employee.get("card_no") or "").strip()
        if not card_no:
            continue

        daily = _build_daily_transactions_and_intervals_from_events(
            selected_date=selected_date,
            raw_window_events=events_by_card.get(card_no, []),
            swap_applied=swap_applied,
        )
        sessions_count = len(daily.get("rows") or [])
        duration_minutes = _to_int(daily.get("duration_minutes"))
        in_minutes = _to_int(daily.get("totalInMinutes")) or _to_int(daily.get("total_in_minutes")) or 0
        out_minutes = _to_int(daily.get("totalOutMinutes")) or _to_int(daily.get("total_out_minutes")) or 0
        missing_punch = bool(daily.get("missing_punch"))

        if duration_minutes is not None:
            total_duration_minutes += duration_minutes
            working_rows += 1
        if missing_punch:
            missing_punch_count += 1

        total_in_minutes += max(0, in_minutes)
        total_out_minutes += max(0, out_minutes)
        total_sessions += max(0, sessions_count)

        rows.append(
            {
                "employee_name": employee.get("employee_name") or card_no,
                "card_no": card_no,
                "department": employee.get("department"),
                "first_in": daily.get("first_in"),
                "last_out": daily.get("last_out"),
                "duration_minutes": duration_minutes,
                "duration_hhmm": daily.get("duration_hhmm"),
                "total_in_minutes": in_minutes,
                "total_out_minutes": out_minutes,
                "total_in_hhmm": daily.get("totalInHHMM") or daily.get("total_in"),
                "total_out_hhmm": daily.get("totalOutHHMM") or daily.get("total_out"),
                "sessions_count": sessions_count,
                "missing_punch": missing_punch,
            }
        )

    return {
        "date": selected_date.strftime("%Y-%m-%d"),
        "rows": rows,
        "summary": {
            "total_employees": len(employees),
            "total_working_days": working_rows,
            "total_in_minutes": total_in_minutes,
            "total_out_minutes": total_out_minutes,
            "total_duration_minutes": total_duration_minutes,
            "total_sessions": total_sessions,
            "missing_punch_count": missing_punch_count,
            "total_in_hhmm": _minutes_to_hhmm(total_in_minutes),
            "total_out_hhmm": _minutes_to_hhmm(total_out_minutes),
            "total_duration_hhmm": _minutes_to_hhmm(total_duration_minutes),
            "total_duration_readable": format_duration_readable(total_duration_minutes),
        },
        "mappingVariant": mapping["mappingVariant"],
        "swapApplied": mapping["swapApplied"],
    }


def fetch_monthly_report_all_employees(month_value: str) -> Dict[str, Any]:
    start, end, normalized_month = _month_bounds(month_value)
    detector = _detect_event_variant(event_alias="e", event_type_alias="et")
    mapping = _get_mapping_state(detector=detector)
    swap_applied = bool(mapping["swapApplied"])

    employees = _sorted_employee_rows_for_all(_fetch_all_active_employees())
    window_start = start - timedelta(hours=12)
    window_end = end + timedelta(days=1, hours=settings.shift_out_cutoff_hours)
    events_by_card = _fetch_all_active_events(start=window_start, end=window_end, detector=detector)

    rows: list[dict[str, Any]] = []
    total_in_minutes = 0
    total_out_minutes = 0
    total_work_minutes = 0
    total_working_days = 0
    total_missing_punch = 0
    total_sessions = 0

    for employee in employees:
        card_no = str(employee.get("card_no") or "").strip()
        if not card_no:
            continue

        events = events_by_card.get(card_no, [])
        records = _build_daily_records_for_period_from_events(
            events=events,
            start=start,
            end=end,
            swap_applied=swap_applied,
        )
        period_totals = _compute_period_segment_totals_from_events(
            events=events,
            start=start,
            end=end,
            swap_applied=swap_applied,
        )
        sessions = _count_sessions_from_events(
            events=events,
            window_start=start,
            window_end=end + timedelta(hours=settings.shift_out_cutoff_hours),
            swap_applied=swap_applied,
        )

        working_days = sum(1 for item in records if item.get("first_in"))
        missing_punch_days = sum(1 for item in records if bool(item.get("missing_punch")))
        total_minutes = sum(_to_int(item.get("duration_minutes")) or 0 for item in records)
        average_minutes = int(total_minutes / working_days) if working_days > 0 else 0

        in_minutes = _to_int(period_totals.get("totalInMinutes")) or 0
        out_minutes = _to_int(period_totals.get("totalOutMinutes")) or 0

        total_working_days += working_days
        total_missing_punch += missing_punch_days
        total_work_minutes += total_minutes
        total_in_minutes += in_minutes
        total_out_minutes += out_minutes
        total_sessions += max(0, sessions)

        rows.append(
            {
                "employee_name": employee.get("employee_name") or card_no,
                "card_no": card_no,
                "department": employee.get("department"),
                "working_days": working_days,
                "total_minutes": total_minutes,
                "total_duration_hhmm": _minutes_to_hhmm(total_minutes),
                "total_duration_readable": format_duration_readable(total_minutes),
                "avg_minutes_per_day": average_minutes,
                "avg_duration_hhmm": _minutes_to_hhmm(average_minutes),
                "missing_punch_days": missing_punch_days,
                "sessions_count": sessions,
                "total_in_minutes": in_minutes,
                "total_out_minutes": out_minutes,
                "total_in_hhmm": period_totals.get("totalInHHMM"),
                "total_out_hhmm": period_totals.get("totalOutHHMM"),
            }
        )

    return {
        "month": normalized_month,
        "rows": rows,
        "summary": {
            "total_employees": len(employees),
            "total_working_days": total_working_days,
            "total_in_minutes": total_in_minutes,
            "total_out_minutes": total_out_minutes,
            "total_work_minutes": total_work_minutes,
            "total_sessions": total_sessions,
            "missing_punch_count": total_missing_punch,
            "total_in_hhmm": _minutes_to_hhmm(total_in_minutes),
            "total_out_hhmm": _minutes_to_hhmm(total_out_minutes),
            "total_work_hhmm": _minutes_to_hhmm(total_work_minutes),
            "total_work_readable": format_duration_readable(total_work_minutes),
        },
        "mappingVariant": mapping["mappingVariant"],
        "swapApplied": mapping["swapApplied"],
    }


def fetch_yearly_report_all_employees(year_value: str) -> Dict[str, Any]:
    start, end, normalized_year = _year_bounds(year_value)
    detector = _detect_event_variant(event_alias="e", event_type_alias="et")
    mapping = _get_mapping_state(detector=detector)
    swap_applied = bool(mapping["swapApplied"])

    employees = _sorted_employee_rows_for_all(_fetch_all_active_employees())
    window_start = start - timedelta(hours=12)
    window_end = end + timedelta(days=1, hours=settings.shift_out_cutoff_hours)
    events_by_card = _fetch_all_active_events(start=window_start, end=window_end, detector=detector)

    rows: list[dict[str, Any]] = []
    total_in_minutes = 0
    total_out_minutes = 0
    total_work_minutes = 0
    total_working_days = 0
    total_missing_punch = 0
    total_sessions = 0

    for employee in employees:
        card_no = str(employee.get("card_no") or "").strip()
        if not card_no:
            continue

        events = events_by_card.get(card_no, [])
        records = _build_daily_records_for_period_from_events(
            events=events,
            start=start,
            end=end,
            swap_applied=swap_applied,
        )
        period_totals = _compute_period_segment_totals_from_events(
            events=events,
            start=start,
            end=end,
            swap_applied=swap_applied,
        )
        sessions = _count_sessions_from_events(
            events=events,
            window_start=start,
            window_end=end + timedelta(hours=settings.shift_out_cutoff_hours),
            swap_applied=swap_applied,
        )

        working_days = sum(1 for item in records if item.get("first_in"))
        missing_punch_days = sum(1 for item in records if bool(item.get("missing_punch")))
        total_minutes = sum(_to_int(item.get("duration_minutes")) or 0 for item in records)
        average_minutes = int(total_minutes / working_days) if working_days > 0 else 0

        in_minutes = _to_int(period_totals.get("totalInMinutes")) or 0
        out_minutes = _to_int(period_totals.get("totalOutMinutes")) or 0

        total_working_days += working_days
        total_missing_punch += missing_punch_days
        total_work_minutes += total_minutes
        total_in_minutes += in_minutes
        total_out_minutes += out_minutes
        total_sessions += max(0, sessions)

        rows.append(
            {
                "employee_name": employee.get("employee_name") or card_no,
                "card_no": card_no,
                "department": employee.get("department"),
                "working_days": working_days,
                "total_minutes": total_minutes,
                "total_duration_hhmm": _minutes_to_hhmm(total_minutes),
                "total_duration_readable": format_duration_readable(total_minutes),
                "avg_minutes_per_day": average_minutes,
                "avg_duration_hhmm": _minutes_to_hhmm(average_minutes),
                "missing_punch_days": missing_punch_days,
                "sessions_count": sessions,
                "total_in_minutes": in_minutes,
                "total_out_minutes": out_minutes,
                "total_in_hhmm": period_totals.get("totalInHHMM"),
                "total_out_hhmm": period_totals.get("totalOutHHMM"),
            }
        )

    return {
        "year": normalized_year,
        "rows": rows,
        "summary": {
            "total_employees": len(employees),
            "total_working_days": total_working_days,
            "total_in_minutes": total_in_minutes,
            "total_out_minutes": total_out_minutes,
            "total_work_minutes": total_work_minutes,
            "total_sessions": total_sessions,
            "missing_punch_count": total_missing_punch,
            "total_in_hhmm": _minutes_to_hhmm(total_in_minutes),
            "total_out_hhmm": _minutes_to_hhmm(total_out_minutes),
            "total_work_hhmm": _minutes_to_hhmm(total_work_minutes),
            "total_work_readable": format_duration_readable(total_work_minutes),
        },
        "mappingVariant": mapping["mappingVariant"],
        "swapApplied": mapping["swapApplied"],
    }


def quick_daily_sequence_sanity() -> dict[str, dict[str, Any]]:
    """
    Lightweight self-check helper for interval pairing logic.
    Not executed automatically; call manually in a REPL if needed.
    """

    def _simulate(events: list[tuple[str, int]]) -> dict[str, Any]:
        parsed: list[tuple[datetime, int]] = [
            (datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S"), state) for stamp, state in events
        ]
        parsed.sort(key=lambda item: item[0])

        open_in: datetime | None = None
        last_out: datetime | None = None
        total_in = 0
        total_out = 0

        for event_time, state in parsed:
            if state == 1:
                if last_out is not None and event_time > last_out:
                    total_out += int((event_time - last_out).total_seconds() // 60)
                    last_out = None
                if open_in is None:
                    open_in = event_time
                continue

            if open_in is None:
                last_out = event_time
                continue

            total_in += int((event_time - open_in).total_seconds() // 60)
            open_in = None
            last_out = event_time

        return {
            "total_in_minutes": total_in,
            "total_out_minutes": total_out,
            "missing_out": open_in is not None,
        }

    normal = _simulate(
        [
            ("2026-02-23 08:00:00", 1),
            ("2026-02-23 09:00:00", 0),
            ("2026-02-23 09:30:00", 1),
            ("2026-02-23 12:00:00", 0),
        ]
    )
    assert normal["total_in_minutes"] == 210  # 03:30
    assert normal["total_out_minutes"] == 30  # 00:30

    night = _simulate(
        [
            ("2026-02-23 21:40:00", 1),
            ("2026-02-24 04:00:00", 0),
        ]
    )
    assert night["total_in_minutes"] == 380  # 06:20
    assert night["total_out_minutes"] == 0

    missing = _simulate(
        [
            ("2026-02-23 09:00:00", 1),
            ("2026-02-23 12:00:00", 0),
            ("2026-02-23 13:00:00", 1),
        ]
    )
    assert missing["total_in_minutes"] == 180
    assert missing["missing_out"] is True

    return {
        "normal_shift": normal,
        "night_shift": night,
        "missing_punch": missing,
    }
