from __future__ import annotations

import os
import re
from datetime import datetime
from io import BytesIO
from typing import Any, Sequence
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas
from reportlab.platypus import Flowable, LongTable, Paragraph, SimpleDocTemplate, Spacer, TableStyle

LEFT_MARGIN = 14 * mm
RIGHT_MARGIN = 14 * mm
BOTTOM_MARGIN = 16 * mm
PAGE_WIDTH, PAGE_HEIGHT = A4
CONTENT_WIDTH = PAGE_WIDTH - LEFT_MARGIN - RIGHT_MARGIN

HEADER_TOP_MARGIN = 36.0
HEADER_HEIGHT = 80.0
HEADER_LEFT_PADDING = 42.0
HEADER_RIGHT_PADDING = 42.0
HEADER_LOGO_SIZE = 38.0
HEADER_AFTER_GAP = 12.0
HEADER_TITLE_FONT_SIZE = 22.0
HEADER_SUBTITLE_FONT_SIZE = 11.0

KPI_GAP_X = 10.0
KPI_GAP_Y = 11.0
KPI_GAP_AFTER = 16.0
KPI_CARD_HEIGHT = 64.0
KPI_CARD_RADIUS = 7.0
KPI_CARD_PAD_X = 10.0
KPI_CARD_PAD_Y = 10.0
KPI_VALUE_FONT_SIZE = 12
KPI_LABEL_FONT_SIZE = 8

PDF_LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "logo.png")

PALETTE = {
    "navy": colors.HexColor("#0F172A"),
    "text": colors.HexColor("#0F172A"),
    "muted": colors.HexColor("#64748B"),
    "line": colors.HexColor("#D1D5DB"),
    "card_fill": colors.HexColor("#F8FAFC"),
    "card_stroke": colors.HexColor("#CBD5E1"),
    "stripe_even": colors.HexColor("#F8FAFC"),
    "stripe_odd": colors.HexColor("#F1F5F9"),
    "grid": colors.HexColor("#E2E8F0"),
    "totals": colors.HexColor("#E5ECF5"),
}

_STYLES = getSampleStyleSheet()
SECTION_HEADING_STYLE = ParagraphStyle(
    "section-heading",
    parent=_STYLES["Heading5"],
    fontName="Helvetica-Bold",
    fontSize=11,
    leading=13,
    textColor=PALETTE["navy"],
    spaceAfter=4,
)
TABLE_HEADER_STYLE = ParagraphStyle(
    "table-header",
    parent=_STYLES["BodyText"],
    fontName="Helvetica-Bold",
    fontSize=9,
    leading=11,
    textColor=colors.white,
    wordWrap="CJK",
)
TABLE_CELL_STYLE = ParagraphStyle(
    "table-cell",
    parent=_STYLES["BodyText"],
    fontName="Helvetica",
    fontSize=8.6,
    leading=10,
    textColor=PALETTE["text"],
    wordWrap="CJK",
)
TABLE_TOTAL_LABEL_STYLE = ParagraphStyle(
    "table-total-label",
    parent=TABLE_CELL_STYLE,
    fontName="Helvetica-Bold",
    textColor=PALETTE["navy"],
)
TABLE_TOTAL_VALUE_STYLE = ParagraphStyle(
    "table-total-value",
    parent=TABLE_CELL_STYLE,
    fontName="Helvetica-Bold",
    textColor=PALETTE["navy"],
)


def _safe_text(value: Any, *, fallback: str = "-") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


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


def _minutes_to_hhmm(minutes: int | None) -> str | None:
    if minutes is None or minutes < 0:
        return None
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def _minutes_to_readable(minutes: int | None) -> str:
    if minutes is None or minutes < 0:
        return "N/A"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours} Hrs {mins:02d} Mins"


def _hhmm_to_minutes(value: Any) -> int | None:
    text = _safe_text(value, fallback="")
    if not text:
        return None

    match = re.match(r"^(\d+):(\d{2})$", text)
    if not match:
        return None

    try:
        hours = int(match.group(1))
        mins = int(match.group(2))
    except ValueError:
        return None
    return (hours * 60) + mins


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    if not text:
        return None

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %I:%M:%S %p",
        "%I:%M:%S %p",
        "%H:%M:%S",
        "%H:%M",
    )

    for pattern in formats:
        try:
            parsed = datetime.strptime(text, pattern)
            if pattern in {"%I:%M:%S %p", "%H:%M:%S", "%H:%M"}:
                # Time-only values are not enough for date-time output.
                return parsed
            return parsed
        except ValueError:
            continue
    return None


def _format_dt_12h(value: Any) -> str:
    parsed = _parse_timestamp(value)
    if not parsed:
        return _safe_text(value)
    return parsed.strftime("%Y-%m-%d %I:%M:%S %p")


def _format_time_12h(value: Any) -> str:
    parsed = _parse_timestamp(value)
    if not parsed:
        return _safe_text(value)
    return parsed.strftime("%I:%M:%S %p")


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        cleaned = value.strip()
        return bool(cleaned and cleaned != "-")
    return True


def resolve_logo_path() -> str | None:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    candidates = [
        os.path.join(project_root, "backend", "static", "oilchem_logo.png"),
        os.path.join(project_root, "frontend", "public", "oilchem_logo.png"),
        os.path.join(project_root, "frontend", "public", "logo.png"),
        os.path.join(project_root, "frontend", "public", "Logo(white).png"),
        PDF_LOGO_PATH,
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return None


def _fit_text(canv: canvas.Canvas, text: str, *, font_name: str, font_size: float, max_width: float) -> str:
    if max_width <= 0:
        return ""

    cleaned = _safe_text(text, fallback="")
    if not cleaned:
        return ""

    if canv.stringWidth(cleaned, font_name, font_size) <= max_width:
        return cleaned

    suffix = "..."
    clipped = cleaned
    while clipped and canv.stringWidth(clipped + suffix, font_name, font_size) > max_width:
        clipped = clipped[:-1]
    return (clipped + suffix) if clipped else suffix


def _cell_paragraph(value: Any, *, style: ParagraphStyle, fallback: str = "-") -> Paragraph:
    text = _safe_text(value, fallback=fallback)
    return Paragraph(escape(text), style)


def _draw_logo_in_square(canv: canvas.Canvas, *, logo_path: str | None, x: float, y: float, size: float) -> None:
    canv.saveState()
    canv.setFillColor(colors.HexColor("#EEF3FA"))
    canv.roundRect(x - 4, y - 4, size + 8, size + 8, 6, stroke=0, fill=1)

    if logo_path and os.path.exists(logo_path):
        try:
            reader = ImageReader(logo_path)
            image_width, image_height = reader.getSize()
            if image_width > 0 and image_height > 0:
                scale = min(size / float(image_width), size / float(image_height))
                draw_w = float(image_width) * scale
                draw_h = float(image_height) * scale
                draw_x = x + ((size - draw_w) / 2.0)
                draw_y = y + ((size - draw_h) / 2.0)
                canv.drawImage(
                    reader,
                    draw_x,
                    draw_y,
                    width=draw_w,
                    height=draw_h,
                    preserveAspectRatio=False,
                    mask="auto",
                )
        except Exception:
            pass
    canv.restoreState()


def draw_header(
    canv: canvas.Canvas,
    page_width: float,
    page_height: float,
    *,
    title: str,
    subtitle_lines: Sequence[str],
    logo_path: str | None,
) -> None:
    header_top = page_height - HEADER_TOP_MARGIN
    header_bottom = header_top - HEADER_HEIGHT
    mid_y = header_bottom + (HEADER_HEIGHT / 2.0)

    logo_x = HEADER_LEFT_PADDING
    logo_y = mid_y - (HEADER_LOGO_SIZE / 2.0)
    reserved_left = logo_x + HEADER_LOGO_SIZE + 16.0

    canv.saveState()
    _draw_logo_in_square(canv, logo_path=logo_path, x=logo_x, y=logo_y, size=HEADER_LOGO_SIZE)

    title_font = "Helvetica-Bold"
    title_size = HEADER_TITLE_FONT_SIZE
    title_text = _safe_text(title, fallback="Oilchem Attendance Report")

    title_width = canv.stringWidth(title_text, title_font, title_size)
    title_x = (page_width - title_width) / 2.0

    if title_x < reserved_left:
        title_size = 20.0
        title_width = canv.stringWidth(title_text, title_font, title_size)
        title_x = (page_width - title_width) / 2.0
        if title_x < reserved_left:
            title_x = reserved_left
            max_title_width = max((page_width - HEADER_RIGHT_PADDING) - title_x, 0.0)
            title_text = _fit_text(
                canv,
                title_text,
                font_name=title_font,
                font_size=title_size,
                max_width=max_title_width,
            )

    title_baseline_y = mid_y + (title_size * 0.35)
    canv.setFillColor(PALETTE["text"])
    canv.setFont(title_font, title_size)
    canv.drawString(title_x, title_baseline_y, title_text)

    subtitle_font = "Helvetica"
    subtitle_size = HEADER_SUBTITLE_FONT_SIZE
    subtitle_leading = 14.0
    subtitle_y = title_baseline_y - (title_size * 0.95)

    canv.setFillColor(PALETTE["muted"])
    canv.setFont(subtitle_font, subtitle_size)
    for line in list(subtitle_lines)[:2]:
        subtitle_text = _safe_text(line, fallback="")
        subtitle_width = canv.stringWidth(subtitle_text, subtitle_font, subtitle_size)
        subtitle_x = (page_width - subtitle_width) / 2.0
        if subtitle_x < reserved_left:
            subtitle_x = reserved_left
            max_subtitle_width = max((page_width - HEADER_RIGHT_PADDING) - subtitle_x, 0.0)
            subtitle_text = _fit_text(
                canv,
                subtitle_text,
                font_name=subtitle_font,
                font_size=subtitle_size,
                max_width=max_subtitle_width,
            )
        canv.drawString(subtitle_x, subtitle_y, subtitle_text)
        subtitle_y -= subtitle_leading

    canv.setStrokeColor(PALETTE["line"])
    canv.setLineWidth(0.6)
    canv.line(HEADER_LEFT_PADDING, header_bottom, page_width - HEADER_RIGHT_PADDING, header_bottom)
    canv.restoreState()


class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._saved_page_states: list[dict[str, Any]] = []

    def showPage(self) -> None:
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self) -> None:
        total_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self._draw_footer(total_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def _draw_footer(self, total_pages: int) -> None:
        line_y = 11 * mm
        text_y = 7.6 * mm

        self.saveState()
        self.setStrokeColor(PALETTE["line"])
        self.setLineWidth(0.5)
        self.line(LEFT_MARGIN, line_y, PAGE_WIDTH - RIGHT_MARGIN, line_y)

        self.setFillColor(PALETTE["muted"])
        self.setFont("Helvetica", 8)
        self.drawString(LEFT_MARGIN, text_y, "Generated by Oilchem Entry/Exit Dashboard")
        self.drawRightString(PAGE_WIDTH - RIGHT_MARGIN, text_y, f"Page {self._pageNumber} of {total_pages}")
        self.restoreState()


class KpiRow(Flowable):
    def __init__(self, *, cards: Sequence[dict[str, str]], cols: int, gap: float = KPI_GAP_X, card_h: float = KPI_CARD_HEIGHT) -> None:
        super().__init__()
        self.cards = list(cards)
        self.cols = max(1, cols)
        self.gap = gap
        self.card_h = card_h
        self.width = CONTENT_WIDTH
        self.height = card_h

    def wrap(self, avail_width: float, avail_height: float) -> tuple[float, float]:
        return self.width, self.height

    def draw(self) -> None:
        card_w = (self.width - (self.gap * (self.cols - 1))) / self.cols
        draw_count = min(len(self.cards), self.cols)

        canv = self.canv
        canv.saveState()
        for index in range(draw_count):
            x = index * (card_w + self.gap)
            card = self.cards[index]
            value = _safe_text(card.get("value"), fallback="N/A")
            label = _safe_text(card.get("label"), fallback="-")
            max_text_width = max(0.0, card_w - (KPI_CARD_PAD_X * 2))

            value_font = KPI_VALUE_FONT_SIZE
            while value_font > 9 and canv.stringWidth(value, "Helvetica-Bold", value_font) > max_text_width:
                value_font -= 0.5
            value_draw = _fit_text(
                canv,
                value,
                font_name="Helvetica-Bold",
                font_size=value_font,
                max_width=max_text_width,
            )
            label_draw = _fit_text(
                canv,
                label,
                font_name="Helvetica",
                font_size=KPI_LABEL_FONT_SIZE,
                max_width=max_text_width,
            )

            canv.setFillColor(PALETTE["card_fill"])
            canv.setStrokeColor(PALETTE["card_stroke"])
            canv.setLineWidth(0.8)
            canv.roundRect(x, 0, card_w, self.card_h, KPI_CARD_RADIUS, stroke=1, fill=1)

            canv.setFillColor(PALETTE["text"])
            canv.setFont("Helvetica-Bold", value_font)
            canv.drawString(x + KPI_CARD_PAD_X, self.card_h - KPI_CARD_PAD_Y - value_font, value_draw)

            canv.setFillColor(PALETTE["muted"])
            canv.setFont("Helvetica", KPI_LABEL_FONT_SIZE)
            canv.drawString(x + KPI_CARD_PAD_X, KPI_CARD_PAD_Y, label_draw)
        canv.restoreState()


def _split_kpi_cards(cards: Sequence[dict[str, str]], *, max_cols: int) -> list[list[dict[str, str]]]:
    items = list(cards)
    if not items:
        return []
    if len(items) <= max_cols:
        return [items]

    if len(items) == max_cols + 1:
        first_count = (len(items) + 1) // 2
        return [items[:first_count], items[first_count:]]

    rows: list[list[dict[str, str]]] = []
    index = 0
    total = len(items)
    while index < total:
        remaining = total - index
        if remaining > max_cols and remaining < (max_cols * 2):
            row_count = (remaining + 1) // 2
        else:
            row_count = min(max_cols, remaining)
        rows.append(items[index : index + row_count])
        index += row_count
    return rows


def _append_kpi_grid(
    story: list[Any],
    *,
    cards: Sequence[dict[str, str]],
    max_cols: int = 4,
    gap: float = KPI_GAP_X,
    card_h: float = KPI_CARD_HEIGHT,
    row_gap: float = KPI_GAP_Y,
) -> None:
    rows = _split_kpi_cards(cards, max_cols=max_cols)
    for row_index, row_cards in enumerate(rows):
        story.append(KpiRow(cards=row_cards, cols=max(1, len(row_cards)), gap=gap, card_h=card_h))
        if row_index < (len(rows) - 1):
            story.append(Spacer(1, row_gap))


def _build_section_heading(text: str) -> Paragraph:
    return Paragraph(_safe_text(text, fallback="Section"), SECTION_HEADING_STYLE)


def _build_table(
    *,
    headers: Sequence[str],
    body_rows: Sequence[Sequence[str]],
    col_widths: Sequence[float],
    total_rows: Sequence[tuple[str, str]] | None = None,
    total_span_end: int | None = None,
) -> LongTable:
    header = [_cell_paragraph(cell, style=TABLE_HEADER_STYLE, fallback="") for cell in headers]
    table_data: list[list[Any]] = [header]

    if body_rows:
        for row in body_rows:
            normalized = [_cell_paragraph(cell, style=TABLE_CELL_STYLE, fallback="-") for cell in row]
            if len(normalized) < len(headers):
                normalized.extend(
                    [
                        _cell_paragraph("", style=TABLE_CELL_STYLE, fallback="")
                        for _ in range(len(headers) - len(normalized))
                    ]
                )
            table_data.append(normalized[: len(headers)])
    else:
        fallback_row = [_cell_paragraph("No data", style=TABLE_CELL_STYLE)] + [
            _cell_paragraph("", style=TABLE_CELL_STYLE, fallback="")
            for _ in range(len(headers) - 1)
        ]
        table_data.append(fallback_row)

    total_start = -1
    if total_rows:
        total_start = len(table_data)
        for label, value in total_rows:
            row = [_cell_paragraph("", style=TABLE_CELL_STYLE, fallback="") for _ in headers]
            row[0] = _cell_paragraph(label, style=TABLE_TOTAL_LABEL_STYLE, fallback="-")
            row[-1] = _cell_paragraph(value, style=TABLE_TOTAL_VALUE_STYLE, fallback="N/A")
            table_data.append(row)

    table = LongTable(table_data, colWidths=list(col_widths), repeatRows=1, hAlign="LEFT")

    style_commands: list[tuple[Any, ...]] = [
        ("BACKGROUND", (0, 0), (-1, 0), PALETTE["navy"]),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 0.4, PALETTE["grid"]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]

    body_end = len(table_data) - 1
    if total_start >= 0:
        body_end = total_start - 1

    for row_index in range(1, max(2, body_end + 1)):
        if row_index > body_end:
            break
        background = PALETTE["stripe_even"] if row_index % 2 else PALETTE["stripe_odd"]
        style_commands.append(("BACKGROUND", (0, row_index), (-1, row_index), background))

    if total_start >= 0:
        span_end = total_span_end if total_span_end is not None else len(headers) - 2
        span_end = max(0, min(span_end, len(headers) - 2))
        for row_index in range(total_start, len(table_data)):
            style_commands.extend(
                [
                    ("BACKGROUND", (0, row_index), (-1, row_index), PALETTE["totals"]),
                    ("ALIGN", (0, row_index), (-1, row_index), "LEFT"),
                ]
            )
            if span_end >= 1:
                style_commands.append(("SPAN", (0, row_index), (span_end, row_index)))

    table.setStyle(TableStyle(style_commands))
    return table


def _month_label(month_value: str) -> str:
    text = _safe_text(month_value, fallback="")
    if re.match(r"^\d{4}-\d{2}$", text):
        try:
            parsed = datetime.strptime(text, "%Y-%m")
            return parsed.strftime("%b %Y")
        except ValueError:
            return text
    return text


def _table_rows_from_daily_rows_payload(report: dict[str, Any]) -> tuple[list[list[str]], int]:
    date_fallback = _safe_text(report.get("date"), fallback="-")
    rows_payload = report.get("rows", []) or []

    rows: list[list[str]] = []
    sessions = 0

    for row in rows_payload:
        date_value = _safe_text(row.get("date"), fallback=date_fallback)

        in_raw = row.get("in_raw")
        out_raw = row.get("out_raw")

        in_text = _safe_text(row.get("in"), fallback="")
        out_text = _safe_text(row.get("out"), fallback="")

        if _has_value(in_raw):
            in_text = _format_time_12h(in_raw)
        if _has_value(out_raw):
            out_text = _format_time_12h(out_raw)

        if not in_text or in_text == "-":
            in_text = "-"

        missing_out = not out_text or out_text == "-" or "missing" in out_text.lower()
        if missing_out:
            out_text = "Missing OUT"

        duration_text = _safe_text(row.get("duration"), fallback="-")
        if duration_text == "-":
            minutes = _to_int(row.get("duration_minutes"))
            duration_text = _safe_text(_minutes_to_hhmm(minutes), fallback="-")

        rows.append([date_value, in_text, out_text, duration_text])
        if not missing_out:
            sessions += 1

    if rows:
        return rows, sessions

    # Fallback: derive paired rows from transaction sequence if explicit rows are not present.
    transactions = report.get("transactions", []) or []
    parsed_tx: list[tuple[datetime, str]] = []

    parsed_date = _parse_timestamp(date_fallback) or datetime.strptime(date_fallback, "%Y-%m-%d")
    for index, tx in enumerate(transactions):
        tx_type = _safe_text(tx.get("type"), fallback="").upper()
        if tx_type not in {"IN", "OUT"}:
            continue

        tx_stamp = tx.get("timestamp")
        parsed_stamp = _parse_timestamp(tx_stamp)
        if parsed_stamp is None:
            tx_time = _safe_text(tx.get("time"), fallback="")
            parsed_time = _parse_timestamp(tx_time)
            if parsed_time is not None:
                parsed_stamp = parsed_date.replace(
                    hour=parsed_time.hour,
                    minute=parsed_time.minute,
                    second=parsed_time.second,
                    microsecond=0,
                )

        if parsed_stamp is None:
            continue

        parsed_tx.append((parsed_stamp, tx_type))

    parsed_tx.sort(key=lambda item: item[0])

    current_in: datetime | None = None
    for stamp, tx_type in parsed_tx:
        if tx_type == "IN":
            if current_in is None:
                current_in = stamp
            continue

        if current_in is None:
            continue

        if stamp < current_in:
            continue

        duration_minutes = int((stamp - current_in).total_seconds() // 60)
        rows.append(
            [
                current_in.strftime("%Y-%m-%d"),
                current_in.strftime("%I:%M:%S %p"),
                stamp.strftime("%I:%M:%S %p"),
                _safe_text(_minutes_to_hhmm(duration_minutes), fallback="-"),
            ]
        )
        sessions += 1
        current_in = None

    if current_in is not None:
        rows.append(
            [
                current_in.strftime("%Y-%m-%d"),
                current_in.strftime("%I:%M:%S %p"),
                "Missing OUT",
                "-",
            ]
        )

    return rows, sessions


def _daily_summary_cards(report: dict[str, Any], sessions_count: int) -> list[dict[str, str]]:
    first_in = _format_time_12h(report.get("first_in")) if _has_value(report.get("first_in")) else "N/A"
    last_out = _format_time_12h(report.get("last_out")) if _has_value(report.get("last_out")) else "N/A"

    total_in_hhmm, total_out_hhmm = _resolve_total_in_out_hhmm(report)

    return [
        {"label": "First IN", "value": first_in},
        {"label": "Last OUT", "value": last_out},
        {"label": "Total In Hours", "value": _safe_text(total_in_hhmm, fallback="N/A")},
        {"label": "Total Out Hours", "value": _safe_text(total_out_hhmm, fallback="N/A")},
        {"label": "Total Sessions", "value": str(max(0, sessions_count))},
    ]


def _resolve_total_in_out_hhmm(report: dict[str, Any]) -> tuple[str, str]:
    total_in_hhmm = _safe_text(report.get("total_in"), fallback="")
    total_out_hhmm = _safe_text(report.get("total_out"), fallback="")

    if not total_in_hhmm:
        total_in_hhmm = _safe_text(report.get("totalInHHMM"), fallback="")
    if not total_out_hhmm:
        total_out_hhmm = _safe_text(report.get("totalOutHHMM"), fallback="")

    if not total_in_hhmm:
        in_minutes = _to_int(report.get("total_in_minutes"))
        if in_minutes is None:
            in_minutes = _to_int(report.get("totalInMinutes"))
        total_in_hhmm = _safe_text(_minutes_to_hhmm(in_minutes), fallback="N/A")

    if not total_out_hhmm:
        out_minutes = _to_int(report.get("total_out_minutes"))
        if out_minutes is None:
            out_minutes = _to_int(report.get("totalOutMinutes"))
        total_out_hhmm = _safe_text(_minutes_to_hhmm(out_minutes), fallback="N/A")

    return total_in_hhmm, total_out_hhmm


def _build_document(
    *,
    title: str,
    subtitle_lines: Sequence[str],
    story: Sequence[Any],
) -> bytes:
    buffer = BytesIO()
    logo_path = resolve_logo_path()

    def _draw_page_header(canv: canvas.Canvas, doc: SimpleDocTemplate) -> None:
        draw_header(
            canv,
            page_width=doc.pagesize[0],
            page_height=doc.pagesize[1],
            title=title,
            subtitle_lines=subtitle_lines,
            logo_path=logo_path,
        )

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=LEFT_MARGIN,
        rightMargin=RIGHT_MARGIN,
        topMargin=HEADER_TOP_MARGIN + HEADER_HEIGHT + HEADER_AFTER_GAP,
        bottomMargin=BOTTOM_MARGIN,
    )

    doc.build(
        list(story),
        onFirstPage=_draw_page_header,
        onLaterPages=_draw_page_header,
        canvasmaker=NumberedCanvas,
    )
    return buffer.getvalue()


def generate_daily_pdf(report: dict[str, Any]) -> bytes:
    generated_at = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    report_date = _safe_text(report.get("date"), fallback="N/A")
    employee_name = _safe_text(report.get("employee_name"), fallback="Unknown")
    card_no = _safe_text(report.get("card_no"), fallback="Unknown")

    rows, sessions_count = _table_rows_from_daily_rows_payload(report)
    total_in_hhmm, total_out_hhmm = _resolve_total_in_out_hhmm(report)

    cards = _daily_summary_cards(report, sessions_count)

    story: list[Any] = []
    story.append(KpiRow(cards=cards, cols=5, gap=8.0, card_h=KPI_CARD_HEIGHT))
    story.append(Spacer(1, KPI_GAP_AFTER))

    story.append(_build_section_heading("Daily In/Out Sessions"))
    story.append(Spacer(1, 2))
    story.append(
        _build_table(
            headers=["Date", "IN", "OUT", "Duration (HH:MM)"],
            body_rows=rows,
            col_widths=[CONTENT_WIDTH * 0.20, CONTENT_WIDTH * 0.28, CONTENT_WIDTH * 0.28, CONTENT_WIDTH * 0.24],
            total_rows=[
                ("Total In Hour", total_in_hhmm),
                ("Total Out Hour", total_out_hhmm),
            ],
            total_span_end=2,
        )
    )

    notes = report.get("notes", []) or []
    if notes:
        story.append(Spacer(1, 8))
        story.append(_build_section_heading("Notes"))
        for note in notes:
            story.append(Paragraph(f"- {_safe_text(note)}", _STYLES["BodyText"]))

    return _build_document(
        title="Oilchem Daily Attendance Report",
        subtitle_lines=[
            f"Employee: {employee_name} | Card Number: {card_no}",
            f"Report Date: {report_date} | Generated: {generated_at}",
        ],
        story=story,
    )


def generate_monthly_pdf(report: dict[str, Any]) -> bytes:
    generated_at = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    period = _safe_text(report.get("month"), fallback="N/A")
    employee_name = _safe_text(report.get("employee_name"), fallback="Unknown")
    card_no = _safe_text(report.get("card_no"), fallback="Unknown")

    records = report.get("records", []) or []

    row_data: list[list[str]] = []
    first_logged = "N/A"
    last_logged = "N/A"
    logged_dates: list[str] = []

    for item in records:
        date_value = _safe_text(item.get("date"), fallback="-")
        first_in_value = _format_time_12h(item.get("first_in")) if _has_value(item.get("first_in")) else "-"
        last_out_value = _format_time_12h(item.get("last_out")) if _has_value(item.get("last_out")) else "-"

        duration_value = _safe_text(item.get("duration_hhmm"), fallback="")
        if not duration_value:
            duration_value = _safe_text(_minutes_to_hhmm(_to_int(item.get("duration_minutes"))), fallback="N/A")

        sessions_count = _to_int(item.get("sessions_count"))
        if sessions_count is None:
            sessions_count = 1 if first_in_value != "-" and last_out_value != "-" else 0

        if first_in_value != "-" or last_out_value != "-":
            logged_dates.append(date_value)

        row_data.append(
            [
                date_value,
                first_in_value,
                last_out_value,
                duration_value,
                str(max(0, sessions_count)),
            ]
        )

    if logged_dates:
        first_logged = min(logged_dates)
        last_logged = max(logged_dates)

    total_working_days = _to_int(report.get("total_days"))
    if total_working_days is None:
        total_working_days = sum(1 for item in records if _has_value(item.get("first_in")) and _has_value(item.get("last_out")))

    missing_punch_days = _to_int(report.get("missing_punch_days"))
    if missing_punch_days is None:
        missing_punch_days = sum(1 for item in records if bool(item.get("missing_punch")))

    total_minutes = _to_int(report.get("total_minutes"))
    if total_minutes is None:
        total_minutes = 0
        for item in records:
            minutes = _to_int(item.get("duration_minutes"))
            if minutes is None:
                minutes = _hhmm_to_minutes(item.get("duration_hhmm"))
            if minutes is not None:
                total_minutes += minutes

    avg_minutes = int(total_minutes / total_working_days) if total_working_days > 0 else 0
    total_records = len(records)

    total_in_hhmm, total_out_hhmm = _resolve_total_in_out_hhmm(report)

    cards = [
        {"label": "Total Working Days", "value": str(total_working_days)},
        {"label": "Total Hours", "value": _minutes_to_readable(total_minutes)},
        {"label": "Avg Hours/Day", "value": _minutes_to_readable(avg_minutes)},
        {"label": "Missing Punch Days", "value": str(missing_punch_days)},
        {"label": "Total Records", "value": str(total_records)},
    ]

    info_cards = [
        {"label": "First Day Logged", "value": first_logged},
        {"label": "Last Day Logged", "value": last_logged},
    ]

    grand_total_text = _safe_text(report.get("total_duration_readable"), fallback="")
    if not grand_total_text:
        grand_total_text = _minutes_to_readable(total_minutes)

    story: list[Any] = []
    _append_kpi_grid(story, cards=cards, max_cols=4, gap=KPI_GAP_X, card_h=KPI_CARD_HEIGHT, row_gap=KPI_GAP_Y)
    story.append(Spacer(1, KPI_GAP_Y))
    _append_kpi_grid(story, cards=info_cards, max_cols=2, gap=KPI_GAP_X, card_h=KPI_CARD_HEIGHT - 6, row_gap=KPI_GAP_Y)
    story.append(Spacer(1, KPI_GAP_AFTER))

    story.append(_build_section_heading("Monthly Attendance Summary"))
    story.append(Spacer(1, 2))
    story.append(
        _build_table(
            headers=["Date", "First IN", "Last OUT", "Total Duration", "Sessions Count"],
            body_rows=row_data,
            col_widths=[
                CONTENT_WIDTH * 0.18,
                CONTENT_WIDTH * 0.20,
                CONTENT_WIDTH * 0.20,
                CONTENT_WIDTH * 0.22,
                CONTENT_WIDTH * 0.20,
            ],
            total_rows=[
                ("Grand Total Hours (Month)", grand_total_text),
                ("Total In Hours", total_in_hhmm),
                ("Total Out Hours", total_out_hhmm),
            ],
            total_span_end=3,
        )
    )

    return _build_document(
        title="Oilchem Monthly Attendance Report",
        subtitle_lines=[
            f"Employee: {employee_name} | Card Number: {card_no}",
            f"Period: {period} | Generated: {generated_at}",
        ],
        story=story,
    )


def generate_yearly_pdf(report: dict[str, Any]) -> bytes:
    generated_at = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    year_label = _safe_text(report.get("year"), fallback="N/A")
    employee_name = _safe_text(report.get("employee_name"), fallback="Unknown")
    card_no = _safe_text(report.get("card_no"), fallback="Unknown")

    months = report.get("months", []) or []

    table_rows: list[list[str]] = []
    total_sessions = 0
    total_missing = 0

    for month in months:
        month_name = _month_label(_safe_text(month.get("month"), fallback="-"))
        worked_days = _to_int(month.get("worked_days")) or 0
        missing_days = _to_int(month.get("missing_punch_days")) or 0
        total_missing += missing_days

        total_minutes_month = _to_int(month.get("total_minutes"))
        month_total_text = _safe_text(month.get("total_duration_readable"), fallback="")
        if not month_total_text:
            if total_minutes_month is not None:
                month_total_text = _minutes_to_readable(total_minutes_month)
            else:
                month_total_text = _safe_text(month.get("total_duration_hhmm"), fallback="N/A")

        avg_text = _safe_text(month.get("average_duration_hhmm"), fallback="")
        if not avg_text:
            avg_minutes = _to_int(month.get("average_minutes_per_day"))
            avg_text = _safe_text(_minutes_to_hhmm(avg_minutes), fallback="N/A")

        sessions_count = _to_int(month.get("sessions_count"))
        if sessions_count is None:
            sessions_count = worked_days
        total_sessions += max(0, sessions_count)

        table_rows.append(
            [
                month_name,
                str(worked_days),
                month_total_text,
                avg_text,
                str(missing_days),
            ]
        )

    total_worked_days = _to_int(report.get("total_worked_days"))
    if total_worked_days is None:
        total_worked_days = sum(_to_int(month.get("worked_days")) or 0 for month in months)

    total_minutes_year = _to_int(report.get("total_minutes"))
    if total_minutes_year is None:
        total_minutes_year = sum(_to_int(month.get("total_minutes")) or 0 for month in months)

    avg_minutes_year = int(total_minutes_year / total_worked_days) if total_worked_days > 0 else 0

    missing_punch_year = _to_int(report.get("missing_punch_days"))
    if missing_punch_year is None:
        missing_punch_year = total_missing

    total_hours_year = _safe_text(report.get("total_duration_readable"), fallback="")
    if not total_hours_year:
        total_hours_year = _minutes_to_readable(total_minutes_year)

    cards = [
        {"label": "Total Working Days", "value": str(total_worked_days)},
        {"label": "Total Hours", "value": total_hours_year},
        {"label": "Average Hours/Day", "value": _minutes_to_readable(avg_minutes_year)},
        {"label": "Missing Punch Days", "value": str(missing_punch_year)},
        {"label": "Total Sessions", "value": str(total_sessions)},
    ]

    story: list[Any] = []
    _append_kpi_grid(story, cards=cards, max_cols=4, gap=KPI_GAP_X, card_h=KPI_CARD_HEIGHT, row_gap=KPI_GAP_Y)
    story.append(Spacer(1, KPI_GAP_AFTER))

    story.append(_build_section_heading("Monthly Breakdown"))
    story.append(Spacer(1, 2))
    story.append(
        _build_table(
            headers=["Month", "Working Days", "Total Hours", "Avg Hours/Day", "Missing Punch Days"],
            body_rows=table_rows,
            col_widths=[
                CONTENT_WIDTH * 0.20,
                CONTENT_WIDTH * 0.16,
                CONTENT_WIDTH * 0.28,
                CONTENT_WIDTH * 0.18,
                CONTENT_WIDTH * 0.18,
            ],
        )
    )

    longest_days = report.get("longest_days", []) or []
    if longest_days:
        top_rows: list[list[str]] = []
        for item in list(longest_days)[:5]:
            date_value = _safe_text(item.get("date"), fallback="-")
            duration_text = _safe_text(item.get("total_hours"), fallback="")
            if not duration_text:
                duration_text = _safe_text(item.get("total_duration_readable"), fallback="")
            if not duration_text:
                duration_text = _safe_text(item.get("total_duration_hhmm"), fallback="N/A")
            top_rows.append([date_value, duration_text])

        story.append(Spacer(1, 10))
        story.append(_build_section_heading("Top 5 Longest Work Days"))
        story.append(
            _build_table(
                headers=["Date", "Total Hours"],
                body_rows=top_rows,
                col_widths=[CONTENT_WIDTH * 0.40, CONTENT_WIDTH * 0.60],
            )
        )

    footer_rows = [
        ["Grand Year Total Hours", total_hours_year],
        ["System Name", "Oilchem Entry/Exit Dashboard"],
        ["Generated by", "API System"],
    ]

    story.append(Spacer(1, 10))
    story.append(_build_section_heading("Year Summary"))
    story.append(
        _build_table(
            headers=["Item", "Value"],
            body_rows=footer_rows,
            col_widths=[CONTENT_WIDTH * 0.34, CONTENT_WIDTH * 0.66],
        )
    )

    return _build_document(
        title="Oilchem Yearly Attendance Report",
        subtitle_lines=[
            f"Employee: {employee_name} | Card Number: {card_no}",
            f"Year: {year_label} | Generated: {generated_at}",
        ],
        story=story,
    )


def build_daily_pdf(report: dict[str, Any]) -> bytes:
    return generate_daily_pdf(report)


def build_monthly_pdf(report: dict[str, Any]) -> bytes:
    return generate_monthly_pdf(report)


def build_yearly_pdf(report: dict[str, Any]) -> bytes:
    return generate_yearly_pdf(report)
