from __future__ import annotations

import re
from dataclasses import dataclass

from field_config import NOT_AVAILABLE
from tender_extractor import PageText

MAX_DESCRIPTION_LENGTH = 32000
MAX_EXCERPT_LENGTH = 16000
MAX_REMARK_LENGTH = 12000

CLAUSE_PATTERNS = (
    re.compile(r"(?:ITB|GCC|SCC|BDS)\s+([0-9]+(?:\.[0-9A-Za-z]+)*(?:\s*\([^)]+\))?)", re.IGNORECASE),
    re.compile(r"Clause\s+([0-9]+(?:\.[0-9A-Za-z]+)*(?:\s*\([^)]+\))?)", re.IGNORECASE),
    re.compile(r"^\s*([0-9]+(?:\.[0-9A-Za-z]+)+(?:\s*\([^)]+\))?)\s+(?:[A-Z][A-Za-z]|[A-Z]{2,})"),
)


@dataclass(frozen=True, slots=True)
class SynopsisLayoutRow:
    row_number: int
    serial: str
    label: str


@dataclass(frozen=True, slots=True)
class SynopsisSearchSpec:
    row_number: int
    label: str
    phrases: tuple[str, ...]
    regex_patterns: tuple[str, ...] = ()
    trailing_lines: int = 3
    page_limit: int | None = None


FINAL_SYNOPSIS_LAYOUT: tuple[SynopsisLayoutRow, ...] = (
    SynopsisLayoutRow(4, "1", "Tender Specification No./Tender ID/Tender No."),
    SynopsisLayoutRow(5, "2", "Name of the Purchaser/Employer/Owner"),
    SynopsisLayoutRow(6, "3", "Funding Agency"),
    SynopsisLayoutRow(7, "4", "Name of the work"),
    SynopsisLayoutRow(8, "5", "Scope of work"),
    SynopsisLayoutRow(9, "6", "Type of Contract"),
    SynopsisLayoutRow(10, "7", "Place of the work"),
    SynopsisLayoutRow(11, "8", "EMD/Bid Security Value (in INR)"),
    SynopsisLayoutRow(12, "9", "EMD/Bid Security Validity"),
    SynopsisLayoutRow(13, "10", "Form of EMD"),
    SynopsisLayoutRow(14, "11", "Bid Validity"),
    SynopsisLayoutRow(15, "12", "Estimated Total Project Cost (in INR)"),
    SynopsisLayoutRow(16, "13", "E-Bid Processing Fee (in INR)"),
    SynopsisLayoutRow(17, "14", "Form of E-Bid Processing Fee"),
    SynopsisLayoutRow(18, "15", "Cost of Bidding Document (in INR)"),
    SynopsisLayoutRow(19, "16", "Form of Cost of Bidding Document"),
    SynopsisLayoutRow(20, "17", "Bid submission date"),
    SynopsisLayoutRow(21, "18", "Hard Copy Submission"),
    SynopsisLayoutRow(22, "19", "Techno Commercial Opening"),
    SynopsisLayoutRow(23, "20", "Completion Period"),
    SynopsisLayoutRow(24, "21", "Power of Attorney"),
    SynopsisLayoutRow(25, "22", "Integrity Pact"),
    SynopsisLayoutRow(26, "23", "Performance Security"),
    SynopsisLayoutRow(27, "24", "Bank details of Employer"),
    SynopsisLayoutRow(28, "25", "Qualification Requirement/Qualification Criteria/Eligibility Criteria"),
    SynopsisLayoutRow(29, "25.1", "Technical Qualification/Criteria"),
    SynopsisLayoutRow(30, "25.2", "Financial Qualification/Criteria"),
    SynopsisLayoutRow(31, "25.2A", "Net worth"),
    SynopsisLayoutRow(32, "25.2B", "Average yearly Turnover"),
    SynopsisLayoutRow(33, "25.2C", "Liquid Assets/Working Capital"),
    SynopsisLayoutRow(34, "26", "Payment Terms for Supply Portion"),
    SynopsisLayoutRow(35, "26.1", "Advance (Supply)"),
    SynopsisLayoutRow(36, "26.2", "Advance (Erection)"),
    SynopsisLayoutRow(37, "26.3", "Rate of Interest on Advance"),
    SynopsisLayoutRow(38, "27", "Progressive Payment(All Installments)"),
    SynopsisLayoutRow(39, "28", "Final Installment (Supply) and (All installments )"),
    SynopsisLayoutRow(40, "29", "Final Installment (Erection)"),
    SynopsisLayoutRow(41, "30", "Technology Provider"),
    SynopsisLayoutRow(42, "31", "MOU"),
    SynopsisLayoutRow(43, "32", "SubContractor"),
    SynopsisLayoutRow(44, "33", "License (Labour , Electrical(in electrical at the bid time / after awarded tender))"),
    SynopsisLayoutRow(45, "34", "Prebid (Date with address)"),
    SynopsisLayoutRow(46, "35", "Retaintion money(With percentage)"),
    SynopsisLayoutRow(47, "36", "Award Criteria"),
    SynopsisLayoutRow(48, "37", "Evalution Criteria"),
    SynopsisLayoutRow(49, "38", "Manufacturing Authorization"),
    SynopsisLayoutRow(50, "39", "Mode of bid submission"),
    SynopsisLayoutRow(51, "40", "Defects Liability Period"),
    SynopsisLayoutRow(52, "41", "Latent Defect Waranty Period"),
    SynopsisLayoutRow(53, "42", "Price Variation "),
    SynopsisLayoutRow(54, "43", "Quantity Variation"),
    SynopsisLayoutRow(55, "44", "Liquidity Damages/LD"),
    SynopsisLayoutRow(56, "45", "Taxes and Duties"),
    SynopsisLayoutRow(57, "46", "Surplus Material"),
    SynopsisLayoutRow(58, "47", "Contractor’s Responsibilities"),
    SynopsisLayoutRow(59, "48", "Employer’s Responsibilities"),
    SynopsisLayoutRow(60, "49", "Insurance"),
    SynopsisLayoutRow(61, "50", "Force Majeure"),
    SynopsisLayoutRow(62, "51", "Correspondance Address"),
    SynopsisLayoutRow(63, "52", "Client Communication Details"),
    SynopsisLayoutRow(64, "53", "Tender Uploading Help / assistance"),
    SynopsisLayoutRow(65, "54", "Any Other T&C"),
)

FINAL_SYNOPSIS_LABELS = {row.row_number: row.label for row in FINAL_SYNOPSIS_LAYOUT}

DIRECT_SOURCE_ROWS = {
    **{row_number: row_number for row_number in range(4, 38)},
    51: 45,
    52: 46,
    53: 47,
    54: 48,
    55: 49,
    56: 50,
    57: 51,
    58: 52,
    59: 53,
    60: 54,
    61: 55,
    62: 56,
    63: 57,
    64: 58,
    65: 59,
}

SEARCH_SPECS: dict[int, SynopsisSearchSpec] = {
    41: SynopsisSearchSpec(
        row_number=41,
        label=FINAL_SYNOPSIS_LABELS[41],
        phrases=("technology provider", "technology partner", "technology collaborator"),
        regex_patterns=(
            r"Technology\s+Provider\s*[:\-]\s*([^\n]+)",
            r"Technology\s+Partner\s*[:\-]\s*([^\n]+)",
        ),
        trailing_lines=2,
        page_limit=240,
    ),
    42: SynopsisSearchSpec(
        row_number=42,
        label=FINAL_SYNOPSIS_LABELS[42],
        phrases=("memorandum of understanding", "mou"),
        regex_patterns=(
            r"Memorandum\s+of\s+Understanding(?:\s*\(MOU\))?\s*[:\-]\s*([^\n]+)",
            r"\bMOU\b\s*[:\-]\s*([^\n]+)",
        ),
        trailing_lines=2,
        page_limit=260,
    ),
    43: SynopsisSearchSpec(
        row_number=43,
        label=FINAL_SYNOPSIS_LABELS[43],
        phrases=("subcontractor", "sub-contractor", "sub contractor"),
        regex_patterns=(
            r"Sub-?Contractor\s*[:\-]\s*([^\n]+)",
        ),
        trailing_lines=2,
        page_limit=260,
    ),
    44: SynopsisSearchSpec(
        row_number=44,
        label=FINAL_SYNOPSIS_LABELS[44],
        phrases=("labour license", "labour licence", "electrical license", "electrical licence"),
        regex_patterns=(
            r"(Labou?r\s+Licen[cs]e[^\n]*)",
            r"(Electrical\s+Licen[cs]e[^\n]*)",
        ),
        trailing_lines=3,
        page_limit=260,
    ),
    45: SynopsisSearchSpec(
        row_number=45,
        label=FINAL_SYNOPSIS_LABELS[45],
        phrases=("pre bid", "pre-bid", "prebid"),
        regex_patterns=(
            r"Pre[- ]Bid(?:\s+Meeting)?(?:\s+Date)?\s*[:\-]\s*([^\n]+)",
            r"Date\s*&\s*Time\s+of\s+Pre[- ]Bid\s+Meeting\s*[:\-]?\s*([^\n]+)",
        ),
        trailing_lines=4,
        page_limit=120,
    ),
    46: SynopsisSearchSpec(
        row_number=46,
        label=FINAL_SYNOPSIS_LABELS[46],
        phrases=("retention money", "retention", "retainage", "security deposit"),
        regex_patterns=(
            r"(Retention(?:\s+Money)?\s*[:\-]?\s*[^\n]+)",
            r"(Retainage\s*[:\-]?\s*[^\n]+)",
        ),
        trailing_lines=2,
        page_limit=320,
    ),
    47: SynopsisSearchSpec(
        row_number=47,
        label=FINAL_SYNOPSIS_LABELS[47],
        phrases=("award criteria", "lowest evaluated", "award will be made", "successful bidder"),
        regex_patterns=(
            r"(The\s+Employer\s+will\s+award[^\n.]+(?:\.[^\n.]*)?)",
            r"(lowest\s+evaluated[^\n.]+(?:\.[^\n.]*)?)",
        ),
        trailing_lines=3,
        page_limit=260,
    ),
    48: SynopsisSearchSpec(
        row_number=48,
        label=FINAL_SYNOPSIS_LABELS[48],
        phrases=("evaluation criteria", "bid evaluation", "evaluation of bids", "techno-commercial evaluation"),
        regex_patterns=(
            r"(Evaluation\s+Criteria\s*[:\-]\s*[^\n]+)",
            r"(criteria\s+for\s+evaluation[^\n.]+(?:\.[^\n.]*)?)",
        ),
        trailing_lines=3,
        page_limit=260,
    ),
    49: SynopsisSearchSpec(
        row_number=49,
        label=FINAL_SYNOPSIS_LABELS[49],
        phrases=("manufacturer's authorization", "manufacturing authorization", "manufacturer authorization"),
        regex_patterns=(
            r"(Manufacturer'?s\s+Authorization[^\n]*)",
            r"(Manufacturing\s+Authorization[^\n]*)",
        ),
        trailing_lines=2,
        page_limit=260,
    ),
    50: SynopsisSearchSpec(
        row_number=50,
        label=FINAL_SYNOPSIS_LABELS[50],
        phrases=("mode of bid submission", "submitted online", "submit online", "electronic submission"),
        regex_patterns=(
            r"Mode\s+of\s+Bid\s+Submission\s*[:\-]\s*([^\n]+)",
            r"(Bids?\s+shall\s+be\s+submitted\s+online[^\n.]*\.?)",
        ),
        trailing_lines=3,
        page_limit=140,
    ),
}


def build_synopsis_rows(extraction_bundle: dict[str, object], pages: list[PageText]) -> list[dict[str, object]]:
    row_map = _build_row_map(extraction_bundle.get("rows", []))
    synopsis_rows: list[dict[str, object]] = []

    for layout_row in FINAL_SYNOPSIS_LAYOUT:
        if layout_row.row_number in DIRECT_SOURCE_ROWS:
            source_row = row_map.get(DIRECT_SOURCE_ROWS[layout_row.row_number])
            synopsis_rows.append(_clone_row(source_row, layout_row.row_number, layout_row.label))
            continue

        if layout_row.row_number == 38:
            synopsis_rows.append(_combine_rows(layout_row.row_number, layout_row.label, row_map, (38, 39, 40, 41, 42, 43, 44)))
            continue

        if layout_row.row_number == 39:
            synopsis_rows.append(_combine_rows(layout_row.row_number, layout_row.label, row_map, (39, 40, 41)))
            continue

        if layout_row.row_number == 40:
            synopsis_rows.append(_combine_rows(layout_row.row_number, layout_row.label, row_map, (42, 43, 44)))
            continue

        search_spec = SEARCH_SPECS[layout_row.row_number]
        result = _search_pages(search_spec, pages)

        if layout_row.row_number == 46 and result["value"] == NOT_AVAILABLE:
            result = _find_retention_from_payment_rows(row_map, layout_row.row_number, layout_row.label)
        elif layout_row.row_number == 48 and result["value"] == NOT_AVAILABLE:
            result = _combine_rows(layout_row.row_number, layout_row.label, row_map, (28, 29, 30))
        elif layout_row.row_number == 50 and result["value"] == NOT_AVAILABLE:
            result = _clone_row(row_map.get(21), layout_row.row_number, layout_row.label)

        synopsis_rows.append(result)

    return _enrich_synopsis_rows(synopsis_rows, pages)


def summarize_synopsis_rows(rows: list[dict[str, object]]) -> dict[str, float]:
    populated_rows = [row for row in rows if str(row.get("value", NOT_AVAILABLE)).strip() != NOT_AVAILABLE]
    coverage = len(populated_rows) / len(rows) if rows else 0.0

    confidence_values = [
        _coerce_confidence(row.get("confidence"))
        for row in populated_rows
    ]
    average_confidence = sum(confidence_values) / len(confidence_values) if confidence_values else 0.0
    estimated_accuracy = (average_confidence * 0.7) + (coverage * 0.3)

    return {
        "estimated_accuracy_percentage": round(estimated_accuracy * 100, 2),
        "average_field_confidence_percentage": round(average_confidence * 100, 2),
        "field_coverage_percentage": round(coverage * 100, 2),
    }


def _build_row_map(rows: object) -> dict[int, dict[str, object]]:
    output: dict[int, dict[str, object]] = {}
    if not isinstance(rows, list):
        return output
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            row_number = int(row.get("row_number", 0))
        except (TypeError, ValueError):
            continue
        output[row_number] = row
    return output


def _clone_row(source_row: dict[str, object] | None, row_number: int, label: str) -> dict[str, object]:
    if source_row is None:
        return _not_available_row(row_number, label)
    return {
        "row_number": row_number,
        "label": label,
        "value": _clean_text(str(source_row.get("value", NOT_AVAILABLE))) or NOT_AVAILABLE,
        "section": _clean_text(str(source_row.get("section", NOT_AVAILABLE))) or NOT_AVAILABLE,
        "clause": _clean_text(str(source_row.get("clause", NOT_AVAILABLE))) or NOT_AVAILABLE,
        "page": _clean_text(str(source_row.get("page", NOT_AVAILABLE))) or NOT_AVAILABLE,
        "remark": _sanitize_remark(_clean_text(str(source_row.get("remark", "")))),
        "excerpt": _clean_text(str(source_row.get("excerpt", ""))),
        "confidence": _coerce_confidence(source_row.get("confidence")),
    }


def _combine_rows(
    row_number: int,
    label: str,
    row_map: dict[int, dict[str, object]],
    source_row_numbers: tuple[int, ...],
) -> dict[str, object]:
    source_rows = [row_map[source_row_number] for source_row_number in source_row_numbers if source_row_number in row_map]
    available_rows = [row for row in source_rows if str(row.get("value", NOT_AVAILABLE)).strip() != NOT_AVAILABLE]

    if not available_rows:
        return _not_available_row(row_number, label)

    values = _unique_preserving_order(
        _clean_text(str(row.get("value", "")))
        for row in available_rows
        if _clean_text(str(row.get("value", "")))
    )
    remarks = _unique_preserving_order(
        _clean_text(str(row.get("remark", "")))
        for row in available_rows
        if _clean_text(str(row.get("remark", "")))
    )
    sections = _unique_preserving_order(
        _clean_text(str(row.get("section", "")))
        for row in available_rows
        if _clean_text(str(row.get("section", ""))) and _clean_text(str(row.get("section", ""))) != NOT_AVAILABLE
    )
    clauses = _unique_preserving_order(
        _clean_text(str(row.get("clause", "")))
        for row in available_rows
        if _clean_text(str(row.get("clause", ""))) and _clean_text(str(row.get("clause", ""))) != NOT_AVAILABLE
    )
    pages = _unique_preserving_order(
        _clean_text(str(row.get("page", "")))
        for row in available_rows
        if _clean_text(str(row.get("page", ""))) and _clean_text(str(row.get("page", ""))) != NOT_AVAILABLE
    )
    confidence_values = [_coerce_confidence(row.get("confidence")) for row in available_rows]

    return {
        "row_number": row_number,
        "label": label,
        "value": _trim_length("; ".join(values)),
        "section": " / ".join(sections) if sections else NOT_AVAILABLE,
        "clause": "; ".join(clauses) if clauses else NOT_AVAILABLE,
        "page": ", ".join(pages) if pages else NOT_AVAILABLE,
        "remark": _trim_length("; ".join(remarks), limit=MAX_REMARK_LENGTH) if remarks else "",
        "excerpt": "",
        "confidence": round(sum(confidence_values) / len(confidence_values), 4),
    }


def _search_pages(spec: SynopsisSearchSpec, pages: list[PageText]) -> dict[str, object]:
    regex_matches = _search_pages_by_regex(spec, pages)
    if regex_matches:
        return _merge_search_matches(spec.row_number, spec.label, regex_matches)

    phrase_matches = _search_pages_by_phrase(spec, pages)
    if phrase_matches:
        return _merge_search_matches(spec.row_number, spec.label, phrase_matches)

    return _not_available_row(spec.row_number, spec.label)


def _search_pages_by_regex(spec: SynopsisSearchSpec, pages: list[PageText]) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    for page in _iter_pages_with_priority(pages, spec.page_limit):
        if page.is_toc:
            continue

        for pattern in spec.regex_patterns:
            for match in re.finditer(pattern, page.text, flags=re.IGNORECASE | re.MULTILINE):
                matched_text = match.group(0)
                captured_value = match.group(1) if match.lastindex else matched_text
                value = _clean_value(captured_value)
                if not value:
                    continue

                line_index = _find_line_index(page.lines, matched_text)
                clause = _find_nearby_clause(page, line_index) or NOT_AVAILABLE
                matches.append(
                    {
                        "row_number": spec.row_number,
                        "label": spec.label,
                        "value": value,
                        "section": page.section,
                        "clause": clause,
                        "page": str(page.page_number),
                        "remark": "",
                        "excerpt": matched_text,
                        "confidence": 0.84,
                    }
                )
    return matches


def _search_pages_by_phrase(spec: SynopsisSearchSpec, pages: list[PageText]) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    for page in _iter_pages_with_priority(pages, spec.page_limit):
        if page.is_toc:
            continue

        for line_index, line in enumerate(page.lines):
            if not any(_line_contains_phrase(line, phrase) for phrase in spec.phrases):
                continue

            context_lines = page.lines[line_index : min(len(page.lines), line_index + spec.trailing_lines + 1)]
            value = _extract_context_value(spec.label, spec.phrases, context_lines)
            if not value:
                continue

            clause = _find_nearby_clause(page, line_index) or NOT_AVAILABLE
            matches.append(
                {
                    "row_number": spec.row_number,
                    "label": spec.label,
                    "value": value,
                    "section": page.section,
                    "clause": clause,
                    "page": str(page.page_number),
                    "remark": "",
                    "excerpt": " ".join(context_lines),
                    "confidence": 0.72,
                }
            )
    return matches


def _merge_search_matches(row_number: int, label: str, matches: list[dict[str, object]]) -> dict[str, object]:
    unique_matches: list[dict[str, object]] = []
    seen_keys: set[tuple[str, str, str]] = set()

    for match in matches:
        value = _clean_text(str(match.get("value", "")))
        section = _clean_text(str(match.get("section", NOT_AVAILABLE))) or NOT_AVAILABLE
        clause = _clean_text(str(match.get("clause", NOT_AVAILABLE))) or NOT_AVAILABLE
        if not value:
            continue
        dedupe_key = (_normalize(value), section, clause)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        unique_matches.append(match)

    if not unique_matches:
        return _not_available_row(row_number, label)

    values = _unique_preserving_order(
        _clean_text(str(match.get("value", "")))
        for match in unique_matches
        if _clean_text(str(match.get("value", "")))
    )
    sections = _unique_preserving_order(
        _clean_text(str(match.get("section", "")))
        for match in unique_matches
        if _clean_text(str(match.get("section", ""))) and _clean_text(str(match.get("section", ""))) != NOT_AVAILABLE
    )
    clauses = _unique_preserving_order(
        _clean_text(str(match.get("clause", "")))
        for match in unique_matches
        if _clean_text(str(match.get("clause", ""))) and _clean_text(str(match.get("clause", ""))) != NOT_AVAILABLE
    )
    pages = _unique_preserving_order(
        _clean_text(str(match.get("page", "")))
        for match in unique_matches
        if _clean_text(str(match.get("page", ""))) and _clean_text(str(match.get("page", ""))) != NOT_AVAILABLE
    )
    excerpts = _unique_preserving_order(
        _clean_text(str(match.get("excerpt", "")))
        for match in unique_matches
        if _clean_text(str(match.get("excerpt", "")))
    )
    confidence_values = [_coerce_confidence(match.get("confidence")) for match in unique_matches]

    return {
        "row_number": row_number,
        "label": label,
        "value": _trim_length("; ".join(values)),
        "section": " / ".join(sections) if sections else NOT_AVAILABLE,
        "clause": "; ".join(clauses) if clauses else NOT_AVAILABLE,
        "page": ", ".join(pages) if pages else NOT_AVAILABLE,
        "remark": "",
        "excerpt": _trim_length(" | ".join(excerpts), limit=MAX_EXCERPT_LENGTH),
        "confidence": round(sum(confidence_values) / len(confidence_values), 4) if confidence_values else 0.0,
    }


def _find_retention_from_payment_rows(
    row_map: dict[int, dict[str, object]],
    row_number: int,
    label: str,
) -> dict[str, object]:
    payment_rows = [row_map[source_row_number] for source_row_number in range(34, 45) if source_row_number in row_map]
    matched_rows = [
        row
        for row in payment_rows
        if "retention" in str(row.get("value", "")).lower() or "retention" in str(row.get("remark", "")).lower()
    ]
    if not matched_rows:
        return _not_available_row(row_number, label)
    return _combine_rows(row_number, label, {index: row for index, row in enumerate(matched_rows, start=1)}, tuple(range(1, len(matched_rows) + 1)))


def _extract_context_value(label: str, phrases: tuple[str, ...], context_lines: list[str]) -> str:
    if not context_lines:
        return ""

    first_line = _clean_text(context_lines[0])
    if ":" in first_line:
        _, suffix = first_line.split(":", 1)
        suffix = _clean_text(suffix)
        if suffix and _normalize(suffix) != _normalize(label):
            tail = " ".join(context_lines[1:]) if len(context_lines) > 1 else ""
            combined = f"{suffix} {tail}".strip()
            return _trim_length(combined)

    normalized_label = _normalize(label)
    normalized_phrases = {_normalize(phrase) for phrase in phrases}
    collected_lines: list[str] = []
    for line in context_lines:
        normalized_line = _normalize(line)
        if normalized_line == normalized_label or normalized_line in normalized_phrases:
            continue
        collected_lines.append(line)

    combined = _clean_text(" ".join(collected_lines))
    if not combined:
        return ""
    if _normalize(combined) == normalized_label:
        return ""
    return _trim_length(combined)


def _line_contains_phrase(line: str, phrase: str) -> bool:
    normalized_line = _normalize(line)
    normalized_phrase = _normalize(phrase)
    if not normalized_line or not normalized_phrase:
        return False
    if " " in normalized_phrase:
        return normalized_phrase in normalized_line
    return normalized_phrase in normalized_line.split()


def _not_available_row(row_number: int, label: str) -> dict[str, object]:
    return {
        "row_number": row_number,
        "label": label,
        "value": NOT_AVAILABLE,
        "section": NOT_AVAILABLE,
        "clause": NOT_AVAILABLE,
        "page": NOT_AVAILABLE,
        "remark": "",
        "excerpt": "",
        "confidence": 0.0,
    }


def _enrich_synopsis_rows(rows: list[dict[str, object]], pages: list[PageText]) -> list[dict[str, object]]:
    enriched_rows: list[dict[str, object]] = []
    for row in rows:
        enriched_row = dict(row)
        enriched_row["remark"] = _sanitize_remark(str(enriched_row.get("remark", "")))
        if (
            _clean_text(str(enriched_row.get("value", NOT_AVAILABLE))) != NOT_AVAILABLE
            and _clean_text(str(enriched_row.get("clause", NOT_AVAILABLE))) == NOT_AVAILABLE
        ):
            clause = _backfill_clause(enriched_row, pages)
            if clause:
                enriched_row["clause"] = clause
        if not _clean_text(str(enriched_row.get("remark", ""))):
            contextual_remark = _build_contextual_remark(enriched_row)
            if contextual_remark:
                enriched_row["remark"] = contextual_remark
        enriched_rows.append(enriched_row)
    return enriched_rows


def _sanitize_remark(remark: str) -> str:
    cleaned_remark = _clean_text(remark)
    if not cleaned_remark:
        return ""
    remark_parts = [
        part
        for part in (_clean_text(fragment) for fragment in cleaned_remark.split(";"))
        if part and not part.lower().startswith("extracted using ocr/image text")
    ]
    return _trim_length("; ".join(dict.fromkeys(remark_parts)), limit=MAX_REMARK_LENGTH) if remark_parts else ""


def _backfill_clause(row: dict[str, object], pages: list[PageText]) -> str:
    for page_number in _parse_page_reference(str(row.get("page", ""))):
        if not 1 <= page_number <= len(pages):
            continue
        page = pages[page_number - 1]
        line_index = _locate_row_line_index(page, row)
        if line_index < 0:
            continue
        clause = _find_nearby_clause(page, line_index)
        if clause:
            return clause
    return ""


def _build_contextual_remark(row: dict[str, object]) -> str:
    value = _clean_text(str(row.get("value", "")))
    excerpt = _clean_text(str(row.get("excerpt", "")))
    if not value or value == NOT_AVAILABLE or not excerpt or excerpt == value:
        return ""

    normalized_value = _normalize(value)
    normalized_excerpt = _normalize(excerpt)
    if not normalized_value or not normalized_excerpt:
        return ""
    if normalized_excerpt in normalized_value:
        return ""

    value_tokens = set(normalized_value.split())
    extra_excerpt_tokens = [
        token
        for token in normalized_excerpt.split()
        if len(token) >= 4 and token not in value_tokens
    ]
    if len(extra_excerpt_tokens) < 4:
        return ""
    return _trim_length(f"Source context: {excerpt}", limit=MAX_REMARK_LENGTH)


def _locate_row_line_index(page: PageText, row: dict[str, object]) -> int:
    for candidate_text in (
        _clean_text(str(row.get("excerpt", ""))),
        _clean_text(str(row.get("value", ""))),
        _clean_text(str(row.get("label", ""))),
    ):
        line_index = _find_line_index(page.lines, candidate_text)
        if line_index >= 0:
            return line_index

    normalized_value = _normalize(str(row.get("value", "")))
    candidate_tokens = [
        token
        for token in normalized_value.split()
        if len(token) >= 4 and token not in {"with", "from", "this", "that", "shall", "under", "will", "have"}
    ]
    if not candidate_tokens:
        return -1

    best_index = -1
    best_score = 0
    for index, line in enumerate(page.lines):
        normalized_line = _normalize(line)
        if not normalized_line:
            continue
        score = sum(1 for token in candidate_tokens if token in normalized_line)
        if score > best_score:
            best_score = score
            best_index = index
    return best_index if best_score >= min(2, len(candidate_tokens)) else -1


def _parse_page_reference(page_reference: str) -> list[int]:
    page_numbers: list[int] = []
    for fragment in re.split(r"\s*,\s*", page_reference or ""):
        cleaned_fragment = fragment.strip()
        if not cleaned_fragment:
            continue
        range_match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", cleaned_fragment)
        if range_match is not None:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            if end >= start:
                page_numbers.extend(range(start, end + 1))
            continue
        for match in re.findall(r"\d+", cleaned_fragment):
            page_numbers.append(int(match))
    return page_numbers


def _find_line_index(lines: list[str], target: str) -> int:
    normalized_target = _normalize(target)
    if not normalized_target:
        return -1
    for index, line in enumerate(lines):
        if normalized_target[:80] in _normalize(line):
            return index
    return -1


def _iter_pages_with_priority(pages: list[PageText], page_limit: int | None) -> list[PageText]:
    if page_limit is None:
        return list(pages)
    prioritized_pages = [page for page in pages if page.page_number <= page_limit]
    prioritized_pages.extend(page for page in pages if page.page_number > page_limit)
    return prioritized_pages


def _find_nearby_clause(page: PageText, line_index: int) -> str | None:
    if line_index < 0:
        return None
    window_start = max(0, line_index - 8)
    window_end = min(len(page.lines), line_index + 2)
    for candidate_line in reversed(page.lines[window_start:window_end]):
        for pattern in CLAUSE_PATTERNS:
            match = pattern.search(candidate_line)
            if match is not None:
                return _clean_text(match.group(1)).replace(" ", "")
    return None


def _coerce_confidence(value: object) -> float:
    try:
        return round(min(max(float(value), 0.0), 0.99), 4)
    except (TypeError, ValueError):
        return 0.0


def _unique_preserving_order(values: list[str] | tuple[str, ...] | object) -> list[str]:
    unique_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        if not value or value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values


def _clean_value(value: str) -> str:
    value = _clean_text(value)
    value = value.strip(" -:;,")
    return _trim_length(value) if value else ""


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9%]+", " ", value.lower()).strip()


def _trim_length(value: str, limit: int = MAX_DESCRIPTION_LENGTH) -> str:
    compact = _clean_text(value)
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3].rstrip()}..."
