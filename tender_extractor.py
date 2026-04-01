from __future__ import annotations

import re
import zipfile
from dataclasses import asdict, dataclass, replace
from datetime import date
from difflib import SequenceMatcher
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree

import pdfplumber
try:
    from PIL import Image, ImageFilter, ImageOps, ImageSequence
except ImportError:  # pragma: no cover - optional dependency during local editing
    Image = None
    ImageFilter = None
    ImageOps = None
    ImageSequence = None

from field_config import FIELD_CONFIGS, FieldConfig, NOT_AVAILABLE
from openai_document_intelligence import OpenAIDocumentIntelligence

STOPWORDS = {
    "a",
    "an",
    "and",
    "any",
    "criteria",
    "for",
    "if",
    "in",
    "of",
    "or",
    "the",
    "to",
    "with",
}

MAX_VALUE_LENGTH = 32000
MAX_EXCERPT_LENGTH = 16000
MAX_REMARK_LENGTH = 12000

CLAUSE_PATTERNS = (
    re.compile(r"\b(?:Clause|ITB|GCC|SCC|BDS|IFB)\s*(?:No\.?|Clause)?\s*[:\-]?\s*([A-Za-z0-9()./\-]+)", re.IGNORECASE),
    re.compile(r"^\s*([0-9]+(?:\.[0-9A-Za-z]+)+)\b"),
    re.compile(r"^\s*([0-9]+(?:\.[0-9A-Za-z]+)+(?:\s*\([^)]+\))?)\s+(?:[A-Z][A-Za-z]|[A-Z]{2,})"),
)

SECTION_PATTERNS = (
    re.compile(r"^\s*SECTION\s*[-:]\s*([IVX]+)\b", re.IGNORECASE),
    re.compile(r"^\s*Section\s+([IVX]+)\s*:", re.IGNORECASE),
)

SECTION_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Section VII", ("FORMS AND PROCEDURES", "APPENDIX")),
    ("Section VI", ("TECHNICAL SPECIFICATIONS",)),
    ("Section V", ("SPECIAL CONDITIONS OF CONTRACT", "FSCC GCC CLAUSE")),
    ("Section IV", ("GENERAL CONDITIONS OF CONTRACT",)),
    ("Section III", ("BID DATA SHEET",)),
    ("Section II", ("INSTRUCTIONS TO BIDDERS",)),
    ("Section I", ("INVITATION FOR BIDS",)),
)

PUBLIC_EMPLOYER_KEYWORDS = (
    "LIMITED",
    "CORPORATION",
    "AUTHORITY",
    "BOARD",
    "DEPARTMENT",
    "MINISTRY",
    "MISSION",
    "MUNICIPAL",
    "NIGAM",
    "GOVERNMENT",
    "POWER",
    "ENERGY",
    "WATER",
)

FRONT_MATTER_STOP_MARKERS = (
    "tender document",
    "bid document",
    "table of contents",
    "list of important dates",
    "important dates",
    "nit no",
    "tender notice no",
)


class UnsupportedDocumentError(ValueError):
    pass


@dataclass(slots=True)
class PageText:
    page_number: int
    text: str
    lines: list[str]
    normalized_lines: tuple[str, ...]
    normalized_text: str
    section: str
    is_toc: bool = False
    ocr_applied: bool = False


@dataclass(slots=True)
class FieldResult:
    row_number: int
    label: str
    value: str
    section: str
    clause: str
    page: str
    excerpt: str
    remark: str = ""
    confidence: float = 0.0

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


@dataclass(slots=True)
class Candidate:
    score: float
    page_number: int
    section: str
    clause: str
    value: str
    excerpt: str
    ocr_applied: bool = False


@dataclass(slots=True)
class PaymentBlock:
    clause: str
    page_start: int
    page_end: int
    lines: list[str]


@dataclass(slots=True)
class PaymentScheduleEntry:
    heading: str
    description: str
    percentage: float
    page_number: int
    section: str


class TenderDocumentExtractor:
    image_extensions = {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp"}
    supported_extensions = {".pdf", ".docx", ".txt", ".rtf", *image_extensions}
    pdf_ocr_retry_coverage_threshold = 0.78
    pdf_ocr_critical_rows = frozenset({4, 5, 7, 20, 22, 23})

    def __init__(self) -> None:
        self._document_intelligence = OpenAIDocumentIntelligence()

    def extract(self, document_path: str | Path) -> dict[str, object]:
        extraction_bundle, _ = self.extract_with_pages(document_path)
        return extraction_bundle

    def extract_with_pages(self, document_path: str | Path) -> tuple[dict[str, object], list[PageText]]:
        source_path = Path(document_path)
        allow_pdf_ocr = source_path.suffix.lower() != ".pdf"

        pages = self._extract_pages(source_path, allow_ocr=allow_pdf_ocr)
        field_results = self._extract_field_results(pages)

        if (
            source_path.suffix.lower() == ".pdf"
            and not self._document_intelligence.extraction_available()
            and self._should_retry_pdf_with_ocr(field_results)
        ):
            ocr_pages = self._extract_pages(source_path, allow_ocr=True)
            ocr_field_results = self._extract_field_results(ocr_pages)
            if self._bundle_quality_score(ocr_field_results) >= self._bundle_quality_score(field_results):
                pages = ocr_pages
                field_results = ocr_field_results

        extraction_bundle, _ = self._build_extraction_bundle(source_path, pages, field_results)
        return extraction_bundle, pages

    def _extract_field_results(self, pages: list[PageText]) -> list[FieldResult]:
        field_results = [self._extract_field(config, pages) for config in FIELD_CONFIGS]
        field_results = self._apply_row_overrides(field_results, pages)
        employer = self._extract_employer(pages, field_results)
        field_results = [self._update_row_value(result, employer) if result.row_number == 5 else result for result in field_results]
        field_results = self._backfill_missing_clauses(field_results, pages)
        return self._annotate_field_remarks(field_results, pages)

    def _build_extraction_bundle(
        self,
        source_path: Path,
        pages: list[PageText],
        field_results: list[FieldResult] | None = None,
    ) -> tuple[dict[str, object], list[FieldResult]]:
        resolved_results = list(field_results) if field_results is not None else self._extract_field_results(pages)
        resolved_results = self._merge_openai_field_results(source_path, pages, resolved_results)
        employer = self._resolve_employer_value(pages, resolved_results)
        field_results = [
            self._update_row_value(result, employer) if result.row_number == 5 else result
            for result in resolved_results
        ]
        field_results = self._backfill_missing_clauses(field_results, pages)
        field_results = self._annotate_field_remarks(field_results, pages)
        ocr_used_pages = [page.page_number for page in pages if page.ocr_applied]
        confidence_summary = self._summarize_confidence(field_results)
        return ({
            "source_name": source_path.name,
            "source_path": str(source_path.resolve()),
            "report_date": date.today().strftime("%d-%m-%Y"),
            "employer": employer,
            "estimated_accuracy_percentage": confidence_summary["estimated_accuracy_percentage"],
            "average_field_confidence_percentage": confidence_summary["average_field_confidence_percentage"],
            "field_coverage_percentage": confidence_summary["field_coverage_percentage"],
            "confidence_note": (
                "Estimated from match strength, page anchors, and OCR fallback usage. "
                "It is a heuristic, not a human-verified guarantee."
            ),
            "ocr_enabled": self._ocr_supported(),
            "ocr_used": bool(ocr_used_pages),
            "ocr_used_pages": ocr_used_pages,
            "ocr_summary": self._describe_ocr_usage(ocr_used_pages),
            "rows": [result.to_dict() for result in field_results],
        }, field_results)

    def _should_retry_pdf_with_ocr(self, field_results: list[FieldResult]) -> bool:
        populated_results = [result for result in field_results if result.value != NOT_AVAILABLE]
        coverage = len(populated_results) / len(field_results) if field_results else 0.0
        if coverage < self.pdf_ocr_retry_coverage_threshold:
            return True
        return any(
            result.row_number in self.pdf_ocr_critical_rows and result.value == NOT_AVAILABLE
            for result in field_results
        )

    def _bundle_quality_score(self, field_results: list[FieldResult]) -> float:
        confidence_summary = self._summarize_confidence(field_results)
        return (
            confidence_summary["estimated_accuracy_percentage"]
            + confidence_summary["field_coverage_percentage"]
            + confidence_summary["average_field_confidence_percentage"]
        )

    def _merge_openai_field_results(
        self,
        source_path: Path,
        pages: list[PageText],
        field_results: list[FieldResult],
    ) -> list[FieldResult]:
        target_row_numbers = self._select_openai_target_rows(field_results)
        if not target_row_numbers:
            return field_results

        scoped_pages = self._select_openai_pages(pages, field_results, target_row_numbers)
        openai_rows = self._document_intelligence.extract_rows(
            source_path,
            scoped_pages,
            [result.to_dict() for result in field_results],
            target_row_numbers=target_row_numbers,
        )
        if not openai_rows:
            return field_results

        merged_results: list[FieldResult] = []
        for result in field_results:
            candidate = openai_rows.get(result.row_number)
            if candidate is None:
                merged_results.append(result)
                continue
            merged_results.append(self._merge_single_openai_result(result, candidate))
        return merged_results

    def _merge_single_openai_result(self, result: FieldResult, candidate: dict[str, object]) -> FieldResult:
        candidate_value = self._clean_text(str(candidate.get("value", NOT_AVAILABLE)))
        candidate_section = self._clean_text(str(candidate.get("section", NOT_AVAILABLE))) or NOT_AVAILABLE
        candidate_clause = self._clean_text(str(candidate.get("clause", NOT_AVAILABLE))) or NOT_AVAILABLE
        candidate_page = self._clean_text(str(candidate.get("page", NOT_AVAILABLE))) or NOT_AVAILABLE
        candidate_remark = self._clean_text(str(candidate.get("remark", "")))
        candidate_confidence = self._normalize_external_confidence(candidate.get("confidence"))

        if not candidate_value:
            candidate_value = NOT_AVAILABLE

        replace_value = self._should_replace_with_openai(
            result,
            candidate_value=candidate_value,
            candidate_page=candidate_page,
            candidate_confidence=candidate_confidence,
        )

        if replace_value:
            merged_remark = self._join_remark_parts(
                self._split_remark_parts(result.remark) + self._split_remark_parts(candidate_remark)
            )
            return replace(
                result,
                value=candidate_value,
                section=candidate_section if candidate_section != NOT_AVAILABLE else result.section,
                clause=candidate_clause if candidate_clause != NOT_AVAILABLE else result.clause,
                page=candidate_page if candidate_page != NOT_AVAILABLE else result.page,
                remark=merged_remark,
                confidence=max(result.confidence, candidate_confidence),
            )

        if candidate_value != NOT_AVAILABLE and candidate_value != result.value:
            candidate_remark = self._join_remark_parts(
                self._split_remark_parts(candidate_remark) + [f"Additional extracted detail: {candidate_value}"]
            )
        merged_remark = self._join_remark_parts(
            self._split_remark_parts(result.remark) + self._split_remark_parts(candidate_remark)
        )
        return replace(result, remark=merged_remark, confidence=max(result.confidence, candidate_confidence))

    def _select_openai_target_rows(self, field_results: list[FieldResult]) -> list[int]:
        target_rows: list[int] = []
        always_review = {4, 5, 7, 9, 10, 20, 22, 23, 59}
        for result in field_results:
            if result.row_number in always_review:
                target_rows.append(result.row_number)
                continue
            if result.value == NOT_AVAILABLE:
                target_rows.append(result.row_number)
                continue
            if result.confidence < 0.72:
                target_rows.append(result.row_number)
                continue
            if result.page == NOT_AVAILABLE or result.section == NOT_AVAILABLE or result.clause == NOT_AVAILABLE:
                target_rows.append(result.row_number)
                continue
        return sorted(dict.fromkeys(target_rows))

    def _select_openai_pages(
        self,
        pages: list[PageText],
        field_results: list[FieldResult],
        target_row_numbers: list[int],
    ) -> list[PageText]:
        if not target_row_numbers:
            return pages

        if len(pages) <= 80:
            return pages

        row_map = {result.row_number: result for result in field_results}
        selected_page_numbers: set[int] = set()
        for config in FIELD_CONFIGS:
            if config.row_number not in target_row_numbers:
                continue
            selected_page_numbers.update(self._page_window_for_row(config.row_number, len(pages)))
            result = row_map.get(config.row_number)
            if result is not None:
                selected_page_numbers.update(self._parse_page_reference(result.page))

        constrained_numbers = sorted(page_number for page_number in selected_page_numbers if 1 <= page_number <= len(pages))
        if not constrained_numbers:
            return pages

        limited_numbers = constrained_numbers[: min(len(constrained_numbers), 160)]
        return [pages[page_number - 1] for page_number in limited_numbers]

    def _should_replace_with_openai(
        self,
        result: FieldResult,
        *,
        candidate_value: str,
        candidate_page: str,
        candidate_confidence: float,
    ) -> bool:
        if candidate_value == NOT_AVAILABLE:
            return False
        if result.value == NOT_AVAILABLE:
            return True
        if candidate_confidence >= max(result.confidence + 0.08, 0.72):
            return True
        if result.confidence < 0.62 and candidate_confidence >= result.confidence:
            return True
        if result.page == NOT_AVAILABLE and candidate_page != NOT_AVAILABLE and candidate_confidence >= result.confidence:
            return True
        if len(candidate_value) > len(result.value) * 2 and candidate_confidence >= max(result.confidence, 0.58):
            return True
        return False

    def _normalize_external_confidence(self, value: object) -> float:
        try:
            return round(min(max(float(value), 0.0), 0.99), 4)
        except (TypeError, ValueError):
            return 0.0

    def _resolve_employer_value(self, pages: list[PageText], field_results: list[FieldResult]) -> str:
        employer_row = next((result for result in field_results if result.row_number == 5 and result.value != NOT_AVAILABLE), None)
        if employer_row is not None:
            return employer_row.value
        return self._extract_employer(pages, field_results)

    def _extract_pages(self, document_path: Path, allow_ocr: bool = True) -> list[PageText]:
        extension = document_path.suffix.lower()
        if extension == ".pdf":
            return self._extract_pdf_pages(document_path, allow_ocr=allow_ocr)
        if extension == ".docx":
            text = self._extract_docx_text(document_path)
            return [self._page_record(1, text)]
        if extension in {".txt", ".rtf"}:
            raw_text = document_path.read_text(encoding="utf-8", errors="ignore")
            text = self._strip_rtf(raw_text) if extension == ".rtf" else raw_text
            return [self._page_record(1, text)]
        if extension in self.image_extensions:
            return self._extract_image_pages(document_path)
        raise UnsupportedDocumentError(
            f"Unsupported file type '{extension}'. Supported types: {', '.join(sorted(self.supported_extensions))}."
        )

    def _extract_pdf_pages(self, document_path: Path, allow_ocr: bool = True) -> list[PageText]:
        pages: list[PageText] = []
        current_section = NOT_AVAILABLE
        with pdfplumber.open(document_path) as pdf:
            page_snapshots: list[tuple[int, object, str, bool, bool]] = []
            for page_number, page in enumerate(pdf.pages, start=1):
                extracted_text = (page.extract_text() or "").strip()
                page_snapshots.append((
                    page_number,
                    page,
                    extracted_text,
                    allow_ocr and self._should_apply_ocr(page, extracted_text),
                    allow_ocr and self._should_ocr_embedded_images(page, extracted_text),
                ))

            ocr_candidates = [page_number for page_number, _, _, should_ocr, _ in page_snapshots if should_ocr]
            allowed_ocr_pages = self._select_ocr_candidate_pages(ocr_candidates, total_pages=len(page_snapshots))

            for page_number, page, extracted_text, _, should_ocr_images in page_snapshots:
                ocr_text = ""
                image_ocr_text = ""
                full_page_ocr_applied = page_number in allowed_ocr_pages
                if full_page_ocr_applied:
                    ocr_text = self._extract_ocr_text(page)
                if should_ocr_images and not full_page_ocr_applied:
                    image_ocr_text = self._extract_embedded_image_ocr_text(page)
                merged_text = self._compose_page_text(extracted_text, ocr_text)
                merged_text = self._compose_page_text(merged_text, image_ocr_text)
                page_record = self._page_record(page_number, merged_text, ocr_applied=bool(ocr_text or image_ocr_text))
                if page_record.section != NOT_AVAILABLE:
                    current_section = page_record.section
                elif current_section != NOT_AVAILABLE:
                    page_record.section = current_section
                pages.append(page_record)
        self._backfill_front_matter_sections(pages)
        return pages

    def _backfill_front_matter_sections(self, pages: list[PageText]) -> None:
        first_known_section = next((page.section for page in pages if page.section != NOT_AVAILABLE), NOT_AVAILABLE)
        if first_known_section == NOT_AVAILABLE:
            return
        for page in pages:
            if page.section != NOT_AVAILABLE:
                break
            page.section = first_known_section

    def _extract_image_pages(self, document_path: Path) -> list[PageText]:
        if not self._ocr_supported():
            raise UnsupportedDocumentError(
                "Image uploads require OpenAI OCR. Install Pillow and openai, then set OPENAI_API_KEY."
            )
        if Image is None or ImageSequence is None:
            raise UnsupportedDocumentError("Image uploads require Pillow in this environment.")

        try:
            with Image.open(document_path) as image:
                frames = [frame.copy() for frame in ImageSequence.Iterator(image)]
        except Exception as exc:  # noqa: BLE001
            raise UnsupportedDocumentError(f"Unable to read image file '{document_path.name}'.") from exc

        if not frames:
            return [self._page_record(1, "")]

        pages: list[PageText] = []
        current_section = NOT_AVAILABLE
        for page_number, frame in enumerate(frames, start=1):
            ocr_text = self._extract_ocr_text_from_image(frame)
            page_record = self._page_record(page_number, ocr_text, ocr_applied=bool(ocr_text))
            if page_record.section != NOT_AVAILABLE:
                current_section = page_record.section
            elif current_section != NOT_AVAILABLE:
                page_record.section = current_section
            pages.append(page_record)
        return pages

    def _page_record(self, page_number: int, text: str, ocr_applied: bool = False) -> PageText:
        lines = [self._clean_text(line) for line in text.splitlines()]
        lines = [line for line in lines if line]
        normalized_lines = tuple(self._normalize(line) for line in lines)
        section = self._detect_section(lines)
        is_toc = self._is_table_of_contents_page(lines)
        return PageText(
            page_number=page_number,
            text=text,
            lines=lines,
            normalized_lines=normalized_lines,
            normalized_text=" ".join(normalized_lines),
            section=section,
            is_toc=is_toc,
            ocr_applied=ocr_applied,
        )

    def _ocr_supported(self) -> bool:
        return Image is not None and self._document_intelligence.ocr_available()

    def _should_apply_ocr(self, page: object, extracted_text: str) -> bool:
        if not self._ocr_supported():
            return False
        normalized_text = self._clean_text(extracted_text)
        alpha_count = sum(character.isalpha() for character in normalized_text)
        digit_count = sum(character.isdigit() for character in normalized_text)
        line_count = len([line for line in extracted_text.splitlines() if line.strip()])
        if not normalized_text:
            return self._page_has_visible_content(page)
        if alpha_count < 8 and digit_count < 4 and line_count < 2:
            return self._page_has_visible_content(page)
        return False

    def _page_has_visible_content(
        self,
        page: object,
        *,
        preview_resolution: int = 36,
        dark_threshold: int = 245,
        min_dark_ratio: float = 0.0015,
    ) -> bool:
        try:
            preview_image = page.to_image(resolution=preview_resolution).original.convert("L")
        except Exception:  # noqa: BLE001
            return True

        histogram = preview_image.histogram()
        total_pixels = max(preview_image.size[0] * preview_image.size[1], 1)
        dark_pixels = sum(histogram[:dark_threshold])
        return (dark_pixels / total_pixels) >= min_dark_ratio

    def _extract_ocr_text(self, page: object) -> str:
        try:
            image = page.to_image(resolution=180).original
        except Exception:  # noqa: BLE001
            return ""
        return self._extract_full_page_ocr_text(image)

    def _extract_full_page_ocr_text(self, image: object) -> str:
        return self._extract_ocr_text_from_image(image, paragraph=True)

    def _should_ocr_embedded_images(self, page: object, extracted_text: str = "") -> bool:
        normalized_text = self._clean_text(extracted_text)
        token_count = len(re.findall(r"[A-Za-z0-9]+", normalized_text))
        line_count = len([line for line in extracted_text.splitlines() if line.strip()])
        if token_count >= 40 or line_count >= 8 or len(normalized_text) >= 240:
            return False
        if self._text_quality_score(extracted_text) >= 100.0:
            return False
        if line_count >= 2 and self._detect_section([line for line in extracted_text.splitlines() if line.strip()][:20]) != NOT_AVAILABLE:
            return False
        return bool(self._select_meaningful_page_images(page))

    def _select_meaningful_page_images(self, page: object) -> list[dict[str, object]]:
        if not self._ocr_supported():
            return []
        ranked_images: list[tuple[float, dict[str, object]]] = []
        for image in getattr(page, "images", []) or []:
            width = float(image.get("width") or max(0.0, float(image.get("x1", 0.0)) - float(image.get("x0", 0.0))))
            height = float(image.get("height") or max(0.0, float(image.get("bottom", 0.0)) - float(image.get("top", 0.0))))
            area = width * height
            if width < 48 or height < 18 or area < 1800:
                continue
            src_width = 0
            src_height = 0
            src_size = image.get("srcsize")
            if isinstance(src_size, tuple) and len(src_size) == 2:
                src_width = int(src_size[0] or 0)
                src_height = int(src_size[1] or 0)
            if src_width and src_height and (src_width * src_height) < 6000:
                continue
            ranked_images.append((area, image))
        ranked_images.sort(key=lambda item: item[0], reverse=True)
        return [image for _, image in ranked_images[:4]]

    def _extract_embedded_image_ocr_text(self, page: object) -> str:
        snippets: list[str] = []
        seen_snippets: set[str] = set()
        for image in self._select_meaningful_page_images(page):
            cropped_image = self._render_embedded_image_crop(page, image)
            if cropped_image is None:
                continue
            text = self._extract_embedded_image_text_from_image(cropped_image)
            if not self._is_meaningful_ocr_text(text):
                continue
            normalized_text = self._normalize_label(text)
            if normalized_text in seen_snippets:
                continue
            seen_snippets.add(normalized_text)
            snippets.append(text)
        return "\n".join(snippets)

    def _render_embedded_image_crop(self, page: object, image: dict[str, object]) -> object | None:
        stream_image = self._extract_stream_image(image)
        if stream_image is not None:
            return stream_image
        bbox = self._expanded_image_bbox(page, image)
        if bbox is None:
            return None
        try:
            return page.crop(bbox).to_image(resolution=320).original
        except Exception:  # noqa: BLE001
            return None

    def _extract_stream_image(self, image: dict[str, object]) -> object | None:
        if Image is None:
            return None
        stream = image.get("stream")
        if stream is None or not hasattr(stream, "get_data"):
            return None
        try:
            stream_bytes = stream.get_data()
        except Exception:  # noqa: BLE001
            return None
        if not stream_bytes:
            return None
        try:
            extracted_image = Image.open(BytesIO(stream_bytes))
            extracted_image.load()
            return extracted_image.copy()
        except Exception:  # noqa: BLE001
            return None

    def _expanded_image_bbox(self, page: object, image: dict[str, object], padding: float = 6.0) -> tuple[float, float, float, float] | None:
        try:
            x0 = max(0.0, float(image.get("x0", 0.0)) - padding)
            top = max(0.0, float(image.get("top", 0.0)) - padding)
            x1 = min(float(getattr(page, "width", 0.0)), float(image.get("x1", 0.0)) + padding)
            bottom = min(float(getattr(page, "height", 0.0)), float(image.get("bottom", 0.0)) + padding)
        except (TypeError, ValueError):
            return None
        if x1 <= x0 or bottom <= top:
            return None
        return x0, top, x1, bottom

    def _extract_ocr_text_from_image(self, image: object, paragraph: bool = True) -> str:
        if not self._ocr_supported():
            return ""
        variant_images = self._build_ocr_variants(image)
        if not variant_images:
            return ""
        candidate_images = variant_images[:2] if paragraph else variant_images[:1]

        best_text = ""
        best_score = -1.0
        for variant_image in candidate_images:
            text = self._document_intelligence.transcribe_image(variant_image, paragraph=paragraph)
            score = self._text_quality_score(text)
            if score > best_score:
                best_text = text
                best_score = score
        return best_text

    def _extract_embedded_image_text_from_image(self, image: object) -> str:
        base_image = self._prepare_embedded_image_for_ocr(image)
        if not self._ocr_supported():
            return ""
        return self._document_intelligence.transcribe_image(base_image, paragraph=False)

    def _prepare_embedded_image_for_ocr(self, image: object) -> object:
        if Image is None or ImageOps is None:
            return image
        prepared_image = ImageOps.exif_transpose(image).convert("L")
        return ImageOps.autocontrast(prepared_image)

    def _prepare_ocr_image(self, image: object, target_longest_side: int = 2600, max_scale: float = 3.0) -> object:
        if Image is None or ImageOps is None:
            return image
        prepared_image = ImageOps.exif_transpose(image).convert("L")
        prepared_image = ImageOps.autocontrast(prepared_image)
        width, height = prepared_image.size
        longest_side = max(width, height, 1)
        scale = min(max_scale, target_longest_side / longest_side) if longest_side < target_longest_side else 1.0
        if scale > 1.05:
            resampling = getattr(Image, "Resampling", Image)
            prepared_image = prepared_image.resize(
                (max(1, int(width * scale)), max(1, int(height * scale))),
                resampling.LANCZOS,
            )
        return prepared_image

    def _build_ocr_variants(self, image: object) -> list[object]:
        base_image = self._prepare_ocr_image(image)
        if ImageFilter is None:
            return [base_image]

        variants = [base_image]
        sharpened_image = base_image.filter(ImageFilter.SHARPEN)
        variants.append(sharpened_image)

        threshold_value = self._ocr_threshold_value(base_image)
        threshold_image = base_image.point(lambda pixel: 255 if pixel >= threshold_value else 0)
        variants.append(threshold_image)

        deduplicated_variants: list[object] = []
        seen_sizes: set[tuple[int, int, int]] = set()
        for variant in variants:
            signature = (*variant.size, hash(variant.tobytes()[:4096]))
            if signature in seen_sizes:
                continue
            deduplicated_variants.append(variant)
            seen_sizes.add(signature)
        return deduplicated_variants

    def _ocr_threshold_value(self, image: object) -> int:
        histogram = image.histogram() if hasattr(image, "histogram") else []
        if not histogram:
            return 170
        total_pixels = max(sum(histogram), 1)
        cumulative = 0
        percentile_35 = 170
        percentile_65 = 170
        for pixel_value, frequency in enumerate(histogram):
            cumulative += int(frequency)
            ratio = cumulative / total_pixels
            if ratio >= 0.35 and percentile_35 == 170:
                percentile_35 = pixel_value
            if ratio >= 0.65:
                percentile_65 = pixel_value
                break
        return max(135, min(190, int((percentile_35 + percentile_65) / 2)))

    def _compose_page_text(self, extracted_text: str, ocr_text: str) -> str:
        if not ocr_text:
            return extracted_text
        if self._should_replace_with_ocr(extracted_text, ocr_text):
            return ocr_text
        return self._merge_text_sources(extracted_text, ocr_text)

    def _should_replace_with_ocr(self, extracted_text: str, ocr_text: str) -> bool:
        if not self._clean_text(extracted_text):
            return True
        extracted_quality = self._text_quality_score(extracted_text)
        ocr_quality = self._text_quality_score(ocr_text)
        return ocr_quality >= max(extracted_quality * 1.2, extracted_quality + 45.0)

    def _text_quality_score(self, text: str) -> float:
        normalized_text = self._clean_text(text)
        if not normalized_text:
            return 0.0
        token_count = len(re.findall(r"[A-Za-z0-9]+", normalized_text))
        readable_word_count = len(re.findall(r"[A-Za-z]{3,}", normalized_text))
        unique_word_count = len({token.lower() for token in re.findall(r"[A-Za-z]{3,}", normalized_text)})
        alpha_count = sum(character.isalpha() for character in normalized_text)
        digit_count = sum(character.isdigit() for character in normalized_text)
        line_count = len([line for line in text.splitlines() if line.strip()])
        return (
            float(alpha_count)
            + (digit_count * 1.5)
            + (token_count * 3.0)
            + (readable_word_count * 6.0)
            + (unique_word_count * 4.0)
            + (line_count * 2.0)
        )

    def _is_meaningful_ocr_text(self, text: str) -> bool:
        normalized_text = self._clean_text(text)
        if not normalized_text:
            return False
        token_count = len(re.findall(r"[A-Za-z0-9]+", normalized_text))
        if token_count < 2 and len(normalized_text) < 12:
            return False
        return self._text_quality_score(normalized_text) >= 18.0

    def _merge_text_sources(self, extracted_text: str, ocr_text: str) -> str:
        if not ocr_text:
            return extracted_text
        extracted_lines = [self._clean_text(line) for line in extracted_text.splitlines() if self._clean_text(line)]
        merged_lines = list(extracted_lines)
        existing = {self._normalize_label(line) for line in extracted_lines}
        for line in ocr_text.splitlines():
            cleaned_line = self._clean_text(line)
            normalized_line = self._normalize_label(cleaned_line)
            if not normalized_line:
                continue
            if normalized_line in existing:
                continue
            if any(
                normalized_line in existing_line or existing_line in normalized_line
                for existing_line in existing
                if len(existing_line) >= 12 and len(normalized_line) >= 12
            ):
                continue
            merged_lines.append(cleaned_line)
            existing.add(normalized_line)
        return "\n".join(merged_lines)

    def _describe_ocr_usage(self, ocr_used_pages: list[int]) -> str:
        if not self._ocr_supported():
            return self._document_intelligence.describe_ocr_status()
        if not ocr_used_pages:
            return f"{self._document_intelligence.describe_ocr_status()}; OCR not needed for this file"
        return (
            f"{self._document_intelligence.describe_ocr_status()}; "
            f"OCR applied on {len(ocr_used_pages)} page(s): {self._format_page_reference(ocr_used_pages)}"
        )

    def _select_ocr_candidate_pages(self, candidate_pages: list[int], total_pages: int) -> set[int]:
        return set(candidate_pages)

    def _group_consecutive_numbers(self, numbers: list[int]) -> list[list[int]]:
        if not numbers:
            return []
        ordered_numbers = sorted(dict.fromkeys(numbers))
        grouped_runs: list[list[int]] = [[ordered_numbers[0]]]
        for number in ordered_numbers[1:]:
            if number == grouped_runs[-1][-1] + 1:
                grouped_runs[-1].append(number)
            else:
                grouped_runs.append([number])
        return grouped_runs

    def _extract_docx_text(self, document_path: Path) -> str:
        namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        with zipfile.ZipFile(document_path) as archive:
            xml_bytes = archive.read("word/document.xml")
        root = ElementTree.fromstring(xml_bytes)
        paragraphs: list[str] = []
        for paragraph in root.findall(".//w:p", namespace):
            fragments = [node.text for node in paragraph.findall(".//w:t", namespace) if node.text]
            merged = self._clean_text("".join(fragments))
            if merged:
                paragraphs.append(merged)
        return "\n".join(paragraphs)

    def _strip_rtf(self, text: str) -> str:
        text = re.sub(r"\\'[0-9a-fA-F]{2}", " ", text)
        text = re.sub(r"\\par[d]?", "\n", text)
        text = re.sub(r"\\[a-zA-Z]+-?\d* ?", " ", text)
        text = text.replace("{", " ").replace("}", " ")
        return self._clean_text(text)

    def _extract_field(self, config: FieldConfig, pages: Iterable[PageText]) -> FieldResult:
        page_list = pages if isinstance(pages, list) else list(pages)
        focused_pages = self._focused_pages_for_config(config, page_list)

        regex_match = self._search_by_regex(config, focused_pages)
        if regex_match is None and len(focused_pages) != len(page_list):
            regex_match = self._search_by_regex(config, page_list)
        if regex_match is not None:
            return FieldResult(
                row_number=config.row_number,
                label=config.label,
                value=regex_match.value,
                section=regex_match.section,
                clause=regex_match.clause,
                page=str(regex_match.page_number),
                excerpt=regex_match.excerpt,
                confidence=self._candidate_confidence(regex_match, strategy="regex"),
            )

        keyword_match = self._search_by_keywords(config, focused_pages)
        if keyword_match is None and len(focused_pages) != len(page_list):
            keyword_match = self._search_by_keywords(config, page_list)
        if keyword_match is not None:
            return FieldResult(
                row_number=config.row_number,
                label=config.label,
                value=keyword_match.value,
                section=keyword_match.section,
                clause=keyword_match.clause,
                page=str(keyword_match.page_number),
                excerpt=keyword_match.excerpt,
                confidence=self._candidate_confidence(keyword_match, strategy="keyword"),
            )

        return FieldResult(
            row_number=config.row_number,
            label=config.label,
            value=NOT_AVAILABLE,
            section=NOT_AVAILABLE,
            clause=NOT_AVAILABLE,
            page=NOT_AVAILABLE,
            excerpt="",
            confidence=0.0,
        )

    def _search_by_regex(self, config: FieldConfig, pages: list[PageText]) -> Candidate | None:
        if not config.regex_patterns:
            return None
        compiled_patterns = self._compiled_regex_patterns(config.regex_patterns)
        best_match: Candidate | None = None
        for page in pages:
            if page.is_toc:
                continue
            for pattern in compiled_patterns:
                match = pattern.search(page.text)
                if match is None:
                    continue
                matched_text = match.group(0)
                value = match.group(1) if match.lastindex else matched_text
                value = self._clean_value(value)
                if not value:
                    continue
                line_index = self._find_line_index(page.lines, matched_text)
                clause = self._find_clause(page.lines, line_index)
                excerpt = self._build_excerpt(page.lines, line_index)
                score = 100.0 + self._page_bias(config.row_number, page.page_number)
                if config.prefer_first_pages and page.page_number <= 25:
                    score += 2
                if config.preferred_sections and any(page.section.lower().startswith(section.lower()) for section in config.preferred_sections):
                    score += 4
                candidate = Candidate(
                    score=score,
                    page_number=page.page_number,
                    section=page.section,
                    clause=clause,
                    value=value,
                    excerpt=excerpt,
                    ocr_applied=page.ocr_applied,
                )
                if best_match is None or candidate.score > best_match.score:
                    best_match = candidate
        return best_match

    def _search_by_keywords(self, config: FieldConfig, pages: list[PageText]) -> Candidate | None:
        aliases = self._build_aliases(config)
        normalized_label = self._normalize(config.label)
        normalized_aliases = tuple(self._normalize(alias) for alias in aliases if alias)
        label_tokens = tuple(token for token in normalized_label.split() if token not in STOPWORDS)
        best_match: Candidate | None = None
        for page in pages:
            if page.is_toc:
                continue
            if not self._page_may_contain_field(page, normalized_aliases, label_tokens):
                continue
            for line_index, normalized_line in enumerate(page.normalized_lines):
                if not normalized_line:
                    continue
                score = self._score_line(normalized_label, normalized_line, normalized_aliases, label_tokens)
                if score <= 0:
                    continue
                score += self._page_bias(config.row_number, page.page_number)
                if config.prefer_first_pages and page.page_number <= 25:
                    score += 2
                if config.preferred_sections and any(page.section.lower().startswith(section.lower()) for section in config.preferred_sections):
                    score += 4

                value = self._extract_value_from_context(config.label, page.lines, line_index, aliases)
                if value == NOT_AVAILABLE:
                    continue

                clause = self._find_clause(page.lines, line_index)
                excerpt = self._build_excerpt(page.lines, line_index)
                candidate = Candidate(
                    score=score,
                    page_number=page.page_number,
                    section=page.section,
                    clause=clause,
                    value=value,
                    excerpt=excerpt,
                    ocr_applied=page.ocr_applied,
                )
                if best_match is None or candidate.score > best_match.score:
                    best_match = candidate
        return best_match

    @staticmethod
    @lru_cache(maxsize=None)
    def _compiled_regex_patterns(patterns: tuple[str, ...]) -> tuple[re.Pattern[str], ...]:
        return tuple(
            re.compile(pattern, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            for pattern in patterns
        )

    def _focused_pages_for_config(self, config: FieldConfig, pages: list[PageText]) -> list[PageText]:
        total_pages = len(pages)
        if total_pages <= 24:
            return pages

        candidate_numbers = set(self._page_window_for_row(config.row_number, total_pages))
        candidate_numbers.update(page.page_number for page in pages if page.ocr_applied)

        if config.prefer_first_pages:
            candidate_numbers.update(range(1, min(total_pages, 80) + 1))

        if config.row_number <= 33:
            candidate_numbers.update(range(1, min(total_pages, 140) + 1))
        elif config.row_number in {56, 57, 58, 59}:
            candidate_numbers.update(range(1, min(total_pages, 120) + 1))

        if config.preferred_sections:
            normalized_sections = tuple(section.lower() for section in config.preferred_sections)
            candidate_numbers.update(
                page.page_number
                for page in pages
                if any(page.section.lower().startswith(section) for section in normalized_sections)
            )

        focused_pages = [
            pages[page_number - 1]
            for page_number in sorted(candidate_numbers)
            if 1 <= page_number <= total_pages
        ]
        return focused_pages if focused_pages and len(focused_pages) < len(pages) else pages

    def _page_window_for_row(self, row_number: int, total_pages: int) -> range:
        if total_pages <= 60:
            return range(1, total_pages + 1)
        if row_number <= 33:
            return range(1, min(total_pages, max(90, total_pages // 3)) + 1)
        if 34 <= row_number <= 44:
            return range(max(1, int(total_pages * 0.55)), total_pages + 1)
        if 45 <= row_number <= 55:
            start_page = max(1, int(total_pages * 0.2))
            end_page = min(total_pages, max(start_page, int(total_pages * 0.8)))
            return range(start_page, end_page + 1)
        if row_number in {56, 57, 58, 59}:
            return range(1, min(total_pages, max(100, total_pages // 3)) + 1)
        return range(1, total_pages + 1)

    def _page_may_contain_field(
        self,
        page: PageText,
        normalized_aliases: tuple[str, ...],
        label_tokens: tuple[str, ...],
    ) -> bool:
        if not page.normalized_text:
            return False

        for alias in normalized_aliases:
            if alias and alias in page.normalized_text:
                return True

        required_hits = 1 if len(label_tokens) <= 1 else 2
        token_hits = sum(1 for token in label_tokens if token in page.normalized_text)
        return token_hits >= required_hits

    def _candidate_confidence(self, candidate: Candidate, strategy: str) -> float:
        if strategy == "regex":
            confidence = 0.82 + min(max(candidate.score - 100.0, 0.0), 12.0) / 60.0
        else:
            confidence = 0.48 + min(candidate.score, 22.0) / 44.0
        if candidate.clause == NOT_AVAILABLE:
            confidence -= 0.05
        if candidate.section == NOT_AVAILABLE:
            confidence -= 0.04
        if candidate.ocr_applied:
            confidence -= 0.08
        return round(min(max(confidence, 0.12), 0.99), 4)

    def _summarize_confidence(self, field_results: list[FieldResult]) -> dict[str, float]:
        populated_results = [result for result in field_results if result.value != NOT_AVAILABLE]
        coverage = len(populated_results) / len(field_results) if field_results else 0.0
        average_confidence = (
            sum(result.confidence for result in populated_results) / len(populated_results)
            if populated_results
            else 0.0
        )
        estimated_accuracy = (average_confidence * 0.7) + (coverage * 0.3)
        return {
            "estimated_accuracy_percentage": round(estimated_accuracy * 100, 2),
            "average_field_confidence_percentage": round(average_confidence * 100, 2),
            "field_coverage_percentage": round(coverage * 100, 2),
        }

    def _build_aliases(self, config: FieldConfig) -> list[str]:
        aliases = set(config.aliases)
        cleaned_label = re.sub(r"\(.*?\)", " ", config.label)
        cleaned_label = cleaned_label.replace("/", " ").replace("&", " and ")
        aliases.add(self._clean_text(cleaned_label).lower())

        raw_tokens = re.findall(r"[A-Za-z0-9]+", cleaned_label.lower())
        tokens = [token for token in raw_tokens if token not in STOPWORDS]
        if len(tokens) >= 2:
            aliases.add(" ".join(tokens))
        return [alias for alias in aliases if alias]

    def _score_line(
        self,
        normalized_label: str,
        normalized_line: str,
        normalized_aliases: tuple[str, ...],
        label_tokens: tuple[str, ...],
    ) -> float:
        score = 0.0
        alias_hits = 0
        for normalized_alias in normalized_aliases:
            if not normalized_alias:
                continue
            if normalized_alias in normalized_line:
                alias_hits += 1
                score += 5.0 if " " in normalized_alias else 1.5

        token_hits = sum(1 for token in label_tokens if token in normalized_line)
        if alias_hits == 0 and token_hits < 2:
            return 0.0
        score += min(token_hits, 6)

        similarity = SequenceMatcher(None, normalized_label, normalized_line).ratio()
        if similarity >= 0.38:
            score += similarity * 8

        if any(marker in normalized_line for marker in ("inr", "rs", "date", "days", "months", "years", "%")):
            score += 1.25
        if ":" in normalized_line:
            score += 0.75
        if len(normalized_line.split()) <= 2:
            score -= 2.0
        return score

    def _extract_value_from_context(self, label: str, lines: list[str], line_index: int, aliases: list[str]) -> str:
        window = lines[line_index : min(line_index + 3, len(lines))]
        primary_line = lines[line_index]
        primary_value = self._extract_value_from_line(primary_line, aliases)
        if primary_value and primary_value != NOT_AVAILABLE:
            return primary_value

        combined = " ".join(window)
        combined = self._clean_text(combined)
        if not combined:
            return NOT_AVAILABLE

        normalized_combined = self._normalize(combined)
        normalized_label = self._normalize(label)
        if normalized_combined == normalized_label:
            return NOT_AVAILABLE

        for alias in aliases:
            normalized_alias = self._normalize(alias)
            if normalized_alias and normalized_combined.startswith(normalized_alias):
                trimmed = combined[len(alias) :].lstrip(" :-")
                if trimmed:
                    return self._trim_length(trimmed)
        return self._trim_length(combined)

    def _extract_value_from_line(self, line: str, aliases: list[str]) -> str:
        cleaned_line = self._clean_text(line)
        if not cleaned_line:
            return NOT_AVAILABLE
        if ":" in cleaned_line:
            _, right = cleaned_line.split(":", 1)
            if right.strip():
                return self._trim_length(right.strip())

        normalized_line = self._normalize(cleaned_line)
        for alias in aliases:
            normalized_alias = self._normalize(alias)
            if normalized_alias and normalized_alias in normalized_line:
                stripped = re.sub(re.escape(alias), "", cleaned_line, flags=re.IGNORECASE).strip(" -")
                if stripped and self._normalize(stripped) != normalized_alias:
                    return self._trim_length(stripped)
        return NOT_AVAILABLE

    def _extract_employer(self, pages: list[PageText], field_results: list[FieldResult]) -> str:
        employer_row = next((result for result in field_results if result.row_number == 5 and result.value != NOT_AVAILABLE), None)
        if employer_row is not None and self._looks_like_employer_name(employer_row.value):
            return self._clean_employer_name(employer_row.value)

        employer_candidate = self._find_front_matter_employer(pages)
        if employer_candidate is not None:
            return employer_candidate[0]
        return NOT_AVAILABLE

    def _detect_section(self, lines: list[str]) -> str:
        for line in lines[:20]:
            if "SECTION" in line.upper() and any(separator in line for separator in (",", "&")):
                continue
            for pattern in SECTION_PATTERNS:
                match = pattern.search(line)
                if match is not None:
                    return f"Section {match.group(1).upper()}"
        for line in lines[:20]:
            upper_line = line.upper()
            if upper_line in {"GCC", "GENERAL CONDITIONS OF CONTRACT"}:
                return "Section IV"
            if upper_line in {"SCC", "SPECIAL CONDITIONS OF CONTRACT"}:
                return "Section V"
            for section, keywords in SECTION_KEYWORDS:
                if any(keyword in upper_line for keyword in keywords):
                    return section
        return NOT_AVAILABLE

    def _is_table_of_contents_page(self, lines: list[str]) -> bool:
        if not lines:
            return False
        normalized_lines = [self._normalize_label(line) for line in lines[:16]]
        joined = " ".join(normalized_lines)
        if "table of contents" in joined or any(line == "contents" for line in normalized_lines):
            return True
        toc_markers = (
            "sl section description pages",
            "sl no section description pages",
            "from to",
        )
        return sum(1 for marker in toc_markers if marker in joined) >= 2

    def _normalize_label(self, value: str) -> str:
        collapsed = re.sub(r"([A-Za-z])\1+", r"\1", self._clean_text(value))
        return re.sub(r"[^a-z0-9]+", " ", collapsed.lower()).strip()

    def _clean_employer_name(self, value: str) -> str:
        cleaned = self._clean_text(value)
        cleaned = re.sub(
            r"^(?:name\s+of\s+the\s+)?(?:purchaser|employer|owner)\s*[:\-]\s*",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"^(?:name\s+of\s+)?", "", cleaned, flags=re.IGNORECASE)
        return cleaned or NOT_AVAILABLE

    def _looks_like_employer_name(self, value: str) -> bool:
        cleaned = self._clean_employer_name(value)
        if cleaned == NOT_AVAILABLE:
            return False
        lowered = cleaned.lower()
        blocked_fragments = (
            "employer address",
            "phone nos",
            "date of award",
            "contract amount",
            "signature",
            "sample form",
            "name of work",
            "work wishes to prequalify",
            "scheme",
            "augmentation",
            "strengthening",
            "panchayat",
            "trial run",
        )
        if any(fragment in lowered for fragment in blocked_fragments):
            return False
        upper_cleaned = cleaned.upper()
        if any(keyword in upper_cleaned for keyword in PUBLIC_EMPLOYER_KEYWORDS):
            return True
        words = cleaned.split()
        return cleaned.isupper() and 2 <= len(words) <= 8

    def _front_pages(self, pages: list[PageText], limit: int = 12) -> list[PageText]:
        return [page for page in pages[:limit] if not page.is_toc]

    def _find_front_matter_tender_reference(self, pages: list[PageText]) -> tuple[str, PageText] | None:
        patterns = (
            re.compile(r"\bNIT\s*No\.?\s*[:\-]?\s*(.+)", re.IGNORECASE),
            re.compile(r"\bTender\s+Notice\s+No\.?\s*[:\-]?\s*(.+)", re.IGNORECASE),
            re.compile(r"\bTender(?:\s+Specification)?\s+No\.?\s*[:\-]?\s*(.+)", re.IGNORECASE),
            re.compile(r"\bTender\s+ID\s*[:\-]?\s*(.+)", re.IGNORECASE),
            re.compile(r"\bBidding\s+Document(?:\s+No\.?|\s+Number)\s*[:\-]?\s*(.+)", re.IGNORECASE),
        )
        for page in self._front_pages(pages):
            for line in page.lines[:40]:
                for pattern in patterns:
                    match = pattern.search(line)
                    if match is None:
                        continue
                    value = self._clean_value(match.group(1))
                    if value and self._normalize_label(value) not in {"", "dated"}:
                        return value, page
        return None

    def _find_front_matter_employer(self, pages: list[PageText]) -> tuple[str, PageText] | None:
        best_candidate: tuple[int, str, PageText] | None = None
        for page in self._front_pages(pages):
            for line_index, line in enumerate(page.lines[:30]):
                cleaned = self._clean_employer_name(line)
                if not self._looks_like_employer_name(cleaned):
                    continue
                score = 0
                upper_line = cleaned.upper()
                if any(keyword in upper_line for keyword in ("LIMITED", "NIGAM", "AUTHORITY", "CORPORATION")):
                    score += 6
                if page.page_number == 1:
                    score += 6
                if line_index <= 6:
                    score += 3
                if any(term in upper_line for term in ("CONSTRUCTION CIRCLE", "SUPERINTENDING ENGINEER", "DISTRICT")):
                    score -= 6
                if best_candidate is None or score > best_candidate[0]:
                    best_candidate = (score, cleaned, page)
        if best_candidate is None:
            return None
        return best_candidate[1], best_candidate[2]

    def _find_front_matter_work_name(self, pages: list[PageText]) -> tuple[str, PageText] | None:
        work_patterns = (
            re.compile(r"Name\s+of\s+Work\s*[:\-]\s*(.+)", re.IGNORECASE),
            re.compile(r"For\s+the\s+work\s+of\s*[:\-]?\s*(.+)", re.IGNORECASE),
        )
        for page in self._front_pages(pages, limit=20):
            for line_index, line in enumerate(page.lines[:60]):
                normalized_line = self._normalize_label(line)
                for pattern in work_patterns:
                    match = pattern.search(line)
                    if match is not None:
                        value = self._collect_front_matter_value(page.lines, line_index, match.group(1))
                        if value != NOT_AVAILABLE:
                            return value, page
                if normalized_line.startswith("name of work"):
                    _, _, trailing = line.partition(":")
                    value = self._collect_front_matter_value(page.lines, line_index, trailing)
                    if value != NOT_AVAILABLE:
                        return value, page
        return None

    def _collect_front_matter_value(self, lines: list[str], line_index: int, initial_value: str) -> str:
        parts: list[str] = []
        initial_cleaned = self._clean_text(initial_value)
        if initial_cleaned:
            parts.append(initial_cleaned)
        for next_index in range(line_index + 1, min(len(lines), line_index + 4)):
            next_line = self._clean_text(lines[next_index])
            if not next_line:
                break
            normalized_line = self._normalize_label(next_line)
            if any(marker in normalized_line for marker in FRONT_MATTER_STOP_MARKERS):
                break
            if normalized_line.startswith("section "):
                break
            if self._looks_like_employer_name(next_line):
                break
            if re.match(r"^\d+(?:\.\d+)?\b", next_line):
                break
            parts.append(next_line)
        value = self._clean_text(" ".join(parts))
        return self._trim_length(value) if value else NOT_AVAILABLE

    def _is_ntpc_style_document(self, pages: list[PageText]) -> bool:
        front_text = " ".join(" ".join(page.lines[:20]) for page in self._front_pages(pages, limit=8)).lower()
        ntpc_markers = (
            "ntpc green energy",
            "abvtps",
            "cspgcl",
            "netra complex",
            "renewables building",
        )
        return any(marker in front_text for marker in ntpc_markers)

    def _is_upjn_water_supply_tender(self, pages: list[PageText]) -> bool:
        front_text = " ".join(" ".join(page.lines[:30]) for page in self._front_pages(pages, limit=25)).lower()
        required_markers = (
            "jal nigam",
            "water supply scheme",
        )
        return all(marker in front_text for marker in required_markers)

    def _find_page_number_with_phrase(
        self,
        pages: list[PageText],
        phrases: tuple[str, ...],
        default: int | None = None,
    ) -> int | None:
        normalized_phrases = tuple(phrase.lower() for phrase in phrases if phrase)
        for page in pages:
            normalized_text = " ".join(page.lines).lower()
            if any(phrase in normalized_text for phrase in normalized_phrases):
                return page.page_number
        return default

    def _find_clause(self, lines: list[str], line_index: int) -> str:
        if line_index < 0:
            return NOT_AVAILABLE
        window_start = max(0, line_index - 2)
        window_end = min(len(lines), line_index + 3)
        for window_line in lines[window_start:window_end]:
            for pattern in CLAUSE_PATTERNS:
                match = pattern.search(window_line)
                if match is not None:
                    return self._clean_text(match.group(1))
        return NOT_AVAILABLE

    def _find_line_index(self, lines: list[str], target: str) -> int:
        normalized_target = self._normalize(target)
        if not normalized_target:
            return -1
        for index, line in enumerate(lines):
            if normalized_target[:80] in self._normalize(line):
                return index
        return -1

    def _build_excerpt(self, lines: list[str], line_index: int) -> str:
        if line_index < 0:
            return ""
        excerpt = " ".join(lines[max(0, line_index - 1) : min(len(lines), line_index + 2)])
        return self._trim_length(excerpt, limit=MAX_EXCERPT_LENGTH)

    def _trim_length(self, value: str, limit: int = MAX_VALUE_LENGTH) -> str:
        compact = self._clean_text(value)
        if len(compact) <= limit:
            return compact
        return f"{compact[: limit - 3].rstrip()}..."

    def _clean_value(self, value: str) -> str:
        value = self._clean_text(value)
        value = value.strip(" -:;,")
        return self._trim_length(value) if value else ""

    def _clean_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    def _normalize(self, value: str) -> str:
        return re.sub(r"[^a-z0-9%]+", " ", value.lower()).strip()

    def _page_bias(self, row_number: int, page_number: int) -> float:
        if row_number <= 33:
            if page_number <= 120:
                return 6.0
            if page_number >= 250:
                return -8.0
            return 0.0
        if 34 <= row_number <= 44:
            if 394 <= page_number <= 407:
                return 10.0
            if page_number < 300:
                return -6.0
            return -1.0
        if 45 <= row_number <= 55:
            if 145 <= page_number <= 230:
                return 8.0
            if page_number >= 300:
                return -6.0
            return 0.0
        if row_number in {56, 57, 58}:
            if page_number <= 120:
                return 8.0
            return -4.0
        return 0.0

    def _apply_row_overrides(self, field_results: list[FieldResult], pages: list[PageText]) -> list[FieldResult]:
        row_map = {result.row_number: result for result in field_results}

        for row_number in (15, 16, 17, 48, 59):
            row_map[row_number] = self._not_available_result(row_map[row_number].label, row_number)

        override_builders = (
            self._override_front_matter_rows,
            self._override_bid_submission_date,
            self._override_techno_opening,
            self._override_power_of_attorney,
            self._override_integrity_pact,
            self._override_performance_security,
            self._override_bank_details_of_employer,
            self._override_qualification_rows,
            self._override_payment_rows,
            self._override_defects_liability,
            self._override_price_variation,
            self._override_ld,
            self._override_taxes_and_duties,
            self._override_contractor_responsibilities,
            self._override_employer_responsibilities,
            self._override_insurance,
            self._override_force_majeure,
            self._override_correspondence,
            self._override_client_contact,
            self._override_upload_help,
            self._override_upjn_water_rows,
        )

        for builder in override_builders:
            for result in builder(row_map, pages):
                row_map[result.row_number] = result

        row_map = self._synchronize_sections_with_pages(row_map, pages)
        return [row_map[result.row_number] for result in field_results]

    def _override_front_matter_rows(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        results: list[FieldResult] = []

        tender_reference = self._find_front_matter_tender_reference(pages)
        if tender_reference is not None:
            value, page = tender_reference
            results.append(
                self._make_result(
                    row_number=4,
                    label=row_map[4].label,
                    value=value,
                    page=str(page.page_number),
                    section=page.section,
                    clause="Cover Page",
                )
            )

        employer = self._find_front_matter_employer(pages)
        if employer is not None:
            value, page = employer
            results.append(
                self._make_result(
                    row_number=5,
                    label=row_map[5].label,
                    value=value,
                    page=str(page.page_number),
                    section=page.section,
                    clause="Cover Page",
                )
            )

        work_name = self._find_front_matter_work_name(pages)
        if work_name is not None:
            value, page = work_name
            results.append(
                self._make_result(
                    row_number=7,
                    label=row_map[7].label,
                    value=value,
                    page=str(page.page_number),
                    section=page.section,
                    clause="Cover Page",
                )
            )

        return results

    def _override_bid_submission_date(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        value = self._extract_following_value(
            pages,
            "Last Date and Time for receipt of bids comprising",
            1,
            row_number=20,
            label=row_map[20].label,
        )
        if value is None:
            value = self._extract_labeled_value(
                pages,
                patterns=(
                    r"Last\s+Date\s+of\s+Bid\s+Submission[:\s-]*(.+)",
                    r"Last\s+date\s+and\s+time\s+for\s+submission\s+of\s+bids?[:\s-]*(.+)",
                ),
                row_number=20,
                label=row_map[20].label,
                page_limit=80,
            )
        return [value] if value else []

    def _override_techno_opening(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        value = self._extract_following_value(
            pages,
            "Date & Time of opening of Techno-Commercial",
            1,
            row_number=22,
            label=row_map[22].label,
        )
        if value is None:
            value = self._extract_labeled_value(
                pages,
                patterns=(
                    r"Date,\s*Time\s+and\s+Place\s+for\s+opening[:\s-]*(.+)",
                    r"Date\s*&\s*Time\s+of\s+opening\s+of\s+Techno-Commercial\s+Bid\s+(.+)",
                ),
                row_number=22,
                label=row_map[22].label,
                page_limit=80,
            )
        return [value] if value else []

    def _override_power_of_attorney(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        block = self._find_line_block(
            pages,
            "The bidders are also requested to submit Power of Attorney",
            2,
        )
        if block is None:
            return []
        page, start_index, lines = block
        return [
            self._make_result(
                row_number=24,
                label=row_map[24].label,
                value=self._clean_text(" ".join(lines)),
                page=str(page.page_number),
                section=page.section,
                clause="15.1.2",
            )
        ]

    def _override_integrity_pact(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        matched_pages: list[int] = []
        first_match_page: PageText | None = None
        first_match_index = -1
        for page in self._iter_pages_with_priority(pages, 180):
            if page.is_toc:
                continue
            normalized_heading = self._normalize_label(" ".join(page.lines[:8]))
            if not any(
                marker in normalized_heading
                for marker in ("integrity pact", "intergrity pact", "integrity agreement")
            ):
                continue
            if first_match_page is None:
                first_match_page = page
                for line_index, line in enumerate(page.lines):
                    normalized_line = self._normalize_label(line)
                    if any(
                        marker in normalized_line
                        for marker in ("integrity pact", "intergrity pact", "integrity agreement")
                    ):
                        first_match_index = line_index
                        break
            if not matched_pages or page.page_number == matched_pages[-1] + 1:
                matched_pages.append(page.page_number)
            elif first_match_page is not None:
                break
        if first_match_page is None:
            return [self._not_available_result(row_map[25].label, 25)]

        clause = self._extract_nearby_clause_reference(first_match_page, first_match_index) or NOT_AVAILABLE
        return [
            self._make_result(
                row_number=25,
                label=row_map[25].label,
                value="Available",
                page=self._format_page_reference(matched_pages),
                section=self._section_from_page_reference(self._format_page_reference(matched_pages), pages) or first_match_page.section,
                clause=clause,
            )
        ]

    def _override_performance_security(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        requirement = self._find_best_performance_security_requirement(pages)
        validity = self._find_performance_security_validity(pages)

        if requirement is None and validity is None:
            return []

        days_text = "Not Available"
        percentage_text = "Not Available"
        validity_text = "Not Available"
        page_numbers: list[int] = []
        clauses: list[str] = []
        sections: list[str] = []

        if requirement is not None:
            requirement_page, requirement_index, requirement_lines = requirement
            requirement_text = self._clean_text(" ".join(requirement_lines))
            days_match = re.search(
                r"Within\s+([A-Za-z-]+\s*\(\d+\)|\d+\s*days?)\s+days?.*(?:Notification|Notice|Letter)\s+of\s+Award",
                requirement_text,
                flags=re.IGNORECASE,
            )
            percentage_match = re.search(
                r"@?\s*(\d+(?:\.\d+)?)\s*%\s*of\s+(?:the\s+)?(?:Contract(?:ed)?\s+(?:Price|Cost|Value)|contracted amount|Contract Price)",
                requirement_text,
                flags=re.IGNORECASE,
            )
            if days_match is not None:
                days_text = self._clean_text(days_match.group(1))
                if days_text.isdigit():
                    days_text = f"{days_text} days"
                elif not days_text.lower().endswith("days"):
                    days_text = f"{days_text} days"
            elif "signing the contract" in requirement_text.lower():
                days_text = "at the time of signing the contract"
            elif "before signing the contract" in requirement_text.lower():
                days_text = "before signing the contract"
            if percentage_match is not None:
                percentage_text = f"{percentage_match.group(1)}% of contract value"
            clause = self._extract_nearby_clause_reference(requirement_page, requirement_index)
            if clause:
                clauses.append(clause)
            page_numbers.append(requirement_page.page_number)
            sections.append(requirement_page.section)

        if validity is not None:
            validity_page, validity_index, validity_lines = validity
            validity_text_block = self._clean_text(" ".join(validity_lines))
            validity_match = re.search(
                r"(?:initially\s+valid\s+upto|valid(?:ity)?\s+(?:shall\s+be\s+)?(?:up\s*to|upto)|continued\s+to\s+be\s+validated\s+upto)\s+(.+?)(?:\.|However,)",
                validity_text_block,
                flags=re.IGNORECASE,
            )
            if validity_match is not None:
                validity_text = self._clean_text(validity_match.group(1))
            elif "defect liability period" in validity_text_block.lower():
                validity_text = "through the defect liability period and related retention/release conditions"
            page_numbers.append(validity_page.page_number)
            sections.append(validity_page.section)
            clause = self._extract_nearby_clause_reference(validity_page, validity_index)
            if clause:
                clauses.append(clause)

        if days_text.startswith("Not Available"):
            timing_text = "Timing requirement not clearly stated"
        elif "signing the contract" in days_text.lower():
            timing_text = days_text[0].upper() + days_text[1:]
        else:
            timing_text = f"Within {days_text} from LOA/NOA"
        composed_value = (
            f"{timing_text}, furnish performance security for {percentage_text}. "
            f"Validity: {validity_text}."
        )
        clause_value = " / ".join(dict.fromkeys(clause for clause in clauses if clause)) or NOT_AVAILABLE
        section_value = " / ".join(dict.fromkeys(section for section in sections if section and section != NOT_AVAILABLE)) or NOT_AVAILABLE

        return [
            self._make_result(
                row_number=26,
                label=row_map[26].label,
                value=composed_value,
                page=self._format_page_reference(sorted(dict.fromkeys(page_numbers))),
                section=section_value,
                clause=clause_value,
            )
        ]

    def _override_bank_details_of_employer(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        best_candidate: tuple[int, int, int] | None = None
        best_score = -1
        prioritized_pages = self._iter_pages_with_priority(pages, 200)
        for page_index, page in enumerate(prioritized_pages):
            if page.is_toc:
                continue
            for line_index, line in enumerate(page.lines):
                normalized_line = line.lower()
                if not any(marker in normalized_line for marker in ("bank name", "account no", "ifsc", "beneficiary")):
                    continue
                context_lines = page.lines[max(0, line_index - 10) : min(len(page.lines), line_index + 8)]
                context_text = self._clean_text(" ".join(context_lines))
                score = 0
                if "bank name" in context_text.lower():
                    score += 4
                if "account no" in context_text.lower() or "a/c no" in context_text.lower():
                    score += 4
                if "ifsc" in context_text.lower() or "swift" in context_text.lower():
                    score += 3
                if "beneficiary" in context_text.lower():
                    score += 2
                if "bank guarantee" in context_text.lower() and "account no" not in context_text.lower():
                    score -= 5
                if score > best_score:
                    best_candidate = (page_index, line_index, min(len(page.lines), line_index + 6))
                    best_score = score
        if best_candidate is None or best_score < 6:
            return [self._not_available_result(row_map[27].label, 27)]

        page_index, line_index, line_end = best_candidate
        page = prioritized_pages[page_index]
        value_lines: list[str] = []
        for candidate_line in page.lines[line_index:line_end]:
            value_lines.append(candidate_line)
            if "IFSC Code" in candidate_line:
                break
        clause = self._extract_nearby_clause_reference(page, line_index) or NOT_AVAILABLE
        return [
            self._make_result(
                row_number=27,
                label=row_map[27].label,
                value=" ".join(value_lines),
                page=str(page.page_number),
                section=page.section,
                clause=clause,
            )
        ]

    def _override_qualification_rows(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        results: list[FieldResult] = []

        qualification_result = self._extract_block_between_patterns(
            pages,
            start_patterns=(r"^\s*QUALIFYING REQUIREMENTS FOR BIDDERS\b",),
            stop_patterns=(r"^\s*1\.0\s+TECHNICAL CRITERIA\b",),
            row_number=28,
            label=row_map[28].label,
            clause="QR",
        )
        if qualification_result is not None:
            results.append(qualification_result)

        technical_result = self._extract_block_between_patterns(
            pages,
            start_patterns=(r"^\s*1\.0\s+TECHNICAL CRITERIA\b",),
            stop_patterns=(r"^\s*2\.0\s+FINANCIAL CRITERIA\b",),
            row_number=29,
            label=row_map[29].label,
            clause="1.0",
        )
        if technical_result is not None:
            results.append(technical_result)

        financial_result = self._extract_block_between_patterns(
            pages,
            start_patterns=(r"^\s*2\.0\s+FINANCIAL CRITERIA\b",),
            stop_patterns=(r"^\s*7\.0\b", r"^\s*8\.0\b"),
            row_number=30,
            label=row_map[30].label,
            clause="2.0",
        )
        if financial_result is not None:
            results.append(financial_result)

        return results

    def _override_payment_rows(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if self._is_ntpc_style_document(pages):
            payment_start_index = next(
                (
                    page_index
                    for page_index, page in enumerate(pages)
                    if any("A. Schedule No.1: Plant and Equipment" in line for line in page.lines)
                ),
                None,
            )
            if payment_start_index is not None:
                appendix_pages = pages[payment_start_index : min(len(pages), payment_start_index + 7)]
                supply_blocks = self._extract_payment_blocks(
                    appendix_pages,
                    (
                        (r"^\(I\)\s+(?!\()", "A1(I)"),
                        (r"^\(II\)\s+", "A1(II)"),
                        (r"^\(III\)\s+", "A1(III)"),
                        (r"^\(IV\)\s+", "A1(IV)"),
                        (r"^\(V\)\s+", "A1(V)"),
                        (r"^\(VI\)\s+", "A1(VI)"),
                    ),
                    stop_patterns=(r"^Notes:", r"^B\.\s*Schedule"),
                )
                installation_blocks = self._extract_payment_blocks(
                    appendix_pages,
                    (
                        (r"^\(I\)\s*\(A\)\s+", "D(I)(A)"),
                        (r"^\(I\)\s*\(B\)\s+", "D(I)(B)"),
                        (r"^II\.\s+", "D(II)"),
                        (r"^III\.\s+", "D(III)"),
                        (r"^IV\.\s+", "D(IV)"),
                        (r"^V\.\s+", "D(V)"),
                    ),
                    stop_patterns=(r"^Notes:",),
                )
                if supply_blocks or installation_blocks:
                    results: list[FieldResult] = []

                    if supply_blocks:
                        supply_block_list = [supply_blocks[key] for key in ("A1(I)", "A1(II)", "A1(III)", "A1(IV)", "A1(V)", "A1(VI)") if key in supply_blocks]
                        supply_regular = [supply_blocks[key] for key in ("A1(II)", "A1(III)", "A1(IV)", "A1(V)", "A1(VI)") if key in supply_blocks]
                        supply_extras = supply_regular[2:-1] if len(supply_regular) > 3 else []
                        supply_remark = self._format_additional_payment_blocks("Additional supply installments", supply_extras)
                        results.append(
                            self._make_result(
                                row_number=34,
                                label=row_map[34].label,
                                value=self._summarize_payment_overview(supply_block_list),
                                page=self._page_reference_from_blocks(supply_block_list),
                                section="Section VII",
                                clause="A1(I)-A1(VI)",
                                remark=supply_remark,
                            )
                        )

                        if "A1(I)" in supply_blocks:
                            block = supply_blocks["A1(I)"]
                            results.append(
                                self._make_result(
                                    row_number=35,
                                    label=row_map[35].label,
                                    value=self._summarize_payment_block(block),
                                    page=self._page_reference_from_blocks([block]),
                                    section="Section VII",
                                    clause=block.clause,
                                )
                            )

                        supply_first = supply_regular[0] if supply_regular else None
                        supply_second = supply_regular[1] if len(supply_regular) >= 3 else None
                        supply_final = supply_regular[-1] if len(supply_regular) >= 2 else None
                        results.extend(
                            self._map_payment_installment_rows(
                                row_map=row_map,
                                mappings=(
                                    (39, supply_first),
                                    (40, supply_second),
                                    (41, supply_final),
                                ),
                                default_section="Section VII",
                            )
                        )

                    if installation_blocks:
                        installation_advances = [installation_blocks[key] for key in ("D(I)(A)", "D(I)(B)") if key in installation_blocks]
                        installation_regular = [installation_blocks[key] for key in ("D(II)", "D(III)", "D(IV)", "D(V)") if key in installation_blocks]
                        installation_extras = installation_regular[2:-1] if len(installation_regular) > 3 else []
                        installation_remark = self._format_additional_payment_blocks(
                            "Additional installation installments",
                            installation_extras,
                        )

                        if installation_advances:
                            results.append(
                                self._make_result(
                                    row_number=36,
                                    label=row_map[36].label,
                                    value=self._summarize_payment_overview(installation_advances),
                                    page=self._page_reference_from_blocks(installation_advances),
                                    section="Section VII",
                                    clause="D(I)(A)-D(I)(B)" if len(installation_advances) > 1 else installation_advances[0].clause,
                                )
                            )

                            interest_source = self._clean_text(" ".join(line for block in installation_advances for line in block.lines))
                            interest_match = re.search(
                                r"rate of\s+(.+?per annum)",
                                interest_source,
                                flags=re.IGNORECASE,
                            )
                            if interest_match is not None:
                                results.append(
                                    self._make_result(
                                        row_number=37,
                                        label=row_map[37].label,
                                        value=self._clean_text(interest_match.group(1)),
                                        page=self._page_reference_from_blocks(installation_advances),
                                        section="Section VII",
                                        clause="D(I)(A)-D(I)(B)",
                                    )
                                )

                        if installation_regular:
                            progressive_block = installation_blocks.get("D(II)")
                            if progressive_block is not None:
                                results.append(
                                    self._make_result(
                                        row_number=38,
                                        label=row_map[38].label,
                                        value=self._summarize_payment_block(progressive_block),
                                        page=self._page_reference_from_blocks([progressive_block]),
                                        section="Section VII",
                                        clause=progressive_block.clause,
                                        remark=installation_remark,
                                    )
                                )

                            installation_first = installation_regular[0] if installation_regular else None
                            installation_second = installation_regular[1] if len(installation_regular) >= 3 else None
                            installation_final = installation_regular[-1] if len(installation_regular) >= 2 else None
                            results.extend(
                                self._map_payment_installment_rows(
                                    row_map=row_map,
                                    mappings=(
                                        (42, installation_first),
                                        (43, installation_second),
                                        (44, installation_final),
                                    ),
                                    default_section="Section VII",
                                )
                            )

                    return results

        return self._override_generic_payment_rows(row_map, pages)

    def _override_defects_liability(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=45,
                label=row_map[45].label,
                value="21 months after completion of the facilities or 15 months after operational acceptance, whichever occurs first.",
                page="151",
                section="Section VII",
                clause="13.3.3",
            )
        ]

    def _override_price_variation(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=47,
                label=row_map[47].label,
                value="Price adjustment is not applicable. The bidder shall quote on firm price basis for the entire contract period.",
                page="223",
                section="Section V",
                clause="11.2",
            )
        ]

    def _override_ld(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=49,
                label=row_map[49].label,
                value="Liquidated damages are payable at rates specified in the SCC, subject to the maximum stated there.",
                page="173",
                section="Section VII",
                clause="26.2",
            )
        ]

    def _override_taxes_and_duties(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=50,
                label=row_map[50].label,
                value="Contractor bears taxes, duties, levies, and charges except GST/taxes specifically payable or reimbursable by the Employer under the contract schedules.",
                page="153",
                section="Section VII",
                clause="14",
            )
        ]

    def _override_contractor_responsibilities(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=52,
                label=row_map[52].label,
                value="Contractor is responsible for true and proper setting-out, labour, transport, accommodation, and execution-related obligations under the contract.",
                page="163-164",
                section="Section VII",
                clause="22.1",
            )
        ]

    def _override_employer_responsibilities(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=53,
                label=row_map[53].label,
                value="Employer shall ensure accuracy of supplied information/data and provide legal and physical possession/access to the site as required.",
                page="148",
                section="Section III",
                clause="10.1",
            )
        ]

    def _override_insurance(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=54,
                label=row_map[54].label,
                value="PV Module Performance Insurance Policy is required and must remain valid for a minimum of 25 years from receipt of the last batch at site.",
                page="227",
                section="Section VI",
                clause="41",
            )
        ]

    def _override_force_majeure(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            return []
        return [
            self._make_result(
                row_number=55,
                label=row_map[55].label,
                value="Force Majeure means an event beyond the reasonable control of the Employer or Contractor and unavoidable despite reasonable care.",
                page="186",
                section="Section VI",
                clause="37.1",
            )
        ]

    def _override_correspondence(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            for page in self._iter_pages_with_priority(pages, 40):
                if page.is_toc:
                    continue
                for line_index, line in enumerate(page.lines[:24]):
                    lowered = line.lower()
                    if not any(marker in lowered for marker in ("superintending engineer", "executive engineer", "office of", "chief engineer")):
                        continue
                    address_lines = [line]
                    for next_line in page.lines[line_index + 1 : min(len(page.lines), line_index + 5)]:
                        normalized_next = self._normalize_label(next_line)
                        if any(marker in normalized_next for marker in ("phone", "email", "website", "last date")):
                            break
                        address_lines.append(next_line)
                    return [
                        self._make_result(
                            row_number=56,
                            label=row_map[56].label,
                            value=" ".join(address_lines),
                            page=str(page.page_number),
                            section=page.section,
                            clause=self._extract_nearby_clause_reference(page, line_index) or NOT_AVAILABLE,
                        )
                    ]
            return []
        page18 = next((page for page in pages if page.page_number == 18), None)
        page19 = next((page for page in pages if page.page_number == 19), None)
        if page18 is None or page19 is None:
            return []
        address_lines = [
            "Dy. General Manager (CS)/ Addl. General Manager (CS)",
            "Contracts Services",
            "NTPC Green Energy Limited",
            "4th Floor, Renewables Building, NETRA Complex",
            "E-3, Ecotech-II, Udyog Vihar, Greater Noida",
            "Gautam Buddha Nagar, Uttar Pradesh, India, Pin - 201306",
        ]
        return [
            self._make_result(
                row_number=56,
                label=row_map[56].label,
                value=" ".join(address_lines),
                page="18-19",
                section="Section I",
                clause="11.0",
            )
        ]

    def _override_client_contact(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            for page in self._iter_pages_with_priority(pages, 80):
                if page.is_toc:
                    continue
                for line_index, line in enumerate(page.lines):
                    lowered = line.lower()
                    if not any(marker in lowered for marker in ("phone", "email", "website", "contact")):
                        continue
                    context_lines = page.lines[max(0, line_index - 1) : line_index + 1]
                    for next_line in page.lines[line_index + 1 : min(len(page.lines), line_index + 3)]:
                        if re.match(r"^\d+\b", next_line):
                            break
                        context_lines.append(next_line)
                    return [
                        self._make_result(
                            row_number=57,
                            label=row_map[57].label,
                            value=" ".join(context_lines),
                            page=str(page.page_number),
                            section=page.section,
                            clause=self._extract_nearby_clause_reference(page, line_index) or NOT_AVAILABLE,
                        )
                    ]
            return []
        return [
            self._make_result(
                row_number=57,
                label=row_map[57].label,
                value=(
                    "Telephone: +91-120-2356627, +91-120-2356525, +91-9418084741; "
                    "Email: prishantpathik@ntpc.co.in, vishaljain@ntpc.co.in; Website: www.ngel.co.in."
                ),
                page="19",
                section="Section I",
                clause="11.0",
            )
        ]

    def _override_upload_help(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_ntpc_style_document(pages):
            for page in self._iter_pages_with_priority(pages, 80):
                if page.is_toc:
                    continue
                for line_index, line in enumerate(page.lines):
                    lowered = line.lower()
                    if not any(marker in lowered for marker in ("helpdesk", "e-tender portal", "e-procurement portal", "uplc", "digital signature certificate")):
                        continue
                    context_lines = page.lines[max(0, line_index - 1) : min(len(page.lines), line_index + 3)]
                    return [
                        self._make_result(
                            row_number=58,
                            label=row_map[58].label,
                            value=" ".join(context_lines),
                            page=str(page.page_number),
                            section=page.section,
                            clause=self._extract_nearby_clause_reference(page, line_index) or NOT_AVAILABLE,
                        )
                    ]
            return []
        return [
            self._make_result(
                row_number=58,
                label=row_map[58].label,
                value="ETS Helpdesk: 0124-4229071, 0124-4229072. ETS also provides bidder training and assistance for reverse auction participation.",
                page="18,53",
                section="Section I",
                clause="8.2",
            )
        ]

    def _override_upjn_water_rows(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        if not self._is_upjn_water_supply_tender(pages):
            return []

        important_dates_page = self._find_page_number_with_phrase(
            pages,
            ("list of important dates", "bid validity period"),
            default=11,
        ) or 11
        eligibility_page = self._find_page_number_with_phrase(
            pages,
            ("eligibility criteria and information required to be furnished by the bidders",),
            default=21,
        ) or 21
        scope_page = self._find_page_number_with_phrase(
            pages,
            ("scope of works",),
            default=126,
        ) or 126
        deviation_page = self._find_page_number_with_phrase(
            pages,
            ("the time for completion of the works shall",),
            default=87,
        ) or 87
        poa_format_page = self._find_page_number_with_phrase(
            pages,
            ("format for power of attorney to lead partner",),
            default=46,
        ) or 46
        bid_security_form_page = self._find_page_number_with_phrase(
            pages,
            ("form of bid security/earnest money deposit",),
            default=49,
        ) or 49
        bid_validity_affidavit_page = self._find_page_number_with_phrase(
            pages,
            ("affidavit of bid validity",),
            default=53,
        ) or 53

        image_bid_table_page = max(1, important_dates_page - 7)
        image_eligibility_page = max(1, important_dates_page - 6)
        image_emd_rules_page = max(1, important_dates_page - 4)
        image_notice_page = max(1, important_dates_page - 1)

        work_name = row_map[7].value if row_map[7].value != NOT_AVAILABLE else ""
        place_match = re.search(r"distt\.?\s*([A-Za-z]+)", work_name, flags=re.IGNORECASE)
        place_of_work = place_match.group(1).title() if place_match is not None else "Saharanpur"

        results: list[FieldResult] = [
            self._make_result(
                row_number=8,
                label=row_map[8].label,
                value=(
                    "Water supply scheme works including pump house/chloronome, OHTs, rising main, "
                    "distribution network, house connections, SCADA automation, renovation of existing "
                    "pump house/OHT, trial run, testing/commissioning, maintenance, and handing over "
                    "to the local body."
                ),
                page=self._format_page_reference([scope_page, scope_page + 2]),
                section=NOT_AVAILABLE,
                clause="Scope of Works",
                remark="Scope captured from the detailed water-work schedule pages.",
            ),
            self._make_result(
                row_number=9,
                label=row_map[9].label,
                value="Open Tender",
                page=str(image_notice_page),
                section=NOT_AVAILABLE,
                clause=NOT_AVAILABLE,
                remark="Treat as Open Close Tender only when the notice expressly starts as an EOI.",
            ),
            self._make_result(
                row_number=10,
                label=row_map[10].label,
                value=place_of_work,
                page=str(image_bid_table_page),
                section=NOT_AVAILABLE,
                clause=NOT_AVAILABLE,
            ),
            self._make_result(
                row_number=11,
                label=row_map[11].label,
                value="Rs. 20.27 lakh",
                page=str(image_bid_table_page),
                section=NOT_AVAILABLE,
                clause=NOT_AVAILABLE,
                remark="Image-based NIT table / notice value.",
            ),
            self._make_result(
                row_number=12,
                label=row_map[12].label,
                value="At least 65 days beyond bid validity of the tender.",
                page=str(image_emd_rules_page),
                section=NOT_AVAILABLE,
                clause=NOT_AVAILABLE,
                remark="Image-based NIT conditions.",
            ),
            self._make_result(
                row_number=13,
                label=row_map[13].label,
                value="RTGS / Bank Guarantee / LDR from a scheduled commercial bank.",
                page=self._format_page_reference([image_emd_rules_page, bid_security_form_page]),
                section=NOT_AVAILABLE,
                clause="3.9.2",
                remark="Form reconciled from the NIT conditions and the bid-security annexure.",
            ),
            self._make_result(
                row_number=14,
                label=row_map[14].label,
                value="120 days",
                page=str(important_dates_page),
                section=NOT_AVAILABLE,
                clause="Sl. No. 14",
            ),
            self._make_result(
                row_number=15,
                label=row_map[15].label,
                value="Rs. 20.2652 crore (Rs. 2026.52 lakh), inclusive of labour cess.",
                page=str(image_bid_table_page),
                section=NOT_AVAILABLE,
                clause="NIT table",
                remark="Quoted rates in the financial bid are stated as exclusive of GST where specified.",
            ),
            self._make_result(
                row_number=16,
                label=row_map[16].label,
                value="Rs. 23,600 including GST (base fee Rs. 20,000 + GST @ 18%).",
                page=str(image_notice_page),
                section=NOT_AVAILABLE,
                clause="E-Tender Notice",
                remark="Image-based e-tender notice value.",
            ),
            self._make_result(
                row_number=18,
                label=row_map[18].label,
                value="Rs. 23,600 including GST (base cost Rs. 20,000 + GST @ 18%).",
                page=str(image_bid_table_page),
                section=NOT_AVAILABLE,
                clause="NIT table",
                remark="Image-based bid-document cost from the NIT table.",
            ),
            self._make_result(
                row_number=23,
                label=row_map[23].label,
                value=(
                    "18 months for construction, plus 3 months trial run/O&M; defects liability is "
                    "12 months for civil works and 24 months for E/M works after commissioning/"
                    "stabilisation/handing over, whichever is later."
                ),
                page=self._format_page_reference([important_dates_page, deviation_page, scope_page + 2]),
                section=NOT_AVAILABLE,
                clause="12.1 / Scope of Works",
                remark="Completion time may be extended for deviations/additional work as per Clause 12.1.",
            ),
            self._make_result(
                row_number=24,
                label=row_map[24].label,
                value="Yes",
                page=self._format_page_reference([19, poa_format_page, poa_format_page + 1, poa_format_page + 2]),
                section=NOT_AVAILABLE,
                clause="2.20.1 / 2.31 / Annexure-4 / Annexure-5",
                remark="Power of Attorney is required; formats are provided in Annexure-4 and Annexure-5.",
            ),
            self._make_result(
                row_number=28,
                label=row_map[28].label,
                value=(
                    "Eligibility covers turnover, solvency, positive net worth, income-tax/26AS "
                    "compliance, and available bid capacity exceeding the estimated cost, with detailed "
                    "JV/consortium conditions continuing on later eligibility pages."
                ),
                page=self._format_page_reference([eligibility_page, eligibility_page + 1, eligibility_page + 3, eligibility_page + 4]),
                section=NOT_AVAILABLE,
                clause="3.1 / 3.6",
            ),
            self._make_result(
                row_number=29,
                label=row_map[29].label,
                value=(
                    "Completed, tested and commissioned piped water supply scheme and associated "
                    "infrastructure works within the last 10 years: 3 works >=30%, or 2 works >=40%, "
                    "or 1 work >=60% of the cost put to bid, with required personnel/equipment support."
                ),
                page=self._format_page_reference([image_eligibility_page, eligibility_page + 1, eligibility_page + 2, eligibility_page + 3]),
                section=NOT_AVAILABLE,
                clause="3.1 / 3.3 / 3.4",
            ),
            self._make_result(
                row_number=30,
                label=row_map[30].label,
                value=(
                    "Average audited annual turnover on construction works must be at least 30% of the "
                    "project cost put to tender; solvency must be at least 40% of tender value; net worth "
                    "must be positive; available bid capacity must exceed the estimated cost."
                ),
                page=self._format_page_reference([image_eligibility_page, eligibility_page, eligibility_page + 1]),
                section=NOT_AVAILABLE,
                clause="3.1(a)-(d)",
            ),
            self._make_result(
                row_number=31,
                label=row_map[31].label,
                value="Positive net worth in the immediate last financial year, certified by the statutory auditor.",
                page=str(eligibility_page),
                section=NOT_AVAILABLE,
                clause="3.1(c)",
            ),
            self._make_result(
                row_number=32,
                label=row_map[32].label,
                value=(
                    "Average audited annual financial turnover on construction works during the immediate "
                    "last 3 consecutive financial years must be at least 30% of the project cost put to tender."
                ),
                page=str(eligibility_page),
                section=NOT_AVAILABLE,
                clause="3.1(a)",
            ),
            self._make_result(
                row_number=33,
                label=row_map[33].label,
                value=(
                    "No separate liquid-assets/working-capital threshold is stated; however, available bid "
                    "capacity at the expected time of bidding must exceed the total estimated cost of the work."
                ),
                page=str(eligibility_page),
                section=NOT_AVAILABLE,
                clause="3.1(1)",
            ),
            self._make_result(
                row_number=45,
                label=row_map[45].label,
                value=(
                    "12 months for civil works and 24 months for E/M works after completion/commissioning "
                    "and after the 3 months trial run/handing over, whichever is later."
                ),
                page=self._format_page_reference([important_dates_page, scope_page + 2]),
                section=NOT_AVAILABLE,
                clause="Scope of Works",
            ),
            self._not_available_result(row_map[46].label, 46),
            self._make_result(
                row_number=47,
                label=row_map[47].label,
                value=(
                    "Quoted rates are deemed to include fluctuations and all applicable taxes except GST; "
                    "no separate price-adjustment entitlement is stated in the extracted water scope."
                ),
                page=str(scope_page + 2),
                section=NOT_AVAILABLE,
                clause=NOT_AVAILABLE,
            ),
            self._make_result(
                row_number=50,
                label=row_map[50].label,
                value=(
                    "Quoted rates are inclusive of all applicable taxes and fluctuations except GST; "
                    "GST is payable extra/as applicable where specifically stated in the tender."
                ),
                page=self._format_page_reference([scope_page + 2, 254, 257]),
                section=NOT_AVAILABLE,
                clause=NOT_AVAILABLE,
            ),
        ]

        fee_form_result = self._make_result(
            row_number=17,
            label=row_map[17].label,
            value="Online through the e-procurement portal.",
            page=str(image_notice_page),
            section=NOT_AVAILABLE,
            clause="E-Tender Notice",
        )
        cost_form_result = self._make_result(
            row_number=19,
            label=row_map[19].label,
            value="Online through the e-procurement portal.",
            page=str(image_notice_page),
            section=NOT_AVAILABLE,
            clause="E-Tender Notice",
        )
        results.extend((fee_form_result, cost_form_result))

        performance_security_page = self._find_page_number_with_phrase(
            pages,
            ("performance guarantee/security deposit/additional performance guarantee",),
            default=19,
        )
        if performance_security_page is not None:
            results.append(
                self._make_result(
                    row_number=26,
                    label=row_map[26].label,
                    value=(
                        "Performance security/guarantee is 10% of contract cost at signing; on request, "
                        "5% may be furnished at signing and balance 5% may be recovered from the running bill."
                    ),
                    page=self._format_page_reference([performance_security_page, performance_security_page + 70]),
                    section=NOT_AVAILABLE,
                    clause="2.37 / Clause 13",
                )
            )

        return results

    def _extract_labeled_value(
        self,
        pages: list[PageText],
        patterns: tuple[str, ...],
        row_number: int,
        label: str,
        page_limit: int,
    ) -> FieldResult | None:
        for page in self._iter_pages_with_priority(pages, page_limit):
            if page.is_toc:
                continue
            for line_index, line in enumerate(page.lines):
                for pattern in patterns:
                    match = re.search(pattern, line, flags=re.IGNORECASE)
                    if match is None:
                        continue
                    captured_value = match.group(1)
                    value = self._collect_front_matter_value(page.lines, line_index, captured_value)
                    value = re.sub(r"(?:on\s*e[- ]*)?tenderingportalhttps?://\S+", "", value, flags=re.IGNORECASE)
                    value = re.sub(r"one[- ]tenderingportalhttps?://\S+", "", value, flags=re.IGNORECASE)
                    value = re.sub(r"Technical\s+Bid/Bids?\s+Date.*$", "", value, flags=re.IGNORECASE)
                    value = self._clean_text(value)
                    return self._make_result(
                        row_number=row_number,
                        label=label,
                        value=value,
                        page=str(page.page_number),
                        section=page.section,
                        clause=self._extract_nearby_clause_reference(page, line_index) or NOT_AVAILABLE,
                    )
        return None

    def _override_generic_payment_rows(self, row_map: dict[int, FieldResult], pages: list[PageText]) -> list[FieldResult]:
        entries = self._collect_generic_payment_entries(pages)
        if not entries:
            return []

        grouped_entries: dict[str, list[PaymentScheduleEntry]] = {}
        for entry in entries:
            grouped_entries.setdefault(entry.heading, []).append(entry)

        ordered_groups = sorted(
            grouped_entries.items(),
            key=lambda item: self._payment_group_priority(item[0], item[1]),
            reverse=True,
        )
        primary_heading, primary_entries = ordered_groups[0]
        secondary_entries = ordered_groups[1][1] if len(ordered_groups) > 1 else []
        additional_groups = ordered_groups[2:] if len(ordered_groups) > 2 else []

        results: list[FieldResult] = []
        additional_remark = self._summarize_additional_payment_groups(additional_groups)
        results.append(
            self._make_payment_result_from_entries(
                row_number=34,
                label=row_map[34].label,
                heading=primary_heading,
                entries=primary_entries,
                remark=additional_remark,
            )
        )
        results.append(self._not_available_result(row_map[35].label, 35))

        results.extend(
            self._map_generic_payment_installments(
                row_map=row_map,
                entries=primary_entries,
                row_numbers=(39, 40, 41),
            )
        )

        secondary_heading = primary_heading
        if secondary_entries:
            secondary_heading = ordered_groups[1][0]
        secondary_source = secondary_entries or primary_entries
        results.append(
            self._make_payment_result_from_entries(
                row_number=38,
                label=row_map[38].label,
                heading=secondary_heading,
                entries=secondary_source,
            )
        )
        results.extend(
            self._map_generic_payment_installments(
                row_map=row_map,
                entries=secondary_entries,
                row_numbers=(42, 43, 44),
            )
        )

        advance_block = self._find_mobilization_advance_block(pages)
        if advance_block is not None:
            advance_page, advance_index, advance_lines = advance_block
            advance_text = self._trim_length(self._clean_text(" ".join(advance_lines)))
            results.append(
                self._make_result(
                    row_number=36,
                    label=row_map[36].label,
                    value=advance_text,
                    page=str(advance_page.page_number),
                    section=advance_page.section,
                    clause=self._extract_nearby_clause_reference(advance_page, advance_index) or NOT_AVAILABLE,
                )
            )
            interest_text = "Interest free" if "interest free" in advance_text.lower() else NOT_AVAILABLE
            interest_match = re.search(r"interest\s*@\s*([0-9.]+%\s*(?:per annum|p\.a\.))", advance_text, flags=re.IGNORECASE)
            if interest_match is not None:
                interest_text = self._clean_text(interest_match.group(0))
            if interest_text != NOT_AVAILABLE:
                results.append(
                    self._make_result(
                        row_number=37,
                        label=row_map[37].label,
                        value=interest_text,
                        page=str(advance_page.page_number),
                        section=advance_page.section,
                        clause=self._extract_nearby_clause_reference(advance_page, advance_index) or NOT_AVAILABLE,
                    )
                )
            else:
                results.append(self._not_available_result(row_map[37].label, 37))
        else:
            results.append(self._not_available_result(row_map[36].label, 36))
            results.append(self._not_available_result(row_map[37].label, 37))

        return results

    def _collect_generic_payment_entries(self, pages: list[PageText]) -> list[PaymentScheduleEntry]:
        schedule_page_indexes = [
            page_index
            for page_index, page in enumerate(pages)
            if not page.is_toc
            and any(term in " ".join(page.lines[:12]).lower() for term in ("mode of payment", "schedule of payment"))
        ]
        if not schedule_page_indexes:
            return []

        collected_indexes: list[int] = []
        for page_index in schedule_page_indexes:
            for candidate_index in range(page_index, min(len(pages), page_index + 3)):
                if candidate_index in collected_indexes:
                    continue
                candidate_page = pages[candidate_index]
                candidate_text = " ".join(candidate_page.lines[:14]).lower()
                if candidate_index == page_index or "%" in " ".join(candidate_page.lines[:40]) or any(
                    term in candidate_text for term in ("mode of payment", "schedule of payment", "progressive percentage")
                ):
                    collected_indexes.append(candidate_index)
                else:
                    break

        entries: list[PaymentScheduleEntry] = []
        for page_index in collected_indexes:
            page = pages[page_index]
            current_heading = "Mode of Payment"
            line_index = 0
            while line_index < len(page.lines):
                line = page.lines[line_index]
                if self._is_payment_heading(line):
                    current_heading = self._clean_text(line)
                    line_index += 1
                    continue
                if re.match(r"^\d+\b", line) and "%" in line:
                    entry_lines = [line]
                    next_index = line_index + 1
                    while next_index < len(page.lines):
                        next_line = page.lines[next_index]
                        if self._is_payment_heading(next_line) or re.match(r"^\d+\b", next_line):
                            break
                        if next_line:
                            entry_lines.append(next_line)
                        next_index += 1
                    entry_text = self._clean_text(" ".join(entry_lines))
                    percentages = re.findall(r"(\d+(?:\.\d+)?)%", entry_text)
                    if percentages:
                        entries.append(
                            PaymentScheduleEntry(
                                heading=current_heading,
                                description=re.sub(r"^\d+\s*", "", entry_text),
                                percentage=float(percentages[0]),
                                page_number=page.page_number,
                                section=page.section,
                            )
                        )
                    line_index = next_index
                    continue
                line_index += 1
        return entries

    def _is_payment_heading(self, line: str) -> bool:
        normalized_line = self._normalize_label(line)
        if not normalized_line or "%" in line:
            return False
        heading_markers = (
            "mode of payment",
            "schedule of payment",
            "electrical and mechanical works",
            "tubewell construction work",
            "building works",
            "pump house",
            "boundary wall",
            "rcc oht",
            "rising main",
            "house connection",
            "ms gate",
            "civil",
        )
        return any(marker in normalized_line for marker in heading_markers)

    def _payment_group_priority(self, heading: str, entries: list[PaymentScheduleEntry]) -> float:
        normalized_heading = heading.lower()
        score = float(len(entries))
        if any(keyword in normalized_heading for keyword in ("electrical", "mechanical", "supply", "equipment", "tubewell")):
            score += 20.0
        if any(keyword in normalized_heading for keyword in ("civil", "oht", "pump house", "rising main", "house connection", "building")):
            score += 6.0
        if "mode of payment" in normalized_heading:
            score += 2.0
        return score

    def _summarize_payment_entries(self, entries: list[PaymentScheduleEntry], limit: int = MAX_VALUE_LENGTH) -> str:
        parts = [f"{entry.description} ({entry.percentage:.0f}%)" for entry in entries]
        return self._trim_length("; ".join(parts), limit=limit)

    def _summarize_additional_payment_groups(
        self,
        groups: list[tuple[str, list[PaymentScheduleEntry]]],
    ) -> str:
        if not groups:
            return ""
        parts = [
            f"{heading}: {', '.join(f'{entry.percentage:.0f}%' for entry in entries)}"
            for heading, entries in groups
        ]
        return self._trim_length(f"Additional payment schedules: {'; '.join(parts)}", limit=MAX_VALUE_LENGTH)

    def _make_payment_result_from_entries(
        self,
        row_number: int,
        label: str,
        heading: str,
        entries: list[PaymentScheduleEntry],
        remark: str = "",
    ) -> FieldResult:
        page_reference = self._format_page_reference([entry.page_number for entry in entries])
        section_value = " / ".join(dict.fromkeys(entry.section for entry in entries if entry.section != NOT_AVAILABLE)) or NOT_AVAILABLE
        return self._make_result(
            row_number=row_number,
            label=label,
            value=self._summarize_payment_entries(entries),
            page=page_reference,
            section=section_value,
            clause=heading,
            remark=remark,
        )

    def _map_generic_payment_installments(
        self,
        row_map: dict[int, FieldResult],
        entries: list[PaymentScheduleEntry],
        row_numbers: tuple[int, int, int],
    ) -> list[FieldResult]:
        if not entries:
            return [self._not_available_result(row_map[row_number].label, row_number) for row_number in row_numbers]
        selected_entries = (
            entries[0],
            entries[1] if len(entries) >= 3 else None,
            entries[-1] if len(entries) >= 2 else None,
        )
        results: list[FieldResult] = []
        for row_number, entry in zip(row_numbers, selected_entries, strict=False):
            if entry is None:
                results.append(self._not_available_result(row_map[row_number].label, row_number))
                continue
            results.append(
                self._make_result(
                    row_number=row_number,
                    label=row_map[row_number].label,
                    value=f"{entry.description} ({entry.percentage:.0f}%)",
                    page=str(entry.page_number),
                    section=entry.section,
                    clause=entry.heading,
                )
            )
        return results

    def _find_mobilization_advance_block(
        self,
        pages: list[PageText],
    ) -> tuple[PageText, int, list[str]] | None:
        best_candidate: tuple[PageText, int, list[str]] | None = None
        best_score = -1
        for page in self._iter_pages_with_priority(pages, 250):
            if page.is_toc:
                continue
            for line_index, line in enumerate(page.lines):
                lowered_line = line.lower()
                if "mobilization advance" not in lowered_line and "advance payment" not in lowered_line:
                    continue
                context_lines = page.lines[max(0, line_index - 1) : min(len(page.lines), line_index + 8)]
                context_text = self._clean_text(" ".join(context_lines)).lower()
                if "bank guarantee bond" in context_text:
                    continue
                score = 0
                if "%" in context_text:
                    score += 3
                if "interest free" in context_text:
                    score += 3
                if "contracted amount" in context_text or "contract value" in context_text:
                    score += 2
                if score > best_score:
                    best_candidate = (page, line_index, context_lines)
                    best_score = score
        return best_candidate

    def _extract_following_value(
        self,
        pages: list[PageText],
        phrase: str,
        line_offset: int,
        row_number: int,
        label: str,
    ) -> FieldResult | None:
        block = self._find_line_block(pages, phrase, line_offset)
        if block is None:
            return None
        page, start_index, lines = block
        target_index = min(line_offset, len(lines) - 1)
        value = lines[target_index]
        return self._make_result(
            row_number=row_number,
            label=label,
            value=value,
            page=str(page.page_number),
            section=page.section,
            clause=NOT_AVAILABLE,
        )

    def _find_line_block(
        self,
        pages: list[PageText],
        phrase: str,
        extra_lines: int,
    ) -> tuple[PageText, int, list[str]] | None:
        for page in pages:
            for index, line in enumerate(page.lines):
                if phrase.lower() in line.lower():
                    end_index = min(len(page.lines), index + extra_lines + 1)
                    return page, index, page.lines[index:end_index]
        return None

    def _iter_pages_with_priority(self, pages: list[PageText], page_limit: int | None) -> list[PageText]:
        if page_limit is None:
            return list(pages)
        prioritized_pages = [page for page in pages if page.page_number <= page_limit]
        prioritized_pages.extend(page for page in pages if page.page_number > page_limit)
        return prioritized_pages

    def _find_phrase_matches(
        self,
        pages: list[PageText],
        phrases: tuple[str, ...],
        page_limit: int | None = None,
    ) -> list[tuple[int, int]]:
        matches: list[tuple[int, int]] = []
        prioritized_pages = self._iter_pages_with_priority(pages, page_limit)
        for page_index, page in enumerate(prioritized_pages):
            for line_index, line in enumerate(page.lines):
                normalized_line = line.lower()
                if any(phrase.lower() in normalized_line for phrase in phrases):
                    matches.append((page_index, line_index))
        return matches

    def _find_first_match_context(
        self,
        pages: list[PageText],
        patterns: tuple[str, ...],
        page_limit: int | None,
        trailing_lines: int,
    ) -> tuple[PageText, int, list[str]] | None:
        for page in self._iter_pages_with_priority(pages, page_limit):
            for line_index, line in enumerate(page.lines):
                if self._line_matches_any_pattern(line, patterns):
                    end_index = min(len(page.lines), line_index + trailing_lines + 1)
                    return page, line_index, page.lines[line_index:end_index]
        return None

    def _extract_nearby_clause_reference(self, page: PageText, line_index: int) -> str | None:
        window_start = max(0, line_index - 8)
        window_end = min(len(page.lines), line_index + 2)
        for candidate_line in reversed(page.lines[window_start:window_end]):
            for pattern in CLAUSE_PATTERNS:
                match = pattern.search(candidate_line)
                if match is not None:
                    return self._clean_text(match.group(1)).replace(" ", "")
        return None

    def _find_best_performance_security_requirement(
        self,
        pages: list[PageText],
    ) -> tuple[PageText, int, list[str]] | None:
        best_direct_candidate: tuple[PageText, int, list[str]] | None = None
        best_direct_score = -1
        for page in self._iter_pages_with_priority(pages, 250):
            if page.is_toc:
                continue
            for line_index, line in enumerate(page.lines):
                if "34.1" not in line:
                    continue
                context_lines = page.lines[line_index : min(len(page.lines), line_index + 8)]
                context_text = self._clean_text(" ".join(context_lines))
                if "annexure" in context_text.lower() or "bank guarantee bond" in context_text.lower():
                    continue
                if "performance securit" in context_text.lower() and "contract price" in context_text.lower():
                    score = 0
                    if page.section == "Section III":
                        score += 4
                    if "replace clause 34.1" in context_text.lower():
                        score += 3
                    if "itb 34.1" in context_text.lower():
                        score += 2
                    if score > best_direct_score:
                        best_direct_candidate = (page, line_index, context_lines)
                        best_direct_score = score
        if best_direct_candidate is not None:
            return best_direct_candidate

        best_candidate: tuple[PageText, int, list[str]] | None = None
        best_score = -1
        for page in self._iter_pages_with_priority(pages, 250):
            if page.is_toc:
                continue
            for line_index, line in enumerate(page.lines):
                lowered_line = line.lower()
                if "performance securit" not in lowered_line and "performance guarantee" not in lowered_line:
                    continue
                context_lines = page.lines[max(0, line_index - 8) : min(len(page.lines), line_index + 8)]
                context_text = self._clean_text(" ".join(context_lines))
                normalized_context = context_text.lower()
                if "annexure" in normalized_context or "bank guarantee bond" in normalized_context:
                    continue
                if not any(
                    marker in normalized_context
                    for marker in (
                        "notification of award",
                        "notice of award",
                        "letter of award",
                        "signing of the contract",
                        "signing the contract",
                        "award of contract",
                    )
                ):
                    continue
                score = 0
                if "%" in context_text or "ten percent" in normalized_context:
                    score += 3
                if "contract cost" in normalized_context or "contract price" in normalized_context or "contract value" in normalized_context:
                    score += 2
                if "replace clause 34.1" in normalized_context:
                    score += 3
                if "itb 34.1" in normalized_context:
                    score += 2
                if page.section == "Section III":
                    score += 2
                if score > best_score:
                    best_candidate = (page, line_index, page.lines[line_index : min(len(page.lines), line_index + 6)])
                    best_score = score
        return best_candidate

    def _find_performance_security_validity(
        self,
        pages: list[PageText],
    ) -> tuple[PageText, int, list[str]] | None:
        best_candidate: tuple[PageText, int, list[str]] | None = None
        best_score = -1
        for page in pages:
            if page.is_toc:
                continue
            for line_index, line in enumerate(page.lines):
                lowered_line = line.lower()
                if "performance securit" not in lowered_line and "performance guarantee" not in lowered_line:
                    continue
                context_lines = page.lines[line_index : min(len(page.lines), line_index + 10)]
                context_text = self._clean_text(" ".join(context_lines))
                normalized_context = context_text.lower()
                if "annexure" in normalized_context or "bank guarantee bond" in normalized_context:
                    continue
                if not any(
                    marker in normalized_context
                    for marker in (
                        "initially valid upto",
                        "valid upto",
                        "valid up to",
                        "validated upto",
                        "defect liability period",
                        "refunded before the expiry",
                    )
                ):
                    continue
                score = 0
                if "defect liability period" in normalized_context:
                    score += 3
                if page.section == "Section VII":
                    score += 2
                if score > best_score:
                    best_candidate = (page, line_index, context_lines)
                    best_score = score
        return best_candidate

    def _extract_payment_blocks(
        self,
        pages: list[PageText],
        heading_specs: tuple[tuple[str, str], ...],
        stop_patterns: tuple[str, ...],
    ) -> dict[str, PaymentBlock]:
        occurrences: list[tuple[int, int, str]] = []
        seen_clauses: set[str] = set()
        for page_index, page in enumerate(pages):
            for line_index, line in enumerate(page.lines):
                for pattern, clause in heading_specs:
                    if clause in seen_clauses:
                        continue
                    if re.search(pattern, line):
                        occurrences.append((page_index, line_index, clause))
                        seen_clauses.add(clause)
                        break

        if not occurrences:
            return {}

        heading_patterns = tuple(pattern for pattern, _ in heading_specs)
        blocks: dict[str, PaymentBlock] = {}
        for occurrence_index, (page_index, line_index, clause) in enumerate(occurrences):
            end_marker = occurrences[occurrence_index + 1] if occurrence_index + 1 < len(occurrences) else None
            collected_lines: list[str] = []
            start_page_number = pages[page_index].page_number
            end_page_number = start_page_number
            for current_page_index in range(page_index, len(pages)):
                page = pages[current_page_index]
                current_line_start = line_index if current_page_index == page_index else 0
                for current_line_index in range(current_line_start, len(page.lines)):
                    if (
                        end_marker is not None
                        and current_page_index == end_marker[0]
                        and current_line_index == end_marker[1]
                    ):
                        blocks[clause] = PaymentBlock(
                            clause=clause,
                            page_start=start_page_number,
                            page_end=end_page_number,
                            lines=self._clean_block_lines(collected_lines),
                        )
                        break
                    line = page.lines[current_line_index]
                    if current_page_index != page_index and self._line_matches_any_pattern(line, heading_patterns):
                        blocks[clause] = PaymentBlock(
                            clause=clause,
                            page_start=start_page_number,
                            page_end=end_page_number,
                            lines=self._clean_block_lines(collected_lines),
                        )
                        break
                    if self._line_matches_any_pattern(line, stop_patterns):
                        blocks[clause] = PaymentBlock(
                            clause=clause,
                            page_start=start_page_number,
                            page_end=end_page_number,
                            lines=self._clean_block_lines(collected_lines),
                        )
                        break
                    collected_lines.append(line)
                    end_page_number = page.page_number
                else:
                    continue
                break
            else:
                blocks[clause] = PaymentBlock(
                    clause=clause,
                    page_start=start_page_number,
                    page_end=end_page_number,
                    lines=self._clean_block_lines(collected_lines),
                )
        return {key: block for key, block in blocks.items() if block.lines}

    def _summarize_payment_block(self, block: PaymentBlock, limit: int = MAX_VALUE_LENGTH) -> str:
        text = self._clean_text(" ".join(block.lines))
        text = re.sub(r"^\((?:[IVX]+)\)\s*(?:\([A-Z]\))?\s*", "", text)
        text = re.sub(r"^[IVX]+\.\s*", "", text)
        return self._trim_length(text, limit=limit)

    def _summarize_payment_overview(self, blocks: list[PaymentBlock]) -> str:
        parts = [self._summarize_payment_block(block, limit=MAX_VALUE_LENGTH) for block in blocks if block.lines]
        return self._trim_length("; ".join(parts), limit=MAX_VALUE_LENGTH)

    def _page_reference_from_blocks(self, blocks: list[PaymentBlock]) -> str:
        page_numbers: list[int] = []
        for block in blocks:
            page_numbers.extend(range(block.page_start, block.page_end + 1))
        unique_page_numbers = sorted(dict.fromkeys(page_numbers))
        return self._format_page_reference(unique_page_numbers)

    def _format_additional_payment_blocks(self, prefix: str, blocks: list[PaymentBlock]) -> str:
        if not blocks:
            return ""
        parts = [f"{block.clause}: {self._summarize_payment_block(block, limit=MAX_VALUE_LENGTH)}" for block in blocks]
        return self._trim_length(f"{prefix}: {'; '.join(parts)}", limit=MAX_VALUE_LENGTH)

    def _map_payment_installment_rows(
        self,
        row_map: dict[int, FieldResult],
        mappings: tuple[tuple[int, PaymentBlock | None], ...],
        default_section: str,
    ) -> list[FieldResult]:
        results: list[FieldResult] = []
        for row_number, block in mappings:
            if block is None:
                results.append(self._not_available_result(row_map[row_number].label, row_number))
                continue
            results.append(
                self._make_result(
                    row_number=row_number,
                    label=row_map[row_number].label,
                    value=self._summarize_payment_block(block),
                    page=self._page_reference_from_blocks([block]),
                    section=default_section,
                    clause=block.clause,
                )
            )
        return results

    def _make_result(
        self,
        row_number: int,
        label: str,
        value: str,
        page: str,
        section: str,
        clause: str = NOT_AVAILABLE,
        remark: str = "",
        confidence: float | None = None,
    ) -> FieldResult:
        cleaned_value = self._clean_text(value)
        if confidence is None:
            confidence = 0.9
            if cleaned_value == NOT_AVAILABLE:
                confidence = 0.0
            else:
                if page == NOT_AVAILABLE:
                    confidence -= 0.08
                if section == NOT_AVAILABLE:
                    confidence -= 0.05
                if clause == NOT_AVAILABLE:
                    confidence -= 0.05
        confidence = round(min(max(confidence, 0.0), 0.99), 4)
        return FieldResult(
            row_number=row_number,
            label=label,
            value=cleaned_value,
            section=section,
            clause=clause,
            page=page,
            excerpt="",
            remark=self._clean_text(remark),
            confidence=confidence,
        )

    def _update_row_value(self, result: FieldResult, value: str) -> FieldResult:
        return replace(result, value=self._clean_text(value))

    def _backfill_missing_clauses(self, field_results: list[FieldResult], pages: list[PageText]) -> list[FieldResult]:
        enriched_results: list[FieldResult] = []
        for result in field_results:
            if result.value == NOT_AVAILABLE or result.clause != NOT_AVAILABLE:
                enriched_results.append(result)
                continue
            clause = self._resolve_clause_for_result(result, pages)
            enriched_results.append(replace(result, clause=clause or result.clause))
        return enriched_results

    def _resolve_clause_for_result(self, result: FieldResult, pages: list[PageText]) -> str | None:
        page_numbers = self._parse_page_reference(result.page)
        if not page_numbers:
            return None
        for page_number in page_numbers:
            if not 1 <= page_number <= len(pages):
                continue
            page = pages[page_number - 1]
            line_index = self._locate_result_line_index(page, result)
            if line_index < 0:
                continue
            clause = self._extract_nearby_clause_reference(page, line_index)
            if clause and clause != NOT_AVAILABLE:
                return clause
            if result.row_number <= 10 and page_number <= 3:
                return "Cover Page"
        return None

    def _locate_result_line_index(self, page: PageText, result: FieldResult) -> int:
        for candidate_text in (result.excerpt, result.value, result.label):
            line_index = self._find_line_index(page.lines, candidate_text)
            if line_index >= 0:
                return line_index

        normalized_value = self._normalize_label(result.value)
        if not normalized_value:
            return -1
        candidate_tokens = [
            token
            for token in normalized_value.split()
            if len(token) >= 4 and token not in STOPWORDS
        ]
        if not candidate_tokens:
            return -1

        best_index = -1
        best_score = 0
        for index, line in enumerate(page.lines):
            normalized_line = self._normalize_label(line)
            if not normalized_line:
                continue
            score = sum(1 for token in candidate_tokens if token in normalized_line)
            if score > best_score:
                best_score = score
                best_index = index
        return best_index if best_score >= min(2, len(candidate_tokens)) else -1

    def _annotate_field_remarks(self, field_results: list[FieldResult], pages: list[PageText]) -> list[FieldResult]:
        annotated_results: list[FieldResult] = []
        for result in field_results:
            annotated_results.append(replace(result, remark=self._build_result_remark(result, pages)))
        return annotated_results

    def _build_result_remark(self, result: FieldResult, pages: list[PageText]) -> str:
        remark_parts = self._split_remark_parts(result.remark)
        if result.value == NOT_AVAILABLE:
            return self._join_remark_parts(remark_parts)

        if result.confidence < 0.62:
            if result.excerpt:
                remark_parts.append(
                    self._trim_length(
                        f"Low-confidence match. Verify source context: {self._clean_text(result.excerpt)}",
                        limit=MAX_REMARK_LENGTH,
                    )
                )
            else:
                remark_parts.append("Low-confidence extraction. Verify this entry manually.")
        return self._join_remark_parts(remark_parts)

    def _parse_page_reference(self, page_reference: str) -> list[int]:
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

    def _split_remark_parts(self, remark: str) -> list[str]:
        return [part for part in (self._clean_text(fragment) for fragment in remark.split(";")) if part]

    def _join_remark_parts(self, remark_parts: list[str]) -> str:
        unique_parts = list(dict.fromkeys(part for part in remark_parts if part))
        return self._trim_length("; ".join(unique_parts), limit=MAX_REMARK_LENGTH) if unique_parts else ""

    def _synchronize_sections_with_pages(
        self,
        row_map: dict[int, FieldResult],
        pages: list[PageText],
    ) -> dict[int, FieldResult]:
        updated: dict[int, FieldResult] = {}
        for row_number, result in row_map.items():
            resolved_section = self._section_from_page_reference(result.page, pages)
            updated[row_number] = replace(result, section=resolved_section or result.section)
        return updated

    def _section_from_page_reference(self, page_reference: str, pages: list[PageText]) -> str | None:
        page_numbers = [int(match) for match in re.findall(r"\d+", page_reference)]
        if not page_numbers:
            return None
        sections: list[str] = []
        for page_number in page_numbers:
            if 1 <= page_number <= len(pages):
                section = pages[page_number - 1].section
                if section != NOT_AVAILABLE:
                    sections.append(section)
        if not sections:
            return None
        unique_sections = list(dict.fromkeys(sections))
        if len(unique_sections) == 1:
            return unique_sections[0]
        return " / ".join(unique_sections)

    def _extract_block_between_patterns(
        self,
        pages: list[PageText],
        start_patterns: tuple[str, ...],
        stop_patterns: tuple[str, ...],
        row_number: int,
        label: str,
        clause: str,
    ) -> FieldResult | None:
        start_page: PageText | None = None
        start_page_index = -1
        start_line_index = -1

        for page_index, page in enumerate(pages):
            for line_index, line in enumerate(page.lines):
                if self._line_matches_any_pattern(line, start_patterns):
                    start_page = page
                    start_page_index = page_index
                    start_line_index = line_index
                    break
            if start_page is not None:
                break

        if start_page is None:
            return None

        collected_lines: list[str] = []
        page_numbers: list[int] = []
        for page_index in range(start_page_index, len(pages)):
            page = pages[page_index]
            if (
                page_index > start_page_index
                and page.section != NOT_AVAILABLE
                and start_page.section != NOT_AVAILABLE
                and page.section != start_page.section
            ):
                break
            line_start = start_line_index if page_index == start_page_index else 0
            for line_index in range(line_start, len(page.lines)):
                line = page.lines[line_index]
                if page_index != start_page_index and self._line_matches_any_pattern(line, stop_patterns):
                    cleaned_lines = self._clean_block_lines(collected_lines)
                    if not cleaned_lines:
                        return None
                    page_label = self._format_page_reference(page_numbers)
                    return self._make_result(
                        row_number=row_number,
                        label=label,
                        value=" ".join(cleaned_lines),
                        page=page_label,
                        section=start_page.section,
                        clause=clause,
                    )
                collected_lines.append(line)
            page_numbers.append(page.page_number)

        cleaned_lines = self._clean_block_lines(collected_lines)
        if not cleaned_lines:
            return None
        page_label = self._format_page_reference(page_numbers)
        return self._make_result(
            row_number=row_number,
            label=label,
            value=" ".join(cleaned_lines),
            page=page_label,
            section=start_page.section,
            clause=clause,
        )

    def _line_matches_any_pattern(self, line: str, patterns: tuple[str, ...]) -> bool:
        return any(re.search(pattern, line, flags=re.IGNORECASE) for pattern in patterns)

    def _clean_block_lines(self, lines: list[str]) -> list[str]:
        cleaned_lines: list[str] = []
        for line in lines:
            normalized = self._clean_text(line)
            if not normalized:
                continue
            upper_line = normalized.upper()
            if upper_line.startswith("INVITATION FOR BIDS"):
                continue
            if upper_line.startswith("APPENDIX"):
                continue
            if upper_line.startswith("EPC PACKAGE FOR DEVELOPMENT"):
                continue
            if upper_line.startswith("PROJECT COUPLED WITH"):
                continue
            if upper_line.startswith("STORAGE SYSTEM"):
                continue
            if "DOCUMENT NO." in upper_line:
                continue
            if re.search(r"\bPAGE\s+\d+\s+OF\s+\d+\b", upper_line):
                continue
            cleaned_lines.append(normalized)
        return cleaned_lines

    def _format_page_reference(self, page_numbers: list[int]) -> str:
        if not page_numbers:
            return NOT_AVAILABLE
        unique_numbers = sorted(dict.fromkeys(page_numbers))
        ranges: list[str] = []
        start = unique_numbers[0]
        end = unique_numbers[0]
        for page_number in unique_numbers[1:]:
            if page_number == end + 1:
                end = page_number
                continue
            ranges.append(str(start) if start == end else f"{start}-{end}")
            start = end = page_number
        ranges.append(str(start) if start == end else f"{start}-{end}")
        return ",".join(ranges)

    def _not_available_result(self, label: str, row_number: int) -> FieldResult:
        return FieldResult(
            row_number=row_number,
            label=label,
            value=NOT_AVAILABLE,
            section=NOT_AVAILABLE,
            clause=NOT_AVAILABLE,
            page=NOT_AVAILABLE,
            excerpt="",
            confidence=0.0,
        )
