from __future__ import annotations

import base64
import json
import logging
import os
import re
from io import BytesIO
from pathlib import Path
from typing import Protocol

from env_loader import load_local_env
from field_config import FIELD_CONFIGS, NOT_AVAILABLE

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - optional dependency during local editing
    OpenAI = None  # type: ignore[assignment]


load_local_env()

logger = logging.getLogger(__name__)

DEFAULT_OCR_MODEL = "gpt-4o-mini"
DEFAULT_EXTRACTION_MODEL = "gpt-4o-mini"
DEFAULT_DOCUMENT_MAX_CHARS = 1_200_000
DEFAULT_MAX_FILE_BYTES = 50 * 1024 * 1024


class PageLike(Protocol):
    page_number: int
    text: str
    section: str
    ocr_applied: bool


class OpenAIDocumentIntelligence:
    def __init__(self) -> None:
        self._client: object | None = None

    def ocr_available(self) -> bool:
        return bool(self._api_key and OpenAI is not None)

    def extraction_available(self) -> bool:
        return self.ocr_available()

    @property
    def _api_key(self) -> str:
        return os.getenv("OPENAI_API_KEY", "").strip()

    @property
    def ocr_model(self) -> str:
        return os.getenv("OPENAI_OCR_MODEL", DEFAULT_OCR_MODEL).strip() or DEFAULT_OCR_MODEL

    @property
    def extraction_model(self) -> str:
        return os.getenv("OPENAI_EXTRACTION_MODEL", DEFAULT_EXTRACTION_MODEL).strip() or DEFAULT_EXTRACTION_MODEL

    @property
    def document_max_chars(self) -> int:
        raw_value = os.getenv("OPENAI_DOCUMENT_MAX_CHARS", str(DEFAULT_DOCUMENT_MAX_CHARS)).strip()
        try:
            return max(120_000, int(raw_value))
        except ValueError:
            return DEFAULT_DOCUMENT_MAX_CHARS

    @property
    def max_file_bytes(self) -> int:
        raw_value = os.getenv("OPENAI_MAX_FILE_BYTES", str(DEFAULT_MAX_FILE_BYTES)).strip()
        try:
            return max(1_000_000, min(int(raw_value), DEFAULT_MAX_FILE_BYTES))
        except ValueError:
            return DEFAULT_MAX_FILE_BYTES

    def describe_ocr_status(self) -> str:
        if OpenAI is None:
            return "OCR unavailable in this environment. Install the openai package and set OPENAI_API_KEY."
        if not self._api_key:
            return "OCR unavailable. Set OPENAI_API_KEY to enable OpenAI OCR and field correction."
        return f"OpenAI OCR enabled via {self.ocr_model}"

    def transcribe_image(self, image: object, *, paragraph: bool = True) -> str:
        if not self.ocr_available():
            return ""
        data_url = self._image_to_data_url(image)
        if not data_url:
            return ""

        instruction = (
            "Transcribe every visible character from this tender document image. "
            "Preserve reading order and line breaks. Include table text, headers, numbers, currency, and dates. "
            "Return plain text only."
        )
        if not paragraph:
            instruction = (
                "Transcribe every visible character from this tender document crop. "
                "Preserve short lines and table-like fragments. Return plain text only."
            )

        try:
            response = self._get_client().responses.create(
                model=self.ocr_model,
                input=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": instruction},
                            {"type": "input_image", "image_url": data_url},
                        ],
                    }
                ],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("OpenAI OCR request failed: %s", exc)
            return ""
        return self._clean_text(self._response_text(response))

    def extract_rows(
        self,
        source_path: Path,
        pages: list[PageLike],
        current_rows: list[dict[str, object]],
        target_row_numbers: list[int] | None = None,
    ) -> dict[int, dict[str, object]]:
        if not self.extraction_available():
            return {}
        target_configs = [
            config for config in FIELD_CONFIGS
            if not target_row_numbers or config.row_number in set(target_row_numbers)
        ]
        if not target_configs:
            return {}

        field_lines = [f"{config.row_number}: {config.label}" for config in target_configs]
        existing_row_lines = []
        for row in current_rows:
            row_number = int(row.get("row_number", 0))
            if target_row_numbers and row_number not in target_row_numbers:
                continue
            label = str(row.get("label", "")).strip()
            value = str(row.get("value", NOT_AVAILABLE)).strip()
            section = str(row.get("section", NOT_AVAILABLE)).strip()
            clause = str(row.get("clause", NOT_AVAILABLE)).strip()
            page = str(row.get("page", NOT_AVAILABLE)).strip()
            remark = str(row.get("remark", "")).strip()
            existing_row_lines.append(
                f"{row_number}: {label} | value={value} | section={section} | clause={clause} | page={page} | remark={remark}"
            )

        prompt_lines = [
            "You extract structured tender synopsis data from procurement documents.",
            f"Document name: {source_path.name}",
            "Return JSON only with this exact schema:",
            '{"rows":[{"row_number":4,"label":"...","value":"...","section":"...","clause":"...","page":"...","remark":"...","confidence":0.0}]}',
            f'Use "{NOT_AVAILABLE}" when a value is genuinely absent.',
            "If the source contains extra detail that does not fit cleanly into the main value, keep the main value concise and place the extra detail in remark.",
            "Keep dates, times, units, percentages, and INR amounts exactly as written when possible.",
            'Page values must use formats like "7" or "7-9" or "7,9".',
            "Confidence must be between 0.0 and 0.99.",
            "Fill section, clause, and page whenever the source supports it.",
            "",
            "Rows to fill:",
            *field_lines,
            "",
            "Current extracted rows to correct if needed:",
            *(existing_row_lines or ["None"]),
        ]

        request_content: list[dict[str, str]] = []
        if source_path.suffix.lower() == ".pdf" and self._can_attach_file(source_path):
            prompt_lines.extend(
                [
                    "",
                    "Use the attached PDF as the primary source of truth.",
                    "Review all relevant pages needed for the requested rows before answering.",
                ]
            )
        else:
            document_text = self._build_document_payload(pages)
            if not document_text:
                return {}
            prompt_lines.extend(
                [
                    "",
                    "Tender document text with page markers:",
                    document_text,
                ]
            )

        prompt = "\n".join(
            [
                *prompt_lines,
            ]
        )
        request_content.append({"type": "input_text", "text": prompt})
        if source_path.suffix.lower() == ".pdf" and self._can_attach_file(source_path):
            request_content.append(
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": self._file_to_base64(source_path),
                    },
                }
            )

        try:
            response = self._get_client().responses.create(
                model=self.extraction_model,
                input=[
                    {
                        "role": "user",
                        "content": request_content,
                    }
                ],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("OpenAI extraction request failed: %s", exc)
            return {}

        response_text = self._response_text(response)
        payload = self._parse_json_payload(response_text)
        if not isinstance(payload, dict):
            return {}

        row_map: dict[int, dict[str, object]] = {}
        for row in payload.get("rows", []):
            if not isinstance(row, dict):
                continue
            try:
                row_number = int(row.get("row_number", 0))
            except (TypeError, ValueError):
                continue
            matching_config = next((config for config in FIELD_CONFIGS if config.row_number == row_number), None)
            if matching_config is None:
                continue
            row_map[row_number] = {
                "row_number": row_number,
                "label": matching_config.label,
                "value": self._normalize_value(row.get("value"), empty_fallback=NOT_AVAILABLE),
                "section": self._normalize_value(row.get("section"), empty_fallback=NOT_AVAILABLE),
                "clause": self._normalize_value(row.get("clause"), empty_fallback=NOT_AVAILABLE),
                "page": self._normalize_page(row.get("page")),
                "remark": self._normalize_value(row.get("remark"), empty_fallback=""),
                "confidence": self._normalize_confidence(row.get("confidence")),
            }
        return row_map

    def _can_attach_file(self, source_path: Path) -> bool:
        try:
            return source_path.is_file() and source_path.stat().st_size <= self.max_file_bytes
        except OSError:
            return False

    def _file_to_base64(self, source_path: Path) -> str:
        return base64.b64encode(source_path.read_bytes()).decode("utf-8")

    def _get_client(self) -> object:
        if self._client is not None:
            return self._client
        if OpenAI is None:
            raise RuntimeError("openai package is not installed.")
        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        self._client = OpenAI(api_key=self._api_key)
        return self._client

    def _build_document_payload(self, pages: list[PageLike]) -> str:
        page_blocks: list[str] = []
        for page in pages:
            text = self._clean_text(page.text)
            marker = f"[Page {page.page_number} | Section: {page.section} | OCR used: {'yes' if page.ocr_applied else 'no'}]"
            page_blocks.append(f"{marker}\n{text}")
        joined_text = "\n\n".join(page_blocks).strip()
        if len(joined_text) <= self.document_max_chars:
            return joined_text

        per_page_limit = max(1800, int(self.document_max_chars / max(len(pages), 1)) - 120)
        compact_blocks: list[str] = []
        for page in pages:
            text = self._clean_text(page.text)
            if len(text) > per_page_limit:
                head_limit = max(1200, int(per_page_limit * 0.7))
                tail_limit = max(400, per_page_limit - head_limit - 32)
                text = "\n".join(
                    [
                        text[:head_limit].rstrip(),
                        "[... trimmed for prompt size ...]",
                        text[-tail_limit:].lstrip(),
                    ]
                )
            marker = f"[Page {page.page_number} | Section: {page.section} | OCR used: {'yes' if page.ocr_applied else 'no'}]"
            compact_blocks.append(f"{marker}\n{text}")
        return "\n\n".join(compact_blocks).strip()

    def _image_to_data_url(self, image: object) -> str:
        if not hasattr(image, "save"):
            return ""
        buffer = BytesIO()
        try:
            image.save(buffer, format="PNG")
        except Exception:  # noqa: BLE001
            return ""
        encoded = base64.b64encode(buffer.getvalue()).decode("utf-8")
        return f"data:image/png;base64,{encoded}"

    def _response_text(self, response: object) -> str:
        output_text = getattr(response, "output_text", "")
        if isinstance(output_text, str) and output_text.strip():
            return output_text

        pieces: list[str] = []
        for output in getattr(response, "output", []) or []:
            for content in getattr(output, "content", []) or []:
                text_value = getattr(content, "text", "")
                if isinstance(text_value, str) and text_value.strip():
                    pieces.append(text_value.strip())
        return "\n".join(pieces).strip()

    def _parse_json_payload(self, text: str) -> dict[str, object] | None:
        normalized_text = text.strip()
        if normalized_text.startswith("```"):
            normalized_text = re.sub(r"^```(?:json)?\s*", "", normalized_text, flags=re.IGNORECASE)
            normalized_text = re.sub(r"\s*```$", "", normalized_text)
        try:
            parsed = json.loads(normalized_text)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            pass

        start = normalized_text.find("{")
        end = normalized_text.rfind("}")
        if start < 0 or end <= start:
            logger.warning("OpenAI extraction response did not contain parseable JSON.")
            return None
        try:
            parsed = json.loads(normalized_text[start : end + 1])
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            logger.warning("OpenAI extraction response contained invalid JSON.")
            return None

    def _normalize_confidence(self, value: object) -> float:
        try:
            return round(min(max(float(value), 0.0), 0.99), 4)
        except (TypeError, ValueError):
            return 0.0

    def _normalize_page(self, value: object) -> str:
        text = self._normalize_value(value, empty_fallback=NOT_AVAILABLE)
        if text == NOT_AVAILABLE:
            return text
        fragments = [fragment.strip() for fragment in re.split(r"\s*,\s*", text) if fragment.strip()]
        normalized_fragments: list[str] = []
        for fragment in fragments:
            range_match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", fragment)
            if range_match is not None:
                normalized_fragments.append(f"{int(range_match.group(1))}-{int(range_match.group(2))}")
                continue
            if fragment.isdigit():
                normalized_fragments.append(str(int(fragment)))
        return ",".join(normalized_fragments) if normalized_fragments else NOT_AVAILABLE

    def _normalize_value(self, value: object, *, empty_fallback: str) -> str:
        text = self._clean_text(str(value or ""))
        if not text:
            return empty_fallback
        return text

    def _clean_text(self, value: str) -> str:
        text = value.replace("\r", "\n")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()
