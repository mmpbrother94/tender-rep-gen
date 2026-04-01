from __future__ import annotations

import json
import sys
from pathlib import Path

from PIL import Image, ImageOps

from openai_document_intelligence import OpenAIDocumentIntelligence


def _prepare_image(image_path: Path) -> Image.Image:
    with Image.open(image_path) as source_image:
        prepared_image = ImageOps.exif_transpose(source_image).convert("L")
        prepared_image = ImageOps.autocontrast(prepared_image)
        width, height = prepared_image.size
        longest_side = max(width, height, 1)
        target_longest_side = 2600
        scale = min(3.0, target_longest_side / longest_side) if longest_side < target_longest_side else 1.0
        if scale > 1.05:
            resampling = getattr(Image, "Resampling", Image)
            prepared_image = prepared_image.resize(
                (max(1, int(width * scale)), max(1, int(height * scale))),
                resampling.LANCZOS,
            )
        return prepared_image.copy()


def main() -> int:
    if len(sys.argv) < 2:
        return 1

    image_path = Path(sys.argv[1])
    paragraph = "--no-paragraph" not in sys.argv[2:]
    intelligence = OpenAIDocumentIntelligence()
    if not intelligence.ocr_available():
        raise SystemExit("OpenAI OCR is unavailable. Set OPENAI_API_KEY and install the openai package.")

    prepared_image = _prepare_image(image_path)
    text = intelligence.transcribe_image(prepared_image, paragraph=paragraph)
    sys.stdout.write(json.dumps({"text": text}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
