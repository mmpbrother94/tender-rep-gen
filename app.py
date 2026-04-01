from __future__ import annotations

import logging
import os
import re
import socket
import threading
from datetime import datetime
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from env_loader import load_local_env
from bid_evaluator import BidEvaluationAnalyzer
from excel_writer import ExcelWorkbookWriter
from job_store import JobStore
from synopsis_builder import build_synopsis_rows, summarize_synopsis_rows
from tender_extractor import TenderDocumentExtractor, UnsupportedDocumentError

load_local_env()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("APP_DATA_DIR") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or BASE_DIR)
SYNOPSIS_TEMPLATE_PATH = BASE_DIR / "Tender Synopsis Report (2).xlsx"
BID_EVALUATION_TEMPLATE_PATH = BASE_DIR / "Bid-No Bid Stratergy Sheet V-1.0.xlsx"
UPLOAD_DIR = DATA_DIR / "uploads"
GENERATED_DIR = DATA_DIR / "generated"
JOB_DB_PATH = DATA_DIR / "generation_jobs.sqlite3"
STATIC_DIR = BASE_DIR / "static"
FAVICON_PATH = STATIC_DIR / "favicon.ico"
HTML_PAGE = BASE_DIR / "templates" / "index.html"

for directory in (DATA_DIR, UPLOAD_DIR, GENERATED_DIR):
    directory.mkdir(parents=True, exist_ok=True)

extractor = TenderDocumentExtractor()
bid_evaluator = BidEvaluationAnalyzer()
writer = ExcelWorkbookWriter()
logger = logging.getLogger(__name__)
job_store = JobStore(JOB_DB_PATH)
max_concurrent_generations = max(1, int(os.getenv("MAX_CONCURRENT_GENERATIONS", "1")))
generation_slots = threading.Semaphore(max_concurrent_generations)
job_store.mark_incomplete_jobs_failed(
    "The service restarted before generation finished. Please upload the document again."
)

app = FastAPI(title="Tender Workbook Generator")
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(HTML_PAGE.read_text(encoding="utf-8"))


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    if FAVICON_PATH.exists():
        return FileResponse(FAVICON_PATH, media_type="image/x-icon")
    return Response(status_code=204)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _derive_output_token(extraction_bundle: dict[str, object], fallback_name: str) -> str:
    max_token_length = 80
    fallback_token = Path(fallback_name).stem
    rows = extraction_bundle.get("rows", [])
    tender_ref = ""
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and int(row.get("row_number", 0)) == 4:
                tender_ref = str(row.get("value", "")).strip()
                break
    token = tender_ref or fallback_token
    token = re.sub(r"[^A-Za-z0-9_-]+", "_", token).strip("_")
    token = token[:max_token_length].strip("_")
    fallback_token = re.sub(r"[^A-Za-z0-9_-]+", "_", fallback_token).strip("_")[:max_token_length].strip("_")
    return token or fallback_token


def _build_output_names(output_token: str, timestamp: str, generation_token: str) -> tuple[str, str]:
    synopsis_name = f"{output_token}_synopsis_{timestamp}_{generation_token}.xlsx"
    bid_eval_name = f"{output_token}_bid_evaluation_{timestamp}_{generation_token}.xlsx"
    return synopsis_name, bid_eval_name


def _build_generation_payload(
    extraction_bundle: dict[str, object],
    evaluation_bundle: dict[str, object],
    synopsis_name: str,
    bid_eval_name: str,
) -> dict[str, object]:
    rows = extraction_bundle.get("synopsis_rows", extraction_bundle.get("rows", []))
    preview_rows = rows if isinstance(rows, list) else []
    evaluation_rows = evaluation_bundle.get("criteria", [])
    evaluation_preview = evaluation_rows if isinstance(evaluation_rows, list) else []
    return {
        "message": "Tender workbooks generated successfully.",
        "source_name": extraction_bundle.get("source_name"),
        "employer": extraction_bundle.get("employer"),
        "report_date": extraction_bundle.get("report_date"),
        "estimated_accuracy_percentage": extraction_bundle.get("estimated_accuracy_percentage"),
        "average_field_confidence_percentage": extraction_bundle.get("average_field_confidence_percentage"),
        "field_coverage_percentage": extraction_bundle.get("field_coverage_percentage"),
        "confidence_note": extraction_bundle.get("confidence_note"),
        "ocr_enabled": extraction_bundle.get("ocr_enabled"),
        "ocr_used": extraction_bundle.get("ocr_used"),
        "ocr_used_pages": extraction_bundle.get("ocr_used_pages"),
        "ocr_summary": extraction_bundle.get("ocr_summary"),
        "outputs": {
            "synopsis": {
                "download_url": f"/download/{synopsis_name}",
                "file_name": synopsis_name,
            },
            "bid_evaluation": {
                "download_url": f"/download/{bid_eval_name}",
                "file_name": bid_eval_name,
            },
        },
        "synopsis_preview_rows": preview_rows,
        "bid_evaluation_summary": {
            "total_percentage": evaluation_bundle.get("total_percentage"),
            "category": evaluation_bundle.get("category"),
            "decision": evaluation_bundle.get("decision"),
        },
        "bid_evaluation_preview_rows": evaluation_preview,
    }


def _normalize_request_token(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "", raw_value).strip()
    if not normalized:
        return None
    return normalized[:128]


def _generate_outputs(upload_path: Path, safe_name: str, original_name: str) -> tuple[dict[str, object], dict[str, object], str, str]:
    started_at = perf_counter()
    logger.info("Generation started for %s", original_name)

    extraction_started_at = perf_counter()
    extraction_bundle, pages = extractor.extract_with_pages(upload_path)
    synopsis_rows = build_synopsis_rows(extraction_bundle, pages)
    extraction_bundle["synopsis_rows"] = synopsis_rows
    extraction_bundle.update(summarize_synopsis_rows(synopsis_rows))
    logger.info(
        "Extraction finished for %s in %.2f seconds across %s pages",
        original_name,
        perf_counter() - extraction_started_at,
        len(pages),
    )

    analysis_started_at = perf_counter()
    evaluation_bundle = bid_evaluator.analyze(extraction_bundle, pages)
    logger.info("Evaluation finished for %s in %.2f seconds", original_name, perf_counter() - analysis_started_at)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    generation_token = uuid4().hex[:8]
    output_token = _derive_output_token(extraction_bundle, safe_name)
    synopsis_name, bid_eval_name = _build_output_names(output_token, timestamp, generation_token)
    synopsis_path = GENERATED_DIR / synopsis_name
    bid_eval_path = GENERATED_DIR / bid_eval_name

    write_started_at = perf_counter()
    writer.write_outputs(
        extraction_bundle,
        SYNOPSIS_TEMPLATE_PATH,
        synopsis_path,
        evaluation_bundle,
        BID_EVALUATION_TEMPLATE_PATH,
        bid_eval_path,
    )
    logger.info("Workbook writing finished for %s in %.2f seconds", original_name, perf_counter() - write_started_at)
    logger.info("Generation completed for %s in %.2f seconds", original_name, perf_counter() - started_at)
    return extraction_bundle, evaluation_bundle, synopsis_name, bid_eval_name


def _update_job(job_id: str, **updates: object) -> None:
    job_store.update_job(job_id, **updates)


def _build_job_response(job_id: str, snapshot: dict[str, object]) -> JSONResponse:
    status = str(snapshot.get("status", "queued"))
    if status == "completed":
        result = snapshot.get("result", {})
        if isinstance(result, dict):
            return JSONResponse({"job_id": job_id, "status": status, **result})
        return JSONResponse({"job_id": job_id, "status": status, "message": snapshot.get("message", "Completed.")})
    if status == "failed":
        return JSONResponse(
            {
                "job_id": job_id,
                "status": status,
                "message": snapshot.get("message", "Generation failed."),
                "detail": snapshot.get("detail", "Unable to generate synopsis."),
            }
        )
    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "status": status,
            "message": snapshot.get("message", "Processing is in progress."),
        },
    )


def _run_generation_job(job_id: str, upload_path: Path, safe_name: str, original_name: str) -> None:
    _update_job(job_id, status="queued", message="Upload received. Waiting for an available generation slot.")
    with generation_slots:
        _update_job(job_id, status="running", message="Extraction in progress. You can keep this page open.")
        try:
            extraction_bundle, evaluation_bundle, synopsis_name, bid_eval_name = _generate_outputs(
                upload_path,
                safe_name,
                original_name,
            )
            payload = _build_generation_payload(extraction_bundle, evaluation_bundle, synopsis_name, bid_eval_name)
        except UnsupportedDocumentError as exc:
            _update_job(
                job_id,
                status="failed",
                detail=str(exc),
                message="Generation failed.",
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Generation failed for %s", original_name)
            _update_job(
                job_id,
                status="failed",
                detail=f"Unable to generate synopsis: {exc}",
                message="Generation failed.",
            )
            return

        _update_job(
            job_id,
            status="completed",
            message="Tender workbooks generated successfully.",
            result=payload,
        )


@app.post("/generate")
async def generate_synopsis(request: Request, document: UploadFile = File(...)) -> JSONResponse:
    if not SYNOPSIS_TEMPLATE_PATH.exists():
        raise HTTPException(status_code=500, detail="Tender synopsis template is missing from the application folder.")
    if not BID_EVALUATION_TEMPLATE_PATH.exists():
        raise HTTPException(status_code=500, detail="Bid evaluation template is missing from the application folder.")
    if not document.filename:
        raise HTTPException(status_code=400, detail="Please choose a tender document to upload.")

    request_token = _normalize_request_token(request.headers.get("x-upload-id"))
    if request_token:
        existing_job = job_store.get_job_by_request_token(request_token)
        if isinstance(existing_job, dict):
            logger.info("Reusing generation job %s for request token %s", existing_job["job_id"], request_token)
            return _build_job_response(str(existing_job["job_id"]), existing_job)

    original_name = Path(document.filename).name
    extension = Path(original_name).suffix.lower()
    if extension not in extractor.supported_extensions:
        supported = ", ".join(sorted(extractor.supported_extensions))
        raise HTTPException(status_code=400, detail=f"Unsupported file type '{extension}'. Use one of: {supported}.")

    upload_token = uuid4().hex
    safe_name = re.sub(r"[^A-Za-z0-9._ -]+", "_", original_name)
    upload_path = UPLOAD_DIR / f"{upload_token}_{safe_name}"
    with upload_path.open("wb") as output_stream:
        while True:
            chunk = await document.read(1024 * 1024)
            if not chunk:
                break
            output_stream.write(chunk)
    await document.close()
    logger.info("Uploaded %s to %s", original_name, upload_path.name)

    job_id = uuid4().hex
    job_store.create_job(
        job_id=job_id,
        status="queued",
        message="Upload received. Processing has started.",
        source_name=original_name,
        upload_path=str(upload_path),
        request_token=request_token,
    )

    worker = threading.Thread(
        target=_run_generation_job,
        args=(job_id, upload_path, safe_name, original_name),
        daemon=True,
    )
    worker.start()

    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "status": "queued",
            "message": "Upload received. Processing has started.",
        },
    )


@app.get("/jobs/{job_id}")
def generation_status(job_id: str) -> JSONResponse:
    snapshot = job_store.get_job(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Generation job not found or expired.")
    return _build_job_response(job_id, snapshot)


@app.get("/download/{file_name}")
def download_file(file_name: str) -> FileResponse:
    safe_name = Path(file_name).name
    output_path = (GENERATED_DIR / safe_name).resolve()
    if output_path.parent != GENERATED_DIR.resolve() or not output_path.exists():
        raise HTTPException(status_code=404, detail="Generated file not found.")
    media_type = "application/octet-stream"
    if output_path.suffix.lower() == ".xlsx":
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return FileResponse(
        output_path,
        media_type=media_type,
        filename=safe_name,
    )


if __name__ == "__main__":
    import uvicorn

    def _port_is_available(host: str, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                return False
        return True

    def _resolve_local_port(host: str, preferred_port: int) -> int:
        if "PORT" in os.environ:
            return preferred_port
        if _port_is_available(host, preferred_port):
            return preferred_port
        for candidate_port in range(preferred_port + 1, preferred_port + 21):
            if _port_is_available(host, candidate_port):
                logger.warning(
                    "Port %s is already in use. Starting the app on http://%s:%s instead.",
                    preferred_port,
                    host,
                    candidate_port,
                )
                return candidate_port
        raise OSError(f"No free local port found between {preferred_port} and {preferred_port + 20}.")

    preferred_port = int(os.getenv("PORT", "8000"))
    default_host = "0.0.0.0" if "PORT" in os.environ else "127.0.0.1"
    host = os.getenv("APP_HOST", default_host)
    port = _resolve_local_port(host, preferred_port)
    logger.info("Starting Tender Workbook Generator on http://%s:%s", host, port)
    if host == "0.0.0.0":
        logger.info("Open the app in your browser with http://127.0.0.1:%s on the same machine.", port)
    else:
        logger.info("Open the app in your browser with http://%s:%s", host, port)

    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=False,
    )
