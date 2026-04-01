from __future__ import annotations

import gc
import logging
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from env_loader import load_local_env
from job_store import JobStore
from tender_extractor import UnsupportedDocumentError

import app as app_runtime

load_local_env()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

WORKER_POLL_TIMEOUT_SECONDS = max(1, int(os.getenv("WORKER_POLL_TIMEOUT_SECONDS", "5")))
WORKER_REQUEUE_ON_STARTUP = os.getenv("WORKER_REQUEUE_ON_STARTUP", "true").strip().lower() in {"1", "true", "yes", "on"}
INTERNAL_SERVICE_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "").strip()
WEB_INTERNAL_BASE_URL = os.getenv("WEB_INTERNAL_BASE_URL", "").strip()
WEB_INTERNAL_HOSTPORT = os.getenv("WEB_INTERNAL_HOSTPORT", "").strip()
if not WEB_INTERNAL_BASE_URL and WEB_INTERNAL_HOSTPORT:
    WEB_INTERNAL_BASE_URL = f"http://{WEB_INTERNAL_HOSTPORT}"

job_store = JobStore(app_runtime.JOB_DB_PATH)


def _internal_headers(content_type: str | None = None) -> dict[str, str]:
    headers = {"X-Internal-Token": INTERNAL_SERVICE_TOKEN}
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def _request_bytes(url: str, *, method: str = "GET", data: bytes | None = None, content_type: str | None = None, timeout: int = 600) -> tuple[bytes, dict[str, str]]:
    request = urllib.request.Request(
        url,
        data=data,
        headers=_internal_headers(content_type),
        method=method,
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(), dict(response.headers.items())


def _download_source_file(job_id: str, original_name: str) -> Path:
    source_url = f"{WEB_INTERNAL_BASE_URL}/internal/jobs/{job_id}/source"
    request = urllib.request.Request(source_url, headers=_internal_headers(), method="GET")
    with urllib.request.urlopen(request, timeout=600) as response:
        encoded_name = response.headers.get("X-Source-Name", "")
        resolved_name = urllib.parse.unquote(encoded_name).strip() or original_name or f"{job_id}.bin"
        safe_name = Path(resolved_name).name or f"{job_id}.bin"
        temp_path = Path(tempfile.mkdtemp(prefix="tender-worker-")) / safe_name
        with temp_path.open("wb") as output_stream:
            shutil.copyfileobj(response, output_stream, length=1024 * 1024)
    return temp_path


def _upload_generated_file(job_id: str, artifact_name: str, file_path: Path) -> None:
    query = urllib.parse.urlencode({"file_name": file_path.name})
    target_url = f"{WEB_INTERNAL_BASE_URL}/internal/jobs/{job_id}/outputs/{artifact_name}?{query}"
    _request_bytes(
        target_url,
        method="PUT",
        data=file_path.read_bytes(),
        content_type="application/octet-stream",
        timeout=600,
    )


def _delete_remote_source(job_id: str) -> None:
    target_url = f"{WEB_INTERNAL_BASE_URL}/internal/jobs/{job_id}/source"
    try:
        _request_bytes(target_url, method="DELETE", timeout=120)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to delete source upload for job %s: %s", job_id, exc)


def _cleanup_local_file(file_path: Path) -> None:
    try:
        file_path.unlink(missing_ok=True)
    except OSError:
        logger.warning("Unable to remove local file %s", file_path)
    try:
        parent = file_path.parent
        if parent.name.startswith("tender-worker-"):
            parent.rmdir()
    except OSError:
        pass


def _process_job(job_id: str) -> None:
    snapshot = job_store.get_job(job_id)
    if snapshot is None:
        logger.warning("Skipping missing job %s", job_id)
        return
    if str(snapshot.get("status", "")).strip() == "completed":
        logger.info("Skipping already completed job %s", job_id)
        return

    original_name = Path(str(snapshot.get("source_name", "")).strip()).name or "document"
    safe_name = "".join(character if character.isalnum() or character in "._ -" else "_" for character in original_name).strip() or "document"
    upload_path: Path | None = None
    synopsis_path: Path | None = None
    bid_eval_path: Path | None = None

    try:
        job_store.update_job(
            job_id,
            status="running",
            message="Extraction in progress. You can keep this page open.",
            detail="",
        )
        upload_path = _download_source_file(job_id, original_name)
        extraction_bundle, evaluation_bundle, synopsis_name, bid_eval_name = app_runtime._generate_outputs(
            upload_path,
            safe_name,
            original_name,
        )
        synopsis_path = app_runtime.GENERATED_DIR / synopsis_name
        bid_eval_path = app_runtime.GENERATED_DIR / bid_eval_name
        _upload_generated_file(job_id, "synopsis", synopsis_path)
        _upload_generated_file(job_id, "bid_evaluation", bid_eval_path)
        payload = app_runtime._build_generation_payload(extraction_bundle, evaluation_bundle, synopsis_name, bid_eval_name)
        job_store.update_job(
            job_id,
            status="completed",
            message="Tender workbooks generated successfully.",
            result=payload,
        )
        _delete_remote_source(job_id)
        logger.info("Completed queued job %s", job_id)
    except UnsupportedDocumentError as exc:
        job_store.update_job(job_id, status="failed", message="Generation failed.", detail=str(exc))
        logger.warning("Unsupported document for job %s: %s", job_id, exc)
    except urllib.error.HTTPError as exc:
        detail = f"Unable to exchange files with the web service: {exc.code} {exc.reason}"
        job_store.update_job(job_id, status="failed", message="Generation failed.", detail=detail)
        logger.exception("Internal HTTP failure for job %s", job_id)
    except Exception as exc:  # noqa: BLE001
        job_store.update_job(job_id, status="failed", message="Generation failed.", detail=f"Unable to generate synopsis: {exc}")
        logger.exception("Worker failed while processing job %s", job_id)
    finally:
        if synopsis_path is not None:
            _cleanup_local_file(synopsis_path)
        if bid_eval_path is not None:
            _cleanup_local_file(bid_eval_path)
        if upload_path is not None:
            _cleanup_local_file(upload_path)
        gc.collect()


def _prime_queue() -> None:
    if not WORKER_REQUEUE_ON_STARTUP:
        return
    resume_message = "The worker restarted during generation. Waiting to resume automatically."
    for snapshot in job_store.requeue_incomplete_jobs(resume_message):
        job_store.enqueue_job(str(snapshot.get("job_id", "")).strip())


def main() -> int:
    if not job_store.queue_enabled:
        logger.error("REDIS_URL is required for the Render worker.")
        return 1
    if not INTERNAL_SERVICE_TOKEN:
        logger.error("INTERNAL_SERVICE_TOKEN is required for the Render worker.")
        return 1
    if not WEB_INTERNAL_BASE_URL:
        logger.error("WEB_INTERNAL_BASE_URL or WEB_INTERNAL_HOSTPORT is required for the Render worker.")
        return 1

    _prime_queue()
    logger.info("Render worker listening for jobs via Redis queue.")

    while True:
        job_id = job_store.dequeue_job(timeout_seconds=WORKER_POLL_TIMEOUT_SECONDS)
        if not job_id:
            continue
        _process_job(job_id)
        time.sleep(0.1)


if __name__ == "__main__":
    raise SystemExit(main())
