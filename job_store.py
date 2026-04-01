from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

try:
    from redis import Redis
except ImportError:  # pragma: no cover - optional until the dependency is installed
    Redis = None  # type: ignore[assignment]


class JobStore:
    _JOB_KEY_PREFIX = "tender:job:"
    _REQUEST_KEY_PREFIX = "tender:req:"
    _QUEUE_KEY = "tender:queue"
    _QUEUED_SET_KEY = "tender:queued"

    def __init__(self, db_path: str | Path) -> None:
        self._redis_url = os.getenv("REDIS_URL", "").strip()
        self._redis = self._build_redis_client(self._redis_url)
        self._db_path = Path(db_path)
        if self._redis is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize_sqlite()

    @property
    def queue_enabled(self) -> bool:
        return self._redis is not None

    def create_job(
        self,
        *,
        job_id: str,
        status: str,
        message: str,
        source_name: str,
        upload_path: str,
        request_token: str | None = None,
    ) -> None:
        payload = {
            "job_id": job_id,
            "request_token": request_token,
            "status": status,
            "message": message,
            "detail": "",
            "source_name": source_name,
            "upload_path": upload_path,
            "result": {},
            "created_at": self._timestamp(),
            "updated_at": self._timestamp(),
        }
        self._save_job(payload)

    def update_job(self, job_id: str, **updates: object) -> None:
        if not updates:
            return
        payload = self.get_job(job_id)
        if payload is None:
            return
        for key, value in updates.items():
            payload[key] = value
        payload["updated_at"] = self._timestamp()
        self._save_job(payload)

    def get_job(self, job_id: str) -> dict[str, object] | None:
        if self._redis is not None:
            raw_value = self._redis.get(self._job_key(job_id))
            if not raw_value:
                return None
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                return None
            return payload if isinstance(payload, dict) else None

        with self._connect_sqlite() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return self._sqlite_row_to_dict(row)

    def get_job_by_request_token(self, request_token: str) -> dict[str, object] | None:
        if self._redis is not None:
            job_id = self._redis.get(self._request_key(request_token))
            if not job_id:
                return None
            return self.get_job(job_id)

        with self._connect_sqlite() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE request_token = ?",
                (request_token,),
            ).fetchone()
        return self._sqlite_row_to_dict(row)

    def list_jobs_by_status(self, statuses: tuple[str, ...] | list[str]) -> list[dict[str, object]]:
        normalized_statuses = {str(status).strip() for status in statuses if str(status).strip()}
        if not normalized_statuses:
            return []
        if self._redis is not None:
            matches: list[dict[str, object]] = []
            for key in self._redis.scan_iter(match=f"{self._JOB_KEY_PREFIX}*"):
                raw_value = self._redis.get(key)
                if not raw_value:
                    continue
                try:
                    payload = json.loads(raw_value)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict) and str(payload.get("status", "")).strip() in normalized_statuses:
                    matches.append(payload)
            matches.sort(key=lambda item: str(item.get("created_at", "")))
            return matches

        placeholders = ", ".join("?" for _ in normalized_statuses)
        with self._connect_sqlite() as connection:
            rows = connection.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders}) ORDER BY created_at ASC",
                tuple(sorted(normalized_statuses)),
            ).fetchall()
        return [snapshot for row in rows if (snapshot := self._sqlite_row_to_dict(row)) is not None]

    def requeue_incomplete_jobs(self, message: str) -> list[dict[str, object]]:
        snapshots = self.list_jobs_by_status(["queued", "running"])
        for snapshot in snapshots:
            snapshot["status"] = "queued"
            snapshot["message"] = message
            snapshot["detail"] = ""
            snapshot["updated_at"] = self._timestamp()
            self._save_job(snapshot)
        return snapshots

    def mark_incomplete_jobs_failed(self, detail: str) -> None:
        snapshots = self.list_jobs_by_status(["queued", "running"])
        for snapshot in snapshots:
            snapshot["status"] = "failed"
            snapshot["message"] = "Generation failed."
            snapshot["detail"] = detail
            snapshot["updated_at"] = self._timestamp()
            self._save_job(snapshot)

    def enqueue_job(self, job_id: str) -> bool:
        if self._redis is None:
            return False
        if self._redis.sadd(self._QUEUED_SET_KEY, job_id):
            self._redis.rpush(self._QUEUE_KEY, job_id)
            return True
        return False

    def dequeue_job(self, timeout_seconds: int = 5) -> str | None:
        if self._redis is None:
            return None
        item = self._redis.blpop(self._QUEUE_KEY, timeout=max(1, int(timeout_seconds)))
        if not item:
            return None
        _, job_id = item
        self._redis.srem(self._QUEUED_SET_KEY, job_id)
        return str(job_id)

    def _save_job(self, payload: dict[str, object]) -> None:
        if self._redis is not None:
            job_id = str(payload.get("job_id", "")).strip()
            if not job_id:
                return
            if payload.get("request_token"):
                self._redis.set(self._request_key(str(payload["request_token"])), job_id)
            self._redis.set(self._job_key(job_id), json.dumps(payload))
            return

        timestamp = self._timestamp()
        result = payload.get("result", {})
        with self._connect_sqlite() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO jobs (
                    job_id, request_token, status, message, detail, source_name, upload_path, result_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("job_id"),
                    payload.get("request_token"),
                    payload.get("status"),
                    payload.get("message"),
                    payload.get("detail"),
                    payload.get("source_name"),
                    payload.get("upload_path"),
                    json.dumps(result if isinstance(result, dict) else {}),
                    payload.get("created_at") or timestamp,
                    payload.get("updated_at") or timestamp,
                ),
            )
            connection.commit()

    def _initialize_sqlite(self) -> None:
        with self._connect_sqlite() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    request_token TEXT UNIQUE,
                    status TEXT NOT NULL,
                    message TEXT,
                    detail TEXT,
                    source_name TEXT,
                    upload_path TEXT,
                    result_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at)")
            connection.commit()

    def _connect_sqlite(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _build_redis_client(redis_url: str) -> object | None:
        if not redis_url:
            return None
        if Redis is None:
            raise RuntimeError("REDIS_URL is configured but the redis package is not installed.")
        return Redis.from_url(redis_url, decode_responses=True, socket_timeout=30, socket_connect_timeout=30)

    def _sqlite_row_to_dict(self, row: sqlite3.Row | None) -> dict[str, object] | None:
        if row is None:
            return None
        result_json = str(row["result_json"] or "").strip()
        try:
            result = json.loads(result_json) if result_json else {}
        except json.JSONDecodeError:
            result = {}
        return {
            "job_id": row["job_id"],
            "request_token": row["request_token"],
            "status": row["status"],
            "message": row["message"],
            "detail": row["detail"],
            "source_name": row["source_name"],
            "upload_path": row["upload_path"],
            "result": result if isinstance(result, dict) else {},
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _job_key(self, job_id: str) -> str:
        return f"{self._JOB_KEY_PREFIX}{job_id}"

    def _request_key(self, request_token: str) -> str:
        return f"{self._REQUEST_KEY_PREFIX}{request_token}"

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()
