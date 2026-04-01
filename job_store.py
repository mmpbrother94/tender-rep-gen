from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class JobStore:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
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
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at)"
            )
            connection.commit()

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
        timestamp = self._timestamp()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO jobs (
                    job_id, request_token, status, message, detail, source_name, upload_path, result_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, request_token, status, message, "", source_name, upload_path, "", timestamp, timestamp),
            )
            connection.commit()

    def update_job(self, job_id: str, **updates: object) -> None:
        if not updates:
            return
        columns: list[str] = []
        values: list[object] = []
        for key, value in updates.items():
            column_name = "result_json" if key == "result" else key
            columns.append(f"{column_name} = ?")
            if key == "result":
                values.append(json.dumps(value))
            else:
                values.append(value)
        columns.append("updated_at = ?")
        values.append(self._timestamp())
        values.append(job_id)

        with self._connect() as connection:
            connection.execute(
                f"UPDATE jobs SET {', '.join(columns)} WHERE job_id = ?",
                values,
            )
            connection.commit()

    def get_job(self, job_id: str) -> dict[str, object] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def get_job_by_request_token(self, request_token: str) -> dict[str, object] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE request_token = ?",
                (request_token,),
            ).fetchone()
        return self._row_to_dict(row)

    def list_jobs_by_status(self, statuses: tuple[str, ...] | list[str]) -> list[dict[str, object]]:
        normalized_statuses = tuple(dict.fromkeys(str(status).strip() for status in statuses if str(status).strip()))
        if not normalized_statuses:
            return []
        placeholders = ", ".join("?" for _ in normalized_statuses)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders}) ORDER BY created_at ASC",
                normalized_statuses,
            ).fetchall()
        return [snapshot for row in rows if (snapshot := self._row_to_dict(row)) is not None]

    def requeue_incomplete_jobs(self, message: str) -> list[dict[str, object]]:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs
                SET status = 'queued',
                    message = ?,
                    detail = '',
                    updated_at = ?
                WHERE status IN ('queued', 'running')
                """,
                (message, self._timestamp()),
            )
            connection.commit()
        return self.list_jobs_by_status(["queued"])

    def mark_incomplete_jobs_failed(self, detail: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    message = 'Generation failed.',
                    detail = ?,
                    updated_at = ?
                WHERE status IN ('queued', 'running')
                """,
                (detail, self._timestamp()),
            )
            connection.commit()

    def _row_to_dict(self, row: sqlite3.Row | None) -> dict[str, object] | None:
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

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()
