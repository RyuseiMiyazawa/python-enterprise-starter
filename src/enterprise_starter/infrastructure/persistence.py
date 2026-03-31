from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from enterprise_starter.domain.events import WorkflowEvent
from enterprise_starter.domain.models import PersistedRun, TaskRecord, WorkflowSummary


class WorkflowRunStoreProtocol(Protocol):
    def save_run(
        self,
        summary: WorkflowSummary,
        outputs: dict[str, Any],
        records: dict[str, TaskRecord],
        events: list[WorkflowEvent],
    ) -> None: ...


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteWorkflowRunStore:
    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._database_path)

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at_utc TEXT NOT NULL,
                    finished_at_utc TEXT NOT NULL,
                    total_wall_time_seconds REAL NOT NULL,
                    critical_path_seconds REAL NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS queued_runs (
                    job_id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 100,
                    scheduled_at_utc TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    next_attempt_at_utc TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    lease_owner TEXT,
                    lease_expires_at_utc TEXT,
                    heartbeat_at_utc TEXT,
                    run_id TEXT,
                    last_error TEXT,
                    dead_letter_reason TEXT
                )
                """
            )
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(queued_runs)").fetchall()
            }
            migrations = [
                ("priority", "ALTER TABLE queued_runs ADD COLUMN priority INTEGER NOT NULL DEFAULT 100"),
                ("scheduled_at_utc", "ALTER TABLE queued_runs ADD COLUMN scheduled_at_utc TEXT NOT NULL DEFAULT ''"),
                ("attempt_count", "ALTER TABLE queued_runs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0"),
                ("max_attempts", "ALTER TABLE queued_runs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3"),
                ("next_attempt_at_utc", "ALTER TABLE queued_runs ADD COLUMN next_attempt_at_utc TEXT NOT NULL DEFAULT ''"),
                ("last_error", "ALTER TABLE queued_runs ADD COLUMN last_error TEXT"),
                ("dead_letter_reason", "ALTER TABLE queued_runs ADD COLUMN dead_letter_reason TEXT"),
            ]
            for column_name, statement in migrations:
                if column_name not in columns:
                    try:
                        connection.execute(statement)
                    except sqlite3.OperationalError as exc:
                        if "duplicate column name" not in str(exc):
                            raise
            connection.execute(
                """
                UPDATE queued_runs
                SET next_attempt_at_utc = COALESCE(NULLIF(next_attempt_at_utc, ''), created_at_utc)
                """
            )
            connection.execute(
                """
                UPDATE queued_runs
                SET scheduled_at_utc = COALESCE(NULLIF(scheduled_at_utc, ''), created_at_utc)
                """
            )
            connection.commit()

    def save_run(
        self,
        summary: WorkflowSummary,
        outputs: dict[str, Any],
        records: dict[str, TaskRecord],
        events: list[WorkflowEvent],
    ) -> None:
        status = "failed" if summary.failed else "succeeded"
        finished_at = datetime.now(timezone.utc)
        started_at = finished_at - timedelta(seconds=summary.total_wall_time_seconds)
        payload_json = json.dumps(
            {
                "summary": {
                    "run_id": summary.run_id,
                    "workflow_name": summary.workflow_name,
                    "completed": summary.completed,
                    "failed": summary.failed,
                    "cached": summary.cached,
                    "skipped": summary.skipped,
                    "task_durations": summary.task_durations,
                },
                "outputs": outputs,
                "records": {
                    name: {
                        "state": record.state,
                        "attempts": record.attempts,
                        "started_at": record.started_at,
                        "finished_at": record.finished_at,
                        "error": record.error,
                    }
                    for name, record in records.items()
                },
                "events": [
                    {
                        "kind": event.kind,
                        "task_name": event.task_name,
                        "timestamp": event.timestamp.isoformat(),
                        "payload": event.payload,
                    }
                    for event in events
                ],
            },
            sort_keys=True,
        )
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO workflow_runs (
                    run_id,
                    workflow_name,
                    status,
                    started_at_utc,
                    finished_at_utc,
                    total_wall_time_seconds,
                    critical_path_seconds,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary.run_id,
                    summary.workflow_name,
                    status,
                    started_at.isoformat(),
                    finished_at.isoformat(),
                    summary.total_wall_time_seconds,
                    summary.critical_path_seconds,
                    payload_json,
                ),
            )
            connection.commit()

    def list_runs(self, limit: int = 20) -> list[PersistedRun]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT
                    run_id,
                    workflow_name,
                    status,
                    started_at_utc,
                    finished_at_utc,
                    total_wall_time_seconds,
                    critical_path_seconds,
                    payload_json
                FROM workflow_runs
                ORDER BY rowid DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            PersistedRun(
                run_id=row[0],
                workflow_name=row[1],
                status=row[2],
                started_at_utc=row[3],
                finished_at_utc=row[4],
                total_wall_time_seconds=row[5],
                critical_path_seconds=row[6],
                payload_json=row[7],
            )
            for row in rows
        ]

    def get_run(self, run_id: str) -> PersistedRun | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT
                    run_id,
                    workflow_name,
                    status,
                    started_at_utc,
                    finished_at_utc,
                    total_wall_time_seconds,
                    critical_path_seconds,
                    payload_json
                FROM workflow_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return PersistedRun(
            run_id=row[0],
            workflow_name=row[1],
            status=row[2],
            started_at_utc=row[3],
            finished_at_utc=row[4],
            total_wall_time_seconds=row[5],
            critical_path_seconds=row[6],
            payload_json=row[7],
        )

    def enqueue_run(
        self,
        workflow_name: str,
        payload: dict[str, Any],
        max_attempts: int = 3,
        priority: int = 100,
        scheduled_at_utc: str | None = None,
    ) -> dict[str, Any]:
        job_id = uuid4().hex
        now = utc_now_iso()
        scheduled_at = scheduled_at_utc or now
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO queued_runs (
                    job_id,
                    workflow_name,
                    payload_json,
                    status,
                    priority,
                    scheduled_at_utc,
                    attempt_count,
                    max_attempts,
                    next_attempt_at_utc,
                    created_at_utc,
                    updated_at_utc,
                    lease_owner,
                    lease_expires_at_utc,
                    heartbeat_at_utc,
                    run_id,
                    last_error,
                    dead_letter_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    workflow_name,
                    json.dumps(payload, sort_keys=True),
                    "queued",
                    priority,
                    scheduled_at,
                    0,
                    max_attempts,
                    scheduled_at,
                    now,
                    now,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            )
            connection.commit()
        return self.get_queue_job(job_id)

    def claim_next_run(self, worker_id: str, lease_seconds: int = 30) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_expires = (now + timedelta(seconds=lease_seconds)).isoformat()
        aging_window_minutes = 15
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT job_id
                FROM queued_runs
                WHERE (
                    (
                        (status = 'queued' OR status = 'retryable')
                        AND next_attempt_at_utc <= ?
                        AND scheduled_at_utc <= ?
                    )
                    OR (status = 'leased' AND (lease_expires_at_utc IS NULL OR lease_expires_at_utc < ?))
                )
                ORDER BY
                    CASE
                        WHEN priority - CAST((((julianday(?) - julianday(created_at_utc)) * 1440.0) / ?) AS INTEGER) < 0
                            THEN 0
                        ELSE priority - CAST((((julianday(?) - julianday(created_at_utc)) * 1440.0) / ?) AS INTEGER)
                    END ASC,
                    scheduled_at_utc ASC,
                    created_at_utc ASC
                LIMIT 1
                """,
                (now_iso, now_iso, now_iso, now_iso, aging_window_minutes, now_iso, aging_window_minutes),
            ).fetchone()
            if row is None:
                return None
            job_id = row[0]
            connection.execute(
                """
                UPDATE queued_runs
                SET status = 'leased',
                    attempt_count = attempt_count + 1,
                    updated_at_utc = ?,
                    lease_owner = ?,
                    lease_expires_at_utc = ?,
                    heartbeat_at_utc = ?
                WHERE job_id = ?
                """,
                (now_iso, worker_id, lease_expires, now_iso, job_id),
            )
            connection.commit()
        return self.get_queue_job(job_id)

    def heartbeat_run(self, job_id: str, worker_id: str, lease_seconds: int = 30) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_expires = (now + timedelta(seconds=lease_seconds)).isoformat()
        with closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE queued_runs
                SET updated_at_utc = ?,
                    heartbeat_at_utc = ?,
                    lease_expires_at_utc = ?
                WHERE job_id = ?
                  AND lease_owner = ?
                  AND status = 'leased'
                """,
                (now_iso, now_iso, lease_expires, job_id, worker_id),
            )
            connection.commit()
        if cursor.rowcount == 0:
            return None
        return self.get_queue_job(job_id)

    def cancel_job(self, job_id: str, reason: str = "cancelled_by_user") -> dict[str, Any] | None:
        now_iso = utc_now_iso()
        with closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE queued_runs
                SET status = 'cancelled',
                    updated_at_utc = ?,
                    lease_expires_at_utc = NULL,
                    last_error = ?,
                    dead_letter_reason = CASE
                        WHEN dead_letter_reason IS NULL THEN ?
                        ELSE dead_letter_reason
                    END
                WHERE job_id = ?
                  AND status IN ('queued', 'retryable', 'leased')
                """,
                (now_iso, reason, reason, job_id),
            )
            connection.commit()
        if cursor.rowcount == 0:
            return None
        return self.get_queue_job(job_id)

    def complete_run_job(
        self,
        job_id: str,
        worker_id: str,
        status: str,
        run_id: str | None = None,
        last_error: str | None = None,
    ) -> dict[str, Any] | None:
        now_iso = utc_now_iso()
        with closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE queued_runs
                SET status = ?,
                    updated_at_utc = ?,
                    run_id = ?,
                    lease_expires_at_utc = NULL,
                    last_error = ?
                WHERE job_id = ?
                  AND lease_owner = ?
                """,
                (status, now_iso, run_id, last_error, job_id, worker_id),
            )
            connection.commit()
        if cursor.rowcount == 0:
            return None
        return self.get_queue_job(job_id)

    def retry_or_dead_letter_job(
        self,
        job_id: str,
        worker_id: str,
        error_message: str,
        backoff_seconds: int = 5,
    ) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT attempt_count, max_attempts
                FROM queued_runs
                WHERE job_id = ? AND lease_owner = ?
                """,
                (job_id, worker_id),
            ).fetchone()
            if row is None:
                return None
            attempt_count, max_attempts = row
            next_attempt = (now + timedelta(seconds=backoff_seconds)).isoformat()
            if attempt_count >= max_attempts:
                cursor = connection.execute(
                    """
                    UPDATE queued_runs
                    SET status = 'dead_letter',
                        updated_at_utc = ?,
                        lease_expires_at_utc = NULL,
                        last_error = ?,
                        dead_letter_reason = ?
                    WHERE job_id = ? AND lease_owner = ?
                    """,
                    (now_iso, error_message, "max_attempts_exceeded", job_id, worker_id),
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE queued_runs
                    SET status = 'retryable',
                        updated_at_utc = ?,
                        lease_expires_at_utc = NULL,
                        last_error = ?,
                        next_attempt_at_utc = ?
                    WHERE job_id = ? AND lease_owner = ?
                    """,
                    (now_iso, error_message, next_attempt, job_id, worker_id),
                )
            connection.commit()
        if cursor.rowcount == 0:
            return None
        return self.get_queue_job(job_id)

    def list_queue_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT
                    job_id,
                    workflow_name,
                    payload_json,
                    status,
                    priority,
                    scheduled_at_utc,
                    attempt_count,
                    max_attempts,
                    next_attempt_at_utc,
                    created_at_utc,
                    updated_at_utc,
                    lease_owner,
                    lease_expires_at_utc,
                    heartbeat_at_utc,
                    run_id,
                    last_error,
                    dead_letter_reason
                FROM queued_runs
                ORDER BY created_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._queue_row_to_dict(row) for row in rows]

    def get_queue_job(self, job_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT
                    job_id,
                    workflow_name,
                    payload_json,
                    status,
                    priority,
                    scheduled_at_utc,
                    attempt_count,
                    max_attempts,
                    next_attempt_at_utc,
                    created_at_utc,
                    updated_at_utc,
                    lease_owner,
                    lease_expires_at_utc,
                    heartbeat_at_utc,
                    run_id,
                    last_error,
                    dead_letter_reason
                FROM queued_runs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return self._queue_row_to_dict(row)

    def _queue_row_to_dict(self, row: tuple[Any, ...]) -> dict[str, Any]:
        effective_priority = max(
            0,
            int(row[4]) - int((datetime.now(timezone.utc) - datetime.fromisoformat(row[9])).total_seconds() // 900),
        )
        return {
            "job_id": row[0],
            "workflow_name": row[1],
            "payload": json.loads(row[2]),
            "status": row[3],
            "priority": row[4],
            "effective_priority": effective_priority,
            "scheduled_at_utc": row[5],
            "attempt_count": row[6],
            "max_attempts": row[7],
            "next_attempt_at_utc": row[8],
            "created_at_utc": row[9],
            "updated_at_utc": row[10],
            "lease_owner": row[11],
            "lease_expires_at_utc": row[12],
            "heartbeat_at_utc": row[13],
            "run_id": row[14],
            "last_error": row[15],
            "dead_letter_reason": row[16],
        }
