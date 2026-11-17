"""In-memory job registry.

Jobs live in memory only (diagnostic service; nothing to persist across
restarts).  All mutable fields are guarded by a per-job lock; the
``threading.Event`` flags are written by the API/cancel threads and read
by the worker threads.

Retention policy (see :class:`JobStore`): a record whose background
processing is not finished yet -- queued, running, or already decided
but with the cancel send / session cleanup still pending -- is NEVER
evicted, so it stays queryable and cancellable.  Only records the
background has fully released are subject to the configured history
capacity.
"""
from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Job:
    def __init__(self, statements: list[str], cleanup_delay_ms: int | None = None,
                 break_connection_after: bool = False):
        self.id = uuid.uuid4().hex[:12]
        self.lock = threading.Lock()
        self.status = "queued"  # queued -> running -> succeeded|failed|cancelled
        self.created_at = utcnow()
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.connection_id: int | None = None
        self.cleanup_delay_ms = cleanup_delay_ms  # test hook; None = server default
        self.break_connection_after = break_connection_after  # test hook
        self.error: str | None = None
        # cancel plumbing
        self.cancel_requested = threading.Event()
        self.cancel_send_done = threading.Event()
        self.cancel_accepted_at: str | None = None
        # Set once the background is completely done with this record:
        # SQL stopped AND any accepted cancel send finished AND session
        # cleanup completed AND the connection returned -- or the job
        # never ran at all (cancelled while queued).  Only released
        # records are eligible for history eviction.
        self.released = threading.Event()
        self.records = [
            {
                "index": i,
                "sql": sql,
                "status": "pending",  # pending|running|ok|error|cancelled|skipped
                "started_at": None,
                "duration_ms": None,
                "columns": None,
                "rows": None,
                "rowcount": None,
                "truncated": False,
                "error": None,
            }
            for i, sql in enumerate(statements)
        ]

    # ---- mutations (worker / API threads) ---------------------------------

    def mark_running(self, connection_id: int) -> bool:
        with self.lock:
            if self.status in TERMINAL_STATES:
                return False
            self.status = "running"
            self.started_at = utcnow()
            self.connection_id = connection_id
            return True

    def stmt_running(self, idx: int) -> None:
        with self.lock:
            rec = self.records[idx]
            rec["status"] = "running"
            rec["started_at"] = utcnow()

    def stmt_ok(self, idx, columns, rows, rowcount, truncated, duration_ms) -> None:
        with self.lock:
            self.records[idx].update(
                status="ok",
                columns=columns,
                rows=rows,
                rowcount=rowcount,
                truncated=truncated,
                duration_ms=duration_ms,
            )

    def stmt_error(self, idx, error, duration_ms) -> None:
        with self.lock:
            self.records[idx].update(
                status="error", error=error, duration_ms=duration_ms
            )

    def stmt_cancelled(self, idx, error, duration_ms) -> None:
        with self.lock:
            self.records[idx].update(
                status="cancelled", error=error, duration_ms=duration_ms
            )

    def skip_from(self, idx: int) -> None:
        with self.lock:
            for rec in self.records[idx:]:
                if rec["status"] == "pending":
                    rec["status"] = "skipped"

    def finish(self, status: str, error: str | None = None) -> bool:
        """Transition to a terminal state.  First terminal transition wins."""
        with self.lock:
            if self.status in TERMINAL_STATES:
                return False
            self.status = status
            self.finished_at = utcnow()
            self.error = error
            return True

    def finish_if_queued(self, status: str, error: str | None = None) -> bool:
        """Atomically move a *queued* job to a terminal state.

        Returns False if the job has already left the queued state
        (running or terminal); the caller must then take the
        running/terminal path.  Because this is atomic with
        :meth:`mark_running` (both take the job lock), a True result
        guarantees no worker will ever start the job.
        """
        with self.lock:
            if self.status != "queued":
                return False
            self.status = status
            self.finished_at = utcnow()
            self.error = error
            return True

    # ---- reads -------------------------------------------------------------

    def is_terminal(self) -> bool:
        with self.lock:
            return self.status in TERMINAL_STATES

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "status": self.status,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "connection_id": self.connection_id,
                "cleanup_delay_ms": self.cleanup_delay_ms,
                "error": self.error,
                "cancel_requested": self.cancel_requested.is_set(),
                "cancel_accepted_at": self.cancel_accepted_at,
                "statements": [dict(rec) for rec in self.records],
            }

    def summary(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "status": self.status,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "connection_id": self.connection_id,
                "num_statements": len(self.records),
            }


class JobStore:
    """Thread-safe registry of job records.

    Retention policy
    ----------------
    * Active records -- queued, running, or decided but not yet released
      (cancel send / session cleanup still pending) -- are NEVER evicted:
      they stay queryable and cancellable no matter how many newer jobs
      arrive, and they do not count against the history capacity.
    * Released records (the background is completely done with them) are
      evicted oldest-first once more than ``capacity`` of them are
      retained: the configured history capacity is still enforced for
      completed jobs.
    """

    def __init__(self, capacity: int):
        self._lock = threading.Lock()
        self._jobs: OrderedDict[str, Job] = OrderedDict()
        self._capacity = capacity

    def add(self, job: Job) -> None:
        with self._lock:
            self._jobs[job.id] = job
            self._evict_locked()

    def mark_released(self, job: Job) -> None:
        """Mark ``job`` as no longer used by the background and evict
        surplus finished records.  Idempotent."""
        job.released.set()
        with self._lock:
            self._evict_locked()

    def _evict_locked(self) -> None:
        finished = [
            job_id for job_id, job in self._jobs.items() if job.released.is_set()
        ]
        surplus = len(finished) - self._capacity
        if surplus <= 0:
            return
        # OrderedDict iterates in insertion order: oldest finished first.
        for job_id in finished[:surplus]:
            del self._jobs[job_id]

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_summaries(self, limit: int = 200) -> list[dict]:
        with self._lock:
            jobs = list(self._jobs.values())
        return [job.summary() for job in reversed(jobs[-limit:])]
