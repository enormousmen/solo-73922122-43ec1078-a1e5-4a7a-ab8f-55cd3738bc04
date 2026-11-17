"""Unit tests for the job registry retention policy (no database needed).

The policy under test:
* active records (queued / running / finished-but-not-released) are never
  evicted, no matter how far the history capacity is exceeded;
* released records (background fully done) are evicted oldest-first once
  more than ``capacity`` of them are retained.
"""
import threading

from app.store import Job, JobStore


def make_job() -> Job:
    return Job(["SELECT 1"])


class TestRetention:
    def test_active_jobs_survive_capacity_pressure(self):
        # The reported bug: JOB_HISTORY=2, three submitted jobs -> the
        # first (still queued) vanished (404 on get/cancel).
        store = JobStore(capacity=2)
        jobs = [make_job() for _ in range(3)]
        for job in jobs:
            store.add(job)
        for job in jobs:
            assert store.get(job.id) is job, "unfinished job was evicted"
        assert len(store.list_summaries()) == 3

    def test_finished_records_evicted_oldest_first(self):
        store = JobStore(capacity=2)
        jobs = [make_job() for _ in range(4)]
        for job in jobs:
            store.add(job)
        store.mark_released(jobs[0])
        store.mark_released(jobs[1])
        # two finished records == capacity: nothing evicted yet
        assert store.get(jobs[0].id) is jobs[0]
        assert store.get(jobs[1].id) is jobs[1]
        store.mark_released(jobs[2])
        # third finished record overflows the capacity -> oldest goes
        assert store.get(jobs[0].id) is None
        assert store.get(jobs[1].id) is jobs[1]
        assert store.get(jobs[2].id) is jobs[2]
        assert store.get(jobs[3].id) is jobs[3]  # still active: untouchable
        store.mark_released(jobs[3])
        assert store.get(jobs[1].id) is None
        assert store.get(jobs[2].id) is jobs[2]
        assert store.get(jobs[3].id) is jobs[3]

    def test_active_jobs_do_not_count_against_capacity(self):
        store = JobStore(capacity=1)
        active = [make_job() for _ in range(5)]
        done = [make_job() for _ in range(3)]
        for job in active + done:
            store.add(job)
        for job in done:
            store.mark_released(job)
        # only the newest finished record is kept, all active ones remain
        assert store.get(done[0].id) is None
        assert store.get(done[1].id) is None
        assert store.get(done[2].id) is done[2]
        for job in active:
            assert store.get(job.id) is job

    def test_release_is_idempotent(self):
        store = JobStore(capacity=1)
        job = make_job()
        store.add(job)
        store.mark_released(job)
        store.mark_released(job)
        assert store.get(job.id) is job

    def test_release_before_add(self):
        # A worker may finish a fast job before submit_job() registers it
        # (pool.submit happens before store.add).  The record must still
        # be stored and count as finished from the start.
        store = JobStore(capacity=1)
        j1 = make_job()
        store.mark_released(j1)  # not in the store yet: must not crash
        store.add(j1)
        assert store.get(j1.id) is j1  # finished count 1 <= capacity
        j2 = make_job()
        store.add(j2)
        store.mark_released(j2)
        assert store.get(j1.id) is None
        assert store.get(j2.id) is j2

    def test_zero_capacity_keeps_only_active_jobs(self):
        store = JobStore(capacity=0)
        j1, j2 = make_job(), make_job()
        store.add(j1)
        store.add(j2)
        store.mark_released(j1)
        assert store.get(j1.id) is None
        assert store.get(j2.id) is j2  # still active

    def test_concurrent_submit_finish_release(self):
        """Hammer the store from many threads: no lost active records, no
        crashes, and the finished-record cap holds once all is released."""
        store = JobStore(capacity=5)
        jobs = [make_job() for _ in range(200)]
        errors = []

        def worker(job):
            try:
                store.add(job)
                # every unfinished record must be retrievable at all times
                if not job.released.is_set():
                    assert store.get(job.id) is job
                store.mark_released(job)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(j,)) for j in jobs]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        remaining = store.list_summaries(limit=1000)
        # capacity enforced for finished records (which of the last few
        # survive depends on thread interleaving, so only count is stable)
        assert len(remaining) == 5
        assert all(s["id"] in {j.id for j in jobs} for s in remaining)


class TestQueuedCancelRace:
    def test_finish_if_queued_atomic(self):
        job = make_job()
        assert job.finish_if_queued("cancelled", "cancelled while queued")
        assert job.status == "cancelled"
        assert job.finished_at is not None
        # a worker picking it up afterwards cannot start it
        assert not job.mark_running(0)
        # and a second cancel attempt does not win either
        assert not job.finish_if_queued("cancelled")

    def test_finish_if_queued_loses_to_mark_running(self):
        job = make_job()
        assert job.mark_running(0)
        assert not job.finish_if_queued("cancelled")
        assert job.status == "running"

    def test_finish_if_queued_after_terminal(self):
        job = make_job()
        assert job.finish("succeeded")
        assert not job.finish_if_queued("cancelled")
        assert job.status == "succeeded"
