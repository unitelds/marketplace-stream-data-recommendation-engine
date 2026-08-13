"""In-process metrics collector — thread-safe, zero extra dependencies."""

from __future__ import annotations

import glob
import json as _json
import os as _os
import time
from collections import defaultdict, deque
from threading import Lock

# Each gunicorn worker writes its snapshot to a /tmp file so the metrics
# endpoint can aggregate across all 24 workers regardless of which one
# handles the poll request.
_METRICS_DIR = "/tmp"
_METRICS_PFX = "toki_worker_metrics_"


def _worker_path() -> str:
    return f"{_METRICS_DIR}/{_METRICS_PFX}{_os.getpid()}.json"


def _persist(snap: dict) -> None:
    """Atomically write this worker's snapshot via a temp-file rename."""
    tmp = _worker_path() + ".tmp"
    try:
        with open(tmp, "w") as fh:
            _json.dump({"_written_at": time.time(), **snap}, fh)
        _os.replace(tmp, _worker_path())
    except Exception:
        pass


class MetricsCollector:
    """Singleton accumulating ingestion and recommendation counters."""

    _instance: "MetricsCollector | None" = None

    def __init__(self) -> None:
        self._lock = Lock()
        self.started_at = time.time()

        self.batches_total: int = 0
        self.events_received: int = 0
        self.events_processed: int = 0
        self.events_failed: int = 0
        self.consumer_batches_total: int = 0
        self.consumer_events_received: int = 0
        self.consumer_events_processed: int = 0
        self.infer_timeouts: int = 0
        self.by_activity: dict[str, int] = defaultdict(int)

        self.recs_served: int = 0
        self.by_strategy: dict[str, int] = defaultdict(int)
        self.by_endpoint: dict[str, int] = defaultdict(int)
        self.by_device: dict[str, int] = defaultdict(int)

        # 10-second rolling ring buffer — 60 slots = 10 min history
        self._ring: deque[dict] = deque(maxlen=60)
        self._slot_start = time.time()
        self._cur: dict = {"ts": self._slot_start, "events": 0, "recs": 0}

    def _rotate(self) -> None:
        now = time.time()
        if now - self._slot_start >= 10:
            self._ring.append(self._cur)
            self._slot_start = now
            self._cur = {"ts": now, "events": 0, "recs": 0}

    def record_ingestion(
        self,
        *,
        processed: int,
        failed: int,
        activity_counts: dict[str, int],
        consumer: bool = False,
    ) -> None:
        with self._lock:
            self._rotate()
            if consumer:
                self.consumer_batches_total += 1
                self.consumer_events_received += processed + failed
                self.consumer_events_processed += processed
            else:
                self.batches_total += 1
                self.events_received += processed + failed
                self.events_processed += processed
                self.events_failed += failed
            self._cur["events"] += processed
            for act, cnt in activity_counts.items():
                self.by_activity[act] += cnt

    def record_recommendations(
        self,
        *,
        count: int,
        strategy: str,
        endpoint: str,
        device: str = "unknown",
    ) -> None:
        if count <= 0:
            return
        with self._lock:
            self._rotate()
            self.recs_served += count
            self.by_strategy[strategy] += count
            self.by_endpoint[endpoint] += count
            self.by_device[device] += count
            self._cur["recs"] += count

    def record_infer_timeout(self, skipped: int) -> None:
        with self._lock:
            self.infer_timeouts += skipped

    def snapshot(self) -> dict:
        with self._lock:
            self._rotate()
            snap = {
                "uptime_seconds": round(time.time() - self.started_at),
                "ingestion": {
                    "batches": self.batches_total,
                    "events_received": self.events_received,
                    "events_processed": self.events_processed,
                    "events_failed": self.events_failed,
                    "consumer_batches": self.consumer_batches_total,
                    "consumer_events_received": self.consumer_events_received,
                    "consumer_events_processed": self.consumer_events_processed,
                    "infer_timeouts": self.infer_timeouts,
                    "by_activity": dict(self.by_activity),
                },
                "recommendations": {
                    "served_total": self.recs_served,
                    "by_strategy": dict(self.by_strategy),
                    "by_endpoint": dict(self.by_endpoint),
                    "by_device": dict(self.by_device),
                },
                "timeseries": list(self._ring) + [self._cur],
            }
        _persist(snap)
        return snap

    @classmethod
    def get(cls) -> "MetricsCollector":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


metrics = MetricsCollector.get()


def _start_background_flush(interval: float = 5.0) -> None:
    """Start a per-worker daemon thread that flushes metrics to disk every 5 s."""
    import threading

    def _loop() -> None:
        while True:
            time.sleep(interval)
            try:
                metrics.snapshot()
            except Exception:
                pass

    t = threading.Thread(target=_loop, daemon=True, name="metrics-flusher")
    t.start()


_start_background_flush()


def _sum_dicts(dicts: list[dict]) -> dict:
    out: dict[str, int] = {}
    for d in dicts:
        for k, v in d.items():
            out[k] = out.get(k, 0) + v
    return out


def _merge_timeseries(all_ts: list[list[dict]]) -> list[dict]:
    """Sum slot-by-slot across workers, aligned from the most recent slot."""
    if not all_ts:
        return []
    max_len = max(len(t) for t in all_ts)
    result = []
    for i in range(max_len):
        ev, rc, ts_v = 0, 0, 0.0
        for t in all_ts:
            off = len(t) - max_len + i
            if 0 <= off < len(t):
                slot = t[off]
                ev += slot.get("events", 0)
                rc += slot.get("recs", 0)
                ts_v = max(ts_v, slot.get("ts", 0.0))
        result.append({"ts": ts_v, "events": ev, "recs": rc})
    return result


def aggregate_all_workers() -> dict:
    """Merge snapshots from all live gunicorn workers via /tmp files.

    Workers write their own file on every snapshot() call.
    Files older than 30 s are from dead workers and are skipped.
    """
    metrics.snapshot()  # ensure this worker's file is current

    snaps, now = [], time.time()
    for path in sorted(glob.glob(f"{_METRICS_DIR}/{_METRICS_PFX}*.json")):
        try:
            with open(path) as fh:
                data = _json.load(fh)
            if now - data.get("_written_at", 0) < 30:
                snaps.append(data)
        except Exception:
            pass

    if not snaps:
        return metrics.snapshot()

    all_ing = [s.get("ingestion", {}) for s in snaps]
    all_rec = [s.get("recommendations", {}) for s in snaps]

    return {
        "uptime_seconds": max(s.get("uptime_seconds", 0) for s in snaps),
        "ingestion": {
            "batches": sum(s.get("batches", 0) for s in all_ing),
            "events_received": sum(s.get("events_received", 0) for s in all_ing),
            "events_processed": sum(s.get("events_processed", 0) for s in all_ing),
            "events_failed": sum(s.get("events_failed", 0) for s in all_ing),
            "consumer_batches": sum(s.get("consumer_batches", 0) for s in all_ing),
            "consumer_events_received": sum(
                s.get("consumer_events_received", 0) for s in all_ing
            ),
            "consumer_events_processed": sum(
                s.get("consumer_events_processed", 0) for s in all_ing
            ),
            "infer_timeouts": sum(s.get("infer_timeouts", 0) for s in all_ing),
            "by_activity": _sum_dicts([s.get("by_activity", {}) for s in all_ing]),
        },
        "recommendations": {
            "served_total": sum(s.get("served_total", 0) for s in all_rec),
            "by_strategy": _sum_dicts([s.get("by_strategy", {}) for s in all_rec]),
            "by_endpoint": _sum_dicts([s.get("by_endpoint", {}) for s in all_rec]),
            "by_device": _sum_dicts([s.get("by_device", {}) for s in all_rec]),
        },
        "timeseries": _merge_timeseries([s.get("timeseries", []) for s in snaps]),
    }
