from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, Dict, Tuple


class _AzureOCRStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reset()

    def _reset(self) -> None:
        self.calls_total = 0
        self.retry_total = 0
        self.status_counts: Dict[int, int] = defaultdict(int)
        self.retry_after_sum = 0.0
        self.cache_hits = 0
        self.cache_misses = 0
        self.latency_samples: deque[float] = deque(maxlen=2000)
        self.delay_samples: deque[float] = deque(maxlen=2000)

    def record(
        self,
        *,
        status_code: int,
        latency_ms: float,
        retry_after_s: float,
        cache_hit: bool,
        throttling_delay_ms: float,
        retried: bool,
    ) -> None:
        with self._lock:
            self.calls_total += 1
            if retried:
                self.retry_total += 1
            self.status_counts[status_code] += 1
            if retry_after_s:
                self.retry_after_sum += retry_after_s
            if cache_hit:
                self.cache_hits += 1
            else:
                self.cache_misses += 1
            if latency_ms >= 0:
                self.latency_samples.append(latency_ms)
            if throttling_delay_ms >= 0:
                self.delay_samples.append(throttling_delay_ms)

    def snapshot(self, reset: bool = False) -> Dict[str, Any]:
        with self._lock:
            data = {
                "calls_total": self.calls_total,
                "retry_total": self.retry_total,
                "status_counts": dict(self.status_counts),
                "retry_after_sum": self.retry_after_sum,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "latency_samples": list(self.latency_samples),
                "delay_samples": list(self.delay_samples),
            }
            if reset:
                self._reset()
            return data


_stats = _AzureOCRStats()


def record_call(
    *,
    status_code: int,
    latency_ms: float,
    retry_after_s: float = 0.0,
    cache_hit: bool = False,
    throttling_delay_ms: float = 0.0,
    retried: bool = False,
) -> None:
    _stats.record(
        status_code=status_code,
        latency_ms=latency_ms,
        retry_after_s=retry_after_s,
        cache_hit=cache_hit,
        throttling_delay_ms=throttling_delay_ms,
        retried=retried,
    )


def snapshot(reset: bool = False) -> Dict[str, Any]:
    return _stats.snapshot(reset=reset)
