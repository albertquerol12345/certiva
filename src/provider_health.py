from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from .config import settings


@dataclass
class ProviderState:
    kind: str
    name: str
    threshold: int
    consecutive_failures: int = 0
    total_failures: int = 0
    degraded: bool = False
    degrade_count: int = 0
    first_failure_ts: float | None = None
    degrade_durations: List[float] = field(default_factory=list)

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.first_failure_ts = None
        if self.degraded:
            self.degraded = False

    def record_failure(self) -> bool:
        self.total_failures += 1
        self.consecutive_failures += 1
        now = time.time()
        if self.first_failure_ts is None:
            self.first_failure_ts = now
        if self.consecutive_failures >= max(1, self.threshold):
            if not self.degraded:
                self.degraded = True
                self.degrade_count += 1
                duration = now - (self.first_failure_ts or now)
                self.degrade_durations.append(duration)
        return self.degraded

    def as_dict(self) -> Dict[str, float | int | str | bool]:
        avg_time = 0.0
        if self.degrade_durations:
            avg_time = sum(self.degrade_durations) / len(self.degrade_durations)
        return {
            "kind": self.kind,
            "name": self.name,
            "threshold": self.threshold,
            "degraded": self.degraded,
            "total_failures": self.total_failures,
            "degrade_count": self.degrade_count,
            "avg_time_to_degrade": avg_time,
        }


_states: Dict[Tuple[str, str], ProviderState] = {}


def _threshold_for(kind: str) -> int:
    if kind == "ocr":
        return max(1, getattr(settings, "ocr_breaker_threshold", 3))
    if kind == "llm":
        return max(1, getattr(settings, "llm_breaker_threshold", 3))
    return 3


def _state(kind: str, name: str) -> ProviderState:
    key = (kind, name)
    if key not in _states:
        _states[key] = ProviderState(kind=kind, name=name, threshold=_threshold_for(kind))
    return _states[key]


def record_success(kind: str, name: str) -> None:
    _state(kind, name).record_success()


def record_failure(kind: str, name: str) -> bool:
    return _state(kind, name).record_failure()


def is_degraded(kind: str, name: str) -> bool:
    return _state(kind, name).degraded


def snapshot() -> Dict[Tuple[str, str], Dict[str, float | int | str | bool]]:
    return {key: state.as_dict() for key, state in _states.items()}


def reset_all() -> None:
    _states.clear()
