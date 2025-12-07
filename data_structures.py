from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Set, List, Tuple, Optional

@dataclass
class Version:
    value: int
    commit_time: int
    writer: Optional[str]
    sites: Set[int]

@dataclass
class VariableHistory:
    name: str
    versions: List[Version] = field(default_factory=list)

    def latest_before(self, ts: int) -> Optional[Version]:
        best: Optional[Version] = None
        for v in self.versions:
            if v.commit_time <= ts and (best is None or v.commit_time > best.commit_time):
                best = v
        return best

@dataclass
class Site:
    id: int
    is_up: bool = True
    data: Dict[str, int] = field(default_factory=dict)
    can_read: Dict[str, bool] = field(default_factory=dict)
    failure_times: List[int] = field(default_factory=list)
    recovery_times: List[int] = field(default_factory=list)

    def fail(self, time: int) -> None:
        if not self.is_up:
            return
        self.is_up = False
        self.failure_times.append(time)
        print(f"Site {self.id} fails")

    def recover(self, time: int, is_replicated) -> None:
        if self.is_up:
            return
        self.is_up = True
        self.recovery_times.append(time)
        print(f"Site {self.id} recovers")

@dataclass
class Transaction:
    tid: str
    start_time: int
    status: str = "active"  # "active", "committed", "aborted"
    read_vars: Set[str] = field(default_factory=set)
    write_buffer: Dict[str, int] = field(default_factory=dict)
    write_sites: Dict[str, Set[int]] = field(default_factory=dict)
    site_write_times: Dict[int, int] = field(default_factory=dict)
    commit_time: Optional[int] = None