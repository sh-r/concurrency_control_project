from __future__ import annotations
import re
from typing import Dict, Set, List, Tuple, Optional

from data_structures import Version, VariableHistory, Site, Transaction

class RepCRec:
    """
    Replicated Concurrency and Recovery (RepCRec) transaction manager.

    - Manages 20 variables x1..x20 across 10 sites.
    - Odd-indexed variables have a single home site.
    - Even-indexed variables are replicated at all sites.
    - Implements:
        * Available copies replication.
        * Serializable Snapshot Isolation (SSI).
        * First-committer-wins on write-write conflicts.
        * Abort on write-site failures (available copies rule).
        * SSI-style cycle detection using RW + WW edges.
        * Snapshot-based read logic for replicated variables.
        * Waiting + unblocking logic for reads under failures.
    """

    def __init__(self):
        self.time = 0
        self.sites: Dict[int, Site] = {}
        self.vars: Dict[str, VariableHistory] = {}
        self.txns: Dict[str, Transaction] = {}

        # For First-Committer-Wins on WW conflicts:
        # var -> (last_writer_tid, commit_time)
        self.var_last_writer: Dict[str, Tuple[str, int]] = {}

        # For SSI-style conflict info
        # readers[var] = set of tids that have read var
        self.readers: Dict[str, Set[str]] = {}

        # Blocked reads for snapshot-waiting (e.g., Test 25)
        # tid -> (var, snapshot_value, snapshot_sites)
        #   snapshot_value: value as of T.start_time
        #   snapshot_sites: list of sites that had a valid snapshot at read time
        self.blocked_reads: Dict[str, Tuple[str, int, List[int]]] = {}

        self._init_sites_and_vars()

    # -------------
    # Khushboo kx2252
    # -------------

    @staticmethod
    def is_replicated(idx: int) -> bool:
        # even-indexed vars are replicated
        return idx % 2 == 0

    @staticmethod
    def home_site_for_odd(idx: int) -> int:
        # index in 1..20, sites 1..10, rule: 1 + (index mod 10)
        return 1 + (idx % 10)

    def _init_sites_and_vars(self) -> None:
        # create 10 sites
        for sid in range(1, 11):
            self.sites[sid] = Site(id=sid)

        # initialize x1..x20 with 10*i and initial version at time 0
        for i in range(1, 21):
            name = f"x{i}"
            vh = VariableHistory(name=name)
            initial_sites: Set[int] = set()

            if self.is_replicated(i):
                # even: replicated across all sites
                for sid, site in self.sites.items():
                    site.data[name] = 10 * i
                    site.can_read[name] = True
                    initial_sites.add(sid)
            else:
                # odd: single home site
                home = self.home_site_for_odd(i)
                site = self.sites[home]
                site.data[name] = 10 * i
                site.can_read[name] = True
                initial_sites.add(home)

            vh.versions.append(
                Version(
                    value=10 * i,
                    commit_time=0,
                    writer=None,
                    sites=initial_sites,
                )
            )
            self.vars[name] = vh

    def _ensure_txn(self, tid: str) -> Transaction:
        if tid not in self.txns:
            raise ValueError(f"Transaction {tid} not begun")
        return self.txns[tid]

    def site_up_continuously(self, site: Site, start: int, end: int) -> bool:
        """Return True iff site had no failures in (start, end]."""
        for f in site.failure_times:
            if start < f <= end:
                return False
        return True

    # -------------
    # Shika sr7463
    # -------------

    def _build_conflict_graph(self, candidate_tid: str) -> Dict[str, Set[str]]:
        """
        Build conflict graph (RW + WW edges) for all non-aborted
        transactions that are either already committed or the candidate
        transaction that is trying to commit now.
        """
        nodes: Set[str] = set()
        for tid, t in self.txns.items():
            if t.status == "aborted":
                continue
            if t.status == "committed" or tid == candidate_tid:
                nodes.add(tid)

        graph: Dict[str, Set[str]] = {tid: set() for tid in nodes}

        # Helper: interval [start, end] for a txn
        def interval(tid: str) -> Tuple[int, int]:
            t = self.txns[tid]
            start = t.start_time
            if t.commit_time is not None:
                end = t.commit_time
            else:
                # for candidate or still-active txns we approximate end as now
                end = self.time
            return start, end

        # RW edges: reader -> writer
        for var, readers_set in self.readers.items():
            # writers are txns that have this var in their write_buffer
            writers = [tid for tid in nodes if var in self.txns[tid].write_buffer]

            for r in readers_set:
                if r not in nodes:
                    continue
                r_start, r_end = interval(r)
                for w in writers:
                    if w == r:
                        continue
                    w_start, w_end = interval(w)
                    # overlapping intervals?
                    if r_start < w_end and w_start < r_end:
                        graph[r].add(w)

        # WW edges: earlier writer -> later writer
        for var_name, vh in self.vars.items():
            # collect committed writers from version history
            writers_info: Dict[str, int] = {}
            for v in vh.versions:
                if v.writer is not None:
                    writers_info[v.writer] = v.commit_time
            # plus candidate if it writes this var
            cand_t = self.txns.get(candidate_tid)
            if cand_t and var_name in cand_t.write_buffer:
                writers_info[candidate_tid] = self.time

            # keep only those in nodes
            writers = [(tid, ctime) for tid, ctime in writers_info.items() if tid in nodes]
            writers.sort(key=lambda x: x[1])  # by commit time

            # pairwise edges: earlier writer -> later writer (no concurrency check)
            for i in range(len(writers)):
                ti, _ = writers[i]
                for j in range(i + 1, len(writers)):
                    tj, _ = writers[j]
                    graph[ti].add(tj)

        return graph

    def _has_cycle_involving(self, graph: Dict[str, Set[str]], tid: str) -> bool:
        """Return True if there is a directed cycle in `graph` that includes `tid`."""
        visited: Set[str] = set()
        stack: Set[str] = set()

        def dfs(u: str) -> bool:
            visited.add(u)
            stack.add(u)
            for v in graph.get(u, ()):
                if v not in visited:
                    if dfs(v):
                        return True
                elif v in stack:
                    # Found a cycle; see if tid is in the cycle stack
                    if tid in stack:
                        return True
            stack.remove(u)
            return False

        # We only care about cycles that include tid, so start DFS from tid
        if tid not in graph:
            return False
        return dfs(tid)

    def _should_abort_due_to_cycle(self, tid: str) -> bool:
        graph = self._build_conflict_graph(tid)
        return self._has_cycle_involving(graph, tid)

    # -------------
    # Commands
    # -------------

    def begin(self, tid: str) -> None:
        # start new transaction at current global time
        if tid in self.txns and self.txns[tid].status == "active":
            return
        t = Transaction(tid=tid, start_time=self.time)
        self.txns[tid] = t
        print(f"begin({tid}) at time {self.time}")

    def read(self, tid: str, var: str) -> None:
        """
        R(Ti, xj):
        - Read-your-own-write (if Ti wrote xj).
        - Otherwise, snapshot as of Ti.start_time.
        - For replicated vars: enforce continuous uptime condition.
        - For both replicated and unreplicated: may wait if no site currently usable.
        """
        t = self._ensure_txn(tid)
        if t.status != "active":
            return

        # If this txn currently has a blocked read, don't try again here.
        # (Input scripts generally don't issue further operations on a waiting txn.)
        if tid in self.blocked_reads:
            return

        # --- Read-your-own-write (RYOW) ---
        if var in t.write_buffer:
            val = t.write_buffer[var]
            print(f"{var}: {val}")
            t.read_vars.add(var)
            self.readers.setdefault(var, set()).add(tid)
            return

        idx = int(var[1:])
        vh = self.vars[var]
        # Snapshot version: latest version committed before or at T.start_time
        snap = vh.latest_before(t.start_time)
        if snap is None:
            print(f"{var}: read failed (no committed version)")
            t.status = "aborted"
            print(f"{tid} aborts (no committed version for {var})")
            return

        # case 1: non-replicated (odd index)
        if not self.is_replicated(idx):
            home = self.home_site_for_odd(idx)
            site = self.sites[home]
            val = snap.value

            if not site.is_up:
                # Spec: transaction should wait for an unreplicated item on a failed site.
                self.blocked_reads[tid] = (var, val, [home])
                print(f"{tid} waits because home site {home} for {var} is down")
                return

            if home not in snap.sites:
                print(f"{var}: read failed (snapshot not available at site {home})")
                t.status = "aborted"
                print(f"{tid} aborts (no snapshot for {var} at site {home})")
                return

            print(f"{var}: {val}")
            t.read_vars.add(var)
            self.readers.setdefault(var, set()).add(tid)
            return

        # case 2: replicated (even index) with snapshot + waiting logic
        candidate_val = snap.value
        snapshot_sites: List[int] = []
        up_readable_sites: List[int] = []

        for sid in snap.sites:
            site = self.sites[sid]
            # must have been up continuously from commit to T.begin to be eligible for snapshot
            if not self.site_up_continuously(site, snap.commit_time, t.start_time):
                continue
            snapshot_sites.append(sid)
            # currently usable?
            if site.is_up and site.can_read.get(var, True):
                up_readable_sites.append(sid)

        if not snapshot_sites:
            # This is the "no available snapshot" case (Tests 23 & 24)
            print(f"{var}: read failed (no site has continuous up-time for snapshot)")
            t.status = "aborted"
            print(f"{tid} aborts (no available snapshot for {var})")
            return

        if up_readable_sites:
            # we can read immediately
            chosen_site = up_readable_sites[0]
            _ = chosen_site  # for clarity
            print(f"{var}: {candidate_val}")
            t.read_vars.add(var)
            self.readers.setdefault(var, set()).add(tid)
            return

        # snapshot exists but no currently up readable site -> T must wait (Test 25)
        self.blocked_reads[tid] = (var, candidate_val, snapshot_sites)
        print(f"{tid} waits because no site is currently up for snapshot of {var}")

    def write(self, tid: str, var: str, value: int) -> None:
        """W(Ti, xj, v): buffer write to all currently up sites holding xj."""
        t = self._ensure_txn(tid)
        if t.status != "active":
            return

        idx = int(var[1:])
        write_time = self.time

        # determine which sites currently up that hold this variable
        sites_for_var: Set[int] = set()
        if self.is_replicated(idx):
            # replicated: logically on all sites
            for sid, site in self.sites.items():
                if not site.is_up:
                    continue
                sites_for_var.add(sid)
        else:
            # non-replicated: only at its home site
            home = self.home_site_for_odd(idx)
            site = self.sites[home]
            if site.is_up:
                sites_for_var.add(home)

        if not sites_for_var:
            print(f"{tid} waits/aborts: no site up for {var}")
            t.status = "aborted"
            print(f"{tid} aborts (no site up for write {var})")
            return

        # buffer the value
        t.write_buffer[var] = value
        existing = t.write_sites.get(var, set())
        new_sites = existing | sites_for_var
        t.write_sites[var] = new_sites

        # record earliest write time per site, for the available copies abort rule
        for sid in sites_for_var:
            if sid not in t.site_write_times:
                t.site_write_times[sid] = write_time

        print(f"W({tid}, {var}, {value}) buffered")

    def end(self, tid: str) -> None:
        """end(Ti): try to commit, may abort due to failures or SSI conflicts."""
        t = self._ensure_txn(tid)
        print(f"end({tid}) at time {self.time}")

        if t.status == "aborted":
            print(f"{tid} aborts")
            return

        # 1) Available copies abort rule: any write-site that failed after write?
        for sid, wtime in t.site_write_times.items():
            site = self.sites[sid]
            for f in site.failure_times:
                if wtime < f <= self.time:
                    t.status = "aborted"
                    print(f"{tid} aborts (site {sid} failed after write)")
                    return

        # 2) First-committer-wins (write-write conflicts)
        for var in t.write_buffer:
            if var in self.var_last_writer:
                other_tid, other_ctime = self.var_last_writer[var]
                if other_tid != tid and other_ctime > t.start_time:
                    t.status = "aborted"
                    print(f"{tid} aborts (first-committer-wins on {var} vs {other_tid})")
                    return

        # 3) SSI / cycle detection (RW + WW edges)
        if self._should_abort_due_to_cycle(tid):
            t.status = "aborted"
            print(f"{tid} aborts (cycle detected)")
            return

        # 4) Commit: apply all buffered writes to the appropriate sites
        t.commit_time = self.time
        for var, val in t.write_buffer.items():
            idx = int(var[1:])
            var_sites = t.write_sites.get(var, set())
            committed_sites: Set[int] = set()

            for sid in var_sites:
                site = self.sites[sid]
                if not site.is_up:
                    continue
                site.data[var] = val
                if self.is_replicated(idx):
                    site.can_read[var] = True
                committed_sites.add(sid)

            # update version history
            vh = self.vars[var]
            vh.versions.append(
                Version(
                    value=val,
                    commit_time=self.time,
                    writer=tid,
                    sites=committed_sites,
                )
            )
            self.var_last_writer[var] = (tid, self.time)

        t.status = "committed"
        print(f"{tid} commits")

    def fail(self, site_id: int) -> None:
        """fail(i): mark site i down and record failure time."""
        site = self.sites.get(site_id)
        if not site:
            return
        site.fail(self.time)
        # Blocked reads remain blocked until some recovery allows them

    def _try_unblock_reads(self) -> None:
        """
        After a recovery, see if any blocked reads can now be satisfied.

        Important subtlety:
        - For blocked snapshot reads, we already froze:
            * the snapshot value (as of T.start_time)
            * the eligible snapshot_sites (continuous uptime up to T.start_time)
        - Spec's "read gate" for replicated vars applies only to **transactions
          that begin after recovery**.
        - All blocked reads here come from transactions that began *before*
          the recovery, so they may use the snapshot even if can_read[var]
          is False on a recovered site.
        """
        to_remove: List[str] = []
        for tid, (var, val, snapshot_sites) in list(self.blocked_reads.items()):
            t = self.txns.get(tid)
            if not t or t.status != "active":
                to_remove.append(tid)
                continue
            for sid in snapshot_sites:
                site = self.sites[sid]
                # For blocked snapshot reads, we only require the site to be up now.
                if site.is_up:
                    print(f"{var}: {val}")
                    t.read_vars.add(var)
                    self.readers.setdefault(var, set()).add(tid)
                    to_remove.append(tid)
                    break
        for tid in to_remove:
            del self.blocked_reads[tid]

    def recover(self, site_id: int) -> None:
        """
        recover(i): bring site i back up.
        - Odd vars: immediately readable.
        - Even vars: readable only if this site has the latest committed version;
                     otherwise gate stays closed until a new commit writes here.
        """
        site = self.sites.get(site_id)
        if not site:
            return
        site.recover(self.time, self.is_replicated)

        # Recompute can_read for ALL variables at this site:
        for var_name, vh in self.vars.items():
            idx = int(var_name[1:])
            if not self.is_replicated(idx):
                # odd variables: single home, local is authoritative
                site.can_read[var_name] = True
            else:
                # replicated: site can read this var iff it has the latest committed version
                latest = vh.versions[-1]
                if site_id in latest.sites:
                    site.can_read[var_name] = True
                else:
                    site.can_read[var_name] = False

        # Some blocked reads might now be satisfiable (e.g., Test 25)
        self._try_unblock_reads()

    def dump(self) -> None:
        """
        dump(): print committed values of all copies of all variables at all sites,
        sorted by site and then by variable index ascending.
        """
        for sid in sorted(self.sites.keys()):
            site = self.sites[sid]
            parts = []
            for var in sorted(site.data.keys(), key=lambda v: int(v[1:])):
                parts.append(f"{var}: {site.data[var]}")
            print(f"site {sid} - " + ", ".join(parts))

    # -------------
    # Input parsing
    # -------------

    def execute_line(self, line: str) -> None:
        """
        Parse and execute a single line of input.
        - Ignores blank lines and comment-only lines.
        - Each real command advances global time by 1.
        """
        line = line.strip()
        if not line:
            return

        # Strip inline comments after //
        if "//" in line:
            line = line.split("//", 1)[0].strip()
            if not line:
                return

        # Ignore non-command lines (e.g., explanations or expected-output text)
        if not (
            line.startswith("begin(")
            or line.startswith("R(")
            or line.startswith("W(")
            or line.startswith("end(")
            or line.startswith("fail(")
            or line.startswith("recover(")
            or line == "dump()"
        ):
            return

        # Each real command advances global time by 1
        self.time += 1

        # begin(Ti)
        m = re.match(r"begin\((T\d+)\)", line)
        if m:
            self.begin(m.group(1))
            return

        # R(Ti, xj)
        m = re.match(r"R\((T\d+),\s*x(\d+)\)", line)
        if m:
            tid, idx = m.group(1), int(m.group(2))
            self.read(tid, f"x{idx}")
            return

        # W(Ti, xj, v)
        m = re.match(r"W\((T\d+),\s*x(\d+),\s*(-?\d+)\)", line)
        if m:
            tid, idx, val = m.group(1), int(m.group(2)), int(m.group(3))
            self.write(tid, f"x{idx}", val)
            return

        # end(Ti)
        m = re.match(r"end\((T\d+)\)", line)
        if m:
            self.end(m.group(1))
            return

        # fail(i)
        m = re.match(r"fail\((\d+)\)", line)
        if m:
            self.fail(int(m.group(1)))
            return

        # recover(i)
        m = re.match(r"recover\((\d+)\)", line)
        if m:
            self.recover(int(m.group(1)))
            return

        # dump()
        if line == "dump()":
            self.dump()
            return

        print(f"Unrecognized command: {line}")