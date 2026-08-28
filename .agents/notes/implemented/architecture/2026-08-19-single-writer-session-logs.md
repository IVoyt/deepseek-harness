# Agent Note: Session persistence single-writer guard — lock sidecar plus durable-tail verification

Status: implemented

English | [中文](2026-08-19-single-writer-session-logs.zh.md)

## Problem

Session logs are event-sourced with `seq = log.length` contiguity, and each live process derives its next `seq` from its **own** in-memory copy (`Session.seq` getter). Nothing prevented two dsh processes from mounting the same store: JSONL `appendLines` opens the artifact in `'a'` mode and appends blindly, and SQLite's WAL admits concurrent writers. In production this shipped as a dsh-web session that failed to load: `corrupt session log: seq gap in committed region at line 13166 (expected 90666, got 90662)`. The artifact held two writers' tails: one process had seeded through `session/end-seed` and written an interrupted `tool/result@90662`, while a second process with a stale view wrote `approval/decided@90662` — the branch the user actually allowed — sixteen minutes later. Two events share one seq, so the committed region is no longer contiguous.

A second, quieter gap lived in repair: `commitRepair` trusted the preparing read, so a writer that committed and exited between that read and the repair would lose its rows to the deletion.

## Decision

**An advisory lock sidecar names the writer; liveness decides takeover.** Before the first write to a log (initial materialization, append, or repair), the backend creates `<artifact>.lock` with `O_EXCL` containing `{pid, createdAt}`. On a pre-existing sidecar: same pid re-adopts; a dead pid (`kill(pid, 0)`) is removed and re-acquired, with a warning naming the stale pid; a live foreign pid rejects the write and names its pid. A sidecar that does not parse as a valid record is refused rather than guessed: a live owner torn mid-write must not be mistaken for stale. `release()` removes the sidecar only when its payload still names this process, and is best-effort; a leftover sidecar is reclaimed by the next acquirer.

**The first write verifies the durable base; a dead writer's torn tail is discarded.** Once per process per log, the durable tail is decoded and compared against the in-memory view: a tail that advanced, shrank, or vanished outside this process rejects loudly instead of forking the seq space. A structurally torn tail left by a dead writer is truncated to its complete records and re-appended with the usual recovery closers. Lock and verification cover exactly the operations that mutate a log — reads and listing never take the lock, and in-process `:memory:` SQLite takes none.

**`commitRepair` now takes the observed revision.** The seam is `commitRepair(meta, tornMarker, closers, revision)`: the backend re-verifies, under the guard, that the store still carries the revision the preparing `loadStored` read observed (the JSONL backend compares the file identity, SQLite the row's revision) before deleting anything. A writer that committed and exited in between is detected, and its rows survive.

The guard logic is one shared module in `@deepseek-ai/dsh-session-persistence` (`single-writer.ts`, exported as `./single-writer`), used by both first-party backends; the on-disk format is unchanged (`SESSION_FORMAT_VERSION` stays 0).

## Consequences

The incident failure now fails as a loud, self-explaining rejection on the second process's first write — naming the live owner's pid, or the advanced/shrunk/vanished tail — instead of as corruption discovered at the next load. The user's damaged session was salvaged by keeping the shared prefix (seq 0..90661) plus the approved branch (seq 90662..98054) and dropping the unapproved fork tail; the original artifact is kept as `session.jsonl.zstd.corrupt.bak`. Processes running the pre-fix code keep their stale in-memory views, so a restart is required for them to resume the repaired log from disk and to take the guard with them. The `commitRepair` seam change is a cross-package interface change: third-party `PersistenceBackend` implementations must add the `revision` parameter.

The guard is deliberately host-local and advisory: it coordinates dsh processes sharing one root or database file, and provides no protection against another user or tool writing the artifact directly (the SQLite README's existing principal boundary covers the same ground for the database entry). Both backend READMEs and the shared seam README document the guard and its limits.

## Alternatives considered

- **Exclusive OS locks (`flock`/lock-file handles)** — platform semantics diverge (release-on-close, fork inheritance, cross-socket behavior), and a lock held by a process that never cleanly exits still needs a liveness story; the sidecar supplies that uniformly and is readable by humans and tools.
- **Refusing to start when a foreign writer is present** — rejects legitimate topologies (read-only observers, parallel read-side consumers) to defend against the one operation that actually corrupts: a second mutating process.
- **Re-verify the durable tail on every append** — pays a full decode per batch; the first-write base check plus lock ownership already closes the stale-view fork, because the lock stays with this process for its whole lifetime and every new acquirer re-verifies.
- **A shared coordination service** — a cross-process store or socket for writer election adds a dependency and a failure mode to the most critical path in the harness; an advisory file guard degrades gracefully (worst case: the old corruption behavior, never a refused legitimate single-writer start).