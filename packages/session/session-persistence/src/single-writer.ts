/**
 * Cross-process single-writer guard for one durable session artifact. Two DSH
 * processes that mount the same session store must never both append to one
 * session log: each process advances its in-memory event sequence
 * independently, and a second writer whose view of the log is stale forks the
 * durable log into an unreadable seq gap (or, on repair, truncates the other
 * writer's committed tail). The guard is an advisory O_EXCL sidecar: the
 * holder owns the artifact for the life of the process, and a sidecar left by
 * a dead owner is taken over after a liveness probe. A live foreign owner is
 * rejected loudly; the guard protects nothing against another principal.
 * @module @deepseek-ai/dsh-session-persistence/single-writer
 */

import { open, readFile, rm } from 'node:fs/promises'
import type { FileHandle } from 'node:fs/promises'

/** Lock sidecar payload: the owner identity used for liveness probes. */
interface LockRecord {
  pid: number
  createdAt: number
}

/**
 * One held single-writer guard. Guards are process-scoped: a guard stays
 * valid until the process exits or {@link release} runs; it is never renewed
 * or transferred.
 */
export interface SingleWriterHandle {
  /** Path of the sidecar this guard holds. */
  readonly lockPath: string
  /**
   * Release the guard: remove the sidecar only when this process still owns
   * it. Idempotent and never throwing — a leftover sidecar is reclaimed by
   * the next acquirer's liveness probe, so a failed release only costs one
   * stale lock.
   */
  release(): Promise<void>
}

/**
 * Whether a process with this pid exists right now: `kill(pid, 0)` succeeds
 * or reports EPERM (exists, other principal) for a live pid and ESRCH for a
 * dead one.
 * @param pid - the process id to probe.
 * @returns true when the pid names a live process.
 */
export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    // EPERM: the pid exists but belongs to another principal — treat as alive.
    /* v8 ignore next -- EPERM needs a process owned by another principal; no test principal is available */
    return (error as NodeJS.ErrnoException).code === 'EPERM'
  }
}

function ownHandle(lockPath: string): SingleWriterHandle {
  const release = async (): Promise<void> => {
    try {
      const record = parseLockRecord(await readFile(lockPath, 'utf8'))
      if (record === undefined || record.pid !== process.pid) return
    } catch {
      // Unreadable or absent sidecar: ownership is no longer provably ours.
      return
    }
    try {
      await rm(lockPath, { force: true })
    } catch {
      /* v8 ignore next -- best-effort release: a surviving sidecar is reclaimed by the next acquirer */
    }
  }
  return { lockPath, release }
}

/**
 * Parse a sidecar payload. The owner's pid must be present and valid for a
 * liveness probe; a payload without one is returned as undefined so the
 * caller can decide (acquisition refuses, release gives up ownership).
 * @param raw - sidecar file content.
 * @returns the parsed record, or undefined when the payload is not usable.
 */
function parseLockRecord(raw: string): LockRecord | undefined {
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return undefined
  }
  const record = parsed as Partial<LockRecord>
  if (typeof record.pid !== 'number' || !Number.isSafeInteger(record.pid) || record.pid <= 0) return undefined
  if (typeof record.createdAt !== 'number' || !Number.isSafeInteger(record.createdAt) || record.createdAt < 0) return undefined
  return record as LockRecord
}

/**
 * Acquire exclusive single-writer ownership of one durable artifact. The
 * sidecar is created atomically next to the artifact (`<artifact>.lock`) and
 * holds the owner pid plus acquisition time. On contention: a same-process
 * sidecar is re-adopted; a sidecar whose owner pid is dead is taken over; a
 * live foreign owner rejects with the owner pid in the message.
 * @param artifactPath - the durable file this guard protects.
 * @param onStaleTakeover - optional observer for a sidecar taken over from a
 *   dead owner, so the caller can name that owner in diagnostics.
 * @returns the held guard.
 * @throws when a live foreign process owns the artifact, or the sidecar
 *   exists but its payload is not a usable lock record (a live owner may have
 *   left a partial write; refusing is the only corruption-safe outcome).
 */
export async function acquireSingleWriter(
  artifactPath: string,
  onStaleTakeover?: (stale: LockRecord) => void,
): Promise<SingleWriterHandle> {
  const lockPath = artifactPath + '.lock'
  for (;;) {
    let created: FileHandle | undefined
    try {
      created = await open(lockPath, 'wx', 0o600)
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
      const record = parseLockRecord(await readFile(lockPath, 'utf8'))
      if (record === undefined) {
        throw new Error(
          `cannot resolve the owner of lock sidecar ${JSON.stringify(lockPath)} next to `
          + `${JSON.stringify(artifactPath)}: its payload is not a readable lock record. `
          + 'Stop any dsh process that may be writing this session, then remove the sidecar to continue',
        )
      }
      if (record.pid === process.pid) return ownHandle(lockPath)
      if (isProcessAlive(record.pid)) {
        throw new Error(
          `${JSON.stringify(artifactPath)} is being written by another dsh process `
          + `(pid ${record.pid}, since ${new Date(record.createdAt).toISOString()}). `
          + 'Stop that process first; if it is already gone its lock sidecar '
          + `${JSON.stringify(lockPath)} is released automatically on the next attempt`,
        )
      }
      onStaleTakeover?.(record)
      // The owner is dead: reclaim the sidecar and retry the atomic create.
      // A racing reclaimer simply loses EEXIST and re-probes the new owner.
      await rm(lockPath, { force: true })
      continue
    }
    try {
      // Sync before close: a crash between the write and the sync would leave
      // a partial sidecar behind a live owner, and the next acquirer must not
      // mistake that for a stale lock.
      await created.writeFile(`${JSON.stringify({ pid: process.pid, createdAt: Date.now() })}\n`)
      await created.sync()
    } finally {
      await created.close()
    }
    return ownHandle(lockPath)
  }
}
