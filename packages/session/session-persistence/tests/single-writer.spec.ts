/**
 * Raw cross-process semantics of the single-writer guard: atomic sidecar
 * creation, same-process re-adoption, live-foreign rejection naming the owner
 * pid, dead-owner takeover (verified against a real spawned process),
 * malformed-sidecar refusal, and ownership-aware release.
 */

import { afterEach, describe, expect, it, vi } from 'vitest'
import { ChildProcess, spawn } from 'node:child_process'
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { acquireSingleWriter, isProcessAlive } from '../src/single-writer.ts'
import type { SingleWriterHandle } from '../src/single-writer.ts'

const dirs: string[] = []

/** Resolve once `child` is known exited (already-exited children resolve immediately). */
function waitExit(child: ChildProcess): Promise<void> {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) resolve()
    else child.once('exit', () => { resolve() })
  })
}

const children: ChildProcess[] = []

afterEach(async () => {
  for (const child of children.splice(0)) {
    child.kill('SIGKILL')
    await waitExit(child)
  }
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

async function freshDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dsh-single-writer-'))
  dirs.push(dir)
  return dir
}

/** Start a live foreign node process; the returned pid is guaranteed set. */
function liveForeignProcess(): { child: ChildProcess; pid: number } {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 60_000)'], { stdio: 'ignore' })
  children.push(child)
  if (child.pid === undefined) throw new Error('test fixture: spawned process has no pid')
  return { child, pid: child.pid }
}

/** Stand up a sidecar owned by `pid` next to the artifact. */
async function writeSidecar(artifactPath: string, pid: number): Promise<void> {
  await writeFile(artifactPath + '.lock', `${JSON.stringify({ pid, createdAt: Date.now() })}\n`, { mode: 0o600 })
}

describe('acquireSingleWriter', () => {
  it('creates a same-process sidecar and re-adopts it on re-acquire', async () => {
    const dir = await freshDir()
    const artifact = join(dir, 'artifact')
    const first = await acquireSingleWriter(artifact)
    expect(first.lockPath).toBe(artifact + '.lock')
    const record = JSON.parse(await readFile(artifact + '.lock', 'utf8')) as { pid: number; createdAt: number }
    expect(record.pid).toBe(process.pid)
    expect(typeof record.createdAt).toBe('number')

    const again = await acquireSingleWriter(artifact)
    expect(again.lockPath).toBe(first.lockPath)

    await first.release()
    await expect(stat(artifact + '.lock')).rejects.toThrow()
    await again.release() // idempotent after the sidecar is gone
  })

  it('rejects a live foreign owner and names its pid in the error', async () => {
    const dir = await freshDir()
    const artifact = join(dir, 'artifact')
    const foreign = liveForeignProcess()
    await writeSidecar(artifact, foreign.pid)
    await expect(acquireSingleWriter(artifact))
      .rejects.toThrow(new RegExp(`being written by another dsh process \\(pid ${foreign.pid}`))
    await expect(stat(artifact + '.lock')).resolves.toBeDefined() // untouched
  })

  it('takes over a sidecar left by a dead owner and reports the stale pid', async () => {
    const dir = await freshDir()
    const artifact = join(dir, 'artifact')
    const dying = spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore' })
    children.push(dying)
    const deadPid = dying.pid
    if (deadPid === undefined) throw new Error('test fixture: spawned process has no pid')
    await new Promise<void>((resolve) => { dying.once('exit', () => { resolve() }) })
    await writeSidecar(artifact, deadPid)

    const onStale = vi.fn()
    const handle = await acquireSingleWriter(artifact, onStale)
    expect(onStale).toHaveBeenCalledTimes(1)
    expect(onStale).toHaveBeenCalledWith(expect.objectContaining({ pid: deadPid }))
    const record = JSON.parse(await readFile(artifact + '.lock', 'utf8')) as { pid: number }
    expect(record.pid).toBe(process.pid)
    await handle.release()
  })

  it('refuses a sidecar whose payload is not a readable lock record', async () => {
    const dir = await freshDir()
    const artifact = join(dir, 'artifact')
    await writeFile(artifact + '.lock', Buffer.from([1, 2, 3, 254, 0, 200]))
    await expect(acquireSingleWriter(artifact))
      .rejects.toThrow(/cannot resolve the owner of lock sidecar/)
  })

  it('refuses a sidecar with a present but unusable pid', async () => {
    const dir = await freshDir()
    const artifact = join(dir, 'artifact')
    await writeFile(artifact + '.lock', `${JSON.stringify({ pid: -3, createdAt: Date.now() })}\n`)
    await expect(acquireSingleWriter(artifact))
      .rejects.toThrow(/cannot resolve the owner of lock sidecar/)
  })

  it('release gives up a sidecar this process no longer owns', async () => {
    const dir = await freshDir()
    const artifact = join(dir, 'artifact')
    const handle: SingleWriterHandle = await acquireSingleWriter(artifact)
    const foreign = liveForeignProcess()
    await writeSidecar(artifact, foreign.pid) // ownership now belongs to the foreign process
    await handle.release()
    const record = JSON.parse(await readFile(artifact + '.lock', 'utf8')) as { pid: number }
    expect(record.pid).toBe(foreign.pid)
  })
})

describe('isProcessAlive', () => {
  it('reports the probing process itself as alive', () => {
    expect(isProcessAlive(process.pid)).toBe(true)
  })

  it('reports a spawned child as alive', () => {
    const child = liveForeignProcess()
    expect(isProcessAlive(child.pid)).toBe(true)
  })

  it('reports a pid in the unreachable range as dead', () => {
    // macOS caps pids below 99999; this value cannot name a live process.
    expect(isProcessAlive(4294967295)).toBe(false)
  })
})
