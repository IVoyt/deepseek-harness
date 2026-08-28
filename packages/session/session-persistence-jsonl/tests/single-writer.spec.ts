/**
 * Single-writer guard semantics for the JSONL backend: one durable log has at
 * most one writing process. A live foreign owner rejects the first write
 * loudly (naming its pid); a dead owner's sidecar is taken over; a durable
 * tail that moved outside this process (advanced or shrank) rejects instead
 * of forking the seq space; a dead writer's torn tail is discarded; and the
 * guard releases on dispose. Same-process double mounts exercise the base
 * verification, while the raw cross-process rejection is covered by
 * `session-persistence`'s single-writer spec against a spawned process.
 */

import { afterEach, describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { ChildProcess, spawn } from 'node:child_process'
import { appendFile, mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import { compressZstdFrame, decompressZstdPrefix, scanZstdFrames } from '../src/zstd.ts'
import { eventLines, logPath, toHeaderLine, type JsonlCompression } from '../src/format.ts'
import { meta, oneTurnLog } from '../../session-persistence/tests/contract.ts'

const roots: string[] = []
const contexts: Context[] = []
const children: ChildProcess[] = []
const WORK = '/w'

/** Resolve once `child` is known exited (already-exited children resolve immediately). */
function waitExit(child: ChildProcess): Promise<void> {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) resolve()
    else child.once('exit', () => { resolve() })
  })
}


afterEach(async () => {
  for (const child of children.splice(0)) {
    child.kill('SIGKILL')
    await waitExit(child)
  }
  for (const ctx of contexts.splice(0).reverse()) await ctx.fiber.dispose()
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true })
})

async function freshRoot(prefix = 'dsh-jsonl-single-writer-'): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), prefix))
  roots.push(root)
  return root
}

async function mount(root: string, compression: JsonlCompression): Promise<Context> {
  const ctx = new Context()
  contexts.push(ctx)
  await ctx.plugin(SessionStore)
  await ctx.plugin(JsonlSessionPersistence, { root, compression })
  return ctx
}

/** One assistant delta at a fixed seq, shaped for the packed-row encoder. */
function chunkEvent(seq: number): SessionEvent {
  return {
    type: 'assistant/chunk',
    seq,
    time: 1000 + seq,
    data: { turn: 1, step: 1, chunk: { type: 'text-delta', index: 0, text: `t${seq}` } },
  } as unknown as SessionEvent
}

/** Spawn a live foreign process and stand up its sidecar next to `artifactPath`. */
async function liveForeignOwner(artifactPath: string): Promise<number> {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 60_000)'], { stdio: 'ignore' })
  children.push(child)
  const pid = child.pid
  if (pid === undefined) throw new Error('test fixture: spawned owner has no pid')
  await writeFile(artifactPath + '.lock', `${JSON.stringify({ pid, createdAt: Date.now() })}\n`, { mode: 0o600 })
  return pid
}

/** Append one structurally torn Zstandard frame after the complete frames. */
async function appendTornFrame(path: string, plaintext: string): Promise<void> {
  const frame = await compressZstdFrame(plaintext)
  const ends = [
    frame.length - 1, frame.length - 4,
    ...[0.9, 0.75, 0.6, 0.5, 0.4, 0.25].map(ratio => Math.floor(frame.length * ratio)),
  ]
  for (const end of ends) {
    const candidate = frame.subarray(0, end)
    if (scanZstdFrames(candidate).tornStart !== 0) continue
    try {
      const decoded = (await decompressZstdPrefix(candidate)).toString('utf8')
      if (decoded.length > 0 && !decoded.endsWith('\n')) {
        await appendFile(path, candidate)
        return
      }
    } catch {
      // A cut before the first decodable block: keep searching.
    }
  }
  throw new Error('test fixture could not produce a torn Zstandard frame')
}

/** Incompressible noise so the frame has many blocks a torn cut can land between. */
function deterministicNoise(length: number): string {
  let state = 0x12345678
  let output = ''
  for (let index = 0; index < length; index++) {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0
    output += String.fromCharCode(33 + (state % 90))
  }
  return output
}

describe.each([
  { compression: 'zstd' as const },
  { compression: 'none' as const },
])('JSONL single-writer guard (%(compression)s)', ({ compression }) => {
  it('rejects the first write while a live foreign process holds the lock', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('locked-session')
    const m = meta(id, WORK)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, oneTurnLog())

    const b = await mount(root, compression)
    const artifact = logPath(root, WORK, id, compression)
    const foreignPid = await liveForeignOwner(artifact)
    await expect(
      (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(100)], true),
    ).rejects.toThrow(new RegExp(`being written by another dsh process \\(pid ${foreignPid}`))
    // The rejected write must not have touched the log.
    expect(await (b.sessionPersistence as JsonlSessionPersistence).readStoredRevision(id))
      .toEqual(await (a.sessionPersistence as JsonlSessionPersistence).readStoredRevision(id))
  })

  it('takes over a dead owner and continues the verified durable tail', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('taken-over-session')
    const m = meta(id, WORK)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, oneTurnLog())
    const nextSeq = oneTurnLog().length
    const artifact = logPath(root, WORK, id, compression)
    // The first writer exited without releasing: simulate that with a
    // sidecar whose owner pid is dead.
    const child = spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore' })
    children.push(child)
    const deadPid = child.pid
    if (deadPid === undefined) throw new Error('test fixture: spawned owner has no pid')
    await waitExit(child)
    await writeFile(artifact + '.lock', `${JSON.stringify({ pid: deadPid, createdAt: Date.now() })}\n`, { mode: 0o600 })

    const b = await mount(root, compression)
    await (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(nextSeq)], true)
    const loaded = await b.sessionPersistence.load(id)
    expect(loaded.events.length).toBe(nextSeq + 1)
    expect(loaded.events.at(-1)?.seq).toBe(nextSeq)
  })

  it('rejects a first write that would continue past an advanced durable tail', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('advanced-session')
    const m = meta(id, WORK)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0), chunkEvent(1), chunkEvent(2)])

    const b = await mount(root, compression)
    // B's in-memory view ends at seq 1 (it never saw the third event): its
    // first write must be rejected, not appended after the advanced tail.
    await expect(
      (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(2), chunkEvent(3)], true),
    ).rejects.toThrow(/advanced outside this process/)
    const loaded = await b.sessionPersistence.load(id)
    expect(loaded.events.length).toBe(3)
  })

  it('rejects a first write to a log shrank below this process view', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('shrank-session')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, compression)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0), chunkEvent(1), chunkEvent(2)])
    // Replace the log with a shorter rewrite (the last event lost externally),
    // re-encoded exactly the way the backend encodes a prefix.
    const loaded = await a.sessionPersistence.load(id)
    const prefix = loaded.events.slice(0, 2)
    const headerLine = JSON.stringify(toHeaderLine(m))
    const body = eventLines(prefix, true)
    if (compression === 'zstd') {
      await writeFile(artifact, Buffer.concat([
        await compressZstdFrame(`${headerLine}\n`),
        await compressZstdFrame(body + '\n'),
      ]))
    } else {
      await writeFile(artifact, `${headerLine}\n${body}\n`)
    }

    const b = await mount(root, compression)
    // B's view still ends at seq 2, but the log now ends at seq 1.
    await expect(
      (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(3)], true),
    ).rejects.toThrow(/shrank or was replaced outside this process/)
  })

  it('rejects a first write when the log file vanished from disk', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('vanished-session')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, compression)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0), chunkEvent(1)])
    await rm(artifact)

    const b = await mount(root, compression)
    await expect(
      (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(2)], true),
    ).rejects.toThrow(/is missing/)
  })

  it('discards a dead writer torn tail before the first append', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('torn-tail-session')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, compression)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0), chunkEvent(1)])
    if (compression === 'zstd') {
      await appendTornFrame(artifact, `${JSON.stringify({
        type: 'assistant/chunk',
        seq: 99,
        time: 1,
        data: { turn: 1, step: 1, chunk: { type: 'text-delta', index: 0, text: deterministicNoise(300_000) } },
      })}\n`)
    } else {
      await appendFile(artifact, '{"type":"assistant/chunk","seq":99,"time":1,"data":{"turn":1,"st')
    }

    const b = await mount(root, compression)
    await (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(2)], true)
    const loaded = await b.sessionPersistence.load(id)
    expect(loaded.events.map(event => event.seq)).toEqual([0, 1, 2])
  })

  it('serves reads and listing while the guard is held', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('read-while-held')
    const m = meta(id, WORK)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, oneTurnLog())
    const artifact = logPath(root, WORK, id, compression)
    await expect(stat(artifact + '.lock')).resolves.toBeDefined()

    const b = await mount(root, compression)
    const listed = await b.sessionPersistence.list()
    expect(listed.some(header => header.id === id)).toBe(true)
    const inspected = await b.sessionPersistence.inspect(id)
    expect(inspected.events).toHaveLength(oneTurnLog().length)
    const snapshots = await b.sessionPersistence.listSnapshots()
    expect(snapshots.some(snapshot => snapshot.header.id === id)).toBe(true)
  })

  it('releases the guard on dispose so a later process can write', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('released-session')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, compression)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0)])
    await expect(stat(artifact + '.lock')).resolves.toBeDefined()
    contexts.splice(contexts.indexOf(a), 1)
    await a.fiber.dispose()
    await expect(stat(artifact + '.lock')).rejects.toThrow()

    const b = await mount(root, compression)
    await (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(1)], true)
    const loaded = await b.sessionPersistence.load(id)
    expect(loaded.events.map(event => event.seq)).toEqual([0, 1])
  })

  it('rejects a repair whose revision no longer matches the store', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('stale-repair-session')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, compression)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0), chunkEvent(1)])
    const backend = a.sessionPersistence as JsonlSessionPersistence
    const observed = await backend.readStoredRevision(id)
    if (observed === undefined) throw new Error('test fixture: revision missing')

    // The store moves on after the read (another writer commits, then exits).
    await a.sessionPersistence.append(id, [chunkEvent(2)])
    if (compression === 'zstd') {
      await appendFile(artifact, await compressZstdFrame(`${JSON.stringify(chunkEvent(3))}\n`))
    } else {
      await appendFile(artifact, `${JSON.stringify(chunkEvent(3))}\n`)
    }

    await expect(backend.commitRepair(m, undefined, [], observed)).rejects.toThrow(/changed since it was read/)
    const fresh = await backend.readStoredRevision(id)
    if (fresh === undefined) throw new Error('test fixture: revision missing after write')
    await backend.commitRepair(m, undefined, [chunkEvent(4)], fresh)
    const loaded = await a.sessionPersistence.load(id)
    expect(loaded.events.map(event => event.seq)).toEqual([0, 1, 2, 3, 4])
  })

  it('names a dead owner when taking over its sidecar', async () => {
    const root = await freshRoot()
    const a = await mount(root, compression)
    const id = SessionId('named-takeover')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, compression)
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0)])
    const dying = spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore' })
    children.push(dying)
    const deadPid = dying.pid
    if (deadPid === undefined) throw new Error('test fixture: spawned process has no pid')
    await waitExit(dying)
    await writeFile(artifact + '.lock', `${JSON.stringify({ pid: deadPid, createdAt: Date.now() })}\n`)

    const b = await mount(root, compression)
    const warn = vi.spyOn(b.logger, 'warn')
    await (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(1)], true)
    expect(warn).toHaveBeenCalledWith(expect.stringContaining(`dead pid ${deadPid}`))
    warn.mockRestore()
  })
})

describe('JSONL single-writer guard: header-only and encoding edges', () => {
  it('verifies the base against a header-only log (no events yet)', async () => {
    const root = await freshRoot('dsh-jsonl-single-writer-edge-')
    const b = await mount(root, 'none')
    const id = SessionId('header-only')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, 'none')
    await mkdir(dirname(artifact), { recursive: true })
    await writeFile(artifact, `${JSON.stringify(toHeaderLine(m))}\n`)

    await (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(0)], true)
    const loaded = await b.sessionPersistence.load(id)
    expect(loaded.events.map(event => event.seq)).toEqual([0])
  })

  it('propagates a corrupt committed region from the base verification', async () => {
    const root = await freshRoot('dsh-jsonl-single-writer-edge-')
    const a = await mount(root, 'none')
    const id = SessionId('corrupt-base')
    const m = meta(id, WORK)
    const artifact = logPath(root, WORK, id, 'none')
    await a.sessionPersistence.create(m)
    await a.sessionPersistence.append(id, [chunkEvent(0), chunkEvent(1)])
    // Forge a committed-region gap: seq 1 missing and a turn/end follows the gap.
    const loaded = await a.sessionPersistence.load(id)
    const gap = [
      ...loaded.events.slice(0, 1),
      chunkEvent(2),
      { type: 'turn/end', seq: 3, time: 4, data: { turn: 1, reason: { kind: 'completed' } } } as unknown as SessionEvent,
    ]
    const headerLine = JSON.stringify(toHeaderLine(m))
    const body = eventLines(gap, true)
    await writeFile(artifact, `${headerLine}\n${body}\n`)

    const b = await mount(root, 'none')
    await expect(
      (b.sessionPersistence as JsonlSessionPersistence).appendBatch(m, [chunkEvent(2)], true),
    ).rejects.toThrow(/seq gap in committed region/)
  })
})
