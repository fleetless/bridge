#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
/**
 * A client that publishes to one publisher of a robot in bursts, through the
 * cloud's REST API, and falls silent between them. A development fixture,
 * never installed on a robot by the package: it is the other end of
 * `failsafe_probe.py`. Node 22, nothing to install.
 *
 *   node drive.mjs --api http://localhost:8080 --robot <robot id> \
 *       --publisher drive --session-file session.json --cycles 205
 *
 * One cycle: `--hz` publishes a second for `--on-seconds`, then nothing for
 * `--off-seconds`, which is one silence for the failsafe to end. Each
 * publish is `POST /api/robots/:id/publishers/:slug` with `{ message }`,
 * sent when its slot comes due, whether the one before has been answered or
 * not, with at most `--in-flight` unanswered at a time; a slot that finds
 * them all taken waits for one and is counted as behind. The silence starts
 * once the burst's last publish has been answered.
 *
 * `--session-file` is a JSON file with a developer session, `access_token`
 * and `refresh_token`. It is read once and deleted, and the session is
 * refreshed (`POST /api/auth/refresh`) before its access token runs out.
 * Neither token is ever printed.
 *
 * It prints a line every ten cycles and a JSON summary at the end. Exit code
 * 0 after the last cycle, 1 when the cloud refused the session, 2 for
 * arguments it cannot use.
 */
import { readFileSync, rmSync } from 'node:fs'
import { parseArgs } from 'node:util'
import { setTimeout as sleep } from 'node:timers/promises'

const { values: args } = parseArgs({
  options: {
    api: { type: 'string', default: 'http://localhost:8080' },
    robot: { type: 'string' },
    publisher: { type: 'string', default: 'drive' },
    'session-file': { type: 'string' },
    message: { type: 'string', default: '{"linear_speed":0,"angular_speed":0.5}' },
    hz: { type: 'string', default: '20' },
    'on-seconds': { type: 'string', default: '10' },
    'off-seconds': { type: 'string', default: '1' },
    cycles: { type: 'string', default: '0' },
    'in-flight': { type: 'string', default: '8' },
  },
})

function usage(why) {
  console.error(`drive: ${why}`)
  process.exit(2)
}

if (!args.robot) usage('--robot names no robot')
if (!args['session-file']) usage('--session-file names no file')
const hz = Number(args.hz)
const onMs = Number(args['on-seconds']) * 1000
const offMs = Number(args['off-seconds']) * 1000
const cycles = Number(args.cycles)
const inFlight = Number(args['in-flight'])
if (!(hz > 0 && onMs > 0 && offMs > 0 && Number.isInteger(cycles) && cycles >= 0 && Number.isInteger(inFlight) && inFlight >= 1)) usage('--hz, --on-seconds, --off-seconds, --cycles and --in-flight take positive numbers')
let message
try { message = JSON.parse(args.message) } catch { usage('--message is not JSON') }

const api = args.api.replace(/\/+$/, '')
let session
try {
  session = JSON.parse(readFileSync(args['session-file'], 'utf8'))
} finally {
  rmSync(args['session-file'], { force: true })
}
if (typeof session?.access_token !== 'string' || typeof session?.refresh_token !== 'string') usage('the session file holds no access_token and refresh_token')

/** Seconds since the epoch at which an access token runs out, from its own payload. */
function expiresAt(token) {
  try {
    return JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8')).exp ?? 0
  } catch {
    return 0
  }
}

async function freshSession() {
  if (expiresAt(session.access_token) - Date.now() / 1000 > 120) return
  const res = await fetch(`${api}/api/auth/refresh`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ refresh_token: session.refresh_token }),
    signal: AbortSignal.timeout(10_000),
  })
  if (!res.ok) {
    console.error(`drive: the session was not refreshed (${res.status})`)
    process.exit(1)
  }
  const body = await res.json()
  session = { access_token: body.access_token, refresh_token: body.refresh_token ?? session.refresh_token }
  stats.refreshed += 1
}

const url = `${api}/api/robots/${encodeURIComponent(args.robot)}/publishers/${encodeURIComponent(args.publisher)}`
const stats = { cycles: 0, sent: 0, ok: 0, refused: {}, failed: 0, behind: 0, refreshed: 0, latencies: [] }

async function publishOnce() {
  const started = performance.now()
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { authorization: `Bearer ${session.access_token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ message }),
      signal: AbortSignal.timeout(5_000),
    })
    await res.arrayBuffer()
    stats.sent += 1
    if (res.status === 204) stats.ok += 1
    else stats.refused[res.status] = (stats.refused[res.status] ?? 0) + 1
  } catch {
    stats.failed += 1
  }
  stats.latencies.push(performance.now() - started)
}

function quantile(values, share) {
  if (!values.length) return null
  const sorted = [...values].sort((a, b) => a - b)
  return Number(sorted[Math.max(0, Math.ceil(share * sorted.length) - 1)].toFixed(1))
}

let stopping = false
process.on('SIGINT', () => { stopping = true })
process.on('SIGTERM', () => { stopping = true })

const periodMs = 1000 / hz
const perBurst = Math.round(onMs / periodMs)
while (!stopping && (cycles === 0 || stats.cycles < cycles)) {
  await freshSession()
  const burstStart = performance.now()
  const pending = new Set()
  for (let slot = 0; slot < perBurst && !stopping; slot += 1) {
    const due = burstStart + slot * periodMs
    const wait = due - performance.now()
    if (wait > 0) await sleep(wait)
    if (pending.size >= inFlight) {
      stats.behind += 1
      await Promise.race(pending)
    }
    const sent = publishOnce().finally(() => pending.delete(sent))
    pending.add(sent)
  }
  await Promise.all(pending)
  stats.cycles += 1
  if (stats.cycles % 10 === 0) {
    console.log(`drive: ${stats.cycles} cycles, ${stats.ok}/${stats.sent} accepted, ${stats.failed} failed, ${stats.behind} behind, p50 ${quantile(stats.latencies, 0.5)} ms, p99 ${quantile(stats.latencies, 0.99)} ms`)
  }
  if (!stopping) await sleep(offMs)
}

const { latencies, ...counts } = stats
console.log(JSON.stringify({ event: 'drive_summary', ...counts, latency_ms: { p50: quantile(latencies, 0.5), p99: quantile(latencies, 0.99), max: quantile(latencies, 1) } }))
