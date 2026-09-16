/**
 * SPDX-License-Identifier: Apache-2.0
 *
 * Browser proof for `subscribedQualityUpdate` handling (3c.4): does a real
 * LiveKit server actually pause `LivePublisher` when its only viewer leaves,
 * and resume it when a viewer comes back?
 *
 *   node quality-check.mjs <room>
 *
 * Prints two JSON lines to stdout, `DISCONNECT_AT <ms>` and `RECONNECT_AT
 * <ms>` -- `Date.now()` at the moment this driver asked the room to
 * disconnect and, later, at the moment a second viewer's video arrived. The
 * orchestrating shell script correlates those against the publisher's own
 * log (which timestamps `Live publish paused`/`resumed`) to answer "how long
 * after the viewer left did the publisher notice" without this file needing
 * to see the publisher's process at all -- the same separation
 * `run-proof.sh` already keeps between the publisher and the viewer.
 *
 * Two independent viewers rather than one reused connection: reusing a
 * `Room` object across disconnect/reconnect risks measuring the SDK's own
 * reconnect logic instead of the server's fresh subscription, which is the
 * one thing 3c.4 changed the server's behaviour on.
 *
 * Same environment as `viewer-check.mjs`: `CHROME_PATH`/an installed
 * Chromium, and `PLAYWRIGHT_CORE`.
 */
import { createServer } from 'node:http'
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { createHmac } from 'node:crypto'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const ROOM = process.argv[2] || 'fleetless-live-quality'
const KEY = process.env.LK_KEY || 'devkey'
const SECRET = process.env.LK_SECRET || 'secret'
const WS = process.env.LK_URL || 'ws://localhost:7880'
const WAIT_S = Number(process.env.QUALITY_WAIT_S || 20)

async function loadChromium() {
  const from = process.env.PLAYWRIGHT_CORE
  if (from) return (await import(from)).chromium
  try {
    return (await import('playwright-core')).chromium
  } catch {
    throw new Error(
      'No playwright-core. Set PLAYWRIGHT_CORE to the path of a playwright-core ' +
        'entry point (index.mjs), or run this with NODE_PATH pointing at one.',
    )
  }
}

function findChrome() {
  if (process.env.CHROME_PATH) return process.env.CHROME_PATH
  const cache = join(process.env.HOME ?? '', '.cache', 'ms-playwright')
  if (existsSync(cache)) {
    const builds = readdirSync(cache)
      .filter((d) => /^chromium-\d+$/.test(d))
      .sort((a, b) => Number(b.split('-')[1]) - Number(a.split('-')[1]))
    for (const build of builds) {
      const exe = join(cache, build, 'chrome-linux64', 'chrome')
      if (existsSync(exe)) return exe
    }
  }
  for (const exe of ['/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser']) {
    if (existsSync(exe)) return exe
  }
  throw new Error('No Chromium found. Set CHROME_PATH to a browser executable.')
}

const b64 = (o) => Buffer.from(typeof o === 'string' ? o : JSON.stringify(o)).toString('base64url')
function token(room, identity) {
  const now = Math.floor(Date.now() / 1000)
  const head = b64({ alg: 'HS256', typ: 'JWT' })
  const body = b64({
    iss: KEY, sub: identity, name: identity, nbf: now - 10, exp: now + 3600,
    video: { room, roomJoin: true, canPublish: false, canSubscribe: true, canPublishData: false },
  })
  const sig = createHmac('sha256', SECRET).update(`${head}.${body}`).digest('base64url')
  return `${head}.${body}.${sig}`
}

const chromium = await loadChromium()
const html = readFileSync(join(HERE, 'viewer.html'))
const server = createServer((_, res) => {
  res.writeHead(200, { 'content-type': 'text/html' })
  res.end(html)
})
await new Promise((r) => server.listen(0, '127.0.0.1', r))
const port = server.address().port
const browser = await chromium.launch({
  executablePath: findChrome(),
  args: ['--no-sandbox', '--disable-dev-shm-usage'],
})

async function connectAndWaitForVideo(identity) {
  const page = await browser.newPage({ viewport: { width: 320, height: 240 } })
  await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: 'load', timeout: 20000 })
  const state = await page.evaluate(
    ([u, t]) => window.__connect(u, t),
    [WS, token(ROOM, identity)],
  )
  if (state.error) throw new Error(`viewer ${identity} could not connect: ${state.error}`)
  for (let i = 0; i < 20; i++) {
    const w = await page.evaluate(() => document.getElementById('v').videoWidth)
    if (w > 0) return page
    await page.waitForTimeout(1000)
  }
  throw new Error(`viewer ${identity} never saw video`)
}

let exitCode = 1
try {
  console.log('  connecting first viewer...')
  const first = await connectAndWaitForVideo('quality-viewer-1')
  console.log('  first viewer sees video; disconnecting')
  const disconnectAt = Date.now()
  await first.evaluate(() => window.__disconnect())
  await first.close()
  console.log(`DISCONNECT_AT ${disconnectAt}`)

  console.log(`  waiting ${WAIT_S}s before the second viewer joins`)
  await new Promise((r) => setTimeout(r, WAIT_S * 1000))

  console.log('  connecting second viewer...')
  const second = await connectAndWaitForVideo('quality-viewer-2')
  const reconnectAt = Date.now()
  console.log(`RECONNECT_AT ${reconnectAt}`)
  await second.close()
  console.log('  VERDICT: PASS(both viewers saw video)')
  exitCode = 0
} catch (e) {
  console.log('  VERDICT: FAIL(' + (e && e.message) + ')')
} finally {
  await browser.close()
  server.close()
}
process.exit(exitCode)
