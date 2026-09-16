/**
 * SPDX-License-Identifier: Apache-2.0
 *
 * Browser proof for the bridge's live publish: does a real browser, running
 * the official `livekit-client`, DECODE MOVING VIDEO from `LivePublisher`?
 *
 *   node viewer-check.mjs <room> [--expect-none] [--expect-still]
 *   node viewer-check.mjs <room> --hold-seconds <n>
 *
 * What it asserts, and why each one is separate:
 *   - `state.connected && !state.error` — checked before anything else.
 *     A subscription that never reached the server (LiveKit down, the wrong
 *     `LK_URL`, a dev stack torn down) produces the same `videoWidth == 0` a
 *     genuinely empty room does, and the two are not the same finding: one
 *     says "no publisher is in the room", the other says "this instrument
 *     never reached the room at all". Failing to connect is its own FAIL,
 *     under every flag including `--expect-none` — a break test that means
 *     to prove "no video, honestly" must not pass for the wrong reason.
 *   - `videoWidth > 0` — the first fact that can only be true if media
 *     arrived. A subscribed track, a participant entry, even an attached
 *     <video> element are all true of a peer connection that carried nothing.
 *   - two frames 1 s apart DIFFER — a still image also has `videoWidth > 0`.
 *     The publisher paints a bar that sweeps and a block that flips every
 *     frame, so a frozen picture is distinguishable from a live one.
 *   - the frame is not flat — a decoder producing one grey rectangle would
 *     pass the difference test if it flickered.
 *
 * The two break switches invert the verdict, one per assertion, because a
 * single break cannot prove both: `--expect-none` is run with no publisher and
 * must report that no video arrived; `--expect-still` is run against a
 * publisher painting a frozen frame and must report that video arrived and did
 * not move. An instrument that passes either of those without the switch is
 * measuring something other than what it says.
 *
 * `--hold-seconds <n>` is a third mode, for `run-soak.sh`: connect, subscribe,
 * and stay subscribed for `n` seconds so the publisher never sees the room go
 * to zero viewers and pause -- the per-frame path a soak exists to exercise
 * runs only while a viewer is actually watching. This mode does check for
 * video content, at every sample rather than once: a subscribed video track
 * (a remote participant publishing one, not merely a connected socket) AND
 * `videoWidth > 0` on the attached element, for the whole hold. A room with
 * no publisher at all, or one whose publisher is deleted partway through,
 * must FAIL here -- `state.connected` alone is true of both and proves
 * neither.
 *
 * Environment: `CHROME_PATH` (or an installed Chromium) and
 * `PLAYWRIGHT_CORE`, the path to a `playwright-core` entry point — the bridge
 * ships no JavaScript dependencies of its own and this fixture does not add
 * any.
 */
import { createServer } from 'node:http'
import { existsSync, readdirSync, readFileSync, mkdirSync } from 'node:fs'
import { createHmac } from 'node:crypto'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const ROOM = process.argv[2] || 'fleetless-live-proof'
const EXPECT_NONE = process.argv.includes('--expect-none')
const EXPECT_STILL = process.argv.includes('--expect-still')
const holdIdx = process.argv.indexOf('--hold-seconds')
const HOLD_SECONDS = holdIdx >= 0 ? Number(process.argv[holdIdx + 1]) : 0
const KEY = process.env.LK_KEY || 'devkey'
const SECRET = process.env.LK_SECRET || 'secret'
const WS = process.env.LK_URL || 'ws://localhost:7880'

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

/**
 * Find a Chromium this host actually has. playwright-core resolves a browser
 * build number baked into the package, and a host whose installed build is a
 * different one fails with "Executable doesn't exist" — which reads as *the
 * tooling is broken* rather than *the tooling is looking in the wrong place*.
 */
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
const page = await browser.newPage({ viewport: { width: 900, height: 700 } })
const errs = []
page.on('console', (m) => { if (m.type() === 'error') errs.push(m.text()) })
page.on('pageerror', (e) => errs.push('PAGEERROR ' + e.message))

let verdict = 'UNKNOWN'
try {
  await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: 'load', timeout: 20000 })
  const state = await page.evaluate(([u, t]) => window.__connect(u, t), [WS, token(ROOM, 'fleetless-proof-viewer')])
  console.log('  connect state:', JSON.stringify(state))
  if (state.error) console.log('  connect error:', state.error)

  // The one check every mode below shares: a subscription that never
  // reached the server is its own failure, under every flag. Without this,
  // `--expect-none` against a LiveKit that is not running produces the same
  // `videoWidth == 0` a genuinely empty room does, and reports
  // "PASS(no video, as expected)" for an instrument that proved nothing --
  // the break test whose whole job is catching that failed for the same
  // reason it exists to catch.
  if (!state.connected || state.error) {
    console.log('  NEVER CONNECTED: state.connected=' + state.connected + ' error=' + state.error)
    verdict = 'FAIL(could not subscribe -- ' + (state.error || 'not connected') + ')'
    console.log('  VERDICT: ' + verdict)
    await browser.close()
    server.close()
    process.exit(1)
  }

  if (HOLD_SECONDS > 0) {
    // `connected` alone is true of a viewer sitting alone in an empty room --
    // that is a live socket to LiveKit and nothing else. What `run-soak.sh`
    // needs told apart from that is a subscribed VIDEO TRACK, decoding actual
    // frames, for the whole hold: a remote participant publishing video (from
    // `state.tracks`, kept accurate by the TrackUnsubscribed handler in
    // viewer.html) AND `videoWidth > 0` on the attached <video> element at
    // every sample. `videoWidth` is the second half on purpose: a track can
    // be subscribed (an entry in `tracks`) before the decoder has produced a
    // single frame, and it drops back to 0 when the element is detached on
    // unsubscribe -- so checking it every sample, not only at the start,
    // is what makes a publisher deleted mid-hold show up before the loop
    // exits rather than only at the final check.
    let held = null
    let failReason = null
    for (let elapsed = 0; elapsed < HOLD_SECONDS; elapsed += 5) {
      await page.waitForTimeout(Math.min(5000, (HOLD_SECONDS - elapsed) * 1000))
      held = await page.evaluate(() => ({ ...window.__state, videoWidth: document.getElementById('v').videoWidth }))
      if (!held.connected || held.error) {
        failReason = `subscription dropped (connected=${held.connected} error=${held.error})`
      } else if (!held.tracks.some((t) => t.kind === 'video')) {
        failReason = 'no subscribed video track (room empty, or the publisher went away)'
      } else if (!held.videoWidth) {
        failReason = 'subscribed to a video track but decoding no frames (videoWidth=0)'
      }
      if (failReason) {
        console.log(`  t+${elapsed}s: ${failReason} -- tracks=${JSON.stringify(held.tracks)}`)
        break
      }
    }
    verdict = !failReason
      ? `PASS(subscribed video track decoding frames for the whole ${HOLD_SECONDS}s hold)`
      : `FAIL(${failReason})`
    console.log('  VERDICT: ' + verdict)
    await browser.close()
    server.close()
    process.exit(verdict.startsWith('PASS') ? 0 : 1)
  }

  let a = null
  for (let i = 0; i < 20; i++) {
    await page.waitForTimeout(1000)
    a = await page.evaluate(() => window.__grab())
    if (a && a.w > 0) break
    if (i % 4 === 0) console.log(`  t+${i}s no decoded frame yet`)
  }
  if (!a) {
    console.log('  NO VIDEO: videoWidth stayed 0 after 20 s')
    console.log('  subscribed tracks:', JSON.stringify(await page.evaluate(() => window.__state.tracks)))
    verdict = EXPECT_NONE ? 'PASS(no video, as expected)' : 'FAIL(no video)'
  } else {
    await page.waitForTimeout(1000)
    const b = await page.evaluate(() => window.__grab())
    let changed = 0
    for (let i = 0; i < a.px.length; i++) if (Math.abs(a.px[i] - b.px[i]) > 12) changed++
    const pctChanged = ((changed / a.px.length) * 100).toFixed(1)
    const spread = a.max - a.min
    console.log(`  frame A: ${a.w}x${a.h} min=${a.min} max=${a.max} mean=${a.mean} readyState=${a.readyState}`)
    console.log(`  frame B: ${b.w}x${b.h} min=${b.min} max=${b.max} mean=${b.mean}`)
    console.log(`  subpixels differing by >12 across 1 s: ${changed}/${a.px.length} (${pctChanged}%)`)
    console.log(`  spread within frame A (max-min): ${spread}`)
    console.log(`  subscribed tracks: ${JSON.stringify(await page.evaluate(() => window.__state.tracks))}`)
    const moving = changed > a.px.length * 0.01
    const textured = spread > 40
    if (EXPECT_NONE) {
      verdict = 'FAIL(video arrived but none was expected)'
    } else if (EXPECT_STILL) {
      verdict = moving
        ? 'FAIL(the frozen publisher produced moving video)'
        : `PASS(video arrived and did not move, as expected; textured=${textured})`
    } else {
      verdict = moving && textured ? 'PASS(moving video)' : `FAIL(w>0 but moving=${moving} textured=${textured})`
    }
    mkdirSync(join(HERE, 'logs'), { recursive: true })
    await page.screenshot({ path: join(HERE, `logs/viewer-${ROOM}.png`) }).catch(() => {})
  }
  console.log('  page console errors:', errs.length)
  for (const e of errs.slice(0, 6)) console.log('    ' + e.slice(0, 200))
} finally {
  await browser.close()
  server.close()
}
console.log('  VERDICT: ' + verdict)
process.exit(verdict.startsWith('PASS') ? 0 : 1)
