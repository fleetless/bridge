// SPDX-License-Identifier: Apache-2.0
// The shared test list: "the release logic lives in each repository".
// Every copy of release.mjs must pass exactly these cases; that, and not a
// shared dependency, is what keeps the copies from drifting apart.
//
//   node --test '.github/release/*.test.mjs'

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  ReleaseError,
  addUnreleased,
  autoMerge,
  changelogSection,
  checkPassed,
  checkPin,
  commitBump,
  continuationVersion,
  createTag,
  dispatchPayload,
  dispatchTarget,
  github,
  main,
  nextVersion,
  npmState,
  npmWait,
  prereleaseVersion,
  promptGate,
  publishRelease,
  recipeGate,
  releasePr,
  requireTag,
  rotateChangelog,
  siteRelease,
} from './release.mjs'

const c = (subject, body = '') => ({ subject, body })

// ── the version rule: the design's table, row by row ────────────────────

test('empty range: refused', () => {
  assert.throws(() => nextVersion({ current: '1.4.2', commits: [] }), (e) => e instanceof ReleaseError && /nothing to release/.test(e.message))
})
test('patch only: fix and docs → patch', () => {
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('fix: a'), c('docs: b')] }).version, '1.4.3')
})
test('feature → minor', () => {
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('fix: a'), c('feat: b')] }).version, '1.5.0')
})
test('breaking by ! → major', () => {
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('feat!: a')] }).version, '2.0.0')
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('feat(auth-config)!: a')] }).version, '2.0.0')
})
test('breaking by footer → major', () => {
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('fix: a', 'text\n\nBREAKING CHANGE: gone')] }).version, '2.0.0')
})
test('breaking below 1.0 → minor', () => {
  assert.equal(nextVersion({ current: '0.22.0', commits: [c('feat!: a')] }).version, '0.23.0')
})
test('feature below 1.0 → minor', () => {
  assert.equal(nextVersion({ current: '0.22.0', commits: [c('feat: a')] }).version, '0.23.0')
})
test('override raises', () => {
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('fix: a')], bump: 'minor' }).version, '1.5.0')
})
test('override cannot lower', () => {
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('feat: a')], bump: 'patch' }).version, '1.5.0')
})
test('no Conventional prefix → patch', () => {
  assert.equal(nextVersion({ current: '1.4.2', commits: [c('Update README')] }).version, '1.4.3')
})
test('an explicit major below 1.0 leaves 0.x', () => {
  assert.equal(nextVersion({ current: '0.22.0', commits: [c('fix: a')], bump: 'major' }).version, '1.0.0')
})
test('no tag yet: the first version', () => {
  assert.deepEqual(nextVersion({ current: null, commits: [c('feat: a')], first: '1.0.0' }), { version: '1.0.0', bump: 'first' })
})
test('an unknown bump is a usage error', () => {
  assert.throws(() => nextVersion({ current: '1.0.0', commits: [c('fix: a')], bump: 'huge' }), (e) => e.code === 2)
})
test('commitBump reads the type, not the words', () => {
  assert.equal(commitBump(c('fix: add a feature')), 'patch')
  assert.equal(commitBump(c('feature: not a type')), 'patch')
})
test('requireTag: no tag reachable from HEAD is refused, naming the bootstrap tag', () => {
  assert.throws(() => requireTag(null), (e) => e instanceof ReleaseError && /create the bootstrap tag first/.test(e.message))
})
test('requireTag: a reachable tag passes through unchanged', () => {
  assert.equal(requireTag('v0.22.0'), 'v0.22.0')
})

// ── the changelog rotation ──────────────────────────────────────────────

const CHANGELOG = '# Changelog\n\nPreamble.\n\n## [Unreleased]\n\n### Added\n\n- A thing.\n\n## [1.4.2] — 2026-09-01\n\n- Old.\n'

test('Unreleased with entries: renamed, a new empty Unreleased above it', () => {
  assert.equal(
    rotateChangelog(CHANGELOG, { version: '1.5.0', date: '2026-09-23' }),
    '# Changelog\n\nPreamble.\n\n## [Unreleased]\n\n## [1.5.0] — 2026-09-23\n\n### Added\n\n- A thing.\n\n## [1.4.2] — 2026-09-01\n\n- Old.\n',
  )
})
test('an empty Unreleased without an empty line: refused (contracts, sdk)', () => {
  const empty = '# Changelog\n\n## [Unreleased]\n\n## [1.4.2] — 2026-09-01\n'
  assert.throws(() => rotateChangelog(empty, { version: '1.4.3', date: '2026-09-23' }), (e) => e instanceof ReleaseError && /is empty/.test(e.message))
})
test('an empty Unreleased with an empty line: the one fixed line (docs)', () => {
  const empty = '# Changelog\n\n## [Unreleased]\n\n## [0.22.0] — 2026-09-22\n'
  assert.equal(
    rotateChangelog(empty, { version: '0.22.1', date: '2026-09-23', emptyLine: 'The documentation changed; the product did not.' }),
    '# Changelog\n\n## [Unreleased]\n\n## [0.22.1] — 2026-09-23\n\nThe documentation changed; the product did not.\n\n## [0.22.0] — 2026-09-22\n',
  )
})
test('already rotated (a re-run): unchanged', () => {
  const once = rotateChangelog(CHANGELOG, { version: '1.5.0', date: '2026-09-23' })
  assert.equal(rotateChangelog(once, { version: '1.5.0', date: '2026-09-24' }), once)
})
test('no Unreleased heading: refused', () => {
  assert.throws(() => rotateChangelog('# Changelog\n\n## [1.0.0] — 2026-01-01\n', { version: '1.0.1', date: '2026-09-23' }), /no "## \[Unreleased\]"/)
})

// ── the contracts pin rule ──────────────────────────────────────────────

const deployed = { components: { cloud: { contracts_pin: '3.0.0' }, console: { contracts_pin: '3.0.0' } } }

test('cloud keeps the deployed pin: no flag needed', () => {
  assert.equal(checkPin(deployed, { component: 'cloud', pin: '3.0.0' }), null)
})
test('cloud moves the pin only with the bump', () => {
  assert.match(checkPin(deployed, { component: 'cloud', pin: '3.1.0' }), /deployed cloud pins 3\.0\.0.*contracts_bump/)
  assert.equal(checkPin(deployed, { component: 'cloud', pin: '3.1.0', bump: true }), null)
})
test('console and docs follow the deployed cloud, and cannot move the pin', () => {
  assert.equal(checkPin(deployed, { component: 'console', pin: '3.0.0' }), null)
  assert.match(checkPin(deployed, { component: 'docs', pin: '3.1.0' }), /cloud leads the pin and docs follows it/)
  assert.match(checkPin(deployed, { component: 'console', pin: '3.0.0', bump: true }), /console cannot move the contracts pin/)
})
test('no deployed cloud: nothing to follow', () => {
  assert.equal(checkPin({ components: {} }, { component: 'console', pin: '9.9.9' }), null)
})
test('a pin that is not X.Y.Z answers its own message before comparing anything', () => {
  // `node -p` prints the string "undefined" for a dependency that moved out
  // from under it; the old check reported that as a mismatch against the
  // deployed pin instead of naming the real problem.
  assert.match(checkPin(deployed, { component: 'cloud', pin: 'undefined' }), /no exact @fleetless\/contracts pin to check \(got 'undefined'\) — is it still in dependencies\?/)
  assert.match(checkPin(deployed, { component: 'console', pin: '3.x' }), /no exact @fleetless\/contracts pin to check \(got '3\.x'\)/)
})

// ── the agent-prompt gate ───────────────────────────────────────────────

const TOOLS = ["export const a = { name: 'console_app_create', run() {} }", "const b = {\n  name: 'console_app_auth_config_registration_put',\n}"]

test('a prompt naming only defined tools passes', () => {
  assert.equal(promptGate('call console_app_create, then console_app_auth_config_registration_put', TOOLS), null)
})
test('a prompt naming the tool deleted on 2026-09-22 is refused, by name', () => {
  assert.match(promptGate('write it back with console_app_auth_config_put', TOOLS), /names console_app_auth_config_put, which this cloud does not define/)
})
test('a tools directory that defines nothing is not a pass', () => {
  assert.match(promptGate('console_app_create', ['nothing here']), /no console_\* tool is defined/)
})

// ── the recipe gate ─────────────────────────────────────────────────────

const PROMPT = '# 🤖\n\n## The prompt\n\n```text\nStep one.\n   (c) the origin, default http://localhost:3000;\nStep two.\n```\n'
const RECIPE = "Intro.\n\nThe block is the starter's own `AGENT-SETUP.md` prompt, with (c) in words.\n\n```text\nStep one.\n   (c) the origin, defaulting to the one the dev server prints;\nStep two.\n```\n"

test('the declared line may differ, nothing else', () => {
  assert.equal(recipeGate(RECIPE, PROMPT), null)
  assert.match(recipeGate(RECIPE.replace('Step two.', 'Step 2.'), PROMPT), /line 3 differs/)
})
test('a recipe without the block is refused', () => {
  assert.match(recipeGate('no block here', PROMPT), /no ```text block/)
})
test('blocks of different length are refused', () => {
  assert.match(recipeGate(RECIPE, PROMPT.replace('Step two.\n', 'Step two.\nStep three.\n')), /3 lines.*4/)
})

// ── GitHub operations, against a recording fake ─────────────────────────

function fake(routes) {
  const calls = []
  const api = async (method, path, opts = {}) => {
    calls.push({ method, path, body: opts.body, raw: opts.raw })
    for (const [pattern, answer] of routes) if (pattern.test(`${method} ${path}`)) return typeof answer === 'function' ? answer(opts) : answer
    throw new Error(`unexpected call: ${method} ${path}`)
  }
  return { api, calls }
}
const SHA = 'a'.repeat(40)
const OTHER = 'b'.repeat(40)

// A fetch-shaped fake for github() itself, one level below the recording
// `api` fake above (which stands in for github() and never exercises its
// 404 handling at all).
function fetchFake(status, text = '') {
  return async () => ({ status, ok: status >= 200 && status < 300, text: async () => text, json: async () => JSON.parse(text || '{}') })
}

test('github: a 404 on a read (GET) answers null', async () => {
  assert.equal(await github('GET', '/repos/fleetless/web/git/ref/tags/v1.0.0', { token: 't', fetchImpl: fetchFake(404) }), null)
})
test('github: a 404 on a delete of something already gone (DELETE) answers null', async () => {
  assert.equal(await github('DELETE', '/repos/fleetless/web/releases/assets/1', { token: 't', fetchImpl: fetchFake(404) }), null)
})
test('github: a 404 on a write (POST) is a refusal, never a null the caller mistakes for success', async () => {
  await assert.rejects(
    github('POST', '/repos/fleetless/web/git/tags', { token: 't', body: {}, fetchImpl: fetchFake(404, 'Not Found') }),
    (e) => e instanceof ReleaseError && /^POST \/repos\/fleetless\/web\/git\/tags: 404/.test(e.message),
  )
})
test('github: the dispatch path cannot report success on a 404', async () => {
  // This is the exact call the CLI's `dispatch` case makes. Before the fix,
  // a 404'd dispatches endpoint answered null here, the CLI printed
  // "dispatched release" and exited 0, and nothing was recorded.
  await assert.rejects(
    github('POST', '/repos/fleetless/fleetless/dispatches', { token: 't', body: { event_type: 'release', client_payload: {} }, fetchImpl: fetchFake(404, 'Not Found') }),
    (e) => e instanceof ReleaseError,
  )
})
test('github: a GraphQL refusal (a 200 with errors) is a ReleaseError, not a silent success', async () => {
  await assert.rejects(
    github('POST', '/graphql', { token: 't', graphql: true, body: { query: 'x' }, fetchImpl: fetchFake(200, JSON.stringify({ errors: [{ message: 'nope' }] })) }),
    (e) => e instanceof ReleaseError && /nope/.test(e.message),
  )
})
test('github: a GraphQL success answers its data', async () => {
  assert.deepEqual(
    await github('POST', '/graphql', { token: 't', graphql: true, body: { query: 'x' }, fetchImpl: fetchFake(200, JSON.stringify({ data: { ok: true } })) }),
    { ok: true },
  )
})

test('createTag: an absent tag is created as an annotated tag by the App', async () => {
  const { api, calls } = fake([
    [/^GET .*\/git\/ref\/tags\/v1\.0\.0$/, null],
    [/^POST .*\/git\/tags$/, { sha: 'c'.repeat(40) }],
    [/^POST .*\/git\/refs$/, { ref: 'refs/tags/v1.0.0' }],
  ])
  assert.equal(await createTag({ repo: 'fleetless/web', version: '1.0.0', sha: SHA, api }), 'created')
  assert.deepEqual(calls[1].body, { tag: 'v1.0.0', message: 'v1.0.0', object: SHA, type: 'commit' })
  assert.deepEqual(calls[2].body, { ref: 'refs/tags/v1.0.0', sha: 'c'.repeat(40) })
})
test('createTag: an existing tag at the same commit is reused (a re-run)', async () => {
  const { api } = fake([
    [/^GET .*\/git\/ref\/tags\//, { object: { type: 'tag', sha: 'd'.repeat(40) } }],
    [/^GET .*\/git\/tags\//, { object: { sha: SHA } }],
  ])
  assert.equal(await createTag({ repo: 'fleetless/web', version: '1.0.0', sha: SHA, api }), 'exists')
})
test('createTag: an existing tag at another commit is refused, never moved', async () => {
  const { api, calls } = fake([[/^GET .*\/git\/ref\/tags\//, { object: { type: 'commit', sha: OTHER } }]])
  await assert.rejects(createTag({ repo: 'fleetless/web', version: '1.0.0', sha: SHA, api }), /already exists at b+, not at a+/)
  assert.equal(calls.filter((x) => x.method === 'POST').length, 0)
})
test('createTag: a missing answer from the tag-object POST is a ReleaseError, not a TypeError on tag.sha', async () => {
  const { api } = fake([
    [/^GET .*\/git\/ref\/tags\//, null],
    [/^POST .*\/git\/tags$/, null],
  ])
  await assert.rejects(createTag({ repo: 'fleetless/web', version: '1.0.0', sha: SHA, api }), (e) => e instanceof ReleaseError && /could not create the tag object v1\.0\.0/.test(e.message))
})

const RELEASE = (assets, extra = {}) => ({
  id: 7,
  upload_url: 'https://uploads.github.com/repos/fleetless/web/releases/7/assets{?name,label}',
  assets,
  draft: false,
  author: { login: 'github-actions[bot]' },
  ...extra,
})

test('siteRelease: a new release gets exactly the tarball and SHA256SUMS', async () => {
  const { api, calls } = fake([
    [/^GET .*\/releases\/tags\/v1\.0\.0$/, null],
    [/^POST \/repos\/fleetless\/web\/releases$/, RELEASE([])],
    [/^POST https:\/\/uploads\.github\.com\//, {}],
  ])
  assert.equal(await siteRelease({ repo: 'fleetless/web', version: '1.0.0', asset: 'dist.tar.gz', api, bytes: Buffer.from('site') }), 'uploaded')
  const uploads = calls.filter((x) => x.path.startsWith('https://uploads.github.com/'))
  assert.deepEqual(uploads.map((u) => u.path.split('?name=')[1]), ['dist.tar.gz', 'SHA256SUMS'])
  assert.match(uploads[1].raw.toString(), /^[0-9a-f]{64}  dist\.tar\.gz\n$/)
})
test('siteRelease: both assets present (a re-run): left alone', async () => {
  const { api, calls } = fake([[/^GET .*\/releases\/tags\//, RELEASE([{ id: 1, name: 'dist.tar.gz' }, { id: 2, name: 'SHA256SUMS' }])]])
  assert.equal(await siteRelease({ repo: 'fleetless/web', version: '1.0.0', asset: 'dist.tar.gz', api, bytes: Buffer.from('site') }), 'exists')
  assert.equal(calls.length, 1)
})
test('siteRelease: half an upload is removed and both go up again', async () => {
  const { api, calls } = fake([
    [/^GET .*\/releases\/tags\//, RELEASE([{ id: 1, name: 'dist.tar.gz' }])],
    [/^DELETE .*\/releases\/assets\/1$/, {}],
    [/^POST https:\/\/uploads\.github\.com\//, {}],
  ])
  assert.equal(await siteRelease({ repo: 'fleetless/web', version: '1.0.0', asset: 'dist.tar.gz', api, bytes: Buffer.from('site') }), 'uploaded')
  assert.deepEqual(calls.map((x) => x.method), ['GET', 'DELETE', 'POST', 'POST'])
})
test('siteRelease: an asset this workflow did not make is refused, not overwritten', async () => {
  const { api, calls } = fake([[/^GET .*\/releases\/tags\//, RELEASE([{ id: 9, name: 'evil.sh' }])]])
  await assert.rejects(siteRelease({ repo: 'fleetless/web', version: '1.0.0', asset: 'dist.tar.gz', api, bytes: Buffer.from('site') }), /did not make: evil\.sh/)
  assert.equal(calls.length, 1)
})
test('siteRelease: a draft release is refused, not reused', async () => {
  const { api, calls } = fake([[/^GET .*\/releases\/tags\//, RELEASE([], { draft: true })]])
  await assert.rejects(siteRelease({ repo: 'fleetless/web', version: '1.0.0', asset: 'dist.tar.gz', api, bytes: Buffer.from('site') }), /is a draft/)
  assert.equal(calls.length, 1)
})
test('siteRelease: a release not made by github-actions[bot] is refused, not reused', async () => {
  const { api, calls } = fake([[/^GET .*\/releases\/tags\//, RELEASE([], { author: { login: 'someone' } })]])
  await assert.rejects(siteRelease({ repo: 'fleetless/web', version: '1.0.0', asset: 'dist.tar.gz', api, bytes: Buffer.from('site') }), /not made by github-actions\[bot\] \(author: someone\)/)
  assert.equal(calls.length, 1)
})

const ENV = { GITHUB_SERVER_URL: 'https://github.com', GITHUB_REPOSITORY: 'fleetless/web', GITHUB_RUN_ID: '42' }

test('dispatchPayload: www carries its asset and no pin', () => {
  assert.deepEqual(dispatchPayload({ component: 'www', version: '1.0.0', commit: SHA, 'asset-name': 'dist.tar.gz' }, ENV), {
    component: 'www', version: '1.0.0', commit: SHA, run_url: 'https://github.com/fleetless/web/actions/runs/42', asset_name: 'dist.tar.gz',
  })
})
test('dispatchPayload: cloud carries ref, pin, schema and the bump as a string', () => {
  const p = dispatchPayload({ component: 'cloud', version: '0.23.0', commit: SHA, ref: 'ghcr.io/fleetless/cloud@sha256:x', 'contracts-pin': '3.0.0', schema: 'expand', 'contracts-bump': true }, ENV)
  assert.equal(p.contracts_bump, 'true')
  assert.equal(p.schema, 'expand')
  assert.equal(dispatchPayload({ component: 'console', version: '0.22.0', commit: SHA, ref: 'r', 'contracts-pin': '3.0.0' }, ENV).contracts_bump, 'false')
})
test('dispatchPayload: docs needs its pin; an unknown component is a usage error', () => {
  assert.throws(() => dispatchPayload({ component: 'docs', version: '0.23.0', commit: SHA, 'asset-name': 'site-dist.tar.gz' }, ENV), /--contracts-pin/)
  assert.throws(() => dispatchPayload({ component: 'caddy', version: '1.0.0', commit: SHA }, ENV), (e) => e.code === 2)
})
test('dispatchPayload: --schema must be expand or contract', () => {
  assert.throws(
    () => dispatchPayload({ component: 'cloud', version: '0.23.0', commit: SHA, ref: 'r', 'contracts-pin': '3.0.0', schema: 'migrate' }, ENV),
    (e) => e instanceof ReleaseError && e.code === 2 && /--schema must be expand or contract \(got 'migrate'\)/.test(e.message),
  )
  assert.equal(
    dispatchPayload({ component: 'cloud', version: '0.23.0', commit: SHA, ref: 'r', 'contracts-pin': '3.0.0', schema: 'contract' }, ENV).schema,
    'contract',
  )
})
test('dispatchPayload: a contracts-pin that is not X.Y.Z is a usage error, not a dispatched mismatch', () => {
  assert.throws(
    () => dispatchPayload({ component: 'cloud', version: '0.23.0', commit: SHA, ref: 'r', 'contracts-pin': 'undefined' }, ENV),
    (e) => e instanceof ReleaseError && e.code === 2 && /no exact @fleetless\/contracts pin to check \(got 'undefined'\)/.test(e.message),
  )
  assert.throws(
    () => dispatchPayload({ component: 'docs', version: '0.23.0', commit: SHA, 'asset-name': 'site-dist.tar.gz', 'contracts-pin': 'undefined' }, ENV),
    (e) => e instanceof ReleaseError && e.code === 2,
  )
})

test('releasePr: an open PR for the branch is reused', async () => {
  const { api, calls } = fake([[/^GET .*\/pulls\?state=open/, [{ number: 12 }]]])
  assert.equal(await releasePr({ repo: 'fleetless/docs', version: '0.23.0', branch: 'release/0.23.0', api }), 12)
  assert.equal(calls.length, 1)
})
test('releasePr: find-only answers null when there is none', async () => {
  const { api } = fake([[/^GET .*\/pulls\?state=open/, []]])
  assert.equal(await releasePr({ repo: 'fleetless/docs', version: '0.23.0', branch: 'release/0.23.0', findOnly: true, api }), null)
})

// ── auto-merge, and the merge that ends the button run ───────────────────

const PR_HEAD = 'h'.repeat(40)
const PR_NODE = 'PR_kwDOExample'

test('autoMerge: a PR merged already answers merged, without GraphQL', async () => {
  const { api, calls } = fake([[/^GET .*\/pulls\/5$/, { merged: true, merge_commit_sha: SHA }]])
  assert.equal(await autoMerge({ repo: 'fleetless/docs', number: '5', api }), 'merged')
  assert.equal(calls.length, 1)
})
test('autoMerge: a closed, unmerged PR is refused', async () => {
  const { api } = fake([[/^GET .*\/pulls\/5$/, { state: 'closed', merged: false }]])
  await assert.rejects(autoMerge({ repo: 'fleetless/docs', number: '5', api }), /closed without merging/)
})
test('autoMerge: a PR behind main is refused: run Release again', async () => {
  const { api } = fake([[/^GET .*\/pulls\/5$/, { state: 'open', mergeable_state: 'behind', head: { sha: PR_HEAD } }]])
  await assert.rejects(autoMerge({ repo: 'fleetless/docs', number: '5', api }), /main moved; run Release again/)
})
test('autoMerge: a dirty PR (a conflict) is refused the same way', async () => {
  const { api } = fake([[/^GET .*\/pulls\/5$/, { state: 'open', mergeable_state: 'dirty', head: { sha: PR_HEAD } }]])
  await assert.rejects(autoMerge({ repo: 'fleetless/docs', number: '5', api }), /main moved; run Release again/)
})
test('autoMerge: a failed required check is refused at once, named', async () => {
  const { api } = fake([
    [/^GET .*\/pulls\/5$/, { state: 'open', mergeable_state: 'blocked', node_id: PR_NODE, head: { sha: PR_HEAD } }],
    [/^GET .*\/check-runs$/, { check_runs: [{ name: 'site', status: 'completed', conclusion: 'failure' }] }],
  ])
  await assert.rejects(autoMerge({ repo: 'fleetless/docs', number: '5', check: 'site', api }), /check site failed/)
})
test('autoMerge: a pending check turns auto-merge on, rebase, for that node', async () => {
  const { api, calls } = fake([
    [/^GET .*\/pulls\/5$/, { state: 'open', mergeable_state: 'blocked', node_id: PR_NODE, head: { sha: PR_HEAD } }],
    [/^GET .*\/check-runs$/, { check_runs: [{ name: 'site', status: 'in_progress', conclusion: null }] }],
    [/^POST \/graphql$/, {}],
  ])
  assert.equal(await autoMerge({ repo: 'fleetless/docs', number: '5', check: 'site', api }), 'enabled')
  const call = calls.find((x) => x.path === '/graphql')
  assert.equal(call.method, 'POST')
  assert.equal(call.body.variables.id, PR_NODE)
  assert.match(call.body.query, /enablePullRequestAutoMerge/)
  assert.match(call.body.query, /REBASE/)
})
test('autoMerge: a clean-status PR whose check succeeded is merged here', async () => {
  const { api, calls } = fake([
    [/^GET .*\/pulls\/5$/, { state: 'open', mergeable_state: 'clean', node_id: PR_NODE, head: { sha: PR_HEAD } }],
    [/^GET .*\/check-runs$/, { check_runs: [{ name: 'site', status: 'completed', conclusion: 'success' }] }],
    [/^POST \/graphql$/, () => { throw new ReleaseError('Pull request is in clean status') }],
    [/^PUT .*\/pulls\/5\/merge$/, { sha: SHA, merged: true }],
  ])
  assert.equal(await autoMerge({ repo: 'fleetless/docs', number: '5', check: 'site', api }), 'merged')
  assert.deepEqual(calls.find((x) => x.method === 'PUT').body, { merge_method: 'rebase' })
})
test('autoMerge: a clean-status PR whose check has not succeeded is refused, no merge call', async () => {
  const { api, calls } = fake([
    [/^GET .*\/pulls\/5$/, { state: 'open', mergeable_state: 'clean', node_id: PR_NODE, head: { sha: PR_HEAD } }],
    [/^GET .*\/check-runs$/, { check_runs: [{ name: 'site', status: 'in_progress', conclusion: null }] }],
    [/^POST \/graphql$/, () => { throw new ReleaseError('Pull request is in clean status') }],
  ])
  await assert.rejects(autoMerge({ repo: 'fleetless/docs', number: '5', check: 'site', api }), /clean but its site check has not succeeded/)
  assert.equal(calls.filter((x) => x.method === 'PUT').length, 0)
})
test('autoMerge: auto-merge turned off on the repository is refused, naming the setting', async () => {
  const { api } = fake([
    [/^GET .*\/pulls\/5$/, { state: 'open', mergeable_state: 'blocked', node_id: PR_NODE, head: { sha: PR_HEAD } }],
    [/^GET .*\/check-runs$/, { check_runs: [{ name: 'verify', status: 'in_progress', conclusion: null }] }],
    [/^POST \/graphql$/, () => { throw new ReleaseError('Auto merge is not allowed for the repository') }],
  ])
  await assert.rejects(autoMerge({ repo: 'fleetless/docs', number: '5', api }), /allow_auto_merge/)
})

// ── the continuation: which merged PR carries on a release ───────────────

const EVENT = (pr) => ({
  repository: { full_name: 'fleetless/docs' },
  pull_request: {
    merged: true,
    merged_by: { login: 'fleetless-release[bot]' },
    head: { ref: 'release/0.23.1', repo: { full_name: 'fleetless/docs' } },
    ...pr,
  },
})

test('continuation: the App merging release/X.Y.Z of the same repository continues it', () => {
  assert.equal(continuationVersion(EVENT({})), '0.23.1')
})
test('continuation: a pull request that was not merged is nothing, not an error', () => {
  assert.equal(continuationVersion(EVENT({ merged: false })), null)
})
test('continuation: a normal branch closed is nothing', () => {
  assert.equal(continuationVersion(EVENT({ head: { ref: 'fix/22-something', repo: { full_name: 'fleetless/docs' } } })), null)
})
test('continuation: a release branch merged by a person is refused', () => {
  assert.throws(() => continuationVersion(EVENT({ merged_by: { login: 'ade21' } })), /merged by ade21, not fleetless-release\[bot\]/)
})
test('continuation: a release branch from a fork is refused', () => {
  assert.throws(() => continuationVersion(EVENT({ head: { ref: 'release/0.23.1', repo: { full_name: 'someone/docs' } } })), /came from someone\/docs/)
})
test('continuation: release/1.2 is not a version and is refused', () => {
  assert.throws(() => continuationVersion(EVENT({ head: { ref: 'release/1.2', repo: { full_name: 'fleetless/docs' } } })), /is not release\/X\.Y\.Z/)
})

// ── the belt and braces before the tag ───────────────────────────────────

test('checkPassed: a successful required check on the commit passes', async () => {
  const { api } = fake([[/^GET .*\/check-runs$/, { check_runs: [{ name: 'verify', status: 'completed', conclusion: 'success' }] }]])
  assert.equal(await checkPassed({ repo: 'fleetless/contracts', sha: SHA, check: 'verify', api }), true)
})
test('checkPassed: failure, neutral, in progress, another name and none are all refused', async () => {
  const cases = [
    [{ name: 'verify', status: 'completed', conclusion: 'failure' }],
    [{ name: 'verify', status: 'completed', conclusion: 'neutral' }],
    [{ name: 'verify', status: 'in_progress', conclusion: null }],
    [{ name: 'other', status: 'completed', conclusion: 'success' }],
    [],
  ]
  for (const check_runs of cases) {
    const { api } = fake([[/^GET .*\/check-runs$/, { check_runs }]])
    await assert.rejects(checkPassed({ repo: 'fleetless/contracts', sha: SHA, check: 'verify', api }), /no successful verify check run/)
  }
})

// ── the CLI itself ──────────────────────────────────────────────────────

test('CLI auto-merge: a non-numeric --number is a usage error before any network call', async () => {
  // '1/../x' is the shape that matters: exactly what a path-traversal
  // attempt through `/pulls/${number}` looks like, and neither GH_TOKEN nor
  // GITHUB_REPOSITORY is set for this test — a late check would fail on
  // "GITHUB_REPOSITORY is not set" instead.
  await assert.rejects(main(['auto-merge', '--number', '1/../x']), (e) => e instanceof ReleaseError && e.code === 2 && /--number '1\/\.\.\/x' is not a plain number/.test(e.message))
})
test('CLI merge-pr: gone (usage exit 2)', async () => {
  await assert.rejects(main(['merge-pr', '--number', '5']), (e) => e.code === 2)
})

// ── B2: the changelog entry a workflow writes, and a release's notes ────

test('addUnreleased: an empty Unreleased gets the entry between blank lines', () => {
  assert.equal(
    addUnreleased('# Changelog\n\n## [Unreleased]\n\n## [1.0.0] — 2026-09-23\n', '@fleetless/sdk 4.1.0.'),
    '# Changelog\n\n## [Unreleased]\n\n- @fleetless/sdk 4.1.0.\n\n## [1.0.0] — 2026-09-23\n',
  )
})
test('addUnreleased: after the last entry already there', () => {
  assert.equal(
    addUnreleased('## [Unreleased]\n\n### Fixed\n\n- A thing.\n\n## [1.0.0] — 2026-09-23\n', 'Another.'),
    '## [Unreleased]\n\n### Fixed\n\n- A thing.\n- Another.\n\n## [1.0.0] — 2026-09-23\n',
  )
})
test('addUnreleased: the same entry twice (a re-run) changes nothing', () => {
  const once = addUnreleased('## [Unreleased]\n\n## [1.0.0] — 2026-09-23\n', 'X.')
  assert.equal(addUnreleased(once, 'X.'), once)
})
test('addUnreleased: no Unreleased heading, or a line that is not one line, is refused', () => {
  assert.throws(() => addUnreleased('## [1.0.0] — 2026-09-23\n', 'X.'), /no "## \[Unreleased\]"/)
  assert.throws(() => addUnreleased('## [Unreleased]\n', 'a\nb'), (e) => e.code === 2)
  assert.throws(() => addUnreleased('## [Unreleased]\n', '  '), (e) => e.code === 2)
})
test('changelogSection: the notes of one version, without its heading', () => {
  const text = '## [Unreleased]\n\n## [1.1.0] — 2026-09-24\n\n### Changed\n\n- B.\n\n## [1.0.0] — 2026-09-23\n\n- A.\n'
  assert.equal(changelogSection(text, '1.1.0'), '### Changed\n\n- B.')
  assert.equal(changelogSection(text, '1.0.0'), '- A.')
  assert.throws(() => changelogSection(text, '0.9.0'), /no "## \[0\.9\.0\]" heading/)
})

// ── B2: pre-releases and the npm registry ──────────────────────────────

test('prereleaseVersion: the first pre-release of a version is -next.1', () => {
  assert.equal(prereleaseVersion('4.1.0', ['4.0.0', '4.0.1-next.3']), '4.1.0-next.1')
})
test('prereleaseVersion: the next free number, never one npm holds', () => {
  assert.equal(prereleaseVersion('4.1.0', ['4.1.0-next.1', '4.1.0-next.3', '4.1.0-rc.9']), '4.1.0-next.4')
})

const TARBALL = 'https://registry.npmjs.org/@fleetless/sdk/-/sdk-4.1.0.tgz'
function registry({ listed = true, served = true, status = 200 } = {}) {
  const calls = []
  const fetchImpl = async (url, opts = {}) => {
    calls.push({ url, method: opts.method ?? 'GET' })
    if (url === TARBALL) return { status: served ? 200 : 404, ok: served }
    if (status !== 200) return { status, ok: false }
    const versions = listed ? { '4.1.0': { dist: { tarball: TARBALL } } } : {}
    return { status: 200, ok: true, json: async () => ({ versions }) }
  }
  return { fetchImpl, calls }
}

test('npmState: absent, listed (metadata before tarball), served', async () => {
  assert.equal(await npmState('@fleetless/sdk', '4.1.0', registry({ listed: false })), 'absent')
  assert.equal(await npmState('@fleetless/sdk', '4.1.0', registry({ served: false })), 'listed')
  const r = registry()
  assert.equal(await npmState('@fleetless/sdk', '4.1.0', r), 'served')
  assert.equal(r.calls[0].url, 'https://registry.npmjs.org/@fleetless%2fsdk')
})
test('npmState: a package npm has never seen is absent, a registry error is refused', async () => {
  assert.equal(await npmState('@fleetless/sdk', '4.1.0', registry({ status: 404 })), 'absent')
  await assert.rejects(npmState('@fleetless/sdk', '4.1.0', registry({ status: 503 })), /answered 503/)
})
test('npmState: a package name that is not one is a usage error before any request', async () => {
  const r = registry()
  await assert.rejects(npmState('../etc', '4.1.0', r), (e) => e.code === 2)
  assert.equal(r.calls.length, 0)
})
test('npmWait: waits through "listed" until the tarball is served', async () => {
  let served = false
  const fetchImpl = async (url) => {
    if (url === TARBALL) return { status: served ? 200 : 404, ok: served }
    return { status: 200, ok: true, json: async () => ({ versions: { '4.1.0': { dist: { tarball: TARBALL } } } }) }
  }
  let sleeps = 0
  const sleep = async () => {
    sleeps++
    served = sleeps >= 2
  }
  assert.equal(await npmWait('@fleetless/sdk', '4.1.0', { fetchImpl, sleep }), 'served')
  assert.equal(sleeps, 2)
})
test('npmWait: still not served at the deadline is refused, with the state', async () => {
  let t = 0
  await assert.rejects(
    npmWait('@fleetless/sdk', '4.1.0', { ...registry({ served: false }), sleep: async () => {}, timeoutMs: 1000, now: () => (t += 600) }),
    /is listed on npm, not served/,
  )
})

// ── B2: a GitHub release of several files, or of notes alone ────────────

const DEBS = ['ros-humble-fleetless-bridge_4.0.1-0jammy_all.deb', 'ros-jazzy-fleetless-bridge_4.0.1-0noble_all.deb', 'ros-lyrical-fleetless-bridge_4.0.1-0resolute_all.deb']

test('publishRelease: every file, then one SHA256SUMS over exactly them', async () => {
  const { api, calls } = fake([
    [/^GET .*\/releases\/tags\/v4\.0\.1$/, null],
    [/^POST \/repos\/fleetless\/bridge\/releases$/, RELEASE([])],
    [/^POST https:\/\/uploads\.github\.com\//, {}],
  ])
  const files = DEBS.map((name) => ({ name, bytes: Buffer.from(name) }))
  assert.equal(await publishRelease({ repo: 'fleetless/bridge', version: '4.0.1', files, notes: 'notes', api }), 'uploaded')
  assert.equal(calls[1].body.body, 'notes')
  const uploads = calls.filter((x) => x.path.startsWith('https://uploads.github.com/'))
  assert.deepEqual(uploads.map((u) => u.path.split('?name=')[1]), [...DEBS, 'SHA256SUMS'])
  const sums = uploads[3].raw.toString().trim().split('\n')
  assert.deepEqual(sums.map((l) => l.split('  ')[1]), DEBS)
})
test('publishRelease: notes alone (app-starter): created, nothing uploaded', async () => {
  const { api, calls } = fake([
    [/^GET .*\/releases\/tags\/v1\.0\.0$/, null],
    [/^POST \/repos\/fleetless\/app-starter\/releases$/, RELEASE([])],
  ])
  assert.equal(await publishRelease({ repo: 'fleetless/app-starter', version: '1.0.0', notes: '- First.', api }), 'created')
  assert.deepEqual(calls[1].body, { tag_name: 'v1.0.0', name: 'v1.0.0', body: '- First.' })
  assert.equal(calls.length, 2)
})
test('publishRelease: notes alone, the release already there (a re-run): left alone', async () => {
  const { api, calls } = fake([[/^GET .*\/releases\/tags\//, RELEASE([])]])
  assert.equal(await publishRelease({ repo: 'fleetless/app-starter', version: '1.0.0', notes: 'x', api }), 'exists')
  assert.equal(calls.length, 1)
})
test('publishRelease: notes alone, but the release carries an asset: refused', async () => {
  const { api } = fake([[/^GET .*\/releases\/tags\//, RELEASE([{ id: 3, name: 'x.zip' }])]])
  await assert.rejects(publishRelease({ repo: 'fleetless/app-starter', version: '1.0.0', notes: 'x', api }), /did not make: x\.zip/)
})
test('publishRelease: two of three packages present: all removed, all uploaded again', async () => {
  const { api, calls } = fake([
    [/^GET .*\/releases\/tags\//, RELEASE([{ id: 1, name: DEBS[0] }, { id: 2, name: DEBS[1] }])],
    [/^DELETE .*\/releases\/assets\/[12]$/, {}],
    [/^POST https:\/\/uploads\.github\.com\//, {}],
  ])
  const files = DEBS.map((name) => ({ name, bytes: Buffer.from(name) }))
  assert.equal(await publishRelease({ repo: 'fleetless/bridge', version: '4.0.1', files, notes: 'n', api }), 'uploaded')
  assert.deepEqual(calls.map((x) => x.method), ['GET', 'DELETE', 'DELETE', 'POST', 'POST', 'POST', 'POST'])
})
test('publishRelease: a SHA256SUMS handed in is a usage error — it is written here', async () => {
  const { api } = fake([])
  await assert.rejects(publishRelease({ repo: 'fleetless/bridge', version: '4.0.1', files: [{ name: 'SHA256SUMS', bytes: Buffer.from('') }], notes: 'n', api }), (e) => e.code === 2)
})

// ── B2: where each release is announced ─────────────────────────────────

test('dispatchTarget: bridge → the ops repository\'s apt publish, sdk → app-starter, the rest → record-release', () => {
  assert.deepEqual(dispatchTarget('bridge'), { repo: 'fleetless/fleetless', event: 'bridge-release' })
  assert.deepEqual(dispatchTarget('sdk'), { repo: 'fleetless/app-starter', event: 'sdk-release' })
  assert.deepEqual(dispatchTarget('www'), { repo: 'fleetless/fleetless', event: 'release' })
})
test('dispatchPayload: bridge carries the tag publish-apt.yml reads', () => {
  assert.deepEqual(dispatchPayload({ component: 'bridge', version: '4.0.1', commit: SHA }, { ...ENV, GITHUB_REPOSITORY: 'fleetless/bridge' }), {
    component: 'bridge', version: '4.0.1', commit: SHA, run_url: 'https://github.com/fleetless/bridge/actions/runs/42', tag: 'v4.0.1',
  })
})
test('dispatchPayload: sdk carries its version and nothing a deploy would read', () => {
  assert.deepEqual(Object.keys(dispatchPayload({ component: 'sdk', version: '4.1.0', commit: SHA }, ENV)).sort(), ['commit', 'component', 'run_url', 'version'])
})
test('dispatchPayload: a version that is not X.Y.Z, or a commit that is not 40 hex, is a usage error', () => {
  assert.throws(() => dispatchPayload({ component: 'sdk', version: '4.1', commit: SHA }, ENV), (e) => e.code === 2)
  assert.throws(() => dispatchPayload({ component: 'bridge', version: '4.0.1', commit: 'HEAD' }, ENV), (e) => e.code === 2 && /40-hex/.test(e.message))
})
