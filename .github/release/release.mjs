#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// The release logic of one repository, per the per-repository release
// design. This file sits in every repository with a Release button, byte
// for byte. What keeps the copies from drifting apart is the shared test
// list in release.test.mjs, not a shared dependency: public repositories
// cannot call an action from a private one, and a shared repository would
// sit in the path of every release. Change it here, then in every copy.
//
//   release.mjs next [--bump auto|patch|minor|major] [--first X.Y.Z] [--require-tag]   prints the next version
//   release.mjs prerelease --package NAME [--bump B]                    prints the next X.Y.Z-next.N on npm
//   release.mjs tag-at-head                                             prints the vX.Y.Z tag on HEAD, or nothing
//   release.mjs release-commit --version X.Y.Z                          prints main's "chore(release): X.Y.Z" commit, or nothing
//   release.mjs rotate --file F --version X.Y.Z --date YYYY-MM-DD [--empty-line TEXT]
//   release.mjs changelog-add --file F --line TEXT                      one "- TEXT" under "## [Unreleased]"
//   release.mjs changelog-section --file F --version X.Y.Z              prints that version's notes
//   release.mjs pin-gate --manifest F --component C --pin X.Y.Z [--bump] [--warn]
//   release.mjs prompt-gate --prompt F --tools-dir D [--warn]
//   release.mjs recipe-gate --recipe F --prompt F [--warn]
//   release.mjs create-tag --version X.Y.Z --sha SHA                    GH_TOKEN (the App), GITHUB_REPOSITORY
//   release.mjs site-release --version X.Y.Z --asset F                  GH_TOKEN (GITHUB_TOKEN), GITHUB_REPOSITORY
//   release.mjs github-release --version X.Y.Z [--dir D] [--notes-file F]   GH_TOKEN (GITHUB_TOKEN), GITHUB_REPOSITORY
//   release.mjs npm-state --package NAME --version V                    prints absent, listed or served
//   release.mjs npm-wait --package NAME --version V [--timeout-minutes 20]
//   release.mjs dispatch --component C --version X.Y.Z --commit SHA [--ref R] [--contracts-pin P]
//                        [--schema S] [--contracts-bump] [--asset-name N]   GH_TOKEN (the App)
//   release.mjs release-pr --version X.Y.Z --branch B [--find-only]    GH_TOKEN (the App), GITHUB_REPOSITORY
//   release.mjs merge-pr --number N [--timeout-minutes 30]              GH_TOKEN (the App), GITHUB_REPOSITORY
//
// Exit codes: 0 ok, 1 refused (the reason on stderr), 2 usage.

import { execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { readFileSync, readdirSync, realpathSync, writeFileSync } from 'node:fs'
import { basename, join } from 'node:path'
import { fileURLToPath } from 'node:url'

export class ReleaseError extends Error {
  constructor(message, code = 1) {
    super(message)
    this.code = code
  }
}

const SEMVER = /^(\d+)\.(\d+)\.(\d+)$/
const RANK = { patch: 0, minor: 1, major: 2 }
const BUMPS = ['auto', 'patch', 'minor', 'major']

export function parseVersion(v) {
  const m = SEMVER.exec(String(v))
  if (!m) throw new ReleaseError(`'${v}' is not X.Y.Z`, 2)
  return m.slice(1).map(Number)
}

/** The bump one commit asks for, before the 0.x rule. */
export function commitBump({ subject, body = '' }) {
  if (/^[a-z]+(\([^)]*\))?!:/.test(subject) || /^BREAKING[ -]CHANGE:/m.test(body)) return 'major'
  if (/^feat(\([^)]*\))?:/.test(subject)) return 'minor'
  return 'patch'
}

/**
 * The next version from the commits since `current` (null: no tag yet).
 * Below 1.0 a breaking change is a minor — cloud went 0.21 → 0.22 for the
 * auth-config split — but an explicit `major` is the way out of 0.x.
 * An explicit bump only ever raises what the commits ask for.
 */
export function nextVersion({ current, commits, bump = 'auto', first = '1.0.0' }) {
  if (!BUMPS.includes(bump)) throw new ReleaseError(`--bump must be ${BUMPS.join(', ')} (got '${bump}')`, 2)
  if (commits.length === 0) throw new ReleaseError('nothing to release: no commits since the last tag')
  if (current === null) {
    parseVersion(first)
    return { version: first, bump: 'first' }
  }
  const [major, minor, patch] = parseVersion(current)
  let wanted = commits.map(commitBump).reduce((a, b) => (RANK[b] > RANK[a] ? b : a), 'patch')
  if (major === 0 && wanted === 'major') wanted = 'minor'
  if (bump !== 'auto' && RANK[bump] > RANK[wanted]) wanted = bump
  const version =
    wanted === 'major' ? `${major + 1}.0.0` : wanted === 'minor' ? `${major}.${minor + 1}.0` : `${major}.${minor}.${patch + 1}`
  return { version, bump: wanted }
}

/**
 * "## [Unreleased]" becomes "## [X.Y.Z] — date", under a new empty
 * "## [Unreleased]". The release only moves notes; it writes none, except
 * `emptyLine` where a repository allows an empty section (docs). Without it
 * an empty section is refused: a published package's readers need the why.
 * Already rotated (a re-run) returns the text unchanged.
 */
export function rotateChangelog(text, { version, date, emptyLine = null }) {
  parseVersion(version)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new ReleaseError(`'${date}' is not YYYY-MM-DD`, 2)
  const escaped = version.replace(/\./g, '\\.')
  if (new RegExp(`^## \\[${escaped}\\]`, 'm').test(text)) return text
  const lines = text.split('\n')
  const at = lines.findIndex((l) => /^## \[Unreleased\]\s*$/.test(l))
  if (at === -1) throw new ReleaseError('the changelog has no "## [Unreleased]" heading to release')
  let end = lines.findIndex((l, i) => i > at && /^## \[/.test(l))
  if (end === -1) end = lines.length
  const body = lines.slice(at + 1, end)
  const empty = body.every((l) => l.trim() === '')
  if (empty && emptyLine === null) throw new ReleaseError('"## [Unreleased]" is empty: a release needs its notes')
  const notes = empty ? ['', emptyLine, ''] : body
  return [...lines.slice(0, at), '## [Unreleased]', '', `## [${version}] — ${date}`, ...notes, ...lines.slice(end)].join('\n')
}

function unreleasedSpan(lines) {
  const at = lines.findIndex((l) => /^## \[Unreleased\]\s*$/.test(l))
  if (at === -1) throw new ReleaseError('the changelog has no "## [Unreleased]" heading')
  let end = lines.findIndex((l, i) => i > at && /^## \[/.test(l))
  if (end === -1) end = lines.length
  return { at, end }
}

/**
 * One entry, "- TEXT", at the end of "## [Unreleased]" — for a pull request
 * a workflow opens (app-starter's sdk bump), which has no person to write it.
 * The same entry already there (a re-run) returns the text unchanged.
 */
export function addUnreleased(text, entry) {
  if (typeof entry !== 'string' || entry.trim() === '' || entry.includes('\n')) throw new ReleaseError('--line must be one non-empty line', 2)
  const lines = text.split('\n')
  const { at, end } = unreleasedSpan(lines)
  const bullet = `- ${entry}`
  if (lines.slice(at + 1, end).includes(bullet)) return text
  let last = end - 1
  while (last > at && lines[last].trim() === '') last--
  if (last === at) return [...lines.slice(0, at + 1), '', bullet, '', ...lines.slice(end)].join('\n')
  return [...lines.slice(0, last + 1), bullet, ...lines.slice(last + 1)].join('\n')
}

/** The notes under "## [X.Y.Z]", without the heading: a GitHub release's body. */
export function changelogSection(text, version) {
  parseVersion(version)
  const lines = text.split('\n')
  const escaped = version.replace(/\./g, '\\.')
  const at = lines.findIndex((l) => new RegExp(`^## \\[${escaped}\\]`).test(l))
  if (at === -1) throw new ReleaseError(`the changelog has no "## [${version}]" heading`)
  let end = lines.findIndex((l, i) => i > at && /^## \[/.test(l))
  if (end === -1) end = lines.length
  return lines.slice(at + 1, end).join('\n').trim()
}

/**
 * A pre-release for a branch that consumers pin before the final version
 * exists: they pin `X.Y.Z-next.N` so their CI is green, and re-pin the final
 * version before they merge. It is the version Release would give this
 * branch now, with the next free `-next.N` that npm does not hold yet.
 */
export function prereleaseVersion(next, published) {
  parseVersion(next)
  const pattern = new RegExp(`^${next.replace(/\./g, '\\.')}-next\\.(\\d+)$`)
  const taken = published.map((v) => pattern.exec(v)).filter(Boolean).map((m) => Number(m[1]))
  return `${next}-next.${taken.length ? Math.max(...taken) + 1 : 1}`
}

/**
 * The contracts pin rule, the same one the ops repository's manifest rule
 * enforces when the ops repository records a release: cloud leads, console
 * and docs follow. Checked here first, so a refusal arrives before anything
 * is built.
 */
export function checkPin(manifest, { component, pin, bump = false }) {
  if (!SEMVER.test(String(pin))) return `no exact @fleetless/contracts pin to check (got '${pin}') — is it still in dependencies?`
  const cloud = manifest?.components?.cloud
  if (component === 'cloud') {
    if (cloud && cloud.contracts_pin !== pin && !bump)
      return `cloud pins @fleetless/contracts ${pin} but the deployed cloud pins ${cloud.contracts_pin}. A release that moves the pin says so: tick contracts_bump.`
    return null
  }
  if (bump) return `${component} cannot move the contracts pin: cloud leads, ${component} follows. Release cloud with contracts_bump first.`
  if (cloud && cloud.contracts_pin !== pin)
    return `${component} pins @fleetless/contracts ${pin} but the deployed cloud pins ${cloud.contracts_pin}. cloud leads the pin and ${component} follows it: release cloud first.`
  return null
}

export function promptTools(text) {
  return new Set(text.match(/\bconsole_[a-z0-9_]+\b/g) ?? [])
}

export function definedTools(sources) {
  const out = new Set()
  for (const s of sources) for (const m of s.matchAll(/\bname:\s*'(console_[a-z0-9_]+)'/g)) out.add(m[1])
  return out
}

/** app-starter's agent prompt may name only tools this cloud commit defines. */
export function promptGate(prompt, sources) {
  const defined = definedTools(sources)
  if (defined.size === 0) return 'no console_* tool is defined in the tools directory — is it the right directory?'
  const missing = [...promptTools(prompt)].filter((t) => !defined.has(t)).sort()
  if (missing.length === 0) return null
  return `app-starter's AGENT-SETUP.md names ${missing.join(', ')}, which this cloud does not define`
}

function textBlock(text, marker) {
  const lines = text.split('\n')
  const from = marker === null ? 0 : lines.findIndex((l) => l.includes(marker))
  if (from === -1) return null
  const start = lines.findIndex((l, i) => i >= from && l.trim() === '```text')
  if (start === -1) return null
  const end = lines.findIndex((l, i) => i > start && l.trim() === '```')
  if (end === -1) return null
  return lines.slice(start + 1, end)
}

/**
 * The docs recipe says its prompt block *is* AGENT-SETUP.md's, apart from
 * one declared line: item (c)'s default origin, spelled out in words.
 */
export function recipeGate(recipe, prompt, { marker = "The block is the starter's own", declared = '   (c) ' } = {}) {
  const a = textBlock(recipe, marker)
  const b = textBlock(prompt, null)
  if (!a) return `the recipe has no \`\`\`text block after a line containing "${marker}"`
  if (!b) return 'AGENT-SETUP.md has no ```text block'
  if (a.length !== b.length) return `the recipe's prompt has ${a.length} lines, AGENT-SETUP.md's has ${b.length}`
  for (let i = 0; i < a.length; i++) {
    if (a[i] === b[i]) continue
    if (a[i].startsWith(declared) && b[i].startsWith(declared)) continue
    return `line ${i + 1} differs:\n  recipe:         ${a[i]}\n  AGENT-SETUP.md: ${b[i]}`
  }
  return null
}

// ── git ─────────────────────────────────────────────────────────────────

const git = (...args) => execFileSync('git', args, { encoding: 'utf8' }).trim()
const RELEASE_TAG = /^v\d+\.\d+\.\d+$/

export function currentTag() {
  return git('tag', '--list', 'v[0-9]*.[0-9]*.[0-9]*', '--merged', 'HEAD', '--sort=-v:refname').split('\n').find((t) => RELEASE_TAG.test(t)) ?? null
}

export function commitsSince(tag) {
  const out = git('log', '--format=%H%x1f%s%x1f%b%x1e', tag ? `${tag}..HEAD` : 'HEAD')
  return out
    .split('\x1e')
    .map((r) => r.replace(/^\n+/, ''))
    .filter((r) => r.trim() !== '')
    .map((r) => {
      const [sha, subject, body = ''] = r.split('\x1f')
      return { sha, subject, body }
    })
}

/** `next --require-tag`: docs has a bootstrap tag and no other "current version"; refuse rather than guess 1.0.0. */
export function requireTag(current) {
  if (current === null) throw new ReleaseError('no vX.Y.Z tag is reachable from HEAD — create the bootstrap tag first')
  return current
}

export function tagAtHead() {
  return git('tag', '--points-at', 'HEAD').split('\n').find((t) => RELEASE_TAG.test(t)) ?? null
}

export function releaseCommitOnMain(version) {
  const want = `chore(release): ${version}`
  const line = git('log', 'origin/main', '-n', '200', '--format=%H%x1f%s')
    .split('\n')
    .find((l) => l.split('\x1f')[1] === want)
  return line ? line.split('\x1f')[0] : null
}

// ── GitHub ──────────────────────────────────────────────────────────────

/**
 * One REST call. A 404 on a read (GET) or on a delete of something already
 * gone (DELETE) answers null — both are legitimate "not there". A 404 on a
 * write (POST, PUT, PATCH) is not a softer form of success: it means the
 * dispatch, the tag object or the merge never happened, so it is a refusal
 * like any other failed response, not a null the caller might mistake for success.
 */
export async function github(method, path, { body, raw, base = 'https://api.github.com', token = process.env.GH_TOKEN, fetchImpl = globalThis.fetch } = {}) {
  if (!token) throw new ReleaseError('GH_TOKEN is not set', 2)
  const headers = { authorization: `Bearer ${token}`, accept: 'application/vnd.github+json', 'x-github-api-version': '2022-11-28' }
  if (raw) headers['content-type'] = 'application/octet-stream'
  else if (body) headers['content-type'] = 'application/json'
  const res = await fetchImpl(`${base}${path}`, { method, headers, body: raw ?? (body ? JSON.stringify(body) : undefined) })
  if (res.status === 404 && (method === 'GET' || method === 'DELETE')) return null
  if (!res.ok) throw new ReleaseError(`${method} ${path}: ${res.status} ${await res.text()}`)
  return res.status === 204 ? {} : res.json()
}

/** A release tag, created by the App. An existing tag is reused, never moved. */
export async function createTag({ repo, version, sha, api = github }) {
  parseVersion(version)
  const ref = await api('GET', `/repos/${repo}/git/ref/tags/v${version}`)
  if (ref) {
    let at = ref.object.sha
    if (ref.object.type === 'tag') at = (await api('GET', `/repos/${repo}/git/tags/${at}`)).object.sha
    if (at !== sha) throw new ReleaseError(`v${version} already exists at ${at}, not at ${sha} — a tag is not moved`)
    return 'exists'
  }
  const tag = await api('POST', `/repos/${repo}/git/tags`, { body: { tag: `v${version}`, message: `v${version}`, object: sha, type: 'commit' } })
  if (!tag) throw new ReleaseError(`could not create the tag object v${version} in ${repo}`)
  const made = await api('POST', `/repos/${repo}/git/refs`, { body: { ref: `refs/tags/v${version}`, sha: tag.sha } })
  if (!made) throw new ReleaseError(`could not create refs/tags/v${version} in ${repo}`)
  return 'created'
}

/**
 * The GitHub release for vX.Y.Z: exactly `files` and a SHA256SUMS over them,
 * or — with no files (app-starter) — the notes alone. Uploaded with the token
 * this runs under, GITHUB_TOKEN, because the ops repository's
 * fetch-site-release.sh and fetch-bridge-release.sh refuse any other
 * uploader. Every asset present (a re-run): left alone, the earlier build
 * stands. Some of them: removed, all uploaded again. Anything else: refused,
 * not overwritten.
 *
 * `files` is [{ name, bytes }]; the notes are the body of a release this
 * call creates, and are not rewritten on one that exists.
 */
export async function publishRelease({ repo, version, files = [], notes, api = github }) {
  parseVersion(version)
  const names = files.map((f) => f.name)
  if (names.includes('SHA256SUMS')) throw new ReleaseError('SHA256SUMS is written here, not handed in', 2)
  if (new Set(names).size !== names.length) throw new ReleaseError(`two assets share a name: ${names.join(', ')}`, 2)
  let release = await api('GET', `/repos/${repo}/releases/tags/v${version}`)
  let created = false
  if (release) {
    // A release this workflow did not make — hand-drafted, or by someone
    // else's identity — is not a re-run to continue; it is refused, the same
    // way a foreign asset on an otherwise-matching release is refused below.
    if (release.draft) throw new ReleaseError(`the release v${version} in ${repo} is a draft — refusing to reuse it`)
    if (release.author?.login !== 'github-actions[bot]')
      throw new ReleaseError(`the release v${version} in ${repo} was not made by github-actions[bot] (author: ${release.author?.login ?? 'unknown'}) — refusing to reuse it`)
  } else {
    release = await api('POST', `/repos/${repo}/releases`, { body: { tag_name: `v${version}`, name: `v${version}`, body: notes } })
    if (!release) throw new ReleaseError(`could not create the release v${version} in ${repo}`)
    created = true
  }
  const expected = names.length ? [...names, 'SHA256SUMS'] : []
  const have = new Map((release.assets ?? []).map((a) => [a.name, a]))
  const foreign = [...have.keys()].filter((n) => !expected.includes(n))
  if (foreign.length) throw new ReleaseError(`the release v${version} carries assets this workflow did not make: ${foreign.join(', ')}`)
  if (expected.every((n) => have.has(n))) return created ? 'created' : 'exists'
  for (const a of have.values()) await api('DELETE', `/repos/${repo}/releases/assets/${a.id}`)
  const upload = release.upload_url.replace(/\{.*\}$/, '')
  const put = (n, data) => api('POST', `${upload}?name=${encodeURIComponent(n)}`, { raw: data, base: '' })
  let sums = ''
  for (const f of files) {
    await put(f.name, f.bytes)
    sums += `${createHash('sha256').update(f.bytes).digest('hex')}  ${f.name}\n`
  }
  await put('SHA256SUMS', Buffer.from(sums))
  return 'uploaded'
}

/** A site's release: its one tarball, and a body that only names it. */
export function siteRelease({ repo, version, asset, api = github, bytes = readFileSync(asset) }) {
  return publishRelease({ repo, version, files: [{ name: basename(asset), bytes }], notes: `${repo.split('/')[1]} v${version}`, api })
}

// ── npm ─────────────────────────────────────────────────────────────────

const PACKAGE = /^(@[a-z0-9-]+\/)?[a-z0-9._-]+$/

/**
 * The versions npm lists for a package. The abbreviated document is the one
 * `npm install` reads, and carries each version's tarball URL.
 */
export async function npmVersions(pkg, { registry = 'https://registry.npmjs.org', fetchImpl = globalThis.fetch } = {}) {
  if (!PACKAGE.test(pkg)) throw new ReleaseError(`'${pkg}' is not an npm package name`, 2)
  const res = await fetchImpl(`${registry}/${pkg.replace('/', '%2f')}`, { headers: { accept: 'application/vnd.npm.install-v1+json' } })
  if (res.status === 404) return {}
  if (!res.ok) throw new ReleaseError(`the npm registry answered ${res.status} for ${pkg}`)
  return (await res.json()).versions ?? {}
}

/**
 * npm has two clocks: the metadata lists a version minutes before its tarball
 * is served (seen on 2026-09-22, contracts 3.0.0). `listed` means a publish
 * happened and must not be retried — npm refuses a version twice; `served`
 * means a consumer can install it.
 */
export async function npmState(pkg, version, { registry, fetchImpl = globalThis.fetch } = {}) {
  const versions = await npmVersions(pkg, { registry, fetchImpl })
  const entry = versions[version]
  if (!entry) return 'absent'
  const tarball = await fetchImpl(entry.dist.tarball, { method: 'HEAD' })
  return tarball.ok ? 'served' : 'listed'
}

export async function npmWait(pkg, version, { timeoutMs = 20 * 60 * 1000, intervalMs = 15_000, registry, fetchImpl = globalThis.fetch, sleep = (ms) => new Promise((r) => setTimeout(r, ms)), now = Date.now } = {}) {
  const deadline = now() + timeoutMs
  for (;;) {
    const state = await npmState(pkg, version, { registry, fetchImpl })
    if (state === 'served') return state
    if (now() > deadline) throw new ReleaseError(`${pkg}@${version} is ${state} on npm, not served, after ${Math.round(timeoutMs / 60000)} minutes`)
    await sleep(intervalMs)
  }
}

/**
 * Where a component's release is announced. cloud, console, docs and www ask
 * the ops repository to record and deploy them; bridge asks it to publish to
 * apt; sdk asks app-starter to open the pull request that re-pins it.
 */
export function dispatchTarget(component) {
  if (component === 'bridge') return { repo: 'fleetless/fleetless', event: 'bridge-release' }
  if (component === 'sdk') return { repo: 'fleetless/app-starter', event: 'sdk-release' }
  return { repo: 'fleetless/fleetless', event: 'release' }
}

/**
 * The payload the receiving workflow expects: record-release.yml's header
 * lists the release one, publish-apt.yml reads bridge's `tag`, app-starter's
 * sdk-bump.yml reads sdk's `version`.
 */
export function dispatchPayload(flags, env = process.env) {
  const need = (k) => {
    if (typeof flags[k] !== 'string' || flags[k] === '') throw new ReleaseError(`--${k} <value> is required`, 2)
    return flags[k]
  }
  const needPin = (k) => {
    const v = need(k)
    if (!SEMVER.test(v)) throw new ReleaseError(`no exact @fleetless/contracts pin to check (got '${v}') — is it still in dependencies?`, 2)
    return v
  }
  const needSchema = (k) => {
    const v = need(k)
    if (v !== 'expand' && v !== 'contract') throw new ReleaseError(`--${k} must be expand or contract (got '${v}')`, 2)
    return v
  }
  const component = need('component')
  const version = need('version')
  parseVersion(version)
  const commit = need('commit')
  if (!/^[0-9a-f]{40}$/.test(commit)) throw new ReleaseError(`--commit '${commit}' is not a 40-hex commit`, 2)
  const payload = {
    component,
    version,
    commit,
    run_url: `${env.GITHUB_SERVER_URL}/${env.GITHUB_REPOSITORY}/actions/runs/${env.GITHUB_RUN_ID}`,
  }
  if (component === 'cloud' || component === 'console') {
    payload.ref = need('ref')
    payload.contracts_pin = needPin('contracts-pin')
    payload.contracts_bump = flags['contracts-bump'] === true ? 'true' : 'false'
    if (component === 'cloud') payload.schema = needSchema('schema')
  } else if (component === 'docs' || component === 'www') {
    payload.asset_name = need('asset-name')
    if (component === 'docs') payload.contracts_pin = needPin('contracts-pin')
  } else if (component === 'bridge') {
    payload.tag = `v${version}`
  } else if (component !== 'sdk') throw new ReleaseError(`--component must be cloud, console, docs, www, bridge or sdk (got '${component}')`, 2)
  return payload
}

export async function releasePr({ repo, version, branch, findOnly = false, api = github }) {
  const owner = repo.split('/')[0]
  const open = await api('GET', `/repos/${repo}/pulls?state=open&head=${encodeURIComponent(`${owner}:${branch}`)}`)
  if (open && open.length) return open[0].number
  if (findOnly) return null
  const pr = await api('POST', `/repos/${repo}/pulls`, {
    body: {
      title: `chore(release): ${version}`,
      head: branch,
      base: 'main',
      body: `The release commit for ${version}: the changelog, and the version files where there are any. Opened by the release workflow; it merges itself once the required checks pass.`,
    },
  })
  return pr.number
}

/**
 * Wait for the required checks, then merge with a rebase and answer the
 * commit main now carries. A failed check or a PR that fell behind main is
 * refused at once — a release that waits thirty minutes for a red check is a
 * release nobody is watching.
 */
export async function mergePr({ repo, number, timeoutMs = 30 * 60 * 1000, intervalMs = 20_000, api = github, sleep = (ms) => new Promise((r) => setTimeout(r, ms)), now = Date.now }) {
  const deadline = now() + timeoutMs
  for (;;) {
    const pr = await api('GET', `/repos/${repo}/pulls/${number}`)
    if (!pr) throw new ReleaseError(`no pull request #${number} in ${repo}`)
    if (pr.merged) return pr.merge_commit_sha
    if (pr.state === 'closed') throw new ReleaseError(`the release PR #${number} was closed without merging`)
    if (pr.mergeable_state === 'clean') {
      const result = await api('PUT', `/repos/${repo}/pulls/${number}/merge`, { body: { merge_method: 'rebase' } })
      if (result && result.sha) return result.sha
      // The PUT answered without a sha: a stale re-GET's merge_commit_sha can
      // still be the pre-merge test-merge commit, which create-tag would tag
      // forever and verify-dispatch would refuse as not on main. Trust it
      // only once the PR itself reports merged.
      const merged = await api('GET', `/repos/${repo}/pulls/${number}`)
      if (!merged || merged.merged !== true) throw new ReleaseError(`the release PR #${number}'s merge did not report a commit sha, and a re-read does not show it merged`)
      return merged.merge_commit_sha
    }
    if (pr.mergeable_state === 'behind' || pr.mergeable_state === 'dirty')
      throw new ReleaseError(`the release PR #${number} is ${pr.mergeable_state}: main moved; run Release again`)
    const runs = await api('GET', `/repos/${repo}/commits/${pr.head.sha}/check-runs`)
    const failed = (runs?.check_runs ?? []).filter((r) => r.status === 'completed' && ['failure', 'cancelled', 'timed_out'].includes(r.conclusion))
    if (failed.length) throw new ReleaseError(`the release PR #${number}'s check ${failed.map((r) => r.name).join(', ')} failed`)
    if (now() > deadline) throw new ReleaseError(`the release PR #${number} was not mergeable within ${Math.round(timeoutMs / 60000)} minutes (state: ${pr.mergeable_state})`)
    await sleep(intervalMs)
  }
}

// ── CLI ─────────────────────────────────────────────────────────────────

function parseArgs(argv) {
  const [command, ...rest] = argv
  const flags = {}
  for (let i = 0; i < rest.length; i++) {
    if (!rest[i].startsWith('--')) throw new ReleaseError(`unexpected argument '${rest[i]}'`, 2)
    const key = rest[i].slice(2)
    const next = rest[i + 1]
    if (next === undefined || next.startsWith('--')) flags[key] = true
    else {
      flags[key] = next
      i++
    }
  }
  return { command, flags }
}

function need(flags, key) {
  if (typeof flags[key] !== 'string' || flags[key] === '') throw new ReleaseError(`--${key} <value> is required`, 2)
  return flags[key]
}

function gate(message, warn) {
  if (message === null) return ''
  if (warn) {
    console.log(`::warning::${message.replace(/\n/g, '%0A')}`)
    return ''
  }
  throw new ReleaseError(message)
}

function repository() {
  const repo = process.env.GITHUB_REPOSITORY
  if (!repo) throw new ReleaseError('GITHUB_REPOSITORY is not set', 2)
  return repo
}

export async function main(argv) {
  const { command, flags } = parseArgs(argv)
  switch (command) {
    case 'next': {
      const current = currentTag()
      if (flags['require-tag'] === true) requireTag(current)
      const commits = commitsSince(current)
      const { version, bump } = nextVersion({ current: current ? current.slice(1) : null, commits, bump: flags.bump ?? 'auto', first: flags.first ?? '1.0.0' })
      console.error(`${current ?? 'no tag yet'} + ${commits.length} commit(s) → ${version} (${bump})`)
      return version
    }
    case 'prerelease': {
      const pkg = need(flags, 'package')
      const current = currentTag()
      const commits = commitsSince(current)
      const { version: next } = nextVersion({ current: current ? current.slice(1) : null, commits, bump: flags.bump ?? 'auto' })
      const version = prereleaseVersion(next, Object.keys(await npmVersions(pkg)))
      console.error(`${current ?? 'no tag yet'} + ${commits.length} commit(s) → ${version}`)
      return version
    }
    case 'changelog-add': {
      const file = need(flags, 'file')
      writeFileSync(file, addUnreleased(readFileSync(file, 'utf8'), need(flags, 'line')))
      return ''
    }
    case 'changelog-section':
      return changelogSection(readFileSync(need(flags, 'file'), 'utf8'), need(flags, 'version'))
    case 'tag-at-head':
      return tagAtHead() ?? ''
    case 'release-commit':
      return releaseCommitOnMain(need(flags, 'version')) ?? ''
    case 'rotate': {
      const file = need(flags, 'file')
      const text = readFileSync(file, 'utf8')
      writeFileSync(file, rotateChangelog(text, { version: need(flags, 'version'), date: need(flags, 'date'), emptyLine: typeof flags['empty-line'] === 'string' ? flags['empty-line'] : null }))
      return ''
    }
    case 'pin-gate': {
      const manifest = JSON.parse(readFileSync(need(flags, 'manifest'), 'utf8'))
      return gate(checkPin(manifest, { component: need(flags, 'component'), pin: need(flags, 'pin'), bump: flags.bump === true }), flags.warn === true)
    }
    case 'prompt-gate': {
      const dir = need(flags, 'tools-dir')
      const sources = readdirSync(dir).filter((f) => f.endsWith('.ts')).map((f) => readFileSync(join(dir, f), 'utf8'))
      return gate(promptGate(readFileSync(need(flags, 'prompt'), 'utf8'), sources), flags.warn === true)
    }
    case 'recipe-gate':
      return gate(recipeGate(readFileSync(need(flags, 'recipe'), 'utf8'), readFileSync(need(flags, 'prompt'), 'utf8')), flags.warn === true)
    case 'create-tag': {
      const sha = need(flags, 'sha')
      if (!/^[0-9a-f]{40}$/.test(sha)) throw new ReleaseError(`--sha '${sha}' is not a 40-hex commit`, 2)
      const version = need(flags, 'version')
      const outcome = await createTag({ repo: repository(), version, sha })
      console.error(`v${version}: ${outcome} at ${sha}`)
      return ''
    }
    case 'site-release': {
      const version = need(flags, 'version')
      const outcome = await siteRelease({ repo: repository(), version, asset: need(flags, 'asset') })
      console.error(`release v${version}: assets ${outcome}`)
      return ''
    }
    case 'github-release': {
      const version = need(flags, 'version')
      // --dir: every regular file in it except a SHA256SUMS, which is
      // written again over exactly what goes up.
      const dir = typeof flags.dir === 'string' ? flags.dir : null
      const files = dir
        ? readdirSync(dir, { withFileTypes: true })
            .filter((e) => e.isFile() && e.name !== 'SHA256SUMS')
            .map((e) => e.name)
            .sort()
            .map((name) => ({ name, bytes: readFileSync(join(dir, name)) }))
        : []
      if (dir && files.length === 0) throw new ReleaseError(`${dir} holds no file to release`)
      const notes = typeof flags['notes-file'] === 'string' ? readFileSync(flags['notes-file'], 'utf8').trim() : `${repository().split('/')[1]} v${version}`
      const outcome = await publishRelease({ repo: repository(), version, files, notes })
      console.error(`release v${version}: ${files.length ? `${files.length} file(s) + SHA256SUMS` : 'notes only'}, ${outcome}`)
      return ''
    }
    case 'npm-state':
      return npmState(need(flags, 'package'), need(flags, 'version'))
    case 'npm-wait': {
      const minutes = Number(flags['timeout-minutes'] ?? 20)
      return npmWait(need(flags, 'package'), need(flags, 'version'), { timeoutMs: minutes * 60 * 1000 })
    }
    case 'dispatch': {
      const payload = dispatchPayload(flags)
      const { repo, event } = dispatchTarget(payload.component)
      await github('POST', `/repos/${repo}/dispatches`, { body: { event_type: event, client_payload: payload } })
      console.error(`dispatched ${event} to ${repo}: ${JSON.stringify(payload)}`)
      return ''
    }
    case 'release-pr': {
      const number = await releasePr({ repo: repository(), version: need(flags, 'version'), branch: need(flags, 'branch'), findOnly: flags['find-only'] === true })
      return number === null ? '' : String(number)
    }
    case 'merge-pr': {
      const number = need(flags, 'number')
      if (!/^\d+$/.test(number)) throw new ReleaseError(`--number '${number}' is not a plain number`, 2)
      const minutes = Number(flags['timeout-minutes'] ?? 30)
      return mergePr({ repo: repository(), number, timeoutMs: minutes * 60 * 1000 })
    }
    default:
      throw new ReleaseError(`unknown command '${command ?? ''}'`, 2)
  }
}

const invokedDirectly = (() => {
  try {
    return process.argv[1] !== undefined && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))
  } catch {
    return false
  }
})()
if (invokedDirectly) {
  main(process.argv.slice(2)).then(
    (out) => {
      if (out) console.log(out)
    },
    (error) => {
      if (error instanceof ReleaseError) {
        console.error(`release: ${error.message}`)
        process.exit(error.code)
      }
      throw error
    },
  )
}
