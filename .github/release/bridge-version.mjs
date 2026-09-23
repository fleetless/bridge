#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// The bridge's release commit: the three places a version has to agree
// (test/test_packaging.py ties them together), written from one number.
// This file is the bridge's own; release.mjs beside it is the shared one.
//
//   bridge-version.mjs write --version X.Y.Z [--date "RFC 2822"]
//       __version__, package.xml, and a new top entry in debian/changelog.in
//       listing the commit subjects since the last release tag
//   bridge-version.mjs notes
//       the top debian/changelog.in entry, as a GitHub release's body
//
// Exit codes: 0 ok, 1 refused (the reason on stderr), 2 usage.

import { readFileSync, realpathSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { ReleaseError, commitsSince, currentTag, parseVersion } from './release.mjs'

export function setInitVersion(text, version) {
  parseVersion(version)
  const pattern = /^__version__ = "[^"]*"$/m
  if (!pattern.test(text)) throw new ReleaseError('fleetless_bridge/__init__.py has no `__version__ = "…"` line')
  return text.replace(pattern, `__version__ = "${version}"`)
}

export function setPackageXml(text, version) {
  parseVersion(version)
  const pattern = /<version>[^<]*<\/version>/
  if (!pattern.test(text)) throw new ReleaseError('package.xml has no <version> element')
  return text.replace(pattern, `<version>${version}</version>`)
}

/** RFC 2822, which dpkg-parsechangelog requires: "Wed, 23 Sep 2026 17:00:00 +0000". */
export function debianDate(date) {
  return date.toUTCString().replace(/GMT$/, '+0000')
}

/**
 * A new top entry for `version`, in the shape of the ones before it: the same
 * package line, one bullet per commit subject in the range, and the
 * maintainer line of the entry below it. The release writes subjects, not
 * prose: a commit subject is the release note, so it should read like one.
 * A top entry already at `version` (a re-run) returns the text unchanged.
 */
export function debianEntry(text, { version, subjects, date }) {
  parseVersion(version)
  const lines = text.split('\n')
  const head = /^(\S+) \(([^)]+)\) (.*)$/.exec(lines[0])
  if (!head) throw new ReleaseError('debian/changelog.in does not start with an entry line')
  if (head[2].split('-')[0] === version) return text
  const maintainer = /^ -- (.+?>) {2}/m.exec(text)
  if (!maintainer) throw new ReleaseError('debian/changelog.in has no " -- Name <email>  date" line to copy')
  const listed = subjects.filter((s) => !/^chore\(release\):/.test(s))
  if (listed.length === 0) throw new ReleaseError('no commit subjects to list: nothing to release')
  const revision = head[2].slice(head[2].indexOf('-'))
  return [
    `${head[1]} (${version}${revision}) ${head[3]}`,
    '',
    ...listed.map((s) => `  * ${s}`),
    '',
    ` -- ${maintainer[1]}  ${date}`,
    '',
    text,
  ].join('\n')
}

/** The top entry's bullets, placeholders named rather than filled: one release carries every distribution. */
export function releaseNotes(text) {
  const lines = text.split('\n')
  const end = lines.findIndex((l) => l.startsWith(' -- '))
  if (end === -1) throw new ReleaseError('debian/changelog.in has no complete top entry')
  return lines
    .slice(1, end)
    .map((l) => l.replace(/^ {2}/, ''))
    .join('\n')
    .replace(/@ROS_DISTRO@/g, '<distro>')
    .replace(/@DEB_CODENAME@/g, '<codename>')
    .trim()
}

function parseArgs(argv) {
  const [command, ...rest] = argv
  const flags = {}
  for (let i = 0; i < rest.length; i += 2) {
    if (!rest[i].startsWith('--') || rest[i + 1] === undefined) throw new ReleaseError(`unexpected argument '${rest[i]}'`, 2)
    flags[rest[i].slice(2)] = rest[i + 1]
  }
  return { command, flags }
}

export function main(argv) {
  const { command, flags } = parseArgs(argv)
  const changelog = 'debian/changelog.in'
  switch (command) {
    case 'write': {
      const version = flags.version
      if (!version) throw new ReleaseError('--version <X.Y.Z> is required', 2)
      // Oldest first, the order the changes were made in.
      const subjects = commitsSince(currentTag()).map((c) => c.subject).reverse()
      const date = flags.date ?? debianDate(new Date())
      writeFileSync('fleetless_bridge/__init__.py', setInitVersion(readFileSync('fleetless_bridge/__init__.py', 'utf8'), version))
      writeFileSync('package.xml', setPackageXml(readFileSync('package.xml', 'utf8'), version))
      writeFileSync(changelog, debianEntry(readFileSync(changelog, 'utf8'), { version, subjects, date }))
      return ''
    }
    case 'notes':
      return releaseNotes(readFileSync(changelog, 'utf8'))
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
  try {
    const out = main(process.argv.slice(2))
    if (out) console.log(out)
  } catch (error) {
    if (error instanceof ReleaseError) {
      console.error(`bridge-version: ${error.message}`)
      process.exit(error.code)
    }
    throw error
  }
}
