// SPDX-License-Identifier: Apache-2.0
// The bridge's own release commit. The shared cases are release.test.mjs's.
//
//   node --test '.github/release/*.test.mjs'

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { debianDate, debianEntry, releaseNotes, setInitVersion, setPackageXml } from './bridge-version.mjs'

const CHANGELOG = [
  'ros-@ROS_DISTRO@-fleetless-bridge (4.0.0-0@DEB_CODENAME@) @DEB_CODENAME@; urgency=medium',
  '',
  '  * **Low-bandwidth mode.** Prose.',
  '',
  ' -- Maintainer Name <maintainer@example.org>  Mon, 21 Sep 2026 18:00:00 +0200',
  '',
  'ros-@ROS_DISTRO@-fleetless-bridge (3.1.0-0@DEB_CODENAME@) @DEB_CODENAME@; urgency=medium',
  '',
  '  * Older.',
  '',
  ' -- Maintainer Name <maintainer@example.org>  Tue, 08 Sep 2026 12:00:00 +0200',
  '',
].join('\n')
const DATE = 'Wed, 23 Sep 2026 17:00:00 +0000'

test('setInitVersion: the one __version__ line, nothing else', () => {
  const text = '"""Doc mentioning __version__."""\n\n__version__ = "4.0.0"\n'
  assert.equal(setInitVersion(text, '4.0.1'), '"""Doc mentioning __version__."""\n\n__version__ = "4.0.1"\n')
  assert.throws(() => setInitVersion('nothing\n', '4.0.1'), /no `__version__/)
})
test('setPackageXml: the package\'s <version>, not the XML declaration', () => {
  const text = '<?xml version="1.0"?>\n<package format="3">\n  <version>4.0.0</version>\n</package>\n'
  assert.equal(setPackageXml(text, '4.0.1'), '<?xml version="1.0"?>\n<package format="3">\n  <version>4.0.1</version>\n</package>\n')
})
test('debianDate: RFC 2822 in UTC', () => {
  assert.equal(debianDate(new Date(Date.UTC(2026, 8, 23, 17, 0, 0))), DATE)
})
test('debianEntry: a new top entry, one bullet per subject, the maintainer copied', () => {
  const out = debianEntry(CHANGELOG, { version: '4.0.1', subjects: ['fix(sampling): a non-finite float becomes null on the wire', 'feat: low-bandwidth mode'], date: DATE })
  assert.equal(
    out.split('\n').slice(0, 8).join('\n'),
    [
      'ros-@ROS_DISTRO@-fleetless-bridge (4.0.1-0@DEB_CODENAME@) @DEB_CODENAME@; urgency=medium',
      '',
      '  * fix(sampling): a non-finite float becomes null on the wire',
      '  * feat: low-bandwidth mode',
      '',
      ` -- Maintainer Name <maintainer@example.org>  ${DATE}`,
      '',
      'ros-@ROS_DISTRO@-fleetless-bridge (4.0.0-0@DEB_CODENAME@) @DEB_CODENAME@; urgency=medium',
    ].join('\n'),
  )
  assert.ok(out.endsWith(CHANGELOG))
})
test('debianEntry: a release commit in the range is not listed', () => {
  const out = debianEntry(CHANGELOG, { version: '4.0.1', subjects: ['chore(release): 4.0.0', 'fix: a'], date: DATE })
  assert.doesNotMatch(out.split(' -- ')[0], /chore\(release\)/)
})
test('debianEntry: already at the version (a re-run): unchanged', () => {
  const once = debianEntry(CHANGELOG, { version: '4.0.1', subjects: ['fix: a'], date: DATE })
  assert.equal(debianEntry(once, { version: '4.0.1', subjects: ['fix: a', 'fix: b'], date: DATE }), once)
})
test('debianEntry: every housekeeping type is left out, a fix is kept', () => {
  const subjects = [
    'chore: bump a dev dependency',
    'ci: run on ubuntu-latest',
    'build(deb): drop a stale dependency',
    'test(camera-sources): drop a bound',
    'style: reformat',
    'fix(sampling): a non-finite float becomes null on the wire',
  ]
  const bullets = debianEntry(CHANGELOG, { version: '4.0.1', subjects, date: DATE }).split(' -- ')[0]
  assert.match(bullets, /^ {2}\* fix\(sampling\): /m)
  for (const type of ['chore', 'ci', 'build', 'test', 'style']) assert.doesNotMatch(bullets, new RegExp(`\\* ${type}`))
})
test('debianEntry: the release scope is left out whatever its type', () => {
  const subjects = ['feat(release): a Release button', 'fix(release): the review\'s four minors', 'feat(assets): a per-robot store']
  const bullets = debianEntry(CHANGELOG, { version: '4.0.1', subjects, date: DATE }).split(' -- ')[0]
  assert.doesNotMatch(bullets, /\(release\)/)
  assert.match(bullets, /\* feat\(assets\): a per-robot store/)
})
test('debianEntry: nothing user-facing is refused', () => {
  assert.throws(
    () => debianEntry(CHANGELOG, { version: '4.0.1', subjects: ['chore(release): 4.0.0', 'ci: pin the runner'], date: DATE }),
    /nothing for apt to announce/,
  )
})
test('debianEntry: a version line without a Debian revision keeps the whole version', () => {
  const text = CHANGELOG.replace('(4.0.0-0@DEB_CODENAME@)', '(4.0.0)')
  const out = debianEntry(text, { version: '4.0.1', subjects: ['fix: a'], date: DATE })
  assert.equal(out.split('\n')[0], 'ros-@ROS_DISTRO@-fleetless-bridge (4.0.1) @DEB_CODENAME@; urgency=medium')
})
test('releaseNotes: the top entry\'s bullets, placeholders named', () => {
  const text = 'ros-@ROS_DISTRO@-fleetless-bridge (4.0.1-0@DEB_CODENAME@) x\n\n  * On @DEB_CODENAME@ for @ROS_DISTRO@,\n    wrapped.\n  * Two.\n\n -- M <m@example.org>  d\n\nolder\n'
  assert.equal(releaseNotes(text), '* On <codename> for <distro>,\n  wrapped.\n* Two.')
})
