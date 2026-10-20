'use strict'

const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')

const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8')
const start = html.indexOf('function sourceStateBadge(')
const end = html.indexOf('function renderSources()', start)

function render(status) {
  const context = {
    statusBySource: { arome: status },
    dirName: () => 'arome',
    schedulerError: null,
    t: { stWaiting: 'awaiting publication', stLate: 'publication late', stUpToDate: 'up to date' },
  }
  vm.runInNewContext(html.slice(start, end), context)
  return context.sourceStateBadge({ model: 'arome' })
}

test('an open probing window is visible even while the previous run is still current', () => {
  assert.match(render({ lastRun: '20261019T03', upToDate: true, pendingRun: '20261019T06' }), /awaiting publication/)
})

test('an unsuccessful probe cannot hide the publication-late indicator', () => {
  for (const lastOutcome of ['up-to-date', 'unavailable']) {
    assert.match(render({ lastRun: '20261019T03', publicationLate: true, pendingRun: '20261019T06', lastOutcome }), /publication late/)
  }
})

test('a completed latest run removes the waiting badge', () => {
  assert.match(render({ lastRun: '20261019T06', upToDate: true, pendingRun: null }), /up to date/)
})

test('late publication is also visible before the first successful download', () => {
  assert.match(render({ lastRun: null, publicationLate: true, lastOutcome: 'unavailable' }), /publication late/)
})
