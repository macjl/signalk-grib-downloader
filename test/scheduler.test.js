'use strict'

const { test } = require('node:test')
const assert = require('node:assert')

const {
  expectedRunStamp, probeRunStamp, nextPublishAt, candidateRuns, publicationWindow,
  runStamp, downloaderSourceConfig, sourceDirName, MODEL_MAX_HOURS, MODEL_SCHEDULES,
} = require('../dist/scheduler.js')

test('expectedRunStamp uses upper bounds, including per-cycle AROME delays', () => {
  assert.strictEqual(expectedRunStamp('gfs', new Date('2026-06-11T11:00:00Z')), '20260611T00')
  // At 12:00 UTC → 06:30 → cycle 06
  assert.strictEqual(expectedRunStamp('gfs', new Date('2026-06-11T12:00:00Z')), '20260611T06')
  // Day rollover: at 03:00 UTC → 21:30 previous day → cycle 18
  assert.strictEqual(expectedRunStamp('gfs', new Date('2026-06-11T03:00:00Z')), '20260610T18')
  assert.strictEqual(expectedRunStamp('arome', new Date('2026-10-19T08:45:00Z')), '20261019T03')
  assert.strictEqual(expectedRunStamp('arome', new Date('2026-10-19T11:49:59Z')), '20261019T03')
  assert.strictEqual(expectedRunStamp('arome', new Date('2026-10-19T11:50:00Z')), '20261019T06')
})

test('every configured cycle opens at the lower bound and remains eligible after the upper bound', () => {
  for (const [model, schedule] of Object.entries(MODEL_SCHEDULES)) {
    assert.ok(schedule.probeEveryMinutes > 0)
    for (let minute = 0; minute < 1440; minute += schedule.cycleEveryMinutes) {
      const run = new Date(Date.UTC(2026, 9, 19, 0, minute))
      const stamp = runStamp(run)
      const window = publicationWindow(model, run)
      assert.ok(window.earliestMinutes >= 0 && window.latestMinutes >= window.earliestMinutes)
      const opens = run.getTime() + window.earliestMinutes * 60_000
      const closes = run.getTime() + window.latestMinutes * 60_000
      assert.ok(!candidateRuns(model, new Date(opens - 1)).some(r => runStamp(r) === stamp))
      assert.strictEqual(probeRunStamp(model, new Date(opens)), stamp)
      assert.notStrictEqual(expectedRunStamp(model, new Date(closes - 1)), stamp)
      assert.strictEqual(expectedRunStamp(model, new Date(closes)), stamp)
      assert.ok(candidateRuns(model, new Date(closes + 60_000)).some(r => runStamp(r) === stamp))
    }
  }
})

test('nextPublishAt follows lower bounds across midnight and skips completed runs', () => {
  assert.strictEqual(nextPublishAt('gfs', new Date('2026-10-19T03:29:59Z')).toISOString(), '2026-10-19T03:30:00.000Z')
  assert.strictEqual(nextPublishAt('gfs', new Date('2026-10-19T03:30:00Z')).toISOString(), '2026-10-19T09:30:00.000Z')
  assert.strictEqual(nextPublishAt('arome', new Date('2026-10-19T09:59:59Z')).toISOString(), '2026-10-19T10:00:00.000Z')
  assert.strictEqual(nextPublishAt('arome', new Date('2026-10-19T10:00:00Z')).toISOString(), '2026-10-19T12:10:00.000Z')
  assert.strictEqual(nextPublishAt('arome', new Date('2026-10-20T00:00:00Z')).toISOString(), '2026-10-20T00:10:00.000Z')
  assert.strictEqual(nextPublishAt('arome', new Date('2026-10-20T00:00:00Z'), '20261019T21').toISOString(), '2026-10-20T01:50:00.000Z')
})

test('candidate windows overlap without losing an older unpublished cycle', () => {
  const runs = candidateRuns('arome', new Date('2026-10-19T12:10:00Z')).map(runStamp)
  assert.deepStrictEqual(runs, ['20261019T09', '20261019T06', '20261019T03', '20261019T00'])
  assert.strictEqual(expectedRunStamp('arome', new Date('2026-10-19T12:10:00Z')), '20261019T06')
})

test('source identity is derived from model and resolution', () => {
  assert.strictEqual(sourceDirName({ model: 'gfs' }), 'gfs-0p25')
  assert.strictEqual(sourceDirName({ model: 'gfs', resolution: '0p50' }), 'gfs-0p50')
  assert.strictEqual(sourceDirName({ model: 'arome' }), 'arome-0025')
  assert.strictEqual(sourceDirName({ model: 'arpege', resolution: '025' }), 'arpege-025')
  assert.strictEqual(sourceDirName({ model: 'icon-eu' }), 'icon-eu')
})

test('gfs source config: steps, bbox, resolution, stable identity', () => {
  const c = downloaderSourceConfig(
    { model: 'gfs', hours: 384 },
    { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
  )
  assert.strictEqual(c.name, 'gfs-0p25')
  assert.strictEqual(c.resolution, '0p25')
  assert.deepStrictEqual(c.steps, { from: 0, to: 384, by: 3 })
  assert.deepStrictEqual(c.bbox, [35, -6, 45, 17])
  assert.strictEqual(c.name, 'gfs-0p25')
  assert.strictEqual(c.directory, undefined)
})

test('hours are capped at the model maximum', () => {
  const c = downloaderSourceConfig({ model: 'icon-eu', hours: 999 })
  assert.deepStrictEqual(c.steps, { from: 0, to: MODEL_MAX_HOURS['icon-eu'], by: 3 })
  assert.strictEqual(c.name, 'icon-eu')
})

test('arome groups follow the requested duration', () => {
  const c24 = downloaderSourceConfig({ model: 'arome', hours: 24 })
  assert.deepStrictEqual(c24.groups, ['00H06H', '07H12H', '13H18H', '19H24H'])
  const cAll = downloaderSourceConfig({ model: 'arome' })
  assert.strictEqual(cAll.groups.length, 9)
  assert.deepStrictEqual(c24.packages, ['SP1'])
})

test('arpege 01 groups cover hourly ranges', () => {
  const c = downloaderSourceConfig({ model: 'arpege', hours: 48 })
  assert.deepStrictEqual(c.groups, ['000H012H', '013H024H', '025H036H', '037H048H'])
})
