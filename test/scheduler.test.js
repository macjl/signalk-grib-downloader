'use strict'

const { test } = require('node:test')
const assert = require('node:assert')

const { expectedRunStamp, downloaderSourceConfig, sourceDirName, MODEL_MAX_HOURS } = require('../dist/scheduler.js')

test('expectedRunStamp respects cadence and publication delay', () => {
  // GFS: cadence 6h, delay 5.5h. At 11:00 UTC → 11:00-5:30 = 05:30 → cycle 00
  assert.strictEqual(expectedRunStamp('gfs', new Date('2026-06-11T11:00:00Z')), '20260611T00')
  // At 12:00 UTC → 06:30 → cycle 06
  assert.strictEqual(expectedRunStamp('gfs', new Date('2026-06-11T12:00:00Z')), '20260611T06')
  // Day rollover: at 03:00 UTC → 21:30 previous day → cycle 18
  assert.strictEqual(expectedRunStamp('gfs', new Date('2026-06-11T03:00:00Z')), '20260610T18')
  // AROME: cadence 3h, delay 2.75h. At 12:00 → 09:15 → cycle 09
  assert.strictEqual(expectedRunStamp('arome', new Date('2026-06-11T12:00:00Z')), '20260611T09')
})

test('source directory name is derived from model and resolution', () => {
  assert.strictEqual(sourceDirName({ model: 'gfs' }), 'gfs-0p25')
  assert.strictEqual(sourceDirName({ model: 'gfs', resolution: '0p50' }), 'gfs-0p50')
  assert.strictEqual(sourceDirName({ model: 'arome' }), 'arome-0025')
  assert.strictEqual(sourceDirName({ model: 'arpege', resolution: '025' }), 'arpege-025')
  assert.strictEqual(sourceDirName({ model: 'icon-eu' }), 'icon-eu')
})

test('gfs source config: steps, bbox, resolution, derived directory', () => {
  const c = downloaderSourceConfig(
    { model: 'gfs', hours: 384 },
    '/data',
    { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
  )
  assert.strictEqual(c.name, 'gfs-0p25')
  assert.strictEqual(c.resolution, '0p25')
  assert.deepStrictEqual(c.steps, { from: 0, to: 384, by: 3 })
  assert.deepStrictEqual(c.bbox, [35, -6, 45, 17])
  assert.strictEqual(c.directory, '/data/gfs-0p25')
})

test('hours are capped at the model maximum', () => {
  const c = downloaderSourceConfig({ model: 'icon-eu', hours: 999 }, '/data')
  assert.deepStrictEqual(c.steps, { from: 0, to: MODEL_MAX_HOURS['icon-eu'], by: 3 })
  assert.strictEqual(c.directory, '/data/icon-eu')
})

test('arome groups follow the requested duration', () => {
  const c24 = downloaderSourceConfig({ model: 'arome', hours: 24 }, '/data')
  assert.deepStrictEqual(c24.groups, ['00H06H', '07H12H', '13H18H', '19H24H'])
  const cAll = downloaderSourceConfig({ model: 'arome' }, '/data')
  assert.strictEqual(cAll.groups.length, 9)
  assert.deepStrictEqual(c24.packages, ['SP1'])
})

test('arpege 01 groups cover hourly ranges', () => {
  const c = downloaderSourceConfig({ model: 'arpege', hours: 48 }, '/data')
  assert.deepStrictEqual(c.groups, ['000H012H', '013H024H', '025H036H', '037H048H'])
})
