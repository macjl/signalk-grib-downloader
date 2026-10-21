'use strict'

// Smoketests for the internet-aware auto scheduling pieces of the
// orchestrator: the offline gate on tick() and the nextTickAt() schedule
// computation. No network is touched — a live 'online' tick would fetch.

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const { Orchestrator } = require('../dist/orchestrator.js')
const { probeRunStamp, fetchFingerprint, nextPublishAt } = require('../dist/scheduler.js')

const tmpRoot = () => fs.mkdtempSync(path.join(os.tmpdir(), 'grib-orch-'))
// Scheduling tests use a producer double; HTTP publication is tested separately.
const publisher = () => {
  let latest = null
  return {
    latest: () => latest, complete: (stamp, params) => { latest = { stamp, params } },
    reference: () => latest ? { datasetId: 'fixture', version: 'a'.repeat(64) } : null,
    pendingStamp: () => null, check: async () => {}, verify: async () => true,
    publish: async (_source, plan, params) => { latest = { stamp: plan.stamp, params } },
  }
}

test('auto tick is suppressed while offline', async () => {
  const logs = []
  const orch = new Orchestrator(
    [{ model: 'icon-eu', autoDownload: true }],
    publisher(),
    (msg) => logs.push(msg)
  )
  // A live tick would log "auto: 1 source(s) behind" and hit the network.
  await orch.tick('offline')
  assert.deepStrictEqual(logs, [])
  assert.strictEqual(orch.status()[0].lastOutcome, null)
})

// Regression: signalk-internet publishes 'captive' behind a captive
// portal. It used to fall back to 'unknown', which behaves as 'online' —
// so automatic downloads started, fetching the portal's login page.
test('auto tick is suppressed behind a captive portal', async () => {
  const logs = []
  const orch = new Orchestrator(
    [{ model: 'icon-eu', autoDownload: true }],
    publisher(),
    (msg) => logs.push(msg)
  )
  await orch.tick('captive')
  assert.deepStrictEqual(logs, [])
  assert.strictEqual(orch.status()[0].lastOutcome, null)
})

test('nextTickAt starts missing data immediately and waits for the next lower bound once current', () => {
  const root = tmpRoot()
  const setting = { model: 'icon-eu', autoDownload: true }
  const catalogue = publisher()
  const orch = new Orchestrator([setting], catalogue, () => {})

  // No data on disk: do not wait for an upper bound or a retry interval.
  const before = Date.now()
  const retry = orch.nextTickAt(setting)
  assert.ok(retry.getTime() >= before && retry.getTime() <= Date.now())

  catalogue.complete(probeRunStamp('icon-eu'), fetchFingerprint(setting))

  const scheduled = orch.nextTickAt(setting)
  const expected = nextPublishAt('icon-eu').getTime()
  assert.ok(Math.abs(scheduled.getTime() - expected) < 2000)
})

function publicationFixture(t) {
  const root = tmpRoot()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  let now = Date.parse('2026-10-19T10:00:00Z')
  t.mock.method(Date, 'now', () => now)
  const source = { model: 'arome', resolution: '0025', autoDownload: true }
  const catalogue = publisher()
  const complete = stamp => catalogue.complete(stamp, fetchFingerprint(source))
  complete('20261019T03')
  const requests = []
  let available = false
  t.mock.method(global, 'fetch', async (url, options) => {
    requests.push({ url, method: options.method ?? 'GET' })
    return available ? new Response(Buffer.from('GRIB fixture')) : new Response(null, { status: 404 })
  })
  const orch = new Orchestrator([source], catalogue, () => {}, () => {})
  return {
    orch, source, requests, complete,
    at: time => { now = Date.parse(`2026-10-19T${time}Z`) },
    publish: () => { available = true },
  }
}

test('a catalogue publisher is required even for manual downloads', () => {
  assert.throws(() => new Orchestrator([], undefined, () => {}), /publisher is required/)
})

test('probes start at the lower bound and keep the same cadence across the lateness threshold', async t => {
  const f = publicationFixture(t)
  f.at('09:59:59')
  assert.strictEqual(f.orch.nextTickAt(f.source).toISOString(), '2026-10-19T10:00:00.000Z')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 0)
  f.at('10:00:00')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 1)
  assert.strictEqual(f.orch.status()[0].upToDate, true)
  assert.strictEqual(f.orch.status()[0].pendingRun, '20261019T06')
  assert.strictEqual(f.orch.status()[0].publicationLate, false)
  f.at('10:04:00')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 1, 'other scheduler ticks must not re-probe early')
  f.at('10:05:00')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 2)
  f.at('11:49:00')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 3)
  assert.strictEqual(f.orch.nextTickAt(f.source).toISOString(), '2026-10-19T11:54:00.000Z')
  f.at('11:50:00')
  assert.strictEqual(f.orch.status()[0].publicationLate, true)
  assert.strictEqual(f.orch.nextTickAt(f.source).toISOString(), '2026-10-19T11:54:00.000Z')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 3)
  f.at('11:54:00')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 4)
  assert.strictEqual(f.orch.nextTickAt(f.source).toISOString(), '2026-10-19T11:59:00.000Z')
})

test('a successful in-window download stops probing until the next cycle', async t => {
  const f = publicationFixture(t)
  await f.orch.tick()
  f.publish()
  f.at('10:05:00')
  await f.orch.tick()
  assert.strictEqual(f.orch.lastRunStamp(f.source), '20261019T06')
  assert.strictEqual(f.orch.status()[0].lastOutcome, 'downloaded')
  assert.strictEqual(f.orch.status()[0].pendingRun, null)
  assert.strictEqual(f.orch.nextTickAt(f.source).toISOString(), '2026-10-19T12:10:00.000Z')
  const count = f.requests.length
  f.at('11:55:00')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, count)
  assert.strictEqual(f.orch.status()[0].publicationLate, false)
})

test('new windows take precedence over a long override interval, without postponing existing retries', async t => {
  const f = publicationFixture(t)
  f.at('12:08:00')
  const timing = { intervalMinutes: 60 }
  await f.orch.tick('online', timing)
  assert.strictEqual(f.orch.nextTickAt(f.source, timing).toISOString(), '2026-10-19T12:10:00.000Z')
  f.at('12:09:00')
  await f.orch.tick('online', timing)
  assert.strictEqual(f.requests.length, 1)
  f.at('12:10:00')
  await f.orch.tick('online', timing)
  assert.strictEqual(f.requests.length, 3, 'probe the new 09 cycle, then the still-missing 06 cycle')
})

test('metered probes retain their scaled interval before and after the upper bound', async t => {
  const f = publicationFixture(t)
  const timing = { scale: 3 }
  f.at('11:45:00')
  await f.orch.tick('metered', timing)
  f.at('11:50:00')
  assert.strictEqual(f.orch.status()[0].publicationLate, true)
  assert.strictEqual(f.orch.nextTickAt(f.source, timing).toISOString(), '2026-10-19T12:00:00.000Z')
  await f.orch.tick('metered', timing)
  assert.strictEqual(f.requests.length, 1)
  f.at('12:00:00')
  await f.orch.tick('metered', timing)
  assert.strictEqual(f.requests.length, 2)
})

test('a newer complete cycle supersedes older missing cycles and offline checks do not advance probes', async t => {
  const f = publicationFixture(t)
  await f.orch.tick('offline')
  await f.orch.tick('captive')
  assert.strictEqual(f.requests.length, 0)
  await f.orch.tick('online')
  assert.strictEqual(f.requests.length, 1)
  f.at('12:10:00')
  f.complete('20261019T09')
  await f.orch.tick()
  assert.strictEqual(f.requests.length, 1)
  assert.strictEqual(f.orch.status()[0].pendingRun, null)
  assert.strictEqual(f.orch.status()[0].publicationLate, false)
})

test('approving catalogue access retries a failed source immediately instead of waiting for its probe interval', async () => {
  let attempts = 0
  const catalogue = publisher()
  catalogue.check = async () => { attempts++; throw new Error('HTTP 401') }
  const orch = new Orchestrator([{ model: 'icon-eu', autoDownload: true }], catalogue, () => {})
  await orch.tick()
  assert.equal(attempts, 1)
  await orch.tick()
  assert.equal(attempts, 1)
  orch.retryAfterAccessApproval()
  await orch.tick()
  assert.equal(attempts, 2)
  await orch.stop()
})
