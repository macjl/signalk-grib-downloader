'use strict'

// Smoketests for the plugin host's internet-state handling: the auto
// scheduler must pause behind a captive portal exactly like offline —
// previously 'captive' was rejected and fell back to 'unknown', which
// behaves as 'online', so automatic downloads started against the
// portal's login page. No network is touched: the source is kept up to
// date on disk whenever a live 'online' tick could otherwise fire.

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const makePlugin = require('../dist/index.js')
const { probeRunStamp, fetchFingerprint } = require('../dist/scheduler.js')

const BBOX = { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
const SOURCE = { model: 'icon-eu', autoDownload: true }
const INTERNET_STATE_PATH = 'network.internet.state'

const tmpDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'grib-host-'))

// Mock Signal K server with just the surface the plugin host touches.
const makeServer = (dataDir, { seed } = {}) => {
  // These scheduling fixtures already have catalogue access; onboarding is
  // exercised separately through the actual Access Requests HTTP lifecycle.
  fs.writeFileSync(path.join(dataDir, 'catalogue-token'), 'scheduler-test-token\n')
  const stateHandlers = []
  const pluginStatuses = []
  const debugs = []
  return {
    pluginStatuses,
    debugs,
    getSelfPath: (p) => (p === INTERNET_STATE_PATH ? seed : undefined),
    streambundle: {
      getSelfStream: () => ({
        onValue: (fn) => { stateHandlers.push(fn); return () => {} },
      }),
    },
    setPluginStatus: (msg) => pluginStatuses.push(msg),
    debug: (msg) => debugs.push(String(msg)),
    getDataDirPath: () => dataDir,
    publishInternetState: (v) => stateHandlers.forEach(fn => fn(v)),
  }
}

const makeRouter = () => {
  const routes = {}
  const router = {
    get: (p, h) => { routes[`GET ${p}`] = h },
    put: (p, h) => { routes[`PUT ${p}`] = h },
    post: (p, h) => { routes[`POST ${p}`] = h },
    delete: (p, h) => { routes[`DELETE ${p}`] = h },
  }
  return { router, call: (method, p, request = {}) => {
    let payload
    const res = {
      status: () => res,
      json: (v) => { payload = v },
    }
    const result = routes[`${method} ${p}`](request, res)
    return result?.then ? result.then(() => payload) : payload
  } }
}

// Durable receipts make the source current without local GRIB files.
const writeCommittedReceipt = (_root, dataDir) => {
  fs.mkdirSync(path.join(dataDir, 'catalogue-publications'), { recursive: true })
  fs.writeFileSync(path.join(dataDir, 'catalogue-publications/icon-eu.json'), JSON.stringify({
    endpoint: 'http://127.0.0.1:3000', records: [{ stamp: probeRunStamp('icon-eu'),
      params: fetchFingerprint(SOURCE, BBOX), datasetId: 'test-dataset', version: 'a'.repeat(64) }]
  }))
}

const settle = (ms = 50) => new Promise((resolve) => setTimeout(resolve, ms))

const startPlugin = ({ seed, prepare } = {}) => {
  const dataDir = tmpDir()
  const gribsRoot = path.join(dataDir, 'downloads')
  fs.writeFileSync(path.join(dataDir, 'settings.json'),
    JSON.stringify({ bbox: BBOX, sources: [SOURCE] }))
  if (prepare) prepare(undefined, dataDir)
  const server = makeServer(dataDir, { seed })
  const plugin = makePlugin(server)
  const { router, call } = makeRouter()
  plugin.registerWithRouter(router)
  plugin.start({})
  return { plugin, server, call, gribsRoot, dataDir, stop: () => plugin.stop() }
}

test('retired shared-directory settings cannot select a local-only downloader', async t => {
  const dataDir = tmpDir(), retiredRoot = tmpDir()
  t.after(() => fs.rmSync(dataDir, { recursive: true, force: true }))
  t.after(() => fs.rmSync(retiredRoot, { recursive: true, force: true }))
  fs.writeFileSync(path.join(dataDir, 'settings.json'), JSON.stringify({ sources: [SOURCE] }))
  fs.mkdirSync(path.join(retiredRoot, 'icon-eu'), { recursive: true })
  fs.writeFileSync(path.join(retiredRoot, 'icon-eu', `.run-${probeRunStamp('icon-eu')}.complete`), '{}')
  const plugin = makePlugin(makeServer(dataDir, { seed: 'offline' }))
  const { router, call } = makeRouter()
  plugin.registerWithRouter(router)
  t.after(() => plugin.stop())
  assert.strictEqual(plugin.schema().properties.gribsRoot, undefined)
  plugin.start({ gribsRoot: retiredRoot, inputMode: 'local' })
  assert.strictEqual(call('GET', '/status').sources[0].lastRun, null,
    'retired filesystem completion markers are never read')
  assert.ok(fs.existsSync(path.join(retiredRoot, 'icon-eu')))
})

test('entering captive pauses the auto scheduler; leaving it catches up', async () => {
  // Source up to date from the start: every 'online'-like tick finds
  // nothing to download, so no network is ever touched.
  const t = startPlugin({ prepare: writeCommittedReceipt })
  try {
    // Sanity: online-like state schedules the next auto check.
    await settle()
    let status = t.call('GET', '/status')
    assert.strictEqual(status.internetState, 'unknown')
    assert.notStrictEqual(status.nextAutoTickAt, null)

    // Regression transition online/unknown → captive: the publisher
    // reports a captive portal. Previously rejected → 'unknown' →
    // treated as online → downloads could start against the portal.
    t.server.publishInternetState('captive')
    await settle()
    status = t.call('GET', '/status')
    assert.strictEqual(status.internetState, 'captive') // accepted, not coerced
    assert.strictEqual(status.nextAutoTickAt, null) // timer cancelled
    assert.match(t.server.pluginStatuses.at(-1), /captive portal — auto paused/)

    // Transition captive → online: catch-up check runs and scheduling
    // resumes (no download — the source is up to date).
    t.server.publishInternetState('online')
    await settle()
    status = t.call('GET', '/status')
    assert.strictEqual(status.internetState, 'online')
    assert.notStrictEqual(status.nextAutoTickAt, null)
    assert.doesNotMatch(t.server.pluginStatuses.at(-1), /captive|offline|metered/)
    assert.strictEqual(status.sources[0].running, false)
  } finally {
    t.stop()
  }
})

test('no automatic download starts behind a captive portal', async () => {
  // Seeded captive (portal captured before the plugin started), source
  // behind schedule — the old code mapped this to 'online' and fetched.
  const t = startPlugin({ seed: 'captive' })
  try {
    await settle()
    const status = t.call('GET', '/status')
    assert.strictEqual(status.internetState, 'captive')
    assert.strictEqual(status.nextAutoTickAt, null)
    assert.strictEqual(status.sources[0].running, false)
    assert.strictEqual(status.sources[0].lastOutcome, null)
    assert.ok(t.server.debugs.every(m => !m.includes('auto: ')))
    assert.ok(t.server.pluginStatuses.every(m => !m.includes('downloading')))

    // Leaving the portal with the source since brought up to date:
    // the catch-up tick runs, downloads nothing, and reschedules.
    writeCommittedReceipt(t.gribsRoot, t.dataDir)
    t.server.publishInternetState('online')
    await settle()
    const after = t.call('GET', '/status')
    assert.strictEqual(after.internetState, 'online')
    assert.strictEqual(after.sources[0].lastOutcome, null) // no fetch happened
    assert.notStrictEqual(after.nextAutoTickAt, null)
  } finally {
    t.stop()
  }
})

test('settings accept independent manual GFS and GFS-Wave sources and reject unsupported wave resolution', async t => {
  const dataDir = tmpDir(), server = makeServer(dataDir, { seed: 'offline' })
  const plugin = makePlugin(server), { router, call } = makeRouter()
  t.after(async () => { await plugin.stop(); fs.rmSync(dataDir, { recursive: true, force: true }) })
  plugin.registerWithRouter(router)
  plugin.start({ sources: [] })
  const sources = [{ model: 'gfs', autoDownload: false }, { model: 'gfs-wave', hours: 6, autoDownload: false }]
  assert.deepStrictEqual(call('PUT', '/settings', { body: { sources, bbox: BBOX } }), { ok: true })
  assert.deepStrictEqual(call('GET', '/status').sources.map(s => s.name), ['gfs-0p25', 'gfs-wave-0p25'])
  assert.strictEqual(call('GET', '/status').nextAutoTickAt, null)
  assert.match(call('PUT', '/settings', { body: { sources: [{ model: 'gfs-wave', resolution: '0p50' }] } }).error, /only 0p25/)
  assert.deepStrictEqual(call('GET', '/settings').sources, sources)
  call('PUT', '/settings', { body: { sources: [sources[0]], bbox: BBOX } })
  // Deletion of a removed source must still parse its hyphenated model ID.
  assert.deepStrictEqual(await call('DELETE', '/source-data/:name', { params: { name: 'gfs-wave-0p25' } }), { ok: true })
})
