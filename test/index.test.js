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
const { CataloguePublisher } = require('../dist/catalogue')
const downloader = require('../dist/downloader')
const { probeRunStamp, fetchFingerprint } = require('../dist/scheduler.js')
const { waitFor } = require('./fixtures/http.cjs')

const BBOX = { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
const SOURCE = { model: 'icon-eu', autoDownload: true }
const INTERNET_STATE_PATH = 'network.internet.state'

const tmpDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'grib-host-'))

// Mock Signal K server with just the surface the plugin host touches.
const makeServer = (dataDir, { seed } = {}) => {
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
  plugin.start({ legacyExportEnabled: false })
  return { plugin, server, call, gribsRoot, dataDir, stop: () => plugin.stop() }
}

test('default legacy export does not treat historical files as a standalone acquisition source', async t => {
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
  assert.strictEqual(plugin.schema().properties.legacyExportEnabled.default, true)
  assert.strictEqual(plugin.schema().properties.gribsRoot.default, '~/.signalk/gribs')
  plugin.start({ gribsRoot: retiredRoot, inputMode: 'local' })
  assert.strictEqual(call('GET', '/status').sources[0].lastRun, null,
    'retired filesystem completion markers are never read')
  assert.ok(fs.existsSync(path.join(retiredRoot, 'icon-eu')))
})

test('entering captive pauses the auto scheduler; leaving it catches up', async context => {
  // Source up to date from the start: every 'online'-like tick finds
  // nothing to download, so no network is ever touched.
  const t = startPlugin({ prepare: (root, dataDir) => {
    writeCommittedReceipt(root, dataDir)
    const registry = (globalThis[Symbol.for('macjl.forecast-datasets.catalogues.v1')] ??= new Map())
    const scope = path.dirname(dataDir)
    registry.set(scope, { contractVersion: '0.1', publications: () => ({}) })
    context.after(() => registry.delete(scope))
  } })
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

test('settings enforce the GFS-Wave resolution only for direct downloads', async t => {
  const dataDir = tmpDir(), server = makeServer(dataDir, { seed: 'offline' })
  const plugin = makePlugin(server), { router, call } = makeRouter()
  t.after(async () => { await plugin.stop(); fs.rmSync(dataDir, { recursive: true, force: true }) })
  plugin.registerWithRouter(router)
  plugin.start({ sources: [], remoteServers: [{ id: 'shore', url: 'https://shore.example' }] })
  const sources = [{ model: 'gfs', autoDownload: false }, { model: 'gfs-wave', hours: 6, autoDownload: false }]
  assert.deepStrictEqual(call('PUT', '/settings', { body: { sources, bbox: BBOX } }), { ok: true })
  assert.deepStrictEqual(call('GET', '/status').sources.map(s => s.name), ['gfs-0p25', 'gfs-wave-0p25'])
  assert.strictEqual(call('GET', '/status').nextAutoTickAt, null)
  assert.match(call('PUT', '/settings', { body: { sources: [{ model: 'gfs-wave', resolution: '0p50' }] } }).error, /only 0p25/)
  assert.deepStrictEqual(call('GET', '/settings').sources, sources)
  call('PUT', '/settings', { body: { sources: [sources[0]], bbox: BBOX } })
  // Deletion of a removed source must still parse its hyphenated model ID.
  assert.deepStrictEqual(await call('DELETE', '/source-data/:name', { params: { name: 'gfs-wave-0p25' } }), { ok: true })
  const remote = { ...sources[1], remoteServer: 'shore', remoteModel: 'AROME', resolution: '0025' }
  assert.strictEqual(call('PUT', '/settings', { body: { sources: [remote], bbox: BBOX } }).ok, true)
  assert.deepStrictEqual(call('GET', '/settings').sources, [remote])
})

test('settings save during a transfer and take effect on the next acquisition', async t => {
  const dataDir = tmpDir()
  const { router, call } = makeRouter()
  const plugin = makePlugin(makeServer(dataDir, { seed: 'offline' }))
  const source = { model: 'gfs', resolution: '0p25', hours: 3, autoDownload: false }
  const updated = { ...source, hours: 6, retentionDays: 2 }
  const newArea = { ...BBOX, lonMin: -7 }
  const acquisitions = [], signals = []
  let release
  const blocked = new Promise(resolve => { release = resolve })
  t.mock.method(CataloguePublisher.prototype, 'check', async signal => {
    signals.push(signal)
    if (signals.length === 1) await blocked
    signal.throwIfAborted()
  })
  t.mock.method(downloader, 'runAvailable', async () => true)
  t.mock.method(CataloguePublisher.prototype, 'publish', async (settings, _plan, params) => {
    acquisitions.push({ settings, params })
  })
  t.after(async () => {
    release()
    await plugin.stop()
    fs.rmSync(dataDir, { recursive: true, force: true })
  })
  plugin.registerWithRouter(router)
  plugin.start({ legacyExportEnabled: false, sources: [source], bbox: BBOX })
  const download = () => call('POST', '/download/:name', { params: { name: 'gfs-0p25' } })
  const status = () => call('GET', '/status').sources[0]

  download()
  await waitFor(() => signals.length === 1)
  assert.strictEqual(status().running, true)
  assert.deepStrictEqual(call('PUT', '/settings', { body: { sources: [], bbox: newArea } }), { ok: true })
  assert.deepStrictEqual(call('PUT', '/settings', { body: { sources: [updated], bbox: newArea } }), { ok: true })
  assert.deepStrictEqual(call('GET', '/settings').sources, [updated])
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(path.join(dataDir, 'settings.json'))).bbox, newArea)
  assert.strictEqual(status().running, true)
  assert.strictEqual(signals[0].aborted, false)

  download()
  assert.strictEqual(signals.length, 1)
  release()
  await waitFor(() => !status().running)
  assert.strictEqual(status().lastOutcome, 'downloaded')
  assert.deepStrictEqual(acquisitions[0], { settings: source, params: fetchFingerprint(source, BBOX) })

  download()
  await waitFor(() => !status().running)
  assert.deepStrictEqual(acquisitions[1], { settings: updated, params: fetchFingerprint(updated, newArea) })
})

test('saving settings does not interrupt an existing source deletion', async t => {
  const dataDir = tmpDir()
  const { router, call } = makeRouter()
  const plugin = makePlugin(makeServer(dataDir, { seed: 'offline' }))
  let release, activeSignal
  const blocked = new Promise(resolve => { release = resolve })
  t.mock.method(CataloguePublisher.prototype, 'removeSource', async (_source, signal) => {
    activeSignal = signal
    await blocked
    signal.throwIfAborted()
  })
  t.after(async () => {
    release()
    await plugin.stop()
    fs.rmSync(dataDir, { recursive: true, force: true })
  })
  plugin.registerWithRouter(router)
  plugin.start({ legacyExportEnabled: false })
  const deletion = call('DELETE', '/source-data/:name', { params: { name: 'gfs-0p25' } })
  await waitFor(() => activeSignal)
  const source = { model: 'arome', resolution: '0025', autoDownload: false }
  assert.deepStrictEqual(call('PUT', '/settings', { body: { sources: [source], bbox: BBOX } }), { ok: true })
  assert.deepStrictEqual(call('GET', '/settings').sources, [source])
  assert.strictEqual(activeSignal.aborted, false)
  release()
  assert.deepStrictEqual(await deletion, { ok: true })
})

test('a deletion confirmed during a transfer is saved with the settings and resumed after restart', async t => {
  const dataDir = tmpDir()
  const gfs = { model: 'gfs', resolution: '0p25', autoDownload: false }
  const removed = { model: 'arome', resolution: '0025', autoDownload: false }
  const name = 'arome-0025'
  let release, activeSignal, plugin
  const blocked = new Promise(resolve => { release = resolve })
  t.mock.method(CataloguePublisher.prototype, 'check', async signal => {
    activeSignal = signal
    await blocked
    signal.throwIfAborted()
  })
  const deleted = []
  t.mock.method(CataloguePublisher.prototype, 'removeSource', async source => { deleted.push(source) })
  t.after(async () => {
    release()
    await plugin?.stop()
    fs.rmSync(dataDir, { recursive: true, force: true })
  })
  function start() {
    const { router, call } = makeRouter()
    plugin = makePlugin(makeServer(dataDir, { seed: 'offline' }))
    plugin.registerWithRouter(router)
    plugin.start({ legacyExportEnabled: false, sources: [gfs, removed], bbox: BBOX })
    return call
  }
  let call = start()
  call('POST', '/download/:name', { params: { name: 'gfs-0p25' } })
  await waitFor(() => activeSignal)
  assert.deepStrictEqual(call('PUT', '/settings', { body: { sources: [gfs], deleteSources: [name] } }), {
    ok: true, pendingDataDeletions: [name]
  })
  assert.deepStrictEqual(call('GET', '/status').pendingDataDeletions, [name])
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(path.join(dataDir, 'settings.json'))).pendingDataDeletions, [name])
  await settle()
  assert.deepStrictEqual(deleted, [])
  assert.strictEqual(activeSignal.aborted, false)

  const stopped = plugin.stop()
  release()
  await stopped
  call = start()
  assert.deepStrictEqual(call('GET', '/settings').sources, [gfs])
  await waitFor(() => !call('GET', '/status').pendingDataDeletions.length)
  assert.deepStrictEqual(deleted, [name])
  assert.strictEqual(JSON.parse(fs.readFileSync(path.join(dataDir, 'settings.json'))).pendingDataDeletions, undefined)
})

test('adding a removed source again cancels its queued deletion before it starts', async t => {
  const dataDir = tmpDir()
  const source = { model: 'arome', resolution: '0025', autoDownload: false }
  const { router, call } = makeRouter()
  const plugin = makePlugin(makeServer(dataDir, { seed: 'offline' }))
  const deletion = t.mock.method(CataloguePublisher.prototype, 'removeSource', async () => {})
  t.after(async () => {
    await plugin.stop()
    fs.rmSync(dataDir, { recursive: true, force: true })
  })
  plugin.registerWithRouter(router)
  plugin.start({ legacyExportEnabled: false, sources: [source] })
  const queued = call('PUT', '/settings', { body: { sources: [], deleteSources: ['arome-0025'] } })
  assert.deepStrictEqual(queued.pendingDataDeletions, ['arome-0025'])
  assert.deepStrictEqual(call('PUT', '/settings', { body: { sources: [source] } }), { ok: true })
  await settle()
  assert.deepStrictEqual(call('GET', '/status').pendingDataDeletions, [])
  assert.strictEqual(deletion.mock.callCount(), 0)
})
