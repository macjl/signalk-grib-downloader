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
const { expectedRunStamp, fetchFingerprint } = require('../dist/scheduler.js')

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
  return { router, call: (method, p) => {
    let payload
    const res = {
      status: () => res,
      json: (v) => { payload = v },
    }
    routes[`${method} ${p}`]({}, res)
    return payload
  } }
}

// An up-to-date marker for the source, so 'online' ticks find nothing
// to download and never touch the network.
const writeUpToDateMarker = (root) => {
  const dir = path.join(root, 'icon-eu')
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(
    path.join(dir, `.run-${expectedRunStamp('icon-eu')}.complete`),
    JSON.stringify({ params: fetchFingerprint(SOURCE, BBOX) })
  )
}

const settle = (ms = 50) => new Promise((resolve) => setTimeout(resolve, ms))

const startPlugin = ({ seed, prepare } = {}) => {
  const dataDir = tmpDir()
  const gribsRoot = tmpDir()
  fs.writeFileSync(path.join(dataDir, 'settings.json'),
    JSON.stringify({ bbox: BBOX, sources: [SOURCE] }))
  if (prepare) prepare(gribsRoot)
  const server = makeServer(dataDir, { seed })
  const plugin = makePlugin(server)
  const { router, call } = makeRouter()
  plugin.registerWithRouter(router)
  plugin.start({ gribsRoot })
  return { plugin, server, call, gribsRoot, stop: () => plugin.stop() }
}

test('entering captive pauses the auto scheduler; leaving it catches up', async () => {
  // Source up to date from the start: every 'online'-like tick finds
  // nothing to download, so no network is ever touched.
  const t = startPlugin({ prepare: writeUpToDateMarker })
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
    writeUpToDateMarker(t.gribsRoot)
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
