'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { createRequire } = require('node:module')
const { CataloguePublisher } = require('../dist/catalogue')
const { toDownloadSource, planRun } = require('../dist/downloader')
const { server, grib, waitFor, hash, API } = require('./fixtures/http.cjs')
const providerPath = process.env.FORECAST_DATASET_PROVIDER_PATH
const integration = { skip: !providerPath && 'Set FORECAST_DATASET_PROVIDER_PATH to test against the actual streaming provider source' }

test('a fresh local AROME source publishes and exports without creating a catalogue token', integration, async t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'actual-local-provider-'))
  const data = path.join(root, 'downloader')
  const exportRoot = path.join(root, 'gribs')
  const tokenFile = path.join(data, 'catalogue-token')
  const bytes = grib(1024)
  const upstream = []
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(new URL(url).hostname, 'meteofrance-pnt.s3.rbx.io.cloud.ovh.net', 'no catalogue HTTP or device request')
    upstream.push(options.method ?? 'GET')
    return new Response(options.method === 'HEAD' ? null : bytes)
  })

  const requireProvider = createRequire(path.join(path.resolve(providerPath), 'package.json'))
  let resources
  const provider = requireProvider('./dist')({
    getDataDirPath: () => path.join(root, 'provider'), registerResourceProvider: resource => { resources = resource.methods },
    handleMessage() {}, setPluginStatus() {}, setPluginError: message => { throw new Error(message) }, error() {},
  })
  const downloader = require('../dist')({
    getDataDirPath: () => data, getSelfPath: () => 'online',
    streambundle: { getSelfStream: () => ({ onValue: () => () => {} }) },
    setPluginStatus() {}, debug() {},
  })
  const routes = new Map()
  const router = Object.fromEntries(['get', 'post', 'put', 'delete'].map(method =>
    [method, (route, handler) => routes.set(method + route, handler)]))
  const call = async (method, route, req = {}) => {
    let result
    const res = { status: () => res, json: value => { result = value } }
    await routes.get(method + route)(req, res)
    return result
  }
  t.after(async () => {
    await downloader.stop()
    await provider.stop()
    fs.rmSync(root, { recursive: true, force: true })
  })

  downloader.registerWithRouter(router)
  const source = { model: 'arome', resolution: '0025', hours: 6 }
  downloader.start({ catalogueUrl: 'https://former-catalogue.example', catalogueTokenFile: '/old/token', sources: [source], gribsRoot: exportRoot })
  assert.equal((await call('get', '/status')).catalogueTransport, 'waiting')
  assert.deepEqual(upstream, [])

  await provider.start({})
  const sourceFolder = path.join(exportRoot, 'arome-0025')
  let marker
  await waitFor(() => {
    if (!fs.existsSync(sourceFolder)) return false
    marker = fs.readdirSync(sourceFolder).find(name => name.endsWith('.complete'))
    return marker
  })
  const status = await call('get', '/status')
  const [published] = Object.values(await resources.listResources({}))
  assert.equal(status.catalogueTransport, 'local')
  assert.equal(status.catalogueAccess, undefined)
  assert.equal(downloader.schema().properties.catalogueUrl, undefined)
  assert.equal(downloader.schema().properties.catalogueTokenFile, undefined)
  assert.equal(published.model, 'AROME')
  assert.equal(published.id, status.sources[0].catalogue.datasetId)
  assert.equal(fs.existsSync(tokenFile), false)
  const exported = fs.readdirSync(sourceFolder).filter(name => name.endsWith('.grb2'))
  assert.equal(exported.length, published.assetCount)
  for (const file of exported) assert.deepEqual(fs.readFileSync(path.join(sourceFolder, file)), bytes)

  // A saved credential from an older installation never triggers HTTP.
  fs.writeFileSync(tokenFile, 'previous-http-credential')
  await provider.stop()
  assert.equal((await call('get', '/status')).catalogueTransport, 'waiting')
  await provider.start({})
  assert.equal((await call('get', '/status')).catalogueTransport, 'local')
  assert.equal((await resources.listResources({}))[published.id].version, published.version)
  assert.equal(upstream.filter(method => method === 'GET').length, exported.length)
})

async function setup(t, options = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'actual-direct-provider-'))
  const data = path.join(root, 'downloader')
  fs.mkdirSync(data)
  const requireProvider = createRequire(path.join(path.resolve(providerPath), 'package.json'))
  const express = requireProvider('express'), createProvider = requireProvider('./dist')
  const plugin = createProvider({ getDataDirPath: () => path.join(root, 'provider'), registerResourceProvider: () => {},
    handleMessage: () => {}, setPluginStatus: () => {}, setPluginError: message => { throw new Error(message) }, error: () => {} })
  await plugin.start(options.settings ?? {})
  t.after(() => plugin.stop())
  const app = express(), router = express.Router()
  router.access = () => router
  plugin.registerWithRouter(router)
  let loseCommit = false
  app.use((req, res, next) => {
    if (req.headers.authorization !== 'Bearer integration-token') return res.status(401).end()
    next()
  })
  app.use(API, router)
  const provider = await server(t, app)
  const upstream = await server(t, options.handler ?? ((_req, res) => res.end(grib())))
  const source = { model: 'gfs', hours: 3, ...options.source }
  const descriptor = toDownloadSource(source); descriptor.base = upstream.base
  const plan = planRun(descriptor, new Date('2026-10-21T00:00:00Z'))
  const registry = globalThis[Symbol.for('macjl.forecast-datasets.catalogues.v1')]
  const publisher = legacyExport => new CataloguePublisher(data, () => {
    const local = registry.get(root)
    if (!local) return
    return { ...local, async commit(id) {
      const record = await local.commit(id)
      if (loseCommit) { loseCommit = false; throw new Error('Commit acknowledgement lost') }
      return record
    } }
  }, legacyExport)
  const publish = (signal) => publisher().publish(source, plan, descriptor.fingerprint, () => {}, signal)
  const get = async route => { const response = await fetch(provider.base + API + route, { headers: { Authorization: 'Bearer integration-token' } }); assert.equal(response.status, 200); return response.json() }
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  return { root, data, source, publisher, publish, get, plan, upstream, loseCommit: () => { loseCommit = true } }
}

test('actual provider: interrupted second asset preserves the first and resumes to one committed dataset', integration, async t => {
  let broken = true
  const bytes = grib(64 * 1024)
  const f = await setup(t, { handler: (req, res) => {
    if (broken && req.url.includes('f003')) { res.writeHead(200); res.write(bytes.subarray(0, 100)); return setImmediate(() => res.destroy()) }
    res.end(bytes)
  } })
  await assert.rejects(f.publish())
  assert.deepEqual(await f.get('/datasets'), {})
  let stages = Object.values(await f.get('/publications'))
  // Client rejection can precede the provider's asynchronous socket cleanup.
  // Observe the documented pending state before asserting its reset progress.
  for (let n = 0; n < 100 && stages[0]?.state === 'receiving'; n++) {
    await new Promise(resolve => setTimeout(resolve, 10))
    stages = Object.values(await f.get('/publications'))
  }
  assert.equal(stages.length, 1); assert.equal(stages[0].progress.assetsCompleted, 1)
  assert.equal(stages[0].state, 'pending')
  assert.equal(stages[0].progress.assets[f.plan.assets[1].id].bytesReceived, 0)
  broken = false
  const ref = await f.publish()
  assert.equal(Object.keys(await f.get('/datasets')).length, 1)
  assert.deepEqual(await f.get('/publications'), {})
  assert.equal(f.upstream.calls.filter(c => c.url.includes('f000')).length, 1)
  const detail = await f.get('/datasets/' + ref.datasetId)
  assert.ok(detail.assets.every(a => a.size === bytes.length && a.sha256 === hash(bytes)))
  assert.deepEqual(fs.readdirSync(f.data).sort(), ['catalogue-publications'])
})

test('actual provider: receiving progress has unknown total, and cancellation never publishes partial data', integration, async t => {
  let pendingResponse
  const bytes = grib(1024 * 1024)
  const f = await setup(t, { handler: (_req, res) => { pendingResponse = res; res.writeHead(200); res.write(bytes.subarray(0, 4096)) } })
  const controller = new AbortController(), transfer = f.publish(controller.signal)
  // Attach rejection handling immediately while inspecting live staging progress.
  const failed = assert.rejects(transfer)
  await waitFor(() => pendingResponse)
  let stage
  for (let n = 0; n < 100; n++) {
    stage = Object.values(await f.get('/publications'))[0]
    if (stage?.progress.bytesReceived > 0) break
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert.equal(stage.state, 'receiving'); assert.equal(stage.progress.totalBytes, null)
  assert.ok(stage.progress.bytesReceived > 0)
  assert.deepEqual(await f.get('/datasets'), {})
  controller.abort(new Error('test cancellation')); await failed
  for (let n = 0; n < 100; n++) {
    stage = Object.values(await f.get('/publications'))[0]
    if (stage.state === 'pending') break
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  assert.equal(stage.state, 'pending'); assert.equal(stage.progress.bytesReceived, 0)
  assert.equal(stage.progress.assetsCompleted, 0)
  assert.deepEqual(await f.get('/datasets'), {})
})

test('actual provider: streamed size limit rejects the asset and leaves no completed dataset', integration, async t => {
  const f = await setup(t, { settings: { maxAssetMiB: 64 / 1024 ** 2 } })
  await assert.rejects(f.publish())
  assert.deepEqual(await f.get('/datasets'), {})
  const stage = Object.values(await f.get('/publications'))[0]
  assert.equal(stage.progress.assetsCompleted, 0)
  assert.equal(stage.progress.bytesReceived, 0)
})

test('actual provider: lost commit response recovers the measured content without another upload', integration, async t => {
  const f = await setup(t)
  f.loseCommit()
  await assert.rejects(f.publish())
  assert.equal(Object.keys(await f.get('/datasets')).length, 1)
  const ref = await f.publish()
  assert.ok(ref.version)
  assert.equal(f.upstream.calls.length, 2)
  assert.deepEqual(await f.get('/publications'), {})
})

test('actual provider: wave retries, publication and retention never replace atmospheric acquisitions', integration, async t => {
  let broken = true
  const bytes = grib(128)
  const f = await setup(t, { handler: (req, res) => {
    if (broken && req.url.includes('filter_gfswave') && req.url.includes('f003')) {
      res.writeHead(200); res.write(bytes.subarray(0, 20)); return setImmediate(() => res.destroy())
    }
    res.end(bytes)
  } })
  const atmospheric = await f.publish()
  const source = { model: 'gfs-wave', hours: 3, archiveRuns: 0 }
  const descriptor = toDownloadSource(source); descriptor.base = f.upstream.base
  const plan = planRun(descriptor, new Date('2026-10-21T00:00:00Z'))
  const publish = () => f.publisher().publish(source, plan, descriptor.fingerprint, () => {})
  await assert.rejects(publish())
  assert.deepEqual(Object.keys(await f.get('/datasets')), [atmospheric.datasetId])
  broken = false
  const waves = await publish()
  assert.notEqual(waves.datasetId, atmospheric.datasetId)
  const detail = await f.get('/datasets/' + waves.datasetId)
  assert.equal(detail.model, 'GFS-WAVE')
  assert.equal(detail.requested.source, 'gfs-wave-0p25')
  assert.equal(detail.assets.length, 2)
  assert.ok(detail.assets.every(a => a.size === bytes.length && a.sha256 === hash(bytes)))
  assert.equal(f.upstream.calls.filter(c => c.url.includes('filter_gfswave') && c.url.includes('f000')).length, 1)
  const next = await f.publisher().publish(source, planRun(descriptor, new Date('2026-10-21T06:00:00Z')), descriptor.fingerprint, () => {})
  assert.deepEqual(Object.keys(await f.get('/datasets')).sort(), [atmospheric.datasetId, next.datasetId].sort())
  await f.publisher().removeSource(source)
  assert.deepEqual(Object.keys(await f.get('/datasets')), [atmospheric.datasetId])
  assert.deepEqual(fs.readdirSync(f.data).sort(), ['catalogue-publications'])
})

test('actual provider: legacy export reads immutable committed originals and writes the historical marker without upstream downloads', integration, async t => {
  const { LegacyGribExporter, legacyFilename } = require('../dist/legacy-export')
  const f = await setup(t), ref = await f.publish()
  const root = path.join(f.root, 'legacy-gribs'), exporter = new LegacyGribExporter(root)
  const publisher = f.publisher(exporter), upstream = f.upstream.calls.length
  await publisher.exportLatest(f.source, () => {})
  assert.equal(exporter.status('gfs-0p25').state, 'complete')
  const detail = await f.get('/datasets/' + ref.datasetId)
  for (const asset of detail.assets) {
    const file = path.join(root, 'gfs-0p25', legacyFilename('gfs-0p25', f.plan.stamp, asset.id))
    const bytes = fs.readFileSync(file)
    assert.equal(bytes.length, asset.size)
    assert.equal(hash(bytes), asset.sha256)
  }
  const marker = JSON.parse(fs.readFileSync(path.join(root, 'gfs-0p25', `.run-${f.plan.stamp}.complete`)))
  assert.deepEqual(marker, { params: detail.requested.parameters })
  assert.equal(f.upstream.calls.length, upstream)
})

test('actual provider: day-based retention preserves multiple acquisitions with archiveRuns set to zero', integration, async t => {
  const f = await setup(t, { source: { retentionDays: 3, archiveRuns: 0 } })
  const first = await f.publish()
  const descriptor = toDownloadSource(f.source)
  descriptor.base = f.upstream.base
  const next = planRun(descriptor, new Date('2026-10-21T06:00:00Z'))
  const second = await f.publisher().publish(f.source, next, descriptor.fingerprint, () => {})
  const list = await f.get('/datasets')
  assert.deepEqual(Object.keys(list).sort(), [first.datasetId, second.datasetId].sort())
  for (const dataset of Object.values(list))
    assert.equal(Date.parse(dataset.expiresAt) - Date.parse(dataset.createdAt), 3 * 86_400_000)
  const journal = JSON.parse(fs.readFileSync(path.join(f.data, 'catalogue-publications', 'local', 'gfs-0p25.json'), 'utf8'))
  assert.equal(journal.records.length, 2)
  for (const entry of journal.records) assert.equal(entry.expiresAt, list[entry.datasetId].expiresAt)
})
