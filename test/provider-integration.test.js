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

async function setup(t, options = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'actual-direct-provider-'))
  const data = path.join(root, 'downloader')
  fs.mkdirSync(data)
  const tokenFile = path.join(data, 'token'); fs.writeFileSync(tokenFile, 'integration-token')
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
    if (loseCommit && req.url.endsWith('/commit')) {
      loseCommit = false
      res.json = () => { res.destroy(); return res }
    }
    next()
  })
  app.use(API, router)
  const provider = await server(t, app)
  const upstream = await server(t, options.handler ?? ((_req, res) => res.end(grib())))
  const source = { model: 'gfs', hours: 3 }
  const descriptor = toDownloadSource(source); descriptor.base = upstream.base
  const plan = planRun(descriptor, new Date('2026-10-21T00:00:00Z'))
  const publisher = () => new CataloguePublisher(provider.base, tokenFile, data)
  const publish = (signal) => publisher().publish(source, plan, descriptor.fingerprint, () => {}, signal)
  const get = async route => { const response = await fetch(provider.base + API + route, { headers: { Authorization: 'Bearer integration-token' } }); assert.equal(response.status, 200); return response.json() }
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  return { root, data, publisher, publish, get, plan, upstream, loseCommit: () => { loseCommit = true } }
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
  assert.deepEqual(fs.readdirSync(f.data).sort(), ['catalogue-publications', 'token'])
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
  assert.deepEqual(fs.readdirSync(f.data).sort(), ['catalogue-publications', 'token'])
})
