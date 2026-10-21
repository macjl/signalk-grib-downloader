'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const http = require('node:http')
const { createHash, randomUUID } = require('node:crypto')
const { CataloguePublisher } = require('../dist/catalogue')
const { Orchestrator } = require('../dist/orchestrator')
const { probeRunStamp, fetchFingerprint } = require('../dist/scheduler')

async function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'grib-catalogue-'))
  const directory = path.join(root, 'gfs-0p25')
  fs.mkdirSync(directory)
  const tokenFile = path.join(root, 'token')
  fs.writeFileSync(tokenFile, 'test-readwrite-token', { mode: 0o600 })
  const source = { model: 'gfs', resolution: '0p25', hours: 3 }
  const staged = new Map(), datasets = new Map(), calls = [], declarations = [], bodies = new Map()
  let failAsset = null, loseCommit = false, deny = false, conflict = false
  const server = http.createServer(async (req, res) => {
    try {
      calls.push({ method: req.method, url: req.url })
      const json = (status, body) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body)) }
      if (deny || req.headers.authorization !== 'Bearer test-readwrite-token') return json(401, {})
      const parts = req.url.replace('/plugins/signalk-forecast-dataset-provider/', '').split('/')
      const [kind, id, action, assetId] = parts
      if (kind === 'status') return json(200, { contractVersion: '0.1' })
      if (kind === 'publications' && req.method === 'POST' && !id) {
        let text = ''; for await (const chunk of req) text += chunk
        const declaration = JSON.parse(text)
        declarations.push(declaration)
        const record = { id: randomUUID(), datasetId: randomUUID(), declaration, uploaded: {} }
        staged.set(record.id, record)
        return json(201, record)
      }
      if (kind === 'publications') {
        const record = staged.get(id)
        if (!record) return json(404, {})
        if (req.method === 'DELETE') { staged.delete(id); res.writeHead(204); return res.end() }
        if (req.method === 'GET') return json(200, record)
        if (action === 'assets') {
          const chunks = []; for await (const chunk of req) chunks.push(chunk)
          if (failAsset === assetId) return json(503, {})
          const bytes = Buffer.concat(chunks), asset = record.declaration.assets.find(a => a.id === assetId)
          assert.equal(bytes.length, asset.size)
          assert.equal(createHash('sha256').update(bytes).digest('hex'), asset.sha256)
          if (record.uploaded[assetId]) return json(409, {})
          record.uploaded[assetId] = asset
          bodies.set(assetId, bytes)
          return json(201, asset)
        }
        if (action === 'commit') {
          assert.equal(Object.keys(record.uploaded).length, record.declaration.assets.length)
          const committed = { id: record.datasetId, version: createHash('sha256').update(JSON.stringify(record.uploaded)).digest('hex'), assets: Object.values(record.uploaded) }
          datasets.set(record.datasetId, committed); staged.delete(id)
          if (loseCommit) { loseCommit = false; return res.destroy() }
          return json(201, committed)
        }
      }
      if (kind === 'datasets') {
        if (!datasets.has(id)) return json(404, {})
        if (req.method === 'DELETE') {
          if (conflict) return json(409, {})
          datasets.delete(id); res.writeHead(204); return res.end()
        }
        return json(200, datasets.get(id))
      }
      json(404, {})
    } catch (error) { res.writeHead(500); res.end(String(error)) }
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); fs.rmSync(root, { recursive: true, force: true }) })
  const endpoint = `http://127.0.0.1:${server.address().port}`
  const publisher = () => new CataloguePublisher(endpoint, tokenFile, root)
  const run = (stamp = probeRunStamp('gfs'), params = fetchFingerprint(source)) => {
    const files = [0, 3].map(n => `gfs-0p25__${stamp}__f00${n}.grb2`)
    const bytes = files.map((_, n) => Buffer.from([71,82,73,66,0,255,n,128,56,56,56,56]))
    files.forEach((f, i) => fs.writeFileSync(path.join(directory, f), bytes[i]))
    fs.writeFileSync(path.join(directory, `.run-${stamp}.complete`), JSON.stringify({ params, files }))
    return { stamp, files, bytes }
  }
  return { root, directory, source, staged, datasets, calls, declarations, bodies, publisher, run,
    fail: id => { failAsset = id }, loseCommit: () => { loseCommit = true },
    deny: value => { deny = value }, conflict: value => { conflict = value } }
}

test('publishes exact multi-asset bytes with sparse declared metadata and no verified inventory', async t => {
  const f = await fixture(t), r = f.run()
  const ref = await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  assert.equal(f.datasets.size, 1)
  const declaration = f.calls.find(c => c.method === 'POST')
  assert.ok(declaration)
  assert.equal(f.declarations[0].producer, 'signalk-grib-downloader')
  assert.equal(f.declarations[0].metadata.model, 'GFS')
  assert.equal(f.declarations[0].requested.source, 'gfs-0p25')
  for (const key of ['bounds', 'forecast', 'fields', 'inventory']) assert.equal(f.declarations[0].metadata[key], undefined)
  for (let i = 0; i < r.files.length; i++) assert.deepEqual(f.bodies.get(r.files[i].replace('.', '_')), r.bytes[i])
  assert.match(ref.href, /forecastDatasets/)
  const puts = f.calls.filter(c => c.method === 'PUT').length
  await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  assert.equal(f.datasets.size, 1)
  assert.equal(f.calls.filter(c => c.method === 'PUT').length, puts)
})

test('restart resumes only missing assets after a failed upload and hides incomplete content', async t => {
  const f = await fixture(t), r = f.run()
  f.fail(r.files[1].replace('.', '_'))
  await assert.rejects(f.publisher().publish(f.source, f.directory, r.stamp, () => {}), /HTTP 503/)
  assert.equal(f.datasets.size, 0); assert.equal(f.staged.size, 1)
  f.fail(null)
  await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  assert.equal(f.staged.size, 0); assert.equal(f.datasets.size, 1)
  assert.equal(f.calls.filter(c => c.method === 'PUT' && c.url.endsWith(r.files[0].replace('.', '_'))).length, 1)
})

test('lost commit response is recovered without duplicate datasets or another upload', async t => {
  const f = await fixture(t), r = f.run()
  f.loseCommit()
  await assert.rejects(f.publisher().publish(f.source, f.directory, r.stamp, () => {}))
  const ref = await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  assert.ok(ref.version); assert.equal(f.datasets.size, 1)
  assert.equal(f.calls.filter(c => c.method === 'PUT').length, 2)
})

test('external catalogue deletion republishes local bytes; parameter changes publish a new acquisition', async t => {
  const f = await fixture(t), r = f.run()
  await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  f.datasets.clear()
  await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  assert.equal(f.datasets.size, 1)
  f.run(r.stamp, { changed: true })
  await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  assert.equal(f.datasets.size, 1, 'default retention removes previous acquisition after commit')
  assert.ok(f.publisher().reference(f.source, r.stamp, { changed: true }))
})

test('retention preserves configured acquisitions and never forces a provider deletion conflict', async t => {
  const f = await fixture(t)
  const source = { ...f.source, archiveRuns: 1 }
  for (const stamp of ['20261020T00', '20261020T06', '20261020T12']) {
    f.run(stamp); await f.publisher().publish(source, f.directory, stamp, () => {})
  }
  assert.equal(f.datasets.size, 2)
  f.conflict(true)
  await assert.rejects(f.publisher().removeSource(source), /HTTP 409/)
  assert.equal(f.datasets.size, 2)
  f.conflict(false); await f.publisher().removeSource(source)
  assert.equal(f.datasets.size, 0)
})

test('manifest loss and authentication failure preserve local completed files', async t => {
  const f = await fixture(t), r = f.run()
  f.deny(true)
  await assert.rejects(f.publisher().publish(f.source, f.directory, r.stamp, () => {}), /HTTP 401/)
  assert.ok(fs.existsSync(path.join(f.directory, r.files[0])))
  fs.unlinkSync(path.join(f.directory, r.files[1]))
  await assert.rejects(f.publisher().publish(f.source, f.directory, r.stamp, () => {}), /manifest/)
})

test('catalogue outage retries before upstream download and status stays pending until commit', async t => {
  const f = await fixture(t), r = f.run()
  const source = { ...f.source, autoDownload: true }
  const orch = new Orchestrator([source], f.root, () => {}, () => {}, undefined, f.publisher())
  assert.equal(orch.status()[0].publicationPending, true)
  assert.equal(orch.status()[0].upToDate, false)
  f.deny(true)
  await orch.downloadSource(source)
  assert.equal(orch.status()[0].lastOutcome, 'failed')
  assert.ok(f.calls.every(c => c.url.startsWith('/plugins/signalk-forecast-dataset-provider/')))
  f.deny(false)
  await orch.downloadSource(source)
  assert.equal(orch.status()[0].publicationPending, false)
  assert.equal(orch.status()[0].upToDate, true)
  await orch.stop()
  assert.equal(await orch.downloadSource(source), false)
})

test('catalogue endpoints reject credentials, query strings and unencrypted remote transport', () => {
  for (const url of ['http://remote.test', 'https://user:secret@remote.test', 'https://remote.test?token=secret']) {
    assert.throws(() => new CataloguePublisher(url, '', '/tmp'))
  }
})

test('legacy empty completion markers can publish without inventing fetch parameters', async t => {
  const f = await fixture(t), r = f.run()
  fs.writeFileSync(path.join(f.directory, `.run-${r.stamp}.complete`), '')
  await f.publisher().publish(f.source, f.directory, r.stamp, () => {})
  assert.equal(f.declarations[0].requested.parameters, null)
  assert.ok(f.publisher().reference(f.source, r.stamp, null))
})

test('stopping the orchestrator aborts and waits for an active publication', async t => {
  const f = await fixture(t)
  f.run()
  const publisher = f.publisher()
  let started
  const ready = new Promise(resolve => { started = resolve })
  t.mock.method(publisher, 'publish', async (_source, _directory, _stamp, _log, signal) => {
    started()
    await new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }))
  })
  const orch = new Orchestrator([f.source], f.root, () => {}, () => {}, undefined, publisher)
  const transfer = orch.downloadSource(f.source)
  await ready
  assert.equal(orch.isRunning(), true)
  await orch.stop()
  await transfer
  assert.equal(orch.isRunning(), false)
  assert.equal(orch.status()[0].lastOutcome, 'failed')
  assert.equal(f.datasets.size, 0)
})
