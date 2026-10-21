'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { CataloguePublisher } = require('../dist/catalogue')
const { Orchestrator } = require('../dist/orchestrator')
const { fixture, waitFor, grib } = require('./fixtures/http.cjs')

const publish = (f, publisher = f.publisher(), plan = f.plan(), params = f.descriptor.fingerprint) => publisher.publish(f.source, plan, params, () => {})

test('streams unknown-size assets directly with hashes, complete receipts and sparse metadata', async t => {
  const f = await fixture(t), plan = f.plan()
  const ref = await publish(f)
  assert.equal(f.datasets.size, 1); assert.ok(ref.version)
  const declaration = f.declarations[0]
  assert.equal(declaration.producer, 'signalk-grib-downloader')
  assert.equal(declaration.requested.source, 'gfs-0p25')
  for (const asset of declaration.assets) { assert.equal(asset.size, undefined); assert.equal(asset.sha256, undefined) }
  for (const key of ['bounds', 'fields', 'inventory']) assert.equal(declaration.metadata[key], undefined)
  for (const asset of plan.assets) assert.deepEqual(f.bodies.get(asset.id), f.bytes)
  assert.ok(f.upstream.calls.every(c => !c.authorization), 'catalogue token never reaches upstream')
  assert.deepEqual(fs.readdirSync(f.root).sort(), ['catalogue-publications', 'token'])
  const count = f.upstream.calls.length
  await publish(f)
  assert.equal(f.upstream.calls.length, count, 'committed receipt avoids upstream work')
  assert.equal(f.datasets.size, 1)
})

test('a stalled provider applies backpressure to upstream rather than buffering the entire GRIB', { timeout: 10000 }, async t => {
  let release, uploadStarted = false, sent = 0
  const gate = new Promise(resolve => { release = resolve })
  const total = 32 * 1024 * 1024
  const block = Buffer.alloc(64 * 1024, 17)
  const f = await fixture(t, { source: { model: 'gfs', hours: 0 },
    beforeAssetRead: async () => { uploadStarted = true; await gate },
    handler: (_req, res) => {
      const header = Buffer.alloc(16)
      header.write('GRIB'); header[7] = 2; header.writeBigUInt64BE(BigInt(total), 8)
      res.writeHead(200); res.write(header); sent = 16
      const pump = () => {
        if (res.destroyed) return
        while (sent < total - 4) {
          const chunk = block.subarray(0, Math.min(block.length, total - 4 - sent))
          sent += chunk.length
          if (!res.write(chunk)) { res.once('drain', pump); return }
        }
        sent += 4; res.end('7777')
      }
      pump()
    },
  })
  const transfer = publish(f, f.publisher())
  t.after(() => release())
  try {
    await waitFor(() => uploadStarted)
    await new Promise(resolve => setTimeout(resolve, 100))
    assert.ok(sent < total, 'upstream socket must stop while the provider is not reading')
  } finally { release() }
  await transfer
  assert.equal(f.datasets.size, 1)
})

test('a restart resumes only missing assets and commits no incomplete acquisition', async t => {
  const f = await fixture(t), plan = f.plan()
  f.flags.failAsset = plan.assets[1].id
  await assert.rejects(publish(f), /503/)
  assert.equal(f.datasets.size, 0); assert.equal(f.staged.size, 1)
  const firstUploads = () => f.provider.calls.filter(c => c.method === 'PUT' && c.url.endsWith(plan.assets[0].id)).length
  assert.equal(firstUploads(), 1)
  f.flags.failAsset = null
  await publish(f, f.publisher())
  assert.equal(firstUploads(), 1); assert.equal(f.datasets.size, 1); assert.equal(f.staged.size, 0)
  assert.equal(f.upstream.calls.filter(c => c.method === 'GET').length, 3)
})

for (const flag of ['loseCreate', 'loseUpload', 'loseCommit']) {
  test(`lost ${flag} response recovers without duplicate datasets or completed uploads`, async t => {
    const f = await fixture(t)
    f.flags[flag] = true
    await assert.rejects(publish(f))
    await publish(f, f.publisher())
    assert.equal(f.declarations.length, 1, 'creation identity survives a lost response')
    assert.equal(f.datasets.size, 1); assert.equal(f.staged.size, 0)
    assert.equal(f.provider.calls.filter(c => c.method === 'PUT').length, 2)
    assert.equal(f.upstream.calls.filter(c => c.method === 'GET').length, 2)
  })
}

test('generic 404 on a committed acquisition preserves its receipt and cannot duplicate it', async t => {
  const f = await fixture(t), ref = await publish(f)
  const file = path.join(f.root, 'catalogue-publications/gfs-0p25.json'), before = fs.readFileSync(file, 'utf8')
  f.flags.disabled = true
  await assert.rejects(publish(f), /404/)
  assert.equal(fs.readFileSync(file, 'utf8'), before)
  f.flags.disabled = false
  assert.deepEqual(await publish(f), ref)
  assert.equal(f.datasets.size, 1); assert.equal(f.declarations.length, 1)
})

test('generic 404 during staging recovery or deletion never discards the acquisition journal', async t => {
  const f = await fixture(t), plan = f.plan()
  f.flags.failAsset = plan.assets[1].id
  await assert.rejects(publish(f))
  const file = path.join(f.root, 'catalogue-publications/gfs-0p25.json'), before = fs.readFileSync(file, 'utf8')
  f.flags.disabled = true
  await assert.rejects(publish(f), /404/)
  await assert.rejects(f.publisher().removeSource(f.source), /404/)
  assert.equal(fs.readFileSync(file, 'utf8'), before)
  f.flags.disabled = false; f.flags.failAsset = null
  await publish(f)
  assert.equal(f.datasets.size, 1); assert.equal(f.declarations.length, 1)
})

test('explicit DATASET_NOT_FOUND permits reacquisition from upstream', async t => {
  const f = await fixture(t), first = await publish(f)
  f.datasets.clear()
  const second = await publish(f)
  assert.notEqual(first.datasetId, second.datasetId); assert.equal(f.datasets.size, 1)
})

test('retention follows catalogue acquisitions and respects active-reader conflicts', async t => {
  const f = await fixture(t, { source: { model: 'gfs', hours: 0, archiveRuns: 1 } })
  for (const hour of ['00', '06', '12']) await publish(f, f.publisher(), f.plan(new Date(`2026-10-20T${hour}:00:00Z`)))
  assert.equal(f.datasets.size, 2)
  f.flags.conflict = true
  await assert.rejects(f.publisher().removeSource(f.source), /409/)
  assert.equal(f.datasets.size, 2)
  f.flags.conflict = false
  await f.publisher().removeSource(f.source)
  assert.equal(f.datasets.size, 0)
})

test('same-cycle changed parameters select the newest acquisition even when archives are retained', async t => {
  const f = await fixture(t, { source: { model: 'gfs', hours: 0, archiveRuns: 1 } }), publisher = f.publisher()
  const first = await publish(f)
  const params = { changedArea: true }
  const second = await publish(f, publisher, f.plan(), params)
  assert.notEqual(first.datasetId, second.datasetId); assert.equal(f.datasets.size, 2)
  assert.deepEqual(publisher.latest(f.source).params, params)
})

test('a provider receipt mismatch fails closed before commit', async t => {
  const f = await fixture(t)
  f.flags.corruptReceipt = true
  await assert.rejects(publish(f), /receipt differs/)
  assert.equal(f.datasets.size, 0)
  assert.ok(!f.provider.calls.some(c => c.url.endsWith('/commit')))
})

test('a clean upstream EOF containing truncated GRIB never authorizes commit', async t => {
  const f = await fixture(t, { bytes: grib().subarray(0, 50) })
  await assert.rejects(publish(f), /truncated/)
  assert.equal(f.datasets.size, 0)
  assert.ok(!f.provider.calls.some(c => c.url.endsWith('/commit')))
})

test('a malformed final asset preserves previous forecasts and the successfully received first asset', async t => {
  let invalid = false
  const f = await fixture(t, { handler: (req, res) => res.end(invalid && req.url.includes('f003') ? grib().subarray(0, 25) : grib()) })
  const first = await publish(f)
  invalid = true
  await assert.rejects(publish(f, f.publisher(), f.plan(), { changed: true }))
  assert.equal(f.datasets.size, 1); assert.ok(f.datasets.has(first.datasetId))
  assert.equal(f.staged.size, 1)
  assert.equal(Object.keys([...f.staged.values()][0].uploaded).length, 1)
})

test('provider outage fails before opening upstream requests and reports a pending acquisition after failure', async t => {
  const f = await fixture(t), publisher = f.publisher()
  const orch = new Orchestrator([f.source], publisher, () => {})
  t.after(() => orch.stop())
  f.flags.denied = true
  await orch.downloadSource(f.source)
  assert.equal(orch.status()[0].lastOutcome, 'failed')
  assert.equal(f.upstream.calls.length, 0)
  f.flags.denied = false
  const original = globalThis.fetch
  t.mock.method(globalThis, 'fetch', (url, options) => original(String(url).startsWith('https://nomads.ncep.noaa.gov') ? String(url).replace('https://nomads.ncep.noaa.gov', f.upstream.base) : url, options))
  f.flags.failAsset = f.plan().assets[1].id
  await orch.downloadSource(f.source)
  assert.equal(orch.status()[0].publicationPending, true)
  assert.equal(orch.status()[0].lastRun, null)
  f.flags.failAsset = null
  await orch.downloadSource(f.source)
  assert.equal(orch.status()[0].lastOutcome, 'downloaded')
  assert.equal(orch.status()[0].publicationPending, false)
  assert.equal(orch.status()[0].upToDate, true)
})

test('stopping aborts both sides of a stalled direct stream and waits for completion', async t => {
  let closed = false
  const f = await fixture(t, { handler: (req, res) => { if (req.method === 'HEAD') return res.end(); res.write(grib(4096).subarray(0, 30)); res.on('close', () => { closed = true }) } })
  const original = globalThis.fetch
  t.mock.method(globalThis, 'fetch', (url, options) => original(String(url).startsWith('https://nomads.ncep.noaa.gov') ? String(url).replace('https://nomads.ncep.noaa.gov', f.upstream.base) : url, options))
  const orch = new Orchestrator([f.source], f.publisher(), () => {})
  t.after(() => orch.stop())
  const transfer = orch.downloadSource(f.source)
  await waitFor(() => f.upstream.calls.some(c => c.method === 'GET'))
  await orch.stop(); await transfer
  await waitFor(() => closed)
  assert.equal(orch.isRunning(), false); assert.equal(f.datasets.size, 0)
  assert.equal(await orch.downloadSource(f.source), false)
  assert.ok(!f.provider.calls.some(c => c.url.endsWith('/commit')))
})

test('legacy committed journals remain usable without any local GRIB or completion marker', async t => {
  const f = await fixture(t), first = await publish(f)
  const file = path.join(f.root, 'catalogue-publications/gfs-0p25.json')
  const journal = JSON.parse(fs.readFileSync(file))
  const record = journal.records[0]
  record.assets = Object.values(record.received); delete record.received; delete record.acquisitionId
  fs.writeFileSync(file, JSON.stringify(journal))
  assert.deepEqual(await publish(f, f.publisher()), first)
  assert.equal(f.upstream.calls.length, 2)
})

test('catalogue endpoints reject credentials, query strings and unencrypted remote transport', () => {
  for (const url of ['http://remote.test', 'https://user:secret@remote.test', 'https://remote.test?token=secret']) assert.throws(() => new CataloguePublisher(url, '', '/tmp'))
})
