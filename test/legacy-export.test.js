'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const os = require('node:os')
const { createHash, randomUUID } = require('node:crypto')
const { Readable, PassThrough } = require('node:stream')
const { LegacyGribExporter, legacyFilename } = require('../dist/legacy-export')
const { Orchestrator } = require('../dist/orchestrator')
const { planRun, toDownloadSource } = require('../dist/downloader')
const { fixture } = require('./fixtures/http.cjs')
const hash = value => createHash('sha256').update(value).digest('hex')
function local(t) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'legacy-gribs-')), root = path.join(tmp, 'exports')
  t.after(() => fs.rmSync(tmp, { recursive: true, force: true }))
  const exporter = new LegacyGribExporter(root), signal = new AbortController().signal
  const run = (stamp = '20261108T00', contents = ['first GRIB', 'second GRIB']) => ({ source: 'gfs-0p25', stamp, datasetId: randomUUID(),
    version: hash(JSON.stringify({ stamp, contents })), params: { hours: contents.length * 3 }, archiveRuns: 0,
    assets: contents.map((content, i) => ({ id: `gfs-0p25__${stamp}__f${String(i * 3).padStart(3, '0')}_grb2`, size: Buffer.byteLength(content), sha256: hash(content), content })) })
  const read = async asset => Readable.from([Buffer.from(asset.content)])
  const folder = path.join(root, 'gfs-0p25')
  const marker = stamp => path.join(folder, `.run-${stamp}.complete`)
  return { tmp, root, exporter, signal, run, read, folder, marker }
}

test('historical names are restored for GFS, GFS-Wave, Météo-France and ICON', () => {
  const date = new Date('2026-11-08T00:00:00Z')
  for (const source of [{ model: 'gfs', hours: 3 }, { model: 'gfs-wave', hours: 3 }, { model: 'arome', hours: 6 }, { model: 'arpege', hours: 6 }, { model: 'icon-eu', hours: 3 }]) {
    const descriptor = toDownloadSource(source), plan = planRun(descriptor, date)
    const names = plan.assets.map(asset => legacyFilename(descriptor.name, plan.stamp, asset.id))
    const suffix = source.model === 'arome' ? 'SP1_00H06H' : source.model === 'arpege' ? 'SP1_000H012H' : 'f000'
    assert.equal(names[0], `${descriptor.name}__20261108T00__${suffix}.grb2`)
    assert.equal(new Set(names).size, plan.assets.length)
  }
})

test('all assets are verified in staging before the historical completion marker is published', async t => {
  const f = local(t), run = f.run(); let reads = 0
  assert.equal(fs.existsSync(f.root), false)
  await f.exporter.export(run, async asset => {
    reads++
    assert.equal(fs.existsSync(f.marker(run.stamp)), false)
    assert.equal(fs.readdirSync(f.folder).filter(name => name.endsWith('.grb2')).length, 0)
    return f.read(asset)
  }, f.signal)
  assert.equal(reads, 2)
  for (const asset of run.assets) assert.equal(fs.readFileSync(path.join(f.folder, legacyFilename(run.source, run.stamp, asset.id)), 'utf8'), asset.content)
  const marker = JSON.parse(fs.readFileSync(f.marker(run.stamp)))
  assert.deepEqual(marker, { params: run.params })
  assert.equal(f.exporter.status(run.source).state, 'complete')
  assert.equal(fs.readdirSync(f.folder).some(name => name.startsWith('.legacy-export-')), false)
})

test('a failed replacement preserves the previous completed files and marker', async t => {
  const f = local(t), previous = f.run(); await f.exporter.export(previous, f.read, f.signal)
  const before = fs.readFileSync(f.marker(previous.stamp), 'utf8')
  const next = f.run(previous.stamp, ['changed GRIB', 'other content'])
  await assert.rejects(f.exporter.export(next, async asset => Readable.from([Buffer.alloc(asset.size, 9)]), f.signal), /SHA-256 mismatch/)
  assert.equal(fs.readFileSync(f.marker(previous.stamp), 'utf8'), before)
  assert.equal(fs.readFileSync(path.join(f.folder, 'gfs-0p25__20261108T00__f000.grb2'), 'utf8'), previous.assets[0].content)
  assert.equal(f.exporter.status(previous.source).state, 'error')
  assert.equal(fs.readdirSync(f.folder).some(name => name.startsWith('.legacy-export-')), false)
})

test('truncated or oversized catalogue transfers cannot publish a complete run', async t => {
  for (const delta of [-1, 1]) {
    const f = local(t), run = f.run()
    await assert.rejects(f.exporter.export(run, async asset => Readable.from([Buffer.alloc(asset.size + delta)]), f.signal), /size|exceeds/)
    assert.equal(fs.existsSync(f.marker(run.stamp)), false)
    assert.equal(fs.readdirSync(f.folder).filter(name => name.endsWith('.grb2')).length, 0)
  }
})

test('cancelling a blocked export keeps the prior complete run and removes partial output', async t => {
  const f = local(t), previous = f.run(); await f.exporter.export(previous, f.read, f.signal)
  const next = f.run('20261108T06'), controller = new AbortController()
  await assert.rejects(f.exporter.export(next, async () => {
    const stream = new PassThrough(); stream.write('GRIB')
    setImmediate(() => controller.abort())
    return stream
  }, controller.signal), /abort/i)
  assert.equal(fs.existsSync(f.marker(previous.stamp)), true)
  assert.equal(fs.existsSync(f.marker(next.stamp)), false)
  assert.equal(fs.readdirSync(f.folder).some(name => name.startsWith('.legacy-export-')), false)
})

test('restart reuses verified exported files without downloading catalogue assets again', async t => {
  const f = local(t), run = f.run(); await f.exporter.export(run, f.read, f.signal)
  const restarted = new LegacyGribExporter(f.root)
  await restarted.export(run, async () => { throw new Error('Unexpected catalogue read') }, f.signal)
  assert.ok(restarted.matches(run.source, run.stamp, run.version))
})

test('same-run parameter replacement removes obsolete GRIB assets and keeps unrelated files', async t => {
  const f = local(t), previous = f.run(); await f.exporter.export(previous, f.read, f.signal)
  fs.writeFileSync(path.join(f.folder, 'notes.txt'), 'keep')
  const next = f.run(previous.stamp, ['new subset'])
  await f.exporter.export(next, f.read, f.signal)
  assert.equal(fs.existsSync(path.join(f.folder, 'gfs-0p25__20261108T00__f003.grb2')), false)
  assert.equal(fs.readFileSync(path.join(f.folder, 'notes.txt'), 'utf8'), 'keep')
  assert.deepEqual(JSON.parse(fs.readFileSync(f.marker(next.stamp))).params, next.params)
})

test('promotion failure rolls back files and restores the previous marker last', async t => {
  const f = local(t), previous = f.run(); await f.exporter.export(previous, f.read, f.signal)
  const before = fs.readFileSync(f.marker(previous.stamp), 'utf8')
  const rename = fs.renameSync; let failed = false
  fs.renameSync = (from, to) => {
    if (!failed && to === f.marker(previous.stamp) && path.basename(path.dirname(from)).startsWith('.legacy-export-')) {
      failed = true; throw new Error('simulated disk error during marker rename')
    }
    return rename(from, to)
  }
  try { await assert.rejects(f.exporter.export(f.run(previous.stamp, ['replacement']), f.read, f.signal), /simulated disk error/) }
  finally { fs.renameSync = rename }
  assert.equal(failed, true)
  assert.equal(fs.readFileSync(f.marker(previous.stamp), 'utf8'), before)
  for (const asset of previous.assets) assert.equal(fs.readFileSync(path.join(f.folder, legacyFilename(previous.source, previous.stamp, asset.id)), 'utf8'), asset.content)
})

test('retention keeps the newest active run and N complete historical archives without deleting unrelated files', async t => {
  const f = local(t)
  for (const stamp of ['20261108T00', '20261108T06', '20261108T12', '20261108T18']) {
    const run = f.run(stamp); run.archiveRuns = 2
    await f.exporter.export(run, f.read, f.signal)
  }
  const archive = path.join(f.folder, 'archive')
  assert.deepEqual(fs.readdirSync(f.folder).filter(name => name.endsWith('.complete')), ['.run-20261108T18.complete'])
  assert.deepEqual(fs.readdirSync(archive).filter(name => name.endsWith('.complete')), ['.run-20261108T06.complete', '.run-20261108T12.complete'])
  fs.writeFileSync(path.join(archive, 'unrelated.grb2'), 'keep')
  fs.writeFileSync(path.join(f.folder, 'other-source__20261108T00__f000.grb2'), 'keep')
  const current = f.run('20261108T18')
  assert.equal(f.exporter.matches(current.source, current.stamp, current.version, 0), false)
  await f.exporter.export(current, async () => { throw new Error('Retention changes must not reacquire files') }, f.signal)
  assert.equal(f.exporter.matches(current.source, current.stamp, current.version, 0), true)
  assert.deepEqual(fs.readdirSync(archive), ['unrelated.grb2'])
  assert.equal(fs.readFileSync(path.join(f.folder, 'other-source__20261108T00__f000.grb2'), 'utf8'), 'keep')
})

test('invalid source, asset paths and receipts are rejected before writing export files', async t => {
  const f = local(t)
  for (const [source, stamp, id] of [['../other', '20261108T00', 'x'], ['gfs-0p25', '../stamp', 'x'], ['gfs-0p25', '20261108T00', '../outside_grb2']]) {
    assert.throws(() => legacyFilename(source, stamp, id), /Invalid/)
  }
  const run = f.run(); run.assets[0].sha256 = 'bad-hash'
  await assert.rejects(f.exporter.export(run, f.read, f.signal), /receipt/)
  assert.equal(fs.existsSync(f.root), false)
})

test('managed source symlinks cannot redirect exports into another directory', { skip: process.platform === 'win32' }, async t => {
  const f = local(t), outside = path.join(f.tmp, 'outside')
  fs.mkdirSync(f.root); fs.mkdirSync(outside)
  fs.symlinkSync(outside, f.folder)
  await assert.rejects(f.exporter.export(f.run(), f.read, f.signal), /requires a directory/)
  assert.deepEqual(fs.readdirSync(outside), [])
})

test('disabled legacy export creates no directories and reads no catalogue asset bodies', async t => {
  const f = await fixture(t), publisher = f.publisher()
  await publisher.publish(f.source, f.plan(), f.descriptor.fingerprint, () => {})
  const before = f.provider.calls.length
  await publisher.exportLatest(f.source, () => {})
  assert.equal(f.provider.calls.length, before)
  assert.equal(publisher.legacyExportStatus(f.source), undefined)
  assert.deepEqual(fs.readdirSync(f.root).sort(), ['catalogue-publications', 'token'])
})

test('committed catalogue originals export with version-bound authenticated URLs and no upstream reacquisition', async t => {
  const f = await fixture(t), exporter = new LegacyGribExporter(path.join(f.root, 'gribs')), publisher = f.publisher(exporter)
  const plan = f.plan(), ref = await publisher.publish(f.source, plan, f.descriptor.fingerprint, () => {})
  assert.equal(fs.existsSync(exporter.root), false)
  assert.equal(publisher.exportPending(f.source), true)
  const upstreamCalls = f.upstream.calls.length
  await publisher.exportLatest(f.source, () => {})
  assert.equal(publisher.exportPending(f.source), false)
  assert.equal(f.upstream.calls.length, upstreamCalls)
  const reads = f.provider.calls.filter(call => call.method === 'GET' && call.url.includes('/assets/'))
  assert.equal(reads.length, plan.assets.length)
  assert.ok(reads.every(call => call.url.includes(`/datasets/${ref.datasetId}/versions/${ref.version}/assets/`) && call.authorization === 'Bearer test-readwrite-token'))
  const filename = path.join(exporter.root, 'gfs-0p25', legacyFilename('gfs-0p25', plan.stamp, plan.assets[0].id))
  assert.deepEqual(fs.readFileSync(filename), f.bytes)
  fs.writeFileSync(filename, Buffer.alloc(f.bytes.length, 9))
  await publisher.exportLatest(f.source, () => {})
  assert.deepEqual(fs.readFileSync(filename), f.bytes)
  assert.deepEqual(f.bodies.get(plan.assets[0].id), f.bytes, 'exports are independent copies, never catalogue hard links')
})

test('an export disk failure is reported separately and can be retried without downloading weather data again', async t => {
  const f = await fixture(t), root = path.join(f.root, 'gribs')
  fs.writeFileSync(root, 'not a directory')
  const exporter = new LegacyGribExporter(root), publisher = f.publisher(exporter)
  await publisher.publish(f.source, f.plan(), f.descriptor.fingerprint, () => {})
  const upstream = f.upstream.calls.length, datasets = f.datasets.size
  const orch = new Orchestrator([f.source], publisher, () => {})
  await orch.downloadSource(f.source)
  assert.equal(orch.status()[0].lastError, null)
  assert.equal(orch.status()[0].legacyExport.state, 'error')
  assert.equal(publisher.exportPending(f.source), true)
  assert.equal(f.datasets.size, datasets)
  fs.unlinkSync(root)
  await orch.downloadSource(f.source)
  assert.equal(orch.status()[0].legacyExport.state, 'complete')
  assert.equal(f.upstream.calls.length, upstream)
  assert.equal(f.datasets.size, datasets)
  await orch.stop()
})

test('legacy export defaults to enabled and backfills an already-current catalogue run', async t => {
  const bbox = { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
  const f = await fixture(t, { bbox }), publisher = f.publisher()
  const plan = f.plan()
  await publisher.publish(f.source, plan, f.descriptor.fingerprint, () => {})
  const root = path.join(f.root, 'legacy'), routes = {}
  const server = { getDataDirPath: () => f.root, getSelfPath: () => 'online', debug: () => {}, setPluginStatus: () => {},
    streambundle: { getSelfStream: () => ({ onValue: () => () => {} }) } }
  const plugin = require('../dist/index')(server), router = {}
  for (const method of ['get', 'post', 'put', 'delete']) router[method] = (url, handler) => { routes[method + ' ' + url] = handler }
  plugin.registerWithRouter(router)
  t.after(() => plugin.stop())
  const upstream = f.upstream.calls.length
  plugin.start({ catalogueUrl: f.provider.base, catalogueTokenFile: f.tokenFile, gribsRoot: root, sources: [f.source], bbox })
  const marker = path.join(root, 'gfs-0p25', `.run-${plan.stamp}.complete`)
  const { waitFor } = require('./fixtures/http.cjs')
  await waitFor(() => fs.existsSync(marker))
  let status
  const res = { status: () => res, json: value => { status = value } }
  routes['get /status']({}, res)
  assert.equal(status.legacyExportEnabled, true)
  assert.equal(status.sources[0].legacyExport.state, 'complete')
  assert.equal(status.sources[0].legacyExport.directory, path.join(root, 'gfs-0p25'))
  assert.equal(f.upstream.calls.length, upstream)
  assert.equal(f.datasets.size, 1)
  await plugin.stop()
})

test('archive promotion failure restores the older active run instead of losing its files', async t => {
  const f = local(t), first = f.run(); first.archiveRuns = 1
  await f.exporter.export(first, f.read, f.signal)
  const next = f.run('20261108T06'); next.archiveRuns = 1
  const archive = path.join(f.folder, 'archive'), rename = fs.renameSync; let failed = false
  fs.renameSync = (from, to) => {
    if (!failed && path.dirname(to) === archive && path.basename(to).endsWith('__f003.grb2')) {
      failed = true; throw new Error('simulated archive rename failure')
    }
    return rename(from, to)
  }
  try { await assert.rejects(f.exporter.export(next, f.read, f.signal), /archive rename failure/) }
  finally { fs.renameSync = rename }
  assert.equal(failed, true)
  assert.equal(fs.existsSync(f.marker(first.stamp)), true)
  assert.equal(fs.existsSync(path.join(archive, `.run-${first.stamp}.complete`)), false)
  for (const asset of first.assets) assert.equal(fs.readFileSync(path.join(f.folder, legacyFilename(first.source, first.stamp, asset.id)), 'utf8'), asset.content)
  await f.exporter.export(next, async () => { throw new Error('No transfer should be needed for retention retry') }, f.signal)
  assert.equal(fs.existsSync(path.join(archive, `.run-${first.stamp}.complete`)), true)
  assert.equal(fs.existsSync(f.marker(first.stamp)), false)
})

test('a smaller same-run subset reuses matching files and removes obsolete timesteps', async t => {
  const f = local(t), first = f.run(); await f.exporter.export(first, f.read, f.signal)
  const next = f.run(first.stamp, [first.assets[0].content])
  await f.exporter.export(next, async () => { throw new Error('Existing unchanged file should be reused') }, f.signal)
  assert.equal(fs.existsSync(path.join(f.folder, 'gfs-0p25__20261108T00__f003.grb2')), false)
  assert.deepEqual(JSON.parse(fs.readFileSync(f.marker(first.stamp))), { params: next.params })
})

test('explicitly disabling the transition export is respected and leaves the target untouched', async t => {
  const bbox = { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
  const f = await fixture(t, { bbox }), plan = f.plan()
  await f.publisher().publish(f.source, plan, f.descriptor.fingerprint, () => {})
  const root = path.join(f.root, 'disabled-export'), routes = {}
  const server = { getDataDirPath: () => f.root, getSelfPath: () => 'online', debug: () => {}, setPluginStatus: () => {}, streambundle: { getSelfStream: () => ({ onValue: () => () => {} }) } }
  const plugin = require('../dist/index')(server), router = {}
  for (const method of ['get', 'post', 'put', 'delete']) router[method] = (url, handler) => { routes[method + ' ' + url] = handler }
  plugin.registerWithRouter(router); t.after(() => plugin.stop())
  const before = f.provider.calls.length
  plugin.start({ catalogueUrl: f.provider.base, catalogueTokenFile: f.tokenFile, legacyExportEnabled: false, gribsRoot: root, sources: [f.source], bbox })
  await new Promise(resolve => setTimeout(resolve, 25))
  let status
  const res = { status: () => res, json: value => { status = value } }
  routes['get /status']({}, res)
  assert.equal(status.legacyExportEnabled, false)
  assert.equal(status.sources[0].legacyExport, undefined)
  assert.equal(fs.existsSync(root), false)
  assert.equal(f.provider.calls.length, before)
  await plugin.stop()
})
