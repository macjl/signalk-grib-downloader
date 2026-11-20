'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { RemoteCatalogue, normalizeRemoteUrl } = require('../dist/remote-catalogue')
const { Orchestrator } = require('../dist/orchestrator')
const { sourceDirName } = require('../dist/scheduler')
const { server, fixture, grib, hash, API } = require('./fixtures/http.cjs')

const source = { model: 'arome', resolution: '0025', remoteServer: 'shore', autoDownload: false }
const datasetId = 'e62ec07f-f099-4a94-9e82-ac2fa21fded1'
const croppedId = '434211f2-ea42-4122-acb3-68eb65766202'
const jobId = 'a6cf22b2-a73a-4669-b421-cb89d60f2bbf'
const bbox = { lonMin: -6, latMin: 44, lonMax: -1, latMax: 48 }
const version = 'a'.repeat(64), cropVersion = 'b'.repeat(64)
const root = (id, v) => `${API}/datasets/${id}/versions/${v}`
const originalPath = root(datasetId, version), croppedPath = root(croppedId, cropVersion)
const jobPath = `${API}/requests/${jobId}`

async function setup(t, { processor = false, time = false, area = true, statusAvailable = true,
  userLevel = 'readwrite', missing = false, broken = false, unsafe = false, badHash = false, customModel = false,
  parameters, verifiedForecast, coverage, pendingArea = false, busy = 0 } = {}) {
  const original = grib(2048), crop = grib(256)
  const tokenDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-signalk-token-'))
  const tokenFile = path.join(tokenDir, 'token')
  fs.writeFileSync(tokenFile, 'remote-token')
  t.after(() => fs.rmSync(tokenDir, { recursive: true, force: true }))
  const manifest = (id, v, name, data) => ({
    id: name, href: root(id, v) + '/assets/' + name, size: data.length,
    sha256: badHash ? 'f'.repeat(64) : hash(data)
  })
  const row = {
    id: datasetId, version, state: 'complete', kind: 'forecast', format: 'grib2',
    run: '2026-11-09T15:00:00Z', model: 'AROME', producer: 'signalk-grib-downloader'
  }
  if (customModel) {
    delete row.run
    row.createdAt = '2026-11-09T15:33:00Z'
    row.model = 'WRF Manche / 3 km'
    row.producer = 'signalk-local-forecast'
    row.grid = { resolution: '3km' }
  }
  const fields = [
    { parameter: 'eastward_wind', level: { type: 'heightAboveGround', value: 10, unit: 'm' } },
    { parameter: 'air_pressure_at_mean_sea_level' }
  ]
  const main = { ...row, requested: { source: 'arome-0025', parameters },
    ...(coverage ? { bounds: coverage } : {}),
    ...(time === true ? { forecast: { from: '2026-11-09T15:00:00Z', to: '2026-11-10T03:00:00Z' } } : {}),
    ...(verifiedForecast ? { forecast: verifiedForecast } : {}),
    assets: [
      manifest(datasetId, version, 'arome-0025__run__first_grb2', original),
      manifest(datasetId, version, 'arome-0025__run__second_grb2', original)
    ],
    capabilities: { operations: processor ? [{ operation: 'export', href: originalPath + '/requests',
      selectors: [...(area ? ['bbox'] : []), ...(time ? ['from', 'to', 'fields'] : [])],
      ...(pendingArea ? { pendingSelectors: ['bbox'] } : {}),
      ...(time === true ? { fields, forecastInterval: { stepSeconds: 3600, maxHours: 51 } } : {}) }] : [] }
  }
  if (unsafe) main.assets[0].href = 'https://unrelated.invalid/asset'
  const derived = { ...row, id: croppedId, version: cropVersion, producer: 'signalk-grib-processor',
    provenance: { source: { id: datasetId, version } },
    bounds: [-6, 44, -1, 48],
    assets: [manifest(croppedId, cropVersion, 'forecast_grib2', crop)]
  }
  const requests = [], received = []
  const peer = await server(t, async (req, res) => {
    assert.equal(req.headers.authorization, 'Bearer remote-token')
    const url = req.url.split('?')[0]
    requests.push([req.method, url])
    const json = (status, data) => {
      res.writeHead(status, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(data))
    }
    if (url === API + '/datasets') {
      const model = new URL(req.url, 'http://localhost').searchParams.get('model')
      return json(200, !model || model === row.model ? {
        [datasetId]: row,
        mirror: { ...row, id: 'mirror', model: 'Looped model', provenance: { channel: 'signalk' } },
        temporary: { ...row, id: 'temporary', producer: 'signalk-grib-processor', model: 'Temporary crop',
          expiresAt: '2099-01-01T00:00:00Z' },
      } : {})
    }
    if (url === API + '/status') return json(200, statusAvailable
      ? { operations: ['export'], processing: 'available' } : { operations: [], processing: 'not-implemented' })
    if (url === '/skServer/loginStatus') return json(200, { status: 'loggedIn', userLevel })
    if (url === originalPath) return json(200, main)
    if (url === originalPath + '/requests') {
      let bytes = ''
      for await (const chunk of req) bytes += chunk
      received.push(JSON.parse(bytes))
      if (busy > 0) {
        busy--
        res.setHeader('Retry-After', '0')
        return json(429, { code: 'PROCESSING_BUSY' })
      }
      if (missing) return json(503, { code: 'PROCESSOR_UNAVAILABLE' })
      return json(202, { id: jobId, href: jobPath, state: 'running' })
    }
    if (url === jobPath) {
      const { bbox, from, to } = received.at(-1).selection
      const forecast = time === 'range'
        ? { from: '2026-11-09T15:30:00Z', to: '2026-11-09T20:15:00Z' } : { from, to }
      return json(200, broken ? { id: jobId, href: jobPath, state: 'failed' }
        : { id: jobId, href: jobPath, state: 'complete', result: { ...derived, bounds: bbox, ...(from ? { forecast } : {}) } })
    }
    if (main.assets.some(asset => asset.href === url)) return res.end(original)
    if (derived.assets[0].href === url) return res.end(crop)
    return json(404, { code: 'DATASET_NOT_FOUND' })
  })
  const f = await fixture(t)
  const adapter = new RemoteCatalogue({ id: 'shore', url: peer.base, tokenFile })
  const map = new Map([['shore', adapter]])
  const orchestrator = (area, setting = source) => new Orchestrator([setting], f.publisher(), () => {}, () => {}, area, map)
  return { f, orchestrator, requests, received, original, crop, peer, adapter, tokenFile, fields }
}

for (const [options, reason] of [
  [{}, undefined],
  [{ processor: true, statusAvailable: false }, 'processing-unavailable'],
  [{ processor: true, userLevel: 'readonly' }, 'write-required'],
]) {
  test(`Signal K source with ${reason ?? 'no crop capability'} imports all original assets`, async t => {
    const { f, orchestrator, requests, received, original, adapter } = await setup(t, options)
    const preview = await adapter.preview('AROME', '0025', AbortSignal.timeout(5000))
    assert.equal(preview.area, false)
    assert.equal(preview.horizon, undefined)
    assert.equal(preview.reason, reason)
    assert.ok(requests.every(([method, url]) => method === 'GET' && !url.includes('/assets/')))
    const orch = orchestrator(bbox)
    assert.equal(await orch.downloadSource(source, 'area'), false, 'a previously previewed crop must not become a full download')
    assert.equal(f.declarations.length, 0)
    assert.equal(await orch.downloadSource(source), true, orch.status()[0].lastError)
    assert.equal(orch.status()[0].lastOutcome, 'downloaded')
    const [declaration] = f.declarations
    assert.equal(declaration.requested.source, 'signalk-shore__arome-0025')
    assert.equal(sourceDirName({ ...source, remoteServer: undefined }), 'arome-0025')
    assert.equal(declaration.provenance.channel, 'signalk')
    assert.equal(declaration.provenance.mode, 'complete')
    assert.deepEqual(declaration.provenance.remoteSource, { id: datasetId, version })
    assert.equal(declaration.assets.length, 2)
    assert.ok(declaration.assets.every(asset => asset.size === original.length && asset.sha256 === hash(original)))
    const previousRequests = requests.filter(([, url]) => url.includes('/assets/')).length
    assert.equal(await orch.downloadSource(source), true)
    assert.equal(orch.status()[0].lastOutcome, 'up-to-date')
    assert.equal(requests.filter(([, url]) => url.includes('/assets/')).length, previousRequests)
    assert.equal(f.datasets.size, 1)
    assert.equal(received.length, 0)
    assert.ok(f.provider.calls.every(call => call.authorization === 'Bearer test-readwrite-token'))
  })
}

test('sends the selected map area, downloads only the crop, and repeats on a changed area of the same cycle', async t => {
  const { f, orchestrator, requests, received, crop } = await setup(t, { processor: true })
  const first = orchestrator(bbox)
  assert.equal(await first.downloadSource(source), true, first.status()[0].lastError)
  assert.deepEqual(received[0].selection.bbox, [-6, 44, -1, 48])
  assert.equal(f.declarations[0].assets[0].size, crop.length)
  assert.equal(f.declarations[0].provenance.mode, 'subset')
  assert.deepEqual(f.declarations[0].provenance.remoteContent, { id: croppedId, version: cropVersion })
  assert.ok(requests.filter(([, url]) => url.includes('/assets/')).every(([, url]) => url.startsWith(croppedPath)))
  assert.equal(await first.downloadSource(source), true)
  assert.equal(received.length, 1, 'a repeat does not submit another crop job')
  const second = orchestrator({ ...bbox, lonMin: -5.99 })
  assert.equal(await second.downloadSource(source), true, second.status()[0].lastError)
  assert.equal(received.length, 2)
  assert.deepEqual(received[1].selection.bbox, [-5.99, 44, -1, 48])
  assert.equal(f.datasets.size, 1)
})

test('a remote export uses the overlap, retries a busy slot and imports only the crop', async t => {
  const { adapter, f, orchestrator, requests, received } = await setup(t, {
    processor: true, coverage: [-5, 45, 9, 55], busy: 1
  })
  assert.deepEqual(await adapter.preview('AROME', '0025', AbortSignal.timeout(3000), [-6, 44, -1, 48]), {
    area: true, bounds: [-5, 45, 9, 55], horizon: undefined, reason: undefined, areaClipped: true
  })
  const orch = orchestrator(bbox)
  assert.equal(await orch.downloadSource(source, 'area'), true, orch.status()[0].lastError)
  assert.deepEqual(received.map(request => request.selection.bbox), [[-5, 45, -1, 48], [-5, 45, -1, 48]])
  assert.equal(f.datasets.size, 1)
  assert.equal(orch.status()[0].configStale, false)
  assert.ok(requests.filter(([, url]) => url.includes('/assets/')).every(([, url]) => url.startsWith(croppedPath)))
})

test('no remote forecast is downloaded for an empty map overlap, with or without a processor', async t => {
  for (const processor of [false, true]) {
    const { adapter, f, orchestrator, requests } = await setup(t, { processor, coverage: [0, 38, 16, 55] })
    assert.deepEqual(await adapter.preview('AROME', '0025', AbortSignal.timeout(3000), [-6, 44, -1, 48]), {
      area: false, bounds: [0, 38, 16, 55], reason: 'outside-coverage'
    })
    const orch = orchestrator(bbox)
    assert.equal(await orch.downloadSource(source), true)
    assert.equal(orch.status()[0].lastOutcome, 'unavailable')
    assert.equal(f.datasets.size, 0)
    assert.ok(requests.every(([method, url]) => method !== 'POST' && !url.includes('/assets/')))
  }
})

test('a pending grid analysis does not cause a premature full download', async t => {
  const { adapter, f, orchestrator, requests } = await setup(t, { processor: true, area: false, pendingArea: true })
  assert.equal((await adapter.preview('AROME', '0025', AbortSignal.timeout(3000), [-6, 44, -1, 48])).reason, 'coverage-pending')
  const orch = orchestrator(bbox)
  assert.equal(await orch.downloadSource(source), false)
  assert.match(orch.status()[0].lastError, /still being analyzed/)
  assert.equal(f.datasets.size, 0)
  assert.ok(requests.every(([method, url]) => method !== 'POST' && !url.includes('/assets/')))
})

for (const area of [false, true]) {
  test(`previews and downloads a forecast interval ${area ? 'with' : 'without'} spatial cropping`, async t => {
    const { f, orchestrator, requests, received, adapter, fields } = await setup(t, { processor: true, time: true, area })
    assert.deepEqual(await adapter.preview('AROME', '0025', AbortSignal.timeout(5000)), {
      area, horizon: { minHours: 0, maxHours: 12, stepHours: 1 }, reason: undefined
    })
    assert.ok(requests.every(([method, url]) => method === 'GET' && !url.includes('/assets/')))
    const short = { ...source, remoteHours: 6 }
    const orch = orchestrator(bbox, short)
    assert.equal(await orch.downloadSource(short, area ? 'both' : 'time'), true, orch.status()[0].lastError)
    const forecast = { from: '2026-11-09T15:00:00.000Z', to: '2026-11-09T21:00:00.000Z' }
    assert.deepEqual(received[0].selection, { ...forecast, fields, ...(area ? { bbox: [-6, 44, -1, 48] } : {}) })
    assert.equal(f.declarations[0].provenance.mode, 'subset')
    assert.deepEqual(f.declarations[0].provenance.forecast, forecast)
    assert.deepEqual(f.declarations[0].metadata.forecast, forecast)
    assert.equal(orch.status()[0].upToDate, true)
    assert.equal(await orch.downloadSource(short), true)
    assert.equal(received.length, 1)
    const shorter = { ...short, remoteHours: 3 }
    const next = orchestrator(bbox, shorter)
    assert.equal(next.status()[0].configStale, true)
    assert.equal(await next.downloadSource(shorter), true, next.status()[0].lastError)
    assert.equal(received[1].selection.to, '2026-11-09T18:00:00.000Z')
    assert.ok(requests.filter(([, url]) => url.includes('/assets/')).every(([, url]) => url.startsWith(croppedPath)))
  })
}

test('uses a remote time range without published fields or a fixed forecast cadence', async t => {
  const { f, orchestrator, received, adapter } = await setup(t, { processor: true, time: 'range', area: false })
  assert.deepEqual(await adapter.preview('AROME', '0025', AbortSignal.timeout(5000)), {
    area: false, horizon: { minHours: 0, stepHours: 1 }, reason: undefined
  })
  const selected = { ...source, remoteHours: 6 }
  const orch = orchestrator(bbox, selected)
  assert.equal(await orch.downloadSource(selected, 'time'), true, orch.status()[0].lastError)
  assert.deepEqual(received[0].selection, {
    from: '2026-11-09T15:00:00.000Z', to: '2026-11-09T21:00:00.000Z'
  })
  assert.deepEqual(f.declarations[0].metadata.forecast, {
    from: '2026-11-09T15:30:00Z', to: '2026-11-09T20:15:00Z'
  })
})

test('uses the acquisition groups of a complete remote run to bound the slider', async t => {
  const { adapter } = await setup(t, { processor: true, time: 'range',
    parameters: { groups: ['00H06H', '07H12H', '13H18H', '19H24H'] } })
  const signal = AbortSignal.timeout(5000)
  assert.deepEqual((await adapter.preview('AROME', '0025', signal)).horizon, {
    minHours: 0, maxHours: 24, stepHours: 1
  })
  const candidate = await adapter.find({ ...source, remoteHours: 8 }, bbox, signal)
  assert.deepEqual(candidate.selection, {
    bbox: [-6, 44, -1, 48], from: '2026-11-09T15:00:00.000Z', to: '2026-11-09T23:00:00.000Z'
  })
})

test('prefers verified irregular validity times over the acquisition groups and snaps the export cutoff', async t => {
  const times = ['2026-11-09T15:30:00Z', '2026-11-09T17:00:00Z', '2026-11-09T20:15:00Z']
  const { adapter, orchestrator, received } = await setup(t, { processor: true, time: 'range', area: false,
    parameters: { groups: ['00H06H'] }, verifiedForecast: { from: times[0], to: times.at(-1), times } })
  const signal = AbortSignal.timeout(5000)
  assert.deepEqual((await adapter.preview('AROME', '0025', signal)).horizon, {
    minHours: 0.5, maxHours: 5.25, leads: [0.5, 2, 5.25]
  })
  const lower = await adapter.find({ ...source, remoteHours: 3 }, bbox, signal)
  assert.equal(lower.selection.to, '2026-11-09T17:00:00.000Z')
  const configured = { ...source, remoteHours: 5.25 }
  const orch = orchestrator(bbox, configured)
  assert.equal(await orch.downloadSource(configured, 'time'), true, orch.status()[0].lastError)
  assert.deepEqual(received[0].selection, {
    from: '2026-11-09T15:30:00.000Z', to: '2026-11-09T20:15:00.000Z'
  })
})

test('failed crops and invalid remote links or checksums do not silently download or commit originals', async t => {
  for (const options of [{ processor: true, broken: true }, { processor: true, missing: true }, { unsafe: true }, { badHash: true }]) {
    const { f, orchestrator, requests } = await setup(t, options)
    const orch = orchestrator(bbox)
    assert.equal(await orch.downloadSource(source), false)
    assert.equal(orch.status()[0].lastOutcome, 'failed')
    assert.equal(f.datasets.size, 0)
    if (!options.badHash) assert.ok(requests.every(([, url]) => !url.includes('/assets/')))
  }
})

test('remote server URLs require HTTPS off loopback and cannot contain credentials', () => {
  assert.throws(() => normalizeRemoteUrl('http://shore.example'))
  assert.throws(() => normalizeRemoteUrl('https://user:pass@shore.example'))
  assert.equal(normalizeRemoteUrl('https://shore.example/signalk/'), 'https://shore.example/signalk')
})

test('discovers and downloads a named GRIB2 model published by a different producer', async t => {
  const { f, orchestrator, adapter } = await setup(t, { customModel: true })
  const model = 'WRF Manche / 3 km'
  assert.deepEqual(await adapter.models(AbortSignal.timeout(5000)), [{ model, resolutions: ['3km'] }])
  assert.equal(sourceDirName({ ...source, remoteModel: 'AROME' }), sourceDirName(source), 'existing remote identity is stable')
  const custom = { ...source, remoteModel: model, resolution: '3km' }
  const name = sourceDirName(custom)
  assert.match(name, /^signalk-shore__model-[a-f0-9]{24}-3km$/)
  const orch = orchestrator(bbox, custom)
  assert.ok(Number.isFinite(orch.nextTickAt(custom).getTime()))
  assert.equal(await orch.downloadSource(custom), true, orch.status()[0].lastError)
  assert.equal(orch.status()[0].model, model)
  const record = [...f.datasets.values()][0]
  assert.equal(record.metadata.model, model)
  assert.match(record.metadata.name, /WRF Manche \/ 3 km/)
  assert.equal(record.metadata.run, undefined, 'catalogue creation time is not a model cycle')
  assert.equal(record.requested.source, name)
  assert.equal(record.assets.length, 2)
})
