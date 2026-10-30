'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')
const http = require('http')
const { Writable } = require('stream')

const {
  toDownloadSource,
  fetchSource,
  latestCompleteStamp,
  fetchFingerprint,
  fingerprintsEqual,
  loadBz2,
} = require('../dist/downloader.js')
const { sourceDirName } = require('../dist/scheduler.js')

// ── Local HTTP test server ──────────────────────────────────────────────────
// Serves deterministic bytes per request so the fetchers can be exercised
// without any live upstream. Records the request log so tests can assert
// probe + GET behaviour (retries, ordering, subsetting params).
function startServer(handler) {
  const log = []
  const server = http.createServer((req, res) => {
    log.push({ method: req.method, url: req.url })
    try {
      handler(req, res)
    } catch (e) {
      res.statusCode = 500
      res.end(String(e))
    }
  })
  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', () => {
      const base = `http://127.0.0.1:${server.address().port}`
      // closeAllConnections: a stalled/dangling request can never hang the suite.
      const close = () => new Promise((r) => { server.closeAllConnections(); server.close(() => r()) })
      resolve({ base, log, close })
    })
    server.on('error', reject)
  })
}

// Payload builder: N bytes of predictable content so tests can assert sizes.
function payload(n, seed = 1) {
  const b = Buffer.alloc(n)
  for (let i = 0; i < n; i++) b[i] = (i * seed) & 0xff
  return b
}

// Source data lives in <gribsRoot>/<name>; the fetcher creates that subdir.
function srcDir(gribsRoot, setting) {
  return path.join(gribsRoot, sourceDirName(setting))
}

// Poll `cond` until truthy or `timeoutMs` elapses; resolves to the last result.
async function waitFor(cond, timeoutMs = 5000, stepMs = 10) {
  const start = Date.now()
  while (!cond() && Date.now() - start < timeoutMs) {
    await new Promise((r) => setTimeout(r, stepMs))
  }
  return cond()
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

let tmpdir

test.before(async () => {
  tmpdir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'grib-dl-'))
})

// ── Fingerprint parity ──────────────────────────────────────────────────────

test('fingerprint is order-independent across the parameter object', () => {
  // The fingerprint normalizes object keys to a canonical sorted order, so
  // two equivalent parameter sets compare equal regardless of insertion order.
  const a = fetchFingerprint({ model: 'gfs', resolution: '0p25', hours: 24 },
    { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  // Re-fetching the same settings yields the same fingerprint.
  const b = fetchFingerprint({ model: 'gfs', resolution: '0p25', hours: 24 },
    { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  assert.ok(fingerprintsEqual(a, b))
})

test('fingerprint changes when the download area changes', () => {
  const a = fetchFingerprint({ model: 'gfs', hours: 24 },
    { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  const b = fetchFingerprint({ model: 'gfs', hours: 24 },
    { latMin: 36, lonMin: -6, latMax: 45, lonMax: 17 })
  assert.ok(!fingerprintsEqual(a, b))
})

test('toDownloadSource resolves the directory under the local gribsRoot', () => {
  const ds = toDownloadSource(
    { model: 'gfs', resolution: '0p25', hours: 24 },
    '/var/gribs',
    { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
  )
  assert.strictEqual(ds.name, 'gfs-0p25')
  assert.strictEqual(ds.directory, '/var/gribs/gfs-0p25')
  assert.deepStrictEqual(ds.bbox, [35, -6, 45, 17])
  assert.deepStrictEqual(ds.steps, { from: 0, to: 24, by: 3 })
  assert.strictEqual(ds.archiveRuns, 0)
})

// ── Marker round-trip ───────────────────────────────────────────────────────

test('latestCompleteStamp reads back a written run marker', () => {
  const dir = path.join(tmpdir, 'marker-rt')
  fs.mkdirSync(dir, { recursive: true })
  const fp = fetchFingerprint({ model: 'gfs', hours: 24 }, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  fs.writeFileSync(path.join(dir, '.run-20260101T00.complete'), JSON.stringify({ params: fp }))
  assert.strictEqual(latestCompleteStamp(dir), '20260101T00')
})

// ── GFS smoke test (probe + subset download + marker) ───────────────────────

test('gfs: downloads one file per step, writes a marker, reports downloaded', async () => {
  const root = path.join(tmpdir, 'gfs-run')
  const setting = { model: 'gfs', resolution: '0p25', hours: 6 }     // steps 0,3,6
  const srv = await startServer((req, res) => {
    if (req.url.endsWith('.idx')) { res.end('idx\n'); return }       // probe
    res.end(payload(16, 7))                                           // grib_filter CGI
  })

  const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  ds.base = srv.base
  const log = []
  const outcome = await fetchSource(ds, (m) => log.push(m))

  assert.strictEqual(outcome, 'downloaded')
  const dir = srcDir(root, setting)
  const files = fs.readdirSync(dir).filter(f => f.endsWith('.grb2'))
  assert.strictEqual(files.length, 3)                                // f000, f003, f006
  assert.ok(srv.log.some(e => e.method === 'HEAD' && e.url.endsWith('.idx')), 'probed the .idx')
  assert.ok(srv.log.some(e => e.url.includes('leftlon=-6') && e.url.includes('toplat=45')), 'bbox subset params sent')
  assert.ok(srv.log.some(e => e.url.includes('lev_850_mb=on')), '850 mb level requested (front-detection fields: TMP/UGRD/VGRD @ 850)')
  assert.ok(latestCompleteStamp(dir) !== null, 'marker written')
  await srv.close()
})

test('gfs waves: combines every step and atomically publishes a complete OpenCPN run', async () => {
  const root = path.join(tmpdir, 'gfs-wave-run')
  const exportRoot = path.join(tmpdir, 'opencpn-exports')
  const setting = { model: 'gfs', resolution: '0p25', hours: 6, includeWaves: true }
  const atmosPayload = Buffer.from('ATMOSPHERE')
  const wavePayload = Buffer.from('WAVES')
  const srv = await startServer((req, res) => {
    if (req.method === 'HEAD') { res.end('idx\n'); return }
    res.end(req.url.includes('filter_gfswave.pl') ? wavePayload : atmosPayload)
  })

  const ds = toDownloadSource(
    setting,
    root,
    { latMin: 11, lonMin: -62, latMax: 13, lonMax: -60 },
    exportRoot
  )
  ds.base = srv.base
  const outcome = await fetchSource(ds, () => {})

  assert.strictEqual(outcome, 'downloaded')
  const dir = srcDir(root, setting)
  const stepFiles = fs.readdirSync(dir).filter(f => f.endsWith('.grb2')).sort()
  assert.strictEqual(stepFiles.length, 3)
  const expectedStep = Buffer.concat([atmosPayload, wavePayload])
  for (const file of stepFiles) {
    assert.ok(fs.readFileSync(path.join(dir, file)).equals(expectedStep), `${file} contains atmosphere then waves`)
  }
  assert.strictEqual(fs.readdirSync(dir).filter(f => /\.(atmos|wave|part)$/.test(f)).length, 0, 'no component or partial files remain')
  const exportDir = path.join(exportRoot, 'gfs-0p25')
  const bundles = fs.readdirSync(exportDir).filter(f => f.endsWith('__complete.grb2'))
  assert.strictEqual(bundles.length, 1)
  assert.ok(fs.readFileSync(path.join(exportDir, bundles[0])).equals(Buffer.concat([expectedStep, expectedStep, expectedStep])))
  assert.strictEqual(fs.readdirSync(exportDir).filter(f => f.endsWith('.part')).length, 0, 'bundle is atomically published')
  assert.ok(srv.log.some(e => e.method === 'HEAD' && e.url.includes('/wave/gridded/')), 'wave publication is probed')
  const waveRequests = srv.log.filter(e => e.method === 'GET' && e.url.includes('filter_gfswave.pl'))
  assert.strictEqual(waveRequests.length, 3)
  assert.ok(waveRequests.every(e => e.url.includes('var_HTSGW=on') && e.url.includes('var_WVDIR=on') && e.url.includes('var_WVPER=on')))
  assert.ok(waveRequests.every(e => e.url.includes('leftlon=-62') && e.url.includes('toplat=13')), 'wave request uses the same bbox')
  assert.ok(latestCompleteStamp(dir) !== null, 'marker is written only after bundle publication')

  const bundlePath = path.join(exportDir, bundles[0])
  fs.unlinkSync(bundlePath)
  const getCount = srv.log.filter(e => e.method === 'GET').length
  const recoveryLog = []
  assert.strictEqual(await fetchSource(ds, line => recoveryLog.push(line)), 'downloaded')
  assert.ok(recoveryLog.some(line => line.includes('bundle missing')), 'missing export is detected')
  assert.ok(fs.existsSync(bundlePath), 'missing export is rebuilt from completed step files')
  assert.strictEqual(srv.log.filter(e => e.method === 'GET').length, getCount, 'rebuild does not redownload completed steps')
  await srv.close()
})

test('gfs waves: rejects resolutions that cannot share the 0.25 degree wave grid', async () => {
  const root = path.join(tmpdir, 'gfs-wave-wrong-resolution')
  const setting = { model: 'gfs', resolution: '0p50', hours: 6, includeWaves: true }
  const ds = toDownloadSource(setting, root, undefined, path.join(tmpdir, 'exports-wrong-resolution'))
  const logs = []
  const outcome = await fetchSource(ds, line => logs.push(line))
  assert.strictEqual(outcome, 'failed')
  assert.ok(logs.some(line => line.includes('requires GFS 0p25')))
  assert.strictEqual(latestCompleteStamp(srcDir(root, setting)), null)
})

test('gfs waves: rejects an export directory that overlaps the provider source', async () => {
  const root = path.join(tmpdir, 'gfs-wave-overlap')
  const setting = { model: 'gfs', resolution: '0p25', hours: 6, includeWaves: true }
  const ds = toDownloadSource(setting, root, undefined, root)
  const logs = []
  const outcome = await fetchSource(ds, line => logs.push(line))
  assert.strictEqual(outcome, 'failed')
  assert.ok(logs.some(line => line.includes('must be separate')))
  assert.strictEqual(latestCompleteStamp(srcDir(root, setting)), null)
})

test('gfs waves: failed same-cycle replacement preserves the previous complete run', async () => {
  const root = path.join(tmpdir, 'gfs-wave-safe-replacement')
  const exportRoot = path.join(tmpdir, 'gfs-wave-safe-replacement-export')
  const plainSetting = { model: 'gfs', resolution: '0p25', hours: 0 }
  const waveSetting = { ...plainSetting, includeWaves: true }
  let failWave = false
  const atmosPayload = Buffer.from('OLD-ATMOSPHERE')
  const wavePayload = Buffer.from('NEW-WAVES')
  const srv = await startServer((req, res) => {
    if (req.method === 'HEAD') { res.end('idx\n'); return }
    if (req.url.includes('filter_gfswave.pl')) {
      if (failWave) { res.statusCode = 500; res.end(); return }
      res.end(wavePayload)
      return
    }
    res.end(atmosPayload)
  })

  const plain = toDownloadSource(plainSetting, root, { latMin: 11, lonMin: -62, latMax: 13, lonMax: -60 })
  plain.base = srv.base
  assert.strictEqual(await fetchSource(plain, () => {}), 'downloaded')
  const dir = srcDir(root, plainSetting)
  const stamp = latestCompleteStamp(dir)
  assert.ok(stamp)
  const oldStep = path.join(dir, `gfs-0p25__${stamp}__f000.grb2`)
  const oldMarker = path.join(dir, `.run-${stamp}.complete`)
  const oldStepBytes = fs.readFileSync(oldStep)
  const oldMarkerBytes = fs.readFileSync(oldMarker)

  const waves = toDownloadSource(
    waveSetting,
    root,
    { latMin: 11, lonMin: -62, latMax: 13, lonMax: -60 },
    exportRoot
  )
  waves.base = srv.base
  failWave = true
  assert.strictEqual(await fetchSource(waves, () => {}), 'failed')
  assert.ok(fs.readFileSync(oldStep).equals(oldStepBytes), 'old step survives failed replacement byte-for-byte')
  assert.ok(fs.readFileSync(oldMarker).equals(oldMarkerBytes), 'old completion marker survives failed replacement')
  assert.strictEqual(latestCompleteStamp(dir), stamp)

  failWave = false
  assert.strictEqual(await fetchSource(waves, () => {}), 'downloaded')
  assert.ok(fs.readFileSync(oldStep).equals(Buffer.concat([atmosPayload, wavePayload])), 'successful replacement publishes combined step')
  assert.ok(JSON.parse(fs.readFileSync(oldMarker, 'utf8')).params.wave_variables.includes('HTSGW'))
  assert.ok(fs.existsSync(path.join(exportRoot, 'gfs-0p25', `gfs-0p25__${stamp}__complete.grb2`)))
  assert.strictEqual(fs.readdirSync(dir).filter(file => file.includes('replacement-')).length, 0, 'staging and backup directories are removed')

  const combinedStepBytes = fs.readFileSync(oldStep)
  const waveMarkerBytes = fs.readFileSync(oldMarker)
  const changedArea = toDownloadSource(
    waveSetting,
    root,
    { latMin: 10, lonMin: -63, latMax: 14, lonMax: -59 },
    exportRoot
  )
  changedArea.base = srv.base
  const realRenameSync = fs.renameSync
  fs.renameSync = (from, to) => {
    if (String(from).includes('.replacement-staging-') && String(from).endsWith('.grb2') && String(to).endsWith('.grb2')) {
      throw new Error('simulated commit failure')
    }
    return realRenameSync(from, to)
  }
  try {
    assert.strictEqual(await fetchSource(changedArea, () => {}), 'failed')
  } finally {
    fs.renameSync = realRenameSync
  }
  assert.ok(fs.readFileSync(oldStep).equals(combinedStepBytes), 'commit failure restores the previous combined step')
  assert.ok(fs.readFileSync(oldMarker).equals(waveMarkerBytes), 'commit failure restores the previous marker')
  await srv.close()
})

// The fingerprint drives the up-to-date check: a run fetched before the 850 mb
// level joined the defaults carries a marker without variables/levels and must
// re-fetch rather than report up-to-date.
test('gfs: fingerprint includes the resolved variables and levels', async () => {
  const setting = { model: 'gfs', resolution: '0p25', hours: 6 }
  const bbox = { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
  const fp = fetchFingerprint(setting, bbox)
  assert.ok(JSON.stringify(fp).includes('850_mb'), '850 mb level in the fingerprint')
  assert.ok(JSON.stringify(fp).includes('UGRD'), 'variables in the fingerprint')
  // A pre-850 marker fingerprint (no variables/levels keys) never matches.
  const legacy = { resolution: '0p25', steps: { from: 0, to: 6, by: 3 }, bbox: [35, -6, 45, 17] }
  assert.ok(!fingerprintsEqual(legacy, fp), 'legacy marker without variables/levels is stale')
})

// ── Météo-France smoke test (AROME: packages × groups) ───────────────────────

test('arome: downloads each package×group, writes a marker', async () => {
  const root = path.join(tmpdir, 'arome-run')
  const setting = { model: 'arome', resolution: '0025', hours: 18 }  // 3 groups
  const srv = await startServer((req, res) => {
    // Assert the requested path has no unreplaced placeholders
    assert.ok(!req.url.includes('{d}') && !req.url.includes('{p}') && !req.url.includes('{g}'),
      `URL contains unreplaced placeholders: ${req.url}`)
    // SP1 should appear in both the directory and filename
    const sp1Count = (req.url.match(/SP1/g) || []).length
    assert.strictEqual(sp1Count, 2, `SP1 should appear twice in URL, found ${sp1Count}: ${req.url}`)
    if (req.method === 'HEAD') { res.end(); return }                 // probe
    res.end(payload(32, 3))
  })

  const ds = toDownloadSource(setting, root)
  ds.base = srv.base
  const outcome = await fetchSource(ds, () => {})

  assert.strictEqual(outcome, 'downloaded')
  const dir = srcDir(root, setting)
  const files = fs.readdirSync(dir).filter(f => f.endsWith('.grb2'))
  assert.strictEqual(files.length, 3)                                // 3 groups × 1 package
  assert.ok(latestCompleteStamp(dir) !== null, 'marker written')
  await srv.close()
})

// ── ICON-EU smoke test (bz2 decompress + concat per step) ───────────────────

test('icon-eu: decompresses bz2 fragments and concatenates per step', async () => {
  const root = path.join(tmpdir, 'icon-run')
  const setting = { model: 'icon-eu', hours: 0 }                     // step f000 only
  // Real bzip2 streams so the wasm decompressor path is exercised end-to-end.
  // loadBz2() pre-feeds the wasm binary, dodging the package's Windows path
  // resolution bug — the very path under test.
  const bz2 = await loadBz2()
  const gribA = payload(64, 11)
  const gribB = payload(64, 23)
  const compA = Buffer.from(bz2.compress(gribA, 5, gribA.length + 1024))
  const compB = Buffer.from(bz2.compress(gribB, 5, gribB.length + 1024))

  const srv = await startServer((req, res) => {
    if (req.method === 'HEAD') { res.end(); return }                 // probe
    res.end(req.url.includes('t_2m') ? compA : compB)
  })

  const ds = toDownloadSource(setting, root)
  ds.variables = ['t_2m', 'u_10m']                                  // 2 vars
  ds.base = srv.base
  const outcome = await fetchSource(ds, () => {})

  assert.strictEqual(outcome, 'downloaded')
  const dir = srcDir(root, setting)
  const files = fs.readdirSync(dir).filter(f => f.endsWith('.grb2'))
  assert.strictEqual(files.length, 1)
  const out = fs.readFileSync(path.join(dir, files[0]))
  assert.strictEqual(out.length, gribA.length + gribB.length)
  assert.ok(out.equals(Buffer.concat([gribA, gribB])), 'concatenated GRIB matches')
  assert.ok(latestCompleteStamp(dir) !== null, 'marker written')
  await srv.close()
})

// ── up-to-date path: existing marker with matching fingerprint ───────────────

test('gfs: reports up-to-date when the latest marker matches the settings', async () => {
  const root = path.join(tmpdir, 'gfs-uptodate')
  const setting = { model: 'gfs', resolution: '0p25', hours: 6 }
  const dir = srcDir(root, setting)
  fs.mkdirSync(dir, { recursive: true })
  const srv = await startServer((req, res) => { res.end(payload(16)) })

  const fp = fetchFingerprint(setting, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  // Write markers for every recent 6h cycle stamp (newest-first) so the
  // fetcher's newest candidate — computed with its own Date.now() — matches
  // a written marker and reports up-to-date instead of downloading.
  const now = new Date(Date.now() - 3.5 * 3_600_000)
  now.setUTCMinutes(0, 0, 0)
  const cycleH = Math.floor(now.getUTCHours() / 6) * 6
  for (let i = 0; i < 4; i++) {
    const t = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), cycleH, 0, 0))
    t.setUTCHours(cycleH - 6 * i)
    const stamp = `${t.getUTCFullYear()}${String(t.getUTCMonth() + 1).padStart(2, '0')}${String(t.getUTCDate()).padStart(2, '0')}T${String(t.getUTCHours()).padStart(2, '0')}`
    fs.writeFileSync(path.join(dir, `.run-${stamp}.complete`), JSON.stringify({ params: fp }))
  }

  const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  ds.base = srv.base
  const outcome = await fetchSource(ds, () => {})

  assert.strictEqual(outcome, 'up-to-date')
  assert.strictEqual(srv.log.length, 0, 'no HTTP calls for an up-to-date run')
  await srv.close()
})

// ── unavailable path: probe fails for every candidate ────────────────────────

test('gfs: reports unavailable when no run is published yet', async () => {
  const root = path.join(tmpdir, 'gfs-unavail')
  const setting = { model: 'gfs', resolution: '0p25', hours: 6 }
  const srv = await startServer((req, res) => { res.statusCode = 404; res.end() })

  const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  ds.base = srv.base
  const outcome = await fetchSource(ds, () => {})

  assert.strictEqual(outcome, 'unavailable')
  await srv.close()
})

// ── failed path: a step download fails ───────────────────────────────────────

test('gfs: reports failed when a step download returns 500', async () => {
  const root = path.join(tmpdir, 'gfs-fail')
  const setting = { model: 'gfs', resolution: '0p25', hours: 6 }
  const srv = await startServer((req, res) => {
    if (req.url.endsWith('.idx')) { res.end('idx\n'); return }
    res.statusCode = 500
    res.end()
  })

  const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  ds.base = srv.base
  const log = []
  const outcome = await fetchSource(ds, line => log.push(line))

  assert.strictEqual(outcome, 'failed')
  assert.ok(log.some(line => line.includes('step f000 attempt 1/3 failed: Error: HTTP 500 (retrying in 2s)')))
  assert.ok(log.some(line => line.includes('step f000 attempt 2/3 failed: Error: HTTP 500 (retrying in 4s)')))
  assert.ok(log.some(line => line.includes('step f000 attempt 3/3 failed: Error: HTTP 500 (no retries left)')))
  assert.ok(log.some(line => line.includes('failed at step f000 — aborting')))
  assert.strictEqual(latestCompleteStamp(srcDir(root, setting)), null, 'no marker for a failed run')
  await srv.close()
})

// ── cleanup / archive rotation ──────────────────────────────────────────────

test('cleanup archives older runs when archiveRuns > 0 and purges leftovers', async () => {
  const root = path.join(tmpdir, 'cleanup')
  const setting = { model: 'gfs', resolution: '0p25', hours: 0, archiveRuns: 2 }
  const dir = srcDir(root, setting)
  fs.mkdirSync(dir, { recursive: true })
  // Two complete runs + one leftover of an interrupted run (all older than
  // whatever the fetcher will download next, so they get archived/purged).
  fs.writeFileSync(path.join(dir, '.run-20260101T00.complete'), '{"params":1}')
  fs.writeFileSync(path.join(dir, '.run-20260101T06.complete'), '{"params":1}')
  fs.writeFileSync(path.join(dir, 'gfs-0p25__20260101T00__f000.grb2'), payload(8))
  fs.writeFileSync(path.join(dir, 'gfs-0p25__20260101T06__f000.grb2'), payload(8))
  fs.writeFileSync(path.join(dir, 'gfs-0p25__20260101T03__f000.grb2'), payload(8)) // leftover, no marker

  const srv = await startServer((req, res) => {
    if (req.url.endsWith('.idx')) { res.end('idx\n'); return }
    res.end(payload(16))
  })
  const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  ds.base = srv.base
  await fetchSource(ds, () => {})
  await srv.close()

  const files = fs.readdirSync(dir)
  assert.ok(files.some(f => f.startsWith('.run-')), 'a marker exists')
  assert.ok(!files.includes('gfs-0p25__20260101T03__f000.grb2'), 'leftover purged')
  const archive = path.join(dir, 'archive')
  assert.ok(fs.existsSync(archive), 'archive dir created')
  assert.ok(fs.readdirSync(archive).some(f => f.includes('20260101T00')), 'older run archived')
})

test('cleanup deletes older runs when archiveRuns is 0', async () => {
  const root = path.join(tmpdir, 'cleanup-nodarchive')
  const setting = { model: 'gfs', resolution: '0p25', hours: 0, archiveRuns: 0 }
  const dir = srcDir(root, setting)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(path.join(dir, '.run-20260101T00.complete'), '{"params":1}')
  fs.writeFileSync(path.join(dir, 'gfs-0p25__20260101T00__f000.grb2'), payload(8))

  const srv = await startServer((req, res) => {
    if (req.url.endsWith('.idx')) { res.end('idx\n'); return }
    res.end(payload(16))
  })
  const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  ds.base = srv.base
  await fetchSource(ds, () => {})
  await srv.close()

  const files = fs.readdirSync(dir)
  assert.ok(!files.includes('gfs-0p25__20260101T00__f000.grb2'), 'older run deleted (no archive)')
  assert.ok(!fs.existsSync(path.join(dir, 'archive')), 'no archive dir when archiveRuns=0')
})

// ── cancellation: aborting the run cancels the in-flight download ───────────

test('gfs: aborting the signal cancels the in-flight download without retries', async () => {
  const root = path.join(tmpdir, 'gfs-abort')
  const setting = { model: 'gfs', resolution: '0p25', hours: 0 }   // single step f000
  // The GET response emits one chunk, then stalls forever. A download that
  // ignores the abort signal would never finish on its own.
  let responseClosed = false
  const srv = await startServer((req, res) => {
    if (req.url.endsWith('.idx')) { res.end('idx\n'); return }       // probe
    res.writeHead(200)
    res.write(payload(4096))
    res.on('close', () => { responseClosed = true })
  })

  const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  ds.base = srv.base
  const controller = new AbortController()
  const log = []

  // Guard: fail the test instead of hanging if the abort does not propagate.
  let settled = false
  const done = Promise.race([
    sleep(10_000).then(() => ' hung — abort did not propagate'),
    fetchSource(ds, (m) => log.push(m), controller.signal).then((o) => { settled = true; return ` ${o}` }),
  ])
  await waitFor(() => srv.log.some(e => e.method === 'GET'))        // download started
  controller.abort(new Error('test cancel'))
  const outcome = await done

  assert.ok(settled, `fetchSource resolved promptly:${outcome}`)
  assert.strictEqual(outcome.trim(), 'failed')
  assert.ok(log.some(m => m.includes('failed')), 'failure is logged')
  // No retry after the caller aborted — the aborted GET is the last request.
  assert.strictEqual(srv.log.filter(e => e.method === 'GET').length, 1)
  // The stalled connection was actively torn down by the client.
  assert.ok(await waitFor(() => responseClosed), 'server saw the aborted connection close')
  const dir = srcDir(root, setting)
  assert.strictEqual(fs.readdirSync(dir).filter(f => f.endsWith('.part')).length, 0, 'no .part leftover')
  assert.strictEqual(latestCompleteStamp(dir), null, 'no marker written')
  await srv.close()
})

// ── backpressure: a slow disk write paces the network read ─────────────────

test('gfs: honours write-stream backpressure instead of buffering the body', async () => {
  const root = path.join(tmpdir, 'gfs-backpressure')
  const setting = { model: 'gfs', resolution: '0p25', hours: 0 }     // single step f000
  const chunk = payload(4096, 5)
  const total = 256 * 1024                                           // 64 chunks
  // Server-side pump with its own backpressure handling.
  const srv = await startServer((req, res) => {
    if (req.url.endsWith('.idx')) { res.end('idx\n'); return }
    res.writeHead(200)
    let sent = 0
    const pump = () => {
      while (sent < total) {
        sent += chunk.length
        if (!res.write(chunk)) { res.once('drain', pump); return }
      }
      res.end()
    }
    pump()
  })

  // Replace the .part WriteStream with a slow one (2 ms per write, tiny
  // high-water mark) so write() returns false immediately. If the downloader
  // ignores backpressure, writes pile up ahead of 'drain' events and the
  // whole body would be buffered in the stream's internal queue.
  const events = []
  const realCreateWriteStream = fs.createWriteStream
  fs.createWriteStream = (p) => {
    fs.writeFileSync(p, Buffer.alloc(0))
    const slow = new Writable({
      highWaterMark: 16,
      write(buf, _enc, cb) {
        fs.appendFileSync(p, buf)                                    // real .part data
        setTimeout(cb, 2)
      },
    })
    const origWrite = slow.write.bind(slow)
    slow.write = (buf, ...rest) => {
      const ok = origWrite(buf, ...rest)
      events.push(ok ? 'write' : 'write-false')
      return ok
    }
    slow.on('drain', () => events.push('drain'))
    return slow
  }

  try {
    const ds = toDownloadSource(setting, root, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
    ds.base = srv.base
    const outcome = await fetchSource(ds, () => {})
    assert.strictEqual(outcome, 'downloaded')
  } finally {
    fs.createWriteStream = realCreateWriteStream
    await srv.close()
  }

  assert.ok(events.includes('write-false'), 'backpressure actually occurred')
  for (let i = 0; i < events.length; i++) {
    if (events[i] !== 'write-false') continue
    const next = events[i + 1]
    assert.ok(next === 'drain' || next === undefined,
      `write() returning false must be followed by drain, got ${next}`)
  }
  // Throttling must not corrupt the download: full payload, no leftovers.
  const dir = srcDir(root, setting)
  const files = fs.readdirSync(dir).filter(f => f.endsWith('.grb2'))
  assert.strictEqual(files.length, 1)
  const expected = Buffer.alloc(total)
  for (let off = 0; off < total; off += chunk.length) chunk.copy(expected, off)
  assert.ok(expected.equals(fs.readFileSync(path.join(dir, files[0]))), 'file matches the served payload')
  assert.ok(latestCompleteStamp(dir) !== null, 'marker written')
})
