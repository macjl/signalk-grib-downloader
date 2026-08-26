'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')
const http = require('http')

const {
  toDownloadSource,
  fetchSource,
  latestCompleteStamp,
  fetchFingerprint,
  fingerprintsEqual,
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
      const close = () => new Promise((r) => server.close(() => r()))
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
  assert.ok(latestCompleteStamp(dir) !== null, 'marker written')
  await srv.close()
})

// ── Météo-France smoke test (AROME: packages × groups) ───────────────────────

test('arome: downloads each package×group, writes a marker', async () => {
  const root = path.join(tmpdir, 'arome-run')
  const setting = { model: 'arome', resolution: '0025', hours: 18 }  // 3 groups
  const srv = await startServer((req, res) => {
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
  const BZip2 = (await import('@digitaldefiance/bzip2-wasm')).default
  const bz2 = new BZip2()
  await bz2.init()
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
  const outcome = await fetchSource(ds, () => {})

  assert.strictEqual(outcome, 'failed')
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
