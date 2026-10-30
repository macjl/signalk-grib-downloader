'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const { toDownloadSource, planRun, readAsset, runAvailable, loadBz2 } = require('../dist/downloader')
const { GribStreamValidator } = require('../dist/grib-stream')
const { fetchFingerprint, fingerprintsEqual } = require('../dist/scheduler')
const { server, grib } = require('./fixtures/http.cjs')
const run = new Date('2026-10-21T06:00:00Z')
const collect = async iterable => { const chunks = []; for await (const chunk of iterable) chunks.push(chunk); return Buffer.concat(chunks) }

test('source descriptors contain acquisition parameters and no filesystem directory', () => {
  const source = toDownloadSource({ model: 'gfs', hours: 6 }, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  assert.equal(source.directory, undefined)
  assert.deepEqual(source.steps, { from: 0, to: 6, by: 3 })
  assert.deepEqual(source.bbox, [35, -6, 45, 17])
})

test('GFS plans stable asset IDs, the last-step probe, subsetting and 850 hPa fields', () => {
  const source = toDownloadSource({ model: 'gfs', resolution: '0p25', hours: 6 }, { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 })
  const plan = planRun(source, run)
  assert.equal(plan.stamp, '20261021T06'); assert.equal(plan.assets.length, 3)
  assert.match(plan.probe, /f006.idx$/)
  assert.equal(plan.assets[0].id, 'gfs-0p25__20261021T06__f000_grb2')
  assert.match(plan.assets[0].urls[0], /leftlon=-6/)
  assert.match(plan.assets[0].urls[0], /lev_850_mb=on/)
  assert.match(plan.assets[0].urls[0], /filter_gfs_0p25/)
})
for (const [model, resolution] of [['arome', '0025'], ['arpege', '01']]) {
  test(`${model} plans all selected SP1 groups with no unexpanded URL placeholder`, () => {
    const source = toDownloadSource({ model, resolution, hours: 18 })
    const plan = planRun(source, run)
    assert.equal(plan.assets.length, source.groups.length)
    for (const asset of plan.assets) {
      assert.equal((asset.urls[0].match(/SP1/g) || []).length, 2)
      assert.ok(!/[{}]/.test(asset.urls[0]))
      assert.match(asset.urls[0], /2026-10-21T06:00:00Z/)
    }
  })
}

test('fetch fingerprints preserve legacy compatibility and change with area or requested fields', () => {
  const source = { model: 'gfs', hours: 6 }, box = { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 }
  const fp = fetchFingerprint(source, box)
  assert.ok(fingerprintsEqual(fp, toDownloadSource(source, box).fingerprint))
  assert.ok(!fingerprintsEqual(fp, fetchFingerprint(source, { ...box, latMax: 46 })))
  assert.ok(JSON.stringify(fp).includes('850_mb'))
  assert.ok(JSON.stringify(fp).includes('UGRD'))
  assert.ok(!fingerprintsEqual(fp, { resolution: '0p25', steps: { from: 0, to: 6, by: 3 }, bbox: [35, -6, 45, 17] }))
})

test('direct read yields data before upstream EOF and applies downstream backpressure', async t => {
  let finish
  const first = grib(1024 * 1024)
  const upstream = await server(t, (_req, res) => {
    res.writeHead(200)
    res.write(first.subarray(0, 4096))
    finish = () => res.end(first.subarray(4096))
  })
  const controller = new AbortController()
  t.after(() => controller.abort())
  const reader = readAsset({ id: 'asset', urls: [upstream.base] }, controller.signal)
  const chunk = await reader.next()
  assert.ok(!chunk.done && chunk.value.length >= 16, 'consumer receives chunks while response is open')
  finish()
  assert.deepEqual(Buffer.concat([chunk.value, await collect(reader)]), first)
})

test('ICON-EU decompresses one variable at a time and concatenates directly into an asset', async t => {
  const bz2 = await loadBz2(), a = grib(64, 11), b = grib(80, 23)
  const compA = Buffer.from(bz2.compress(a, 5, a.length + 1024)), compB = Buffer.from(bz2.compress(b, 5, b.length + 1024))
  const upstream = await server(t, (req, res) => { if (req.method === 'HEAD') return res.end(); res.end(req.url.includes('t_2m') ? compA : compB) })
  const source = toDownloadSource({ model: 'icon-eu', hours: 0 })
  source.variables = ['t_2m', 'u_10m']; source.base = upstream.base
  const plan = planRun(source, run)
  assert.equal(plan.assets.length, 1); assert.equal(plan.assets[0].urls.length, 2)
  assert.equal(await runAvailable(plan), true)
  assert.deepEqual(await collect(readAsset(plan.assets[0], new AbortController().signal)), Buffer.concat([a,b]))
})

test('transport-level upstream truncation propagates and a 200 HTML body is rejected', async t => {
  const upstream = await server(t, (req, res) => {
    if (req.url === '/truncated') { res.writeHead(200, { 'Content-Length': 128 }); res.write(grib().subarray(0, 20)); return setImmediate(() => res.destroy()) }
    res.end('<html>upstream service unavailable</html>')
  })
  for (const suffix of ['/truncated', '/html']) await assert.rejects(collect(readAsset({ id: 'asset', urls: [upstream.base + suffix] }, new AbortController().signal)))
})

test('missing upstream runs remain unavailable and cancellation is never swallowed', async t => {
  const upstream = await server(t, (_req, res) => { res.writeHead(404); res.end() })
  assert.equal(await runAvailable({ probe: upstream.base }), false)
  const controller = new AbortController(); controller.abort(new Error('cancelled'))
  await assert.rejects(runAvailable({ probe: upstream.base }, controller.signal), /cancelled/)
})

test('an upstream server error is reported rather than pretending no run exists', async t => {
  const upstream = await server(t, (_req, res) => { res.writeHead(503); res.end() })
  await assert.rejects(runAvailable({ probe: upstream.base }), /503/)
})

test('GRIB framing accepts split headers, split footers and multiple messages', () => {
  const bytes = Buffer.concat([grib(65), grib(129)])
  for (const step of [1, 3, 16, 64, 4096]) {
    const validator = new GribStreamValidator()
    for (let n = 0; n < bytes.length; n += step) validator.push(bytes.subarray(n, n + step))
    validator.finish()
  }
})

test('GRIB framing rejects empty, truncated, trailing, invalid-length and invalid-footer content', () => {
  const badLength = grib(); badLength.writeBigUInt64BE(19n, 8)
  const badFooter = grib(); badFooter.write('oops', badFooter.length - 4)
  for (const bytes of [Buffer.alloc(0), grib().subarray(0, 70), Buffer.concat([grib(), Buffer.from('trailing')]), badLength, badFooter]) {
    assert.throws(() => { const validator = new GribStreamValidator(); validator.push(bytes); validator.finish() })
  }
})
