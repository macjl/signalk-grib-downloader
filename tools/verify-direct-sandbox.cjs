// Real HTTP verification inside the explicitly selected sandbox.
// Creates and removes only its uniquely named acquisition; never edits settings.
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const http = require('node:http')
const assert = require('node:assert/strict')
const { createHash } = require('node:crypto')
assert.equal(process.env.SIGNALK_CONTAINER_ID, 'signalk-sandbox')
const root = process.env.SIGNALK_NODE_CONFIG_DIR
assert.ok(root && path.isAbsolute(root), 'Set SIGNALK_NODE_CONFIG_DIR to the absolute Signal K configuration directory')
const name = 'signalk-grib-downloader'
const installed = path.join(root, 'node_modules', name)
const { CataloguePublisher } = require(path.join(installed, 'dist/catalogue'))
assert.equal(require(path.join(installed, 'package.json')).version, '0.4.0-beta.3')
const endpoint = 'http://127.0.0.1:3000', api = '/plugins/signalk-forecast-dataset-provider'
const tokenFile = path.join(root, 'plugin-config-data', name, 'catalogue-token')
const token = fs.readFileSync(tokenFile, 'utf8').trim()
const headers = { Authorization: `Bearer ${token}` }
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'grib-direct-live-'))
const publisher = () => new CataloguePublisher(endpoint, tokenFile, scratch)
const source = { model: 'gfs', resolution: `streamcheck${Date.now()}`, archiveRuns: 0 }
const bytes = fs.readFileSync(path.join(root, 'node_modules/signalk-grib-weather-provider/test/fixtures/gfs.t00z.pgrb2.1p00.850.grb2'))
const hash = value => createHash('sha256').update(value).digest('hex')
const configs = () => Object.fromEntries(fs.readdirSync(path.join(root, 'plugin-config-data')).filter(f => f.endsWith('.json')).map(f => [f, hash(fs.readFileSync(path.join(root, 'plugin-config-data', f)))]))
async function get(route) {
  const response = await fetch(endpoint + api + route, { headers })
  assert.equal(response.status, 200, route)
  return response.json()
}
async function until(check, label) {
  for (let n = 0; n < 100; n++) { const value = await check(); if (value) return value; await new Promise(resolve => setTimeout(resolve, 25)) }
  throw new Error(`Timed out: ${label}`)
}
let broken = true, secondOpened = false, upstreamClosed = false
const requests = []
const upstream = http.createServer((req, res) => {
  assert.equal(req.headers.authorization, undefined, 'producer token must never be forwarded upstream')
  requests.push(req.url)
  if (broken && req.url === '/part-1') {
    secondOpened = true
    res.writeHead(200, { 'Content-Type': 'application/octet-stream' })
    res.write(bytes.subarray(0, 64))
    res.on('close', () => { upstreamClosed = true })
  } else res.end(bytes)
})
const controller = new AbortController()
async function main() {
  const initial = Object.keys(await get('/datasets')).sort()
  const initialStages = Object.keys(await get('/publications')).sort()
  const beforeConfigs = configs()
  await new Promise(resolve => upstream.listen(0, '127.0.0.1', resolve))
  const base = `http://127.0.0.1:${upstream.address().port}`
  const date = new Date().toISOString(), stamp = date.slice(0, 10).replaceAll('-', '') + 'T' + date.slice(11, 13)
  const plan = { stamp, probe: base, assets: [0, 1].map(n => ({ id: `part-${n}`, urls: [base + `/part-${n}`] })) }
  const params = { verification: 'direct-stream', id: source.resolution }
  const transfer = publisher().publish(source, plan, params, () => {}, controller.signal)
  const rejected = assert.rejects(transfer)
  await until(() => secondOpened, 'second upstream stream')
  const stage = await until(async () => Object.values(await get('/publications')).find(p =>
    p.declaration.requested?.source === `gfs-${source.resolution}` && p.state === 'receiving' && p.progress.assetsCompleted === 1 && p.progress.assets['part-1'].bytesReceived > 0), 'receiving progress')
  assert.equal(stage.progress.totalBytes, null)
  assert.ok(stage.declaration.assets.every(a => a.size === undefined && a.sha256 === undefined))
  assert.deepEqual(Object.keys(await get('/datasets')).sort(), initial, 'no incomplete dataset in discovery')
  controller.abort(new Error('intentional verification interruption'))
  await rejected
  await until(() => upstreamClosed, 'upstream cancellation')
  const pending = await until(async () => {
    const p = await get('/publications/' + stage.id)
    return p.state === 'pending' && p.progress.assets['part-1'].bytesReceived === 0 ? p : null
  }, 'partial-file cleanup')
  assert.equal(pending.progress.assetsCompleted, 1)
  assert.equal(pending.uploaded['part-0'].sha256, hash(bytes))
  broken = false
  const ref = await publisher().publish(source, plan, params, () => {})
  assert.equal(requests.filter(url => url === '/part-0').length, 1, 'completed first asset is not fetched again')
  assert.equal(requests.filter(url => url === '/part-1').length, 2)
  const detail = await get('/datasets/' + ref.datasetId)
  assert.equal(detail.assets.length, 2)
  for (const asset of detail.assets) {
    assert.equal(asset.size, bytes.length); assert.equal(asset.sha256, hash(bytes))
    const response = await fetch(endpoint + asset.href, { headers })
    assert.equal(response.status, 200)
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), bytes)
  }
  assert.deepEqual(fs.readdirSync(scratch), ['catalogue-publications'], 'only a JSON journal exists on the downloader side')
  await publisher().removeSource(source)
  assert.deepEqual(Object.keys(await get('/datasets')).sort(), initial)
  assert.deepEqual(Object.keys(await get('/publications')).sort(), initialStages)
  assert.deepEqual(configs(), beforeConfigs)
  console.log(JSON.stringify({ result: 'PASS', version: '0.4.0-beta.3', streamedRealGrib: true, assets: 2,
    unknownTotalWhileReceiving: true, incompleteDatasetHidden: true, retainedCompletedAsset: true,
    resumedOnlyMissingAsset: true, originalsMatch: true, noIntermediateGrib: true,
    preexistingDatasetsPreserved: initial.length, configurationsPreserved: Object.keys(beforeConfigs).length,
    syntheticPublicationAndDatasetRemoved: true }))
}
main().catch(error => { console.error(error.message); process.exitCode = 1 }).finally(async () => {
  controller.abort()
  upstream.closeAllConnections()
  await new Promise(resolve => upstream.close(resolve))
  try { await publisher().removeSource(source) } catch (error) { console.error('Fixture cleanup failed: ' + error.message); process.exitCode = 1 }
  fs.rmSync(scratch, { recursive: true, force: true })
})
