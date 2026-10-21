// Read-only integration checks inside the selected sandbox. No tokens logged.
const fs = require('node:fs')
const path = require('node:path')
const assert = require('node:assert/strict')
const { createHash, createHmac } = require('node:crypto')
assert.equal(process.env.SIGNALK_CONTAINER_ID, 'signalk-sandbox')
const root = process.env.SIGNALK_NODE_CONFIG_DIR
assert.ok(root && path.isAbsolute(root), 'Set SIGNALK_NODE_CONFIG_DIR to the absolute Signal K configuration directory')
const name = 'signalk-grib-downloader', base = 'http://127.0.0.1:3000'
const security = JSON.parse(fs.readFileSync(path.join(root, 'security.json')))
const admin = security.users.find(u => u.type === 'admin')
const secret = security.secretKey || fs.readFileSync(path.join(root, 'security-secret.key'), 'utf8').trim()
const h = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url')
const p = Buffer.from(JSON.stringify({ id: admin.username, exp: Math.floor(Date.now()/1000) + 300 })).toString('base64url')
const adminHeaders = { Authorization: `Bearer ${h}.${p}.${createHmac('sha256', secret).update(`${h}.${p}`).digest('base64url')}` }
const data = path.join(root, 'plugin-config-data', name)
const token = fs.readFileSync(path.join(data, 'catalogue-token'), 'utf8').trim()
const headers = { Authorization: `Bearer ${token}` }
const get = async (route, auth = headers) => {
  const r = await fetch(base + route, { headers: auth })
  assert.equal(r.status, 200, route)
  return r.json()
}
async function main() {
  assert.equal(JSON.parse(fs.readFileSync(path.join(root, 'node_modules', name, 'package.json'))).version, '0.4.0-beta.3')
  const provider = await get('/plugins/signalk-forecast-dataset-provider/status')
  assert.equal(provider.contractVersion, '0.1')
  const status = await get(`/plugins/${name}/status`, adminHeaders)
  assert.ok(!JSON.stringify(status).includes(token))
  const source = status.sources.find(s => s.catalogue)
  assert.ok(source, 'A configured source must have published its local completed run')
  assert.equal(source.publicationPending, false)
  const detail = await get(source.catalogue.href)
  assert.equal(detail.version, source.catalogue.version)
  assert.equal(detail.producer, name)
  assert.equal(detail.format, 'grib2')
  assert.equal(detail.inventory.status, 'unverified')
  const list = await get('/signalk/v2/api/resources/forecastDatasets?producer=' + name)
  assert.ok(list[detail.id])
  const verified = await get('/signalk/v2/api/resources/forecastDatasets?producer=' + name + '&verified=true')
  assert.ok(!verified[detail.id])
  const journal = JSON.parse(fs.readFileSync(path.join(data, 'catalogue-publications', source.name + '.json')))
  const receipt = journal.records.find(r => r.datasetId === detail.id)
  assert.ok(receipt)
  let total = 0
  for (const asset of detail.assets) {
    const response = await fetch(base + asset.href, { headers })
    assert.equal(response.status, 200)
    const bytes = Buffer.from(await response.arrayBuffer())
    assert.equal(bytes.length, asset.size)
    assert.equal(createHash('sha256').update(bytes).digest('hex'), asset.sha256)
    const expected = receipt.received?.[asset.id] ?? receipt.assets.find(a => a.id === asset.id)
    assert.ok(expected && expected.size === bytes.length && expected.sha256 === createHash('sha256').update(bytes).digest('hex'))
    assert.equal(bytes.subarray(0, 4).toString(), 'GRIB')
    total += bytes.length
  }
  const asset = detail.assets[0]
  let response = await fetch(base + asset.href, { headers: { ...headers, Range: 'bytes=0-15' } })
  assert.equal(response.status, 206)
  assert.equal((await response.arrayBuffer()).byteLength, 16)
  response = await fetch(base + asset.href)
  assert.ok([401,403].includes(response.status))
  const marker = JSON.parse(fs.readFileSync(path.join(root, 'local-packages/grib-downloader-forecast-backup.json')))
  const hashes = JSON.parse(fs.readFileSync(path.join(marker.backup, 'plugin-config-hashes.json')))
  for (const [file, hash] of Object.entries(hashes)) {
    if (file === name + '.json') continue
    assert.equal(createHash('sha256').update(fs.readFileSync(path.join(root, 'plugin-config-data', file))).digest('hex'), hash, file)
  }
  const expectedConfig = JSON.parse(fs.readFileSync(path.join(marker.backup, 'downloader-config.json')))
  delete expectedConfig.configuration.gribsRoot
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(root, 'plugin-config-data', name + '.json'))), expectedConfig)
  const before = JSON.parse(fs.readFileSync(path.join(marker.backup, 'plugin-data/settings.json')))
  const after = JSON.parse(fs.readFileSync(path.join(data, 'settings.json')))
  assert.deepEqual(after, before)
  const device = security.devices.find(d => d.clientId === JSON.parse(Buffer.from(token.split('.')[1], 'base64url')).device)
  assert.equal(device.permissions, 'readwrite')
  assert.equal(fs.statSync(path.join(data, 'catalogue-token')).mode & 0o777, 0o600)
  console.log(JSON.stringify({ result: 'PASS', source: source.name, run: receipt.stamp, datasetId: detail.id, assets: detail.assets.length, bytes: total, configurationsPreserved: Object.keys(hashes).length, originalHashes: 'all verified', httpRange: '206', anonymousAccess: 'denied', inventory: 'unverified', producerPermissions: device.permissions, running: source.running, lastError: source.lastError }))
}
main().catch(error => { console.error(error.message); process.exitCode = 1 })
