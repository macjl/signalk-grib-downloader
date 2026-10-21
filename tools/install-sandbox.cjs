// Run as root inside the explicitly selected signalk-sandbox container.
// Updates only this plugin. Keeps existing dependency trees and configurations.
const fs = require('node:fs')
const path = require('node:path')
const assert = require('node:assert/strict')
const { execFileSync } = require('node:child_process')
const { createHash, createHmac } = require('node:crypto')
const root = process.env.SIGNALK_NODE_CONFIG_DIR
assert.ok(root && path.isAbsolute(root), 'Set SIGNALK_NODE_CONFIG_DIR to the absolute Signal K configuration directory')
const name = 'signalk-grib-downloader'
const base = 'http://127.0.0.1:3000'
const archive = process.argv[2]
assert.equal(process.env.SIGNALK_CONTAINER_ID, 'signalk-sandbox')
assert.ok(archive && path.isAbsolute(archive) && path.relative(root, archive).startsWith('local-packages/') && !path.relative(root, archive).includes('..'))
assert.ok(fs.existsSync(archive))
const stamp = new Date().toISOString().replace(/[:.]/g, '-')
const backup = path.join(root, 'local-packages', `grib-downloader-before-forecast-${stamp}`)
fs.mkdirSync(backup, { mode: 0o700 })
const configDir = path.join(root, 'plugin-config-data')
for (const file of ['package.json', 'package-lock.json', 'node_modules/.package-lock.json', 'security.json']) {
  if (fs.existsSync(path.join(root, file))) fs.copyFileSync(path.join(root, file), path.join(backup, file.replaceAll('/', '-')))
}
const configs = Object.fromEntries(fs.readdirSync(configDir).filter(f => f.endsWith('.json')).map(f => [f, createHash('sha256').update(fs.readFileSync(path.join(configDir, f))).digest('hex')]))
fs.writeFileSync(path.join(backup, 'plugin-config-hashes.json'), JSON.stringify(configs))
const data = path.join(configDir, name)
fs.copyFileSync(path.join(configDir, name + '.json'), path.join(backup, 'downloader-config.json'))
const destination = path.join(root, 'node_modules', name)
execFileSync('tar', ['-czf', path.join(backup, 'previous-plugin.tgz'), '-C', destination, '.'])
const staging = path.join(root, 'node_modules', `.grib-downloader-install-${stamp}`)
fs.mkdirSync(staging)
execFileSync('tar', ['-xzf', archive, '--strip-components=1', '--no-same-owner', '-C', staging])
const pkg = JSON.parse(fs.readFileSync(path.join(staging, 'package.json')))
assert.equal(pkg.name, name)
assert.equal(pkg.version, '0.4.0-beta.2')
assert.ok(fs.existsSync(path.join(staging, 'dist/catalogue.js')))
assert.ok(require(require.resolve('@digitaldefiance/bzip2-wasm/package.json', { paths: [destination] })).version)
execFileSync('chown', ['-R', 'node:node', staging])

const atomic = (file, object) => {
  const previous = fs.statSync(file)
  fs.writeFileSync(file + '.forecast.tmp', JSON.stringify(object, null, 2) + '\n', { mode: previous.mode & 0o777 })
  fs.chownSync(file + '.forecast.tmp', previous.uid, previous.gid)
  fs.renameSync(file + '.forecast.tmp', file)
}

async function main() {
  // Provision an independent readwrite device through the standard Access API.
  // Admin credentials are short lived, held only in memory, and never logged.
  const security = JSON.parse(fs.readFileSync(path.join(root, 'security.json')))
  const admin = security.users.find(u => u.type === 'admin')
  assert.ok(admin)
  const secret = security.secretKey || fs.readFileSync(path.join(root, 'security-secret.key'), 'utf8').trim()
  const h = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url')
  const p = Buffer.from(JSON.stringify({ id: admin.username, exp: Math.floor(Date.now() / 1000) + 300 })).toString('base64url')
  const auth = { Authorization: `Bearer ${h}.${p}.${createHmac('sha256', secret).update(`${h}.${p}`).digest('base64url')}` }
  const call = (route, method = 'GET', body, headers = auth) => fetch(base + route, { method, headers: { ...headers, ...(body ? { 'Content-Type': 'application/json' } : {}) }, ...(body ? { body: JSON.stringify(body) } : {}) })
  let response = await call('/plugins/signalk-forecast-dataset-provider/status')
  assert.equal(response.status, 200)
  assert.equal((await response.json()).contractVersion, '0.1')
  const config = JSON.parse(fs.readFileSync(path.join(configDir, name + '.json')))
  assert.ok(!config.configuration.catalogueUrl || config.configuration.catalogueUrl === base, 'Installer only provisions the local sandbox catalogue')
  const tokenFile = config.configuration.catalogueTokenFile || path.join(data, 'catalogue-token')
  assert.ok(path.isAbsolute(tokenFile) && tokenFile.startsWith(data + '/'))
  if (!fs.existsSync(tokenFile)) {
    const clientId = `grib-catalogue-${stamp}`
    response = await call('/signalk/v1/access/requests', 'POST', { clientId, description: 'GRIB Downloader forecast catalogue producer', permissions: 'readwrite' }, {})
    assert.equal(response.status, 202)
    const pending = await response.json()
    response = await call(`/skServer/security/access/requests/${clientId}/approved`, 'PUT', { permissions: 'readwrite', expiration: 'NEVER' })
    assert.equal(response.status, 200)
    response = await call(`/signalk/v1/requests/${pending.requestId}`)
    assert.equal(response.status, 200)
    const approved = await response.json()
    assert.equal(approved.accessRequest.permission, 'APPROVED')
    assert.ok(approved.accessRequest.token)
    fs.writeFileSync(tokenFile, approved.accessRequest.token + '\n', { mode: 0o600 })
    fs.chownSync(tokenFile, 1000, 1000)
  }
  const token = fs.readFileSync(tokenFile, 'utf8').trim()
  response = await call('/plugins/signalk-forecast-dataset-provider/status', 'GET', undefined, { Authorization: `Bearer ${token}` })
  assert.equal(response.status, 200)
  // Freeze only this plugin before snapshotting its journal and swapping code.
  // The original enabled configuration was backed up before this API call.
  response = await call(`/plugins/${name}/config`, 'POST', { ...config, enabled: false })
  assert.equal(response.status, 200)
  for (let n = 0; n < 100; n++) {
    const state = await call(`/plugins/${name}/status`)
    if (state.status === 503) break
    assert.ok(n < 99, 'Downloader did not stop before installation')
    await new Promise(resolve => setTimeout(resolve, 50))
  }
  fs.cpSync(data, path.join(backup, 'plugin-data'), { recursive: true })
  const previous = path.join(root, 'node_modules', '.grib-downloader-before-forecast')
  assert.ok(!fs.existsSync(previous))
  fs.renameSync(destination, previous)
  fs.renameSync(staging, destination)
  // A complete archive of the previous package is already in the backup.
  const relative = path.relative(root, archive)
  const packages = JSON.parse(fs.readFileSync(path.join(root, 'package.json')))
  packages.dependencies[name] = `file:${relative}`
  atomic(path.join(root, 'package.json'), packages)
  const entry = { version: pkg.version, resolved: `file:${relative}`, integrity: `sha512-${createHash('sha512').update(fs.readFileSync(archive)).digest('base64')}`, license: pkg.license, engines: pkg.engines, dependencies: pkg.dependencies, peerDependencies: pkg.peerDependencies }
  for (const file of ['package-lock.json', 'node_modules/.package-lock.json']) {
    const full = path.join(root, file)
    if (!fs.existsSync(full)) continue
    const lock = JSON.parse(fs.readFileSync(full))
    lock.packages[`node_modules/${name}`] = entry
    if (lock.packages['']) lock.packages[''].dependencies[name] = `file:${relative}`
    atomic(full, lock)
  }
  // All acquisition bytes now belong to the provider; retain legacy files for rollback.
  delete config.configuration.gribsRoot
  atomic(path.join(configDir, name + '.json'), config)
  fs.rmSync(previous, { recursive: true })
  fs.writeFileSync(path.join(root, 'local-packages/grib-downloader-forecast-backup.json'), JSON.stringify({ backup, previous, version: pkg.version }))
  console.log(`Installed ${name} ${pkg.version}; backup: ${backup}; obsolete shared-directory setting removed; other plugin configurations preserved`)
}
main().catch(error => { console.error(error.message); process.exitCode = 1 })
