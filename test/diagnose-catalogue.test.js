'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs'), os = require('node:os'), path = require('node:path')
const { execFileSync } = require('node:child_process')
const { createHash } = require('node:crypto')

test('standalone diagnostics redact credentials, paths, endpoints and access-request identifiers', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'redacted-catalogue-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const secrets = ['private-server.example', 'raw-test-token', 'private-client-identifier', 'private-request-identifier']
  for (const name of ['signalk-grib-downloader', 'signalk-grib-weather-provider']) {
    const dir = path.join(root, 'plugin-config-data', name)
    fs.mkdirSync(dir, { recursive: true })
    const file = path.join(dir, 'catalogue-token')
    fs.writeFileSync(file, secrets[1], { mode: 0o600 })
    fs.writeFileSync(dir + '.json', JSON.stringify({ enabled: true, configuration: { catalogueUrl: 'https://user:password@' + secrets[0] } }))
    fs.writeFileSync(path.join(dir, 'catalogue-access-' + createHash('sha256').update(file).digest('hex').slice(0, 16) + '.json'), JSON.stringify({ clientId: secrets[2], requestId: secrets[3], tokenHash: createHash('sha256').update(secrets[1]).digest('hex') }))
  }
  const output = execFileSync(process.execPath, [path.join(__dirname, '../docs/diagnose-catalogue.cjs'), root], { encoding: 'utf8' })
  const report = JSON.parse(output)
  assert.equal(report.plugins.length, 2)
  for (const plugin of report.plugins) {
    assert.equal(plugin.tokenReadable, true)
    assert.equal(plugin.savedRequest.automaticTokenMatches, true)
    assert.match(plugin.savedRequest.requestFingerprint, /^[a-f0-9]{12}$/)
    assert.equal(plugin.catalogueUrlValid, false)
  }
  for (const value of [...secrets, root, 'password']) assert.ok(!output.includes(value))
})
