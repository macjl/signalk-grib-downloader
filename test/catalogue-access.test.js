'use strict'
const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const http = require('node:http')
const { randomUUID, createHash } = require('node:crypto')
const { CatalogueAccess } = require('../dist/catalogue-access')
const pkg = require('../package.json')
const permissions = pkg.name === 'signalk-grib-downloader' ? 'readwrite' : 'readonly'
const waitFor = async fn => {
  for (let i = 0; i < 400; i++) { if (fn()) return; await new Promise(resolve => setTimeout(resolve, 10)) }
  throw new Error('Access lifecycle condition not reached')
}
async function fixture(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'catalogue-access-'))
  const requests = [], polls = [], id = randomUUID(), secret = 'secret-token-do-not-expose'
  const f = { directory, id, secret, requests, polls, catalogueRequests: [], approved: false, denied: false, pollError: false, missing: false, redirected: false, noToken: false, held: false }
  const server = http.createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk
    res.setHeader('Content-Type', 'application/json')
    if (req.method === 'POST' && req.url === '/prefix/signalk/v1/access/requests') {
      requests.push({ body: JSON.parse(body), auth: req.headers.authorization })
      if (f.redirected) { res.writeHead(302, { Location: '/steal' }); return res.end() }
      res.statusCode = 202; return res.end(JSON.stringify({ state: 'PENDING', requestId: id }))
    }
    if (req.url === '/prefix/signalk/v1/requests/' + id) {
      polls.push(req.url)
      if (f.held) { f.release = () => res.end(JSON.stringify({ state: 'COMPLETED', accessRequest: { permission: 'APPROVED', token: f.secret } })); return }
      if (f.pollError) { res.statusCode = 503; return res.end(secret) }
      if (f.missing) { f.missing = false; res.statusCode = 500; res.setHeader('Content-Type', 'text/plain'); return res.end('Unable to check request: not found') }
      return res.end(JSON.stringify({ state: f.approved || f.denied ? 'COMPLETED' : 'PENDING', requestId: id,
        ...(f.approved ? { accessRequest: { permission: 'APPROVED', ...(f.noToken ? {} : { token: f.secret }) } } : {}),
        ...(f.denied ? { accessRequest: { permission: 'DENIED' } } : {}) }))
    }
    f.catalogueRequests.push({ url: req.url, auth: req.headers.authorization })
    res.statusCode = 404; res.end('{}')
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const options = { endpoint: `http://127.0.0.1:${server.address().port}/prefix`, tokenFile: path.join(directory, 'catalogue-token'), dataDir: directory,
    pluginId: pkg.name, permissions, pollMs: 5, retryMs: 10 }
  const managers = []
  f.options = options
  f.manager = (overrides = {}) => { const access = new CatalogueAccess({ ...options, ...overrides }); managers.push(access); return access }
  t.after(async () => { await Promise.all(managers.map(m => m.stop())); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); fs.rmSync(directory, { recursive: true, force: true }) })
  return f
}

test('missing token creates the least-privilege device request and approval persists a private token', async t => {
  const f = await fixture(t); let ready = 0
  const access = f.manager({ onReady: () => ready++ }); access.start()
  await waitFor(() => f.polls.length)
  assert.equal(f.requests.length, 1)
  assert.equal(f.requests[0].body.permissions, permissions)
  assert.ok(f.requests[0].body.clientId.startsWith(pkg.name + '-'))
  assert.equal(f.requests[0].auth, undefined)
  assert.throws(() => access.token(), /approve the request/)
  assert.equal(access.status().state, 'pending')
  f.approved = true
  await waitFor(() => access.status().state === 'ready')
  assert.equal(access.token(), f.secret); assert.equal(ready, 1)
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), f.secret + '\n')
  assert.ok(!JSON.stringify(access.status()).includes(f.secret))
  assert.ok(!JSON.stringify(access.status()).includes(f.id))
  assert.ok(!fs.readFileSync(path.join(f.directory, 'catalogue-access-' + createHash('sha256').update(f.options.tokenFile).digest('hex').slice(0, 16) + '.json'), 'utf8').includes(f.secret))
  if (process.platform !== 'win32') {
    assert.equal(fs.statSync(f.options.tokenFile).mode & 0o777, 0o600)
    assert.equal(fs.statSync(path.join(f.directory, 'catalogue-access-' + createHash('sha256').update(f.options.tokenFile).digest('hex').slice(0, 16) + '.json')).mode & 0o777, 0o600)
  }
})

test('an existing configured token is reused without an access request or file replacement', async t => {
  const f = await fixture(t)
  const tokenFile = path.join(f.directory, 'custom-token')
  fs.writeFileSync(tokenFile, 'existing-token\n')
  const access = f.manager({ tokenFile }); access.start()
  await waitFor(() => access.status().state === 'ready')
  assert.equal(access.token(), 'existing-token'); assert.equal(f.requests.length, 0)
  assert.equal(fs.readFileSync(tokenFile, 'utf8'), 'existing-token\n')
})

test('plugin restart resumes the same pending request instead of requesting a second device', async t => {
  const f = await fixture(t), first = f.manager(); first.start()
  await waitFor(() => f.polls.length); await first.stop()
  const second = f.manager(); second.start(); f.approved = true
  await waitFor(() => second.status().state === 'ready')
  assert.equal(f.requests.length, 1); assert.equal(second.token(), f.secret)
})

test('denial never writes a token or repeatedly asks the administrator', async t => {
  const f = await fixture(t), first = f.manager(); f.denied = true; first.start()
  await waitFor(() => first.status().state === 'denied')
  const polls = f.polls.length
  await new Promise(resolve => setTimeout(resolve, 35))
  assert.equal(f.polls.length, polls); assert.equal(f.requests.length, 1)
  assert.equal(fs.existsSync(f.options.tokenFile), false)
  assert.throws(() => first.token(), /denied/)
  await first.stop(); f.denied = false
  const second = f.manager(); second.start()
  await waitFor(() => f.requests.length === 2)
  assert.equal(f.requests[0].body.clientId, f.requests[1].body.clientId)
})

test('a transient poll error retains the request identity and cannot leak its response body', async t => {
  const f = await fixture(t), access = f.manager(); f.pollError = true; access.start()
  await waitFor(() => access.status().state === 'error')
  assert.ok(!JSON.stringify(access.status()).includes(f.secret))
  f.pollError = false; f.approved = true
  await waitFor(() => access.status().state === 'ready')
  assert.equal(f.requests.length, 1)
})

test('the exact Signal K missing-request response permits recovery after server restart', async t => {
  const f = await fixture(t), access = f.manager(); f.missing = true; access.start()
  await waitFor(() => f.requests.length === 2)
  f.approved = true
  await waitFor(() => access.status().state === 'ready')
  assert.equal(f.requests[0].body.clientId, f.requests[1].body.clientId)
})

test('stop aborts an in-flight poll and cannot persist a token approved afterwards', async t => {
  const f = await fixture(t), access = f.manager(); f.held = true; access.start()
  await waitFor(() => f.release)
  await access.stop(); f.release()
  await new Promise(resolve => setTimeout(resolve, 25))
  assert.equal(fs.existsSync(f.options.tokenFile), false)
  const polls = f.polls.length
  await new Promise(resolve => setTimeout(resolve, 25))
  assert.equal(f.polls.length, polls)
})

test('approval without a token is not considered authorized', async t => {
  const f = await fixture(t), access = f.manager(); f.approved = true; f.noToken = true; access.start()
  await waitFor(() => access.status().state === 'error')
  assert.equal(fs.existsSync(f.options.tokenFile), false)
  f.noToken = false
  await waitFor(() => access.status().state === 'ready')
})

test('a change of catalogue server cannot reuse an automatically requested token', async t => {
  const f = await fixture(t), first = f.manager(); first.start(); f.approved = true
  await waitFor(() => first.status().state === 'ready'); await first.stop()
  const second = f.manager({ endpoint: 'https://other.example' })
  assert.throws(() => second.token(), /Catalogue server changed/)
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), f.secret + '\n')
})

test('access request redirects are refused and never followed', async t => {
  const f = await fixture(t), access = f.manager(); f.redirected = true; access.start()
  await waitFor(() => access.status().state === 'error')
  assert.equal(f.polls.length, 0); assert.equal(fs.existsSync(f.options.tokenFile), false)
})

test('remote plaintext and URLs containing credentials are rejected', () => {
  for (const endpoint of ['http://example.org', 'https://user:secret@example.org', 'https://example.org?token=secret']) {
    assert.throws(() => new CatalogueAccess({ endpoint, tokenFile: path.join(os.tmpdir(), 'catalogue-token'), dataDir: os.tmpdir(), pluginId: pkg.name, permissions }), /Invalid catalogue access URL/)
  }
})

test('plugin startup exposes a pending request, resumes after approval and stops polling on stop', async t => {
  const f = await fixture(t)
  const messages = []
  const server = {
    getDataDirPath: () => f.directory, getSelfPath: () => undefined,
    streambundle: { getSelfStream: () => ({ onValue: () => () => {} }) },
    setPluginStatus: message => messages.push(message), setPluginError: message => messages.push(message), debug: () => {},
    weatherApi: { register: () => {}, unRegister: () => {} },
  }
  const plugin = require('../dist/index')(server), routes = {}
  const router = {}
  for (const method of ['get', 'post', 'put', 'delete']) router[method] = (url, handler) => { routes[method + ' ' + url] = handler }
  plugin.registerWithRouter(router)
  const status = () => {
    let result
    const res = { status: () => res, json: value => { result = value } }
    routes['get /status']({}, res)
    return result
  }
  t.after(() => plugin.stop())
  plugin.start({ catalogueUrl: f.options.endpoint, sources: permissions === 'readwrite' ? [{ model: 'gfs', resolution: '0p25', hours: 6, autoDownload: true }] : [] })
  await waitFor(() => f.requests.length)
  assert.equal(status().catalogueAccess.state, 'pending')
  assert.equal(f.catalogueRequests.length, 0)
  assert.equal(f.requests[0].body.permissions, permissions)
  assert.ok(messages.some(message => /Access Requests/.test(message)))
  f.approved = true
  for (let n = 0; n < 800 && status().catalogueAccess.state !== 'ready'; n++) await new Promise(resolve => setTimeout(resolve, 10))
  assert.equal(status().catalogueAccess.state, 'ready')
  await waitFor(() => f.catalogueRequests.length)
  assert.equal(f.catalogueRequests[0].auth, 'Bearer ' + f.secret)
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), f.secret + '\n')
  assert.ok(!JSON.stringify(status()).includes(f.secret))
  assert.equal(f.requests.length, 1)
  await plugin.stop()
  const polls = f.polls.length
  await new Promise(resolve => setTimeout(resolve, 20))
  assert.equal(f.polls.length, polls)
})

test('a rejected automatically issued token requests approval again without deleting a newer token', async t => {
  const f = await fixture(t), access = f.manager(); f.approved = true; access.start()
  await waitFor(() => access.status().state === 'ready')
  f.approved = false; access.rejected(f.secret)
  assert.equal(fs.existsSync(f.options.tokenFile), false)
  await waitFor(() => f.requests.length === 2)
  assert.equal(access.status().state, 'pending')
  f.secret = 'renewed-token'; f.approved = true
  await waitFor(() => access.status().state === 'ready')
  assert.equal(access.token(), 'renewed-token')
  access.rejected('secret-token-do-not-expose')
  assert.equal(access.token(), 'renewed-token')
  fs.writeFileSync(f.options.tokenFile, 'newer-manually-rotated-token\n')
  access.rejected('renewed-token')
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), 'newer-manually-rotated-token\n')
  assert.equal(f.requests.length, 2)
})

test('a rejected externally configured token is preserved for its owner to rotate', async t => {
  const f = await fixture(t)
  fs.writeFileSync(f.options.tokenFile, 'manual-token\n')
  const access = f.manager(); access.start()
  access.rejected('manual-token')
  assert.equal(access.status().state, 'error')
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), 'manual-token\n')
  assert.equal(f.requests.length, 0)
})

test('an approval in flight cannot overwrite a token configured manually in the meantime', async t => {
  const f = await fixture(t), access = f.manager(); f.held = true; access.start()
  await waitFor(() => f.release)
  fs.writeFileSync(f.options.tokenFile, 'manually-configured-during-approval\n')
  f.release()
  await waitFor(() => access.status().state === 'ready')
  assert.equal(access.token(), 'manually-configured-during-approval')
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), 'manually-configured-during-approval\n')
})
