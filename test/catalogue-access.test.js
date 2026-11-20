'use strict'

// Regressions for Signal K's shared HTTP 401 authentication/permission response.
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
  const f = { directory, id, secret, requests, polls, catalogueRequests: [], tokenChecks: [], tokenValid: undefined, checkError: false, checkHeld: false, approved: false, denied: false, pollError: false, missing: false, redirected: false, noToken: false, held: false }
  const server = http.createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk
    res.setHeader('Content-Type', 'application/json')
    if (req.url === '/prefix/loginStatus') {
      f.tokenChecks.push(req.headers.authorization)
      if (f.checkHeld) { f.releaseCheck = () => res.end(JSON.stringify({ status: 'notLoggedIn', authenticationRequired: true })); return }
      if (f.checkError) { res.statusCode = 503; return res.end(f.secret) }
      return res.end(JSON.stringify(f.loginResponse ?? { status: (f.tokenValid ?? f.approved) ? 'loggedIn' : 'notLoggedIn', authenticationRequired: true }))
    }
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
    res.statusCode = 401; res.end('{}')
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

test('remote access waits for a click and resumes the same pending read/write request after restart', async t => {
  const f = await fixture(t)
  const first = f.manager({ manualRequest: true })
  first.start()
  assert.equal(first.status().state, 'idle')
  await new Promise(resolve => setTimeout(resolve, 25))
  assert.equal(f.requests.length, 0)
  first.request()
  first.request()
  await waitFor(() => f.polls.length)
  assert.equal(f.requests.length, 1)
  assert.equal(f.requests[0].body.permissions, 'readwrite')
  await first.stop()
  const resumed = f.manager({ manualRequest: true })
  f.approved = true
  resumed.start()
  await waitFor(() => resumed.status().state === 'ready')
  assert.equal(resumed.currentToken(), f.secret)
  assert.equal(f.requests.length, 1)
})

test('remote denial persists across restart until the administrator retries', async t => {
  const f = await fixture(t)
  const first = f.manager({ manualRequest: true })
  f.denied = true
  first.start(); first.request()
  await waitFor(() => first.status().state === 'denied')
  await first.stop()
  const resumed = f.manager({ manualRequest: true })
  resumed.start()
  assert.equal(resumed.status().state, 'denied')
  assert.equal(f.requests.length, 1)
  f.denied = false
  resumed.request()
  await waitFor(() => f.requests.length === 2)
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

test('remote token request is bound to a saved server and approved token is used without overwriting the manual file', async t => {
  const f = await fixture(t)
  fs.writeFileSync(f.options.tokenFile, 'destination-token\n')
  const manual = path.join(f.directory, 'shore-manual-token')
  fs.writeFileSync(manual, 'manual-token\n')
  const plugin = require('../dist/index')({
    getDataDirPath: () => f.directory, getSelfPath: () => 'offline',
    streambundle: { getSelfStream: () => ({ onValue: () => () => {} }) },
    setPluginStatus() {}, debug() {},
  })
  t.after(() => plugin.stop())
  const routes = {}, router = {}
  for (const method of ['get', 'post', 'put', 'delete'])
    router[method] = (route, handler) => { routes[method + ' ' + route] = handler }
  plugin.registerWithRouter(router)
  async function call(route, req = {}) {
    let status = 200, body
    const res = { set: () => res, status: code => { status = code; return res }, json: value => { body = value } }
    await routes[route](req, res)
    return { status, body }
  }
  plugin.start({ legacyExportEnabled: false, remoteServers: [{ id: 'shore', url: f.options.endpoint, tokenFile: manual }] })
  const status = async () => (await call('get /remote-access')).body[0]
  const request = (server, endpoint) => call('post /remote-access/:server', { params: { server }, body: { endpoint } })
  const models = () => call('get /remote-models/:server', { params: { server: 'shore' } })
  assert.equal((await status()).state, 'idle')
  assert.equal((await request('unknown', f.options.endpoint)).status, 404)
  assert.equal((await request('shore', 'https://other.example')).status, 409)
  assert.equal(f.requests.length, 0)
  await models()
  assert.equal(f.catalogueRequests.at(-1).auth, 'Bearer manual-token')
  assert.equal((await request('shore', f.options.endpoint)).status, 202)
  f.approved = true
  for (let i = 0; i < 700 && (await status()).state !== 'ready'; i++)
    await new Promise(resolve => setTimeout(resolve, 10))
  const ready = await status()
  assert.equal(ready.state, 'ready')
  assert.ok(!JSON.stringify(ready).includes(f.secret))
  assert.ok(!JSON.stringify(ready).includes(f.id))
  assert.equal(f.requests.length, 1)
  assert.match(f.requests[0].body.description, /remote forecast source shore/)
  await models()
  assert.equal(f.catalogueRequests.at(-1).auth, 'Bearer ' + f.secret)
  assert.equal(fs.readFileSync(manual, 'utf8'), 'manual-token\n')
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), 'destination-token\n')
})

test('a rejected automatically issued token requests approval again without deleting a newer token', async t => {
  const f = await fixture(t), access = f.manager(); f.approved = true; access.start()
  await waitFor(() => access.status().state === 'ready')
  f.approved = false; access.rejected(f.secret)
  await waitFor(() => f.requests.length === 2)
  assert.equal(fs.existsSync(f.options.tokenFile), false)
  assert.equal(f.tokenChecks[0], 'Bearer ' + f.secret)
  assert.equal(access.status().diagnostics.renewals, 1)
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
  assert.equal(access.status().state, 'ready')
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), 'manual-token\n')
  assert.equal(f.requests.length, 0)
  fs.writeFileSync(f.options.tokenFile, 'rotated-manual-token\n')
  assert.equal(access.token(), 'rotated-manual-token')
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

test('catalogue 401 with an authenticated token never deletes it or asks for another approval', async t => {
  const f = await fixture(t), access = f.manager()
  f.approved = true; f.tokenValid = true; access.start()
  await waitFor(() => access.status().state === 'ready')
  for (let n = 0; n < 3; n++) {
    access.rejected(access.token())
    await waitFor(() => access.status().diagnostics.tokenChecks >= n + 1 && access.status().diagnostics.lastIssue?.code === 'CATALOGUE_DENIED_TOKEN_VALID')
  }
  assert.equal(f.requests.length, 1)
  assert.equal(access.status().state, 'ready')
  assert.equal(access.status().diagnostics.renewals, 0)
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), f.secret + '\n')
  assert.ok(!JSON.stringify(access.status()).includes(f.secret))
  assert.ok(!JSON.stringify(access.status()).includes(f.id))
  assert.ok(!JSON.stringify(access.status()).includes(f.options.endpoint))
})

test('unavailable token validation preserves the credential and retries without another approval', async t => {
  const f = await fixture(t)
  let ready = 0
  const access = f.manager({ onReady: () => ready++ })
  f.approved = true; access.start()
  await waitFor(() => access.status().state === 'ready')
  f.checkError = true
  access.rejected(f.secret)
  await waitFor(() => access.status().state === 'error')
  assert.equal(access.status().diagnostics.lastIssue.phase, 'tokenCheck')
  assert.equal(access.status().diagnostics.lastIssue.httpStatus, 503)
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), f.secret + '\n')
  assert.equal(f.requests.length, 1)
  f.checkError = false; f.tokenValid = true
  await waitFor(() => access.status().diagnostics.lastIssue?.code === 'CATALOGUE_DENIED_TOKEN_VALID')
  assert.equal(access.status().state, 'ready')
  assert.equal(f.requests.length, 1)
  assert.equal(ready, 2, 'resume the caller once after validation becomes available')
})

test('a rotation or stop during invalid-token validation cannot remove a credential', async t => {
  for (const action of ['rotate', 'stop']) {
    const f = await fixture(t), access = f.manager()
    f.approved = true; access.start()
    await waitFor(() => access.status().state === 'ready')
    f.checkHeld = true; access.rejected(f.secret)
    await waitFor(() => f.releaseCheck)
    if (action === 'rotate') fs.writeFileSync(f.options.tokenFile, 'rotated-manual-token\n')
    else await access.stop()
    f.releaseCheck()
    await new Promise(resolve => setTimeout(resolve, 25))
    assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), action === 'rotate' ? 'rotated-manual-token\n' : f.secret + '\n')
    assert.equal(f.requests.length, 1)
  }
})

test('simultaneous catalogue rejections share one validation and one renewal request', async t => {
  const f = await fixture(t), access = f.manager()
  f.approved = true; access.start()
  await waitFor(() => access.status().state === 'ready')
  f.approved = false; f.checkHeld = true
  for (let n = 0; n < 10; n++) access.rejected(f.secret)
  await waitFor(() => f.releaseCheck)
  assert.equal(f.tokenChecks.length, 1)
  f.releaseCheck()
  await waitFor(() => f.requests.length === 2)
  assert.equal(access.status().diagnostics.renewals, 1)
})

test('redacted diagnostics distinguish failed approval persistence without exposing identifiers', async t => {
  const f = await fixture(t)
  const access = f.manager(), originalWrite = fs.writeFileSync
  fs.writeFileSync = (file, ...args) => {
    if (String(file).startsWith(f.options.tokenFile + '.')) throw Object.assign(new Error(f.secret + f.options.endpoint), { code: 'EACCES' })
    return originalWrite(file, ...args)
  }
  try {
    f.approved = true; access.start()
    await waitFor(() => access.status().state === 'error')
    const diagnostic = access.status().diagnostics.lastIssue
    assert.equal(diagnostic.phase, 'storage')
    assert.equal(diagnostic.code, 'TOKEN_WRITE_FAILED')
    assert.equal(diagnostic.fsCode, 'EACCES')
    assert.ok(access.status().diagnostics.approvalResponses >= 1)
    assert.equal(access.status().diagnostics.tokenWrites, 0)
    assert.equal(f.requests.length, 1)
    const status = JSON.stringify(access.status())
    for (const value of [f.secret, f.id, f.options.endpoint, f.options.tokenFile]) assert.ok(!status.includes(value))
  } finally { await access.stop(); fs.writeFileSync = originalWrite }
})

test('an inconclusive login response cannot discard the credential or request approval', async t => {
  const f = await fixture(t), access = f.manager()
  f.approved = true; access.start()
  await waitFor(() => access.status().state === 'ready')
  f.loginResponse = { status: 'notLoggedIn' } // Not the explicit authenticated-server response.
  access.rejected(f.secret)
  await waitFor(() => access.status().state === 'error')
  assert.equal(access.status().diagnostics.lastIssue.code, 'TOKEN_CHECK_INCONCLUSIVE')
  assert.equal(fs.readFileSync(f.options.tokenFile, 'utf8'), f.secret + '\n')
  assert.equal(f.requests.length, 1)
  f.loginResponse = { status: 'loggedIn', authenticationRequired: true }
  await waitFor(() => access.status().state === 'ready')
  assert.equal(f.requests.length, 1)
})
