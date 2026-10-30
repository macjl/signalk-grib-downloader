const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const http = require('node:http')
const assert = require('node:assert/strict')
const { createHash, randomUUID } = require('node:crypto')
const { CataloguePublisher } = require('../../dist/catalogue')
const { toDownloadSource, planRun } = require('../../dist/downloader')
const { candidateRuns } = require('../../dist/scheduler')
const API = '/plugins/signalk-forecast-dataset-provider'
const hash = bytes => createHash('sha256').update(bytes).digest('hex')
function grib(size = 128, fill = 42) {
  const bytes = Buffer.alloc(size, fill)
  bytes.write('GRIB'); bytes[7] = 2; bytes.writeBigUInt64BE(BigInt(size), 8); bytes.write('7777', size - 4)
  return bytes
}
async function server(t, handler) {
  const calls = []
  const instance = http.createServer(async (req, res) => {
    calls.push({ method: req.method, url: req.url, authorization: req.headers.authorization })
    try { await handler(req, res) } catch (error) { if (!res.destroyed) { res.writeHead(500); res.end(String(error)) } }
  })
  await new Promise(resolve => instance.listen(0, '127.0.0.1', resolve))
  t.after(async () => { instance.closeAllConnections(); await new Promise(resolve => instance.close(resolve)) })
  return { base: `http://127.0.0.1:${instance.address().port}`, calls, instance }
}
async function fixture(t, options = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'direct-grib-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const tokenFile = path.join(root, 'token')
  fs.writeFileSync(tokenFile, 'test-readwrite-token', { mode: 0o600 })
  const source = options.source ?? { model: 'gfs', resolution: '0p25', hours: 3 }
  const staged = new Map(), datasets = new Map(), bodies = new Map(), declarations = []
  const flags = { disabled: false, denied: false, failAsset: null, loseCreate: false, loseCommit: false, loseUpload: false, conflict: false, corruptReceipt: false }
  const provider = await server(t, async (req, res) => {
    const json = (status, value) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(value)) }
    if (flags.disabled) { res.writeHead(404); return res.end('Plugin temporarily unavailable') }
    if (flags.denied || req.headers.authorization !== 'Bearer test-readwrite-token') return json(401, {})
    const [kind, id, action, assetId] = req.url.replace(API + '/', '').split('/')
    if (kind === 'status') return json(200, { contractVersion: '0.1' })
    if (kind === 'publications' && !id && req.method === 'GET') return json(200, Object.fromEntries(staged))
    if (kind === 'publications' && !id && req.method === 'POST') {
      let body = ''; for await (const chunk of req) body += chunk
      const declaration = JSON.parse(body), stage = { id: randomUUID(), datasetId: randomUUID(), declaration, uploaded: {} }
      declarations.push(declaration); staged.set(stage.id, stage)
      if (flags.loseCreate) { flags.loseCreate = false; return res.destroy() }
      return json(201, stage)
    }
    if (kind === 'publications') {
      const stage = staged.get(id)
      if (!stage) return json(404, { code: 'PUBLICATION_NOT_FOUND' })
      if (req.method === 'DELETE') { staged.delete(id); res.writeHead(204); return res.end() }
      if (req.method === 'GET') return json(200, stage)
      if (action === 'assets') {
        assert.equal(req.headers['content-length'], undefined, 'unknown-size upload is chunked')
        if (stage.uploaded[assetId]) return json(409, { code: 'ASSET_ALREADY_UPLOADED' })
        if (options.beforeAssetRead) await options.beforeAssetRead(req)
        const chunks = []; for await (const chunk of req) chunks.push(chunk)
        if (flags.failAsset === assetId) return json(503, {})
        const bytes = Buffer.concat(chunks)
        const receipt = { id: assetId, size: bytes.length, sha256: hash(bytes), mediaType: 'application/octet-stream' }
        stage.uploaded[assetId] = receipt; bodies.set(assetId, bytes)
        if (flags.loseUpload) { flags.loseUpload = false; return res.destroy() }
        return json(201, flags.corruptReceipt ? { ...receipt, sha256: '0'.repeat(64) } : receipt)
      }
      if (action === 'commit') {
        if (stage.declaration.assets.some(a => !stage.uploaded[a.id])) return json(409, { code: 'INCOMPLETE_PUBLICATION' })
        const committed = { id: stage.datasetId, version: hash(Buffer.from(JSON.stringify(stage.uploaded))), assets: Object.values(stage.uploaded), ...stage.declaration }
        committed.assets = Object.values(stage.uploaded)
        datasets.set(committed.id, committed); staged.delete(id)
        if (flags.loseCommit) { flags.loseCommit = false; return res.destroy() }
        return json(201, committed)
      }
    }
    if (kind === 'datasets') {
      if (!datasets.has(id)) return json(404, { code: 'DATASET_NOT_FOUND' })
      if (req.method === 'DELETE') {
        if (flags.conflict) return json(409, { code: 'DATASET_BUSY' })
        datasets.delete(id); res.writeHead(204); return res.end()
      }
      return json(200, datasets.get(id))
    }
    return json(404, {})
  })
  const bytes = options.bytes ?? grib()
  const upstream = await server(t, options.handler ?? ((req, res) => { if (req.method === 'HEAD') return res.end(); res.end(bytes) }))
  const descriptor = toDownloadSource(source, options.bbox)
  descriptor.base = upstream.base
  const plan = (date = candidateRuns(source.model)[0]) => planRun(descriptor, date)
  const publisher = () => new CataloguePublisher(provider.base, tokenFile, root)
  return { root, tokenFile, source, provider, upstream, bytes, descriptor, plan, publisher, flags, staged, datasets, bodies, declarations }
}
async function waitFor(check) {
  const deadline = Date.now() + 5000
  while (!check()) { if (Date.now() > deadline) throw new Error('Condition not reached'); await new Promise(resolve => setTimeout(resolve, 10)) }
}
module.exports = { fixture, server, grib, waitFor, hash, API }
