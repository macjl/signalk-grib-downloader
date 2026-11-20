const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const http = require('node:http')
const { Readable } = require('node:stream')
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
  const source = options.source ?? { model: 'gfs', resolution: '0p25', hours: 3 }
  const staged = new Map(), datasets = new Map(), bodies = new Map(), declarations = []
  const flags = { disabled: false, denied: false, failAsset: null, loseCreate: false, loseCommit: false, loseUpload: false, conflict: false, corruptReceipt: false }
  const calls = []
  const fault = (status, code) => Object.assign(new Error(code ? `${status} (${code})` : String(status)), { status, code })
  const call = (method, url) => {
    calls.push({ method, url })
    if (flags.disabled) throw fault(404)
    if (flags.denied) throw fault(503)
  }
  const stageFor = id => {
    const stage = staged.get(id)
    if (!stage) throw fault(404, 'PUBLICATION_NOT_FOUND')
    return stage
  }
  const datasetFor = (id, version) => {
    const dataset = datasets.get(id)
    if (!dataset || (version && dataset.version !== version)) throw fault(404, 'DATASET_NOT_FOUND')
    return dataset
  }
  const service = {
    contractVersion: '0.1',
    publications() {
      call('GET', '/publications')
      return structuredClone(Object.fromEntries(staged))
    },
    publication(id) {
      call('GET', `/publications/${id}`)
      return structuredClone(stageFor(id))
    },
    begin(declaration) {
      call('POST', '/publications')
      const stage = { id: randomUUID(), datasetId: randomUUID(), declaration, uploaded: {} }
      declarations.push(declaration)
      staged.set(stage.id, stage)
      if (flags.loseCreate) { flags.loseCreate = false; throw fault(503) }
      return structuredClone(stage)
    },
    async upload(id, asset, body) {
      call('PUT', `/publications/${id}/assets/${asset}`)
      const stage = stageFor(id)
      if (stage.uploaded[asset]) throw fault(409, 'ASSET_ALREADY_UPLOADED')
      if (options.beforeAssetRead) await options.beforeAssetRead(body)
      const chunks = []
      for await (const chunk of body) chunks.push(chunk)
      if (flags.failAsset === asset) throw fault(503)
      const bytes = Buffer.concat(chunks)
      const receipt = { id: asset, size: bytes.length, sha256: hash(bytes), mediaType: 'application/octet-stream' }
      stage.uploaded[asset] = receipt
      bodies.set(asset, bytes)
      if (flags.loseUpload) { flags.loseUpload = false; throw fault(503) }
      return flags.corruptReceipt ? { ...receipt, sha256: '0'.repeat(64) } : structuredClone(receipt)
    },
    commit(id) {
      call('POST', `/publications/${id}/commit`)
      const stage = stageFor(id)
      if (stage.declaration.assets.some(a => !stage.uploaded[a.id])) throw fault(409, 'INCOMPLETE_PUBLICATION')
      const committed = { ...stage.declaration, id: stage.datasetId, version: hash(Buffer.from(JSON.stringify(stage.uploaded))), assets: Object.values(stage.uploaded) }
      datasets.set(committed.id, committed)
      staged.delete(id)
      if (flags.loseCommit) { flags.loseCommit = false; throw fault(503) }
      return structuredClone(committed)
    },
    abort(id) {
      call('DELETE', `/publications/${id}`)
      stageFor(id)
      staged.delete(id)
    },
    dataset(id, version) {
      call('GET', `/datasets/${id}`)
      return structuredClone(datasetFor(id, version))
    },
    remove(id) {
      call('DELETE', `/datasets/${id}`)
      datasetFor(id)
      if (flags.conflict) throw fault(409, 'DATASET_BUSY')
      datasets.delete(id)
    },
    readAsset(id, version, asset) {
      call('GET', `/datasets/${id}/versions/${version}/assets/${asset}`)
      if (!datasetFor(id, version).assets.some(a => a.id === asset)) throw fault(404, 'ASSET_NOT_FOUND')
      return Readable.from([bodies.get(asset)])
    },
  }
  const registry = (globalThis[Symbol.for('macjl.forecast-datasets.catalogues.v1')] ??= new Map())
  registry.set(root, service)
  t.after(() => registry.delete(root))
  const provider = { calls }
  const bytes = options.bytes ?? grib()
  const upstream = await server(t, options.handler ?? ((req, res) => { if (req.method === 'HEAD') return res.end(); res.end(bytes) }))
  const descriptor = toDownloadSource(source, options.bbox)
  descriptor.base = upstream.base
  const plan = (date = candidateRuns(source.model)[0]) => planRun(descriptor, date)
  const dataDir = path.join(root, 'downloader')
  const publisher = legacyExport => new CataloguePublisher(dataDir, () => registry.get(root), legacyExport)
  return { root, dataDir, source, provider, upstream, bytes, descriptor, plan, publisher, flags, staged, datasets, bodies, declarations }
}
async function waitFor(check) {
  const deadline = Date.now() + 5000
  while (!check()) { if (Date.now() > deadline) throw new Error('Condition not reached'); await new Promise(resolve => setTimeout(resolve, 10)) }
}
module.exports = { fixture, server, grib, waitFor, hash, API }
