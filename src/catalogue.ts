// Direct streaming producer for Forecast Dataset Provider >=0.1.0-beta.1, contract 0.1.
// Only receipts and resumable acquisition metadata are stored by the downloader.
import * as fs from 'node:fs'
import * as path from 'node:path'
import { createHash, randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { SourceSetting } from './types'
import { fingerprintsEqual, sourceDirName } from './scheduler'
import { readAsset, RunPlan, RemoteAsset } from './downloader'

const API = '/plugins/signalk-forecast-dataset-provider'
const PRODUCER = 'signalk-grib-downloader'
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i
interface Asset { id: string; size?: number; sha256?: string; mediaType: string }
interface ReceivedAsset extends Asset { size: number; sha256: string }
interface RecordEntry {
  stamp: string
  params: unknown
  assets: Asset[]
  datasetId?: string
  publicationId?: string
  acquisitionId?: string
  received?: Record<string, ReceivedAsset>
  version?: string
}
interface Journal { endpoint: string; records: RecordEntry[] }
export interface CatalogueReference { datasetId: string; version: string; href: string }
export interface CompletedRun { stamp: string; params: unknown }
export class CatalogueHttpError extends Error {
  constructor(readonly status: number, readonly code?: string) {
    super(`Forecast Dataset Provider: HTTP ${status}${code ? ` (${code})` : ''}${status === 401 || status === 403 ? ' (check the readwrite token file)' : ''}`)
  }
}
const missing = (error: unknown, code: string) => error instanceof CatalogueHttpError && error.status === 404 && error.code === code

export class CataloguePublisher {
  readonly endpoint: string
  constructor(endpoint: string, private tokenFile: string, private dataDir: string) {
    const url = new URL(endpoint)
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error('Catalogue URL must be HTTP(S), without credentials, query or fragment')
    if (url.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) throw new Error('Use HTTPS for a remote catalogue server')
    this.endpoint = url.href.replace(/\/$/, '')
  }
  private file(source: SourceSetting): string {
    const name = sourceDirName(source)
    if (!/^[A-Za-z0-9_-]+$/.test(name)) throw new Error('Invalid source name')
    return path.join(this.dataDir, 'catalogue-publications', `${name}.json`)
  }
  private read(source: SourceSetting): Journal {
    try {
      const journal = JSON.parse(fs.readFileSync(this.file(source), 'utf8')) as Journal
      if (journal.endpoint !== this.endpoint) throw new Error('Catalogue URL changed: preserve the old publication journal before switching servers')
      return journal
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { endpoint: this.endpoint, records: [] }
      throw error
    }
  }
  private save(source: SourceSetting, journal: Journal): void {
    const file = this.file(source)
    fs.mkdirSync(path.dirname(file), { recursive: true })
    const fd = fs.openSync(file + '.tmp', 'w', 0o600)
    try { fs.writeFileSync(fd, JSON.stringify(journal, null, 2)); fs.fsyncSync(fd) } finally { fs.closeSync(fd) }
    fs.renameSync(file + '.tmp', file)
    const dir = fs.openSync(path.dirname(file), 'r')
    try { fs.fsyncSync(dir) } finally { fs.closeSync(dir) }
  }
  latest(source: SourceSetting): CompletedRun | null {
    const all = this.read(source).records
    const records = all.filter(r => r.version).sort((a,b) => b.stamp.localeCompare(a.stamp) || all.indexOf(b) - all.indexOf(a))
    const record = records[0]
    return record ? { stamp: record.stamp, params: record.params } : null
  }
  pendingStamp(source: SourceSetting, params: unknown): string | null {
    return this.read(source).records.filter(r => !r.version && fingerprintsEqual(r.params, params)).map(r => r.stamp).sort().slice(-1)[0] ?? null
  }
  reference(source: SourceSetting, stamp: string | null, params: unknown): CatalogueReference | null {
    const record = this.read(source).records.find(r => r.stamp === stamp && r.version && fingerprintsEqual(r.params, params))
    return record?.version && record.datasetId ? { datasetId: record.datasetId, version: record.version, href: `/signalk/v2/api/resources/forecastDatasets/${record.datasetId}` } : null
  }
  private async request(route: string, method: string, signal?: AbortSignal, json?: unknown, stream?: Readable): Promise<any> {
    const token = this.tokenFile ? fs.readFileSync(this.tokenFile, 'utf8').trim() : ''
    const headers: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {}
    if (json !== undefined) headers['Content-Type'] = 'application/json'
    if (stream) headers['Content-Type'] = 'application/octet-stream'
    const response = await fetch(this.endpoint + API + route, {
      method, headers, body: (stream ?? (json !== undefined ? JSON.stringify(json) : undefined)) as any,
      ...(stream ? { duplex: 'half' } : {}), redirect: 'error',
      signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(stream ? 3600_000 : 300_000)]) : AbortSignal.timeout(stream ? 3600_000 : 300_000),
    })
    if (!response.ok) {
      let code: string | undefined
      try { const body = await response.json() as { code?: unknown }; if (typeof body.code === 'string' && /^[A-Z_]+$/.test(body.code)) code = body.code } catch { /* generic proxy/plugin error */ }
      throw new CatalogueHttpError(response.status, code)
    }
    return response.status === 204 ? undefined : response.json()
  }
  async check(signal?: AbortSignal): Promise<void> {
    const status = await this.request('/status', 'GET', signal)
    if (status.contractVersion !== '0.1') throw new Error('Forecast catalogue contract 0.1 is required')
    // This endpoint is available with the unknown-size streaming extension.
    await this.publications(signal)
  }
  private async publications(signal?: AbortSignal): Promise<any[]> {
    const list = await this.request('/publications', 'GET', signal)
    if (!list || typeof list !== 'object' || Array.isArray(list)) throw new Error('Forecast Dataset Provider streaming publication listing is required')
    return Object.values(list)
  }
  async verify(source: SourceSetting, stamp: string, params: unknown, signal?: AbortSignal): Promise<boolean> {
    const journal = this.read(source)
    const record = journal.records.find(r => r.stamp === stamp && r.version && fingerprintsEqual(r.params, params))
    if (!record) return false
    try { await this.request(`/datasets/${record.datasetId}/versions/${record.version}`, 'GET', signal); return true }
    catch (error) {
      if (!missing(error, 'DATASET_NOT_FOUND')) throw error
      journal.records = journal.records.filter(r => r !== record)
      this.save(source, journal)
      return false
    }
  }
  private expected(record: RecordEntry, id: string): ReceivedAsset | undefined {
    const declared = record.assets.find(a => a.id === id)
    return record.received?.[id] ?? (declared?.sha256 && declared.size ? declared as ReceivedAsset : undefined)
  }
  private same(a: ReceivedAsset, b: ReceivedAsset): boolean {
    return a.id === b.id && a.size === b.size && a.sha256 === b.sha256 && a.mediaType === b.mediaType
  }
  private validateStage(source: SourceSetting, record: RecordEntry, stage: any): void {
    if (!UUID.test(stage.id) || !UUID.test(stage.datasetId) || (record.publicationId && stage.id !== record.publicationId) || (record.datasetId && stage.datasetId !== record.datasetId) ||
      stage.declaration?.kind !== 'forecast' || stage.declaration?.format !== 'grib2' ||
      stage.declaration?.producer !== PRODUCER || stage.declaration.requested?.source !== sourceDirName(source) ||
      !fingerprintsEqual(stage.declaration.requested?.parameters, record.params) ||
      !Array.isArray(stage.declaration.assets) || stage.declaration.assets.length !== record.assets.length ||
      record.assets.some(a => !stage.declaration.assets.some((b: Asset) => a.id === b.id)) || !stage.uploaded || typeof stage.uploaded !== 'object') throw new Error('Staging publication does not match this acquisition')
  }
  private finish(source: SourceSetting, journal: Journal, record: RecordEntry, committed: any): void {
    if (committed.id !== record.datasetId || !/^[a-f0-9]{64}$/.test(committed.version) || !Array.isArray(committed.assets) || committed.assets.length !== record.assets.length ||
      record.assets.some(a => { const expected = this.expected(record, a.id), found = committed.assets.find((b: Asset) => a.id === b.id); return !expected || !found || !this.same(expected, found) })) throw new Error('Committed catalogue content does not match the acquired streams')
    record.version = committed.version
    delete record.publicationId
    this.save(source, journal)
  }
  async publish(source: SourceSetting, plan: RunPlan, params: unknown, log: (message: string) => void, signal?: AbortSignal): Promise<CatalogueReference> {
    signal?.throwIfAborted()
    const journal = this.read(source)
    let record = journal.records.find(r => r.stamp === plan.stamp && fingerprintsEqual(r.params, params))
    if (record?.version) {
      if (await this.verify(source, plan.stamp, params, signal)) { await this.prune(source, journal, log, signal); return this.reference(source, plan.stamp, params)! }
      return this.publish(source, plan, params, log, signal)
    }
    const assets: Asset[] = plan.assets.map(a => ({ id: a.id, mediaType: 'application/octet-stream' }))
    if (!assets.length || new Set(assets.map(a => a.id)).size !== assets.length) throw new Error('Invalid acquisition asset plan')
    if (record && (record.assets.length !== assets.length || record.assets.some(a => !assets.some(b => a.id === b.id)))) throw new Error('Resumed acquisition plan differs from the original declaration')
    if (!record) {
      // Persist the identity BEFORE creation: a lost POST response can be
      // recovered through the provider's publication listing without duplicates.
      record = { stamp: plan.stamp, params, assets, acquisitionId: randomUUID(), received: {} }
      journal.records.push(record)
      this.save(source, journal)
    }
    let stage: any
    if (record.publicationId) {
      try { stage = await this.request(`/publications/${record.publicationId}`, 'GET', signal) }
      catch (error) {
        if (!missing(error, 'PUBLICATION_NOT_FOUND')) throw error
        let committed
        try { committed = await this.request(`/datasets/${record.datasetId}`, 'GET', signal) }
        catch (absent) {
          if (!missing(absent, 'DATASET_NOT_FOUND')) throw absent
          journal.records = journal.records.filter(r => r !== record)
          this.save(source, journal)
          return this.publish(source, plan, params, log, signal)
        }
        this.finish(source, journal, record, committed)
        await this.prune(source, journal, log, signal)
        return this.reference(source, plan.stamp, params)!
      }
    } else {
      const matches = (await this.publications(signal)).filter(p => p.declaration?.producer === PRODUCER && p.declaration?.provenance?.downloaderAcquisitionId === record!.acquisitionId)
      if (matches.length > 1) throw new Error('Ambiguous acquisition recovery; preserve the journal and inspect staging')
      if (matches.length) stage = matches[0]
      else {
        const stamp = plan.stamp, run = `${stamp.slice(0, 4)}-${stamp.slice(4, 6)}-${stamp.slice(6, 8)}T${stamp.slice(9)}:00:00Z`
        stage = await this.request('/publications', 'POST', signal, {
          kind: 'forecast', format: 'grib2', producer: PRODUCER,
          metadata: { name: `${sourceDirName(source)} ${stamp}`, model: source.model.toUpperCase(), run },
          requested: { source: sourceDirName(source), parameters: params },
          provenance: { channel: 'internet', source: sourceDirName(source), run, downloaderAcquisitionId: record.acquisitionId }, assets,
        })
      }
      this.validateStage(source, record, stage)
      record.datasetId = stage.datasetId; record.publicationId = stage.id
      this.save(source, journal)
    }
    this.validateStage(source, record, stage)
    for (const asset of plan.assets) {
      signal?.throwIfAborted()
      const uploaded = stage.uploaded[asset.id]
      if (uploaded) {
        const expected = this.expected(record, asset.id)
        if (!expected || !this.same(expected, uploaded)) throw new Error('Completed staging asset has no matching acquisition receipt; abort this publication before retrying')
        continue
      }
      log(`catalogue: streaming ${asset.id}`)
      await this.upload(source, journal, record, asset, signal)
    }
    const committed = await this.request(`/publications/${record.publicationId}/commit`, 'POST', signal)
    this.finish(source, journal, record, committed)
    log(`catalogue: published ${record.datasetId} (${assets.length} assets)`)
    await this.prune(source, journal, log, signal)
    return this.reference(source, plan.stamp, params)!
  }
  private async upload(source: SourceSetting, journal: Journal, record: RecordEntry, asset: RemoteAsset, signal?: AbortSignal): Promise<void> {
    const controller = new AbortController()
    const active = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal
    const hash = createHash('sha256')
    let size = 0, complete = false, upstreamError: unknown
    const owner = this
    const body = Readable.from((async function* () {
      try {
        for await (const chunk of readAsset(asset, active)) { size += chunk.length; hash.update(chunk); yield chunk }
        const received = { id: asset.id, size, sha256: hash.digest('hex'), mediaType: 'application/octet-stream' }
        record.received ??= {}
        record.received[asset.id] = received
        // Persist successful upstream EOF before the provider can acknowledge it.
        owner.save(source, journal)
        complete = true
      } catch (error) { upstreamError = error; throw error }
    })(), { objectMode: false, highWaterMark: 64 * 1024 })
    try {
      const received = await this.request(`/publications/${record.publicationId}/assets/${asset.id}`, 'PUT', active, undefined, body)
      if (!complete || !this.same(record.received![asset.id], received)) throw new Error('Provider upload receipt differs from the completed upstream stream')
    } catch (error) { throw upstreamError ?? error }
    finally { controller.abort(); body.destroy() }
  }
  private async deleteRecord(record: RecordEntry, signal?: AbortSignal): Promise<void> {
    // An interrupted creation may have no publication ID locally yet.
    if (!record.publicationId && !record.version && record.acquisitionId) {
      const stages = (await this.publications(signal)).filter(p => p.declaration?.provenance?.downloaderAcquisitionId === record.acquisitionId && p.declaration?.producer === PRODUCER)
      for (const stage of stages) await this.request(`/publications/${stage.id}`, 'DELETE', signal)
    }
    if (record.publicationId) {
      try { await this.request(`/publications/${record.publicationId}`, 'DELETE', signal) }
      catch (error) { if (!missing(error, 'PUBLICATION_NOT_FOUND')) throw error }
    }
    if (record.datasetId) {
      try { await this.request(`/datasets/${record.datasetId}`, 'DELETE', signal) }
      catch (error) { if (!missing(error, 'DATASET_NOT_FOUND')) throw error }
    }
  }
  private async prune(source: SourceSetting, journal: Journal, log: (message: string) => void, signal?: AbortSignal): Promise<void> {
    const keep = Math.floor(source.archiveRuns ?? 0) + 1
    const completed = journal.records.filter(r => r.version).sort((a,b) => b.stamp.localeCompare(a.stamp) || journal.records.indexOf(b) - journal.records.indexOf(a))
    const obsolete = journal.records.filter(r => !r.version).concat(completed.slice(keep))
    for (const record of obsolete) {
      try { await this.deleteRecord(record, signal) }
      catch (error) { signal?.throwIfAborted(); log(`catalogue: retention deferred: ${error}`); continue }
      journal.records = journal.records.filter(r => r !== record)
      this.save(source, journal)
    }
  }
  async removeSource(source: SourceSetting, signal?: AbortSignal): Promise<void> {
    const journal = this.read(source)
    for (const record of [...journal.records]) {
      await this.deleteRecord(record, signal)
      journal.records = journal.records.filter(r => r !== record)
      this.save(source, journal)
    }
  }
}
