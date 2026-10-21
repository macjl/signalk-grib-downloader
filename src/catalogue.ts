// Producer for Forecast Dataset Provider's experimental HTTP contract 0.1.
// The provider alone owns catalogue storage; never access its directories.
import * as fs from 'node:fs'
import * as path from 'node:path'
import { createHash } from 'node:crypto'
import { SourceSetting } from './types'
import { fingerprintsEqual, sourceDirName } from './scheduler'

const API = '/plugins/signalk-forecast-dataset-provider'
const PRODUCER = 'signalk-grib-downloader'
interface Asset { id: string; size: number; sha256: string; mediaType: string }
interface RecordEntry {
  stamp: string
  params: unknown
  assets: Asset[]
  datasetId: string
  publicationId?: string
  version?: string
}
interface Journal { endpoint: string; records: RecordEntry[] }
export interface CatalogueReference { datasetId: string; version: string; href: string }

export class CatalogueHttpError extends Error {
  constructor(readonly status: number) {
    super(`Forecast Dataset Provider: HTTP ${status}${status === 401 || status === 403 ? ' (check the readwrite token file)' : ''}`)
  }
}

export class CataloguePublisher {
  readonly endpoint: string
  constructor(endpoint: string, private tokenFile: string, private dataDir: string) {
    const url = new URL(endpoint)
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
      throw new Error('Catalogue URL must be an HTTP(S) server URL without credentials, query or fragment')
    }
    if (url.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) {
      throw new Error('Use HTTPS for a remote catalogue server')
    }
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
    fs.writeFileSync(file + '.tmp', JSON.stringify(journal, null, 2), { mode: 0o600 })
    fs.renameSync(file + '.tmp', file)
  }
  reference(source: SourceSetting, stamp: string | null, params: unknown): CatalogueReference | null {
    if (!stamp) return null
    const record = this.read(source).records.find(r => r.stamp === stamp && r.version && fingerprintsEqual(r.params, params))
    return record?.version ? {
      datasetId: record.datasetId, version: record.version,
      href: `/signalk/v2/api/resources/forecastDatasets/${record.datasetId}`,
    } : null
  }

  private async request(route: string, method: string, signal?: AbortSignal, json?: unknown, asset?: { file: string; size: number }): Promise<any> {
    // Read on each request so rotating a token requires no plugin restart.
    const token = this.tokenFile ? fs.readFileSync(this.tokenFile, 'utf8').trim() : ''
    const headers: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {}
    if (json !== undefined) headers['Content-Type'] = 'application/json'
    if (asset) {
      headers['Content-Type'] = 'application/octet-stream'
      headers['Content-Length'] = String(asset.size)
    }
    const body = asset ? fs.createReadStream(asset.file) : json !== undefined ? JSON.stringify(json) : undefined
    try {
      const response = await fetch(this.endpoint + API + route, {
        method, headers, body: body as any, ...(asset ? { duplex: 'half' } : {}),
        redirect: 'error',
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(300_000)]) : AbortSignal.timeout(300_000),
      })
      if (!response.ok) {
        await response.body?.cancel()
        throw new CatalogueHttpError(response.status)
      }
      return response.status === 204 ? undefined : await response.json()
    } finally {
      if (asset) (body as fs.ReadStream).destroy()
    }
  }

  async publish(source: SourceSetting, directory: string, stamp: string, log: (message: string) => void, signal?: AbortSignal): Promise<CatalogueReference> {
    const markerText = fs.readFileSync(path.join(directory, `.run-${stamp}.complete`), 'utf8').trim()
    const marker = markerText ? JSON.parse(markerText) : {}
    const params = marker.params ?? null
    const journal = this.read(source)
    let record = journal.records.find(r => r.stamp === stamp && fingerprintsEqual(r.params, params))
    if (record?.version) {
      // Recover from external deletion without treating a local receipt as proof.
      try {
        await this.request(`/datasets/${record.datasetId}/versions/${record.version}`, 'GET', signal)
        await this.prune(source, journal, log, signal)
        return this.reference(source, stamp, params)!
      } catch (error) {
        if (!(error instanceof CatalogueHttpError) || error.status !== 404) throw error
        journal.records = journal.records.filter(r => r !== record)
        this.save(source, journal)
        record = undefined
      }
    }
    const files = fs.readdirSync(directory).filter(f => f.startsWith(`${sourceDirName(source)}__${stamp}__`) && f.endsWith('.grb2')).sort()
    if (marker.files && JSON.stringify(marker.files) !== JSON.stringify(files)) throw new Error('Completed GRIB manifest does not match local files')
    if (!files.length) throw new Error(`No completed GRIB assets for ${stamp}`)
    const assets: Asset[] = []
    for (const file of files) {
      signal?.throwIfAborted()
      const full = path.join(directory, file)
      if (!fs.lstatSync(full).isFile()) throw new Error('GRIB asset must be a regular file')
      const hash = createHash('sha256')
      let size = 0
      for await (const chunk of fs.createReadStream(full, { signal })) { hash.update(chunk); size += chunk.length }
      assets.push({ id: file.replace('.', '_'), size, sha256: hash.digest('hex'), mediaType: 'application/octet-stream' })
    }
    // Resumed content must match the exact original staged declaration.
    if (record && JSON.stringify(record.assets) !== JSON.stringify(assets)) {
      if (record.publicationId) await this.request(`/publications/${record.publicationId}`, 'DELETE', signal).catch(error => {
        if (!(error instanceof CatalogueHttpError) || error.status !== 404) throw error
      })
      journal.records = journal.records.filter(r => r !== record)
      this.save(source, journal)
      record = undefined
    }
    if (!record) {
      const status = await this.request('/status', 'GET', signal)
      if (status.contractVersion !== '0.1') throw new Error('Forecast catalogue contract 0.1 is required')
      const run = `${stamp.slice(0, 4)}-${stamp.slice(4, 6)}-${stamp.slice(6, 8)}T${stamp.slice(9)}:00:00Z`
      const stage = await this.request('/publications', 'POST', signal, {
        kind: 'forecast', format: 'grib2', producer: PRODUCER,
        metadata: { name: `${sourceDirName(source)} ${stamp}`, model: source.model.toUpperCase(), run },
        // Acquisition choices are claims, never a verified meteorological inventory.
        requested: { source: sourceDirName(source), parameters: params },
        provenance: { channel: 'internet', source: sourceDirName(source), run },
        assets,
      })
      record = { stamp, params, assets, datasetId: stage.datasetId, publicationId: stage.id }
      journal.records.push(record)
      this.save(source, journal)
    }
    let stage: any
    try {
      stage = await this.request(`/publications/${record.publicationId}`, 'GET', signal)
    } catch (error) {
      if (!(error instanceof CatalogueHttpError) || error.status !== 404) throw error
      // A commit response can be lost after the server has committed successfully.
      let committed: any
      try { committed = await this.request(`/datasets/${record.datasetId}`, 'GET', signal) } catch (missing) {
        if (!(missing instanceof CatalogueHttpError) || missing.status !== 404) throw missing
        journal.records = journal.records.filter(r => r !== record)
        this.save(source, journal)
        return this.publish(source, directory, stamp, log, signal)
      }
      this.finish(source, journal, record, committed)
      await this.prune(source, journal, log, signal)
      return this.reference(source, stamp, params)!
    }
    for (let i = 0; i < assets.length; i++) {
      const asset = assets[i]
      const uploaded = stage.uploaded[asset.id]
      if (uploaded) {
        if (uploaded.sha256 !== asset.sha256 || uploaded.size !== asset.size) throw new Error('Staged asset checksum mismatch')
        continue
      }
      log(`catalogue: uploading ${files[i]} (${asset.size} bytes)`)
      await this.request(`/publications/${record.publicationId}/assets/${asset.id}`, 'PUT', signal, undefined, { file: path.join(directory, files[i]), size: asset.size })
    }
    const committed = await this.request(`/publications/${record.publicationId}/commit`, 'POST', signal)
    this.finish(source, journal, record, committed)
    log(`catalogue: published ${record.datasetId} (${record.assets.length} assets)`)
    await this.prune(source, journal, log, signal)
    return this.reference(source, stamp, params)!
  }

  private finish(source: SourceSetting, journal: Journal, record: RecordEntry, committed: any): void {
    if (committed.id !== record.datasetId || !/^[a-f0-9]{64}$/.test(committed.version) ||
      !Array.isArray(committed.assets) || committed.assets.length !== record.assets.length ||
      record.assets.some(a => !committed.assets.some((b: Asset) => a.id === b.id && a.size === b.size && a.sha256 === b.sha256))) {
      throw new Error('Committed catalogue content does not match the download')
    }
    record.version = committed.version
    delete record.publicationId
    this.save(source, journal)
  }

  private async prune(source: SourceSetting, journal: Journal, log: (message: string) => void, signal?: AbortSignal): Promise<void> {
    const keep = Math.floor(source.archiveRuns ?? 0) + 1
    // Superseded unfinished publications must not accumulate indefinitely.
    for (const record of [...journal.records].filter(r => !r.version)) {
      try {
        await this.request(`/publications/${record.publicationId}`, 'DELETE', signal)
      } catch (error) {
        if (!(error instanceof CatalogueHttpError) || error.status !== 404) {
          log(`catalogue: staging cleanup deferred: ${error}`)
          continue
        }
        // A disappeared staging entry can mean a lost commit response.
        await this.request(`/datasets/${record.datasetId}`, 'DELETE', signal).catch(missing => {
          if (!(missing instanceof CatalogueHttpError) || missing.status !== 404) throw missing
        })
      }
      journal.records = journal.records.filter(r => r !== record)
      this.save(source, journal)
    }
    const completed = journal.records.filter(r => r.version).sort((a, b) => b.stamp.localeCompare(a.stamp) || journal.records.indexOf(b) - journal.records.indexOf(a))
    for (const record of completed.slice(keep)) {
      try {
        await this.request(`/datasets/${record.datasetId}`, 'DELETE', signal)
      } catch (error) {
        if (!(error instanceof CatalogueHttpError) || error.status !== 404) {
          log(`catalogue: retention deferred for ${record.datasetId}: ${error}`)
          continue
        }
      }
      journal.records = journal.records.filter(r => r !== record)
      this.save(source, journal)
    }
  }

  async removeSource(source: SourceSetting, signal?: AbortSignal): Promise<void> {
    const journal = this.read(source)
    for (const record of [...journal.records]) {
      const route = record.version ? `/datasets/${record.datasetId}` : `/publications/${record.publicationId}`
      try { await this.request(route, 'DELETE', signal) } catch (error) {
        if (!(error instanceof CatalogueHttpError) || error.status !== 404) throw error
        if (!record.version) await this.request(`/datasets/${record.datasetId}`, 'DELETE', signal).catch(missing => {
          if (!(missing instanceof CatalogueHttpError) || missing.status !== 404) throw missing
        })
      }
      journal.records = journal.records.filter(r => r !== record)
      this.save(source, journal)
    }
  }
}
