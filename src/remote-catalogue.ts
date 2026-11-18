import { readFileSync } from 'node:fs'
import { setTimeout as delay } from 'node:timers/promises'
import { remoteModelName, runStamp, sourceDirName } from './scheduler'
import type { Bbox, RemoteServerSetting, SourceSetting } from './types'
import type { RunPlan } from './downloader'

const api = '/plugins/signalk-forecast-dataset-provider'
const uuid = /^[a-f\d]{8}(?:-[a-f\d]{4}){3}-[a-f\d]{12}$/i
const hash = /^[a-f\d]{64}$/
const assetName = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/
export const remoteId = /^[a-z0-9][a-z0-9-]{0,31}$/

type Zone = [number, number, number, number]
interface Asset {
  id: string
  href: string
  size: number
  sha256: string
}
interface Dataset {
  id: string
  version: string
  run?: string
  createdAt?: string
  name?: string
  kind: string
  state: string
  format: string
  model: string
  producer: string
  expiresAt?: string
  requested?: { source?: string }
  provenance?: { channel?: string; source?: { id: string; version: string } }
  bounds?: Zone
  forecast?: { from: string; to: string }
  fields?: unknown[]
  grid?: Record<string, unknown>
  assets: Asset[]
  capabilities?: { operations?: { operation: string; href?: string; selectors?: string[] }[] }
}
export interface RemoteCandidate {
  dataset: Dataset
  stamp: string
  selection?: Zone
  operation?: string
}
interface Job { id: string; href: string; state: string; result?: Dataset }
class RemoteError extends Error {
  constructor(readonly status: number, readonly code?: string) {
    super(`Remote Signal K: HTTP ${status}${code ? ' (' + code + ')' : ''}`)
  }
}
class ProcessorUnavailable extends Error {}

export function normalizeRemoteUrl(value: string): string {
  const parsed = new URL(value)
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(parsed.hostname)
  if ((parsed.protocol !== 'https:' && !(loopback && parsed.protocol === 'http:')) ||
      parsed.username || parsed.password || parsed.search || parsed.hash)
    throw new Error('Remote Signal K requires an HTTPS URL, without credentials, query or fragment (HTTP is allowed on loopback)')
  return parsed.href.replace(/\/$/, '')
}
export const remoteZone = (bbox?: Bbox): Zone | undefined =>
  bbox ? [bbox.lonMin, bbox.latMin, bbox.lonMax, bbox.latMax] : undefined

export function validRemoteModel(value: unknown): value is string {
  return typeof value === 'string' && value.length <= 512 && value.trim().length > 0 &&
    !/[\x00-\x1f\x7f]/.test(value)
}
const resolutionCode = /^[a-zA-Z0-9]{1,16}$/
function reportedResolution(dataset: Dataset): string | undefined {
  const resolution = dataset.grid?.resolution
  if (typeof resolution === 'string' && resolutionCode.test(resolution)) return resolution
  if (dataset.producer === 'signalk-grib-downloader') {
    const source = dataset.requested?.source ?? (typeof dataset.name === 'string' ? dataset.name.split(' ')[0] : undefined)
    const prefix = dataset.model.toLowerCase() + '-'
    if (typeof source === 'string' && source.startsWith(prefix) && resolutionCode.test(source.slice(prefix.length)))
      return source.slice(prefix.length)
  }
}
function runDate(dataset: Dataset): Date | undefined {
  const value = dataset.run ?? dataset.createdAt
  if (typeof value !== 'string') return
  const date = new Date(value)
  if (Number.isFinite(date.getTime())) return date
}
function usable(dataset: Dataset) {
  return dataset.state === 'complete' && dataset.kind === 'forecast' && dataset.format === 'grib2' &&
    validRemoteModel(dataset.model) && !(dataset.producer === 'signalk-grib-processor' && dataset.expiresAt) &&
    dataset.provenance?.channel !== 'signalk' &&
    (!dataset.expiresAt || Date.parse(dataset.expiresAt) > Date.now())
}
function versionPath(dataset: Pick<Dataset, 'id' | 'version'>): string {
  if (!uuid.test(dataset.id) || !hash.test(dataset.version))
    throw new Error('Invalid remote dataset identity')
  return `${api}/datasets/${dataset.id}/versions/${dataset.version}`
}

export class RemoteCatalogue {
  readonly endpoint: string
  constructor(private readonly setting: RemoteServerSetting) {
    this.endpoint = normalizeRemoteUrl(setting.url)
  }

  private token(): string | undefined {
    if (!this.setting.tokenFile) return undefined
    try {
      const token = readFileSync(this.setting.tokenFile, 'utf8').trim()
      if (token) return token
    } catch {
      // Keep private token file paths out of the status shown in the webapp.
    }
    throw new Error('The configured remote Signal K token is missing or unreadable')
  }

  private async request<T>(route: string, signal: AbortSignal, body?: object): Promise<T> {
    const token = this.token()
    const response = await fetch(this.endpoint + route, {
      method: body ? 'POST' : 'GET',
      redirect: 'error',
      signal: AbortSignal.any([signal, AbortSignal.timeout(30_000)]),
      headers: {
        Accept: 'application/json',
        ...(token ? { Authorization: 'Bearer ' + token } : {}),
        ...(body ? { 'Content-Type': 'application/json' } : {})
      },
      ...(body ? { body: JSON.stringify(body) } : {})
    })
    if (!response.ok) {
      const error = await response.json().catch(() => undefined) as { code?: unknown } | undefined
      const code = typeof error?.code === 'string' && /^[A-Z0-9_]{1,60}$/.test(error.code) ? error.code : undefined
      throw new RemoteError(response.status, code)
    }
    return await response.json() as T
  }

  private async *list(signal: AbortSignal, model?: string): AsyncGenerator<Dataset> {
    const limit = 100
    const seen = new Set<string>()
    const query = model === undefined ? '' : '&model=' + encodeURIComponent(model)
    for (let offset = 0; ; offset += limit) {
      const result = await this.request(`${api}/datasets?format=grib2&kind=forecast${query}&sort=run&limit=${limit}&offset=${offset}`, signal)
      if (!result || typeof result !== 'object' || Array.isArray(result)) throw new Error('Invalid remote catalogue listing')
      const entries = Object.values(result) as Dataset[]
      let added = 0
      for (const row of entries) {
        if (!row || typeof row.id !== 'string' || seen.has(row.id)) continue
        seen.add(row.id)
        added++
        if (usable(row) && runDate(row)) yield row
      }
      if (entries.length < limit) return
      if (!added) throw new Error('Remote catalogue pagination returned repeated datasets')
    }
  }

  async models(signal: AbortSignal): Promise<{ model: string; resolutions: string[] }[]> {
    const rows = new Map<string, Set<string>>()
    for await (const row of this.list(signal)) {
      const resolutions = rows.get(row.model) ?? new Set<string>()
      const resolution = reportedResolution(row)
      if (resolution) resolutions.add(resolution)
      rows.set(row.model, resolutions)
    }
    return [...rows].sort(([a], [b]) => a.localeCompare(b))
      .map(([model, resolutions]) => ({ model, resolutions: [...resolutions].sort() }))
  }

  async find(source: SourceSetting, bbox: Bbox | undefined, signal: AbortSignal): Promise<RemoteCandidate | undefined> {
    const model = remoteModelName(source)
    for await (const row of this.list(signal, model)) {
      if (row.model !== model) continue
      const href = versionPath(row)
      let dataset: Dataset
      try { dataset = await this.request<Dataset>(href, signal) }
      catch (error) {
        if (error instanceof RemoteError && error.status === 404 && error.code === 'DATASET_NOT_FOUND') continue
        throw error
      }
      if (!dataset || dataset.id !== row.id || dataset.version !== row.version)
        throw new Error('Remote catalogue returned a different dataset version')
      const date = runDate(dataset)
      if (!date || !usable(dataset) || dataset.model !== model || !Array.isArray(dataset.assets) || !dataset.assets.length) continue
      if (source.resolution) {
        const expected = `${model.toLowerCase()}-${source.resolution}`
        if (reportedResolution(dataset) !== source.resolution &&
            dataset.requested?.source !== expected &&
            !dataset.assets.every(asset => typeof asset?.id === 'string' && asset.id.startsWith(expected + '__'))) continue
      }
      const operation = dataset.capabilities?.operations?.find(op =>
        op.operation === 'export' && op.selectors?.includes('bbox'))
      if (operation?.href && operation.href !== href + '/requests')
        throw new Error('Unexpected remote processor link')
      return {
        dataset,
        stamp: runStamp(date),
        ...(bbox && operation ? { selection: remoteZone(bbox), operation: href + '/requests' } : {})
      }
    }
  }

  fingerprint(candidate: RemoteCandidate) {
    const { dataset, selection } = candidate
    // Serialize coordinates as text to retain grid precision. Historical
    // fingerprints round numeric coordinates for the legacy export format.
    return { remoteEndpoint: this.endpoint, remoteId: dataset.id, remoteVersion: dataset.version,
      mode: selection ? 'subset' : 'complete', ...(selection ? { area: JSON.stringify(selection) } : {}) }
  }

  private async crop(candidate: RemoteCandidate, signal: AbortSignal): Promise<Dataset> {
    const active = AbortSignal.any([signal, AbortSignal.timeout(180_000)])
    let job: Job
    try {
      job = await this.request<Job>(candidate.operation!, active, {
        operation: 'export', output: { format: 'grib2', layout: 'single-file' },
        selection: { bbox: candidate.selection }
      })
    } catch (error) {
      if (error instanceof RemoteError &&
          ([405, 501].includes(error.status) ||
            (error.status === 404 && error.code !== 'DATASET_NOT_FOUND') ||
            (error.status === 503 && error.code === 'PROCESSOR_UNAVAILABLE')))
        throw new ProcessorUnavailable()
      throw error
    }
    const href = uuid.test(job?.id) ? `${api}/requests/${job.id}` : undefined
    if (!href || job.href !== href) throw new Error('Invalid remote processor job link')
    while (job.state === 'running') {
      await delay(1000, undefined, { signal: active })
      job = await this.request<Job>(href, active)
      if (job?.href !== href) throw new Error('Remote processor returned an unrelated job')
    }
    if (job.state !== 'complete')
      throw new Error(`Remote GRIB crop did not complete (${job.state === 'expired' ? 'expired' : 'failed'})`)
    const result = job.result as Dataset | undefined
    if (!result || result.format !== 'grib2' || result.state !== 'complete' ||
        result.provenance?.source?.id !== candidate.dataset.id || result.provenance.source.version !== candidate.dataset.version)
      throw new Error('Invalid remote GRIB crop result')
    versionPath(result)
    const requested = candidate.selection!
    const actual = result.bounds
    const tolerance = 1e-8
    if (!Array.isArray(actual) || actual.length !== 4 || actual.some(value => !Number.isFinite(value)) ||
        actual[0] < requested[0] - tolerance || actual[1] < requested[1] - tolerance ||
        actual[2] > requested[2] + tolerance || actual[3] > requested[3] + tolerance ||
        actual[0] >= actual[2] || actual[1] >= actual[3])
      throw new Error('Remote GRIB crop exceeds the requested area or has no valid bounds')
    return result
  }

  async prepare(source: SourceSetting, candidate: RemoteCandidate, log: (message: string) => void, signal: AbortSignal) {
    let dataset = candidate.dataset
    if (candidate.selection) {
      log('Requesting selected area from the remote GRIB Processor')
      try { dataset = await this.crop(candidate, signal) }
      catch (error) {
        if (!(error instanceof ProcessorUnavailable)) throw error
        candidate = { dataset: candidate.dataset, stamp: candidate.stamp }
        log('Remote GRIB Processor unavailable; using the complete dataset')
      }
    }
    const href = versionPath(dataset)
    if (!Array.isArray(dataset.assets) || !dataset.assets.length || dataset.assets.length > 1024)
      throw new Error('Remote dataset has an invalid GRIB file count')
    const token = this.token()
    const seen = new Set<string>()
    const byId = dataset.assets.slice().sort((a, b) => String(a?.id).localeCompare(String(b?.id)))
    const assets = byId.map((asset, index) => {
      if (!asset || !assetName.test(asset.id) || seen.has(asset.id) || !hash.test(asset.sha256) ||
          !Number.isSafeInteger(asset.size) || asset.size < 1 ||
          asset.href !== href + '/assets/' + asset.id)
        throw new Error('Invalid remote asset link or checksum')
      seen.add(asset.id)
      return {
        id: `${sourceDirName(source)}__${candidate.stamp}__part${String(index + 1).padStart(4,'0')}_grb2`,
        urls: [this.endpoint + asset.href],
        catalogue: { token, size: asset.size, sha256: asset.sha256 }
      }
    })
    const { bounds, forecast, fields, grid } = dataset
    const plan: RunPlan = {
      stamp: candidate.stamp, probe: '', assets,
      remote: {
        metadata: {
          model: candidate.dataset.model,
          run: candidate.dataset.run === undefined ? undefined : new Date(candidate.dataset.run).toISOString(),
          ...(bounds ? { bounds } : {}), ...(forecast ? { forecast } : {}),
          ...(fields ? { fields } : {}), ...(grid ? { grid } : {}) },
        provenance: {
          remoteServer: this.setting.id, remoteEndpoint: this.endpoint,
          remoteSource: { id: candidate.dataset.id, version: candidate.dataset.version },
          remoteContent: { id: dataset.id, version: dataset.version },
          mode: candidate.selection ? 'subset' : 'complete',
          ...(candidate.selection ? { bbox: candidate.selection } : {})
        }
      }
    }
    return { plan, params: this.fingerprint(candidate) }
  }
}
