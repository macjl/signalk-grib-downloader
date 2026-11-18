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
type Field = { parameter: string; level?: { type: string; value?: number; unit?: string } }
type Selection = { bbox?: Zone; fields?: Field[]; from?: string; to?: string }
interface ExportOperation {
  operation: string
  href?: string
  selectors?: string[]
  fields?: Field[]
  forecastInterval?: { stepSeconds: number; maxHours: number }
}
interface Horizon {
  minHours: number
  maxHours: number
  stepHours: number
}
interface Availability {
  area: boolean
  horizon?: Horizon
  reason?: 'processing-unavailable' | 'write-required'
}
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
  capabilities?: { operations?: ExportOperation[] }
}
export interface RemoteCandidate {
  dataset: Dataset
  stamp: string
  selection?: Selection
  operation?: string
  requestedHours?: number
}
interface Job { id: string; href: string; state: string; result?: Dataset }
class RemoteError extends Error {
  constructor(readonly status: number, readonly code?: string) {
    super(`Remote Signal K: HTTP ${status}${code ? ' (' + code + ')' : ''}`)
  }
}
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

const hour = 3_600_000
function forecastHorizon(dataset: Dataset, operation: ExportOperation): Horizon | undefined {
  const interval = operation.forecastInterval
  if (!operation.selectors?.includes('from') || !operation.selectors.includes('to') ||
      !operation.selectors.includes('fields') || !Array.isArray(operation.fields) ||
      !operation.fields.length || operation.fields.some(field => typeof field?.parameter !== 'string' || !field.parameter) ||
      !interval || !Number.isSafeInteger(interval.stepSeconds / 3600) || interval.stepSeconds <= 0 ||
      !Number.isSafeInteger(interval.maxHours) || interval.maxHours < 1 || !dataset.run) return
  const stepHours = interval.stepSeconds / 3600
  const step = stepHours * hour
  const run = Date.parse(dataset.run)
  // This slider represents whole hours relative to the model cycle.
  if (!Number.isFinite(run) || run % hour !== 0) return
  const from = dataset.forecast ? Date.parse(dataset.forecast.from) : run
  const first = Math.ceil(Math.max(run, from) / step) * step
  const to = dataset.forecast ? Date.parse(dataset.forecast.to) : run + interval.maxHours * hour
  const last = Math.floor(Math.min(to, first + interval.maxHours * hour) / step) * step
  if (!Number.isFinite(first) || !Number.isFinite(last) || last <= first) return
  return { minHours: (first - run) / hour, maxHours: (last - run) / hour, stepHours }
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

  private async match(model: string, resolution: string | undefined, signal: AbortSignal): Promise<Dataset | undefined> {
    for await (const row of this.list(signal, model)) {
      if (row.model !== model) continue
      let dataset: Dataset
      try { dataset = await this.request<Dataset>(versionPath(row), signal) }
      catch (error) {
        if (error instanceof RemoteError && error.status === 404 && error.code === 'DATASET_NOT_FOUND') continue
        throw error
      }
      if (!dataset || dataset.id !== row.id || dataset.version !== row.version)
        throw new Error('Remote catalogue returned a different dataset version')
      const date = runDate(dataset)
      if (!date || !usable(dataset) || dataset.model !== model || !Array.isArray(dataset.assets) || !dataset.assets.length) continue
      if (resolution) {
        const expected = `${model.toLowerCase()}-${resolution}`
        if (reportedResolution(dataset) !== resolution &&
            dataset.requested?.source !== expected &&
            !dataset.assets.every(asset => typeof asset?.id === 'string' && asset.id.startsWith(expected + '__'))) continue
      }
      return dataset
    }
  }

  private async availability(dataset: Dataset, signal: AbortSignal): Promise<Availability & { operation?: ExportOperation }> {
    const operation = dataset.capabilities?.operations?.find(op => op.operation === 'export')
    if (!operation) return { area: false }
    const area = operation.selectors?.includes('bbox') ?? false
    const horizon = forecastHorizon(dataset, operation)
    if (!area && !horizon) return { area: false }
    if (!operation.href) return { area: false, reason: 'processing-unavailable' }
    if (operation.href !== versionPath(dataset) + '/requests')
      throw new Error('Unexpected remote processor link')
    let status: { processing?: string; operations?: string[] }
    try { status = await this.request(`${api}/status`, signal) }
    catch (error) {
      if (error instanceof RemoteError && ([404, 405, 501].includes(error.status) ||
        (error.status === 503 && error.code === 'PROCESSOR_UNAVAILABLE')))
        return { area: false, reason: 'processing-unavailable' }
      throw error
    }
    if (!Array.isArray(status?.operations)) throw new Error('Invalid remote processing status')
    if (status.processing !== 'available' || !status.operations.includes('export'))
      return { area: false, reason: 'processing-unavailable' }
    if (!this.setting.tokenFile) return { area: false, reason: 'write-required' }
    try {
      const login = await this.request<{ status?: string; userLevel?: string }>('/skServer/loginStatus', signal)
      if (login?.status !== 'loggedIn' || !['readwrite', 'admin'].includes(login.userLevel ?? ''))
        return { area: false, reason: 'write-required' }
    } catch (error) {
      if (error instanceof RemoteError && [401, 403].includes(error.status))
        return { area: false, reason: 'write-required' }
      throw error
    }
    return { area, horizon, operation }
  }

  async preview(model: string, resolution: string | undefined, signal: AbortSignal): Promise<Availability | null> {
    const dataset = await this.match(model, resolution, signal)
    if (!dataset) return null
    const { area, horizon, reason } = await this.availability(dataset, signal)
    return { area, horizon, reason }
  }

  async find(source: SourceSetting, bbox: Bbox | undefined, signal: AbortSignal): Promise<RemoteCandidate | undefined> {
    const dataset = await this.match(remoteModelName(source), source.resolution, signal)
    if (!dataset) return
    const { area, horizon, operation } = await this.availability(dataset, signal)
    const selection: Selection = {}
    if (area && bbox) selection.bbox = remoteZone(bbox)
    if (horizon && source.remoteHours !== undefined) {
      const { minHours, maxHours, stepHours } = horizon
      const requested = Math.max(minHours, Math.min(maxHours, source.remoteHours))
      const end = minHours + Math.floor((requested - minHours) / stepHours) * stepHours
      const run = Date.parse(dataset.run!)
      selection.fields = operation!.fields
      selection.from = new Date(run + minHours * hour).toISOString()
      selection.to = new Date(run + end * hour).toISOString()
    }
    return {
      dataset,
      stamp: runStamp(runDate(dataset)!),
      requestedHours: source.remoteHours,
      ...(selection.bbox || selection.from ? { selection, operation: operation!.href } : {})
    }
  }

  fingerprint(candidate: RemoteCandidate) {
    const { dataset, selection, requestedHours } = candidate
    // Serialize coordinates as text to retain grid precision. Historical
    // fingerprints round numeric coordinates for the legacy export format.
    return { remoteEndpoint: this.endpoint, remoteId: dataset.id, remoteVersion: dataset.version,
      mode: selection ? 'subset' : 'complete',
      ...(selection?.bbox ? { area: JSON.stringify(selection.bbox) } : {}),
      ...(selection?.from ? { from: selection.from, to: selection.to, fields: selection.fields } : {}),
      ...(requestedHours === undefined ? {} : { requestedHours }) }
  }

  private async crop(candidate: RemoteCandidate, signal: AbortSignal): Promise<Dataset> {
    const active = AbortSignal.any([signal, AbortSignal.timeout(180_000)])
    const selection = candidate.selection!
    let job = await this.request<Job>(candidate.operation!, active, {
      operation: 'export', output: { format: 'grib2', layout: 'single-file' }, selection
    })
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
    if (selection.from &&
        (Date.parse(result.forecast?.from ?? '') !== Date.parse(selection.from) ||
          Date.parse(result.forecast?.to ?? '') !== Date.parse(selection.to!)))
      throw new Error('Remote GRIB crop does not match the requested forecast interval')
    const requested = selection.bbox
    if (!requested) return result
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
      log('Requesting a subset from the remote GRIB Processor')
      dataset = await this.crop(candidate, signal)
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
          ...(candidate.selection?.bbox ? { bbox: candidate.selection.bbox } : {}),
          ...(candidate.selection?.from ? { forecast: { from: candidate.selection.from, to: candidate.selection.to } } : {})
        }
      }
    }
    return { plan, params: this.fingerprint(candidate) }
  }
}
