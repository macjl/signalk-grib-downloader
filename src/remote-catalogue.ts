import { readFileSync } from 'node:fs'
import { setTimeout as delay } from 'node:timers/promises'
import { remoteModelName, runStamp, sourceDirName } from './scheduler'
import type { Bbox, RemoteServerSetting, SourceSetting } from './types'
import type { RunPlan } from './downloader'
import { mapZone, overlap, validZone, type Zone } from './coverage'
import { normalizeRemoteUrl } from './remote-url'

const api = '/plugins/signalk-forecast-dataset-provider'
const uuid = /^[a-f\d]{8}(?:-[a-f\d]{4}){3}-[a-f\d]{12}$/i
const hash = /^[a-f\d]{64}$/
const assetName = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/
export const remoteId = /^[a-z0-9][a-z0-9-]{0,31}$/

type Field = { parameter: string; level?: { type: string; value?: number; unit?: string } }
type Selection = { bbox?: Zone; fields?: Field[]; from?: string; to?: string }
interface ExportOperation {
  operation: string
  href?: string
  selectors?: string[]
  pendingSelectors?: string[]
  fields?: Field[]
  forecastInterval?: { stepSeconds: number; maxHours: number }
}
interface Horizon {
  minHours: number
  maxHours?: number
  stepHours?: number
  leads?: number[]
}
interface Availability {
  area: boolean
  bounds?: Zone
  horizon?: Horizon
  reason?: 'processing-unavailable' | 'write-required' | 'outside-coverage' | 'split-area' | 'coverage-pending'
  areaClipped?: boolean
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
  requested?: { source?: string; parameters?: { groups?: unknown; steps?: unknown } }
  provenance?: { channel?: string; source?: { id: string; version: string } }
  bounds?: Zone
  forecast?: { from: string; to: string; times?: string[]; stepSeconds?: number }
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
  exactForecastEndpoints?: boolean
  requestedHours?: number
  requestedArea?: Zone
}
interface Job { id: string; href: string; state: string; result?: Dataset }
class RemoteError extends Error {
  constructor(readonly status: number, readonly code?: string, readonly retryAfter?: string | null) {
    super(`Remote Signal K: HTTP ${status}${code ? ' (' + code + ')' : ''}`)
  }
}
export const remoteZone = (bbox?: Bbox): Zone | undefined =>
  bbox ? mapZone(bbox) : undefined

function availableArea(dataset: Dataset, requested?: Zone): Zone[] | undefined {
  if (!requested) return
  return validZone(dataset.bounds) ? overlap(requested, dataset.bounds) : [requested]
}

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

function acquisitionHorizon(dataset: Dataset, run: number): Horizon | undefined {
  if (dataset.producer !== 'signalk-grib-downloader') return
  const parameters = dataset.requested?.parameters
  const lead = (value: unknown): value is number =>
    typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 &&
    Number.isFinite(new Date(run + value * hour).getTime())
  const groups = parameters?.groups
  if (Array.isArray(groups) && groups.length) {
    const ranges: [number, number][] = []
    for (const group of groups) {
      const range = typeof group === 'string' && /^(\d{2,4})H(\d{2,4})H$/.exec(group)
      if (!range || Number(range[1]) > Number(range[2])) return
      ranges.push([Number(range[1]), Number(range[2])])
    }
    return { minHours: Math.min(...ranges.map(r => r[0])), maxHours: Math.max(...ranges.map(r => r[1])), stepHours: 1 }
  }
  const steps = parameters?.steps
  if (Array.isArray(steps)) {
    if (steps.length && steps.every(lead))
      return { minHours: Math.min(...steps), maxHours: Math.max(...steps), stepHours: 1 }
  } else if (steps && typeof steps === 'object' && 'from' in steps && 'to' in steps && 'by' in steps) {
    const { from, to, by } = steps
    if (lead(from) && lead(to) && lead(by) && by > 0 && to >= from)
      return { minHours: from, maxHours: from + Math.floor((to - from) / by) * by, stepHours: 1 }
  }
}

function forecastHorizon(dataset: Dataset, operation: ExportOperation): Horizon | undefined {
  if (!operation.selectors?.includes('from') || !operation.selectors.includes('to')) return
  const forecast = dataset.forecast
  const run = Date.parse(dataset.run ?? (forecast?.times?.length ? forecast.from : ''))
  if (!Number.isFinite(run)) return
  const interval = operation.forecastInterval
  if (!interval) {
    if (Array.isArray(forecast?.times) && forecast.times.length && forecast.times.length <= 10000) {
      const times = forecast.times.map(Date.parse)
      if (times[0] === Date.parse(forecast.from) && times[times.length - 1] === Date.parse(forecast.to) &&
          times.every((value, i) => Number.isFinite(value) && (i === 0 || value > times[i - 1]!))) {
        const leads = times.map(time => (time - run) / hour)
        return { minHours: leads[0]!, maxHours: leads[leads.length - 1]!, leads }
      }
    }
    const from = Date.parse(dataset.forecast?.from ?? '')
    const to = Date.parse(dataset.forecast?.to ?? '')
    if (Number.isFinite(from) && Number.isFinite(to) && to >= Math.max(run, from)) {
      return {
        minHours: Math.max(0, Math.ceil((from - run) / hour)),
        maxHours: Math.ceil((to - run) / hour),
        stepHours: 1
      }
    }
    // Direct acquisitions can describe their original lead hours without a verified inventory.
    return acquisitionHorizon(dataset, run) ?? { minHours: 0, stepHours: 1 }
  }
  if (!operation.selectors.includes('fields') || !Array.isArray(operation.fields) ||
      !operation.fields.length || operation.fields.some(field => typeof field?.parameter !== 'string' || !field.parameter) ||
      !Number.isSafeInteger(interval.stepSeconds / 3600) || interval.stepSeconds <= 0 ||
      !Number.isSafeInteger(interval.maxHours) || interval.maxHours < 1) return
  const stepHours = interval.stepSeconds / 3600
  const step = stepHours * hour
  // This slider represents whole hours relative to the model cycle.
  if (run % hour !== 0) return
  const from = dataset.forecast ? Date.parse(dataset.forecast.from) : run
  const first = Math.ceil(Math.max(run, from) / step) * step
  const to = dataset.forecast ? Date.parse(dataset.forecast.to) : run + interval.maxHours * hour
  const last = Math.floor(Math.min(to, first + interval.maxHours * hour) / step) * step
  if (!Number.isFinite(first) || !Number.isFinite(last) || last <= first) return
  return { minHours: (first - run) / hour, maxHours: (last - run) / hour, stepHours }
}

export class RemoteCatalogue {
  readonly endpoint: string
  constructor(private readonly setting: RemoteServerSetting, private readonly managedToken?: () => string | undefined) {
    this.endpoint = normalizeRemoteUrl(setting.url, setting.allowHttp)
  }

  private token(): string | undefined {
    const managed = this.managedToken?.()
    if (managed) return managed
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
      throw new RemoteError(response.status, code, response.headers.get('Retry-After'))
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

  private async match(model: string, resolution: string | undefined, signal: AbortSignal, requested?: Zone): Promise<Dataset | undefined> {
    let outsideCoverage: Dataset | undefined
    for await (const row of this.list(signal, model)) {
      if (row.model !== model) continue
      if (outsideCoverage && (Boolean(row.run) !== Boolean(outsideCoverage.run) ||
          runDate(row)?.getTime() !== runDate(outsideCoverage)?.getTime())) break
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
      if (availableArea(dataset, requested)?.length === 0) {
        outsideCoverage ??= dataset
        continue
      }
      return dataset
    }
    // Keep the latest matching run's coverage for the map if all its variants are disjoint.
    return outsideCoverage
  }

  private async availability(dataset: Dataset, signal: AbortSignal, checkArea: boolean): Promise<Availability & { operation?: ExportOperation }> {
    const operation = dataset.capabilities?.operations?.find(op => op.operation === 'export')
    if (!operation) return { area: false }
    const area = operation.selectors?.includes('bbox') ?? false
    const pendingArea = checkArea && operation.pendingSelectors?.includes('bbox')
    const horizon = forecastHorizon(dataset, operation)
    if (!area && !horizon && !pendingArea) return { area: false }
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
    if (pendingArea) return { area: false, horizon, reason: 'coverage-pending' }
    if (!this.token()) return { area: false, reason: 'write-required' }
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

  async preview(model: string, resolution: string | undefined, signal: AbortSignal, requested?: Zone): Promise<Availability | null> {
    const dataset = await this.match(model, resolution, signal, requested)
    if (!dataset) return null
    const coverage = validZone(dataset.bounds) ? { bounds: dataset.bounds } : {}
    const areas = availableArea(dataset, requested)
    if (areas?.length === 0) return { ...coverage, area: false, reason: 'outside-coverage' }
    const { area, horizon, reason } = await this.availability(dataset, signal, Boolean(requested))
    if (area && areas && areas.length > 1) return { ...coverage, area: false, reason: 'split-area' }
    const clipped = area && areas?.[0]?.some((value, i) => value !== requested?.[i])
    return { ...coverage, area, horizon, reason, ...(clipped ? { areaClipped: true } : {}) }
  }

  async find(source: SourceSetting, bbox: Bbox | undefined, signal: AbortSignal): Promise<RemoteCandidate | undefined> {
    const requestedArea = remoteZone(bbox)
    const dataset = await this.match(remoteModelName(source), source.resolution, signal, requestedArea)
    if (!dataset) return
    const areas = availableArea(dataset, requestedArea)
    if (areas?.length === 0) return
    const { area, horizon, operation, reason } = await this.availability(dataset, signal, Boolean(requestedArea))
    if (reason === 'coverage-pending') throw new Error('Remote GRIB coverage is still being analyzed')
    const selection: Selection = {}
    const exactForecastEndpoints = Boolean(operation?.forecastInterval || horizon?.leads)
    if (area && areas) {
      if (areas.length > 1) throw new Error('Coverage intersects the map in two areas across the antimeridian; select one side at a time')
      selection.bbox = areas[0]
    }
    if (horizon && source.remoteHours !== undefined) {
      const { minHours, maxHours, stepHours = 1, leads } = horizon
      if (leads && source.remoteHours < minHours)
        throw new Error('No verified forecast time at or before the configured cutoff')
      const requested = Math.max(minHours, Math.min(maxHours ?? Infinity, source.remoteHours))
      const end = leads
        ? leads.reduce((latest, lead) => lead <= requested ? lead : latest, minHours)
        : minHours + Math.floor((requested - minHours) / stepHours) * stepHours
      const run = Date.parse(dataset.run ?? dataset.forecast!.from)
      const first = Date.parse(dataset.forecast?.from ?? '')
      const from = !exactForecastEndpoints && Number.isFinite(first) && first <= run + end * hour
        ? Math.max(run, first) : run + minHours * hour
      if (operation?.forecastInterval) selection.fields = operation.fields
      selection.from = new Date(Math.round(from)).toISOString()
      selection.to = new Date(run + Math.round(end * hour)).toISOString()
    }
    return {
      dataset,
      stamp: runStamp(runDate(dataset)!),
      requestedHours: source.remoteHours,
      ...(selection.bbox?.some((value, i) => value !== requestedArea?.[i]) ? { requestedArea } : {}),
      ...(selection.bbox || selection.from ? { selection, operation: operation!.href, exactForecastEndpoints } : {})
    }
  }

  fingerprint(candidate: RemoteCandidate) {
    const { dataset, selection, requestedHours } = candidate
    // Serialize coordinates as text to retain grid precision. Historical
    // fingerprints round numeric coordinates for the legacy export format.
    return { remoteEndpoint: this.endpoint, remoteId: dataset.id, remoteVersion: dataset.version,
      mode: selection ? 'subset' : 'complete',
      ...(selection?.bbox ? { area: JSON.stringify(selection.bbox) } : {}),
      ...(candidate.requestedArea ? { requestedArea: JSON.stringify(candidate.requestedArea) } : {}),
      ...(selection?.from ? { from: selection.from, to: selection.to } : {}),
      ...(selection?.fields ? { fields: selection.fields } : {}),
      ...(requestedHours === undefined ? {} : { requestedHours }) }
  }

  private async startCrop(candidate: RemoteCandidate, signal: AbortSignal): Promise<Job> {
    const waiting = AbortSignal.any([signal, AbortSignal.timeout(150_000)])
    for (;;) {
      try {
        return await this.request<Job>(candidate.operation!, waiting, {
          operation: 'export', output: { format: 'grib2', layout: 'single-file' }, selection: candidate.selection
        })
      } catch (error) {
        if (!(error instanceof RemoteError && error.status === 429 && error.code === 'PROCESSING_BUSY')) throw error
        const raw = error.retryAfter ?? ''
        const wait = /^\d+$/.test(raw) ? Number(raw) * 1000 : Date.parse(raw) - Date.now()
        const backoff = Math.max(250, Number.isFinite(wait) ? wait : 1000) + Math.random() * 250
        await delay(Math.min(backoff, 150_000), undefined, { signal: waiting })
      }
    }
  }

  private async crop(candidate: RemoteCandidate, signal: AbortSignal): Promise<Dataset> {
    const selection = candidate.selection!
    let job = await this.startCrop(candidate, signal)
    const active = AbortSignal.any([signal, AbortSignal.timeout(180_000)])
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
    if (selection.from) {
      const from = Date.parse(selection.from), to = Date.parse(selection.to!)
      const first = Date.parse(result.forecast?.from ?? ''), last = Date.parse(result.forecast?.to ?? '')
      if (!(from <= first && first <= last && last <= to) ||
          (candidate.exactForecastEndpoints && (first !== from || last !== to)))
        throw new Error('Remote GRIB crop does not match the requested forecast interval')
    }
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
          ...(bounds ? { bounds } : {}),
          ...(forecast ? { forecast: { from: forecast.from, to: forecast.to } } : {}),
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
