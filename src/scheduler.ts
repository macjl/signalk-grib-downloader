import { ModelId, SourceSetting, Bbox } from './types'

export interface PublicationWindow {
  earliestMinutes: number
  latestMinutes: number
}

export interface ModelSchedule {
  cycleEveryMinutes: number
  probeEveryMinutes: number
  defaultWindow: PublicationWindow
  windowsByCycleHourUtc?: Partial<Record<number, PublicationWindow>>
}

// Offsets from the UTC cycle, not wall-clock publication hours. Lower
// bounds allow short forecasts to arrive first; upper bounds cover the
// full supported horizon. See README for observations and their limits.
export const MODEL_SCHEDULES: Record<ModelId, ModelSchedule> = {
  gfs: {
    cycleEveryMinutes: 360, probeEveryMinutes: 5,
    defaultWindow: { earliestMinutes: 210, latestMinutes: 330 },
  },
  'gfs-wave': {
    // Initial estimates follow GFS; independent wave-file probes determine
    // actual availability and late runs continue to be retried.
    cycleEveryMinutes: 360, probeEveryMinutes: 5,
    defaultWindow: { earliestMinutes: 210, latestMinutes: 330 },
  },
  arome: {
    cycleEveryMinutes: 180, probeEveryMinutes: 5,
    defaultWindow: { earliestMinutes: 100, latestMinutes: 350 },
    windowsByCycleHourUtc: {
      0: { earliestMinutes: 110, latestMinutes: 185 },
      3: { earliestMinutes: 100, latestMinutes: 180 },
      6: { earliestMinutes: 240, latestMinutes: 350 },
      9: { earliestMinutes: 190, latestMinutes: 285 },
      12: { earliestMinutes: 175, latestMinutes: 275 },
      15: { earliestMinutes: 170, latestMinutes: 255 },
      18: { earliestMinutes: 240, latestMinutes: 340 },
      21: { earliestMinutes: 190, latestMinutes: 280 },
    },
  },
  arpege: {
    cycleEveryMinutes: 360, probeEveryMinutes: 5,
    defaultWindow: { earliestMinutes: 145, latestMinutes: 330 },
    windowsByCycleHourUtc: {
      0: { earliestMinutes: 165, latestMinutes: 255 },
      6: { earliestMinutes: 210, latestMinutes: 330 },
      12: { earliestMinutes: 145, latestMinutes: 270 },
      18: { earliestMinutes: 210, latestMinutes: 300 },
    },
  },
  'icon-eu': {
    cycleEveryMinutes: 360, probeEveryMinutes: 5,
    defaultWindow: { earliestMinutes: 150, latestMinutes: 240 },
  },
}

const MINUTE_MS = 60_000

export function runStamp(run: Date): string {
  return run.toISOString().slice(0, 13).replaceAll('-', '')
}

export function publicationWindow(model: ModelId, run: Date): PublicationWindow {
  const schedule = MODEL_SCHEDULES[model]
  return schedule.windowsByCycleHourUtc?.[run.getUTCHours()] ?? schedule.defaultWindow
}

// Enumerate actual cycles: variable delays cannot be implemented by
// subtracting one delay from now. Windows may overlap across midnight.
function cyclesAround(model: ModelId, now: Date): Date[] {
  const schedule = MODEL_SCHEDULES[model]
  const windows = [schedule.defaultWindow, ...Object.values(schedule.windowsByCycleHourUtc ?? {})]
  const longestDelay = Math.max(...windows.map(w => w!.latestMinutes))
  const cadenceMs = schedule.cycleEveryMinutes * MINUTE_MS
  const base = Math.floor(now.getTime() / cadenceMs) * cadenceMs
  const lookback = Math.ceil(longestDelay / schedule.cycleEveryMinutes) + 4
  return Array.from({ length: lookback + 2 }, (_, i) => new Date(base + (1 - i) * cadenceMs))
}

// Newest-first candidates whose lower bound has opened. The upper bound
// never excludes a run: late publications remain eligible for probing.
export function candidateRuns(model: ModelId, now: Date = new Date(Date.now()), count = 4): Date[] {
  return cyclesAround(model, now)
    .filter(run => run.getTime() + publicationWindow(model, run).earliestMinutes * MINUTE_MS <= now.getTime())
    .slice(0, count)
}

export function probeRunStamp(model: ModelId, now: Date = new Date(Date.now())): string {
  return runStamp(candidateRuns(model, now, 1)[0])
}

export const MODEL_MAX_HOURS: Record<ModelId, number> = {
  gfs: 384,
  'gfs-wave': 384,
  arome: 51,
  arpege: 102,
  'icon-eu': 120,
}

// Météo-France time-range package groups per (model, resolution).
const MF_GROUPS: Record<string, { group: string; fromH: number }[]> = {
  'arome/0025': [
    '00H06H', '07H12H', '13H18H', '19H24H', '25H30H',
    '31H36H', '37H42H', '43H48H', '49H51H',
  ].map(g => ({ group: g, fromH: parseInt(g, 10) })),
  'arome/001': Array.from({ length: 52 }, (_, h) => ({
    group: `${String(h).padStart(2, '0')}H`, fromH: h,
  })),
  'arpege/01': [
    '000H012H', '013H024H', '025H036H', '037H048H', '049H060H',
    '061H072H', '073H084H', '085H096H', '097H102H',
  ].map(g => ({ group: g, fromH: parseInt(g, 10) })),
  'arpege/025': [
    '000H024H', '025H048H', '049H072H', '073H102H',
  ].map(g => ({ group: g, fromH: parseInt(g, 10) })),
}

export function defaultResolution(model: ModelId): string | undefined {
  if (model === 'gfs' || model === 'gfs-wave') return '0p25'
  if (model === 'arome') return '0025'
  if (model === 'arpege') return '01'
  return undefined
}

// Stable catalogue source ID: <model>[-<resolution>]. Atmospheric and wave
// acquisitions keep independent publication journals and retention histories.
export function sourceDirName(s: SourceSetting): string {
  const res = s.resolution || defaultResolution(s.model)
  return res ? `${s.model}-${res}` : s.model
}

// Upper bounds affect the user's lateness indicator only, never probing.
export function expectedRunStamp(model: ModelId, now: Date = new Date(Date.now())): string {
  const run = cyclesAround(model, now).find(run =>
    run.getTime() + publicationWindow(model, run).latestMinutes * MINUTE_MS <= now.getTime())!
  return runStamp(run)
}

// Next unopened lower bound for a run newer than the data already held.
export function nextPublishAt(model: ModelId, now: Date = new Date(Date.now()), lastRun?: string | null): Date {
  const times = cyclesAround(model, now)
    .filter(run => !lastRun || runStamp(run) > lastRun)
    .map(run => run.getTime() + publicationWindow(model, run).earliestMinutes * MINUTE_MS)
    .filter(time => time > now.getTime())
  return new Date(Math.min(...times))
}

// Build the per-source fetch descriptor from a SourceSetting.
// GFS fetch parameters served by the NOMADS grib_filter: the variables and
// levels cross-product. Part of the fetch fingerprint, so a change here
// re-fetches affected runs instead of leaving them silently stale.
export const GFS_FETCH_VARS = ['UGRD', 'VGRD', 'GUST', 'TMP', 'PRMSL', 'RH', 'APCP', 'TCDC']
export const GFS_FETCH_LEVELS = ['10_m_above_ground', '2_m_above_ground', '850_mb', 'surface', 'mean_sea_level', 'entire_atmosphere']
// NOAA GFS-Wave selectors used in contributor haipule's downloader PR #19.
export const GFS_WAVE_VARS = ['HTSGW', 'WVDIR', 'WVPER']

export function downloaderSourceConfig(s: SourceSetting, bbox?: Bbox) {
  const model = s.model
  const res = s.resolution || defaultResolution(model)
  const hours = Math.min(s.hours ?? MODEL_MAX_HOURS[model], MODEL_MAX_HOURS[model])
  const dirName = sourceDirName(s)
  const entry: Record<string, unknown> = {
    name: dirName,
    model,
  }
  if (model === 'gfs' || model === 'gfs-wave') {
    if (model === 'gfs-wave' && res !== '0p25') throw new Error('GFS-Wave supports only 0p25 resolution')
    entry.resolution = res
    entry.steps = { from: 0, to: hours, by: 3 }
    entry.variables = model === 'gfs-wave' ? GFS_WAVE_VARS : GFS_FETCH_VARS
    entry.levels = model === 'gfs-wave' ? ['surface'] : GFS_FETCH_LEVELS
    if (bbox) entry.bbox = [bbox.latMin, bbox.lonMin, bbox.latMax, bbox.lonMax]
  } else if (model === 'icon-eu') {
    entry.steps = { from: 0, to: hours, by: 3 }
  } else {
    entry.resolution = res
    entry.packages = ['SP1']
    const groups = MF_GROUPS[`${model}/${res}`]
    if (!groups) throw new Error(`Unknown ${model} resolution: ${res}`)
    entry.groups = groups.filter(g => g.fromH <= hours).map(g => g.group)
  }
  return entry
}

// Canonical fingerprint of the parameters that shape a source's downloaded
// data — must mirror downloader.py's fetch_fingerprint/normalize_params
// (floats rounded to 2 decimals, dict keys sorted, operational keys
// excluded). Stored in publication journals; preserve compatibility with
// receipts and run markers produced by the earlier downloader.
const FP_EXCLUDE = new Set(['name', 'model', 'directory', 'archive_runs', 'keep_runs'])

function normalizeParams(v: unknown): unknown {
  if (typeof v === 'number') return Number.isInteger(v) ? v : Math.round(v * 100) / 100
  if (Array.isArray(v)) return v.map(normalizeParams)
  if (v !== null && typeof v === 'object') {
    const out: Record<string, unknown> = {}
    for (const k of Object.keys(v as object).sort()) {
      out[k] = normalizeParams((v as Record<string, unknown>)[k])
    }
    return out
  }
  return v
}

export function fetchFingerprint(s: SourceSetting, bbox?: Bbox): unknown {
  const entry = downloaderSourceConfig(s, bbox)
  const params: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(entry)) {
    if (!FP_EXCLUDE.has(k)) params[k] = v
  }
  return normalizeParams(params)
}

export function fingerprintsEqual(a: unknown, b: unknown): boolean {
  // Normalize both sides: the marker side comes from Python's json.dump
  // and may differ in float formatting or key order.
  return JSON.stringify(normalizeParams(a)) === JSON.stringify(normalizeParams(b))
}
