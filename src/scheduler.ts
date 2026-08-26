import { ModelId, SourceSetting, Bbox } from './types'

// Model run timing: cycle cadence and delay before the *complete* run
// (all requested forecast hours) is published on the distribution server.
const MODEL_TIMING: Record<ModelId, { cadenceH: number; delayH: number }> = {
  gfs:       { cadenceH: 6, delayH: 5.5 },   // f384 lands ~5h15 after cycle time
  arome:     { cadenceH: 3, delayH: 2.75 },
  arpege:    { cadenceH: 6, delayH: 4.5 },
  'icon-eu': { cadenceH: 6, delayH: 4.0 },
}

export const MODEL_MAX_HOURS: Record<ModelId, number> = {
  gfs: 384,
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
  if (model === 'gfs') return '0p25'
  if (model === 'arome') return '0025'
  if (model === 'arpege') return '01'
  return undefined
}

// Directory name (and source ID): <model>[-<resolution>].
// This is the contract with signalk-grib-weather-provider, which discovers
// subdirectories of the shared root and serves each as a weather provider.
export function sourceDirName(s: SourceSetting): string {
  const res = s.resolution || defaultResolution(s.model)
  return res ? `${s.model}-${res}` : s.model
}

// Stamp ("YYYYMMDDTHH") of the most recent run expected to be fully
// published at `now`, per the model's cadence and publication delay.
export function expectedRunStamp(model: ModelId, now: Date = new Date()): string {
  const { cadenceH, delayH } = MODEL_TIMING[model]
  const t = new Date(now.getTime() - delayH * 3_600_000)
  const cycleHour = Math.floor(t.getUTCHours() / cadenceH) * cadenceH
  const y = t.getUTCFullYear()
  const m = String(t.getUTCMonth() + 1).padStart(2, '0')
  const d = String(t.getUTCDate()).padStart(2, '0')
  return `${y}${m}${d}T${String(cycleHour).padStart(2, '0')}`
}

// Build the per-source fetch descriptor from a SourceSetting.
// `dataRoot` is the local gribs root; the source directory is <root>/<name>.
export function downloaderSourceConfig(s: SourceSetting, dataRoot: string, bbox?: Bbox) {
  const model = s.model
  const res = s.resolution || defaultResolution(model)
  const hours = Math.min(s.hours ?? MODEL_MAX_HOURS[model], MODEL_MAX_HOURS[model])
  const dirName = sourceDirName(s)
  const entry: Record<string, unknown> = {
    name: dirName,
    model,
    directory: `${dataRoot}/${dirName}`,
    archive_runs: Math.max(0, Math.floor(s.archiveRuns ?? 0)),
  }
  if (model === 'gfs') {
    entry.resolution = res
    entry.steps = { from: 0, to: hours, by: 3 }
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
// excluded). Stored by the downloader in run markers; a run is only up to
// date if its marker fingerprint matches the current settings.
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
  const entry = downloaderSourceConfig(s, '', bbox)
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
