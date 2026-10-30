import * as fs from 'fs'
import * as path from 'path'
import { createRequire } from 'module'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import { Bbox, ModelId, SourceSetting } from './types'
import { candidateRuns, downloaderSourceConfig, fetchFingerprint, fingerprintsEqual, runStamp, sourceDirName, GFS_FETCH_VARS, GFS_FETCH_LEVELS, GFS_WAVE_VARS } from './scheduler'

// Native in-process GRIB downloader — a TypeScript port of the previous
// containerised Python job (grib-downloader/downloader.py). No container
// runtime, no image pulls, no Python. Runs on Node ≥ 18.17 (uses global fetch
// and AbortSignal.any).
//
// Outcome protocol per source (mirrors the Python logs):
//   'downloaded' | 'up-to-date' | 'unavailable' | 'failed'
// Log lines are streamed to the caller via `onLog`.

export type Outcome = 'downloaded' | 'up-to-date' | 'unavailable' | 'failed'

export interface DownloadSource {
  name: string
  model: ModelId
  directory: string            // absolute on-disk source directory
  resolution?: string
  steps?: { from: number; to: number; by: number } | number[]
  bbox?: number[]              // [latMin, lonMin, latMax, lonMax] (GFS only)
  variables?: string[]
  levels?: string[]
  packages?: string[]
  groups?: string[]
  archiveRuns?: number
  includeWaves?: boolean
  exportDirectory?: string
  /** Override the upstream base URL (testing / proxying). When set, the
   *  fetcher uses `<base>/...` instead of the model's hardcoded distribution URL. */
  base?: string
  /** Canonical fetch-parameter fingerprint — computed once from the
   *  SourceSetting + bbox via downloaderSourceConfig, the same shape the
   *  orchestrator compares run markers against. Written into run markers so a
   *  run is only up to date if its marker fingerprint matches the settings. */
  fingerprint: unknown
}

export interface FetchResult {
  outcome: Outcome
  log: string[]                // tail of log lines for the webapp status view
}

const UA = 'grib-downloader/0.1 (signalk-grib-weather-provider)'
const HTTP_TIMEOUT_MS = 60_000
const RETRIES = 3
const HTTP_BACKOFF_BASE_MS = 2_000

// ── HTTP helpers ─────────────────────────────────────────────────────────────

// Per-request timeout, combined with the caller's run-level abort signal
// (if any) so aborting a run also cancels its in-flight requests.
// AbortSignal.any: Node ≥ 18.17.
function requestSignal(signal?: AbortSignal): AbortSignal {
  const timeout = AbortSignal.timeout(HTTP_TIMEOUT_MS)
  return signal ? AbortSignal.any([signal, timeout]) : timeout
}

async function httpOk(url: string, signal?: AbortSignal): Promise<boolean> {
  try {
    const r = await fetch(url, { method: 'HEAD', headers: { 'User-Agent': UA }, redirect: 'follow', signal: requestSignal(signal) })
    return r.status === 200
  } catch {
    return false
  }
}

// Stream a URL to `dest` atomically (.part → rename). True on success.
// Retries with linear backoff; an empty body is a failure. The body is piped
// to disk (network read paced by file-write backpressure) instead of being
// buffered in memory. A caller-initiated abort is not retried.
type DownloadFailureLogger = (attempt: number, totalAttempts: number, error: unknown, retryInMs: number | null) => void

async function download(
  url: string,
  dest: string,
  signal?: AbortSignal,
  onFailure?: DownloadFailureLogger
): Promise<boolean> {
  const part = dest + '.part'
  for (let attempt = 1; attempt <= RETRIES; attempt++) {
    let out: fs.WriteStream | null = null
    try {
      const r = await fetch(url, { headers: { 'User-Agent': UA }, redirect: 'follow', signal: requestSignal(signal) })
      if (r.status !== 200 || !r.body) throw new Error(`HTTP ${r.status}`)
      out = fs.createWriteStream(part)
      await pipeline(Readable.fromWeb(r.body), out)
      out = null                            // pipeline already ended the stream
      const stat = fs.statSync(part)
      if (stat.size === 0) throw new Error('empty download')
      fs.renameSync(part, dest)
      return true
    } catch (err) {
      if (out) { try { out.destroy() } catch { /* ignore */ } }
      const retryInMs = !signal?.aborted && attempt < RETRIES
        ? HTTP_BACKOFF_BASE_MS * attempt
        : null
      onFailure?.(attempt, RETRIES, err, retryInMs)
      if (signal?.aborted) break            // run aborted — do not retry
      if (retryInMs !== null) await sleep(retryInMs)
    }
  }
  try { fs.unlinkSync(part) } catch { /* may not exist */ }
  return false
}

// Fetch a whole buffer (used for the bz2 GRIB fragments, which are small).
async function fetchBuffer(url: string, signal?: AbortSignal): Promise<Buffer> {
  const r = await fetch(url, { headers: { 'User-Agent': UA }, redirect: 'follow', signal: requestSignal(signal) })
  if (r.status !== 200) throw new Error(`HTTP ${r.status} for ${url}`)
  return Buffer.from(await r.arrayBuffer())
}

function sleep(ms: number): Promise<void> { return new Promise(r => setTimeout(r, ms)) }

function logDownloadFailure(
  log: (message: string) => void,
  context: string
): DownloadFailureLogger {
  return (attempt, totalAttempts, error, retryInMs) => {
    const detail = error instanceof Error ? `${error.name}: ${error.message}` : String(error)
    const next = retryInMs === null ? 'no retries left' : `retrying in ${retryInMs / 1000}s`
    log(`${context} attempt ${attempt}/${totalAttempts} failed: ${detail} (${next})`)
  }
}

// ── Run / cycle / marker helpers ─────────────────────────────────────────────

type StepsSpec = { from: number; to: number; by: number } | number[] | undefined
function stepsList(spec: StepsSpec): number[] {
  if (Array.isArray(spec)) return spec.map(s => Math.floor(s))
  if (spec && typeof spec === 'object') {
    const out: number[] = []
    for (let s = spec.from; s <= spec.to; s += spec.by) out.push(s)
    return out
  }
  return []
}

function markerPath(directory: string, stamp: string): string {
  return path.join(directory, `.run-${stamp}.complete`)
}

function latestCompleteStamp(directory: string): string | null {
  let files: string[]
  try { files = fs.readdirSync(directory) } catch { return null }
  const stamps = files
    .map(f => /^\.run-(\d{8}T\d{2})\.complete$/.exec(f)?.[1])
    .filter((s): s is string => !!s)
  return stamps.length > 0 ? stamps.sort().reverse()[0] : null
}

function readMarkerParams(directory: string, stamp: string): unknown {
  try {
    const content = fs.readFileSync(markerPath(directory, stamp), 'utf-8').trim()
    if (!content) return null
    const data = JSON.parse(content)
    return (data && typeof data === 'object' && 'params' in data) ? (data as { params: unknown }).params : null
  } catch {
    return null
  }
}

function writeMarker(directory: string, stamp: string, params: unknown): void {
  fs.writeFileSync(markerPath(directory, stamp), JSON.stringify({ params }))
}

// Run stamp encoded in a downloaded filename: '...__<stamp>__...' or a marker.
function fileRunStamp(filename: string): string | null {
  const m = /__(\d{8}T\d{2})__/.exec(filename)
  if (m) return m[1]
  const mm = /^\.run-(\d{8}T\d{2})\.complete$/.exec(filename)
  return mm ? mm[1] : null
}

function wipeRun(directory: string, stamp: string): void {
  let files: string[]
  try { files = fs.readdirSync(directory) } catch { return }
  for (const f of files) {
    if (f.includes(`__${stamp}__`) || f === `.run-${stamp}.complete`) {
      try { fs.unlinkSync(path.join(directory, f)) } catch { /* ignore */ }
    }
  }
}

// After a run completes: keep only the newest complete run in the active dir.
// Older complete runs → archive/ (if archiveRuns > 0) or deleted; leftovers of
// interrupted runs older than the newest complete run are purged, along with
// stale .part files (> 1h). The archive keeps the newest archiveRuns runs.
function cleanupOldRuns(directory: string, archiveRuns: number, log: (m: string) => void, prefix: string): void {
  let files: string[]
  try { files = fs.readdirSync(directory) } catch { return }
  const markers = new Set(files.map(f => /^\.run-(\d{8}T\d{2})\.complete$/.exec(f)?.[1]).filter((s): s is string => !!s))
  if (markers.size === 0) return
  const newest = [...markers].sort().reverse()[0]
  const archiveDir = path.join(directory, 'archive')
  const archived = new Set<string>()
  const deleted = new Set<string>()

  for (const f of files) {
    const full = path.join(directory, f)
    let st: fs.Stats
    try { st = fs.statSync(full) } catch { continue }
    if (!st.isFile()) continue
    if (f.endsWith('.part')) {
      try { if (Date.now() - st.mtimeMs > 3_600_000) fs.unlinkSync(full); log(`${prefix}: removed stale partial file ${f}`) } catch { /* ignore */ }
      continue
    }
    const stamp = fileRunStamp(f)
    if (stamp === null || stamp >= newest) continue
    try {
      if (markers.has(stamp) && archiveRuns > 0) {
        fs.mkdirSync(archiveDir, { recursive: true })
        fs.renameSync(full, path.join(archiveDir, f))
        archived.add(stamp)
      } else {
        fs.unlinkSync(full)
        deleted.add(stamp)
      }
    } catch (e) {
      log(`${prefix}: cannot clean ${f}: ${e}`)
    }
  }
  for (const s of [...archived].sort()) log(`${prefix}: archived run ${s}`)
  for (const s of [...deleted].sort()) log(`${prefix}: purged run ${s}`)

  if (archiveRuns > 0 && fs.existsSync(archiveDir)) {
    const afiles = fs.readdirSync(archiveDir)
    // Distinct run stamps only (a run may have several files + a marker).
    const astamps = [...new Set(afiles.map(fileRunStamp).filter((s): s is string => !!s))].sort().reverse()
    for (const old of astamps.slice(archiveRuns)) {
      for (const f of afiles) {
        if (fileRunStamp(f) === old) {
          try { fs.unlinkSync(path.join(archiveDir, f)) } catch (e) { log(`${prefix}: cannot prune archive ${f}: ${e}`) }
        }
      }
      log(`${prefix}: pruned archived run ${old}`)
    }
  }
}

// ── GFS (NOMADS grib_filter) ─────────────────────────────────────────────────

const GFS_PRODUCTS: Record<string, string> = { '0p25': 'pgrb2.0p25', '0p50': 'pgrb2full.0p50', '1p00': 'pgrb2.1p00' }
async function concatenateFilesAtomic(inputs: string[], dest: string): Promise<void> {
  const part = dest + '.part'
  try { fs.unlinkSync(part) } catch { /* may not exist */ }
  try {
    for (let i = 0; i < inputs.length; i++) {
      await pipeline(
        fs.createReadStream(inputs[i]),
        fs.createWriteStream(part, { flags: i === 0 ? 'w' : 'a' })
      )
    }
    if (fs.statSync(part).size === 0) throw new Error('empty concatenated output')
    fs.renameSync(part, dest)
  } catch (e) {
    try { fs.unlinkSync(part) } catch { /* may not exist */ }
    throw e
  }
}

function cleanupOpenCpnExports(directory: string, name: string, archiveRuns: number, log: (m: string) => void): void {
  let files: string[]
  try { files = fs.readdirSync(directory) } catch { return }
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const pattern = new RegExp(`^${escaped}__(\\d{8}T\\d{2})__complete\\.grb2$`)
  const runs = files
    .map(file => ({ file, stamp: pattern.exec(file)?.[1] }))
    .filter((entry): entry is { file: string; stamp: string } => !!entry.stamp)
    .sort((a, b) => b.stamp.localeCompare(a.stamp))
  const keep = Math.max(1, archiveRuns + 1)
  for (const old of runs.slice(keep)) {
    try {
      fs.unlinkSync(path.join(directory, old.file))
      log(`${name}: removed old OpenCPN export ${old.file}`)
    } catch (e) {
      log(`${name}: cannot remove old OpenCPN export ${old.file}: ${e}`)
    }
  }
}

function runFiles(directory: string, stamp: string): string[] {
  try {
    return fs.readdirSync(directory).filter(file =>
      file.includes(`__${stamp}__`) || file === `.run-${stamp}.complete`
    )
  } catch {
    return []
  }
}

// Replace a completed same-cycle GFS run only after its full replacement and
// OpenCPN bundle have been staged. Every old file is moved to a hidden backup
// first; any commit failure restores it before the error is returned.
function commitStagedGfsRun(
  directory: string,
  stagingDirectory: string,
  stamp: string,
  fingerprint: unknown,
  stagedBundle?: string,
  finalBundle?: string
): void {
  const backupDirectory = path.join(directory, `.replacement-backup-${stamp}`)
  const bundleBackup = finalBundle ? finalBundle + '.replacement-backup' : undefined
  fs.rmSync(backupDirectory, { recursive: true, force: true })
  fs.mkdirSync(backupDirectory, { recursive: true })
  let bundlePublished = false
  let oldBundleBackedUp = false
  try {
    for (const file of runFiles(directory, stamp)) {
      fs.renameSync(path.join(directory, file), path.join(backupDirectory, file))
    }
    for (const file of fs.readdirSync(stagingDirectory)) {
      if (file.endsWith('.grb2')) fs.renameSync(path.join(stagingDirectory, file), path.join(directory, file))
    }
    if (stagedBundle && finalBundle) {
      if (fs.existsSync(finalBundle)) {
        try { fs.unlinkSync(bundleBackup!) } catch { /* may not exist */ }
        fs.renameSync(finalBundle, bundleBackup!)
        oldBundleBackedUp = true
      }
      fs.renameSync(stagedBundle, finalBundle)
      bundlePublished = true
    }
    writeMarker(directory, stamp, fingerprint)
    // The replacement is committed once the new marker exists. Cleanup must
    // not turn a successful commit into a rollback after backups are removed.
    try { fs.rmSync(backupDirectory, { recursive: true, force: true }) } catch { /* non-fatal leftover */ }
    if (oldBundleBackedUp) {
      try { fs.unlinkSync(bundleBackup!) } catch { /* non-fatal leftover */ }
    }
    try { fs.rmSync(stagingDirectory, { recursive: true, force: true }) } catch { /* non-fatal leftover */ }
  } catch (e) {
    for (const file of runFiles(directory, stamp)) {
      try { fs.unlinkSync(path.join(directory, file)) } catch { /* ignore */ }
    }
    let restoreError: unknown = null
    try {
      for (const file of fs.readdirSync(backupDirectory)) {
        try {
          fs.renameSync(path.join(backupDirectory, file), path.join(directory, file))
        } catch (err) {
          restoreError = restoreError ?? err
        }
      }
    } catch (err) { restoreError = restoreError ?? err }
    if (bundlePublished && finalBundle) {
      try { fs.unlinkSync(finalBundle) } catch { /* ignore */ }
    }
    if (oldBundleBackedUp && bundleBackup && finalBundle) {
      try { fs.renameSync(bundleBackup, finalBundle) } catch (err) { restoreError = restoreError ?? err }
    }
    // Never recursively remove a backup that could still contain an original
    // file after an incomplete rollback.
    try { fs.rmdirSync(backupDirectory) } catch { /* keep non-empty backup for recovery */ }
    if (restoreError) {
      throw new Error(`${e}; rollback incomplete (backup retained at ${backupDirectory}): ${restoreError}`)
    }
    throw e
  }
}

async function gfsFetch(src: DownloadSource, log: (m: string) => void, signal?: AbortSignal): Promise<Outcome> {
  const res = src.resolution ?? '0p25'
  const directory = src.directory
  const steps = stepsList(src.steps ?? { from: 0, to: 24, by: 3 })
  const bbox = src.bbox
  const variables = src.variables ?? GFS_FETCH_VARS
  const levels = src.levels ?? GFS_FETCH_LEVELS
  const name = src.name
  const prod = GFS_PRODUCTS[res] ?? `pgrb2.${res}`
  const fingerprint = src.fingerprint

  if (src.includeWaves && res !== '0p25') {
    log(`${name}: GFS-Wave requires GFS 0p25 resolution`)
    return 'failed'
  }
  if (src.includeWaves && !src.exportDirectory) {
    log(`${name}: GFS-Wave requires an OpenCPN export directory`)
    return 'failed'
  }
  if (src.includeWaves && path.resolve(src.exportDirectory!) === path.resolve(directory)) {
    log(`${name}: OpenCPN export directory must be separate from the provider source directory`)
    return 'failed'
  }

  for (const run of candidateRuns(src.model)) {
    const stamp = runStamp(run)
    if (signal?.aborted) { log(`${name}: aborted — cancelling run search`); return 'failed' }
    let paramsChanged = false
    const latest = latestCompleteStamp(directory)
    if (latest !== null && latest >= stamp) {
      if (fingerprintsEqual(readMarkerParams(directory, latest), fingerprint)) {
        const bundle = src.exportDirectory
          ? path.join(src.exportDirectory, `${name}__${latest}__complete.grb2`)
          : null
        if (!src.includeWaves || (bundle && fs.existsSync(bundle) && fs.statSync(bundle).size > 0)) return 'up-to-date'
        log(`${name}: OpenCPN bundle missing — rebuilding run ${latest}`)
      }
      if (latest > stamp) continue
      else if (!fingerprintsEqual(readMarkerParams(directory, latest), fingerprint)) paramsChanged = true
    }
    const date = stamp.slice(0, 8)
    const hh = stamp.slice(9, 11)
    const nomads = src.base ?? 'https://nomads.ncep.noaa.gov'
    const probe = `${nomads}/pub/data/nccf/com/gfs/prod/gfs.${date}/${hh}/atmos/gfs.t${hh}z.${prod}.f${String(steps[steps.length - 1]).padStart(3, '0')}.idx`
    if (!(await httpOk(probe, signal))) { log(`${name}: run ${stamp} not published yet (probe failed)`); continue }
    if (src.includeWaves) {
      const waveProbe = `${nomads}/pub/data/nccf/com/gfs/prod/gfs.${date}/${hh}/wave/gridded/gfswave.t${hh}z.global.0p25.f${String(steps[steps.length - 1]).padStart(3, '0')}.grib2.idx`
      if (!(await httpOk(waveProbe, signal))) { log(`${name}: GFS-Wave run ${stamp} not published yet (probe failed)`); continue }
    }

    const stagingDirectory = path.join(directory, `.replacement-staging-${stamp}`)
    const outputDirectory = paramsChanged ? stagingDirectory : directory
    if (paramsChanged) {
      log(`${name}: fetch parameters changed — staging replacement run ${stamp}`)
      fs.rmSync(stagingDirectory, { recursive: true, force: true })
      fs.mkdirSync(stagingDirectory, { recursive: true })
    }

    log(`${name}: downloading run ${stamp} (${steps.length} steps)`)
    const params = variables.map(v => `var_${v}=on`).concat(levels.map(l => `lev_${l}=on`))
    if (bbox) {
      const [lat0, lon0, lat1, lon1] = bbox
      params.push('subregion=', `leftlon=${lon0}`, `rightlon=${lon1}`, `bottomlat=${lat0}`, `toplat=${lat1}`)
    }
    for (const step of steps) {
      const dest = path.join(outputDirectory, `${name}__${stamp}__f${String(step).padStart(3, '0')}.grb2`)
      if (fs.existsSync(dest)) continue
      const fff = String(step).padStart(3, '0')
      const url = `${nomads}/cgi-bin/filter_gfs_${res}.pl?dir=%2Fgfs.${date}%2F${hh}%2Fatmos&file=gfs.t${hh}z.${prod}.f${fff}&` + params.join('&')
      if (!src.includeWaves) {
        if (!(await download(url, dest, signal, logDownloadFailure(log, `${name}: run ${stamp} step f${fff}`)))) {
          log(`${name}: run ${stamp} failed at step f${fff} — aborting`)
          return 'failed'
        }
        continue
      }

      const atmos = dest + '.atmos'
      const wave = dest + '.wave'
      if (!fs.existsSync(atmos) && !(await download(
        url,
        atmos,
        signal,
        logDownloadFailure(log, `${name}: run ${stamp} atmosphere f${fff}`)
      ))) {
        log(`${name}: run ${stamp} atmosphere failed at step f${fff} — aborting`)
        return 'failed'
      }
      const waveParams = GFS_WAVE_VARS.map(v => `var_${v}=on`)
        .concat(['lev_surface=on'])
      if (bbox) {
        const [lat0, lon0, lat1, lon1] = bbox
        waveParams.push('subregion=', `leftlon=${lon0}`, `rightlon=${lon1}`, `bottomlat=${lat0}`, `toplat=${lat1}`)
      }
      const waveUrl = `${nomads}/cgi-bin/filter_gfswave.pl?dir=%2Fgfs.${date}%2F${hh}%2Fwave%2Fgridded&file=gfswave.t${hh}z.global.0p25.f${fff}.grib2&` + waveParams.join('&')
      if (!fs.existsSync(wave) && !(await download(
        waveUrl,
        wave,
        signal,
        logDownloadFailure(log, `${name}: run ${stamp} GFS-Wave f${fff}`)
      ))) {
        log(`${name}: run ${stamp} GFS-Wave failed at step f${fff} — aborting`)
        return 'failed'
      }
      try {
        await concatenateFilesAtomic([atmos, wave], dest)
        fs.unlinkSync(atmos)
        fs.unlinkSync(wave)
      } catch (e) {
        log(`${name}: run ${stamp} cannot combine step f${fff}: ${e} — aborting`)
        return 'failed'
      }
    }
    let stagedBundle: string | undefined
    let finalBundle: string | undefined
    if (src.includeWaves) {
      const exportDir = src.exportDirectory!
      fs.mkdirSync(exportDir, { recursive: true })
      finalBundle = path.join(exportDir, `${name}__${stamp}__complete.grb2`)
      stagedBundle = paramsChanged ? finalBundle + '.replacement-staged' : finalBundle
      const stepFiles = steps.map(step => path.join(outputDirectory, `${name}__${stamp}__f${String(step).padStart(3, '0')}.grb2`))
      try {
        await concatenateFilesAtomic(stepFiles, stagedBundle)
      } catch (e) {
        log(`${name}: run ${stamp} cannot publish OpenCPN bundle: ${e} — aborting`)
        return 'failed'
      }
    }
    if (paramsChanged) {
      try {
        commitStagedGfsRun(directory, stagingDirectory, stamp, fingerprint, stagedBundle, finalBundle)
      } catch (e) {
        log(`${name}: run ${stamp} replacement commit failed: ${e} — previous run restored`)
        return 'failed'
      }
    } else {
      writeMarker(directory, stamp, fingerprint)
    }
    if (src.includeWaves) cleanupOpenCpnExports(src.exportDirectory!, name, src.archiveRuns ?? 0, log)
    cleanupOldRuns(directory, src.archiveRuns ?? 0, log, name)
    log(`${name}: run ${stamp} complete`)
    return 'downloaded'
  }
  log(`${name}: no published run found among recent cycles`)
  return 'unavailable'
}

// ── Météo-France (AROME / ARPEGE, OVH public bucket) ─────────────────────────

const MF_BASE = 'https://meteofrance-pnt.s3.rbx.io.cloud.ovh.net/pnt'
const MF_MODELS: Record<string, { template: string }> = {
  'arome/0025': { template: '{d}/arome/0025/{p}/arome__0025__{p}__{g}__{d}.grib2' },
  'arome/001': { template: '{d}/arome/001/{p}/arome__001__{p}__{g}__{d}.grib2' },
  'arpege/025': { template: '{d}/arpege/025/{p}/arpege__025__{p}__{g}__{d}.grib2' },
  'arpege/01': { template: '{d}/arpege/01/{p}/arpege__01__{p}__{g}__{d}.grib2' },
}

function mfUrl(base: string, template: string, d: string, p: string, g: string): string {
  return `${base}/${template.replaceAll('{d}', d).replaceAll('{p}', p).replaceAll('{g}', g)}`
}

async function mfFetch(src: DownloadSource, log: (m: string) => void, signal?: AbortSignal): Promise<Outcome> {
  const model = src.model
  const res = src.resolution ?? (model === 'arome' ? '0025' : '025')
  const key = `${model}/${res}`
  const spec = MF_MODELS[key]
  if (!spec) { log(`${src.name}: unknown ${model} resolution ${res}`); return 'failed' }
  const groups = src.groups ?? []
  const packages = src.packages ?? ['SP1']
  const directory = src.directory
  const name = src.name
  const fingerprint = src.fingerprint

  const mfBase = src.base ?? MF_BASE
  for (const run of candidateRuns(src.model)) {
    const stamp = runStamp(run)
    if (signal?.aborted) { log(`${name}: aborted — cancelling run search`); return 'failed' }
    let paramsChanged = false
    const latest = latestCompleteStamp(directory)
    if (latest !== null && latest >= stamp) {
      if (fingerprintsEqual(readMarkerParams(directory, latest), fingerprint)) return 'up-to-date'
      if (latest > stamp) continue
      paramsChanged = true
    }
    const d = `${stamp.slice(0, 4)}-${stamp.slice(4, 6)}-${stamp.slice(6, 8)}T${stamp.slice(9, 11)}:00:00Z`
    const probe = mfUrl(mfBase, spec.template, d, packages[0], groups[groups.length - 1])
    if (!(await httpOk(probe, signal))) { log(`${name}: run ${stamp} not published yet (probe failed)`); continue }

    if (paramsChanged) { log(`${name}: fetch parameters changed — re-fetching run ${stamp}`); wipeRun(directory, stamp) }

    log(`${name}: downloading run ${stamp} (${packages.length} packages × ${groups.length} groups)`)
    for (const p of packages) {
      for (const g of groups) {
        const dest = path.join(directory, `${name}__${stamp}__${p}_${g}.grb2`)
        if (fs.existsSync(dest)) continue
        const url = mfUrl(mfBase, spec.template, d, p, g)
        if (!(await download(url, dest, signal, logDownloadFailure(log, `${name}: run ${stamp} ${p}/${g}`)))) {
          log(`${name}: run ${stamp} failed at ${p}/${g} — aborting`)
          return 'failed'
        }
      }
    }
    writeMarker(directory, stamp, fingerprint)
    cleanupOldRuns(directory, src.archiveRuns ?? 0, log, name)
    log(`${name}: run ${stamp} complete`)
    return 'downloaded'
  }
  log(`${name}: no published run found among recent cycles`)
  return 'unavailable'
}

// ── DWD ICON-EU (opendata.dwd.de) ────────────────────────────────────────────

const ICON_EU_DEFAULT_VARS = ['t_2m', 'u_10m', 'v_10m', 'vmax_10m', 'pmsl', 'relhum_2m', 'tot_prec', 'clct']
const ICON_EU_BASE = 'https://opendata.dwd.de/weather/nwp/icon-eu/grib'

// Minimal surface of @digitaldefiance/bzip2-wasm we drive, plus the raw
// emscripten module the wrapper class expects on `wasmModule`.
interface Bz2RawModule {
  _malloc(n: number): number
  _free(p: number): void
  HEAPU8: Uint8Array
  setValue(p: number, v: number, t: string): void
  getValue(p: number, t: string): number
  _BZ2_bzBuffToBuffDecompress(...args: number[]): number
}
interface Bz2 {
  wasmModule: Bz2RawModule
  init(): Promise<void>
  compress(d: Uint8Array, blockSize: number, len: number): Uint8Array
  decompress(c: Uint8Array, n: number): Uint8Array
}

// @digitaldefiance/bzip2-wasm@1.1.1 locates its bzip2.wasm by slicing the
// literal 7 characters 'file://' off its module URL. POSIX survives
// ('file:///a/b' → '/a/b'); on Windows 'file:///D:/a/b' → '/D:/a/b', which
// the fs layer resolves against the working directory's drive and fails as
// 'D:\D:\a\b'. The package exposes no hook to correct the path, so hand the
// raw emscripten factory the wasm bytes ourselves and attach the module to
// the package's BZip2 wrapper — init()'s file lookup never runs. The glue
// still evaluates its Node environment prologue, which expects the CJS
// globals the package's own init() would have staged (they are unread there
// when a wasmBinary is supplied, so the values only need to exist).
export async function loadBz2(): Promise<Bz2> {
  const req = createRequire(__filename)
  const pkgDir = path.dirname(req.resolve('@digitaldefiance/bzip2-wasm/package.json'))
  const wasmBinary = new Uint8Array(fs.readFileSync(path.join(pkgDir, 'bzip2-1.0.8', 'bzip2.wasm')))
  const BZip2 = req('@digitaldefiance/bzip2-wasm').default as { new (): Bz2 }
  const loadRaw = req('@digitaldefiance/bzip2-wasm/bzip2-1.0.8/bzip2.mjs').default as
    (moduleArg?: { wasmBinary?: Uint8Array }) => Promise<Bz2RawModule>
  const inst = new BZip2()
  const globals = globalThis as unknown as { __dirname?: string; require?: unknown }
  const savedDirname = globals.__dirname
  const savedRequire = globals.require
  globals.__dirname = pkgDir
  globals.require = req
  try {
    inst.wasmModule = await loadRaw({ wasmBinary })
  } finally {
    if (savedDirname === undefined) delete globals.__dirname
    else globals.__dirname = savedDirname
    if (savedRequire === undefined) delete globals.require
    else globals.require = savedRequire
  }
  return inst
}

// Lazy wasm init — non-ICON-EU sources never load it.
let bz2Promise: Promise<Bz2> | null = null
async function bz2Decompress(comp: Buffer): Promise<Buffer> {
  if (!bz2Promise) bz2Promise = loadBz2()
  const inst = await bz2Promise
  // Output size is unknown for GRIB2; grow until it fits (BZ_OUTBUFF_FULL).
  let outCap = Math.max(comp.length * 4, 128)
  for (;;) {
    try {
      return Buffer.from(inst.decompress(comp, outCap))
    } catch (e) {
      if (!/OUTBUFF_FULL/i.test(String(e))) throw e
      outCap *= 2
    }
  }
}

async function iconEuFetch(src: DownloadSource, log: (m: string) => void, signal?: AbortSignal): Promise<Outcome> {
  const directory = src.directory
  const steps = stepsList(src.steps ?? { from: 0, to: 48, by: 3 })
  const variables = src.variables ?? ICON_EU_DEFAULT_VARS
  const name = src.name
  const fingerprint = src.fingerprint

  const iconBase = src.base ?? ICON_EU_BASE
  for (const run of candidateRuns(src.model)) {
    const stamp = runStamp(run)
    if (signal?.aborted) { log(`${name}: aborted — cancelling run search`); return 'failed' }
    let paramsChanged = false
    const latest = latestCompleteStamp(directory)
    if (latest !== null && latest >= stamp) {
      if (fingerprintsEqual(readMarkerParams(directory, latest), fingerprint)) return 'up-to-date'
      if (latest > stamp) continue
      paramsChanged = true
    }
    const hh = stamp.slice(9, 11)
    const ymdh = stamp.replace('T', '')
    const v0 = variables[0]
    const probe = `${iconBase}/${hh}/${v0}/icon-eu_europe_regular-lat-lon_single-level_${ymdh}_${String(steps[steps.length - 1]).padStart(3, '0')}_${v0.toUpperCase()}.grib2.bz2`
    if (!(await httpOk(probe, signal))) { log(`${name}: run ${stamp} not published yet (probe failed)`); continue }

    if (paramsChanged) { log(`${name}: fetch parameters changed — re-fetching run ${stamp}`); wipeRun(directory, stamp) }

    log(`${name}: downloading run ${stamp} (${steps.length} steps × ${variables.length} vars)`)
    for (const step of steps) {
      const dest = path.join(directory, `${name}__${stamp}__f${String(step).padStart(3, '0')}.grb2`)
      if (fs.existsSync(dest)) continue
      const part = dest + '.part'
      try {
        // One GRIB per variable — decompress and concatenate into one file
        // per step (the provider expects all variables of a validity time
        // in a single GRIB file).
        const chunks: Buffer[] = []
        for (const v of variables) {
          const url = `${iconBase}/${hh}/${v}/icon-eu_europe_regular-lat-lon_single-level_${ymdh}_${String(step).padStart(3, '0')}_${v.toUpperCase()}.grib2.bz2`
          const comp = await fetchBuffer(url, signal)
          chunks.push(await bz2Decompress(comp))
        }
        fs.writeFileSync(part, Buffer.concat(chunks))
        fs.renameSync(part, dest)
      } catch (e) {
        log(`${name}: run ${stamp} failed at step f${String(step).padStart(3, '0')}: ${e} — aborting`)
        try { fs.unlinkSync(part) } catch { /* ignore */ }
        return 'failed'
      }
    }
    writeMarker(directory, stamp, fingerprint)
    cleanupOldRuns(directory, src.archiveRuns ?? 0, log, name)
    log(`${name}: run ${stamp} complete`)
    return 'downloaded'
  }
  log(`${name}: no published run found among recent cycles`)
  return 'unavailable'
}

// ── Public entry point ───────────────────────────────────────────────────────

const FETCHERS: Record<ModelId, (s: DownloadSource, log: (m: string) => void, signal?: AbortSignal) => Promise<Outcome>> = {
  gfs: gfsFetch,
  arome: mfFetch,
  arpege: mfFetch,
  'icon-eu': iconEuFetch,
}

// Build the on-disk DownloadSource for a SourceSetting, resolving the
// directory under the local gribsRoot (no container path remapping).
export function toDownloadSource(s: SourceSetting, gribsRoot: string, bbox?: Bbox, opencpnExportRoot?: string): DownloadSource {
  const cfg = downloaderSourceConfig(s, gribsRoot, bbox) as Record<string, unknown>
  return {
    name: cfg.name as string,
    model: cfg.model as ModelId,
    directory: cfg.directory as string,
    resolution: cfg.resolution as string | undefined,
    steps: cfg.steps as DownloadSource['steps'],
    variables: cfg.variables as string[] | undefined,
    levels: cfg.levels as string[] | undefined,
    bbox: cfg.bbox as number[] | undefined,
    packages: cfg.packages as string[] | undefined,
    groups: cfg.groups as string[] | undefined,
    archiveRuns: (cfg.archive_runs as number | undefined) ?? 0,
    includeWaves: Array.isArray(cfg.wave_variables),
    exportDirectory: Array.isArray(cfg.wave_variables) && opencpnExportRoot
      ? path.join(opencpnExportRoot, cfg.name as string)
      : undefined,
    // Canonical fingerprint — the same shape the orchestrator compares run
    // markers against, so a run is up to date iff its marker matches.
    fingerprint: fetchFingerprint(s, bbox),
  }
}

// Fetch one source's latest run. `signal` aborts the work in flight: every
// HTTP request of the run is bound to it, so aborting cancels the current
// download instead of leaving it running in the background.
export async function fetchSource(src: DownloadSource, onLog: (line: string) => void, signal?: AbortSignal): Promise<Outcome> {
  const log: string[] = []
  const sink = (m: string) => { log.push(m); onLog(m) }
  const fetcher = FETCHERS[src.model]
  if (!fetcher) { sink(`${src.name}: unknown model ${src.model}`); return 'failed' }
  fs.mkdirSync(src.directory, { recursive: true })
  try {
    const outcome = await fetcher(src, sink, signal)
    sink(`${src.name}: ${outcome}`)
    return outcome
  } catch (e) {
    sink(`${src.name}: unexpected error: ${e}`)
    return 'failed'
  }
}

export { sourceDirName, fetchFingerprint, fingerprintsEqual, latestCompleteStamp }
