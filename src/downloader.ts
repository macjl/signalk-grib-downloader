import * as fs from 'node:fs'
import * as path from 'node:path'
import { createRequire } from 'node:module'
import { Bbox, ModelId, SourceSetting } from './types'
import { downloaderSourceConfig, fetchFingerprint, runStamp, GFS_FETCH_VARS, GFS_FETCH_LEVELS, GFS_WAVE_VARS } from './scheduler'
import { GribStreamValidator } from './grib-stream'

export type Outcome = 'downloaded' | 'up-to-date' | 'unavailable' | 'failed'
export interface DownloadSource {
  name: string
  model: ModelId
  resolution?: string
  steps?: { from: number; to: number; by: number } | number[]
  bbox?: number[]
  variables?: string[]
  levels?: string[]
  packages?: string[]
  groups?: string[]
  base?: string // Upstream override for isolated tests/proxies; never contains the catalogue token.
  fingerprint: unknown
}
export interface RemoteAsset { id: string; urls: string[]; compression?: 'bzip2' }
export interface RunPlan { stamp: string; probe: string; assets: RemoteAsset[] }
const UA = 'signalk-grib-downloader/0.4 (direct forecastDatasets producer)'
const HTTP_TIMEOUT_MS = 60_000
const MAX_ICON_COMPRESSED_BYTES = 128 * 1024 ** 2
const MAX_ICON_DECOMPRESSED_BYTES = 512 * 1024 ** 2
const GFS_PRODUCTS: Record<string, string> = { '0p25': 'pgrb2.0p25', '0p50': 'pgrb2full.0p50', '1p00': 'pgrb2.1p00' }
const MF_BASE = 'https://meteofrance-pnt.s3.rbx.io.cloud.ovh.net/pnt'
const ICON_BASE = 'https://opendata.dwd.de/weather/nwp/icon-eu/grib'
const ICON_VARS = ['t_2m', 'u_10m', 'v_10m', 'vmax_10m', 'pmsl', 'relhum_2m', 'tot_prec', 'clct']

function requestSignal(signal?: AbortSignal): AbortSignal {
  const timeout = AbortSignal.timeout(HTTP_TIMEOUT_MS)
  return signal ? AbortSignal.any([signal, timeout]) : timeout
}
function stepsList(spec: DownloadSource['steps']): number[] {
  if (Array.isArray(spec)) return spec.map(Math.floor)
  if (spec) {
    if (!(spec.by > 0) || !Number.isFinite(spec.to) || !Number.isFinite(spec.from)) throw new Error('Invalid forecast steps')
    const result: number[] = []
    for (let n = spec.from; n <= spec.to; n += spec.by) result.push(n)
    return result
  }
  return []
}
export function toDownloadSource(setting: SourceSetting, bbox?: Bbox): DownloadSource {
  const cfg = downloaderSourceConfig(setting, bbox)
  return { name: cfg.name as string, model: setting.model, resolution: cfg.resolution as string | undefined,
    steps: cfg.steps as DownloadSource['steps'], bbox: cfg.bbox as number[] | undefined,
    variables: cfg.variables as string[] | undefined, levels: cfg.levels as string[] | undefined,
    packages: cfg.packages as string[] | undefined, groups: cfg.groups as string[] | undefined,
    fingerprint: fetchFingerprint(setting, bbox) }
}

export function planRun(src: DownloadSource, run: Date): RunPlan {
  const stamp = runStamp(run), hh = stamp.slice(9), ymdh = stamp.replace('T', '')
  const asset = (suffix: string, urls: string[], compression?: 'bzip2'): RemoteAsset => ({
    id: `${src.name}__${stamp}__${suffix}_grb2`, urls, ...(compression ? { compression } : {}),
  })
  if (src.model === 'gfs-wave') {
    if ((src.resolution ?? '0p25') !== '0p25') throw new Error('GFS-Wave supports only 0p25 resolution')
    const steps = stepsList(src.steps ?? { from: 0, to: 24, by: 3 })
    if (!steps.length) throw new Error('No forecast steps configured')
    const base = src.base ?? 'https://nomads.ncep.noaa.gov', date = stamp.slice(0, 8)
    const file = (step: number) => `gfswave.t${hh}z.global.0p25.f${String(step).padStart(3, '0')}.grib2`
    const params = (src.variables ?? GFS_WAVE_VARS).map(v => `var_${v}=on`).concat(['lev_surface=on'])
    if (src.bbox) {
      const [lat0, lon0, lat1, lon1] = src.bbox
      params.push('subregion=', `leftlon=${lon0}`, `rightlon=${lon1}`, `bottomlat=${lat0}`, `toplat=${lat1}`)
    }
    return { stamp,
      probe: `${base}/pub/data/nccf/com/gfs/prod/gfs.${date}/${hh}/wave/gridded/${file(steps[steps.length - 1])}.idx`,
      assets: steps.map(step => asset(`f${String(step).padStart(3, '0')}`, [
        `${base}/cgi-bin/filter_gfswave.pl?dir=%2Fgfs.${date}%2F${hh}%2Fwave%2Fgridded&file=${file(step)}&${params.join('&')}`,
      ])),
    }
  }
  if (src.model === 'gfs') {
    const resolution = src.resolution ?? '0p25', product = GFS_PRODUCTS[resolution]
    if (!product) throw new Error('Unsupported GFS resolution')
    const base = src.base ?? 'https://nomads.ncep.noaa.gov', date = stamp.slice(0, 8)
    const steps = stepsList(src.steps ?? { from: 0, to: 24, by: 3 })
    if (!steps.length) throw new Error('No forecast steps configured')
    const params = (src.variables ?? GFS_FETCH_VARS).map(v => `var_${v}=on`).concat((src.levels ?? GFS_FETCH_LEVELS).map(l => `lev_${l}=on`))
    if (src.bbox) {
      const [lat0, lon0, lat1, lon1] = src.bbox
      params.push('subregion=', `leftlon=${lon0}`, `rightlon=${lon1}`, `bottomlat=${lat0}`, `toplat=${lat1}`)
    }
    const file = (step: number) => `gfs.t${hh}z.${product}.f${String(step).padStart(3, '0')}`
    return { stamp,
      probe: `${base}/pub/data/nccf/com/gfs/prod/gfs.${date}/${hh}/atmos/${file(steps[steps.length - 1])}.idx`,
      assets: steps.map(step => asset(`f${String(step).padStart(3, '0')}`, [
        `${base}/cgi-bin/filter_gfs_${resolution}.pl?dir=%2Fgfs.${date}%2F${hh}%2Fatmos&file=${file(step)}&${params.join('&')}`,
      ])),
    }
  }
  if (src.model === 'arome' || src.model === 'arpege') {
    const resolution = src.resolution ?? (src.model === 'arome' ? '0025' : '025')
    const groups = src.groups ?? [], packages = src.packages ?? ['SP1']
    if (!groups.length || !packages.length) throw new Error('No Météo-France groups configured')
    const date = `${stamp.slice(0, 4)}-${stamp.slice(4, 6)}-${stamp.slice(6, 8)}T${hh}:00:00Z`
    const url = (p: string, g: string) => `${src.base ?? MF_BASE}/${date}/${src.model}/${resolution}/${p}/${src.model}__${resolution}__${p}__${g}__${date}.grib2`
    return { stamp, probe: url(packages[0], groups[groups.length - 1]),
      assets: packages.flatMap(p => groups.map(g => asset(`${p}_${g}`, [url(p, g)]))),
    }
  }
  if (src.model !== 'icon-eu') throw new Error('Unsupported forecast model')
  const variables = src.variables ?? ICON_VARS, steps = stepsList(src.steps ?? { from: 0, to: 48, by: 3 })
  if (!variables.length || !steps.length) throw new Error('No ICON variables or steps configured')
  const url = (step: number, variable: string) => `${src.base ?? ICON_BASE}/${hh}/${variable}/icon-eu_europe_regular-lat-lon_single-level_${ymdh}_${String(step).padStart(3, '0')}_${variable.toUpperCase()}.grib2.bz2`
  return { stamp, probe: url(steps[steps.length - 1], variables[0]),
    assets: steps.map(step => asset(`f${String(step).padStart(3, '0')}`, variables.map(v => url(step, v)), 'bzip2')),
  }
}

export async function runAvailable(plan: RunPlan, signal?: AbortSignal): Promise<boolean> {
  try {
    const response = await fetch(plan.probe, { method: 'HEAD', headers: { 'User-Agent': UA }, redirect: 'follow', signal: requestSignal(signal) })
    await response.body?.cancel()
    if (response.status === 200) return true
    if ([403, 404, 410, 425].includes(response.status)) return false
    throw new Error(`Upstream probe: HTTP ${response.status}`)
  } catch (error) {
    signal?.throwIfAborted()
    throw error
  }
}

async function* upstream(asset: RemoteAsset, signal: AbortSignal): AsyncGenerator<Buffer> {
  for (const url of asset.urls) {
    signal.throwIfAborted()
    const controller = new AbortController(), active = AbortSignal.any([signal, controller.signal])
    let timer: ReturnType<typeof setTimeout>
    const touch = () => {
      clearTimeout(timer)
      timer = setTimeout(() => controller.abort(new Error('Upstream transfer inactivity timeout')), 120_000)
    }
    touch()
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
    try {
      const response = await fetch(url, { headers: { 'User-Agent': UA, 'Accept-Encoding': 'identity' }, redirect: 'follow', signal: active })
      if (response.status !== 200 || !response.body) {
        await response.body?.cancel()
        throw new Error(`Upstream asset: HTTP ${response.status}`)
      }
      reader = response.body.getReader()
      if (asset.compression === 'bzip2') {
        // The WASM API decompresses one fragment at a time. Never collect all
        // variables/steps together, and never write an intermediate GRIB to disk.
        const chunks: Buffer[] = []
        let size = 0
        for (;;) {
          const { done, value } = await reader.read()
          if (done) break
          touch()
          size += value.length
          if (size > MAX_ICON_COMPRESSED_BYTES) throw new Error('ICON compressed fragment exceeds memory limit')
          chunks.push(Buffer.from(value))
        }
        yield await bz2Decompress(Buffer.concat(chunks))
      } else {
        for (;;) {
          const { done, value } = await reader.read()
          if (done) break
          touch()
          signal.throwIfAborted()
          yield Buffer.from(value)
        }
      }
    } finally {
      clearTimeout(timer!)
      controller.abort()
      if (reader) { await reader.cancel().catch(() => {}); reader.releaseLock() }
    }
  }
}
export async function* readAsset(asset: RemoteAsset, signal: AbortSignal): AsyncGenerator<Buffer> {
  const validator = new GribStreamValidator()
  for await (const chunk of upstream(asset, signal)) { validator.push(chunk); yield chunk }
  signal.throwIfAborted()
  validator.finish()
}

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
      if (outCap > MAX_ICON_DECOMPRESSED_BYTES) throw new Error('ICON fragment exceeds decompression limit')
    }
  }
}
