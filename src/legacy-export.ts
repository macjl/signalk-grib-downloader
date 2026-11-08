// Optional filesystem bridge for consumers of the historical downloader layout.
import * as fs from 'node:fs'
import * as path from 'node:path'
import { createHash } from 'node:crypto'
import { Readable, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'

export interface LegacyAsset { id: string; size: number; sha256: string }
export interface LegacyRun {
  source: string; stamp: string; datasetId: string; version: string
  params: unknown; assets: LegacyAsset[]; archiveRuns: number
}
export interface LegacyExportStatus {
  directory: string; state: 'pending' | 'exporting' | 'complete' | 'error'
  lastRun: string | null; version: string | null; lastError: string | null
}
const STAMP = /^\d{8}T\d{2}$/
const HASH = /^[a-f0-9]{64}$/
function sourceName(name: string) {
  if (!/^[A-Za-z0-9_-]+$/.test(name)) throw new Error('Invalid legacy export source')
  return name
}
export function legacyFilename(source: string, stamp: string, id: string): string {
  sourceName(source)
  if (!STAMP.test(stamp) || !/^[A-Za-z0-9_-]{1,128}$/.test(id) ||
      !id.startsWith(`${source}__${stamp}__`) || !id.endsWith('_grb2')) throw new Error('Invalid legacy export asset name')
  return id.slice(0, -5) + '.grb2'
}
function regular(file: string): fs.Stats | undefined {
  try {
    const stat = fs.lstatSync(file)
    if (!stat.isFile()) throw new Error(`Legacy export requires a regular file: ${file}`)
    return stat
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined
    throw error
  }
}
function directory(file: string) {
  fs.mkdirSync(file, { recursive: true })
  const stat = fs.lstatSync(file)
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error(`Legacy export requires a directory: ${file}`)
}
function syncFile(file: string) {
  const fd = fs.openSync(file, 'r+')
  try { fs.fsyncSync(fd) } finally { fs.closeSync(fd) }
}
function syncDirectory(file: string) {
  if (process.platform === 'win32') return
  const fd = fs.openSync(file, 'r')
  try { fs.fsyncSync(fd) } finally { fs.closeSync(fd) }
}

export class LegacyGribExporter {
  private states = new Map<string, LegacyExportStatus>()
  private archiveCounts = new Map<string, number>()
  constructor(readonly root: string) {
    if (!path.isAbsolute(root)) throw new Error('GRIB export root must be absolute')
  }
  status(source: string): LegacyExportStatus {
    return this.states.get(source) ?? { directory: path.join(this.root, sourceName(source)), state: 'pending', lastRun: null, version: null, lastError: null }
  }
  matches(source: string, stamp: string, version: string, archiveRuns?: number): boolean {
    const state = this.status(source)
    return state.state === 'complete' && state.lastRun === stamp && state.version === version &&
      (archiveRuns === undefined || this.archiveCounts.get(source) === archiveRuns)
  }
  failed(source: string, error: unknown) {
    this.states.set(source, { ...this.status(source), state: 'error', lastError: String(error) })
  }
  private async valid(file: string, asset: LegacyAsset, signal: AbortSignal): Promise<boolean> {
    const stat = regular(file)
    if (!stat || stat.size !== asset.size) return false
    const hash = createHash('sha256')
    let size = 0
    for await (const chunk of fs.createReadStream(file, { signal })) {
      size += chunk.length; hash.update(chunk)
    }
    return size === asset.size && hash.digest('hex') === asset.sha256
  }
  async export(run: LegacyRun, read: (asset: LegacyAsset) => Promise<Readable>, signal: AbortSignal): Promise<void> {
    const source = sourceName(run.source), folder = path.join(this.root, source)
    let scratch: string | undefined
    let preserveScratch = false
    this.states.set(source, { ...this.status(source), state: 'exporting', lastError: null })
    try {
      signal.throwIfAborted()
      if (!STAMP.test(run.stamp) || !HASH.test(run.version) || !run.assets.length ||
          !Number.isSafeInteger(run.archiveRuns) || run.archiveRuns < 0) throw new Error('Invalid legacy export acquisition')
      const files = run.assets.map(asset => {
        if (!Number.isSafeInteger(asset.size) || asset.size < 1 || !HASH.test(asset.sha256)) throw new Error('Invalid legacy export asset receipt')
        return { asset, name: legacyFilename(source, run.stamp, asset.id) }
      })
      if (new Set(files.map(f => f.name)).size !== files.length) throw new Error('Duplicate legacy export file')
      // Only the explicitly configured root may be a symlink; managed children
      // and files may not redirect writes or retention into another location.
      fs.mkdirSync(this.root, { recursive: true })
      directory(folder)
      const marker = `.run-${run.stamp}.complete`
      regular(path.join(folder, marker))
      let allValid = true
      for (const file of files) if (!(await this.valid(path.join(folder, file.name), file.asset, signal))) allValid = false
      const markerValue = JSON.stringify({ params: run.params })
      scratch = fs.mkdtempSync(path.join(folder, '.legacy-export-'))
      fs.writeFileSync(path.join(scratch, marker), markerValue, { flag: 'wx' })
      syncFile(path.join(scratch, marker))
      if (!allValid) {
        for (const file of files) {
          signal.throwIfAborted()
          const dest = path.join(scratch, file.name + '.part')
          const local = path.join(folder, file.name)
          const input = await this.valid(local, file.asset, signal) ? fs.createReadStream(local) : await read(file.asset)
          let size = 0
          const hash = createHash('sha256')
          const meter = new Transform({ transform(chunk: Buffer, _encoding, callback) {
            size += chunk.length
            if (size > file.asset.size) return callback(new Error('Legacy export asset exceeds its catalogue size'))
            hash.update(chunk); callback(null, chunk)
          } })
          await pipeline(input, meter, fs.createWriteStream(dest, { flags: 'wx' }), { signal })
          if (size !== file.asset.size || hash.digest('hex') !== file.asset.sha256) throw new Error('Legacy export asset size or SHA-256 mismatch')
          syncFile(dest)
        }
      }
      signal.throwIfAborted()
      // No awaits during promotion: withdraw a same-run marker before changing
      // its files, then publish the new marker last. Roll back ordinary rename
      // failures so the previously completed run stays usable.
      const backup = path.join(scratch, 'previous')
      directory(backup)
      const moved: string[] = [], installed: string[] = []
      const obsolete = fs.readdirSync(folder).filter(name =>
        name.startsWith(`${source}__${run.stamp}__`) && /^[A-Za-z0-9_.-]+\.grb2$/.test(name) &&
        !files.some(file => file.name === name) && regular(path.join(folder, name)))
      try {
        if (regular(path.join(folder, marker))) {
          fs.renameSync(path.join(folder, marker), path.join(backup, marker)); moved.push(marker)
        }
        for (const name of obsolete) {
          fs.renameSync(path.join(folder, name), path.join(backup, name)); moved.push(name)
        }
        if (!allValid) for (const file of files) {
          if (regular(path.join(folder, file.name))) {
            fs.renameSync(path.join(folder, file.name), path.join(backup, file.name)); moved.push(file.name)
          }
          fs.renameSync(path.join(scratch, file.name + '.part'), path.join(folder, file.name)); installed.push(file.name)
        }
        fs.renameSync(path.join(scratch, marker), path.join(folder, marker)); installed.push(marker)
        syncDirectory(folder)
      } catch (error) {
        try {
          for (const name of installed.reverse()) fs.rmSync(path.join(folder, name), { force: true })
          for (const name of moved.reverse()) fs.renameSync(path.join(backup, name), path.join(folder, name))
        } catch (rollback) {
          preserveScratch = true
          throw new Error(`Legacy export rollback failed; recovery files kept in ${scratch}: ${rollback}`)
        }
        throw error
      }
      this.retain(folder, source, run.stamp, run.archiveRuns)
      this.archiveCounts.set(source, run.archiveRuns)
      this.states.set(source, { directory: folder, state: 'complete', lastRun: run.stamp, version: run.version, lastError: null })
    } catch (error) {
      this.failed(source, error)
      throw error
    } finally {
      if (scratch && !preserveScratch) fs.rmSync(scratch, { recursive: true, force: true })
    }
  }
  private retain(folder: string, source: string, current: string, count: number) {
    const archive = path.join(folder, 'archive')
    const runFiles = (dir: string, stamp: string) => fs.readdirSync(dir).filter(name => {
      const owned = name === `.run-${stamp}.complete` ||
        (name.startsWith(`${source}__${stamp}__`) && /^[A-Za-z0-9_.-]+\.grb2$/.test(name))
      return owned && regular(path.join(dir, name))
    }).sort((a, b) => Number(b.startsWith('.run-')) - Number(a.startsWith('.run-')))
    const stamps = (dir: string) => fs.readdirSync(dir).flatMap(name => {
      const match = /^\.run-(\d{8}T\d{2})\.complete$/.exec(name)
      return match && regular(path.join(dir, name)) ? [match[1]] : []
    }).sort().reverse()
    for (const stamp of stamps(folder).filter(stamp => stamp < current)) {
      const files = runFiles(folder, stamp)
      // The historical marker is the compatibility contract. Move/delete it
      // first so retention never leaves a complete marker pointing at no files.
      const marker = `.run-${stamp}.complete`
      if (count > 0) {
        directory(archive)
        const staging = fs.mkdtempSync(path.join(archive, '.legacy-archive-'))
        const previous = path.join(staging, 'previous'); directory(previous)
        const withdrawn: string[] = [], replaced: string[] = [], installed: string[] = []
        let preserve = false
        try {
          // Withdraw the source marker first. Publish the archive marker last.
          for (const name of files) {
            fs.renameSync(path.join(folder, name), path.join(staging, name)); withdrawn.push(name)
          }
          if (regular(path.join(archive, marker))) {
            fs.renameSync(path.join(archive, marker), path.join(previous, marker)); replaced.push(marker)
          }
          for (const name of [...files.filter(name => name !== marker), marker]) {
            if (regular(path.join(archive, name))) {
              fs.renameSync(path.join(archive, name), path.join(previous, name)); replaced.push(name)
            }
            fs.renameSync(path.join(staging, name), path.join(archive, name)); installed.push(name)
          }
          syncDirectory(archive)
        } catch (error) {
          try {
            for (const name of installed.reverse()) fs.renameSync(path.join(archive, name), path.join(staging, name))
            for (const name of replaced.reverse()) fs.renameSync(path.join(previous, name), path.join(archive, name))
            for (const name of withdrawn.reverse()) fs.renameSync(path.join(staging, name), path.join(folder, name))
          } catch (rollback) {
            preserve = true
            throw new Error(`Legacy archive rollback failed; recovery files kept in ${staging}: ${rollback}`)
          }
          throw error
        } finally {
          if (!preserve) fs.rmSync(staging, { recursive: true, force: true })
        }
      } else for (const name of files) fs.unlinkSync(path.join(folder, name))
    }
    if (fs.existsSync(archive)) {
      directory(archive)
      for (const stamp of stamps(archive).slice(count)) {
        for (const name of runFiles(archive, stamp)) fs.unlinkSync(path.join(archive, name))
      }
    }
  }
}
