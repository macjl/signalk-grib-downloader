import * as fs from 'fs'
import * as path from 'path'
import { Bbox, SourceSetting, SourceStatus } from './types'
import { expectedRunStamp, fetchFingerprint, fingerprintsEqual, sourceDirName, MODEL_MAX_HOURS } from './scheduler'
import { fetchSource, toDownloadSource, type Outcome } from './downloader'

// Per-source download timeout — equivalent to the previous container job
// timeout (JOB_TIMEOUT_S = 3600). Aborts a runaway download so the
// scheduler and manual triggers don't hang indefinitely.
const SOURCE_TIMEOUT_MS = 3600_000
const LOG_TAIL = 30

interface SourceState {
  running: boolean
  lastError: string | null
  lastOutcome: Outcome | null
  lastFinishedAt: string | null
  lastLog: string[]
}

export class Orchestrator {
  private states = new Map<string, SourceState>()

  constructor(
    private sources: SourceSetting[],
    private gribsRoot: string,    // local path where source subdirs live
    private log: (msg: string) => void,
    private onChange: () => void, // notify status updates
    private bbox?: Bbox
  ) {
    for (const s of sources) {
      this.states.set(sourceDirName(s), {
        running: false, lastError: null, lastOutcome: null, lastFinishedAt: null, lastLog: [],
      })
    }
  }

  autoSources(): SourceSetting[] {
    return this.sources.filter(s => s.autoDownload !== false)
  }

  // Fetch-parameter fingerprint recorded in a run marker, or null
  // (legacy/empty markers — treated as a parameter change).
  private markerParams(source: SourceSetting, stamp: string): unknown {
    try {
      const p = path.join(this.gribsRoot, sourceDirName(source), `.run-${stamp}.complete`)
      const content = fs.readFileSync(p, 'utf-8').trim()
      if (!content) return null
      return JSON.parse(content).params ?? null
    } catch {
      return null
    }
  }

  // A run only matches the settings if it was downloaded with the same
  // fetch parameters (area, duration, groups, variables, …).
  private paramsOk(source: SourceSetting, stamp: string | null): boolean {
    if (stamp === null) return true
    const stored = this.markerParams(source, stamp)
    if (stored === null) return false
    return fingerprintsEqual(stored, fetchFingerprint(source, this.bbox))
  }

  // Latest completed run stamp, read from the downloader's marker files.
  lastRunStamp(source: SourceSetting): string | null {
    const dir = path.join(this.gribsRoot, sourceDirName(source))
    try {
      const stamps = fs.readdirSync(dir)
        .map(f => /^\.run-(\d{8}T\d{2})\.complete$/.exec(f)?.[1])
        .filter((s): s is string => !!s)
      return stamps.length > 0 ? stamps.sort().reverse()[0] : null
    } catch {
      return null
    }
  }

  status(): SourceStatus[] {
    return this.sources.map(s => {
      const name = sourceDirName(s)
      const st = this.states.get(name)!
      const lastRun = this.lastRunStamp(s)
      const expected = expectedRunStamp(s.model)
      const paramsOk = this.paramsOk(s, lastRun)
      return {
        name,
        model: s.model,
        autoDownload: s.autoDownload !== false,
        lastRun,
        expectedRun: expected,
        upToDate: lastRun !== null && lastRun >= expected && paramsOk,
        configStale: lastRun !== null && !paramsOk,
        running: st.running,
        lastError: st.lastError,
        lastOutcome: st.lastOutcome,
        lastFinishedAt: st.lastFinishedAt,
        lastLog: st.lastLog,
      }
    })
  }

  // Sources whose expected run is newer than the last completed one.
  staleSources(sources: SourceSetting[] = this.autoSources()): SourceSetting[] {
    return sources.filter(s => {
      const last = this.lastRunStamp(s)
      return last === null || last < expectedRunStamp(s.model) || !this.paramsOk(s, last)
    })
  }

  // Download one source (no-op if it is already running).
  // Returns true if a fetch was executed.
  async downloadSource(source: SourceSetting): Promise<boolean> {
    const name = sourceDirName(source)
    const st = this.states.get(name)!
    if (st.running) return false

    fs.mkdirSync(path.join(this.gribsRoot, name), { recursive: true })

    st.running = true
    st.lastError = null
    st.lastLog = []
    this.onChange()
    this.log(`${name}: starting download`)

    const ds = toDownloadSource(source, this.gribsRoot, this.bbox)
    const onProgress = (line: string) => {
      st.lastLog.push(line)
      if (st.lastLog.length > LOG_TAIL) st.lastLog.shift()
      this.log(`  ${name}: ${line}`)
    }

    try {
      const outcome = await Promise.race([
        fetchSource(ds, onProgress),
        new Promise<Outcome>((_, reject) =>
          setTimeout(() => reject(new Error(`timeout after ${SOURCE_TIMEOUT_MS / 1000}s`)), SOURCE_TIMEOUT_MS)),
      ])
      st.lastOutcome = outcome
      if (outcome === 'failed') {
        st.lastError = `download failed: ${st.lastLog.slice(-3).join(' | ') || 'see log'}`
        this.log(`${name}: ${st.lastError}`)
      } else {
        this.log(`${name}: download finished (${outcome})`)
      }
      return true
    } catch (err) {
      st.lastOutcome = 'failed'
      st.lastError = String(err)
      this.log(`${name}: download error: ${err}`)
      return false
    } finally {
      st.running = false
      st.lastFinishedAt = new Date().toISOString()
      this.onChange()
    }
  }

  // Download the given sources sequentially (bandwidth-friendly).
  async downloadAll(sources?: SourceSetting[]): Promise<void> {
    for (const s of sources ?? this.sources) {
      await this.downloadSource(s)
    }
  }

  // Automatic tick: download every auto-enabled source whose expected run is missing.
  async tick(): Promise<void> {
    const stale = this.staleSources().filter(s => !this.states.get(sourceDirName(s))!.running)
    if (stale.length > 0) {
      this.log(`auto: ${stale.length} source(s) behind: ${stale.map(s => sourceDirName(s)).join(', ')}`)
      await this.downloadAll(stale)
    }
  }

  findSource(name: string): SourceSetting | undefined {
    return this.sources.find(s => sourceDirName(s) === name)
  }

  maxHours(model: SourceSetting['model']): number {
    return MODEL_MAX_HOURS[model]
  }
}
