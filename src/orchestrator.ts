import * as fs from 'fs'
import * as path from 'path'
import { Bbox, InternetState, SourceSetting, SourceStatus } from './types'
import { expectedRunStamp, fetchFingerprint, fingerprintsEqual, nextPublishAt, sourceDirName, MODEL_MAX_HOURS } from './scheduler'
import { fetchSource, toDownloadSource, type Outcome } from './downloader'

// Per-source download timeout — equivalent to the previous container job
// timeout (JOB_TIMEOUT_S = 3600). Firing it aborts the run's AbortController,
// which cancels the in-flight fetches, so a timed-out download actually
// stops instead of continuing against the same .part/output files.
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

  // Sources whose expected run is newer than the last completed one, or
  // whose on-disk data was fetched with different parameters.
  private isBehind(source: SourceSetting): boolean {
    const last = this.lastRunStamp(source)
    return last === null || last < expectedRunStamp(source.model) || !this.paramsOk(source, last)
  }

  staleSources(sources: SourceSetting[] = this.autoSources()): SourceSetting[] {
    return sources.filter(s => this.isBehind(s))
  }

  // When the auto scheduler should next look at `source`: after
  // `retryMs` if it is behind (late run, failed download, changed
  // parameters), otherwise just after the model's next run is expected to
  // publish (plus `slackMs`). The caller decides the timings; this only
  // knows the model's publication schedule and what is on disk.
  nextTickAt(source: SourceSetting, timing: { retryMs: number; slackMs: number }): Date {
    if (this.isBehind(source)) return new Date(Date.now() + timing.retryMs)
    return new Date(nextPublishAt(source.model).getTime() + timing.slackMs)
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

    // The timeout actively cancels the download via the abort signal (the
    // fetchers surface it as outcome 'failed'), and the timer is always
    // cleared once the run settles.
    const controller = new AbortController()
    const timer = setTimeout(
      () => controller.abort(new Error(`timeout after ${SOURCE_TIMEOUT_MS / 1000}s`)),
      SOURCE_TIMEOUT_MS
    )

    try {
      const outcome = await fetchSource(ds, onProgress, controller.signal)
      st.lastOutcome = outcome
      if (outcome === 'failed') {
        st.lastError = controller.signal.aborted
          ? `download aborted: ${controller.signal.reason ?? 'aborted'}`
          : `download failed: ${st.lastLog.slice(-3).join(' | ') || 'see log'}`
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
      clearTimeout(timer)
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

  // Automatic tick: download every auto-enabled source whose expected run
  // is missing. Suppressed entirely while offline or behind a captive
  // portal — neither reaches the open internet, so downloads would only
  // fetch the portal's login page. Normally the scheduler does not even
  // fire then, but the gate also covers catch-up triggers and state-change
  // races. 'metered' only stretches the schedule (caller-side): when a
  // stretched tick fires, downloads proceed. 'unknown' (no publisher of
  // network.internet.state) behaves as 'online'.
  async tick(internet: InternetState = 'online'): Promise<void> {
    if (internet === 'offline' || internet === 'captive') return
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
