import * as fs from 'fs'
import * as path from 'path'
import { CataloguePublisher } from './catalogue'
import { Bbox, InternetState, SourceSetting, SourceStatus } from './types'
import { expectedRunStamp, fetchFingerprint, fingerprintsEqual, nextPublishAt, probeRunStamp, sourceDirName, MODEL_MAX_HOURS, MODEL_SCHEDULES } from './scheduler'
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
  lastProbeAt: number | null
  lastProbeRun: string | null
}

export interface ProbeTiming {
  intervalMinutes?: number
  scale?: number
}

export class Orchestrator {
  private states = new Map<string, SourceState>()
  private controllers = new Set<AbortController>()
  private stopped = false

  isRunning(): boolean { return [...this.states.values()].some(s => s.running) }
  async stop(): Promise<void> {
    this.stopped = true
    for (const controller of this.controllers) controller.abort(new Error('plugin stopped'))
    while (this.controllers.size) await new Promise(resolve => setTimeout(resolve, 10))
  }

  private published(source: SourceSetting, stamp: string | null): boolean {
    return !!this.catalogue.reference(source, stamp, stamp ? this.markerParams(source, stamp) : null)
  }

  constructor(
    private sources: SourceSetting[],
    private gribsRoot: string,    // private acquisition workspace
    private catalogue: CataloguePublisher,
    private log: (msg: string) => void,
    private onChange: () => void = () => {}, // notify status updates
    private bbox?: Bbox
  ) {
    if (!catalogue) throw new Error('Forecast Dataset Provider publisher is required')
    for (const s of sources) {
      this.states.set(sourceDirName(s), {
        running: false, lastError: null, lastOutcome: null, lastFinishedAt: null, lastLog: [],
        lastProbeAt: null, lastProbeRun: null,
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
      const target = probeRunStamp(s.model)
      const paramsOk = this.paramsOk(s, lastRun)
      return {
        name,
        model: s.model,
        autoDownload: s.autoDownload !== false,
        lastRun,
        expectedRun: expected,
        pendingRun: lastRun !== null && lastRun >= target ? null : target,
        publicationLate: lastRun === null || lastRun < expected,
        upToDate: lastRun !== null && lastRun >= expected && paramsOk && this.published(s, lastRun),
        catalogue: this.catalogue.reference(s, lastRun, lastRun ? this.markerParams(s, lastRun) : null),
        publicationPending: lastRun !== null && !this.published(s, lastRun),
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
    return last === null || last < expectedRunStamp(source.model) || !this.paramsOk(source, last) || !this.published(source, last)
  }

  staleSources(sources: SourceSetting[] = this.autoSources()): SourceSetting[] {
    return sources.filter(s => this.isBehind(s))
  }

  private needsProbe(source: SourceSetting): boolean {
    const last = this.lastRunStamp(source)
    return last === null || last < probeRunStamp(source.model) || !this.paramsOk(source, last) || !this.published(source, last)
  }

  // Per-source probe cadence is anchored to the last attempt, so another
  // model's timer cannot cause extra probes or postpone this source's turn.
  // Opening a newer cycle's window takes precedence over an older retry.
  nextTickAt(source: SourceSetting, timing: ProbeTiming = {}): Date {
    const now = new Date(Date.now())
    const st = this.states.get(sourceDirName(source))!
    const interval = (timing.intervalMinutes ?? MODEL_SCHEDULES[source.model].probeEveryMinutes)
      * 60_000 * (timing.scale ?? 1)
    if (st.running) return new Date(now.getTime() + interval)
    const nextWindow = nextPublishAt(source.model, now, this.lastRunStamp(source))
    if (!this.needsProbe(source)) return nextWindow
    const target = probeRunStamp(source.model, now)
    const due = st.lastProbeAt === null || st.lastProbeRun !== target
      ? now.getTime()
      : Math.max(now.getTime(), st.lastProbeAt + interval)
    return new Date(Math.min(due, nextWindow.getTime()))
  }

  // Download one source (no-op if it is already running).
  // Returns true if a fetch was executed.
  async downloadSource(source: SourceSetting): Promise<boolean> {
    const name = sourceDirName(source)
    const st = this.states.get(name)!
    if (st.running || this.stopped) return false

    fs.mkdirSync(path.join(this.gribsRoot, name), { recursive: true })

    st.running = true
    st.lastError = null
    st.lastLog = []
    st.lastProbeAt = Date.now()
    st.lastProbeRun = probeRunStamp(source.model)
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
    this.controllers.add(controller)
    const timer = setTimeout(
      () => controller.abort(new Error(`timeout after ${SOURCE_TIMEOUT_MS / 1000}s`)),
      SOURCE_TIMEOUT_MS
    )

    try {
      // Publish a completed acquisition before attempting another download.
      // On catalogue failure keep these files and retry without fetching upstream.
      const previous = this.lastRunStamp(source)
      if (previous) await this.catalogue.publish(source, ds.directory, previous, onProgress, controller.signal)
      const outcome = await fetchSource(ds, onProgress, controller.signal)
      const latest = this.lastRunStamp(source)
      if (latest && latest !== previous) await this.catalogue.publish(source, ds.directory, latest, onProgress, controller.signal)
      // A parameter change can replace the files for the same model cycle.
      else if (latest && outcome === 'downloaded') await this.catalogue.publish(source, ds.directory, latest, onProgress, controller.signal)
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
      this.controllers.delete(controller)
      clearTimeout(timer)
      st.running = false
      st.lastFinishedAt = new Date(Date.now()).toISOString()
      this.onChange()
    }
  }

  // Download the given sources sequentially (bandwidth-friendly).
  async downloadAll(sources?: SourceSetting[]): Promise<void> {
    for (const s of sources ?? this.sources) {
      if (this.stopped) break
      await this.downloadSource(s)
    }
  }

  // Automatic tick: probe eligible sources even before their upper bound.
  // Suppressed entirely while offline or behind a captive
  // portal — neither reaches the open internet, so downloads would only
  // fetch the portal's login page. Normally the scheduler does not even
  // fire then, but the gate also covers catch-up triggers and state-change
  // races. 'metered' only stretches the schedule (caller-side): when a
  // stretched tick fires, downloads proceed. 'unknown' (no publisher of
  // network.internet.state) behaves as 'online'.
  async tick(internet: InternetState = 'online', timing: ProbeTiming = {}): Promise<void> {
    if (this.stopped) return
    if (internet === 'offline' || internet === 'captive') return
    const due = this.autoSources().filter(s => this.needsProbe(s)
      && !this.states.get(sourceDirName(s))!.running
      && this.nextTickAt(s, timing).getTime() <= Date.now())
    if (due.length > 0) {
      this.log(`auto: ${due.length} source(s) to check: ${due.map(s => sourceDirName(s)).join(', ')}`)
      // Re-evaluate after each download: sources share a sequential queue.
      for (const source of due) {
        if (this.nextTickAt(source, timing).getTime() <= Date.now()) await this.downloadSource(source)
      }
    }
  }

  findSource(name: string): SourceSetting | undefined {
    return this.sources.find(s => sourceDirName(s) === name)
  }

  maxHours(model: SourceSetting['model']): number {
    return MODEL_MAX_HOURS[model]
  }
}
