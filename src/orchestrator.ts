import { CataloguePublisher } from './catalogue'
import { Bbox, InternetState, SourceSetting, SourceStatus } from './types'
import { candidateRuns, expectedRunStamp, fetchFingerprint, fingerprintsEqual, nextPublishAt, probeRunStamp, remoteModelName, sourceDirName, MODEL_MAX_HOURS, MODEL_SCHEDULES } from './scheduler'
import { RemoteCatalogue, remoteZone } from './remote-catalogue'
import { planRun, runAvailable, toDownloadSource, type Outcome } from './downloader'
import { directCoverage, mapZone, overlap } from './coverage'

// Per-source download timeout — equivalent to the previous container job
// timeout (JOB_TIMEOUT_S = 3600). Firing it aborts the run's AbortController,
// which cancels the in-flight fetches, so a timed-out download actually
// stops instead of leaving an upstream or provider upload running.
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
    return !!this.catalogue.reference(source, stamp, this.catalogue.latest(source)?.params)
  }

  constructor(
    private sources: SourceSetting[],
    private catalogue: CataloguePublisher,
    private log: (msg: string) => void,
    private onChange: () => void = () => {}, // notify status updates
    private bbox?: Bbox,
    private remotes: ReadonlyMap<string, RemoteCatalogue> = new Map()
  ) {
    if (!catalogue) throw new Error('Forecast Dataset Provider publisher is required')
    this.updateSettings(sources, bbox)
  }

  updateSettings(sources: SourceSetting[], bbox?: Bbox): void {
    this.sources = sources
    this.bbox = bbox
    const names = new Set(sources.map(sourceDirName))
    for (const [name, state] of this.states) {
      if (!names.has(name) && !state.running) this.states.delete(name)
    }
    for (const name of names) {
      if (this.states.has(name)) continue
      this.states.set(name, {
        running: false, lastError: null, lastOutcome: null, lastFinishedAt: null, lastLog: [],
        lastProbeAt: null, lastProbeRun: null,
      })
    }
  }

  autoSources(): SourceSetting[] {
    return this.sources.filter(s => s.autoDownload !== false && (!s.remoteServer || this.remotes.has(s.remoteServer)))
  }

  // The provider owns GRIB bytes. Scheduling reads only durable publication receipts.
  private paramsOk(source: SourceSetting, stamp: string | null, bbox?: Bbox): boolean {
    if (!stamp) return true
    const latest = this.catalogue.latest(source)?.params
    if (source.remoteServer) {
      const prior = latest as { remoteEndpoint?: string; area?: string; requestedArea?: string; requestedHours?: number } | undefined
      return prior?.remoteEndpoint === this.remotes.get(source.remoteServer)?.endpoint &&
        prior?.requestedHours === source.remoteHours &&
        (prior?.area === undefined || (prior.requestedArea ?? prior.area) === JSON.stringify(remoteZone(bbox)))
    }
    return fingerprintsEqual(latest, fetchFingerprint(source, bbox))
  }

  lastRunStamp(source: SourceSetting): string | null {
    return this.catalogue.latest(source)?.stamp ?? null
  }

  status(): SourceStatus[] {
    return this.sources.map(s => {
      const name = sourceDirName(s)
      const st = this.states.get(name)!
      const lastRun = this.lastRunStamp(s)
      const expected = s.remoteServer ? st.lastProbeRun ?? lastRun ?? '' : expectedRunStamp(s.model)
      const target = s.remoteServer ? null : probeRunStamp(s.model)
      const paramsOk = this.paramsOk(s, lastRun, this.bbox)
      return {
        name,
        model: s.remoteServer ? remoteModelName(s) : s.model,
        ...(s.remoteServer ? { remoteServer: s.remoteServer } : {}),
        autoDownload: s.autoDownload !== false,
        lastRun,
        expectedRun: expected,
        pendingRun: target && !(lastRun !== null && lastRun >= target) ? target : null,
        publicationLate: !s.remoteServer && (lastRun === null || lastRun < expected),
        upToDate: (!s.remoteServer || st.lastOutcome !== 'unavailable') &&
          lastRun !== null && lastRun >= expected && paramsOk && this.published(s, lastRun),
        catalogue: this.catalogue.reference(s, lastRun, this.catalogue.latest(s)?.params),
        publicationPending: !s.remoteServer && this.catalogue.pendingStamp(s, fetchFingerprint(s, this.bbox)) !== null,
        configStale: lastRun !== null && !paramsOk,
        running: st.running,
        lastError: st.lastError,
        lastOutcome: st.lastOutcome,
        lastFinishedAt: st.lastFinishedAt,
        lastLog: st.lastLog,
        legacyExport: this.catalogue.legacyExportStatus?.(s),
      }
    })
  }

  // Sources whose expected run is newer than the last completed one, or
  // whose last publication used different parameters.
  private isBehind(source: SourceSetting): boolean {
    const last = this.lastRunStamp(source)
    if (source.remoteServer) return last === null || !this.paramsOk(source, last, this.bbox) || !this.published(source, last)
    return last === null || last < expectedRunStamp(source.model) || !this.paramsOk(source, last, this.bbox) || !this.published(source, last)
  }

  staleSources(sources: SourceSetting[] = this.autoSources()): SourceSetting[] {
    return sources.filter(s => this.isBehind(s))
  }

  private needsProbe(source: SourceSetting): boolean {
    const last = this.lastRunStamp(source)
    if (source.remoteServer) return this.remotes.has(source.remoteServer)
    return last === null || last < probeRunStamp(source.model) || !this.paramsOk(source, last, this.bbox) || !this.published(source, last) || !!this.catalogue.exportPending?.(source)
  }

  // Per-source probe cadence is anchored to the last attempt, so another
  // model's timer cannot cause extra probes or postpone this source's turn.
  // Opening a newer cycle's window takes precedence over an older retry.
  nextTickAt(source: SourceSetting, timing: ProbeTiming = {}): Date {
    const now = new Date(Date.now())
    const st = this.states.get(sourceDirName(source))!
    const interval = (timing.intervalMinutes ?? (source.remoteServer ? 5 : MODEL_SCHEDULES[source.model].probeEveryMinutes))
      * 60_000 * (timing.scale ?? 1)
    if (st.running) return new Date(now.getTime() + interval)
    if (source.remoteServer) return new Date(Math.max(now.getTime(), (st.lastProbeAt ?? 0) + interval))
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
  async downloadSource(source: SourceSetting, expectedSubset?: 'area' | 'time' | 'both'): Promise<boolean> {
    const name = sourceDirName(source)
    const st = this.states.get(name)
    if (!st || st.running || this.stopped || !this.findSource(name)) return false
    const bbox = this.bbox

    st.running = true
    st.lastError = null
    st.lastLog = []
    st.lastProbeAt = Date.now()
    if (!source.remoteServer) st.lastProbeRun = probeRunStamp(source.model)
    this.onChange()
    this.log(`${name}: starting download`)

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
      const domain = !source.remoteServer && directCoverage(source)
      if (domain && bbox && !overlap(mapZone(bbox), domain).length) {
        st.lastOutcome = 'unavailable'
        onProgress('The model does not cover the requested map area')
        return true
      }
      // Check the provider and existing receipt before opening an upstream body.
      // A generic 404 must never erase a committed acquisition's identity.
      await this.catalogue.check(controller.signal)
      const previous = this.catalogue.latest(source)
      if (previous) await this.catalogue.verify(source, previous.stamp, previous.params, controller.signal)
      await this.catalogue.exportLatest?.(source, onProgress, controller.signal)
      if (source.remoteServer) {
        const remote = this.remotes.get(source.remoteServer)
        if (!remote) throw new Error('This remote Signal K server is no longer configured')
        const signal = controller.signal
        const candidate = await remote.find(source, bbox, signal)
        st.lastProbeRun = candidate?.stamp ?? null
        if (!candidate) {
          st.lastOutcome = 'unavailable'
          onProgress('No complete matching GRIB dataset covering the requested map area on the remote server')
          return true
        }
        if (((expectedSubset === 'area' || expectedSubset === 'both') && !candidate.selection?.bbox) ||
            ((expectedSubset === 'time' || expectedSubset === 'both') && !candidate.selection?.from))
          throw new Error('Remote subsetting availability changed. Refresh the source before downloading.')
        const latest = this.catalogue.latest(source)
        const params = remote.fingerprint(candidate)
        if ((latest?.stamp === candidate.stamp && fingerprintsEqual(latest.params, params)) ||
            (latest && latest.stamp > candidate.stamp && this.paramsOk(source, latest.stamp, bbox))) {
          st.lastOutcome = 'up-to-date'
          return true
        }
        const ready = await remote.prepare(source, candidate, onProgress, signal)
        if (latest?.stamp === candidate.stamp && fingerprintsEqual(latest.params, ready.params)) {
          st.lastOutcome = 'up-to-date'
          return true
        }
        if (!candidate.selection) onProgress('Importing complete remote GRIB dataset')
        await this.catalogue.publish(source, ready.plan, ready.params, onProgress, signal)
        await this.catalogue.exportLatest?.(source, onProgress, signal)
        st.lastOutcome = 'downloaded'
        return true
      }

      const ds = toDownloadSource(source, bbox)
      let outcome: Outcome = 'unavailable'
      const pending = this.catalogue.pendingStamp(source, ds.fingerprint)
      if (pending) {
        const run = new Date(`${pending.slice(0, 4)}-${pending.slice(4, 6)}-${pending.slice(6, 8)}T${pending.slice(9)}:00:00Z`)
        await this.catalogue.publish(source, planRun(ds, run), ds.fingerprint, onProgress, controller.signal)
        outcome = 'downloaded'
      }
      for (const run of candidateRuns(source.model)) {
        controller.signal.throwIfAborted()
        const plan = planRun(ds, run), latest = this.catalogue.latest(source)
        if (latest && latest.stamp >= plan.stamp) {
          if (fingerprintsEqual(latest.params, ds.fingerprint)) { if (outcome !== 'downloaded') outcome = 'up-to-date'; break }
          if (latest.stamp > plan.stamp) continue
        }
        if (!(await runAvailable(plan, controller.signal))) { onProgress(`run ${plan.stamp} not published yet`); continue }
        await this.catalogue.publish(source, plan, ds.fingerprint, onProgress, controller.signal)
        outcome = 'downloaded'
        break
      }
      if (outcome === 'downloaded') await this.catalogue.exportLatest?.(source, onProgress, controller.signal)
      st.lastOutcome = outcome
      this.log(`${name}: download finished (${outcome})`)
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
      if (!this.findSource(name)) this.states.delete(name)
      this.onChange()
    }
  }

  // Download the given sources sequentially (bandwidth-friendly).
  async downloadAll(sources?: SourceSetting[]): Promise<void> {
    for (const queued of sources ?? this.sources) {
      if (this.stopped) break
      const source = this.findSource(sourceDirName(queued))
      if (source) await this.downloadSource(source)
    }
  }

  // A newly approved credential permits an immediate retry of failed work.
  retryAfterAccessApproval(remoteServer: string): void {
    for (const source of this.sources) {
      const state = this.states.get(sourceDirName(source))!
      if (source.remoteServer === remoteServer)
        state.lastProbeAt = null
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
      for (const queued of due) {
        const source = this.findSource(sourceDirName(queued))
        if (source && source.autoDownload !== false &&
            this.nextTickAt(source, timing).getTime() <= Date.now()) await this.downloadSource(source)
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
