import * as fs from 'fs'
import * as path from 'path'
import { Bbox, SourceSetting, SourceStatus } from './types'
import { downloaderSourceConfig, expectedRunStamp, fetchFingerprint, fingerprintsEqual, sourceDirName, MODEL_MAX_HOURS } from './scheduler'

const PLUGIN_ID = 'signalk-grib-downloader'
const JOB_TIMEOUT_S = 3600
const LOG_TAIL = 30

type Outcome = 'downloaded' | 'up-to-date' | 'unavailable' | 'failed'

interface SourceState {
  running: boolean
  lastError: string | null
  lastOutcome: Outcome | null
  lastFinishedAt: string | null
  lastLog: string[]
}

export class Orchestrator {
  private states = new Map<string, SourceState>()
  private configPath: string | null = null

  constructor(
    private sources: SourceSetting[],
    private gribsRoot: string,
    private image: string,
    private dataDir: string,        // plugin data dir (SignalK-visible)
    private log: (msg: string) => void,
    private onChange: () => void,   // notify status updates
    private bbox?: Bbox
  ) {
    for (const s of sources) {
      this.states.set(sourceDirName(s), {
        running: false, lastError: null, lastOutcome: null, lastFinishedAt: null, lastLog: [],
      })
    }
  }

  private containers(): any {
    return (globalThis as any).__signalk_containerManager ?? null
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

  // Write the downloader config (JSON is valid YAML) into the plugin data
  // dir so the job container can mount and read it.
  private writeConfig(dataRootInJob: string, sources: SourceSetting[] = this.sources): string {
    const config = {
      sources: sources.map(s =>
        downloaderSourceConfig(s, dataRootInJob, this.bbox)
      ),
    }
    const p = path.join(this.dataDir, 'downloader-config.json')
    fs.mkdirSync(this.dataDir, { recursive: true })
    fs.writeFileSync(p, JSON.stringify(config, null, 2))
    return p
  }

  // Download one source (no-op if its job is already running).
  // Returns true if a job was executed.
  async downloadSource(source: SourceSetting): Promise<boolean> {
    const name = sourceDirName(source)
    const st = this.states.get(name)!
    if (st.running) return false

    const containers = this.containers()
    if (!containers?.getRuntime?.()) {
      st.lastError = 'signalk-container not available'
      this.onChange()
      return false
    }

    const rData = await containers.resolveHostPath(this.gribsRoot)
    const rCfg = await containers.resolveHostPath(this.dataDir)
    if (!rData || !rCfg) {
      const bad = !rData ? this.gribsRoot : this.dataDir
      st.lastError =
        `${bad} is not reachable from the container runtime. When SignalK ` +
        `runs in a container, the GRIB root must live inside a mounted ` +
        `volume — e.g. inside the SignalK data directory (the default).`
      this.onChange()
      return false
    }
    const dataRootInJob = rData.subPath ? `/data/${rData.subPath}` : '/data'
    const cfgInJob = (rCfg.subPath ? `/cfg/${rCfg.subPath}` : '/cfg') + '/downloader-config.json'

    fs.mkdirSync(path.join(this.gribsRoot, name), { recursive: true })
    this.writeConfig(dataRootInJob)

    st.running = true
    st.lastError = null
    st.lastLog = []
    this.onChange()
    this.log(`${name}: starting download job`)

    try {
      const result = await containers.runJob({
        image: this.image,
        command: [
          'python3', '/app/downloader.py',
          '--config', cfgInJob,
          '--once', '--source', name,
        ],
        inputs:  { '/cfg': rCfg.source },
        outputs: { '/data': rData.source },
        timeout: JOB_TIMEOUT_S,
        ownerPluginId: PLUGIN_ID,
        label: `grib-download ${name}`,
        onProgress: (line: string) => {
          st.lastLog.push(line)
          if (st.lastLog.length > LOG_TAIL) st.lastLog.shift()
          this.log(`  ${name}: ${line}`)
        },
      })
      // The downloader logs a final outcome line per source:
      //   "HH:MM:SS INFO <name>: downloaded|up-to-date|unavailable|failed"
      const outcomeRe = new RegExp(`INFO ${name}: (downloaded|up-to-date|unavailable|failed)\\s*$`)
      for (let i = st.lastLog.length - 1; i >= 0; i--) {
        const m = outcomeRe.exec(st.lastLog[i])
        if (m) { st.lastOutcome = m[1] as Outcome; break }
      }
      if (result.status !== 'completed' || result.exitCode !== 0) {
        st.lastOutcome = 'failed'
        st.lastError = `job ${result.status} (exit ${result.exitCode}): ${result.log?.slice(-3).join(' | ')}`
        this.log(`${name}: ${st.lastError}`)
      } else {
        this.log(`${name}: download job finished (${st.lastOutcome ?? 'outcome unknown'})`)
      }
      return true
    } catch (err) {
      st.lastOutcome = 'failed'
      st.lastError = String(err)
      this.log(`${name}: job error: ${err}`)
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
