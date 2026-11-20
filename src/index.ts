import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { createHash } from 'node:crypto'
import { Path, Plugin, ServerAPI } from '@signalk/server-api'
import { Orchestrator, ProbeTiming } from './orchestrator'
import { CataloguePublisher } from './catalogue'
import { CatalogueAccess } from './catalogue-access'
import { LegacyGribExporter } from './legacy-export'
import { sourceDirName } from './scheduler'
import { RemoteCatalogue, normalizeRemoteUrl, remoteId, validRemoteModel } from './remote-catalogue'
import { AppSettings, InternetState, PluginSettings, SourceSetting } from './types'
import { directDomains, mapZone, validZone } from './coverage'

const PLUGIN_ID = 'signalk-grib-downloader'
const DEFAULT_CHECK_INTERVAL_MINUTES = 5

const DEFAULT_METERED_MULTIPLIER = 3
// setTimeout delays are 32-bit — never schedule further out than this.
const TIMER_MAX_MS = 2_147_483_000

// SignalK path publishing the uplink state (online | offline | metered |
// captive), maintained by whichever connectivity-tracking plugin is
// installed.
const INTERNET_STATE_PATH = 'network.internet.state' as Path

// Expand home-relative token-file paths.
function expandHome(p: string): string {
  if (p === '~') return os.homedir()
  if (p.startsWith('~/')) return path.join(os.homedir(), p.slice(2))
  return p
}

// The plugin config panel holds infrastructure and scheduler settings.
// Operational download choices (area and sources) are managed in the webapp at
// /signalk-grib-downloader/ and stored in <dataDir>/settings.json — the
// admin form cannot clobber it.
const buildSchema = (defaultInterval: number, defaultMultiplier: number) => ({
  type: 'object',
  description:
    'Sources and download area are managed in the GRIB Downloader webapp ' +
    '(Webapps → GRIB Downloader).',
  properties: {
    checkIntervalMinutes: {
      type: 'number',
      title: 'Publication probe interval (minutes)',
      description:
        'Overrides the model\'s probe interval while waiting for publication. ' +
        'The same interval applies before and after the late-publication threshold. ' +
        'Manual downloads are always available from the webapp.',
      default: defaultInterval,
      minimum: 1,
    },
    meteredIntervalMultiplier: {
      type: 'number',
      title: 'Metered connection slowdown (×)',
      description:
        'On a metered connection (pay-per-MB satellite), automatic checks wait ' +
        'this many times longer between publication probes. ' +
        'Manual downloads are never blocked.',
      default: defaultMultiplier,
      minimum: 1,
    },
    catalogueUrl: {
      type: 'string', title: 'Forecast catalogue server URL',
      description: 'Signal K server running Forecast Dataset Provider with streamed publications (0.1.0-beta.1 or later). Remote servers require HTTPS.',
      default: 'http://127.0.0.1:3000',
    },
    catalogueTokenFile: {
      type: 'string', title: 'Catalogue token file',
      description: 'Token file, default <plugin data directory>/catalogue-token. When missing, approve the automatic readwrite request in Signal K Security > Access Requests. File contents are never returned by the webapp.',
      default: '',
    },
    remoteServers: {
      type: 'array',
      title: 'Remote Signal K sources',
      description: 'Save the server, then request a read/write token from its row in Plugin Config.',
      items: {
        type: 'object',
        required: ['id', 'url'],
        properties: {
          id: { type: 'string', title: 'Server identifier', pattern: remoteId.source },
          url: { type: 'string', title: 'Signal K HTTPS base URL' },
          tokenFile: { type: 'string', title: 'Remote token file',
            description: 'Optional file for an existing token. An approved token requested from this panel is stored separately and takes precedence.' },
        },
      },
    },
    legacyExportEnabled: {
      type: 'boolean', title: 'Export legacy GRIB files', default: true,
      description: 'Enabled by default during the transition. While enabled, GRIB files are stored twice: in Forecast Dataset Provider and in the export directory, approximately doubling GRIB disk usage. Uncheck to disable when no longer needed. The catalogue is still required.',
    },
    gribsRoot: {
      type: 'string', title: 'Legacy GRIB export directory', default: '~/.signalk/gribs',
      description: 'Used only when legacy export is enabled. Each source has its own directory; archiveRuns also controls local historical archives.',
    },
  },
})

function parseSourceName(name: string): SourceSetting | undefined {
  const remote = /^signalk-([a-z0-9][a-z0-9-]{0,31})__(gfs-wave|gfs|arome|arpege|icon-eu)(?:-([a-zA-Z0-9]{1,16}))?$/.exec(name)
  if (remote) return { remoteServer: remote[1], model: remote[2] as SourceSetting['model'], resolution: remote[3] }
  if (name === 'icon-eu') return { model: 'icon-eu' }
  const match = /^(gfs-wave|gfs|arome|arpege)-([A-Za-z0-9]+)$/.exec(name)
  return match ? { model: match[1] as SourceSetting['model'], resolution: match[2] } : undefined
}

const DEFAULT_APP_SETTINGS: AppSettings = {
  bbox: { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 },
  sources: [],
}

module.exports = (server: ServerAPI): Plugin => {
  let orchestrator: Orchestrator | null = null
  let timer: ReturnType<typeof setTimeout> | null = null
  let nextTickAtMs: number | null = null
  let ticking = false
  let internetState: InternetState = 'unknown'
  let stopInternetStream: (() => void) | null = null
  let infra: PluginSettings = {}
  let remotes = new Map<string, RemoteCatalogue>()
  let remoteAccesses = new Map<string, CatalogueAccess>()
  let catalogue: CataloguePublisher
  let access: CatalogueAccess | null = null
  const deleting = new Map<string, AbortController>()
  let settings: AppSettings = { ...DEFAULT_APP_SETTINGS }
  let legacyIntervalMinutes: number | undefined

  const settingsPath = () => path.join(server.getDataDirPath(), 'settings.json')

  const validInterval = (value: unknown): number | undefined => {
    const n = Number(value)
    return Number.isFinite(n) && n >= 1 ? Math.floor(n) : undefined
  }

  const configuredIntervalMinutes = () =>
    validInterval(infra.checkIntervalMinutes) ??
    legacyIntervalMinutes

  const intervalMinutes = () => configuredIntervalMinutes() ?? DEFAULT_CHECK_INTERVAL_MINUTES

  const validMultiplier = (value: unknown): number | undefined => {
    const n = Number(value)
    return Number.isFinite(n) && n >= 1 ? n : undefined
  }

  const meteredMultiplier = () =>
    validMultiplier(infra.meteredIntervalMultiplier) ?? DEFAULT_METERED_MULTIPLIER

  const normalizeSettings = (raw: AppSettings): AppSettings => {
    const legacyManual = raw.mode === 'manual'
    const normalized = {
      ...DEFAULT_APP_SETTINGS,
      ...raw,
      checkIntervalMinutes: validInterval(raw.checkIntervalMinutes),
      mode: undefined,
      sources: (raw.sources ?? []).map(source => {
        const { enabled, ...rest } = source
        return {
          ...rest,
          autoDownload: source.autoDownload ?? (!legacyManual && enabled !== false),
        }
      }),
    }
    if (!normalized.checkIntervalMinutes) delete normalized.checkIntervalMinutes
    return normalized
  }

  const loadSettings = (legacy: AppSettings): AppSettings => {
    try {
      const raw = JSON.parse(fs.readFileSync(settingsPath(), 'utf-8')) as AppSettings
      legacyIntervalMinutes = validInterval(raw.checkIntervalMinutes)
      const loaded = normalizeSettings(raw)
      saveSettings(loaded)
      return loaded
    } catch {
      // First run — migrate any operational fields a previous version
      // stored in the plugin options, then persist them to settings.json.
      legacyIntervalMinutes = validInterval(legacy.checkIntervalMinutes)
      const migrated = normalizeSettings({
        ...DEFAULT_APP_SETTINGS,
        ...(legacy.mode ? { mode: legacy.mode } : {}),
        ...(legacy.checkIntervalMinutes ? { checkIntervalMinutes: legacy.checkIntervalMinutes } : {}),
        ...(legacy.bbox ? { bbox: legacy.bbox } : {}),
        ...(legacy.sources ? { sources: legacy.sources } : {}),
      })
      saveSettings(migrated)
      return migrated
    }
  }

  const saveSettings = (s: AppSettings) => {
    fs.mkdirSync(server.getDataDirPath(), { recursive: true })
    fs.writeFileSync(settingsPath(), JSON.stringify(s, null, 2))
  }

  const remoteAccessStatus = () => Array.from(remoteAccesses, ([id, authorizer]) => {
    const { state, diagnostics } = authorizer.status()
    let visibleState = state
    if (state === 'ready') {
      try { if (!authorizer.currentToken()) visibleState = 'idle' }
      catch { visibleState = 'error' }
    }
    return { id, endpoint: remotes.get(id)!.endpoint, state: visibleState,
      httpStatus: state === 'error' ? diagnostics.lastIssue?.httpStatus : undefined }
  })

  const updateStatus = () => {
    if (!orchestrator) return
    if (access && access.status().state !== 'ready') {
      server.setPluginStatus(access.status().message)
      return
    }
    const parts = orchestrator.status().map(s => {
      if (s.running) return `${s.name}: downloading…`
      if (s.lastError) return `${s.name}: error`
      if (s.publicationPending) return `${s.name}: catalogue publication pending`
      const mode = s.autoDownload ? 'auto' : 'manual'
      if (s.publicationLate) return `${s.name}: ${mode} publication late (${s.expectedRun})`
      if (s.pendingRun) return `${s.name}: ${mode} awaiting publication (${s.pendingRun})`
      return `${s.name}: ${mode} ${s.upToDate ? '✓' : '…'} ${s.lastRun ?? 'no data'}`
    }).filter(Boolean)
    const gate = internetState === 'offline'
      ? 'offline — auto paused'
      : internetState === 'captive'
        ? 'captive portal — auto paused'
        : internetState === 'metered'
          ? 'metered — auto slowed'
          : null
    if (gate) parts.unshift(gate)
    server.setPluginStatus(parts.join(' · ') || 'no sources (configure in the webapp)')
  }

  // ── Internet-aware, publication-aware auto scheduling ──────────────
  // Each source starts probing at its model's lower publication bound.
  // The model interval (or explicit user override) stays constant when
  // publication passes the upper bound; only the displayed status changes.
  // `network.internet.state` (when some plugin publishes it) gates the
  // schedule: offline and captive portals pause auto downloads, metered
  // stretches every wait by meteredIntervalMultiplier. Without the path
  // the state stays 'unknown', which behaves exactly as 'online' — no
  // delta publisher required.

  const isInternetState = (v: unknown): v is InternetState =>
    v === 'online' || v === 'offline' || v === 'metered' || v === 'captive' || v === 'unknown'

  // No auto scheduling or ticking while there is no usable internet:
  // offline means no link, captive means every request lands on the
  // portal's login page. Both pause the scheduler identically.
  const autoPaused = () => internetState === 'offline' || internetState === 'captive'

  // Metering stretches probe intervals, never the lateness threshold.
  const timingScale = () => internetState === 'metered' ? meteredMultiplier() : 1

  const probeTiming = (): ProbeTiming => ({
    intervalMinutes: configuredIntervalMinutes(),
    scale: timingScale(),
  })

  // Delay until some auto source next needs attention.
  const nextAutoTickDelayMs = (): number | null => {
    const orch = orchestrator
    if (!orch) return null
    const auto = orch.autoSources()
    if (auto.length === 0) return null
    const timing = probeTiming()
    const earliest = Math.min(...auto.map(s => orch.nextTickAt(s, timing).getTime()))
    return Math.min(TIMER_MAX_MS, Math.max(0, earliest - Date.now()))
  }

  const scheduleNextTick = (): void => {
    if (timer) { clearTimeout(timer); timer = null }
    nextTickAtMs = null
    if (!orchestrator || autoPaused() || (access && access.status().state !== 'ready')) return
    const delay = nextAutoTickDelayMs()
    if (delay === null) return
    nextTickAtMs = Date.now() + delay
    timer = setTimeout(runTick, delay)
    server.debug(`auto: next check in ${Math.round(delay / 60_000)} min`)
  }

  // One auto tick, rescheduling once it settles. A tick awaits its
  // downloads; a trigger arriving mid-flight is skipped and the settling
  // tick reschedules, so downloads stay sequential.
  const runTick = (): void => {
    if (timer) { clearTimeout(timer); timer = null }
    nextTickAtMs = null
    if (!orchestrator || ticking || deleting.size) return
    if (access && access.status().state !== 'ready') { updateStatus(); return }
    ticking = true
    orchestrator.tick(internetState, probeTiming())
      .catch((err: unknown) => server.debug(`tick error: ${err}`))
      .finally(() => {
        ticking = false
        scheduleNextTick()
        updateStatus()
      })
  }

  const onInternetState = (value: unknown): void => {
    const next = isInternetState(value) ? value : 'unknown'
    if (next === internetState) return
    const prev = internetState
    internetState = next
    server.debug(`internet: ${prev} → ${next}`)
    if (next === 'offline' || next === 'captive') {
      if (timer) { clearTimeout(timer); timer = null }
      nextTickAtMs = null
      updateStatus()
      return
    }
    // Connectivity (re)established or throttling changed: when we were
    // paused (offline or captive) or slowed, catch up right away instead
    // of waiting for the (possibly far-away) next scheduled tick.
    if (prev === 'offline' || prev === 'captive' || prev === 'metered') runTick()
    else scheduleNextTick()
    updateStatus()
  }

  // (Re)build the orchestrator and auto scheduler from current settings.
  const apply = (): void => {
    if (timer) { clearTimeout(timer); timer = null }
    nextTickAtMs = null

    const sources = settings.sources ?? []
    orchestrator?.stop()
    orchestrator = new Orchestrator(
      sources,
      catalogue,
      (msg: string) => server.debug(msg),
      updateStatus,
      settings.bbox,
      remotes
    )

    if (sources.some(s => s.autoDownload !== false)) {
      runTick() // immediate catch-up; reschedules when it settles
    } else {
      updateStatus()
    }
  }

  const plugin: Plugin = {
    id: PLUGIN_ID,
    name: 'GRIB Downloader',
    schema: () => buildSchema(intervalMinutes(), meteredMultiplier()),

    start: (options: PluginSettings & AppSettings) => {
      infra = {
        catalogueUrl: options?.catalogueUrl,
        catalogueTokenFile: options?.catalogueTokenFile,
        legacyExportEnabled: options?.legacyExportEnabled !== false,
        gribsRoot: options?.gribsRoot,
        checkIntervalMinutes: validInterval(options?.checkIntervalMinutes),
        meteredIntervalMultiplier: validMultiplier(options?.meteredIntervalMultiplier),
      }
      const tokenFile = expandHome(infra.catalogueTokenFile || path.join(server.getDataDirPath(), 'catalogue-token'))
      if (!path.isAbsolute(tokenFile)) throw new Error('Catalogue token file must be absolute')
      remotes = new Map()
      remoteAccesses = new Map()
      const destination = normalizeRemoteUrl(infra.catalogueUrl || 'http://127.0.0.1:3000')
      for (const peer of options?.remoteServers ?? []) {
        if (!peer || typeof peer.id !== 'string' || !remoteId.test(peer.id) || remotes.has(peer.id))
          throw new Error('Remote Signal K server identifiers must be valid and unique')
        const remoteToken = peer.tokenFile ? expandHome(peer.tokenFile) : undefined
        if (remoteToken && (!path.isAbsolute(remoteToken) || path.resolve(remoteToken) === path.resolve(tokenFile)))
          throw new Error('Configure a separate absolute token file for the remote Signal K source')
        const endpoint = normalizeRemoteUrl(peer.url)
        if (endpoint === destination) throw new Error('Use a different server for the remote source and destination catalogue')
        const key = createHash('sha256').update(endpoint).digest('hex')
        const dataDir = path.join(server.getDataDirPath(), 'remote-access', peer.id + '-' + key)
        const managedToken = path.join(dataDir, 'token')
        if (path.resolve(managedToken) === path.resolve(tokenFile))
          throw new Error('Configure a separate token file for the destination catalogue')
        const authorizer = new CatalogueAccess({
          endpoint, tokenFile: managedToken, dataDir,
          pluginId: PLUGIN_ID, permissions: 'readwrite', manualRequest: true,
          description: `GRIB Downloader — remote forecast source ${peer.id}`,
          onReady: () => { orchestrator?.retryAfterAccessApproval(peer.id); updateStatus(); if (orchestrator && !autoPaused()) runTick() },
        })
        const remote = new RemoteCatalogue({ ...peer, tokenFile: remoteToken }, () => authorizer.currentToken())
        remotes.set(peer.id, remote)
        remoteAccesses.set(peer.id, authorizer)
      }
      const legacyExport = infra.legacyExportEnabled ? new LegacyGribExporter(expandHome(infra.gribsRoot || '~/.signalk/gribs')) : undefined
      catalogue = new CataloguePublisher(infra.catalogueUrl || 'http://127.0.0.1:3000', tokenFile, server.getDataDirPath(), () => authorizer.token(), token => authorizer.rejected(token), legacyExport)
      const authorizer = new CatalogueAccess({
        endpoint: catalogue.endpoint, tokenFile, dataDir: server.getDataDirPath(),
        pluginId: PLUGIN_ID, permissions: 'readwrite', onChange: updateStatus,
        onReady: () => { orchestrator?.retryAfterAccessApproval(); updateStatus(); if (orchestrator && !autoPaused()) runTick() },
      })
      access = authorizer
      settings = loadSettings(options ?? {})

      // Follow network.internet.state if some plugin publishes it: seed
      // from the server's tree, then follow its deltas. Without a
      // publisher neither ever fires and the state stays 'unknown',
      // which behaves exactly as 'online' (today's behavior).
      const seed = server.getSelfPath(INTERNET_STATE_PATH)
      internetState = isInternetState(seed) ? seed : 'unknown'
      stopInternetStream = server.streambundle
        .getSelfStream(INTERNET_STATE_PATH)
        .onValue(onInternetState)

      apply()
      access.start()
      for (const authorizer of remoteAccesses.values()) authorizer.start()
    },

    stop: async () => {
      const active = orchestrator
      orchestrator = null
      const previousAccess = access
      const previousRemoteAccess = remoteAccesses
      access = null
      remoteAccesses = new Map()
      await Promise.all([previousAccess?.stop(), ...Array.from(previousRemoteAccess.values(), authorizer => authorizer.stop())])
      stopInternetStream?.()
      stopInternetStream = null
      if (timer) { clearTimeout(timer); timer = null }
      nextTickAtMs = null
      for (const controller of deleting.values()) controller.abort(new Error('plugin stopped'))
      await active?.stop()
      while (deleting.size) await new Promise(resolve => setTimeout(resolve, 10))
    },

    registerWithRouter: (router: any) => {
      router.get('/status', (_req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        res.json({
          catalogueUrl: catalogue.endpoint,
          catalogueAccess: access?.status(),
          remoteAccess: remoteAccessStatus(),
          legacyExportEnabled: infra.legacyExportEnabled === true,
          checkIntervalMinutes: intervalMinutes(),
          internetState,
          nextAutoTickAt: nextTickAtMs === null ? null : new Date(nextTickAtMs).toISOString(),
          sources: orchestrator.status(),
        })
      })

      // Operational settings, webapp-managed. ('/config' would collide with
      // SignalK's built-in plugin config routes.)
      router.get('/settings', (_req: any, res: any) => {
        res.json({ ...settings, checkIntervalMinutes: intervalMinutes(), remoteServerIds: [...remotes.keys()], directDomains,
          sourceNames: (settings.sources ?? []).map(sourceDirName) })
      })

      router.get('/remote-models/:server', async (req: any, res: any) => {
        const peer = remotes.get(req.params.server)
        if (!peer) return res.status(404).json({ error: 'Unknown remote Signal K server' })
        try {
          res.json({ models: await peer.models(AbortSignal.timeout(20_000)) })
        } catch (error) {
          res.status(502).json({ error: error instanceof Error ? error.message : 'Remote catalogue unavailable' })
        }
      })

      router.get('/remote-access', (_req: any, res: any) => {
        res.set('Cache-Control', 'no-store').json(remoteAccessStatus())
      })

      router.post('/remote-access/:server', (req: any, res: any) => {
        const peer = remotes.get(req.params.server)
        const authorizer = remoteAccesses.get(req.params.server)
        if (!peer || !authorizer) return res.status(404).json({ error: 'Save the remote server configuration first' })
        if (req.body?.endpoint !== peer.endpoint)
          return res.status(409).json({ error: 'The saved remote server URL changed' })
        try { authorizer.request() }
        catch { return res.status(500).json({ error: 'Cannot read the managed remote token file' }) }
        res.status(202).json({ state: authorizer.status().state })
      })

      router.get('/remote-preview/:server', async (req: any, res: any) => {
        const peer = remotes.get(req.params.server)
        if (!peer) return res.status(404).json({ error: 'Unknown remote Signal K server' })
        const { model, resolution, bbox } = req.query
        if (!validRemoteModel(model) ||
            (resolution !== undefined && (typeof resolution !== 'string' || !/^[a-zA-Z0-9]{1,16}$/.test(resolution))))
          return res.status(400).json({ error: 'Choose a remote model and a valid resolution' })
        const zone = typeof bbox === 'string' && bbox.split(',').every(part => part.trim())
          ? bbox.split(',').map(Number) : undefined
        const valid = validZone(zone) && zone[0] < zone[2]
        if (bbox !== undefined && !valid)
          return res.status(400).json({ error: 'Choose a valid map area' })
        try {
          res.set('Cache-Control', 'no-store').json(await peer.preview(model, resolution, AbortSignal.timeout(20_000), valid ? zone : undefined))
        } catch (error) {
          res.status(502).json({ error: error instanceof Error ? error.message : 'Remote catalogue unavailable' })
        }
      })

      router.put('/settings', (req: any, res: any) => {
        if (orchestrator?.isRunning() || deleting.size) return res.status(409).json({ error: 'Wait for the current transfer or deletion to finish' })
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const nextRaw = req.body as AppSettings
        if (!nextRaw || typeof nextRaw !== 'object' || (nextRaw.sources !== undefined && !Array.isArray(nextRaw.sources))) {
          return res.status(400).json({ error: 'invalid settings' })
        }
        const { sources, bbox, mode, checkIntervalMinutes } = nextRaw
        const next = normalizeSettings({ sources, bbox: bbox ?? settings.bbox, mode, checkIntervalMinutes })
        if (next.bbox && (!validZone(mapZone(next.bbox)) || next.bbox.lonMin >= next.bbox.lonMax))
          return res.status(400).json({ error: 'Choose a valid map area' })
        const valid = ['gfs', 'gfs-wave', 'arome', 'arpege', 'icon-eu']
        if ((next.sources ?? []).some(s => !valid.includes(s.model))) {
          return res.status(400).json({ error: `model must be one of: ${valid.join(', ')}` })
        }
        if ((next.sources ?? []).some(s => !s.remoteServer && s.model === 'gfs-wave' && s.resolution && s.resolution !== '0p25')) {
          return res.status(400).json({ error: 'GFS-Wave supports only 0p25 resolution' })
        }
        if ((next.sources ?? []).some(s => s.remoteServer && (!remotes.has(s.remoteServer) ||
            (s.resolution && !/^[a-zA-Z0-9]{1,16}$/.test(s.resolution))))) {
          return res.status(400).json({ error: 'Choose a configured remote Signal K server and a valid resolution' })
        }
        if ((next.sources ?? []).some(s => s.remoteModel !== undefined && (!s.remoteServer || !validRemoteModel(s.remoteModel)))) {
          return res.status(400).json({ error: 'Choose a valid model advertised by the remote catalogue' })
        }
        if ((next.sources ?? []).some(s => s.remoteHours !== undefined &&
            (!s.remoteServer || typeof s.remoteHours !== 'number' ||
              !Number.isSafeInteger(Math.round(s.remoteHours * 3_600_000))))) {
          return res.status(400).json({ error: 'Invalid remote forecast cutoff' })
        }
        const names = (next.sources ?? []).map(s => sourceDirName(s))
        if (names.some(name => !/^[A-Za-z0-9_-]+$/.test(name))) return res.status(400).json({ error: 'invalid source name' })
        if (new Set(names).size !== names.length) {
          return res.status(400).json({ error: 'duplicate source: each (server, model, resolution) combination must be unique' })
        }
        if ((next.sources ?? []).some(s =>
            s.archiveRuns !== undefined && (!Number.isFinite(s.archiveRuns) || s.archiveRuns < 0))) {
          return res.status(400).json({ error: 'archiveRuns must be a number ≥ 0' })
        }
        if ((next.sources ?? []).some(s =>
            s.retentionDays !== undefined && (!Number.isInteger(s.retentionDays) || s.retentionDays < 0 || s.retentionDays > 365))) {
          return res.status(400).json({ error: 'retentionDays must be a whole number between 0 and 365' })
        }
        try {
          saveSettings(next)
        } catch (err) {
          return res.status(500).json({ error: String(err) })
        }
        settings = next
        apply()
        res.json((next.sources ?? []).some(s => s.remoteServer) ? { ok: true, sourceNames: names } : { ok: true })
      })

      router.post('/download', (_req: any, res: any) => {
        if (deleting.size) return res.status(409).json({ error: 'Source deletion in progress' })
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const sources = settings.sources ?? []
        const stale = orchestrator.staleSources(sources)
        orchestrator.downloadAll().catch(err => server.debug(`download error: ${err}`))
        res.status(202).json({
          started: sources.map(sourceDirName),
          behind: stale.map(sourceDirName),
        })
      })

      // Only delete catalogue IDs recorded by this downloader; provider locks
      // and derived-data conflicts are respected (no forced deletion).
      router.delete('/source-data/:name', async (req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const name = req.params.name
        if (!/^[A-Za-z0-9_-]+$/.test(name)) return res.status(400).json({ error: 'invalid source name' })
        if (orchestrator?.isRunning() || deleting.has(name)) return res.status(409).json({ error: 'Source is busy' })
        const remoteJournal = /^signalk-[a-z0-9][a-z0-9-]{0,31}__model-[a-f0-9]{24}(?:-[a-zA-Z0-9]{1,16})?$/.test(name)
        const source = orchestrator?.findSource(name) ?? parseSourceName(name) ?? (remoteJournal ? name : undefined)
        if (!source) return res.status(400).json({ error: 'invalid source name' })
        const controller = new AbortController()
        deleting.set(name, controller)
        try {
          await catalogue.removeSource(source, controller.signal)
          controller.signal.throwIfAborted()
          res.json({ ok: true })
        } catch (error) {
          res.status(502).json({ error: String(error) })
        } finally { deleting.delete(name); scheduleNextTick() }
      })

      router.post('/download/:name', (req: any, res: any) => {
        if (deleting.size) return res.status(409).json({ error: 'Source deletion in progress' })
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const source = orchestrator.findSource(req.params.name)
        if (!source) return res.status(404).json({ error: `no source named ${req.params.name}` })
        const expectedSubset = req.query?.subset
        if (expectedSubset !== undefined && (!source.remoteServer || !['area', 'time', 'both'].includes(expectedSubset)))
          return res.status(400).json({ error: 'Invalid remote subsetting mode' })
        orchestrator.downloadSource(source, expectedSubset).catch(err => server.debug(`download error: ${err}`))
        res.status(202).json({ started: [sourceDirName(source)] })
      })
    },
  }

  return plugin
}
