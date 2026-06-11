import * as fs from 'fs'
import * as path from 'path'
import { Plugin, ServerAPI } from '@signalk/server-api'
import { Orchestrator } from './orchestrator'
import { sourceDirName } from './scheduler'
import { AppSettings, PluginSettings } from './types'

const PLUGIN_ID = 'signalk-grib-downloader'
const DEFAULT_IMAGE = 'ghcr.io/macjl/grib-downloader:latest'
const DEFAULT_ROOT = '/tmp/gribs'

// The plugin config panel only holds infrastructure settings. Everything
// operational (mode, area, sources, interval) is managed in the webapp at
// /signalk-grib-downloader/ and stored in <dataDir>/settings.json — the
// admin form cannot clobber it.
const CONFIG_SCHEMA = {
  type: 'object',
  description:
    'Sources, download area, auto/manual mode and scheduling are managed in ' +
    'the GRIB Downloader webapp (Webapps → GRIB Downloader).',
  properties: {
    gribsRoot: {
      type: 'string',
      title: 'GRIB root directory',
      description:
        'Each source downloads into <root>/<model>-<resolution>. Point the ' +
        'signalk-grib-weather-provider root at the same directory. ' +
        'Must be reachable from the container runtime.',
      default: DEFAULT_ROOT,
    },
    downloaderImage: {
      type: 'string',
      title: 'Downloader container image',
      default: DEFAULT_IMAGE,
    },
  },
}

const DEFAULT_APP_SETTINGS: AppSettings = {
  mode: 'auto',
  checkIntervalMinutes: 10,
  bbox: { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 },
  sources: [],
}

module.exports = (server: ServerAPI): Plugin => {
  let orchestrator: Orchestrator | null = null
  let timer: ReturnType<typeof setInterval> | null = null
  let infra: PluginSettings = {}
  let settings: AppSettings = { ...DEFAULT_APP_SETTINGS }

  const settingsPath = () => path.join(server.getDataDirPath(), 'settings.json')

  const loadSettings = (legacy: AppSettings): AppSettings => {
    try {
      return { ...DEFAULT_APP_SETTINGS, ...JSON.parse(fs.readFileSync(settingsPath(), 'utf-8')) }
    } catch {
      // First run — migrate any operational fields a previous version
      // stored in the plugin options, then persist them to settings.json.
      const migrated: AppSettings = {
        ...DEFAULT_APP_SETTINGS,
        ...(legacy.mode ? { mode: legacy.mode } : {}),
        ...(legacy.checkIntervalMinutes ? { checkIntervalMinutes: legacy.checkIntervalMinutes } : {}),
        ...(legacy.bbox ? { bbox: legacy.bbox } : {}),
        ...(legacy.sources ? { sources: legacy.sources } : {}),
      }
      saveSettings(migrated)
      return migrated
    }
  }

  const saveSettings = (s: AppSettings) => {
    fs.mkdirSync(server.getDataDirPath(), { recursive: true })
    fs.writeFileSync(settingsPath(), JSON.stringify(s, null, 2))
  }

  const updateStatus = () => {
    if (!orchestrator) return
    const mode = settings.mode ?? 'auto'
    const parts = orchestrator.status().map(s => {
      if (!s.enabled) return null
      if (s.running) return `${s.name}: downloading…`
      if (s.lastError) return `${s.name}: error`
      return `${s.name}: ${s.upToDate ? '✓' : '…'} ${s.lastRun ?? 'no data'}`
    }).filter(Boolean)
    server.setPluginStatus(`${mode} — ${parts.join(' · ') || 'no sources (configure in the webapp)'}`)
  }

  // (Re)build the orchestrator and timer from current infra + settings.
  const apply = (): string | null => {
    const containers = (globalThis as any).__signalk_containerManager
    if (!containers) return 'signalk-container plugin is required but not running'

    if (timer) { clearInterval(timer); timer = null }

    const sources = settings.sources ?? []
    orchestrator = new Orchestrator(
      sources,
      infra.gribsRoot ?? DEFAULT_ROOT,
      infra.downloaderImage ?? DEFAULT_IMAGE,
      server.getDataDirPath(),
      (msg: string) => server.debug(msg),
      updateStatus,
      settings.bbox
    )

    if ((settings.mode ?? 'auto') === 'auto' && sources.length > 0) {
      const interval = (settings.checkIntervalMinutes ?? 10) * 60_000
      containers.whenReady().then(() => {
        if (!containers.getRuntime()) {
          server.setPluginError('No container runtime detected')
          return
        }
        orchestrator?.tick().catch((err: unknown) => server.debug(`tick error: ${err}`))
        timer = setInterval(
          () => orchestrator?.tick().catch((err: unknown) => server.debug(`tick error: ${err}`)),
          interval
        )
      })
    }
    updateStatus()
    return null
  }

  const plugin: Plugin = {
    id: PLUGIN_ID,
    name: 'GRIB Downloader',
    schema: () => CONFIG_SCHEMA,

    start: (options: PluginSettings & AppSettings) => {
      infra = { gribsRoot: options?.gribsRoot, downloaderImage: options?.downloaderImage }
      settings = loadSettings(options ?? {})
      const err = apply()
      if (err) server.setPluginError(err)
    },

    stop: () => {
      if (timer) { clearInterval(timer); timer = null }
      orchestrator = null
    },

    registerWithRouter: (router: any) => {
      router.get('/status', (_req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        res.json({ mode: settings.mode ?? 'auto', sources: orchestrator.status() })
      })

      // Operational settings, webapp-managed. ('/config' would collide with
      // SignalK's built-in plugin config routes.)
      router.get('/settings', (_req: any, res: any) => {
        res.json(settings)
      })

      router.put('/settings', (req: any, res: any) => {
        const next = req.body as AppSettings
        if (!next || typeof next !== 'object') {
          return res.status(400).json({ error: 'invalid settings' })
        }
        const valid = ['gfs', 'arome', 'arpege', 'icon-eu']
        if ((next.sources ?? []).some(s => !valid.includes(s.model))) {
          return res.status(400).json({ error: `model must be one of: ${valid.join(', ')}` })
        }
        const names = (next.sources ?? []).map(s => sourceDirName(s))
        if (new Set(names).size !== names.length) {
          return res.status(400).json({ error: 'duplicate source: each (model, resolution) pair must be unique' })
        }
        try {
          saveSettings(next)
        } catch (err) {
          return res.status(500).json({ error: String(err) })
        }
        settings = { ...DEFAULT_APP_SETTINGS, ...next }
        const applyErr = apply()
        if (applyErr) return res.status(500).json({ error: applyErr })
        res.json({ ok: true })
      })

      router.post('/download', (_req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const stale = orchestrator.staleSources()
        orchestrator.downloadAll().catch(err => server.debug(`download error: ${err}`))
        res.status(202).json({
          started: orchestrator.enabledSources().map(sourceDirName),
          behind: stale.map(sourceDirName),
        })
      })

      router.post('/download/:name', (req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const source = orchestrator.findSource(req.params.name)
        if (!source) return res.status(404).json({ error: `no source named ${req.params.name}` })
        orchestrator.downloadSource(source).catch(err => server.debug(`download error: ${err}`))
        res.status(202).json({ started: [sourceDirName(source)] })
      })
    },
  }

  return plugin
}
