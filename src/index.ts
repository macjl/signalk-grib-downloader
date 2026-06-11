import { Plugin, ServerAPI } from '@signalk/server-api'
import { Orchestrator } from './orchestrator'
import { PluginSettings } from './types'

const PLUGIN_ID = 'signalk-grib-downloader'
const DEFAULT_IMAGE = 'ghcr.io/macjl/grib-downloader:latest'

// The schema mirrors what the webapp manages — keeping it complete means the
// admin plugin-config panel stays usable and never drops fields on save.
// The primary management UI is the webapp at /signalk-grib-downloader/.
const CONFIG_SCHEMA = {
  type: 'object',
  properties: {
    mode: {
      type: 'string',
      title: 'Mode',
      description:
        'auto: download each model run as soon as it is published. ' +
        'manual: download only when triggered from the webapp (intermittent connections)',
      enum: ['auto', 'manual'],
      default: 'auto',
    },
    checkIntervalMinutes: {
      type: 'number',
      title: 'Check interval (minutes, auto mode)',
      default: 10,
      minimum: 1,
    },
    gribsRoot: {
      type: 'string',
      title: 'GRIB root directory',
      description:
        'Each source downloads into <root>/<source-name>. Point the ' +
        'signalk-grib-weather-provider sources at the same directories. ' +
        'Must be reachable from the container runtime.',
      default: '/tmp/gribs',
    },
    downloaderImage: {
      type: 'string',
      title: 'Downloader container image',
      default: DEFAULT_IMAGE,
    },
    bbox: {
      type: 'object',
      title: 'Download area (GFS only — other models distribute whole domains)',
      properties: {
        latMin: { type: 'number', title: 'South latitude', default: 35 },
        lonMin: { type: 'number', title: 'West longitude', default: -6 },
        latMax: { type: 'number', title: 'North latitude', default: 45 },
        lonMax: { type: 'number', title: 'East longitude', default: 17 },
      },
    },
    sources: {
      type: 'array',
      title: 'Sources',
      items: {
        type: 'object',
        required: ['name', 'model'],
        properties: {
          name: {
            type: 'string',
            title: 'Source ID',
            description: 'Also the subdirectory name under the GRIB root (e.g. "gfs-025")',
          },
          model: {
            type: 'string',
            title: 'Model',
            enum: ['gfs', 'arome', 'arpege', 'icon-eu'],
            default: 'gfs',
          },
          resolution: {
            type: 'string',
            title: 'Resolution',
            description: 'gfs: 0p25 | 0p50 | 1p00 — arome: 0025 | 001 — arpege: 01 | 025 — icon-eu: (fixed)',
          },
          hours: {
            type: 'number',
            title: 'Forecast duration (hours)',
            description: 'Capped per model: gfs 384, arome 51, arpege 102, icon-eu 120',
          },
          enabled: { type: 'boolean', title: 'Enabled', default: true },
        },
      },
    },
  },
}

module.exports = (server: ServerAPI): Plugin => {
  let orchestrator: Orchestrator | null = null
  let timer: ReturnType<typeof setInterval> | null = null
  let currentOptions: PluginSettings = {}

  const updateStatus = () => {
    if (!orchestrator) return
    const mode = currentOptions.mode ?? 'auto'
    const parts = orchestrator.status().map(s => {
      if (!s.enabled) return null
      if (s.running) return `${s.name}: downloading…`
      if (s.lastError) return `${s.name}: error`
      return `${s.name}: ${s.upToDate ? '✓' : '…'} ${s.lastRun ?? 'no data'}`
    }).filter(Boolean)
    server.setPluginStatus(`${mode} — ${parts.join(' · ') || 'no sources'}`)
  }

  // (Re)build the orchestrator and timer from a settings object.
  const applyOptions = (options: PluginSettings): string | null => {
    const containers = (globalThis as any).__signalk_containerManager
    if (!containers) return 'signalk-container plugin is required but not running'

    if (timer) { clearInterval(timer); timer = null }
    currentOptions = options

    const sources = options.sources ?? []
    orchestrator = new Orchestrator(
      sources,
      options.gribsRoot ?? '/tmp/gribs',
      options.downloaderImage ?? DEFAULT_IMAGE,
      server.getDataDirPath(),
      (msg: string) => server.debug(msg),
      updateStatus,
      options.bbox
    )

    if ((options.mode ?? 'auto') === 'auto' && sources.length > 0) {
      const interval = (options.checkIntervalMinutes ?? 10) * 60_000
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

    start: (options: PluginSettings) => {
      const err = applyOptions(options ?? {})
      if (err) server.setPluginError(err)
    },

    stop: () => {
      if (timer) { clearInterval(timer); timer = null }
      orchestrator = null
    },

    registerWithRouter: (router: any) => {
      router.get('/status', (_req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        res.json({ mode: currentOptions.mode ?? 'auto', sources: orchestrator.status() })
      })

      // /config collides with SignalK's built-in plugin config routes — use /settings.
      router.get('/settings', (_req: any, res: any) => {
        res.json(currentOptions)
      })

      router.put('/settings', (req: any, res: any) => {
        const options = req.body as PluginSettings
        if (!options || typeof options !== 'object') {
          return res.status(400).json({ error: 'invalid configuration' })
        }
        const names = (options.sources ?? []).map(s => s.name)
        if (names.some(n => !n || !/^[A-Za-z0-9._-]+$/.test(n))) {
          return res.status(400).json({ error: 'source names must be non-empty and URL-safe' })
        }
        if (new Set(names).size !== names.length) {
          return res.status(400).json({ error: 'source names must be unique' })
        }
        ;(server as any).savePluginOptions(options, (err: unknown) => {
          if (err) return res.status(500).json({ error: String(err) })
          const applyErr = applyOptions(options)
          if (applyErr) return res.status(500).json({ error: applyErr })
          res.json({ ok: true })
        })
      })

      router.post('/download', (_req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const stale = orchestrator.staleSources()
        orchestrator.downloadAll().catch(err => server.debug(`download error: ${err}`))
        res.status(202).json({
          started: orchestrator.enabledSources().map(s => s.name),
          behind: stale.map(s => s.name),
        })
      })

      router.post('/download/:name', (req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const source = orchestrator.findSource(req.params.name)
        if (!source) return res.status(404).json({ error: `no source named ${req.params.name}` })
        orchestrator.downloadSource(source).catch(err => server.debug(`download error: ${err}`))
        res.status(202).json({ started: [source.name] })
      })
    },
  }

  return plugin
}
