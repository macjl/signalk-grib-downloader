import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Plugin, ServerAPI } from '@signalk/server-api'
import { Orchestrator } from './orchestrator'
import { sourceDirName } from './scheduler'
import { AppSettings, PluginSettings } from './types'

const PLUGIN_ID = 'signalk-grib-downloader'
const DEFAULT_CHECK_INTERVAL_MINUTES = 10

// "~/gribs" is not expanded by Node — resolve it ourselves.
function expandHome(p: string): string {
  if (p === '~') return os.homedir()
  if (p.startsWith('~/')) return path.join(os.homedir(), p.slice(2))
  return p
}

// The plugin config panel holds infrastructure and scheduler settings.
// Operational download choices (area and sources) are managed in the webapp at
// /signalk-grib-downloader/ and stored in <dataDir>/settings.json — the
// admin form cannot clobber it.
const buildSchema = (defaultRoot: string, defaultInterval: number) => ({
  type: 'object',
  description:
    'Sources and download area are managed in the GRIB Downloader webapp ' +
    '(Webapps → GRIB Downloader).',
  properties: {
    checkIntervalMinutes: {
      type: 'number',
      title: 'Automatic check interval (minutes)',
      description:
        'How often auto-enabled sources are checked for a newly published run. ' +
        'Manual downloads are always available from the webapp.',
      default: defaultInterval,
      minimum: 1,
    },
    gribsRoot: {
      type: 'string',
      title: 'GRIB root directory',
      description:
        'Each source downloads into <root>/<model>-<resolution>. Point the ' +
        'signalk-grib-weather-provider root at the same directory. ' +
        '"~" is expanded.',
      default: defaultRoot,
    },
  },
})

const DEFAULT_APP_SETTINGS: AppSettings = {
  bbox: { latMin: 35, lonMin: -6, latMax: 45, lonMax: 17 },
  sources: [],
}

module.exports = (server: ServerAPI): Plugin => {
  let orchestrator: Orchestrator | null = null
  let timer: ReturnType<typeof setInterval> | null = null
  let infra: PluginSettings = {}
  let settings: AppSettings = { ...DEFAULT_APP_SETTINGS }
  let legacyIntervalMinutes: number | undefined

  const DEFAULT_ROOT = '~/.signalk/gribs'
  const gribsRoot = () => expandHome(infra.gribsRoot || DEFAULT_ROOT)

  const settingsPath = () => path.join(server.getDataDirPath(), 'settings.json')

  const validInterval = (value: unknown): number | undefined => {
    const n = Number(value)
    return Number.isFinite(n) && n >= 1 ? Math.floor(n) : undefined
  }

  const intervalMinutes = () =>
    validInterval(infra.checkIntervalMinutes) ??
    legacyIntervalMinutes ??
    DEFAULT_CHECK_INTERVAL_MINUTES

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

  const updateStatus = () => {
    if (!orchestrator) return
    const parts = orchestrator.status().map(s => {
      if (s.running) return `${s.name}: downloading…`
      if (s.lastError) return `${s.name}: error`
      const mode = s.autoDownload ? 'auto' : 'manual'
      return `${s.name}: ${mode} ${s.upToDate ? '✓' : '…'} ${s.lastRun ?? 'no data'}`
    }).filter(Boolean)
    server.setPluginStatus(parts.join(' · ') || 'no sources (configure in the webapp)')
  }

  // (Re)build the orchestrator and timer from current infra + settings.
  const apply = (): void => {
    if (timer) { clearInterval(timer); timer = null }

    const sources = settings.sources ?? []
    orchestrator = new Orchestrator(
      sources,
      gribsRoot(),
      (msg: string) => server.debug(msg),
      updateStatus,
      settings.bbox
    )

    if (sources.some(s => s.autoDownload !== false)) {
      const interval = intervalMinutes() * 60_000
      orchestrator.tick().catch((err: unknown) => server.debug(`tick error: ${err}`))
      timer = setInterval(
        () => orchestrator?.tick().catch((err: unknown) => server.debug(`tick error: ${err}`)),
        interval
      )
    }
    updateStatus()
  }

  const plugin: Plugin = {
    id: PLUGIN_ID,
    name: 'GRIB Downloader',
    schema: () => buildSchema(DEFAULT_ROOT, intervalMinutes()),

    start: (options: PluginSettings & AppSettings) => {
      infra = {
        gribsRoot: options?.gribsRoot,
        checkIntervalMinutes: validInterval(options?.checkIntervalMinutes),
      }
      settings = loadSettings(options ?? {})
      apply()
    },

    stop: () => {
      if (timer) { clearInterval(timer); timer = null }
      orchestrator = null
    },

    registerWithRouter: (router: any) => {
      router.get('/status', (_req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        res.json({ checkIntervalMinutes: intervalMinutes(), sources: orchestrator.status() })
      })

      // Operational settings, webapp-managed. ('/config' would collide with
      // SignalK's built-in plugin config routes.)
      router.get('/settings', (_req: any, res: any) => {
        res.json({ ...settings, checkIntervalMinutes: intervalMinutes() })
      })

      router.put('/settings', (req: any, res: any) => {
        const nextRaw = req.body as AppSettings
        if (!nextRaw || typeof nextRaw !== 'object') {
          return res.status(400).json({ error: 'invalid settings' })
        }
        const next = normalizeSettings(nextRaw)
        const valid = ['gfs', 'arome', 'arpege', 'icon-eu']
        if ((next.sources ?? []).some(s => !valid.includes(s.model))) {
          return res.status(400).json({ error: `model must be one of: ${valid.join(', ')}` })
        }
        const names = (next.sources ?? []).map(s => sourceDirName(s))
        if (new Set(names).size !== names.length) {
          return res.status(400).json({ error: 'duplicate source: each (model, resolution) pair must be unique' })
        }
        if ((next.sources ?? []).some(s =>
            s.archiveRuns !== undefined && (!Number.isFinite(s.archiveRuns) || s.archiveRuns < 0))) {
          return res.status(400).json({ error: 'archiveRuns must be a number ≥ 0' })
        }
        try {
          saveSettings(next)
        } catch (err) {
          return res.status(500).json({ error: String(err) })
        }
        settings = next
        apply()
        res.json({ ok: true })
      })

      router.post('/download', (_req: any, res: any) => {
        if (!orchestrator) return res.status(503).json({ error: 'plugin not started' })
        const sources = settings.sources ?? []
        const stale = orchestrator.staleSources(sources)
        orchestrator.downloadAll().catch(err => server.debug(`download error: ${err}`))
        res.status(202).json({
          started: sources.map(sourceDirName),
          behind: stale.map(sourceDirName),
        })
      })

      // Delete a source's downloaded data (gribs, markers, archive). The
      // provider unregisters the source and purges its caches at next scan.
      router.delete('/source-data/:name', (req: any, res: any) => {
        const name = req.params.name
        if (!/^[A-Za-z0-9._-]+$/.test(name)) {
          return res.status(400).json({ error: 'invalid source name' })
        }
        const dir = path.join(gribsRoot(), name)
        if (!fs.existsSync(dir)) return res.json({ ok: true, existed: false })
        fs.rm(dir, { recursive: true, force: true }, (err) => {
          if (err) return res.status(500).json({ error: String(err) })
          server.debug(`deleted source data: ${dir}`)
          res.json({ ok: true, existed: true })
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
