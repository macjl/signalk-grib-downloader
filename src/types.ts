export type ModelId = 'gfs' | 'arome' | 'arpege' | 'icon-eu'

// The source directory name is derived: <model>[-<resolution>] (e.g.
// "gfs-0p25", "icon-eu") — one directory per (model, resolution) pair,
// created under gribsRoot. The provider discovers it by name.
export interface SourceSetting {
  model: ModelId
  resolution?: string   // gfs: 0p25|0p50|1p00 ; arome: 0025|001 ; arpege: 01|025
  hours?: number        // forecast duration to download (capped per model)
  archiveRuns?: number  // past runs kept in <source>/archive/ (0 = none, no upper limit)
  autoDownload?: boolean // default true: include this source in automatic checks
  enabled?: boolean      // legacy: false migrates to autoDownload false
}

export interface Bbox {
  latMin: number
  lonMin: number
  latMax: number
  lonMax: number
}

// Connectivity, as published on `network.internet.state` by whatever
// plugin tracks the uplink (e.g. @meri-imperiumi/signalk-internet).
// 'captive' (behind a captive portal: nominal connectivity, but nothing
// but the portal's login page is reachable) pauses the auto scheduler
// exactly like 'offline'. 'unknown' (path absent / no publisher
// installed) behaves exactly as 'online'.
export type InternetState = 'online' | 'offline' | 'metered' | 'captive' | 'unknown'

// Infrastructure settings — managed in the SignalK plugin config panel.
export interface PluginSettings {
  gribsRoot?: string                 // local path where source subdirs live
  checkIntervalMinutes?: number      // fallback/retry gap for the auto scheduler
  meteredIntervalMultiplier?: number // auto waits stretch by this factor when metered
}

// Operational settings — managed in the webapp, persisted by the plugin
// in its own data file (<dataDir>/settings.json), out of reach of the
// admin config form.
export interface AppSettings {
  mode?: 'auto' | 'manual'       // legacy global mode
  checkIntervalMinutes?: number  // legacy: now managed in PluginSettings
  bbox?: Bbox                 // applied to models with server-side subsetting (GFS)
  sources?: SourceSetting[]
}

export interface SourceStatus {
  name: string
  model: ModelId
  autoDownload: boolean
  lastRun: string | null      // stamp of last completed run (from marker files)
  expectedRun: string         // stamp of the run that should be available now
  upToDate: boolean
  configStale: boolean        // data on disk was fetched with different parameters (area, duration, …)
  running: boolean
  lastError: string | null
  lastOutcome: 'downloaded' | 'up-to-date' | 'unavailable' | 'failed' | null
  lastFinishedAt: string | null
  lastLog: string[]
}
