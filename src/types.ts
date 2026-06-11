export type ModelId = 'gfs' | 'arome' | 'arpege' | 'icon-eu'

// The source directory name is derived: <model>[-<resolution>] (e.g.
// "gfs-0p25", "icon-eu") — one directory per (model, resolution) pair,
// created under gribsRoot. The provider discovers it by name.
export interface SourceSetting {
  model: ModelId
  resolution?: string   // gfs: 0p25|0p50|1p00 ; arome: 0025|001 ; arpege: 01|025
  hours?: number        // forecast duration to download (capped per model)
  archiveRuns?: number  // past runs kept in <source>/archive/ (0 = none, no upper limit)
  enabled?: boolean     // default true
}

export interface Bbox {
  latMin: number
  lonMin: number
  latMax: number
  lonMax: number
}

// Infrastructure settings — managed in the SignalK plugin config panel.
export interface PluginSettings {
  gribsRoot?: string          // SignalK-visible path where source subdirs live
  downloaderImage?: string
}

// Operational settings — managed in the webapp, persisted by the plugin
// in its own data file (<dataDir>/settings.json), out of reach of the
// admin config form.
export interface AppSettings {
  mode?: 'auto' | 'manual'
  checkIntervalMinutes?: number
  bbox?: Bbox                 // applied to models with server-side subsetting (GFS)
  sources?: SourceSetting[]
}

export interface SourceStatus {
  name: string
  model: ModelId
  enabled: boolean
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
