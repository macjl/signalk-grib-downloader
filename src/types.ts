export type ModelId = 'gfs' | 'arome' | 'arpege' | 'icon-eu'

// The source directory name is derived: <model>[-<resolution>] (e.g.
// "gfs-0p25", "icon-eu") — one directory per (model, resolution) pair,
// created under gribsRoot. The provider discovers it by name.
export interface SourceSetting {
  model: ModelId
  resolution?: string   // gfs: 0p25|0p50|1p00 ; arome: 0025|001 ; arpege: 01|025
  hours?: number        // forecast duration to download (capped per model)
  enabled?: boolean     // default true
}

export interface Bbox {
  latMin: number
  lonMin: number
  latMax: number
  lonMax: number
}

export interface PluginSettings {
  mode?: 'auto' | 'manual'
  checkIntervalMinutes?: number
  gribsRoot?: string          // SignalK-visible path where source subdirs live
  downloaderImage?: string
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
  running: boolean
  lastError: string | null
  lastFinishedAt: string | null
  lastLog: string[]
}
