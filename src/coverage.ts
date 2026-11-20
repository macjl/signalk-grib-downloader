import type { Bbox, SourceSetting } from './types'

export type Zone = [number, number, number, number]

export const mapZone = (bbox: Bbox): Zone => [bbox.lonMin, bbox.latMin, bbox.lonMax, bbox.latMax]

export function validZone(value: unknown): value is Zone {
  return Array.isArray(value) && value.length === 4 && value.every(Number.isFinite) &&
    value[0] >= -180 && value[0] <= 180 && value[2] >= -180 && value[2] <= 180 &&
    value[1] >= -90 && value[3] <= 90 && value[1] < value[3] && value[0] !== value[2]
}

// The requested map box is continuous in [-180, 180]. A source may wrap.
export function overlap(requested: Zone, coverage: Zone): Zone[] {
  const south = Math.max(requested[1], coverage[1]), north = Math.min(requested[3], coverage[3])
  if (south >= north) return []
  const [west, , east] = coverage
  const intervals = west < east ? [[west, east]] : [[-180, east], [west, 180]]
  return intervals.flatMap(([left, right]): Zone[] => {
    const w = Math.max(requested[0], left!), e = Math.min(requested[2], right!)
    return w < e ? [[w, south, e, north]] : []
  })
}

// Product boundaries published by Météo-France (EURW1S40, EURW1S100, EURAT01)
// and DWD (ICON-EU). The NOAA products and ARPEGE GLOB025 use world grids.
const regionalDomains: Record<string, Zone> = {
  'arome-0025': [-12, 37.5, 16, 55.4],
  'arome-001': [-12, 37.5, 16, 55.4],
  'arpege-01': [-32, 20, 42, 72],
  'icon-eu': [-23.5, 29.5, 62.5, 70.5]
}

const world: Zone = [-180, -90, 180, 90]
export const directDomains: Record<string, Zone> = {
  ...regionalDomains,
  gfs: world,
  'gfs-wave': world,
  'arpege-025': world
}

export function directCoverage(source: SourceSetting): Zone | undefined {
  if (source.model === 'arome') return regionalDomains['arome-' + (source.resolution || '0025')]
  if (source.model === 'arpege') return regionalDomains['arpege-' + (source.resolution || '025')]
  return regionalDomains[source.model]
}
