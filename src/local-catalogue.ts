import type { Readable } from 'node:stream'

// The Provider exposes this versioned interface inside the Signal K process.
export interface LocalCatalogue {
  contractVersion: string
  publications(): unknown
  publication(id: string): unknown
  begin(declaration: unknown): Promise<unknown>
  upload(id: string, asset: string, body: Readable, signal: AbortSignal): Promise<unknown>
  commit(id: string): Promise<unknown>
  abort(id: string): Promise<void>
  dataset(id: string, version?: string): unknown
  remove(id: string): Promise<void>
  readAsset(id: string, version: string, asset: string, signal: AbortSignal): Readable
}

const key = Symbol.for('macjl.forecast-datasets.catalogues.v1')
export const DEFAULT_CATALOGUE_URL = 'http://127.0.0.1:3000'
export const WAITING_FOR_LOCAL_CATALOGUE = 'Waiting for Forecast Dataset Provider with local catalogue support'

export function findLocalCatalogue(scope: string): LocalCatalogue | undefined {
  return (globalThis as typeof globalThis & { [key]?: Map<string, LocalCatalogue> })[key]?.get(scope)
}
