// Read-only, redacted diagnostics for both catalogue clients. No security.json,
// token values, server addresses, filesystem paths or raw request IDs are output.
const fs = require('node:fs')
const path = require('node:path')
const os = require('node:os')
const { createHash } = require('node:crypto')

const code = error => /^[A-Z0-9_]{1,32}$/.test(error?.code ?? '') ? error.code : 'UNAVAILABLE'
const fingerprint = value => typeof value === 'string' && value ? createHash('sha256').update(value).digest('hex').slice(0, 12) : undefined
function read(file) { try { return JSON.parse(fs.readFileSync(file, 'utf8')) } catch { return undefined } }
function version(root, name) {
  const value = read(path.join(root, 'node_modules', name, 'package.json'))?.version
  return typeof value === 'string' && /^\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?$/.test(value) ? value : 'not found'
}
async function probe(endpoint, route, token) {
  try {
    const response = await fetch(endpoint + route, { headers: { Accept: 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) }, redirect: 'error', signal: AbortSignal.timeout(15000) })
    let data
    try { data = await response.json() } catch { /* no response text is logged */ }
    const result = { httpStatus: response.status }
    if (route === '/loginStatus') {
      if (['loggedIn', 'notLoggedIn'].includes(data?.status)) result.authentication = data.status
      if (['readonly', 'readwrite', 'admin'].includes(data?.userLevel)) result.permission = data.userLevel
      if (typeof data?.authenticationRequired === 'boolean') result.authenticationRequired = data.authenticationRequired
    } else if (typeof data?.contractVersion === 'string' && /^\d+\.\d+$/.test(data.contractVersion)) result.contractVersion = data.contractVersion
    return result
  } catch (error) { return { errorCode: code(error.cause ?? error) } }
}
async function main() {
  if (!process.argv[2]) throw Error('usage')
  const root = path.resolve(process.argv[2])
  const providerConfig = read(path.join(root, 'plugin-config-data', 'signalk-forecast-dataset-provider.json'))
  const result = { diagnosticNode: process.version, localProviderVersion: version(root, 'signalk-forecast-dataset-provider'),
    localProviderConfigReadable: !!providerConfig, localProviderEnabled: providerConfig?.enabled === true, plugins: [] }
  for (const name of ['signalk-grib-downloader', 'signalk-grib-weather-provider']) {
    const configDir = path.join(root, 'plugin-config-data'), dataDir = path.join(configDir, name)
    const config = read(path.join(configDir, name + '.json')), options = config?.configuration ?? {}
    const entry = { plugin: name, version: version(root, name), configReadable: !!config, enabled: config?.enabled === true }
    const supplied = options.catalogueTokenFile || path.join(dataDir, 'catalogue-token')
    const tokenFile = supplied.startsWith('~/') ? path.join(os.homedir(), supplied.slice(2)) : supplied
    if (!path.isAbsolute(tokenFile)) { entry.tokenFileError = 'NOT_ABSOLUTE'; result.plugins.push(entry); continue }
    let token
    try {
      const stat = fs.statSync(tokenFile)
      entry.tokenFileMode = (stat.mode & 0o777).toString(8)
      token = fs.readFileSync(tokenFile, 'utf8').trim()
      entry.tokenReadable = true; entry.tokenPresent = !!token
    } catch (error) { entry.tokenReadable = false; entry.tokenFileError = code(error) }
    const state = read(path.join(dataDir, 'catalogue-access-' + createHash('sha256').update(tokenFile).digest('hex').slice(0, 16) + '.json'))
    entry.savedRequest = { present: !!state, clientFingerprint: fingerprint(state?.clientId), requestFingerprint: fingerprint(state?.requestId), denied: state?.denied === true,
      automaticTokenMatches: !!token && state?.tokenHash === createHash('sha256').update(token).digest('hex') }
    try {
      const url = new URL(options.catalogueUrl || 'http://127.0.0.1:3000')
      const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)
      if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || (url.protocol === 'http:' && !loopback)) throw Error('url')
      const endpoint = url.href.replace(/\/$/, '')
      entry.catalogueIsLoopback = loopback
      entry.savedEndpointMatches = !state || state.endpoint === endpoint
      entry.login = await probe(endpoint, '/loginStatus', token)
      entry.catalogue = await probe(endpoint, '/plugins/signalk-forecast-dataset-provider/status', token)
    } catch { entry.catalogueUrlValid = false }
    result.plugins.push(entry)
  }
  process.stdout.write(JSON.stringify(result, null, 2) + '\n')
}
main().catch(() => { console.error('Diagnostic unavailable. Supply the Signal K configuration directory and run as its service user.'); process.exitCode = 1 })
