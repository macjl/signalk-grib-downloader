// Signal K device access requests: only the administrator grants permissions.
import * as fs from 'node:fs'
import * as path from 'node:path'
import { randomUUID, createHash } from 'node:crypto'

type Permission = 'readonly' | 'readwrite'
type State = 'pending' | 'ready' | 'denied' | 'error'
interface Pending { endpoint: string; clientId: string; requestId?: string; denied?: boolean; tokenHash?: string }
interface Options {
  endpoint: string; tokenFile: string; dataDir: string; pluginId: string
  permissions: Permission; onChange?: () => void; onReady?: () => void
  pollMs?: number; retryMs?: number
}
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i

type Phase = 'request' | 'poll' | 'tokenCheck' | 'storage' | 'catalogue'
class AccessFailure extends Error {
  constructor(readonly phase: Phase, readonly code: string, readonly httpStatus?: number, readonly fsCode?: string) { super(code) }
}
function storageFailure(error: unknown, code: string): AccessFailure {
  const value = (error as NodeJS.ErrnoException).code
  return new AccessFailure('storage', code, undefined, value && /^[A-Z0-9_]{1,32}$/.test(value) ? value : undefined)
}

export class CatalogueAccess {
  private controller = new AbortController()
  private timer?: ReturnType<typeof setTimeout>
  private work?: Promise<void>
  private pending?: Pending
  private state: State = 'pending'
  private message = ''
  private stateFile: string
  private started = false
  private rejectedToken?: string
  private lastIssue?: { phase: Phase; code: string; httpStatus?: number; fsCode?: string }
  private counters = { requestAttempts: 0, approvalResponses: 0, tokenWrites: 0, tokenChecks: 0, renewals: 0 }

  constructor(private options: Options) {
    const url = new URL(options.endpoint)
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash ||
        (url.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))) throw new Error('Invalid catalogue access URL: remote servers require HTTPS')
    options.endpoint = url.href.replace(/\/$/, '')
    if (!path.isAbsolute(options.tokenFile)) throw new Error('Catalogue token file must be absolute')
    this.stateFile = path.join(options.dataDir, 'catalogue-access-' + createHash('sha256').update(options.tokenFile).digest('hex').slice(0, 16) + '.json')
    try { this.pending = JSON.parse(fs.readFileSync(this.stateFile, 'utf8')) } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error('Cannot read catalogue access request state')
    }
    if (this.pending && (typeof this.pending.clientId !== 'string' || !this.pending.clientId ||
        typeof this.pending.endpoint !== 'string' || (this.pending.requestId && !UUID.test(this.pending.requestId)))) throw new Error('Invalid catalogue access request state')
    if (this.pending?.endpoint !== options.endpoint && !fs.existsSync(options.tokenFile)) this.pending = undefined
    this.message = this.waitingMessage()
    // A deliberate plugin restart permits one new request after an explicit denial.
    if (this.pending?.denied) { delete this.pending.requestId; delete this.pending.denied }
    try {
      if (this.readToken()) { this.state = 'ready'; this.message = 'Catalogue access authorized' }
    } catch { /* start() reports unreadable files or a changed server safely. */ }
  }
  status() { return { state: this.state, permissions: this.options.permissions, message: this.message,
    diagnostics: { ...this.counters, hasPendingRequest: !!this.pending?.requestId, hasAutomaticToken: !!this.pending?.tokenHash, lastIssue: this.lastIssue } } }
  private waitingMessage() { return `Awaiting Signal K ${this.options.permissions} access: approve the request in Security > Access Requests on the catalogue server` }
  private change(state: State, message: string) {
    if (this.controller.signal.aborted) return
    const changed = this.state !== state || this.message !== message
    this.state = state; this.message = message
    if (changed) this.options.onChange?.()
  }
  private readToken(): string | undefined {
    if (this.pending && this.pending.endpoint !== this.options.endpoint) throw new Error('Catalogue server changed: configure a separate catalogueTokenFile for the new server')
    try { return fs.readFileSync(this.options.tokenFile, 'utf8').trim() || undefined } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw storageFailure(error, 'TOKEN_READ_FAILED')
      return undefined
    }
  }
  token(): string {
    const token = this.readToken()
    if (token) return token
    if (this.state === 'ready' && !this.controller.signal.aborted) {
      this.change('pending', this.waitingMessage())
      this.tick()
    }
    throw new Error(this.message || this.waitingMessage())
  }
  rejected(token: string) {
    // Only renew tokens issued to this plugin; never remove a manually
    // configured credential or a newer token after a concurrent 401.
    if (!token || this.controller.signal.aborted || this.readToken() !== token) return
    if (!this.pending?.tokenHash || this.pending.tokenHash !== createHash('sha256').update(token).digest('hex')) return
    // Signal K also uses 401 for insufficient route permissions (including an
    // absent/unregistered provider). Confirm authentication before discarding
    // a credential; another approval cannot fix an unavailable catalogue.
    this.rejectedToken = token
    this.lastIssue = { phase: 'catalogue', code: 'CATALOGUE_HTTP_401', httpStatus: 401 }
    this.tick()
  }
  start() {
    if (this.started) return
    this.started = true
    this.tick()
  }
  private atomic(file: string, value: string, code = 'STATE_WRITE_FAILED') {
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true })
      const tmp = file + '.' + randomUUID() + '.tmp'
      try {
        fs.writeFileSync(tmp, value, { mode: 0o600, flag: 'wx' })
        fs.renameSync(tmp, file)
      } finally { fs.rmSync(tmp, { force: true }) }
    } catch (error) { throw storageFailure(error, code) }
  }
  private save() { this.atomic(this.stateFile, JSON.stringify(this.pending) + '\n') }
  private async call(route: string, body?: object): Promise<any> {
    const phase = body ? 'request' : 'poll'
    if (body) this.counters.requestAttempts++
    try {
      const response = await fetch(this.options.endpoint + route, {
        method: body ? 'POST' : 'GET', redirect: 'error',
        signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(15_000)]),
        ...(body ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {}),
      })
      if (!response.ok) {
        // Signal K's request table is volatile and returns this exact response
        // after a server restart. Generic proxy/catalogue errors retain the ID.
        if (!body && response.status === 500 && (await response.text()).trim() === 'Unable to check request: not found') return { missing: true }
        await response.body?.cancel()
        throw new AccessFailure(phase, 'ACCESS_HTTP_ERROR', response.status)
      }
      return await response.json()
    } catch (error) {
      if (error instanceof AccessFailure) throw error
      throw new AccessFailure(phase, 'ACCESS_TRANSPORT_OR_RESPONSE_ERROR')
    }
  }
  private async checkRejectedToken(): Promise<void> {
    const token = this.rejectedToken!
    this.counters.tokenChecks++
    let response: Response
    let result: { status?: string; authenticationRequired?: boolean }
    try {
      response = await fetch(this.options.endpoint + '/loginStatus', {
        headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }, redirect: 'error',
        signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(15_000)]),
      })
      if (!response.ok) { await response.body?.cancel(); throw new AccessFailure('tokenCheck', 'TOKEN_CHECK_HTTP_ERROR', response.status) }
      result = await response.json() as typeof result
    } catch (error) {
      if (error instanceof AccessFailure) throw error
      throw new AccessFailure('tokenCheck', 'TOKEN_CHECK_TRANSPORT_OR_RESPONSE_ERROR')
    }
    this.controller.signal.throwIfAborted()
    // A rotation or manually configured credential wins over this in-flight check.
    if (this.readToken() !== token || this.pending?.tokenHash !== createHash('sha256').update(token).digest('hex')) {
      this.rejectedToken = undefined
      return
    }
    if (result?.status === 'loggedIn') {
      this.rejectedToken = undefined
      this.lastIssue = { phase: 'catalogue', code: 'CATALOGUE_DENIED_TOKEN_VALID', httpStatus: 401 }
      const resume = this.state !== 'ready'
      this.change('ready', 'Signal K token is valid, but catalogue access was refused: check Forecast Dataset Provider installation, enablement, URL and permissions')
      // Resume a scheduler paused by a failed validation, once. Ordinary 401s
      // with an already-ready credential must not trigger an immediate retry.
      if (resume) this.options.onReady?.()
      return
    }
    if (result?.status !== 'notLoggedIn' || result.authenticationRequired !== true) throw new AccessFailure('tokenCheck', 'TOKEN_CHECK_INCONCLUSIVE')
    try { fs.unlinkSync(this.options.tokenFile) } catch (error) { throw storageFailure(error, 'TOKEN_REMOVE_FAILED') }
    delete this.pending!.requestId; delete this.pending!.tokenHash; delete this.pending!.denied
    this.save()
    this.rejectedToken = undefined
    this.counters.renewals++
    this.change('pending', this.waitingMessage())
  }
  private async step() {
    if (this.rejectedToken) {
      await this.checkRejectedToken()
      // A valid token leaves the caller's catalogue error in place.
      if (this.state === 'ready') return
    }
    if (this.readToken()) {
      const firstReady = this.state !== 'ready'
      this.change('ready', 'Catalogue access authorized')
      if (firstReady) this.options.onReady?.()
      return
    }
    if (!this.pending) {
      this.pending = { endpoint: this.options.endpoint, clientId: this.options.pluginId + '-' + randomUUID() }
      this.save()
    }
    const response = this.pending.requestId
      ? await this.call('/signalk/v1/requests/' + this.pending.requestId)
      : await this.call('/signalk/v1/access/requests', {
        clientId: this.pending.clientId, description: this.options.pluginId + ' — forecast catalogue', permissions: this.options.permissions,
      })
    this.controller.signal.throwIfAborted()
    // Respect a credential supplied manually while the approval was in flight.
    if (this.readToken()) {
      this.change('ready', 'Catalogue access authorized')
      this.options.onReady?.()
      return
    }
    if (!response || typeof response !== 'object' || Array.isArray(response)) throw new AccessFailure('poll', 'INVALID_ACCESS_RESPONSE')
    if (response.missing) {
      delete this.pending.requestId; this.save()
      this.change('pending', this.waitingMessage())
      return
    }
    if (response.requestId) {
      if (typeof response.requestId !== 'string' || !UUID.test(response.requestId)) throw new AccessFailure('request', 'INVALID_REQUEST_ID')
      this.pending.requestId = response.requestId; this.save()
    }
    if (response.accessRequest?.permission === 'APPROVED') {
      this.counters.approvalResponses++
      const token = response.accessRequest.token
      if (typeof token !== 'string' || !token.trim() || /[\r\n]/.test(token)) throw new AccessFailure('poll', 'APPROVAL_MISSING_TOKEN')
      this.atomic(this.options.tokenFile, token.trim() + '\n', 'TOKEN_WRITE_FAILED')
      this.counters.tokenWrites++
      this.pending.tokenHash = createHash('sha256').update(token.trim()).digest('hex'); this.save()
      this.change('ready', 'Catalogue access authorized')
      this.options.onReady?.()
    } else if (response.accessRequest?.permission === 'DENIED' || response.state === 'COMPLETED') {
      this.pending.denied = true; this.save()
      this.change('denied', 'Signal K access denied: restart the plugin to submit a new request')
    } else if (response.state === 'PENDING' && this.pending.requestId) {
      this.change('pending', this.waitingMessage())
    } else throw new AccessFailure('poll', 'INVALID_ACCESS_RESPONSE')
  }
  private tick() {
    if (this.controller.signal.aborted) return
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    if (this.work) return
    this.work = this.step().catch((error: unknown) => {
      if (this.controller.signal.aborted) return
      const failure = error instanceof AccessFailure ? error : new AccessFailure('storage', 'ACCESS_STATE_ERROR')
      this.lastIssue = { phase: failure.phase, code: failure.code, httpStatus: failure.httpStatus, fsCode: failure.fsCode }
      // Never include server response bodies, tokens or request IDs in status.
      this.change('error', 'Catalogue access unavailable: check the server URL, Access Requests and token file; retrying')
    }).finally(() => {
      this.work = undefined
      if (!this.controller.signal.aborted && (this.rejectedToken || !['ready', 'denied'].includes(this.state))) {
        this.timer = setTimeout(() => this.tick(), this.state === 'error' ? (this.options.retryMs ?? 30_000) : (this.options.pollMs ?? 5_000))
        this.timer.unref()
      }
    })
  }
  async stop() {
    this.controller.abort()
    if (this.timer) clearTimeout(this.timer)
    await this.work
  }
}
