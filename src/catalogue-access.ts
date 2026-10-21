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

export class CatalogueAccess {
  private controller = new AbortController()
  private timer?: ReturnType<typeof setTimeout>
  private work?: Promise<void>
  private pending?: Pending
  private state: State = 'pending'
  private message = ''
  private stateFile: string
  private started = false

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
  status() { return { state: this.state, permissions: this.options.permissions, message: this.message } }
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
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error('Cannot read the catalogue token file')
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
    if (!this.pending?.tokenHash || this.pending.tokenHash !== createHash('sha256').update(token).digest('hex')) {
      this.change('error', 'Configured catalogue token rejected: replace it or remove the file and restart the plugin to request access')
      return
    }
    fs.unlinkSync(this.options.tokenFile)
    delete this.pending.requestId; delete this.pending.tokenHash; delete this.pending.denied
    this.save()
    this.change('pending', this.waitingMessage())
    this.tick()
  }
  start() {
    if (this.started) return
    this.started = true
    this.tick()
  }
  private atomic(file: string, value: string) {
    fs.mkdirSync(path.dirname(file), { recursive: true })
    const tmp = file + '.' + randomUUID() + '.tmp'
    try {
      fs.writeFileSync(tmp, value, { mode: 0o600, flag: 'wx' })
      fs.renameSync(tmp, file)
    } finally { fs.rmSync(tmp, { force: true }) }
  }
  private save() { this.atomic(this.stateFile, JSON.stringify(this.pending) + '\n') }
  private async call(route: string, body?: object): Promise<any> {
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
      throw new Error(`Signal K access request: HTTP ${response.status}`)
    }
    return response.json()
  }
  private async step() {
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
    if (response.missing) {
      delete this.pending.requestId; this.save()
      this.change('pending', this.waitingMessage())
      return
    }
    if (response.requestId) {
      if (typeof response.requestId !== 'string' || !UUID.test(response.requestId)) throw new Error('Invalid Signal K access request identity')
      this.pending.requestId = response.requestId; this.save()
    }
    if (response.accessRequest?.permission === 'APPROVED') {
      const token = response.accessRequest.token
      if (typeof token !== 'string' || !token.trim() || /[\r\n]/.test(token)) throw new Error('Signal K approved access without a valid token')
      this.atomic(this.options.tokenFile, token.trim() + '\n')
      this.pending.tokenHash = createHash('sha256').update(token.trim()).digest('hex'); this.save()
      this.change('ready', 'Catalogue access authorized')
      this.options.onReady?.()
    } else if (response.accessRequest?.permission === 'DENIED' || response.state === 'COMPLETED') {
      this.pending.denied = true; this.save()
      this.change('denied', 'Signal K access denied: restart the plugin to submit a new request')
    } else if (response.state === 'PENDING' && this.pending.requestId) {
      this.change('pending', this.waitingMessage())
    } else throw new Error('Invalid Signal K access response')
  }
  private tick() {
    if (this.controller.signal.aborted) return
    this.work = this.step().catch(() => {
      // Never include server response bodies, tokens or request IDs in status.
      this.change('error', 'Catalogue access unavailable: check the server URL, Access Requests and token file; retrying')
    }).then(() => {
      if (!this.controller.signal.aborted && !['ready', 'denied'].includes(this.state)) {
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
