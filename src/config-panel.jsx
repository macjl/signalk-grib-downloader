import React, { useEffect, useState } from 'react'

const api = '/plugins/signalk-grib-downloader/remote-access'
const french = navigator.language.toLowerCase().startsWith('fr')
const t = french ? {
  remotes: 'Sources Signal K distantes',
  add: 'Ajouter un serveur',
  remove: 'Supprimer le serveur',
  server: 'Identifiant du serveur',
  url: 'URL du serveur Signal K distant',
  allowHttp: 'Autoriser HTTP sur un réseau de confiance',
  allowHttpHint: 'Pour un réseau privé de confiance comme Tailscale. Le jeton Signal K est transmis en HTTP.',
  optionalToken: 'Fichier d’un jeton existant (facultatif)',
  request: 'Demander un jeton en lecture/écriture',
  retry: 'Réessayer la demande',
  approve: 'Ouvrir les demandes d’accès du serveur distant',
  pending: 'En attente d’approbation sur le serveur distant.',
  ready: 'Jeton en lecture/écriture enregistré.',
  denied: 'Demande refusée sur le serveur distant.',
  failed: 'Demande en échec. Vérifiez l’URL et l’accès au serveur distant.',
  devices: 'Le serveur distant refuse la demande. Vérifiez que les demandes d’accès des appareils y sont autorisées.',
  saveFirst: 'Enregistrez et activez le plugin pour demander un jeton pour ce serveur.',
  requestFailed: 'La demande n’a pas pu être lancée. Vérifiez que ce serveur est enregistré et que le plugin est actif.',
  schedule: 'Téléchargements automatiques',
  interval: 'Intervalle de vérification (minutes)',
  slowdown: 'Ralentissement sur connexion mesurée (×)',
  export: 'Exporter les fichiers GRIB pour les applications utilisant des fichiers locaux',
  directory: 'Répertoire d’export GRIB',
  save: 'Enregistrer la configuration',
} : {
  remotes: 'Remote Signal K sources',
  add: 'Add a server',
  remove: 'Remove server',
  server: 'Server identifier',
  url: 'Remote Signal K server URL',
  allowHttp: 'Allow HTTP on a trusted network',
  allowHttpHint: 'For a trusted private network such as Tailscale. The Signal K token is sent over HTTP.',
  optionalToken: 'Existing token file (optional)',
  request: 'Request a read/write token',
  retry: 'Retry request',
  approve: 'Open access requests on the remote server',
  pending: 'Waiting for approval on the remote server.',
  ready: 'Read/write token saved.',
  denied: 'The remote server denied the request.',
  failed: 'The request failed. Check the remote server URL and connectivity.',
  devices: 'The remote server refused the request. Check that device access requests are enabled there.',
  saveFirst: 'Save the configuration and enable the plugin to request a token for this server.',
  requestFailed: 'Could not start the request. Check that this server is saved and the plugin is enabled.',
  schedule: 'Automatic downloads',
  interval: 'Publication probe interval (minutes)',
  slowdown: 'Metered connection slowdown (×)',
  export: 'Export GRIB files for applications that use local files',
  directory: 'GRIB export directory',
  save: 'Save configuration',
}

function Field({ label, hint, value, onChange, ...attrs }) {
  return <label className="d-block mb-3">
    <span>{label}</span>
    <input className="form-control" value={value ?? ''} onChange={onChange} {...attrs} />
    {hint && <small className="form-text">{hint}</small>}
  </label>
}

function endpointOf(url) {
  try { return new URL(url).href.replace(/\/$/, '') }
  catch { return undefined }
}

function RemoteToken({ access, refresh }) {
  const [busy, setBusy] = useState(false)
  const [failed, setFailed] = useState(false)
  const state = access?.state
  const error = [401, 403, 404].includes(access?.httpStatus) ? t.devices : t.failed
  const message = failed ? t.requestFailed : !access ? t.saveFirst : {
    pending: t.pending, ready: t.ready, denied: t.denied, error,
  }[state]

  async function request() {
    setBusy(true)
    setFailed(false)
    try {
      const response = await fetch(`${api}/${encodeURIComponent(access.id)}`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ endpoint: access.endpoint }),
      })
      if (!response.ok) throw new Error()
      await refresh()
    } catch {
      setFailed(true)
    } finally {
      setBusy(false)
    }
  }

  return <div>
    <button className="btn btn-outline-primary btn-sm" type="button" onClick={request}
      disabled={!access || busy || state === 'pending' || state === 'ready'}>
      {state === 'error' || state === 'denied' ? t.retry : t.request}
    </button>
    {message && <div className="form-text" role="status">{message}</div>}
    {state === 'pending' && <a className="d-block" href={access.endpoint + '/admin/#/security/access/requests'}
      target="_blank" rel="noopener noreferrer">{t.approve}</a>}
  </div>
}

export default function PluginConfigurationPanel({ configuration, save }) {
  const [config, setConfig] = useState(() => {
    const current = { ...configuration }
    delete current.catalogueUrl
    delete current.catalogueTokenFile
    return current
  })
  const [accesses, setAccesses] = useState([])
  const peers = config.remoteServers ?? []
  const hasPeers = peers.length > 0
  const exportEnabled = config.legacyExportEnabled !== false

  async function refresh(signal) {
    try {
      const response = await fetch(api, { signal, cache: 'no-store', credentials: 'same-origin' })
      const body = response.ok ? await response.json() : []
      if (!signal?.aborted) setAccesses(Array.isArray(body) ? body : [])
    } catch {
      if (!signal?.aborted) setAccesses([])
    }
  }

  useEffect(() => {
    if (!hasPeers) return
    const controller = new AbortController()
    const poll = () => refresh(controller.signal)
    poll()
    const interval = setInterval(poll, 3000)
    return () => { clearInterval(interval); controller.abort() }
  }, [hasPeers])

  function set(key, value) {
    setConfig(current => ({ ...current, [key]: value }))
  }

  function setPeer(index, key, value) {
    set('remoteServers', peers.map((peer, i) => i === index ? { ...peer, [key]: value } : peer))
  }

  return <form onSubmit={event => { event.preventDefault(); save(config) }}>
    <fieldset className="mb-4">
      <legend className="h6">{t.remotes}</legend>
      {peers.map((peer, index) => {
        const endpoint = endpointOf(peer.url)
        const access = accesses.find(entry => entry.id === peer.id && entry.endpoint === endpoint)
        return <div className="border rounded p-3 mb-3" key={index}>
          <Field label={t.server} value={peer.id} required pattern="[a-z0-9][a-z0-9\-]{0,31}" placeholder="shore"
            onChange={event => setPeer(index, 'id', event.target.value)} />
          <Field label={t.url} type="url" value={peer.url} required placeholder="https://signalk.example.org"
            onChange={event => setPeer(index, 'url', event.target.value)} />
          <label className="d-block mb-3">
            <input type="checkbox" className="me-2" checked={peer.allowHttp === true}
              onChange={event => setPeer(index, 'allowHttp', event.target.checked)} />
            {t.allowHttp}
            <small className="form-text d-block">{t.allowHttpHint}</small>
          </label>
          <Field label={t.optionalToken} value={peer.tokenFile}
            onChange={event => setPeer(index, 'tokenFile', event.target.value)} />
          <RemoteToken access={access} refresh={refresh} key={peer.id + ':' + endpoint} />
          <button type="button" className="btn btn-outline-danger btn-sm mt-2"
            onClick={() => set('remoteServers', peers.filter((_, i) => i !== index))}>{t.remove}</button>
        </div>
      })}
      <button type="button" className="btn btn-outline-primary btn-sm"
        onClick={() => set('remoteServers', [...peers, { id: '', url: '' }])}>{t.add}</button>
    </fieldset>
    <fieldset className="mb-4">
      <legend className="h6">{t.schedule}</legend>
      <Field label={t.interval} type="number" min="1" value={config.checkIntervalMinutes}
        onChange={event => set('checkIntervalMinutes', event.target.value === '' ? undefined : event.target.valueAsNumber)} />
      <Field label={t.slowdown} type="number" min="1" step="any" placeholder="3" value={config.meteredIntervalMultiplier}
        onChange={event => set('meteredIntervalMultiplier', event.target.value === '' ? undefined : event.target.valueAsNumber)} />
    </fieldset>
    <label className="d-block mb-3">
      <input type="checkbox" className="me-2" checked={exportEnabled}
        onChange={event => set('legacyExportEnabled', event.target.checked)} />
      {t.export}
    </label>
    {exportEnabled && <Field label={t.directory} value={config.gribsRoot} placeholder="~/.signalk/gribs"
      onChange={event => set('gribsRoot', event.target.value)} />}
    <button type="submit" className="btn btn-primary">{t.save}</button>
  </form>
}
