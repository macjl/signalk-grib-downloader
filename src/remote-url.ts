export function normalizeRemoteUrl(value: string, allowHttp = false): string {
  const url = new URL(value)
  if (!['https:', 'http:'].includes(url.protocol) ||
      url.username || url.password || url.search || url.hash)
    throw new Error('Remote Signal K URL must use HTTP or HTTPS, without credentials, query or fragment')
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)
  if (url.protocol === 'http:' && !loopback && allowHttp !== true)
    throw new Error('Remote Signal K requires HTTPS. Enable HTTP for this source only on a trusted network.')
  return url.href.replace(/\/$/, '')
}
