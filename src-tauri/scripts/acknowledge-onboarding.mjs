import http from 'node:http'
import { randomUUID } from 'node:crypto'

export const UPSTREAM_WELCOME_NOTICE_VERSION = '2026-08-13.1'
const SETTINGS_NAMESPACE = 'ui-onboarding'
const ACKNOWLEDGEMENT_FIELD = 'welcomeNoticeVersion'
const CALL_TIMEOUT_MS = 10_000

const [launchUrl] = process.argv.slice(2)
if (launchUrl === undefined) {
  throw new Error('usage: acknowledge-onboarding.mjs LAUNCH_URL')
}

const launch = new URL(launchUrl)
const token = launch.searchParams.get('token') ?? undefined
const origin = launch.origin

// Newer Harness releases require the browser-session cookie minted from the
// startup token before any API request is accepted. Exchange once, then carry
// the cookie on every request, mirroring what the browser does.
function exchangeHarnessCookie(targetOrigin, harnessToken) {
  return new Promise((resolve, reject) => {
    const request = http.request(
      `${targetOrigin}/?token=${encodeURIComponent(harnessToken)}`,
      { method: 'GET' },
      response => {
        response.resume()
        const setCookie = (response.headers['set-cookie'] ?? []).find(value =>
          value.startsWith('dsh-auth-'),
        )
        const cookie = setCookie?.split(';')[0]?.trim()
        if (response.statusCode === 303 && cookie) resolve(cookie)
        else reject(new Error(`harness rejected its startup token (HTTP ${response.statusCode})`))
      },
    )
    request.setTimeout(CALL_TIMEOUT_MS, () => {
      request.destroy(new Error('harness token exchange timed out'))
    })
    request.once('error', reject)
    request.end()
  })
}

const cookieHeader = token === undefined ? undefined : await exchangeHarnessCookie(origin, token)

async function callSettings(method, payload) {
  const rpcId = randomUUID()
  const response = await fetch(`${origin}/api/${method.replace('.', '/')}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(cookieHeader === undefined ? {} : { cookie: cookieHeader }),
    },
    body: JSON.stringify({ type: 'client-request', rpcId, method: method.replace('.', '/'), payload: { args: payload } }),
    signal: AbortSignal.timeout(CALL_TIMEOUT_MS),
  })
  if (!response.ok) {
    throw new Error(`Harness settings ${method} failed: HTTP ${response.status}`)
  }
  const message = await response.json()
  if (message.type !== 'server-response' || message.rpcId !== rpcId) {
    throw new Error(`Harness returned an unrecognised settings response for ${method}`)
  }
  if (message.result?.ok !== true) {
    throw new Error(
      `Harness settings ${method} failed: ${message.result?.error?.message ?? 'unknown error'}`,
    )
  }
  return message.result.value
}

function namespaceViewFrom(value, method) {
  if (typeof value !== 'object' || value === null || typeof value.ns !== 'string' || value.ns.length === 0) {
    throw new Error(`Harness returned an unexpected settings namespace for ${method}`)
  }
  return value
}

function versionFrom(view) {
  const value = view?.value
  if (typeof value !== 'object' || value === null) return undefined
  const version = value[ACKNOWLEDGEMENT_FIELD]
  return typeof version === 'string' ? version : undefined
}

const described = await callSettings('settings.describe', {})
if (!Array.isArray(described?.namespaces)) {
  throw new Error('Harness returned an unexpected settings description')
}
const view = described.namespaces
  .map(candidate => namespaceViewFrom(candidate, 'settings.describe'))
  .find(candidate => candidate.ns === SETTINGS_NAMESPACE)
if (view === undefined) throw new Error('Harness onboarding settings are unavailable')

if (versionFrom(view) !== UPSTREAM_WELCOME_NOTICE_VERSION) {
  const mutated = namespaceViewFrom(
    await callSettings('settings.mutate', {
      ns: SETTINGS_NAMESPACE,
      ops: [{
        op: 'set',
        path: [ACKNOWLEDGEMENT_FIELD],
        value: UPSTREAM_WELCOME_NOTICE_VERSION,
      }],
    }),
    'settings.mutate',
  )
  if (versionFrom(mutated) !== UPSTREAM_WELCOME_NOTICE_VERSION) {
    throw new Error('Harness did not retain the welcome notice acknowledgement')
  }
}
