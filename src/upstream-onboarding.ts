import { randomUUID } from 'node:crypto'
import { request as httpRequest } from 'node:http'

const SETTINGS_NAMESPACE = 'ui-onboarding'
const ACKNOWLEDGEMENT_FIELD = 'welcomeNoticeVersion'
const SETTINGS_CALL_TIMEOUT_MS = 10_000

/** Welcome notice shipped by the pinned DeepSeek Harness release. */
export const UPSTREAM_WELCOME_NOTICE_VERSION = '2026-08-13.1'

interface WelcomeNoticeGateway {
  readVersion(): Promise<string | undefined>
  writeVersion(version: string): Promise<void>
}

/**
 * Newer Harness releases mint a browser-session cookie from the startup token
 * before accepting any API request. Exchange the token once like a browser
 * loading the authenticated root URL would, then carry the cookie onward.
 */
export function exchangeHarnessCookie(origin: string, token: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const upstream = httpRequest(
      `${origin}/?token=${encodeURIComponent(token)}`,
      { method: 'GET' },
      response => {
        response.resume()
        const setCookie = response.headers['set-cookie']?.find(value =>
          value.startsWith('dsh-auth-'),
        )
        const cookie = setCookie?.split(';')[0]?.trim()
        if (response.statusCode === 303 && cookie) resolve(cookie)
        else reject(new Error(`harness rejected its startup token (HTTP ${String(response.statusCode)})`))
      },
    )
    upstream.setTimeout(10_000, () => {
      upstream.destroy(new Error('harness token exchange timed out'))
    })
    upstream.once('error', reject)
    upstream.end()
  })
}

interface SettingsNamespaceView {
  ns: string
  value: unknown
}

interface SettingsEnvelopeResult {
  ok?: boolean
  value?: unknown
  error?: { message?: string }
}

interface SettingsEnvelope {
  type?: string
  rpcId?: string
  result?: SettingsEnvelopeResult
}

function namespaceViewFrom(value: unknown, method: string): SettingsNamespaceView {
  const view = value as SettingsNamespaceView | undefined
  if (
    typeof view !== 'object'
    || view === null
    || typeof view.ns !== 'string'
    || view.ns.length === 0
  ) {
    throw new Error(`Harness returned an unexpected settings namespace for ${method}`)
  }
  return view
}

function describeValueFrom(value: unknown, method: string): SettingsNamespaceView[] {
  const described = value as { namespaces?: unknown } | undefined
  if (typeof described !== 'object' || described === null || !Array.isArray(described.namespaces)) {
    throw new Error(`Harness returned an unexpected settings description for ${method}`)
  }
  return described.namespaces.map(view => namespaceViewFrom(view, method))
}

/** Post one RPC envelope to the loopback Harness and unwrap its result value. */
async function callSettings(
  origin: string,
  cookie: string | undefined,
  method: 'settings.describe' | 'settings.mutate',
  payload: unknown,
): Promise<unknown> {
  const rpcId = randomUUID()
  const response = await fetch(`${origin}/api/${method.replace('.', '/')}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(cookie === undefined ? {} : { cookie }),
    },
    body: JSON.stringify({ type: 'client-request', rpcId, method: method.replace('.', '/'), payload: { args: payload } }),
    signal: AbortSignal.timeout(SETTINGS_CALL_TIMEOUT_MS),
  })
  if (!response.ok) {
    throw new Error(`Harness settings ${method} failed: HTTP ${String(response.status)}`)
  }
  const message = await response.json() as SettingsEnvelope
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

class LoopbackSettingsGateway implements WelcomeNoticeGateway {
  constructor(
    private readonly origin: string,
    private readonly cookie?: string,
  ) {}

  async readVersion(): Promise<string | undefined> {
    const namespaces = describeValueFrom(
      await callSettings(this.origin, this.cookie, 'settings.describe', {}),
      'settings.describe',
    )
    const view = namespaces.find(candidate => candidate.ns === SETTINGS_NAMESPACE)
    if (view === undefined) throw new Error('Harness onboarding settings are unavailable')
    return versionFrom(view)
  }

  async writeVersion(version: string): Promise<void> {
    const view = namespaceViewFrom(
      await callSettings(this.origin, this.cookie, 'settings.mutate', {
        ns: SETTINGS_NAMESPACE,
        ops: [{ op: 'set', path: [ACKNOWLEDGEMENT_FIELD], value: version }],
      }),
      'settings.mutate',
    )
    if (versionFrom(view) !== version) {
      throw new Error('Harness did not retain the welcome notice acknowledgement')
    }
  }
}

function versionFrom(view: { value: unknown }): string | undefined {
  if (typeof view?.value !== 'object' || view.value === null) return undefined
  const version = (view.value as Record<string, unknown>)[ACKNOWLEDGEMENT_FIELD]
  return typeof version === 'string' ? version : undefined
}

/** Record the pinned upstream notice as acknowledged before the Web UI loads. */
export async function acknowledgeUpstreamWelcomeNotice(gateway: WelcomeNoticeGateway): Promise<boolean> {
  if (await gateway.readVersion() === UPSTREAM_WELCOME_NOTICE_VERSION) return false
  await gateway.writeVersion(UPSTREAM_WELCOME_NOTICE_VERSION)
  return true
}

/**
 * Suppress the non-functional upstream welcome notice through its settings API.
 * Accepts the supervisor's start URL, which carries the Harness startup token
 * when the bundled release requires it.
 */
export async function suppressUpstreamWelcomeNotice(startUrl: string): Promise<void> {
  const url = new URL(startUrl)
  const token = url.searchParams.get('token') ?? undefined
  const cookie = token === undefined ? undefined : await exchangeHarnessCookie(url.origin, token)
  await acknowledgeUpstreamWelcomeNotice(new LoopbackSettingsGateway(url.origin, cookie))
}
