import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import http, { type Server } from 'node:http'
import { type AddressInfo } from 'node:net'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import test from 'node:test'
import {
  acknowledgeUpstreamWelcomeNotice,
  exchangeHarnessCookie,
  UPSTREAM_WELCOME_NOTICE_VERSION,
} from '../src/upstream-onboarding.js'

class MemoryGateway {
  writes: string[] = []

  constructor(private version: string | undefined) {}

  async readVersion(): Promise<string | undefined> {
    return this.version
  }

  async writeVersion(version: string): Promise<void> {
    this.version = version
    this.writes.push(version)
  }
}

test('acknowledges the current upstream welcome notice once', async () => {
  const gateway = new MemoryGateway(undefined)

  assert.equal(await acknowledgeUpstreamWelcomeNotice(gateway), true)
  assert.deepEqual(gateway.writes, [UPSTREAM_WELCOME_NOTICE_VERSION])
  assert.equal(await acknowledgeUpstreamWelcomeNotice(gateway), false)
  assert.deepEqual(gateway.writes, [UPSTREAM_WELCOME_NOTICE_VERSION])
})

test('replaces an acknowledgement for an older upstream notice', async () => {
  const gateway = new MemoryGateway('older-notice')

  assert.equal(await acknowledgeUpstreamWelcomeNotice(gateway), true)
  assert.deepEqual(gateway.writes, [UPSTREAM_WELCOME_NOTICE_VERSION])
})

test('pins the acknowledgement to the bundled upstream declaration', async () => {
  const require = createRequire(import.meta.url)
  const manifest = require.resolve('@deepseek-ai/dsh-client-ui-settings-models/package.json')
  const declaration = await readFile(join(dirname(manifest), 'lib', 'types', 'onboarding-copy.d.ts'), 'utf8')
  const match = /WELCOME_NOTICE_VERSION = "([^"]+)"/.exec(declaration)

  assert.equal(match?.[1], UPSTREAM_WELCOME_NOTICE_VERSION)

  const tauriHelper = await readFile(
    join(process.cwd(), 'src-tauri', 'scripts', 'acknowledge-onboarding.mjs'),
    'utf8',
  )
  const tauriMatch = /UPSTREAM_WELCOME_NOTICE_VERSION = '([^']+)'/.exec(tauriHelper)
  assert.equal(tauriMatch?.[1], UPSTREAM_WELCOME_NOTICE_VERSION)
})

test('exchanges the harness startup token for its browser-session cookie', async (t) => {
  const token = 'k'.repeat(43)
  const goodCookie = 'dsh-auth-aCrWA3UKIm2=v1.eyJ2ZXJzaW9uIjoxfQ.sig'
  const seenUrls: string[] = []
  const server: Server = http.createServer((request, response) => {
    seenUrls.push(request.url ?? '')
    const url = new URL(request.url ?? '/', 'http://dsh.invalid')
    if (url.searchParams.get('token') === token) {
      response.writeHead(303, {
        location: '/',
        'set-cookie': [`${goodCookie}; Max-Age=2592000; Path=/; HttpOnly; SameSite=Strict`],
      })
      response.end()
    } else {
      response.writeHead(401)
      response.end()
    }
  })
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())))
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`

  assert.equal(await exchangeHarnessCookie(origin, token), goodCookie)
  assert.deepEqual(seenUrls, [`/?token=${encodeURIComponent(token)}`])
  await assert.rejects(exchangeHarnessCookie(origin, 'w'.repeat(43)), /rejected its startup token/)
})
