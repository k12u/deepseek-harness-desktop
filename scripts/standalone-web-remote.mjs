/**
 * Standalone server-side supervisor for the DeepSeek Harness Web UI and the
 * DSH Remote translation proxy.
 *
 * Spawns `dsh web` on loopback, reads the startup token from its readiness
 * line, launches the LAN-remote proxy that both carries the Harness browser
 * cookie and translates the phone (Remote v1) wire contract onto Harness
 * 0.1.2, then supervises both processes: a Harness restart mints a new launch
 * token, so the proxy is restarted with it, while phone and browser
 * credentials survive independently (the phone trusts this proxy's bearer or
 * the tailnet; browsers keep their signed Harness cookie).
 *
 * Usage:
 *   node scripts/standalone-web-remote.mjs [--dsh-bin dsh] [--web-port 8080]
 *        [--remote-port 8766] [--bind loopback|lan] [--remote-token <64 hex>]
 *        [--harness-version <version>]
 *
 * Process tree:
 *   this launcher
 *   ├── dsh web --host 127.0.0.1 --port <web-port> --no-open
 *   └── lan-remote-proxy.mjs --target http://127.0.0.1:<web-port>
 *                             --token <remote-token> --port <remote-port>
 *                             --harness-token <launch token>
 */

import { spawn } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import process from 'node:process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const READY_LINE =
  /^dsh web:\s+(http:\/\/127\.0\.0\.1:(\d+))\/?(?:\?token=([A-Za-z0-9_-]{16,128}))?(?:\s|$)/
const HARNESS_STOP_GRACE_MS = 5_000
const PROXY_STOP_GRACE_MS = 3_000
const RESTART_MIN_MS = 1_000
const RESTART_MAX_MS = 15_000

function option(name, fallback = undefined) {
  const index = process.argv.indexOf(name)
  return index >= 0 && process.argv[index + 1] !== undefined
    ? process.argv[index + 1]
    : fallback
}

function fail(message) {
  process.stderr.write(`[dsh-web-remote] ${message}\n`)
  process.exit(1)
}

const log = message => process.stdout.write(`[dsh-web-remote] ${message}\n`)
const logError = message => process.stderr.write(`[dsh-web-remote] ${message}\n`)

const dshBin = option('--dsh-bin', 'dsh')
const webPort = Number(option('--web-port', '8080'))
const remotePort = Number(option('--remote-port', '8766'))
const bind = option('--bind', 'loopback')
const harnessVersion = option('--harness-version', 'unknown')
const explicitRemoteToken = option('--remote-token')
if (!Number.isInteger(webPort) || webPort < 1 || webPort > 65_535) fail('web-port must be 1-65535')
if (!Number.isInteger(remotePort) || remotePort < 1 || remotePort > 65_535) {
  fail('remote-port must be 1-65535')
}
if (bind !== 'loopback' && bind !== 'lan') fail('bind must be loopback or lan')
const remoteToken = /^[a-f0-9]{64}$/.test(explicitRemoteToken ?? '')
  ? explicitRemoteToken
  : randomBytes(32).toString('hex')

const proxyScript = join(dirname(fileURLToPath(import.meta.url)), 'lan-remote-proxy.mjs')

let harness
let proxy
let stopping = false
let harnessRestartTimer

function spawnChild(command, args, label, onExit) {
  const child = spawn(command, args, {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env,
  })
  child.on('error', error => logError(`${label} failed to start: ${error.message}`))
  child.once('exit', () => onExit(child))
  const pipe = source => {
    source.setEncoding('utf8')
    let pending = ''
    source.on('data', chunk => {
      pending += chunk
      const lines = pending.split(/\r?\n/)
      pending = lines.pop() ?? ''
      for (const line of lines) emitLine(label, line)
    })
    source.on('end', () => {
      if (pending.length > 0) emitLine(label, pending)
      pending = ''
    })
  }
  pipe(child.stdout)
  pipe(child.stderr)
  return child
}

let readinessEmitted = false
let onReady = () => {}

function emitLine(label, rawLine) {
  // The launch token must never rest in persistent logs; only the single
  // printed bootstrap URL carries it.
  const safe = rawLine.replace(/\?token=[A-Za-z0-9_-]+/g, '?token=[redacted]')
  if (label === 'web') process.stdout.write(`[harness] ${safe}\n`)
  else process.stdout.write(`[proxy] ${safe}\n`)
  if (label !== 'web' || readinessEmitted) return
  const match = READY_LINE.exec(rawLine)
  if (match === null) return
  readinessEmitted = true
  const onReadyCallback = onReady
  onReady = () => {}
  onReadyCallback([match[1], match[3]])
}

function startHarness() {
  return new Promise(resolveReady => {
    onReady = pair => resolveReady(pair)
    log('starting dsh web…')
    const args = dshBin.endsWith('.js')
      ? [process.execPath, '--expose-internals', dshBin]
      : [dshBin]
    harness = spawnChild(args[0], [
      ...args.slice(1),
      'web', '--host', '127.0.0.1', '--port', String(webPort), '--no-open',
    ], 'web', exited => {
      if (harness === exited) harness = undefined
    })
  })
}

function startProxy(harnessUrl, harnessToken) {
  log('starting remote proxy…')
  proxy = spawnChild(process.execPath, [
    proxyScript,
    '--target', harnessUrl,
    '--token', remoteToken,
    '--port', String(remotePort),
    ...(harnessToken === undefined ? [] : ['--harness-token', harnessToken]),
    '--harness-version', harnessVersion,
    '--bind', bind,
  ], 'proxy', exited => {
    if (proxy === exited) proxy = undefined
  })
}

function stopChild(child, graceMs, label) {
  return new Promise(resolve => {
    if (child === undefined || child.exitCode !== null || child.signalCode !== null) {
      resolve()
      return
    }
    const exited = new Promise(resolveExit => {
      child.once('exit', () => resolveExit())
    })
    child.kill('SIGTERM')
    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      resolve()
    }, graceMs)
    void exited.then(() => {
      clearTimeout(timer)
      resolve()
    })
    void label
  })
}

async function stopAll() {
  await stopChild(proxy, PROXY_STOP_GRACE_MS, 'proxy')
  proxy = undefined
  await stopChild(harness, HARNESS_STOP_GRACE_MS, 'web')
  harness = undefined
}

async function boot() {
  while (!stopping) {
    const [harnessUrl, harnessToken] = await startHarness()
    log('harness ready (token captured)' + (harnessToken === undefined ? ' (no token; older harness)' : ''))
    if (harnessToken !== undefined) {
      log('browser bootstrap URL (open once, the session cookie then persists):')
      log(`  ${harnessUrl}/?token=${harnessToken}`)
    }
    startProxy(harnessUrl, harnessToken)
    await new Promise(resolve => {
      const check = setInterval(() => {
        if (harness === undefined || proxy === undefined) {
          clearInterval(check)
          resolve()
        }
      }, 250)
    })
    if (stopping) break
    const delay = RESTART_MIN_MS + Math.random() * (RESTART_MAX_MS - RESTART_MIN_MS)
    logError(`a managed process exited; restarting in ${Math.round(delay)} ms`)
    await stopAll()
    await new Promise(resolve => { harnessRestartTimer = setTimeout(resolve, delay) })
    readinessEmitted = false
  }
}

boot().catch(error => {
  logError(`supervisor failed: ${error.message}`)
  void stopAll().then(() => process.exit(1))
})

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    if (stopping) return
    stopping = true
    if (harnessRestartTimer) clearTimeout(harnessRestartTimer)
    if (proxyRestartTimer) clearTimeout(proxyRestartTimer)
    log('shutting down…')
    void stopAll().then(() => process.exit(0))
  })
}
