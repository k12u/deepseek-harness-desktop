import http from 'node:http'
import net from 'node:net'
import os from 'node:os'
import process from 'node:process'
import { randomUUID } from 'node:crypto'
import { timingSafeEqual } from 'node:crypto'
import { realpathSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { WebSocket, WebSocketServer } from 'ws'
import {
  controlFrameToV1,
  recordsToV1History,
  remapModelCatalog,
  respondToUpstreamEventResult,
  resolvedFrame,
  routeV1Method,
  translateRequest,
  upstreamEventToV1,
} from './lan-remote-translate.mjs'

const READINESS_MARK = 'dsh lan remote:'
const HARNESS_TOKEN_PATTERN = /^[A-Za-z0-9_-]{16,128}$/
const HARNESS_COOKIE_PATTERN = /^dsh-auth-[A-Za-z0-9_-]+=[^;\s]+$/
const ALLOWED_HTTP_PATHS = new Set([
  '/api/host.describe',
  '/api/workspace.list',
  '/api/session.list',
  '/api/session.create',
  '/api/session.history',
  '/api/session.attachment',
  '/api/session.models',
  '/api/session.selectModel',
  '/api/session.prompt',
  '/api/session.updateQueue',
  '/api/session.cancel',
  '/api/fileReferences/list',
  '/api/sessionReferenceResolver/candidates',
  '/api/subagent.list',
  '/api/subagent.history',
  '/api/subagent.prompt',
  '/api/subagent.interrupt',
  '/api/respond',
])
const ALLOWED_UPGRADE_PATH = '/api/events.mux'
const UPSTREAM_MUX_PATH = '/api/remote.mux'
const EVENTS_STREAM_ENDPOINT = '$events'
const WORKSPACE_STREAM_ENDPOINT = 'workspace/follow'
const CONTROL_STREAM_ENDPOINT = 'session/control'
const SESSION_STREAM_ENDPOINT = 'session/follow'
const MAX_MANAGED_SESSION_STREAMS = 32
const HISTORY_WAIT_MS = 8_000
const MUX_RECONNECT_MIN_MS = 500
const MUX_RECONNECT_MAX_MS = 5_000
export const MAX_BODY_BYTES = 136 * 1024 * 1024

export function inspectDeclaredBodyLength(value, maxBytes = MAX_BODY_BYTES) {
  if (value === undefined) return { status: 'ok', bytes: undefined }
  if (Array.isArray(value) || typeof value !== 'string') return { status: 'invalid' }
  const normalized = value.trim()
  if (!/^\d+$/.test(normalized)) return { status: 'invalid' }
  const bytes = Number(normalized)
  if (!Number.isSafeInteger(bytes) || bytes > maxBytes) return { status: 'too-large' }
  return { status: 'ok', bytes }
}

export function streamedBodyExceedsLimit(bytes, maxBytes = MAX_BODY_BYTES) {
  return bytes > maxBytes
}

class BodyTooLargeError extends Error {
  constructor() {
    super('request body is too large')
    this.code = 'BODY_TOO_LARGE'
  }
}

class ClientAbortedError extends Error {
  constructor() {
    super('client aborted the request body')
    this.code = 'CLIENT_ABORTED'
  }
}

function option(name) {
  const index = process.argv.indexOf(name)
  return index >= 0 ? process.argv[index + 1] : undefined
}

function fail(message) {
  process.stderr.write(`[dsh-lan-remote] ${message}\n`)
  process.exit(1)
}

function privateIPv4(address) {
  const octets = address.split('.').map(Number)
  return octets.length === 4
    && octets.every(value => Number.isInteger(value) && value >= 0 && value <= 255)
    && (octets[0] === 10
      || (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31)
      || (octets[0] === 192 && octets[1] === 168))
}

function preferredAddress() {
  const explicit = process.env.DSH_LAN_REMOTE_HOST?.trim()
  if (explicit && privateIPv4(explicit)) return explicit
  const candidates = []
  for (const entries of Object.values(os.networkInterfaces())) {
    for (const entry of entries || []) {
      if (entry.family === 'IPv4' && !entry.internal && privateIPv4(entry.address)) {
        candidates.push(entry.address)
      }
    }
  }
  candidates.sort((left, right) => {
    const leftPreferred = left.startsWith('192.168.') ? 0 : 1
    const rightPreferred = right.startsWith('192.168.') ? 0 : 1
    return leftPreferred - rightPreferred || left.localeCompare(right)
  })
  return candidates[0]
}

function authorized(request, token, requireToken = true) {
  const value = request.headers.authorization
  if (typeof value !== 'string' || value.length === 0) return !requireToken
  if (!value.startsWith('Bearer ')) return false
  const received = Buffer.from(value.slice(7), 'utf8')
  const expected = Buffer.from(token, 'utf8')
  return received.length === expected.length && timingSafeEqual(received, expected)
}

function reject(socketOrResponse, status, message, close = false) {
  if ('writeHead' in socketOrResponse) {
    if (socketOrResponse.headersSent || socketOrResponse.destroyed) return
    const headers = {
      'Cache-Control': 'no-store',
      'Content-Type': 'text/plain; charset=utf-8',
    }
    if (close) headers.Connection = 'close'
    socketOrResponse.writeHead(status, headers)
    socketOrResponse.end(message)
    return
  }
  socketOrResponse.end(
    `HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`,
  )
}

export function proxyHeaders(request, target, declaredBytes, cookie = '') {
  const headers = {
    accept: request.headers.accept || 'application/json',
    host: `${target.hostname}:${target.port}`,
  }
  if (request.headers['content-type']) headers['content-type'] = request.headers['content-type']
  if (request.headers['user-agent']) headers['user-agent'] = request.headers['user-agent']
  if (declaredBytes !== undefined) headers['content-length'] = String(declaredBytes)
  if (cookie) headers.cookie = cookie
  return headers
}

export function forwardRequestBody(request, upstream, maxBytes = MAX_BODY_BYTES) {
  return new Promise((resolve, rejectPromise) => {
    let size = 0
    let settled = false

    const cleanup = () => {
      request.off('data', onData)
      request.off('end', onEnd)
      request.off('aborted', onAborted)
      request.off('close', onRequestClose)
      request.off('error', onRequestError)
      upstream.off('drain', onDrain)
      upstream.off('error', onUpstreamError)
    }
    const fail = error => {
      if (settled) return
      settled = true
      cleanup()
      request.resume()
      rejectPromise(error)
    }
    const onDrain = () => {
      if (!settled) request.resume()
    }
    const onData = chunk => {
      request.pause()
      size += chunk.length
      if (streamedBodyExceedsLimit(size, maxBytes)) {
        fail(new BodyTooLargeError())
        return
      }
      try {
        if (upstream.write(chunk)) request.resume()
        else upstream.once('drain', onDrain)
      } catch (error) {
        fail(error)
      }
    }
    const onEnd = () => {
      if (settled) return
      settled = true
      cleanup()
      try {
        upstream.end()
        resolve(size)
      } catch (error) {
        rejectPromise(error)
      }
    }
    const onAborted = () => fail(new ClientAbortedError())
    const onRequestClose = () => {
      if (!request.complete) fail(new ClientAbortedError())
    }
    const onRequestError = error => fail(error)
    const onUpstreamError = error => fail(error)

    request.on('data', onData)
    request.on('end', onEnd)
    request.on('aborted', onAborted)
    request.on('close', onRequestClose)
    request.on('error', onRequestError)
    upstream.on('error', onUpstreamError)
    request.resume()
  })
}

function upgradeRequest(request, target, cookie = '') {
  const headers = [
    `GET ${ALLOWED_UPGRADE_PATH} HTTP/1.1`,
    `Host: ${target.hostname}:${target.port}`,
    'Connection: Upgrade',
    'Upgrade: websocket',
  ]
  for (const name of ['sec-websocket-key', 'sec-websocket-version', 'sec-websocket-protocol']) {
    const value = request.headers[name]
    if (typeof value === 'string' && value.length <= 512) headers.push(`${name}: ${value}`)
  }
  if (cookie) headers.push(`Cookie: ${cookie}`)
  return `${headers.join('\r\n')}\r\n\r\n`
}

function inspectHttpRequest(request, response, token, session) {
  const path = new URL(request.url || '/', 'http://remote.invalid').pathname
  if (!authorized(request, token, session.requireToken)) {
    reject(response, 401, 'Unauthorized', true)
    request.resume()
    return undefined
  }
  if (request.method !== 'POST' || !ALLOWED_HTTP_PATHS.has(path)) {
    reject(response, 404, 'Not Found', true)
    request.resume()
    return undefined
  }
  const declared = inspectDeclaredBodyLength(request.headers['content-length'])
  if (declared.status === 'invalid') {
    reject(response, 400, 'Bad Request', true)
    request.resume()
    return undefined
  }
  if (declared.status === 'too-large') {
    reject(response, 413, 'Payload Too Large', true)
    request.resume()
    return undefined
  }
  return { path, declaredBytes: declared.bytes }
}

async function proxyHttpRequest(request, response, target, token, session, sendContinue = false) {
  const inspected = inspectHttpRequest(request, response, token, session)
  if (!inspected) return
  if (sendContinue) response.writeContinue()

  let upstreamResponse
  let upstreamResponseError
  let proxyingUpstreamResponse = false
  let settleUpstream
  const upstreamOutcome = new Promise(resolve => {
    settleUpstream = resolve
  })
  let upstream
  try {
    upstream = http.request({
      hostname: target.hostname,
      port: Number(target.port),
      method: 'POST',
      path: inspected.path,
      headers: proxyHeaders(request, target, inspected.declaredBytes, session.cookie),
    }, value => {
      upstreamResponse = value
      const failUpstreamResponse = error => {
        if (upstreamResponseError) return
        upstreamResponseError = error
        if (!proxyingUpstreamResponse) return
        if (!response.headersSent) reject(response, 502, 'Bad Gateway', true)
        else response.destroy(error)
      }
      value.once('aborted', () => failUpstreamResponse(new Error('upstream response aborted')))
      value.once('error', failUpstreamResponse)
      settleUpstream({ response: value })
    })
    upstream.once('error', error => settleUpstream({ error }))
  } catch (error) {
    reject(response, 502, 'Bad Gateway', true)
    request.resume()
    return
  }

  const stopUpstreamWhenClientLeaves = () => {
    if (!response.writableEnded) {
      upstream.destroy()
      upstreamResponse?.destroy()
    }
  }
  response.once('close', stopUpstreamWhenClientLeaves)

  try {
    await forwardRequestBody(request, upstream)
    const outcome = await upstreamOutcome
    if (outcome.error) throw outcome.error
    if (upstreamResponseError) throw upstreamResponseError
    if (response.destroyed) {
      outcome.response.destroy()
      return
    }

    const headers = { ...outcome.response.headers, 'cache-control': 'no-store' }
    delete headers['set-cookie']
    proxyingUpstreamResponse = true
    response.writeHead(outcome.response.statusCode || 502, headers)
    outcome.response.pipe(response)
  } catch (error) {
    upstream.destroy(error instanceof Error ? error : undefined)
    upstreamResponse?.destroy()
    if (error?.code === 'CLIENT_ABORTED' || request.aborted || response.destroyed) return
    if (error?.code === 'BODY_TOO_LARGE') {
      reject(response, 413, 'Payload Too Large', true)
      return
    }
    reject(response, 502, 'Bad Gateway', true)
  }
}

/** One long-lived upstream Harness mux connection with reconnecting streams. */
class HarnessBridge {
  #target
  #cookie = ''
  #harnessVersion
  #log
  #socket
  #stopped = false
  #reconnectTimer
  #reconnectDelay = MUX_RECONNECT_MIN_MS
  #subscribers = new Set()
  #pendingByEventId = new Map()
  #pendingByRpcId = new Map()
  #clientId = ''
  #workspace = { items: [], archivedSessionIds: [] }
  #workspaceReady = false
  #workspaceWaiters = []
  #controlProjections = new Map()
  #controlSessions = new Set()
  #sessionStreams = new Map()

  constructor(target, harnessVersion, log = () => {}) {
    this.#target = target
    this.#harnessVersion = harnessVersion
    this.#log = log
  }

  /** Attach the exchanged Harness cookie before starting. */
  setCookie(cookie) {
    this.#cookie = cookie
  }

  get harnessVersion() {
    return this.#harnessVersion
  }

  /** Start the upstream mux and its managed streams. */
  start() {
    this.#connect()
  }

  stop() {
    this.#stopped = true
    if (this.#reconnectTimer) clearTimeout(this.#reconnectTimer)
    this.#socket?.close()
    for (const subscriber of this.#subscribers) {
      try { subscriber.close() } catch { /* phone socket already gone */ }
    }
    this.#subscribers.clear()
  }

  #connect() {
    if (this.#stopped) return
    const wsUrl = `${this.#target.protocol === 'https:' ? 'wss' : 'ws'}://${this.#target.host}${UPSTREAM_MUX_PATH}`
    const socket = new WebSocket(wsUrl, { headers: this.#cookie ? { cookie: this.#cookie } : {} })
    this.#socket = socket
    socket.on('open', () => {
      this.#reconnectDelay = MUX_RECONNECT_MIN_MS
      this.#sessionStreams.clear()
      this.#attach(EVENTS_STREAM_ENDPOINT, {})
      this.#attach(WORKSPACE_STREAM_ENDPOINT, {})
      this.#attach(CONTROL_STREAM_ENDPOINT, {})
      for (const key of this.#controlSessions) {
        this.#ensureSessionStream(key, { kind: 'session', sessionId: key })
      }
    })
    socket.on('message', data => {
      let message
      try { message = JSON.parse(data.toString()) } catch { return }
      this.#receive(message)
    })
    socket.on('error', error => {
      this.#log(`upstream mux error: ${error.message}`)
    })
    socket.on('close', () => {
      for (const waiter of this.#workspaceWaiters) waiter.reject(new Error('upstream mux closed'))
      this.#workspaceWaiters = []
      for (const entry of this.#sessionStreams.values()) {
        for (const waiter of entry.waiters.splice(0)) waiter.reject(new Error('upstream mux closed'))
      }
      if (!this.#stopped) {
        this.#reconnectTimer = setTimeout(() => this.#connect(), this.#reconnectDelay)
        this.#reconnectDelay = Math.min(this.#reconnectDelay * 2, MUX_RECONNECT_MAX_MS)
      }
    })
  }

  #attach(endpoint, args) {
    const streamId = randomUUID()
    this.#send({ type: 'open', streamId, endpoint, payload: { args } })
    return streamId
  }

  #send(message) {
    if (this.#socket?.readyState !== WebSocket.OPEN) return false
    this.#socket.send(JSON.stringify(message))
    return true
  }

  #receive(message) {
    if (message.type === 'item') {
      this.#receiveItem(message.streamId, message.value)
      return
    }
    if (message.type === 'cancel' && typeof message.eventId === 'string') {
      this.#resolvePending(message.eventId, 'upstream')
    }
  }

  #receiveItem(streamId, value) {
    if (typeof value !== 'object' || value === null) return
    if (value.type === 'ready' && typeof value.clientId === 'string') {
      this.#clientId = value.clientId
      return
    }
    if (value.type === 'baseline' && Array.isArray(value.value?.items) && value.value.archivedSessionIds !== undefined) {
      this.#workspace = value.value
      this.#workspaceReady = true
      for (const waiter of this.#workspaceWaiters.splice(0)) waiter.resolve()
      return
    }
    if (value.type === 'upsert' && value.workspace?.workspaceId !== undefined) {
      this.#workspace = {
        ...this.#workspace,
        items: [
          ...this.#workspace.items.filter(item => item.workspaceId !== value.workspace.workspaceId),
          value.workspace,
        ],
      }
      return
    }
    if (value.type === 'remove' && typeof value.workspaceId === 'string') {
      this.#workspace = {
        ...this.#workspace,
        items: this.#workspace.items.filter(item => item.workspaceId !== value.workspaceId),
      }
      return
    }
    if (value.type === 'order' && Array.isArray(value.workspaceIds)) {
      const byId = new Map(this.#workspace.items.map(item => [item.workspaceId, item]))
      this.#workspace = {
        ...this.#workspace,
        items: value.workspaceIds.map(id => byId.get(id)).filter(Boolean),
      }
      return
    }
    if (value.type === 'archived' && Array.isArray(value.archivedSessionIds)) {
      this.#workspace = { ...this.#workspace, archivedSessionIds: value.archivedSessionIds }
      return
    }
    if (value.type === 'baseline' && value.value?.queues !== undefined) {
      this.#controlSessions = new Set(Object.keys(value.value.queues ?? {}))
      for (const [sessionId, projections] of Object.entries(value.value.projections ?? {})) {
        this.#controlProjections.set(sessionId, projections?.values ?? {})
      }
      return
    }
    if (value.type === 'queue' && typeof value.sessionId === 'string') {
      this.#broadcastFrames(controlFrameToV1(value).map(payload => ({ rpcId: randomUUID(), payload })))
      return
    }
    if (value.type === 'projection' && typeof value.sessionId === 'string') {
      const values = this.#controlProjections.get(value.sessionId) ?? {}
      values[value.key] = value.value
      this.#controlProjections.set(value.sessionId, values)
      this.#broadcastFrames(controlFrameToV1(value).map(payload => ({ rpcId: randomUUID(), payload })))
      return
    }
    if (value.type === 'waterfall') {
      this.#receiveWaterfall(value)
      return
    }
    if (value.type === 'snapshot' || value.type === 'event') {
      this.#receiveSessionFrame(streamId, value)
    }
  }

  #receiveWaterfall(value) {
    const { frames, pending } = upstreamEventToV1(value)
    if (pending !== undefined) {
      this.#pendingByEventId.set(pending.eventId, pending)
      this.#pendingByRpcId.set(pending.eventId, pending)
    }
    this.#broadcastFrames(frames.map(payload => ({
      rpcId: pending?.eventId ?? randomUUID(),
      payload,
    })))
  }

  #resolvePending(eventId, source) {
    const pending = this.#pendingByEventId.get(eventId)
    this.#pendingByEventId.delete(eventId)
    for (const [rpcId, tracked] of this.#pendingByRpcId) {
      if (tracked.eventId === eventId) this.#pendingByRpcId.delete(rpcId)
    }
    if (pending === undefined) return
    this.#broadcastFrames([{
      rpcId: eventId,
      payload: resolvedFrame(pending.kind, pending.sessionId, eventId),
    }])
    this.#log(`interaction ${pending.kind} resolved (${source})`)
  }

  #receiveSessionFrame(streamId, frame) {
    const entry = this.#sessionStreams.get(streamId)
    if (entry === undefined) return
    if (frame.type === 'snapshot') {
      entry.snapshot = frame
      for (const waiter of entry.waiters.splice(0)) waiter.resolve()
      return
    }
    entry.events.push(frame)
    this.#broadcastFrames([{
      rpcId: randomUUID(),
      payload: { type: 'session/event', sessionId: entry.sessionId },
    }])
  }

  /**
   * Open a follow stream for one session address, keeping at most
   * MAX_MANAGED_SESSION_STREAMS of them alive (LRU).
   */
  #ensureSessionStream(sessionId, address) {
    const key = address?.kind === 'subagent'
      ? `subagent:${address.parentSessionId}/${address.childSessionId}/${address.mode}`
      : `session:${sessionId}`
    const existing = [...this.#sessionStreams.values()].find(candidate => candidate.key === key)
    if (existing !== undefined) {
      this.#sessionStreams.delete(existing.streamId)
      this.#sessionStreams.set(existing.streamId, existing)
      return existing
    }
    const streamId = this.#attach(SESSION_STREAM_ENDPOINT, { request: { address } })
    const entry = {
      key,
      streamId,
      sessionId,
      address,
      snapshot: undefined,
      events: [],
      waiters: [],
    }
    this.#sessionStreams.set(streamId, entry)
    while (this.#sessionStreams.size > MAX_MANAGED_SESSION_STREAMS) {
      const oldest = this.#sessionStreams.keys().next().value
      const evicted = this.#sessionStreams.get(oldest)
      this.#sessionStreams.delete(oldest)
      this.#send({ type: 'cancel', streamId: oldest })
      for (const waiter of evicted.waiters.splice(0)) {
        waiter.reject(new Error('history stream evicted'))
      }
    }
    return entry
  }

  /**
   * Wait for one session's snapshot (bounded), then serve the v1 history
   * response from the accumulated window.
   */
  async history(sessionId, address, maxMessages) {
    const entry = this.#ensureSessionStream(sessionId, address)
    if (entry.snapshot === undefined) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const index = entry.waiters.indexOf(waiter)
          if (index >= 0) entry.waiters.splice(index, 1)
          reject(new Error('history snapshot timed out'))
        }, HISTORY_WAIT_MS)
        const waiter = {
          resolve: () => {
            clearTimeout(timer)
            resolve()
          },
          reject: error => {
            clearTimeout(timer)
            reject(error)
          },
        }
        entry.waiters.push(waiter)
      })
    }
    const snapshot = entry.snapshot
    if (snapshot === undefined) throw new Error('history snapshot unavailable')
    return recordsToV1History(
      snapshot.records,
      snapshot.hasMore,
      snapshot.projections === undefined ? undefined : { values: snapshot.projections.values ?? {} },
      maxMessages,
    )
  }

  /** The current workspace snapshot in the v1 `workspace.list` shape. */
  async workspaceList() {
    if (!this.#workspaceReady) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const index = this.#workspaceWaiters.indexOf(waiter)
          if (index >= 0) this.#workspaceWaiters.splice(index, 1)
          reject(new Error('workspace baseline timed out'))
        }, HISTORY_WAIT_MS)
        const waiter = {
          resolve: () => {
            clearTimeout(timer)
            resolve()
          },
          reject: error => {
            clearTimeout(timer)
            reject(error)
          },
        }
        this.#workspaceWaiters.push(waiter)
      })
    }
    return this.#workspace
  }

  /** One host-wide live state snapshot for host.describe synthesis. */
  attachedSessionCount() {
    return this.#controlSessions.size
  }

  sessionSelection(sessionId) {
    const values = this.#controlProjections.get(sessionId)
    return values?.modelSelection
  }

  /** Relay a pending phone interaction answer to the upstream waterfall. */
  respond(body) {
    const mapping = respondToUpstreamEventResult(body)
    if (mapping === undefined) return Promise.resolve({ accepted: false, reason: 'unsupported response' })
    const pending = this.#pendingByRpcId.get(body.rpcId)
      ?? this.#pendingByEventId.get(body.result?.value?.approvalId ?? '')
    if (pending === undefined) {
      return Promise.resolve({ accepted: false, reason: 'unknown interaction' })
    }
    const eventId = pending.eventId
    return this.#upstreamPost('$events/result', { args: {
      clientId: this.#clientId,
      eventId,
      outcome: mapping.outcome,
    } }).then(() => {
      this.#resolvePending(eventId, 'phone')
      return { accepted: true, reason: '' }
    }, error => ({ accepted: false, reason: error.message }))
  }

  /** One unary upstream RPC; resolves with the raw upstream envelope. */
  call(method, payload) {
    return this.#upstreamPost(method, payload)
  }

  #upstreamPost(apiPath, payload) {
    return new Promise((resolve, reject) => {
      const rpcId = randomUUID()
      const body = JSON.stringify({
        type: 'client-request',
        rpcId,
        method: apiPath,
        payload,
      })
      const request = http.request(`${this.#target.origin}/api/${apiPath}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(body),
          ...(this.#cookie ? { cookie: this.#cookie } : {}),
        },
      }, response => {
        const chunks = []
        response.on('data', chunk => chunks.push(chunk))
        response.on('end', () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')))
          } catch (error) {
            reject(new Error(`upstream returned invalid JSON: ${error.message}`))
          }
        })
      })
      request.setTimeout(300_000, () => {
        request.destroy(new Error('upstream call timed out'))
      })
      request.once('error', reject)
      request.end(body)
    })
  }

  /** Register one connected phone mux for v1 frame broadcasts. */
  subscribe(send, close) {
    const subscriber = { send, close }
    this.#subscribers.add(subscriber)
    return () => this.#subscribers.delete(subscriber)
  }

  #broadcastFrames(frames) {
    if (frames.length === 0) return
    for (const subscriber of this.#subscribers) {
      for (const frame of frames) {
        try { subscriber.send(frame) } catch { /* drop */ }
      }
    }
  }
}

export function createLanRemoteServer(target, token, session = { cookie: '', requireToken: true }, bridge) {
  const server = http.createServer()
  server.on('request', (request, response) => {
    void (bridge
      ? proxyV1HttpRequest(request, response, target, token, session, bridge)
      : proxyHttpRequest(request, response, target, token, session))
  })
  server.on('checkContinue', (request, response) => {
    void (bridge
      ? proxyV1HttpRequest(request, response, target, token, session, bridge, true)
      : proxyHttpRequest(request, response, target, token, session, true))
  })

  server.on('upgrade', (request, socket, head) => {
    const path = new URL(request.url || '/', 'http://remote.invalid').pathname
    if (!authorized(request, token, session.requireToken)) return reject(socket, 401, 'Unauthorized')
    if (path !== ALLOWED_UPGRADE_PATH) return reject(socket, 404, 'Not Found')
    if (bridge === undefined) {
      const upstream = net.connect(Number(target.port), target.hostname, () => {
        upstream.write(upgradeRequest(request, target, session.cookie))
        if (head.length) upstream.write(head)
        socket.pipe(upstream).pipe(socket)
      })
      upstream.on('error', () => socket.destroy())
      socket.on('error', () => upstream.destroy())
      return
    }
    attachPhoneMux(request, socket, head, bridge)
  })

  server.on('clientError', (_error, socket) => reject(socket, 400, 'Bad Request'))
  server.headersTimeout = 10_000
  server.requestTimeout = 300_000
  server.keepAliveTimeout = 5_000
  server.maxRequestsPerSocket = 100
  return server
}

/** Terminate the phone's v1 events mux and feed it translated frames. */
function attachPhoneMux(request, socket, head, bridge) {
  const wss = new WebSocketServer({ noServer: true })
  wss.handleUpgrade(request, socket, head, phoneSocket => {
    const unsubscribe = bridge.subscribe(
      frame => {
        if (phoneSocket.readyState === WebSocket.OPEN) phoneSocket.send(JSON.stringify(frame))
      },
      () => phoneSocket.close(),
    )
    phoneSocket.on('close', unsubscribe)
    phoneSocket.on('error', unsubscribe)
  })
}

/** Read one v1 request body fully (bounded) and parse it as JSON. */
function readJsonBody(request, maxBytes = MAX_BODY_BYTES) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    let settled = false
    const done = (error, value) => {
      if (settled) return
      settled = true
      request.off('data', onData)
      request.off('end', onEnd)
      request.off('aborted', onAborted)
      request.off('error', onError)
      if (error !== undefined) {
        request.resume()
        reject(error)
      } else resolve(value)
    }
    const onData = chunk => {
      size += chunk.length
      if (streamedBodyExceedsLimit(size, maxBytes)) {
        done(new BodyTooLargeError())
        return
      }
      chunks.push(chunk)
    }
    const onEnd = () => {
      try { done(undefined, JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')) }
      catch { done(new Error('request body is not valid JSON')) }
    }
    const onAborted = () => done(new ClientAbortedError())
    const onError = error => done(error)
    request.on('data', onData)
    request.on('end', onEnd)
    request.on('aborted', onAborted)
    request.on('error', onError)
    request.resume()
  })
}

function v1Envelope(rpcId, result) {
  return { rpcId, result }
}

function v1Failure(rpcId, code, message) {
  return v1Envelope(rpcId, { ok: false, error: { code, message, details: {} } })
}

/** Serve one phone HTTP request through the translation bridge. */
async function proxyV1HttpRequest(request, response, target, token, session, bridge, sendContinue = false) {
  const inspected = inspectHttpRequest(request, response, token, session)
  if (!inspected) return
  if (sendContinue) response.writeContinue()
  const v1Method = inspected.path.slice('/api/'.length)

  let body
  try {
    body = await readJsonBody(request)
  } catch (error) {
    if (error?.code === 'BODY_TOO_LARGE') return reject(response, 413, 'Payload Too Large', true)
    if (error?.code === 'CLIENT_ABORTED' || request.aborted || response.destroyed) return
    return reject(response, 400, 'Bad Request', true)
  }
  if (response.destroyed) return

  const rpcId = typeof body.rpcId === 'string' ? body.rpcId : randomUUID()
  if (v1Method === 'respond') {
    const receipt = await bridge.respond(body)
    if (response.destroyed) return
    const receiptJson = JSON.stringify(receipt)
    response.writeHead(200, {
      'content-type': 'application/json',
      'content-length': Buffer.byteLength(receiptJson),
      'cache-control': 'no-store',
    })
    response.end(receiptJson)
    return
  }

  let result
  try {
    result = { ok: true, value: await serveV1Method(bridge, v1Method, body.payload) }
  } catch (error) {
    if (request.aborted || response.destroyed) return
    result = {
      ok: false,
      error: {
        code: error.code ?? 'remote/unavailable',
        message: error.message,
        details: error.details ?? {},
      },
    }
  }
  if (response.destroyed) return
  const responseJson = JSON.stringify(v1Envelope(rpcId, result))
  response.writeHead(200, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(responseJson),
    'cache-control': 'no-store',
  })
  response.end(responseJson)
}

async function serveV1Method(bridge, v1Method, payload) {
  const route = routeV1Method(v1Method)
  if (route === undefined) {
    const error = new Error(`unsupported remote method ${v1Method}`)
    error.code = 'remote/unsupported'
    throw error
  }
  if (route.synthetic === 'host.describe') {
    return { version: bridge.harnessVersion, attachedSessions: bridge.attachedSessionCount() }
  }
  if (route.synthetic === 'workspace.list') {
    return await bridge.workspaceList()
  }
  if (route.synthetic === 'session.history' || route.synthetic === 'subagent.history') {
    const sessionId = route.synthetic === 'session.history'
      ? payload.sessionId
      : payload.childSessionId
    const address = route.synthetic === 'session.history'
      ? { kind: 'session', sessionId }
      : {
          kind: 'subagent',
          parentSessionId: payload.parentSessionId,
          childSessionId: payload.childSessionId,
          mode: payload.mode ?? 'continuable',
        }
    return await bridge.history(sessionId, address, payload.maxMessages)
  }
  if (route.synthetic === 'session.models') {
    const catalog = unwrapUpstreamResult(await bridge.call('session/modelCatalog', { args: {} }))
    return remapModelCatalog(catalog, bridge.sessionSelection(payload.sessionId))
  }
  const translated = translateRequest(v1Method, payload, route.upstream)
  return unwrapUpstreamResult(await bridge.call(translated.method, translated.payload))
}

function unwrapUpstreamResult(envelope) {
  if (typeof envelope !== 'object' || envelope === null || typeof envelope.result !== 'object') {
    const error = new Error('harness returned an invalid envelope')
    error.code = 'remote/unavailable'
    throw error
  }
  if (envelope.result.ok !== true) {
    const error = new Error(envelope.result.error?.message ?? 'harness rejected the request')
    error.code = envelope.result.error?.code ?? 'remote/rejected'
    error.details = envelope.result.error?.details
    throw error
  }
  return envelope.result.value
}

/**
 * Exchange the Harness startup token for its browser-session cookie exactly
 * once, mirroring what a browser does when it opens the authenticated root
 * URL. Returns the `name=value` pair to attach to every forwarded request.
 */
export function exchangeHarnessCookie(target, harnessToken) {
  return new Promise((resolve, reject) => {
    const upstream = http.request({
      hostname: target.hostname,
      port: Number(target.port),
      method: 'GET',
      path: `/?token=${encodeURIComponent(harnessToken)}`,
      headers: { accept: 'text/html', host: `${target.hostname}:${target.port}` },
    }, response => {
      response.resume()
      const setCookie = response.headers['set-cookie']?.find(value => value.startsWith('dsh-auth-'))
      const cookie = setCookie?.split(';')[0]?.trim()
      if (response.statusCode === 303 && cookie && HARNESS_COOKIE_PATTERN.test(cookie)) {
        resolve(cookie)
      } else {
        reject(new Error(`harness rejected its startup token (HTTP ${String(response.statusCode)})`))
      }
    })
    upstream.once('timeout', () => {
      upstream.destroy(new Error('harness token exchange timed out'))
    })
    upstream.setTimeout(10_000)
    upstream.once('error', reject)
    upstream.end()
  })
}

function launchedDirectly() {
  if (!process.argv[1]) return false
  try {
    return realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))
  } catch {
    return false
  }
}

function main() {
  const targetValue = option('--target')
  const token = option('--token')
  const harnessToken = option('--harness-token')
  const harnessVersion = option('--harness-version') ?? 'unknown'
  const bind = option('--bind') ?? 'lan'
  const port = Number(option('--port'))
  if (!targetValue || !token || !Number.isInteger(port) || port < 1 || port > 65535) {
    fail('usage: --target http://127.0.0.1:<port> --token <secret> --port <port> [--harness-token <secret>] [--harness-version <version>] [--bind lan|loopback]')
  }
  if (!/^[a-f0-9]{64}$/.test(token)) fail('token must be 64 lowercase hexadecimal characters')
  if (bind !== 'lan' && bind !== 'loopback') fail('bind must be lan or loopback')
  if (harnessToken !== undefined && !HARNESS_TOKEN_PATTERN.test(harnessToken)) {
    fail('harness token must be 16-128 URL-safe base64 characters')
  }

  const target = new URL(targetValue)
  if (target.protocol !== 'http:' || target.hostname !== '127.0.0.1' || !target.port) {
    fail('target must be an explicit loopback HTTP endpoint')
  }

  const bindAddress = bind === 'loopback' ? '127.0.0.1' : preferredAddress()
  if (!bindAddress) fail('no private IPv4 address is available')
  // The Tailscale Serve path reaches this proxy over loopback only, inside the
  // user's own tailnet, so the LAN bearer credential stays optional there. The
  // direct LAN bind keeps requiring it, as before.
  const session = { cookie: '', requireToken: bind === 'lan' }
  const log = message => process.stderr.write(`[dsh-lan-remote] ${message}\n`)

  const bridge = new HarnessBridge(target, harnessVersion, log)
  let bootstrap = Promise.resolve()
  if (harnessToken !== undefined) {
    bootstrap = exchangeHarnessCookie(target, harnessToken).then(cookie => {
      session.cookie = cookie
      bridge.setCookie(cookie)
    })
  }
  bootstrap.catch(error => {
    fail(`unable to authenticate with Harness: ${error.message}`)
  })

  const server = createLanRemoteServer(target, token, session, bridge)
  const listen = address => new Promise((resolve, rejectPromise) => {
    const once = () => {
      server.off('error', onError)
      resolve()
    }
    const onError = error => {
      server.off('listening', once)
      rejectPromise(error)
    }
    server.once('listening', once)
    server.once('error', onError)
    server.listen(port, address)
  })

  void bootstrap
    .then(async () => {
      await listen(bindAddress)
      bridge.start()
      process.stdout.write(`${READINESS_MARK} http://${bindAddress}:${port}/\n`)
    })
    .catch(error => {
      if (error instanceof Error && error.message) fail(error.message)
      fail(`unable to bind ${bindAddress}:${port}`)
    })

  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => {
      bridge.stop()
      server.close(() => process.exit(0))
    })
  }
}

if (launchedDirectly()) main()
