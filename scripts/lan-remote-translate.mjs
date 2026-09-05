/**
 * Wire translation from the DSH Remote v1 phone contract to the DeepSeek
 * Harness 0.1.x typert surface.
 *
 * The phone contract (docs/REMOTE_PROTOCOL_V1.md) speaks dotted RPC paths with
 * flat payloads. Harness 0.1.2 renamed every path to `namespace/method`,
 * wrapped arguments in `{args}` / `{args:{request}}`, moved approvals and
 * questions onto the `$events` waterfall stream, and reshaped workspace,
 * history, and model responses. This module owns the pure mapping between the
 * two; the proxy owns the stateful bridges around it.
 */

/** Unique per-request client-minted prompt identity required by 0.1.2. */
export function mintRequestId() {
  return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}-${Math.random().toString(16).slice(2)}`
}

/**
 * Static v1 RPC routes. `wrap` maps the flat v1 payload to 0.1.2 args;
 * `passArgs` forwards the phone payload unchanged (it is already args form).
 */
export const RPC_ROUTES = {
  'session.list': {
    path: 'session/list',
    wrap: payload => ({ _request: { ...(payload.cursor === undefined ? {} : { cursor: payload.cursor }) } }),
  },
  'session.create': { path: 'session/create', wrap: payload => ({ request: { ...payload } }) },
  'session.attachment': { path: 'session/attachment', wrap: payload => ({ request: { ...payload } }) },
  'session.selectModel': { path: 'session/selectModel', wrap: payload => ({ request: { ...payload } }) },
  'session.prompt': {
    path: 'session/prompt',
    wrap: payload => ({ request: { ...payload, requestId: mintRequestId() } }),
  },
  'session.updateQueue': { path: 'session/updateQueue', wrap: payload => ({ request: { ...payload } }) },
  'session.cancel': { path: 'session/cancel', wrap: payload => ({ request: { ...payload } }) },
  'fileReferences/list': { path: 'fileReferences/list', passArgs: true },
  'sessionReferenceResolver/candidates': { path: 'sessionReferenceResolver/candidates', passArgs: true },
  'subagent.list': {
    path: 'subagents/list',
    wrap: payload => ({ parentSessionId: payload.parentSessionId }),
  },
  'subagent.prompt': {
    path: 'subagents/prompt',
    wrap: payload => ({
      request: {
        ...payload,
        requestId: mintRequestId(),
        mode: 'continuable',
      },
    }),
  },
  'subagent.interrupt': {
    path: 'subagents/interruptByParent',
    wrap: payload => ({
      childSessionId: payload.childSessionId,
      parentSessionId: payload.parentSessionId,
      mode: 'continuable',
    }),
  },
}

/** v1 methods served by proxy state rather than a direct upstream call. */
export const SYNTHETIC_METHODS = new Set([
  'host.describe',
  'workspace.list',
  'session.history',
  'subagent.history',
  'session.models',
  'respond',
])

/** Which handler serves one v1 method. */
export function routeV1Method(method) {
  if (SYNTHETIC_METHODS.has(method)) return { synthetic: method }
  const route = RPC_ROUTES[method]
  if (route === undefined) return undefined
  return { upstream: route }
}

/** Compose the 0.1.2 envelope for a static route. */
export function translateRequest(method, payload, route) {
  if (route.passArgs === true) {
    return { method: route.path, payload: payload && typeof payload === 'object' ? payload : {} }
  }
  return { method: route.path, payload: { args: route.wrap({ ...(payload ?? {}) }) } }
}

/** Map one 0.1.2 `session/modelCatalog` value onto the v1 `session.models` value. */
export function remapModelCatalog(catalog, sessionSelection) {
  if (typeof catalog !== 'object' || catalog === null) return undefined
  const selection = sessionSelection?.next ?? sessionSelection?.lastUsed ?? catalog.default
  return {
    current: selection,
    routable: Array.isArray(catalog.routableProviders) && catalog.routableProviders.length > 0,
    groups: catalog.groups ?? [],
    failures: catalog.failures ?? [],
  }
}

const CHUNK_ROW_KINDS = {
  'text-chunks': 'text-delta',
  'reasoning-chunks': 'reasoning-delta',
  'tool-call-chunks': 'tool-call-delta',
}

/**
 * Expand one packed `session/chunks` history record back into the individual
 * `assistant/chunk` events the v1 phone fold consumes.
 */
export function expandChunkRecord(record) {
  const event = record?.event ?? {}
  const kind = CHUNK_ROW_KINDS[event.type]
  const data = event.data ?? {}
  const texts = Array.isArray(data.texts) ? data.texts : undefined
  const args = Array.isArray(data.args) ? data.args : undefined
  const members = texts ?? args ?? []
  return members.map((value, offset) => ({
    type: 'event',
    event: {
      type: 'assistant/chunk',
      seq: event.seq0 + offset,
      time: event.time0 + (data.dt?.slice(0, offset) ?? []).reduce((sum, gap) => sum + gap, 0),
      data: {
        turn: data.turn,
        step: data.step,
        chunk: {
          type: kind,
          index: data.index,
          ...(texts === undefined ? {} : { text: value }),
          ...(args === undefined ? {} : { args: value }),
        },
      },
    },
  }))
}

/** Map one 0.1.2 follow/control record list onto v1 history entries. */
export function recordsToV1History(records, hasMore, projections, maxMessages) {
  const events = []
  for (const record of Array.isArray(records) ? records : []) {
    if (record?.type === 'event') events.push({ event: record.event })
    else if (record?.type === 'chunks') events.push(...expandChunkRecord(record))
  }
  const limit = Number.isInteger(maxMessages) && maxMessages > 0 ? maxMessages : events.length
  const windowed = events.slice(-limit)
  return {
    events: windowed,
    hasMore: hasMore === true || windowed.length < events.length,
    ...(projections === undefined ? {} : { projections }),
  }
}

/**
 * Map one 0.1.2 `session/control` frame onto v1 live payloads.
 * Returns an array of v1 `{type, sessionId, ...}` payloads (possibly empty).
 */
export function controlFrameToV1(frame) {
  if (typeof frame !== 'object' || frame === null) return []
  if (frame.type === 'projection') {
    return [{ type: 'session/projection', sessionId: frame.sessionId }]
  }
  if (frame.type === 'queue') {
    return [{
      type: 'session/queue',
      sessionId: frame.sessionId,
      items: (frame.items ?? []).map(item => ({
        id: item.id,
        placement: item.placement,
        ...(item.message === undefined ? {} : { message: item.message }),
      })),
    }]
  }
  return []
}

/**
 * Map one upstream `$events` item onto v1 live payloads and interaction state.
 * `interaction` describes the pending waterfall for the phone.
 */
export function upstreamEventToV1(item) {
  if (typeof item !== 'object' || item === null) return { frames: [] }
  if (item.type === 'waterfall') {
    if (item.event === 'approval/request') {
      const request = item.request ?? {}
      return {
        frames: [{
          type: 'approval/requested',
          sessionId: item.agentId,
          approvalId: item.eventId,
          ...(request.toolName === undefined ? {} : { toolName: request.toolName }),
          ...(request.reason === undefined ? {} : { reason: request.reason }),
        }],
        pending: {
          eventId: item.eventId,
          kind: 'approval',
          sessionId: item.agentId,
        },
      }
    }
    if (item.event === 'user-questions/request') {
      const request = item.request ?? {}
      return {
        frames: [{
          type: 'question/requested',
          sessionId: item.agentId,
          questions: request.questions ?? [],
        }],
        pending: {
          eventId: item.eventId,
          kind: 'question',
          sessionId: item.agentId,
        },
      }
    }
    return { frames: [] }
  }
  return { frames: [] }
}

/** v1 frame for a resolved interaction, as the phone's resolver expects. */
export function resolvedFrame(kind, sessionId, correlation) {
  if (kind === 'approval') {
    return { type: 'approval/resolved', sessionId, approvalId: correlation }
  }
  return { type: 'question/resolved', sessionId, questionRpcId: correlation }
}

/**
 * Map a v1 `/api/respond` body onto an upstream `$events/result` outcome.
 * The proxy correlates the pending interaction by the body's `rpcId` (frame
 * rpcId) or the approval id, both of which carry the upstream eventId.
 */
export function respondToUpstreamEventResult(body) {
  const result = body?.result
  if (typeof result !== 'object' || result === null) return undefined
  if (result.ok === true) {
    const value = result.value
    if (typeof value !== 'object' || value === null) return undefined
    if (typeof value.approvalId === 'string' && typeof value.outcome === 'string') {
      return { kind: 'approval', outcome: { kind: 'result', value: value.outcome } }
    }
    if (typeof value.answer === 'object' && value.answer !== null) {
      return { kind: 'question', outcome: { kind: 'result', value: value.answer } }
    }
    return undefined
  }
  return {
    kind: undefined,
    outcome: {
      kind: 'rejected',
      error: {
        code: result.error?.code ?? 'cancelled',
        message: result.error?.message ?? 'the phone dismissed this request',
        details: result.error?.details ?? {},
      },
    },
  }
}
