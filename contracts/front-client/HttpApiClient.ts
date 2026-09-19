/**
 * HttpApiClient — the real `ApiClient` of the desktop front, over the local HTTP API of
 * `agentic-local-app` (`http://127.0.0.1:8765/api/v1` by default).
 *
 * It implements `ApiClient` verbatim: same method names, same parameter types, same return
 * types. Every method that performs a request accepts one extra **optional** trailing
 * `AbortSignal`, which keeps the implementation assignable to the interface while letting a
 * screen cancel a read it no longer needs.
 *
 * No dependency: `fetch`, `EventSource`, `URLSearchParams` and `setTimeout` only. `fetch` and
 * the `EventSource` factory are injectable so the client is testable without a server.
 *
 * Reference: `docs/contracts/front-backend-v1.md` in the `agentic-local-app` repository. Section
 * numbers quoted in the doc comments below are that document's sections, and the "G-n" markers
 * are the numbered gaps of its "état des lieux" (§9) — read it before changing any mapping here.
 */

import type { ApiClient } from './ApiClient';
import type {
  AuditEntry,
  ChatMessage,
  ContextWindowStatus,
  ConversationStatus,
  CycleType,
  HistoryEvent,
  HistorySession,
  LiveDbTable,
  ModelOption,
  Plan,
  PlanStatus,
  PlanTask,
  SessionBudget,
  SessionSnapshot,
  SignInConfig,
  TaskStatus,
  WhoAmI,
} from './types';

// ------------------------------------------------------------------------------------------------
// constants (§2 transport, §3 routes, §4 errors)
// ------------------------------------------------------------------------------------------------

/** Default base URL of the local API (`api.host` / `api.port` of `config.toml`). */
export const DEFAULT_BASE_URL = 'http://127.0.0.1:8765/api/v1';

/** `event:` of the frame the server sends to a client it had to unsubscribe (§5.3). */
export const DROPPED_EVENT = 'dropped';

/**
 * Backend session states that carry no conversation state, mapped onto the front union (§6.2).
 * `RUNNING` and `INTERRUPTING` have no exact front equivalent; the closest member is used.
 */
const SESSION_STATUS_FALLBACK: Readonly<Record<string, ConversationStatus>> = {
  READY: 'READY',
  RUNNING: 'ACTIVE',
  INTERRUPTING: 'INTERRUPTED',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
};

/**
 * `SessionState.PAUSED` (ADR-025) is a real state of the backend and is **missing** from the
 * front's `ConversationStatus` union — gap G-6. The client emits the true value through this one
 * deliberate widening rather than disguising a paused session as something else: a front that
 * cannot see `PAUSED` cannot show the pause banner the decision is about. Add `'PAUSED'` to
 * `ConversationStatus` in `types.ts` and this cast disappears.
 */
export const PAUSED_STATUS = 'PAUSED' as ConversationStatus;

/** The 10 conversation states of the backend, all of them members of the front union. */
const CONVERSATION_STATUSES: readonly string[] = [
  'NEW',
  'ACTIVE',
  'WAITING_MODEL_RESPONSE',
  'RUNNING_PLAN',
  'WAITING_USER',
  'ROTATING',
  'INTERRUPTED',
  'COMPLETED',
  'FAILED',
  'CLOSED',
];

const PLAN_STATUSES: readonly PlanStatus[] = [
  'PENDING',
  'RUNNING',
  'COMPLETED',
  'STOPPED_ON_FAILURE',
  'SHORT_CIRCUITED_ON_SUCCESS',
  'INTERRUPTED',
  'FAILED',
];

const TASK_STATUSES: readonly TaskStatus[] = [
  'PENDING',
  'WAITING_DEPENDENCY',
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'TIMED_OUT',
  'SKIPPED',
  'CANCELLED',
  'INTERRUPTED',
];

const CYCLE_TYPES: readonly CycleType[] = ['discovery', 'execution', 'clarification', 'resume'];

const CONTEXT_WINDOW_STATUSES: readonly ContextWindowStatus[] = [
  'HEALTHY',
  'WARNING',
  'SATURATED',
];

/** Every event type that can move the snapshot — the whole catalogue but `task.output` (§5.2). */
export const SESSION_EVENT_TYPES: readonly string[] = [
  'session.created',
  'session.state_changed',
  'session.paused',
  'conversation.created',
  'conversation.state_changed',
  'cycle.started',
  'cycle.ended',
  'plan.received',
  'plan.state_changed',
  'task.state_changed',
  'final_answer.received',
  'user_response.received',
  'failure.recorded',
  'retry.scheduled',
  'breaker.state_changed',
  'context.window_state_changed',
  'rotation.started',
  'rotation.completed',
  'rotation.failed',
  'budget.updated',
  'budget.exceeded',
  'interruption.requested',
  'interruption.completed',
  'recovery.started',
  'recovery.action',
  'recovery.completed',
];

/** Event types that mean a new chat turn may exist (§5.2). */
export const MESSAGE_EVENT_TYPES: readonly string[] = [
  'message.outbound',
  'message.inbound',
  'message.rejected',
  'message.retransmitted',
  'final_answer.received',
  'user_response.received',
];

/** Fields of `SignInConfig` the backend has no place for today — gaps G-3, G-4 (§9). */
export const SIGN_IN_FIELDS_DROPPED: readonly (keyof SignInConfig)[] = [
  'userId',
  'skills',
  'effort',
];

/** Hard stop on paging loops, so a malformed `next_offset` can never spin forever. */
const MAX_PAGES = 1_000;

// ------------------------------------------------------------------------------------------------
// errors (§4)
// ------------------------------------------------------------------------------------------------

/**
 * One error of the API, parsed from the uniform envelope `{"error": <NormalizedError>}` (§4.1).
 *
 * `status` is the HTTP status, `0` for a failure that never reached the server (network down) or
 * for a refusal the client itself decides (`MODEL_NOT_ACTIVE`).
 */
export class ApiError extends Error {
  /** `error_code` — the closed list of §4.2; the only field a screen should branch on. */
  readonly code: string;
  /** `error_type` — the family of §4.2 (`AUTHN_ERROR`, `NETWORK_ERROR`, …). */
  readonly type: string;
  /** `retryable` as the backend computed it; the front never re-derives it from the status. */
  readonly retryable: boolean;
  /** `recoverable` — the session may continue after this error. */
  readonly recoverable: boolean;
  /** `severity` — `low` | `medium` | `high` | `critical`. */
  readonly severity: string;
  /** `origin` — the component that produced the error. */
  readonly origin: string;
  /** `details` — free-form, never a secret (§4.1). */
  readonly details: Readonly<Record<string, unknown>>;
  /** HTTP status, `0` when the request never got one. */
  readonly status: number;

  constructor(init: {
    code: string;
    type: string;
    status: number;
    message: string;
    retryable?: boolean;
    recoverable?: boolean;
    severity?: string;
    origin?: string;
    details?: Record<string, unknown>;
  }) {
    super(init.message);
    this.name = 'ApiError';
    this.code = init.code;
    this.type = init.type;
    this.status = init.status;
    this.retryable = init.retryable ?? false;
    this.recoverable = init.recoverable ?? true;
    this.severity = init.severity ?? 'high';
    this.origin = init.origin ?? 'http_api';
    this.details = init.details ?? {};
  }

  /** `true` when the session is paused and waiting for a token (§7.2). */
  get isAuthenticationError(): boolean {
    return this.type === 'AUTHN_ERROR';
  }

  /** `true` when the loop still owns the session: the send button must stay blocked (§7.3). */
  get isSessionBusy(): boolean {
    return this.code === 'SESSION_BUSY';
  }
}

// ------------------------------------------------------------------------------------------------
// narrowing helpers — every wire value arrives as `unknown` and is narrowed here, never cast
// ------------------------------------------------------------------------------------------------

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function asList(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asText(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback;
}

function asTextOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function asNumberOrNull(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function asBoolean(value: unknown, fallback = false): boolean {
  return typeof value === 'boolean' ? value : fallback;
}

function asTextList(value: unknown): string[] {
  return asList(value).filter((item): item is string => typeof item === 'string');
}

function oneOf<T extends string>(value: unknown, allowed: readonly T[], fallback: T): T {
  return typeof value === 'string' && (allowed as readonly string[]).includes(value)
    ? (value as T)
    : fallback;
}

function parseJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
}

// ------------------------------------------------------------------------------------------------
// injection points
// ------------------------------------------------------------------------------------------------

/** The subset of `fetch` the client uses. */
export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

/** Builds the live stream. Injected so a test can hand back a fake without a server. */
export type EventSourceFactory = (url: string) => EventSource;

/** Bounded backoff of the live stream (§5.3). */
export interface ReconnectPolicy {
  /** Delay before the first retry, in milliseconds. */
  readonly initialDelayMs: number;
  /** Ceiling of the delay, in milliseconds — this is what makes the backoff bounded. */
  readonly maxDelayMs: number;
  /** Multiplier applied at each attempt. */
  readonly factor: number;
  /** Random share of the delay, in `[0, 1]`, to spread simultaneous reconnections. */
  readonly jitter: number;
  /** Give up after this many consecutive failures; `Infinity` keeps trying. */
  readonly maxAttempts: number;
}

export const DEFAULT_RECONNECT_POLICY: ReconnectPolicy = {
  initialDelayMs: 500,
  maxDelayMs: 15_000,
  factor: 2,
  jitter: 0.2,
  maxAttempts: Number.POSITIVE_INFINITY,
};

export interface HttpApiClientOptions {
  /** Defaults to the global `fetch`. */
  readonly fetch?: FetchLike;
  /** Defaults to `(url) => new EventSource(url)`. */
  readonly eventSource?: EventSourceFactory;
  /** Live-stream reconnection; merged over {@link DEFAULT_RECONNECT_POLICY}. */
  readonly reconnect?: Partial<ReconnectPolicy>;
  /** Page size asked of the paginated routes; the server caps nothing below it. */
  readonly pageSize?: number;
  /**
   * `goal` sent to `POST /sessions` by {@link HttpApiClient.signIn} — gap G-4: the backend
   * requires an opening `goal` and `user_message`, and `SignInConfig` carries neither.
   */
  readonly openingGoal?: string;
  /** `user_message` sent to `POST /sessions` by {@link HttpApiClient.signIn} — gap G-4. */
  readonly openingMessage?: string;
  /** Called when a live stream gives up after `reconnect.maxAttempts`. */
  readonly onStreamGaveUp?: (sessionId: string, error: ApiError) => void;
}

// ------------------------------------------------------------------------------------------------
// client
// ------------------------------------------------------------------------------------------------

/**
 * The real `ApiClient`. Drop it in beside `MockApiClient` and swap the one line of `ApiProvider`
 * that builds the client (see `contracts/front-client/README.md`).
 */
export class HttpApiClient implements ApiClient {
  /** Base URL, without a trailing slash. */
  readonly baseUrl: string;
  /** `false` — `GET /skills` does not exist (gap G-3); `listKnownSkills` returns `[]`. */
  readonly supportsKnownSkills = false;

  private readonly http: FetchLike;
  private readonly openStream: EventSourceFactory;
  private readonly reconnect: ReconnectPolicy;
  private readonly pageSize: number;
  private readonly openingGoal: string;
  private readonly openingMessage: string;
  private readonly onStreamGaveUp: ((sessionId: string, error: ApiError) => void) | null;

  constructor(baseUrl: string = DEFAULT_BASE_URL, options: HttpApiClientOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.http = options.fetch ?? ((input, init) => fetch(input, init));
    this.openStream = options.eventSource ?? ((url) => new EventSource(url));
    this.reconnect = { ...DEFAULT_RECONNECT_POLICY, ...(options.reconnect ?? {}) };
    this.pageSize = options.pageSize ?? 200;
    this.openingGoal = options.openingGoal ?? 'Desktop console session';
    this.openingMessage = options.openingMessage ?? 'Session opened from the desktop console.';
    this.onStreamGaveUp = options.onStreamGaveUp ?? null;
  }

  // ---------------------------------------------------------------------------------------------
  // §1 identity — GET /whoami
  // ---------------------------------------------------------------------------------------------
  /**
   * The user of this machine. The backend also returns `source` (which step of ADR-024 §5
   * answered) and `host`; `WhoAmI` has room for neither — gap G-1. Use {@link whoAmIVerbose}
   * to show the "Auto-detected" badge honestly.
   */
  async whoAmI(signal?: AbortSignal): Promise<WhoAmI> {
    const body = asRecord(await this.call('/whoami', { signal }));
    return { userId: asText(body['user_id']) };
  }

  /** `GET /whoami` unabridged: `{userId, source, host}` (§3.1). */
  async whoAmIVerbose(
    signal?: AbortSignal,
  ): Promise<{ userId: string; source: string; host: string | null }> {
    const body = asRecord(await this.call('/whoami', { signal }));
    return {
      userId: asText(body['user_id']),
      source: asText(body['source'], 'unknown'),
      host: asTextOrNull(body['host']),
    };
  }

  // ---------------------------------------------------------------------------------------------
  // §2 models — GET /models
  // ---------------------------------------------------------------------------------------------
  /**
   * The model catalogue, active profile first. `ModelOption` has no `active` flag, so the picker
   * cannot mark the one profile that this process can actually use — gap G-2. Use
   * {@link activeModelName} to disable the others, as ADR-024 requires (§7.1).
   */
  async listModels(signal?: AbortSignal): Promise<ModelOption[]> {
    const body = asRecord(await this.call('/models', { signal }));
    return asList(body['models']).map((item): ModelOption => {
      const profile = asRecord(item);
      const name = asText(profile['name']);
      return {
        id: name,
        name: asText(profile['display_name'], name) || name,
        provider: asText(profile['provider']),
        codec: asText(profile['codec']),
        requiresCredentials: asBoolean(profile['requires_credentials']),
      };
    });
  }

  /** Name of the active profile — the only model this process can talk to (ADR-024 §2). */
  async activeModelName(signal?: AbortSignal): Promise<string> {
    const body = asRecord(await this.call('/models', { signal }));
    return asText(body['active']);
  }

  // ---------------------------------------------------------------------------------------------
  // §3 skills — no route
  // ---------------------------------------------------------------------------------------------
  /**
   * Not implemented on the backend: there is no `GET /skills` (gap G-3). An empty list is
   * returned so the "Add skill" field degrades to free text instead of the screen failing;
   * {@link supportsKnownSkills} says so explicitly.
   */
  async listKnownSkills(): Promise<string[]> {
    return [];
  }

  // ---------------------------------------------------------------------------------------------
  // §5 session — POST /credentials, POST /sessions
  // ---------------------------------------------------------------------------------------------
  /**
   * Opens a session. Three calls, in this order (§3.4):
   *
   * 1. `POST /credentials` when `accessToken` is set — the token of the **active** profile;
   * 2. `GET /models`, to refuse a `modelId` that is not the active profile (ADR-024 §2: one model
   *    per process, changing it means restarting) with `ApiError(code = 'MODEL_NOT_ACTIVE')`;
   * 3. `POST /sessions`, with `working_space` and, when the three limits are all present,
   *    `session_budget`.
   *
   * `userId`, `skills` and `effort` are **dropped**: no route accepts them (gaps G-3, G-4), and
   * the backend takes its user id from `transport.user_id`. `goal` and `user_message` are
   * mandatory server-side and absent from `SignInConfig`, so they come from `openingGoal` /
   * `openingMessage` — the model therefore receives one opening `user_request` the user never
   * typed (gap G-4).
   */
  async signIn(config: SignInConfig, signal?: AbortSignal): Promise<SessionSnapshot> {
    if (config.accessToken !== undefined && config.accessToken !== '') {
      await this.setCredentials(config.accessToken, signal);
    }
    if (config.modelId !== '') {
      const active = await this.activeModelName(signal);
      if (active !== '' && config.modelId !== active) {
        throw new ApiError({
          code: 'MODEL_NOT_ACTIVE',
          type: 'SYSTEM_ERROR',
          status: 0,
          message:
            `the running process serves the model profile "${active}"; ` +
            `"${config.modelId}" requires restarting the application (ADR-024 §2)`,
          retryable: false,
          origin: 'HttpApiClient',
          details: { requested: config.modelId, active },
        });
      }
    }
    const body: Record<string, unknown> = {
      goal: this.openingGoal,
      user_message: this.openingMessage,
    };
    if (config.workingSpace !== undefined && config.workingSpace !== '') {
      body['working_space'] = config.workingSpace;
    }
    const budget = wireBudget(config.sessionBudget);
    if (budget !== null) {
      body['session_budget'] = budget;
    }
    const created = asRecord(await this.call('/sessions', { method: 'POST', body, signal }));
    return this.getSession(asText(created['session_id']), signal);
  }

  /**
   * `POST /credentials` — the token of the active profile, for the pop-in that follows a 401
   * (§3.10). The value is written to the environment variable the transport reads and is never
   * echoed back; nothing here logs it either.
   */
  async setCredentials(token: string, signal?: AbortSignal): Promise<void> {
    await this.call('/credentials', { method: 'POST', body: { token }, signal });
  }

  /**
   * Current state of the session, from `GET /sessions/{sid}/snapshot` (§3.5).
   *
   * `conversationId` is the front's name for what the backend calls a **session id** — the
   * identifier every `/sessions/{sid}` route takes. A backend *conversation* is the inner object
   * a rotation or an interruption replaces; the front never addresses one (§6.1).
   */
  async getSession(conversationId: string, signal?: AbortSignal): Promise<SessionSnapshot> {
    const snapshot = asRecord(
      await this.call(`/sessions/${encodeURIComponent(conversationId)}/snapshot`, { signal }),
    );
    return toSessionSnapshot(conversationId, snapshot);
  }

  /**
   * Sends a `user_request` (§3.6). `POST /sessions/{sid}/messages` answers `202` with the session
   * record, not with a chat turn, so the turn is read back from `GET /sessions/{sid}/chat` and,
   * failing that, synthesised locally with a `local-` identifier.
   *
   * The backend never queues: while the loop owns the session it answers `409 SESSION_BUSY`
   * (`ApiError.isSessionBusy`), which is the owner decision of §7.3 — the send button is blocked
   * while a session runs. `ChatMessage.queued` is therefore never set (gap G-8).
   */
  async sendMessage(
    conversationId: string,
    text: string,
    signal?: AbortSignal,
  ): Promise<ChatMessage> {
    const session = encodeURIComponent(conversationId);
    await this.call(`/sessions/${session}/messages`, {
      method: 'POST',
      body: { user_message: text },
      signal,
    });
    const turns = await this.readChat(conversationId, signal);
    for (let index = turns.length - 1; index >= 0; index -= 1) {
      const turn = turns[index];
      if (turn !== undefined && turn.role === 'user' && turn.text === text) {
        return turn;
      }
    }
    return {
      id: `local-${Date.now().toString(36)}`,
      role: 'user',
      text,
      createdAt: new Date().toISOString(),
    };
  }

  // ---------------------------------------------------------------------------------------------
  // §6 interruption — POST /sessions/{sid}/interrupt
  // ---------------------------------------------------------------------------------------------
  /**
   * Interrupts the session and returns its new state (§3.7). The route answers an
   * `InterruptionReport`, not a snapshot, so the snapshot is read right after; the route only
   * returns once `READY` is reached, so that read is never a half-interrupted state.
   *
   * It is accepted in every state, paused included, which is why the stop button stays live
   * while the send button is blocked (§7.3).
   */
  async interrupt(conversationId: string, signal?: AbortSignal): Promise<SessionSnapshot> {
    const session = encodeURIComponent(conversationId);
    await this.call(`/sessions/${session}/interrupt`, { method: 'POST', signal });
    return this.getSession(conversationId, signal);
  }

  /**
   * `POST /sessions/{sid}/resume` — continues a session paused on a 401, once a token has been
   * posted (§3.9). `409 SESSION_NOT_RESUMABLE` when the session is not resumable.
   */
  async resume(conversationId: string, signal?: AbortSignal): Promise<SessionSnapshot> {
    const session = encodeURIComponent(conversationId);
    await this.call(`/sessions/${session}/resume`, { method: 'POST', signal });
    return this.getSession(conversationId, signal);
  }

  /**
   * `GET /sessions/{sid}/pause` — why the session is paused, for the banner (§3.9, §7.2).
   * `null` when it is not paused (the route answers `404 NOT_PAUSED`).
   */
  async pauseReason(
    conversationId: string,
    signal?: AbortSignal,
  ): Promise<PauseReason | null> {
    const session = encodeURIComponent(conversationId);
    try {
      const body = asRecord(await this.call(`/sessions/${session}/pause`, { signal }));
      return {
        reason: asText(body['reason']),
        errorCode: asText(body['error_code']),
        errorType: asText(body['error_type']),
        operation: asText(body['operation']),
        since: asText(body['since']),
      };
    } catch (error) {
      if (error instanceof ApiError && error.code === 'NOT_PAUSED') {
        return null;
      }
      throw error;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // §8 live stream — GET /sessions/{sid}/events (SSE)
  // ---------------------------------------------------------------------------------------------
  /**
   * Subscribes to the state of one session. The stream carries events, not snapshots (§5.2), so
   * every event triggers one coalesced `GET /sessions/{sid}/snapshot`; one refresh is also done
   * immediately, so a screen that subscribes is painted without waiting for the first event.
   *
   * Returns the unsubscribe function: calling it closes the stream and cancels any pending
   * reconnection. Reconnections are bounded (see {@link ReconnectPolicy}) and resume the stream
   * from the last identifier seen, so nothing audited is missed.
   */
  subscribeSession(
    conversationId: string,
    onUpdate: (snapshot: SessionSnapshot) => void,
  ): () => void {
    let stopped = false;
    const refresh = coalesce(async () => {
      const snapshot = await this.getSession(conversationId);
      if (!stopped) {
        onUpdate(snapshot);
      }
    });
    const close = this.stream(conversationId, SESSION_EVENT_TYPES, () => {
      void refresh();
    });
    void refresh();
    return () => {
      stopped = true;
      close();
    };
  }

  /**
   * Subscribes to the chat of one session. The live stream carries message **identifiers and
   * types, never bodies** (§5.2, gap G-9), so every message event triggers one coalesced
   * `GET /sessions/{sid}/chat` and only the turns not seen yet are delivered.
   *
   * The first read delivers the turns already stored: `ApiClient` has no method to read the chat
   * history, so this subscription is the only way a screen can fill itself (§9, `subscribeMessages`).
   */
  subscribeMessages(
    conversationId: string,
    onMessage: (message: ChatMessage) => void,
  ): () => void {
    let stopped = false;
    const delivered = new Set<string>();
    const refresh = coalesce(async () => {
      const turns = await this.readChat(conversationId);
      if (stopped) {
        return;
      }
      for (const turn of turns) {
        if (!delivered.has(turn.id)) {
          delivered.add(turn.id);
          onMessage(turn);
        }
      }
    });
    const close = this.stream(conversationId, MESSAGE_EVENT_TYPES, () => {
      void refresh();
    });
    void refresh();
    return () => {
      stopped = true;
      close();
    };
  }

  // ---------------------------------------------------------------------------------------------
  // §10 history — GET /admin/sessions, GET /sessions/{sid}/audit
  // ---------------------------------------------------------------------------------------------
  /** Every session of the store, newest first (`GET /admin/sessions`, §3.11). */
  async listHistorySessions(signal?: AbortSignal): Promise<HistorySession[]> {
    const items = await this.pagedByOffset('/admin/sessions', signal);
    return items.map((item): HistorySession => {
      const session = asRecord(item);
      return {
        id: asText(session['session_id']),
        state: frontStatusOfSession(asText(session['status'])),
        createdAt: asText(session['created_at']),
        updatedAt: asText(session['updated_at']),
        userId: asText(session['user_id']),
      };
    });
  }

  /**
   * The audited timeline of one session (`GET /sessions/{sid}/audit`, §3.12), **newest first**,
   * like every other list this client returns.
   *
   * `messageIn` / `messageOut` are not in the audit payload — an audited message event carries
   * `message_id` and `message_type` only (gap G-10). They are filled by joining
   * `GET /sessions/{sid}/messages` on that identifier, as the canonical JSON of the protocol
   * message; the join is one extra request per call.
   */
  async listHistoryEvents(sessionId: string, signal?: AbortSignal): Promise<HistoryEvent[]> {
    const session = encodeURIComponent(sessionId);
    const [events, messages] = await Promise.all([
      this.pagedByAfter(`/sessions/${session}/audit`, signal),
      this.call(`/sessions/${session}/messages`, { signal }),
    ]);
    const bodies = new Map<string, { direction: string; payload: string }>();
    for (const item of asList(messages)) {
      const message = asRecord(item);
      const id = asText(message['message_id']);
      if (id !== '') {
        bodies.set(id, {
          direction: asText(message['direction']),
          payload: JSON.stringify(message['payload'] ?? {}),
        });
      }
    }
    const timeline = events.map((item): HistoryEvent => {
      const event = asRecord(item);
      const payload = asRecord(event['payload']);
      const body = bodies.get(asText(payload['message_id']));
      const entry: HistoryEvent = {
        id: asText(event['event_id']),
        sessionId: asText(event['session_id'], sessionId),
        type: asText(event['event_type']),
        ts: asText(event['timestamp']),
        auditEntryId: asText(event['event_id']),
      };
      if (body !== undefined && body.direction === 'inbound') {
        entry.messageIn = body.payload;
      }
      if (body !== undefined && body.direction === 'outbound') {
        entry.messageOut = body.payload;
      }
      return entry;
    });
    return timeline.reverse();
  }

  // ---------------------------------------------------------------------------------------------
  // §11 and §12 live database — GET /admin/*, POST /admin/reset-database
  // ---------------------------------------------------------------------------------------------
  /** One of the three administration tables, whole, newest first (§3.11). */
  async listLiveDb(table: LiveDbTable, signal?: AbortSignal): Promise<unknown[]> {
    return this.pagedByOffset(`/admin/${table}`, signal);
  }

  /**
   * Empties the store (§3.13). The route is gated: it answers `403 ADMIN_DISABLED` and touches
   * nothing unless `api.allow_destructive_admin` is `true` in `config.toml`. The front must keep
   * its own confirmation and surface that refusal as a configuration problem, not a bug.
   */
  async clearDatabase(signal?: AbortSignal): Promise<void> {
    await this.call('/admin/reset-database', { method: 'POST', signal });
  }

  /** The audit chain of every session, newest first, with its hashes (§3.11). */
  async listAudit(signal?: AbortSignal): Promise<AuditEntry[]> {
    const items = await this.pagedByOffset('/admin/audit', signal);
    return items.map((item): AuditEntry => {
      const event = asRecord(item);
      return {
        id: asText(event['event_id']),
        eventType: asText(event['event_type']),
        ts: asText(event['timestamp']),
        prevHash: asText(event['previous_event_hash']),
        hash: asText(event['event_hash']),
      };
    });
  }

  // ---------------------------------------------------------------------------------------------
  // internals
  // ---------------------------------------------------------------------------------------------

  /** `GET /sessions/{sid}/chat` mapped onto `ChatMessage`, oldest first (§3.8). */
  private async readChat(conversationId: string, signal?: AbortSignal): Promise<ChatMessage[]> {
    const session = encodeURIComponent(conversationId);
    const body = asRecord(await this.call(`/sessions/${session}/chat`, { signal }));
    return asList(body['messages']).map((item): ChatMessage => {
      const turn = asRecord(item);
      return {
        id: asText(turn['id']),
        role: oneOf(turn['role'], ['user', 'assistant', 'system'] as const, 'system'),
        text: asText(turn['text']),
        createdAt: asText(turn['created_at']),
      };
    });
  }

  /** One request. Returns the parsed body, or `null` for a `204`. Throws {@link ApiError}. */
  private async call(
    path: string,
    init: { method?: string; body?: unknown; signal?: AbortSignal | undefined } = {},
  ): Promise<unknown> {
    const request: RequestInit = {
      method: init.method ?? 'GET',
      headers: init.body === undefined ? { Accept: 'application/json' } : jsonHeaders(),
    };
    if (init.body !== undefined) {
      request.body = JSON.stringify(init.body);
    }
    if (init.signal !== undefined) {
      request.signal = init.signal;
    }
    let response: Response;
    try {
      response = await this.http(`${this.baseUrl}${path}`, request);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw new ApiError({
        code: 'NETWORK_UNREACHABLE',
        type: 'NETWORK_ERROR',
        status: 0,
        message: `${this.baseUrl} is unreachable: ${describe(error)}`,
        retryable: true,
        origin: 'HttpApiClient',
        details: { path },
      });
    }
    const raw = response.status === 204 ? '' : await response.text();
    const parsed = raw === '' ? null : parseJson(raw);
    if (!response.ok) {
      throw toApiError(response.status, parsed, path);
    }
    return parsed;
  }

  /** `{items, limit, offset, next_offset}` paging, followed to the end. */
  private async pagedByOffset(path: string, signal?: AbortSignal): Promise<unknown[]> {
    const collected: unknown[] = [];
    let offset = 0;
    for (let page = 0; page < MAX_PAGES; page += 1) {
      const query = new URLSearchParams({
        limit: String(this.pageSize),
        offset: String(offset),
      });
      const body = asRecord(await this.call(`${path}?${query.toString()}`, { signal }));
      collected.push(...asList(body['items']));
      const next = asNumberOrNull(body['next_offset']);
      if (next === null || next <= offset) {
        break;
      }
      offset = next;
    }
    return collected;
  }

  /** `{items, after, limit, next_after}` paging of the audit chain, followed to the end. */
  private async pagedByAfter(path: string, signal?: AbortSignal): Promise<unknown[]> {
    const collected: unknown[] = [];
    let after: number | null = null;
    for (let page = 0; page < MAX_PAGES; page += 1) {
      const query = new URLSearchParams({ limit: String(this.pageSize) });
      if (after !== null) {
        query.set('after', String(after));
      }
      const body = asRecord(await this.call(`${path}?${query.toString()}`, { signal }));
      collected.push(...asList(body['items']));
      const next = asNumberOrNull(body['next_after']);
      if (next === null || (after !== null && next <= after)) {
        break;
      }
      after = next;
    }
    return collected;
  }

  /**
   * One SSE subscription with bounded backoff. `onEvent` is called for every frame of the
   * requested types; the `dropped` frame (§5.3) and any transport error reopen the stream from
   * the last identifier seen.
   */
  private stream(
    sessionId: string,
    eventTypes: readonly string[],
    onEvent: (type: string, data: unknown) => void,
  ): () => void {
    let closed = false;
    let source: EventSource | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let attempt = 0;
    let lastEventId: string | null = null;

    const open = (): void => {
      if (closed) {
        return;
      }
      const query = new URLSearchParams({ event_types: eventTypes.join(',') });
      if (lastEventId !== null) {
        query.set('last_event_id', lastEventId);
      }
      const url =
        `${this.baseUrl}/sessions/${encodeURIComponent(sessionId)}/events?${query.toString()}`;
      const opened = this.openStream(url);
      source = opened;
      opened.onopen = (): void => {
        attempt = 0;
      };
      opened.onerror = (): void => {
        reopen();
      };
      for (const type of [...eventTypes, DROPPED_EVENT]) {
        opened.addEventListener(type, (event: Event): void => {
          const frame = event as MessageEvent<unknown>;
          const id = frame.lastEventId;
          if (typeof id === 'string' && id !== '') {
            lastEventId = id;
          }
          if (type === DROPPED_EVENT) {
            reopen();
            return;
          }
          onEvent(type, typeof frame.data === 'string' ? parseJson(frame.data) : null);
        });
      }
    };

    const reopen = (): void => {
      if (closed || timer !== null) {
        return;
      }
      source?.close();
      source = null;
      if (attempt >= this.reconnect.maxAttempts) {
        closed = true;
        this.onStreamGaveUp?.(
          sessionId,
          new ApiError({
            code: 'STREAM_UNAVAILABLE',
            type: 'NETWORK_ERROR',
            status: 0,
            message: `the live stream of ${sessionId} could not be reopened`,
            retryable: true,
            origin: 'HttpApiClient',
            details: { attempts: attempt },
          }),
        );
        return;
      }
      const delay = backoffDelay(attempt, this.reconnect);
      attempt += 1;
      timer = setTimeout(() => {
        timer = null;
        open();
      }, delay);
    };

    open();
    return (): void => {
      closed = true;
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
      source?.close();
      source = null;
    };
  }
}

/** Why a session is paused, as `GET /sessions/{sid}/pause` returns it (§3.9). */
export interface PauseReason {
  /** `credentials_required` today — the only reason the backend pauses on (ADR-025 §2). */
  readonly reason: string;
  /** Error code of the refused call, e.g. `HTTP_401`. */
  readonly errorCode: string;
  /** Error family, `AUTHN_ERROR` for a 401. */
  readonly errorType: string;
  /** The remote operation that was refused: `INIT`, `POST` or `GET`. */
  readonly operation: string;
  /** ISO 8601 instant the session was paused. */
  readonly since: string;
}

// ------------------------------------------------------------------------------------------------
// wire → front mapping (§6)
// ------------------------------------------------------------------------------------------------

/** `RuntimeSnapshot` of §4.1 mapped onto the front's `SessionSnapshot` (§6.3). */
export function toSessionSnapshot(
  conversationId: string,
  snapshot: Record<string, unknown>,
): SessionSnapshot {
  const session = asRecord(snapshot['session']);
  const conversation = isRecord(snapshot['conversation']) ? snapshot['conversation'] : null;
  const cycle = isRecord(snapshot['cycle']) ? snapshot['cycle'] : null;
  const plan = isRecord(snapshot['plan']) ? snapshot['plan'] : null;
  return {
    conversationId: asText(session['session_id'], conversationId),
    status: frontStatus(asText(session['status']), conversation),
    currentCycleId: cycle === null ? null : asTextOrNull(cycle['cycle_id']),
    cycleType: cycle === null ? null : oneOf(cycle['cycle_type'], CYCLE_TYPES, 'discovery'),
    retryCount: cycle === null ? 0 : asNumber(cycle['retry_count']),
    cycleStartedAt: cycle === null ? null : asTextOrNull(cycle['started_at']),
    currentPlan: plan === null ? null : toPlan(plan, asList(snapshot['tasks'])),
    contextWindow:
      conversation === null
        ? 'HEALTHY'
        : oneOf(conversation['context_window_state'], CONTEXT_WINDOW_STATUSES, 'HEALTHY'),
    budget: toBudget(asRecord(session['session_budget'])),
  };
}

function toPlan(plan: Record<string, unknown>, tasks: readonly unknown[]): Plan {
  const planId = asText(plan['plan_id']);
  return {
    planId,
    status: oneOf(plan['status'], PLAN_STATUSES, 'PENDING'),
    tasks: tasks
      .map(asRecord)
      .filter((task) => asText(task['plan_id'], planId) === planId)
      .map((task): PlanTask => {
        const entry: PlanTask = {
          taskId: asText(task['task_id']),
          summary: asText(task['cmd']) || asText(task['type'], 'cmd'),
          status: oneOf(task['status'], TASK_STATUSES, 'PENDING'),
          dependsOn: asTextList(task['depends_on']),
        };
        const reason = asTextOrNull(task['reason']);
        if (reason !== null) {
          entry.error = reason;
        }
        return entry;
      }),
  };
}

function toBudget(budget: Record<string, unknown>): SessionBudget {
  return {
    maxCycles: asNumber(budget['max_cycles']),
    usedCycles: asNumber(budget['consumed_cycles']),
    maxPlans: asNumber(budget['max_plans']),
    usedPlans: asNumber(budget['consumed_plans']),
    maxTotalDurationMs: asNumber(budget['max_total_duration_ms']),
    usedDurationMs: asNumber(budget['consumed_duration_ms']),
  };
}

/**
 * Backend state → front state (§6.2). A running session shows the state of its conversation,
 * which is where the 10 detailed values live; every other session state uses the table.
 */
function frontStatus(
  sessionStatus: string,
  conversation: Record<string, unknown> | null,
): ConversationStatus {
  if (sessionStatus === 'PAUSED') {
    return PAUSED_STATUS;
  }
  if (sessionStatus === 'RUNNING' && conversation !== null) {
    const status = asText(conversation['status']);
    if (CONVERSATION_STATUSES.includes(status)) {
      return status as ConversationStatus;
    }
  }
  return frontStatusOfSession(sessionStatus);
}

/** Session state alone, with no conversation to refine it (history rows, §6.2). */
function frontStatusOfSession(sessionStatus: string): ConversationStatus {
  if (sessionStatus === 'PAUSED') {
    return PAUSED_STATUS;
  }
  return SESSION_STATUS_FALLBACK[sessionStatus] ?? 'NEW';
}

/**
 * `SessionBudget` of the backend: the three limits, all required and all `> 0`. A partial budget
 * would be refused with `422` (gap G-5), so an incomplete one is dropped and the configured
 * defaults apply.
 */
function wireBudget(budget: SignInConfig['sessionBudget']): Record<string, number> | null {
  if (budget === undefined) {
    return null;
  }
  const { maxCycles, maxPlans, maxTotalDurationMs } = budget;
  if (
    typeof maxCycles !== 'number' ||
    typeof maxPlans !== 'number' ||
    typeof maxTotalDurationMs !== 'number'
  ) {
    return null;
  }
  return {
    max_cycles: maxCycles,
    max_plans: maxPlans,
    max_total_duration_ms: maxTotalDurationMs,
  };
}

// ------------------------------------------------------------------------------------------------
// small utilities
// ------------------------------------------------------------------------------------------------

function jsonHeaders(): Record<string, string> {
  return { 'Content-Type': 'application/json', Accept: 'application/json' };
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError';
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** The error envelope of §4.1, or a uniform fallback when the body is not one. */
function toApiError(status: number, parsed: unknown, path: string): ApiError {
  const error = asRecord(asRecord(parsed)['error']);
  const details = asRecord(error['details']);
  const code = asText(error['error_code'], `HTTP_${status}`);
  return new ApiError({
    code,
    type: asText(error['error_type'], 'SYSTEM_ERROR'),
    status,
    message: asText(details['message'], `${code} on ${path}`),
    retryable: asBoolean(error['retryable']),
    recoverable: asBoolean(error['recoverable'], true),
    severity: asText(error['severity'], 'high'),
    origin: asText(error['origin'], 'http_api'),
    details,
  });
}

/** `min(max, initial × factor^attempt)` with jitter — bounded by construction (§5.3). */
export function backoffDelay(attempt: number, policy: ReconnectPolicy): number {
  const raw = policy.initialDelayMs * Math.pow(policy.factor, Math.max(0, attempt));
  const bounded = Math.min(policy.maxDelayMs, raw);
  const spread = bounded * policy.jitter;
  return Math.max(0, Math.round(bounded - spread / 2 + Math.random() * spread));
}

/**
 * Wraps an async task so that overlapping calls collapse: one run at a time, and at most one
 * more queued behind it. A burst of events therefore costs two reads, not one per event.
 */
function coalesce(task: () => Promise<void>): () => Promise<void> {
  let running = false;
  let again = false;
  const run = async (): Promise<void> => {
    if (running) {
      again = true;
      return;
    }
    running = true;
    try {
      await task();
    } catch {
      // A subscription must never throw into the event loop of the stream: the next event or
      // the next reconnection retries, and the screen keeps the state it already had.
    } finally {
      running = false;
      if (again) {
        again = false;
        await run();
      }
    }
  };
  return run;
}

export default HttpApiClient;
