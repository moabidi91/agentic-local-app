import type {
  AuditEntry,
  ChatMessage,
  HistoryEvent,
  HistorySession,
  LiveDbTable,
  ModelOption,
  SessionSnapshot,
  SignInConfig,
  WhoAmI,
} from './types';

/**
 * The one interface every screen talks to. This is the code-level twin of
 * agentic-front/contrat-interface.md — each method below is numbered to
 * match that document's sections, so wiring the real backend later means
 * writing one HttpApiClient implementing this interface, not touching the
 * screens themselves.
 *
 * MockApiClient (mock.ts) implements this against fake data so the whole
 * app is clickable/demoable ahead of the backend contract being settled.
 */
export interface ApiClient {
  /** §1 Identité — GET /whoami [besoin backend] */
  whoAmI(): Promise<WhoAmI>;

  /** §2 Modèles / transports disponibles — GET /models [besoin backend] */
  listModels(): Promise<ModelOption[]>;

  /** §3 Skills — GET /skills (optional, guided add) [besoin backend] */
  listKnownSkills(): Promise<string[]>;

  /** §5 Session — create/resume a conversation and sign in. */
  signIn(config: SignInConfig): Promise<SessionSnapshot>;

  /** §5 Session — read current state. */
  getSession(conversationId: string): Promise<SessionSnapshot>;

  /** §5 Session — send a user_request. May be queued if the session is mid-cycle (§7). */
  sendMessage(conversationId: string, text: string): Promise<ChatMessage>;

  /** §6 Interruption — existing mechanism (user_interrupt), no new backend need. */
  interrupt(conversationId: string): Promise<SessionSnapshot>;

  /** Subscribe to session + chat updates. §8 Flux live (SSE, falls back to polling). */
  subscribeSession(
    conversationId: string,
    onUpdate: (snapshot: SessionSnapshot) => void,
  ): () => void;
  subscribeMessages(
    conversationId: string,
    onMessage: (message: ChatMessage) => void,
  ): () => void;

  /** §10 Historique — list known sessions and one session's event timeline. */
  listHistorySessions(): Promise<HistorySession[]>;
  listHistoryEvents(sessionId: string): Promise<HistoryEvent[]>;

  /** §11 Inspection live de la base (lecture) [besoin backend]. */
  listLiveDb(table: LiveDbTable): Promise<unknown[]>;

  /** §12 Vidage de la base [besoin backend], POST /admin/reset-database. */
  clearDatabase(): Promise<void>;

  /** §10 Audit — for the hash-chain check on LiveDatabaseScreen. */
  listAudit(): Promise<AuditEntry[]>;
}
