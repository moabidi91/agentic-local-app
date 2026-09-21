# Module map — paquets Python, classes, règles de dépendance

Ce document est le **contrat de structure** : chaque composant de la spec a un module, une classe et un fichier de tests désignés. Les agents qui implémentent une phase ne créent pas d'autres modules publics sans mettre ce document à jour.

## 1. Arborescence

```
src/agentic_local_app/
├── __init__.py
├── config.py                      AppConfig, load_config, load_dotenv          (ADR-018)
├── domain/                        pur, aucune I/O
│   ├── states.py                  toutes les énumérations (§5, §6, §12)
│   ├── transitions.py             tables de transitions + assert_transition     (§5, ADR-007)
│   ├── models.py                  records persistés (§16 + ADR)
│   ├── errors.py                  ErrorType, NormalizedError, exceptions        (§6)
│   ├── events.py                  EventType, Event                              (ADR-015/018)
│   ├── clock.py                   Clock, SystemClock, FakeClock                 (ADR-017)
│   ├── ids.py                     IdGenerator, UuidIdGenerator, SequentialIdGenerator
│   ├── commands.py                invoked(), VerdictPrograms : le programme d'une ligne de commande (ADR-029)
│   ├── shell.py                   ShellDialect, DetectedShell, ExecutionEnvironment, detect_shell (ADR-030)
│   ├── dialects.py                ShellTranslator : le dictionnaire bash <-> PowerShell           (ADR-030)
│   └── canonical.py               canonical_json, size_bytes, chain_hash        (ADR-017)
├── lifecycle/
│   └── conversation_lifecycle.py  ConversationLifecycleManager (sessions + conversations)  [phase 1]
├── protocol/
│   ├── messages.py                schémas pydantic de tous les messages (§12 + ADR)        [phase 2]
│   ├── adapter.py                 ProtocolAdapter : build / parse / validate, EXPECTED_INBOUND
│   └── PROTOCOL_INSTRUCTIONS.md   texte envoyé au modèle à l'init (ADR-004)
├── persistence/
│   ├── interface.py               ConversationStore (ABC)
│   ├── memory.py                  InMemoryConversationStore
│   └── sqlite_store.py            SqliteConversationStore (WAL, transactions)               [phase 3]
├── execution/
│   ├── platform.py                PlatformAdapter (posix / windows) : détection du shell, lancement par dialecte, spawn, terminate [phase 4]
│   ├── executor.py                CommandExecutor (ABC), SubprocessCommandExecutor, RawExecution
│   ├── payload_guard.py           PayloadGuard : apply(), fit_message(), serve_chunk()
│   ├── result_collector.py        ResultCollector : un execution_result par plan
│   └── plan_runner.py             PlanRunner : DAG, locks, workers, stop conditions, drain     [phase 5]
├── interruption/
│   └── handler.py                 InterruptionHandler (jeton = CancellationToken par session)   [phase 6]
├── transport/                     ADR-020 : providers choisis par configuration ; ADR-021 : codecs
│   ├── base.py                    TransportGateway (ABC), PostAck, GetResult, InFlightGuard, OP_*, validate_options  [phase 7]
│   ├── http_base.py               HttpProviderBase (template method httpx), HttpCall, InvalidResponseError
│   ├── registry.py                PluginRegistry (base générique) + TransportRegistry : register / names / resolve / create, entry points
│   ├── providers/
│   │   ├── generic_http.py        GenericHttpProvider = HttpTransportGateway (contrat ADR-004)
│   │   └── templated_http.py      TemplatedHttpProvider + TemplatedOptions (API décrite par options)
│   ├── codecs/                    ADR-021 : forme brute des réponses d'un modèle <-> enveloppes protocolaires
│   │   ├── base.py                MessageCodec (ABC), CodecError (UNPARSEABLE_REPLY), excerpt_of
│   │   ├── registry.py            CodecRegistry (PluginRegistry) : entry points agentic_local_app.codecs, CODEC_*
│   │   ├── decorator.py           CodecTransport (TransportGateway décoré), apply_codec
│   │   ├── passthrough.py         PassthroughCodec (« passthrough », identité, défaut)
│   │   ├── json_text.py           JsonTextCodec + JsonTextOptions (texte, prose, clôtures Markdown), extraction JSON
│   │   └── tool_call.py           ToolCallCodec + ToolCallOptions (arguments d'un appel d'outil)
│   ├── gateway.py                 façade de compatibilité : réexporte base.* et HttpTransportGateway
│   └── fake.py                    FakeTransportGateway (scénarios scriptés, éléments bruts admis), FakeTransportProvider (« fake »)
├── resilience/
│   ├── failure_manager.py         FailureManager : classify(), decide()                         [phase 7]
│   ├── retry_controller.py        RetryController : backoff borné déterministe
│   └── circuit_breaker.py         CircuitBreaker : CLOSED / OPEN / HALF_OPEN
├── context/
│   ├── window.py                  ContextWindowMonitor (octets, seuils, erreurs)                [phase 8]
│   ├── reducer.py                 ContextReducer : compose()/persist()/build(), paliers
│   └── rotation.py                RotationCoordinator : séquence ADR-014, PendingOutbound
├── observability/
│   ├── event_bus.py               EventBus, RecordingSubscriber
│   ├── audit_log.py               AuditLog (chaîne sha256), verify()                            [phase 10]
│   ├── execution_tracker.py       ExecutionTracker : snapshot §4.1 à deux niveaux
│   └── telemetry.py               TelemetryService : compteurs, histogrammes, export texte
├── orchestration/
│   ├── protocol_orchestrator.py   ProtocolOrchestrator : la boucle, budget, rotation             [phase 9]
│   ├── conversation_manager.py    ConversationManager : façade demandes / interruptions
│   ├── recovery.py                RecoveryCoordinator + RecoveryReport
│   └── wiring.py                  build_application(config) : assemble tout (injection)
├── interfaces/
│   ├── cli.py                     typer : run / serve / status / credentials / resume / config / transport / codec / shell / protocol / mock-server [phase 9]
│   └── http_api.py                FastAPI : REST + SSE (ADR-018)
└── testing/
    ├── fake_executor.py           FakeCommandExecutor (sorties, délais, annulation simulés)     [phase 4]
    └── mock_model_server.py       serveur FastAPI jouant le modèle à partir d'un scénario     [phase 7/9]

tests/
├── conftest.py                    fixtures partagées : clock, ids, store mémoire, bus enregistreur, config
├── unit/
│   ├── test_phase1_state_machines.py
│   ├── test_phase2_protocol.py
│   ├── test_phase3_persistence.py
│   ├── test_phase4_task_execution.py
│   ├── test_phase5_plan_execution.py
│   ├── test_phase6_interruption.py
│   ├── test_phase7_transport_failures.py
│   ├── test_phase7_providers.py   registre, HttpProviderBase, generic_http, templated_http, câblage (ADR-020)
│   ├── test_phase7_codecs.py      codecs, extraction JSON, CodecTransport, CodecRegistry, câblage, CLI (ADR-021)
│   ├── test_phase8_context_rotation.py
│   └── test_phase10_observability.py
└── integration/
    ├── test_phase9_orchestration.py
    └── test_phase9_orchestration_codec.py   la boucle à travers un codec : texte clôturé, UNPARSEABLE_REPLY, rotation
```

Un fichier de tests par phase peut être découpé en plusieurs (`test_phase7_transport.py`, `test_phase7_resilience.py`…) tant que le préfixe `test_phaseN_` et le marqueur `@pytest.mark.phaseN` sont conservés.

## 2. Règles de dépendance

```mermaid
flowchart TB
    IF[interfaces] --> ORC[orchestration]
    IF -. "transport list / show : registre seulement" .-> TR
    ORC --> LC[lifecycle] & PA[protocol] & EX[execution] & IH[interruption] & CX[context] & RS[resilience] & TR[transport]
    LC & PA & EX & IH & CX & RS & TR --> OBS[observability]
    LC & EX & CX & IH --> PS[persistence]
    LC & PA & EX & IH & CX & RS & TR & OBS & PS --> DOM[domain]
    CF[config] --> DOM
    IF & ORC & EX & TR & CX --> CF
```

1. `domain` ne dépend de rien d'autre que pydantic et la bibliothèque standard.
2. Personne n'importe `interfaces` ni `orchestration` en dehors d'eux-mêmes.
3. `persistence`, `execution.executor`, `transport` sont des **frontières** : une ABC + une implémentation réelle + un double. Le reste du code ne connaît que l'ABC. Pour `transport`, l'implémentation réelle est le **provider** choisi par `transport.provider` (ADR-020) : les providers dérivent de l'ABC (le plus souvent via `HttpProviderBase`), sont résolus par `TransportRegistry` (nom enregistré, `paquet.module:Classe`, entry point `agentic_local_app.transports`) et instanciés par `build_application` ; ajouter un provider ne modifie ni l'ABC ni les providers existants. Le **codec** choisi par `transport.codec` (ADR-021) est orthogonal au provider : `build_application` enveloppe le transport dans `CodecTransport` (lui-même `TransportGateway`) sauf pour `passthrough` ; un codec est pur et ne connaît ni le provider ni l'orchestrateur, qui ne le connaissent pas non plus.
4. Aucune horloge (`datetime.now`, `time.*`) ni aléa (`uuid4`, `random`) en dehors de `domain/clock.py`, `domain/ids.py` et du `jitter` optionnel de `RetryController` (test d'inspection en phase 10).
5. Toute transition d'état passe par `domain.transitions.assert_transition` puis est **persistée avant publication** (ADR-015).

## 3. Signatures publiques attendues

Les agents implémentent exactement ces surfaces (les paramètres optionnels peuvent être ajoutés). Les lignes des phases livrées reflètent les signatures effectives du code ; en cas de doute, le code fait foi.

| Classe | Méthodes publiques |
|---|---|
| `ConversationLifecycleManager(store, bus, clock, ids)` | `create_session(goal, user_message, user_id, budget, auto_close) -> SessionRecord` · `transition_session(session_id, to, *, reason=None, **updates) -> SessionRecord` · `create_conversation(session_id, *, parent_conversation_id=None, context_window_state=HEALTHY) -> ConversationRecord` · `transition_conversation(conversation_id, to, *, reason=None, **updates) -> ConversationRecord` · `transition_context_window(conversation_id, to, *, reason=None) -> ConversationRecord` · `update_conversation(conversation_id, **updates)` · `update_session(session_id, **updates)` · `interrupt_conversation(conversation_id, *, reason) -> ConversationRecord` · `get_session / get_conversation` |
| `ProtocolAdapter(config)` | `build_user_request(conversation, message_id, goal, user_message, budget)`, `build_execution_result(conversation, message_id, content)`, `build_context_resume_request(conversation, message_id, *, original_conversation_id, goal, context_summary, pending_message_type)`, `build_protocol_correction_request(conversation, message_id, *, error, expected, rejected_message_id, attempt, max_attempts)` (ADR-023, fait tenir sous `payload.max_message_bytes`) → `OutboundMessage(envelope, payload, canonical, size_bytes, message_type)` · `parse_inbound(raw_messages: list[dict], *, expected, conversation, known_message_ids, known_plan_ids, known_task_ids, stored_output_task_ids, expected_original_conversation_id=None) -> InboundMessage` · `expected_inbound(last_outbound: MessageRecord | None, conversation) -> frozenset[MessageType]` (table ADR-007 + drapeau `protocol.allow_direct_response` d'ADR-022, fonctions de module `expected_inbound_for(situation, *, allow_direct_response)` et `last_substantive_outbound(messages)`, qui saute les `protocol_correction_request` — une correction n'ouvre aucune ligne de la table, ADR-023) · `plan_to_records(inbound, *, session, conversation, cycle_id, clock) -> (PlanRecord, list[TaskRecord])` · `render_instructions(config, *, environment: ExecutionEnvironment \| None = None) -> str` (ADR-030 §3 : l'environnement annoncé, détecté depuis la configuration quand il n'est pas fourni ; ADR-031 : le contrat, dont les commandes d'exemple sont rendues dans le dialecte annoncé depuis `EXAMPLE_COMMANDS`) · validation des contenus en deux passes, la seconde en JSON strict (ADR-031 §4 : un entier ou un booléen écrit autrement est refusé, jamais converti) |
| `SqliteConversationStore(path)` | toute l'ABC `ConversationStore` |
| `CommandExecutor` (ABC) | `async execute(spec: CommandSpec, *, cancel: CancellationToken, on_output=None) -> RawExecution` |
| `PayloadGuard(config.payload)` | `apply(stdout: bytes, stderr: bytes, budget: int) -> TruncatedOutput` · `fit_message(result: ExecutionResultContent, max_message_bytes) -> ExecutionResultContent` · `serve_chunk(store, session_id, ref_task_id, stream, offset, max_bytes) -> ChunkResult` · `effective_budget(task, plan_default) -> int` |
| `ResultCollector(verdict_programs=None, translator=None)` | `build(plan: PlanRecord, tasks: list[TaskRecord], task_outputs: dict[str, TruncatedOutput], chunk_results: dict[str, ChunkResult]) -> ExecutionResultContent` (`execution`, `failure_is_verdict` et `translation` sont **dérivés** de l'enregistrement, jamais stockés — ADR-029 §4, ADR-030 §4) |
| `PlatformAdapter(config.execution, *, process_table=None, which=shutil.which, platform=None)` | `detect_shell(shell=None) -> DetectedShell` · `environment() -> ExecutionEnvironment` · `default_shell() -> str` · `build_launch(cmd, shell) -> LaunchSpec(program, args, dialect)` · `spawn_kwargs()` · `process_group_id(pid)` · `async terminate_gracefully(proc, pgid)` · `async kill(proc, pgid)` · `terminate_orphan(pid, pgid, started_at) -> bool` ; fonctions de module `select_platform(config, *, platform=None, process_table=None, which=None)`, `default_translator(config, *, platform=None, which=None)`, `powershell_script(cmd)`, `encode_powershell_command(cmd)` (ADR-003, ADR-016, ADR-030) |
| `ShellTranslator(target: ShellDialect, *, enabled=True)` | `translate(cmd) -> CommandTranslation \| None` (`None` = rien n'a été tenté) · propriétés `target`, `source`, `enabled` ; fonctions de module `rules_for(source)`, `describe_dictionary()`, `source_dialect_for(target)` ; données `TRANSLATION_RULES`, `REFUSED_PROGRAMS` (ADR-030 §4) |
| `PlanRunner(store, bus, executor, payload_guard, clock, ids, config, *, failure_manager=None, result_collector=None, scratch=None, translator=None)` | `async run(plan: PlanRecord, tasks: list[TaskRecord], session: SessionRecord, *, interrupt: CancellationToken) -> PlanOutcome(plan, tasks, execution_result \| None, interrupted, budget_exceeded, stop_reason)` |
| `InterruptionHandler(store, bus, lifecycle, clock, ids, config, *, transport=None)` | `token_for(session_id) -> CancellationToken` · `register_loop(session_id) -> asyncio.Event` · `loop_finished(session_id)` · `async interrupt(session_id, *, reason="user_interrupt") -> InterruptionReport` (borné par `interrupt_drain_timeout_ms`) · `is_interrupting(session_id)` · `raise_if_interrupted(session_id)` |
| `HttpTransportGateway(config.transport, clock, *, transport=None, sleep=asyncio.sleep)` (= `GenericHttpProvider`, ADR-020) | `async init_conversation(instructions, metadata) -> str` · `async post_message(remote_conversation_id, payload) -> PostAck` · `async get_messages(remote_conversation_id, after) -> GetResult` · `async wait_for_reply(remote_conversation_id, after) -> GetResult` (polling borné, `MODEL_GET_TIMEOUT`) · `async close_conversation(remote_conversation_id)` · `abandon()` |
| `HttpProviderBase(config.transport, clock, *, transport=None, sleep=asyncio.sleep)` (ABC, ADR-020) | toute l'ABC `TransportGateway` + points d'extension : `headers(operation) -> dict` · `build_init(instructions, metadata) -> HttpCall` · `parse_init(status, body) -> str` · `build_post(remote_conversation_id, payload) -> HttpCall` · `parse_post(status, body, *, payload) -> PostAck` · `build_get(remote_conversation_id, after) -> HttpCall` · `parse_get(status, body) -> GetResult` · `build_close(remote_conversation_id) -> HttpCall \| None` · `classify_error(operation, status, body, headers) -> TransportError` · `redact_url(url) -> str` · attribut de classe `options_model` |
| `TemplatedHttpProvider(config.transport, clock, *, transport=None, sleep=asyncio.sleep)` | `HttpProviderBase` piloté par `TemplatedOptions` (`options_model`) : `headers` communs, `init` / `post` / `get` / `close` (`method`, `url`, `headers`, `body`, `expected_statuses`, `conversation_id_path`, `accepted_path`, `message_id_path`, `messages_path`, `message_path`, `cursor_path`) |
| `TransportRegistry` (classe, état de processus ; `PluginRegistry[TransportGateway]`) | `register(name)` (décorateur de classe) · `names() -> list[str]` · `list_providers() -> list[PluginInfo]` · `resolve(spec) -> type[TransportGateway]` · `describe(spec) -> (PluginInfo, type)` · `validate(provider, options) -> BaseModel \| None` · `create(config, *, clock, **kwargs) -> TransportGateway` |
| `MessageCodec(options=None)` (ABC, ADR-021) | `decode_inbound(raw_messages: list[Any]) -> list[dict]` · `encode_outbound(payload: dict) -> Any` (défaut : identité) · `error(index, raw, reason, **details) -> CodecError` · attributs de classe `options_model`, `name` ; `CodecError(*, codec, index, excerpt, reason, **details)` = `TransportError(MODEL_PROTOCOL_ERROR, UNPARSEABLE_REPLY)` · `with_details(**more)` |
| `PassthroughCodec()` · `JsonTextCodec(JsonTextOptions)` · `ToolCallCodec(ToolCallOptions)` | les codecs intégrés (`passthrough`, `json_text`, `tool_call`) ; `json_text.py` expose aussi `find_json_spans(text)`, `extract_first_json(text)`, `strip_code_fence(text)`, `envelopes_of(...)`, `complete_envelope(...)` |
| `CodecTransport(inner: TransportGateway, codec: MessageCodec)` | toute l'ABC `TransportGateway` (encode au POST, décode au GET, délègue le reste) · `aclose()` · propriétés `inner`, `codec` ; `apply_codec(transport, codec) -> TransportGateway` (nu pour `passthrough`) |
| `CodecRegistry` (classe ; `PluginRegistry[MessageCodec]`) | `register(name)` · `names()` · `list_codecs() -> list[PluginInfo]` · `resolve(spec)` · `describe(spec)` · `validate(codec, options)` · `create(config) -> MessageCodec` |
| `FailureManager(config, store, bus, clock, ids, retry=None, breaker=None)` | `classify(exc) -> NormalizedError` · `decide(error, attempt, *, operation) -> Decision(kind: retry \| abort \| rotate \| fail, delay_ms, reason)` · `record(error, *, session_id, conversation_id=None, plan_id=None, task_id=None) -> FailureRecord` · `record_decision(...) -> RetryDecisionRecord` · `handle(exc, attempt, *, operation, session_id, ...) -> (NormalizedError, Decision)` · `note_success()` |
| `RetryController(config.retry)` | `delay_ms(attempt) -> int` · `can_retry(attempt) -> bool` |
| `CircuitBreaker(config.circuit_breaker, clock, bus)` | `allow() -> bool` · `record_success()` · `record_failure()` · `state` |
| `ContextWindowMonitor(config.context)` | `evaluate(conversation, *, projected_outbound_bytes=0, error=None) -> ContextWindowState` (monotone) · `should_rotate_on_unusable_reply(conversation, error) -> bool` (ADR-019 §2) · `thresholds()` · `account(conversation_bytes, message_bytes)` |
| `ContextReducer(config, store, clock, ids)` | `compose(session, source, *, pending_message_type, target_conversation_id) -> (payload, size, step)` (pur, lève `RotationFailedError`) · `persist(...) -> ContextSummaryRecord` · `build(...) -> ContextSummaryRecord` |
| `RotationCoordinator(config, store, bus, clock, ids, lifecycle, adapter, transport, reducer, monitor, instructions)` | `async rotate(session, source, pending: PendingOutbound(message_type, original_message_id, build)) -> RotationResult(child, source, summary, resume_cycle, retransmitted_message_id, ack_message_id)` |
| `AuditLog(store, clock, ids)` | `handle(event) -> AuditEvent \| None` (abonné critique via `subscribe(bus)`) · `verify(session_id, *, page_size=…) -> AuditVerification` · `recompute_hash(event)` · `last(session_id)` |
| `ExecutionTracker(store, clock)` | `handle(event)` · `subscribe(bus)` · `snapshot(session_id) -> RuntimeSnapshot` · `rebuild(session_id) -> RuntimeSnapshot` |
| `TelemetryService(clock)` | `handle(event)` · `subscribe(bus)` · `render_text() -> str` · `metrics() -> dict` · `reset()` |
| `ProtocolOrchestrator(...)` | `async run_session(session_id)` · `async continue_session(session_id, user_message)` |
| `ConversationManager(...)` | `async start_session(*, goal=None, user_message=None, budget=None, auto_close=None, working_space=None, skills=None, effort=None, user_id=None) -> SessionRecord` (ADR-026 : le dossier de l'utilisateur est validé puis lié ; ADR-027 §4 : `skills` / `effort` tracés dans `session.created` ; ADR-028 : `goal` et `user_message` appariés et facultatifs — sans eux la session reste `READY`, sans conversation et sans rien envoyer —, `user_id` par défaut égal à l'identité machine) · `async continue_session(session_id, user_message)` (ADR-028 : remplit le but d'une session `READY` qui n'en a pas) · `async resume_session(session_id)` (ADR-016, ADR-025) · `async interrupt(session_id)` · `async wait(session_id, *, timeout_ms=None)` · `snapshot(session_id)` · `final_answer(session_id)` · `user_responses(session_id)` / `last_reply(session_id)` (ADR-022, relus dans la table des messages) · `corrections(session_id)` (ADR-023) · `paused_reason(session_id)` (ADR-025 §7) · `recovery_report` |
| `RecoveryCoordinator(store, lifecycle, bus, executor_platform, clock)` | `recover() -> RecoveryReport` |

## 4. Doubles de test (§18.3)

| Frontière | Double | Où |
|---|---|---|
| shell | `FakeCommandExecutor` : sorties configurables par `cmd` ou par `task_id`, délai simulé (avec `FakeClock`), échec de spawn, blocage jusqu'à annulation, tranches de sortie live | `testing/fake_executor.py` |
| réseau | `FakeTransportGateway` : file de réponses scriptées par conversation (enveloppes, ou éléments bruts pour un transport décoré par un codec), erreurs injectables (type, code, HTTP), latence, `init` retournant un id déterministe ; `FakeTransportProvider` la rend sélectionnable par `transport.provider = "fake"` | `transport/fake.py` |
| base | `InMemoryConversationStore` (+ `fail_next_write`) | `persistence/memory.py` |
| temps / ids | `FakeClock`, `SequentialIdGenerator` | `domain/` |
| bus | `RecordingSubscriber` | `observability/event_bus.py` |

Fixtures communes dans `tests/conftest.py` : `clock`, `ids`, `store`, `bus`, `recorder` (abonné à tout), `config` (défauts + `data_dir` temporaire), `lifecycle`.
