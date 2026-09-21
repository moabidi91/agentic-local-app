"""Phase 2 — the protocol contract the model receives (ADR-031, on ADR-004, ADR-022, ADR-023,
ADR-029 and ADR-030).

``PROTOCOL_INSTRUCTIONS.md`` is the whole contract between the application and the model, and its
examples are what a model copies first. Two families of tests make the text impossible to drift
from the code.

1. **The examples are replayed.** Every fenced block of the *rendered* text — for a POSIX, a
   PowerShell, a ``cmd`` and an unrecognised shell, under several configurations — carries a label
   in its fence line (``json A2``, ``json M5 after A5 refused DUPLICATE_MESSAGE_ID``,
   ``json fragment translation``). The replay runs each one through the real ``ProtocolAdapter`` in
   the situation the text places it in: a message the model sends is accepted; a message marked
   ``refused`` is refused with exactly the code the text gives; a message the model receives
   validates against its content model, in the very form the application serialises. The
   application's own messages are then rebuilt by the real components — the results by the
   ``ResultCollector`` after the real truncation, the summary of a rotation by the
   ``ContextReducer``, the correction by the adapter — and must equal what the text shows.
2. **The dictionary is compared with the models.** Every table of section 2 lists exactly the
   fields of its pydantic model, with the right JSON type, required-ness, default and allowed
   values; the ``context_summary`` table lists the keys the reducer writes; the error-code table
   the codes the adapter can refuse with.

Around them: the dialect of the examples, the rules stated first and last, the size of the text and
what it leaves of the context budget.
"""

from __future__ import annotations

import json
import re
import types
import typing
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from functools import cache
from typing import Any

import pytest
from pydantic import BaseModel
from pydantic_core import PydanticUndefined

from agentic_local_app.config import AppConfig, ExecutionSection, PayloadSection, ProtocolSection
from agentic_local_app.context.reducer import ContextReducer
from agentic_local_app.context.window import ContextWindowMonitor
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.commands import VerdictPrograms
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.errors import NormalizedError, ProtocolError
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    BlobRecord,
    ConversationRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.shell import (
    DetectedShell,
    ExecutionEnvironment,
    ShellDialect,
    ShellSource,
)
from agentic_local_app.domain.states import (
    CONCLUDING_MESSAGE_TYPES,
    OUTBOUND_MESSAGE_TYPES,
    PLAN_MESSAGE_TYPES,
    ContextWindowState,
    ConversationState,
    MessageType,
    OutputStream,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.execution.payload_guard import PayloadGuard
from agentic_local_app.execution.result_collector import ResultCollector
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol import adapter as adapter_module
from agentic_local_app.protocol.adapter import (
    EXAMPLE_COMMANDS,
    InboundMessage,
    OutboundSituation,
    ProtocolAdapter,
    expected_inbound_for,
    peek_field,
    render_instructions,
)
from agentic_local_app.protocol.messages import (
    ContextResumeAckContent,
    ContextResumeRequestContent,
    Envelope,
    ExecutionResultContent,
    FinalAnswerContent,
    PlanContent,
    ProtocolCorrectionRequestContent,
    SessionBudgetContent,
    StateSummary,
    SystemErrorContent,
    TaskMessage,
    TaskRef,
    TaskResult,
    TaskTranslation,
    UserRequestContent,
    UserResponseContent,
    content_model_for,
)
from agentic_local_app.transport.codecs import CodecError, JsonTextCodec, ToolCallCodec

pytestmark = pytest.mark.phase2

T0 = datetime(2026, 9, 21, 9, 0, tzinfo=UTC)

#: The size the contract must stay under (ADR-031 §6): it is sent again at every rotation.
MAX_CONTRACT_BYTES = 60 * 1024

ENVIRONMENTS: dict[str, ExecutionEnvironment] = {
    "posix": ExecutionEnvironment(
        operating_system="Linux",
        shell=DetectedShell.of("/usr/bin/bash", ShellSource.DETECTED),
        cwd="/home/dev/billing",
    ),
    "powershell": ExecutionEnvironment(
        operating_system="Windows",
        shell=DetectedShell.of("C:\\Program Files\\PowerShell\\7\\pwsh.exe", ShellSource.DETECTED),
        cwd="C:\\work\\billing",
    ),
    "cmd": ExecutionEnvironment(
        operating_system="Windows",
        shell=DetectedShell.of("cmd", ShellSource.CONFIGURED),
        cwd="C:\\work\\billing",
    ),
    "unknown": ExecutionEnvironment(
        operating_system="Linux",
        shell=DetectedShell.of("fish", ShellSource.CONFIGURED),
        cwd="/home/dev/billing",
    ),
}

CONFIGS: dict[str, AppConfig] = {
    "default": AppConfig(),
    "strict-first-reply": AppConfig(protocol=ProtocolSection(allow_direct_response=False)),
    "corrections-off": AppConfig(protocol=ProtocolSection(max_correction_attempts=0)),
    "verdicts-off": AppConfig(execution=ExecutionSection(verdict_programs=[])),
    "translation-off": AppConfig(execution=ExecutionSection(translate_commands=False)),
    "custom-limits": AppConfig(
        payload=PayloadSection(
            default_max_output_bytes=1111,
            hard_max_output_bytes=22222,
            max_message_bytes=133333,
            max_state_summary_bytes=4444,
        ),
        execution=ExecutionSection(default_task_timeout_ms=55555, max_task_timeout_ms=666666),
    ),
}

#: Every (machine, configuration) the replay covers: the four dialects under the default
#: configuration, and every configuration on the two machines the contract must serve.
RENDERINGS: list[tuple[str, str]] = sorted(
    {(machine, "default") for machine in ENVIRONMENTS}
    | {(machine, name) for machine in ("posix", "powershell") for name in CONFIGS}
)


@cache
def rendered(machine: str, config_name: str = "default") -> str:
    return render_instructions(CONFIGS[config_name], environment=ENVIRONMENTS[machine])


# =============================================================================================
# Blocks: every fence of the rendered text, with the label of its fence line
# =============================================================================================

_FENCE = re.compile(r"^```(?P<lang>[a-z]+)(?P<info>[^\n]*)\n(?P<body>.*?)^```[ \t]*$", re.M | re.S)
_LABEL = re.compile(r"^[A-Z]\d+$")


@dataclass(frozen=True)
class Block:
    """One fenced block: ``json <label> [after <label>] [refused <CODE>]`` or ``json fragment <kind>``."""

    lang: str
    body: str
    start: int
    end: int
    label: str | None = None
    after: str | None = None
    refused: str | None = None
    fragment: str | None = None

    @property
    def data(self) -> Any:
        return json.loads(self.body)


def blocks_of(text: str) -> list[Block]:
    blocks: list[Block] = []
    for match in _FENCE.finditer(text):
        block = Block(match["lang"], match["body"], match.start(), match.end())
        words = match["info"].split()
        if words[:1] == ["fragment"]:
            assert len(words) == 2, f"a fragment names its kind: {match['info']!r}"
            block = replace(block, fragment=words[1])
        elif words:
            label, rest = words[0], words[1:]
            assert _LABEL.match(label), f"not a label: {label!r}"
            marks: dict[str, str] = {}
            while rest:
                assert len(rest) >= 2 and rest[0] in ("after", "refused"), match["info"]
                marks[rest[0]] = rest[1]
                rest = rest[2:]
            block = replace(
                block, label=label, after=marks.get("after"), refused=marks.get("refused")
            )
        blocks.append(block)
    return blocks


def labelled(text: str) -> dict[str, Block]:
    found: dict[str, Block] = {}
    for block in blocks_of(text):
        if block.label is not None:
            assert block.label not in found, f"label {block.label} used twice"
            found[block.label] = block
    return found


# =============================================================================================
# The replay: the situation of every example, and what the real adapter makes of it
# =============================================================================================


@dataclass(frozen=True)
class State:
    """What the application knows when a message arrives — exactly what ``parse_inbound`` needs."""

    conversation: str | None = None
    concluded: frozenset[str] = frozenset()
    situation: OutboundSituation | None = None
    message_ids: frozenset[str] = frozenset()
    plan_ids: frozenset[str] = frozenset()
    task_ids: frozenset[str] = frozenset()
    #: the accepted plan whose ``execution_result`` is awaited
    pending_plan: InboundMessage | None = None
    #: the resume request being acknowledged: its ``original_conversation_id`` and pending type
    original: str | None = None
    pending_type: str | None = None
    #: the refusal the previous message got, for the correction request that answers it
    refusal: tuple[NormalizedError, frozenset[MessageType], str | None] | None = None


def conversation_record(state: State) -> ConversationRecord:
    return ConversationRecord(
        conversation_id="conv-local",
        session_id="sess-contract",
        remote_conversation_id=state.conversation,
        status=ConversationState.ACTIVE,
        auto_close_on_final_answer=False,
        final_answer_received=state.conversation in state.concluded,
        created_at=T0,
        updated_at=T0,
    )


@dataclass
class Replay:
    """Evaluates the labelled blocks of one rendering, each in the state its text gives it."""

    text: str
    config: AppConfig
    states: dict[str, State] = field(default_factory=dict)
    refusals: dict[str, ProtocolError] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.adapter = ProtocolAdapter(self.config)
        self.blocks = labelled(self.text)

    def run(self) -> None:
        for label in self.blocks:
            self.state_after(label)

    def state_after(self, label: str) -> State:
        if label not in self.states:
            block = self.blocks[label]
            self.states[label] = self._apply(block, self._state_before(block))
        return self.states[label]

    def _state_before(self, block: Block) -> State:
        assert block.label is not None
        if block.after is not None:
            assert block.after in self.blocks, f"{block.label}: after an unknown {block.after}"
            return self.state_after(block.after)
        previous = f"{block.label[0]}{int(block.label[1:]) - 1}"
        return self.state_after(previous) if previous in self.blocks else State()

    def expected(self, state: State) -> frozenset[MessageType]:
        if state.situation is None:
            return frozenset()
        return expected_inbound_for(
            state.situation, allow_direct_response=self.config.protocol.allow_direct_response
        )

    def parse(self, raw: list[Any], state: State) -> InboundMessage:
        return self.adapter.parse_inbound(
            raw,
            expected=self.expected(state),
            conversation=conversation_record(state),
            known_message_ids=set(state.message_ids),
            known_plan_ids=set(state.plan_ids),
            known_task_ids=set(state.task_ids),
            stored_output_task_ids=set(state.task_ids),
            expected_original_conversation_id=(
                state.original
                if state.situation is OutboundSituation.CONTEXT_RESUME_REQUEST
                else None
            ),
        )

    # ------------------------------------------------------------------------------------------
    def _apply(self, block: Block, state: State) -> State:
        if block.lang == "text":
            return self._prose_wrapped(block, state)
        assert block.lang == "json", block.label
        data = block.data
        if (
            isinstance(data, dict)
            and data.get("type") in {m.value for m in OUTBOUND_MESSAGE_TYPES}
            and block.refused is None
        ):
            return self._receive(block, data, state)
        return self._send(block, data if isinstance(data, list) else [data], state)

    def _receive(self, block: Block, data: dict[str, Any], state: State) -> State:
        envelope = Envelope.model_validate(data)
        model = content_model_for(envelope.type)
        content = model.model_validate(envelope.content)
        # exactly the form the application puts on the wire (ProtocolAdapter._outbound)
        assert content.model_dump(mode="json", exclude_none=True) == envelope.content, block.label
        assert envelope.message_id not in state.message_ids, f"{block.label}: id reused"
        state = replace(state, message_ids=state.message_ids | {envelope.message_id})
        match envelope.type:
            case MessageType.USER_REQUEST:
                situation = (
                    OutboundSituation.FOLLOW_UP_USER_REQUEST
                    if envelope.conversation_id in state.concluded
                    else OutboundSituation.INITIAL_USER_REQUEST
                )
                return replace(
                    state, conversation=envelope.conversation_id, situation=situation, refusal=None
                )
            case MessageType.EXECUTION_RESULT:
                assert isinstance(content, ExecutionResultContent)
                assert envelope.conversation_id == state.conversation, block.label
                plan = state.pending_plan
                assert plan is not None, f"{block.label}: a result with no plan outstanding"
                assert isinstance(plan.content, PlanContent)
                assert content.plan_id == plan.content.plan_id, block.label
                declared = [task.task_id for task in plan.content.tasks]
                reported = [result.task_id for result in content.results] + [
                    ref.task_id
                    for ref in (
                        *content.skipped_tasks,
                        *content.cancelled_tasks,
                        *content.interrupted_tasks,
                    )
                ]
                assert sorted(reported, key=declared.index) == declared, block.label
                results = [result.task_id for result in content.results]
                assert results == sorted(results, key=declared.index), "declaration order"
                return replace(
                    state,
                    situation=OutboundSituation.EXECUTION_RESULT,
                    pending_plan=None,
                    refusal=None,
                )
            case MessageType.CONTEXT_RESUME_REQUEST:
                assert isinstance(content, ContextResumeRequestContent)
                assert envelope.conversation_id != state.conversation, "a new conversation"
                assert content.original_conversation_id == state.conversation, block.label
                return replace(
                    state,
                    conversation=envelope.conversation_id,
                    situation=OutboundSituation.CONTEXT_RESUME_REQUEST,
                    original=content.original_conversation_id,
                    pending_type=content.pending_message_type,
                    refusal=None,
                )
            case MessageType.PROTOCOL_CORRECTION_REQUEST:
                assert isinstance(content, ProtocolCorrectionRequestContent)
                assert envelope.conversation_id == state.conversation, block.label
                assert state.refusal is not None, f"{block.label}: no refused message to correct"
                # transparent for the expectation (ADR-023 §5): the situation does not move
                return replace(state, refusal=None)
            case other:  # pragma: no cover - the four outbound types are handled above
                raise AssertionError(f"{block.label}: {other} is not sent by the application")

    def _send(self, block: Block, raw: list[Any], state: State) -> State:
        try:
            inbound = self.parse(raw, state)
        except ProtocolError as exc:
            code = exc.error.error_code
            assert block.refused == code, (
                f"{block.label} is refused with {code} ({exc.error.details}); "
                f"the text says {block.refused or 'accepted'}"
            )
            self.refusals[block.label or ""] = exc
            # the refused reply is persisted under its readable, fresh message_id (ADR-023)
            first = raw[0] if raw else None
            readable = peek_field(first, "message_id")
            taken = readable if isinstance(readable, str) and readable else None
            if taken is not None and taken in state.message_ids:
                taken = None
            return replace(
                state,
                message_ids=state.message_ids | ({taken} if taken else set()),
                refusal=(exc.error, self.expected(state), taken),
            )
        assert block.refused is None, f"{block.label} is accepted; the text says {block.refused}"
        envelope = inbound.envelope
        state = replace(state, message_ids=state.message_ids | {envelope.message_id}, refusal=None)
        if inbound.message_type in PLAN_MESSAGE_TYPES:
            plan = inbound.content
            assert isinstance(plan, PlanContent)
            return replace(
                state,
                plan_ids=state.plan_ids | {plan.plan_id},
                task_ids=state.task_ids | {task.task_id for task in plan.tasks},
                pending_plan=inbound,
                situation=None,
            )
        if inbound.message_type in CONCLUDING_MESSAGE_TYPES:
            return replace(
                state, concluded=state.concluded | {envelope.conversation_id}, situation=None
            )
        # context_resume_ack: the pending message is re-sent in the new conversation (ADR-014)
        assert inbound.message_type is MessageType.CONTEXT_RESUME_ACK
        resent = (
            OutboundSituation.EXECUTION_RESULT
            if state.pending_type == MessageType.EXECUTION_RESULT.value
            else OutboundSituation.INITIAL_USER_REQUEST
        )
        return replace(state, situation=resent, original=None, pending_type=None)

    def _prose_wrapped(self, block: Block, state: State) -> State:
        """A reply that is not a bare envelope: what each way of reading replies makes of it."""
        assert block.refused == "UNPARSEABLE_REPLY", block.label
        # read as pure JSON — the arguments of a tool call: no envelope can be read at all
        with pytest.raises(CodecError) as unreadable:
            ToolCallCodec().decode_inbound([{"arguments": block.body}])
        assert unreadable.value.error.error_code == "UNPARSEABLE_REPLY"
        # read as a message object (passthrough): a string is not an envelope
        with pytest.raises(ProtocolError) as not_an_envelope:
            self.parse([block.body], state)
        assert not_an_envelope.value.error.error_code == "SCHEMA_INVALID"
        # read as text carrying JSON (json_text): the envelope survives, the prose is thrown away
        embedded = next(line for line in block.body.splitlines() if line.startswith("{"))
        assert JsonTextCodec().decode_inbound([block.body]) == [json.loads(embedded)]
        return state


def replayed(machine: str, config_name: str) -> Replay:
    replay = Replay(rendered(machine, config_name), CONFIGS[config_name])
    replay.run()
    return replay


# =============================================================================================
# 1. The examples, replayed
# =============================================================================================


@pytest.mark.parametrize(("machine", "config_name"), RENDERINGS)
def given_rendered_contract_when_blocks_read_then_every_json_block_is_labelled_and_parses(
    machine: str, config_name: str
) -> None:
    blocks = blocks_of(rendered(machine, config_name))
    json_blocks = [block for block in blocks if block.lang == "json"]
    for block in json_blocks:
        assert block.label is not None or block.fragment is not None, block.body[:80]
        block.data  # noqa: B018 - it must parse
    # the unlabelled text blocks are the grammar tree and the budget formula, nothing else
    plain = [block for block in blocks if block.lang == "text" and block.label is None]
    assert len(plain) == 2
    labels = labelled(rendered(machine, config_name))
    assert {"A1", "A2", "A3", "A4", "A5", "A6", "B1", "B2", "C1", "C2", "C3", "C4"} <= set(labels)
    assert {"P1", "K1", "R1", "R2"} <= set(labels)
    assert sum(1 for label in labels if label.startswith("M")) >= 10


@pytest.mark.parametrize(("machine", "config_name"), RENDERINGS)
def given_rendered_contract_when_examples_replayed_then_each_is_accepted_or_refused_as_written(
    machine: str, config_name: str
) -> None:
    replay = replayed(machine, config_name)
    assert set(replay.states) == set(replay.blocks)
    refused = {label for label, block in replay.blocks.items() if block.refused}
    assert set(replay.refusals) == refused - {"M3"}  # M3 is read by the codecs, see _prose_wrapped


def given_rendered_contract_when_messages_listed_then_every_type_is_exemplified() -> None:
    blocks = labelled(rendered("posix"))
    shown = {
        block.data["type"]
        for block in blocks.values()
        if block.lang == "json" and isinstance(block.data, dict) and not block.refused
    }
    exchanged = {m.value for m in MessageType} - {"chunk_request", "system_error"}
    assert shown == exchanged
    assert "system_error" not in rendered("posix").split("## 3.")[1].split("## 4.")[0]


def given_refused_example_when_read_then_the_text_before_it_names_the_code_it_gets() -> None:
    text = rendered("posix")
    blocks = blocks_of(text)
    for previous, block in zip(blocks, blocks[1:], strict=False):
        if block.refused is None:
            continue
        prose = text[previous.end : block.start]
        assert f"`{block.refused}`" in prose, f"{block.label}: the text does not say why"
        if block.lang == "text":  # read by the codecs: the text names every outcome it can get
            assert "`SCHEMA_INVALID`" in prose and "discarded unread" in prose
        assert "Right:" in text[block.end :].split("```")[0] or block.label == "C2"


def given_correction_walkthrough_when_replayed_then_the_fixed_message_is_the_refused_one_repaired() -> (
    None
):
    blocks = labelled(rendered("powershell"))
    refused, fixed = blocks["C2"].data, blocks["C4"].data
    assert (
        refused["content"]["expects_reply"] == "yes" and fixed["content"]["expects_reply"] is True
    )
    assert fixed["message_id"] != refused["message_id"]
    assert {**refused["content"], "expects_reply": True} == fixed["content"]


# =============================================================================================
# 2. The application's messages, rebuilt by the real components
# =============================================================================================


def _records(replay: Replay, plan_label: str) -> tuple[PlanRecord, dict[str, TaskRecord]]:
    """The accepted plan of ``plan_label`` projected as the orchestrator projects it."""
    state = replay.state_after(plan_label)
    assert state.pending_plan is not None
    plan, tasks = replay.adapter.plan_to_records(
        state.pending_plan,
        session=_session(replay.blocks["A1"].data),
        conversation=conversation_record(state),
        cycle_id="cyc-contract",
        clock=FakeClock(T0),
    )
    return plan, {task.task_id: task for task in tasks}


def _session(request: dict[str, Any], **counters: Any) -> SessionRecord:
    content = UserRequestContent.model_validate(request["content"])
    budget = content.session_budget
    return SessionRecord(
        session_id="sess-contract",
        status=SessionState.RUNNING,
        goal=content.goal,
        user_message=content.user_message,
        user_id="u",
        auto_close_on_final_answer=False,
        budget=SessionBudget(
            max_cycles=budget.max_cycles,
            max_plans=budget.max_plans,
            max_total_duration_ms=budget.max_total_duration_ms,
        ),
        started_at=T0,
        created_at=T0,
        updated_at=T0,
        **counters,
    )


def _ran(
    replay: Replay, plan_label: str, result_label: str
) -> tuple[PlanRecord, list[TaskRecord], dict[str, Any]]:
    """The records of ``plan_label`` once its tasks ended as ``result_label`` reports, the kept
    output recomputed by the real truncation from streams of the reported sizes."""
    plan, tasks = _records(replay, plan_label)
    result = ExecutionResultContent.model_validate(replay.blocks[result_label].data["content"])
    guard = PayloadGuard(replay.config.payload)
    outputs = {}
    ended = []
    for reported in result.results:
        task = tasks[reported.task_id]
        assert reported.stdout_total is not None and reported.stderr_total is not None
        stdout, stderr = reported.stdout.encode(), reported.stderr.encode()
        # the complete streams: anything before what was kept, then what was kept
        full_stdout = b"." * (reported.stdout_total - len(stdout)) + stdout
        full_stderr = b"." * (reported.stderr_total - len(stderr)) + stderr
        kept = guard.apply(full_stdout, full_stderr, task.max_output_bytes_applied or 0)
        outputs[task.task_id] = kept
        ended.append(
            task.model_copy(
                update={
                    "status": TaskState(reported.status.upper()),
                    "exit_code": reported.exit_code,
                    "duration_ms": reported.duration_ms,
                    "timed_out": reported.timed_out,
                    "reason": reported.reason,
                    "truncated": kept.truncated,
                    "original_size_bytes": kept.original_size_bytes,
                    "stdout_total": kept.stdout_total,
                    "stderr_total": kept.stderr_total,
                    "stdout_range": kept.stdout_range,
                    "stderr_range": kept.stderr_range,
                }
            )
        )
    finished = plan.model_copy(
        update={"status": PlanState(result.status.upper()), "stop_reason": result.stop_reason}
    )
    return finished, ended, outputs


@pytest.mark.parametrize(("machine", "config_name"), RENDERINGS)
@pytest.mark.parametrize(("plan_label", "result_label"), [("A2", "A3"), ("A4", "A5")])
def given_example_results_when_rebuilt_by_the_collector_then_identical_to_the_text(
    machine: str, config_name: str, plan_label: str, result_label: str
) -> None:
    replay = replayed(machine, config_name)
    plan, tasks, outputs = _ran(replay, plan_label, result_label)
    collector = ResultCollector(
        VerdictPrograms(replay.config.execution.verdict_programs),
        ShellTranslator(
            ENVIRONMENTS[machine].dialect, enabled=replay.config.execution.translate_commands
        ),
    )
    built = collector.build(plan, tasks, outputs, {})
    shown = replay.blocks[result_label].data["content"]
    assert built.model_dump(mode="json", exclude_none=True) == shown


def given_build_example_when_rendered_then_its_truncation_is_the_one_the_text_explains() -> None:
    content = labelled(rendered("posix"))["A5"].data["content"]
    build = content["results"][0]
    assert build["truncated"] is True and build["execution"] == "ran"
    assert build["failure_is_verdict"] is True  # mvn is a verdict program by default
    start, end = build["stdout_range"]
    assert end == build["stdout_total"] and end - start == len(build["stdout"].encode())
    assert f"`[{start}, {end}]`" in rendered("posix")  # section 7.1 reads this very range
    assert end - start == build["max_output_bytes_applied"]  # a stream alone gets the whole budget


def given_verdicts_off_when_rendered_then_the_build_example_carries_no_verdict() -> None:
    text = rendered("posix", "verdicts-off")
    build = labelled(text)["A5"].data["content"]["results"][0]
    assert "failure_is_verdict" not in build
    assert "recognises no program" in text
    assert "`t4` ran and failed with exit code 1" in text


@pytest.mark.parametrize("machine", ["posix", "powershell", "cmd"])
def given_rotation_example_when_rebuilt_by_the_reducer_then_identical_to_the_text(
    machine: str,
) -> None:
    replay = replayed(machine, "default")
    request = replay.blocks["R1"].data
    summary = request["content"]["context_summary"]
    store = InMemoryConversationStore()
    session = _session(
        replay.blocks["A1"].data,
        consumed_cycles=summary["budget"]["consumed_cycles"],
        consumed_plans=summary["budget"]["consumed_plans"],
    )
    source = conversation_record(replay.state_after("A4"))
    store.save_session(session)
    store.save_conversation(source)
    for plan_label, result_label in (("A2", "A3"), ("A4", "A5")):
        plan, tasks, outputs = _ran(replay, plan_label, result_label)
        store.save_plan(plan)
        store.save_tasks(tasks)
        for task in tasks:
            for stream, total in (
                (OutputStream.STDOUT, task.stdout_total),
                (OutputStream.STDERR, task.stderr_total),
            ):
                store.save_blob(
                    BlobRecord(
                        blob_id=f"blob-{task.task_id}-{stream.value}",
                        session_id=session.session_id,
                        task_id=task.task_id,
                        blob_type=stream,
                        content=b"." * (total or 0),
                        size_bytes=total or 0,
                        created_at=T0,
                    )
                )
        assert outputs  # the truncation above is the one the store now describes
    clock = FakeClock(T0 + timedelta(milliseconds=summary["budget"]["consumed_duration_ms"]))
    reducer = ContextReducer(replay.config, store, clock, SequentialIdGenerator())
    draft = reducer.compose(session, source, pending_message_type=MessageType.EXECUTION_RESULT)
    assert draft.reduction_step == 0
    assert draft.payload == summary
    assert request["content"]["goal"] == session.goal
    assert request["content"]["pending_message_type"] == "execution_result"


@pytest.mark.parametrize("config_name", list(CONFIGS))
def given_correction_example_when_rebuilt_by_the_adapter_then_identical_to_the_text(
    config_name: str,
) -> None:
    replay = replayed("powershell", config_name)
    shown = replay.blocks["C3"].data
    state = replay.state_after("C2")
    assert state.refusal is not None
    error, expected, rejected = state.refusal
    attempts = replay.config.protocol.max_correction_attempts
    real = replay.adapter.build_protocol_correction_request(
        conversation_record(state),
        shown["message_id"],
        error=error,
        expected=expected,
        rejected_message_id=rejected,
        attempt=1,
        max_attempts=attempts or ProtocolSection().max_correction_attempts,
    )
    content = dict(shown["content"])
    shortened = content.pop("reminder")
    assert shortened.endswith(" [...]")
    assert real.payload["content"]["reminder"].startswith(shortened.removesuffix(" [...]"))
    assert {k: v for k, v in real.payload["content"].items() if k != "reminder"} == content
    assert real.payload["conversation_id"] == shown["conversation_id"]


@pytest.mark.parametrize("machine", list(ENVIRONMENTS))
def given_translation_fragment_when_rendered_then_the_dictionary_computed_it_for_this_machine(
    machine: str,
) -> None:
    (fragment,) = [block for block in blocks_of(rendered(machine)) if block.fragment]
    assert fragment.fragment == "translation"
    translation = TaskTranslation.model_validate(fragment.data)
    assert translation.model_dump(mode="json", exclude_none=True) == fragment.data
    target = ShellDialect.POSIX if machine == "posix" else ShellDialect.POWERSHELL
    assert translation.to_dialect == target.value and translation.status == "translated"
    decided = ShellTranslator(target).translate(translation.original_cmd)
    assert decided is not None and decided.executed == translation.executed_cmd
    assert list(decided.rules) == translation.rules


# =============================================================================================
# 3. The field dictionary against the models
# =============================================================================================

#: The heading of each dictionary table, and the model whose fields it must list exactly.
DICTIONARY: dict[str, type[BaseModel]] = {
    "### 2.1 The envelope — every message": Envelope,
    "#### Plan content — `discovery_plan`, `execution_plan`, `priority_clarification`": PlanContent,
    "#### Task — an object of `tasks`": TaskMessage,
    "#### `state_summary`": StateSummary,
    "#### `final_answer` content": FinalAnswerContent,
    "#### `user_response` content": UserResponseContent,
    "#### `context_resume_ack` content": ContextResumeAckContent,
    "#### `user_request` content": UserRequestContent,
    "#### `session_budget`": SessionBudgetContent,
    "#### `execution_result` content": ExecutionResultContent,
    "#### Task result — an object of `results`": TaskResult,
    "#### Task reference — an object of `skipped_tasks`, `cancelled_tasks`, `interrupted_tasks`": (
        TaskRef
    ),
    "#### `translation` — in a task result": TaskTranslation,
    "#### `context_resume_request` content": ContextResumeRequestContent,
    "#### `protocol_correction_request` content": ProtocolCorrectionRequestContent,
}
#: The tables of the messages the model sends: they also give required-ness and defaults.
SENT = {
    Envelope,
    PlanContent,
    TaskMessage,
    StateSummary,
    FinalAnswerContent,
    UserResponseContent,
    ContextResumeAckContent,
}


@dataclass(frozen=True)
class Row:
    fields: tuple[str, ...]
    cells: tuple[str, ...]


def table_after(text: str, heading: str) -> list[Row]:
    """The first table following ``heading`` (a line of its own): one row per data line."""
    lines = text.split(f"\n{heading}\n", 1)[1].splitlines()
    start = next(index for index, line in enumerate(lines) if line.startswith("|"))
    rows: list[Row] = []
    for line in lines[start + 2 :]:  # the header and its rule
        if not line.startswith("|"):
            break
        cells = tuple(cell.strip() for cell in line.strip()[1:-1].split("|"))
        rows.append(Row(tuple(re.findall(r"`([a-z_]+)`", cells[0])), cells))
    return rows


def _json_type(annotation: Any) -> str:
    """The JSON type word the dictionary uses for a Python annotation."""
    arguments = [a for a in typing.get_args(annotation) if a is not type(None)]
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        return _json_type(arguments[0])
    if origin is typing.Literal:
        return "string"
    if origin in (list, tuple):
        return "list"
    if (
        origin is dict
        or annotation is dict
        or (isinstance(annotation, type) and issubclass(annotation, BaseModel))
    ):
        return "object"
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return "string"
    return {str: "string", int: "integer", bool: "boolean"}[annotation]


def _allowed(annotation: Any) -> list[str] | None:
    """The values of an enumerated field (an ``Enum`` or a ``Literal``), else ``None``."""
    arguments = [a for a in typing.get_args(annotation) if a is not type(None)]
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        return _allowed(arguments[0])
    if origin is typing.Literal:
        return [str(value) for value in arguments]
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return [str(member.value) for member in annotation]
    return None


@pytest.mark.parametrize("heading", list(DICTIONARY))
def given_dictionary_table_when_compared_with_its_model_then_same_fields_types_and_rules(
    heading: str,
) -> None:
    model = DICTIONARY[heading]
    text = rendered("posix")
    rows = table_after(text, heading)
    listed = [name for row in rows for name in row.fields]
    assert len(listed) == len(set(listed)), f"{heading}: a field listed twice"
    assert set(listed) == set(model.model_fields), (
        f"{heading}: {set(listed) ^ set(model.model_fields)}"
    )
    for row in rows:
        if not row.cells[1]:  # a compact row of a received message, described in its section
            assert model not in SENT, f"{heading}: {row.fields} has no type"
            continue
        annotations = {model.model_fields[name].annotation for name in row.fields}
        kinds = {_json_type(annotation) for annotation in annotations}
        assert len(kinds) == 1, f"{heading}: {row.fields} mix JSON types"
        assert row.cells[1].startswith(kinds.pop()), f"{heading}: {row.fields} {row.cells[1]}"
        if model not in SENT:
            continue
        (name,) = row.fields
        info = model.model_fields[name]
        required = row.cells[2]
        assert required.startswith("yes") is info.is_required(), f"{name}: {required}"
        default = info.get_default(call_default_factory=True)
        if default is not None and default is not PydanticUndefined:
            written = json.dumps(default.value if isinstance(default, Enum) else default)
            assert f"`{written.strip(chr(34))}`" in required, f"{name}: default {written}"
        allowed = _allowed(info.annotation)
        if allowed is not None and model is not Envelope:
            assert re.findall(r"`([a-z_]+)`", row.cells[3]) == allowed, f"{name}: {row.cells[3]}"


def given_context_summary_table_when_compared_with_the_reducer_then_same_keys() -> None:
    text = rendered("posix")
    listed = [name for row in table_after(text, "#### `context_summary`") for name in row.fields]
    written = labelled(text)["R1"].data["content"]["context_summary"]  # rebuilt by the reducer
    assert listed == list(written)


def given_error_code_table_when_compared_with_the_adapter_then_every_refusal_code_is_explained() -> (
    None
):
    text = rendered("posix")
    section = text.split("#### `protocol_correction_request` content", 1)[1].split("## 3.")[0]
    table = section.split("| `error_code` | Your message… |", 1)[1]
    listed = set(re.findall(r"`([A-Z_]+)`", table))
    assert listed == set(adapter_module._CORRECTION_HINTS)


def given_dictionary_when_read_then_every_message_the_model_sends_or_receives_has_its_table() -> (
    None
):
    covered = {model for model in DICTIONARY.values()}
    # every content model of a message type (chunk_request is a task type, not a message)
    contents = {content_model_for(m) for m in MessageType if m is not MessageType.CHUNK_REQUEST}
    assert contents - covered == {SystemErrorContent}  # internal, never exchanged (ADR-007)


# =============================================================================================
# 4. Rules first and last, and the rendered policy
# =============================================================================================


def _numbered(section: str) -> list[str]:
    return re.findall(r"^(\d+)\. ", section, re.M)


def given_contract_when_read_then_the_rules_open_it_and_the_same_checks_close_it() -> None:
    text = rendered("powershell")
    headings = re.findall(r"^## (\d+)\. (.*)$", text, re.M)
    assert headings[0] == ("1", "The contract")
    assert headings[-1][1].startswith("Before every message")
    first = text.split("## 1. The contract", 1)[1].split("**What happens when", 1)[0]
    last = text.split(f"## {headings[-1][0]}. {headings[-1][1]}", 1)[1]
    rules, checks = _numbered(first), _numbered(last)
    assert rules == checks == [str(n) for n in range(1, len(rules) + 1)] and len(rules) == 10
    assert first.count("MUST") >= 10
    # the rules the owner asked for, one line each
    for needle in (
        "exactly one message: one JSON envelope",
        "Field names MUST be exactly",
        "MUST NOT add a field",
        "case included",
        "a boolean is `true` or `false`",
        "`conversation_id` MUST repeat",
        "MUST each be new for the whole session",
        "allowed at that moment",
        "the application alone sends",
        "MUST NOT write prose",
    ):
        assert needle in first, needle


def given_corrections_on_when_rendered_then_the_bound_is_announced_first_and_in_section_10() -> (
    None
):
    text = render_instructions(
        AppConfig(protocol=ProtocolSection(max_correction_attempts=3)),
        environment=ENVIRONMENTS["posix"],
    )
    first = text.split("## 2.")[0]
    assert "Up to **3** refused replies in a row" in first
    assert "one more ends the session in failure" in first
    correction = text.split("## 10.")[1].split("## 11.")[0]
    assert "Up to **3** refused replies in a row each get a correction" in correction
    assert "the next refused reply ends the session in failure" in correction
    assert '"max_attempts": 3' in text


def given_corrections_off_when_rendered_then_the_first_refusal_ends_the_session_everywhere() -> (
    None
):
    text = rendered("posix", "corrections-off")
    first = text.split("## 2.")[0]
    assert "the first refused reply **ends the session**" in first
    assert "protocol_correction_request` (section 10) naming" not in first
    assert "The correction policy is **disabled**" in text
    assert "in this one, `C2` would end the session" in text


# =============================================================================================
# 5. The dialect of the examples
# =============================================================================================


def _commands(text: str) -> set[str]:
    commands: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if isinstance(node.get("cmd"), str):
                commands.add(node["cmd"])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for block in labelled(text).values():
        if block.lang == "json":
            walk(block.data)
    return commands


#: The commands every dialect writes the same way (a program started by name).
NEUTRAL_COMMANDS = {
    "java -version",
    "mvn -version",
    "mvn -B clean install",
    "mvn -B -e clean install",
}


@pytest.mark.parametrize("machine", list(ENVIRONMENTS))
def given_machine_when_rendered_then_every_example_command_is_written_in_its_dialect(
    machine: str,
) -> None:
    text = rendered(machine)
    dialect = ENVIRONMENTS[machine].dialect
    own = set(EXAMPLE_COMMANDS[dialect].values())
    assert _commands(text) == NEUTRAL_COMMANDS | own
    # outside the announcement, whose one line of advice contrasts the dialects on purpose
    before, rest = text.split("## 6. ", 1)
    elsewhere = before + rest.split("## 7. ", 1)[1]
    for other, commands in EXAMPLE_COMMANDS.items():
        for command in set(commands.values()) - own:
            encoded = json.dumps(command)[1:-1]
            assert command not in elsewhere and encoded not in elsewhere, (other, command)
    # and the dictionary never rewrites them on their own shell (ADR-030 §4)
    translator = ShellTranslator(dialect)
    assert all(translator.translate(command) is None for command in _commands(text))


def given_two_machines_when_rendered_then_only_commands_announcement_and_translation_differ() -> (
    None
):
    def neutral(machine: str) -> str:
        text = rendered(machine)
        for name, command in EXAMPLE_COMMANDS[ENVIRONMENTS[machine].dialect].items():
            text = text.replace(json.dumps(command)[1:-1], f"<{name}>")
        before, rest = text.split("## 6. ", 1)
        after = rest.split("## 7. ", 1)[1]
        fragment = next(b for b in blocks_of(text) if b.fragment)
        return (before + after).replace(fragment.body, "<translation>")

    assert rendered("posix") != rendered("powershell")
    assert neutral("posix") == neutral("powershell")


# =============================================================================================
# 6. Size: the contract is counted in the context window and sent again at every rotation
# =============================================================================================


@pytest.mark.parametrize("machine", list(ENVIRONMENTS))
def given_default_config_when_rendered_then_the_contract_stays_under_its_size_budget(
    machine: str,
) -> None:
    config = AppConfig()
    size = ContextWindowMonitor.instructions_bytes(rendered(machine))
    assert size <= MAX_CONTRACT_BYTES, f"{size} bytes"
    # ADR-013: the instructions are counted in context_bytes; they must leave the window HEALTHY
    # with room for the exchange, rotation after rotation (each child receives them again)
    thresholds = ContextWindowMonitor(config.context).thresholds()
    assert size / config.context.budget_bytes < 0.16
    assert size < thresholds.warning_bytes // 4


@pytest.mark.parametrize("machine", ["posix", "powershell"])
def given_default_config_when_a_conversation_opens_then_the_window_is_healthy(machine: str) -> None:
    config = AppConfig()
    monitor = ContextWindowMonitor(config.context)
    request = labelled(rendered(machine))["A1"].data
    opened = ContextWindowMonitor.account(
        ContextWindowMonitor.instructions_bytes(rendered(machine)),
        ProtocolAdapter(config)
        .build_user_request(
            conversation_record(State(conversation=request["conversation_id"])),
            request["message_id"],
            request["content"]["goal"],
            request["content"]["user_message"],
            SessionBudget(max_cycles=20, max_plans=10, max_total_duration_ms=300_000),
        )
        .size_bytes,
    )
    conversation = conversation_record(State(conversation="conv-1001")).model_copy(
        update={"context_bytes": opened}
    )
    assert monitor.evaluate(conversation) is ContextWindowState.HEALTHY
