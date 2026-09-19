"""``PayloadGuard`` — output budgets, truncation, message cap and chunk serving (§2.5, §3.10 ;
ADR-010, ADR-011).

Everything here is pure or reads the store; nothing is persisted. Four services:

- :meth:`PayloadGuard.effective_budget` — ``min(task ?? plan ?? app default, hard max)`` (ADR-010);
- :meth:`PayloadGuard.apply` — the truncation algorithm of ADR-011, as amended by ADR-029 §1
  (a guaranteed share per stream), on raw bytes, returning the kept bytes and their ``[start, end)``
  ranges in the original streams;
- :meth:`PayloadGuard.fit_message` — the deterministic re-truncation of an ``execution_result`` that
  exceeds ``max_message_bytes`` (ADR-010): longest ``stdout`` halved first, ``stderr`` last;
- :meth:`PayloadGuard.serve_chunk` — a ``chunk_request`` answered from the stored blobs (ADR-011).

Decoding to text happens **only** here (:func:`decode_output`, UTF-8 with replacement, ADR-003):
ranges and budgets are always counted in bytes of the raw stream.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from agentic_local_app.config import PayloadSection
from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.states import OutputStream
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.protocol.messages import ExecutionResultContent, TaskResult

__all__ = [
    "ChunkError",
    "ChunkErrorCode",
    "ChunkResult",
    "PayloadGuard",
    "TruncatedOutput",
    "decode_output",
]

ChunkErrorCode = Literal["CHUNK_REF_NOT_FOUND", "CHUNK_RANGE_INVALID"]


def decode_output(data: bytes) -> str:
    """UTF-8 with replacement characters (ADR-003): a cut inside a multi-byte sequence never raises."""
    return data.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class TruncatedOutput:
    """Result of :meth:`PayloadGuard.apply`. Ranges are ``[start, end)`` byte intervals of the
    original stream and ``stream[start:end] == kept`` always holds."""

    stdout_kept: bytes
    stderr_kept: bytes
    truncated: bool
    original_size_bytes: int
    stdout_total: int
    stderr_total: int
    stdout_range: tuple[int, int]
    stderr_range: tuple[int, int]


@dataclass(frozen=True)
class ChunkResult:
    """A served ``chunk_request`` (ADR-011): ``range`` is ``[offset, offset + len(data))``."""

    data: bytes
    range: tuple[int, int]
    total: int
    eof: bool


@dataclass(frozen=True)
class ChunkError:
    """A ``chunk_request`` that cannot be served: a task ``FAILED`` with ``reason = code`` (ADR-008 §5)."""

    code: ChunkErrorCode
    details: dict[str, Any] = field(default_factory=dict)


class PayloadGuard:
    def __init__(self, config: PayloadSection) -> None:
        self._config = config

    # ---- budgets (ADR-010) ---------------------------------------------------------------
    def effective_budget(self, task_max: int | None, plan_default: int | None) -> int:
        declared = (
            task_max
            if task_max is not None
            else plan_default
            if plan_default is not None
            else self._config.default_max_output_bytes
        )
        return min(declared, self._config.hard_max_output_bytes)

    # ---- truncation (ADR-011, §1 amended by ADR-029 §1) ----------------------------------
    def apply(self, stdout: bytes, stderr: bytes, budget: int) -> TruncatedOutput:
        """Keep the **end** of each stream, each guaranteed its own share of the budget.

        ADR-029 §1, amending ADR-011 §1: each stream keeps at least ``min(len(stream), budget // 2)``
        bytes, and whatever one stream does not use is given to the other — stderr first, which is
        all that is left of the stderr priority ADR-011 gave it. A noisy stderr can therefore no
        longer delete stdout entirely, which is where the verdict of a JVM build tool lives.
        ``len(stdout_kept) + len(stderr_kept) <= budget`` by construction.
        """
        if budget < 0:
            raise ValueError("budget must be >= 0")
        stderr_total, stdout_total = len(stderr), len(stdout)
        half = budget // 2
        stderr_keep, stdout_keep = min(stderr_total, half), min(stdout_total, half)
        spare = budget - stderr_keep - stdout_keep
        taken = min(spare, stderr_total - stderr_keep)
        stderr_keep, spare = stderr_keep + taken, spare - taken
        stdout_keep += min(spare, stdout_total - stdout_keep)
        stderr_kept = stderr[stderr_total - stderr_keep :] if stderr_keep else b""
        stdout_kept = stdout[stdout_total - stdout_keep :] if stdout_keep else b""
        return TruncatedOutput(
            stdout_kept=stdout_kept,
            stderr_kept=stderr_kept,
            truncated=stderr_keep < stderr_total or stdout_keep < stdout_total,
            original_size_bytes=stderr_total + stdout_total,
            stdout_total=stdout_total,
            stderr_total=stderr_total,
            stdout_range=(stdout_total - stdout_keep, stdout_total),
            stderr_range=(stderr_total - stderr_keep, stderr_total),
        )

    @staticmethod
    def decode(data: bytes) -> str:
        return decode_output(data)

    # ---- message cap (ADR-010) -----------------------------------------------------------
    @staticmethod
    def message_size(content: ExecutionResultContent) -> int:
        """Bytes of the canonical serialisation of ``content`` without ``None`` fields — the same
        measure the protocol adapter applies to the outbound message body."""
        return size_bytes(content.model_dump(mode="json", exclude_none=True))

    def fit_message(
        self, content: ExecutionResultContent, max_message_bytes: int
    ) -> ExecutionResultContent:
        """Re-truncate deterministically until ``message_size(content) <= max_message_bytes``.

        While too big: the result with the longest ``stdout`` (first in list order on ties) keeps
        only the **last half** of it (``stdout_range`` start moves forward, ``truncated`` set).
        Once every ``stdout`` is empty the same rule applies to ``stderr``. When everything is empty
        and the message still does not fit — the envelope and metadata alone exceed the cap — the
        content is returned as is: the caller decides (this is never a cause of rotation).

        Halving works on the UTF-8 encoding of the text kept; the range is byte-exact when the kept
        bytes were valid UTF-8 (the usual case) and approximate by the width of replacement
        characters otherwise. Nothing is lost: the raw blob stays retrievable by ``chunk_request``.
        """
        results = list(content.results)
        while (
            self.message_size(content.model_copy(update={"results": results})) > max_message_bytes
        ):
            index = self._longest(results, "stdout")
            if index is None:
                index = self._longest(results, "stderr")
                if index is None:
                    break
                results[index] = self._halve(results[index], "stderr")
            else:
                results[index] = self._halve(results[index], "stdout")
        if results == list(content.results):
            return content
        return content.model_copy(update={"results": results})

    @staticmethod
    def _longest(results: list[TaskResult], stream: Literal["stdout", "stderr"]) -> int | None:
        best: int | None = None
        best_size = 0
        for index, result in enumerate(results):
            size = len(getattr(result, stream).encode("utf-8"))
            if size > best_size:
                best, best_size = index, size
        return best

    @staticmethod
    def _halve(result: TaskResult, stream: Literal["stdout", "stderr"]) -> TaskResult:
        encoded: bytes = getattr(result, stream).encode("utf-8")
        keep = len(encoded) // 2
        kept = encoded[len(encoded) - keep :] if keep else b""
        current_range: tuple[int, int] | None = getattr(result, f"{stream}_range")
        total: int | None = getattr(result, f"{stream}_total")
        if current_range is None:
            current_range = (0, len(encoded))
        if total is None:
            total = current_range[1]
        end = current_range[1]
        return result.model_copy(
            update={
                stream: decode_output(kept),
                f"{stream}_range": (end - len(kept), end),
                f"{stream}_total": total,
                "truncated": True,
            }
        )

    # ---- chunk requests (ADR-011) --------------------------------------------------------
    def serve_chunk(
        self,
        store: ConversationStore,
        session_id: str,
        ref_task_id: str,
        stream: OutputStream,
        offset: int,
        max_bytes: int,
    ) -> ChunkResult | ChunkError:
        """Bytes ``[offset, offset + min(max_bytes, hard_max_output_bytes))`` of the stored stream.

        Unknown task or stream for this session → ``CHUNK_REF_NOT_FOUND``; ``offset`` outside
        ``[0, total)`` or a non-positive ``max_bytes`` → ``CHUNK_RANGE_INVALID`` (an empty stream
        therefore has no valid offset). Never a protocol error (ADR-008 §5).
        """
        blob = store.get_blob_for_task(session_id, ref_task_id, stream)
        if blob is None:
            return ChunkError(
                "CHUNK_REF_NOT_FOUND", {"ref_task_id": ref_task_id, "stream": stream.value}
            )
        total = blob.size_bytes
        capped = min(max_bytes, self._config.hard_max_output_bytes)
        if offset < 0 or offset >= total or capped <= 0:
            return ChunkError(
                "CHUNK_RANGE_INVALID",
                {
                    "ref_task_id": ref_task_id,
                    "stream": stream.value,
                    "offset": offset,
                    "max_bytes": max_bytes,
                    "total": total,
                },
            )
        data = store.read_blob_range(blob.blob_id, offset, capped)
        end = offset + len(data)
        return ChunkResult(data=data, range=(offset, end), total=total, eof=end >= total)
