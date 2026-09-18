"""``templated_http`` — any HTTP API described by ``transport.options`` (ADR-020).

The provider is entirely driven by its options: for each operation a method, a URL template,
header templates, an optional body template and the response paths to read. Example::

    [transport]
    provider = "templated_http"
    [transport.options]
    headers = { "X-Api-Key" = "${env:MY_MODEL_KEY}" }          # common to every operation
    [transport.options.init]
    method = "POST"
    url = "https://api.example.com/v1/threads"
    body = { instructions = "{instructions}", user = "{user_id}", meta = "{metadata_json}" }
    conversation_id_path = "data.id"
    expected_statuses = [200, 201]
    [transport.options.post]
    url = "https://api.example.com/v1/threads/{conversation_id}/messages"
    body = { role = "user", content = "{message_json}" }
    accepted_path = "ok"                    # optional: absent => accepted on an expected status
    message_id_path = "id"                  # optional: absent => message_id of the sent message
    [transport.options.get]
    method = "GET"
    url = "https://api.example.com/v1/threads/{conversation_id}/messages?since={after}"
    messages_path = "items"
    message_path = "payload"                # optional: the protocol message inside each item
    cursor_path = "next_cursor"             # optional: absent or null => last message_id
    [transport.options.close]               # optional table: absent => local close only
    method = "DELETE"
    url = "https://api.example.com/v1/threads/{conversation_id}"

**Placeholders** — ``{conversation_id}`` and ``{after}`` (get, post, close), ``{instructions}``
and ``{metadata_json}`` (init), ``{message_json}``, ``{message_id}`` and ``{message_type}``
(post), ``{user_id}`` and ``{token}`` (everywhere). In a URL the placeholder values are
percent-encoded (``${env:...}`` values are inserted verbatim, so a variable may hold a base URL).
A body leaf whose whole value is ``"{message_json}"`` or ``"{metadata_json}"``
receives the object itself; inside a longer string these two become canonical JSON text. Only
string leaves are substituted, other leaves pass through. A placeholder used by an operation that
does not provide it is rejected when the options are validated (``TRANSPORT_OPTIONS_INVALID``).
When a message codec (ADR-021) posts the **text** form of a message, ``{message_json}`` receives
that text, ``{message_id}`` and ``{message_type}`` are empty, and the acknowledgement's
``message_id`` comes from ``message_id_path`` or is restored by the codec decorator.

**Environment** — ``${env:VAR}`` anywhere (URL, headers, body leaves) is replaced by the variable
at call time (never stored); a missing variable is ``ConfigError(TRANSPORT_ENV_MISSING)``, as is
``{token}`` when the variable named by ``transport.token_env`` is unset. The default headers are
only ``Accept: application/json`` (plus what the base adds for a body): nothing identifying or
authenticating is sent unless the options say so. Values resolved from the environment never
appear in error details (``url`` is redacted).

**Response paths** — dotted notation with list indices (``data.items[0].id``). A missing path or
an unexpected type is ``TransportError(MODEL_PROTOCOL_ERROR, INVALID_RESPONSE_BODY)`` with
``details.path``. The messages read from ``messages_path`` must be a list of objects, each being a
protocol message as is (or holding one at ``message_path``).

Everything else — timeouts, gzip, polling, the HTTP -> ``ErrorType`` table, ``abandon()`` — is
inherited from :class:`~agentic_local_app.transport.http_base.HttpProviderBase`.
"""

from __future__ import annotations

import asyncio
import copy
import os
import re
from collections.abc import Awaitable, Callable, Iterator, Mapping
from typing import Annotated, Any, ClassVar, Literal, cast
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from agentic_local_app.config import TransportSection
from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import ConfigError
from agentic_local_app.transport.base import OP_CLOSE, OP_GET, OP_INIT, OP_POST, GetResult, PostAck
from agentic_local_app.transport.http_base import HttpCall, HttpProviderBase, InvalidResponseError
from agentic_local_app.transport.registry import TransportRegistry

__all__ = [
    "CallTemplate",
    "CloseTemplate",
    "GetTemplate",
    "InitTemplate",
    "PostTemplate",
    "TemplatedHttpProvider",
    "TemplatedOptions",
    "extract_path",
    "parse_path",
]

HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
HttpStatus = Annotated[int, Field(ge=100, le=599)]

# ------------------------------------------------------------------------------------------------
# placeholders
# ------------------------------------------------------------------------------------------------
P_CONVERSATION_ID = "conversation_id"
P_AFTER = "after"
P_INSTRUCTIONS = "instructions"
P_USER_ID = "user_id"
P_METADATA_JSON = "metadata_json"
P_MESSAGE_JSON = "message_json"
P_MESSAGE_ID = "message_id"
P_MESSAGE_TYPE = "message_type"
P_TOKEN = "token"

COMMON_PLACEHOLDERS: frozenset[str] = frozenset({P_USER_ID, P_TOKEN})
PLACEHOLDERS_BY_OPERATION: dict[str, frozenset[str]] = {
    OP_INIT: COMMON_PLACEHOLDERS | {P_INSTRUCTIONS, P_METADATA_JSON},
    OP_POST: COMMON_PLACEHOLDERS
    | {P_CONVERSATION_ID, P_MESSAGE_JSON, P_MESSAGE_ID, P_MESSAGE_TYPE},
    OP_GET: COMMON_PLACEHOLDERS | {P_CONVERSATION_ID, P_AFTER},
    OP_CLOSE: COMMON_PLACEHOLDERS | {P_CONVERSATION_ID},
}
ALL_PLACEHOLDERS: frozenset[str] = frozenset().union(*PLACEHOLDERS_BY_OPERATION.values())
#: Placeholders inserted as JSON objects when they are the whole value of a body leaf.
OBJECT_PLACEHOLDERS: frozenset[str] = frozenset({P_METADATA_JSON, P_MESSAGE_JSON})

_ENV_PATTERN = r"\$\{env:(?P<env>[A-Za-z_][A-Za-z0-9_]*)\}"
_PLACEHOLDER_PATTERN = r"\{(?P<name>" + "|".join(sorted(ALL_PLACEHOLDERS)) + r")\}"
_TEMPLATE_RE = re.compile(_ENV_PATTERN + "|" + _PLACEHOLDER_PATTERN)
_ENV_RE = re.compile(_ENV_PATTERN)
_PLACEHOLDER_RE = re.compile(_PLACEHOLDER_PATTERN)
_PATH_SEGMENT_RE = re.compile(r"^(?P<key>[^.\[\]]*)(?P<indices>(?:\[\d+\])*)$")
_INDEX_RE = re.compile(r"\[(\d+)\]")
_ACCEPT_HEADER = ("Accept", "application/json")


def _string_leaves(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _string_leaves(item)
    elif isinstance(value, list):
        for item in value:
            yield from _string_leaves(item)


def _placeholders_in(*values: Any) -> set[str]:
    return {
        match.group("name")
        for value in values
        for leaf in _string_leaves(value)
        for match in _PLACEHOLDER_RE.finditer(leaf)
    }


def _env_names_in(*values: Any) -> set[str]:
    return {
        match.group("env")
        for value in values
        for leaf in _string_leaves(value)
        for match in _ENV_RE.finditer(leaf)
    }


def _check_placeholders(operation: str, allowed: frozenset[str], *values: Any) -> None:
    unexpected = sorted(_placeholders_in(*values) - allowed)
    if unexpected:
        raise ValueError(
            f"placeholder(s) {', '.join('{' + name + '}' for name in unexpected)} not available "
            f"for {operation.lower()} (allowed: {', '.join(sorted(allowed))})"
        )


# ------------------------------------------------------------------------------------------------
# response paths
# ------------------------------------------------------------------------------------------------
def parse_path(path: str) -> tuple[str | int, ...]:
    """``data.items[0].id`` -> ``("data", "items", 0, "id")``; ``ValueError`` on a malformed path."""
    steps: list[str | int] = []
    for segment in path.split("."):
        match = _PATH_SEGMENT_RE.match(segment)
        if match is None or (not match.group("key") and not match.group("indices")):
            raise ValueError(f"malformed response path {path!r} (segment {segment!r})")
        if match.group("key"):
            steps.append(match.group("key"))
        steps.extend(int(index) for index in _INDEX_RE.findall(match.group("indices")))
    return tuple(steps)


def extract_path(value: Any, path: str, *, reported_as: str | None = None) -> Any:
    """The value at ``path`` in a decoded JSON body; ``InvalidResponseError`` when absent."""
    current = value
    for step in parse_path(path):
        if isinstance(step, int):
            if not isinstance(current, list) or not 0 <= step < len(current):
                raise InvalidResponseError(reason="path_not_found", path=reported_as or path)
            current = current[step]
        else:
            if not isinstance(current, dict) or step not in current:
                raise InvalidResponseError(reason="path_not_found", path=reported_as or path)
            current = current[step]
    return current


def _validate_path(path: str | None) -> str | None:
    if path is not None:
        parse_path(path)
    return path


# ------------------------------------------------------------------------------------------------
# options model
# ------------------------------------------------------------------------------------------------
class _Options(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CallTemplate(_Options):
    """One operation: method, URL template, header templates, body template, expected statuses."""

    operation: ClassVar[str] = ""

    method: HttpMethod = "POST"
    url: str = Field(min_length=1)
    headers: dict[str, str] = Field(default_factory=dict)
    body: Any = None
    expected_statuses: list[HttpStatus] = Field(default_factory=lambda: [200, 201, 202, 204])

    @field_validator("method", mode="before")
    @classmethod
    def _upper_method(cls, value: Any) -> Any:
        return value.upper() if isinstance(value, str) else value

    @field_validator("expected_statuses")
    @classmethod
    def _at_least_one_status(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError("must list at least one expected status")
        return value

    @model_validator(mode="after")
    def _only_available_placeholders(self) -> CallTemplate:
        allowed = PLACEHOLDERS_BY_OPERATION[type(self).operation]
        _check_placeholders(type(self).operation, allowed, self.url, self.headers, self.body)
        return self


class InitTemplate(CallTemplate):
    operation: ClassVar[str] = OP_INIT

    conversation_id_path: str = Field(min_length=1)
    expected_statuses: list[HttpStatus] = Field(default_factory=lambda: [200, 201])

    _paths = field_validator("conversation_id_path")(_validate_path)


class PostTemplate(CallTemplate):
    operation: ClassVar[str] = OP_POST

    accepted_path: str | None = None
    message_id_path: str | None = None
    expected_statuses: list[HttpStatus] = Field(default_factory=lambda: [200, 201, 202])

    _paths = field_validator("accepted_path", "message_id_path")(_validate_path)


class GetTemplate(CallTemplate):
    operation: ClassVar[str] = OP_GET

    method: HttpMethod = "GET"
    messages_path: str = Field(min_length=1)
    message_path: str | None = None
    cursor_path: str | None = None
    expected_statuses: list[HttpStatus] = Field(default_factory=lambda: [200])

    _paths = field_validator("messages_path", "message_path", "cursor_path")(_validate_path)


class CloseTemplate(CallTemplate):
    operation: ClassVar[str] = OP_CLOSE

    expected_statuses: list[HttpStatus] = Field(default_factory=lambda: list(range(200, 300)))


class TemplatedOptions(_Options):
    """``transport.options`` of the ``templated_http`` provider."""

    headers: dict[str, str] = Field(default_factory=dict)
    init: InitTemplate
    post: PostTemplate
    get: GetTemplate
    close: CloseTemplate | None = None

    @field_validator("headers")
    @classmethod
    def _common_headers_placeholders(cls, value: dict[str, str]) -> dict[str, str]:
        _check_placeholders("common headers", COMMON_PLACEHOLDERS, value)
        return value


# ------------------------------------------------------------------------------------------------
# rendering
# ------------------------------------------------------------------------------------------------
class _Renderer:
    """Substitutes ``${env:VAR}`` and the placeholders of one call, in a single pass."""

    def __init__(
        self,
        operation: str,
        values: Mapping[str, Any],
        *,
        token_env: str,
        token: str | None,
        environ: Mapping[str, str],
    ) -> None:
        self._operation = operation
        self._values = values
        self._token_env = token_env
        self._token = token
        self._environ = environ

    def text(self, template: str, *, encode: bool = False) -> str:
        def replace(match: re.Match[str]) -> str:
            env_name = match.group("env")
            if env_name is not None:
                return self._env(env_name)
            value = self._text_value(match.group("name"))
            return quote(value, safe="") if encode else value

        return _TEMPLATE_RE.sub(replace, template)

    def leaf(self, template: str) -> Any:
        """A body leaf: the object itself for a whole-value object placeholder, else text."""
        match = _PLACEHOLDER_RE.fullmatch(template)
        if match is not None and match.group("name") in OBJECT_PLACEHOLDERS:
            return copy.deepcopy(self._values[match.group("name")])
        return self.text(template)

    def body(self, template: Any) -> Any:
        if isinstance(template, str):
            return self.leaf(template)
        if isinstance(template, dict):
            return {key: self.body(item) for key, item in template.items()}
        if isinstance(template, list):
            return [self.body(item) for item in template]
        return template

    def headers(self, *layers: Mapping[str, str]) -> dict[str, str]:
        """Merge header layers case-insensitively (a later layer replaces an earlier name)."""
        merged: dict[str, tuple[str, str]] = {}
        for layer in layers:
            for name, value in layer.items():
                merged[name.lower()] = (name, self.text(value))
        return dict(merged.values())

    def _env(self, name: str) -> str:
        value = self._environ.get(name)
        if value is None:
            raise ConfigError("TRANSPORT_ENV_MISSING", variable=name, operation=self._operation)
        return value

    def _text_value(self, name: str) -> str:
        if name == P_TOKEN:
            if self._token is None:
                raise ConfigError(
                    "TRANSPORT_ENV_MISSING",
                    variable=self._token_env,
                    operation=self._operation,
                    placeholder=P_TOKEN,
                )
            return self._token
        value = self._values[name]
        if name in OBJECT_PLACEHOLDERS:
            return canonical_json(value)
        return "" if value is None else str(value)


# ------------------------------------------------------------------------------------------------
# provider
# ------------------------------------------------------------------------------------------------
@TransportRegistry.register("templated_http")
class TemplatedHttpProvider(HttpProviderBase):
    """The provider whose HTTP dialogue is described by :class:`TemplatedOptions`."""

    options_model: ClassVar[type[BaseModel] | None] = TemplatedOptions

    def __init__(
        self,
        config: TransportSection,
        clock: Clock,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        super().__init__(config, clock, transport=transport, sleep=sleep)
        self._templates = cast(TemplatedOptions, self.options)
        self._env_names = tuple(sorted(_env_names_in(self._templates.model_dump())))

    # ------------------------------------------------------------------ hooks --------------
    def headers(self, operation: str) -> dict[str, str]:
        """Only ``Accept``: identification and authentication come from the configured headers."""
        return dict([_ACCEPT_HEADER])

    def build_init(self, instructions: str, metadata: dict[str, Any]) -> HttpCall:
        return self._call(
            OP_INIT,
            self._templates.init,
            {P_INSTRUCTIONS: instructions, P_METADATA_JSON: metadata},
            parse_json=True,
        )

    def parse_init(self, status: int, body: Any) -> str:
        path = self._templates.init.conversation_id_path
        return _as_identifier(extract_path(body, path), path)

    def build_post(self, remote_conversation_id: str, payload: dict[str, Any]) -> HttpCall:
        template = self._templates.post
        return self._call(
            OP_POST,
            template,
            {
                P_CONVERSATION_ID: remote_conversation_id,
                P_MESSAGE_JSON: payload,
                P_MESSAGE_ID: _payload_field(payload, "message_id"),
                P_MESSAGE_TYPE: _payload_field(payload, "type"),
            },
            parse_json=template.accepted_path is not None or template.message_id_path is not None,
        )

    def parse_post(self, status: int, body: Any, *, payload: dict[str, Any]) -> PostAck:
        template = self._templates.post
        sent_id = _payload_field(payload, "message_id")
        if template.accepted_path is not None:
            accepted = extract_path(body, template.accepted_path)
            if not isinstance(accepted, bool):
                raise InvalidResponseError(
                    reason="unexpected_type", path=template.accepted_path, expected="boolean"
                )
            if accepted is False:
                raise InvalidResponseError(
                    "POST_NOT_ACCEPTED",
                    path=template.accepted_path,
                    message_id=_optional_identifier(body, template.message_id_path, sent_id),
                )
        if template.message_id_path is not None:
            message_id = _as_identifier(
                extract_path(body, template.message_id_path), template.message_id_path
            )
        else:
            message_id = "" if sent_id is None else str(sent_id)
        return PostAck(message_id=message_id, accepted=True, http_status=status)

    def build_get(self, remote_conversation_id: str, after: str | None) -> HttpCall:
        return self._call(
            OP_GET,
            self._templates.get,
            {P_CONVERSATION_ID: remote_conversation_id, P_AFTER: after or ""},
            parse_json=True,
        )

    def parse_get(self, status: int, body: Any) -> GetResult:
        template = self._templates.get
        items = extract_path(body, template.messages_path)
        if not isinstance(items, list):
            raise InvalidResponseError(
                reason="unexpected_type", path=template.messages_path, expected="array"
            )
        messages: list[dict[str, Any]] = []
        for index, item in enumerate(items):
            item_path = f"{template.messages_path}[{index}]"
            message = item
            if template.message_path is not None:
                item_path = f"{item_path}.{template.message_path}"
                message = extract_path(item, template.message_path, reported_as=item_path)
            if not isinstance(message, dict):
                raise InvalidResponseError(
                    reason="unexpected_type", path=item_path, expected="object"
                )
            messages.append(message)
        cursor: str | None = None
        if template.cursor_path is not None:
            value = _optional_path(body, template.cursor_path)
            if value is not None and not isinstance(value, str):
                raise InvalidResponseError(
                    reason="unexpected_type", path=template.cursor_path, expected="string"
                )
            cursor = value
        if cursor is None and messages:
            last_id = messages[-1].get("message_id")
            cursor = last_id if isinstance(last_id, str) and last_id else None
        return GetResult(messages=messages, cursor=cursor, http_status=status)

    def build_close(self, remote_conversation_id: str) -> HttpCall | None:
        template = self._templates.close
        if template is None:
            return None
        return self._call(
            OP_CLOSE, template, {P_CONVERSATION_ID: remote_conversation_id}, parse_json=False
        )

    def redact_url(self, url: str) -> str:
        """Besides the bearer token, every value resolved from ``${env:...}`` is masked."""
        url = super().redact_url(url)
        secrets = sorted(
            (value for value in (os.environ.get(name) for name in self._env_names) if value),
            key=len,
            reverse=True,
        )
        for secret in secrets:
            url = url.replace(secret, "***")
        return url

    # ------------------------------------------------------------------ internals ----------
    def _call(
        self, operation: str, template: CallTemplate, values: Mapping[str, Any], *, parse_json: bool
    ) -> HttpCall:
        renderer = _Renderer(
            operation,
            {P_USER_ID: self._config.user_id, **values},
            token_env=self._config.token_env,
            token=self._config.token,
            environ=os.environ,
        )
        return HttpCall(
            template.method,
            renderer.text(template.url, encode=True),
            renderer.headers(self.headers(operation), self._templates.headers, template.headers),
            None if template.body is None else renderer.body(template.body),
            frozenset(template.expected_statuses),
            parse_json=parse_json,
        )


def _payload_field(payload: Any, key: str) -> Any:
    """A field of the posted payload when it is the protocol envelope; ``None`` when a codec
    (ADR-021, ``outbound = "text"``) handed over another form, such as canonical JSON text."""
    return payload.get(key) if isinstance(payload, Mapping) else None


def _as_identifier(value: Any, path: str) -> str:
    """A non-empty string, or an integer rendered as text; anything else is unexpected."""
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    raise InvalidResponseError(reason="unexpected_type", path=path, expected="non-empty string")


def _optional_path(body: Any, path: str) -> Any:
    try:
        return extract_path(body, path)
    except InvalidResponseError:
        return None


def _optional_identifier(body: Any, path: str | None, fallback: Any) -> Any:
    if path is not None:
        value = _optional_path(body, path)
        if value is not None:
            return value
    return fallback
