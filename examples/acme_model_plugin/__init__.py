"""``acme_model_plugin`` — a transport provider and a message codec written **outside** the
application, the way a team plugs its own model in (guides 03 and 04, ADR-020, ADR-021).

Nothing here is registered in the application: the two classes are selected from
``config.toml`` by their import path (see ``examples/config.acme.toml``), which is the
open/closed way of adding an implementation — no line of ``agentic_local_app`` changes.

- :class:`~acme_model_plugin.provider.AcmeHttpProvider` speaks the (fictional) *ACME Threads*
  HTTP API: its own URLs, its ``X-Api-Key`` authentication, its own body shapes, its throttling
  reply — the HTTP dialect;
- :class:`~acme_model_plugin.codec.StreamedTextCodec` speaks the model behind it, which answers
  in streamed text chunks that must be concatenated before the JSON of the protocol message can be
  read — the model's shape.

The tests in ``tests/unit/test_phase7_examples_plugin.py`` and
``tests/integration/test_phase9_examples_plugin.py`` exercise both, so the guides that quote this
code cannot rot silently.
"""

from acme_model_plugin.codec import StreamedTextCodec, StreamedTextOptions
from acme_model_plugin.provider import AcmeHttpProvider, AcmeOptions

__all__ = ["AcmeHttpProvider", "AcmeOptions", "StreamedTextCodec", "StreamedTextOptions"]
