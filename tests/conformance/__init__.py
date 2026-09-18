"""Protocol conformance battery: what the application does with everything a model can get wrong.

Each module holds end-to-end cases (the real orchestrator, the real adapter, the real failure
policy — only the network, the shell, the clock and the identifiers are doubles) registered in
:mod:`tests.conformance.registry`; ``tools/protocol_conformance_report.py`` renders the matrix.
"""
