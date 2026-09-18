"""Shared pytest fixtures.

Every unit test must be isolated from external dependencies (spec §18.3): no real shell,
no real network, no real database. Fixtures providing the test doubles are added phase by phase.
"""
