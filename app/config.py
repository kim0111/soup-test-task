"""Tunables (Docs/SPEC.md §5). Times are in seconds."""

import os

DATABASE_URL = os.environ.get("DATABASE_URL", "")

LEASE_SECONDS = 5.0
HEARTBEAT_SECONDS = 1.0
REAP_SECONDS = 1.0
MAX_ATTEMPTS = 5
REAP_BATCH = 500

# API connection pool. Requests beyond POOL_MAX wait up to POOL_TIMEOUT_SECONDS, then get a 503.
POOL_MIN = 2
POOL_MAX = 20
POOL_TIMEOUT_SECONDS = 3.0

# Deliberate bugs that prove the tests and the chaos harness can fail (SPEC §8.4).
MUTATION = os.environ.get("MUTATION", "")
if MUTATION not in ("", "M1", "M2", "M3", "M4"):
    # A typo would silently run the correct code and make a mutation look "caught by nothing".
    raise ValueError(f"unknown MUTATION={MUTATION!r}")
