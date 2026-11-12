# Chaos runs

Every run, including failed ones, appended by the harness. Never edited; notes go in the last column only.

| run | seed | mutation | zombie | jobs | verdict | failed / gaps | wall s | kills (busy) | re-exec | exhausted | notes | code |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261112-091214-s1-j1000 | 1 | - | - | 1000 | PASS | - | 38 | 19 (16) | 7 | 0 | debug, rate 30. Queue never built up; 9 of 16 orphaned jobs were cancelled by the targeted canceller before the reaper could rerun them (7 re-exec). Retuned: targets need a fresh lease, rate 40 Code: uncommitted Phase 7 tree (the header's commit 55f353d has no chaos/), before coverage gates. |
| 20261112-091344-s2-j1000 | 2 | - | - | 1000 | PASS | - | 40 | 16 (14) | 12 | 0 | debug after the retune: 27 cancels hit queued jobs, 12 re-exec from 14 busy kills Code: uncommitted Phase 7 tree (the header's commit 55f353d has no chaos/), before coverage gates. |
| 20261112-091436-s3-z-j1000 | 3 | - | yes | 1000 | PASS | - | 36 | 16 (13) | 14 | 0 | debug of the --zombie actor: 2 freezes, 0 chances (jobs cancelled / finished by SIGCONT) Code: uncommitted Phase 7 tree (the header's commit 55f353d has no chaos/), before coverage gates. Would now be INCOMPLETE (0 zombie chances). |
| 20261112-091525-s4-M1-j1000 | 4 | M1 | - | 1000 | FAIL | I8(101) | 39 | 15 (14) | 123 | 0 | smoke: the harness can fail. M1 caught by I8 only, as SPEC §8.4 says Code: uncommitted Phase 7 tree (the header's commit 55f353d has no chaos/), before coverage gates. |
| 20261112-091615-s1 | 1 | - | - | 10000 | PASS | - | 303 | 146 (135) | 119 | 0 | Phase 7 full run A Code: uncommitted Phase 7 tree (the header's commit 55f353d has no chaos/), before coverage gates. |
| 20261112-093316-s8-j1000 | 8 | - | - | 1000 | ERROR | KeyboardInterrupt (see harness.log) | 12 | 5 | | | deliberate `docker stop` at ~12 s to test the crash path: row written, but the process hung until docker's SIGKILL (non-daemon pool threads) → fixed with os._exit after the row | 8ed6bd1aaf8f |
| 20261112-093356-s8-j1000 | 8 | - | - | 1000 | ERROR | KeyboardInterrupt (see harness.log) | 12 | 5 | | | same crash test after the fix: stop took 1 s, exit 3 | 92e74e49e3e3 |
| 20261112-093421-s3-z-j1000 | 3 | - | yes | 1000 | PASS | - | 37 | 16 (15) | 16 | 0 | zombie debug after the review fixes (frozen jobs and their new owners spared by cancellers/killer, a freeze every ~10 s): 3 chances in 3 freezes, vs 0 of 2 in 091436 (same seed) | 92e74e49e3e3 |
