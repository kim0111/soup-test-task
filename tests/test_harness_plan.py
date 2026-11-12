"""The chaos workload is a pure function of the seed (SPEC §8.1), with the mix the spec asks for."""

from collections import Counter

from chaos.harness import make_plan

N = 10_000


def test_same_seed_same_plan_other_seed_other_plan():
    assert make_plan(1, 500, rate=50) == make_plan(1, 500, rate=50)
    assert make_plan(1, 500, rate=50) != make_plan(2, 500, rate=50)


def test_job_mix_matches_spec():
    plan = make_plan(7, N, rate=30)
    main = {}  # key → the body most of its copies carry
    for s in plan.submits:
        main.setdefault(s.key, Counter())[tuple(sorted(s.body.items()))] += 1
    assert len(main) == N
    bodies = [dict(c.most_common(1)[0][0]) for c in main.values()]

    long = sum(b["duration_ms"] >= 2000 for b in bodies) / N
    fail = sum(b["fail"] for b in bodies) / N
    other_body = sum(len(c) > 1 for c in main.values()) / N
    assert 0.04 < long < 0.06
    assert 0.02 < fail < 0.04
    assert 0.005 < other_body < 0.015
    assert all(20 <= b["duration_ms"] <= 200 or 2000 <= b["duration_ms"] <= 5000 for b in bodies)

    copies = Counter(sum(c.values()) - (len(c) - 1) for c in main.values())  # same-body copies
    assert set(copies) == {1, 2, 3}


def test_copies_are_partly_concurrent_partly_delayed():
    plan = make_plan(3, 2000, rate=30)
    first = {}
    gaps = []
    for s in plan.submits:
        if s.key in first:
            gaps.append(s.at - first[s.key])
        else:
            first[s.key] = s.at
    assert any(g == 0 for g in gaps) and any(g > 0 for g in gaps)
    assert max(gaps) <= 5


def test_submits_are_time_ordered_and_paced_by_rate():
    plan = make_plan(4, 3000, rate=30)
    times = [s.at for s in plan.submits]
    assert times == sorted(times)
    assert 95 < max(times) < 3000 / 30 + 6


def test_about_ten_percent_of_keys_get_a_cancel_within_the_run():
    plan = make_plan(5, N, rate=30)
    assert 0.09 * N <= len(plan.cancels) <= 0.11 * N
    assert all(0 <= c.at <= N / 30 + 10 for c in plan.cancels)
    targeted = sum(c.running_long for c in plan.cancels) / len(plan.cancels)
    assert 0.2 < targeted < 0.4


# --- a PASS must not be vacuous ------------------------------------------------------------------

from collections import Counter as _C  # noqa: E402

from chaos.harness import gates  # noqa: E402


def numbers(**over):
    base = dict(
        requests=_C({("create", 201): 100, ("create", 200): 90, ("create", 409): 2,
                     ("create", 503): 3, ("create", None): 1,
                     ("cancel", 200): 9, ("cancel", 409): 1, ("cancel", None): 1}),
        jobs=100, checked=100, kills_busy=5, re_executions=4, running_cancels=3,
        zombie=False, zombie_chances=0,
    )
    return {**base, **over}


def test_a_real_run_has_no_gates():
    assert gates(**numbers()) == ([], [])
    assert gates(**numbers(zombie=True, zombie_chances=2)) == ([], [])


def test_unexpected_answers_fail_the_run():
    failures, _ = gates(**numbers(requests=_C({("create", 201): 100, ("create", 500): 1,
                                               ("cancel", 404): 2})))
    assert failures == ["create 500 ×1", "cancel 404 ×2"]


def test_a_run_that_exercised_nothing_is_incomplete():
    _, gaps = gates(**numbers(checked=98, kills_busy=0, re_executions=0, running_cancels=0,
                              zombie=True, zombie_chances=0))
    assert gaps == [
        "2 of 100 keys never acknowledged (I1 can't check them)",
        "no kill hit a busy worker",
        "no re-execution",
        "no cancel of a running job",
        "--zombie: no zombie woke up next to a newer running attempt",
    ]


def test_one_unacknowledged_key_in_a_hundred_is_tolerated():
    assert gates(**numbers(checked=99)) == ([], [])


# --- raw logs are kept, compressed (lossless) ----------------------------------------------------

import gzip  # noqa: E402

from chaos.harness import compress_logs  # noqa: E402


def test_raw_logs_are_gzipped_byte_for_byte_and_reports_stay_plain(tmp_path):
    raw = {
        "client.jsonl": b'{"op": "create", "status": 201}\n' * 100,
        "workers/w0-g0.log": b"claimed job=x attempt=1\n" * 100,
        "workers/w1-g3.log": b"",
    }
    plain = {"report.txt": b"verdict PASS\n", "report.json": b"{}", "harness.log": b"header\n"}
    (tmp_path / "workers").mkdir()
    for name, data in {**raw, **plain}.items():
        (tmp_path / name).write_bytes(data)

    compress_logs(tmp_path)

    for name, data in raw.items():
        assert not (tmp_path / name).exists()
        assert gzip.decompress((tmp_path / f"{name}.gz").read_bytes()) == data
    for name, data in plain.items():
        assert (tmp_path / name).read_bytes() == data
    assert sorted(p.name for p in tmp_path.rglob("*.gz")) == [
        "client.jsonl.gz", "w0-g0.log.gz", "w1-g3.log.gz"]
