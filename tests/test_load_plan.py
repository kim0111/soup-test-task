"""The 100 rps load test (SPEC §10, Docs/VERDICT.md): the request plan is a pure function of the
seed, and the criteria are computed exactly as VERDICT.md states them (written before any run)."""

import math
from collections import Counter

from loadtest.load import (
    MATRIX_CODE, criteria, is_error, make_plan, percentile, summarize,
)

N_RPS, N_S = 100, 300  # the verdict run: 30,000 requests


# --- plan ----------------------------------------------------------------------------------------

def test_same_seed_same_plan_other_seed_other_plan():
    assert make_plan(1, rps=100, duration=20, mix="short") == make_plan(1, rps=100, duration=20, mix="short")
    assert make_plan(1, rps=100, duration=20, mix="short") != make_plan(2, rps=100, duration=20, mix="short")


def test_open_loop_schedule_is_fixed_by_the_rate_alone():
    plan = make_plan(3, rps=N_RPS, duration=N_S, mix="short")
    assert len(plan) == N_RPS * N_S
    assert all(math.isclose(r.at, i / N_RPS) for i, r in enumerate(plan))


def test_request_mix_matches_spec():
    plan = make_plan(5, rps=N_RPS, duration=N_S, mix="short")
    share = {op: n / len(plan) for op, n in Counter(r.op for r in plan).items()}
    assert abs(share["get"] - 0.60) < 0.01
    assert abs(share["create"] - 0.25) < 0.01
    assert abs(share["list"] - 0.10) < 0.01
    assert abs(share["cancel"] - 0.05) < 0.007


def test_about_ten_percent_of_creates_retry_an_earlier_key_with_the_same_body():
    plan = make_plan(5, rps=N_RPS, duration=N_S, mix="short")
    first: dict[str, dict] = {}
    retries = creates = 0
    for r in plan:
        if r.op != "create":
            continue
        creates += 1
        if r.retry:
            retries += 1
            assert first[r.key] == r.body  # KeyError = a retry of a key never sent before
        else:
            assert r.key not in first
            first[r.key] = r.body
    assert 0.08 < retries / creates < 0.12


def test_short_mix_has_only_short_jobs_and_three_percent_fail():
    bodies = [r.body for r in make_plan(7, rps=N_RPS, duration=N_S, mix="short")
              if r.op == "create" and not r.retry]
    assert all(20 <= b["duration_ms"] <= 200 for b in bodies)
    assert 0.02 < sum(b["fail"] for b in bodies) / len(bodies) < 0.04


def test_chaos_mix_has_five_percent_long_jobs():
    bodies = [r.body for r in make_plan(7, rps=N_RPS, duration=N_S, mix="chaos")
              if r.op == "create" and not r.retry]
    long = [b for b in bodies if b["duration_ms"] >= 2000]
    assert 0.04 < len(long) / len(bodies) < 0.06
    assert all(2000 <= b["duration_ms"] <= 5000 for b in long)
    assert all(20 <= b["duration_ms"] <= 200 for b in bodies if b not in long)


def test_list_asks_for_fifty_and_half_filter_queued():
    lists = [r.params for r in make_plan(9, rps=N_RPS, duration=N_S, mix="short") if r.op == "list"]
    assert all(p["limit"] == 50 for p in lists)
    assert 0.45 < sum(p.get("status") == "queued" for p in lists) / len(lists) < 0.55


# --- what counts as an error (VERDICT C2) -----------------------------------------------------------

def test_expected_answers_are_not_errors():
    for op, status in [("create", 201), ("create", 200), ("get", 200), ("list", 200),
                       ("cancel", 200), ("cancel", 409)]:
        assert not is_error(op, status), (op, status)


def test_503_timeouts_404_and_anything_unexpected_are_errors():
    for op, status in [("create", 503), ("get", 503), ("get", 500), ("get", 404),
                       ("cancel", 404), ("create", 409), ("create", 400), ("list", 422),
                       ("get", None), ("create", None)]:
        assert is_error(op, status), (op, status)


# --- latency -------------------------------------------------------------------------------------

def test_percentile_is_nearest_rank():
    values = [i / 1000 for i in range(1, 101)]  # 1..100 ms
    assert percentile(values, 0.50) == 0.050
    assert percentile(values, 0.99) == 0.099
    assert percentile([0.005], 0.99) == 0.005


def rec(t, op, status, lat, lag=0.001, **kw):
    return {"t": t, "op": op, "status": status, "lat": lat, "lag": lag, **kw}


def test_timeouts_count_as_slower_than_any_answer():
    # 98 fast answers + 2 timeouts out of 100: p99 must not be a fast number.
    recs = [rec(40, "get", 200, 0.004) for _ in range(98)] + [rec(40, "get", None, None)] * 2
    assert summarize(recs, warmup=30)["endpoints"]["get"]["p99_ms"] == math.inf


def test_warm_up_is_excluded_from_latency_but_not_from_errors():
    # 3 % slow, failing warm-up requests: well above the 1 % a p99 may ignore.
    recs = ([rec(1, "get", 503, 1.5)] * 30
            + [rec(40, "get", 200, 0.004)] * 970)
    s = summarize(recs, warmup=30)
    assert s["endpoints"]["get"]["p99_ms"] == 4.0
    assert s["errors"] == 30 and s["requests"] == 1000
    assert math.isclose(s["error_rate"], 0.03)


def test_skipped_requests_are_not_sent_and_count_as_late():
    recs = [{"t": 0.0, "op": "get", "skipped": True, "lag": 0.0}] + [rec(40, "get", 200, 0.004)] * 99
    s = summarize(recs, warmup=30)
    assert s["requests"] == 99 and s["skipped"] == 1
    assert math.isclose(s["on_time"], 0.99)


def test_on_time_means_sent_within_fifty_ms_of_schedule():
    recs = [rec(40, "get", 200, 0.004, lag=0.049)] * 98 + [rec(40, "get", 200, 0.004, lag=0.051)] * 2
    assert math.isclose(summarize(recs, warmup=30)["on_time"], 0.98)


# --- criteria and verdict (VERDICT C1–C5 + validity) --------------------------------------------------

def good_summary(**over):
    s = {
        "requests": 30_000, "skipped": 0, "errors": 0, "error_rate": 0.0, "on_time": 1.0,
        "endpoints": {op: {"n": 1000, "p50_ms": 3.0, "p99_ms": 20.0}
                      for op in ("create", "get", "list", "cancel")},
    }
    s.update(over)
    return s


def judge(summary=None, **over):
    kw = {"queue": [(t, 3, 4) for t in range(0, 300)], "drain_s": 6.0, "invariants_ok": True,
          "code": MATRIX_CODE, "kills_busy": 50, "re_executions": 50}
    kw.update(over)
    return criteria(summary or good_summary(), **kw)


def failed(rows):
    return sorted(r["id"] for r in rows if not r["ok"])


def test_a_clean_run_passes():
    verdict, rows = judge()
    assert verdict == "PASS" and failed(rows) == []


def test_error_rate_must_be_strictly_below_a_tenth_of_a_percent():
    assert judge(good_summary(errors=29, error_rate=29 / 30_000))[0] == "PASS"
    verdict, rows = judge(good_summary(errors=30, error_rate=0.001))
    assert verdict == "FAIL" and failed(rows) == ["C2"]


def test_each_endpoint_p99_must_be_below_200_ms():
    s = good_summary()
    s["endpoints"]["cancel"]["p99_ms"] = 200.0
    verdict, rows = judge(s)
    assert verdict == "FAIL" and failed(rows) == ["C3"]


def test_an_endpoint_with_no_samples_fails_latency():
    s = good_summary()
    s["endpoints"]["list"] = {"n": 0, "p50_ms": None, "p99_ms": None}
    assert failed(judge(s)[1]) == ["C3"]


def test_queue_bound_applies_after_warm_up_only():
    assert judge(queue=[(10, 500, 4), (31, 100, 4)])[0] == "PASS"
    verdict, rows = judge(queue=[(10, 3, 4), (31, 101, 4)])
    assert verdict == "FAIL" and failed(rows) == ["C4"]


def test_queue_must_drain_within_sixty_seconds():
    assert failed(judge(drain_s=60.5)[1]) == ["C4"]
    assert failed(judge(drain_s=None)[1]) == ["C4"]  # never drained


def test_invariants_and_code_hash_are_criteria():
    assert failed(judge(invariants_ok=False)[1]) == ["C5"]
    assert failed(judge(code="000000000000")[1]) == ["C1"]


def test_a_run_that_did_not_load_or_kill_is_invalid_never_a_pass():
    assert judge(good_summary(on_time=0.989))[0] == "INVALID"
    assert judge(kills_busy=0)[0] == "INVALID"
    assert judge(re_executions=0)[0] == "INVALID"
    # Invalid wins over a failing criterion: the numbers don't describe the system under 100 rps.
    assert judge(good_summary(on_time=0.5, errors=900, error_rate=0.03))[0] == "INVALID"


# --- the generator's send path (review finding: the anti-coordinated-omission property was untested) --

import argparse
import asyncio
import io
import time

import httpx

from loadtest import load
from loadtest.load import LoadRun, Req


def bare_run() -> LoadRun:
    run = LoadRun(argparse.Namespace(mix="short", seed=1, duration=300.0, api="http://test"))
    run.clog = io.StringIO()  # no run dir: nothing is written to results/
    return run


def send(run: LoadRun, req: Req, handler) -> dict:
    async def go():
        async with httpx.AsyncClient(base_url="http://test",
                                     transport=httpx.MockTransport(handler)) as client:
            await run._send(client, req)
    asyncio.run(go())
    return run.records[-1]


def test_latency_counts_from_the_scheduled_time_not_from_the_send():
    run = bare_run()
    run.t0 = time.monotonic() - 0.2  # this request was due 200 ms ago: the generator is late
    rec = send(run, Req(0.0, "list", params={"limit": 50}),
               lambda r: httpx.Response(200, json={"items": [], "next_cursor": None}))
    assert rec["status"] == 200
    assert rec["lag"] >= 0.2
    assert rec["lat"] >= 0.2  # measured from the send it would be ~0 ms: the generator's delay hidden


def test_no_answer_within_the_timeout_is_an_unknown_outcome(monkeypatch):
    monkeypatch.setattr(load, "TIMEOUT", 0.05)

    async def slow(request):
        await asyncio.sleep(1.0)
        return httpx.Response(201, json={"id": "j1"})

    run = bare_run()
    run.t0 = time.monotonic()
    rec = send(run, Req(0.0, "create", "k1", {"duration_ms": 50, "fail": False}), slow)
    assert (rec["status"], rec["lat"], rec["error"]) == (None, None, "timeout")
    assert run.known == []  # an unacknowledged key is never a get/cancel target


def test_only_the_first_acknowledgement_of_a_key_becomes_a_target():
    run = bare_run()
    run.t0 = time.monotonic()
    body = {"duration_ms": 50, "fail": False}
    send(run, Req(0.0, "create", "k1", body), lambda r: httpx.Response(201, json={"id": "j1"}))
    send(run, Req(0.0, "create", "k1", body, retry=True), lambda r: httpx.Response(200, json={"id": "j1"}))
    assert run.known == [("k1", "j1")]
    seen = []
    rec = send(run, Req(0.0, "cancel", u=0.99),
               lambda r: seen.append(r.url.path) or httpx.Response(200, json={"id": "j1"}))
    assert seen == ["/jobs/j1/cancel"] and (rec["key"], rec["id"]) == ("k1", "j1")
