-- Schema for the training-job queue. See Docs/SPEC.md §3.

CREATE TABLE jobs (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key  text  NOT NULL UNIQUE,
  request_hash     text  NOT NULL,                -- sha256 of the validated body (after defaults)
  payload          jsonb NOT NULL,                -- {"duration_ms": 300, "fail": false}
  status           text  NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
  attempt          int   NOT NULL DEFAULT 0,      -- fencing token, +1 on every claim
  worker_id        text,                          -- diagnostics only
  lease_expires_at timestamptz,
  error            text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz,
  CHECK ((status = 'running') = (lease_expires_at IS NOT NULL))
);
CREATE INDEX ix_jobs_queued  ON jobs (created_at, id)   WHERE status = 'queued';
CREATE INDEX ix_jobs_running ON jobs (lease_expires_at) WHERE status = 'running';
CREATE INDEX ix_jobs_page    ON jobs (created_at DESC, id DESC);

-- The side effect. Deliberately NO UNIQUE(job_id): a fencing bug must show up as a countable
-- duplicate, not be hidden by a constraint.
CREATE TABLE job_effects (
  id         bigserial PRIMARY KEY,
  job_id     uuid NOT NULL REFERENCES jobs(id),
  attempt    int  NOT NULL,
  worker_id  text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ix_job_effects_job ON job_effects (job_id);

-- Audit log, written in the same transaction as the status change, so a kill can't separate them.
CREATE TABLE job_transitions (
  seq         bigserial PRIMARY KEY,               -- total order
  job_id      uuid NOT NULL,
  from_status text,                                -- NULL on insert
  to_status   text NOT NULL,
  attempt     int  NOT NULL,
  at          timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ix_job_transitions_job ON job_transitions (job_id, seq);

CREATE FUNCTION log_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO job_transitions (job_id, from_status, to_status, attempt)
  VALUES (NEW.id,
          CASE WHEN TG_OP = 'UPDATE' THEN OLD.status END,
          NEW.status,
          NEW.attempt);
  RETURN NULL;
END
$$;

-- Deliberately NO `WHEN (OLD.status IS DISTINCT FROM NEW.status)`: a double claim writes
-- running -> running, and that row is exactly what invariant I8 must see (mutation M1).
CREATE TRIGGER trg_job_transitions
  AFTER INSERT OR UPDATE OF status ON jobs
  FOR EACH ROW EXECUTE FUNCTION log_job_transition();
