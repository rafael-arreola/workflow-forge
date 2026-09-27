CREATE TABLE wf_meta (id INTEGER PRIMARY KEY CHECK (id=1), owner TEXT);
INSERT INTO wf_meta(id,owner) VALUES(1,NULL);
CREATE TABLE wf_packages (id TEXT PRIMARY KEY, body BLOB NOT NULL) STRICT;
CREATE TABLE wf_runs (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=0),
    state TEXT NOT NULL CHECK(state IN ('accepted','running','waiting','blocked','cancelling','succeeded','failed','cancelled')),
    cancel_requested INTEGER NOT NULL CHECK(cancel_requested IN (0,1)),
    deadline_at_ms INTEGER NOT NULL CHECK(deadline_at_ms>=0),
    finished_at_ms INTEGER,
    retained_bytes INTEGER NOT NULL CHECK(retained_bytes>=0),
    invocation_count INTEGER NOT NULL CHECK(invocation_count>=0),
    unresolved_count INTEGER NOT NULL CHECK(unresolved_count>=0 AND unresolved_count<=invocation_count),
    package_id TEXT NOT NULL REFERENCES wf_packages(id),
    body BLOB NOT NULL
) STRICT;
CREATE INDEX wf_runs_active ON wf_runs(state,id);
CREATE INDEX wf_runs_finished ON wf_runs(finished_at_ms,id);
CREATE TABLE wf_invocations (
    run_id TEXT NOT NULL REFERENCES wf_runs(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    retained_bytes INTEGER NOT NULL CHECK(retained_bytes>=0),
    unresolved INTEGER NOT NULL CHECK(unresolved IN (0,1)),
    body BLOB NOT NULL,
    PRIMARY KEY(run_id,path)
) STRICT;
CREATE INDEX wf_invocations_unresolved ON wf_invocations(run_id,path) WHERE unresolved=1;
CREATE TABLE wf_receipts (
    scope TEXT NOT NULL,
    receipt_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL CHECK(expires_at_ms>=0),
    request BLOB NOT NULL,
    PRIMARY KEY(scope,receipt_key)
) STRICT;
CREATE INDEX wf_receipts_expiry ON wf_receipts(expires_at_ms);
