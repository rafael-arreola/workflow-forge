CREATE TABLE wf_artifacts (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    media_type TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('staging','ready')),
    bytes INTEGER NOT NULL CHECK(bytes>=0),
    chunks INTEGER NOT NULL CHECK(chunks>=0),
    orphan_after_ms INTEGER NOT NULL CHECK(orphan_after_ms>=0),
    ever_owned INTEGER NOT NULL CHECK(ever_owned IN (0,1))
) STRICT;
CREATE TABLE wf_artifact_chunks (
    artifact_id TEXT NOT NULL REFERENCES wf_artifacts(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK(sequence>=0),
    body BLOB NOT NULL CHECK(length(body)>0 AND length(body)<=65536),
    PRIMARY KEY(artifact_id,sequence)
) STRICT;
CREATE TABLE wf_artifact_owners (
    artifact_id TEXT NOT NULL REFERENCES wf_artifacts(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES wf_runs(id) ON DELETE CASCADE,
    PRIMARY KEY(artifact_id,run_id)
) STRICT;
CREATE INDEX wf_artifact_runs ON wf_artifact_owners(run_id,artifact_id);
