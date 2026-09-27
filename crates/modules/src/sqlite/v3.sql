ALTER TABLE wf_runs ADD COLUMN next_wakeup_at_ms INTEGER CHECK(next_wakeup_at_ms>=0);
CREATE INDEX wf_runs_wakeup ON wf_runs(state,next_wakeup_at_ms);
