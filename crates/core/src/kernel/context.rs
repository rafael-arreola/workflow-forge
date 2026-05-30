use std::time::Instant;

use uuid::Uuid;

pub struct WorkflowContext {
    pub workflow_id: String,
    execution_at: Instant,
}

impl WorkflowContext {
    pub fn new() -> Self {
        let workflow_id = Uuid::now_v7().to_string();
        Self {
            workflow_id,
            execution_at: Instant::now(),
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    pub fn get_elapsed(&self) -> std::time::Duration {
        self.execution_at.elapsed()
    }
}
