use crate::process::task::ProcessId;

#[derive(Debug, thiserror::Error)]
pub enum SupervisorBuilderError {
    #[error("Child with id: `{0}` is significant cannot be added to a supervisor that doesn't auto-terminate")]
    SignificantChild(ProcessId),
    #[error("Child with id: `{0}` has already been added, ignoring")]
    ChildExists(ProcessId),
}
