use darksteel::prelude::*;
use std::time::Duration;

async fn task_short(_: TaskContext<TaskError>) -> TaskResult<TaskError> {
    tracing::info!("Sleeping a short time!");
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("Slept a short time!");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = SupervisorConfig::default();
    let executor = Environment::new();

    // Both of these tasks will run to completion and be restarted individually.
    let short1 = Task::new(task_short);
    let short2 = Task::new(task_short);

    config.restart_policy = SupervisorRestartPolicy::OneForOne;

    let supervisor = Supervisor::build_with_config(Default::default(), config)
        .with(short1)?
        .with(short2)?
        .finish();

    executor.start(supervisor).await;

    Ok(())
}
