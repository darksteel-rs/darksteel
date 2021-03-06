use darksteel::prelude::*;
use std::time::Duration;

async fn task_short(_: TaskContext<TaskError>) -> TaskResult<TaskError> {
    println!("Sleeping a short time!");
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("Slept a short time!");
    Ok(())
}

async fn task_long(_: TaskContext<TaskError>) -> TaskResult<TaskError> {
    println!("Sleeping a long time!");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("Slept a long time!");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let executor = Environment::new();
    // This task will always run to completion and trigger a full restart due to
    // the default supervisor restart policy being "OneForAll," which triggers
    // every process to restart if one fails.
    let short = Task::new(task_short);
    // This task will never run to completion and be brutally terminated per its
    // child spec.
    let long = Task::new(task_long);

    let supervisor = Supervisor::build().with(short)?.with(long)?.finish();

    executor.start(supervisor).await;

    Ok(())
}
