use darksteel::prelude::*;
use std::{sync::Arc, time::Duration};

async fn task_short(_: TaskContext<TaskError>) -> TaskResult<TaskError> {
    println!("Sleeping a short time!");
    tokio::time::sleep(Duration::from_secs(4)).await;
    println!("Slept a short time!");
    Ok(())
}

fn supervisor_group() -> anyhow::Result<Arc<Supervisor<TaskError>>> {
    let short1 = Task::new(task_short);
    let short2 = Task::new(task_short);

    Ok(Supervisor::build().with(short1)?.with(short2)?.finish())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let executor = Environment::new();

    // Supervisors can supervise other supervisors creating a tree
    let supervisor = Supervisor::build()
        .with(supervisor_group()?)?
        .with(supervisor_group()?)?
        .finish();

    executor.start(supervisor).await;

    Ok(())
}
