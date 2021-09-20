use darksteel::prelude::*;
use std::time::Duration;

async fn task_short(_: Modules) -> TaskResult<UniversalError> {
    println!("Sleeping a short time!");
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("Slept a short time!");
    Ok(())
}

async fn task_long(_: Modules) -> TaskResult<UniversalError> {
    println!("Sleeping a long time!");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("Slept a long time!");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = SupervisorConfig::default();
    let executor = Environment::new();
    // Both of these tasks will run to completion but the order the tasks are
    // added will determine dependence, first being least and last being most.
    // Tasks that exit will have their dependent tasks restarted. In this
    // instance, when the short task exits, it only restarts itself. When long
    // exits, it restarts itself and short.
    let short = Task::new(task_short);
    let long = Task::new(task_long);

    config.restart_policy = RestartPolicy::RestForOne;

    let supervisor = Supervisor::build_with_config(Default::default(), config)
        .with(long)?
        .with(short)?
        .finish();

    executor.start(supervisor).await;

    Ok(())
}
