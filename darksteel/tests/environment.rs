use darksteel::{
    error::UniversalError,
    prelude::*,
    process::{
        supervisor::{RestartPolicy, SupervisorConfig},
        task::Task,
    },
};
use std::time::Duration;
use tracing_subscriber::{fmt::Layer, prelude::*};

//
//  SETUP
//
fn log() {
    let fmt_layer = Layer::default();
    tracing_subscriber::registry().with(fmt_layer).init();
}

//
//  TESTS
//
async fn task_hello(_: Modules) -> TaskResult<UniversalError> {
    tracing::info!("Hello, world!");
    Ok(())
}

async fn task_short(_: Modules) -> TaskResult<UniversalError> {
    tracing::info!("Sleeping a short time!");
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("Slept a short time!");
    Ok(())
}

async fn task_long(_: Modules) -> TaskResult<UniversalError> {
    tracing::info!("Sleeping a long time!");
    tokio::time::sleep(Duration::from_secs(3)).await;
    tracing::info!("Slept a long time!");
    Ok(())
}

#[tokio::test]
async fn hello() -> anyhow::Result<()> {
    log();

    let executor = Environment::new();
    let task = Task::new(task_hello);

    executor.start(task).await;

    Ok(())
}

#[tokio::test]
async fn restart_one() -> anyhow::Result<()> {
    log();

    let mut config = SupervisorConfig::default();
    let executor = Environment::new();
    let short1 = Task::new(task_short);
    let short2 = Task::new(task_short);

    config.restart_policy = RestartPolicy::OneForOne;

    let supervisor = Supervisor::build_with_config(Default::default(), config)
        .with(short1)?
        .with(short2)?
        .finish();

    executor.start(supervisor).await;

    Ok(())
}

#[tokio::test]
async fn restart_all() -> anyhow::Result<()> {
    log();

    let executor = Environment::new();
    let short = Task::new(task_short);
    let long = Task::new(task_long);

    let supervisor = Supervisor::build().with(short)?.with(long)?.finish();

    executor.start(supervisor).await;

    Ok(())
}

#[tokio::test]
async fn restart_rest() -> anyhow::Result<()> {
    log();

    let mut config = SupervisorConfig::default();
    let executor = Environment::new();
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
