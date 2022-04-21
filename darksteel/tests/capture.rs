use darksteel::prelude::*;

#[tokio::test]
async fn capture() -> anyhow::Result<()> {
    let mut config = ProcessConfig::default();

    config.name("task");
    config.restart_policy = ChildRestartPolicy::Temporary;

    let executor = Environment::<TaskError>::new();
    let potato = Box::new(4);
    let task = Task::new_with_config(config, move |_| async move {
        println!("{}", potato);
        Ok(())
    });

    executor.start(task).await;

    Ok(())
}
