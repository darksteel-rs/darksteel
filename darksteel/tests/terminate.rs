use darksteel::prelude::*;
use std::time::Duration;

#[tokio::test]
async fn handler() -> anyhow::Result<()> {
    let mut config = ProcessConfig::default();

    config.name("task");
    config.restart_policy = ChildRestartPolicy::Temporary;

    let executor = Environment::<TaskError>::new();
    let task = Task::new_with_config(config, |_| async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("You shouldn't see me!");
        Ok(())
    });
    let terminator = Task::new(|context| async move {
        // Terminate the other process.
        if let Some(runtime) = context.runtime.upgrade() {
            if let Some(reference) = runtime.get_sender_by_name("task").await {
                reference.send(ProcessSignal::Terminate).unwrap();
            }
        }

        // Tell the terminator to shut itself down.
        context
            .process
            .sender()
            .send(ProcessSignal::Shutdown)
            .unwrap();

        Ok(())
    });

    executor.start_multiple(&[task, terminator]).await;

    Ok(())
}
