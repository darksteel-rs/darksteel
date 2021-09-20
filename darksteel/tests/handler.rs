use darksteel::{prelude::*, process::handler::Handler};
use std::time::{Duration, SystemTime};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let time = SystemTime::now();
    let mut config = ProcessConfig::default();

    config.name("task");

    let executor = Environment::new();
    let task = Task::<UniversalError>::new_with_config(config, |_| async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(())
    });
    let handler = Handler::<UniversalError>::new(|context| async move {
        if let Some(runtime) = context.runtime() {
            if let Some(reference) = runtime.get_sender_by_name("task").await {
                reference.send(ProcessSignal::Terminate).unwrap();
            }
        }
    });

    executor.start_multiple(&[task, handler]).await;

    assert!(Duration::from_secs(10) > SystemTime::now().duration_since(time).unwrap());

    Ok(())
}
