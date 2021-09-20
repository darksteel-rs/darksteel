use darksteel::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let executor = Environment::new();
    let task = Task::<UniversalError>::new(|_| async {
        println!("Hello, world!");
        Ok(())
    });

    executor.start(task).await;

    Ok(())
}
