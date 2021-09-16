use darksteel::modules::distributed::node::NodeConfig;
use darksteel::modules::distributed::{discovery::Discovery, node::Node};
use darksteel::prelude::*;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::ops::Range;
use std::time::Duration;
use tokio::time::interval;

struct Local(u8, Range<u32>);

impl Local {
    fn new(subnet: u8, range: Range<u32>) -> Self {
        Self(subnet, range)
    }
}

#[darksteel::async_trait]
impl Discovery for Local {
    async fn discover(&self) -> anyhow::Result<Vec<IpAddr>> {
        Ok(self
            .1
            .clone()
            .into_iter()
            .map(|index| format!("127.0.{}.{}", self.0, index).parse().unwrap())
            .collect())
    }
}

#[darksteel::identity("StateMachine")]
#[derive(Clone, Default, Serialize, Deserialize)]
struct StateMachine {
    state: String,
}

impl StateMachine {
    fn state(&self) -> String {
        self.state.clone()
    }
}

#[darksteel::distributed]
impl StateMachine {
    fn update(&mut self, state: String) {
        self.state = state;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_form() -> anyhow::Result<()> {
    let mut nodes = Vec::new();
    let mut interval = interval(Duration::from_secs(2));
    interval.tick().await;

    for index in 1..5 {
        let node = Node::build_with_config(NodeConfig::new(
            // Make sure the IPs are on different subnets across tests
            format!("127.0.0.{}", index).parse().unwrap(),
            "test",
            Local::new(0, 1..5),
        ))
        .with::<StateMachine>()
        .finish()
        .await?;

        nodes.push(node);
    }

    for controller in &nodes {
        controller.initialise().await.unwrap();
    }

    interval.tick().await;

    for node in &nodes {
        assert_ne!(node.leader().await, None);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_distribute() -> anyhow::Result<()> {
    let mut nodes = Vec::new();
    let mut interval = interval(Duration::from_secs(2));
    interval.tick().await;

    for index in 1..5 {
        let node = Node::build_with_config(NodeConfig::new(
            // Make sure the IPs are on different subnets across tests
            format!("127.0.1.{}", index).parse().unwrap(),
            "test",
            Local::new(0, 1..5),
        ))
        .with::<StateMachine>()
        .finish()
        .await?;

        nodes.push(node);
    }

    for node in &nodes {
        node.initialise().await.unwrap();
    }

    interval.tick().await;

    for node in &nodes {
        if Some(node.id()) == node.leader().await {
            let mutation = StateMachine::create_update("It works!".into());
            node.commit(mutation).await?;
        }
    }

    interval.tick().await;

    for node in &nodes {
        if let Some(state) = node.state::<StateMachine>().await {
            assert_eq!(state.state(), "It works!");
        }
    }

    Ok(())
}