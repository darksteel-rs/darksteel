#![doc = include_str!("../../README.md")]
//! # Getting started!
//!
//! ## Basic Example
//!```rust
#![doc = include_str!("../examples/hello.rs")]
//! ```
//!
//! ## Basic Supervisor
//!```rust
#![doc = include_str!("../examples/supervisor_restart_all.rs")]
//! ```

/// Contains the environment data structure.
pub mod environment;
/// Contains the top-level error types and structs.
pub mod error;
/// Contains the traits for creating identities to be used in the distributed
/// system.
pub mod identity;
/// A collection of basic modules to be shared by all running [tasks](crate::process::task::Task).
pub mod modules;
/// All of the processes that can be spawned in an [`Environment`](crate::environment::Environment).
pub mod process;

/// The default prelude with all the most important types.
pub mod prelude {
    pub use crate::environment::Environment;
    pub use crate::error::{TaskError, UserError};
    pub use crate::identity::IdentityTrait;
    pub use crate::modules::{Module, Modules};
    pub use crate::process::{
        supervisor::{
            AutomaticTerminationPolicy, Supervisor, SupervisorConfig, SupervisorRestartPolicy,
        },
        task::{Task, TaskContext, TaskErrorTrait, TaskResult},
        *,
    };
}

/// Re-export of `async_trait`
pub use async_trait::async_trait;
pub use darksteel_codegen::{distributed, identity};
