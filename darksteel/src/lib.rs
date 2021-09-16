pub mod environment;
pub mod error;
pub mod identity;
pub mod modules;
pub mod process;

pub mod prelude {
    pub use crate::environment::Environment;
    pub use crate::identity::IdentityTrait;
    pub use crate::modules::{IntoModule, Modules};
    pub use crate::process::{
        supervisor::Supervisor,
        task::{TaskError, TaskResult},
        *,
    };
}

pub use async_trait::async_trait;
pub use darksteel_codegen::{distributed, identity};