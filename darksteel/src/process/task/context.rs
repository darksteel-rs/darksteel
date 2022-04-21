use crate::{
    modules::Modules,
    process::{container::ProcessRef, runtime::Runtime},
};
use std::sync::Weak;

use super::TaskErrorTrait;

pub struct TaskContext<E>
where
    E: TaskErrorTrait,
{
    pub modules: Modules,
    pub process: ProcessRef<E>,
    pub runtime: Weak<Runtime<E>>,
}

impl<E> TaskContext<E>
where
    E: TaskErrorTrait,
{
    pub fn new(modules: Modules, process: ProcessRef<E>, runtime: Weak<Runtime<E>>) -> Self {
        Self {
            modules,
            process,
            runtime,
        }
    }
}
