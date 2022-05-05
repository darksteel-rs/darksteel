use darksteel::prelude::*;

#[derive(Clone)]
struct Foo;
#[derive(Clone)]
struct Bar;

#[darksteel::async_trait]
impl Module for Foo {
    async fn module(_: &Modules) -> Result<Self, UserError> {
        Ok(Foo)
    }
}

#[darksteel::async_trait]
impl Module for Bar {
    async fn module(modules: &Modules) -> Result<Self, UserError> {
        modules.handle::<Foo>().await?;
        Ok(Bar)
    }
}

/// This test proves that objects with dependent modules can be lazily created
#[tokio::test]
async fn dependencies() {
    let modules = Modules::new();

    modules.handle::<Bar>().await.ok();
}
