use darksteel::prelude::*;

#[derive(Clone)]
struct Foo;
#[derive(Clone)]
struct Bar;

#[darksteel::async_trait]
impl IntoModule for Foo {
    async fn module(_: &Modules) -> Self {
        Foo
    }
}

#[darksteel::async_trait]
impl IntoModule for Bar {
    async fn module(modules: &Modules) -> Self {
        modules.handle::<Foo>().await;
        Bar
    }
}

/// This test proves that objects with dependent modules can be lazily created
#[tokio::test]
async fn dependencies() {
    let modules = Modules::new();

    modules.handle::<Bar>().await;
}
