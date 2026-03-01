#[async_trait::async_trait]
pub trait StateMachine: Send + Sync + 'static {
    async fn apply(&self, command: &[u8]) -> anyhow::Result<Vec<u8>>;
}
