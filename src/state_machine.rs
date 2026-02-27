// TODO: this should be used by raft node
// TODO: shared kv store should implement this
trait StateMachine {
    fn apply(&mut self, command: &[u8]) -> anyhow::Result<Vec<u8>>;
}
