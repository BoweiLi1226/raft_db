use std::{
    collections::HashMap,
    sync::{Arc, atomic},
    time::Duration,
};

use tokio::{
    sync::{Mutex, Notify, oneshot},
    task::JoinSet,
    time,
};
use tonic::{Request, transport::Channel};

use crate::{
    raft::{
        AppendEntriesArgs, AppendEntriesReply, LogEntry, RequestVoteArgs, RequestVoteReply,
        raft_config::RaftConfig,
        raft_proto::raft_client::RaftClient,
        raft_state::{RaftState, Role},
    },
    state_machine::StateMachine,
};

#[derive(Debug)]
pub struct RaftNode<T: StateMachine> {
    pub config: Arc<RaftConfig>,
    pub state: Mutex<RaftState>,
    peer_clients: HashMap<u32, RaftClient<Channel>>,
    apply_notify: Arc<Notify>,
    response_channels: Mutex<HashMap<u64, oneshot::Sender<Vec<u8>>>>,
    state_machine: T,
}

impl<T: StateMachine> RaftNode<T> {
    pub fn new(raft_config: Arc<RaftConfig>, state_machine: T) -> Arc<Self> {
        let peer_clients = raft_config.make_clients();

        let raft_node = Arc::new(Self {
            config: Arc::clone(&raft_config),
            state: Mutex::new(RaftState::new(Arc::clone(&raft_config))),
            peer_clients,
            apply_notify: Arc::new(Notify::new()),
            response_channels: Mutex::new(HashMap::new()),
            state_machine,
        });

        Arc::clone(&raft_node).start_background_tasks();
        raft_node
    }

    pub async fn start_command(&self, command: &[u8]) -> Option<oneshot::Receiver<Vec<u8>>> {
        let mut guard = self.state.lock().await;
        if Role::Leader != guard.role {
            None
        } else {
            let term = guard.term;
            let index = guard.log.len() as u64;
            guard.log.push(LogEntry {
                term,
                index,
                command: command.to_vec(),
            });
            tracing::info!(
                "Raft Node {}: I am processing log at index {}",
                self.config.me,
                index,
            );

            let (tx, rx) = oneshot::channel();
            self.response_channels.lock().await.insert(index, tx);
            Some(rx)
        }
    }

    fn start_background_tasks(self: Arc<Self>) {
        let raft_node = Arc::clone(&self);
        tokio::spawn(async move { raft_node.election_ticker().await });

        let raft_node = Arc::clone(&self);
        tokio::spawn(async move { raft_node.heartbeat_ticker().await });

        let raft_node = Arc::clone(&self);
        tokio::spawn(async move { raft_node.apply_ticker().await });
    }

    async fn apply_ticker(self: Arc<Self>) -> ! {
        loop {
            let notified = self.apply_notify.notified();
            let to_apply = {
                let state = self.state.lock().await;
                if state.commit_index > state.last_applied {
                    let start = (state.last_applied + 1) as usize;
                    let end = state.commit_index as usize;
                    let entries = state.log[start..=end].to_vec();
                    Some(entries)
                } else {
                    None
                }
            };

            if let Some(entries) = to_apply {
                self.apply(&entries).await;
                let mut state = self.state.lock().await;
                state.last_applied += entries.len() as u64;
                if state.commit_index > state.last_applied {
                    continue;
                }
            } else {
                notified.await;
            }
        }
    }

    async fn apply(&self, to_apply: &[LogEntry]) {
        for log_entry in to_apply {
            tracing::info!(
                "Raft Node {}: I am applying log at index {}",
                self.config.me,
                log_entry.index
            );
            if let Ok(result) = self.state_machine.apply(&log_entry.command).await
                && let Some(sender) = self.response_channels.lock().await.remove(&log_entry.index)
            {
                tracing::info!(
                    "Raft Node {}: I am notifying log at index {}",
                    self.config.me,
                    log_entry.index
                );
                let _ = sender.send(result);
            }
        }
    }

    async fn election_ticker(self: Arc<Self>) -> ! {
        // TODO: Pending Improvement?
        // Right now if last_tick gets reset right after sleep starts,
        // should_elect would be false.
        // Not sure if this should be the case though.
        loop {
            let timeout = Duration::from_millis(rand::random_range(350..=600));
            time::sleep(timeout).await;
            let should_elect = {
                let state = self.state.lock().await;
                state.role != Role::Leader && state.timeout(timeout)
            };
            if should_elect {
                Arc::clone(&self).election().await;
            }
        }
    }

    async fn election(self: Arc<Self>) {
        let (args, current_term) = {
            let mut state = self.state.lock().await;
            state.become_candidate();
            let Some(last_log) = state.log.last() else {
                panic!(
                    "Raft node {}: I should have had at least one dummy log entry",
                    self.config.me,
                );
            };
            (
                RequestVoteArgs {
                    term: state.term,
                    candidate_id: self.config.me,
                    last_log_index: (state.log.len() - 1) as u64,
                    last_log_term: last_log.term,
                },
                state.term,
            )
        };

        let notify_on_vote = Arc::new(Notify::new());

        // voted for self
        let vote_obtained = Arc::new(atomic::AtomicU32::new(1));
        let vote_needed = (self.config.nodes.len() / 2 + 1) as u32;

        // rpc calls
        let mut tasks = JoinSet::new();
        for client in self.peer_clients.values() {
            let request = Request::new(args);
            let mut client = client.clone();
            let vote_obtained = Arc::clone(&vote_obtained);
            let notify_on_vote = Arc::clone(&notify_on_vote);
            let raft_node = Arc::clone(&self);
            tasks.spawn(async move {
                if let Ok(response) = client.request_vote(request).await {
                    let reply = response.into_inner();
                    if reply.vote_granted {
                        let current_vote = vote_obtained.fetch_add(1, atomic::Ordering::SeqCst) + 1;
                        if current_vote >= vote_needed {
                            notify_on_vote.notify_one();
                        }
                    } else {
                        let mut state = raft_node.state.lock().await;
                        if reply.term > state.term {
                            state.become_follower(reply.term);
                        }
                    }
                }
            });
        }

        tokio::select! {
            _ = notify_on_vote.notified() => {
                let mut state = self.state.lock().await;
                if state.term == current_term && state.role == Role::Candidate {
                    state.become_leader();
                } else {
                    return;
                }
            }
            _ = time::sleep(Duration::from_millis(300)) => {}
        }
        tasks.abort_all();
    }

    async fn heartbeat_ticker(self: Arc<Self>) -> ! {
        loop {
            time::sleep(Duration::from_millis(100)).await;
            let should_send_heartbeat = {
                let state = self.state.lock().await;
                state.role == Role::Leader
            };
            if should_send_heartbeat {
                Arc::clone(&self).heartbeat().await;
            }
        }
    }

    async fn heartbeat(self: Arc<Self>) {
        let mut tasks = JoinSet::new();
        let (mut requests, target_index) = {
            let state = self.state.lock().await;
            let Some(next_indices) = &state.next_indices else {
                return; // Not leader any more
            };
            let mut requests = HashMap::with_capacity(next_indices.len());
            let target_index = (state.log.len() - 1) as u64;
            for (&id, &start_index) in next_indices {
                let prev_log_index = start_index - 1;
                let prev_log_term = state.log[prev_log_index as usize].term;
                let mut entries = Vec::with_capacity((prev_log_index + 1 - start_index) as usize);
                entries.extend_from_slice(&state.log[(start_index as usize)..]);
                requests.insert(
                    id,
                    Request::new(AppendEntriesArgs {
                        term: state.term,
                        leader_id: self.config.me,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: state.commit_index,
                    }),
                );
            }
            (requests, target_index)
        };
        let number_of_success = Arc::new(atomic::AtomicU32::new(1));
        let success_needed_for_commit = (self.config.nodes.len() / 2 + 1) as u32;
        let notify_commit = Arc::new(Notify::new());
        for (&id, client) in &self.peer_clients {
            let Some(request) = requests.remove(&id) else {
                tracing::error!(
                    "Raft node {}: I didn't initialize append entries request properly for node {}",
                    self.config.me,
                    id
                );
                continue;
            };
            let mut client = client.clone();
            let number_of_success = Arc::clone(&number_of_success);
            let notify_commit = Arc::clone(&notify_commit);
            let raft_node = Arc::clone(&self);
            tasks.spawn(async move {
                if let Ok(response) = client.append_entries(request).await {
                    let reply = response.into_inner();
                    let mut state = raft_node.state.lock().await;
                    if reply.term > state.term {
                        state.become_follower(reply.term);
                    }

                    if reply.success {
                        let Some(next_indices) = state.next_indices.as_mut() else {
                            return;
                        };
                        next_indices.insert(id, target_index + 1);
                        let Some(match_indices) = state.match_indices.as_mut() else {
                            return;
                        };
                        match_indices.insert(id, target_index);
                        let number_of_success =
                            number_of_success.fetch_add(1, atomic::Ordering::SeqCst) + 1;
                        if number_of_success >= success_needed_for_commit {
                            state.commit_index = target_index;
                            raft_node.apply_notify.notify_waiters();
                            notify_commit.notify_one();
                        }
                    } else {
                        let Some(next_indices) = state.next_indices.as_mut() else {
                            return;
                        };
                        let next_index = next_indices[&id];
                        if next_index > 1 {
                            next_indices.insert(id, next_index - 1);
                        }
                    }
                }
            });
        }
        tokio::select! {
            _ = notify_commit.notified() => {},
            _ = time::sleep(Duration::from_millis(80)) => {},
        }
    }

    pub async fn handle_request_vote(&self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply::default();
        let mut state = self.state.lock().await;
        reply.term = state.term;
        reply.vote_granted = false;
        // reject vote if received request from node with lower term
        if args.term < state.term {
            return reply;
        } else {
            state.become_follower(args.term);
        }
        // reject if log of the other node is not up to date
        if !state.is_other_node_log_up_to_date(args.last_log_term, args.last_log_index) {
            return reply;
        }
        match state.voted_for {
            None => {
                reply.vote_granted = true;
                state.voted_for = Option::Some(args.candidate_id);
                tracing::info!(
                    "Raft node {}: I voted for {} in term {}.",
                    self.config.me,
                    args.candidate_id,
                    state.term,
                );
            }
            Some(id) if id == args.candidate_id => {
                reply.vote_granted = true;
                tracing::info!(
                    "Raft node {}: I already voted for {} in term {}.",
                    self.config.me,
                    args.candidate_id,
                    state.term,
                );
            }
            Some(already_voted_for) => {
                tracing::info!(
                    "Raft node {}: I refused to vote for node {} in term {} because I already voted for {}.",
                    self.config.me,
                    args.candidate_id,
                    state.term,
                    already_voted_for,
                );
            }
        }
        reply
    }

    pub async fn handle_append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let mut reply = AppendEntriesReply::default();
        let mut state = self.state.lock().await;
        reply.term = state.term;
        reply.success = false;

        // heartbeat comes from invalid leader
        if args.term < state.term {
            return reply;
        }

        // heartbeat comes from valid leader from now on
        state.reset_tick();
        if (args.term > state.term) || (args.term == state.term && state.role != Role::Follower) {
            state.become_follower(args.term);
        }

        if !state.contains_prev_log(args.prev_log_index, args.prev_log_term) {
            return reply;
        }
        state.append_log(args.prev_log_index, &args.entries);
        state.update_commit(args.leader_commit);

        if state.commit_index > state.last_applied {
            self.apply_notify.notify_waiters();
        }

        reply.success = true;
        reply
    }
}
