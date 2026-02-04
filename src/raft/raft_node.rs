use std::{
    collections::HashMap,
    sync::{Arc, atomic},
    time::Duration,
};

use tokio::{
    sync::{Mutex, Notify, mpsc},
    task::JoinSet,
    time,
};
use tonic::{Request, transport::Channel};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, RequestVoteArgs, RequestVoteReply,
    raft_config::RaftConfig,
    raft_proto::raft_client::RaftClient,
    raft_state::{RaftState, Role},
};

#[derive(Debug)]
pub struct RaftNode {
    pub config: RaftConfig,
    pub state: Mutex<RaftState>,
    pub log_tx: mpsc::Sender<LogEntry>,
    peer_clients: HashMap<u32, RaftClient<Channel>>,
}

impl RaftNode {
    pub fn new(raft_config: RaftConfig) -> Arc<Self> {
        let peer_clients = raft_config.make_clients();
        let me = raft_config.me;
        let peer_ids: Vec<u32> = raft_config.nodes.iter().map(|(&id, _)| id).collect();
        let (log_tx, log_rx) = mpsc::channel::<LogEntry>(15);

        let raft_node = Arc::new(Self {
            config: raft_config,
            state: Mutex::new(RaftState::with_self_and_peer_ids(me, peer_ids)),
            log_tx,
            peer_clients,
        });

        Arc::clone(&raft_node).start_background_tasks(log_rx);
        raft_node
    }

    pub async fn get_state(self: Arc<Self>) -> (Role, u32, u64) {
        let state = self.state.lock().await;
        (state.role, self.config.me, state.term)
    }

    fn start_background_tasks(self: Arc<Self>, log_rx: mpsc::Receiver<LogEntry>) {
        let raft_node = Arc::clone(&self);
        tokio::spawn(async move { raft_node.election_ticker().await });

        let raft_node = Arc::clone(&self);
        tokio::spawn(async move { raft_node.heartbeat_ticker().await });

        let raft_node = Arc::clone(&self);
        tokio::spawn(async move { raft_node.apply_ticker(log_rx).await });
    }

    async fn apply_ticker(self: Arc<Self>, log_rx: mpsc::Receiver<LogEntry>) -> ! {
        let mut log_rx = log_rx;
        loop {
            let mut batch: Vec<LogEntry> = Vec::with_capacity(15);
            // only awaken if we receive one log entry from channel
            match log_rx.recv().await {
                Some(log_entry) => batch.push(log_entry),
                None => panic!("Raft node {}: My apply channel is closed", self.config.me),
            }

            // we wait for a total of 100 milliseconds or 20 messages
            let sleep = time::sleep(Duration::from_millis(100));
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut sleep => { break; }
                    msg = log_rx.recv() => {
                        if let Some(msg) = msg {
                            batch.push(msg);
                            if batch.len() >= 20 {
                                break;
                            }
                        } else {
                            panic!("Raft node {}: My apply channel is closed", self.config.me);
                        }
                    }
                }
            }
            Arc::clone(&self).apply(&batch).await;
        }
    }

    async fn apply(self: Arc<Self>, to_apply: &[LogEntry]) {
        todo!("To be implemented");
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
                state.role != Role::LEADER && state.timeout(timeout)
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
                if state.term == current_term && state.role == Role::CANDIDATE {
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
                state.role == Role::LEADER
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
        let notify_on_commit_update = Arc::new(Notify::new());
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
            let notify_on_commit_update = Arc::clone(&notify_on_commit_update);
            let raft_node = Arc::clone(&self);
            tasks.spawn(async move {
                if let Ok(response) = client.append_entries(request).await {
                    let reply = response.into_inner();
                    let mut state = raft_node.state.lock().await;
                    if reply.term > state.term {
                        state.become_follower(reply.term);
                    }

                    let Some(match_indices) = state.match_indices.as_mut() else {
                        return;
                    };
                    if reply.success {
                        match_indices.insert(id, target_index);
                    }

                    let Some(next_indices) = state.next_indices.as_mut() else {
                        return;
                    };
                    if reply.success {
                        next_indices.insert(id, target_index + 1);
                        let number_of_success =
                            number_of_success.fetch_add(1, atomic::Ordering::SeqCst) + 1;
                        if number_of_success >= success_needed_for_commit {
                            notify_on_commit_update.notify_one();
                        }
                    } else {
                        let next_index = next_indices[&id];
                        if next_index > 1 {
                            next_indices.insert(id, next_index - 1);
                        }
                    }
                }
            });
        }
        tokio::select! {
            _ = notify_on_commit_update.notified() => {
                let to_apply = {
                    let mut state = self.state.lock().await;
                    let last_index = (state.log.len() - 1) as u64;
                    let mut entries = vec![];
                    if target_index > state.commit_index && target_index <= last_index && state.log[target_index as usize].term == state.term {
                        state.commit_index = target_index;
                        if state.commit_index > state.last_applied {
                            let start = (state.last_applied + 1) as usize;
                            let end = state.commit_index as usize;
                            entries = state.log[start..=end].to_vec()
                        }
                    }
                    entries
                };

                let mut logs_sent = 0;
                for log in to_apply.into_iter() {
                    let Ok(_) = self.log_tx.send(log).await else {
                        tracing::error!(
                            "Raft node {}: I failed to send log",
                            self.config.me,
                        );
                        break;
                    };
                    logs_sent += 1;
                }
                if logs_sent > 0 {
                    self.state.lock().await.last_applied += logs_sent;
                }
            }
            _ = time::sleep(Duration::from_millis(100)) => {}
        }
        tasks.abort_all();
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
        if (args.term > state.term) || (args.term == state.term && state.role != Role::FOLLOWER) {
            state.become_follower(args.term);
        }

        if !state.contains_prev_log(args.prev_log_index, args.prev_log_term) {
            return reply;
        }
        state.append_log(args.prev_log_index, &args.entries);
        state.update_commit(args.leader_commit);

        // notify applier
        let mut to_apply = vec![];
        if state.commit_index > state.last_applied {
            let start = (state.last_applied + 1) as usize;
            let end = state.commit_index as usize;
            to_apply = state.log[start..=end].to_vec();
        }

        drop(state);

        let mut logs_sent = 0;
        for log in to_apply.into_iter() {
            let Ok(_) = self.log_tx.send(log).await else {
                tracing::error!("Raft node {}: I failed to send log", self.config.me,);
                break;
            };
            logs_sent += 1;
        }
        if logs_sent > 0 {
            self.state.lock().await.last_applied += logs_sent;
        }

        reply.success = true;
        reply
    }
}
