use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use config::RedisConfig;
use futures_util::StreamExt;
use redis::aio::{ConnectionManager, PubSubSink, PubSubStream};
use redis::{AsyncCommands, Client};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::broker::PubSubBackend;
use crate::error::PubSubError;
use crate::keys::{KeySpace, validate_segment};
use crate::message::PubSubEnvelope;

const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(2);
const CMD_CHANNEL_CAPACITY: usize = 512;

enum ActorCmd {
    RetainRoom {
        room_id: u64,
        reply: oneshot::Sender<Result<bool, PubSubError>>,
    },
    ReleaseRoom {
        room_id: u64,
        reply: oneshot::Sender<Result<bool, PubSubError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Redis-backed implementation of [`PubSubBackend`] using a multiplexed [`ConnectionManager`] for
/// writes and a dedicated [`redis::aio::PubSub`] listener task for dynamic room/node subscriptions.
#[derive(Clone)]
pub struct RedisPubSub {
    node_id: Arc<str>,
    keyspace: KeySpace,
    cmd_tx: mpsc::Sender<ActorCmd>,
    event_tx: broadcast::Sender<PubSubEnvelope>,
    conn_manager: ConnectionManager,
    local_users: Arc<Mutex<HashMap<String, usize>>>,
    shutting_down: Arc<AtomicBool>,
    actor_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl std::fmt::Debug for RedisPubSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSub")
            .field("node_id", &self.node_id)
            .field("prefix", &self.keyspace.prefix())
            .field("shutting_down", &self.shutting_down.load(Ordering::Relaxed))
            .finish()
    }
}

impl RedisPubSub {
    /// Connects to Redis, subscribes to this node's direct channel, and spawns the listener loop.
    pub async fn connect(
        cfg: &RedisConfig,
        node_id: impl Into<String>,
    ) -> Result<Self, PubSubError> {
        if cfg.channel_capacity == 0 {
            return Err(PubSubError::Validation(
                "redis channel_capacity must be greater than 0".to_string(),
            ));
        }
        let node_id_clean = validate_segment(&node_id.into(), "node_id")?;
        let keyspace = KeySpace::new(&cfg.key_prefix)?;
        let node_channel = keyspace.node_channel(&node_id_clean)?;

        let client = Client::open(cfg.url.as_str())?;
        let conn_manager = client.get_connection_manager().await?;

        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.subscribe(&node_channel).await?;
        let (sink, stream) = pubsub.split();

        let (event_tx, _rx) = broadcast::channel(cfg.channel_capacity);
        let (cmd_tx, cmd_rx) = mpsc::channel(CMD_CHANNEL_CAPACITY);

        let shutting_down = Arc::new(AtomicBool::new(false));
        let actor_handle = tokio::spawn(run_pubsub_actor(ActorContext {
            client,
            node_channel,
            keyspace: keyspace.clone(),
            sink,
            stream,
            cmd_rx,
            event_tx: event_tx.clone(),
        }));

        Ok(Self {
            node_id: Arc::from(node_id_clean),
            keyspace,
            cmd_tx,
            event_tx,
            conn_manager,
            local_users: Arc::new(Mutex::new(HashMap::new())),
            shutting_down,
            actor_handle: Arc::new(Mutex::new(Some(actor_handle))),
        })
    }

    /// Returns the [`KeySpace`] configured for this broker.
    pub fn keyspace(&self) -> &KeySpace {
        &self.keyspace
    }

    fn ensure_open(&self) -> Result<(), PubSubError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(PubSubError::Closed);
        }
        Ok(())
    }
}

struct ActorContext {
    client: Client,
    node_channel: String,
    keyspace: KeySpace,
    sink: PubSubSink,
    stream: PubSubStream,
    cmd_rx: mpsc::Receiver<ActorCmd>,
    event_tx: broadcast::Sender<PubSubEnvelope>,
}

async fn handle_actor_command(
    cmd: ActorCmd,
    sink: &mut PubSubSink,
    keyspace: &KeySpace,
    room_counts: &mut HashMap<u64, usize>,
) -> bool {
    match cmd {
        ActorCmd::RetainRoom { room_id, reply } => {
            let current = room_counts.get(&room_id).copied().unwrap_or(0);
            if current == 0 {
                let channel = keyspace.room_channel(room_id);
                if let Err(err) = sink.subscribe(&channel).await {
                    let _ = reply.send(Err(PubSubError::Redis(err)));
                    return true;
                }
            }
            room_counts.insert(room_id, current + 1);
            let _ = reply.send(Ok(current == 0));
            true
        }
        ActorCmd::ReleaseRoom { room_id, reply } => {
            let current = room_counts.get(&room_id).copied().unwrap_or(0);
            match current {
                0 => {
                    let _ = reply.send(Ok(false));
                }
                1 => {
                    let channel = keyspace.room_channel(room_id);
                    if let Err(err) = sink.unsubscribe(&channel).await {
                        let _ = reply.send(Err(PubSubError::Redis(err)));
                        return true;
                    }
                    room_counts.remove(&room_id);
                    let _ = reply.send(Ok(true));
                }
                n => {
                    room_counts.insert(room_id, n - 1);
                    let _ = reply.send(Ok(false));
                }
            }
            true
        }
        ActorCmd::Shutdown { reply } => {
            let _ = reply.send(());
            false
        }
    }
}

async fn reconnect_pubsub(
    client: &Client,
    node_channel: &str,
    keyspace: &KeySpace,
    room_counts: &mut HashMap<u64, usize>,
    cmd_rx: &mut mpsc::Receiver<ActorCmd>,
) -> Option<(PubSubSink, PubSubStream)> {
    let mut backoff = INITIAL_RECONNECT_BACKOFF;

    loop {
        match client.get_async_pubsub().await {
            Ok(mut pubsub) => {
                if let Err(err) = pubsub.subscribe(node_channel).await {
                    tracing::warn!(error = %err, "failed to re-subscribe node channel after reconnect");
                } else {
                    let mut all_ok = true;
                    for &room_id in room_counts.keys() {
                        let channel = keyspace.room_channel(room_id);
                        if let Err(err) = pubsub.subscribe(&channel).await {
                            tracing::warn!(room_id, error = %err, "failed to re-subscribe room after reconnect");
                            all_ok = false;
                            break;
                        }
                    }
                    if all_ok {
                        tracing::info!(
                            node_channel,
                            resubscribed_rooms = room_counts.len(),
                            "re-established Redis Pub/Sub connection"
                        );
                        return Some(pubsub.split());
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, delay_ms = backoff.as_millis(), "Redis Pub/Sub reconnect failed");
            }
        }

        let sleep = tokio::time::sleep(backoff);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                maybe_cmd = cmd_rx.recv() => {
                    match maybe_cmd {
                        None => return None,
                        Some(ActorCmd::Shutdown { reply }) => {
                            let _ = reply.send(());
                            return None;
                        }
                        Some(ActorCmd::RetainRoom { reply, .. })
                        | Some(ActorCmd::ReleaseRoom { reply, .. }) => {
                            let _ = reply.send(Err(PubSubError::Closed));
                        }
                    }
                }
            }
        }

        backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
    }
}

async fn run_pubsub_actor(mut ctx: ActorContext) {
    let mut room_counts: HashMap<u64, usize> = HashMap::new();

    loop {
        tokio::select! {
            maybe_cmd = ctx.cmd_rx.recv() => {
                match maybe_cmd {
                    Some(cmd) => {
                        if !handle_actor_command(cmd, &mut ctx.sink, &ctx.keyspace, &mut room_counts).await {
                            break;
                        }
                    }
                    None => break,
                }
            }
            maybe_msg = ctx.stream.next() => {
                match maybe_msg {
                    Some(msg) => {
                        let channel_name = msg.get_channel_name().to_string();
                        match msg.get_payload::<String>() {
                            Ok(raw) => match PubSubEnvelope::decode_json(&raw) {
                                Ok(envelope) => {
                                    let _ = ctx.event_tx.send(envelope);
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        channel = %channel_name,
                                        error = %err,
                                        "ignoring malformed Pub/Sub JSON envelope"
                                    );
                                }
                            },
                            Err(err) => {
                                tracing::warn!(
                                    channel = %channel_name,
                                    error = %err,
                                    "ignoring non-UTF-8 Redis Pub/Sub payload"
                                );
                            }
                        }
                    }
                    None => {
                        tracing::warn!(node_channel = %ctx.node_channel, "Redis Pub/Sub stream disconnected; reconnecting");
                        match reconnect_pubsub(
                            &ctx.client,
                            &ctx.node_channel,
                            &ctx.keyspace,
                            &mut room_counts,
                            &mut ctx.cmd_rx,
                        ).await {
                            Some((new_sink, new_stream)) => {
                                ctx.sink = new_sink;
                                ctx.stream = new_stream;
                            }
                            None => break,
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl PubSubBackend for RedisPubSub {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn subscribe_events(&self) -> broadcast::Receiver<PubSubEnvelope> {
        self.event_tx.subscribe()
    }

    async fn register_user_session(&self, username: &str) -> Result<bool, PubSubError> {
        self.ensure_open()?;
        let clean_user = validate_segment(username, "username")?;
        let redis_key = self.keyspace.user_nodes_key(&clean_user)?;

        let mut users = self.local_users.lock().await;
        let current = users.get(&clean_user).copied().unwrap_or(0);
        if current == 0 {
            let mut mgr = self.conn_manager.clone();
            let _: usize = mgr.sadd(&redis_key, self.node_id.as_ref()).await?;
        }
        users.insert(clean_user, current + 1);
        Ok(current == 0)
    }

    async fn unregister_user_session(&self, username: &str) -> Result<bool, PubSubError> {
        let clean_user = validate_segment(username, "username")?;
        let redis_key = self.keyspace.user_nodes_key(&clean_user)?;

        let mut users = self.local_users.lock().await;
        let current = users.get(&clean_user).copied().unwrap_or(0);
        match current {
            0 => Ok(false),
            1 => {
                let mut mgr = self.conn_manager.clone();
                let _: usize = mgr.srem(&redis_key, self.node_id.as_ref()).await?;
                users.remove(&clean_user);
                Ok(true)
            }
            n => {
                users.insert(clean_user, n - 1);
                Ok(false)
            }
        }
    }

    async fn lookup_user_nodes(&self, username: &str) -> Result<Vec<String>, PubSubError> {
        let clean_user = validate_segment(username, "username")?;
        let redis_key = self.keyspace.user_nodes_key(&clean_user)?;
        let mut mgr = self.conn_manager.clone();
        let mut nodes: Vec<String> = mgr.smembers(&redis_key).await?;
        nodes.sort_unstable();
        Ok(nodes)
    }

    async fn prune_stale_user_node(
        &self,
        username: &str,
        stale_node_id: &str,
    ) -> Result<(), PubSubError> {
        let clean_user = validate_segment(username, "username")?;
        let clean_node = validate_segment(stale_node_id, "stale_node_id")?;
        let redis_key = self.keyspace.user_nodes_key(&clean_user)?;
        let mut mgr = self.conn_manager.clone();
        let _: usize = mgr.srem(&redis_key, &clean_node).await?;
        Ok(())
    }

    async fn subscribe_room(&self, room_id: u64) -> Result<bool, PubSubError> {
        self.ensure_open()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCmd::RetainRoom {
                room_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PubSubError::Closed)?;
        reply_rx.await.map_err(|_| PubSubError::Closed)?
    }

    async fn unsubscribe_room(&self, room_id: u64) -> Result<bool, PubSubError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Ok(false);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCmd::ReleaseRoom {
                room_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PubSubError::Closed)?;
        reply_rx.await.map_err(|_| PubSubError::Closed)?
    }

    async fn publish_room(&self, envelope: &PubSubEnvelope) -> Result<usize, PubSubError> {
        self.ensure_open()?;
        let room_id = match envelope {
            PubSubEnvelope::RoomBroadcast { room_id, .. } => *room_id,
            PubSubEnvelope::DirectMessage { .. } => {
                return Err(PubSubError::Validation(
                    "publish_room requires RoomBroadcast envelope".to_string(),
                ));
            }
        };
        let channel = self.keyspace.room_channel(room_id);
        let payload = envelope.encode_json()?;
        let mut mgr = self.conn_manager.clone();
        let receivers: usize = mgr.publish(&channel, payload).await?;
        Ok(receivers)
    }

    async fn publish_direct(
        &self,
        target_node_id: &str,
        envelope: &PubSubEnvelope,
    ) -> Result<usize, PubSubError> {
        self.ensure_open()?;
        if !matches!(envelope, PubSubEnvelope::DirectMessage { .. }) {
            return Err(PubSubError::Validation(
                "publish_direct requires DirectMessage envelope".to_string(),
            ));
        }
        let channel = self.keyspace.node_channel(target_node_id)?;
        let payload = envelope.encode_json()?;
        let mut mgr = self.conn_manager.clone();
        let receivers: usize = mgr.publish(&channel, payload).await?;
        Ok(receivers)
    }

    async fn shutdown(&self) -> Result<(), PubSubError> {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let remaining_users: Vec<String> = {
            let mut users = self.local_users.lock().await;
            users.drain().map(|(username, _)| username).collect()
        };

        let mut mgr = self.conn_manager.clone();
        for username in remaining_users {
            if let Ok(redis_key) = self.keyspace.user_nodes_key(&username)
                && let Err(err) = mgr
                    .srem::<_, _, usize>(&redis_key, self.node_id.as_ref())
                    .await
            {
                tracing::warn!(%username, error = %err, "failed to unregister user session from Redis during shutdown");
            }
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(ActorCmd::Shutdown { reply: reply_tx })
            .await
            .is_ok()
        {
            let _ = reply_rx.await;
        }

        if let Some(handle) = self.actor_handle.lock().await.take()
            && let Err(err) = handle.await
        {
            tracing::warn!(error = %err, "Redis Pub/Sub actor task ended unexpectedly");
        }

        Ok(())
    }
}
