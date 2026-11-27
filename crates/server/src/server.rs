use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Context;
use auth::Authenticator;
use cluster::NodeInfo;
use config::ServerConfig;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::connection::{Connection, ConnectionId, DEFAULT_AUTH_TIMEOUT};
use crate::room::RoomManager;
use crate::session::SessionRegistry;

const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(50);
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

struct ActiveConnectionGuard(Arc<AtomicUsize>);

impl ActiveConnectionGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Async TCP server accepting binary-protocol client connections for a chat node.
pub struct Server {
    config: ServerConfig,
    node: NodeInfo,
    authenticator: Arc<dyn Authenticator>,
    room_manager: RoomManager,
    session_registry: SessionRegistry,
    auth_timeout: Duration,
    active_connections: Arc<AtomicUsize>,
}

impl Server {
    /// Creates a new [`Server`] bound to `config`, identified by `node`, and enforcing `authenticator`.
    pub fn new(
        config: ServerConfig,
        node: NodeInfo,
        authenticator: Arc<dyn Authenticator>,
    ) -> Self {
        let session_registry = SessionRegistry::for_node(node.node_id.clone());
        Self {
            config,
            node,
            authenticator,
            room_manager: RoomManager::new(),
            session_registry,
            auth_timeout: DEFAULT_AUTH_TIMEOUT,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Replaces the [`RoomManager`] used by connections accepted by this server.
    pub fn with_room_manager(mut self, room_manager: RoomManager) -> Self {
        self.room_manager = room_manager;
        self
    }

    /// Returns a reference to the server's [`RoomManager`].
    pub fn room_manager(&self) -> &RoomManager {
        &self.room_manager
    }

    /// Replaces the [`SessionRegistry`] used by connections accepted by this server.
    pub fn with_session_registry(mut self, session_registry: SessionRegistry) -> Self {
        self.session_registry = session_registry;
        self
    }

    /// Returns a reference to the server's [`SessionRegistry`].
    pub fn session_registry(&self) -> &SessionRegistry {
        &self.session_registry
    }

    /// Overrides the pre-authentication handshake timeout applied to accepted connections.
    pub fn with_auth_timeout(mut self, auth_timeout: Duration) -> Self {
        self.auth_timeout = auth_timeout;
        self
    }

    /// Returns the number of currently active client connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Returns a shared handle to the active connection counter.
    pub fn active_connections_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active_connections)
    }

    /// Binds a [`TcpListener`] to the configured `host:port`.
    pub async fn bind(&self) -> anyhow::Result<TcpListener> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        TcpListener::bind(&addr)
            .await
            .with_context(|| format!("failed to bind TCP listener on {addr}"))
    }

    /// Binds the TCP listener and runs the accept loop until a shutdown signal is received.
    pub async fn run(self) -> anyhow::Result<()> {
        let listener = self.bind().await?;
        self.run_with_listener(listener, shutdown_signal()).await
    }

    /// Runs the accept loop on a pre-bound `listener` until `shutdown` resolves.
    pub async fn run_with_listener<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> anyhow::Result<()>
    where
        F: Future<Output = ()>,
    {
        let local_addr = listener
            .local_addr()
            .context("failed to read local listener address")?;

        tracing::info!(
            node_id = %self.node.node_id,
            region = %self.node.region,
            listen_addr = %local_addr,
            "TCP server listening"
        );

        let (shutdown_tx, _) = watch::channel(false);
        let next_conn_id = AtomicU64::new(1);
        let mut tasks = JoinSet::new();

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!(
                        node_id = %self.node.node_id,
                        active_connections = self.active_connections(),
                        "shutdown signal received; stopping accept loop"
                    );
                    break;
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, peer_addr)) => {
                            if let Err(err) = stream.set_nodelay(true) {
                                tracing::debug!(peer = %peer_addr, error = %err, "failed to set TCP_NODELAY");
                            }
                            let (reader, writer) = stream.into_split();
                            let conn_id = ConnectionId(next_conn_id.fetch_add(1, Ordering::Relaxed));
                            let conn_shutdown = shutdown_tx.subscribe();
                            let guard = ActiveConnectionGuard::new(Arc::clone(&self.active_connections));
                            let authenticator = Arc::clone(&self.authenticator);
                            let room_manager = self.room_manager.clone();
                            let session_registry = self.session_registry.clone();
                            let auth_timeout = self.auth_timeout;

                            tasks.spawn(async move {
                                let _guard = guard;
                                let mut conn = Connection::new(
                                    conn_id,
                                    peer_addr,
                                    reader,
                                    writer,
                                    authenticator,
                                    room_manager,
                                    session_registry,
                                )
                                .with_auth_timeout(auth_timeout);
                                if let Err(err) = conn.run(conn_shutdown).await {
                                    tracing::debug!(
                                        connection_id = %conn_id,
                                        peer = %peer_addr,
                                        error = %err,
                                        "connection closed with error"
                                    );
                                }
                            });
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "failed to accept TCP connection");
                            tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        }
                    }
                }
                Some(join_res) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(join_err) = join_res
                        && join_err.is_panic()
                    {
                        tracing::error!(error = %join_err, "connection task panicked");
                    }
                }
            }
        }

        drop(listener);
        let _ = shutdown_tx.send(true);

        let drain_result = tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, async {
            while let Some(join_res) = tasks.join_next().await {
                if let Err(join_err) = join_res
                    && join_err.is_panic()
                {
                    tracing::error!(error = %join_err, "connection task panicked during shutdown");
                }
            }
        })
        .await;

        if drain_result.is_err() {
            tracing::warn!(
                remaining = tasks.len(),
                "shutdown drain timed out; aborting remaining connections"
            );
            tasks.shutdown().await;
        }

        tracing::info!(node_id = %self.node.node_id, "TCP server shut down cleanly");
        Ok(())
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                if let Err(err) = res {
                    tracing::warn!(error = %err, "failed to listen for ctrl_c");
                }
            }
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %err, "failed to listen for ctrl_c");
        }
    }
}
