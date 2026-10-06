// FluxCache - Graceful Shutdown
//
// Provides a cooperative shutdown mechanism using tokio's CancellationToken pattern.
// All server tasks monitor this signal and clean up on shutdown.

use tokio::sync::broadcast;

/// Shutdown signal broadcaster.
///
/// Components subscribe to this to receive shutdown notifications.
/// The server signals shutdown on SIGINT/SIGTERM or explicit request.
#[derive(Debug, Clone)]
pub struct ShutdownSignal {
    sender: broadcast::Sender<()>,
}

impl ShutdownSignal {
    /// Create a new shutdown signal.
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1);
        ShutdownSignal { sender }
    }

    /// Subscribe to receive shutdown notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    /// Signal all subscribers to shut down.
    pub fn shutdown(&self) {
        let _ = self.sender.send(());
    }

    /// Check if shutdown has been signaled.
    pub fn is_shutdown(&self) -> bool {
        self.sender.receiver_count() == 0
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Wait for an OS shutdown signal (SIGINT or SIGTERM on Unix).
pub async fn wait_for_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => {
                tracing::info!("Received SIGINT, initiating graceful shutdown");
            }
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM, initiating graceful shutdown");
            }
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("failed to listen for Ctrl-C");
        tracing::info!("Received Ctrl-C, initiating graceful shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_signal() {
        let signal = ShutdownSignal::new();
        let mut rx = signal.subscribe();

        signal.shutdown();

        // Should receive the shutdown signal
        assert!(rx.recv().await.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let signal = ShutdownSignal::new();
        let mut rx1 = signal.subscribe();
        let mut rx2 = signal.subscribe();

        signal.shutdown();

        assert!(rx1.recv().await.is_ok());
        assert!(rx2.recv().await.is_ok());
    }
}
