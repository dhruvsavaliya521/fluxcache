// FluxCache - Singleflight / Request Coalescing
//
// Implements the singleflight pattern: when multiple requests arrive for
// the same key simultaneously, only one "loader" executes and all waiters
// receive the shared result.
//
// Design: Uses a DashMap-like approach with tokio broadcast channels.
// When a key is being loaded, subsequent requesters subscribe to the
// broadcast channel and wait for the result.

use bytes::Bytes;
use std::collections::HashMap;

use tokio::sync::{broadcast, Mutex};

/// Error from singleflight operations.
#[derive(Debug, thiserror::Error, Clone)]
pub enum SingleflightError {
    #[error("Loader failed: {0}")]
    LoaderFailed(String),

    #[error("Request was cancelled")]
    Cancelled,

    #[error("Channel closed")]
    ChannelClosed,
}

/// Result of a singleflight operation.
#[derive(Debug, Clone)]
pub enum SingleflightResult {
    /// This caller was the "owner" who executed the loader.
    Owner(Result<Bytes, SingleflightError>),
    /// This caller was a "waiter" who received a shared result.
    Waiter(Result<Bytes, SingleflightError>),
}

impl SingleflightResult {
    /// Extract the inner result regardless of owner/waiter status.
    pub fn into_result(self) -> Result<Bytes, SingleflightError> {
        match self {
            SingleflightResult::Owner(r) => r,
            SingleflightResult::Waiter(r) => r,
        }
    }
}

/// In-flight request state.
struct InFlight {
    /// Broadcast sender for sharing the result.
    tx: broadcast::Sender<Result<Bytes, String>>,
}

/// Singleflight group for request coalescing.
///
/// Multiple callers requesting the same key will share a single
/// loader execution. Only the first caller runs the loader;
/// subsequent callers wait for the result.
pub struct SingleflightGroup {
    /// Map of currently in-flight keys.
    in_flight: Mutex<HashMap<String, InFlight>>,
    /// Metrics: how many requests were deduplicated.
    dedup_count: std::sync::atomic::AtomicU64,
}

impl SingleflightGroup {
    /// Create a new singleflight group.
    pub fn new() -> Self {
        SingleflightGroup {
            in_flight: Mutex::new(HashMap::new()),
            dedup_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Execute a loader for the given key, deduplicating concurrent requests.
    ///
    /// If no in-flight request exists for this key, the loader is executed.
    /// If an in-flight request exists, the caller waits for the shared result.
    ///
    /// The loader function receives the key and returns a Result<Bytes, String>.
    pub async fn do_work<F, Fut>(&self, key: &str, loader: F) -> SingleflightResult
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = Result<Bytes, String>>,
    {
        // Check if there's already an in-flight request
        {
            let map = self.in_flight.lock().await;
            if let Some(in_flight) = map.get(key) {
                // Subscribe to the existing broadcast
                let mut rx = in_flight.tx.subscribe();
                self.dedup_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Drop the lock before waiting
                drop(map);

                // Wait for the result
                match rx.recv().await {
                    Ok(Ok(data)) => {
                        return SingleflightResult::Waiter(Ok(data));
                    }
                    Ok(Err(e)) => {
                        return SingleflightResult::Waiter(Err(SingleflightError::LoaderFailed(e)));
                    }
                    Err(_) => {
                        return SingleflightResult::Waiter(Err(SingleflightError::ChannelClosed));
                    }
                }
            }
        }

        // No in-flight request — we're the owner
        let (tx, _rx) = broadcast::channel(1);
        {
            let mut map = self.in_flight.lock().await;
            map.insert(key.to_string(), InFlight { tx: tx.clone() });
        }

        // Execute the loader
        let result = loader(key.to_string()).await;

        // Broadcast the result (ignore errors — no receivers is fine)
        let _ = tx.send(result.clone());

        // Remove from in-flight
        {
            let mut map = self.in_flight.lock().await;
            map.remove(key);
        }

        match result {
            Ok(data) => SingleflightResult::Owner(Ok(data)),
            Err(e) => SingleflightResult::Owner(Err(SingleflightError::LoaderFailed(e))),
        }
    }

    /// Get the number of deduplicated requests.
    pub fn dedup_count(&self) -> u64 {
        self.dedup_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for SingleflightGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_single_request() {
        let group = Arc::new(SingleflightGroup::new());

        let result = group
            .do_work("key1", |_key| async { Ok(Bytes::from("value1")) })
            .await;

        match result {
            SingleflightResult::Owner(Ok(data)) => assert_eq!(data, Bytes::from("value1")),
            _ => panic!("Expected Owner(Ok)"),
        }
    }

    #[tokio::test]
    async fn test_loader_error_propagates() {
        let group = Arc::new(SingleflightGroup::new());

        let result = group
            .do_work("key1", |_key| async {
                Err::<Bytes, String>("load failed".to_string())
            })
            .await;

        match result {
            SingleflightResult::Owner(Err(SingleflightError::LoaderFailed(msg))) => {
                assert_eq!(msg, "load failed");
            }
            _ => panic!("Expected Owner(Err)"),
        }
    }

    #[tokio::test]
    async fn test_concurrent_deduplication() {
        let group = Arc::new(SingleflightGroup::new());
        let call_count = Arc::new(AtomicU32::new(0));

        let mut handles = vec![];

        for _ in 0..10 {
            let group = Arc::clone(&group);
            let call_count = Arc::clone(&call_count);

            handles.push(tokio::spawn(async move {
                group
                    .do_work("shared_key", |_key| {
                        let call_count = Arc::clone(&call_count);
                        async move {
                            call_count.fetch_add(1, Ordering::SeqCst);
                            // Simulate slow load
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            Ok(Bytes::from("shared_value"))
                        }
                    })
                    .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            let data = result.into_result().unwrap();
            assert_eq!(data, Bytes::from("shared_value"));
        }

        // The loader should have been called only once
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_different_keys_parallel() {
        let group = Arc::new(SingleflightGroup::new());

        let g1 = Arc::clone(&group);
        let h1 = tokio::spawn(async move {
            g1.do_work("key1", |_| async { Ok(Bytes::from("v1")) })
                .await
                .into_result()
                .unwrap()
        });

        let g2 = Arc::clone(&group);
        let h2 = tokio::spawn(async move {
            g2.do_work("key2", |_| async { Ok(Bytes::from("v2")) })
                .await
                .into_result()
                .unwrap()
        });

        assert_eq!(h1.await.unwrap(), Bytes::from("v1"));
        assert_eq!(h2.await.unwrap(), Bytes::from("v2"));
    }

    #[tokio::test]
    async fn test_failed_request_does_not_poison() {
        let group = Arc::new(SingleflightGroup::new());

        // First request fails
        let result = group
            .do_work("key1", |_| async {
                Err::<Bytes, String>("fail".to_string())
            })
            .await;
        assert!(result.into_result().is_err());

        // Second request should be able to succeed
        let result = group
            .do_work("key1", |_| async { Ok(Bytes::from("success")) })
            .await;
        assert_eq!(result.into_result().unwrap(), Bytes::from("success"));
    }
}
