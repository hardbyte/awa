//! A cheap cooperative stop signal shared by every task in the run.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Clone)]
pub struct Stop {
    sender: Arc<watch::Sender<bool>>,
}

impl Stop {
    pub fn new() -> Self {
        Self {
            sender: Arc::new(watch::channel(false).0),
        }
    }

    pub fn trigger(&self) {
        self.sender.send_replace(true);
    }

    pub fn is_stopped(&self) -> bool {
        *self.sender.borrow()
    }

    /// Sleeps for `duration` or until stopped. Returns true when stopped.
    pub async fn sleep(&self, duration: Duration) -> bool {
        if self.is_stopped() {
            return true;
        }
        let mut receiver = self.sender.subscribe();
        tokio::select! {
            _ = tokio::time::sleep(duration) => false,
            _ = receiver.wait_for(|stopped| *stopped) => true,
        }
    }

    /// Completes once stopped.
    pub async fn wait(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|stopped| *stopped).await;
    }
}

impl Default for Stop {
    fn default() -> Self {
        Self::new()
    }
}
