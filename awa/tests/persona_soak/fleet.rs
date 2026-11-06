//! A persona's group of runtimes, with the capacity bookkeeping the stall check
//! needs and the lifecycle operations the operator and chaos personas use.

use super::config::Persona;
use super::db::{ClientFactory, ClientHost};
use super::registry::QueueInfo;
use super::shared::Shared;
use sqlx::PgPool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct Fleet {
    pub name: String,
    pub persona: Persona,
    /// Every runtime in the fleet serves these queues with this many workers.
    served: Vec<(Arc<QueueInfo>, i64)>,
    factory: ClientFactory,
    pool_size: u32,
    worker_threads: usize,
    url: String,
    hosts: Mutex<Vec<Option<ClientHost>>>,
    budget_label: String,
}

impl Fleet {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        shared: &Arc<Shared>,
        name: &str,
        persona: Persona,
        replicas: usize,
        pool_size: u32,
        worker_threads: usize,
        served: Vec<(Arc<QueueInfo>, i64)>,
        factory: ClientFactory,
    ) -> Result<Arc<Self>, String> {
        let url = shared.config.database_url.clone();
        Self::start_with_url(
            shared,
            url,
            name,
            persona,
            replicas,
            pool_size,
            worker_threads,
            served,
            factory,
        )
        .await
    }

    /// As [`Fleet::start`], with every runtime connecting through `url`, for a fleet that
    /// runs as a role other than the one in the run's database URL.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_url(
        shared: &Arc<Shared>,
        url: String,
        name: &str,
        persona: Persona,
        replicas: usize,
        pool_size: u32,
        worker_threads: usize,
        served: Vec<(Arc<QueueInfo>, i64)>,
        factory: ClientFactory,
    ) -> Result<Arc<Self>, String> {
        let budget_label = name.to_string();
        let granted = shared.budget.reserve(
            &budget_label,
            pool_size * replicas as u32,
            2 * replicas as u32,
        );
        let pool_size = (granted / replicas as u32).max(2);
        let fleet = Arc::new(Self {
            name: name.to_string(),
            persona,
            served,
            factory,
            pool_size,
            worker_threads,
            url,
            hosts: Mutex::new(Vec::new()),
            budget_label,
        });
        for index in 0..replicas {
            let host = fleet.spawn_host(index).await?;
            fleet.hosts.lock().await.push(Some(host));
        }
        shared
            .fleets
            .lock()
            .expect("fleets")
            .insert(name.to_string(), fleet.clone());
        Ok(fleet)
    }

    async fn spawn_host(&self, index: usize) -> Result<ClientHost, String> {
        let host = ClientHost::spawn(
            self.url.clone(),
            format!("{}_{index}", self.name),
            self.pool_size,
            self.worker_threads,
            self.factory.clone(),
        )
        .await?;
        for (queue, workers) in &self.served {
            queue.capacity.fetch_add(*workers, Ordering::Relaxed);
        }
        Ok(host)
    }

    fn release_capacity(&self) {
        for (queue, workers) in &self.served {
            queue.capacity.fetch_sub(*workers, Ordering::Relaxed);
        }
    }

    pub async fn replicas(&self) -> usize {
        self.hosts.lock().await.len()
    }

    pub async fn live_replicas(&self) -> Vec<usize> {
        self.hosts
            .lock()
            .await
            .iter()
            .enumerate()
            .filter_map(|(index, host)| host.is_some().then_some(index))
            .collect()
    }

    pub async fn pool_of(&self, index: usize) -> Option<PgPool> {
        self.hosts
            .lock()
            .await
            .get(index)?
            .as_ref()
            .map(|host| host.pool().clone())
    }

    /// Stops runtime `index` gracefully and starts a fresh one in its place.
    pub async fn restart(&self, index: usize, timeout: Duration) -> Result<bool, String> {
        let old = self
            .hosts
            .lock()
            .await
            .get_mut(index)
            .and_then(Option::take);
        let Some(old) = old else { return Ok(false) };
        self.release_capacity();
        old.shutdown(timeout).await;
        let fresh = self.spawn_host(index).await?;
        self.hosts.lock().await[index] = Some(fresh);
        Ok(true)
    }

    /// Kills runtime `index` with no shutdown handshake, then (optionally) starts a
    /// replacement after `downtime`.
    pub async fn kill(&self, index: usize, downtime: Option<Duration>) -> Result<bool, String> {
        let old = self
            .hosts
            .lock()
            .await
            .get_mut(index)
            .and_then(Option::take);
        let Some(old) = old else { return Ok(false) };
        self.release_capacity();
        old.kill();
        if let Some(downtime) = downtime {
            tokio::time::sleep(downtime).await;
            let fresh = self.spawn_host(index).await?;
            self.hosts.lock().await[index] = Some(fresh);
        }
        Ok(true)
    }

    pub async fn shutdown_all(&self, timeout: Duration, shared: &Shared) {
        let hosts: Vec<ClientHost> = self
            .hosts
            .lock()
            .await
            .iter_mut()
            .filter_map(Option::take)
            .collect();
        for host in hosts {
            self.release_capacity();
            host.shutdown(timeout).await;
        }
        shared.budget.release(&self.budget_label);
    }
}
