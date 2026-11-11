//! The coordinator's path after an attempt ended: collecting its outputs.
//!
//! [`Coordinator::collect`] is a pass of its own, beside [`Coordinator::tick`]:
//! fetching a large artifact must not keep the tick from renewing leases.
//! One pass runs at a time; the workers are worked on side by side, each
//! within the budget of the collector.

use super::link::Budgeted;
use super::{join_all, Coordinator, CoordinatorError, Result, WorkerLink};
use crate::ids::WorkerId;
use crate::results::{CollectConfig, CollectError, CollectReport, Collector, ResultStore};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

type Collected = std::result::Result<CollectReport, CollectError>;

impl Coordinator {
    /// One pass of result collection: continue the collection of every
    /// ended attempt, on every worker, from what is durable. A worker that
    /// is not attached or does not answer is not an error; its attempts
    /// stay `collecting` and nothing is run again.
    pub async fn collect(
        &self,
        files: &Arc<ResultStore>,
        cfg: CollectConfig,
    ) -> Result<CollectReport> {
        let _one = self.collecting.lock().await;
        self.check_fenced()?;
        let collector = Collector::new(Arc::clone(&self.store), Arc::clone(files), cfg);
        let attached: Vec<(WorkerId, Arc<dyn WorkerLink>)> = {
            let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            workers
                .iter()
                .map(|(w, l)| (w.clone(), Arc::clone(l)))
                .collect()
        };
        let scan = cfg.scan;
        let open = self.db(move |s| s.collectable(None, scan)).await?;
        let mut others: BTreeSet<WorkerId> = open.into_iter().map(|c| c.record.worker_id).collect();
        let deadline = Instant::now() + Duration::from_millis(cfg.budget_ms);
        let mut passes: Vec<Pin<Box<dyn Future<Output = Collected> + Send + '_>>> = Vec::new();
        let collector = &collector;
        for (worker, link) in attached {
            others.remove(&worker);
            passes.push(Box::pin(async move {
                let link = Budgeted {
                    inner: link,
                    deadline,
                };
                collector
                    .collect_worker(&worker, Some(&link), deadline)
                    .await
            }));
        }
        // What no worker is needed for is closed also without a link.
        for worker in others {
            passes.push(Box::pin(async move {
                collector.collect_worker(&worker, None, deadline).await
            }));
        }
        let mut report = CollectReport::default();
        let mut failed = None;
        for pass in join_all(passes).await {
            match pass {
                Ok(part) => report.merge(part),
                Err(e) => failed = failed.or(Some(e)),
            }
        }
        match failed {
            None => Ok(report),
            Some(CollectError::Fenced) => Err(self.fence()),
            Some(CollectError::Storage(e)) => Err(e.into()),
            Some(CollectError::Aborted) => Err(CoordinatorError::Aborted),
            Some(CollectError::Files(e)) => Err(CoordinatorError::Results(e.to_string())),
        }
    }
}
