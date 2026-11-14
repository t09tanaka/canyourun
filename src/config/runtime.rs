//! Construction shared by resident parent and worker entry points.
//! Inputs are explicit: these factories never mutate the process environment.
use super::resolve::{ResolvedConfig, RuntimeSettings};
use crate::coordinator::{ClientLink, Coordinator, CoordinatorError};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{WorkerBootId, WorkerId};
use crate::protocol::session::Connected;
use crate::scheduler::{CostModel, Scheduler};
use crate::setup::builder::SetupConfig;
use crate::storage::CoordinatorStore;
use crate::worker::agent::{Agent, AgentConfig};
use crate::worker::machine::Machine;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

impl RuntimeSettings {
    pub fn start_coordinator(
        &self,
        store: CoordinatorStore,
        costs: Arc<dyn CostModel>,
    ) -> Result<Coordinator, CoordinatorError> {
        let scheduler = Scheduler::new(self.scheduler, costs).map_err(CoordinatorError::Invalid)?;
        Coordinator::start_with(store, self.coordinator, scheduler)
    }
}

/// A local identity is explicit; remote names must match their verified hello.
pub enum WorkerTarget<'a> {
    Local(&'a WorkerId),
    Remote(&'a str),
}

impl ResolvedConfig {
    /// Only a completed protocol handshake can enter this construction path.
    /// Identity mismatches leave both the scheduler and attached links untouched.
    pub fn attach_worker(
        &self,
        coordinator: &Coordinator,
        target: WorkerTarget<'_>,
        connected: Connected,
    ) -> Result<(), ApiError> {
        let invalid = |message| ApiError::new(ErrorCode::InvalidInput, message);
        let settings = self
            .runtime_settings()
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        if &connected.worker.cluster_id != coordinator.store().cluster_id() {
            return Err(invalid("worker hello cluster does not match coordinator"));
        }
        let id = &connected.worker.worker_id;
        let priority = match target {
            WorkerTarget::Local(expected) => {
                if !self.local_worker.value || expected != id {
                    return Err(invalid(
                        "local worker identity does not match configuration",
                    ));
                }
                self.local_worker_priority.value
            }
            WorkerTarget::Remote(name) => {
                let worker = self
                    .workers
                    .iter()
                    .find(|w| w.id == name)
                    .ok_or_else(|| invalid("worker is not configured"))?;
                if worker.id != id.as_str() {
                    return Err(invalid(
                        "worker hello identity does not match configured name",
                    ));
                }
                worker.priority.value
            }
        };
        coordinator.scheduler().set_priority(id, priority);
        coordinator.attach(
            id.clone(),
            Arc::new(ClientLink::new(
                connected.client,
                Duration::from_millis(settings.coordinator.worker_budget_ms),
            )),
        );
        Ok(())
    }
}

/// Machine identity, paths and command environment supplied by the caller.
#[derive(Debug, Clone)]
pub struct WorkerInputs {
    pub state_dir: PathBuf,
    pub worker_id: WorkerId,
    pub boot_id: WorkerBootId,
    pub supervisor_exe: PathBuf,
    pub cluster_key_path: PathBuf,
    pub env: BTreeMap<String, String>,
}

/// Settings for both consumers and setup builders, derived together.
pub struct WorkerRuntime {
    agent: AgentConfig,
    setup: SetupConfig,
    env: BTreeMap<String, String>,
    cluster_key_path: PathBuf,
}

impl ResolvedConfig {
    pub fn worker_runtime(
        &self,
        inputs: WorkerInputs,
    ) -> Result<WorkerRuntime, super::ConfigError> {
        let settings = self.runtime_settings()?;
        let mut agent = AgentConfig::new(
            inputs.state_dir.clone(),
            inputs.worker_id.clone(),
            inputs.boot_id,
            inputs.supervisor_exe.clone(),
        );
        agent.max_active_task = settings.coordinator.limits.max_active_task;
        agent.prefetch_per_worker = settings.coordinator.limits.prefetch_per_worker;
        agent.forecast_ttl_ms = settings.scheduler.rules.forecast_ttl_ms;
        agent.term_grace_ms = settings.term_grace_ms;
        agent.log_limit_bytes = settings.log_limit_bytes;
        agent.retention_ms = settings.retention_ms;
        agent.profile = self
            .profile
            .value
            .clone()
            .unwrap_or_else(|| "default".into());
        let mut setup = SetupConfig::new(inputs.state_dir, inputs.worker_id, inputs.supervisor_exe);
        setup.env = inputs.env.clone();
        setup.profile = agent.profile.clone();
        setup.traffic_state_dir = agent.traffic_state_dir.clone();
        setup.traffic_config = agent.traffic_config.clone();
        setup.lease_ms = settings.coordinator.lease_ms;
        setup.term_grace_ms = agent.term_grace_ms;
        setup.log_limit_bytes = agent.log_limit_bytes;
        setup.kill_wait_ms = agent.kill_wait_ms;
        setup.poll_ms = agent.poll_ms;
        setup.inhibit_sleep = agent.inhibit_sleep;
        Ok(WorkerRuntime {
            agent,
            setup,
            env: inputs.env,
            cluster_key_path: inputs.cluster_key_path,
        })
    }
}

impl WorkerRuntime {
    pub fn agent_config(&self) -> &AgentConfig {
        &self.agent
    }
    pub fn setup_config(&self) -> &SetupConfig {
        &self.setup
    }
    /// Preserve Machine's Git preflight and typed BLOCKED errors.
    pub fn open_host(&self) -> Result<Arc<Machine>, ApiError> {
        Machine::open(
            &self.agent.state_dir,
            self.env.clone(),
            self.cluster_key_path.clone(),
        )
        .and_then(|machine| machine.with_setup(self.setup.clone()))
        .map(Arc::new)
    }

    pub fn start(self) -> Result<Arc<Agent>, ApiError> {
        let host = self.open_host()?;
        Agent::start(self.agent, host)
    }
}
