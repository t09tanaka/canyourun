//! Merging the layers into one resolved configuration.
//!
//! Precedence per value: explicit CLI argument, project config, the legacy
//! `.local-ci.toml` fallback (same level as the project file, consulted after
//! it), user config, built-in default. Every value keeps its origin so a
//! command can show where it came from. Contradictions are collected and
//! returned together as one [`ConfigError::Invalid`].

use super::error::ConfigError;
use super::legacy::{secret_label, secret_spec, Fallback, MAPPING};
use super::model::{
    OutputWhen, ProjectConfig, SecretSpec, ServiceMode, SourceMode, Strategy, ToolchainMatch,
    Unsupported, UserConfig,
};
use std::collections::BTreeSet;
use std::fmt;

// ---- defaults (spec chapters 5, 9, 12, 13, 22.1) ---------------------------

pub const DEFAULT_HEARTBEAT_SECONDS: u64 = 5;
pub const DEFAULT_STALE_SECONDS: u64 = 15;
pub const DEFAULT_LEASE_SECONDS: u64 = 60;
pub const DEFAULT_TERM_GRACE_SECONDS: u64 = 10;
pub const DEFAULT_FORECAST_TTL_MS: u64 = 2000;
pub const DEFAULT_PREFETCH_PER_WORKER: u32 = 1;
pub const DEFAULT_MAX_ACTIVE_TASK: u32 = 2;
pub const DEFAULT_LOG_SPOOL_LIMIT_BYTES: u64 = 1 << 30;
pub const DEFAULT_RETENTION_DAYS: u32 = 7;
pub const DEFAULT_PRIORITY: i64 = 0;
pub const MIN_PRIORITY: i64 = -100;
pub const MAX_PRIORITY: i64 = 100;
pub const DEFAULT_PRIORITY_STEP_SECONDS: u64 = 60;
pub const MAX_PRIORITY_STEP_SECONDS: u64 = 3600;
pub const DEFAULT_WORKFLOWS_DIR: &str = ".github/workflows";
pub const DEFAULT_STATUS_CONTEXT: &str = "local-ci";

// ---- origins ---------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    Cli,
    Project(String),
    LegacyLocalCi(String),
    User(String),
    Default,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Origin::Cli => write!(f, "command-line argument"),
            Origin::Project(p) => write!(f, "project config {p}"),
            Origin::LegacyLocalCi(p) => write!(f, "legacy {p}"),
            Origin::User(p) => write!(f, "user config {p}"),
            Origin::Default => write!(f, "built-in default"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value<T> {
    pub value: T,
    pub origin: Origin,
}

/// Explicit CLI arguments. The clap connection fills this later.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overlay {
    pub heartbeat_seconds: Option<u64>,
    pub lease_seconds: Option<u64>,
    pub timeout_seconds: Option<u64>,
    pub toolchain_match: Option<ToolchainMatch>,
    pub source_mode: Option<SourceMode>,
    pub profile: Option<String>,
    pub requires: Option<Vec<String>>,
}

/// A parsed file and where it was read from.
#[derive(Debug, Clone, PartialEq)]
pub struct Loaded<T> {
    pub path: String,
    pub value: T,
}

#[derive(Debug, Clone, Copy)]
pub struct Inputs<'a> {
    pub overlay: &'a Overlay,
    pub project: Option<&'a Loaded<ProjectConfig>>,
    pub legacy: Option<&'a Fallback>,
    pub user: Option<&'a Loaded<UserConfig>>,
}

// ---- result ----------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSpec {
    pub id: String,
    pub ssh: String,
    /// `None` for an ephemeral worker (it installs no service).
    pub service_mode: Option<ServiceMode>,
    pub labels: Vec<String>,
    pub ephemeral: bool,
    /// `-100..=100`, bigger is preferred. Validated by `resolve`.
    pub priority: Value<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSpec {
    pub name: String,
    pub paths: Vec<String>,
    pub required: bool,
    pub when: OutputWhen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConfig {
    pub local_worker: Value<bool>,
    pub local_worker_priority: Value<i64>,
    pub heartbeat_seconds: Value<u64>,
    pub stale_seconds: Value<u64>,
    pub lease_seconds: Value<u64>,
    pub term_grace_seconds: Value<u64>,
    pub max_active_task: Value<u32>,
    pub log_spool_limit_bytes: Value<u64>,
    pub retention_days: Value<u32>,
    pub strategy: Value<Strategy>,
    pub forecast_ttl_ms: Value<u64>,
    pub prefetch_per_worker: Value<u32>,
    pub priority_step_seconds: Value<u64>,
    pub workers: Vec<WorkerSpec>,
    pub source_mode: Value<SourceMode>,
    pub include: Value<Vec<String>>,
    pub exclude: Value<Vec<String>>,
    pub secret_files: Value<Vec<SecretSpec>>,
    pub profile: Value<Option<String>>,
    pub requires: Value<Vec<String>>,
    pub pass_env: Value<Vec<String>>,
    pub timeout_seconds: Value<Option<u64>>,
    pub toolchain_match: Value<ToolchainMatch>,
    pub workflows_dirs: Value<Vec<String>>,
    /// `[ci] no_cache`: workflows (`ci.yml`) or jobs (`ci.yml:e2e`) whose
    /// results are never reused. Loaded here; acted on by the template layer.
    pub no_cache: Value<Vec<String>>,
    pub status_context: Value<String>,
    pub unsupported: Value<Unsupported>,
    pub outputs: Vec<OutputSpec>,
    /// `[prepare] commands`; empty when the project has none.
    pub prepare_commands: Vec<Vec<String>>,
    /// Settings that were present but are not applied. `ci plan` and `doctor`
    /// show them.
    pub notices: Vec<Notice>,
}

/// A setting that was read and deliberately not applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub key: &'static str,
    pub file: String,
    pub reason: String,
}

/// One line of a "show the effective config" listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: &'static str,
    pub value: String,
    pub origin: Origin,
}

impl ResolvedConfig {
    /// Every resolved value with its origin, in a stable order.
    pub fn entries(&self) -> Vec<Entry> {
        fn e<T>(key: &'static str, v: &Value<T>, show: impl Fn(&T) -> String) -> Entry {
            Entry {
                key,
                value: show(&v.value),
                origin: v.origin.clone(),
            }
        }
        fn list(v: &[String]) -> String {
            format!("[{}]", v.join(", "))
        }
        vec![
            e("coordinator.local_worker", &self.local_worker, |v| {
                v.to_string()
            }),
            e(
                "coordinator.local_worker_priority",
                &self.local_worker_priority,
                |v| v.to_string(),
            ),
            e(
                "coordinator.heartbeat_seconds",
                &self.heartbeat_seconds,
                |v| v.to_string(),
            ),
            e("coordinator.stale_seconds", &self.stale_seconds, |v| {
                v.to_string()
            }),
            e("coordinator.lease_seconds", &self.lease_seconds, |v| {
                v.to_string()
            }),
            e(
                "coordinator.term_grace_seconds",
                &self.term_grace_seconds,
                |v| v.to_string(),
            ),
            e("coordinator.max_active_task", &self.max_active_task, |v| {
                v.to_string()
            }),
            e(
                "coordinator.log_spool_limit_bytes",
                &self.log_spool_limit_bytes,
                |v| v.to_string(),
            ),
            e("coordinator.retention_days", &self.retention_days, |v| {
                v.to_string()
            }),
            e("scheduler.strategy", &self.strategy, |v| {
                strategy_str(*v).to_string()
            }),
            e("scheduler.forecast_ttl_ms", &self.forecast_ttl_ms, |v| {
                v.to_string()
            }),
            e(
                "scheduler.prefetch_per_worker",
                &self.prefetch_per_worker,
                |v| v.to_string(),
            ),
            e(
                "scheduler.priority_step_seconds",
                &self.priority_step_seconds,
                |v| v.to_string(),
            ),
            e("source.mode", &self.source_mode, |v| {
                source_mode_str(*v).to_string()
            }),
            e("source.include", &self.include, |v| list(v)),
            e("source.exclude", &self.exclude, |v| list(v)),
            e("source.secret_files", &self.secret_files, |v| {
                list(&v.iter().map(secret_label).collect::<Vec<_>>())
            }),
            e("execution.profile", &self.profile, |v| {
                v.clone().unwrap_or_else(|| "(none)".to_string())
            }),
            e("execution.requires", &self.requires, |v| list(v)),
            e("execution.pass_env", &self.pass_env, |v| list(v)),
            e("execution.timeout_seconds", &self.timeout_seconds, |v| {
                v.map_or_else(|| "(none)".to_string(), |s| s.to_string())
            }),
            e("toolchain.match", &self.toolchain_match, |v| {
                toolchain_str(*v).to_string()
            }),
            e("ci.workflows_dirs", &self.workflows_dirs, |v| list(v)),
            e("ci.no_cache", &self.no_cache, |v| list(v)),
            e("ci.status_context", &self.status_context, |v| v.clone()),
            e("ci.unsupported", &self.unsupported, |v| {
                unsupported_str(*v).to_string()
            }),
        ]
    }
}

pub fn strategy_str(s: Strategy) -> &'static str {
    match s {
        Strategy::EarliestFinish => "earliest_finish",
    }
}

pub fn source_mode_str(s: SourceMode) -> &'static str {
    match s {
        SourceMode::Worktree => "worktree",
        SourceMode::Commit => "commit",
    }
}

pub fn toolchain_str(m: ToolchainMatch) -> &'static str {
    match m {
        ToolchainMatch::Exact => "exact",
        ToolchainMatch::Minor => "minor",
        ToolchainMatch::Major => "major",
        ToolchainMatch::Any => "any",
    }
}

pub fn unsupported_str(u: Unsupported) -> &'static str {
    match u {
        Unsupported::Error => "error",
        Unsupported::Warn => "warn",
    }
}

// ---- merging ---------------------------------------------------------------

/// The origins to attach when a layer supplies the value. A layer without a
/// file never supplies one, so its placeholder is never shown.
struct Layers {
    project: Origin,
    legacy: Origin,
    user: Origin,
}

fn origin_of(l: &Layers, has: [bool; 4]) -> Origin {
    if has[0] {
        Origin::Cli
    } else if has[1] {
        l.project.clone()
    } else if has[2] {
        l.legacy.clone()
    } else if has[3] {
        l.user.clone()
    } else {
        Origin::Default
    }
}

/// The first layer that supplies a value wins; the origin says which one.
/// Straight-line on purpose: every instantiation runs the same code.
fn pick<T>(
    l: &Layers,
    cli: Option<T>,
    project: Option<T>,
    legacy: Option<T>,
    user: Option<T>,
    default: T,
) -> Value<T> {
    let origin = origin_of(
        l,
        [
            cli.is_some(),
            project.is_some(),
            legacy.is_some(),
            user.is_some(),
        ],
    );
    let value = cli.or(project).or(legacy).or(user).unwrap_or(default);
    Value { value, origin }
}

fn dirs_problem(d: &str) -> Option<String> {
    let n = local_ci::plan::norm_dir(d);
    if n.is_empty() || std::path::Path::new(&n).is_absolute() || n.split('/').any(|c| c == "..") {
        Some(format!(
            "workflows_dirs {d:?}: give a directory below the repository root"
        ))
    } else {
        None
    }
}

fn normalize_dirs(dirs: &[String], problems: &mut Vec<String>, origin: &str) -> Vec<String> {
    if dirs.is_empty() {
        problems.push(format!(
            "{origin}: workflows_dirs is empty; list a directory or leave the key out"
        ));
    }
    let mut out: Vec<String> = Vec::new();
    for d in dirs {
        if let Some(p) = dirs_problem(d) {
            problems.push(format!("{origin}: {p}"));
            continue;
        }
        let n = local_ci::plan::norm_dir(d);
        if !out.contains(&n) {
            out.push(n);
        }
    }
    out
}

/// A conflict between the project file and the legacy file on one mapped key.
fn conflict<T>(
    problems: &mut Vec<String>,
    mapping_index: usize,
    project: &Option<Vec<T>>,
    legacy: &Option<Vec<T>>,
    paths: (&str, &str),
    show: impl Fn(&Vec<T>) -> String,
) where
    T: Ord,
{
    if let (Some(p), Some(l)) = (project, legacy) {
        if !same_set(p, l) {
            let m = MAPPING[mapping_index];
            problems.push(format!(
                "{} {} = {} conflicts with {} {} = {}; make them agree or remove one",
                paths.0,
                m.rtr,
                show(p),
                paths.1,
                m.local_ci,
                show(l),
            ));
        }
    }
}

/// Lists compare as sets: order and repeats are not a contradiction.
fn same_set<T: Ord>(a: &[T], b: &[T]) -> bool {
    a.iter().collect::<BTreeSet<_>>() == b.iter().collect::<BTreeSet<_>>()
}

fn strs(v: &[String]) -> String {
    format!("{v:?}")
}

fn secrets_show(v: &[SecretSpec]) -> String {
    strs(&v.iter().map(secret_label).collect::<Vec<_>>())
}

fn describe<T: fmt::Display>(name: &str, v: &Value<T>) -> String {
    format!("{name} = {} ({})", v.value, v.origin)
}

fn check_nonzero(problems: &mut Vec<String>, name: &str, v: u64, origin: &Origin) {
    if v == 0 {
        problems.push(format!("{name} = 0 ({origin}) must be greater than 0"));
    }
}

/// Cross-checks of the liveness settings (spec chapter 12).
fn check_timing(r: &ResolvedConfig, problems: &mut Vec<String>) {
    let hb = &r.heartbeat_seconds;
    let stale = &r.stale_seconds;
    let lease = &r.lease_seconds;
    check_nonzero(problems, "heartbeat_seconds", hb.value, &hb.origin);
    check_nonzero(problems, "stale_seconds", stale.value, &stale.origin);
    check_nonzero(problems, "lease_seconds", lease.value, &lease.origin);
    check_nonzero(
        problems,
        "term_grace_seconds",
        r.term_grace_seconds.value,
        &r.term_grace_seconds.origin,
    );
    check_nonzero(
        problems,
        "forecast_ttl_ms",
        r.forecast_ttl_ms.value,
        &r.forecast_ttl_ms.origin,
    );
    check_nonzero(
        problems,
        "log_spool_limit_bytes",
        r.log_spool_limit_bytes.value,
        &r.log_spool_limit_bytes.origin,
    );
    check_nonzero(
        problems,
        "max_active_task",
        u64::from(r.max_active_task.value),
        &r.max_active_task.origin,
    );
    check_nonzero(
        problems,
        "retention_days",
        u64::from(r.retention_days.value),
        &r.retention_days.origin,
    );
    check_priority(
        problems,
        "local_worker_priority",
        r.local_worker_priority.value,
        &r.local_worker_priority.origin,
    );
    let step = &r.priority_step_seconds;
    if step.value > MAX_PRIORITY_STEP_SECONDS {
        problems.push(format!(
            "{} must be at most {MAX_PRIORITY_STEP_SECONDS}",
            describe("priority_step_seconds", step)
        ));
    }
    if let Some(t) = r.timeout_seconds.value {
        check_nonzero(problems, "timeout_seconds", t, &r.timeout_seconds.origin);
    }
    if hb.value >= lease.value {
        problems.push(format!(
            "{} must be less than {}",
            describe("heartbeat_seconds", hb),
            describe("lease_seconds", lease)
        ));
    }
    if hb.value.saturating_mul(2) > stale.value {
        problems.push(format!(
            "{} must be at least twice {}",
            describe("stale_seconds", stale),
            describe("heartbeat_seconds", hb)
        ));
    }
    if stale.value >= lease.value {
        problems.push(format!(
            "{} must be less than {}",
            describe("stale_seconds", stale),
            describe("lease_seconds", lease)
        ));
    }
}

fn check_priority(problems: &mut Vec<String>, name: &str, v: i64, origin: &Origin) {
    if !(MIN_PRIORITY..=MAX_PRIORITY).contains(&v) {
        problems.push(format!(
            "{name} = {v} ({origin}) must be between {MIN_PRIORITY} and {MAX_PRIORITY}"
        ));
    }
}

fn check_workers(
    user: &UserConfig,
    layers: &Layers,
    problems: &mut Vec<String>,
) -> Vec<WorkerSpec> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for w in &user.workers {
        let id_ok = !w.id.is_empty()
            && w.id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
        if !id_ok {
            problems.push(format!(
                "worker id {:?}: use letters, digits, '-', '_' and '.'",
                w.id
            ));
        }
        if !seen.insert(w.id.clone()) {
            problems.push(format!("worker id {:?} is listed twice", w.id));
        }
        if w.ssh.trim().is_empty() {
            problems.push(format!("worker {:?}: ssh is empty", w.id));
        }
        if w.labels.iter().any(|l| l.trim().is_empty()) {
            problems.push(format!("worker {:?}: a label is empty", w.id));
        }
        let ephemeral = w.ephemeral.unwrap_or(false);
        if ephemeral && w.service_mode.is_some() {
            problems.push(format!(
                "worker {:?}: an ephemeral worker installs no service; remove service_mode",
                w.id
            ));
        }
        let priority = pick(layers, None, None, None, w.priority, DEFAULT_PRIORITY);
        check_priority(
            problems,
            &format!("worker {:?}: priority", w.id),
            priority.value,
            &priority.origin,
        );
        out.push(WorkerSpec {
            id: w.id.clone(),
            ssh: w.ssh.clone(),
            service_mode: if ephemeral {
                None
            } else {
                Some(w.service_mode.unwrap_or(ServiceMode::User))
            },
            labels: w.labels.clone(),
            ephemeral,
            priority,
        });
    }
    out
}

fn check_outputs(project: &ProjectConfig, problems: &mut Vec<String>) -> Vec<OutputSpec> {
    let mut names = BTreeSet::new();
    let mut out = Vec::new();
    for o in &project.outputs {
        if o.name.trim().is_empty() {
            problems.push("[[outputs]]: name is empty".to_string());
        }
        if !names.insert(o.name.clone()) {
            problems.push(format!("[[outputs]] name {:?} is listed twice", o.name));
        }
        if o.paths.is_empty() || o.paths.iter().any(|p| p.trim().is_empty()) {
            problems.push(format!(
                "[[outputs]] {:?}: paths needs at least one non-empty glob",
                o.name
            ));
        }
        out.push(OutputSpec {
            name: o.name.clone(),
            paths: o.paths.clone(),
            required: o.required.unwrap_or(false),
            when: o.when.unwrap_or(OutputWhen::Always),
        });
    }
    out
}

fn check_prepare(project: &ProjectConfig, problems: &mut Vec<String>) -> Vec<Vec<String>> {
    let cmds = project
        .prepare
        .as_ref()
        .map(|p| p.commands.clone())
        .unwrap_or_default();
    for c in &cmds {
        if c.first().is_none_or(|a| a.trim().is_empty()) {
            problems.push(format!("[prepare] commands: {c:?} has no program"));
        }
    }
    cmds
}

const NOTICE_REASON: &str =
    "not supported by remote-task-runner; dependencies are prepared by fast-worktree's [[clone]]";

/// Unsupported `.local-ci.toml` keys that actually carry a value.
fn legacy_notices(legacy: &Fallback) -> Vec<Notice> {
    let mut out = Vec::new();
    if legacy.link_node_modules {
        out.push(Notice {
            key: "link-node-modules",
            file: legacy.path.clone(),
            reason: NOTICE_REASON.to_string(),
        });
    }
    if !legacy.ignore_env.is_empty() {
        out.push(Notice {
            key: "[setup] ignore-env",
            file: legacy.path.clone(),
            reason: NOTICE_REASON.to_string(),
        });
    }
    out
}

/// Merge the layers. Returns every problem found, not only the first.
pub fn resolve(inp: &Inputs<'_>) -> Result<ResolvedConfig, ConfigError> {
    let mut problems: Vec<String> = Vec::new();
    let o = inp.overlay;
    let empty_user = UserConfig::default();
    let empty_project = ProjectConfig::default();
    let user = inp.user.map_or(&empty_user, |l| &l.value);
    let project = inp.project.map_or(&empty_project, |l| &l.value);
    let project_path = inp.project.map_or(String::new(), |l| l.path.clone());
    let legacy_path = inp.legacy.map_or(String::new(), |l| l.path.clone());
    let layers = Layers {
        project: Origin::Project(inp.project.map_or(String::new(), |l| l.path.clone())),
        legacy: Origin::LegacyLocalCi(inp.legacy.map_or(String::new(), |l| l.path.clone())),
        user: Origin::User(inp.user.map_or(String::new(), |l| l.path.clone())),
    };

    let coord = user.coordinator.clone().unwrap_or_default();
    let sched = user.scheduler.clone().unwrap_or_default();
    let defaults = user.defaults.clone().unwrap_or_default();
    let src = project.source.clone().unwrap_or_default();
    let exec = project.execution.clone().unwrap_or_default();
    let ci = project.ci.clone().unwrap_or_default();
    let legacy = inp.legacy.cloned().unwrap_or_default();

    // workflows_dirs: normalise the project value like local-ci does, then
    // compare it with the legacy one.
    let project_dirs = ci
        .workflows_dirs
        .as_ref()
        .map(|d| normalize_dirs(d, &mut problems, &project_path));
    let project_secrets: Option<Vec<SecretSpec>> = src
        .secret_files
        .as_ref()
        .map(|v| v.iter().map(secret_spec).collect());
    if let Some(s) = &project_secrets {
        if s.iter().any(|x| x.glob.trim().is_empty()) {
            problems.push("[source] secret_files: a glob is empty".to_string());
        }
    }
    // Both Some is the only case that can conflict; an absent file has no values.
    let paths = (project_path.as_str(), legacy_path.as_str());
    conflict(
        &mut problems,
        0,
        &project_dirs,
        &legacy.workflows_dirs,
        paths,
        |v| strs(v),
    );
    conflict(
        &mut problems,
        1,
        &src.include,
        &legacy.include,
        paths,
        |v| strs(v),
    );
    conflict(
        &mut problems,
        2,
        &src.exclude,
        &legacy.exclude,
        paths,
        |v| strs(v),
    );
    conflict(
        &mut problems,
        3,
        &project_secrets,
        &legacy.secret_files,
        paths,
        |v| secrets_show(v),
    );

    conflict(
        &mut problems,
        4,
        &ci.no_cache,
        &legacy.no_cache,
        paths,
        |v| strs(v),
    );
    if let Some(n) = &ci.no_cache {
        if n.iter().any(|e| e.trim().is_empty()) {
            problems.push(format!(
                "{project_path}: [ci] no_cache: give a workflow file (`ci.yml`) or a job (`ci.yml:e2e`)"
            ));
        }
    }

    let notices = legacy_notices(&legacy);
    let some = |v: Option<String>| v.map(Some);
    let r = ResolvedConfig {
        local_worker: pick(&layers, None, None, None, coord.local_worker, true),
        local_worker_priority: pick(
            &layers,
            None,
            None,
            None,
            coord.local_worker_priority,
            DEFAULT_PRIORITY,
        ),
        heartbeat_seconds: pick(
            &layers,
            o.heartbeat_seconds,
            None,
            None,
            coord.heartbeat_seconds,
            DEFAULT_HEARTBEAT_SECONDS,
        ),
        stale_seconds: pick(
            &layers,
            None,
            None,
            None,
            coord.stale_seconds,
            DEFAULT_STALE_SECONDS,
        ),
        lease_seconds: pick(
            &layers,
            o.lease_seconds,
            None,
            None,
            coord.lease_seconds,
            DEFAULT_LEASE_SECONDS,
        ),
        term_grace_seconds: pick(
            &layers,
            None,
            None,
            None,
            coord.term_grace_seconds,
            DEFAULT_TERM_GRACE_SECONDS,
        ),
        max_active_task: pick(
            &layers,
            None,
            None,
            None,
            coord.max_active_task,
            DEFAULT_MAX_ACTIVE_TASK,
        ),
        log_spool_limit_bytes: pick(
            &layers,
            None,
            None,
            None,
            coord.log_spool_limit_bytes,
            DEFAULT_LOG_SPOOL_LIMIT_BYTES,
        ),
        retention_days: pick(
            &layers,
            None,
            None,
            None,
            coord.retention_days,
            DEFAULT_RETENTION_DAYS,
        ),
        strategy: pick(
            &layers,
            None,
            None,
            None,
            sched.strategy,
            Strategy::EarliestFinish,
        ),
        forecast_ttl_ms: pick(
            &layers,
            None,
            None,
            None,
            sched.forecast_ttl_ms,
            DEFAULT_FORECAST_TTL_MS,
        ),
        prefetch_per_worker: pick(
            &layers,
            None,
            None,
            None,
            sched.prefetch_per_worker,
            DEFAULT_PREFETCH_PER_WORKER,
        ),
        priority_step_seconds: pick(
            &layers,
            None,
            None,
            None,
            sched.priority_step_seconds,
            DEFAULT_PRIORITY_STEP_SECONDS,
        ),
        workers: check_workers(user, &layers, &mut problems),
        source_mode: pick(
            &layers,
            o.source_mode,
            src.mode,
            None,
            None,
            SourceMode::Worktree,
        ),
        include: pick(&layers, None, src.include, legacy.include, None, Vec::new()),
        exclude: pick(&layers, None, src.exclude, legacy.exclude, None, Vec::new()),
        secret_files: pick(
            &layers,
            None,
            project_secrets,
            legacy.secret_files,
            None,
            Vec::new(),
        ),
        profile: pick(
            &layers,
            some(o.profile.clone()),
            some(exec.profile.clone()),
            None,
            None,
            None,
        ),
        requires: pick(
            &layers,
            o.requires.clone(),
            exec.requires,
            None,
            None,
            Vec::new(),
        ),
        pass_env: pick(&layers, None, exec.pass_env, None, None, Vec::new()),
        timeout_seconds: pick(
            &layers,
            o.timeout_seconds.map(Some),
            exec.timeout_seconds.map(Some),
            None,
            defaults.timeout_seconds.map(Some),
            None,
        ),
        toolchain_match: pick(
            &layers,
            o.toolchain_match,
            project.toolchain.as_ref().and_then(|t| t.match_),
            None,
            defaults.toolchain_match,
            ToolchainMatch::Exact,
        ),
        workflows_dirs: pick(
            &layers,
            None,
            project_dirs,
            legacy.workflows_dirs,
            None,
            vec![DEFAULT_WORKFLOWS_DIR.to_string()],
        ),
        no_cache: pick(
            &layers,
            None,
            ci.no_cache.clone(),
            legacy.no_cache.clone(),
            None,
            Vec::new(),
        ),
        status_context: pick(
            &layers,
            None,
            ci.status_context.clone(),
            None,
            None,
            DEFAULT_STATUS_CONTEXT.to_string(),
        ),
        unsupported: pick(
            &layers,
            None,
            ci.unsupported,
            None,
            None,
            Unsupported::Error,
        ),
        outputs: check_outputs(project, &mut problems),
        prepare_commands: check_prepare(project, &mut problems),
        notices,
    };

    check_timing(&r, &mut problems);
    if let Some(s) = &ci.status_context {
        if s.trim().is_empty() {
            problems.push("[ci] status_context is empty".to_string());
        }
    }
    if problems.is_empty() {
        Ok(r)
    } else {
        Err(ConfigError::Invalid { problems })
    }
}
