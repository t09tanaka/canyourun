//! The decisions of the setup builder, free of I/O (spec chapter 8): what a
//! spec may hold, how its template key is made, whether a receipt is the one
//! asked for, who still holds a builder, how a builder's end reads to the
//! consumers that waited for it, and where the dependencies of a project
//! come from. Covered by the coverage and mutation gates.

use crate::ids::{BuilderId, Digest};
use crate::state::{Exit, Outcome, Termination};
use crate::wire::{
    canonical_json, CommandSpec, PreparedArtifact, PreparedTemplateReceipt, ReuseScope,
    SchemaVersion, SetupSpec, SetupStep,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// Bump when what a template holds, or how its key is made, changes.
pub const CACHE_SCHEMA: u32 = 1;
pub const MAX_STEPS: usize = 64;
pub const MAX_ID_BYTES: usize = 128;
pub const MAX_RUN_BYTES: usize = 65_536;
pub const MAX_ENV_ENTRIES: usize = 256;
pub const MAX_ENV_BYTES: usize = 65_536;
pub const MAX_LISTED: usize = 64;
pub const MAX_ARTIFACTS: usize = 256;
pub const MAX_PATH_BYTES: usize = 1024;

/// A relative path that stays inside the workspace: no root, no `..`, no
/// empty or NUL-holding part. The empty path is the workspace itself.
pub fn safe_relative(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }
    path.len() <= MAX_PATH_BYTES
        && !path.starts_with('/')
        && path
            .split('/')
            .all(|c| !c.is_empty() && c != "." && c != ".." && !c.contains('\0'))
}

/// The argv that runs `run` with `shell`; `None` for a shell this worker
/// does not run steps with (the step then stays with its consumer).
pub fn shell_argv(shell: Option<&str>, run: &str) -> Option<Vec<String>> {
    let head: &[&str] = match shell {
        None | Some("bash") => &["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c"],
        Some("sh") => &["sh", "-e", "-c"],
        Some(_) => return None,
    };
    let mut argv: Vec<String> = head.iter().map(|s| s.to_string()).collect();
    argv.push(run.to_string());
    Some(argv)
}

/// What a spec from the wire may hold. Everything here is checked before
/// anything is stored or run: the sender's counts, sizes and paths are not
/// trusted.
pub fn validate(spec: &SetupSpec) -> Result<(), String> {
    if spec.steps.len() > MAX_STEPS {
        return Err(format!("more than {MAX_STEPS} setup steps"));
    }
    if spec.toolchain.len() > MAX_LISTED || spec.secrets.len() > MAX_LISTED {
        return Err(format!("more than {MAX_LISTED} tools or secrets"));
    }
    if spec.step_timeout_ms == Some(0) || spec.freshness_secs == Some(0) {
        return Err("a timeout of zero is not a timeout".into());
    }
    let mut ids = BTreeSet::new();
    for s in &spec.steps {
        if s.id.is_empty() || s.id.len() > MAX_ID_BYTES {
            return Err("a step id is empty or too long".into());
        }
        if !ids.insert(s.id.as_str()) {
            return Err(format!("step id {} is there twice", s.id));
        }
        if !safe_relative(&s.dir) {
            return Err(format!("step {}: dir leaves the workspace", s.id));
        }
        if s.run.is_empty() || s.run.len() > MAX_RUN_BYTES {
            return Err(format!("step {}: the script is empty or too long", s.id));
        }
        if s.run.contains('\0') {
            return Err(format!("step {}: the script holds a NUL", s.id));
        }
        let bytes: usize = s.env.iter().map(|(k, v)| k.len() + v.len()).sum();
        if s.env.len() > MAX_ENV_ENTRIES || bytes > MAX_ENV_BYTES {
            return Err(format!("step {}: too much environment", s.id));
        }
        if s.env
            .iter()
            .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
        {
            return Err(format!("step {}: a bad environment entry", s.id));
        }
    }
    Ok(())
}

/// The steps a builder prepares: the leading ones whose environment is all
/// known and whose shell this worker runs. Any other step stays with its
/// consumer, and so does every step after it (it may depend on it).
pub fn hoisted(spec: &SetupSpec) -> &[SetupStep] {
    let n = spec
        .steps
        .iter()
        .position(|s| !s.unresolved_env.is_empty() || shell_argv(s.shell.as_deref(), "").is_none())
        .unwrap_or(spec.steps.len());
    &spec.steps[..n]
}

/// Where a template of this spec may be reused. `no_cache` ends the reuse
/// by other runs; the run's own singleflight stays.
pub fn effective_scope(spec: &SetupSpec) -> ReuseScope {
    if spec.no_cache {
        ReuseScope::Run
    } else {
        spec.scope
    }
}

/// `--no-cache` for a workflow or job: the flag, or a `[ci] no_cache` entry
/// naming the workflow (`ci.yml`) or the job (`ci.yml:e2e`).
pub fn no_cache_applies(flag: bool, listed: &[String], workflow: &str, job: &str) -> bool {
    flag || listed
        .iter()
        .any(|e| e == workflow || *e == format!("{workflow}:{job}"))
}

/// What the key holds besides the spec: facts of this worker and run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyContext {
    pub source_digest: String,
    pub os: String,
    pub arch: String,
    pub profile: String,
    pub run_id: String,
}

/// The identity of the template the spec asks for. It holds the steps the
/// builder prepares (text, shell, directory, effective environment), the
/// toolchain, this worker's OS, architecture and profile, the scope (with
/// the run id for a run scope), the freshness and the secret versions. The
/// same text under another shell or environment is another key.
pub fn template_key(spec: &SetupSpec, ctx: &KeyContext) -> String {
    let scope = effective_scope(spec);
    let mut toolchain: Vec<_> = spec
        .toolchain
        .iter()
        .map(|t| json!([t.name, t.version]))
        .collect();
    toolchain.sort_by_key(|v| v.to_string());
    let mut secrets: Vec<_> = spec
        .secrets
        .iter()
        .map(|s| json!([s.name, s.digest]))
        .collect();
    secrets.sort_by_key(|v| v.to_string());
    let inputs = json!({
        "cache_schema": CACHE_SCHEMA,
        "source": ctx.source_digest,
        "steps": hoisted(spec).iter().map(|s| json!({
            "id": s.id,
            "dir": s.dir,
            "run": s.run,
            "shell": s.shell.as_deref().unwrap_or("bash"),
            "env": s.env,
        })).collect::<Vec<_>>(),
        "toolchain": toolchain,
        "os": ctx.os,
        "arch": ctx.arch,
        "profile": ctx.profile,
        "scope": match scope { ReuseScope::Run => "run", ReuseScope::Persistent => "persistent" },
        "run": match scope { ReuseScope::Run => Value::from(ctx.run_id.clone()), ReuseScope::Persistent => Value::Null },
        "freshness_secs": spec.freshness_secs,
        "secrets": secrets,
    });
    let text = canonical_json(&inputs).expect("a key is made of strings and numbers");
    Digest::of_bytes(text.as_bytes()).to_string()
}

/// A template is not used when it is older than the freshness the spec asks.
pub fn fresh(created_ms: u64, now_ms: u64, freshness_secs: Option<u64>) -> bool {
    match freshness_secs {
        None => true,
        Some(s) => now_ms.saturating_sub(created_ms) <= s.saturating_mul(1000),
    }
}

/// `sha256:` of the receipt's canonical JSON with its own `digest` empty.
pub fn receipt_digest(r: &PreparedTemplateReceipt) -> String {
    let mut v = serde_json::to_value(r).expect("a receipt is plain data");
    v["digest"] = Value::from("");
    let text = canonical_json(&v).expect("a receipt is made of strings and numbers");
    Digest::of_bytes(text.as_bytes()).to_string()
}

/// The receipt for a template just built, with its digest set.
pub fn seal_receipt(
    key: &str,
    source: &str,
    builder: &BuilderId,
    spec: &SetupSpec,
    artifacts: Vec<PreparedArtifact>,
    created_at_ms: u64,
) -> PreparedTemplateReceipt {
    let mut r = PreparedTemplateReceipt {
        schema_version: SchemaVersion::CURRENT,
        template_key: key.to_string(),
        source_digest: source.to_string(),
        builder_id: builder.clone(),
        steps: hoisted(spec).iter().map(|s| s.id.clone()).collect(),
        artifacts,
        created_at_ms,
        digest: String::new(),
    };
    r.digest = receipt_digest(&r);
    r
}

/// Why a receipt is not the one a consumer asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    Schema,
    Digest,
    Key,
    Source,
    Steps,
    Artifacts(String),
    Stale,
}

impl std::fmt::Display for Reject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reject::Schema => f.write_str("the receipt has another schema version"),
            Reject::Digest => f.write_str("the receipt does not match its own digest"),
            Reject::Key => f.write_str("the receipt is for another template key"),
            Reject::Source => f.write_str("the receipt is for another source"),
            Reject::Steps => f.write_str("the receipt names other steps than were asked"),
            Reject::Artifacts(why) => write!(f, "the receipt's artifacts are not usable: {why}"),
            Reject::Stale => f.write_str("the template is older than the freshness asked"),
        }
    }
}

/// Whether `r` is the receipt of the template the spec asks for under `key`
/// and `source`. The trees it describes are checked by whoever reads them.
pub fn check_receipt(
    r: &PreparedTemplateReceipt,
    key: &str,
    source: &str,
    spec: &SetupSpec,
    now_ms: u64,
) -> Result<(), Reject> {
    if r.schema_version != SchemaVersion::CURRENT {
        return Err(Reject::Schema);
    }
    if r.digest != receipt_digest(r) {
        return Err(Reject::Digest);
    }
    if r.template_key != key {
        return Err(Reject::Key);
    }
    if r.source_digest != source {
        return Err(Reject::Source);
    }
    if !r.steps.iter().eq(hoisted(spec).iter().map(|s| &s.id)) {
        return Err(Reject::Steps);
    }
    if r.artifacts.len() > MAX_ARTIFACTS {
        return Err(Reject::Artifacts("too many".into()));
    }
    let mut seen = BTreeSet::new();
    for a in &r.artifacts {
        let bad = a.path.is_empty()
            || !safe_relative(&a.path)
            || a.path.split('/').any(|c| c == ".git")
            || !a.digest.starts_with(Digest::PREFIX);
        if bad {
            return Err(Reject::Artifacts(format!("`{}`", a.path)));
        }
        if !seen.insert(a.path.as_str()) {
            return Err(Reject::Artifacts(format!("`{}` is there twice", a.path)));
        }
    }
    if !fresh(r.created_at_ms, now_ms, spec.freshness_secs) {
        return Err(Reject::Stale);
    }
    Ok(())
}

/// The ids of the steps a verified receipt says were prepared.
pub fn skip_set(r: &PreparedTemplateReceipt) -> BTreeSet<String> {
    r.steps.iter().cloned().collect()
}

/// The commands that are left to run: those that are no prepared setup
/// step, with the dependencies on the dropped ones removed.
pub fn prune_commands(commands: &[CommandSpec], skip: &BTreeSet<String>) -> Vec<CommandSpec> {
    let skipped = |c: &CommandSpec| c.setup_step.as_ref().is_some_and(|s| skip.contains(s));
    let gone: BTreeSet<_> = commands
        .iter()
        .filter(|c| skipped(c))
        .map(|c| c.node_id.clone())
        .collect();
    commands
        .iter()
        .filter(|c| !skipped(c))
        .map(|c| {
            let mut c = c.clone();
            c.depends_on.retain(|d| !gone.contains(d));
            c
        })
        .collect()
}

/// The entries `after` has that `before` did not, in order, leaving out
/// everything of Git's.
pub fn added_entries(before: &BTreeSet<String>, after: &BTreeSet<String>) -> Vec<String> {
    after
        .difference(before)
        .filter(|p| !p.split('/').any(|c| c == ".git"))
        .cloned()
        .collect()
}

/// What to do with one artifact of a template in a consumer's workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    /// Nothing is there: copy it in.
    Copy,
    /// A tree with the digest the receipt has is there already (a copy a
    /// crash cut short of its end).
    AlreadyThere,
    /// Something else is there: it is not overwritten, the consumer runs
    /// the steps itself.
    Conflict,
}

pub fn place(dest_exists: bool, dest_digest: Option<&str>, want: &str) -> Place {
    match (dest_exists, dest_digest) {
        (false, _) => Place::Copy,
        (true, Some(d)) if d == want => Place::AlreadyThere,
        (true, _) => Place::Conflict,
    }
}

/// How a builder ended, as the consumers that waited for it are told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildEnd {
    Built,
    /// A step failed.
    Failed(Option<Exit>),
    TimedOut,
    /// Stopped because no consumer needed it any more, or asked to stop.
    Cancelled,
    /// It could not be seen to end properly: the supervisor died, the
    /// lease ran out, the result is missing.
    Crashed(String),
}

impl BuildEnd {
    pub fn is_built(&self) -> bool {
        *self == BuildEnd::Built
    }

    pub fn describe(&self) -> String {
        match self {
            BuildEnd::Built => "built".into(),
            BuildEnd::Failed(Some(Exit::Code(c))) => format!("a setup step exited with {c}"),
            BuildEnd::Failed(Some(Exit::Signal(s))) => {
                format!("a setup step was killed by signal {s}")
            }
            BuildEnd::Failed(None) => "a setup step failed".into(),
            BuildEnd::TimedOut => "the setup builder timed out".into(),
            BuildEnd::Cancelled => "the setup builder was cancelled".into(),
            BuildEnd::Crashed(why) => format!("the setup builder was lost: {why}"),
        }
    }
}

/// The end the supervisor settled the builder with; no settlement at all
/// is a crash.
pub fn end_of(t: Option<Termination>) -> BuildEnd {
    let Some(t) = t else {
        return BuildEnd::Crashed("it ended without a result".into());
    };
    match t.outcome() {
        Outcome::Succeeded => BuildEnd::Built,
        Outcome::Failed => BuildEnd::Failed(t.exit()),
        Outcome::TimedOut => BuildEnd::TimedOut,
        Outcome::Cancelled => BuildEnd::Cancelled,
        Outcome::Interrupted => BuildEnd::Crashed("it was interrupted".into()),
    }
}

/// What dropping a consumer's reference means for the builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leave {
    /// The consumer held no reference.
    Unknown,
    /// Others still need the builder (or it has ended).
    Keep,
    /// That was the last reference of a builder that is still working.
    StopBuilder,
}

/// A consumer takes a reference. Returns whether it is the first.
pub fn join(refs: &mut BTreeSet<String>, consumer: &str) -> bool {
    let first = refs.is_empty();
    refs.insert(consumer.to_string());
    first
}

pub fn leave(refs: &mut BTreeSet<String>, consumer: &str, building: bool) -> Leave {
    if !refs.remove(consumer) {
        Leave::Unknown
    } else if refs.is_empty() && building {
        Leave::StopBuilder
    } else {
        Leave::Keep
    }
}

/// The builder's lease is renewed only while a consumer holds a reference.
pub fn renew_lease(refs: usize, finished: bool) -> bool {
    refs > 0 && !finished
}

/// Where a project's dependencies are prepared from (spec chapters 6, 8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencySource {
    /// `.fast-worktree.toml`'s `[[clone]]` and `post_add`.
    FastWorktree,
    /// `[prepare] commands` of the project's configuration.
    Prepare,
    None,
}

/// `.fast-worktree.toml` wins over `[prepare]`.
pub fn dependency_source(
    has_fast_worktree_toml: bool,
    has_prepare_commands: bool,
) -> DependencySource {
    if has_fast_worktree_toml {
        DependencySource::FastWorktree
    } else if has_prepare_commands {
        DependencySource::Prepare
    } else {
        DependencySource::None
    }
}

/// An argument as the shell reads it back as the same word.
pub fn shell_quote(arg: &str) -> String {
    let plain = !arg.is_empty()
        && arg
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(&b));
    if plain {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', r"'\''"))
    }
}

/// The setup `[prepare] commands` stand for, or `None` when
/// `.fast-worktree.toml` is there (it wins) or there is no command. The
/// steps run in the workspace root with bash; the template is kept for
/// later runs, unless `no_cache`.
pub fn prepare_spec(
    commands: &[Vec<String>],
    has_fast_worktree_toml: bool,
    no_cache: bool,
) -> Option<SetupSpec> {
    if dependency_source(has_fast_worktree_toml, !commands.is_empty()) != DependencySource::Prepare
    {
        return None;
    }
    let steps = commands
        .iter()
        .enumerate()
        .map(|(i, argv)| {
            let run = argv
                .iter()
                .map(|a| shell_quote(a))
                .collect::<Vec<_>>()
                .join(" ");
            let id = Digest::of_bytes(format!("prepare\n{i}\n{run}").as_bytes()).to_string();
            SetupStep {
                id,
                dir: String::new(),
                run,
                shell: None,
                env: Default::default(),
                unresolved_env: Vec::new(),
            }
        })
        .collect();
    Some(SetupSpec {
        steps,
        toolchain: Vec::new(),
        scope: ReuseScope::Persistent,
        freshness_secs: None,
        secrets: Vec::new(),
        no_cache,
        step_timeout_ms: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{NodeId, RunId};
    use crate::wire::{SecretVersion, ToolRequirement};
    use std::collections::BTreeMap;

    fn step(id: &str) -> SetupStep {
        SetupStep {
            id: id.into(),
            dir: String::new(),
            run: format!("make {id}"),
            shell: None,
            env: BTreeMap::new(),
            unresolved_env: vec![],
        }
    }

    fn spec(ids: &[&str]) -> SetupSpec {
        SetupSpec {
            steps: ids.iter().map(|i| step(i)).collect(),
            toolchain: vec![],
            scope: ReuseScope::Persistent,
            freshness_secs: None,
            secrets: vec![],
            no_cache: false,
            step_timeout_ms: None,
        }
    }

    fn ctx() -> KeyContext {
        KeyContext {
            source_digest: "sha256:src".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            profile: "p1".into(),
            run_id: RunId::generate().to_string(),
        }
    }

    fn bad(s: &SetupSpec) -> String {
        validate(s).unwrap_err()
    }

    #[test]
    fn a_relative_path_stays_inside_the_workspace() {
        for ok in ["", "a", "a/b", "web/app", ".hidden/x", "a.b"] {
            assert!(safe_relative(ok), "{ok}");
        }
        for no in [
            "/a", "..", "a/..", "a/../b", "a//b", "a/", ".", "a/./b", "a\0b",
        ] {
            assert!(!safe_relative(no), "{no:?}");
        }
        assert!(safe_relative(&"a".repeat(MAX_PATH_BYTES)));
        assert!(!safe_relative(&"a".repeat(MAX_PATH_BYTES + 1)));
    }

    #[test]
    fn the_shell_decides_the_argv() {
        let bash = shell_argv(None, "echo hi").unwrap();
        assert_eq!(bash, shell_argv(Some("bash"), "echo hi").unwrap());
        assert_eq!(
            bash,
            [
                "bash",
                "--noprofile",
                "--norc",
                "-eo",
                "pipefail",
                "-c",
                "echo hi"
            ]
        );
        assert_eq!(
            shell_argv(Some("sh"), "x").unwrap(),
            ["sh", "-e", "-c", "x"]
        );
        assert!(shell_argv(Some("pwsh"), "x").is_none());
        assert!(shell_argv(Some(""), "x").is_none());
    }

    #[test]
    fn a_plain_spec_is_valid_and_the_limits_are_exact() {
        assert!(validate(&spec(&[])).is_ok());
        assert!(validate(&spec(&["a", "b"])).is_ok());
        let many: Vec<String> = (0..MAX_STEPS).map(|i| i.to_string()).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(validate(&spec(&many)).is_ok());
        let mut over = many.clone();
        over.push("x");
        assert!(bad(&spec(&over)).contains("more than 64 setup steps"));

        let mut s = spec(&["a"]);
        s.toolchain = (0..MAX_LISTED)
            .map(|i| ToolRequirement {
                name: i.to_string(),
                version: "1".into(),
            })
            .collect();
        assert!(validate(&s).is_ok());
        s.toolchain.push(ToolRequirement {
            name: "x".into(),
            version: "1".into(),
        });
        assert!(bad(&s).contains("tools or secrets"));
        let secrets = |n: usize| -> Vec<SecretVersion> {
            (0..n)
                .map(|i| SecretVersion {
                    name: i.to_string(),
                    digest: "d".into(),
                })
                .collect()
        };
        let mut s = spec(&["a"]);
        s.secrets = secrets(MAX_LISTED);
        assert!(validate(&s).is_ok());
        s.secrets = secrets(MAX_LISTED + 1);
        assert!(bad(&s).contains("tools or secrets"));
    }

    #[test]
    fn a_zero_timeout_or_freshness_is_refused() {
        let mut s = spec(&["a"]);
        s.step_timeout_ms = Some(0);
        assert!(bad(&s).contains("zero"));
        s.step_timeout_ms = Some(1);
        assert!(validate(&s).is_ok());
        s.freshness_secs = Some(0);
        assert!(bad(&s).contains("zero"));
        s.freshness_secs = Some(1);
        assert!(validate(&s).is_ok());
    }

    #[test]
    fn a_step_is_checked_field_by_field() {
        let one = |f: &dyn Fn(&mut SetupStep)| {
            let mut s = spec(&["a"]);
            f(&mut s.steps[0]);
            s
        };
        assert!(bad(&one(&|s| s.id = String::new())).contains("id"));
        assert!(validate(&one(&|s| s.id = "i".repeat(MAX_ID_BYTES))).is_ok());
        assert!(bad(&one(&|s| s.id = "i".repeat(MAX_ID_BYTES + 1))).contains("id"));
        assert!(bad(&one(&|s| s.dir = "../x".into())).contains("dir"));
        assert!(validate(&one(&|s| s.dir = "web".into())).is_ok());
        assert!(bad(&one(&|s| s.run = String::new())).contains("script"));
        assert!(validate(&one(&|s| s.run = "x".repeat(MAX_RUN_BYTES))).is_ok());
        assert!(bad(&one(&|s| s.run = "x".repeat(MAX_RUN_BYTES + 1))).contains("script"));
        assert!(bad(&one(&|s| s.run = "a\0b".into())).contains("NUL"));
        // a shell the worker does not run is not an error: the step stays
        assert!(validate(&one(&|s| s.shell = Some("pwsh".into()))).is_ok());
        // environment
        let env = |n: usize| -> BTreeMap<String, String> {
            (0..n).map(|i| (format!("K{i}"), "v".to_string())).collect()
        };
        assert!(validate(&one(&|s| s.env = env(MAX_ENV_ENTRIES))).is_ok());
        assert!(bad(&one(&|s| s.env = env(MAX_ENV_ENTRIES + 1))).contains("environment"));
        let fat = |n: usize| BTreeMap::from([("K".to_string(), "v".repeat(n - 1))]);
        assert!(validate(&one(&|s| s.env = fat(MAX_ENV_BYTES))).is_ok());
        assert!(bad(&one(&|s| s.env = fat(MAX_ENV_BYTES + 1))).contains("environment"));
        for (k, v) in [("", "v"), ("A=B", "v"), ("A\0", "v"), ("A", "v\0")] {
            let e = BTreeMap::from([(k.to_string(), v.to_string())]);
            assert!(bad(&one(&|s| s.env = e.clone())).contains("bad environment"));
        }
    }

    #[test]
    fn step_ids_are_unique() {
        assert!(bad(&spec(&["a", "b", "a"])).contains("twice"));
        assert!(validate(&spec(&["a", "b"])).is_ok());
    }

    #[test]
    fn only_the_leading_steps_with_a_known_environment_are_hoisted() {
        let ids = |s: &SetupSpec| hoisted(s).iter().map(|x| x.id.clone()).collect::<Vec<_>>();
        let mut s = spec(&["a", "b", "c"]);
        assert_eq!(ids(&s), ["a", "b", "c"]);
        s.steps[1].unresolved_env = vec!["X".into()];
        assert_eq!(ids(&s), ["a"]);
        s.steps[0].unresolved_env = vec!["X".into()];
        assert!(ids(&s).is_empty());
        assert!(hoisted(&spec(&[])).is_empty());
        // a shell this worker does not run: that step and the ones after it
        let mut t = spec(&["a", "b", "c"]);
        t.steps[1].shell = Some("pwsh".into());
        assert_eq!(ids(&t), ["a"]);
        t.steps[1].shell = Some("sh".into());
        assert_eq!(ids(&t), ["a", "b", "c"]);
    }

    #[test]
    fn no_cache_makes_the_scope_a_run_scope() {
        let mut s = spec(&["a"]);
        assert_eq!(effective_scope(&s), ReuseScope::Persistent);
        s.scope = ReuseScope::Run;
        assert_eq!(effective_scope(&s), ReuseScope::Run);
        s.scope = ReuseScope::Persistent;
        s.no_cache = true;
        assert_eq!(effective_scope(&s), ReuseScope::Run);
    }

    #[test]
    fn no_cache_comes_from_the_flag_or_the_list() {
        let list = vec!["ci.yml".to_string(), "other.yml:e2e".to_string()];
        assert!(no_cache_applies(true, &[], "x.yml", "j"));
        assert!(!no_cache_applies(false, &[], "ci.yml", "j"));
        assert!(no_cache_applies(false, &list, "ci.yml", "any"));
        assert!(no_cache_applies(false, &list, "other.yml", "e2e"));
        assert!(!no_cache_applies(false, &list, "other.yml", "unit"));
        assert!(!no_cache_applies(false, &list, "e2e", "other.yml"));
    }

    #[test]
    fn the_key_changes_with_everything_that_decides_what_is_made() {
        let base = spec(&["a", "b"]);
        let c = ctx();
        let k = template_key(&base, &c);
        assert!(k.starts_with("sha256:"));
        assert_eq!(k, template_key(&base.clone(), &c.clone()));
        let differs = |s: &SetupSpec, c: &KeyContext| assert_ne!(k, template_key(s, c), "{s:?}");
        // the recipe, in order
        differs(&spec(&["b", "a"]), &c);
        differs(&spec(&["a"]), &c);
        let with = |f: &dyn Fn(&mut SetupSpec)| {
            let mut s = base.clone();
            f(&mut s);
            s
        };
        differs(&with(&|s| s.steps[0].run = "other".into()), &c);
        differs(&with(&|s| s.steps[0].dir = "web".into()), &c);
        // A16: the shell and the environment
        differs(&with(&|s| s.steps[0].shell = Some("sh".into())), &c);
        differs(
            &with(&|s| {
                s.steps[0].env.insert("CI".into(), "1".into());
            }),
            &c,
        );
        differs(&with(&|s| s.steps[0].id = "z".into()), &c);
        differs(
            &with(&|s| {
                s.toolchain.push(ToolRequirement {
                    name: "node".into(),
                    version: "20".into(),
                })
            }),
            &c,
        );
        differs(&with(&|s| s.freshness_secs = Some(5)), &c);
        differs(
            &with(&|s| {
                s.secrets.push(SecretVersion {
                    name: "T".into(),
                    digest: "v1".into(),
                })
            }),
            &c,
        );
        differs(&with(&|s| s.scope = ReuseScope::Run), &c);
        let cx = |f: &dyn Fn(&mut KeyContext)| {
            let mut x = c.clone();
            f(&mut x);
            x
        };
        differs(&base, &cx(&|x| x.source_digest = "sha256:other".into()));
        differs(&base, &cx(&|x| x.os = "macos".into()));
        differs(&base, &cx(&|x| x.arch = "aarch64".into()));
        differs(&base, &cx(&|x| x.profile = "p2".into()));
        // what the builder does not prepare is not in the key
        let mut tail = base.clone();
        tail.steps[1].unresolved_env = vec!["X".into()];
        assert_eq!(template_key(&tail, &c), template_key(&spec(&["a"]), &c));
    }

    #[test]
    fn the_run_id_is_in_a_run_scope_key_only() {
        let mut s = spec(&["a"]);
        let (c1, c2) = (ctx(), ctx());
        assert_ne!(c1.run_id, c2.run_id);
        assert_eq!(template_key(&s, &c1), template_key(&s, &c2));
        s.scope = ReuseScope::Run;
        assert_ne!(template_key(&s, &c1), template_key(&s, &c2));
        // --no-cache is a run scope: another run's template is not reused
        let mut n = spec(&["a"]);
        n.no_cache = true;
        assert_ne!(template_key(&n, &c1), template_key(&n, &c2));
        assert_eq!(template_key(&n, &c1), template_key(&n.clone(), &c1));
        // the same run shares it (singleflight)
        assert_eq!(template_key(&n, &c1), template_key(&s, &c1));
    }

    #[test]
    fn the_order_of_tools_and_secrets_does_not_change_the_key() {
        let tool = |n: &str| ToolRequirement {
            name: n.into(),
            version: "1".into(),
        };
        let secret = |n: &str| SecretVersion {
            name: n.into(),
            digest: "d".into(),
        };
        let mut a = spec(&["a"]);
        a.toolchain = vec![tool("x"), tool("y")];
        a.secrets = vec![secret("p"), secret("q")];
        let mut b = a.clone();
        b.toolchain.reverse();
        b.secrets.reverse();
        assert_eq!(template_key(&a, &ctx()), {
            let mut c = ctx();
            c.run_id = "other".into();
            template_key(&b, &c)
        });
    }

    #[test]
    fn freshness_is_counted_in_milliseconds_and_the_limit_is_inclusive() {
        assert!(fresh(0, u64::MAX, None));
        assert!(fresh(1000, 6000, Some(5)));
        assert!(!fresh(1000, 6001, Some(5)));
        // the clock went back: not older than anything
        assert!(fresh(9000, 1000, Some(1)));
        assert!(fresh(0, 1000, Some(u64::MAX)));
    }

    fn receipt(s: &SetupSpec, key: &str) -> PreparedTemplateReceipt {
        seal_receipt(
            key,
            "sha256:src",
            &BuilderId::generate(),
            s,
            vec![PreparedArtifact {
                path: "node_modules".into(),
                digest: format!("sha256:{}", "a".repeat(64)),
            }],
            1000,
        )
    }

    #[test]
    fn a_sealed_receipt_names_the_steps_and_its_own_digest() {
        let s = spec(&["a", "b"]);
        let r = receipt(&s, "k");
        assert_eq!(r.steps, ["a", "b"]);
        assert_eq!(r.schema_version, SchemaVersion::CURRENT);
        assert_eq!(r.digest, receipt_digest(&r));
        assert!(r.digest.starts_with("sha256:"));
        assert_eq!(
            skip_set(&r),
            BTreeSet::from(["a".to_string(), "b".to_string()])
        );
        // the digest covers every field
        let mut x = r.clone();
        x.created_at_ms += 1;
        assert_ne!(receipt_digest(&x), r.digest);
        let mut x = r.clone();
        x.artifacts[0].path = "other".into();
        assert_ne!(receipt_digest(&x), r.digest);
        // and not itself
        let mut x = r.clone();
        x.digest = "anything".into();
        assert_eq!(receipt_digest(&x), r.digest);
        // only the hoisted steps are named
        let mut t = spec(&["a", "b"]);
        t.steps[1].unresolved_env = vec!["X".into()];
        assert_eq!(receipt(&t, "k").steps, ["a"]);
    }

    #[test]
    fn a_receipt_is_checked_against_what_was_asked() {
        let mut s = spec(&["a", "b"]);
        let r = receipt(&s, "k");
        let now = 2000;
        assert_eq!(check_receipt(&r, "k", "sha256:src", &s, now), Ok(()));
        assert_eq!(
            check_receipt(&r, "other", "sha256:src", &s, now),
            Err(Reject::Key)
        );
        assert_eq!(
            check_receipt(&r, "k", "sha256:else", &s, now),
            Err(Reject::Source)
        );
        let mut x = r.clone();
        x.schema_version = SchemaVersion(99);
        assert_eq!(
            check_receipt(&x, "k", "sha256:src", &s, now),
            Err(Reject::Schema)
        );
        let mut x = r.clone();
        x.steps.pop();
        assert_eq!(
            check_receipt(&x, "k", "sha256:src", &s, now),
            Err(Reject::Digest)
        );
        // sealed again, but for other steps than were asked
        let other = receipt(&spec(&["a"]), "k");
        assert_eq!(
            check_receipt(&other, "k", "sha256:src", &s, now),
            Err(Reject::Steps)
        );
        let reorder = receipt(&spec(&["b", "a"]), "k");
        assert_eq!(
            check_receipt(&reorder, "k", "sha256:src", &s, now),
            Err(Reject::Steps)
        );
        // freshness
        s.freshness_secs = Some(1);
        let r = receipt(&s, "k");
        assert_eq!(check_receipt(&r, "k", "sha256:src", &s, 2000), Ok(()));
        assert_eq!(
            check_receipt(&r, "k", "sha256:src", &s, 2001),
            Err(Reject::Stale)
        );
    }

    #[test]
    fn the_artifacts_of_a_receipt_are_checked() {
        let s = spec(&["a"]);
        let with = |f: &dyn Fn(&mut PreparedTemplateReceipt)| {
            let mut r = receipt(&s, "k");
            f(&mut r);
            r.digest = receipt_digest(&r);
            check_receipt(&r, "k", "sha256:src", &s, 2000)
        };
        let art = |p: &str, d: &str| PreparedArtifact {
            path: p.into(),
            digest: d.into(),
        };
        let good = format!("sha256:{}", "b".repeat(64));
        let is_artifacts = |r: Result<(), Reject>| matches!(r, Err(Reject::Artifacts(_)));
        assert_eq!(with(&|r| r.artifacts.clear()), Ok(()));
        assert!(is_artifacts(with(&|r| r.artifacts = vec![art("", &good)])));
        assert!(is_artifacts(with(
            &|r| r.artifacts = vec![art("../x", &good)]
        )));
        assert!(is_artifacts(
            with(&|r| r.artifacts = vec![art("/x", &good)])
        ));
        assert!(is_artifacts(with(
            &|r| r.artifacts = vec![art(".git", &good)]
        )));
        assert!(is_artifacts(with(
            &|r| r.artifacts = vec![art("a/.git/b", &good)]
        )));
        assert!(is_artifacts(with(
            &|r| r.artifacts = vec![art("x", "md5:zz")]
        )));
        assert!(is_artifacts(with(
            &|r| r.artifacts = vec![art("x", &good), art("x", &good)]
        )));
        assert_eq!(
            with(&|r| r.artifacts = vec![art("x", &good), art("y/z", &good)]),
            Ok(())
        );
        let many = |n: usize| -> Vec<PreparedArtifact> {
            (0..n).map(|i| art(&format!("d{i}"), &good)).collect()
        };
        assert_eq!(with(&|r| r.artifacts = many(MAX_ARTIFACTS)), Ok(()));
        assert!(is_artifacts(with(
            &|r| r.artifacts = many(MAX_ARTIFACTS + 1)
        )));
    }

    #[test]
    fn a_rejection_says_why() {
        let all = [
            Reject::Schema,
            Reject::Digest,
            Reject::Key,
            Reject::Source,
            Reject::Steps,
            Reject::Artifacts("`x`".into()),
            Reject::Stale,
        ];
        let texts: BTreeSet<String> = all.iter().map(|r| r.to_string()).collect();
        assert_eq!(texts.len(), all.len());
        assert!(Reject::Artifacts("`x`".into()).to_string().contains("`x`"));
        assert!(Reject::Key.to_string().contains("key"));
        assert!(Reject::Source.to_string().contains("source"));
        assert!(Reject::Steps.to_string().contains("steps"));
        assert!(Reject::Stale.to_string().contains("older"));
        assert!(Reject::Digest.to_string().contains("digest"));
        assert!(Reject::Schema.to_string().contains("schema"));
    }

    fn cmd(node: &str, step: Option<&str>, deps: &[&str]) -> CommandSpec {
        CommandSpec {
            node_id: NodeId::parse(node).unwrap(),
            argv: vec!["true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: deps.iter().map(|d| NodeId::parse(d).unwrap()).collect(),
            timeout_ms: None,
            setup_step: step.map(str::to_string),
        }
    }

    #[test]
    fn the_prepared_steps_are_left_out_and_nothing_waits_for_them() {
        let commands = vec![
            cmd("install", Some("a"), &[]),
            cmd("lint", Some("b"), &["install"]),
            cmd("test", None, &["install", "lint"]),
            cmd("other", Some("z"), &[]),
        ];
        let skip = BTreeSet::from(["a".to_string(), "b".to_string()]);
        let left = prune_commands(&commands, &skip);
        let names: Vec<_> = left.iter().map(|c| c.node_id.as_str()).collect();
        assert_eq!(names, ["test", "other"]);
        assert!(left[0].depends_on.is_empty());
        // a step the receipt does not hold stays, and so does a command
        // that is no step
        assert_eq!(prune_commands(&commands, &BTreeSet::new()), commands);
        let some = prune_commands(&commands, &BTreeSet::from(["a".to_string()]));
        assert_eq!(some.len(), 3);
        assert_eq!(
            some[1]
                .depends_on
                .iter()
                .map(|d| d.as_str())
                .collect::<Vec<_>>(),
            ["install", "lint"][1..].to_vec()
        );
    }

    #[test]
    fn what_the_setup_added_is_what_was_not_there_before() {
        let set = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();
        let before = set(&["src", "package.json"]);
        let after = set(&[
            "src",
            "package.json",
            "node_modules",
            "web/node_modules",
            ".git",
        ]);
        assert_eq!(
            added_entries(&before, &after),
            ["node_modules", "web/node_modules"]
        );
        assert!(added_entries(&after, &before).is_empty());
        assert!(added_entries(&after, &set(&["x/.git/y"])).is_empty());
    }

    #[test]
    fn an_artifact_is_never_written_over() {
        assert_eq!(place(false, None, "d"), Place::Copy);
        assert_eq!(place(false, Some("d"), "d"), Place::Copy);
        assert_eq!(place(true, Some("d"), "d"), Place::AlreadyThere);
        assert_eq!(place(true, Some("e"), "d"), Place::Conflict);
        assert_eq!(place(true, None, "d"), Place::Conflict);
    }

    fn term(o: Outcome, exit: Option<Exit>) -> Option<Termination> {
        let timeout = (o == Outcome::TimedOut).then_some(crate::state::TimeoutInfo {
            kind: crate::state::TimeoutKind::Command,
            phase: crate::state::TimeoutPhase::Running,
        });
        Some(Termination::new(o, exit, timeout).unwrap())
    }

    #[test]
    fn the_end_of_a_builder_reads_to_the_consumers_that_waited() {
        assert_eq!(
            end_of(term(Outcome::Succeeded, Some(Exit::Code(0)))),
            BuildEnd::Built
        );
        assert!(end_of(term(Outcome::Succeeded, Some(Exit::Code(0)))).is_built());
        assert_eq!(
            end_of(term(Outcome::Failed, Some(Exit::Code(3)))),
            BuildEnd::Failed(Some(Exit::Code(3)))
        );
        assert_eq!(end_of(term(Outcome::TimedOut, None)), BuildEnd::TimedOut);
        assert_eq!(end_of(term(Outcome::Cancelled, None)), BuildEnd::Cancelled);
        assert!(matches!(
            end_of(term(Outcome::Interrupted, None)),
            BuildEnd::Crashed(_)
        ));
        assert!(matches!(end_of(None), BuildEnd::Crashed(_)));
        for e in [
            BuildEnd::Failed(None),
            BuildEnd::TimedOut,
            BuildEnd::Cancelled,
            BuildEnd::Crashed("x".into()),
        ] {
            assert!(!e.is_built());
        }
    }

    #[test]
    fn every_end_has_its_own_description() {
        let all = [
            BuildEnd::Built,
            BuildEnd::Failed(Some(Exit::Code(3))),
            BuildEnd::Failed(Some(Exit::Signal(9))),
            BuildEnd::Failed(None),
            BuildEnd::TimedOut,
            BuildEnd::Cancelled,
            BuildEnd::Crashed("why".into()),
        ];
        let texts: BTreeSet<String> = all.iter().map(BuildEnd::describe).collect();
        assert_eq!(texts.len(), all.len());
        assert!(BuildEnd::Failed(Some(Exit::Code(3)))
            .describe()
            .contains('3'));
        assert!(BuildEnd::Failed(Some(Exit::Signal(9)))
            .describe()
            .contains('9'));
        assert!(BuildEnd::Crashed("why".into()).describe().contains("why"));
        assert!(BuildEnd::TimedOut.describe().contains("timed out"));
        assert!(BuildEnd::Cancelled.describe().contains("cancelled"));
    }

    #[test]
    fn the_last_reference_of_a_working_builder_stops_it() {
        let mut refs = BTreeSet::new();
        assert!(join(&mut refs, "a"));
        assert!(!join(&mut refs, "b"));
        assert!(!join(&mut refs, "b"));
        assert_eq!(refs.len(), 2);
        assert_eq!(leave(&mut refs, "zz", true), Leave::Unknown);
        assert_eq!(leave(&mut refs, "a", true), Leave::Keep);
        assert_eq!(leave(&mut refs, "b", true), Leave::StopBuilder);
        assert!(refs.is_empty());
        // an ended builder is not stopped
        join(&mut refs, "c");
        assert_eq!(leave(&mut refs, "c", false), Leave::Keep);
        // joining again after everyone left is a first again
        assert!(join(&mut refs, "d"));
    }

    #[test]
    fn the_lease_is_renewed_only_for_a_wanted_working_builder() {
        assert!(renew_lease(1, false));
        assert!(renew_lease(5, false));
        assert!(!renew_lease(0, false));
        assert!(!renew_lease(1, true));
        assert!(!renew_lease(0, true));
    }

    #[test]
    fn fast_worktree_wins_over_prepare() {
        use DependencySource::*;
        assert_eq!(dependency_source(true, true), FastWorktree);
        assert_eq!(dependency_source(true, false), FastWorktree);
        assert_eq!(dependency_source(false, true), Prepare);
        assert_eq!(dependency_source(false, false), None);
    }

    #[test]
    fn a_word_is_quoted_only_when_the_shell_would_change_it() {
        assert_eq!(shell_quote("pnpm"), "pnpm");
        assert_eq!(shell_quote("--frozen-lockfile"), "--frozen-lockfile");
        assert_eq!(shell_quote("a/b.c:d,e=f@g%h+i_j"), "a/b.c:d,e=f@g%h+i_j");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
        assert_eq!(shell_quote("$HOME"), "'$HOME'");
        assert_eq!(shell_quote("a;b"), "'a;b'");
    }

    #[test]
    fn prepare_commands_become_a_setup_unless_fast_worktree_is_there() {
        let cmds = vec![
            vec!["corepack".to_string(), "pnpm".into(), "install".into()],
            vec!["echo".to_string(), "a b".into()],
        ];
        assert!(prepare_spec(&cmds, true, false).is_none());
        assert!(prepare_spec(&[], false, false).is_none());
        let s = prepare_spec(&cmds, false, false).unwrap();
        assert!(validate(&s).is_ok());
        assert_eq!(s.steps.len(), 2);
        assert_eq!(s.steps[0].run, "corepack pnpm install");
        assert_eq!(s.steps[1].run, "echo 'a b'");
        assert_ne!(s.steps[0].id, s.steps[1].id);
        assert!(s
            .steps
            .iter()
            .all(|x| x.dir.is_empty() && x.shell.is_none()));
        assert_eq!(s.scope, ReuseScope::Persistent);
        assert!(!s.no_cache);
        assert!(prepare_spec(&cmds, false, true).unwrap().no_cache);
        // the same command in another place of the list is another step
        let twice = vec![cmds[0].clone(), cmds[0].clone()];
        let t = prepare_spec(&twice, false, false).unwrap();
        assert_ne!(t.steps[0].id, t.steps[1].id);
        assert_eq!(
            s.steps[0].id,
            prepare_spec(&cmds, false, false).unwrap().steps[0].id
        );
    }
}
