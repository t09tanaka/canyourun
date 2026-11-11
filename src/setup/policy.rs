//! The decisions of the setup builder, free of I/O (spec chapter 8): what a
//! spec may hold, how its template key is made, whether a receipt is the one
//! asked for, who still holds a builder, how a builder's end reads to the
//! consumers that waited for it, and where the dependencies of a project
//! come from. Covered by the coverage and mutation gates.

use crate::ids::{BuilderId, Digest};
use crate::state::{Exit, Outcome, Termination};
use crate::wire::{
    canonical_json, CommandSpec, PreparedArtifact, PreparedChange, PreparedRemoval,
    PreparedTemplateReceipt, ReuseScope, SchemaVersion, SetupSpec, SetupStep,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Bump when what a template holds, or how its key is made, changes.
pub const CACHE_SCHEMA: u32 = 2;
/// Changed and removed entries of one template, together.
pub const MAX_CHANGES: usize = 4096;
/// How deep a tree of a workspace or a template may be.
pub const MAX_DEPTH: usize = 128;
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
/// consumer, and so does every step after it (it may depend on it). A spec
/// that names secrets is not prepared at all: a builder is given no secret,
/// so its steps would not run as the consumer's do.
pub fn hoisted(spec: &SetupSpec) -> &[SetupStep] {
    if !spec.secrets.is_empty() {
        return &[];
    }
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
    /// The environment every step of this worker starts from (`PATH`,
    /// `HOME`, ...); a step's own `env` is laid over it.
    pub env: BTreeMap<String, String>,
    /// What the declared tools are on this worker, by name: a fingerprint
    /// of the binary the name resolves to (or that there is none).
    pub tools: Vec<(String, String)>,
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
    let mut tools: Vec<_> = ctx.tools.iter().map(|(n, f)| json!([n, f])).collect();
    tools.sort_by_key(|v| v.to_string());
    let effective = |s: &SetupStep| {
        let mut env = ctx.env.clone();
        env.extend(s.env.clone());
        env
    };
    let inputs = json!({
        "cache_schema": CACHE_SCHEMA,
        "source": ctx.source_digest,
        "steps": hoisted(spec).iter().map(|s| json!({
            "id": s.id,
            "dir": s.dir,
            "run": s.run,
            "shell": s.shell.as_deref().unwrap_or("bash"),
            "env": effective(s),
        })).collect::<Vec<_>>(),
        "toolchain": toolchain,
        "tools": tools,
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
#[allow(clippy::too_many_arguments)]
pub fn seal_receipt(
    key: &str,
    source: &str,
    builder: &BuilderId,
    spec: &SetupSpec,
    artifacts: Vec<PreparedArtifact>,
    modified: Vec<PreparedChange>,
    removed: Vec<PreparedRemoval>,
    created_at_ms: u64,
) -> PreparedTemplateReceipt {
    let mut r = PreparedTemplateReceipt {
        schema_version: SchemaVersion::CURRENT,
        template_key: key.to_string(),
        source_digest: source.to_string(),
        builder_id: builder.clone(),
        steps: hoisted(spec).iter().map(|s| s.id.clone()).collect(),
        artifacts,
        modified,
        removed,
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
    if r.artifacts.len() > MAX_ARTIFACTS || r.modified.len() + r.removed.len() > MAX_CHANGES {
        return Err(Reject::Artifacts("too many".into()));
    }
    let digest = |d: &str| d.starts_with(Digest::PREFIX);
    let added = r.artifacts.iter().map(|a| (&a.path, digest(&a.digest)));
    let modified = r
        .modified
        .iter()
        .map(|m| (&m.path, digest(&m.base) && digest(&m.digest)));
    let removed = r
        .removed
        .iter()
        .map(|x| (&x.path, x.base.as_deref().is_none_or(digest)));
    let mut seen = BTreeSet::new();
    for (path, digests_ok) in added.chain(modified).chain(removed) {
        let bad = path.is_empty() || !safe_relative(path) || in_git(path) || !digests_ok;
        if bad {
            return Err(Reject::Artifacts(format!("`{path}`")));
        }
        if !seen.insert(path.as_str()) {
            return Err(Reject::Artifacts(format!("`{path}` is there twice")));
        }
    }
    // What is put whole (an added tree, a changed file) holds no other entry.
    let whole = r.artifacts.iter().map(|a| &a.path);
    for path in whole.chain(r.modified.iter().map(|m| &m.path)) {
        let under = format!("{path}/");
        let next = seen.range(under.as_str()..).next();
        if next.is_some_and(|p| p.starts_with(&under)) {
            return Err(Reject::Artifacts(format!("`{path}` holds another entry")));
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

/// A path at or under an entry named `.git`.
pub fn in_git(path: &str) -> bool {
    path.split('/').any(|c| c == ".git")
}

fn parent_of(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

/// What one entry of a workspace is: a directory, or a file or link by the
/// digest of what it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sig {
    Dir,
    File(String),
    Link(String),
}

impl Sig {
    fn digest(&self) -> Option<&str> {
        match self {
            Sig::Dir => None,
            Sig::File(d) | Sig::Link(d) => Some(d),
        }
    }
}

/// Why what a builder's steps did is not kept as a template. The steps
/// passed; the consumers run them themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unkept {
    /// A directory became a file or a file a directory.
    TypeChange(String),
    /// Something of Git's own changed (a repository's state, a submodule's
    /// link to it): it lives outside what a template can hold.
    Git(String),
    /// A link that leads out of the workspace.
    Link {
        path: String,
        target: String,
    },
    TooMany,
    /// The workspace could not be read whole.
    Unreadable(String),
}

impl std::fmt::Display for Unkept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unkept::TypeChange(p) => {
                write!(f, "`{p}` changed between a directory and a file")
            }
            Unkept::Git(p) => write!(f, "`{p}` is Git's own state"),
            Unkept::Link { path, target } => {
                write!(
                    f,
                    "the link `{path}` leads out of the workspace, to `{target}`"
                )
            }
            Unkept::TooMany => f.write_str("too many entries were added, changed or removed"),
            Unkept::Unreadable(why) => write!(f, "the workspace could not be read: {why}"),
        }
    }
}

/// What the steps of a builder did to its workspace.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Changes {
    /// The topmost new entries, at any depth.
    pub added: Vec<String>,
    /// Files and links that hold something else now: the path and the
    /// digest it had before.
    pub modified: Vec<(String, String)>,
    pub removed: Vec<PreparedRemoval>,
}

/// Compare the workspace before the steps with the one after them. `after`
/// lists a new directory without what is in it (it is kept whole). Nothing
/// depends on where a step wrote: every entry of the workspace is compared.
pub fn change_set(
    before: &BTreeMap<String, Sig>,
    after: &BTreeMap<String, Sig>,
) -> Result<Changes, Unkept> {
    let mut c = Changes::default();
    let git = |p: &String| Err(Unkept::Git(p.clone()));
    for (path, now) in after {
        match before.get(path) {
            None => {
                let parent = parent_of(path);
                if !parent.is_empty() && !before.contains_key(parent) {
                    // inside something new, which is kept whole
                    continue;
                }
                if in_git(path) {
                    return git(path);
                }
                c.added.push(path.clone());
            }
            Some(was) if was == now => {}
            Some(was) => match (was.digest(), now.digest()) {
                (Some(base), Some(_)) => {
                    if in_git(path) {
                        return git(path);
                    }
                    c.modified.push((path.clone(), base.to_string()));
                }
                _ => return Err(Unkept::TypeChange(path.clone())),
            },
        }
    }
    for (path, was) in before {
        if !after.contains_key(path) {
            if in_git(path) {
                return git(path);
            }
            c.removed.push(PreparedRemoval {
                path: path.clone(),
                base: was.digest().map(str::to_string),
            });
        }
    }
    if c.added.len() > MAX_ARTIFACTS || c.modified.len() + c.removed.len() > MAX_CHANGES {
        return Err(Unkept::TooMany);
    }
    Ok(c)
}

/// What to do with a link the steps made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkPlan {
    /// A relative link that stays in the workspace.
    Keep,
    /// An absolute link into the builder's workspace: the same place, said
    /// relative to the link, so that it holds in a consumer's workspace.
    Rewrite(String),
    /// It leads out of the workspace.
    Refuse,
}

/// `path` walked from `at` by its text alone; `None` when it leaves the top.
fn walk<'a>(mut at: Vec<&'a str>, path: &'a str) -> Option<Vec<&'a str>> {
    for c in path.split('/') {
        match c {
            "" | "." => {}
            ".." => {
                at.pop()?;
            }
            c => at.push(c),
        }
    }
    Some(at)
}

/// Judge the link at `link` (relative to the workspace) with `target`.
/// `roots` are the spellings of the builder's workspace root (as given and
/// resolved), without a trailing slash.
pub fn link_plan(link: &str, target: &str, roots: &[String]) -> LinkPlan {
    let dir = walk(Vec::new(), parent_of(link)).unwrap_or_default();
    if !target.starts_with('/') {
        return match walk(dir, target) {
            Some(_) => LinkPlan::Keep,
            None => LinkPlan::Refuse,
        };
    }
    let inside = roots.iter().find_map(|root| {
        let rest = target.strip_prefix(root.as_str())?;
        (rest.is_empty() || rest.starts_with('/')).then_some(rest)
    });
    let Some(to) = inside.and_then(|rest| walk(Vec::new(), rest)) else {
        return LinkPlan::Refuse;
    };
    let mut rel = vec![".."; dir.len()];
    rel.extend(to);
    if rel.is_empty() {
        rel.push(".");
    }
    LinkPlan::Rewrite(rel.join("/"))
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

/// What to do with a changed entry: `dest` is the digest of what the
/// consumer's workspace holds at its path (none when nothing readable is
/// there). Only an entry that is still what the builder started from is
/// replaced.
pub fn place_modified(dest: Option<&str>, base: &str, want: &str) -> Place {
    match dest {
        Some(d) if d == want => Place::AlreadyThere,
        Some(d) if d == base => Place::Copy,
        _ => Place::Conflict,
    }
}

/// What to do with an entry the steps removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gone {
    /// Nothing is there.
    Already,
    Remove,
    /// Something else is there than the builder started from.
    Conflict,
}

pub fn place_removed(exists: bool, is_dir: bool, dest: Option<&str>, base: Option<&str>) -> Gone {
    if !exists {
        return Gone::Already;
    }
    let same = match base {
        None => is_dir,
        Some(b) => !is_dir && dest == Some(b),
    };
    if same {
        Gone::Remove
    } else {
        Gone::Conflict
    }
}

/// A template in the store, as the eviction sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kept {
    pub name: String,
    pub bytes: u64,
    pub last_used_ms: u64,
    /// A run-scope template whose run has no attempt on this worker.
    pub run_gone: bool,
    /// Somebody copies out of it.
    pub pinned: bool,
}

/// How much the store of templates may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// All templates together.
    pub max_bytes: u64,
    /// A template nobody used for this long goes.
    pub max_age_ms: u64,
    /// A template of a run that is gone goes after this long without use.
    pub run_idle_ms: u64,
}

/// The names of the templates to take out: those not used for too long and
/// those of runs that are gone, then, while the rest is over the size, the
/// ones used least recently. A pinned template always stays.
pub fn evictions(kept: &[Kept], now_ms: u64, limits: &Limits) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest: Vec<&Kept> = Vec::new();
    for k in kept {
        let idle = now_ms.saturating_sub(k.last_used_ms);
        let old = idle > limits.max_age_ms || (k.run_gone && idle > limits.run_idle_ms);
        if old && !k.pinned {
            out.push(k.name.clone());
        } else {
            rest.push(k);
        }
    }
    let mut total = rest.iter().fold(0u64, |n, k| n.saturating_add(k.bytes));
    rest.sort_by(|a, b| (a.last_used_ms, &a.name).cmp(&(b.last_used_ms, &b.name)));
    for k in rest {
        if total <= limits.max_bytes {
            break;
        }
        if !k.pinned {
            total = total.saturating_sub(k.bytes);
            out.push(k.name.clone());
        }
    }
    out
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
    /// The steps passed, but what they did cannot be kept as a template.
    NotKept(String),
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
            BuildEnd::NotKept(why) => {
                format!("what the setup steps did cannot be kept as a template: {why}")
            }
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
            env: BTreeMap::from([("PATH".to_string(), "/bin".to_string())]),
            tools: vec![("node".to_string(), "/bin/node 1 2".to_string())],
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
        // a builder gets no secret: steps that need one are not prepared
        let mut u = spec(&["a", "b"]);
        u.secrets = vec![SecretVersion {
            name: "TOKEN".into(),
            digest: "v1".into(),
        }];
        assert!(ids(&u).is_empty());
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
        // M7: the environment the worker gives every step, and its tools
        let with_ctx = |f: &dyn Fn(&mut KeyContext)| {
            let mut x = c.clone();
            f(&mut x);
            x
        };
        differs(
            &base,
            &with_ctx(&|x| {
                x.env.insert("PATH".into(), "/opt/bin".into());
            }),
        );
        differs(&base, &with_ctx(&|x| x.tools[0].1 = "/bin/node 1 3".into()));
        differs(&base, &with_ctx(&|x| x.tools.clear()));
        // what counts is the environment a step ends up with
        let over = with(&|s| {
            for st in &mut s.steps {
                st.env.insert("PATH".into(), "/bin".into());
            }
        });
        assert_eq!(k, template_key(&over, &c));
        assert_eq!(k, template_key(&over, &with_ctx(&|x| x.env.clear())));
        let two = with_ctx(&|x| x.tools.push(("go".into(), "absent".into())));
        let mut swapped = two.clone();
        swapped.tools.reverse();
        assert_eq!(template_key(&base, &two), template_key(&base, &swapped));
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
            vec![],
            vec![],
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
    fn the_changed_and_removed_entries_of_a_receipt_are_checked() {
        let s = spec(&["a"]);
        let with = |f: &dyn Fn(&mut PreparedTemplateReceipt)| {
            let mut r = receipt(&s, "k");
            r.artifacts.clear();
            f(&mut r);
            r.digest = receipt_digest(&r);
            check_receipt(&r, "k", "sha256:src", &s, 2000)
        };
        let good = format!("sha256:{}", "b".repeat(64));
        let art = |p: &str| PreparedArtifact {
            path: p.into(),
            digest: good.clone(),
        };
        let ch = |p: &str, base: &str, d: &str| PreparedChange {
            path: p.into(),
            base: base.into(),
            digest: d.into(),
        };
        let rm = |p: &str, base: Option<&str>| PreparedRemoval {
            path: p.into(),
            base: base.map(str::to_string),
        };
        let bad = |r: Result<(), Reject>| matches!(r, Err(Reject::Artifacts(_)));
        assert_eq!(
            with(&|r| {
                r.artifacts = vec![art("src/pkg.egg-info")];
                r.modified = vec![ch("package-lock.json", &good, &good)];
                r.removed = vec![rm("old", None), rm("old/a.txt", Some(&good))];
            }),
            Ok(())
        );
        // each digest of a changed entry, and the one of a removed file
        assert!(bad(with(&|r| r.modified = vec![ch("x", "md5:z", &good)])));
        assert!(bad(with(&|r| r.modified = vec![ch("x", &good, "md5:z")])));
        assert!(bad(with(&|r| r.removed = vec![rm("x", Some("md5:z"))])));
        // their paths
        assert!(bad(with(&|r| r.modified = vec![ch("../x", &good, &good)])));
        assert!(bad(with(&|r| r.removed = vec![rm("", None)])));
        assert!(bad(with(&|r| r.removed = vec![rm("a/.git/x", None)])));
        // one path, one entry, whichever list it is in
        assert!(bad(with(&|r| {
            r.artifacts = vec![art("x")];
            r.removed = vec![rm("x", None)];
        })));
        assert!(bad(with(&|r| {
            r.modified = vec![ch("x", &good, &good)];
            r.removed = vec![rm("x", None)];
        })));
        // L4: nothing lies inside an added tree or a changed file
        let inside = with(&|r| r.artifacts = vec![art("foo"), art("foo/bar")]);
        assert_eq!(
            inside,
            Err(Reject::Artifacts("`foo` holds another entry".into()))
        );
        assert!(bad(with(&|r| {
            r.modified = vec![ch("foo", &good, &good)];
            r.removed = vec![rm("foo/bar", None)];
        })));
        // a name that only begins the same is no entry inside
        assert_eq!(
            with(&|r| r.artifacts = vec![art("foo"), art("foo-bar"), art("foo.d/x")]),
            Ok(())
        );
        // a removed directory does hold the removed entries under it
        assert_eq!(
            with(&|r| r.removed = vec![rm("foo", None), rm("foo/bar", Some(&good))]),
            Ok(())
        );
        // the two lists share one limit
        let changes = |n: usize| -> Vec<PreparedChange> {
            (0..n).map(|i| ch(&format!("c{i}"), &good, &good)).collect()
        };
        assert_eq!(
            with(&|r| {
                r.modified = changes(MAX_CHANGES - 1);
                r.removed = vec![rm("gone", None)];
            }),
            Ok(())
        );
        assert!(bad(with(&|r| {
            r.modified = changes(MAX_CHANGES);
            r.removed = vec![rm("gone", None)];
        })));
        assert!(bad(with(&|r| {
            r.modified = changes(2);
            r.removed = (0..MAX_CHANGES - 1)
                .map(|i| rm(&format!("r{i}"), None))
                .collect();
        })));
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

    fn tree(entries: &[(&str, &str)]) -> BTreeMap<String, Sig> {
        entries
            .iter()
            .map(|(p, s)| {
                let sig = match s.split_once(':') {
                    None => Sig::Dir,
                    Some(("l", d)) => Sig::Link(d.to_string()),
                    Some((_, d)) => Sig::File(d.to_string()),
                };
                (p.to_string(), sig)
            })
            .collect()
    }

    const SOURCE: &[(&str, &str)] = &[
        ("package.json", "f:1"),
        ("package-lock.json", "f:2"),
        ("packages", "d"),
        ("packages/a", "d"),
        ("packages/a/index.js", "f:3"),
        ("src", "d"),
        ("src/lib.py", "f:4"),
        ("tool", "l:5"),
    ];

    fn after(more: &[(&str, &str)], without: &[&str]) -> BTreeMap<String, Sig> {
        let mut t = tree(SOURCE);
        for w in without {
            t.remove(*w).unwrap();
        }
        t.extend(tree(more));
        t
    }

    #[test]
    fn what_the_steps_added_is_found_wherever_they_wrote() {
        let before = tree(SOURCE);
        assert_eq!(change_set(&before, &before), Ok(Changes::default()));
        // at the top, under a directory that was there (a workspace
        // package's node_modules, an egg-info beside the sources), and
        // nothing of what lies inside a new directory
        let now = after(
            &[
                ("node_modules", "d"),
                ("packages/a/node_modules", "d"),
                ("packages/a/node_modules/dep", "d"),
                ("packages/a/node_modules/dep/x.js", "f:9"),
                ("src/pkg.egg-info", "d"),
                ("src/made.txt", "f:8"),
                ("link", "l:7"),
            ],
            &[],
        );
        let c = change_set(&before, &now).unwrap();
        assert_eq!(
            c.added,
            [
                "link",
                "node_modules",
                "packages/a/node_modules",
                "src/made.txt",
                "src/pkg.egg-info"
            ]
        );
        assert!(c.modified.is_empty() && c.removed.is_empty());
    }

    #[test]
    fn what_the_steps_changed_or_removed_is_found_with_what_it_was() {
        let before = tree(SOURCE);
        // a lockfile written again, a link pointed elsewhere, a file that
        // became a link
        let now = after(
            &[
                ("package-lock.json", "f:20"),
                ("tool", "l:50"),
                ("src/lib.py", "l:4"),
            ],
            &["package.json", "packages/a/index.js", "packages/a"],
        );
        let c = change_set(&before, &now).unwrap();
        assert!(c.added.is_empty());
        assert_eq!(
            c.modified,
            [
                ("package-lock.json".to_string(), "2".to_string()),
                ("src/lib.py".to_string(), "4".to_string()),
                ("tool".to_string(), "5".to_string()),
            ]
        );
        let rm = |p: &str, base: Option<&str>| PreparedRemoval {
            path: p.into(),
            base: base.map(str::to_string),
        };
        assert_eq!(
            c.removed,
            [
                rm("package.json", Some("1")),
                rm("packages/a", None),
                rm("packages/a/index.js", Some("3")),
            ]
        );
    }

    #[test]
    fn what_cannot_be_kept_is_told_apart() {
        let before = tree(SOURCE);
        // a directory that became a file, and the other way round
        assert_eq!(
            change_set(&before, &after(&[("src", "f:1")], &["src/lib.py"])),
            Err(Unkept::TypeChange("src".into()))
        );
        assert_eq!(
            change_set(&before, &after(&[("package.json", "d")], &[])),
            Err(Unkept::TypeChange("package.json".into()))
        );
        // Git's own: added, changed or removed, at any depth
        let git = |r: Result<Changes, Unkept>, p: &str| assert_eq!(r, Err(Unkept::Git(p.into())));
        git(
            change_set(&before, &after(&[("src/.git", "f:1")], &[])),
            "src/.git",
        );
        git(change_set(&before, &after(&[(".git", "d")], &[])), ".git");
        let mut with_git = tree(SOURCE);
        with_git.extend(tree(&[(".git", "f:1"), ("sub", "d"), ("sub/.git", "f:2")]));
        let mut changed = with_git.clone();
        changed.insert(".git".into(), Sig::File("9".into()));
        git(change_set(&with_git, &changed), ".git");
        let mut removed = with_git.clone();
        removed.remove("sub/.git").unwrap();
        git(change_set(&with_git, &removed), "sub/.git");
        // an entry merely named like it is none of Git's
        assert!(!in_git("a/.github/x") && !in_git(".gitignore") && !in_git(""));
        assert!(in_git(".git") && in_git("a/.git") && in_git("a/.git/b"));
    }

    #[test]
    fn the_limits_of_a_change_set_are_exact() {
        let before = tree(SOURCE);
        let add = |n: usize| {
            let mut t = before.clone();
            t.extend((0..n).map(|i| (format!("n{i}"), Sig::Dir)));
            t
        };
        assert_eq!(
            change_set(&before, &add(MAX_ARTIFACTS))
                .unwrap()
                .added
                .len(),
            MAX_ARTIFACTS
        );
        assert_eq!(
            change_set(&before, &add(MAX_ARTIFACTS + 1)),
            Err(Unkept::TooMany)
        );
        // changed and removed entries share a limit
        let big = |n: usize| -> BTreeMap<String, Sig> {
            (0..n)
                .map(|i| (format!("f{i}"), Sig::File("1".into())))
                .chain([("keep".to_string(), Sig::File("1".into()))])
                .collect()
        };
        let changed_one = |n: usize| {
            let mut t = BTreeMap::new();
            t.insert("keep".to_string(), Sig::File("2".into()));
            (big(n), t)
        };
        let (b, a) = changed_one(MAX_CHANGES - 1);
        let c = change_set(&b, &a).unwrap();
        assert_eq!((c.modified.len(), c.removed.len()), (1, MAX_CHANGES - 1));
        let (b, a) = changed_one(MAX_CHANGES);
        assert_eq!(change_set(&b, &a), Err(Unkept::TooMany));
    }

    #[test]
    fn every_reason_not_to_keep_reads_differently() {
        let all = [
            Unkept::TypeChange("p1".into()),
            Unkept::Git("p2".into()),
            Unkept::Link {
                path: "p3".into(),
                target: "/t".into(),
            },
            Unkept::TooMany,
            Unkept::Unreadable("why".into()),
        ];
        let texts: BTreeSet<String> = all.iter().map(|u| u.to_string()).collect();
        assert_eq!(texts.len(), all.len());
        assert!(all[0].to_string().contains("p1"));
        assert!(all[1].to_string().contains("p2"));
        assert!(all[2].to_string().contains("p3") && all[2].to_string().contains("/t"));
        assert!(all[3].to_string().contains("too many"));
        assert!(all[4].to_string().contains("why"));
    }

    #[test]
    fn a_link_is_kept_only_while_it_stays_in_the_workspace() {
        let roots = vec!["/w/b".to_string(), "/private/w/b".to_string()];
        let plan = |link: &str, target: &str| link_plan(link, target, &roots);
        // relative: the shape a workspace install makes, out of the added
        // tree but inside the workspace
        assert_eq!(plan("node_modules/a", "../packages/a"), LinkPlan::Keep);
        assert_eq!(plan("node_modules/.bin/x", "../dep/bin.js"), LinkPlan::Keep);
        assert_eq!(plan("a", "b"), LinkPlan::Keep);
        assert_eq!(plan("a", "./b/../c"), LinkPlan::Keep);
        assert_eq!(plan("a", ""), LinkPlan::Keep);
        assert_eq!(plan("d/a", ".."), LinkPlan::Keep);
        assert_eq!(plan("a", ".."), LinkPlan::Refuse);
        assert_eq!(plan("d/a", "../../x"), LinkPlan::Refuse);
        assert_eq!(plan("d/a", "x/../../../y"), LinkPlan::Refuse);
        // absolute, into the builder's workspace: said relative to the link
        let rewrite = |s: &str| LinkPlan::Rewrite(s.to_string());
        assert_eq!(plan("a", "/w/b/x/y"), rewrite("x/y"));
        assert_eq!(
            plan("node_modules/.bin/x", "/w/b/node_modules/dep/bin.js"),
            rewrite("../../node_modules/dep/bin.js")
        );
        assert_eq!(plan("d/a", "/private/w/b/x"), rewrite("../x"));
        assert_eq!(plan("a", "/w/b"), rewrite("."));
        assert_eq!(plan("a", "/w/b/"), rewrite("."));
        assert_eq!(plan("d/a", "/w/b"), rewrite(".."));
        assert_eq!(plan("a", "/w/b/x/./y/../z"), rewrite("x/z"));
        // absolute, elsewhere: a path that only begins like the root too
        assert_eq!(plan("a", "/etc/hosts"), LinkPlan::Refuse);
        assert_eq!(plan("a", "/w/bb/x"), LinkPlan::Refuse);
        assert_eq!(plan("a", "/w/b/../x"), LinkPlan::Refuse);
        assert_eq!(link_plan("a", "/w/b/x", &[]), LinkPlan::Refuse);
    }

    #[test]
    fn templates_go_by_age_by_their_run_and_then_least_recently_used() {
        let k = |name: &str, bytes: u64, used: u64| Kept {
            name: name.into(),
            bytes,
            last_used_ms: used,
            run_gone: false,
            pinned: false,
        };
        let l = Limits {
            max_bytes: 100,
            max_age_ms: 1_000,
            run_idle_ms: 10,
        };
        let now = 5_000;
        let none: [&str; 0] = [];
        assert_eq!(evictions(&[], now, &l), none);
        // age: the limit itself is still kept
        let aged = [k("at", 1, 4_000), k("over", 1, 3_999), k("new", 1, 5_000)];
        assert_eq!(evictions(&aged, now, &l), ["over"]);
        // a clock that went back makes nothing old
        assert_eq!(evictions(&[k("ahead", 1, 9_000)], now, &l), none);
        // a run that is gone: after its own, shorter, time without use
        let gone = |name: &str, used: u64| Kept {
            run_gone: true,
            ..k(name, 1, used)
        };
        let runs = [
            gone("idle", 4_989),
            gone("just", 4_990),
            k("other", 1, 4_989),
        ];
        assert_eq!(evictions(&runs, now, &l), ["idle"]);
        // size: the least recently used go until the rest fits; equal times
        // go by name; a store exactly at its size keeps everything
        let full = [
            k("b", 40, 4_500),
            k("a", 40, 4_500),
            k("c", 40, 4_800),
            k("d", 20, 4_900),
        ];
        assert_eq!(evictions(&full, now, &l), ["a"]);
        let fits = [k("a", 60, 4_500), k("b", 40, 4_600)];
        assert_eq!(evictions(&fits, now, &l), none);
        let over = [k("a", 60, 4_500), k("b", 41, 4_600)];
        assert_eq!(evictions(&over, now, &l), ["a"]);
        // what an old template held does not count against the others
        let mixed = [k("old", 90, 0), k("a", 60, 4_500), k("b", 40, 4_600)];
        assert_eq!(evictions(&mixed, now, &l), ["old"]);
        // a pinned template stays, whatever its age, and still counts
        let pin = |x: Kept| Kept { pinned: true, ..x };
        let pinned = [pin(k("old", 90, 0)), k("a", 30, 4_500), k("b", 5, 4_600)];
        assert_eq!(evictions(&pinned, now, &l), ["a"]);
        let all = [pin(k("p", 200, 4_000)), pin(gone("q", 0))];
        assert_eq!(evictions(&all, now, &l), none);
    }

    #[test]
    fn a_changed_entry_replaces_only_what_the_builder_started_from() {
        assert_eq!(
            place_modified(Some("new"), "base", "new"),
            Place::AlreadyThere
        );
        assert_eq!(place_modified(Some("base"), "base", "new"), Place::Copy);
        assert_eq!(place_modified(Some("else"), "base", "new"), Place::Conflict);
        assert_eq!(place_modified(None, "base", "new"), Place::Conflict);
        // a removed entry goes only when it is what was there
        assert_eq!(place_removed(false, false, None, Some("b")), Gone::Already);
        assert_eq!(place_removed(false, true, None, None), Gone::Already);
        assert_eq!(
            place_removed(true, false, Some("b"), Some("b")),
            Gone::Remove
        );
        assert_eq!(
            place_removed(true, false, Some("c"), Some("b")),
            Gone::Conflict
        );
        assert_eq!(place_removed(true, false, None, Some("b")), Gone::Conflict);
        assert_eq!(
            place_removed(true, true, Some("b"), Some("b")),
            Gone::Conflict
        );
        assert_eq!(place_removed(true, true, None, None), Gone::Remove);
        assert_eq!(place_removed(true, false, Some("b"), None), Gone::Conflict);
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
            BuildEnd::NotKept("because".into()),
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
        assert!(BuildEnd::NotKept("because".into())
            .describe()
            .contains("because"));
        assert!(!BuildEnd::NotKept("because".into()).is_built());
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
