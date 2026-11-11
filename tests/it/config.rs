use remote_task_runner::config::legacy::{self, MAPPING, UNMAPPED};
use remote_task_runner::config::load::load;
use remote_task_runner::config::model::{
    OutputWhen, SecretSpec, ServiceMode, SourceMode, Strategy, ToolchainMatch, Unsupported,
};
use remote_task_runner::config::parse::{parse_project, parse_user};
use remote_task_runner::config::paths::{
    legacy_config_path, project_config_path, user_config_path,
};
use remote_task_runner::config::resolve::{
    resolve, Inputs, Loaded, Origin, Overlay, ResolvedConfig, Value,
};
use remote_task_runner::config::ConfigError;
use std::collections::BTreeMap;
use std::path::Path;

const USER_EXAMPLE: &str = r#"
schema_version = 1

[coordinator]
local_worker = true
local_worker_priority = -1
heartbeat_seconds = 5
lease_seconds = 60

[scheduler]
strategy = "earliest_finish"
forecast_ttl_ms = 2000
prefetch_per_worker = 1
priority_step_seconds = 60

[[workers]]
id = "mini-a"
ssh = "ci@mini-a.local"
service_mode = "user"
priority = 10
labels = ["macos", "arm64", "node"]

[[workers]]
id = "mini-b"
ssh = "ci@mini-b.local"
service_mode = "system-user"
labels = ["macos", "x86_64", "node", "docker"]

[[workers]]
id = "colab-1"
ssh = "colab-1"          # alias of an ssh config entry
ephemeral = true
labels = ["linux", "x86_64", "node"]
"#;

const PROJECT_EXAMPLE: &str = r#"
schema_version = 1

[source]
mode = "worktree"
include = []
exclude = ["remote-task-runner-results/**"]
secret_files = [".env.test", "packages/*/.env.test"]

[execution]
profile = "web-dev"
requires = ["macos", "node"]
pass_env = ["CI"]
timeout_seconds = 3600

[toolchain]
match = "exact"

[ci]
workflows_dirs = [".github/workflows"]
status_context = "local-ci"
unsupported = "error"

[[outputs]]
name = "test-results"
paths = ["test-results/**", "coverage/**"]
required = false
when = "always"
"#;

fn user(text: &str) -> Loaded<remote_task_runner::config::model::UserConfig> {
    Loaded {
        path: "/u/config.toml".into(),
        value: parse_user("/u/config.toml", text).unwrap(),
    }
}

fn project(text: &str) -> Loaded<remote_task_runner::config::model::ProjectConfig> {
    Loaded {
        path: "/r/.remote-task-runner.toml".into(),
        value: parse_project("/r/.remote-task-runner.toml", text).unwrap(),
    }
}

fn fallback(text: &str) -> legacy::Fallback {
    legacy::from_text("/r/.local-ci.toml", text).unwrap()
}

fn run(
    u: Option<&str>,
    p: Option<&str>,
    l: Option<&str>,
    overlay: &Overlay,
) -> Result<ResolvedConfig, ConfigError> {
    let u = u.map(user);
    let p = p.map(project);
    let l = l.map(fallback);
    resolve(&Inputs {
        overlay,
        project: p.as_ref(),
        legacy: l.as_ref(),
        user: u.as_ref(),
    })
}

fn problems(r: Result<ResolvedConfig, ConfigError>) -> Vec<String> {
    match r.unwrap_err() {
        ConfigError::Invalid { problems } => problems,
        e => panic!("not Invalid: {e}"),
    }
}

fn v<T>(value: T, origin: Origin) -> Value<T> {
    Value { value, origin }
}

fn s(x: &[&str]) -> Vec<String> {
    x.iter().map(|x| x.to_string()).collect()
}

// ---- the two spec examples -------------------------------------------------

#[test]
fn spec_examples_resolve() {
    let r = run(
        Some(USER_EXAMPLE),
        Some(PROJECT_EXAMPLE),
        None,
        &Overlay::default(),
    )
    .unwrap();
    assert_eq!(r.workers.len(), 3);
    assert_eq!(r.workers[0].id, "mini-a");
    assert_eq!(r.workers[0].service_mode, Some(ServiceMode::User));
    assert_eq!(r.workers[1].service_mode, Some(ServiceMode::SystemUser));
    assert!(!r.workers[1].ephemeral);
    assert_eq!(r.workers[2].service_mode, None);
    assert!(r.workers[2].ephemeral);
    assert_eq!(
        r.workers[1].labels,
        s(&["macos", "x86_64", "node", "docker"])
    );
    assert_eq!(r.workers[1].ssh, "ci@mini-b.local");
    assert_eq!(r.strategy.value, Strategy::EarliestFinish);
    assert_eq!(r.source_mode.value, SourceMode::Worktree);
    assert_eq!(r.exclude.value, s(&["remote-task-runner-results/**"]));
    assert_eq!(
        r.secret_files.value,
        vec![
            SecretSpec {
                glob: ".env.test".into(),
                required: false
            },
            SecretSpec {
                glob: "packages/*/.env.test".into(),
                required: false
            }
        ]
    );
    assert_eq!(r.profile.value.as_deref(), Some("web-dev"));
    assert_eq!(r.requires.value, s(&["macos", "node"]));
    assert_eq!(r.pass_env.value, s(&["CI"]));
    assert_eq!(r.timeout_seconds.value, Some(3600));
    assert_eq!(r.toolchain_match.value, ToolchainMatch::Exact);
    assert_eq!(r.status_context.value, "local-ci");
    assert_eq!(r.unsupported.value, Unsupported::Error);
    assert_eq!(r.outputs.len(), 1);
    assert_eq!(r.outputs[0].name, "test-results");
    assert_eq!(r.outputs[0].when, OutputWhen::Always);
    assert!(!r.outputs[0].required);
    assert!(r.prepare_commands.is_empty());
    // explicit file values keep their file as the origin
    assert_eq!(
        r.heartbeat_seconds,
        v(5, Origin::User("/u/config.toml".into()))
    );
    assert_eq!(
        r.timeout_seconds.origin,
        Origin::Project("/r/.remote-task-runner.toml".into())
    );
}

#[test]
fn defaults_apply_without_any_file() {
    let r = run(None, None, None, &Overlay::default()).unwrap();
    let d = Origin::Default;
    assert_eq!(r.local_worker, v(true, d.clone()));
    assert_eq!(r.heartbeat_seconds, v(5, d.clone()));
    assert_eq!(r.stale_seconds, v(15, d.clone()));
    assert_eq!(r.lease_seconds, v(60, d.clone()));
    assert_eq!(r.term_grace_seconds, v(10, d.clone()));
    assert_eq!(r.max_active_task, v(2, d.clone()));
    assert_eq!(r.log_spool_limit_bytes, v(1 << 30, d.clone()));
    assert_eq!(r.retention_days, v(7, d.clone()));
    assert_eq!(r.strategy, v(Strategy::EarliestFinish, d.clone()));
    assert_eq!(r.forecast_ttl_ms, v(2000, d.clone()));
    assert_eq!(r.prefetch_per_worker, v(1, d.clone()));
    assert_eq!(r.source_mode, v(SourceMode::Worktree, d.clone()));
    assert_eq!(r.include, v(vec![], d.clone()));
    assert_eq!(r.exclude, v(vec![], d.clone()));
    assert_eq!(r.secret_files, v(vec![], d.clone()));
    assert_eq!(r.profile, v(None, d.clone()));
    assert_eq!(r.requires, v(vec![], d.clone()));
    assert_eq!(r.pass_env, v(vec![], d.clone()));
    assert_eq!(r.timeout_seconds, v(None, d.clone()));
    assert_eq!(r.toolchain_match, v(ToolchainMatch::Exact, d.clone()));
    assert_eq!(r.workflows_dirs, v(s(&[".github/workflows"]), d.clone()));
    assert_eq!(r.status_context, v("local-ci".to_string(), d.clone()));
    assert_eq!(r.unsupported, v(Unsupported::Error, d));
    assert!(r.workers.is_empty() && r.outputs.is_empty());
}

// ---- precedence and origins ------------------------------------------------

#[test]
fn precedence_cli_over_project_over_legacy_over_user_over_default() {
    let user_t = r#"
schema_version = 1
[coordinator]
heartbeat_seconds = 2
lease_seconds = 100
[defaults]
timeout_seconds = 10
toolchain_match = "any"
"#;
    let project_t = r#"
schema_version = 1
[execution]
timeout_seconds = 20
[toolchain]
match = "major"
[ci]
workflows_dirs = ["ci/wf"]
"#;
    let legacy_t = "workflows-dirs = [\"legacy/wf\"]\n";

    // user over default; project over user
    let r = run(Some(user_t), Some(project_t), None, &Overlay::default()).unwrap();
    assert_eq!(r.heartbeat_seconds.value, 2);
    assert_eq!(
        r.heartbeat_seconds.origin,
        Origin::User("/u/config.toml".into())
    );
    assert_eq!(r.timeout_seconds.value, Some(20));
    assert!(matches!(r.timeout_seconds.origin, Origin::Project(_)));
    assert_eq!(r.toolchain_match.value, ToolchainMatch::Major);
    assert_eq!(r.stale_seconds.origin, Origin::Default);

    // CLI over project and user
    let o = Overlay {
        heartbeat_seconds: Some(3),
        lease_seconds: Some(90),
        timeout_seconds: Some(30),
        toolchain_match: Some(ToolchainMatch::Minor),
        source_mode: Some(SourceMode::Commit),
        profile: Some("cli-prof".into()),
        requires: Some(s(&["linux"])),
    };
    let r = run(Some(user_t), Some(project_t), None, &o).unwrap();
    assert_eq!(r.heartbeat_seconds, v(3, Origin::Cli));
    assert_eq!(r.lease_seconds, v(90, Origin::Cli));
    assert_eq!(r.timeout_seconds, v(Some(30), Origin::Cli));
    assert_eq!(r.toolchain_match, v(ToolchainMatch::Minor, Origin::Cli));
    assert_eq!(r.source_mode, v(SourceMode::Commit, Origin::Cli));
    assert_eq!(r.profile, v(Some("cli-prof".to_string()), Origin::Cli));
    assert_eq!(r.requires, v(s(&["linux"]), Origin::Cli));

    // user-only defaults reach through when the project is silent
    let r = run(Some(user_t), None, None, &Overlay::default()).unwrap();
    assert_eq!(r.timeout_seconds.value, Some(10));
    assert_eq!(r.toolchain_match.value, ToolchainMatch::Any);

    // legacy beats default, project beats legacy when they agree on nothing
    let r = run(None, None, Some(legacy_t), &Overlay::default()).unwrap();
    assert_eq!(r.workflows_dirs.value, s(&["legacy/wf"]));
    assert_eq!(
        r.workflows_dirs.origin,
        Origin::LegacyLocalCi("/r/.local-ci.toml".into())
    );
}

#[test]
fn entries_list_every_value_with_its_origin() {
    let r = run(
        Some(USER_EXAMPLE),
        Some(PROJECT_EXAMPLE),
        None,
        &Overlay::default(),
    )
    .unwrap();
    let e = r.entries();
    let get = |k: &str| e.iter().find(|x| x.key == k).unwrap().clone();
    assert_eq!(get("coordinator.heartbeat_seconds").value, "5");
    assert_eq!(
        get("coordinator.heartbeat_seconds").origin,
        Origin::User("/u/config.toml".into())
    );
    assert_eq!(get("coordinator.stale_seconds").origin, Origin::Default);
    assert_eq!(get("scheduler.strategy").value, "earliest_finish");
    assert_eq!(get("source.mode").value, "worktree");
    assert_eq!(
        get("source.exclude").value,
        "[remote-task-runner-results/**]"
    );
    assert_eq!(
        get("source.secret_files").value,
        "[.env.test, packages/*/.env.test]"
    );
    assert_eq!(get("execution.profile").value, "web-dev");
    assert_eq!(get("execution.timeout_seconds").value, "3600");
    assert_eq!(get("toolchain.match").value, "exact");
    assert_eq!(get("ci.unsupported").value, "error");
    assert_eq!(e.len(), 26);
    assert_eq!(get("ci.no_cache").value, "[]");
    let none = run(None, None, None, &Overlay::default())
        .unwrap()
        .entries();
    let get2 = |k: &str| none.iter().find(|x| x.key == k).unwrap().value.clone();
    assert_eq!(get2("execution.profile"), "(none)");
    assert_eq!(get2("execution.timeout_seconds"), "(none)");
    assert_eq!(get2("coordinator.local_worker"), "true");
    assert_eq!(get2("coordinator.term_grace_seconds"), "10");
    assert_eq!(get2("coordinator.max_active_task"), "2");
    assert_eq!(get2("coordinator.log_spool_limit_bytes"), "1073741824");
    assert_eq!(get2("coordinator.retention_days"), "7");
    assert_eq!(get2("scheduler.forecast_ttl_ms"), "2000");
    assert_eq!(get2("scheduler.prefetch_per_worker"), "1");
    assert_eq!(get2("ci.workflows_dirs"), "[.github/workflows]");
    assert_eq!(get2("ci.status_context"), "local-ci");
    assert_eq!(get2("execution.requires"), "[]");
    assert_eq!(get2("source.include"), "[]");

    let t = |m: &str, ms: ToolchainMatch| {
        let p = format!("schema_version = 1\n[toolchain]\nmatch = \"{m}\"\n");
        let r = run(None, Some(&p), None, &Overlay::default()).unwrap();
        assert_eq!(r.toolchain_match.value, ms);
        let en = r.entries();
        en.iter()
            .find(|x| x.key == "toolchain.match")
            .unwrap()
            .value
            .clone()
    };
    assert_eq!(t("minor", ToolchainMatch::Minor), "minor");
    assert_eq!(t("major", ToolchainMatch::Major), "major");
    assert_eq!(t("any", ToolchainMatch::Any), "any");
    let p = "schema_version = 1\n[source]\nmode = \"commit\"\n[ci]\nunsupported = \"warn\"\n";
    let en = run(None, Some(p), None, &Overlay::default())
        .unwrap()
        .entries();
    let val = |k: &str| en.iter().find(|x| x.key == k).unwrap().value.clone();
    assert_eq!(val("source.mode"), "commit");
    assert_eq!(val("ci.unsupported"), "warn");
}

#[test]
fn origin_display_names_the_source() {
    assert_eq!(Origin::Cli.to_string(), "command-line argument");
    assert_eq!(Origin::Default.to_string(), "built-in default");
    assert_eq!(Origin::User("/a".into()).to_string(), "user config /a");
    assert_eq!(
        Origin::Project("/b".into()).to_string(),
        "project config /b"
    );
    assert_eq!(Origin::LegacyLocalCi("/c".into()).to_string(), "legacy /c");
}

// ---- contradiction checks --------------------------------------------------

fn user_with(c: &str) -> String {
    format!("schema_version = 1\n[coordinator]\n{c}\n")
}

#[test]
fn heartbeat_not_below_lease_is_an_error() {
    let p = problems(run(
        Some(&user_with(
            "heartbeat_seconds = 60\nstale_seconds = 1000\nlease_seconds = 60",
        )),
        None,
        None,
        &Overlay::default(),
    ));
    assert!(
        p.iter().any(|m| m.contains(
            "heartbeat_seconds = 60 (user config /u/config.toml) must be less than lease_seconds = 60"
        )),
        "{p:?}"
    );
}

#[test]
fn stale_must_cover_two_heartbeats_and_stay_below_lease() {
    let p = problems(run(
        Some(&user_with("heartbeat_seconds = 5\nstale_seconds = 9")),
        None,
        None,
        &Overlay::default(),
    ));
    assert_eq!(p.len(), 1, "{p:?}");
    assert!(p[0].contains("stale_seconds = 9") && p[0].contains("at least twice"));
    // exactly twice is fine
    run(
        Some(&user_with("heartbeat_seconds = 5\nstale_seconds = 10")),
        None,
        None,
        &Overlay::default(),
    )
    .unwrap();

    let p = problems(run(
        Some(&user_with("stale_seconds = 60")),
        None,
        None,
        &Overlay::default(),
    ));
    assert_eq!(p.len(), 1, "{p:?}");
    assert!(
        p[0].contains("stale_seconds = 60")
            && p[0].contains("lease_seconds = 60 (built-in default)")
    );
    run(
        Some(&user_with("stale_seconds = 59")),
        None,
        None,
        &Overlay::default(),
    )
    .unwrap();
}

#[test]
fn contradiction_reports_the_cli_origin() {
    let o = Overlay {
        heartbeat_seconds: Some(120),
        ..Overlay::default()
    };
    let p = problems(run(None, None, None, &o));
    assert!(p
        .iter()
        .any(|m| m.contains("heartbeat_seconds = 120 (command-line argument)")));
}

#[test]
fn zero_values_are_errors() {
    let cases = [
        "heartbeat_seconds = 0",
        "stale_seconds = 0",
        "lease_seconds = 0",
        "term_grace_seconds = 0",
        "log_spool_limit_bytes = 0",
        "max_active_task = 0",
        "retention_days = 0",
    ];
    for c in cases {
        let p = problems(run(Some(&user_with(c)), None, None, &Overlay::default()));
        let name = c.split(' ').next().unwrap();
        assert!(
            p.iter()
                .any(|m| m.starts_with(&format!("{name} = 0")) && m.contains("greater than 0")),
            "{c}: {p:?}"
        );
    }
    let p = problems(run(
        Some("schema_version = 1\n[scheduler]\nforecast_ttl_ms = 0\n"),
        None,
        None,
        &Overlay::default(),
    ));
    assert!(p.iter().any(|m| m.starts_with("forecast_ttl_ms = 0")));
    let p = problems(run(
        None,
        Some("schema_version = 1\n[execution]\ntimeout_seconds = 0\n"),
        None,
        &Overlay::default(),
    ));
    assert!(p.iter().any(|m| m.starts_with("timeout_seconds = 0")));
    // prefetch_per_worker = 0 is allowed
    run(
        Some("schema_version = 1\n[scheduler]\nprefetch_per_worker = 0\n"),
        None,
        None,
        &Overlay::default(),
    )
    .unwrap();
}

#[test]
fn worker_problems() {
    let t = r#"
schema_version = 1
[[workers]]
id = "a b"
ssh = " "
labels = ["ok", ""]
[[workers]]
id = "dup"
ssh = "h"
[[workers]]
id = "dup"
ssh = "h"
ephemeral = true
service_mode = "user"
[[workers]]
id = ""
ssh = "h"
"#;
    let p = problems(run(Some(t), None, None, &Overlay::default()));
    let has = |needle: &str| p.iter().any(|m| m.contains(needle));
    assert!(has("worker id \"a b\""), "{p:?}");
    assert!(has("worker \"a b\": ssh is empty"));
    assert!(has("worker \"a b\": a label is empty"));
    assert!(has("worker id \"dup\" is listed twice"));
    assert!(has(
        "worker \"dup\": an ephemeral worker installs no service"
    ));
    assert!(has("worker id \"\""));
    // an id with every allowed character class passes
    let ok = "schema_version = 1\n[[workers]]\nid = \"A-b_c.9\"\nssh = \"h\"\n";
    let r = run(Some(ok), None, None, &Overlay::default()).unwrap();
    assert_eq!(r.workers[0].service_mode, Some(ServiceMode::User));
    assert!(r.workers[0].labels.is_empty());
}

#[test]
fn project_problems() {
    let t = r#"
schema_version = 1
[source]
secret_files = [" "]
[ci]
status_context = " "
[[outputs]]
name = "a"
paths = []
[[outputs]]
name = "a"
paths = ["x", " "]
[[outputs]]
name = ""
paths = ["x"]
[prepare]
commands = [["pnpm", "install"], [], [" "]]
"#;
    let p = problems(run(None, Some(t), None, &Overlay::default()));
    let has = |needle: &str| p.iter().any(|m| m.contains(needle));
    assert!(has("[source] secret_files: a glob is empty"), "{p:?}");
    assert!(has("[ci] status_context is empty"));
    assert!(has("[[outputs]] \"a\": paths needs at least one"));
    assert!(has("[[outputs]] name \"a\" is listed twice"));
    assert!(has("[[outputs]]: name is empty"));
    assert!(has("[prepare] commands: [] has no program"));
    assert!(has("[prepare] commands: [\" \"] has no program"));
    assert!(!has("[\"pnpm\", \"install\"]"));
}

#[test]
fn outputs_defaults_and_prepare() {
    let t = r#"
schema_version = 1
[[outputs]]
name = "r"
paths = ["out/**"]
[[outputs]]
name = "f"
paths = ["f"]
required = true
when = "failure"
[prepare]
commands = [["corepack", "pnpm", "install", "--frozen-lockfile"]]
"#;
    let r = run(None, Some(t), None, &Overlay::default()).unwrap();
    assert!(!r.outputs[0].required);
    assert_eq!(r.outputs[0].when, OutputWhen::Always);
    assert!(r.outputs[1].required);
    assert_eq!(r.outputs[1].when, OutputWhen::Failure);
    assert_eq!(r.prepare_commands.len(), 1);
}

#[test]
fn project_workflows_dirs_are_validated_and_normalised() {
    let bad = |d: &str| {
        problems(run(
            None,
            Some(&format!(
                "schema_version = 1\n[ci]\nworkflows_dirs = [{d}]\n"
            )),
            None,
            &Overlay::default(),
        ))
    };
    assert!(bad("")
        .iter()
        .any(|m| m.contains("workflows_dirs is empty")));
    for d in ["\"/abs\"", "\"../up\"", "\"a/../b\"", "\".\"", "\"\""] {
        let p = bad(d);
        assert!(
            p.iter().any(|m| m.contains("below the repository root")),
            "{d}: {p:?}"
        );
    }
    let r = run(
        None,
        Some("schema_version = 1\n[ci]\nworkflows_dirs = [\"./a/\", \"a\", \"b\"]\n"),
        None,
        &Overlay::default(),
    )
    .unwrap();
    assert_eq!(r.workflows_dirs.value, s(&["a", "b"]));
}

// ---- unknown keys and parse errors -----------------------------------------

#[test]
fn unknown_keys_are_errors_with_position_and_name() {
    let cases = [
        (
            "project",
            "schema_version = 1\n\n[source]\nmodee = \"worktree\"\n",
            "modee",
            "line 4",
        ),
        (
            "project",
            "schema_version = 1\nbogus = 1\n",
            "bogus",
            "line 2",
        ),
        (
            "project",
            "schema_version = 1\n[[outputs]]\nname = \"a\"\npaths = []\nwhne = \"x\"\n",
            "whne",
            "line 5",
        ),
        (
            "user",
            "schema_version = 1\n\n[coordinator]\nheartbeat_secs = 5\n",
            "heartbeat_secs",
            "line 4",
        ),
        (
            "user",
            "schema_version = 1\n[[workers]]\nid = \"a\"\nssh = \"h\"\nlabel = []\n",
            "label",
            "line 5",
        ),
        (
            "user",
            "schema_version = 1\n[scheduler]\nstrategy = \"nope\"\n",
            "nope",
            "line 3",
        ),
    ];
    for (kind, text, key, line) in cases {
        let e = if kind == "user" {
            parse_user("cfg.toml", text).unwrap_err()
        } else {
            parse_project("cfg.toml", text).unwrap_err()
        };
        let m = e.to_string();
        assert!(m.starts_with("cfg.toml: "), "{m}");
        assert!(m.contains(key), "{m}");
        assert!(m.contains(line), "{m}");
    }
}

#[test]
fn schema_version_is_required_and_checked() {
    assert!(parse_user("u", "").is_err());
    assert!(parse_project("p", "[ci]\n").is_err());
    let e = parse_user("u.toml", "schema_version = 2\n").unwrap_err();
    assert!(e.to_string().contains("schema_version 2 is not supported"));
    let e = parse_project("p.toml", "schema_version = 0\n").unwrap_err();
    assert!(e.to_string().contains("p.toml"));
    parse_user("u", "schema_version = 1\n").unwrap();
    parse_project("p", "schema_version = 1\n").unwrap();
}

#[test]
fn secret_files_accept_both_forms() {
    let p = project(
        "schema_version = 1\n[source]\nsecret_files = [\".env\", { glob = \"a/.env\", required = true }, { glob = \"b\" }]\n",
    );
    let r = resolve(&Inputs {
        overlay: &Overlay::default(),
        project: Some(&p),
        legacy: None,
        user: None,
    })
    .unwrap();
    let got: Vec<(String, bool)> = r
        .secret_files
        .value
        .iter()
        .map(|s| (s.glob.clone(), s.required))
        .collect();
    assert_eq!(
        got,
        vec![
            (".env".to_string(), false),
            ("a/.env".to_string(), true),
            ("b".to_string(), false)
        ]
    );
    assert_eq!(
        legacy::secret_label(&r.secret_files.value[1]),
        "a/.env (required)"
    );
    assert_eq!(legacy::secret_label(&r.secret_files.value[0]), ".env");
}

// ---- .local-ci.toml fallback -----------------------------------------------

const LEGACY_FULL: &str = r#"
workflows-dirs = ["ci/workflows"]
no-cache = ["ci.yml"]
link-node-modules = true

[setup]
ignore-env = ["CI"]

[source]
include = ["gen/**"]
exclude = ["tmp/**"]
secret_files = [".env", { glob = "x/.env", required = true }]
"#;

#[test]
fn legacy_mapping_table_covers_the_shared_keys() {
    let names: Vec<&str> = MAPPING.iter().map(|m| m.local_ci).collect();
    assert_eq!(
        names,
        [
            "workflows-dirs",
            "[source] include",
            "[source] exclude",
            "[source] secret_files",
            "no-cache"
        ]
    );
    assert_eq!(MAPPING[4].rtr, "[ci] no_cache");
    assert_eq!(MAPPING[0].rtr, "[ci] workflows_dirs");
    assert_eq!(UNMAPPED, ["link-node-modules", "[setup] ignore-env"]);
}

#[test]
fn legacy_values_fill_in_when_the_project_is_silent() {
    let r = run(
        None,
        Some("schema_version = 1\n"),
        Some(LEGACY_FULL),
        &Overlay::default(),
    )
    .unwrap();
    let origin = Origin::LegacyLocalCi("/r/.local-ci.toml".into());
    assert_eq!(r.workflows_dirs, v(s(&["ci/workflows"]), origin.clone()));
    assert_eq!(r.include, v(s(&["gen/**"]), origin.clone()));
    assert_eq!(r.exclude, v(s(&["tmp/**"]), origin.clone()));
    assert_eq!(r.secret_files.origin, origin);
    assert_eq!(r.secret_files.value.len(), 2);
    assert!(r.secret_files.value[1].required);
    assert_eq!(r.no_cache, v(s(&["ci.yml"]), origin.clone()));
    // other keys are untouched by the legacy file
    assert_eq!(r.status_context.origin, Origin::Default);
}

#[test]
fn legacy_only_without_a_project_file() {
    let f = legacy::from_text("/x/.local-ci.toml", "").unwrap();
    assert_eq!(
        f,
        legacy::Fallback {
            path: "/x/.local-ci.toml".into(),
            ..Default::default()
        }
    );
    let f = legacy::from_text("/x", "[source]\ninclude = []\n").unwrap();
    assert_eq!(f.include, None);
}

#[test]
fn legacy_agreeing_values_are_fine_and_project_wins_the_origin() {
    let project_t = "schema_version = 1\n[ci]\nworkflows_dirs = [\"./ci/workflows/\"]\n[source]\nexclude = [\"tmp/**\"]\n";
    let r = run(
        None,
        Some(project_t),
        Some(LEGACY_FULL),
        &Overlay::default(),
    )
    .unwrap();
    assert!(matches!(r.workflows_dirs.origin, Origin::Project(_)));
    assert_eq!(r.workflows_dirs.value, s(&["ci/workflows"]));
    assert!(matches!(r.exclude.origin, Origin::Project(_)));
    // keys only the legacy file sets still come through
    assert!(matches!(r.include.origin, Origin::LegacyLocalCi(_)));
}

#[test]
fn legacy_conflicts_name_both_files() {
    let project_t = r#"
schema_version = 1
[ci]
workflows_dirs = ["other"]
[source]
include = ["a"]
exclude = ["b"]
secret_files = [".env"]
"#;
    let p = problems(run(
        None,
        Some(project_t),
        Some(LEGACY_FULL),
        &Overlay::default(),
    ));
    assert_eq!(p.len(), 4, "{p:?}");
    let has = |needle: &str| p.iter().any(|m| m.contains(needle));
    assert!(has("/r/.remote-task-runner.toml [ci] workflows_dirs = [\"other\"] conflicts with /r/.local-ci.toml workflows-dirs = [\"ci/workflows\"]"), "{p:?}");
    assert!(has("[source] include = [\"a\"] conflicts with /r/.local-ci.toml [source] include = [\"gen/**\"]"));
    assert!(has("[source] exclude = [\"b\"]"));
    assert!(has("[source] secret_files = [\".env\"] conflicts with /r/.local-ci.toml [source] secret_files = [\".env\", \"x/.env (required)\"]"));
    // the required flag is part of the value
    let t = "schema_version = 1\n[source]\nsecret_files = [{ glob = \".env\", required = true }]\n";
    let l = "[source]\nsecret_files = [\".env\"]\n";
    assert_eq!(
        problems(run(None, Some(t), Some(l), &Overlay::default())).len(),
        1
    );
    // an empty legacy list is "not set", never a conflict
    run(
        None,
        Some(t),
        Some("[source]\nsecret_files = []\n"),
        &Overlay::default(),
    )
    .unwrap();
}

#[test]
fn legacy_parse_errors_use_local_ci_rules() {
    let e = legacy::from_text("/r/.local-ci.toml", "wokflows-dirs = []\n").unwrap_err();
    let m = e.to_string();
    assert!(m.starts_with("/r/.local-ci.toml: "), "{m}");
    assert!(m.contains("wokflows-dirs"), "{m}");
    assert!(legacy::from_text("x", "workflows-dirs = [\"/abs\"]\n").is_err());
}

// ---- paths -----------------------------------------------------------------

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn user_config_path_follows_xdg() {
    let p = user_config_path(&env(&[("XDG_CONFIG_HOME", "/x/cfg"), ("HOME", "/h")])).unwrap();
    assert_eq!(p, Path::new("/x/cfg/remote-task-runner/config.toml"));
    let p = user_config_path(&env(&[("HOME", "/h")])).unwrap();
    assert_eq!(p, Path::new("/h/.config/remote-task-runner/config.toml"));
    // relative or empty XDG_CONFIG_HOME is ignored
    for bad in ["", "rel/dir"] {
        let p = user_config_path(&env(&[("XDG_CONFIG_HOME", bad), ("HOME", "/h")])).unwrap();
        assert_eq!(p, Path::new("/h/.config/remote-task-runner/config.toml"));
    }
    assert_eq!(
        user_config_path(&env(&[])).unwrap_err(),
        ConfigError::NoConfigHome
    );
    assert_eq!(
        user_config_path(&env(&[("HOME", "")])).unwrap_err(),
        ConfigError::NoConfigHome
    );
}

#[test]
fn project_and_legacy_paths_sit_at_the_repo_root() {
    let root = Path::new("/repo");
    assert_eq!(
        project_config_path(root),
        Path::new("/repo/.remote-task-runner.toml")
    );
    assert_eq!(legacy_config_path(root), Path::new("/repo/.local-ci.toml"));
}

// ---- load from disk --------------------------------------------------------

#[test]
fn load_reads_the_three_files() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let xdg = home.path().join("xdg");
    std::fs::create_dir_all(xdg.join("remote-task-runner")).unwrap();
    std::fs::write(xdg.join("remote-task-runner/config.toml"), USER_EXAMPLE).unwrap();
    std::fs::write(
        repo.path().join(".remote-task-runner.toml"),
        PROJECT_EXAMPLE,
    )
    .unwrap();
    std::fs::write(
        repo.path().join(".local-ci.toml"),
        "no-cache = [\"ci.yml\"]\n",
    )
    .unwrap();
    let e = env(&[("XDG_CONFIG_HOME", xdg.to_str().unwrap())]);
    let r = load(&e, Some(repo.path()), &Overlay::default()).unwrap();
    assert_eq!(r.workers.len(), 3);
    assert_eq!(r.profile.value.as_deref(), Some("web-dev"));
    assert!(
        matches!(&r.heartbeat_seconds.origin, Origin::User(p) if p.ends_with("remote-task-runner/config.toml"))
    );

    // outside a repo only the user file is read; missing files are fine
    let r = load(&e, None, &Overlay::default()).unwrap();
    assert_eq!(r.profile.value, None);
    let e2 = env(&[("HOME", home.path().join("empty").to_str().unwrap())]);
    let r = load(&e2, Some(repo.path()), &Overlay::default()).unwrap();
    assert!(r.workers.is_empty());
    assert_eq!(r.timeout_seconds.value, Some(3600));
}

#[test]
fn load_reports_parse_io_and_missing_home_errors() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let e = env(&[("HOME", home.path().to_str().unwrap())]);
    assert_eq!(
        load(&env(&[]), None, &Overlay::default()).unwrap_err(),
        ConfigError::NoConfigHome
    );
    std::fs::write(
        repo.path().join(".remote-task-runner.toml"),
        "schema_version = 1\nzzz = 1\n",
    )
    .unwrap();
    let err = load(&e, Some(repo.path()), &Overlay::default()).unwrap_err();
    assert!(
        err.to_string().contains("zzz") && err.to_string().contains(".remote-task-runner.toml")
    );

    std::fs::write(
        repo.path().join(".remote-task-runner.toml"),
        "schema_version = 1\n",
    )
    .unwrap();
    std::fs::write(repo.path().join(".local-ci.toml"), "bogus = 1\n").unwrap();
    let err = load(&e, Some(repo.path()), &Overlay::default()).unwrap_err();
    assert!(err.to_string().contains(".local-ci.toml"));

    // a directory where the file should be: not NotFound
    std::fs::remove_file(repo.path().join(".local-ci.toml")).unwrap();
    std::fs::create_dir(repo.path().join(".local-ci.toml")).unwrap();
    let err = load(&e, Some(repo.path()), &Overlay::default()).unwrap_err();
    assert!(matches!(err, ConfigError::Io { .. }), "{err}");

    // a bad user file
    let cfg = home.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::write(cfg.join("config.toml"), "schema_version = 1\nnope = 1\n").unwrap();
    let err = load(&e, None, &Overlay::default()).unwrap_err();
    assert!(err.to_string().contains("nope"));
}

#[test]
fn invalid_error_lists_every_problem() {
    let e = ConfigError::Invalid {
        problems: vec!["a".into(), "b".into()],
    };
    assert_eq!(e.to_string(), "invalid configuration:\n  - a\n  - b");
}

// ---- review fixes ----------------------------------------------------------

#[test]
fn secret_files_typo_inside_a_table_names_key_and_line() {
    let t =
        "schema_version = 1\n[source]\nsecret_files = [{ glob = \".env.test\", requird = true }]\n";
    let m = parse_project("cfg.toml", t).unwrap_err().to_string();
    assert!(m.starts_with("cfg.toml: "), "{m}");
    assert!(m.contains("requird"), "{m}");
    assert!(m.contains("line 3"), "{m}");
    // a wrong type is still an error, and the message says what is allowed
    let m = parse_project("c", "schema_version = 1\n[source]\nsecret_files = [1]\n")
        .unwrap_err()
        .to_string();
    assert!(m.contains("glob string or a table"), "{m}");
    // a table without glob is an error
    assert!(parse_project(
        "c",
        "schema_version = 1\n[source]\nsecret_files = [{ required = true }]\n"
    )
    .is_err());
}

#[test]
fn no_cache_in_the_project_file_and_its_origin() {
    let p = "schema_version = 1\n[ci]\nno_cache = [\"ci.yml\", \"e2e.yml:browser\"]\n";
    let r = run(None, Some(p), None, &Overlay::default()).unwrap();
    assert_eq!(
        r.no_cache,
        v(
            s(&["ci.yml", "e2e.yml:browser"]),
            Origin::Project("/r/.remote-task-runner.toml".into())
        )
    );
    let e = r.entries();
    assert_eq!(
        e.iter().find(|x| x.key == "ci.no_cache").unwrap().value,
        "[ci.yml, e2e.yml:browser]"
    );
    let p = "schema_version = 1\n[ci]\nno_cache = [\"ci.yml\", \" \"]\n";
    let m = problems(run(None, Some(p), None, &Overlay::default()));
    assert_eq!(m.len(), 1, "{m:?}");
    assert!(
        m[0].contains("[ci] no_cache: give a workflow file"),
        "{m:?}"
    );
}

#[test]
fn legacy_no_cache_maps_and_conflicts() {
    let l = "no-cache = [\"ci.yml\"]\n";
    let r = run(None, None, Some(l), &Overlay::default()).unwrap();
    assert_eq!(
        r.no_cache,
        v(
            s(&["ci.yml"]),
            Origin::LegacyLocalCi("/r/.local-ci.toml".into())
        )
    );
    let p = "schema_version = 1\n[ci]\nno_cache = [\"other.yml\"]\n";
    let m = problems(run(None, Some(p), Some(l), &Overlay::default()));
    assert_eq!(m.len(), 1, "{m:?}");
    assert!(m[0].contains("[ci] no_cache = [\"other.yml\"] conflicts with /r/.local-ci.toml no-cache = [\"ci.yml\"]"), "{m:?}");
    // an empty legacy list is "not set"
    let r = run(None, Some(p), Some("no-cache = []\n"), &Overlay::default()).unwrap();
    assert_eq!(r.no_cache.value, s(&["other.yml"]));
}

#[test]
fn list_conflicts_compare_as_sets() {
    let l = r#"
workflows-dirs = ["a", "b"]
no-cache = ["x.yml", "y.yml:job"]
[source]
include = ["i1", "i2"]
exclude = ["e1", "e2"]
secret_files = [".env", { glob = "x/.env", required = true }]
"#;
    let p = r#"
schema_version = 1
[ci]
workflows_dirs = ["b", "a"]
no_cache = ["y.yml:job", "x.yml", "x.yml"]
[source]
include = ["i2", "i1"]
exclude = ["e2", "e1"]
secret_files = [{ glob = "x/.env", required = true }, ".env"]
"#;
    let r = run(None, Some(p), Some(l), &Overlay::default()).unwrap();
    // the project's own order is kept, and it wins the origin
    assert_eq!(r.include.value, s(&["i2", "i1"]));
    assert!(matches!(r.include.origin, Origin::Project(_)));
    assert_eq!(r.no_cache.value, s(&["y.yml:job", "x.yml", "x.yml"]));
    // a genuinely different set still conflicts, per key
    let p2 = p.replace("\"e2\", \"e1\"", "\"e2\", \"e3\"");
    let m = problems(run(None, Some(&p2), Some(l), &Overlay::default()));
    assert_eq!(m.len(), 1, "{m:?}");
    assert!(m[0].contains("[source] exclude"), "{m:?}");
    let p3 = p.replace("\"y.yml:job\", \"x.yml\", \"x.yml\"", "\"y.yml:job\"");
    let m = problems(run(None, Some(&p3), Some(l), &Overlay::default()));
    assert_eq!(m.len(), 1, "{m:?}");
    assert!(m[0].contains("no_cache"), "{m:?}");
}

#[test]
fn unsupported_legacy_keys_become_notices_only_when_set() {
    let r = run(None, None, Some(LEGACY_FULL), &Overlay::default()).unwrap();
    let keys: Vec<(&str, &str)> = r.notices.iter().map(|n| (n.key, n.file.as_str())).collect();
    assert_eq!(
        keys,
        [
            ("link-node-modules", "/r/.local-ci.toml"),
            ("[setup] ignore-env", "/r/.local-ci.toml")
        ]
    );
    assert!(r.notices[0].reason.contains("[[clone]]"), "{:?}", r.notices);
    assert!(r.notices[0].reason.contains("not supported"));
    for l in [
        "",
        "link-node-modules = false\n",
        "[setup]\nignore-env = []\n",
    ] {
        let r = run(None, None, Some(l), &Overlay::default()).unwrap();
        assert!(r.notices.is_empty(), "{l:?}: {:?}", r.notices);
    }
    let r = run(
        None,
        None,
        Some("link-node-modules = true\n"),
        &Overlay::default(),
    )
    .unwrap();
    assert_eq!(r.notices.len(), 1);
    assert_eq!(r.notices[0].key, "link-node-modules");
    let r = run(
        None,
        None,
        Some("[setup]\nignore-env = [\"CI\"]\n"),
        &Overlay::default(),
    )
    .unwrap();
    assert_eq!(r.notices.len(), 1);
    assert_eq!(r.notices[0].key, "[setup] ignore-env");
    assert!(run(None, None, None, &Overlay::default())
        .unwrap()
        .notices
        .is_empty());
}

// ---- worker priority -------------------------------------------------------

#[test]
fn priority_defaults_and_origins() {
    let d = Origin::Default;
    let r = run(None, None, None, &Overlay::default()).unwrap();
    assert_eq!(r.local_worker_priority, v(0, d.clone()));
    assert_eq!(r.priority_step_seconds, v(60, d.clone()));
    let none = r.entries();
    let g = |k: &str| none.iter().find(|x| x.key == k).unwrap().clone();
    assert_eq!(g("coordinator.local_worker_priority").value, "0");
    assert_eq!(g("scheduler.priority_step_seconds").value, "60");

    let u = Origin::User("/u/config.toml".into());
    let r = run(Some(USER_EXAMPLE), None, None, &Overlay::default()).unwrap();
    assert_eq!(r.local_worker_priority, v(-1, u.clone()));
    assert_eq!(r.priority_step_seconds, v(60, u.clone()));
    assert_eq!(r.workers[0].priority, v(10, u.clone()));
    // unset on a worker: the default, with the default origin
    assert_eq!(r.workers[1].priority, v(0, d.clone()));
    assert_eq!(r.workers[2].priority, v(0, d));
    let e = r.entries();
    let g = |k: &str| e.iter().find(|x| x.key == k).unwrap().clone();
    assert_eq!(g("coordinator.local_worker_priority").value, "-1");
    assert_eq!(g("coordinator.local_worker_priority").origin, u);
}

#[test]
fn priority_bounds() {
    let w = |p: i64, extra: &str| {
        format!(
            "schema_version = 1\n{extra}\n[[workers]]\nid = \"w\"\nssh = \"h\"\npriority = {p}\n"
        )
    };
    for ok in [-100, 0, 100] {
        run(Some(&w(ok, "")), None, None, &Overlay::default()).unwrap();
    }
    for bad in [-101, 101, i64::MAX] {
        let p = problems(run(Some(&w(bad, "")), None, None, &Overlay::default()));
        assert!(
            p.iter().any(|m| m.starts_with(&format!(
                "worker \"w\": priority = {bad} (user config /u/config.toml) must be between -100 and 100"
            ))),
            "{p:?}"
        );
    }
    for ok in [-100, 100] {
        let t = format!("schema_version = 1\n[coordinator]\nlocal_worker_priority = {ok}\n");
        run(Some(&t), None, None, &Overlay::default()).unwrap();
    }
    for bad in [-101, 101] {
        let t = format!("schema_version = 1\n[coordinator]\nlocal_worker_priority = {bad}\n");
        let p = problems(run(Some(&t), None, None, &Overlay::default()));
        assert!(p
            .iter()
            .any(|m| m.starts_with(&format!("local_worker_priority = {bad} ("))));
    }
    for ok in [0, 3600] {
        let t = format!("schema_version = 1\n[scheduler]\npriority_step_seconds = {ok}\n");
        run(Some(&t), None, None, &Overlay::default()).unwrap();
    }
    let t = "schema_version = 1\n[scheduler]\npriority_step_seconds = 3601\n";
    let p = problems(run(Some(t), None, None, &Overlay::default()));
    assert!(p
        .iter()
        .any(|m| m.starts_with("priority_step_seconds = 3601 (") && m.contains("at most 3600")));
}

#[test]
fn priority_wrong_types_and_unknown_keys_are_errors() {
    for t in [
        "schema_version = 1\n[scheduler]\npriority_step_seconds = -1\n",
        "schema_version = 1\n[scheduler]\npriority_step_seconds = \"60\"\n",
        "schema_version = 1\n[coordinator]\nlocal_worker_priority = 1.5\n",
        "schema_version = 1\n[[workers]]\nid = \"w\"\nssh = \"h\"\npriority = \"high\"\n",
        "schema_version = 1\n[[workers]]\nid = \"w\"\nssh = \"h\"\npriorty = 1\n",
        "schema_version = 1\n[scheduler]\npriority_step = 1\n",
    ] {
        assert!(parse_user("/u/config.toml", t).is_err(), "{t}");
    }
}
