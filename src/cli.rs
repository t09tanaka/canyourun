//! Command-line definition and dispatch.

use clap::{Args, Parser, Subcommand};
use serde::Serialize;

/// Process exit codes. The single definition site.
pub mod exit {
    /// Success.
    pub const SUCCESS: i32 = 0;
    /// Invalid input (bad arguments); clap also exits with this code.
    pub const INPUT_ERROR: i32 = 2;
    /// The command is defined but not implemented yet.
    pub const NOT_IMPLEMENTED: i32 = 70;
}

/// Version of the `--json` envelope.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Parser)]
#[command(name = "remote-task-runner", version, about)]
pub struct Cli {
    /// Print a structured result on stdout (progress and diagnostics go to stderr).
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Configure the coordinator and the local worker.
    Init,
    /// Run CI for the project.
    Ci(CiArgs),
    /// Run one command and wait for its result.
    Exec(ExecArgs),
    /// Submit a command without waiting.
    Submit(SubmitArgs),
    /// List runs and homework.
    Ls,
    /// Show one run or homework.
    Show(IdArgs),
    /// Show logs.
    Logs(LogsArgs),
    /// Wait for completion.
    Wait(IdArgs),
    /// Request cancellation.
    Cancel(IdArgs),
    /// Retry a terminal attempt.
    Retry(IdArgs),
    /// Fetch outputs.
    Outputs(OutputsArgs),
    /// Estimate a command.
    Estimate(SubmitArgs),
    /// Check the setup.
    Doctor,
    /// Manage workers.
    Workers(WorkersArgs),
    /// Run as a worker (internal).
    #[command(hide = true)]
    Worker(WorkerArgs),
}

#[derive(Debug, Args)]
pub struct CiArgs {
    #[command(subcommand)]
    pub sub: Option<CiSub>,
    /// Snapshot kind (for example `commit`).
    #[arg(long)]
    pub snapshot: Option<String>,
    /// Git ref to run.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum CiSub {
    /// Print the plan without running.
    Plan,
}

#[derive(Debug, Args)]
pub struct ExecArgs {
    /// Run in the current directory instead of a snapshot.
    #[arg(long)]
    pub in_place: bool,
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Args)]
pub struct SubmitArgs {
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Args)]
pub struct IdArgs {
    pub id: String,
}

#[derive(Debug, Args)]
pub struct LogsArgs {
    pub id: String,
    /// Keep streaming.
    #[arg(long)]
    pub follow: bool,
}

#[derive(Debug, Args)]
pub struct OutputsArgs {
    pub id: String,
    /// Destination directory.
    #[arg(long)]
    pub dir: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct WorkersArgs {
    #[command(subcommand)]
    pub sub: Option<WorkersSub>,
}

#[derive(Debug, Subcommand)]
pub enum WorkersSub {
    /// Register a worker.
    Add {
        name: String,
        /// SSH destination, for example `user@host`.
        #[arg(long)]
        ssh: Option<String>,
        /// Install the worker over SSH.
        #[arg(long)]
        install: bool,
    },
    /// Start a worker.
    Start { name: String },
    /// Stop new placement; finish accepted work.
    Drain { name: String },
    /// Resume placement.
    Resume { name: String },
    /// Stop a worker.
    Stop {
        name: String,
        /// Stop immediately and cancel running work.
        #[arg(long)]
        cancel_running: bool,
    },
    /// Upgrade a worker.
    Upgrade { name: String },
    /// Show differences from the project requirements.
    Diff { name: String },
    /// Sync a worker with the project requirements.
    Sync {
        name: String,
        /// Project to sync.
        #[arg(long)]
        project: Option<String>,
    },
    /// Forget a worker.
    Forget { name: String },
}

#[derive(Debug, Args)]
pub struct WorkerArgs {
    /// Speak the protocol over stdin/stdout.
    #[arg(long)]
    pub stdio: bool,
    /// Exit when the work is done.
    #[arg(long)]
    pub ephemeral: bool,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
}

/// The `--json` envelope written to stdout.
#[derive(Debug, Serialize)]
pub struct Envelope {
    pub schema_version: u32,
    pub ok: bool,
    pub error: ErrorBody,
}

impl Envelope {
    pub fn not_implemented() -> Self {
        Envelope {
            schema_version: SCHEMA_VERSION,
            ok: false,
            error: ErrorBody {
                code: "NOT_IMPLEMENTED",
                message: "this command is not implemented yet".to_string(),
            },
        }
    }
}

/// Run a parsed command and return the process exit code.
pub fn run(cli: Cli) -> i32 {
    let envelope = Envelope::not_implemented();
    if cli.json {
        let line = serde_json::to_string(&envelope).expect("envelope serializes");
        println!("{line}");
    } else {
        eprintln!("error: {}", envelope.error.message);
    }
    exit::NOT_IMPLEMENTED
}

/// Entry point used by the binary: parse arguments, run, return the exit code.
pub fn main() -> i32 {
    match Cli::try_parse() {
        Ok(cli) => run(cli),
        Err(e) => {
            let code = if e.use_stderr() {
                exit::INPUT_ERROR
            } else {
                exit::SUCCESS
            };
            let _ = e.print();
            code
        }
    }
}
