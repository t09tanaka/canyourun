# remote-task-runner

Run CI and development commands across your own Macs on the LAN: one coordinator, many workers, with uncommitted changes, durable queues, cancellation and crash recovery.

Status: in development. The specification (Japanese) is in [docs/spec.md](docs/spec.md), with progress and remaining acceptance work in [docs/plan.md](docs/plan.md).

Builds on [local-ci](https://github.com/t09tanaka/local-ci) for CI planning and [task-traffic-controller](https://github.com/t09tanaka/task-traffic-controller) for per-host resource admission.

## Development

The CLI surface is defined, but public orchestration commands remain under development. Unimplemented commands return `NOT_IMPLEMENTED` (exit code 70; with `--json`, an envelope with `schema_version` on stdout).

Use the current stable Rust toolchain for formatting and linting. The minimum supported compiler is Rust 1.89. Workspace integration tests require fast-worktree 0.4.0 or newer and portool; resource admission uses task-traffic-controller 0.8.1 or newer. Initialize and check the worktree's portool allocation before starting services or integration tests.

Run the configured CI against committed HEAD:

```sh
local-ci run --include-disabled
```

## License

MIT
