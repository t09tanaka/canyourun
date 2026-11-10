# remote-task-runner

Run CI and development commands across your own Macs on the LAN: one coordinator, many workers, with uncommitted changes, durable queues, cancellation and crash recovery.

Status: in development. The specification (Japanese) is in [docs/spec.md](docs/spec.md).

Builds on [local-ci](https://github.com/t09tanaka/local-ci) for CI planning and [task-traffic-controller](https://github.com/t09tanaka/canirun) for per-host resource admission.

## License

MIT
