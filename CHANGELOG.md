# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.1] - 2025-06-14

### Changed

- Renamed PyPI distribution from `agent-orchestrator` to `dandelion-orchestrator`.
- Fixed streaming event delivery.

## [0.1.0] - 2025-06-14

### Added

- Workflow engine with 8 node types: `agent`, `tool`, `transform`, `human`,
  `condition`, `parallel`, `subflow`, `loop`.
- Shared state with `{{path.to.value}}` template resolution and `| default(...)` filter.
- Condition expressions: `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`,
  `and`, `or`, parentheses.
- Human checkpoint and resume flow with response schema validation.
- Human nodes in parallel branches (sequential fallback) and subflow nodes.
- Loop nodes with condition-based exit and max iteration caps.
- Tool confirmation, permissions, risk levels, and policy decisions.
- Tool and agent input/output schema validation.
- Node retry with exponential backoff and error edges.
- Pluggable checkpoint, event, and artifact stores.
- Built-in stores: in-memory, file, SQLite, Redis.
- Replayable event compaction.
- Event schema versioning with migration registry.
- Message/SSE event adapter for stream continuity.
- Optional Claude Agent SDK runner.
- Persistence plugin registry for custom store providers.
- Execution leases to prevent concurrent run advancement.
- Observability hooks via `WorkflowObserver`.
- Pending action timeout with configurable default decisions.
- GitHub Actions CI for Python 3.12 and 3.13 with Redis service.
- MIT License.

[0.1.1]: https://github.com/gly-hub/agent-orchestrator/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/gly-hub/agent-orchestrator/releases/tag/v0.1.0
