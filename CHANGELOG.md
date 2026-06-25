# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.0.0] - 2026-06-25

### Added
- `docker-compose.yml` to spin up SurrealDB for local testing (v2.x on port 8008, v3.x on port 8009)
- GitHub Actions test workflow running the suite against both SurrealDB v2.x and v3.x on every push/PR
- `SURREALDB_URL` environment variable to configure the test connection (defaults to `ws://localhost:8018/rpc`)

### Changed
- **BREAKING**: bumped `surrealdb` to `>=2.0.0,<3.0.0`, which requires a SurrealDB **server v2.0.0 or newer** (tested against v2.6.5 and v3.1.5)
- Updated dependencies: `langchain-core` 1.4.x, `langgraph` 1.2.x (pulls in `langgraph-checkpoint` 4.x), and dev tooling (mypy 2.x, pytest 9.x, ruff 0.15.x, pytest-asyncio 1.x)
- `setup()` now creates the `checkpoint`/`write` tables and the cascade-delete event instead of only flipping an internal flag

### Fixed
- Compatibility with the surrealdb SDK 2.0, where `SELECT` on a non-existent table raises `NotFoundError` instead of returning an empty result. Tables are now provisioned lazily on the first connection, fixing read-before-write failures (and the cascading `KeyError` seen under concurrent async usage)
- `get_state_history()` / `list()` crashing on control writes (e.g. `branch:to:*`) that serialize to `("null", b"")`. Pending writes are now deserialized using each write's own `type` field instead of the checkpoint's type, which previously fed an empty payload to the msgpack decoder
- Scoped the `checkpoint_delete` cascade event to `thread_id` + `checkpoint_ns` + `checkpoint_id` so deleting a checkpoint can no longer remove writes from another thread/namespace that happens to share the same `checkpoint_id`

## [2.0.0] - 2026-01-21

### Added
- Support for langgraph 1.x and langchain-core 1.x
- New `metadata_type` field in checkpoint schema for proper serialization
- Backward compatibility with legacy checkpoint data format

### Changed
- Refactored serialization to use `dumps_typed` API for metadata
- Updated minimum dependencies:
  - `langchain-core>=1.0.0`
  - `langgraph>=1.0.0`

### Fixed
- "Failed to serialize checkpoint data" error when using langgraph 1.x

## [1.5.1] - 2026-01-21

### Changed
- Pinned dependencies to legacy versions to ensure stability:
  - `langchain-core>=0.3.40,<1.0.0`
  - `langgraph>=0.3.2,<1.0.0`
  - `langgraph-checkpoint>=2.0.0,<3.0.0`

### Notes
- This is a maintenance release for users who need to stay on langgraph 0.x
- For langgraph 1.x support, upgrade to version 2.0.0

[Unreleased]: https://github.com/lfnovo/langgraph-checkpoint-surrealdb/compare/v3.0.0...HEAD
[3.0.0]: https://github.com/lfnovo/langgraph-checkpoint-surrealdb/compare/v2.0.0...v3.0.0
[2.0.0]: https://github.com/lfnovo/langgraph-checkpoint-surrealdb/compare/v1.5.1...v2.0.0
[1.5.1]: https://github.com/lfnovo/langgraph-checkpoint-surrealdb/compare/v1.5.0...v1.5.1
