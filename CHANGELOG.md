# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/lfnovo/langgraph-checkpoint-surrealdb/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/lfnovo/langgraph-checkpoint-surrealdb/compare/v1.5.1...v2.0.0
[1.5.1]: https://github.com/lfnovo/langgraph-checkpoint-surrealdb/compare/v1.5.0...v1.5.1
