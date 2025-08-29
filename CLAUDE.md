# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Keystone is a pure-Rust object storage system built on redb, providing S3-like bucket/key/object hierarchy with ACID compliance. The project follows a strict Test-Driven Development (TDD) methodology and implements a phased architecture approach.

## Common Commands

```bash
# Build the library
cargo build

# Run all tests
cargo test

# Run tests for a specific module
cargo test storage::

# Run a single test
cargo test test_basic_put_get

# Check code without building
cargo check

# Run tests with verbose output
cargo test -- --nocapture
```

## Architecture Overview

The codebase follows a layered architecture designed for extensibility and testability:

### Phase 1: Storage Abstraction Layer (SAL) - **COMPLETED**
- **KeyValueStore trait** (`src/storage/mod.rs`): Generic interface abstracting storage operations
- **redb adapter** (`src/storage/redb_adapter.rs`): Concrete implementation using redb as backend
- **Core operations**: get, put, delete, scan_prefix, transactions
- **Design principle**: Isolates object storage logic from specific database implementation

### Planned Architecture (from technical specification):

**Phase 2: Core Data Model** - Key mapping and serialization logic
- Bucket/key hierarchy mapping to flat key-value space
- Key format: `<bucket_name>\0<object_key>` 
- Metadata keys: `__meta__\0bucket\0<bucket_name>`
- Object serialization using Serde + bincode
- Large object chunking strategy

**Phase 3: Public API** - User-facing interfaces  
- `Datastore` struct: Main entry point for database operations
- `Bucket` struct: Scoped operations within a bucket
- Comprehensive error handling with `thiserror`

**Phase 4: Testing & Validation** - Performance and reliability testing

## Key Technical Decisions

- **Storage Engine**: redb chosen for stability (stable on-disk format) over sled (experimental)
- **Serialization**: bincode for performance over JSON
- **Error Handling**: Structured errors with `thiserror` for library consumers
- **Testing Strategy**: Unit tests with mocked storage + integration tests with real redb
- **Transaction Model**: ACID transactions with automatic rollback on error

## Development Approach

The project strictly follows TDD "Red-Green-Refactor" cycle:
1. Write failing test defining desired functionality
2. Write minimal code to make test pass  
3. Refactor while maintaining passing tests

Each phase must be fully completed and tested before proceeding to the next phase.

## Current Implementation Status

- ✅ Phase 1: Storage Abstraction Layer complete with comprehensive tests
- 🚧 Phase 2: Core Data Model (in progress)
- ⏳ Phase 3: Public API (pending)
- ⏳ Phase 4: Testing & Validation (pending)

## Project Structure Philosophy

- `src/storage/`: Storage abstraction layer - isolates redb implementation
- `src/data/`: (Planned) Core data structures and business logic  
- `src/`: (Planned) Public API layer
- `tests/`: Integration tests using real storage backend
- `Docs/`: Technical specification and implementation phases

The layered design ensures that higher-level components depend only on abstractions, not concrete implementations, enabling future backend swapping and comprehensive unit testing.