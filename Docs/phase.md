# Keystone Object Storage System - Phase Implementation Plan

This document outlines the structured implementation phases for the Keystone pure-Rust object storage system, following Test-Driven Development (TDD) methodology.

## Overview

The implementation follows an inside-out approach, building from the storage core outwards to the public API. Each phase is fully tested and validated before proceeding to the next, minimizing integration issues and simplifying debugging.

## Phase 1: Storage Abstraction Layer (SAL)

**Goal**: Create a clean internal boundary between object storage logic and the key-value store implementation.

### Tasks
- Define private `KeyValueStore` trait specifying essential operations:
  - `get(key)`, `put(key, value)`, `delete(key)`
  - `scan_prefix(prefix)`
  - Transaction management methods
- Implement `KeyValueStore` trait for redb
- Create redb adapter as the only component directly depending on redb crate

### TDD Focus
- Unit tests validating the redb adapter
- Tests that call adapter methods and verify correct database operations
- Isolation testing using trait boundaries

### Deliverables
- `src/storage/mod.rs` - KeyValueStore trait definition
- `src/storage/redb_adapter.rs` - redb implementation
- Unit tests in `src/storage/tests.rs`

## Phase 2: Core Data Model and Business Logic

**Goal**: Implement core object store logic including bucket/key mapping, serialization, and large object handling.

### Tasks
- Implement `Object` and `ObjectMetadata` structs with Serde
- Build key-prefixing logic for bucket/object hierarchy:
  - Format: `<bucket_name>\0<object_key>`
  - Bucket metadata: `__meta__\0bucket\0<bucket_name>`
- Implement chunking strategy for large objects:
  - Manifest objects for metadata and chunk references
  - Data chunks: `__data__\0<bucket_name>\0<object_key>\0<chunk_part_number>`
- All logic depends only on `KeyValueStore` trait, not redb directly

### TDD Focus
- Unit tests with mock `KeyValueStore` implementation (HashMap-based)
- Serialization round-trip testing
- Key generation and parsing validation
- Chunking logic verification

### Deliverables
- `src/data/mod.rs` - Core data structures
- `src/data/chunking.rs` - Large object handling
- `src/data/keys.rs` - Key generation logic
- Mock storage implementation for testing
- Comprehensive unit test suite

## Phase 3: Public API Implementation

**Goal**: Implement public-facing `Datastore` and `Bucket` structs with complete API.

### Tasks
- Implement `Datastore` struct with methods:
  - `new(path)` - Open/create datastore
  - `create_bucket(name)` - Create bucket
  - `delete_bucket(name)` - Delete bucket and contents
  - `bucket(name)` - Get bucket handle
  - `transaction(closure)` - Atomic multi-operations
- Implement `Bucket` struct with methods:
  - `put<T: Serialize>(key, data)` - Store object
  - `get<T: Deserialize>(key)` - Retrieve object
  - `delete(key)` - Delete object
  - `list(prefix)` - List objects by prefix
- Implement comprehensive error handling with `thiserror`
- Map internal errors to appropriate public `Error` variants

### TDD Focus
- Integration tests in `/tests` directory
- End-to-end API validation
- Error handling verification
- Real redb database usage

### Deliverables
- `src/lib.rs` - Public API exports
- `src/datastore.rs` - Datastore implementation
- `src/bucket.rs` - Bucket implementation
- `src/error.rs` - Error types and handling
- Integration test suite in `/tests`

## Phase 4: Testing and Validation

**Goal**: Complete testing coverage and validate system performance characteristics.

### Tasks
- Comprehensive TDD cycle completion
- Concurrency testing with multiple readers/writers
- Large object stress testing (100MB+ files)
- Memory efficiency validation (streaming operations)
- Error condition testing for all failure modes
- Performance benchmarking

### TDD Focus
- Property-based testing for edge cases
- Concurrent access patterns
- Resource usage monitoring
- Failure recovery testing

### Deliverables
- Complete test suite with high coverage
- Performance benchmarks
- Stress test validation
- Documentation examples

## Implementation Strategy

### Test-Driven Development Cycle
1. **Red**: Write failing test defining desired functionality
2. **Green**: Write minimum code to make test pass
3. **Refactor**: Improve code quality while maintaining passing tests

### Project Structure
```
src/
├── lib.rs              # Public API exports
├── datastore.rs        # Datastore implementation
├── bucket.rs          # Bucket implementation
├── error.rs           # Error handling
├── data/
│   ├── mod.rs         # Core data structures
│   ├── chunking.rs    # Large object handling
│   └── keys.rs        # Key generation logic
└── storage/
    ├── mod.rs         # KeyValueStore trait
    └── redb_adapter.rs # redb implementation

tests/
├── integration.rs     # End-to-end API tests
├── concurrency.rs     # Concurrent access tests
└── large_objects.rs   # Large file handling tests
```

### Key Architectural Principles
- **Layered Architecture**: Clean separation between storage, logic, and API layers
- **Dependency Inversion**: Core logic depends on abstractions, not implementations  
- **Type Safety**: Leverage Rust's type system for compile-time guarantees
- **Error Transparency**: Comprehensive, actionable error types
- **Performance**: Streaming I/O and efficient memory usage

## Success Criteria

Each phase must meet the following criteria before proceeding:
- All tests pass
- Code coverage targets met
- Performance benchmarks satisfied
- Documentation complete
- Code review passed

## Future Extensibility

The architecture supports future enhancements:
- **Networking**: Web API layer using axum/tokio
- **Authentication**: Middleware-based security
- **Multiple Backends**: Additional KeyValueStore implementations
- **Distribution**: Cluster-aware storage backends

This phased approach ensures a robust, maintainable, and extensible object storage system that meets current requirements while providing a solid foundation for future development.