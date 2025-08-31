# Repository Guidelines

## Project Structure & Module Organization
- `src/lib.rs`: Library entry; re-exports core types.
- `src/storage/`: Storage abstraction and backends
  - `mod.rs`: `KeyValueStore`, `Transaction`, `StorageError`.
  - `redb_adapter.rs`: redb-backed implementation.
- `Docs/`: Design and phase docs (`phase.md`, `rustdb.md`).
- `.github/workflows/`: CI and Claude automation.
- Tests live alongside modules via `#[cfg(test)]`; add integration tests under `tests/` if needed.

## Build, Test, and Development Commands
- `cargo build`: Compile the library in debug mode.
- `cargo test`: Run unit/integration tests.
- `cargo doc --open`: Build and open API docs locally.
- `cargo fmt` / `cargo fmt --check`: Format / verify formatting.
- `cargo clippy -- -D warnings`: Lint and deny warnings.
Tip: ensure `rustup component add rustfmt clippy` first.

## Coding Style & Naming Conventions
- Rust 2021, 4-space indent, max line length per rustfmt defaults.
- Names: `snake_case` (functions/modules), `CamelCase` (types/traits), `SCREAMING_SNAKE_CASE` (consts).
- Errors: implement via `thiserror`; return `StorageResult<T>`.
- Organization: keep backend-specific code in `src/storage/*`; only public API in `lib.rs`.
- Lints: keep code `clippy`-clean; prefer explicit errors over `unwrap()` outside tests.

## Testing Guidelines
- Framework: Rust built-in test harness; temp FS via `tempfile`.
- Unit tests: co-located `mod tests { ... }` blocks (see `src/storage/mod.rs`).
- Integration tests: place files in `tests/` (e.g., `tests/redb_integration.rs`).
- Conventions: `test_*` function names; assert behavior and error mapping; no external services.

## Commit & Pull Request Guidelines
- Commits: concise, imperative, one topic per commit.
  - Examples: “Refactors prefix scan end bound calculation”, “Introduces streaming prefix iterators”.
- PRs: clear description, linked issues, test coverage for changes, updated docs when relevant, CI green.
- Reviews: GitHub Actions run in `.github/workflows`; you can tag `@claude` in PR/issue comments for automated feedback.

## Security & Configuration Tips
- Don’t commit DB files or `target/` (see `.gitignore`).
- Use temp directories for tests; avoid network or global state.
- Persist data paths via explicit config when adding binaries.

