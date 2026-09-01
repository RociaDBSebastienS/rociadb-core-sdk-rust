# Repository Guidelines

## Project Structure & Module Organization

This repository contains only the Rust SDK for RociaDB's gRPC services. Rust APIs live in `src/`, with protobuf generation controlled by `build.rs`. Canonical schemas are in `proto/`. Keep generated `target/` content out of source changes.

The Node.js/TypeScript SDK has moved to its own sibling repository
([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)) and is no longer part of this checkout — there is no
`typescript/` directory here, and no `npm` commands apply to this repo.

## Build, Test, and Development Commands

- `mise install` installs the pinned Rust toolchain and protobuf compiler.
- `cargo build` compiles the SDK and regenerates gRPC bindings when proto inputs change.
- `cargo test` runs unit, integration, and documentation tests.
- `cargo fmt --all -- --check` verifies standard Rust formatting; run `cargo fmt --all` to apply it.
- `cargo clippy --all-targets --all-features -- -D warnings` treats lint warnings as failures.
- `cargo doc --no-deps` checks and builds API documentation locally.

If `PROTOC` is not already set to a real `protoc` binary (as it is after `mise install`), prefix the above with `PROTOC=/path/to/protoc`.

Run formatting, Clippy, and tests before submitting changes.

## Coding Style & Naming Conventions

Use rustfmt defaults and idiomatic Rust naming. Public methods return `rocia_db_sdk::Result<T>` (an alias for `std::result::Result<T, RociaDbError>`, defined in `src/error.rs`) rather than `anyhow::Result` — extend the existing `RociaDbError` variants (or add a new one) for a new fallible case instead of reaching for `anyhow`, and avoid panics or unchecked casts in public paths. Write Rust documentation and comments in English only — do not introduce French text or `EN:`/`FR:` prefixes. Do not edit generated output. Protobuf changes must originate in canonical `proto/` here and be mirrored byte-for-byte into the sibling [`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts) repository's own copy — that repository uses TypeScript strict mode, two-space indentation, `camelCase` values, and `PascalCase` types, but none of its files live in this checkout.

## Testing Guidelines

Add focused Rust unit tests in `#[cfg(test)] mod tests`; place public API scenarios in `tests/`. Keep unit tests deterministic and independent of live RociaDB or OAuth services. Cover success paths, invalid configuration, authentication behavior, pagination, and serialization edge cases relevant to each change.

## Commit & Pull Request Guidelines

Use concise, imperative commit subjects (for example, `Add query sort support`) and keep commits narrowly scoped, matching the style of existing history. Pull requests should explain the behavior change, motivation, validation commands, and any compatibility impact. Link relevant issues; include request/response examples for API changes and note regenerated protobuf effects. Screenshots are only useful for documentation rendering changes.

## Security & Configuration

Authentication reads `AUTH_TOKEN_URL`, `AUTH_CLIENT_ID`, and `AUTH_CLIENT_SECRET`. Never commit real credentials, tokens, or environment files. Use `disable_auth()` only for controlled local or test environments.
