# Contributing to kvr

Thank you for your interest in contributing! This document covers the basics.

## Development setup

```bash
git clone https://github.com/treeol/kvrust.git
cd kvrust
cargo build
```

## Quality gate

All contributions must pass the CI checks. Run them locally before pushing:

```bash
# Format check (must produce no diff)
cargo fmt --check

# Lint (warnings are errors, includes tests/benches)
cargo clippy --all-targets -- -D warnings

# Tests
cargo test
```

If `cargo fmt --check` reports diffs, run `cargo fmt` to fix them.

## Pull request process

1. Fork the repository and create a branch from `master`.
2. Make your changes with clear, focused commits.
3. Ensure the quality gate passes locally.
4. Open a pull request with a description of **what** changed and **why**.
5. If adding a new feature, include tests. If fixing a bug, add a regression
   test that fails before your fix and passes after.

## Style conventions

- Follow `rustfmt` defaults — do not override formatting.
- Resolve all `clippy` warnings (CI treats them as errors).
- Use `///` doc comments on all public items. Include a short summary line,
  then detail if needed. Examples in doc comments are run as doctests.
- Keep commits focused: one logical change per commit.

## Reporting bugs

Open a [GitHub issue](https://github.com/treeol/kvrust/issues) with:

- Rust version (`rustc --version`)
- Operating system
- Steps to reproduce
- Expected vs actual behavior

## Reporting security vulnerabilities

Do **not** open a public issue for security vulnerabilities. See
[SECURITY.md](SECURITY.md) for the reporting process.
