# Contributing to Igloo

Thank you for your interest in contributing! This guide will help you get started and avoid common pitfalls, especially if you're new to Rust or open source.

## Quick Start

1. **Set up your environment:**
   ```bash
   ./scripts/setup.sh
   ```
   This will install all dependencies, set up pre-commit hooks, and build the project.

2. **Run all checks locally before pushing:**
   ```bash
   ./scripts/check.sh
   ```
   This runs the same checks as CI (formatting, lint, build, tests).

## Common CI Errors & Fixes

- **Formatting failed:**
  - Error: `cargo fmt --all -- --check` failed.
  - Fix: Run `cargo fmt --all` locally and commit the changes.

- **Clippy (lint) failed:**
  - Error: `cargo clippy --workspace --all-targets -- -D warnings` failed.
  - Fix: Run `cargo clippy --workspace --all-targets` locally and fix the warnings.

- **Build failed:**
  - Error: `cargo build --workspace --all-targets` failed.
  - Fix: Check the error message, fix the code, and try again.

- **Tests failed:**
  - Error: `cargo test --workspace --all-targets` failed.
  - Fix: Run `cargo test --workspace --all-targets` locally and fix any failing tests.

## Pre-commit Hooks

We use [pre-commit](https://pre-commit.com/) to run checks before each commit. If you skipped setup, you can install it manually:
```bash
pip install pre-commit
pre-commit install
```

## Contributing Ideas
- See the Roadmap in README.md for high-level goals
- Open issues for bugs, feature requests, or questions
- Submit pull requests for code, docs, or tests
- Review the code of conduct and style guidelines

## Need Help?
- Check the [README.md](./README.md) for more details.
- Ask in the team chat or open an issue if you're stuck!
- Don't hesitate to ask questions—everyone was new once!