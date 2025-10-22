# Changelog

All notable changes to this project will be documented in this file.

## Unreleased
### Added
- Declared explicit MSRV with `rust-version = "1.90"` in `Cargo.toml`.
- Added `rust-toolchain.toml` to pin toolchain (Rust 1.90.0) and ensure `rustfmt` + `clippy` components are available.

### Changed
- Upgraded Rust edition from 2018 to 2021.
- Replaced deprecated non-format `panic!(err_msg)` calls with `panic!("{}", err_msg)` (edition compliance / future-proofing).
- Cross-platform thread id helper: replaced Linux-only `libc::SYS_gettid` usage with conditional `pthread_self` / Windows `GetCurrentThreadId` / Linux syscall.
- Removed obsolete `regex::internal::Input` import (crate internal API not public in current `regex`).
- General warning cleanup: removed unused imports, unnecessary `mut`, simplified comparisons, and eliminated invalid pattern usages.

### Fixed
- Build failures on Rust 1.90 caused by private module import and unsupported constant (`SYS_gettid` on non-Linux platforms).

### Internal / Maintenance
- Initial clippy pass run; enumerated follow-up lint tasks (left intentionally un-fixed to keep this PR focused on toolchain upgrade).
- Established follow-up roadmap (dependency modernization, structopt→clap migration, CI workflow, clippy phase 2, benchmarking, release bump).

### Pending / Not Included Yet
- Migration from `structopt` to `clap` v4 derive.
- Dependency upgrades: `crossterm`, `prettytable-rs`, `jemallocator`, `zstd`, `itertools`, `assert_cmd`, `predicates`, `crossbeam*` family.
- Comprehensive clippy cleanup and formatting pass.
- CI workflow and benchmark suite.
- Version bump (consider `0.10.2`).

---

## 0.10.1 (previous)
- (Refer to prior history; this entry documents the starting point for the Rust 1.90 upgrade.)
