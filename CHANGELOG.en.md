# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

Categories: `Added` / `Changed` / `Fixed` / `Removed` / `Security` / `Docs`.
Day-to-day changes go under `[Unreleased]`; on release, turn it into
`[x.y.z] - YYYY-MM-DD` and open a fresh empty `[Unreleased]`.

[中文](CHANGELOG.md) | [English](CHANGELOG.en.md)

## [Unreleased]

<!-- Append changes for the next release here -->

## [0.2.0] - 2026-11-27

### Added

- Android tablet app: Kotlin engine + embedded local panel server + foreground download service, sharing the same HTML panel as desktop
- Actionable message when HTTPS certificate validation fails, plus an "ignore certificate errors" switch

### Fixed

- Share links that require an anonymous login: detect the login page and parse the real filename
- `HEAD` blocked by a CDN (404/405) is no longer misreported as "server does not support downloads"
- Android: finished files land in the system Downloads folder on all Android versions

### Docs

- README now explains login-gated share links and how to paste a Cookie header

## [0.1.0] - 2026-11-26

### Added

- Initial release: self-tuning multi-threaded engine (probe → plan → ramp-up → steady-state AIMD → finish)
- Probe: `HEAD` for length plus a Range probe to confirm real range support, measuring single-connection baseline and ETag
- Dynamic chunk queue where workers grab the next chunk, avoiding a slow chunk stalling the job
- Resume: `.part` + `.part.json` validated on size / chunk_size / etag / last_modified
- Rate limiting (token bucket) and optional whole-file hash verification (md5 / sha1 / sha256)
- Single-file HTML panel fed by 9 SSE event types
- CLI (rich) with custom request headers
- Mock origin server and self-contained engine/panel tests
