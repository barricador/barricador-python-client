# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.0] - 2026-08-30
### Changed
- **BREAKING (behavioral): flag synchronization now polls by default.** `streaming_enabled` defaults
  to `False` on both `BarricadorClient` and `AsyncBarricadorClient`; the ruleset is refreshed every
  `poll_interval` (new, default 30.0s) using a conditional `If-None-Match` request, so an unchanged
  ruleset returns `304 Not Modified`. Holding an SSE stream open bills backend instance time for the
  whole connection. Restore the previous behavior with `streaming_enabled=True`.
- Propagation latency in the default mode is now up to one `poll_interval`.

### Added
- `poll_interval` constructor argument.
- `Transport.bootstrap_conditional()` for ETag-aware fetches.

## [0.2.0] - 2026-06-25
### Changed
- Rebrand to barricador; package is now barricador-client.

## [0.1.1] - 2026-06-22
### Changed
- Default base URL is now `https://app.barricador.com` (was `app.barricador.io`).

## [0.1.0] - 2026-06-21
### Added
- Initial public release.
