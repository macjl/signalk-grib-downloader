# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] — 2026-07-03

### Added
- Add an App Store screenshot for the GRIB Downloader webapp.
- Show a clear webapp warning when `signalk-container` cannot start download
  jobs because the container runtime is unavailable.

### Changed
- Replace the global Auto/Manual mode with per-source automatic download
  toggles: checked sources are included in scheduled downloads, unchecked
  sources remain available for manual downloads.
- Move the automatic check interval to the Signal K plugin settings while
  keeping the effective interval visible in the webapp.
- Manual "download all" now downloads every configured source, regardless of
  whether that source is enabled for automatic scheduling.

### Fixed
- Keep manual-only sources in the generated downloader config so individual
  manual downloads work consistently.
- Migrate older `enabled` and global `mode` settings to the new per-source
  `autoDownload` behavior.
- Preserve the legacy webapp interval as a fallback when migrating to the new
  plugin-level scheduler interval setting.

## [0.1.3] — 2026-07-03

### Changed
- Updated development dependencies for TypeScript 6, Node 26 type definitions and SignalK server API 2.29.
- Explicitly include Node types in the TypeScript configuration.

## [0.1.2] — 2026-06-11

### Changed
- The GRIB root defaults to `~/.signalk/gribs` — readable, and inside the mounted volume on containerized installs

## [0.1.1] — 2026-06-11

### Fixed
- `~` is now expanded in the GRIB root directory setting
- The GRIB root defaults to `<signalk-config>/gribs` instead of `/tmp/gribs` — reachable from the container runtime on every deployment
- The "path not reachable" error now explains the mounted-volume requirement

## [0.1.0] — 2026-06-11

### Added
- Download orchestration for GFS, AROME, ARPEGE and ICON-EU through
  containerised [grib-downloader](https://github.com/macjl/grib-downloader)
  jobs (signalk-container)
- Auto mode: downloads each model run as soon as it is published
  (per-model cadence and publication delay, upstream availability probing);
  manual mode for intermittent connections
- Management webapp: download area on a map, sources with duration sliders
  and volume estimation, per-source status badges and download logs,
  auto/manual switch, per-source and global download triggers
- Source directories derived as `<model>-<resolution>` under a shared root —
  the only contract with signalk-grib-weather-provider's source discovery
- Fetch-parameter fingerprint in run markers: changing the area, duration,
  groups or variables re-fetches the data
- Per-source archive of past runs (`<source>/archive/`, configurable count)
- Optional data deletion when removing a source
- Settings managed by the webapp (`settings.json` in the plugin data dir);
  plugin config panel only holds infrastructure settings
- French and English UI based on browser language
