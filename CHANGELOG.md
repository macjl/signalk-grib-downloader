# Changelog

All notable changes to this project will be documented in this file.

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
