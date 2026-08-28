# Changelog

All notable changes to this project will be documented in this file.

## [0.2.3] — 2026-08-28

### Changed
- **Internet-aware, publication-aware scheduling.** The flat check timer
  (`setInterval` every `checkIntervalMinutes`) is replaced by an adaptive
  scheduler: each auto source is checked shortly after its model's next
  run is expected to publish (per-model cadence and publication delay plus
  a 5-minute slack). `checkIntervalMinutes` becomes the fallback/retry
  cadence — the longest we wait before retrying a late or failed run.
- The plugin follows `network.internet.state` (`online` | `offline` |
  `metered` | `captive`) when some plugin publishes it: `offline` pauses
  auto downloads until connectivity returns, then a single catch-up check
  runs; `metered` stretches every automatic wait by the new
  `meteredIntervalMultiplier` plugin setting (default ×3) to save
  bandwidth on pay-per-MB links. Without the path (no publisher
  installed) the state stays `unknown`, which behaves exactly as
  `online` — no regression for existing setups.
- `captive` (behind a captive portal, as published by
  *@meri-imperiumi/signalk-internet*) pauses the auto scheduler and the
  plugin status exactly like `offline` — previously it was rejected and
  fell back to `unknown` ≈ `online`, so automatic downloads could start
  against the portal's login page.
- Manual downloads are never gated server-side; the webapp asks for
  confirmation before triggering one while `metered`, `offline` or
  `captive`.
- `GET /status` now reports `internetState` and `nextAutoTickAt`; the
  webapp shows a connectivity badge ("metered — auto slowed",
  "offline — auto paused", "captive — auto paused") and the next
  scheduled check time.

## [0.2.2] — 2026-08-27

### Changed
- **Removed the container dependency.** Downloads now run in-process in
  TypeScript — no `signalk-container` plugin, no container runtime, no image
  pulls. The plugin runs on a plain Signal K server with Node ≥ 18.17.
  - Removed `signalk.requires: ["signalk-container"]`.
  - Removed the `downloaderImage` plugin config option and the container
    runtime/`runJob` plumbing from the orchestrator and plugin host.
  - `gribsRoot` is a plain local path (no host-path resolution).
- Ported the per-model fetch logic (GFS/NOMADS, AROME/ARPEGE Météo-France,
  ICON-EU/DWD) from the containerised `grib-downloader` Python script to a
  native TypeScript module using Node's `fetch`/streams. The outcome protocol
  (`downloaded | up-to-date | unavailable | failed`), the source directory
  naming, and the run-marker fingerprint format are unchanged, so existing
  on-disk data and `signalk-grib-weather-provider` keep working.
- ICON-EU `.bz2` GRIB fragments are decompressed in-process via
  `@digitaldefiance/bzip2-wasm` (no host `bzip2` binary needed).

### Added
- Smoketests for the native downloader: a local HTTP test server exercises
  each model's fetch path (download, up-to-date, unavailable, failed), the
  bzip2 decompress+concat path for ICON-EU, marker/fingerprint round-trips,
  and cleanup/archive rotation.
- Tests for run cancellation (aborting the signal tears down the in-flight
  request, no retries, no `.part` leftovers) and for write-stream
  backpressure (a slow disk write paces the network read instead of
  buffering the whole body in memory).

### Fixed
- The per-source download timeout now actively aborts the run: an
  `AbortSignal` is propagated from the orchestrator through every fetch, and
  the timeout timer is cleared once the run settles. Previously the timeout
  only stopped *waiting* — the abandoned download kept running in the
  background (and retrying), so a later trigger could run a second download
  concurrently against the same `.part` and output files.
- GRIB bodies are streamed to disk with `stream/promises` `pipeline`
  (`Readable.fromWeb(response.body)` → `createWriteStream`), honouring
  backpressure. Previously `write()` return values were ignored, so a large
  Météo-France GRIB could be buffered entirely in memory.
- Météo-France AROME and ARPEGE URLs now replace every template placeholder;
  downloads previously probed a filename containing a literal `{p}` and were
  incorrectly reported unavailable.

## [0.2.1] — 2026-07-03

### Fixed
- Include both icons expected by Signal K: a root `icon.svg` for the App Store
  and `public/icon.svg` for the webapp.

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
