# Changelog

All notable changes to this project will be documented in this file.

## [0.5.0] - 2026-10-30

- Add independent GFS-Wave 0.25-degree sources selecting HTSGW, WVDIR and WVPER
  from NOAA's wave GRIB filter, with area subsetting and three-hour forecast steps.
- Stream wave acquisitions into separate immutable catalogue datasets, using
  existing asset-level recovery, authentication and source-specific retention.
- Add GFS-Wave to webapp selection, status, settings validation and source deletion.
- Use NOAA URL/parameter selections documented by haipule in PR #19. No OpenCPN
  export or Weather API wave decoder is included.
- Promote the sandbox-tested 0.5.0-alpha.1 implementation after maintainer
  validation, with existing sources unchanged and no automatic wave downloads
  enabled by the upgrade.

## [0.4.0] - 2026-10-30

- First stable catalogue-based downloader, including direct streaming,
  asset-level restart recovery and automatic readwrite access requests.
- Breaking upgrade from 0.3.x: publish only to Forecast Dataset Provider 0.1.0;
  update GRIB Weather Provider to 0.7.0 in the same maintenance window. Existing
  local GRIB files are preserved but not imported automatically.
- Document installation, access approvals, verification and rollback in
  docs/upgrading.md. The complete weather chain requires Node 24+.
- Failed replacement acquisitions retain previously committed forecasts (#15).

## [0.4.0-beta.3] - 2026-10-21

- Request catalogue access automatically through Signal K Access Requests when
  no token is configured; require administrator approval with this plugin's
  least-privilege permissions and persist the token privately.
- Resume pending requests across restarts, report waiting/denied states and
  resume catalogue work immediately after approval. Stop polling on plugin stop.
- Renew automatically issued tokens rejected with HTTP 401 while preserving
  manually configured or concurrently rotated tokens.
- Cover the access lifecycle, plugin startup and approval-driven resumption;
  validate both permission levels against an actual Signal K server.

## [0.4.0-beta.2] - 2026-10-21

- Keep journal writes compatible with Windows: flush the file before rename,
  and flush the containing directory on systems that support directory fsync.
- Stream GFS and Météo-France bodies directly to Forecast Dataset Provider 0.1.1+;
  declare asset IDs without sizes and let the provider measure size and SHA-256.
- Decompress ICON-EU in memory one variable at a time and stream concatenated
  GRIB messages without temporary GRIB files, markers or local archives.
- Schedule from durable catalogue receipts, preserving beta.1 publication journals.
- Resume only missing provider assets; recover lost create, upload and commit
  responses using acquisition IDs and fsynced stream receipts.
- Preserve publication references on generic 404 responses; require explicit
  provider not-found codes before discarding an identity.
- Reject empty, truncated or non-GRIB2 streams before commit and abort both
  upstream and upload on cancellation. Preserve previous committed acquisitions.
- Add actual provider integration tests for progress, interruption, limits and
  lost commit responses; retain publication-window and connectivity tests.

## [0.4.0-beta.1] - 2026-10-21

- Require catalogue publication in every orchestrator path; remove the internal
  standalone local-output bypass and the shared-directory `gribsRoot` setting.
- Keep downloads in the private `<plugin data directory>/downloads/` workspace.
  Existing directories remain untouched and can be copied into that workspace.
- Publish completed GRIB runs to Forecast Dataset Provider 0.1 via authenticated,
  streaming multi-asset HTTP publication with SHA-256 validation.
- Persist publication journals for asset-level restart recovery and lost commit
  responses; publish existing completed runs without downloading them again.
- Expose publication-pending state and committed dataset links in the webapp.
- Apply archive retention and explicit source-data deletion to owned catalogue
  acquisitions, respecting provider conflicts and preserving local cache use.
- Use a readwrite token file; replace the Weather Provider requirement with
  Forecast Dataset Provider. Require Node 20.19+ and Signal K 2.34+ within 2.x.
- Abort transfers on stop and reject configuration/deletion races while busy.

## [0.3.0] - 2026-10-20

### Added
- **850 mb level in the default GFS fetch.** The NOMADS filter now requests
  `lev_850_mb=on` alongside the existing levels, adding 850 hPa temperature,
  wind U/V and relative humidity to every downloaded step. Downstream consumers
  need these to derive automatic surface fronts (thermal front parameter),
  e.g. for front-passage timing in a passage briefing. Variables and levels are
  now part of the fetch fingerprint, so the latest run of every existing GFS
  source re-fetches once to pick the new level up.

### Changed
- GFS download volume estimate raised from ~15 to ~21 bytes per grid point per
  step to reflect the four added 850 mb messages.
- **Per-cycle publication windows.** A shared model schedule now controls both
  automatic checks and downloader run selection. AROME and ARPEGE use windows
  specific to each UTC cycle instead of a fixed publication delay; GFS and
  ICON-EU also have explicit earliest/latest publication bounds.
- Automatic probing starts at the lower publication bound and continues every
  five minutes by default, even after the upper bound has passed. An explicitly
  configured `checkIntervalMinutes`, including a migrated legacy value,
  overrides the model interval. Metered connections still multiply the probe
  interval without shifting the publication thresholds.
- The webapp and plugin status distinguish **awaiting publication** from
  **publication late**. `GET /status` adds `pendingRun` and `publicationLate`
  for each source; the existing status fields remain available.
- Updated development dependency locks for `@signalk/server-api` and
  `@types/node`.

### Fixed
- **ICON-EU on Windows.** `@digitaldefiance/bzip2-wasm` 1.1.1 resolves its
  `bzip2.wasm` by stripping `'file://'` from its module URL, which mangles
  Windows paths (`file:///D:/…` → `/D:/…` → `D:\D:\…`) and aborts the wasm
  load. The loader now reads the wasm file itself and hands the bytes to the
  emscripten factory, bypassing the package's path search entirely.
- A source's probe interval is anchored to its own last attempt, so another
  model's timer cannot trigger premature checks or postpone a retry. A newly
  opened cycle can be checked while an older cycle is still late.
- Keep a newer complete run instead of falling back to an older candidate
  when the publication windows overlap.

### Upgrade notes
- Existing GFS sources are fetched again once because the default field set
  and its fingerprint now include 850 hPa. The existing replacement-resilience
  limitation, where a failed re-fetch can remove the previous complete run,
  remains tracked in [#15](https://github.com/macjl/signalk-grib-downloader/issues/15).
- Existing decoded weather-provider caches can retain their previous field
  set when a GRIB is replaced under the same filename. Cache invalidation is
  tracked in [signalk-grib-weather-provider#16](https://github.com/macjl/signalk-grib-weather-provider/issues/16).

## [0.2.4] — 2026-08-28

### Changed
- Declare `signalk-grib-weather-provider` as a required Signal K plugin: it
  serves the GRIB files downloaded by this plugin. The optional
  `@meri-imperiumi/signalk-internet` integration remains a recommendation.

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
