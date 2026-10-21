# Sandbox verification — direct catalogue streaming

Target: the existing `signalk-sandbox` container, Signal K 2.34.0 / Node 24.21.0,
with Forecast Dataset Provider and Forecast Dataset Browser 0.1.1. Downloader:
`feature/forecast-dataset-provider`, package 0.4.0-beta.2.

## Acquisition and upgrade

Original GRIB bytes go straight from the upstream HTTP response to provider
staging. The downloader stores only settings and JSON publication journals.
GFS/Météo-France use streamed bodies; ICON uses one bounded bzip2 fragment at a
time in memory. Commit requires successful upstream EOF and GRIB2 framing checks.

Previously committed beta.1 journals remain valid. The model-cycle scheduler
now reads those receipts without needing the old shared directory or completion
markers. Legacy files are retained for rollback and are never imported or
removed implicitly. The provider remains the sole storage owner for new data.

## Deployment helpers

`tools/install-sandbox.cjs` runs as root inside the explicitly selected container
and checks `SIGNALK_CONTAINER_ID=signalk-sandbox`. Supply a built npm archive under
`/home/node/.signalk/local-packages/`. It preserves the existing dependency tree,
archives the old package and manifests, backs up all plugin-configuration hashes
and reuses the existing independent readwrite producer token.

It stops only the downloader through its configuration API before snapshotting
its journal and swapping packages. It writes the original activation/settings
back with only the obsolete `gribsRoot` field removed. Restart the existing
container to load the new code; do not pull/recreate the image, alter mounts or
resolve the whole npm tree. On error after stopping, restore the backed-up
configuration before restarting; the previous package/manifests are also saved.

The backup pointer is `local-packages/grib-downloader-forecast-backup.json`.
Private backups include security/token material and remain inside a private
backup directory. For rollback, stop the downloader and restore its archived
package, manifests, configuration and journal, then restart the container.

`tools/verify-sandbox.cjs` makes readonly checks against real existing datasets:
receipt hashes, catalogue discovery, original bytes, Range, anonymous rejection,
producer permission and preservation of the other plugin configurations/settings.
It does not delete or republish existing acquisitions.

`tools/verify-direct-sandbox.cjs` uses the installed producer against the real
provider and a loopback upstream serving an actual GFS fixture. It creates only
a uniquely named two-asset test acquisition with its journal in `/tmp`:

- Observe receiving state with a completed first asset and an unknown total.
- Confirm incomplete content is absent from dataset discovery.
- Interrupt the second upstream body and await provider partial-file cleanup.
- Resume using a new producer instance; the first asset is not fetched again.
- Verify exact original bytes, measured sizes and hashes after commit.
- Confirm the downloader created only a JSON journal, without intermediate GRIBs.
- Delete only that test publication/dataset and verify preservation of the
  pre-existing dataset IDs, other staging entries and all plugin configurations.

No authentication token is written into project files or printed by either helper.

## Isolated validation

The full suite passes **60 tests, zero failures and zero skipped tests** under
Node 24 in a temporary sandbox directory, including four integration cases using
the actual installed provider 0.1.1 code in independent temporary storage. These
cover real receiving progress, interrupted uploads, size limits and lost commit
responses. Additional HTTP fault tests cover lost create/upload responses,
generic 404s, incorrect hash receipts, truncated GRIB streams and cancellation.
A 32 MiB slow-receiver test confirms backpressure reaches the upstream socket.

## Recorded live result — 2026-10-21

Version 0.4.0-beta.2 is installed and enabled on the existing sandbox. Its backup
is `local-packages/grib-downloader-before-forecast-2026-10-21T18-17-28-841Z`.
Provider/browser remain at 0.1.1; the Weather Provider remains at 0.7.0-beta.1.

The direct-stream helper passed every live check listed above using the real GFS
regional fixture and the installed producer. The incomplete second asset had
unknown total length, cancellation removed its partial upload, and a fresh
producer instance fetched only that missing asset. The committed original bytes
and measured hashes matched. The downloader's temporary data held only a JSON
journal. The uniquely named fixture was deleted after verification.

The two pre-existing GFS/AROME dataset IDs were preserved, along with all 46
plugin configurations throughout the fixture test. Installation changed only the
downloader configuration's obsolete directory option; its operational settings
and all other configuration files are unchanged. Existing GFS bytes (129 assets,
46,285,067 bytes) still match the saved receipts and HTTP Range remains usable.
The Weather API still reports 129 GFS slices and 52 AROME slices without errors.

A further readonly check streamed every existing asset through the new GRIB2
framing validator: all 129 GFS assets and all 9 AROME assets (465,623,211 bytes)
passed. No existing dataset was republished for this verification. Legacy local
directories were retained; no new GRIB files are created by the downloader.

## Sandbox helper configuration

The deployment and verification helpers require `SIGNALK_NODE_CONFIG_DIR` to
point to the absolute Signal K configuration directory inside the selected
container. Pass it explicitly with Docker (for this sandbox,
`--env SIGNALK_NODE_CONFIG_DIR=/home/node/.signalk`) alongside the existing
`SIGNALK_CONTAINER_ID=signalk-sandbox` guard. The helpers do not assume a home
directory and are excluded from the npm archive.

## Automatic access requests (0.4.0-beta.3, 2026-10-21)

All 76 tests passed on Node 24, including 15 access lifecycle and plugin-host
checks. Two synthetic devices were also requested against the actual sandbox
server, approved through its admin API with readwrite and readonly permissions,
and verified against the Forecast Dataset Provider with their issued tokens.
The requests were visible through the same API as the Signal K Admin UI. Token
files used mode 0600. The two test devices and temporary token files were removed;
existing devices and all 46 plugin configurations were preserved. This check did
not redeploy either installed plugin.
