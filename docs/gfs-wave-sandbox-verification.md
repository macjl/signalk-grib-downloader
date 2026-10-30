# GFS-Wave sandbox verification — 2026-10-30

Local sandbox build: **0.5.0-alpha.1**, branch `feature/gfs-wave-catalogue`.
This build is installed only on `signalk-sandbox`; no npm release was created.

## Automated checks

- All 80 tests pass on Node 24.21.0 with the actual Forecast Dataset Provider
  enabled through `FORECAST_DATASET_PROVIDER_PATH`; no skipped tests.
- New coverage checks native wave filter URLs, requested surface fields, last-step
  probes, stable independent identity, horizon and area fingerprints, unsupported
  resolution rejection, settings/status and deletion after removing a source.
- Actual-provider coverage interrupts a wave acquisition after atmospheric data
  is committed, resumes only missing wave assets, and verifies that wave retention
  and deletion never remove the atmospheric acquisition.
- A Chromium check using the HTML retrieved from the installed sandbox verifies
  the GFS-Wave selector, sole 0p25 resolution, 384-hour maximum, six-hour manual
  settings payload and successful save feedback without page errors. API writes
  were intercepted locally; this check did not edit sandbox settings.

## Deployment and real acquisition

Only GRIB Downloader was replaced, retaining its existing runtime dependencies.
The sandbox runs Node 24.22.1. The container image, overlays and other plugin
packages were preserved. Before replacement, the downloader was stopped through
its configuration API, its package/data and dependency manifests were backed up,
and its exact original configuration was restored for the container restart.

Backup inside the sandbox:
`/home/node/.signalk/local-packages/grib-before-gfs-wave-2026-10-30T22-30-28-432Z`.
The installed archive is in the same `local-packages` parent directory.

The installed downloader selected the available **20261030T18** GFS-Wave run and
streamed f000 and f003 for the small Mediterranean area 42–44°N, 6–8°E directly
from NOAA into the sandbox catalogue. The resulting independent dataset had model
`GFS-WAVE`, requested source `gfs-wave-0p25`, two assets and **1483 bytes** total.

Each downloaded asset's size and SHA-256 matched the provider receipt. ecCodes
verified three surface wave messages per asset on the native 0.25-degree grid:
combined wind-wave/swell significant height, mean wind-wave period and wind-wave
direction. Wave cell centres need not coincide with atmospheric GFS grid nodes.
The downloader's scratch directory contained only its publication journal,
with no intermediate GRIB files.

The acquisition created by this check was deleted afterward. Final checks
confirmed all **46 plugin configuration files**, the downloader's operational
settings and all **5 preexisting catalogue datasets/content versions** were
preserved. Existing GFS and AROME sources remain configured; no new automatic
wave download was enabled.

## User testing

Reload the GRIB Downloader webapp, add a GFS-Wave source, choose the duration and
download area, save, then trigger a download (or enable automatic downloads).
Its independent acquisition appears in Forecast Dataset Browser as `GFS-WAVE`.

This step adds acquisition/catalogue publication only. There is no OpenCPN
export. The current GRIB Weather Provider cannot decode these wave-only fields
and reports a conversion error for that source while retaining atmospheric
forecasts; Weather API wave support is a separate follow-up.
