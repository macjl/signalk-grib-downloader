# signalk-grib-downloader

Signal K plugin that **schedules GRIB downloads** for
[signalk-grib-weather-provider](https://github.com/macjl/signalk-grib-weather-provider) —
automatic after each model run, or on demand for intermittent connections —
with a management webapp.

## What it does

Each configured source (model + resolution) downloads into
`<root>/<model>-<resolution>/` through an in-process fetcher — no container
runtime, no image pulls. Point signalk-grib-weather-provider's root at the
same directory and every source is served as a weather provider automatically.

Supported models: **GFS** (0.25°/0.5°/1°, NOMADS server-side area subsetting),
**AROME** (0.025°/0.01°), **ARPEGE** (0.1°/0.25°), **ICON-EU**.

## Modes

- **Auto** — knows each model's publication schedule (cadence + delay) and
  downloads every run as soon as it is published. Run availability is probed
  upstream, so a not-yet-published run shows as *awaiting publication*, not an
  error.
- **Manual** — nothing downloads without an explicit trigger (webapp button or
  `POST /plugins/signalk-grib-downloader/download[/<source>]`). Made for
  metered satellite connections.

## The webapp

Everything operational is managed in the webapp (Webapps → GRIB Downloader),
not in the plugin config panel:

- **Download area** drawn on a map (draggable rectangle corners) — applied to
  GFS, the only model with server-side subsetting
- **Sources**: model, resolution, forecast duration on a slider showing the
  model maximum, past-runs archive count, enable/disable
- **Volume estimation** per source and per run, following the area and
  duration live
- **Status**: per-source badges (up to date / downloading / awaiting
  publication / settings changed / behind / error) with the last download log
  expandable for analysis
- Deleting a source optionally deletes its downloaded data

Settings are stored in the plugin's data directory (`settings.json`), written
only through the webapp — the admin config panel only holds infrastructure
(the GRIB root directory).

## Data lifecycle

- A run is downloaded atomically and finished with a marker recording the
  **fetch fingerprint** (area, duration, groups, variables…). Changing any of
  those re-fetches the data; nothing goes stale silently.
- When a newer run completes, the previous one is deleted — or moved to
  `<source>/archive/` if the source keeps archives (configurable count, no
  upper limit). Leftovers of interrupted runs are purged.
- Old data is **never deleted while offline**: cleanup only happens when a
  newer run has been fully downloaded.

## Requirements

- Signal K server ≥ 2.x with Node ≥ 18
- [signalk-grib-weather-provider](https://github.com/macjl/signalk-grib-weather-provider)
  ≥ 0.2.0 to serve the downloaded data

No container runtime is required — downloads run in-process. (ICON-EU
downloads decompress `.bz2` GRIB fragments in-process via a bundled wasm
bzip2 library.)

## License

Apache-2.0
