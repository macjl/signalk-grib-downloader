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

- **Auto** — follows each model's publication windows and probes for a new
  run from its lower bound. Before the upper bound, missing files mean
  *awaiting publication*. Beyond it, the status becomes *publication late*;
  probing continues at exactly the same interval.
- **Manual** — nothing downloads without an explicit trigger (webapp button or
  `POST /plugins/signalk-grib-downloader/download[/<source>]`). Made for
  metered satellite connections.

## Publication schedule

`MODEL_SCHEDULES` in `src/scheduler.ts` is the single source of cycle cadence,
probe interval, and publication windows for both the scheduler and downloader.
All offsets below are minutes after the **UTC cycle**, not local clock times.
The default probe interval is **5 minutes** for all four models. The plugin's
`checkIntervalMinutes`, if explicitly configured (including a migrated legacy
setting), overrides this interval; it is no longer a special late-run fallback.

| Model | Cycle interval | Lower bound | Upper bound |
| --- | --- | --- | --- |
| GFS | 6 hours | 210 min | 330 min |
| AROME | 3 hours | Per-cycle table below | Per-cycle table below |
| ARPEGE | 6 hours | Per-cycle table below | Per-cycle table below |
| ICON-EU | 6 hours | 150 min | 240 min |

| UTC cycle | AROME lower / upper (min) | ARPEGE lower / upper (min) |
| --- | --- | --- |
| 00 | 110 / 185 | 165 / 255 |
| 03 | 100 / 180 | - |
| 06 | 240 / 350 | 210 / 330 |
| 09 | 190 / 285 | - |
| 12 | 175 / 275 | 145 / 270 |
| 15 | 170 / 255 | - |
| 18 | 240 / 340 | 210 / 300 |
| 21 | 190 / 280 | - |

The lower bound allows the first forecast files to appear, so a short requested
horizon need not wait for the model's full forecast. The downloader probes the
last requested forecast step/group, downloads the requested files, and publishes
a completion marker only after all downloads succeed. Upper bounds conservatively
cover the complete supported horizon and both Meteo-France resolutions; they
may therefore be generous for shorter forecasts. They are estimates, not an
upstream availability guarantee. An exceptional late publication does not move
the bounds automatically or slow down subsequent probes.

For example, the AROME 06 UTC run starts probing at 10:00 UTC. If it is still
missing at 11:50 UTC, the status changes to late and the same five-minute probing
continues. A newly opened cycle is checked even if an older cycle is still late.
Once a newer run is complete, older missing cycles no longer need probing.
Each source keeps its own last-probe time, so another model's timer cannot
trigger repeated early checks or push back an existing retry.

Offline and captive connections suspend automatic work. On a metered connection,
`meteredIntervalMultiplier` stretches the probe interval uniformly before and
after the upper bound. Publication thresholds themselves remain absolute UTC
times. Manual downloads use the same eligible cycles and bypass the automatic
probe interval.

### Basis for the initial windows

These are deliberately bounded starting estimates, combining the existing
supported cycles with small margins around observations:

- [NCEP production status](https://www.nco.ncep.noaa.gov/pmb/nwprod/prodstat/index.html)
  and NOMADS GFS 0.25-degree index headers for 2026-10-19: first files appeared
  about 211-215 minutes after the cycle, f384 about 311-315 minutes after it.
- Meteo-France public bucket HEAD `Last-Modified` observations for 2026-10-17
  through 2026-10-19, for SP1's last group at both AROME and ARPEGE resolutions,
  plus first-group observations on 2026-10-19. These confirm the per-cycle
  differences described in [issue #14](https://github.com/macjl/signalk-grib-downloader/issues/14).
  Some samples exceeded seven hours; these remain late events, rather than
  becoming normal deadlines. ARPEGE 0.25-degree full runs reached 313 minutes
  after 06 UTC, hence its wider upper bound.
- [DWD's ICON model documentation](https://www.dwd.de/SharedDocs/downloads/DE/modelldokumentationen/nwv/icon/icon_dbbeschr_aktuell.pdf?view=nasPublication)
  confirms the four full-horizon ICON-EU cycles. Headers from the 2026-10-19
  12/18 UTC `t_2m` files showed initial steps around 160-164 minutes and f120
  around 216 minutes. The five-day horizon keeps the 6-hour cycle cadence;
  shorter intermediate ICON runs are not added by this change.

## The webapp

Everything operational is managed in the webapp (Webapps → GRIB Downloader),
not in the plugin config panel:

- **Download area** drawn on a map (draggable rectangle corners) — applied to
  GFS, the only model with server-side subsetting
- **Sources**: model, resolution, forecast duration on a slider showing the
  model maximum, past-runs archive count, enable/disable. GFS 0.25 sources can
  optionally add GFS-Wave data and publish an OpenCPN run bundle.
- **Volume estimation** per source and per run, following the area and
  duration live
- **Status**: per-source badges (up to date / downloading / awaiting
  publication / publication late / settings changed / behind / error) with the last download log
  expandable for analysis
- Deleting a source optionally deletes its downloaded data

Settings are stored in the plugin's data directory (`settings.json`), written
only through the webapp — the admin config panel only holds infrastructure
(the GRIB root directory and the separate OpenCPN export root).

## GFS-Wave and OpenCPN export

For a **GFS 0.25** source, enable **Waves + OpenCPN export** in the webapp to:

1. download `HTSGW`, `WVDIR`, and `WVPER` from the matching GFS-Wave cycle,
   forecast hour, bounding box, and 0.25-degree grid;
2. append those messages to each normal atmospheric step file; and
3. publish all completed steps, in forecast-hour order, as one GRIB2 file:
   `<OpenCPN export root>/gfs-0p25/gfs-0p25__<run>__complete.grb2`.

The export root defaults to `~/.signalk/gribs-opencpn` and is configurable in
the Signal K plugin settings. It must stay separate from the normal GRIB root:
the weather provider should scan only the normal root, not the complete-run
exports. The downloader rejects an export path that resolves to the provider's
source directory.

Both the per-step file and the complete-run export are published atomically.
The run marker is written only after atmosphere, waves, and the OpenCPN bundle
all succeed. Existing configurations remain atmosphere-only until the option
is enabled. If a completed run's export is removed, the next check rebuilds it
from the existing step files without downloading those steps again.

### Test and acceptance boundary

The downloader's automated and live acceptance tests end when a complete-run
GRIB2 file has been created and validated. Acceptance requires that:

- all requested forecast steps were downloaded and the completion marker was
  published only after the last successful step;
- every combined step contains the requested atmospheric and wave messages;
- the complete-run file was published atomically in forecast-hour order;
- the result is recognised as GRIB edition 2 and contains the expected fields,
  forecast times, geographic coverage, and run timestamp; and
- a failed or interrupted run leaves the previous complete data usable.

Loading, displaying, and using that file in OpenCPN or its Weather Routing
plugin are downstream manual integration tests. They are useful release
checks, but they are not part of the downloader's automated acceptance scope
and do not block completion of a downloader test run.

When enabling waves or otherwise changing GFS fetch parameters for an already
downloaded model cycle, the replacement is built in a hidden staging directory.
The old step files and marker remain active until the new steps and optional
OpenCPN bundle are complete. The commit keeps a temporary backup and restores
the previous run if switching files fails; an incomplete rollback retains that
backup for manual recovery instead of deleting it.

## Data lifecycle

- A run is downloaded atomically and finished with a marker recording the
  **fetch fingerprint** (area, duration, groups, variables…). Changing any of
  those re-fetches the data; nothing goes stale silently.
- When a newer run completes, the previous one is deleted — or moved to
  `<source>/archive/` if the source keeps archives (configurable count, no
  upper limit). Leftovers of interrupted runs are purged.
- Old data is **never deleted while offline**: cleanup only happens when a
  newer run has been fully downloaded.
- OpenCPN exports follow the source's archive count: the current bundle plus
  the configured number of past bundles are retained in the export directory.

## Requirements

- Signal K server ≥ 2.x with Node ≥ 18.17
- [signalk-grib-weather-provider](https://github.com/macjl/signalk-grib-weather-provider)
  ≥ 0.2.0 to serve the downloaded data

No container runtime is required — downloads run in-process. (ICON-EU
downloads decompress `.bz2` GRIB fragments in-process via a bundled wasm
bzip2 library.)

## License

Apache-2.0
