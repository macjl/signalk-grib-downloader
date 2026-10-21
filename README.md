# signalk-grib-downloader

Signal K plugin that **downloads GRIB forecasts and publishes them as
`forecastDatasets`** through
[Forecast Dataset Provider](https://github.com/macjl/signalk-forecast-dataset-provider)'s
experimental HTTP contract 0.1. Automatic publication follows each model run;
manual downloads remain available for intermittent connections.

## What it does

Each configured source downloads into the private
`<plugin data directory>/downloads/<model>-<resolution>/` workspace, then
publishes all files for the completed acquisition as one immutable multi-asset
dataset. Consumers discover datasets through `/signalk/v2/api/resources/forecastDatasets`
and follow version-bound HTTP asset URLs; they do not need shared directories.
The provider owns its stored content independently of the local download cache.
No container runtime, Python or GRIB decoder is required by this downloader.

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
  model maximum, past-runs archive count, enable/disable
- **Volume estimation** per source and per run, following the area and
  duration live
- **Status**: per-source badges (up to date / downloading / awaiting
  publication / publication late / settings changed / behind / error) with the last download log
  expandable for analysis
- Deleting a source optionally deletes its downloaded data

Settings are stored in the plugin's data directory (`settings.json`), written
only through the webapp — the admin config panel only holds infrastructure
(catalogue connection and scheduler settings).

## Catalogue connection

Requires **Signal K 2.34.x or later within 2.x, Node 20.19+**, and an enabled
**Forecast Dataset Provider 0.1.x**. The previous Weather Provider dependency is
replaced by Forecast Dataset Provider. Weather API decoding remains a separate
consumer responsibility; provider 0.1 does not decode, index or subset GRIBs.
Publication to the catalogue is mandatory for automatic and manual downloads.
Other plugins consume published datasets through the catalogue API.

In Plugin Config:

- `catalogueUrl`: Signal K server base URL, default `http://127.0.0.1:3000`.
  Adjust for another port or deployment prefix. Remote endpoints require HTTPS.
- `catalogueTokenFile`: absolute path to a **readwrite application token**,
  default `<plugin data directory>/catalogue-token`. Approve a dedicated device
  through Signal K's standard Access Requests API and store its token here with
  permissions `0600` for the server user. File contents never enter the webapp,
  status response, journal, provenance or logs. Rotate by replacing the file.

The acquisition workspace is private and has no configurable shared-directory
output. Old `gribsRoot` options are ignored, and their directories are left
untouched. To reuse completed downloads when upgrading, stop the downloader and
copy the old source directories (including completion markers) into
`<plugin data directory>/downloads/` before restarting. Do not copy or edit the
catalogue provider's storage. Operational source settings and existing published
datasets remain intact.

An unavailable provider or invalid token leaves publication pending with an
error in the webapp. The next automatic check or manual trigger retries it
**before downloading upstream again**. Automatic work still obeys offline,
captive and metered-connection settings. Completed runs in the private workspace
are published on their next automatic check or manual trigger; no upstream
re-download is needed. The workspace retains originals for transfer recovery
and configured archives, and is not an integration API.

## Data lifecycle

- A completion marker records the download fingerprint and file manifest.
  A complete local download and a committed catalogue dataset are distinct:
  status includes `publicationPending` and a `catalogue` reference (dataset ID,
  version and discovery URL). A source becomes up to date after publication.
- Files stream over authenticated HTTP with exact lengths and SHA-256 hashes.
  No GRIB is buffered wholesale for catalogue upload. Content becomes visible
  only after atomic commit of every asset.
- A private journal in `<plugin data directory>/catalogue-publications/` records
  staging IDs, hashes and committed references. Restart resumes only missing
  assets. A lost commit response is recovered using the saved dataset ID.
  Contract 0.1 has no idempotency key for a lost *create-publication* response;
  such an orphaned staging entry must be removed through the provider API.
- Metadata declares source, model and cycle; requested fetch parameters are
  retained as acquisition claims. This downloader never marks requested bounds,
  forecast periods or fields as **verified inventory**. A trusted independent
  indexer must supply that metadata. Strict coverage/field searches therefore
  exclude these datasets until indexed.
- Once a newer acquisition commits, keep it plus `archiveRuns` previous
  acquisitions in the catalogue. Retention touches only dataset IDs in this
  downloader's journal, respects provider deletion conflicts and retries
  deferred cleanup on a later publication. Local archives follow the existing
  run-based policy. No older run is cleaned up merely because the link is offline.
- Deleting source data from the webapp deletes both recorded catalogue data and
  local files. Deleting just the source configuration leaves both intact.
  Busy transfers block settings changes and data deletion; stopping the plugin
  aborts and waits for active transfers so a restart cannot race its journal.

Keep the publication journal with the plugin's settings when moving servers.
Changing catalogue servers requires preserving that old journal separately.
Do not edit the provider's managed storage or delete journal files to implement
retention.

## Development

```sh
npm ci
npm test
npm pack
```

Tests cover model downloads, scheduling, HTTP publication, asset-level restart
recovery, a lost commit response, hashes, catalogue deletion/republication,
retention conflicts and publication-pending status. See
[the sandbox verification](docs/sandbox-verification.md) for deployment results.

## License

Apache-2.0
