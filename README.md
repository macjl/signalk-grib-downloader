# signalk-grib-downloader

Signal K plugin that **downloads GRIB forecasts and publishes them as
`forecastDatasets`** through
[Forecast Dataset Provider](https://github.com/macjl/signalk-forecast-dataset-provider)'s
experimental HTTP contract 0.1 with its streaming extension (stable provider 0.1.0 or later).
Automatic publication follows each model run;
manual downloads remain available for intermittent connections.

**Upgrading from 0.3.x:** update Weather Provider to 0.7.0 in the same maintenance
window and install Forecast Dataset Provider 0.1.0. The complete chain requires
Node.js 24+. Follow the [coordinated upgrade guide](docs/upgrading.md) for access
approval, existing-file handling and verification.

## What it does

Each configured source streams its selected model run directly into a staged
multi-asset publication in Forecast Dataset Provider. GFS, GFS-Wave and Météo-France bodies
are forwarded with backpressure; ICON-EU fragments are decompressed in memory,
one variable at a time, and streamed into one asset per timestep. The downloader
writes no GRIB, partial file, completion marker or local archive.

The provider measures each asset's actual length and SHA-256 during reception.
Only an explicit commit after all upstream streams complete makes the dataset
usable. Consumers discover datasets through `/signalk/v2/api/resources/forecastDatasets`
and retrieve their version-bound assets over HTTP.
No container runtime, Python or GRIB decoder is required by this downloader.

Supported models: **GFS** (0.25°/0.5°/1°, NOMADS server-side area subsetting),
**GFS-Wave** (0.25°, NOMADS server-side area subsetting), **AROME** (0.025°/0.01°),
**ARPEGE** (0.1°/0.25°), **ICON-EU**.

### GFS-Wave

Add a **GFS-Wave** source in the webapp (model `gfs-wave`, resolution `0p25`).
It publishes separate `gfs-wave-0p25` forecast datasets with model `GFS-WAVE`;
atmospheric GFS sources, journals and retention remain independent. The shared
download-area rectangle applies to both products. Manual/automatic mode and
archive retention work as for the other sources. No wave source is enabled
automatically when upgrading.

Each acquisition selects NOAA's `HTSGW` (combined wind-wave and swell significant
height), `WVDIR` (wind-wave direction) and `WVPER` (wind-wave period) at the surface,
with one original GRIB2 asset per three-hour step, from f000 to the configured
horizon (maximum 384 hours). The directions/periods are wind-wave parameters,
not directions/periods for the entire combined sea. Publication waits for all
requested wave assets; it never depends on an atmospheric acquisition.

The files come from `gfs.YYYYMMDD/HH/wave/gridded/` and `filter_gfswave.pl`.
These URL/parameter choices follow [haipule's contribution in PR #19](https://github.com/macjl/signalk-grib-downloader/pull/19).
Originals stream directly into the catalogue, without local assembly or OpenCPN
export. Weather API decoding of wave-only datasets is separate work: the current
GRIB Weather Provider does not recognize these wave fields and reports a
conversion error for this source, while retaining its atmospheric forecasts.
Wave download-volume estimates are shown as unavailable until measured.

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
| GFS-Wave | 6 hours | 210 min | 330 min |
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
a complete dataset only after all selected assets have arrived successfully.
Upper bounds conservatively
cover the complete supported horizon and both Meteo-France resolutions; they
may therefore be generous for shorter forecasts. They are estimates, not an
upstream availability guarantee. An exceptional late publication does not move
the bounds automatically or slow down subsequent probes.

GFS-Wave initially uses the same estimated window as atmospheric GFS, but probes
its own last requested wave file. This is an initial scheduling estimate, not a
measured wave publication SLA; a late or missing wave run continues to be probed.

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
**Forecast Dataset Provider 0.1.0 or later**, with streamed publications. The previous Weather Provider dependency is
replaced by Forecast Dataset Provider. Weather API decoding remains a separate
consumer responsibility; provider 0.1 does not decode, index or subset GRIBs.
Publication to the catalogue is mandatory for automatic and manual downloads.
Other plugins consume published datasets through the catalogue API.

In Plugin Config:

- `catalogueUrl`: Signal K server base URL, default `http://127.0.0.1:3000`.
  Adjust for another port or deployment prefix. Remote endpoints require HTTPS.
- `catalogueTokenFile`: absolute path to a **readwrite application token**,
  default `<plugin data directory>/catalogue-token`. When the file is missing,
  the plugin automatically requests access. In the catalogue server's Signal K
  Admin UI, open **Security → Access Requests** and approve the
  **signalk-grib-downloader** request with **readwrite** permissions. The plugin
  saves the approved token with permissions `0600` and resumes automatic work.
  No file needs to be created manually. Pending requests survive plugin restarts;
  a denied request requires a plugin restart to submit a new one.

Existing token files are reused and reread for rotation. Automatically issued
tokens rejected with HTTP 401 trigger a fresh approval request; externally
configured tokens are preserved for their owner to rotate. Token contents and
access request IDs never enter the webapp, status responses or logs. On a remote
catalogue, approve the request on that server and enable device access requests
there. Configure a separate token file when switching catalogue servers.

The provider owns all acquisition bytes, including unfinished publications and
archives. The downloader retains only settings and a private publication journal.
Old `gribsRoot` options are ignored; existing directories are left untouched.
Committed journals from beta.1 are reused without reading old files or markers,
so already published acquisitions do not need an upstream download. Unpublished
legacy files are not imported automatically; the next eligible acquisition is
streamed from its upstream source.

An unavailable provider or invalid token reports an error before any upstream
body is opened. The next automatic check or manual trigger retries the acquisition.
Automatic work still obeys offline, captive and metered-connection settings.

## Data lifecycle

- Declare the expected asset identifiers, with unknown sizes, then stream their
  bodies to the provider. Its acquisition view exposes real received bytes and
  completed assets; total bytes remain unknown until all lengths are known.
- A journal in `<plugin data directory>/catalogue-publications/` records model
  cycles, fetch parameters, acquisition UUIDs, staging/dataset IDs and measured
  hash receipts. It is fsynced and atomically replaced. No upstream credentials
  or GRIB bytes enter this journal.
- Resume after interruption at **asset level**: completed provider assets are
  retained and only missing assets are fetched again. Partial-byte resume is
  not implemented. A new acquisition must complete before retention removes an
  earlier committed dataset.
- Recover lost create responses through an acquisition UUID persisted before the
  POST and stored in provenance; recover lost upload/commit responses through
  publication status and the saved hash/dataset receipts. A generic 404 never
  means deletion: only explicit `DATASET_NOT_FOUND` or `PUBLICATION_NOT_FOUND`
  responses authorize clearing the corresponding reference.
- GRIB2 framing is checked while streaming (message lengths and terminators).
  A transport error, empty stream, HTML response or truncated GRIB prevents
  commit and aborts the upload. Framing does not prove meteorological field or
  geographic completeness; verified inventory still requires an indexer.
- Metadata declares source, model and cycle. Requested parameters remain claims,
  never verified coverage or fields. Strict verified searches exclude these
  datasets until an independent trusted indexer supplies inventory.
- Keep the newest committed acquisition plus `archiveRuns` previous acquisitions
  **in the catalogue**. Only IDs in this downloader's journal are eligible for
  deletion. Provider conflicts defer cleanup until another publication.
- Deleting source data in the webapp deletes its recorded catalogue acquisitions
  and staging publications. Removing only the source configuration leaves them
  intact. Old filesystem directories are never scanned or removed automatically.
  Settings changes and deletion are blocked while a transfer runs. Plugin stop
  aborts both HTTP sides and waits for the active transfer.

A source has a one-hour acquisition timeout; upstream streams also have a
120-second inactivity timeout. GFS/Météo-France use bounded stream buffers.
The bzip2 WASM API requires a whole fragment in memory: ICON is processed one
variable at a time, with a 128 MiB compressed and 512 MiB decompression ceiling
per fragment, rather than buffering an entire timestep or model run.

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

Tests cover model plans, direct streaming, GRIB framing, cancellation,
asset-level restart recovery, lost create/upload/commit responses, generic 404s,
retention conflicts and the unchanged publication-window scheduler.

For integration with the actual provider source (build it first):

```sh
FORECAST_DATASET_PROVIDER_PATH=/absolute/path/to/signalk-forecast-dataset-provider npm test
```

Without that variable, the four cross-plugin HTTP tests explicitly skip; the
normal suite still exercises the producer against a fault-injecting HTTP server.
See [sandbox verification](docs/sandbox-verification.md) for the live checks.

## License

Apache-2.0
