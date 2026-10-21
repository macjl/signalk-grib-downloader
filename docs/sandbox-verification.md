# Sandbox verification — Forecast Dataset Provider integration

Verified on 2026-10-21 against the existing `signalk-sandbox` container:
Signal K 2.34.0, Node 24.21.0, Forecast Dataset Provider 0.1.0.
Downloader branch: `feature/forecast-dataset-provider`, package 0.4.0-beta.1.

## Deployment

The subsequent catalogue-only change passes all 50 tests and removes the
`gribsRoot` setting and standalone orchestrator path. It has not yet replaced
the installed sandbox package. Before installing it, stop the downloader and
copy its old source directories (including completion markers and archives)
into `<plugin data directory>/downloads/`. The helper removes only the obsolete
`gribsRoot` setting from this plugin's configuration; other settings are retained.

Build with `npm ci && npm test && npm pack`. The sandbox-specific helper
`tools/install-sandbox.cjs` runs as root **inside the selected container**, with
the absolute archive path under `/home/node/.signalk/local-packages` as its
argument. It asserts `SIGNALK_CONTAINER_ID=signalk-sandbox`; it is not a general
installer. Copy the helper into the container before invoking it, then restart
that existing container to reload the package. Do not recreate the Compose
project, pull a new server image or resolve the sandbox's entire npm tree.

The helper archives the previous installed plugin, saves package manifests,
plugin data and configuration hashes, and provisions a dedicated **readwrite**
application token through the standard Signal K Access Requests API. A temporary
admin token exists only in memory. The producer token is stored as a server-user
owned `0600` file and is never logged. Existing plugin configurations, source
settings and dependencies are preserved. Existing Weather Provider 0.6.0 remains
enabled against the local GRIB directory.

Initial rollback material is under
`/home/node/.signalk/local-packages/grib-downloader-before-forecast-2026-10-21T09-59-43-656Z`.
`local-packages/grib-downloader-forecast-backup.json` identifies the latest
installation backup. It contains sensitive security/application-token backups;
keep its private directory permissions. To roll back, stop the downloader,
restore its archived package and package manifests, then restart the container.
Catalogue datasets remain owned by the provider and need not be deleted.

## Live verification

`tools/verify-sandbox.cjs` runs inside the same container, uses short-lived admin
authentication for downloader status, and uses the independent readwrite
producer token for catalogue discovery/content. It makes no test datasets and
does not delete real downloads.

The initial real acquisition published successfully:

- Source `gfs-0p25`, model run `20261021T00`, 129 GRIB assets.
- 46,261,938 original bytes, all SHA-256 hashes verified through HTTP and
  compared byte for byte with the existing local files.
- Dataset `9108256e-e0fd-42bc-8e44-bbdc109f4a32`, discovered through the core
  `/signalk/v2/api/resources/forecastDatasets` API.
- HTTP Range returns 206; anonymous content access is denied.
- Inventory remains `unverified`; strict verified discovery excludes it.
- All 45 pre-existing plugin configuration files and operational downloader
  settings are unchanged. Producer permission is `readwrite`, not admin.
- Downloader status has a catalogue reference, no pending publication and no
  error. The existing files were published without fetching them upstream again.

Local tests additionally cover failed-upload restart recovery, no duplicate
uploads after a lost commit response, external dataset deletion, changed fetch
parameters, archive retention, provider deletion conflicts, missing local files,
authentication failure, legacy empty markers and cancellation on plugin stop.
Real GRIB decoding/indexing, automatic processing and Weather API catalogue
consumption are outside provider 0.1 and are not claimed by this change.
