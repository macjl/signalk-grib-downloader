# Legacy export sandbox verification — 2026-11-08

Version **0.6.0**, runtime commit `62b8e585f89bfb81aa04e7675a6e2746235e65ec`, was
installed and tested on `signalk-sandbox` with Node 24.22.1. The existing plugin
package, operational settings, root package metadata and historical GRIB directory
were backed up before installation. No dependency tree was re-resolved.

## Transition defaults and real data

The original plugin options contained neither `legacyExportEnabled` nor
`gribsRoot`. After restart, export activated by default in
`/home/node/.signalk/gribs` without adding an activation setting. All 42 installed
package files matched the prepared archive.

| Source | Run | Exported GRIB files | Bytes |
| --- | --- | ---: | ---: |
| GFS 0.25° | 20261108T06 | 129 | 47,806,291 |
| AROME 0.025° | 20261108T06 | 9 | 479,928,829 |
| GFS-Wave 0.25° | 20261108T06 | 129 | 3,058,607 |
| **Total** | | **267** | **530,793,727** |

Every exported original matched the catalogue's declared size and SHA-256.
Historical filenames and exact `{ "params": ... }` marker contents were checked.
Existing acquisitions were copied from the provider without reacquiring those
runs from the weather services.

## Failure, retention and UI checks

Three small, uniquely named **analysis** datasets used a real GFS GRIB sample.
Keeping the fixtures out of forecast discovery prevented interference with the
Weather Provider. Checks passed for:

- newest run in the active source directory, one prior run in `archive/`;
- repair of a deliberately corrupted export while the catalogue original stayed
  unchanged, proving the outputs are independent copies;
- filesystem failure reported separately, followed by successful retry;
- interrupted export leaving the preceding completion marker intact;
- removal of only the synthetic datasets and their export directory afterwards.

Chromium exercised the actual installed webapp in French. The storage warning
was visible with export enabled and explicitly explained provider plus local
copies and approximately doubled GRIB disk usage. Explicitly disabling the
option hid the warning and left all output files untouched. Original options
were restored, export became active again, and no page JavaScript errors occurred.
The config schema also showed the checkbox checked by default and the same disk
usage warning.

## Preservation and automated validation

The six pre-existing datasets, all 46 plugin configuration files, operational
settings, security configuration and 43 unrelated root dependencies were
preserved. Existing runtime dependencies were reused. Original GRIB files were
backed up before the transition's configured archive retention was applied.
Backup manifests and the detailed redacted report remain under the sandbox's
`local-packages/grib-downloader-before-legacy-*` directories.

Automated validation passed: **106 tests on Node 24**, including the actual
Dataset Provider integration, and **18 export tests on Node 20**. Signal K's
reusable CI also passed for the runtime commit. Browser and live fixture
credentials were kept in memory and were not saved in this repository.
