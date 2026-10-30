# Coordinated forecast-plugin upgrade

Stable versions: Forecast Dataset Provider **0.1.0**, GRIB Downloader **0.4.0**,
GRIB Weather Provider **0.7.0**, Forecast Dataset Browser **0.1.0**.
The catalogue HTTP contract remains **0.1**; it is not an adopted Signal K standard.

## Requirements and upgrade from local-file releases

The complete chain requires **Node.js 24+** (the Weather Provider's ecCodes
WebAssembly decoder needs memory64) and **Signal K 2.34+ within 2.x**.
Downloader 0.3.x and Weather Provider 0.6.x shared local files. Upgrade both
together: Downloader 0.4.x publishes only to the catalogue, and Weather Provider
0.7.x reads only from the catalogue. Updating only one breaks that exchange.

1. Back up the Signal K configuration, plugin data and existing GRIB directories.
   Check the running server's Node version before installing.
2. Install and enable Forecast Dataset Provider 0.1.0. Its catalogue directory
   must be on a local filesystem supporting hard links and atomic rename.
3. Update Downloader to 0.4.0 and Weather Provider to 0.7.0 in the same maintenance
   window, then restart the server. Install Browser 0.1.0 to inspect the catalogue.
   For a manual installation, stop the server and run this from its configuration
   directory before restarting it:

   ```sh
   npm install signalk-forecast-dataset-provider@0.1.0 signalk-grib-downloader@0.4.0 signalk-grib-weather-provider@0.7.0 signalk-forecast-dataset-browser@0.1.0
   ```

4. In Plugin Config, verify both plugins' catalogue server URL. On the catalogue
   server, enable device access requests and approve the separate requests in
   **Security → Access Requests**: **readwrite** for GRIB Downloader, **readonly**
   for GRIB Weather Provider. Valid existing token files are reused. Do not give
   the consumer the producer's token.
5. Allow a new acquisition, or trigger one manually when connectivity permits.
   Old shared GRIB files are left untouched and **are not imported automatically**.
   Until a complete acquisition is published and decoded, its Weather API source
   has no catalogue forecasts. Consider the download cost on metered connections.
6. In Browser, check that acquisition progress becomes a complete dataset. Check
   the Weather Provider's status and a Weather API point request inside its actual
   forecast area and time range. Existing source-based Weather API provider IDs
   remain selectable. Readonly sessions can browse/download; deletion and pending
   acquisition monitoring require readwrite/admin permission.

## Upgrade from the catalogue betas

Published datasets, staged uploads, downloader journals and valid tokens are
reused. No new upstream download is required for an already published acquisition.
Weather Provider rebuilds decoded caches once from its verified private GRIB
mirror, including during a catalogue outage. This takes CPU and temporary disk
space; originals and content versions remain unchanged. A failed new conversion
reports an error instead of publishing a partial source.

## Recovery and limits

Keep the backup and old local GRIB directories until the new chain works. To
return to the local-file architecture, restore Downloader 0.3.x and Weather
Provider 0.6.x together with their backed-up configuration. Do not delete or
restore catalogue content merely to roll back plugin code.

The catalogue stays format-agnostic. This release does not add Zarr indexing,
subsetting processors, GFS-Wave acquisition or OpenCPN single-file export.
