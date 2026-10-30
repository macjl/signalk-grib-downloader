# Repeated catalogue access requests

This concerns both GRIB Downloader and GRIB Weather Provider (downloader issue #20).

## Reproduced failure and scope

Signal K 2.35.1 returns HTTP 401 both for failed authentication and for insufficient
route permissions. An absent/unregistered Forecast Dataset Provider route defaults
to administrator-only access: an otherwise valid readwrite/readonly device token
therefore receives 401. Previously both clients deleted their automatically issued
token on any catalogue 401 and requested approval again with the same client ID.
Approving again cannot repair a missing or inaccessible catalogue route.

The corrected clients check the rejected credential against the same server's
`/loginStatus` before invalidating it. A logged-in token is retained and the status
points to provider installation/enablement, URL and permissions. A new approval is
requested only after an explicit `notLoggedIn` response with authentication enabled.
Network errors, redirects and ambiguous validation responses retain the token and
retry validation. Concurrent rejections share one check. Stop and credential
rotation cannot delete an in-flight replacement credential.

This is a reproduced cause matching the symptoms, not proof of the reporter's
installation state. Installed versions, provider availability, file access and the
actual approval/token lifecycle must still be checked before closing the issue.

## Collect diagnostics without disclosing credentials

The standalone collector is maintained in GRIB Downloader at
[docs/diagnose-catalogue.cjs](https://github.com/macjl/signalk-grib-downloader/blob/fix/catalogue-access-renewal/docs/diagnose-catalogue.cjs).
It covers both plugins. Run it as the Signal K service user, supplying the actual
configuration directory, for example:

```sh
node diagnose-catalogue.cjs "$HOME/.signalk"
```

The script performs read-only checks. It reports installed plugin versions,
local-provider enablement, token readability/mode, redacted request fingerprints,
automatic-token matching and HTTP/authentication results. It does not print tokens,
addresses, paths, usernames or raw request/client IDs, and does not read security.json.
Share only this JSON output. The Node version reported is the diagnostic process;
confirm the running server's version and Node version in Signal K's footer too.
For a remote catalogue, local-provider installation/enablement is not authoritative;
check the catalogue server separately.

Interpretation:

- Provider absent/disabled locally plus a loopback catalogue URL: install/enable it
  and check the connection before approving again.
- Login says `loggedIn` but catalogue returns 401: authentication works; check the
  provider route and permissions. Repeated approval cannot solve this response.
- Login says `notLoggedIn`: the credential is not accepted by that server.
- Token read errors (such as EACCES), or a missing/empty token: investigate access
  to the configured file and whether the approval response was persisted.
- Stable client and request fingerprints across captures: the same saved request
  is being followed. A changed request fingerprint with a stable client fingerprint
  identifies a different saved request for the same client.
- No saved access-request state with a readable token can be normal for a manually
  provisioned token. It does not, by itself, demonstrate a bug.

Do not remove existing token/state files merely to gather these diagnostics.

## Additional diagnostics after installing the fix

The administrator-only plugin status endpoint includes `catalogueAccess.diagnostics`.
Share that object and the plugin version, rather than the full status/configuration.
It contains counters for request attempts, approval responses, token writes,
authentication checks and renewals, plus a redacted `lastIssue` with phase/code and,
when applicable, HTTP or filesystem error code. Counters reset on plugin restart;
`lastIssue` records the latest incident, not necessarily a current failure.

Examples: `CATALOGUE_DENIED_TOKEN_VALID`, `TOKEN_CHECK_HTTP_ERROR`,
`APPROVAL_MISSING_TOKEN`, `TOKEN_WRITE_FAILED` / `EACCES`.

## Verification

The original and corrected managers were compared against real Signal K 2.35.1
HTTP 401 and loginStatus responses for both permission levels. Approval operations
were intercepted by a local fixture, using private temporary copies of existing
tokens: no real access request, installed token or security setting was changed.
Three catalogue denials caused three fixture approval requests before the fix and
zero after it for both clients. Tests also cover invalid-token renewal, validation
outages/ambiguity, concurrent denials, rotation, stop and redacted storage failures.
