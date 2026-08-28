'use strict'

// Smoketests for the internet-aware auto scheduling pieces of the
// orchestrator: the offline gate on tick() and the nextTickAt() schedule
// computation. No network is touched — a live 'online' tick would fetch.

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const { Orchestrator } = require('../dist/orchestrator.js')
const { expectedRunStamp, fetchFingerprint, nextPublishAt } = require('../dist/scheduler.js')

const tmpRoot = () => fs.mkdtempSync(path.join(os.tmpdir(), 'grib-orch-'))

test('auto tick is suppressed while offline', async () => {
  const logs = []
  const orch = new Orchestrator(
    [{ model: 'icon-eu', autoDownload: true }],
    tmpRoot(),
    (msg) => logs.push(msg)
  )
  // A live tick would log "auto: 1 source(s) behind" and hit the network.
  await orch.tick('offline')
  assert.deepStrictEqual(logs, [])
  assert.strictEqual(orch.status()[0].lastOutcome, null)
})

// Regression: signalk-internet publishes 'captive' behind a captive
// portal. It used to fall back to 'unknown', which behaves as 'online' —
// so automatic downloads started, fetching the portal's login page.
test('auto tick is suppressed behind a captive portal', async () => {
  const logs = []
  const orch = new Orchestrator(
    [{ model: 'icon-eu', autoDownload: true }],
    tmpRoot(),
    (msg) => logs.push(msg)
  )
  await orch.tick('captive')
  assert.deepStrictEqual(logs, [])
  assert.strictEqual(orch.status()[0].lastOutcome, null)
})

test('nextTickAt retries behind sources, follows publication when up to date', () => {
  const root = tmpRoot()
  const setting = { model: 'icon-eu', autoDownload: true }
  const orch = new Orchestrator([setting], root, () => {})
  const timing = { retryMs: 600_000, slackMs: 300_000 }

  // No data on disk → behind → retry after retryMs
  const before = Date.now()
  const retry = orch.nextTickAt(setting, timing)
  assert.ok(retry.getTime() >= before + 599_000 && retry.getTime() <= Date.now() + 601_000)

  // Simulate the expected run completed on disk (fingerprinted marker)
  const dir = path.join(root, 'icon-eu')
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(
    path.join(dir, `.run-${expectedRunStamp('icon-eu')}.complete`),
    JSON.stringify({ params: fetchFingerprint(setting) })
  )

  // Up to date → wait for the model's next publication + slack
  const scheduled = orch.nextTickAt(setting, timing)
  const expected = nextPublishAt('icon-eu').getTime() + timing.slackMs
  assert.ok(Math.abs(scheduled.getTime() - expected) < 2000)
})
