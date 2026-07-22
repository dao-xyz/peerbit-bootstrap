import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { inspect } from "node:util";
import { readAndValidateRolloutConfig } from "./rollout-config.mjs";
import {
  assertWebSocketUpgrade,
  formatErrorSummary,
  parseBootstrapRemotes,
  runRollingSelfUpdate,
  toSafeError,
} from "./rolling-self-update.mjs";

const PEER_ID = "12D3KooWKj1J1hHxrYyB37qDDGCi9aU2vcHzDZhtMk7te7dEmqqT";
const MULTIADDR = `/dns4/bootstrap.example.com/tcp/4003/wss/p2p/${PEER_ID}`;
const TARGET_INTEGRITY =
  "sha512-D4xWIfN9erw3ap/b2SQXzYhHHN1UfSWi2DjfYhcgDKUwl9ivJQKLYxbsLe72ZJvKCm2iGyG4Gq9z0dTAjxMo/w==";
const ROLLBACK_INTEGRITY =
  "sha512-wrPcM191ghqZCjbJbpXKb01bvgMvBT/crZlmUBsdPgvZBPtkjouGzRo+O6VhpGLrrSRN53fF7YiRBS0KX1TBZA==";
const TARGET_FINGERPRINT = Object.freeze({
  peerbit: "5.3.10",
  "@peerbit/blocks": "4.2.6",
  "@peerbit/crypto": "3.1.4",
  "@peerbit/program": "6.0.39",
  "@peerbit/pubsub": "5.3.4",
  "@peerbit/time": "3.0.1",
});
const ROLLBACK_FINGERPRINT = Object.freeze({
  peerbit: "5.3.0",
  "@peerbit/blocks": "4.2.0",
  "@peerbit/crypto": "3.1.2",
  "@peerbit/program": "6.0.32",
  "@peerbit/pubsub": "5.3.0",
  "@peerbit/time": "3.0.0",
});

const baseConfig = () => ({
  bootstrapFile: "bootstrap-5.env",
  expectedCurrentVersion: "6.0.36",
  targetVersion: "8.0.0",
  rollbackVersion: "6.0.36",
  targetIntegrity: TARGET_INTEGRITY,
  rollbackIntegrity: ROLLBACK_INTEGRITY,
  targetFingerprint: { ...TARGET_FINGERPRINT },
  rollbackFingerprint: { ...ROLLBACK_FINGERPRINT },
  batchSize: 1,
  waitReadyTimeoutMs: 1_000,
  waitReadyDelayMs: 100,
  rollbackOnFailure: true,
  rerollReason: "test-v6-to-v8",
});

const writeJson = (file, value) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);

const makeConfigRepo = (mutate = () => {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-rollout-config-"));
  const config = baseConfig();
  const packageJson = {
    name: "peerbit-bootstrap-ops",
    private: true,
    type: "module",
    dependencies: {
      "@dao-xyz/borsh": "^6.0.1",
      "@peerbit/crypto": "3.1.4",
      "@peerbit/server": "8.0.0",
      "@peerbit/server-legacy": "npm:@peerbit/server@6.0.36",
    },
  };
  const lock = {
    name: "peerbit-bootstrap-ops",
    lockfileVersion: 3,
    requires: true,
    packages: {
      "": {
        name: "peerbit-bootstrap-ops",
        dependencies: { ...packageJson.dependencies },
      },
      "node_modules/@peerbit/server": {
        version: "8.0.0",
        resolved: "https://registry.npmjs.org/@peerbit/server/-/server-8.0.0.tgz",
        integrity: TARGET_INTEGRITY,
        dependencies: { ...TARGET_FINGERPRINT },
      },
      "node_modules/@peerbit/server-legacy": {
        name: "@peerbit/server",
        version: "6.0.36",
        resolved: "https://registry.npmjs.org/@peerbit/server/-/server-6.0.36.tgz",
        integrity: ROLLBACK_INTEGRITY,
        dependencies: { ...ROLLBACK_FINGERPRINT },
      },
    },
  };
  const files = { root, config, packageJson, lock };
  mutate(files);
  fs.mkdirSync(path.join(root, "rollouts"), { recursive: true });
  if (
    config.bootstrapFile === "bootstrap-5.env" &&
    !fs.existsSync(path.join(root, config.bootstrapFile))
  ) {
    fs.writeFileSync(path.join(root, config.bootstrapFile), `${MULTIADDR}\n`);
  }
  writeJson(path.join(root, "package.json"), packageJson);
  writeJson(path.join(root, "package-lock.json"), lock);
  writeJson(path.join(root, "rollouts", "test.json"), config);
  return root;
};

test("config validator accepts the audited dual-client package and lock contract", (t) => {
  const root = makeConfigRepo();
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const config = readAndValidateRolloutConfig({ configFile: "rollouts/test.json", repoRoot: root });
  assert.equal(config.targetVersion, "8.0.0");
  assert.equal(config.bootstrapPath, path.join(fs.realpathSync(root), "bootstrap-5.env"));
});

const invalidConfigCases = [
  ["unknown config keys", ({ config }) => (config.unreviewed = true), /must contain exactly/],
  ["semver ranges", ({ config }) => (config.targetVersion = "^8.0.0"), /exact semver/],
  ["a rollback other than the expected source", ({ config }) => (config.rollbackVersion = "6.0.35"), /must equal/],
  ["non-canonical integrity", ({ config }) => (config.targetIntegrity = "sha512-not-a-digest"), /canonical sha512/],
  ["a weakened fingerprint", ({ config }) => delete config.targetFingerprint["@peerbit/time"], /must contain exactly/],
  ["another bootstrap fleet", ({ config }) => (config.bootstrapFile = "bootstrap-4.env"), /exactly bootstrap-5\.env/],
  ["bootstrap path traversal", ({ config }) => (config.bootstrapFile = "../bootstrap.env"), /exactly bootstrap-5\.env/],
  ["parallel production updates", ({ config }) => (config.batchSize = 2), /batchSize must be exactly 1/],
  ["disabled rollback", ({ config }) => (config.rollbackOnFailure = false), /rollbackOnFailure must be true/],
  [
    "an unpinned legacy alias",
    ({ packageJson }) =>
      (packageJson.dependencies["@peerbit/server-legacy"] = "npm:@peerbit/server@^6.0.36"),
    /must pin @peerbit\/server-legacy/,
  ],
  [
    "a target lock integrity mismatch",
    ({ lock }) => (lock.packages["node_modules/@peerbit/server"].integrity = ROLLBACK_INTEGRITY),
    /integrity does not match/,
  ],
  [
    "a lock fingerprint mismatch",
    ({ lock }) =>
      (lock.packages["node_modules/@peerbit/server"].dependencies["@peerbit/time"] = "9.9.9"),
    /dependency @peerbit\/time must be/,
  ],
  [
    "a non-registry tarball",
    ({ lock }) =>
      (lock.packages["node_modules/@peerbit/server"].resolved = "https://example.com/server.tgz"),
    /canonical npm registry tarball/,
  ],
];

for (const [name, mutate, expected] of invalidConfigCases) {
  test(`config validator rejects ${name}`, (t) => {
    const root = makeConfigRepo(mutate);
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    assert.throws(
      () => readAndValidateRolloutConfig({ configFile: "rollouts/test.json", repoRoot: root }),
      expected,
    );
  });
}

test("config validator rejects a symlinked bootstrap file", (t) => {
  const root = makeConfigRepo(({ root: pendingRoot, config }) => {
    config.bootstrapFile = "bootstrap-5.env";
    fs.writeFileSync(path.join(pendingRoot, "real-bootstrap.env"), `${MULTIADDR}\n`);
    fs.symlinkSync("real-bootstrap.env", path.join(pendingRoot, config.bootstrapFile));
  });
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  assert.throws(
    () => readAndValidateRolloutConfig({ configFile: "rollouts/test.json", repoRoot: root }),
    /must not traverse symlinks/,
  );
});

test("config validator rejects config-file path traversal before reading", (t) => {
  const root = makeConfigRepo();
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  assert.throws(
    () => readAndValidateRolloutConfig({ configFile: "../outside.json", repoRoot: root }),
    /must not contain empty, '\.' or '\.\.'/,
  );
});

test("--validate-config succeeds without loading an administration secret", () => {
  const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.dirname(scriptsDir);
  const env = { ...process.env };
  delete env.PEERBIT_ADMIN_KEY_B64;
  const result = spawnSync(
    process.execPath,
    [
      path.join(scriptsDir, "rolling-self-update.mjs"),
      "--validate-config",
      "--config-file",
      "rollouts/bootstrap-5.json",
    ],
    { cwd: repoRoot, env, encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout), {
    bootstrapFile: "bootstrap-5.env",
    expectedCurrentVersion: "6.0.36",
    targetVersion: "8.0.0",
    rollbackVersion: "6.0.36",
    batchSize: 1,
    waitReadyTimeoutMs: 180000,
    waitReadyDelayMs: 3000,
    rollbackOnFailure: true,
    rerollReason: "peerbit-server-8.0.0-environment-secret-retry",
  });
});

test("bootstrap parsing retains the exact /p2p peer ID", () => {
  const [remote] = parseBootstrapRemotes(`${MULTIADDR}\n`, "fixture.env");
  assert.equal(remote.peerId, PEER_ID);
  assert.equal(remote.multiaddr, MULTIADDR);
  assert.equal(remote.address, "https://bootstrap.example.com");
  assert.equal(remote.publicWebSocketAddress, "https://bootstrap.example.com:4003/");
});

for (const [name, contents, expected] of [
  ["missing peer ID", "/dns4/bootstrap.example.com/tcp/4003/wss", /exactly one \/p2p/],
  ["trailing path after peer ID", `${MULTIADDR}/p2p-circuit`, /must end/],
  ["duplicate peer ID", `${MULTIADDR}\n/dns4/other.example.com/tcp/4003/wss/p2p/${PEER_ID}`, /duplicates peer ID/],
]) {
  test(`bootstrap parsing rejects ${name}`, () => {
    assert.throws(() => parseBootstrapRemotes(contents, "fixture.env"), expected);
  });
}

test("WebSocket probe accepts only the RFC6455 upgrade bound to its request key", () => {
  const requestKey = "dGhlIHNhbXBsZSBub25jZQ==";
  assert.doesNotThrow(() =>
    assertWebSocketUpgrade(
      {
        statusCode: 101,
        headers: {
          upgrade: "websocket",
          connection: "keep-alive, Upgrade",
          "sec-websocket-accept": "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        },
      },
      requestKey,
    ),
  );
});

for (const [name, response, expected] of [
  [
    "a bare 101 response",
    { statusCode: 101, headers: {} },
    /upgrade header is missing/,
  ],
  [
    "a missing Connection upgrade token",
    {
      statusCode: 101,
      headers: {
        upgrade: "websocket",
        connection: "keep-alive",
        "sec-websocket-accept": "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
      },
    },
    /connection upgrade token is missing/,
  ],
  [
    "an accept hash for another request",
    {
      statusCode: 101,
      headers: {
        upgrade: "websocket",
        connection: "Upgrade",
        "sec-websocket-accept": "AAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      },
    },
    /accept hash mismatch/,
  ],
]) {
  test(`WebSocket probe rejects ${name}`, () => {
    assert.throws(
      () => assertWebSocketUpgrade(response, "dGhlIHNhbXBsZSBub25jZQ=="),
      expected,
    );
  });
}

const silentLogger = {
  log() {},
  warn() {},
  error() {},
};

const makeFleet = ({
  initialProtocol = "legacy",
  targetVersions = {},
  rollbackVersions = {},
  omitTargetServerVersion = false,
  omitRollbackServerVersion = false,
  v8DescriptorPeerId = PEER_ID,
  neverVersionsProtocol,
  legacyCreateFailureAt,
  legacyUpdate,
  v8Rollback,
} = {}) => {
  const state = {
    protocol: initialProtocol,
    peerId: PEER_ID,
    targetVersions: {
      "@peerbit/server": "8.0.0",
      ...TARGET_FINGERPRINT,
      ...targetVersions,
    },
    rollbackVersions: {
      "@peerbit/server": "6.0.36",
      ...ROLLBACK_FINGERPRINT,
      ...rollbackVersions,
    },
  };
  if (omitTargetServerVersion) delete state.targetVersions["@peerbit/server"];
  if (omitRollbackServerVersion) delete state.rollbackVersions["@peerbit/server"];
  const calls = { v8Create: [], legacyCreate: [], selfUpdate: [], websocket: [] };

  const api = (protocol) => ({
    peer: {
      id: {
        get: async () => state.peerId,
        ...(protocol === "v8" ? { verify: async () => v8DescriptorPeerId } : {}),
      },
    },
    dependency: {
      versions: async () => {
        if (neverVersionsProtocol === protocol) return new Promise(() => {});
        return {
          ...(state.protocol === "v8" ? state.targetVersions : state.rollbackVersions),
        };
      },
    },
    selfUpdate: async (version) => {
      calls.selfUpdate.push({ protocol, version });
      if (protocol === "legacy") {
        if (legacyUpdate) return legacyUpdate({ state, version });
        state.protocol = "v8";
        return { version };
      }
      if (v8Rollback) return v8Rollback({ state, version });
      state.protocol = "legacy";
      return { version };
    },
  });

  const createV8Client = async (_keypair, options) => {
    calls.v8Create.push({ ...options });
    if (state.protocol !== "v8") throw new Error("signed-request v2 unavailable");
    return api("v8");
  };
  const createLegacyClient = async (_keypair, options) => {
    calls.legacyCreate.push({ ...options });
    if (calls.legacyCreate.length === legacyCreateFailureAt) {
      throw new Error("legacy client creation failed before selfUpdate");
    }
    if (state.protocol !== "legacy") throw new Error("legacy signed request unavailable");
    return api("legacy");
  };
  const waitForPublicWebSocketImpl = async (remote) => calls.websocket.push(remote.peerId);
  return { state, calls, createV8Client, createLegacyClient, waitForPublicWebSocketImpl };
};

const runFleet = (
  fleet,
  configOverrides = {},
  logger = silentLogger,
  sleepImpl = async () => {},
) => {
  const [remote] = parseBootstrapRemotes(MULTIADDR);
  return runRollingSelfUpdate({
    config: { ...baseConfig(), ...configOverrides },
    remotes: [remote],
    keypair: { test: true },
    createV8Client: fleet.createV8Client,
    createLegacyClient: fleet.createLegacyClient,
    waitForPublicWebSocketImpl: fleet.waitForPublicWebSocketImpl,
    sleepImpl,
    logger,
  });
};

test("happy path uses legacy only to initiate v6 -> v8 and verifies a fresh pinned v8 client", async () => {
  const fleet = makeFleet();
  const result = await runFleet(fleet);
  assert.deepEqual(result, { transitioned: 1, alreadyCurrent: 0 });
  assert.deepEqual(fleet.calls.selfUpdate, [{ protocol: "legacy", version: "8.0.0" }]);
  assert.equal(fleet.state.protocol, "v8");
  assert.ok(fleet.calls.v8Create.length >= 2, "v8 is probed before and freshly created after update");
  for (const options of fleet.calls.v8Create) {
    assert.deepEqual(options, { address: "https://bootstrap.example.com", peerId: PEER_ID });
  }
  for (const options of fleet.calls.legacyCreate) {
    assert.deepEqual(options, { address: "https://bootstrap.example.com" });
  }
});

test("an already verified v8 node is idempotently skipped", async () => {
  const fleet = makeFleet({ initialProtocol: "v8" });
  const result = await runFleet(fleet);
  assert.deepEqual(result, { transitioned: 0, alreadyCurrent: 1 });
  assert.deepEqual(fleet.calls.selfUpdate, []);
  assert.deepEqual(fleet.calls.legacyCreate, []);
  assert.equal(fleet.calls.websocket.length, 1);
});

test("source and target fingerprints remain authoritative when /versions omits server", async () => {
  const fleet = makeFleet({
    omitRollbackServerVersion: true,
    omitTargetServerVersion: true,
  });
  const result = await runFleet(fleet);
  assert.deepEqual(result, { transitioned: 1, alreadyCurrent: 0 });
  assert.deepEqual(fleet.calls.selfUpdate, [{ protocol: "legacy", version: "8.0.0" }]);
});

test("an absent server key also permits an idempotent verified v8 skip", async () => {
  const fleet = makeFleet({ initialProtocol: "v8", omitTargetServerVersion: true });
  const result = await runFleet(fleet);
  assert.deepEqual(result, { transitioned: 0, alreadyCurrent: 1 });
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("a present server key with the wrong source version is fatal", async () => {
  const fleet = makeFleet({ rollbackVersions: { "@peerbit/server": "6.0.35" } });
  await assert.rejects(() => runFleet(fleet), /expected @peerbit\/server@6\.0\.36/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("a present server key with the wrong target version is fatal", async () => {
  const fleet = makeFleet({
    initialProtocol: "v8",
    targetVersions: { "@peerbit/server": "8.0.1" },
  });
  await assert.rejects(() => runFleet(fleet), /expected @peerbit\/server@8\.0\.0/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("a signed descriptor peer-ID mismatch blocks all mutation", async () => {
  const fleet = makeFleet({
    initialProtocol: "v8",
    v8DescriptorPeerId: "12D3KooWAnotherPeerIdentityThatMustNeverBeAccepted12345",
  });
  await assert.rejects(() => runFleet(fleet), /signed descriptor peer ID mismatch/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
  assert.deepEqual(fleet.calls.legacyCreate, []);
});

test("a source fingerprint mismatch blocks the transition", async () => {
  const fleet = makeFleet();
  fleet.state.rollbackVersions["@peerbit/time"] = "9.9.9";
  await assert.rejects(() => runFleet(fleet), /legacy preflight: dependency fingerprint mismatch/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("a target fingerprint mismatch triggers and verifies protocol-aware rollback", async () => {
  const fleet = makeFleet({
    targetVersions: { "@peerbit/time": "9.9.9" },
    omitTargetServerVersion: true,
  });
  await assert.rejects(
    () => runFleet(fleet),
    (error) => {
      assert.match(
        error.message,
        /all initiated nodes were rolled back and verified/,
        formatErrorSummary(error),
      );
      return true;
    },
  );
  assert.deepEqual(fleet.calls.selfUpdate, [
    { protocol: "legacy", version: "8.0.0" },
    { protocol: "v8", version: "6.0.36" },
  ]);
  assert.equal(fleet.state.protocol, "legacy");
});

test("a non-exact selfUpdate response is rejected and rolled back", async () => {
  const fleet = makeFleet({
    legacyUpdate: ({ state, version }) => {
      state.protocol = "v8";
      return { version, extra: true };
    },
  });
  await assert.rejects(
    () => runFleet(fleet),
    /all initiated nodes were rolled back and verified/,
  );
  assert.deepEqual(fleet.calls.selfUpdate, [
    { protocol: "legacy", version: "8.0.0" },
    { protocol: "v8", version: "6.0.36" },
  ]);
});

test("rollback failure remains fatal and visible", async () => {
  const fleet = makeFleet({
    targetVersions: { "@peerbit/time": "9.9.9" },
    v8Rollback: async () => {
      throw new Error("rollback endpoint failed");
    },
  });
  await assert.rejects(
    () => runFleet(fleet),
    (error) => {
      assert.match(error.message, /rollback\(s\) failed fatally/);
      assert.ok(error.errors.some((nested) => nested.message.includes("rollback endpoint failed")));
      return true;
    },
  );
  assert.equal(fleet.state.protocol, "v8");
});

test("a never-resolving versions request is bounded by the rollout deadline", async () => {
  const fleet = makeFleet({ neverVersionsProtocol: "legacy" });
  const started = Date.now();
  await assert.rejects(
    () =>
      runFleet(
        fleet,
        {
          waitReadyTimeoutMs: 30,
          waitReadyDelayMs: 5,
        },
        silentLogger,
        (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
      ),
    /protocol-aware preflight.*timed out|Timed out waiting for .*preflight/,
  );
  assert.ok(Date.now() - started < 500, "hung versions call must not escape the deadline");
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("a never-resolving forward selfUpdate that stays legacy remains fatally ambiguous", async () => {
  const fleet = makeFleet({
    legacyUpdate: () => new Promise(() => {}),
  });
  const started = Date.now();
  await assert.rejects(
    () =>
      runFleet(
        fleet,
        {
          waitReadyTimeoutMs: 30,
          waitReadyDelayMs: 5,
        },
        silentLogger,
        (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
      ),
    (error) => {
      assert.match(error.message, /rollback\(s\) failed fatally/);
      assert.doesNotMatch(error.message, /all initiated nodes were rolled back/);
      assert.match(formatErrorSummary(error), /ambiguous/);
      return true;
    },
  );
  assert.ok(Date.now() - started < 500, "hung forward selfUpdate must be bounded");
  assert.deepEqual(fleet.calls.selfUpdate, [{ protocol: "legacy", version: "8.0.0" }]);
  assert.equal(fleet.state.protocol, "legacy");
});

test("a timed-out forward call that later exposes pinned v8 is rolled back", async () => {
  const fleet = makeFleet({
    legacyUpdate: ({ state }) => {
      setTimeout(() => {
        state.protocol = "v8";
      }, 40);
      return new Promise(() => {});
    },
  });
  await assert.rejects(
    () =>
      runFleet(
        fleet,
        {
          waitReadyTimeoutMs: 30,
          waitReadyDelayMs: 5,
        },
        silentLogger,
        (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
      ),
    (error) => {
      assert.match(
        error.message,
        /all initiated nodes were rolled back and verified/,
        formatErrorSummary(error),
      );
      return true;
    },
  );
  assert.deepEqual(fleet.calls.selfUpdate, [
    { protocol: "legacy", version: "8.0.0" },
    { protocol: "v8", version: "6.0.36" },
  ]);
  assert.equal(fleet.state.protocol, "legacy");
});

test("failure before invoking selfUpdate may verify the reviewed legacy state as unchanged", async () => {
  const fleet = makeFleet({ legacyCreateFailureAt: 2 });
  await assert.rejects(
    () =>
      runFleet(fleet, {
        waitReadyTimeoutMs: 30,
        waitReadyDelayMs: 5,
      }),
    /all initiated nodes were rolled back and verified/,
  );
  assert.deepEqual(fleet.calls.selfUpdate, []);
  assert.equal(fleet.state.protocol, "legacy");
});

test("a never-resolving rollback selfUpdate is bounded and remains fatal", async () => {
  const fleet = makeFleet({
    targetVersions: { "@peerbit/time": "9.9.9" },
    v8Rollback: () => new Promise(() => {}),
  });
  const started = Date.now();
  await assert.rejects(
    () =>
      runFleet(fleet, {
        waitReadyTimeoutMs: 30,
        waitReadyDelayMs: 5,
      }),
    /rollback\(s\) failed fatally/,
  );
  assert.ok(Date.now() - started < 500, "hung rollback selfUpdate must be bounded");
  assert.equal(fleet.state.protocol, "v8");
});

test("a child with a handle-less pending operation reaches its deadline and force-exits nonzero", () => {
  const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
  const moduleUrl = pathToFileURL(path.join(scriptsDir, "rolling-self-update.mjs")).href;
  const childSource = `
    import { reportCliFailure, withTimeout } from ${JSON.stringify(moduleUrl)};
    setInterval(() => {}, 10_000);
    try {
      await withTimeout(() => new Promise(() => {}), 25, "child pending operation");
    } catch (error) {
      reportCliFailure(error);
    }
  `;
  const started = Date.now();
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", childSource], {
    encoding: "utf8",
    timeout: 2_000,
  });
  assert.equal(result.signal, null, result.error?.message);
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /child pending operation timed out after 25ms/);
  assert.ok(Date.now() - started < 1_000, "CLI guard must terminate dangling handles promptly");
});

test("Axios-like signature headers are removed before errors are stored, logged, or rendered", async () => {
  const sentinels = [
    "REPLAYABLE_SIGNATURE_SENTINEL",
    "REPLAYABLE_TIMESTAMP_SENTINEL",
    "ADMIN_KEY_SENTINEL",
    "NESTED_REQUEST_SECRET_SENTINEL",
  ];
  const axiosError = new Error(
    `request failed 'X-Peerbit-Signature': '${sentinels[0]}' "X-Peerbit-Signature-Time": "${sentinels[1]}" 'PEERBIT_ADMIN_KEY_B64': '${sentinels[2]}'`,
  );
  axiosError.name = "AxiosError";
  axiosError.config = {
    headers: {
      "X-Peerbit-Signature": sentinels[0],
      "X-Peerbit-Signature-Time": sentinels[1],
      Authorization: `Bearer ${sentinels[2]}`,
    },
  };
  axiosError.request = { secret: sentinels[3] };
  axiosError.response = { config: axiosError.config };

  const directlySafe = toSafeError(new AggregateError([axiosError], "outer failure"));
  const directRendering = [
    formatErrorSummary(directlySafe),
    directlySafe.stack,
    inspect(directlySafe, { depth: 10 }),
  ].join("\n");
  for (const sentinel of sentinels) assert.doesNotMatch(directRendering, new RegExp(sentinel));

  const fleet = makeFleet({
    legacyUpdate: async () => {
      throw axiosError;
    },
  });
  const captured = [];
  const logger = Object.fromEntries(
    ["log", "warn", "error"].map((level) => [
      level,
      (...parts) => captured.push(parts.map(String).join(" ")),
    ]),
  );
  let finalError;
  try {
    await runFleet(
      fleet,
      { waitReadyTimeoutMs: 30, waitReadyDelayMs: 5 },
      logger,
    );
    assert.fail("rollout should fail after the synthetic update error");
  } catch (error) {
    finalError = error;
  }
  const finalRendering = [
    ...captured,
    formatErrorSummary(toSafeError(finalError)),
    finalError.stack,
    inspect(finalError, { depth: 10 }),
  ].join("\n");
  for (const sentinel of sentinels) assert.doesNotMatch(finalRendering, new RegExp(sentinel));
  assert.match(finalRendering, /\[redacted\]/);
});

test("a failed invoked legacy update that leaves v6 visible remains ambiguous", async () => {
  const fleet = makeFleet({
    legacyUpdate: async () => {
      throw new Error("connection dropped during update");
    },
  });
  await assert.rejects(
    () => runFleet(fleet, { waitReadyTimeoutMs: 30, waitReadyDelayMs: 5 }),
    /rollback\(s\) failed fatally/,
  );
  assert.deepEqual(fleet.calls.selfUpdate, [{ protocol: "legacy", version: "8.0.0" }]);
  assert.equal(fleet.state.protocol, "legacy");
});
