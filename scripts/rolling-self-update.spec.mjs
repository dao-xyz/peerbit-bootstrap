import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { inspect } from "node:util";
import {
  classifyRolloutContract,
  readAndValidateRolloutConfig,
} from "./rollout-config.mjs";
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
const LEGACY_INTEGRITY =
  "sha512-wrPcM191ghqZCjbJbpXKb01bvgMvBT/crZlmUBsdPgvZBPtkjouGzRo+O6VhpGLrrSRN53fF7YiRBS0KX1TBZA==";
const FUTURE_INTEGRITY = `sha512-${Buffer.alloc(64, 7).toString("base64")}`;
const V8_FINGERPRINT = Object.freeze({
  peerbit: "5.3.10",
  "@peerbit/blocks": "4.2.6",
  "@peerbit/crypto": "3.1.4",
  "@peerbit/program": "6.0.39",
  "@peerbit/pubsub": "5.3.4",
  "@peerbit/time": "3.0.1",
});
const LEGACY_FINGERPRINT = Object.freeze({
  peerbit: "5.3.0",
  "@peerbit/blocks": "4.2.0",
  "@peerbit/crypto": "3.1.2",
  "@peerbit/program": "6.0.32",
  "@peerbit/pubsub": "5.3.0",
  "@peerbit/time": "3.0.0",
});
const FUTURE_FINGERPRINT = Object.freeze({
  peerbit: "5.4.0",
  "@peerbit/blocks": "4.3.0",
  "@peerbit/crypto": "3.1.5",
  "@peerbit/program": "6.1.0",
  "@peerbit/pubsub": "5.4.0",
  "@peerbit/time": "3.1.0",
});

const commonConfig = () => ({
  bootstrapFile: "bootstrap-5.env",
  batchSize: 1,
  waitReadyTimeoutMs: 1_000,
  waitReadyDelayMs: 100,
  rollbackOnFailure: true,
  rerollReason: "test-rollout",
});

const completedConfig = () => ({
  ...commonConfig(),
  expectedCurrentVersion: "6.0.36",
  targetVersion: "8.0.0",
  rollbackVersion: "6.0.36",
  targetIntegrity: TARGET_INTEGRITY,
  rollbackIntegrity: LEGACY_INTEGRITY,
  targetFingerprint: { ...V8_FINGERPRINT },
  rollbackFingerprint: { ...LEGACY_FINGERPRINT },
});

const activeConfig = () => ({
  ...commonConfig(),
  expectedCurrentVersion: "8.0.0",
  targetVersion: "8.1.0",
  rollbackVersion: "8.0.0",
  targetIntegrity: FUTURE_INTEGRITY,
  rollbackIntegrity: TARGET_INTEGRITY,
  targetFingerprint: { ...FUTURE_FINGERPRINT },
  rollbackFingerprint: { ...V8_FINGERPRINT },
});

const writeJson = (file, value) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);

const makeConfigRepo = ({ active = false, mutate = () => {} } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-rollout-config-"));
  const config = active ? activeConfig() : completedConfig();
  const packageJson = {
    name: "peerbit-bootstrap-ops",
    private: true,
    type: "module",
    dependencies: {
      "@dao-xyz/borsh": "^6.0.1",
      "@peerbit/crypto": config.targetFingerprint["@peerbit/crypto"],
      "@peerbit/server": config.targetVersion,
    },
  };
  const lock = {
    name: "peerbit-bootstrap-ops",
    lockfileVersion: 3,
    requires: true,
    packages: {
      "": { name: "peerbit-bootstrap-ops", dependencies: { ...packageJson.dependencies } },
      "node_modules/@peerbit/server": {
        version: config.targetVersion,
        resolved: `https://registry.npmjs.org/@peerbit/server/-/server-${config.targetVersion}.tgz`,
        integrity: config.targetIntegrity,
        dependencies: { ...config.targetFingerprint },
      },
    },
  };
  const files = { root, config, packageJson, lock };
  mutate(files);
  fs.mkdirSync(path.join(root, "rollouts"), { recursive: true });
  if (!fs.existsSync(path.join(root, "bootstrap-5.env"))) {
    fs.writeFileSync(path.join(root, "bootstrap-5.env"), `${MULTIADDR}\n`);
  }
  writeJson(path.join(root, "package.json"), packageJson);
  writeJson(path.join(root, "package-lock.json"), lock);
  writeJson(path.join(root, "rollouts", "test.json"), config);
  return root;
};

for (const [name, active, mode] of [
  ["completed legacy contract with only the v8 client installed", false, "completed-legacy"],
  ["future v8-native contract", true, "v8-native"],
]) {
  test(`config validator accepts ${name}`, (t) => {
    const root = makeConfigRepo({ active });
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const config = readAndValidateRolloutConfig({ configFile: "rollouts/test.json", repoRoot: root });
    assert.equal(config.rolloutMode, mode);
  });
}

const invalidConfigCases = [
  ["unknown config keys", ({ config }) => (config.unreviewed = true), /must contain exactly/],
  ["semver ranges", ({ config }) => (config.targetVersion = "^8.0.0"), /exact semver/],
  ["a rollback other than the expected source", ({ config }) => (config.rollbackVersion = "6.0.35"), /must equal/],
  ["another legacy migration", ({ config }) => {
    config.expectedCurrentVersion = "7.0.0";
    config.rollbackVersion = "7.0.0";
  }, /legacy rollout contracts are retired/],
  ["a pre-v8 target", ({ config }) => {
    config.targetVersion = "7.9.9";
  }, /v8 or newer/],
  ["a modified completed source fingerprint", ({ config }) => {
    config.rollbackFingerprint["@peerbit/time"] = "3.0.1";
  }, /legacy rollout contracts are retired/],
  ["equal source and target fingerprints", ({ config }) => {
    config.rollbackFingerprint = { ...config.targetFingerprint };
  }, /must differ/],
  ["non-canonical integrity", ({ config }) => (config.targetIntegrity = "sha512-nope"), /canonical sha512/],
  ["another bootstrap fleet", ({ config }) => (config.bootstrapFile = "bootstrap-4.env"), /exactly bootstrap-5\.env/],
  ["parallel updates", ({ config }) => (config.batchSize = 2), /batchSize must be exactly 1/],
  ["disabled rollback", ({ config }) => (config.rollbackOnFailure = false), /rollbackOnFailure must be true/],
  ["a legacy package alias", ({ packageJson }) => {
    packageJson.dependencies["@peerbit/server-legacy"] = "npm:@peerbit/server@6.0.36";
  }, /dependencies must contain exactly/],
  ["a stale legacy lock entry", ({ lock }) => {
    lock.packages["node_modules/@peerbit/server-legacy"] = { version: "6.0.36" };
  }, /must not retain/],
  ["a target lock integrity mismatch", ({ lock }) => {
    lock.packages["node_modules/@peerbit/server"].integrity = LEGACY_INTEGRITY;
  }, /integrity does not match/],
  ["a lock fingerprint mismatch", ({ lock }) => {
    lock.packages["node_modules/@peerbit/server"].dependencies["@peerbit/time"] = "9.9.9";
  }, /dependency @peerbit\/time must be/],
  ["a non-registry target tarball", ({ lock }) => {
    lock.packages["node_modules/@peerbit/server"].resolved = "https://example.com/server.tgz";
  }, /canonical npm registry tarball/],
];

for (const [name, mutate, expected] of invalidConfigCases) {
  test(`config validator rejects ${name}`, (t) => {
    const root = makeConfigRepo({ mutate });
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    assert.throws(
      () => readAndValidateRolloutConfig({ configFile: "rollouts/test.json", repoRoot: root }),
      expected,
    );
  });
}

test("config validator rejects a symlinked bootstrap file", (t) => {
  const root = makeConfigRepo({
    mutate: ({ root: pendingRoot }) => {
      fs.rmSync(path.join(pendingRoot, "bootstrap-5.env"), { force: true });
      fs.writeFileSync(path.join(pendingRoot, "real-bootstrap.env"), `${MULTIADDR}\n`);
      fs.symlinkSync("real-bootstrap.env", path.join(pendingRoot, "bootstrap-5.env"));
    },
  });
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  assert.throws(
    () => readAndValidateRolloutConfig({ configFile: "rollouts/test.json", repoRoot: root }),
    /must not traverse symlinks/,
  );
});

test("config validator rejects config path traversal before reading", (t) => {
  const root = makeConfigRepo();
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  assert.throws(
    () => readAndValidateRolloutConfig({ configFile: "../outside.json", repoRoot: root }),
    /must not contain empty, '\.' or '\.\.'/,
  );
});

test("the repository's unchanged completed config validates without an admin secret", () => {
  const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.dirname(scriptsDir);
  const env = { ...process.env };
  delete env.PEERBIT_ADMIN_KEY_B64;
  const result = spawnSync(
    process.execPath,
    [path.join(scriptsDir, "rolling-self-update.mjs"), "--validate-config", "--config-file", "rollouts/bootstrap-5.json"],
    { cwd: repoRoot, env, encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).targetVersion, "8.0.0");
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
  ["trailing path", `${MULTIADDR}/p2p-circuit`, /must end/],
  ["duplicate identity", `${MULTIADDR}\n/dns4/other.example.com/tcp/4003/wss/p2p/${PEER_ID}`, /duplicates peer ID/],
]) {
  test(`bootstrap parsing rejects ${name}`, () => {
    assert.throws(() => parseBootstrapRemotes(contents, "fixture.env"), expected);
  });
}

test("WebSocket validation requires the RFC6455 accept bound to its request", () => {
  const requestKey = "dGhlIHNhbXBsZSBub25jZQ==";
  assert.doesNotThrow(() => assertWebSocketUpgrade({
    statusCode: 101,
    headers: {
      upgrade: "websocket",
      connection: "keep-alive, Upgrade",
      "sec-websocket-accept": "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
    },
  }, requestKey));
  assert.throws(() => assertWebSocketUpgrade({
    statusCode: 101,
    headers: { upgrade: "websocket", connection: "Upgrade", "sec-websocket-accept": "wrong" },
  }, requestKey), /accept hash mismatch/);
  assert.throws(
    () => assertWebSocketUpgrade({ statusCode: 101, headers: {} }, requestKey),
    /upgrade header is missing/,
  );
});

const silentLogger = { log() {}, warn() {}, error() {} };

const makeFleet = ({
  initialPhase = "source",
  sourceVersions = {},
  targetVersions = {},
  descriptorPeerId = PEER_ID,
  omitServerVersion = false,
  createFailure,
  neverVersions = false,
  neverVersionsAtCreate,
  onCreate,
  forwardUpdate,
  rollbackUpdate,
} = {}) => {
  const config = activeConfig();
  const state = {
    phase: initialPhase,
    peerId: PEER_ID,
    sourceVersions: {
      "@peerbit/server": config.rollbackVersion,
      ...config.rollbackFingerprint,
      ...sourceVersions,
    },
    targetVersions: {
      "@peerbit/server": config.targetVersion,
      ...config.targetFingerprint,
      ...targetVersions,
    },
  };
  if (omitServerVersion) {
    delete state.sourceVersions["@peerbit/server"];
    delete state.targetVersions["@peerbit/server"];
  }
  const calls = { create: [], selfUpdate: [], websocket: [] };
  const createV8Client = async (_keypair, options) => {
    calls.create.push({ ...options });
    const createIndex = calls.create.length;
    if (onCreate) await onCreate({ calls, state });
    if (createFailure) throw createFailure;
    return {
      peer: { id: { get: async () => state.peerId, verify: async () => descriptorPeerId } },
      dependency: {
        versions: async () => {
          if (neverVersions || createIndex === neverVersionsAtCreate) return new Promise(() => {});
          return { ...(state.phase === "source" ? state.sourceVersions : state.targetVersions) };
        },
      },
      selfUpdate: async (version) => {
        calls.selfUpdate.push(version);
        if (version === config.targetVersion) {
          if (forwardUpdate) return forwardUpdate({ state, version });
          state.phase = "target";
          return { version };
        }
        if (rollbackUpdate) return rollbackUpdate({ state, version });
        state.phase = "source";
        return { version };
      },
    };
  };
  const waitForPublicWebSocketImpl = async (remote) => calls.websocket.push(remote.peerId);
  return { config, state, calls, createV8Client, waitForPublicWebSocketImpl };
};

const runFleet = (fleet, {
  config = fleet.config,
  configOverrides = {},
  logger = silentLogger,
  sleepImpl = async () => {},
} = {}) => {
  const [remote] = parseBootstrapRemotes(MULTIADDR);
  return runRollingSelfUpdate({
    config: { ...config, ...configOverrides },
    remotes: [remote],
    keypair: { test: true },
    createV8Client: fleet.createV8Client,
    waitForPublicWebSocketImpl: fleet.waitForPublicWebSocketImpl,
    sleepImpl,
    logger,
  });
};

test("completed legacy contract verifies and idempotently skips the exact pinned v8 target", async () => {
  const fleet = makeFleet({ initialPhase: "target" });
  fleet.config = completedConfig();
  fleet.state.targetVersions = { "@peerbit/server": "8.0.0", ...V8_FINGERPRINT };
  const result = await runFleet(fleet);
  assert.deepEqual(result, { transitioned: 0, alreadyCurrent: 1 });
  assert.deepEqual(fleet.calls.selfUpdate, []);
  assert.equal(fleet.calls.websocket.length, 1);
  for (const options of fleet.calls.create) {
    assert.deepEqual(options, { address: "https://bootstrap.example.com", peerId: PEER_ID });
  }
});

test("completed legacy contract cannot fall back to v1 or mutate when v8 is unreachable", async () => {
  const secretError = new Error("signed-request-v2 unavailable");
  const fleet = makeFleet({ createFailure: secretError });
  await assert.rejects(
    () => runFleet(fleet, {
      config: completedConfig(),
      configOverrides: { waitReadyTimeoutMs: 20, waitReadyDelayMs: 5 },
      sleepImpl: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    }),
    /pinned signed-request-v2 preflight/,
  );
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("completed legacy contract rejects a reachable signed-v2 source as inert", async () => {
  const fleet = makeFleet();
  fleet.state.sourceVersions = {
    "@peerbit/server": "6.0.36",
    ...LEGACY_FINGERPRINT,
  };
  await assert.rejects(
    () => runFleet(fleet, { config: completedConfig() }),
    /completed legacy rollout contract is inert/,
  );
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("completed contract cannot be activated by spoofing rolloutMode", async () => {
  const fleet = makeFleet();
  fleet.state.sourceVersions = {
    "@peerbit/server": "6.0.36",
    ...LEGACY_FINGERPRINT,
  };
  await assert.rejects(
    () => runFleet(fleet, { config: { ...completedConfig(), rolloutMode: "v8-native" } }),
    /rolloutMode must be completed-legacy/,
  );
  assert.deepEqual(fleet.calls.selfUpdate, []);
  assert.deepEqual(fleet.calls.create, []);
});

test("completed legacy contract fails without mutation when the pinned v8 target is not exact", async () => {
  const fleet = makeFleet({ initialPhase: "target", targetVersions: { "@peerbit/time": "9.9.9" } });
  fleet.state.targetVersions["@peerbit/server"] = "8.0.0";
  await assert.rejects(() => runFleet(fleet, { config: completedConfig() }), /target dependency fingerprint mismatch/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("future rollout transitions an exact v8 source with only pinned signed-request-v2 clients", async () => {
  const fleet = makeFleet();
  const result = await runFleet(fleet);
  assert.deepEqual(result, { transitioned: 1, alreadyCurrent: 0 });
  assert.deepEqual(fleet.calls.selfUpdate, ["8.1.0"]);
  assert.equal(fleet.state.phase, "target");
  assert.ok(fleet.calls.create.length >= 3);
  for (const options of fleet.calls.create) {
    assert.deepEqual(options, { address: "https://bootstrap.example.com", peerId: PEER_ID });
  }
});

test("a concurrent source-to-target change before mutation is never rolled back or mutated", async () => {
  const fleet = makeFleet({
    onCreate: ({ calls, state }) => {
      if (calls.create.length === 2) state.phase = "target";
    },
  });
  await assert.rejects(() => runFleet(fleet), /before any selfUpdate was initiated/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
  assert.equal(fleet.state.phase, "target");
});

test("a hanging fresh mutation preflight is bounded and cannot initiate selfUpdate", async () => {
  const fleet = makeFleet({ neverVersionsAtCreate: 2 });
  const started = Date.now();
  await assert.rejects(
    () => runFleet(fleet, {
      configOverrides: { waitReadyTimeoutMs: 25, waitReadyDelayMs: 5 },
      sleepImpl: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    }),
    (error) => {
      assert.match(error.message, /before any selfUpdate was initiated/);
      assert.match(formatErrorSummary(error), /fresh v8 mutation preflight timed out/);
      return true;
    },
  );
  assert.ok(Date.now() - started < 500, "fresh safety read must honor the rollout deadline");
  assert.deepEqual(fleet.calls.selfUpdate, []);
  assert.equal(fleet.calls.create.length, 2);
});

test("future rollout idempotently skips an exact target", async () => {
  const fleet = makeFleet({ initialPhase: "target" });
  assert.deepEqual(await runFleet(fleet), { transitioned: 0, alreadyCurrent: 1 });
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("source and target classification works when /versions omits @peerbit/server", async () => {
  const sourceFleet = makeFleet({ omitServerVersion: true });
  assert.deepEqual(await runFleet(sourceFleet), { transitioned: 1, alreadyCurrent: 0 });
  const targetFleet = makeFleet({ initialPhase: "target", omitServerVersion: true });
  assert.deepEqual(await runFleet(targetFleet), { transitioned: 0, alreadyCurrent: 1 });
});

test("signed descriptor mismatch blocks mutation", async () => {
  const fleet = makeFleet({ descriptorPeerId: "12D3KooWAnotherPeerIdentityThatMustNeverBeAccepted12345" });
  await assert.rejects(() => runFleet(fleet), /signed descriptor peer ID mismatch/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("unknown source fingerprint blocks mutation", async () => {
  const fleet = makeFleet({ sourceVersions: { "@peerbit/time": "9.9.9" } });
  await assert.rejects(() => runFleet(fleet), /source dependency fingerprint mismatch/);
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("target fingerprint failure rolls back and verifies the v8 source", async () => {
  const fleet = makeFleet({ targetVersions: { "@peerbit/time": "9.9.9" } });
  await assert.rejects(() => runFleet(fleet), /all initiated nodes were rolled back and verified/);
  assert.deepEqual(fleet.calls.selfUpdate, ["8.1.0", "8.0.0"]);
  assert.equal(fleet.state.phase, "source");
});

test("target fingerprint failure can roll back a pinned target that omits server version", async () => {
  const fleet = makeFleet({
    omitServerVersion: true,
    targetVersions: { "@peerbit/time": "9.9.9" },
  });
  await assert.rejects(() => runFleet(fleet), /all initiated nodes were rolled back and verified/);
  assert.deepEqual(fleet.calls.selfUpdate, ["8.1.0", "8.0.0"]);
  assert.equal(fleet.state.phase, "source");
});

test("non-exact forward response is rejected and rolled back", async () => {
  const fleet = makeFleet({
    forwardUpdate: ({ state, version }) => {
      state.phase = "target";
      return { version, extra: true };
    },
  });
  await assert.rejects(() => runFleet(fleet), /all initiated nodes were rolled back and verified/);
  assert.deepEqual(fleet.calls.selfUpdate, ["8.1.0", "8.0.0"]);
});

test("rollback failure remains fatal", async () => {
  const fleet = makeFleet({
    targetVersions: { "@peerbit/time": "9.9.9" },
    rollbackUpdate: async () => { throw new Error("rollback endpoint failed"); },
  });
  await assert.rejects(() => runFleet(fleet), /rollback\(s\) failed fatally/);
  assert.equal(fleet.state.phase, "target");
});

test("never-resolving dependency request is bounded", async () => {
  const fleet = makeFleet({ neverVersions: true });
  const started = Date.now();
  await assert.rejects(() => runFleet(fleet, {
    configOverrides: { waitReadyTimeoutMs: 25, waitReadyDelayMs: 5 },
    sleepImpl: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  }), /pinned signed-request-v2 preflight/);
  assert.ok(Date.now() - started < 500);
  assert.deepEqual(fleet.calls.selfUpdate, []);
});

test("ambiguous timed-out forward call that remains on source fails fatally without a second mutation", async () => {
  const fleet = makeFleet({ forwardUpdate: () => new Promise(() => {}) });
  await assert.rejects(() => runFleet(fleet, {
    configOverrides: { waitReadyTimeoutMs: 25, waitReadyDelayMs: 5 },
    sleepImpl: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  }), (error) => {
    assert.match(error.message, /rollback\(s\) failed fatally/);
    assert.match(formatErrorSummary(error), /ambiguous/);
    return true;
  });
  assert.deepEqual(fleet.calls.selfUpdate, ["8.1.0"]);
});

test("timed-out forward call that later exposes target is rolled back", async () => {
  const fleet = makeFleet({
    forwardUpdate: ({ state }) => {
      setTimeout(() => { state.phase = "target"; }, 35);
      return new Promise(() => {});
    },
  });
  await assert.rejects(() => runFleet(fleet, {
    configOverrides: { waitReadyTimeoutMs: 25, waitReadyDelayMs: 5 },
    sleepImpl: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  }), /all initiated nodes were rolled back and verified/);
  assert.deepEqual(fleet.calls.selfUpdate, ["8.1.0", "8.0.0"]);
  assert.equal(fleet.state.phase, "source");
});

test("signature material is redacted from nested errors, logs, and final stacks", async () => {
  const sentinels = ["REPLAYABLE_SIGNATURE", "REPLAYABLE_TIME", "ADMIN_SECRET"];
  const axiosError = new Error(
    `request failed 'X-Peerbit-Signature': '${sentinels[0]}' "X-Peerbit-Signature-Time": "${sentinels[1]}" Authorization: Bearer-${sentinels[2]}`,
  );
  axiosError.name = "AxiosError";
  axiosError.config = { headers: { "X-Peerbit-Signature": sentinels[0], Authorization: sentinels[2] } };
  const safe = toSafeError(new AggregateError([axiosError], "outer"));
  const direct = [formatErrorSummary(safe), safe.stack, inspect(safe, { depth: 10 })].join("\n");
  for (const sentinel of sentinels) assert.doesNotMatch(direct, new RegExp(sentinel));

  const fleet = makeFleet({ forwardUpdate: async () => { throw axiosError; } });
  const captured = [];
  const logger = Object.fromEntries(["log", "warn", "error"].map((level) => [
    level,
    (...parts) => captured.push(parts.map(String).join(" ")),
  ]));
  let finalError;
  try {
    await runFleet(fleet, {
      logger,
      configOverrides: { waitReadyTimeoutMs: 25, waitReadyDelayMs: 5 },
      sleepImpl: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    });
    assert.fail("expected failure");
  } catch (error) {
    finalError = error;
  }
  const rendered = [...captured, formatErrorSummary(finalError), finalError.stack, inspect(finalError, { depth: 10 })].join("\n");
  for (const sentinel of sentinels) assert.doesNotMatch(rendered, new RegExp(sentinel));
  assert.match(rendered, /\[redacted\]/);
});

test("CLI failure guard bounds a child with dangling handles", () => {
  const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
  const moduleUrl = pathToFileURL(path.join(scriptsDir, "rolling-self-update.mjs")).href;
  const source = `
    import { reportCliFailure, withTimeout } from ${JSON.stringify(moduleUrl)};
    setInterval(() => {}, 10_000);
    try { await withTimeout(() => new Promise(() => {}), 25, "child pending operation"); }
    catch (error) { reportCliFailure(error); }
  `;
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", source], {
    encoding: "utf8",
    timeout: 2_000,
  });
  assert.equal(result.signal, null, result.error?.message);
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /timed out after 25ms/);
});

test("contract classifier is deterministic for completed and future configs", () => {
  assert.equal(classifyRolloutContract(completedConfig()), "completed-legacy");
  assert.equal(classifyRolloutContract(activeConfig()), "v8-native");
});
