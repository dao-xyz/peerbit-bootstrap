#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  classifyRolloutContract,
  readAndValidateRolloutConfig,
  rolloutConfigOutput,
} from "./rollout-config.mjs";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class RolloutInvariantError extends Error {
  constructor(message) {
    super(message);
    this.name = "RolloutInvariantError";
  }
}

const ensure = (condition, message, ErrorType = Error) => {
  if (!condition) throw new ErrorType(message);
};

const isPlainObject = (value) =>
  value !== null &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

const sanitizeErrorText = (value) => {
  const singleLine = String(value ?? "unknown")
    .replace(/[\r\n\t\0]+/g, " ")
    .replace(
      /(["']?)(x-peerbit-(?:signature(?:-time)?|timestamp)|authorization|peerbit_admin_key_b64)\1\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^\s,;}\]]+)/gi,
      "$2=[redacted]",
    )
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .trim();
  return singleLine.length > 500 ? `${singleLine.slice(0, 497)}...` : singleLine;
};

const safeErrorName = (error) => {
  const name = typeof error?.name === "string" ? error.name : "Error";
  return /^[A-Za-z][A-Za-z0-9]{0,63}$/.test(name) ? name : "Error";
};

export const formatErrorSummary = (error, depth = 0) => {
  if (depth >= 3) return "Error: nested failure omitted";
  const name = safeErrorName(error);
  const message = sanitizeErrorText(error?.message ?? error);
  let summary = `${name}: ${message}`;
  if (error instanceof AggregateError) {
    const nested = [...error.errors]
      .slice(0, 8)
      .map((item) => formatErrorSummary(item, depth + 1));
    if (nested.length > 0) summary += ` [${nested.join(" | ")}]`;
  }
  return sanitizeErrorText(summary);
};

export const toSafeError = (error) => {
  if (error instanceof AggregateError) {
    return new AggregateError(
      [...error.errors].slice(0, 8).map((item) => toSafeError(item)),
      sanitizeErrorText(error.message),
    );
  }
  if (error instanceof RolloutInvariantError) {
    return new RolloutInvariantError(sanitizeErrorText(error.message));
  }
  return new Error(formatErrorSummary(error));
};

export const withTimeout = async (operation, timeoutMs, description) => {
  ensure(Number.isFinite(timeoutMs) && timeoutMs > 0, `${description} timeout must be positive`);
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${description} timed out after ${Math.ceil(timeoutMs)}ms`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([Promise.resolve().then(operation), timeout]);
  } finally {
    clearTimeout(timer);
  }
};

export const parseArgs = (argv) => {
  const allowed = new Set(["config-file", "validate-config"]);
  const result = {};
  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    ensure(token?.startsWith("--"), `Unexpected positional argument: ${token}`);
    const key = token.slice(2);
    ensure(allowed.has(key), `Unknown argument: --${key}`);
    ensure(result[key] === undefined, `Duplicate argument: --${key}`);
    if (key === "validate-config") {
      result[key] = true;
      continue;
    }
    const value = argv[index + 1];
    ensure(value && !value.startsWith("--"), `Missing value for --${key}`);
    result[key] = value;
    index += 1;
  }
  return result;
};

const parseHost = (type, value, label) => {
  if (type === "ip4") {
    ensure(net.isIP(value) === 4, `${label} has an invalid IPv4 address`);
  } else if (type === "ip6") {
    ensure(net.isIP(value) === 6, `${label} has an invalid IPv6 address`);
  } else {
    ensure(
      value.length <= 253 &&
        value.split(".").every(
          (part) =>
            part.length >= 1 &&
            part.length <= 63 &&
            /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/.test(part),
        ),
      `${label} has an invalid DNS name`,
    );
  }
};

export const parseBootstrapRemotes = (contents, label = "bootstrap file") => {
  ensure(typeof contents === "string", `${label} contents must be text`);
  const lines = contents
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"));
  ensure(lines.length > 0, `No bootstrap remotes found in ${label}`);

  const remoteKeys = new Set();
  const peerIds = new Set();
  const remotes = lines.map((line, index) => {
    const lineLabel = `${label}:${index + 1}`;
    ensure(line.startsWith("/") && !/\s/.test(line), `${lineLabel} must be one multiaddr`);
    const segments = line.slice(1).split("/");
    ensure(segments.every((segment) => segment.length > 0), `${lineLabel} has an empty segment`);
    const p2pIndexes = segments
      .map((segment, segmentIndex) => (segment === "p2p" ? segmentIndex : -1))
      .filter((segmentIndex) => segmentIndex >= 0);
    ensure(p2pIndexes.length === 1, `${lineLabel} must contain exactly one /p2p/<peer-id>`);
    const p2pIndex = p2pIndexes[0];
    ensure(p2pIndex === segments.length - 2, `${lineLabel} must end with /p2p/<peer-id>`);
    ensure(
      segments.length === 7,
      `${lineLabel} must use /<host-protocol>/<host>/tcp/<port>/<ws|wss>/p2p/<peer-id>`,
    );
    const peerId = segments[p2pIndex + 1];
    ensure(
      /^[A-Za-z0-9]{20,128}$/.test(peerId),
      `${lineLabel} contains an invalid peer ID`,
    );

    const hostIndexes = segments
      .map((segment, segmentIndex) =>
        ["dns4", "dns6", "ip4", "ip6"].includes(segment) ? segmentIndex : -1,
      )
      .filter((segmentIndex) => segmentIndex >= 0);
    ensure(hostIndexes.length === 1, `${lineLabel} must contain exactly one host protocol`);
    const hostIndex = hostIndexes[0];
    ensure(hostIndex === 0, `${lineLabel} must begin with its host protocol`);
    ensure(hostIndex + 1 < segments.length, `${lineLabel} is missing its host`);
    const hostType = segments[hostIndex];
    const host = segments[hostIndex + 1];
    parseHost(hostType, host, lineLabel);

    const tcpIndexes = segments
      .map((segment, segmentIndex) => (segment === "tcp" ? segmentIndex : -1))
      .filter((segmentIndex) => segmentIndex >= 0);
    ensure(tcpIndexes.length === 1, `${lineLabel} must contain exactly one /tcp/<port>`);
    const tcpIndex = tcpIndexes[0];
    ensure(tcpIndex === 2, `${lineLabel} must place /tcp/<port> immediately after its host`);
    ensure(tcpIndex + 2 < segments.length, `${lineLabel} is missing its TCP transport`);
    const portText = segments[tcpIndex + 1];
    ensure(/^[1-9]\d{0,4}$/.test(portText), `${lineLabel} has an invalid TCP port`);
    const port = Number(portText);
    ensure(port <= 65_535, `${lineLabel} has an invalid TCP port`);
    const transport = segments[tcpIndex + 2];
    ensure(transport === "ws" || transport === "wss", `${lineLabel} must use ws or wss`);

    const remoteKey = `${hostType}:${host}`;
    ensure(!remoteKeys.has(remoteKey), `${lineLabel} duplicates remote host ${host}`);
    ensure(!peerIds.has(peerId), `${lineLabel} duplicates peer ID ${peerId}`);
    remoteKeys.add(remoteKey);
    peerIds.add(peerId);

    const protocol = hostType.startsWith("ip") ? "http" : "https";
    const urlHost = hostType === "ip6" ? `[${host}]` : host;
    return Object.freeze({
      name: `bootstrap-${index + 1}`,
      host,
      address: `${protocol}://${urlHost}`,
      publicWebSocketAddress: `${transport === "wss" ? "https" : "http"}://${urlHost}:${port}/`,
      peerId,
      multiaddr: line,
    });
  });
  return Object.freeze(remotes);
};

export const readBootstrapRemotes = (bootstrapPath) =>
  parseBootstrapRemotes(fs.readFileSync(bootstrapPath, "utf8"), bootstrapPath);

export const assertWebSocketUpgrade = (response, requestKey) => {
  ensure(response?.statusCode === 101, `expected HTTP 101, got ${response?.statusCode ?? "unknown"}`);
  const upgrade = response.headers?.upgrade;
  ensure(
    typeof upgrade === "string" && upgrade.toLowerCase() === "websocket",
    "WebSocket upgrade header is missing",
  );
  const connection = response.headers?.connection;
  ensure(typeof connection === "string", "WebSocket connection header is missing");
  const connectionTokens = connection
    .toLowerCase()
    .split(",")
    .map((token) => token.trim());
  ensure(connectionTokens.includes("upgrade"), "WebSocket connection upgrade token is missing");
  const expectedAccept = crypto
    .createHash("sha1")
    .update(`${requestKey}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`, "ascii")
    .digest("base64");
  ensure(response.headers?.["sec-websocket-accept"] === expectedAccept, "WebSocket accept hash mismatch");
};

const probePublicWebSocket = async (remote) => {
  await new Promise((resolve, reject) => {
    const url = new URL(remote.publicWebSocketAddress);
    const client = url.protocol === "https:" ? https : http;
    const requestKey = crypto.randomBytes(16).toString("base64");
    const request = client.request(url, {
      method: "GET",
      headers: {
        Connection: "Upgrade",
        Upgrade: "websocket",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": requestKey,
      },
    });
    request.setTimeout(10_000, () => request.destroy(new Error("public WebSocket probe timed out")));
    request.on("upgrade", (response, socket) => {
      socket.destroy();
      try {
        assertWebSocketUpgrade(response, requestKey);
        resolve();
      } catch (error) {
        reject(error);
      }
    });
    request.on("response", (response) => {
      response.resume();
      reject(new Error(`expected HTTP 101, got ${response.statusCode ?? "unknown"}`));
    });
    request.on("error", reject);
    request.end();
  });
};

const retry = async ({ operation, timeoutMs, delayMs, description, sleepImpl = sleep }) => {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  do {
    try {
      const remainingMs = Math.max(1, deadline - Date.now());
      return await withTimeout(operation, remainingMs, `${description} attempt`);
    } catch (error) {
      if (error instanceof RolloutInvariantError) throw error;
      lastError = toSafeError(error);
      if (Date.now() >= deadline) break;
      await sleepImpl(Math.min(delayMs, Math.max(0, deadline - Date.now())));
    }
  } while (Date.now() < deadline);
  throw new Error(
    `Timed out waiting for ${description}. Last error: ${formatErrorSummary(lastError)}`,
  );
};

export const waitForPublicWebSocket = async (
  remote,
  timeoutMs,
  delayMs,
  { sleepImpl = sleep } = {},
) =>
  retry({
    operation: () => probePublicWebSocket(remote),
    timeoutMs,
    delayMs,
    description: `${remote.name} (${remote.publicWebSocketAddress}) public WebSocket`,
    sleepImpl,
  });

const createPinnedV8 = (createV8Client, keypair, remote) =>
  createV8Client(keypair, { address: remote.address, peerId: remote.peerId });

const readClientState = async (api, { verifyDescriptor = false } = {}) => {
  const firstPeerId = await api.peer.id.get();
  const descriptorPeerId = verifyDescriptor ? await api.peer.id.verify() : undefined;
  const versions = await api.dependency.versions();
  const secondPeerId = await api.peer.id.get();
  ensure(
    typeof firstPeerId === "string" && typeof secondPeerId === "string",
    "peer.id.get() must return a string",
    RolloutInvariantError,
  );
  ensure(
    firstPeerId === secondPeerId,
    `peer identity changed during verification (${firstPeerId} -> ${secondPeerId})`,
    RolloutInvariantError,
  );
  if (verifyDescriptor) {
    ensure(
      typeof descriptorPeerId === "string",
      "peer.id.verify() must return the signed descriptor peer ID",
      RolloutInvariantError,
    );
  }
  ensure(isPlainObject(versions), "dependency.versions() must return an object", RolloutInvariantError);
  return { peerId: firstPeerId, descriptorPeerId, versions };
};

const assertIdentity = (state, remote, phase) => {
  ensure(
    state.peerId === remote.peerId,
    `${remote.name}: ${phase} peer ID mismatch: expected ${remote.peerId}, got ${state.peerId}`,
    RolloutInvariantError,
  );
  if (state.descriptorPeerId !== undefined) {
    ensure(
      state.descriptorPeerId === remote.peerId,
      `${remote.name}: ${phase} signed descriptor peer ID mismatch: expected ${remote.peerId}, got ${state.descriptorPeerId}`,
      RolloutInvariantError,
    );
  }
};

export const assertDependencyFingerprint = (
  state,
  expectedServerVersion,
  expectedFingerprint,
  label,
) => {
  assertOptionalServerVersion(state, expectedServerVersion, label);
  const mismatches = Object.entries(expectedFingerprint)
    .filter(([dependency, version]) => state.versions[dependency] !== version)
    .map(
      ([dependency, version]) =>
        `${dependency}: expected ${version}, got ${state.versions[dependency] ?? "missing"}`,
    );
  ensure(
    mismatches.length === 0,
    `${label}: dependency fingerprint mismatch (${mismatches.join("; ")})`,
    RolloutInvariantError,
  );
};

const assertOptionalServerVersion = (state, expectedServerVersion, label) => {
  if (Object.hasOwn(state.versions, "@peerbit/server")) {
    ensure(
      state.versions["@peerbit/server"] === expectedServerVersion,
      `${label}: expected @peerbit/server@${expectedServerVersion}, got ${state.versions["@peerbit/server"]}`,
      RolloutInvariantError,
    );
  }
};

export const assertSelfUpdateResponse = (response, expectedVersion, label) => {
  ensure(isPlainObject(response), `${label}: selfUpdate response must be an object`, RolloutInvariantError);
  const keys = Object.keys(response);
  ensure(
    keys.length === 1 && keys[0] === "version" && response.version === expectedVersion,
    `${label}: selfUpdate must return exactly {"version":"${expectedVersion}"}`,
    RolloutInvariantError,
  );
};

const readFreshV8State = async ({ createV8Client, keypair, remote }) => {
  const api = await createPinnedV8(createV8Client, keypair, remote);
  return { api, state: await readClientState(api, { verifyDescriptor: true }) };
};

const waitForV8Target = async ({
  createV8Client,
  keypair,
  remote,
  config,
  sleepImpl,
}) =>
  retry({
    operation: async () => {
      const result = await readFreshV8State({ createV8Client, keypair, remote });
      assertIdentity(result.state, remote, "v8 postcheck");
      assertDependencyFingerprint(
        result.state,
        config.targetVersion,
        config.targetFingerprint,
        `${remote.name} v8 postcheck`,
      );
      return result;
    },
    timeoutMs: config.waitReadyTimeoutMs,
    delayMs: config.waitReadyDelayMs,
    description: `${remote.name} fresh authenticated v8 client`,
    sleepImpl,
  });

const waitForV8Source = async ({
  createV8Client,
  keypair,
  remote,
  config,
  sleepImpl,
}) =>
  retry({
    operation: async () => {
      const result = await readFreshV8State({ createV8Client, keypair, remote });
      assertIdentity(result.state, remote, "v8 rollback postcheck");
      assertDependencyFingerprint(
        result.state,
        config.rollbackVersion,
        config.rollbackFingerprint,
        `${remote.name} v8 rollback postcheck`,
      );
      return result;
    },
    timeoutMs: config.waitReadyTimeoutMs,
    delayMs: config.waitReadyDelayMs,
    description: `${remote.name} fresh authenticated v8 rollback client`,
    sleepImpl,
  });

const fingerprintMismatches = (state, expectedFingerprint) =>
  Object.entries(expectedFingerprint)
    .filter(([dependency, version]) => state.versions[dependency] !== version)
    .map(
      ([dependency, version]) =>
        `${dependency}: expected ${version}, got ${state.versions[dependency] ?? "missing"}`,
    );

const fingerprintMatches = (state, expectedFingerprint) =>
  fingerprintMismatches(state, expectedFingerprint).length === 0;

const classifyV8State = (state, config, label) => {
  const advertisedServer = state.versions["@peerbit/server"];
  const sourceMatches = fingerprintMatches(state, config.rollbackFingerprint);
  const targetMatches = fingerprintMatches(state, config.targetFingerprint);

  if (advertisedServer !== undefined) {
    ensure(
      advertisedServer === config.expectedCurrentVersion || advertisedServer === config.targetVersion,
      `${label}: unsupported @peerbit/server@${advertisedServer}; expected ${config.expectedCurrentVersion} or ${config.targetVersion}`,
      RolloutInvariantError,
    );
    if (advertisedServer === config.targetVersion) {
      ensure(
        targetMatches,
        `${label}: target dependency fingerprint mismatch (${fingerprintMismatches(
          state,
          config.targetFingerprint,
        ).join("; ")})`,
        RolloutInvariantError,
      );
      return "target";
    }
    ensure(
      sourceMatches,
      `${label}: source dependency fingerprint mismatch (${fingerprintMismatches(
        state,
        config.rollbackFingerprint,
      ).join("; ")})`,
      RolloutInvariantError,
    );
    return "source";
  }

  ensure(
    sourceMatches !== targetMatches,
    sourceMatches
      ? `${label}: source and target fingerprints are ambiguous without @peerbit/server`
      : `${label}: dependency fingerprint matches neither the reviewed source nor target`,
    RolloutInvariantError,
  );
  return targetMatches ? "target" : "source";
};

const preflightRemote = ({ createV8Client, keypair, remote, config, sleepImpl }) =>
  retry({
    operation: async () => {
      const result = await readFreshV8State({ createV8Client, keypair, remote });
      assertIdentity(result.state, remote, "v8 preflight");
      return {
        protocol: "v8",
        phase: classifyV8State(result.state, config, `${remote.name} v8 preflight`),
        ...result,
      };
    },
    timeoutMs: config.waitReadyTimeoutMs,
    delayMs: config.waitReadyDelayMs,
    description: `${remote.name} pinned signed-request-v2 preflight`,
    sleepImpl,
  });

const chunk = (items, size) => {
  const result = [];
  for (let index = 0; index < items.length; index += size) result.push(items.slice(index, index + size));
  return result;
};

const rollbackOne = async ({
  item,
  config,
  keypair,
  createV8Client,
  waitForPublicWebSocketImpl,
  sleepImpl,
  logger,
}) => {
  const { remote } = item;
  const detected = await retry({
    operation: async () => {
      const result = await readFreshV8State({ createV8Client, keypair, remote });
      assertIdentity(result.state, remote, "v8 rollback preflight");
      const advertisedServer = result.state.versions["@peerbit/server"];
      const sourceMatches = fingerprintMatches(result.state, config.rollbackFingerprint);
      const targetMatches = fingerprintMatches(result.state, config.targetFingerprint);

      if (advertisedServer !== undefined) {
        ensure(
          advertisedServer === config.rollbackVersion || advertisedServer === config.targetVersion,
          `${remote.name}: rollback observed unsupported @peerbit/server@${advertisedServer}`,
          RolloutInvariantError,
        );
      }

      if (sourceMatches && (advertisedServer === undefined || advertisedServer === config.rollbackVersion)) {
        if (item.forwardPhase !== "not-called") {
          throw new Error(
            `${remote.name}: forward selfUpdate is ${item.forwardPhase}; source state is ambiguous until the target is observed`,
          );
        }
        return { action: "unchanged", ...result };
      }

      // The exact target fingerprint is preferred. A dependency mismatch may be
      // the reason rollback is running, so a pinned signed descriptor also permits
      // recovery when the server either advertises the target or omits its version.
      ensure(
        targetMatches || advertisedServer === config.targetVersion || advertisedServer === undefined,
        `${remote.name}: rollback could not establish a reviewed target state`,
        RolloutInvariantError,
      );
      return { action: "rollback", ...result };
    },
    timeoutMs: config.waitReadyTimeoutMs,
    delayMs: config.waitReadyDelayMs,
    description: `${remote.name} pinned v8 rollback detection`,
    sleepImpl,
  });

  if (detected.action === "rollback") {
    const response = await withTimeout(
      () => detected.api.selfUpdate(config.rollbackVersion),
      config.waitReadyTimeoutMs,
      `${remote.name} rollback selfUpdate`,
    );
    assertSelfUpdateResponse(response, config.rollbackVersion, `${remote.name} rollback`);
  } else {
    logger.log(`${remote.name}: forward call was not invoked; reviewed v8 source state is unchanged`);
  }

  await waitForV8Source({ createV8Client, keypair, remote, config, sleepImpl });
  await waitForPublicWebSocketImpl(remote, config.waitReadyTimeoutMs, config.waitReadyDelayMs, {
    sleepImpl,
  });
  logger.log(`${remote.name}: rollback verified -> @peerbit/server@${config.rollbackVersion}`);
};

export const runRollingSelfUpdate = async ({
  config,
  remotes,
  keypair,
  createV8Client,
  waitForPublicWebSocketImpl = waitForPublicWebSocket,
  sleepImpl = sleep,
  logger = console,
}) => {
  ensure(config && typeof config === "object", "config is required");
  ensure(Array.isArray(remotes) && remotes.length > 0, "at least one remote is required");
  ensure(typeof createV8Client === "function", "createV8Client is required");
  const rolloutMode = classifyRolloutContract(config);
  if (config.rolloutMode !== undefined) {
    ensure(
      config.rolloutMode === rolloutMode,
      `rolloutMode must be ${rolloutMode} for the reviewed contract`,
      RolloutInvariantError,
    );
  }

  logger.log(`Resolved ${remotes.length} remotes from ${config.bootstrapFile}`);
  const preflight = [];
  for (const remote of remotes) {
    const result = await preflightRemote({
      createV8Client,
      keypair,
      remote,
      config,
      sleepImpl,
    });
    if (result.phase === "target") {
      await waitForPublicWebSocketImpl(
        remote,
        config.waitReadyTimeoutMs,
        config.waitReadyDelayMs,
        { sleepImpl },
      );
      logger.log(`${remote.name}: already on verified @peerbit/server@${config.targetVersion}`);
    } else {
      ensure(
        rolloutMode === "v8-native",
        `${remote.name}: completed legacy rollout contract is inert and the pinned v8 target is not exact; refusing mutation`,
        RolloutInvariantError,
      );
      try {
        await waitForPublicWebSocketImpl(
          remote,
          config.waitReadyTimeoutMs,
          config.waitReadyDelayMs,
          { sleepImpl },
        );
      } catch (error) {
        logger.warn(
          `${remote.name}: source public WebSocket preflight failed; target postcheck remains mandatory: ${
            formatErrorSummary(toSafeError(error))
          }`,
        );
      }
      logger.log(`${remote.name}: verified v8 source @peerbit/server@${config.expectedCurrentVersion}`);
    }
    preflight.push({ remote, phase: result.phase, forwardPhase: "not-called" });
  }

  const sources = preflight.filter((item) => item.phase === "source");
  const transitioned = new Set();
  let completed = 0;

  for (const [batchIndex, batch] of chunk(sources, config.batchSize).entries()) {
    logger.log(`Updating batch ${batchIndex + 1}/${Math.ceil(sources.length / config.batchSize)}`);
    const settled = await Promise.allSettled(
      batch.map(async (item) => {
        try {
          const freshSource = await withTimeout(
            () => readFreshV8State({ createV8Client, keypair, remote: item.remote }),
            config.waitReadyTimeoutMs,
            `${item.remote.name} fresh v8 mutation preflight`,
          );
          assertIdentity(freshSource.state, item.remote, "v8 mutation preflight");
          ensure(
            classifyV8State(
              freshSource.state,
              config,
              `${item.remote.name} v8 mutation preflight`,
            ) === "source",
            `${item.remote.name}: source changed before mutation`,
            RolloutInvariantError,
          );
          transitioned.add(item);
          item.forwardPhase = "ambiguous";
          const response = await withTimeout(
            () => freshSource.api.selfUpdate(config.targetVersion),
            config.waitReadyTimeoutMs,
            `${item.remote.name} forward selfUpdate`,
          );
          assertSelfUpdateResponse(response, config.targetVersion, `${item.remote.name} update`);
          item.forwardPhase = "acknowledged";
          await waitForV8Target({
            createV8Client,
            keypair,
            remote: item.remote,
            config,
            sleepImpl,
          });
          await waitForPublicWebSocketImpl(
            item.remote,
            config.waitReadyTimeoutMs,
            config.waitReadyDelayMs,
            { sleepImpl },
          );
          logger.log(`${item.remote.name}: update verified -> @peerbit/server@${config.targetVersion}`);
          return item;
        } catch (error) {
          throw toSafeError(error);
        }
      }),
    );

    const failures = settled.filter((result) => result.status === "rejected");
    completed += settled.length - failures.length;
    if (failures.length === 0) continue;

    const updateErrors = failures.map((failure) => toSafeError(failure.reason));
    for (const error of updateErrors) {
      logger.error(`Batch failure: ${formatErrorSummary(error)}`);
    }
    if (!config.rollbackOnFailure) {
      throw new AggregateError(updateErrors, `Rolling update failed in batch ${batchIndex + 1}`);
    }
    if (transitioned.size === 0) {
      throw new AggregateError(
        updateErrors,
        `Rolling update failed in batch ${batchIndex + 1} before any selfUpdate was initiated`,
      );
    }

    const rollbackErrors = [];
    logger.log(`Starting protocol-aware rollback for ${transitioned.size} node(s)`);
    for (const item of transitioned) {
      try {
        await rollbackOne({
          item,
          config,
          keypair,
          createV8Client,
          waitForPublicWebSocketImpl,
          sleepImpl,
          logger,
        });
      } catch (error) {
        const safeError = toSafeError(error);
        rollbackErrors.push(safeError);
        logger.error(
          `${item.remote.name}: fatal rollback failure: ${formatErrorSummary(safeError)}`,
        );
      }
    }
    if (rollbackErrors.length > 0) {
      throw new AggregateError(
        [...updateErrors, ...rollbackErrors],
        `Rolling update failed in batch ${batchIndex + 1}; ${rollbackErrors.length} rollback(s) failed fatally`,
      );
    }
    throw new AggregateError(
      updateErrors,
      `Rolling update failed in batch ${batchIndex + 1}; all initiated nodes were rolled back and verified`,
    );
  }

  logger.log(
    `Rolling update succeeded: ${completed} transitioned, ${preflight.length - sources.length} already current.`,
  );
  return { transitioned: completed, alreadyCurrent: preflight.length - sources.length };
};

const decodeKeypair = async () => {
  const encoded = process.env.PEERBIT_ADMIN_KEY_B64;
  ensure(
    typeof encoded === "string" && encoded.length > 0,
    "Missing PEERBIT_ADMIN_KEY_B64 environment variable",
  );
  const canonical = Buffer.from(encoded, "base64").toString("base64");
  ensure(canonical === encoded, "PEERBIT_ADMIN_KEY_B64 must be canonical base64");
  const [{ deserialize }, { Ed25519Keypair }] = await Promise.all([
    import("@dao-xyz/borsh"),
    import("@peerbit/crypto"),
  ]);
  return deserialize(Buffer.from(encoded, "base64"), Ed25519Keypair);
};

export const runCli = async (argv = process.argv) => {
  const args = parseArgs(argv);
  const config = readAndValidateRolloutConfig({ configFile: args["config-file"] });
  if (args["validate-config"]) {
    console.log(JSON.stringify(rolloutConfigOutput(config)));
    return;
  }

  const keypair = await decodeKeypair();
  const { createClient: createV8Client } = await import("@peerbit/server");
  const remotes = readBootstrapRemotes(config.bootstrapPath);
  await runRollingSelfUpdate({
    config,
    remotes,
    keypair,
    createV8Client,
  });
};

const isMain = process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;
export const reportCliFailure = (error) => {
  console.error(formatErrorSummary(toSafeError(error)));
  process.exitCode = 1;
  // A raced-out Axios request cannot be aborted through the public client API.
  // Give stderr a brief flush window, then prevent its socket from pinning CI.
  const forcedExit = setTimeout(() => process.exit(1), 150);
  forcedExit.unref();
};

if (isMain) {
  runCli().catch(reportCliFailure);
}
