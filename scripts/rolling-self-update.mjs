#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { createClient } from "@peerbit/server";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const parseArgs = (argv) => {
  const result = {};
  for (let i = 2; i < argv.length; i++) {
    const token = argv[i];
    if (!token?.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      result[key] = "true";
      continue;
    }
    result[key] = next;
    i += 1;
  }
  return result;
};

const ensure = (condition, message) => {
  if (!condition) throw new Error(message);
};

const readBootstrapRemotes = (bootstrapFile) => {
  const file = path.resolve(process.cwd(), bootstrapFile);
  ensure(fs.existsSync(file), `Missing bootstrap file: ${file}`);
  const lines = fs
    .readFileSync(file, "utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"));

  const seen = new Set();
  const remotes = [];
  for (const line of lines) {
    const match = line.match(/\/(dns4|dns6|ip4|ip6)\/([^/]+)/);
    if (!match) continue;
    const hostType = match[1];
    const host = match[2];
    const key = `${hostType}:${host}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const protocol = hostType.startsWith("ip") ? "http" : "https";
    remotes.push({
      name: `bootstrap-${remotes.length + 1}`,
      host,
      address: `${protocol}://${host}`,
    });
  }
  ensure(remotes.length > 0, `No remotes resolved from ${bootstrapFile}`);
  return remotes;
};

const decodeKeypair = () => {
  const b64 = process.env.PEERBIT_ADMIN_KEY_B64;
  ensure(
    typeof b64 === "string" && b64.length > 0,
    "Missing PEERBIT_ADMIN_KEY_B64 environment variable",
  );
  const bytes = Buffer.from(b64, "base64");
  return deserialize(bytes, Ed25519Keypair);
};

const waitForReady = async (api, remote, timeoutMs, delayMs) => {
  const start = Date.now();
  let lastError;
  while (Date.now() - start < timeoutMs) {
    try {
      await api.peer.id.get();
      return;
    } catch (error) {
      lastError = error;
      await sleep(delayMs);
    }
  }
  throw new Error(
    `Timed out waiting for ${remote.name} (${remote.address}) to become ready. Last error: ${
      lastError?.message || lastError || "unknown"
    }`,
  );
};

const getServerVersion = async (api) => {
  const versions = await api.dependency.versions();
  const v = versions["@peerbit/server"];
  ensure(typeof v === "string" && v.length > 0, "Missing @peerbit/server version");
  return v;
};

const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
};

const run = async () => {
  const args = parseArgs(process.argv);
  const bootstrapFile = args["bootstrap-file"] || "bootstrap-4.env";
  const targetVersion = args["target-version"];
  const batchSize = Math.max(1, Number(args["batch-size"] || "1"));
  const waitReadyTimeoutMs = Math.max(1_000, Number(args["wait-ready-timeout-ms"] || "180000"));
  const waitReadyDelayMs = Math.max(200, Number(args["wait-ready-delay-ms"] || "3000"));
  const rollbackOnFailure = String(args["rollback-on-failure"] || "true") === "true";

  ensure(
    typeof targetVersion === "string" && targetVersion.length > 0,
    "Missing --target-version",
  );

  const keypair = decodeKeypair();
  const remotes = readBootstrapRemotes(bootstrapFile);

  console.log(`Resolved ${remotes.length} remotes from ${bootstrapFile}`);

  const state = [];
  for (const remote of remotes) {
    const api = await createClient(keypair, { address: remote.address });
    await waitForReady(api, remote, waitReadyTimeoutMs, waitReadyDelayMs);
    const peerId = await api.peer.id.get();
    const previousVersion = await getServerVersion(api);
    console.log(`${remote.name}: preflight ready (peerId=${peerId}, version=${previousVersion})`);
    state.push({ remote, previousVersion });
  }

  const updated = [];
  const batches = chunk(state, batchSize);

  const rollback = async (targets) => {
    if (!rollbackOnFailure) return;
    console.log(`Starting rollback for ${targets.length} node(s)`);
    for (const item of targets) {
      try {
        const api = await createClient(keypair, { address: item.remote.address });
        await api.selfUpdate(item.previousVersion);
        await waitForReady(api, item.remote, waitReadyTimeoutMs, waitReadyDelayMs);
        const versionAfter = await getServerVersion(api);
        if (versionAfter !== item.previousVersion) {
          throw new Error(
            `rollback version mismatch: expected ${item.previousVersion}, got ${versionAfter}`,
          );
        }
        console.log(`${item.remote.name}: rollback complete -> ${versionAfter}`);
      } catch (error) {
        console.error(`${item.remote.name}: rollback failed`, error);
      }
    }
  };

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Updating batch ${i + 1}/${batches.length} (${batch.length} node(s))`);

    const settled = await Promise.allSettled(
      batch.map(async (item) => {
        const api = await createClient(keypair, { address: item.remote.address });
        const resp = await api.selfUpdate(targetVersion);
        await waitForReady(api, item.remote, waitReadyTimeoutMs, waitReadyDelayMs);
        const versionAfter = await getServerVersion(api);
        if (versionAfter !== resp.version) {
          throw new Error(
            `version mismatch after update: expected ${resp.version}, got ${versionAfter}`,
          );
        }
        console.log(`${item.remote.name}: update complete -> ${versionAfter}`);
        return item;
      }),
    );

    const succeeded = settled
      .filter((r) => r.status === "fulfilled")
      .map((r) => r.value);
    const failed = settled.filter((r) => r.status === "rejected");

    updated.push(...succeeded);

    if (failed.length > 0) {
      for (const failure of failed) {
        console.error("Batch failure:", failure.reason);
      }
      await rollback([...updated, ...batch.filter((b) => !updated.includes(b))]);
      throw new Error(
        `Rolling update failed in batch ${i + 1}. Rolled back updated nodes where possible.`,
      );
    }
  }

  console.log(`Rolling update succeeded on ${updated.length}/${state.length} nodes.`);
};

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
