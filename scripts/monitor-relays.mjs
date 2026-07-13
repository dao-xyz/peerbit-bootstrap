#!/usr/bin/env node

import crypto from "node:crypto";
import fs from "node:fs";
import https from "node:https";
import { isIPv4 } from "node:net";
import path from "node:path";
import tls from "node:tls";

const FILES = ["bootstrap-4.env", "bootstrap-5.env"];
const WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MINIMUM_CERTIFICATE_VALIDITY_MS = 14 * 24 * 60 * 60 * 1_000;
const DNS_RESOLVERS = [
  { name: "Google", endpoint: "https://dns.google/resolve" },
  { name: "Cloudflare", endpoint: "https://cloudflare-dns.com/dns-query" },
];

const isPublicIpv4 = (address) => {
  if (!isIPv4(address)) return false;
  const [a, b] = address.split(".").map(Number);
  if (a === 0 || a === 10 || a === 127 || a >= 224) return false;
  if (a === 100 && b >= 64 && b <= 127) return false;
  if (a === 169 && b === 254) return false;
  if (a === 172 && b >= 16 && b <= 31) return false;
  if (a === 192 && (b === 0 || b === 168)) return false;
  if (a === 198 && (b === 18 || b === 19)) return false;
  return true;
};

const resolvePublicIpv4 = async (resolver, host) => {
  const url = new URL(resolver.endpoint);
  url.searchParams.set("name", host);
  url.searchParams.set("type", "A");
  const response = await fetch(url, {
    headers: { Accept: "application/dns-json" },
    redirect: "error",
    signal: AbortSignal.timeout(10_000),
  });
  if (response.status !== 200) {
    throw new Error(
      `${resolver.name} DNS-over-HTTPS returned HTTP ${response.status}`,
    );
  }
  const payload = await response.json();
  if (payload.Status !== 0) {
    throw new Error(`${resolver.name} DNS returned status ${payload.Status}`);
  }
  const addresses = [
    ...new Set(
      (Array.isArray(payload.Answer) ? payload.Answer : [])
        .filter((answer) => answer?.type === 1)
        .map((answer) => answer.data),
    ),
  ].sort();
  if (addresses.length === 0) {
    throw new Error(`${resolver.name} DNS returned no IPv4 addresses`);
  }
  if (addresses.length > 8) {
    throw new Error(
      `${resolver.name} DNS returned an unexpected ${addresses.length} addresses`,
    );
  }
  for (const address of addresses) {
    if (!isPublicIpv4(address)) {
      throw new Error(
        `${resolver.name} DNS returned non-public IPv4 address ${address}`,
      );
    }
  }
  return addresses;
};

const readRelays = () => {
  const relays = [];
  for (const file of FILES) {
    const lines = fs
      .readFileSync(path.resolve(process.cwd(), file), "utf8")
      .trimEnd()
      .split("\n");
    for (const [index, multiaddr] of lines.entries()) {
      const parts = multiaddr.split("/");
      if (
        parts.length !== 8 ||
        parts[1] !== "dns4" ||
        parts[3] !== "tcp" ||
        parts[4] !== "4003" ||
        parts[5] !== "wss" ||
        parts[6] !== "p2p"
      ) {
        throw new Error(
          `${file}:${index + 1}: invalid canonical relay multiaddr`,
        );
      }
      relays.push({
        file,
        multiaddr,
        host: parts[2],
        port: 4003,
        peerId: parts[7],
      });
    }
  }
  return relays;
};

const readPeerId = (host, address) =>
  new Promise((resolve, reject) => {
    const request = https.request(
      {
        hostname: address,
        port: 9002,
        path: "/peer/id",
        method: "GET",
        servername: host,
        rejectUnauthorized: true,
        headers: {
          Accept: "text/plain, application/json",
          Host: `${host}:9002`,
        },
      },
      (response) => {
        if (response.statusCode !== 200) {
          response.resume();
          reject(
            new Error(
              `peer API returned HTTP ${response.statusCode || "unknown"}`,
            ),
          );
          return;
        }
        const chunks = [];
        let length = 0;
        response.on("data", (chunk) => {
          length += chunk.length;
          if (length > 4_096) {
            request.destroy(new Error("peer API response exceeded 4 KiB"));
            return;
          }
          chunks.push(chunk);
        });
        response.once("end", () => {
          const text = Buffer.concat(chunks).toString("utf8").trim();
          try {
            const parsed = JSON.parse(text);
            resolve(typeof parsed === "string" ? parsed : text);
          } catch {
            resolve(text);
          }
        });
      },
    );
    request.setTimeout(10_000, () =>
      request.destroy(new Error("peer API timed out")),
    );
    request.once("error", reject);
    request.end();
  });

const probeWebSocket = (host, address, port) =>
  new Promise((resolve, reject) => {
    const key = crypto.randomBytes(16).toString("base64");
    const expectedAccept = crypto
      .createHash("sha1")
      .update(key + WEBSOCKET_GUID)
      .digest("base64");
    let settled = false;
    let received = Buffer.alloc(0);
    const finish = (error) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      if (error) reject(error);
      else resolve();
    };
    const socket = tls.connect({
      host: address,
      port,
      servername: host,
      rejectUnauthorized: true,
    });
    socket.setTimeout(10_000);
    socket.once("secureConnect", () => {
      const certificate = socket.getPeerCertificate();
      const certificateExpiry = Date.parse(certificate.valid_to || "");
      if (!Number.isFinite(certificateExpiry)) {
        finish(new Error("TLS certificate expiry is unavailable"));
        return;
      }
      if (certificateExpiry - Date.now() < MINIMUM_CERTIFICATE_VALIDITY_MS) {
        finish(
          new Error(
            `TLS certificate expires too soon (${new Date(certificateExpiry).toISOString()})`,
          ),
        );
        return;
      }
      socket.write(
        [
          "GET / HTTP/1.1",
          `Host: ${host}:${port}`,
          "Connection: Upgrade",
          "Upgrade: websocket",
          `Sec-WebSocket-Key: ${key}`,
          "Sec-WebSocket-Version: 13",
          "Origin: https://peerbit.org",
          "",
          "",
        ].join("\r\n"),
      );
    });
    socket.on("data", (chunk) => {
      received = Buffer.concat([received, chunk]);
      if (received.length > 16_384) {
        finish(new Error("WebSocket response headers exceeded 16 KiB"));
        return;
      }
      const headerEnd = received.indexOf("\r\n\r\n");
      if (headerEnd < 0) return;
      const lines = received
        .subarray(0, headerEnd)
        .toString("latin1")
        .split("\r\n");
      const status = lines.shift();
      if (!/^HTTP\/1\.[01] 101(?: |$)/.test(status || "")) {
        finish(
          new Error(`WebSocket upgrade returned ${status || "no status line"}`),
        );
        return;
      }
      const headers = new Map();
      for (const line of lines) {
        const separator = line.indexOf(":");
        if (separator <= 0) continue;
        headers.set(
          line.slice(0, separator).trim().toLowerCase(),
          line.slice(separator + 1).trim(),
        );
      }
      if (headers.get("upgrade")?.toLowerCase() !== "websocket") {
        finish(new Error("WebSocket upgrade header is missing"));
        return;
      }
      const connectionTokens = (headers.get("connection") || "")
        .toLowerCase()
        .split(",")
        .map((token) => token.trim());
      if (!connectionTokens.includes("upgrade")) {
        finish(new Error("WebSocket connection upgrade token is missing"));
        return;
      }
      if (headers.get("sec-websocket-accept") !== expectedAccept) {
        finish(new Error("WebSocket accept hash mismatch"));
        return;
      }
      finish();
    });
    socket.once("timeout", () =>
      finish(new Error("WebSocket probe timed out")),
    );
    socket.once("error", (error) => finish(error));
    socket.once("end", () =>
      finish(new Error("WebSocket connection closed before upgrade")),
    );
  });

const probeRelay = async (relay) => {
  try {
    const resolved = await Promise.all(
      DNS_RESOLVERS.map(async (resolver) => ({
        resolver: resolver.name,
        addresses: await resolvePublicIpv4(resolver, relay.host),
      })),
    );
    const expectedAddresses = resolved[0].addresses;
    for (const answer of resolved.slice(1)) {
      if (
        JSON.stringify(answer.addresses) !== JSON.stringify(expectedAddresses)
      ) {
        throw new Error(
          `DNS mismatch: ${resolved.map(({ resolver, addresses }) => `${resolver}=${addresses.join(",")}`).join(" ")}`,
        );
      }
    }
    const addresses = expectedAddresses;
    await Promise.all(
      addresses.map(async (address) => {
        const peerId = await readPeerId(relay.host, address);
        if (peerId !== relay.peerId) {
          throw new Error(
            `${address} peer ID mismatch: expected ${relay.peerId}, got ${peerId || "empty"}`,
          );
        }
        await probeWebSocket(relay.host, address, relay.port);
      }),
    );
    return {
      ok: true,
      message: `${relay.file}: ${relay.host} -> ${addresses.join(", ")}; peer ID and WSS verified`,
    };
  } catch (error) {
    return {
      ok: false,
      message: `${relay.file}: ${relay.host}: ${error.message}`,
    };
  }
};

const relays = readRelays();
const results = [];
const concurrency = 4;
for (let offset = 0; offset < relays.length; offset += concurrency) {
  results.push(
    ...(await Promise.all(
      relays.slice(offset, offset + concurrency).map(probeRelay),
    )),
  );
}

const failures = results.filter((result) => !result.ok);
for (const result of results) {
  if (result.ok) console.log(result.message);
  else console.error(`ERROR: ${result.message}`);
}
if (failures.length > 0) {
  throw new Error(`${failures.length}/${relays.length} relay probes failed`);
}
console.log(
  `All ${relays.length} advertised relay(s) passed production probes`,
);
