#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { domainToASCII } from "node:url";

const ROOT = process.cwd();
const DIST = path.join(ROOT, "dist");
const BOOTSTRAP_FILES = ["bootstrap-4.env", "bootstrap-5.env"];
const REQUIRED_HEADERS = [
  "Access-Control-Allow-Origin: *",
  "Access-Control-Expose-Headers: ETag",
  "Cache-Control: public, max-age=0, must-revalidate",
  "Content-Type: text/plain; charset=utf-8",
  "Cross-Origin-Resource-Policy: cross-origin",
  "X-Content-Type-Options: nosniff",
  "X-Robots-Tag: noindex",
];
const BASE58_ALPHABET =
  "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const BASE58_INDEX = new Map(
  [...BASE58_ALPHABET].map((character, index) => [character, index]),
);

const ensure = (condition, message) => {
  if (!condition) throw new Error(message);
};

const decodeBase58 = (value) => {
  ensure(value.length > 0, "peer ID is empty");
  let number = 0n;
  for (const character of value) {
    const digit = BASE58_INDEX.get(character);
    ensure(
      digit !== undefined,
      `peer ID contains non-base58 character: ${character}`,
    );
    number = number * 58n + BigInt(digit);
  }
  const decoded = [];
  while (number > 0n) {
    decoded.unshift(Number(number & 0xffn));
    number >>= 8n;
  }
  let leadingZeroes = 0;
  for (const character of value) {
    if (character !== "1") break;
    leadingZeroes += 1;
  }
  return Uint8Array.from([...Array(leadingZeroes).fill(0), ...decoded]);
};

const readVarint = (bytes, offset) => {
  let value = 0;
  let shift = 0;
  for (
    let index = offset;
    index < bytes.length && index < offset + 10;
    index += 1
  ) {
    const byte = bytes[index];
    value += (byte & 0x7f) * 2 ** shift;
    ensure(
      Number.isSafeInteger(value),
      "peer ID multihash varint is too large",
    );
    if ((byte & 0x80) === 0) {
      return { value, nextOffset: index + 1 };
    }
    shift += 7;
  }
  throw new Error("peer ID contains an invalid multihash varint");
};

const validatePeerId = (peerId, file, lineNumber) => {
  ensure(
    peerId.length >= 32 && peerId.length <= 128,
    `${file}:${lineNumber}: peer ID length is outside the supported range`,
  );
  const bytes = decodeBase58(peerId);
  const code = readVarint(bytes, 0);
  const digestLength = readVarint(bytes, code.nextOffset);
  const actualLength = bytes.length - digestLength.nextOffset;
  ensure(
    digestLength.value === actualLength,
    `${file}:${lineNumber}: peer ID multihash length mismatch`,
  );
  ensure(
    (code.value === 0x00 && actualLength >= 4 && actualLength <= 42) ||
      (code.value === 0x12 && actualLength === 32),
    `${file}:${lineNumber}: unsupported peer ID multihash`,
  );
};

const validateHostname = (hostname, file, lineNumber) => {
  ensure(
    hostname === hostname.toLowerCase(),
    `${file}:${lineNumber}: DNS hostname must be lowercase`,
  );
  ensure(
    domainToASCII(hostname) === hostname,
    `${file}:${lineNumber}: DNS hostname must be canonical ASCII`,
  );
  ensure(
    hostname.length <= 253 && hostname.includes("."),
    `${file}:${lineNumber}: invalid DNS hostname`,
  );
  for (const label of hostname.split(".")) {
    ensure(
      /^(?!-)[a-z0-9-]{1,63}(?<!-)$/.test(label),
      `${file}:${lineNumber}: invalid DNS label: ${label || "<empty>"}`,
    );
  }
};

const validateMultiaddr = (line, file, lineNumber) => {
  const parts = line.split("/");
  ensure(
    parts.length === 8 && parts[0] === "",
    `${file}:${lineNumber}: expected canonical /dns4/.../tcp/4003/wss/p2p/... multiaddr`,
  );
  const [, dnsProtocol, hostname, tcpProtocol, port, websocket, p2p, peerId] =
    parts;
  ensure(dnsProtocol === "dns4", `${file}:${lineNumber}: expected dns4`);
  validateHostname(hostname, file, lineNumber);
  ensure(tcpProtocol === "tcp", `${file}:${lineNumber}: expected tcp`);
  ensure(
    port === "4003",
    `${file}:${lineNumber}: expected public WSS port 4003`,
  );
  ensure(websocket === "wss", `${file}:${lineNumber}: expected wss`);
  ensure(p2p === "p2p", `${file}:${lineNumber}: expected p2p`);
  validatePeerId(peerId, file, lineNumber);
  return { hostname, peerId, port: Number(port) };
};

const validateBootstrapFile = (file) => {
  const sourcePath = path.join(ROOT, file);
  const stat = fs.lstatSync(sourcePath);
  ensure(
    stat.isFile() && !stat.isSymbolicLink(),
    `${file}: must be a regular file`,
  );
  const bytes = fs.readFileSync(sourcePath);
  ensure(
    bytes.length > 0 && bytes.length <= 8_192,
    `${file}: invalid byte length`,
  );
  const content = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  ensure(!content.startsWith("\uFEFF"), `${file}: UTF-8 BOM is not allowed`);
  ensure(!content.includes("\r"), `${file}: CRLF line endings are not allowed`);
  ensure(
    content.endsWith("\n"),
    `${file}: exactly one final newline is required`,
  );
  ensure(
    !content.endsWith("\n\n"),
    `${file}: exactly one final newline is required`,
  );

  const lines = content.slice(0, -1).split("\n");
  ensure(
    lines.length >= 1 && lines.length <= 32,
    `${file}: invalid address count`,
  );
  const seenLines = new Set();
  const seenHosts = new Set();
  for (const [index, line] of lines.entries()) {
    const lineNumber = index + 1;
    ensure(
      line.length > 0,
      `${file}:${lineNumber}: blank lines are not allowed`,
    );
    ensure(
      line === line.trim(),
      `${file}:${lineNumber}: surrounding whitespace is not allowed`,
    );
    ensure(
      !line.startsWith("#"),
      `${file}:${lineNumber}: comments are not allowed`,
    );
    ensure(!seenLines.has(line), `${file}:${lineNumber}: duplicate multiaddr`);
    seenLines.add(line);
    const { hostname } = validateMultiaddr(line, file, lineNumber);
    ensure(
      !seenHosts.has(hostname),
      `${file}:${lineNumber}: duplicate relay hostname`,
    );
    seenHosts.add(hostname);
  }
  return { bytes, addresses: lines.length };
};

const discoveredBootstrapFiles = fs
  .readdirSync(ROOT)
  .filter((file) => /^bootstrap-.*\.env$/.test(file))
  .sort();
ensure(
  JSON.stringify(discoveredBootstrapFiles) === JSON.stringify(BOOTSTRAP_FILES),
  `Expected exactly ${BOOTSTRAP_FILES.join(", ")}; found ${
    discoveredBootstrapFiles.join(", ") || "none"
  }`,
);

const validated = BOOTSTRAP_FILES.map((file) => [
  file,
  validateBootstrapFile(file),
]);

const wranglerPath = path.join(ROOT, "wrangler.jsonc");
const wranglerStat = fs.lstatSync(wranglerPath);
ensure(
  wranglerStat.isFile() && !wranglerStat.isSymbolicLink(),
  "wrangler.jsonc must be a regular file",
);
const wranglerContent = fs.readFileSync(wranglerPath, "utf8");
const expectedWranglerContent = `{
  "$schema": "tools/wrangler/node_modules/wrangler/config-schema.json",
  "name": "peerbit-bootstrap",
  "compatibility_date": "2026-07-13",
  "workers_dev": false,
  "assets": {
    "directory": "./dist",
    "html_handling": "none",
    "not_found_handling": "none",
  },
  "routes": [
    {
      "pattern": "bootstrap.peerbit.org",
      "custom_domain": true,
    },
  ],
}
`;
ensure(
  wranglerContent === expectedWranglerContent,
  "wrangler.jsonc must exactly match the reviewed asset-only Worker configuration",
);

const headersPath = path.join(ROOT, "static", "_headers");
const headersStat = fs.lstatSync(headersPath);
ensure(
  headersStat.isFile() && !headersStat.isSymbolicLink(),
  "static/_headers must be a regular file",
);
const headersBytes = fs.readFileSync(headersPath);
ensure(
  headersBytes.length > 0 && headersBytes.length <= 8_192,
  "static/_headers: invalid size",
);
const headersContent = new TextDecoder("utf-8", { fatal: true }).decode(
  headersBytes,
);
ensure(
  !headersContent.includes("\r"),
  "static/_headers: CRLF line endings are not allowed",
);
ensure(
  headersContent.endsWith("\n"),
  "static/_headers: final newline is required",
);
const expectedHeadersContent = `${BOOTSTRAP_FILES.map(
  (file) =>
    `/${file}\n${REQUIRED_HEADERS.map((header) => `  ${header}`).join("\n")}`,
).join("\n\n")}\n`;
ensure(
  headersContent === expectedHeadersContent,
  "static/_headers must exactly match the reviewed production header contract",
);

fs.rmSync(DIST, { recursive: true, force: true });
fs.mkdirSync(DIST, { recursive: true });
for (const [file, { bytes }] of validated) {
  fs.writeFileSync(path.join(DIST, file), bytes, { flag: "wx" });
}
fs.writeFileSync(path.join(DIST, "_headers"), headersBytes, { flag: "wx" });

const builtFiles = fs.readdirSync(DIST).sort();
ensure(
  JSON.stringify(builtFiles) ===
    JSON.stringify(["_headers", "bootstrap-4.env", "bootstrap-5.env"]),
  `Unexpected build output: ${builtFiles.join(", ")}`,
);

for (const [file, { bytes, addresses }] of validated) {
  console.log(
    `Validated ${file}: ${addresses} address(es), ${bytes.length} bytes`,
  );
}
console.log(`Built ${builtFiles.join(", ")} in dist/`);
