#!/usr/bin/env node

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const FILES = ["bootstrap-4.env", "bootstrap-5.env"];

const ensure = (condition, message) => {
  if (!condition) throw new Error(message);
};

const parseArgs = (argv) => {
  const options = {
    baseUrl: "https://bootstrap.peerbit.org",
    attempts: 1,
    delayMs: 0,
  };
  for (let index = 2; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    ensure(
      key?.startsWith("--") && value !== undefined,
      `Invalid argument: ${key}`,
    );
    if (key === "--base-url") options.baseUrl = value;
    else if (key === "--attempts") options.attempts = Number(value);
    else if (key === "--delay-ms") options.delayMs = Number(value);
    else throw new Error(`Unknown argument: ${key}`);
  }
  ensure(
    Number.isInteger(options.attempts) && options.attempts >= 1,
    "Invalid attempts",
  );
  ensure(
    Number.isInteger(options.delayMs) && options.delayMs >= 0,
    "Invalid delay-ms",
  );
  const url = new URL(options.baseUrl);
  ensure(url.protocol === "https:", "Public verification requires HTTPS");
  ensure(
    url.username === "" && url.password === "",
    "Credentials are not allowed in base URL",
  );
  url.pathname = url.pathname.replace(/\/$/, "");
  options.baseUrl = url.toString().replace(/\/$/, "");
  return options;
};

const sha256 = (bytes) =>
  crypto.createHash("sha256").update(bytes).digest("hex");
const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const headerTokens = (response, name) =>
  (response.headers.get(name) || "")
    .toLowerCase()
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);

const verifyHeaders = (response, file) => {
  const contentType = response.headers.get("content-type")?.toLowerCase() || "";
  ensure(
    /^text\/plain\s*;\s*charset=utf-8$/.test(contentType),
    `${file}: unexpected content-type ${contentType}`,
  );
  ensure(
    response.headers.get("access-control-allow-origin") === "*",
    `${file}: missing wildcard CORS header`,
  );
  ensure(
    headerTokens(response, "access-control-expose-headers").includes("etag"),
    `${file}: ETag is not exposed to browsers`,
  );
  const cacheControl = headerTokens(response, "cache-control");
  const expectedCacheControl = ["public", "max-age=0", "must-revalidate"];
  ensure(
    cacheControl.length === expectedCacheControl.length &&
      new Set(cacheControl).size === expectedCacheControl.length &&
      expectedCacheControl.every((directive) =>
        cacheControl.includes(directive),
      ),
    `${file}: unexpected cache-control ${cacheControl.join(", ") || "<empty>"}`,
  );
  ensure(
    response.headers.get("cross-origin-resource-policy")?.toLowerCase() ===
      "cross-origin",
    `${file}: missing cross-origin resource policy`,
  );
  ensure(
    response.headers.get("x-content-type-options")?.toLowerCase() === "nosniff",
    `${file}: missing nosniff header`,
  );
  ensure(
    headerTokens(response, "x-robots-tag").includes("noindex"),
    `${file}: missing noindex header`,
  );
};

const fetchStrict = (url, init = {}) =>
  fetch(url, {
    redirect: "manual",
    signal: AbortSignal.timeout(15_000),
    ...init,
  });

const verifyOnce = async (baseUrl) => {
  for (const file of FILES) {
    const expected = fs.readFileSync(path.resolve(process.cwd(), file));
    const url = `${baseUrl}/${file}?verify=${Date.now()}`;
    const response = await fetchStrict(url, {
      headers: {
        "Cache-Control": "no-cache",
        Origin: "https://peerbit.org",
      },
    });
    ensure(
      response.status === 200,
      `${file}: expected HTTP 200, got ${response.status}`,
    );
    ensure(
      response.url === url,
      `${file}: unexpected redirect to ${response.url}`,
    );
    verifyHeaders(response, file);
    const etag = response.headers.get("etag");
    ensure(etag && etag.length > 2, `${file}: missing ETag`);
    const actual = Buffer.from(await response.arrayBuffer());
    ensure(
      expected.equals(actual),
      `${file}: SHA-256 mismatch (expected ${sha256(expected)}, got ${sha256(actual)})`,
    );

    const conditional = await fetchStrict(url, {
      headers: {
        "Cache-Control": "no-cache",
        "If-None-Match": etag,
        Origin: "https://peerbit.org",
      },
    });
    ensure(
      conditional.status === 304,
      `${file}: expected conditional HTTP 304, got ${conditional.status}`,
    );
    console.log(`${file}: HTTP 200/304, SHA-256 ${sha256(actual)}`);
  }

  const missingUrl = `${baseUrl}/not-found-${crypto.randomUUID()}`;
  const missing = await fetchStrict(missingUrl, {
    headers: { "Cache-Control": "no-cache" },
  });
  ensure(
    missing.status === 404,
    `Unknown path: expected HTTP 404, got ${missing.status}`,
  );
  console.log("Unknown path: HTTP 404");
};

const options = parseArgs(process.argv);
let lastError;
for (let attempt = 1; attempt <= options.attempts; attempt += 1) {
  try {
    await verifyOnce(options.baseUrl);
    console.log(
      `Production verification passed on attempt ${attempt}/${options.attempts}`,
    );
    process.exit(0);
  } catch (error) {
    lastError = error;
    console.error(
      `Verification attempt ${attempt}/${options.attempts} failed: ${error.message}`,
    );
    if (attempt < options.attempts) await sleep(options.delayMs);
  }
}
throw lastError;
