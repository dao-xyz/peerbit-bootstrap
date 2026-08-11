import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import {
  IMAGE_SIZE_EXCEPTION_CVES,
  validateImageSizeAuditCommandResult,
} from "./image-size-advisory-exception.mjs";

const packageLock = JSON.parse(readFileSync("package-lock.json", "utf8"));
const npmExecPath = process.env.npm_execpath;
const command = npmExecPath
  ? process.execPath
  : process.platform === "win32"
    ? "npm.cmd"
    : "npm";
const args = npmExecPath
  ? [npmExecPath, "audit", "--omit=dev", "--json"]
  : ["audit", "--omit=dev", "--json"];
const audit = spawnSync(command, args, {
  cwd: process.cwd(),
  env: process.env,
  encoding: "utf8",
  maxBuffer: 16 * 1024 * 1024,
  timeout: 120_000,
});

assert.equal(
  audit.error,
  undefined,
  "failed to start npm audit: " + audit.error?.message,
);
assert.equal(audit.signal, null, "npm audit was terminated by " + audit.signal);
const result = validateImageSizeAuditCommandResult({
  status: audit.status,
  stdout: audit.stdout,
  stderr: audit.stderr,
  packageLock,
});

if (result.status === "temporary-exception") {
  console.log(
    "Accepted only " +
      IMAGE_SIZE_EXCEPTION_CVES.join(" and ") +
      " through the exact reviewed image-size dependency graph; exception expires at " +
      result.expiresAt +
      ".",
  );
} else {
  console.log("Production dependency audit is clean.");
}
