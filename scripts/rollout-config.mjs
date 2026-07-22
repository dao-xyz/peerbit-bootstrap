import fs from "node:fs";
import path from "node:path";

const CONFIG_KEYS = [
  "batchSize",
  "bootstrapFile",
  "expectedCurrentVersion",
  "rerollReason",
  "rollbackFingerprint",
  "rollbackIntegrity",
  "rollbackOnFailure",
  "rollbackVersion",
  "targetFingerprint",
  "targetIntegrity",
  "targetVersion",
  "waitReadyDelayMs",
  "waitReadyTimeoutMs",
];

export const FINGERPRINT_KEYS = [
  "@peerbit/blocks",
  "@peerbit/crypto",
  "@peerbit/program",
  "@peerbit/pubsub",
  "@peerbit/time",
  "peerbit",
];

const ROOT_DEPENDENCY_KEYS = [
  "@dao-xyz/borsh",
  "@peerbit/crypto",
  "@peerbit/server",
  "@peerbit/server-legacy",
];

const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/;

const ensure = (condition, message) => {
  if (!condition) throw new Error(message);
};

const isPlainObject = (value) =>
  value !== null &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

const sortedKeys = (value) => Object.keys(value).sort();

const assertExactKeys = (value, expected, label) => {
  ensure(isPlainObject(value), `${label} must be a JSON object`);
  const actual = sortedKeys(value);
  const wanted = [...expected].sort();
  ensure(
    actual.length === wanted.length && actual.every((key, index) => key === wanted[index]),
    `${label} must contain exactly: ${wanted.join(", ")}; got: ${actual.join(", ") || "none"}`,
  );
};

const assertExactSemver = (value, label) => {
  ensure(typeof value === "string" && SEMVER.test(value), `${label} must be an exact semver`);
};

const assertIntegrity = (value, label) => {
  ensure(
    typeof value === "string" && /^sha512-[A-Za-z0-9+/]{86}==$/.test(value),
    `${label} must be a canonical sha512 SRI string`,
  );
  const encoded = value.slice("sha512-".length);
  const decoded = Buffer.from(encoded, "base64");
  ensure(
    decoded.length === 64 && decoded.toString("base64") === encoded,
    `${label} must encode exactly 64 SHA-512 bytes`,
  );
};

const assertInteger = (value, label, min, max) => {
  ensure(
    Number.isSafeInteger(value) && value >= min && value <= max,
    `${label} must be an integer from ${min} through ${max}`,
  );
};

const resolveRepoFile = (repoRoot, relativeFile, label, suffix) => {
  ensure(
    typeof relativeFile === "string" &&
      relativeFile.length > 0 &&
      relativeFile === relativeFile.trim(),
    `${label} must be a non-empty relative path`,
  );
  ensure(!path.isAbsolute(relativeFile), `${label} must be relative to the repository`);
  ensure(!relativeFile.includes("\\"), `${label} must use '/' path separators`);
  const parts = relativeFile.split("/");
  ensure(
    parts.every((part) => part.length > 0 && part !== "." && part !== ".."),
    `${label} must not contain empty, '.' or '..' path segments`,
  );
  if (suffix) ensure(relativeFile.endsWith(suffix), `${label} must end in ${suffix}`);

  const resolved = path.resolve(repoRoot, relativeFile);
  const rootPrefix = `${path.resolve(repoRoot)}${path.sep}`;
  ensure(resolved.startsWith(rootPrefix), `${label} resolves outside the repository`);
  let cursor = path.resolve(repoRoot);
  for (const part of parts) {
    cursor = path.join(cursor, part);
    ensure(fs.existsSync(cursor), `Missing ${label}: ${resolved}`);
    ensure(!fs.lstatSync(cursor).isSymbolicLink(), `${label} must not traverse symlinks`);
  }
  ensure(fs.existsSync(resolved), `Missing ${label}: ${resolved}`);
  const stat = fs.lstatSync(resolved);
  ensure(stat.isFile() && !stat.isSymbolicLink(), `${label} must be a regular, non-symlink file`);
  ensure(stat.size <= 1024 * 1024, `${label} is unexpectedly large`);
  const real = fs.realpathSync(resolved);
  ensure(real.startsWith(rootPrefix), `${label} resolves outside the repository`);
  return resolved;
};

const readJson = (file, label) => {
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    throw new Error(`Invalid ${label} JSON at ${file}: ${error.message}`);
  }
  ensure(isPlainObject(parsed), `${label} must be a JSON object`);
  return parsed;
};

const assertFingerprint = (fingerprint, label) => {
  assertExactKeys(fingerprint, FINGERPRINT_KEYS, label);
  for (const key of FINGERPRINT_KEYS) {
    assertExactSemver(fingerprint[key], `${label}.${key}`);
  }
};

const assertConfigSchema = (config) => {
  assertExactKeys(config, CONFIG_KEYS, "rollout config");
  ensure(
    config.bootstrapFile === "bootstrap-5.env",
    "bootstrapFile must be exactly bootstrap-5.env for the production migration",
  );
  assertExactSemver(config.expectedCurrentVersion, "expectedCurrentVersion");
  assertExactSemver(config.targetVersion, "targetVersion");
  assertExactSemver(config.rollbackVersion, "rollbackVersion");
  ensure(
    config.expectedCurrentVersion === config.rollbackVersion,
    "expectedCurrentVersion must equal rollbackVersion for a deterministic rollback",
  );
  ensure(config.targetVersion !== config.rollbackVersion, "targetVersion must differ from rollbackVersion");
  assertIntegrity(config.targetIntegrity, "targetIntegrity");
  assertIntegrity(config.rollbackIntegrity, "rollbackIntegrity");
  ensure(config.targetIntegrity !== config.rollbackIntegrity, "targetIntegrity must differ from rollbackIntegrity");
  assertFingerprint(config.targetFingerprint, "targetFingerprint");
  assertFingerprint(config.rollbackFingerprint, "rollbackFingerprint");
  assertInteger(config.batchSize, "batchSize", 1, 32);
  ensure(config.batchSize === 1, "batchSize must be exactly 1 for the production migration");
  assertInteger(config.waitReadyTimeoutMs, "waitReadyTimeoutMs", 1_000, 3_600_000);
  assertInteger(config.waitReadyDelayMs, "waitReadyDelayMs", 100, 60_000);
  ensure(
    config.waitReadyDelayMs < config.waitReadyTimeoutMs,
    "waitReadyDelayMs must be less than waitReadyTimeoutMs",
  );
  ensure(config.rollbackOnFailure === true, "rollbackOnFailure must be true for the production migration");
  ensure(
    typeof config.rerollReason === "string" &&
      /^[a-z0-9][a-z0-9._-]{0,127}$/.test(config.rerollReason),
    "rerollReason must be a lowercase, single-line audit slug",
  );
};

const assertFingerprintMatchesPackage = (entry, fingerprint, label) => {
  ensure(isPlainObject(entry.dependencies), `${label}.dependencies must be an object`);
  for (const key of FINGERPRINT_KEYS) {
    ensure(
      entry.dependencies[key] === fingerprint[key],
      `${label} dependency ${key} must be ${fingerprint[key]}; got ${entry.dependencies[key] ?? "missing"}`,
    );
  }
};

const assertServerLockEntry = ({ entry, version, integrity, fingerprint, label, aliased }) => {
  ensure(isPlainObject(entry), `package-lock.json is missing ${label}`);
  ensure(entry.version === version, `${label}.version must be ${version}`);
  ensure(entry.integrity === integrity, `${label}.integrity does not match the audited config`);
  ensure(
    entry.resolved === `https://registry.npmjs.org/@peerbit/server/-/server-${version}.tgz`,
    `${label}.resolved must be the canonical npm registry tarball`,
  );
  if (aliased) {
    ensure(entry.name === "@peerbit/server", `${label}.name must identify the aliased package`);
  } else {
    ensure(entry.name === undefined || entry.name === "@peerbit/server", `${label}.name is invalid`);
  }
  assertFingerprintMatchesPackage(entry, fingerprint, label);
};

const assertPackageContracts = (repoRoot, config) => {
  const packageFile = resolveRepoFile(repoRoot, "package.json", "package.json", ".json");
  const lockFile = resolveRepoFile(repoRoot, "package-lock.json", "package-lock.json", ".json");
  const packageJson = readJson(packageFile, "package.json");
  const lock = readJson(lockFile, "package-lock.json");

  assertExactKeys(packageJson.dependencies, ROOT_DEPENDENCY_KEYS, "package.json dependencies");
  ensure(packageJson.overrides === undefined, "package.json overrides are forbidden for dual-client rollouts");
  ensure(
    packageJson.dependencies["@peerbit/server"] === config.targetVersion,
    "package.json must pin @peerbit/server to targetVersion",
  );
  ensure(
    packageJson.dependencies["@peerbit/server-legacy"] ===
      `npm:@peerbit/server@${config.rollbackVersion}`,
    "package.json must pin @peerbit/server-legacy to the rollbackVersion npm alias",
  );
  ensure(
    packageJson.dependencies["@peerbit/crypto"] === config.targetFingerprint["@peerbit/crypto"],
    "package.json must pin @peerbit/crypto to targetFingerprint.@peerbit/crypto",
  );

  ensure(lock.lockfileVersion === 3, "package-lock.json must use lockfileVersion 3");
  ensure(lock.requires === true, "package-lock.json must set requires=true");
  ensure(isPlainObject(lock.packages), "package-lock.json packages must be an object");
  const lockRoot = lock.packages[""];
  ensure(isPlainObject(lockRoot), "package-lock.json is missing its root package entry");
  assertExactKeys(lockRoot.dependencies, ROOT_DEPENDENCY_KEYS, "package-lock root dependencies");
  for (const key of ROOT_DEPENDENCY_KEYS) {
    ensure(
      lockRoot.dependencies[key] === packageJson.dependencies[key],
      `package-lock root dependency ${key} must exactly match package.json`,
    );
  }

  assertServerLockEntry({
    entry: lock.packages["node_modules/@peerbit/server"],
    version: config.targetVersion,
    integrity: config.targetIntegrity,
    fingerprint: config.targetFingerprint,
    label: "package-lock target @peerbit/server",
    aliased: false,
  });
  assertServerLockEntry({
    entry: lock.packages["node_modules/@peerbit/server-legacy"],
    version: config.rollbackVersion,
    integrity: config.rollbackIntegrity,
    fingerprint: config.rollbackFingerprint,
    label: "package-lock legacy @peerbit/server",
    aliased: true,
  });
};

export const readAndValidateRolloutConfig = ({
  configFile,
  repoRoot = process.cwd(),
} = {}) => {
  const root = fs.realpathSync(path.resolve(repoRoot));
  ensure(typeof configFile === "string", "Missing --config-file");
  const resolvedConfigFile = resolveRepoFile(root, configFile, "config file", ".json");
  const config = readJson(resolvedConfigFile, "rollout config");
  assertConfigSchema(config);
  const bootstrapPath = resolveRepoFile(root, config.bootstrapFile, "bootstrap file", ".env");
  assertPackageContracts(root, config);
  return Object.freeze({
    ...config,
    targetFingerprint: Object.freeze({ ...config.targetFingerprint }),
    rollbackFingerprint: Object.freeze({ ...config.rollbackFingerprint }),
    configFile,
    configPath: resolvedConfigFile,
    bootstrapPath,
    repoRoot: root,
  });
};

export const rolloutConfigOutput = (config) => ({
  bootstrapFile: config.bootstrapFile,
  expectedCurrentVersion: config.expectedCurrentVersion,
  targetVersion: config.targetVersion,
  rollbackVersion: config.rollbackVersion,
  batchSize: config.batchSize,
  waitReadyTimeoutMs: config.waitReadyTimeoutMs,
  waitReadyDelayMs: config.waitReadyDelayMs,
  rollbackOnFailure: config.rollbackOnFailure,
  rerollReason: config.rerollReason,
});
