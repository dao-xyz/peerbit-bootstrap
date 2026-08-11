import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
  validateImageSizeAuditCommandResult,
  validateImageSizeAuditException,
  validateImageSizePackageLock,
} from "./image-size-advisory-exception.mjs";

const beforeExpiry = "2026-08-11T00:00:00Z";
const approvedExpiry = "2026-08-22T00:00:00Z";
const packageLock = JSON.parse(
  await readFile(new URL("../package-lock.json", import.meta.url), "utf8"),
);

const advisories = [
  {
    source: 1138808,
    name: "image-size",
    dependency: "image-size",
    url: "https://github.com/advisories/GHSA-w3rx-r6r6-pgpr",
    severity: "high",
    range: "<=2.0.2",
  },
  {
    source: 1138809,
    name: "image-size",
    dependency: "image-size",
    url: "https://github.com/advisories/GHSA-5p2g-fcmc-qvqq",
    severity: "high",
    range: "<=2.0.2",
  },
];

const exactAuditReport = {
  auditReportVersion: 2,
  vulnerabilities: {
    "@react-native/community-cli-plugin": {
      name: "@react-native/community-cli-plugin",
      severity: "high",
      isDirect: false,
      via: ["metro", "metro-config"],
      effects: ["react-native"],
      range: "*",
      nodes: ["node_modules/@react-native/community-cli-plugin"],
      fixAvailable: true,
    },
    "@react-native/virtualized-lists": {
      name: "@react-native/virtualized-lists",
      severity: "high",
      isDirect: false,
      via: ["react-native"],
      effects: ["react-native"],
      range: ">=0.85.0-nightly-20260108-1236b6be4",
      nodes: ["node_modules/@react-native/virtualized-lists"],
      fixAvailable: true,
    },
    "image-size": {
      name: "image-size",
      severity: "high",
      isDirect: false,
      via: advisories,
      effects: ["metro"],
      range: "*",
      nodes: ["node_modules/image-size"],
      fixAvailable: true,
    },
    metro: {
      name: "metro",
      severity: "high",
      isDirect: false,
      via: ["image-size", "metro-config", "metro-transform-worker"],
      effects: [
        "@react-native/community-cli-plugin",
        "metro-config",
        "metro-transform-worker",
      ],
      range: ">=0.22.1",
      nodes: ["node_modules/metro"],
      fixAvailable: true,
    },
    "metro-config": {
      name: "metro-config",
      severity: "high",
      isDirect: false,
      via: ["metro"],
      effects: ["@react-native/community-cli-plugin", "metro"],
      range: "*",
      nodes: ["node_modules/metro-config"],
      fixAvailable: true,
    },
    "metro-transform-worker": {
      name: "metro-transform-worker",
      severity: "high",
      isDirect: false,
      via: ["metro"],
      effects: ["metro"],
      range: ">=0.60.0",
      nodes: ["node_modules/metro-transform-worker"],
      fixAvailable: true,
    },
    "react-native": {
      name: "react-native",
      severity: "high",
      isDirect: false,
      via: [
        "@react-native/community-cli-plugin",
        "@react-native/virtualized-lists",
      ],
      effects: ["@react-native/virtualized-lists"],
      range: ">=0.73.0-nightly-20230506-1af868c52",
      nodes: ["node_modules/react-native"],
      fixAvailable: true,
    },
  },
  metadata: {
    vulnerabilities: {
      info: 0,
      low: 0,
      moderate: 0,
      high: 7,
      critical: 0,
      total: 7,
    },
  },
};

const cleanAuditReport = {
  auditReportVersion: 2,
  vulnerabilities: {},
  metadata: {
    vulnerabilities: {
      info: 0,
      low: 0,
      moderate: 0,
      high: 0,
      critical: 0,
      total: 0,
    },
  },
};

const clone = (value) => structuredClone(value);
const rootPackageLock = ({
  serverVersion,
  serverIntegrity,
  peerbitVersion,
  peerbitIntegrity,
}) => {
  const lock = clone(packageLock);
  lock.packages[""].dependencies["@peerbit/server"] = serverVersion;
  Object.assign(lock.packages["node_modules/@peerbit/server"], {
    version: serverVersion,
    resolved:
      "https://registry.npmjs.org/@peerbit/server/-/server-" +
      serverVersion +
      ".tgz",
    integrity: serverIntegrity,
  });
  lock.packages["node_modules/@peerbit/server"].dependencies.peerbit =
    peerbitVersion;
  Object.assign(lock.packages["node_modules/peerbit"], {
    version: peerbitVersion,
    resolved:
      "https://registry.npmjs.org/peerbit/-/peerbit-" + peerbitVersion + ".tgz",
    integrity: peerbitIntegrity,
  });
  return lock;
};
const currentPackageLock = () =>
  rootPackageLock({
    serverVersion: "8.0.7",
    serverIntegrity:
      "sha512-/OJ/ROfsFSBr4wrtZJZu3K2cAd2THKZPUnNQrk40srOhd36j7GlOjBChXAurY0r2aYTD8nBekQ7OWm5x09WDbg==",
    peerbitVersion: "5.3.17",
    peerbitIntegrity:
      "sha512-N5ktJJTORnWoUuX+0sxSiNIPwOjSePU+z1RSwATN0bXKS98cyXlBdcNWyxRuQbFeL8a+0ndos/jyr6MpAB7RfQ==",
  });
const targetPackageLock = () =>
  rootPackageLock({
    serverVersion: "8.0.13",
    serverIntegrity:
      "sha512-Ef6FDWdqlwitQQERQP0mIXX5/2/66naie63e0GXc41760Kzbmtva0IaaUXzgthKaXY6bVVxNGSMntHA6IndL8w==",
    peerbitVersion: "5.3.23",
    peerbitIntegrity:
      "sha512-CKtTt2l2QWZRbA5oZr8Q3Zw4sG81T0+C87hj8j/221fUoDXZsgHShHB80tWmRWrOhLb4E0KRWHiQoAxRgzaVSg==",
  });
const validate = ({
  report = exactAuditReport,
  lock = packageLock,
  now = beforeExpiry,
} = {}) =>
  validateImageSizeAuditException({
    auditReport: report,
    packageLock: lock,
    now,
  });

test("accepts only the exact reviewed npm audit closure", () => {
  assert.equal(IMAGE_SIZE_EXCEPTION_EXPIRES_AT, approvedExpiry);
  assert.deepEqual(validate(), {
    status: "temporary-exception",
    cves: ["CVE-2025-71330", "CVE-2025-71329"],
    expiresAt: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
  });
});

test("accepts only the atomic current and rollout server-to-peerbit roots", () => {
  assert.equal(
    validateImageSizePackageLock(currentPackageLock(), { now: beforeExpiry })
      .status,
    "validated-exception-graph",
  );
  assert.equal(
    validateImageSizePackageLock(targetPackageLock(), { now: beforeExpiry })
      .status,
    "validated-exception-graph",
  );

  for (const [mutate, pattern] of [
    [
      (lock) => (lock.packages[""].dependencies["@peerbit/server"] = "8.0.8"),
      /exactly one reviewed @peerbit\/server variant/,
    ],
    [
      (lock) =>
        (lock.packages["node_modules/@peerbit/server"].version = "8.0.8"),
      /server.*version drifted/,
    ],
    [
      (lock) =>
        (lock.packages["node_modules/@peerbit/server"].resolved += "?mirror"),
      /server.*tarball URL drifted/,
    ],
    [
      (lock) =>
        (lock.packages["node_modules/@peerbit/server"].integrity += "drift"),
      /server.*integrity drifted/,
    ],
    [
      (lock) =>
        (lock.packages["node_modules/@peerbit/server"].dependencies.peerbit =
          "5.3.18"),
      /exact dependencies edge to peerbit/,
    ],
    [
      (lock) => (lock.packages["node_modules/peerbit"].version = "5.3.18"),
      /peerbit.*version drifted/,
    ],
    [
      (lock) => (lock.packages["node_modules/peerbit"].resolved += "?mirror"),
      /peerbit.*tarball URL drifted/,
    ],
    [
      (lock) => (lock.packages["node_modules/peerbit"].integrity += "drift"),
      /peerbit.*integrity drifted/,
    ],
    [
      (lock) =>
        (lock.packages["node_modules/peerbit"].dependencies["@libp2p/webrtc"] =
          "*"),
      /exact dependencies edge to @libp2p\/webrtc/,
    ],
  ]) {
    const lock = currentPackageLock();
    mutate(lock);
    assert.throws(
      () => validateImageSizePackageLock(lock, { now: beforeExpiry }),
      pattern,
    );
  }

  const currentPackagesUnderTargetRoot = currentPackageLock();
  currentPackagesUnderTargetRoot.packages[""].dependencies["@peerbit/server"] =
    "8.0.13";
  assert.throws(
    () =>
      validateImageSizePackageLock(currentPackagesUnderTargetRoot, {
        now: beforeExpiry,
      }),
    /server.*version drifted/,
  );

  const targetPackagesUnderCurrentRoot = targetPackageLock();
  targetPackagesUnderCurrentRoot.packages[""].dependencies["@peerbit/server"] =
    "8.0.7";
  assert.throws(
    () =>
      validateImageSizePackageLock(targetPackagesUnderCurrentRoot, {
        now: beforeExpiry,
      }),
    /server.*version drifted/,
  );
});

test("accepts a clean audit only while the reviewed lock exception is active", () => {
  assert.deepEqual(validate({ report: cleanAuditReport }), { status: "clean" });
  assert.throws(
    () =>
      validate({
        report: cleanAuditReport,
        now: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
      }),
    /expired/,
  );
});

for (const [name, mutate] of [
  [
    "source",
    (report) => (report.vulnerabilities["image-size"].via[0].source += 1),
  ],
  [
    "URL",
    (report) => (report.vulnerabilities["image-size"].via[0].url += "-drift"),
  ],
  [
    "severity",
    (report) =>
      (report.vulnerabilities["image-size"].via[0].severity = "moderate"),
  ],
  [
    "range",
    (report) => (report.vulnerabilities["image-size"].via[0].range = "<=3"),
  ],
]) {
  test("rejects image-size advisory " + name + " drift", () => {
    const report = clone(exactAuditReport);
    mutate(report);
    assert.throws(() => validate({ report }), /unexpected npm audit v2 node/);
  });
}

test("rejects missing and additional image-size advisories", () => {
  const missing = clone(exactAuditReport);
  missing.vulnerabilities["image-size"].via.pop();
  assert.throws(
    () => validate({ report: missing }),
    /unexpected npm audit v2 node/,
  );

  const additional = clone(exactAuditReport);
  additional.vulnerabilities["image-size"].via.push({
    ...advisories[0],
    source: 9999999,
    url: "https://github.com/advisories/GHSA-extra-extra-extra",
  });
  assert.throws(
    () => validate({ report: additional }),
    /unexpected npm audit v2 node/,
  );
});

for (const [name, mutate] of [
  ["via", (node) => node.via.push("unexpected")],
  ["effects", (node) => node.effects.push("unexpected")],
  ["range", (node) => (node.range = ">=0")],
  ["nodes", (node) => node.nodes.push("node_modules/nested/metro")],
  ["isDirect", (node) => (node.isDirect = true)],
  ["fixAvailable", (node) => (node.fixAvailable = false)],
]) {
  test("rejects audit node " + name + " drift", () => {
    const report = clone(exactAuditReport);
    mutate(report.vulnerabilities.metro);
    assert.throws(() => validate({ report }), /unexpected npm audit v2 node/);
  });
}

test("rejects missing or structured npm remediation metadata", () => {
  for (const mutate of [
    (node) => delete node.fixAvailable,
    (node) => (node.fixAvailable = { name: "metro", version: "0.0.0" }),
  ]) {
    const report = clone(exactAuditReport);
    mutate(report.vulnerabilities.metro);
    assert.throws(() => validate({ report }), /unexpected npm audit v2 node/);
  }
});

test("rejects vulnerability closure and count drift", () => {
  const missing = clone(exactAuditReport);
  delete missing.vulnerabilities["metro-config"];
  assert.throws(() => validate({ report: missing }), /closure changed/);

  const additional = clone(exactAuditReport);
  additional.vulnerabilities.unexpected = clone(
    additional.vulnerabilities["metro-config"],
  );
  additional.vulnerabilities.unexpected.name = "unexpected";
  additional.metadata.vulnerabilities.high = 8;
  additional.metadata.vulnerabilities.total = 8;
  assert.throws(
    () => validate({ report: additional }),
    /exactly seven high-severity nodes/,
  );
});

test("rejects unknown audit schemas and malformed zero counts", () => {
  const schema = clone(exactAuditReport);
  schema.auditReportVersion = 3;
  assert.throws(() => validate({ report: schema }), /report v2/);

  const counts = clone(cleanAuditReport);
  counts.metadata.vulnerabilities.high = 1;
  assert.throws(() => validate({ report: counts }), /zero counts/);
});

test("hard-expires at the exact boundary and rejects invalid clocks", () => {
  assert.doesNotThrow(() =>
    validateImageSizePackageLock(packageLock, {
      now: "2026-08-21T23:59:59.999Z",
    }),
  );
  assert.throws(
    () =>
      validateImageSizePackageLock(packageLock, {
        now: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
      }),
    /expired/,
  );
  assert.throws(
    () => validateImageSizePackageLock(packageLock, { now: "not-a-date" }),
    /clock is invalid/,
  );
});

test("pins npm lock schema, package version, tarball URL, integrity, and edges", () => {
  for (const [name, mutate, pattern] of [
    ["schema", (lock) => (lock.lockfileVersion = 4), /package-lock v3/],
    [
      "version",
      (lock) => (lock.packages["node_modules/metro"].version = "0.84.5"),
      /version drifted/,
    ],
    [
      "tarball",
      (lock) => (lock.packages["node_modules/metro"].resolved += "?mirror"),
      /tarball URL drifted/,
    ],
    [
      "integrity",
      (lock) => (lock.packages["node_modules/metro"].integrity += "drift"),
      /integrity drifted/,
    ],
    [
      "edge",
      (lock) =>
        (lock.packages["node_modules/metro"].dependencies["image-size"] = "*"),
      /exact dependencies edge/,
    ],
  ]) {
    const lock = clone(packageLock);
    mutate(lock);
    assert.throws(
      () => validateImageSizePackageLock(lock, { now: beforeExpiry }),
      pattern,
      name,
    );
  }
});

test("pins the image-size queue and inherits runtime tail", () => {
  for (const [mutate, pattern] of [
    [
      (lock) =>
        (lock.packages["node_modules/image-size"].dependencies.queue = "*"),
      /exact dependencies edge to queue/,
    ],
    [
      (lock) =>
        (lock.packages["node_modules/queue"].dependencies.inherits = "*"),
      /exact dependencies edge to inherits/,
    ],
    [
      (lock) => (lock.packages["node_modules/inherits"].version = "2.0.3"),
      /version drifted/,
    ],
  ]) {
    const lock = clone(packageLock);
    mutate(lock);
    assert.throws(
      () => validateImageSizePackageLock(lock, { now: beforeExpiry }),
      pattern,
    );
  }
});

test("rejects lockfile classification drift on root and vulnerable nodes", () => {
  for (const [path, field, value] of [
    ["node_modules/@peerbit/server", "dev", true],
    ["node_modules/peerbit", "optional", true],
    ["node_modules/image-size", "dev", true],
    ["node_modules/metro", "link", true],
    ["node_modules/@peerbit/server", "optional", 1],
    ["node_modules/image-size", "dev", "true"],
  ]) {
    const lock = clone(packageLock);
    lock.packages[path][field] = value;
    assert.throws(
      () => validateImageSizePackageLock(lock, { now: beforeExpiry }),
      /lockfile classification drifted/,
    );
  }

  const peerReclassification = clone(packageLock);
  delete peerReclassification.packages["node_modules/image-size"].peer;
  assert.throws(
    () =>
      validateImageSizePackageLock(peerReclassification, {
        now: beforeExpiry,
      }),
    /lockfile classification drifted/,
  );
});

test("rejects direct, duplicate, and missing vulnerable packages", () => {
  const direct = clone(packageLock);
  direct.packages[""].dependencies["image-size"] = "1.2.1";
  assert.throws(
    () => validateImageSizePackageLock(direct, { now: beforeExpiry }),
    /must not depend directly/,
  );

  const duplicate = clone(packageLock);
  duplicate.packages["node_modules/example/node_modules/image-size"] = clone(
    duplicate.packages["node_modules/image-size"],
  );
  assert.throws(
    () => validateImageSizePackageLock(duplicate, { now: beforeExpiry }),
    /exactly one installed image-size/,
  );

  const missing = clone(packageLock);
  delete missing.packages["node_modules/image-size"];
  assert.throws(
    () => validateImageSizePackageLock(missing, { now: beforeExpiry }),
    /exactly one installed image-size/,
  );
});

for (const dependencyName of [
  "@libp2p/webrtc",
  "react-native-webrtc",
  "react-native",
  "@react-native/community-cli-plugin",
  "@react-native/virtualized-lists",
  "metro",
  "metro-config",
  "metro-transform-worker",
  "image-size",
  "queue",
  "inherits",
]) {
  test("rejects an alternate " + dependencyName + " owner", () => {
    const lock = clone(packageLock);
    lock.packages["node_modules/@peerbit/server"].optionalDependencies ??= {};
    lock.packages["node_modules/@peerbit/server"].optionalDependencies[
      dependencyName
    ] = "*";
    assert.throws(
      () => validateImageSizePackageLock(lock, { now: beforeExpiry }),
      /alternate .* dependency owners/,
    );
  });
}

test("requires @peerbit/server to be the only peerbit owner", () => {
  const lock = clone(packageLock);
  lock.packages["node_modules/@dao-xyz/borsh"].optionalDependencies ??= {};
  lock.packages["node_modules/@dao-xyz/borsh"].optionalDependencies.peerbit =
    "*";
  assert.throws(
    () => validateImageSizePackageLock(lock, { now: beforeExpiry }),
    /only the pinned @peerbit\/server package/,
  );
});

test("requires the root to be the only @peerbit/server owner", () => {
  const lock = clone(packageLock);
  lock.packages["node_modules/@dao-xyz/borsh"].optionalDependencies ??= {};
  lock.packages["node_modules/@dao-xyz/borsh"].optionalDependencies[
    "@peerbit/server"
  ] = "8.0.7";
  assert.throws(
    () => validateImageSizePackageLock(lock, { now: beforeExpiry }),
    /only the bootstrap root may introduce/,
  );
});

test("validates npm audit process status and JSON fail closed", () => {
  assert.equal(
    validateImageSizeAuditCommandResult({
      status: 1,
      stdout: JSON.stringify(exactAuditReport),
      packageLock,
      now: beforeExpiry,
    }).status,
    "temporary-exception",
  );
  assert.equal(
    validateImageSizeAuditCommandResult({
      status: 0,
      stdout: JSON.stringify(cleanAuditReport),
      packageLock,
      now: beforeExpiry,
    }).status,
    "clean",
  );
  for (const args of [
    { status: 0, stdout: JSON.stringify(exactAuditReport) },
    { status: 1, stdout: JSON.stringify(cleanAuditReport) },
    { status: 2, stdout: JSON.stringify(exactAuditReport) },
    { status: null, stdout: JSON.stringify(exactAuditReport) },
    { status: 1, stdout: "not-json" },
    {
      status: 1,
      stdout: JSON.stringify({ error: { summary: "registry unavailable" } }),
    },
  ]) {
    assert.throws(() =>
      validateImageSizeAuditCommandResult({
        ...args,
        packageLock,
        now: beforeExpiry,
      }),
    );
  }
});

test("keeps the exception inside every required validation and production gate", async () => {
  const [packageJsonText, ciWorkflow, rolloutWorkflow] = await Promise.all([
    readFile(new URL("../package.json", import.meta.url), "utf8"),
    readFile(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8"),
    readFile(
      new URL(
        "../.github/workflows/deploy-bootstrap-rollout.yml",
        import.meta.url,
      ),
      "utf8",
    ),
  ]);
  const packageJson = JSON.parse(packageJsonText);
  const workflowStep = (workflow, name) => {
    const marker = "      - name: " + name + "\n";
    const start = workflow.indexOf(marker);
    assert.notEqual(start, -1, "missing workflow step " + name);
    assert.equal(
      workflow.indexOf(marker, start + marker.length),
      -1,
      "duplicate workflow step " + name,
    );
    const next = workflow.indexOf("\n      - name: ", start + marker.length);
    return workflow.slice(start, next === -1 ? workflow.length : next);
  };
  assert.equal(
    packageJson.scripts["test:security-audit"],
    "node --test scripts/image-size-advisory-exception.spec.mjs",
  );
  assert.equal(
    packageJson.scripts["validate:security-audit"],
    "node scripts/validate-image-size-audit.mjs",
  );
  assert.equal(
    packageJson.scripts["security:audit"],
    "npm run test:security-audit && npm run validate:security-audit",
  );
  const ciAuditStep = workflowStep(
    ciWorkflow,
    "Validate rollout dependency graph",
  );
  assert.match(ciAuditStep, /^\s+npm run security:audit\s*$/m);
  assert.doesNotMatch(ciAuditStep, /continue-on-error/);
  assert.doesNotMatch(ciWorkflow, /npm audit --omit=dev --audit-level=high/);
  const rolloutValidationStep = workflowStep(
    rolloutWorkflow,
    "Validate migration contract",
  );
  assert.match(rolloutValidationStep, /^\s+npm run security:audit\s*$/m);
  assert.doesNotMatch(rolloutValidationStep, /continue-on-error/);
  const productionAuditStep = workflowStep(
    rolloutWorkflow,
    "Revalidate temporary dependency exception",
  );
  assert.match(productionAuditStep, /run: npm run security:audit\s*$/);
  assert.doesNotMatch(productionAuditStep, /continue-on-error/);
  assert(
    rolloutWorkflow.indexOf("      - name: Rolling self-update") >
      rolloutWorkflow.indexOf(
        "      - name: Revalidate temporary dependency exception",
      ),
    "the production audit must precede the rolling update",
  );
  assert.doesNotMatch(
    rolloutWorkflow,
    /npm audit --omit=dev --audit-level=high/,
  );
  assert.match(
    await readFile(new URL("../README.md", import.meta.url), "utf8"),
    new RegExp(approvedExpiry.replaceAll(".", "\\.")),
  );
});
