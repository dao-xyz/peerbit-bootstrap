import assert from "node:assert/strict";

export const IMAGE_SIZE_EXCEPTION_EXPIRES_AT = "2026-08-22T00:00:00Z";
export const IMAGE_SIZE_EXCEPTION_CVES = Object.freeze([
  "CVE-2025-71330",
  "CVE-2025-71329",
]);

const dependencyFields = [
  "dependencies",
  "optionalDependencies",
  "peerDependencies",
  "devDependencies",
];

const lockClassificationFields = ["dev", "optional", "peer", "link"];
const peerClassifiedPackages = new Set([
  "react-native",
  "@react-native/community-cli-plugin",
  "@react-native/virtualized-lists",
  "metro",
  "metro-config",
  "metro-transform-worker",
  "image-size",
  "queue",
]);

const expectedAuditCounts = Object.freeze({
  info: 0,
  low: 0,
  moderate: 0,
  high: 7,
  critical: 0,
  total: 7,
});

const zeroAuditCounts = Object.freeze({
  info: 0,
  low: 0,
  moderate: 0,
  high: 0,
  critical: 0,
  total: 0,
});

const expectedImageSizeAdvisories = Object.freeze([
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
]);

const expectedAuditVulnerabilities = Object.freeze({
  "@react-native/community-cli-plugin": {
    severity: "high",
    isDirect: false,
    via: ["metro", "metro-config"],
    effects: ["react-native"],
    range: "*",
    nodes: ["node_modules/@react-native/community-cli-plugin"],
    fixAvailable: true,
  },
  "@react-native/virtualized-lists": {
    severity: "high",
    isDirect: false,
    via: ["react-native"],
    effects: ["react-native"],
    range: ">=0.85.0-nightly-20260108-1236b6be4",
    nodes: ["node_modules/@react-native/virtualized-lists"],
    fixAvailable: true,
  },
  "image-size": {
    severity: "high",
    isDirect: false,
    via: expectedImageSizeAdvisories,
    effects: ["metro"],
    range: "*",
    nodes: ["node_modules/image-size"],
    fixAvailable: true,
  },
  metro: {
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
    severity: "high",
    isDirect: false,
    via: ["metro"],
    effects: ["@react-native/community-cli-plugin", "metro"],
    range: "*",
    nodes: ["node_modules/metro-config"],
    fixAvailable: true,
  },
  "metro-transform-worker": {
    severity: "high",
    isDirect: false,
    via: ["metro"],
    effects: ["metro"],
    range: ">=0.60.0",
    nodes: ["node_modules/metro-transform-worker"],
    fixAvailable: true,
  },
  "react-native": {
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
});

const reviewedPackages = Object.freeze([
  {
    name: "@libp2p/webrtc",
    version: "6.0.29",
    resolved: "https://registry.npmjs.org/@libp2p/webrtc/-/webrtc-6.0.29.tgz",
    integrity:
      "sha512-RzsDn4+d/PhHIplWu7rDWMMpZtkF6kFB+fZayUZQxsApxuL3TdNF/mBR93Y5nEY1GswmAERQzTPr0si91xTXOw==",
    edges: [
      {
        field: "dependencies",
        dependency: "react-native-webrtc",
        range: "^124.0.6",
      },
    ],
  },
  {
    name: "react-native-webrtc",
    version: "124.0.8",
    resolved:
      "https://registry.npmjs.org/react-native-webrtc/-/react-native-webrtc-124.0.8.tgz",
    integrity:
      "sha512-uuQxvmk+mvnk5U0tr+1N42sKZqgm41fJrBA+fmCvML9J9P4roSh2So82t5RHAlu/vE9vxu5AKgivAiH61clCBg==",
    edges: [
      {
        field: "peerDependencies",
        dependency: "react-native",
        range: ">=0.60.0",
      },
    ],
  },
  {
    name: "react-native",
    version: "0.86.2",
    resolved:
      "https://registry.npmjs.org/react-native/-/react-native-0.86.2.tgz",
    integrity:
      "sha512-zbJXGZpwfZGA79Z9ob6Atvfx4nAQL8yJBa35s58E4Oo+khPykfQP2sTeumkKbjwajFYfVayg8pj7Il9nIfTk7A==",
    edges: [
      {
        field: "dependencies",
        dependency: "@react-native/community-cli-plugin",
        range: "0.86.2",
      },
      {
        field: "dependencies",
        dependency: "@react-native/virtualized-lists",
        range: "0.86.2",
      },
    ],
  },
  {
    name: "@react-native/community-cli-plugin",
    version: "0.86.2",
    resolved:
      "https://registry.npmjs.org/@react-native/community-cli-plugin/-/community-cli-plugin-0.86.2.tgz",
    integrity:
      "sha512-YHXNKoM6Y/HjREySZ5arET2xgiHgg67r1MdwJB//MPJAJ0Xc5g0u6UHxY9VzsHO3Y07dre6s0BinYwjt1SEWvQ==",
    edges: [
      { field: "dependencies", dependency: "metro", range: "^0.84.3" },
      {
        field: "dependencies",
        dependency: "metro-config",
        range: "^0.84.3",
      },
    ],
  },
  {
    name: "@react-native/virtualized-lists",
    version: "0.86.2",
    resolved:
      "https://registry.npmjs.org/@react-native/virtualized-lists/-/virtualized-lists-0.86.2.tgz",
    integrity:
      "sha512-uO0J72gh3EvE+1/GHRk18QRyBDTRHRB0AraAfojsRjbT7VMuJwKrZYaKGshavoaEud6aw00ZB9/8mTMIKjjcAw==",
    edges: [
      {
        field: "peerDependencies",
        dependency: "react-native",
        range: "0.86.2",
      },
    ],
  },
  {
    name: "metro",
    version: "0.84.4",
    resolved: "https://registry.npmjs.org/metro/-/metro-0.84.4.tgz",
    integrity:
      "sha512-8ETTubqfD6ornDy2zYDvRcKnVDOXdFJsjetYDBsY4oAsb6NJkiwFR+FaMESyGppFmQUyBQA4H4sFGxzcQSGtFA==",
    edges: [
      { field: "dependencies", dependency: "image-size", range: "^1.0.2" },
      { field: "dependencies", dependency: "metro-config", range: "0.84.4" },
      {
        field: "dependencies",
        dependency: "metro-transform-worker",
        range: "0.84.4",
      },
    ],
  },
  {
    name: "metro-config",
    version: "0.84.4",
    resolved:
      "https://registry.npmjs.org/metro-config/-/metro-config-0.84.4.tgz",
    integrity:
      "sha512-PMotGDjXcXLWo2TMRH+VR99phFNgYTwqh4OoieIKK3yTJa1Jmkl+fZJxDO0jfBvNF+WESHciHvpNuBtXaF3B0Q==",
    edges: [{ field: "dependencies", dependency: "metro", range: "0.84.4" }],
  },
  {
    name: "metro-transform-worker",
    version: "0.84.4",
    resolved:
      "https://registry.npmjs.org/metro-transform-worker/-/metro-transform-worker-0.84.4.tgz",
    integrity:
      "sha512-W1IYMvvXTu4MxYr7d9h7CeG2vpIr3bmLLIavkPY4O1ilzDrvS8z/NEe6y+pC44Ff7raMXQgYSfdqDUwN/i39gg==",
    edges: [{ field: "dependencies", dependency: "metro", range: "0.84.4" }],
  },
  {
    name: "image-size",
    version: "1.2.1",
    resolved: "https://registry.npmjs.org/image-size/-/image-size-1.2.1.tgz",
    integrity:
      "sha512-rH+46sQJ2dlwfjfhCyNx5thzrv+dtmBIhPHk0zgRUukHzZ/kRueTJXoYYsclBaKcSMBWuGbOFXtioLpzTb5euw==",
    edges: [{ field: "dependencies", dependency: "queue", range: "6.0.2" }],
  },
  {
    name: "queue",
    version: "6.0.2",
    resolved: "https://registry.npmjs.org/queue/-/queue-6.0.2.tgz",
    integrity:
      "sha512-iHZWu+q3IdFZFX36ro/lKBkSvfkztY5Y7HMiPlOUjhupPcG2JMfst2KKEpu5XndviX/3UhFbRngUPNKtgvtZiA==",
    edges: [{ field: "dependencies", dependency: "inherits", range: "~2.0.3" }],
  },
  {
    name: "inherits",
    version: "2.0.4",
    resolved: "https://registry.npmjs.org/inherits/-/inherits-2.0.4.tgz",
    integrity:
      "sha512-k/vGaX4/Yla3WzyMCvTQOXYeIHvqOKtnqBduzTHpzpQZzAskKMhZ2K+EnBiSM9zGSoIFeMpXKxa4dYeZIQqewQ==",
    edges: [],
  },
]);

const approvedRootVariants = Object.freeze([
  {
    rootServerRange: "8.0.7",
    server: {
      name: "@peerbit/server",
      version: "8.0.7",
      resolved: "https://registry.npmjs.org/@peerbit/server/-/server-8.0.7.tgz",
      integrity:
        "sha512-/OJ/ROfsFSBr4wrtZJZu3K2cAd2THKZPUnNQrk40srOhd36j7GlOjBChXAurY0r2aYTD8nBekQ7OWm5x09WDbg==",
      edges: [
        { field: "dependencies", dependency: "peerbit", range: "5.3.17" },
      ],
    },
    peerbit: {
      name: "peerbit",
      version: "5.3.17",
      resolved: "https://registry.npmjs.org/peerbit/-/peerbit-5.3.17.tgz",
      integrity:
        "sha512-N5ktJJTORnWoUuX+0sxSiNIPwOjSePU+z1RSwATN0bXKS98cyXlBdcNWyxRuQbFeL8a+0ndos/jyr6MpAB7RfQ==",
      edges: [
        {
          field: "dependencies",
          dependency: "@libp2p/webrtc",
          range: "^6.0.15",
        },
      ],
    },
  },
  {
    rootServerRange: "8.0.13",
    server: {
      name: "@peerbit/server",
      version: "8.0.13",
      resolved:
        "https://registry.npmjs.org/@peerbit/server/-/server-8.0.13.tgz",
      integrity:
        "sha512-Ef6FDWdqlwitQQERQP0mIXX5/2/66naie63e0GXc41760Kzbmtva0IaaUXzgthKaXY6bVVxNGSMntHA6IndL8w==",
      edges: [
        { field: "dependencies", dependency: "peerbit", range: "5.3.23" },
      ],
    },
    peerbit: {
      name: "peerbit",
      version: "5.3.23",
      resolved: "https://registry.npmjs.org/peerbit/-/peerbit-5.3.23.tgz",
      integrity:
        "sha512-CKtTt2l2QWZRbA5oZr8Q3Zw4sG81T0+C87hj8j/221fUoDXZsgHShHB80tWmRWrOhLb4E0KRWHiQoAxRgzaVSg==",
      edges: [
        {
          field: "dependencies",
          dependency: "@libp2p/webrtc",
          range: "^6.0.15",
        },
      ],
    },
  },
]);

const expectedOwners = Object.freeze({
  "@libp2p/webrtc": [
    {
      path: "node_modules/peerbit",
      field: "dependencies",
      range: "^6.0.15",
    },
  ],
  "react-native-webrtc": [
    {
      path: "node_modules/@libp2p/webrtc",
      field: "dependencies",
      range: "^124.0.6",
    },
  ],
  "react-native": [
    {
      path: "node_modules/@react-native/virtualized-lists",
      field: "peerDependencies",
      range: "0.86.2",
    },
    {
      path: "node_modules/react-native-webrtc",
      field: "peerDependencies",
      range: ">=0.60.0",
    },
  ],
  "@react-native/community-cli-plugin": [
    {
      path: "node_modules/react-native",
      field: "dependencies",
      range: "0.86.2",
    },
  ],
  "@react-native/virtualized-lists": [
    {
      path: "node_modules/react-native",
      field: "dependencies",
      range: "0.86.2",
    },
  ],
  metro: [
    {
      path: "node_modules/@react-native/community-cli-plugin",
      field: "dependencies",
      range: "^0.84.3",
    },
    {
      path: "node_modules/metro-config",
      field: "dependencies",
      range: "0.84.4",
    },
    {
      path: "node_modules/metro-transform-worker",
      field: "dependencies",
      range: "0.84.4",
    },
  ],
  "metro-config": [
    {
      path: "node_modules/@react-native/community-cli-plugin",
      field: "dependencies",
      range: "^0.84.3",
    },
    {
      path: "node_modules/metro",
      field: "dependencies",
      range: "0.84.4",
    },
  ],
  "metro-transform-worker": [
    {
      path: "node_modules/metro",
      field: "dependencies",
      range: "0.84.4",
    },
  ],
  "image-size": [
    {
      path: "node_modules/metro",
      field: "dependencies",
      range: "^1.0.2",
    },
  ],
  queue: [
    {
      path: "node_modules/image-size",
      field: "dependencies",
      range: "6.0.2",
    },
  ],
  inherits: [
    {
      path: "node_modules/bl",
      field: "dependencies",
      range: "^2.0.4",
    },
    {
      path: "node_modules/http-errors",
      field: "dependencies",
      range: "~2.0.4",
    },
    {
      path: "node_modules/queue",
      field: "dependencies",
      range: "~2.0.3",
    },
    {
      path: "node_modules/readable-stream",
      field: "dependencies",
      range: "^2.0.3",
    },
    {
      path: "node_modules/tar-fs/node_modules/tar-stream",
      field: "dependencies",
      range: "^2.0.3",
    },
  ],
});

const sortStrings = (values) =>
  [...values].sort((left, right) => left.localeCompare(right));

const sortOwners = (owners) =>
  [...owners].sort((left, right) =>
    (left.path + "\0" + left.field).localeCompare(
      right.path + "\0" + right.field,
    ),
  );

const auditCounts = (metadata) =>
  Object.fromEntries(
    Object.keys(zeroAuditCounts).map((key) => [key, metadata?.[key]]),
  );

const normalizedAdvisory = (advisory) => ({
  source: advisory?.source,
  name: advisory?.name,
  dependency: advisory?.dependency,
  url: advisory?.url,
  severity: advisory?.severity,
  range: advisory?.range,
});

const normalizeVia = (via) =>
  [...via]
    .map((item) => (typeof item === "string" ? item : normalizedAdvisory(item)))
    .sort((left, right) =>
      JSON.stringify(left).localeCompare(JSON.stringify(right)),
    );

const normalizeVulnerability = (vulnerability) => ({
  name: vulnerability?.name,
  severity: vulnerability?.severity,
  isDirect: vulnerability?.isDirect,
  via: normalizeVia(vulnerability?.via ?? []),
  effects: sortStrings(vulnerability?.effects ?? []),
  range: vulnerability?.range,
  nodes: sortStrings(vulnerability?.nodes ?? []),
  fixAvailable: vulnerability?.fixAvailable,
});

const normalizeExpectedVulnerability = (name, vulnerability) => ({
  name,
  ...vulnerability,
  via: normalizeVia(vulnerability.via),
  effects: sortStrings(vulnerability.effects),
  nodes: sortStrings(vulnerability.nodes),
});

const nowMilliseconds = (now) => {
  const value = now instanceof Date ? now.getTime() : new Date(now).getTime();
  assert(Number.isFinite(value), "the image-size exception clock is invalid");
  return value;
};

const assertExceptionActive = (now) => {
  assert(
    nowMilliseconds(now) < Date.parse(IMAGE_SIZE_EXCEPTION_EXPIRES_AT),
    "the temporary image-size advisory exception expired at " +
      IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
  );
};

const packageOwners = (packages, dependencyName) => {
  const owners = [];
  for (const [path, entry] of Object.entries(packages)) {
    for (const field of dependencyFields) {
      if (entry?.[field]?.[dependencyName] !== undefined) {
        owners.push({ path, field, range: entry[field][dependencyName] });
      }
    }
  }
  return sortOwners(owners);
};

const normalizedLockClassification = (entry) =>
  Object.fromEntries(
    lockClassificationFields.map((field) => [
      field,
      entry?.[field] === undefined ? false : entry[field],
    ]),
  );

const expectedLockClassification = (packageName) => ({
  dev: false,
  optional: false,
  peer: peerClassifiedPackages.has(packageName),
  link: false,
});

const assertPinnedPackage = (packages, reviewed) => {
  const path = "node_modules/" + reviewed.name;
  const matchingPaths = Object.keys(packages).filter(
    (candidate) =>
      candidate === path ||
      candidate.endsWith("/node_modules/" + reviewed.name),
  );
  assert.deepEqual(
    matchingPaths,
    [path],
    "the temporary exception requires exactly one installed " + reviewed.name,
  );
  const entry = packages[path];
  assert.equal(entry.version, reviewed.version, path + " version drifted");
  assert.equal(
    entry.resolved,
    reviewed.resolved,
    path + " tarball URL drifted",
  );
  assert.equal(
    entry.integrity,
    reviewed.integrity,
    path + " integrity drifted",
  );
  assert.deepEqual(
    normalizedLockClassification(entry),
    expectedLockClassification(reviewed.name),
    path + " lockfile classification drifted",
  );
  for (const edge of reviewed.edges) {
    assert.equal(
      entry[edge.field]?.[edge.dependency],
      edge.range,
      path +
        " must retain its exact " +
        edge.field +
        " edge to " +
        edge.dependency,
    );
  }
};

export const validateImageSizePackageLock = (
  packageLock,
  { now = new Date() } = {},
) => {
  assertExceptionActive(now);
  assert.equal(
    packageLock?.lockfileVersion,
    3,
    "the temporary exception only accepts npm package-lock v3",
  );
  assert(
    packageLock.packages && typeof packageLock.packages === "object",
    "npm package-lock v3 must contain a packages object",
  );
  const packages = packageLock.packages;
  const rootPackage = packages[""] ?? {};
  const matchingRootVariants = approvedRootVariants.filter(
    ({ rootServerRange }) =>
      rootPackage.dependencies?.["@peerbit/server"] === rootServerRange,
  );
  assert.equal(
    matchingRootVariants.length,
    1,
    "the bootstrap root must select exactly one reviewed @peerbit/server variant",
  );
  const rootVariant = matchingRootVariants[0];
  assertPinnedPackage(packages, rootVariant.server);
  assertPinnedPackage(packages, rootVariant.peerbit);
  for (const packageName of [
    "peerbit",
    ...reviewedPackages.map(({ name }) => name),
  ]) {
    for (const field of dependencyFields) {
      assert.equal(
        rootPackage[field]?.[packageName],
        undefined,
        "the bootstrap root must not depend directly on " + packageName,
      );
    }
  }

  for (const reviewed of reviewedPackages) {
    assertPinnedPackage(packages, reviewed);
  }

  for (const [dependencyName, owners] of Object.entries(expectedOwners)) {
    assert.deepEqual(
      packageOwners(packages, dependencyName),
      sortOwners(owners),
      "the temporary exception rejects alternate " +
        dependencyName +
        " dependency owners",
    );
  }

  assert.deepEqual(
    packageOwners(packages, "@peerbit/server"),
    [
      {
        path: "",
        field: "dependencies",
        range: rootVariant.rootServerRange,
      },
    ],
    "only the bootstrap root may introduce the pinned @peerbit/server package",
  );
  assert.deepEqual(
    packageOwners(packages, "peerbit"),
    [
      {
        path: "node_modules/@peerbit/server",
        field: "dependencies",
        range: rootVariant.peerbit.version,
      },
    ],
    "only the pinned @peerbit/server package may introduce peerbit",
  );

  return {
    status: "validated-exception-graph",
    cves: [...IMAGE_SIZE_EXCEPTION_CVES],
    expiresAt: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
  };
};

export const validateImageSizeAuditException = ({
  auditReport,
  packageLock,
  now = new Date(),
}) => {
  validateImageSizePackageLock(packageLock, { now });
  assert.equal(
    auditReport?.auditReportVersion,
    2,
    "the image-size validator only understands npm audit report v2",
  );
  assert(
    auditReport.vulnerabilities &&
      typeof auditReport.vulnerabilities === "object" &&
      !Array.isArray(auditReport.vulnerabilities),
    "npm audit v2 must contain a vulnerabilities object",
  );

  const counts = auditCounts(auditReport.metadata?.vulnerabilities);
  if (counts.total === 0) {
    assert.deepEqual(
      counts,
      zeroAuditCounts,
      "a zero-finding npm audit must have zero counts at every severity",
    );
    assert.deepEqual(
      Object.keys(auditReport.vulnerabilities),
      [],
      "a zero-finding npm audit must not contain vulnerability nodes",
    );
    return { status: "clean" };
  }

  assert.deepEqual(
    counts,
    expectedAuditCounts,
    "the temporary exception accepts exactly seven high-severity nodes",
  );
  const actualNames = sortStrings(Object.keys(auditReport.vulnerabilities));
  const expectedNames = sortStrings(Object.keys(expectedAuditVulnerabilities));
  assert.deepEqual(
    actualNames,
    expectedNames,
    "the npm audit vulnerability closure changed",
  );
  for (const name of expectedNames) {
    assert.deepEqual(
      normalizeVulnerability(auditReport.vulnerabilities[name]),
      normalizeExpectedVulnerability(name, expectedAuditVulnerabilities[name]),
      "unexpected npm audit v2 node for " + name,
    );
  }

  return {
    status: "temporary-exception",
    cves: [...IMAGE_SIZE_EXCEPTION_CVES],
    expiresAt: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
  };
};

export const validateImageSizeAuditCommandResult = ({
  status,
  stdout,
  stderr = "",
  packageLock,
  now = new Date(),
}) => {
  assert(
    status === 0 || status === 1,
    "npm audit failed operationally with status " +
      String(status) +
      ": " +
      stderr,
  );
  let auditReport;
  try {
    auditReport = JSON.parse(stdout);
  } catch (error) {
    assert.fail("npm audit did not return valid JSON: " + error.message);
  }
  assert.equal(
    auditReport?.error,
    undefined,
    "npm audit returned an operational error: " +
      JSON.stringify(auditReport?.error),
  );
  const result = validateImageSizeAuditException({
    auditReport,
    packageLock,
    now,
  });
  assert.equal(
    status,
    result.status === "clean" ? 0 : 1,
    "npm audit exit status did not match its validated report",
  );
  return result;
};
