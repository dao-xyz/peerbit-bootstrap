# Peerbit Bootstrap 🚀

Bootstrap relay lists for Peerbit applications.

Use the list matching the Peerbit client/network version:

- v4: <https://bootstrap.peerbit.org/bootstrap-4.env>
- v5: <https://bootstrap.peerbit.org/bootstrap-5.env>

The source files on GitHub are an independent emergency fallback:

- v4: <https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/bootstrap-4.env>
- v5: <https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/bootstrap-5.env>

## Cloudflare hosting

The two files are deployed atomically as static assets on an asset-only Cloudflare
Worker. `wrangler.jsonc` owns the `bootstrap.peerbit.org` custom domain, including
DNS and TLS. There is no Worker application code and no AWS dependency.

Every deployment validates the source format, builds `dist/` from scratch, runs a
Wrangler dry run, and verifies production after deployment. Production verification
requires exact bytes and SHA-256, strict HTTPS, CORS, cache and security headers,
ETag revalidation, and a 404 for unknown paths.

Wrangler and all of its transitive dependencies are installed from the committed
lockfile without lifecycle scripts before deployment credentials enter the process.

The stable filenames intentionally use:

```text
Cache-Control: public, max-age=0, must-revalidate
```

This allows ETag revalidation without leaving clients on a stale relay list.

### GitHub production environment

The deploy job requires these environment-scoped values:

- Secret `CLOUDFLARE_API_TOKEN`
- Variable `CLOUDFLARE_ACCOUNT_ID`

The token is scoped to this Cloudflare account and only:

- Account → Workers Scripts → Write
- `peerbit.org` zone → Workers Routes → Write

It does not need DNS, Pages, R2, KV, billing, or token-management access.

Rollback is a revert on `master`, which redeploys the prior atomic asset version.
Cloudflare deployment rollback remains available for an emergency platform-side
rollback.

## Production monitoring

`Monitor bootstrap production` runs every 15 minutes and can also be dispatched manually. It
does not receive deployment credentials. It compares the public files with the
repository and probes every advertised relay for:

- matching public IPv4 answers from Google and Cloudflare DNS;
- strict TLS on the peer API and at least 14 days of certificate validity;
- the exact advertised peer ID; and
- a valid WebSocket upgrade and accept hash on port 4003.

## Relay provisioning

Automated relay provisioning is temporarily disabled. The previous workflow
depended on the retired AWS-backed test-domain service, exported the administration
API port instead of the relay multiaddr, and launched Peerbit outside a service
manager.

Existing nodes must be recovered in place without deleting or resetting
`/root/.peerbit`. Re-enable provisioning only after the Cloudflare DNS and
systemd-supervised implementation has been released and validated. A node must not
be added to a bootstrap file until DNS, strict TLS, exact peer identity, public WSS,
a real libp2p dial/reservation, service enablement, and restart recovery all pass.

## Rolling self-update workflow

`Rolling Bootstrap Self-Update` performs rolling updates of the advertised fleet
through each node's remote `self-update` API, with preflight health checks and
batch-by-batch rollout. If a batch fails, it can roll updated nodes back to their
per-node previous `@peerbit/server` version.

## PR-driven bootstrap rollouts

Changing `rollouts/bootstrap-5.json` on `master` triggers `Deploy Bootstrap Rollout`.
Its secret-free validation job must pass before the direct production-environment
job can access the administration key. Nodes already on the requested version are
skipped.

The signed-request v1 to v2 migration uses exact, lockfile-pinned v6 and v8 admin
clients. The legacy client verifies the reviewed source fingerprint and only
initiates the update. A fresh v8 client then pins the peer ID from the bootstrap
multiaddr, verifies the signed authentication descriptor, and checks the complete
target dependency fingerprint. A failed transition performs a protocol-aware
rollback to the explicit reviewed version and treats an unverifiable rollback as
fatal.

The rollout configuration also pins the npm SHA-512 integrity for both server
versions. CI validates the config, lockfile, package metadata, state-machine tests,
and high/critical dependency audit before the production job can access its secret.
The temporary v6 client alias should be removed after the v8 rollout has completed
and been independently verified.

Rollouts run only from a reviewed `master` push; direct arbitrary-version dispatch
and broad secret inheritance are disabled. The production environment provides
`PEERBIT_ADMIN_KEY_B64`, containing the base64-encoded serialized Peerbit admin
keypair trusted by the bootstrap nodes.
