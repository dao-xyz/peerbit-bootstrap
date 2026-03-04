# Peerbit Bootstrap  🚀

Relay server for bootstrapping Peerbit applications.

This environment file contains a list of addresses you can initially connect to.

Check the Peerbit client/network version you are using.

For v4:

```
https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/bootstrap-4.env
```

For v5:

```
https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/bootstrap-5.env
```

## Rolling self-update workflow

This repo contains a manual GitHub Actions workflow:

- `Rolling Bootstrap Self-Update`

It performs rolling updates of the bootstrap fleet using each node's remote
`self-update` API with preflight health checks and batch-by-batch rollout.

If a batch fails, the workflow can automatically roll back updated nodes to
their per-node previous `@peerbit/server` version.

### Required secret

Configure this repository secret in the `production` environment:

- `PEERBIT_ADMIN_KEY_B64`: base64-encoded serialized Peerbit admin keypair
  that is trusted by bootstrap nodes.

### Typical run settings

- `target_version`: release version to deploy (for example `5.10.14`)
- `bootstrap_file`: `bootstrap-4.env` or `bootstrap-5.env`
- `batch_size`: `1` for safest rolling update
- `rollback_on_failure`: `true`
