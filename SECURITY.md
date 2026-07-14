# Security Policy

## Supported versions

Only the latest release on the `master` branch receives security fixes.

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please report vulnerabilities privately:

1. Go to [github.com/treeol/kvrust/security/advisories/new](https://github.com/treeol/kvrust/security/advisories/new)
2. Create a private security advisory with a description of the issue,
   steps to reproduce, and any known impact.

You should receive a response within 72 hours. If you do not, please follow
up via the same channel.

Please **do not** publish or disclose the vulnerability publicly until a fix
has been released.

## Security model

kvr is designed for **trusted co-container processes only**. Any process that
can reach the Unix Domain Socket path is trusted to read and write all keys.
There is no authentication or authorization beyond filesystem permissions.

The optional TCP listener is **unauthenticated** and intended for local
debugging on loopback only. Do not expose it to untrusted networks.

## Scope

The following are **in scope** for security fixes:

- Memory safety issues (buffer overflows, use-after-free, etc.) — though
  Rust's type system mitigates most of these.
- Crash vectors via malformed protocol frames.
- Snapshot file corruption that could lead to data loss or unexpected
  behavior on load.

The following are **out of scope**:

- Unauthorized access by processes that can already reach the UDS socket
  (by design, the socket is trusted).
- Attacks requiring physical access to the host.
- DoS via resource exhaustion from legitimate protocol usage (enforced by
  configurable limits, not authentication).
