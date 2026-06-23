# Security Policy

goav is pre-v1, but suspected security issues should still be handled
privately first. The goal is to give the maintainer enough detail to reproduce
and scope the issue before public disclosure.

## Supported Versions

No stable release line has been cut yet. Security fixes should target `main`
until a release branch exists.

## Reporting A Vulnerability

Please report suspected vulnerabilities privately through GitHub Security
Advisories for `github.com/thesyncim/goav` when available. Include:

- affected package or adapter
- reproducer or malformed input
- expected impact
- whether cgo or an optional adapter is involved

Please do not open a public issue for a suspected vulnerability before the
maintainer has had time to triage it.

## Scope

The root module is intended to stay pure Go and dependency-light. Optional
transport modules and external adapters may carry their own dependency and
cgo/security posture; reports should identify which module or adapter is
affected.
