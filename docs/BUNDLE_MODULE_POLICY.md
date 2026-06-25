# Bundle module policy

`github.com/thesyncim/goav/bundle` is a package in the root module, not a
nested module.

Current guarantee: importing `github.com/thesyncim/goav` does not import the
bundled adapter packages into the root package dependency graph. Use
`go list -deps github.com/thesyncim/goav` to verify that the root package stays
free of bundled codec, format, filter, RTP, and WebRTC implementations.

Not guaranteed: the root module's `go.mod` excludes bundled backend modules.
The root module still lists bundled backend requirements because `bundle` and
the bundled adapter packages live in the same module.

Nested-module trigger: split `github.com/thesyncim/goav/bundle` into its own
module only if v1 release review, SBOM requirements, vulnerability-scanner
noise, or backend version churn make module-level dependency isolation worth
the extra release and versioning cost.

Until then, keep the distinction precise:

- root package import graph: lean and pinned;
- root module requirements: intentionally include bundled backend modules;
- `goav/bundle`: the batteries-included adapter package.
