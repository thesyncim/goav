# Support

goav is pre-v1. The best support path today is to open a GitHub issue with a
minimal recipe, the selected adapters, the Go version, and the full structured
error output.

For adoption questions, include:

- the input shape or source provider
- the intended operation chain
- the destination type
- whether the workflow must support runtime attach/rebranch
- whether `CGO_ENABLED=0` is required

For performance questions, include the benchmark command, hardware, operating
system, Go version, and whether the numbers come from `scripts/bench/run.sh`.
