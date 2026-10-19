// Package compile is goav's recipe compiler. It reads a Request (a recipe
// DAG, the facts of the input and destination handles the DAG indexes, the
// stream matches, an Env of runtime settings, and a Host for the checks only
// the handles can make), validates it, solves its shapes, plans its joins,
// lowers it to one compose graph, and returns the spec and work plan the root
// package describes, explains, and builds. It never names a root type and
// raises only builderr.Refusal values, which the root converts to
// goav.BuildError.
package compile
