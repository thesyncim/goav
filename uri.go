// URI inputs and destinations: one resolver over the runtime's schemes.

package goav

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/provider"
)

// URIInput declares an input read from a URL: goav.URIInput("https://cdn.example/a.mp4").
// The runtime resolves it while planning through the scheme registered for
// the URL's scheme (runconfig.Scheme; bundle registers file, http, and
// https), and a bare path is a file. The scheme decides what the input is —
// container bytes the runtime probes and demuxes, as for an http resource,
// or media a transport produces, as for whep — and whether it is live.
// Describe, Explain, errors, events, and snapshots show the URL redacted:
// no userinfo, and secret query values (token, key, password, signature)
// replaced; only the scheme sees it verbatim.
func URIInput(uri string, opts ...InputOption) InputSpec {
	spec := InputSpec{}
	if ref := parseURIRef(uri); ref != nil {
		spec.uri = ref
		spec.name = ref.name
		spec.input = format.Input{Name: ref.name, URI: ref.full, Protocol: av.ProtocolID(ref.scheme)}
	}
	return applyInputOptions(inputSpecHandle(spec), opts)
}

// URI declares a destination written to a URL — goav.URI("file:///srv/out.mp4") —
// resolved like URIInput through the runtime's schemes, which open the bytes
// goav muxes once the format and streams are known. The container comes
// from the URL's extension, Format, MIME, or the scheme's contract.
func URI(uri string, opts ...DestinationOption) Destination {
	return destinationHandle(applyDestinationOptions(uriDestination(uri), opts))
}

func uriDestination(uri string) destinationSpec {
	spec := destinationSpec{id: destinationSpecSeq.Add(1)}
	if ref := parseURIRef(uri); ref != nil {
		spec.uri = ref
		spec.name = ref.name
		spec.output = format.Output{Name: ref.name, URI: ref.full, Protocol: av.ProtocolID(ref.scheme)}
	}
	return spec
}

// uriRef is one parsed URI value. raw carries the caller's credentials and
// reaches only the scheme; everything goav renders reads name or full.
type uriRef struct {
	raw    *url.URL
	scheme string
	// name is scheme://host/path with no userinfo, query, or fragment: node
	// names, labels, and format probing read it.
	name string
	// full is the URL with userinfo and secret query values redacted.
	full string
	err  error
}

const redacted = "REDACTED"

// parseURIRef parses uri, or returns nil for an empty one (left to the
// empty-input and empty-destination refusals).
func parseURIRef(uri string) *uriRef {
	if uri == "" {
		return nil
	}
	u, err := url.Parse(uri)
	if err != nil {
		// A *url.Error quotes the input, credentials and all: keep its cause.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return &uriRef{name: "uri", full: "uri", err: fmt.Errorf("not a URL: %w", err)}
	}
	if len(u.Scheme) <= 1 {
		// A bare path, or a Windows drive letter, is a file.
		return &uriRef{raw: &url.URL{Scheme: "file", Path: uri}, scheme: "file", name: uri, full: uri}
	}
	ref := &uriRef{raw: u, scheme: strings.ToLower(u.Scheme)}
	ref.name, ref.full = redactURL(u)
	return ref
}

// redactURL renders u for reports: name drops userinfo, query, and
// fragment; full keeps the query with secret values replaced.
func redactURL(u *url.URL) (name, full string) {
	shown := *u
	shown.User = nil
	shown.RawQuery, shown.ForceQuery = "", false
	shown.Fragment, shown.RawFragment = "", ""
	name = shown.String()
	if u.User != nil {
		shown.User = url.User(redacted)
	}
	shown.RawQuery = redactQuery(u.RawQuery)
	return name, shown.String()
}

func redactQuery(query string) string {
	if query == "" {
		return ""
	}
	parts := strings.Split(query, "&")
	for i, part := range parts {
		key, _, hasValue := strings.Cut(part, "=")
		name, err := url.QueryUnescape(key)
		if err != nil {
			name = key
		}
		if hasValue && secretQueryParam(name) {
			parts[i] = key + "=" + redacted
		}
	}
	return strings.Join(parts, "&")
}

// secretQueryParam reports a query parameter that may carry a credential.
// Over-matching is safe: a redacted harmless value costs nothing.
func secretQueryParam(name string) bool {
	name = strings.ToLower(name)
	for _, marker := range []string{"token", "key", "secret", "pass", "pwd", "sig", "credential", "auth", "jwt"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

// bindSchemes resolves the graph's URI inputs and destinations through rt's
// schemes, once: the handles the compile and the build read. Binding is pure
// (schemes do no I/O until the run opens them), so Describe and Explain bind
// too.
func (g *recipeGraph) bindSchemes(rt *Runtime) {
	for i := range g.inputs {
		if g.inputs[i].uri != nil && !g.inputs[i].uriBound() {
			g.inputs[i] = rt.bindURIInput(g.inputs[i])
		}
	}
	for i := range g.dests {
		if spec := g.dests[i].spec; spec.uri != nil && spec.custom == nil && spec.err == nil {
			g.dests[i].spec = rt.bindURIDestination(spec)
		}
	}
}

func (s InputSpec) uriBound() bool {
	return s.provider != nil || s.bytes != nil || s.err != nil
}

func (r *Runtime) bindURIInput(spec InputSpec) InputSpec {
	ref := spec.uri
	node := spec.inputName(ref.name)
	if ref.err != nil {
		spec.err = uriInvalidError(inputInvalidCode, "resolve input", node, ref.err)
		return spec
	}
	scheme, err := r.scheme(ref.scheme)
	if err != nil {
		spec.err = schemeUnsupportedError(inputSchemeUnsupportedCode, "resolve input", node, ref.scheme, err, r.schemeNames())
		return spec
	}
	in, err := scheme.Input(cloneURL(ref.raw))
	if err == nil && in == nil {
		err = errNilSource
	}
	if err != nil {
		spec.err = schemeUnsupportedError(inputSchemeUnsupportedCode, "resolve input", node, ref.scheme, err, r.schemeNames())
		return spec
	}
	bound := spec.withProvider(in)
	if bound.err != nil {
		spec.err = schemeUnsupportedError(inputSchemeUnsupportedCode, "resolve input", node, ref.scheme, bound.err, r.schemeNames())
		return spec
	}
	return bound
}

func (r *Runtime) bindURIDestination(spec destinationSpec) destinationSpec {
	ref := spec.uri
	node := spec.label(ref.name)
	if ref.err != nil {
		spec.err = uriInvalidError(outputInvalidCode, "resolve destination", node, ref.err)
		return spec
	}
	scheme, err := r.scheme(ref.scheme)
	if err != nil {
		spec.err = schemeUnsupportedError(outputSchemeUnsupportedCode, "resolve destination", node, ref.scheme, err, r.schemeNames())
		return spec
	}
	dest, err := scheme.Destination(cloneURL(ref.raw))
	if err == nil && dest == nil {
		err = errNilDestinationProvider
	}
	if err != nil {
		spec.err = schemeUnsupportedError(outputSchemeUnsupportedCode, "resolve destination", node, ref.scheme, err, r.schemeNames())
		return spec
	}
	contract := dest.Contract()
	spec.custom = dest
	spec.output.Realtime = spec.output.Realtime || contract.Realtime
	if spec.output.Fragment == 0 {
		spec.output.Fragment = contract.Fragment
	}
	if spec.format == "" && len(contract.Formats) != 0 {
		spec.format = contract.Formats[0]
	}
	if spec.output.MIMEType == "" && len(contract.MIMETypes) != 0 {
		spec.output.MIMEType = contract.MIMETypes[0]
	}
	return spec
}

var errNoRuntimeSchemes = errors.New("no runtime resolves it")

func (r *Runtime) scheme(name string) (provider.Scheme, error) {
	if r == nil {
		return nil, errNoRuntimeSchemes
	}
	scheme, ok := r.schemes[name]
	if !ok {
		return nil, fmt.Errorf("no %q scheme is registered", name)
	}
	return scheme, nil
}

func (r *Runtime) schemeNames() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.schemes))
	for name := range r.schemes {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func cloneURL(u *url.URL) *url.URL {
	clone := *u
	if u.User != nil {
		user := *u.User
		clone.User = &user
	}
	return &clone
}

// schemeProviders names where each well-known scheme comes from, so an
// unknown-scheme refusal can name the exact fix.
var schemeProviders = map[string]string{
	"file": `register runconfig.Scheme("file", scheme.File()), or build the runtime with bundle.New`,
}

func uriInvalidError(code errcode.Code, operation, node string, cause error) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    cause.Error(),
		fixes:     buildErrorFixes([]string{"pass a URL such as https://host/path or file:///path, or a bare file path"}),
		cause:     cause,
	}
}

func schemeUnsupportedError(code errcode.Code, operation, node, scheme string, cause error, registered []string) error {
	fixes := []string{}
	if hint, ok := schemeProviders[scheme]; ok && !slices.Contains(registered, scheme) {
		fixes = append(fixes, scheme+": "+hint)
	}
	if !slices.Contains(registered, scheme) {
		fixes = append(fixes, fmt.Sprintf("implement provider.Scheme and register it: runconfig.Scheme(%q, yourScheme)", scheme))
	}
	if errors.Is(cause, errors.ErrUnsupported) {
		fixes = append(fixes, "use the "+scheme+" URL on the end its scheme serves")
	}
	if len(fixes) == 0 {
		fixes = append(fixes, "fix the URL the "+scheme+" scheme refuses")
	}
	shown := "none"
	if len(registered) != 0 {
		shown = strings.Join(registered, ", ")
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    scheme + ": " + cause.Error(),
		fields:    errDetails(errDetail("scheme", scheme), errDetail("registered_schemes", shown)),
		fixes:     buildErrorFixes(fixes),
		cause:     cause,
	}
}
