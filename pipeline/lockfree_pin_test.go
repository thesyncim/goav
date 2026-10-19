package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// messagePathRoots are where a message enters the data plane: every emitter
// a source or stage is handed, and the buffered worker loop.
var messagePathRoots = []string{
	"directEmitter.Emit",
	"directEmitter.EmitDelivery",
	"timeOrderedEmitter.Emit",
	"timeOrderedEmitter.EmitDelivery",
	"bufferedEmitter.Emit",
	"bufferedEmitter.EmitDelivery",
	"bufferedRunner.runNode",
}

// coldCallees are the functions the message path can reach that take a
// lock, each for a reason that is not "a packet or frame went by". The walk
// stops at them; every other function it reaches must take no lock.
var coldCallees = map[string]string{
	"runFailure.report":          "records the run's first error, once; every later delivery sheds",
	"runFailure.firstErr":        "returned only after the run failed",
	"runFailure.reset":           "name shared with the slot resets on the path; called by Run only",
	"coldStats.recordOutEvent":   "event messages only: last-event and per-type counts",
	"coldStats.recordNodeEvent":  "event messages only: a node's last event",
	"coldStats.recordCustomDrop": "only for a custom BufferPolicy.Drop string; known reasons count in atomics",
}

// TestDataPlaneTakesNoMutex is the static half of the no-lock pin. It parses
// this package and walks every call reachable from the message path's
// roots. A method call resolves through the receiver's and parameters'
// declared types and struct field types where they are known, and to every
// method of that name where they are not, so the walk over-approximates and
// can only err toward failing. It fails on any Lock, RLock, TryLock, or
// TryRLock call outside coldCallees. With TestDataPlaneNoContendedMutex (the
// runtime half, under contention) it pins that delivering a packet or frame,
// and shedding one, takes no lock.
func TestDataPlaneTakesNoMutex(t *testing.T) {
	pkg := parsePackage(t)
	for _, root := range messagePathRoots {
		if pkg.funcs[root] == nil {
			t.Fatalf("message path root %s not found; update the pin", root)
		}
	}
	for callee := range coldCallees {
		if pkg.funcs[callee] == nil {
			t.Fatalf("cold callee %s not found; update the pin", callee)
		}
	}
	from := make(map[string]string)
	seen := make(map[string]bool)
	queue := slices.Clone(messagePathRoots)
	var violations []string
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		if seen[key] {
			continue
		}
		seen[key] = true
		if _, cold := coldCallees[key]; cold {
			continue
		}
		fn := pkg.funcs[key]
		scope := pkg.scope(fn)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			for _, callee := range pkg.resolve(scope, call.Fun) {
				if !seen[callee] {
					if _, ok := from[callee]; !ok {
						from[callee] = key
					}
					queue = append(queue, callee)
				}
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "Lock", "RLock", "TryLock", "TryRLock":
					chain := key
					for at := from[key]; at != ""; at = from[at] {
						chain += " < " + at
					}
					violations = append(violations, fset(pkg).Position(call.Pos()).String()+": "+chain)
				}
			}
			return true
		})
	}
	if len(violations) > 0 {
		t.Fatalf("the message path takes a lock:\n\t%s", strings.Join(violations, "\n\t"))
	}
	for _, want := range []string{"directRunner.deliver", "directRunner.release", "bufferedRunner.enqueue", "bufferedNode.await", "ring.push", "timeOrder.yield"} {
		if !seen[want] {
			t.Fatalf("walk never reached %s; the call resolution is broken", want)
		}
	}
}

// parsedPackage is this package's declarations, enough to resolve calls.
type parsedPackage struct {
	fset    *token.FileSet
	funcs   map[string]*ast.FuncDecl
	plain   map[string]bool              // package-level functions
	methods map[string][]string          // method name -> Type.method keys
	fields  map[string]map[string]string // type -> field -> field type
	embeds  map[string][]string          // type -> embedded types
}

func fset(p *parsedPackage) *token.FileSet { return p.fset }

func parsePackage(t *testing.T) *parsedPackage {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	p := &parsedPackage{
		fset:    token.NewFileSet(),
		funcs:   make(map[string]*ast.FuncDecl),
		plain:   make(map[string]bool),
		methods: make(map[string][]string),
		fields:  make(map[string]map[string]string),
		embeds:  make(map[string][]string),
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(p.fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				key := funcKey(d)
				p.funcs[key] = d
				if d.Recv == nil {
					p.plain[key] = true
				} else {
					p.methods[d.Name.Name] = append(p.methods[d.Name.Name], key)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					fields := make(map[string]string)
					for _, f := range st.Fields.List {
						typ := baseType(f.Type)
						if len(f.Names) == 0 {
							p.embeds[ts.Name.Name] = append(p.embeds[ts.Name.Name], typ)
							fields[typ] = typ
							continue
						}
						for _, name := range f.Names {
							fields[name.Name] = typ
						}
					}
					p.fields[ts.Name.Name] = fields
				}
			}
		}
	}
	return p
}

// scope maps a function's receiver and parameter names to their types.
func (p *parsedPackage) scope(fn *ast.FuncDecl) map[string]string {
	scope := make(map[string]string)
	for _, list := range []*ast.FieldList{fn.Recv, fn.Type.Params} {
		if list == nil {
			continue
		}
		for _, f := range list.List {
			for _, name := range f.Names {
				scope[name.Name] = baseType(f.Type)
			}
		}
	}
	return scope
}

// typeOf is the declared type of x when the scope and field types tell it,
// else "".
func (p *parsedPackage) typeOf(scope map[string]string, x ast.Expr) string {
	switch e := x.(type) {
	case *ast.Ident:
		return scope[e.Name]
	case *ast.ParenExpr:
		return p.typeOf(scope, e.X)
	case *ast.UnaryExpr:
		return p.typeOf(scope, e.X)
	case *ast.StarExpr:
		return p.typeOf(scope, e.X)
	case *ast.SelectorExpr:
		if owner := p.typeOf(scope, e.X); owner != "" {
			return p.field(owner, e.Sel.Name)
		}
	}
	return ""
}

// field finds a field of typ, through embedded types.
func (p *parsedPackage) field(typ, name string) string {
	if f, ok := p.fields[typ][name]; ok {
		return f
	}
	for _, embedded := range p.embeds[typ] {
		if f := p.field(embedded, name); f != "" {
			return f
		}
	}
	return ""
}

// method finds typ.name, through embedded types.
func (p *parsedPackage) method(typ, name string) string {
	if key := typ + "." + name; p.funcs[key] != nil {
		return key
	}
	for _, embedded := range p.embeds[typ] {
		if key := p.method(embedded, name); key != "" {
			return key
		}
	}
	return ""
}

// resolve returns the package functions a call expression may run.
func (p *parsedPackage) resolve(scope map[string]string, fun ast.Expr) []string {
	switch x := fun.(type) {
	case *ast.ParenExpr:
		return p.resolve(scope, x.X)
	case *ast.IndexExpr:
		return p.resolve(scope, x.X)
	case *ast.IndexListExpr:
		return p.resolve(scope, x.X)
	case *ast.Ident:
		if p.plain[x.Name] {
			return []string{x.Name}
		}
		return nil
	case *ast.SelectorExpr:
		if typ := p.typeOf(scope, x.X); typ != "" {
			if key := p.method(typ, x.Sel.Name); key != "" {
				return []string{key}
			}
			if _, known := p.fields[typ]; known {
				return nil // a package type without that method: an interface field or a func value
			}
		}
		return p.methods[x.Sel.Name]
	}
	return nil
}

func baseType(x ast.Expr) string {
	for {
		switch e := x.(type) {
		case *ast.StarExpr:
			x = e.X
		case *ast.IndexExpr:
			x = e.X
		case *ast.IndexListExpr:
			x = e.X
		case *ast.Ident:
			return e.Name
		default:
			return ""
		}
	}
}

// TestNodesHoldNoMutex pins the other side: no type the message path works
// on holds a sync.Mutex or RWMutex at all.
func TestNodesHoldNoMutex(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	lockFree := map[string]bool{
		"nodeBase": true, "directNode": true, "bufferedNode": true, "directEmitter": true,
		"bufferedEmitter": true, "ring": true, "ringCell": true, "waiterQueue": true,
		"directWaiter": true, "parkQueue": true, "mpscQueue": true, "parkedProducer": true, "dropController": true,
		"nodeCounters": true, "dropCounts": true, "timeOrder": true, "timeTurn": true,
		"bufferedMessage": true, "inflight": true,
	}
	fset := token.NewFileSet()
	found := make(map[string]bool)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			spec, ok := n.(*ast.TypeSpec)
			if !ok || !lockFree[spec.Name.Name] {
				return true
			}
			found[spec.Name.Name] = true
			st, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				if sel, ok := field.Type.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sync" && (sel.Sel.Name == "Mutex" || sel.Sel.Name == "RWMutex") {
						t.Errorf("%s holds a sync.%s at %s", spec.Name.Name, sel.Sel.Name, fset.Position(field.Pos()))
					}
				}
			}
			return true
		})
	}
	for name := range lockFree {
		if !found[name] {
			t.Errorf("type %s not found; update the pin", name)
		}
	}
}

func funcKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	typ := fn.Recv.List[0].Type
	for {
		switch x := typ.(type) {
		case *ast.StarExpr:
			typ = x.X
			continue
		case *ast.IndexExpr:
			typ = x.X
			continue
		case *ast.IndexListExpr:
			typ = x.X
			continue
		case *ast.Ident:
			return x.Name + "." + fn.Name.Name
		}
		return fn.Name.Name
	}
}
