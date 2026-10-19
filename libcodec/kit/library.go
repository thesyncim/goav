package kit

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Library is one shared library or OS framework a backend binds: the name
// errors give it, where it lives, the fix to show when it cannot load, and
// Symbols, a pointer to the struct its entry points bind into. Path pins the
// file (absolute: libcodec.Library sets it); otherwise the kit takes the
// first of System, the absolute paths a system install puts it at, that
// exists. Nothing searches the working directory. The struct's fields carry
// a sym tag naming the symbol:
//
//	type symbols struct {
//		create   func(**uintptr) int32 `sym:"WelsCreateSVCEncoder"`
//		version  func(*version)        `sym:"WelsGetCodecVersionEx,optional"`
//		keyCB    uintptr               `sym:"kCFTypeDictionaryKeyCallBacks"`
//		realtime uintptr               `sym:"kVTCompressionPropertyKey_RealTime,value"`
//	}
//
// A func field is bound to the function; a uintptr field receives the
// symbol's address, or with ",value" the pointer stored there (a CFStringRef
// constant). A missing ",optional" symbol leaves the field zero.
type Library struct {
	Name    string
	Path    string
	System  []string
	Fix     string
	Symbols any
}

// Bind loads the library from Path and resolves its symbols. A library that
// does not load, or a missing symbol, is an *Error carrying the fix.
func (l Library) Bind(backend string) error {
	return l.bind(backend, l.Path, l.Fix)
}

// bound is one library file's symbols, resolved once per process for each
// symbols type; every declaration that binds the file copies them.
type bound struct {
	once    sync.Once
	symbols reflect.Value
	err     error
}

type boundKey struct {
	path    string
	symbols reflect.Type
}

var boundLibraries sync.Map // boundKey -> *bound

func (l Library) bind(backend, path, fix string) error {
	v := reflect.ValueOf(l.Symbols)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("kit: %s symbols must be a pointer to a struct, got %T", l.Name, l.Symbols)
	}
	if path == "" {
		return unavailable(backend, "load", l.Name, "is not installed", fix, nil)
	}
	if !filepath.IsAbs(path) {
		return unavailable(backend, "load", l.Name, "path "+path+" is not absolute", fix, nil)
	}
	value, _ := boundLibraries.LoadOrStore(boundKey{path, v.Type()}, &bound{})
	b := value.(*bound)
	b.once.Do(func() {
		symbols := reflect.New(v.Type().Elem())
		b.err = resolveSymbols(backend, l.Name, path, fix, symbols.Elem())
		b.symbols = symbols.Elem()
	})
	if b.err != nil {
		return b.err
	}
	v.Elem().Set(b.symbols)
	return nil
}

func resolveSymbols(backend, name, path, fix string, s reflect.Value) error {
	handle, err := openOnce(path)
	if err != nil {
		return unavailable(backend, "load", name, "did not load from "+path, fix, err)
	}
	t := s.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		tag, ok := field.Tag.Lookup("sym")
		if !ok {
			continue
		}
		symbol, options, _ := strings.Cut(tag, ",")
		addr, err := lookup(handle, symbol)
		if err != nil || addr == 0 {
			if strings.Contains(options, "optional") {
				continue
			}
			return unavailable(backend, "symbol", name, "("+path+") has no symbol "+symbol, fix, err)
		}
		target := reflect.NewAt(field.Type, unsafe.Pointer(s.Field(i).UnsafeAddr()))
		switch {
		case field.Type.Kind() == reflect.Func:
			purego.RegisterFunc(target.Interface(), addr)
		case field.Type.Kind() == reflect.Uintptr && strings.Contains(options, "value"):
			target.Elem().SetUint(uint64(*(*uintptr)(pointer(addr))))
		case field.Type.Kind() == reflect.Uintptr:
			target.Elem().SetUint(uint64(addr))
		default:
			return fmt.Errorf("kit: symbol field %s is %s, want a func or uintptr", field.Name, field.Type)
		}
	}
	return nil
}

// BindVTable binds the func fields of table, in order, to the consecutive
// function pointers of a C++ or COM virtual table at vtable: field i calls
// slot i. Pass the receiver as the first argument.
func BindVTable(vtable uintptr, table any) error {
	v := reflect.ValueOf(table)
	if vtable == 0 || v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return errors.New("kit: BindVTable wants a vtable and a pointer to a struct of funcs")
	}
	s := v.Elem()
	for i := range s.NumField() {
		field := s.Type().Field(i)
		if field.Type.Kind() != reflect.Func {
			return fmt.Errorf("kit: vtable field %s is %s, want a func", field.Name, field.Type)
		}
		slot := *(*uintptr)(pointer(vtable + uintptr(i)*unsafe.Sizeof(uintptr(0))))
		if slot == 0 {
			return fmt.Errorf("kit: vtable slot %d (%s) is empty", i, field.Name)
		}
		purego.RegisterFunc(reflect.NewAt(field.Type, unsafe.Pointer(s.Field(i).UnsafeAddr())).Interface(), slot)
	}
	return nil
}

var opened sync.Map // path -> uintptr: a library opens once per process

func openOnce(path string) (uintptr, error) {
	if handle, ok := opened.Load(path); ok {
		return handle.(uintptr), nil
	}
	handle, err := open(path)
	if err != nil {
		return 0, err
	}
	actual, _ := opened.LoadOrStore(path, handle)
	return actual.(uintptr), nil
}

// pointer turns a C address into an unsafe.Pointer. The memory is the C
// library's, outside the Go heap, so the collector never moves or frees it.
func pointer(addr uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&addr))
}

// Symbol looks up one symbol by a name known only at run time (a constant
// a setting names), in the loaded file Path names. A ",value" symbol's
// pointer is the caller's to read.
func (l Library) Symbol(name string) (uintptr, error) {
	handle, ok := opened.Load(l.Path)
	if !ok {
		return 0, fmt.Errorf("kit: %s is not loaded", l.Name)
	}
	return lookup(handle.(uintptr), name)
}

// Value reads the pointer-sized value stored at a data symbol: the
// CFStringRef behind a kVT... constant, say. It reports false when the
// library has no such symbol.
func (l Library) Value(name string) (uintptr, bool) {
	addr, err := l.Symbol(name)
	if err != nil || addr == 0 {
		return 0, false
	}
	return *(*uintptr)(pointer(addr)), true
}
