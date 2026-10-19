package kit

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Library is a shared library or OS framework a backend binds at run time:
// the name errors give it, the paths to try in order, and the fix to show
// when none opens. Nothing loads until Bind runs.
type Library struct {
	Name  string
	Paths []string
	Fix   string
}

// Bind opens the library and resolves the symbols syms declares. syms points
// to a struct whose fields carry a sym tag naming the symbol:
//
//	type symbols struct {
//		create  func(**uintptr) int32 `sym:"WelsCreateSVCEncoder"`
//		version func(*version)        `sym:"WelsGetCodecVersionEx,optional"`
//		keyCB   uintptr               `sym:"kCFTypeDictionaryKeyCallBacks"`
//		realtime uintptr              `sym:"kVTCompressionPropertyKey_RealTime,value"`
//	}
//
// A func field is bound to the function; a uintptr field receives the
// symbol's address, or with ",value" the pointer stored there (a CFStringRef
// constant). A missing ",optional" symbol leaves the field zero; any other
// missing symbol, or a library that does not open, is an *Error carrying the
// library's fix.
func (l Library) Bind(backend string, syms any) error {
	v := reflect.ValueOf(syms)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("kit: Bind wants a pointer to a struct, got %T", syms)
	}
	if len(l.Paths) == 0 {
		return unavailable(backend, "platform", l.Name, "is not available on "+runtime.GOOS+"/"+runtime.GOARCH, l.Fix, nil)
	}
	handle, path, err := openFirst(l.Paths)
	if err != nil {
		return unavailable(backend, "load", l.Name, "not found (tried "+strings.Join(l.Paths, ", ")+")", l.Fix, err)
	}
	s := v.Elem()
	t := s.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		tag, ok := field.Tag.Lookup("sym")
		if !ok {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		addr, err := lookup(handle, name)
		if err != nil || addr == 0 {
			if strings.Contains(options, "optional") {
				continue
			}
			return unavailable(backend, "symbol", l.Name, "("+path+") has no symbol "+name, l.Fix, err)
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

func openFirst(paths []string) (uintptr, string, error) {
	var errs []error
	for _, path := range paths {
		if handle, ok := opened.Load(path); ok {
			return handle.(uintptr), path, nil
		}
		handle, err := open(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		actual, _ := opened.LoadOrStore(path, handle)
		return actual.(uintptr), path, nil
	}
	return 0, "", errors.Join(errs...)
}

// pointer turns a C address into an unsafe.Pointer. The memory is the C
// library's, outside the Go heap, so the collector never moves or frees it.
func pointer(addr uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&addr))
}
