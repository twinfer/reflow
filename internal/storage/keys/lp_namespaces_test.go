package keys

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestAllLPNamespaces_NamesUnique(t *testing.T) {
	seen := make(map[string]bool, len(AllLPNamespaces))
	for _, ns := range AllLPNamespaces {
		if seen[ns.Name] {
			t.Errorf("duplicate LPNamespace.Name %q — names are wire-visible SST filenames; must be unique", ns.Name)
		}
		seen[ns.Name] = true
	}
}

// TestAllLPNamespacesCoversEveryPrefixBuilder is the structural defense
// against silent LP-transfer drops. It walks keys/*.go and finds every
// top-level function with signature `func F(lp uint32) []byte` whose
// name ends in LPPrefix or LPPrefixForLP (the established naming
// convention for LP-prefix range builders), then asserts each one is
// referenced by AllLPNamespaces.
//
// Failure mode this catches: someone adds a new
//
//	func XxxLPPrefix(lp uint32) []byte { ... }
//
// to the keys package but doesn't append the entry to AllLPNamespaces.
// Under steady state, callers that know about XxxLPPrefix read and
// write rows normally. On LP transfer, buildLPSSTs iterates
// AllLPNamespaces and skips the new namespace; onFinishLPTransfer's
// range-delete loop also skips it. The result depends on the transfer
// phase: rows may end up on both source and dest, or vanish entirely
// from the dest after a flip+cleanup. Either way the row is silently
// wrong.
//
// This test fails loudly the moment the new builder lands without a
// registry entry, naming the offending function in the failure
// message.
func TestAllLPNamespacesCoversEveryPrefixBuilder(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse keys dir: %v", err)
	}
	pkg, ok := pkgs["keys"]
	if !ok {
		t.Fatalf("expected to find the keys package in cwd; got %v", pkgKeys(pkgs))
	}

	var declaredBuilders []string
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fn.Recv != nil { // skip methods
				continue
			}
			if !isLPPrefixBuilderName(fn.Name.Name) {
				continue
			}
			if !isLPPrefixBuilderSig(fn.Type) {
				continue
			}
			declaredBuilders = append(declaredBuilders, fn.Name.Name)
		}
	}
	if len(declaredBuilders) == 0 {
		t.Fatalf("AST scan found zero LP-prefix builders in keys/*.go — the test setup is broken")
	}

	registered := make(map[string]bool, len(AllLPNamespaces))
	for _, ns := range AllLPNamespaces {
		registered[funcSimpleName(ns.Prefix)] = true
	}

	for _, name := range declaredBuilders {
		if !registered[name] {
			t.Errorf("keys.%s has signature func(lp uint32) []byte and matches the LP-prefix builder naming convention, but is not registered in keys.AllLPNamespaces — append it or LP-transfer will silently drop its rows", name)
		}
	}
}

func isLPPrefixBuilderName(name string) bool {
	return strings.HasSuffix(name, "LPPrefix") || strings.HasSuffix(name, "LPPrefixForLP")
}

// isLPPrefixBuilderSig matches `func F(lp uint32) []byte`. One ident
// param of type uint32; one result of type []byte. Anything else
// (extra params, non-uint32 param, non-[]byte return) is some other
// API in the keys package and is correctly excluded.
func isLPPrefixBuilderSig(ft *ast.FuncType) bool {
	if ft.Params == nil || len(ft.Params.List) != 1 {
		return false
	}
	if ft.Results == nil || len(ft.Results.List) != 1 {
		return false
	}
	p := ft.Params.List[0]
	if len(p.Names) != 1 { // exactly one named param
		return false
	}
	pid, ok := p.Type.(*ast.Ident)
	if !ok || pid.Name != "uint32" {
		return false
	}
	r := ft.Results.List[0]
	arr, ok := r.Type.(*ast.ArrayType)
	if !ok || arr.Len != nil {
		return false
	}
	rid, ok := arr.Elt.(*ast.Ident)
	if !ok || rid.Name != "byte" {
		return false
	}
	return true
}

func funcSimpleName(fn func(uint32) []byte) string {
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}

func pkgKeys(m map[string]*ast.Package) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
