package secretstore_test

import (
	"strings"
	"testing"

	"github.com/twinfer/reflw/internal/secretstore"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func TestLookupForCASigning_HitAndMissCounters(t *testing.T) {
	r := secretstore.New(nil, nil)
	ctx := t.Context()

	// Stage a secret so the snapshot is populated.
	rec, _, _ := stageSecret(t, "ca/root/active", []byte("EC-PRIVATE-KEY-PEM-BYTES"))
	if err := r.Reconcile(ctx, []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatal(err)
	}

	// Hit.
	bytes, err := r.LookupForCASigning("ca/root/active")
	if err != nil {
		t.Fatalf("LookupForCASigning: %v", err)
	}
	if string(bytes) != "EC-PRIVATE-KEY-PEM-BYTES" {
		t.Errorf("unexpected bytes: %q", string(bytes))
	}

	// Miss — unknown name.
	if _, err := r.LookupForCASigning("ca/root/missing"); err == nil {
		t.Fatal("expected error for missing name")
	} else if !strings.Contains(err.Error(), "not resolved") {
		t.Errorf("error message = %q; want contains 'not resolved'", err.Error())
	}
}

func TestLookupForCASigning_NilResolverErrs(t *testing.T) {
	var r *secretstore.Resolver
	if _, err := r.LookupForCASigning("anything"); err == nil {
		t.Fatal("expected error from nil resolver")
	}
}
