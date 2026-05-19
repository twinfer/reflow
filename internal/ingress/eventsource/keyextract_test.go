package eventsource

import (
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestExtractor_Const(t *testing.T) {
	e, err := newExtractor("const", "abc")
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.Apply(message.NewMessage("", nil))
	if err != nil || got != "abc" {
		t.Fatalf("const: got %q, %v", got, err)
	}
}

func TestExtractor_UUID(t *testing.T) {
	e, _ := newExtractor("uuid", "")
	got, err := e.Apply(message.NewMessage("U-1", nil))
	if err != nil || got != "U-1" {
		t.Fatalf("uuid: got %q, %v", got, err)
	}
	if _, err := e.Apply(message.NewMessage("", nil)); !errors.Is(err, ErrExtractorMissing) {
		t.Fatalf("empty uuid should be missing, got %v", err)
	}
}

func TestExtractor_Header(t *testing.T) {
	e, err := newExtractor("header", "X-Key")
	if err != nil {
		t.Fatal(err)
	}
	msg := message.NewMessage("u", nil)
	msg.Metadata.Set("X-Key", "user-42")
	got, err := e.Apply(msg)
	if err != nil || got != "user-42" {
		t.Fatalf("header hit: got %q, %v", got, err)
	}
	if _, err := e.Apply(message.NewMessage("u", nil)); !errors.Is(err, ErrExtractorMissing) {
		t.Fatalf("header miss: want ErrExtractorMissing, got %v", err)
	}
}

func TestExtractor_Header_RequiresValue(t *testing.T) {
	if _, err := newExtractor("header", ""); err == nil {
		t.Fatal("header without value should error")
	}
}

func TestExtractor_JSON(t *testing.T) {
	e, err := newExtractor("json", "user.id")
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.Apply(message.NewMessage("u", []byte(`{"user":{"id":"u-9"}}`)))
	if err != nil || got != "u-9" {
		t.Fatalf("json hit: got %q, %v", got, err)
	}
	if _, err := e.Apply(message.NewMessage("u", []byte(`{}`))); !errors.Is(err, ErrExtractorMissing) {
		t.Fatalf("json miss: want ErrExtractorMissing, got %v", err)
	}
}

func TestExtractor_JSON_RequiresValue(t *testing.T) {
	if _, err := newExtractor("json", ""); err == nil {
		t.Fatal("json without path should error")
	}
}

func TestExtractor_Empty(t *testing.T) {
	e, err := newExtractor("", "")
	if err != nil || e != nil {
		t.Fatalf("empty from: got %v, %v", e, err)
	}
}

func TestExtractor_Unknown(t *testing.T) {
	if _, err := newExtractor("magic", "x"); err == nil {
		t.Fatal("unknown extractor type should error")
	}
}
