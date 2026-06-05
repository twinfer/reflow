package handler_test

import (
	"errors"
	"testing"

	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

type sample struct {
	Name string `json:"name"`
	N    int    `json:"n"`
}

func TestJSONCodec_RoundTrip(t *testing.T) {
	c := handler.JSONCodec{}
	if c.Name() != "json/default" {
		t.Errorf("Name = %q; want json/default", c.Name())
	}
	in := sample{Name: "abc", N: 7}
	data, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out sample
	if err := c.Decode(data, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out != in {
		t.Errorf("round-trip: got %+v want %+v", out, in)
	}
}

func TestJSONCodec_DecodeRejectsNil(t *testing.T) {
	if err := (handler.JSONCodec{}).Decode([]byte("{}"), nil); !errors.Is(err, handler.ErrNilCodecTarget) {
		t.Errorf("Decode(nil): err=%v; want ErrNilCodecTarget", err)
	}
}

func TestProtoCodec_RoundTripWithProtoMessage(t *testing.T) {
	c := handler.ProtoCodec{}
	if c.Name() != "proto/binary" {
		t.Errorf("Name = %q; want proto/binary", c.Name())
	}
	in := &enginev1.InvocationTarget{
		ServiceName: "S",
		HandlerName: "h",
		ObjectKey:   "k",
	}
	data, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out := &enginev1.InvocationTarget{}
	if err := c.Decode(data, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.GetServiceName() != "S" || out.GetHandlerName() != "h" || out.GetObjectKey() != "k" {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

func TestProtoCodec_RejectsNonProto(t *testing.T) {
	c := handler.ProtoCodec{}
	if _, err := c.Encode(struct{ A int }{1}); err == nil {
		t.Error("Encode non-proto: want error, got nil")
	}
	var notProto struct{}
	if err := c.Decode([]byte{}, &notProto); err == nil {
		t.Error("Decode non-proto: want error, got nil")
	}
}

func TestRawBytesCodec_RoundTrip(t *testing.T) {
	c := handler.RawBytesCodec{}
	if c.Name() != "bytes/raw" {
		t.Errorf("Name = %q; want bytes/raw", c.Name())
	}
	in := []byte("hello")
	data, err := c.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Defensive copy: encoder must not share the input slice.
	in[0] = 'X'
	if string(data) != "hello" {
		t.Errorf("Encode kept reference to caller's slice: %q", data)
	}
	var out []byte
	if err := c.Decode(data, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(out) != "hello" {
		t.Errorf("Decode = %q; want hello", out)
	}
}

func TestRawBytesCodec_RejectsWrongShapes(t *testing.T) {
	c := handler.RawBytesCodec{}
	if _, err := c.Encode("string"); err == nil {
		t.Error("Encode non-[]byte: want error, got nil")
	}
	var s string
	if err := c.Decode([]byte("x"), &s); err == nil {
		t.Error("Decode non-*[]byte: want error, got nil")
	}
}

func TestDefaultCodec_IsJSON(t *testing.T) {
	if handler.DefaultCodec().Name() != "json/default" {
		t.Errorf("DefaultCodec = %s; want JSONCodec", handler.DefaultCodec().Name())
	}
}
