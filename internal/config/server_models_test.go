package config

import (
	"strings"
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

const dmnNoImport = `<?xml version="1.0"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="d" namespace="urn:d">
  <decision name="x" id="x"><literalExpression><text>1</text></literalExpression></decision>
</definitions>`

const dmnWithImport = `<?xml version="1.0"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="d" namespace="urn:d">
  <import namespace="urn:other" name="o"/>
  <decision name="x" id="x"><literalExpression><text>1</text></literalExpression></decision>
</definitions>`

// bpmnWithImport carries a BPMN <import>; the dmn-only guard must not reject it.
const bpmnWithImport = `<?xml version="1.0"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <import importType="x" location="y" namespace="z"/>
  <process id="p" isExecutable="true"><startEvent id="s"/></process>
</definitions>`

func ent(kind, name, xml string) *enginev1.ModelRecord {
	return &enginev1.ModelRecord{
		ModelRef: &enginev1.ModelRef{Kind: kind, Name: name, Version: "v1"},
		Xml:      []byte(xml),
	}
}

func TestShallowPlanModelSet_RejectsDMNImport(t *testing.T) {
	// A plain DMN is accepted (empty bundle).
	if _, err := shallowPlanModelSet([]*enginev1.ModelRecord{ent("dmn", "Plain", dmnNoImport)}, nil); err != nil {
		t.Fatalf("plain dmn rejected: %v", err)
	}

	// A DMN with <import> is rejected — the shallow path can't close the closure.
	_, err := shallowPlanModelSet([]*enginev1.ModelRecord{ent("dmn", "Importer", dmnWithImport)}, nil)
	if err == nil {
		t.Fatal("shallowPlanModelSet accepted a DMN with <import>; want rejection")
	}
	if !strings.Contains(err.Error(), "import") {
		t.Fatalf("error %q should mention import", err)
	}

	// The guard is DMN-only: a BPMN with an <import> element passes.
	if _, err := shallowPlanModelSet([]*enginev1.ModelRecord{ent("bpmn", "Proc", bpmnWithImport)}, nil); err != nil {
		t.Fatalf("bpmn with <import> rejected by dmn-only guard: %v", err)
	}
}
