package processengine

import (
	"strings"
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// badRefDMN compiles (a requiredInput is not wired into the decision graph, and
// the literal references no input) but fails Mangle validation: the requirement
// points at an input datum that does not exist → REF002.
const badRefDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="bad" namespace="urn:bad" id="bad">
  <decision name="Bad" id="bad1">
    <variable name="Bad" typeRef="number"/>
    <informationRequirement><requiredInput href="#nonexistentInput"/></informationRequirement>
    <literalExpression><text>1</text></literalExpression>
  </decision>
</definitions>`

// mainCrossRefLibDMN imports urn:lib AND its decision actually requires the
// imported decision (cross-model informationRequirement) — the case that would
// false-positive REF001 before validation became closure-aware.
const mainCrossRefLibDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="mainx" namespace="urn:mainx" id="mainx">
  <import namespace="urn:lib" name="lib" importType="https://www.omg.org/spec/DMN/20191111/MODEL/"/>
  <decision name="mainXDecision" id="mainXDecision">
    <variable name="mainXDecision" typeRef="number"/>
    <informationRequirement><requiredDecision href="urn:lib#libDecision"/></informationRequirement>
    <literalExpression><text>lib.libDecision + 1</text></literalExpression>
  </decision>
</definitions>`

// TestPlanModelSet_DMNValidationRejectsBadModel: a DMN that compiles but has an
// error-severity validation finding is rejected at registration — DMN now gates
// on dmn.ValidateWithResolver, parity with the bpmn/cmmn cases.
func TestPlanModelSet_DMNValidationRejectsBadModel(t *testing.T) {
	_, err := PlanModelSet([]*enginev1.ModelRecord{
		rec("dmn", "Bad", "v1", badRefDMN),
	}, nil)
	if err == nil {
		t.Fatal("PlanModelSet accepted a DMN with a validation error; want rejection")
	}
	if !strings.Contains(err.Error(), "validation") || !strings.Contains(err.Error(), "REF002") {
		t.Fatalf("error %q does not report a REF002 validation failure", err)
	}
}

// TestPlanModelSet_DMNCrossModelRequirementValidates: a decision whose
// requirement crosses into a pinned import registers cleanly — the imported
// decision resolves as an external element, so no false-positive REF001.
func TestPlanModelSet_DMNCrossModelRequirementValidates(t *testing.T) {
	out, err := PlanModelSet([]*enginev1.ModelRecord{
		rec("dmn", "MainX", "v1", mainCrossRefLibDMN),
		rec("dmn", "Lib", "v1", libDMN),
	}, nil)
	if err != nil {
		t.Fatalf("PlanModelSet rejected a valid cross-model requirement: %v", err)
	}
	pin := findRec(t, out, "MainX").GetBundle().GetImports()["urn:lib"]
	if pin == nil || pin.GetName() != "Lib" {
		t.Fatalf("MainX import not pinned to Lib: %v", pin)
	}
}
