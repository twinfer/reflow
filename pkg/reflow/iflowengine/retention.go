package iflowengine

import (
	"encoding/xml"
	"strconv"
	"strings"

	"github.com/twinfer/iflow/timer"
)

// historyTTLms parses a Camunda historyTimeToLive value into milliseconds: an
// ISO-8601 duration (P30D, PT1H — the modern Camunda/Zeebe form) or a bare
// integer count of days (legacy Camunda 7). Returns 0 for empty/unparseable —
// 0 means "no retention", so the engine deletes the terminal record at once.
func historyTTLms(s string) uint64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if s[0] == 'P' || s[0] == 'p' {
		d, err := timer.ParseISO8601Duration(strings.ToUpper(s))
		if err != nil || d <= 0 {
			return 0
		}
		return uint64(d.Milliseconds())
	}
	// Legacy Camunda 7: a bare integer is a count of days.
	days, err := strconv.Atoi(s)
	if err != nil || days <= 0 {
		return 0
	}
	return uint64(days) * 24 * 60 * 60 * 1000
}

// historyTTLFromBPMN extracts the Camunda historyTimeToLive (as ms) of the first
// executable <process> in a BPMN definitions document, matching iflow's
// first-executable selection. The attribute match is namespace-agnostic on the
// local name (so camunda:historyTimeToLive resolves). Returns 0 when absent.
func historyTTLFromBPMN(xmlBytes []byte) uint64 {
	var doc struct {
		Processes []struct {
			IsExecutable string `xml:"isExecutable,attr"`
			HistoryTTL   string `xml:"historyTimeToLive,attr"`
		} `xml:"process"`
	}
	if err := xml.Unmarshal(xmlBytes, &doc); err != nil {
		return 0
	}
	for _, p := range doc.Processes {
		if p.IsExecutable == "true" {
			return historyTTLms(p.HistoryTTL)
		}
	}
	if len(doc.Processes) > 0 {
		return historyTTLms(doc.Processes[0].HistoryTTL)
	}
	return 0
}

// historyTTLFromCMMN extracts the Camunda historyTimeToLive (as ms) of the first
// <case> in a CMMN definitions document, matching iflow's first-case selection.
func historyTTLFromCMMN(xmlBytes []byte) uint64 {
	var doc struct {
		Cases []struct {
			HistoryTTL string `xml:"historyTimeToLive,attr"`
		} `xml:"case"`
	}
	if err := xml.Unmarshal(xmlBytes, &doc); err != nil {
		return 0
	}
	if len(doc.Cases) > 0 {
		return historyTTLms(doc.Cases[0].HistoryTTL)
	}
	return 0
}
