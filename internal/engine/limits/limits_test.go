package limits

import (
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestEffectiveMaxJournalEntries(t *testing.T) {
	cases := []struct {
		name string
		rec  *enginev1.DeploymentRecord
		want uint32
	}{
		{"nil → default", nil, DefaultMaxJournalEntries},
		{"zero → default", &enginev1.DeploymentRecord{}, DefaultMaxJournalEntries},
		{"override under ceiling", &enginev1.DeploymentRecord{MaxJournalEntries: 500}, 500},
		{"override at ceiling", &enginev1.DeploymentRecord{MaxJournalEntries: MaxAllowedJournalEntries}, MaxAllowedJournalEntries},
		{"override above ceiling clamps", &enginev1.DeploymentRecord{MaxJournalEntries: 1 << 30}, MaxAllowedJournalEntries},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := EffectiveMaxJournalEntries(c.rec); got != c.want {
				t.Errorf("got %d; want %d", got, c.want)
			}
		})
	}
}
