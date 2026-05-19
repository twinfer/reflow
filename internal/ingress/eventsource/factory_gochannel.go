package eventsource

import (
	"log/slog"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

// gochannel is an in-memory broker used by tests and as a "submit via
// in-memory queue" escape hatch. Multiple sources can share an instance
// by passing the same backend.settings["instance"] name — needed because
// a GoChannel is both Subscriber and Publisher and must be the same
// instance for cross-source DLQ delivery.

func init() {
	RegisterFactory("gochannel", newGoChannelFactory())
}

var (
	gochannelInstancesMu sync.Mutex
	gochannelInstances   = map[string]*gochannel.GoChannel{}
)

// GoChannelInstance returns (or creates) the shared in-process GoChannel
// named by id. Exported for tests that want to publish into the same
// broker the dispatcher subscribes to.
func GoChannelInstance(id string) *gochannel.GoChannel {
	gochannelInstancesMu.Lock()
	defer gochannelInstancesMu.Unlock()
	if id == "" {
		id = "default"
	}
	if gc, ok := gochannelInstances[id]; ok {
		return gc
	}
	gc := gochannel.NewGoChannel(gochannel.Config{
		Persistent:                     false,
		BlockPublishUntilSubscriberAck: false,
		PreserveContext:                true,
	}, watermill.NopLogger{})
	gochannelInstances[id] = gc
	return gc
}

// ResetGoChannelInstances drops every cached GoChannel. Called from
// test TearDown to keep instances from leaking across test cases.
func ResetGoChannelInstances() {
	gochannelInstancesMu.Lock()
	defer gochannelInstancesMu.Unlock()
	for _, gc := range gochannelInstances {
		_ = gc.Close()
	}
	gochannelInstances = map[string]*gochannel.GoChannel{}
}

func newGoChannelFactory() Factory {
	return func(_ string, backend BackendConfig, _ *slog.Logger) (message.Subscriber, message.Publisher, error) {
		gc := GoChannelInstance(backend.Settings["instance"])
		return gc, gc, nil
	}
}
