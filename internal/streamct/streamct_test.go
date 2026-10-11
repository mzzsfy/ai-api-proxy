package streamct

import "testing"

func TestMediaType(t *testing.T) {
	tests := []struct {
		name string
		ct   string
		sse  bool
		evs  bool
	}{
		{"sse", "text/event-stream", true, false},
		{"sse parameters", "TEXT/EVENT-STREAM; charset=utf-8", true, false},
		{"eventstream", "application/vnd.amazon.eventstream", false, true},
		{"eventstream parameters", "APPLICATION/VND.AMAZON.EVENTSTREAM; x=y", false, true},
		{"similar prefix", "text/event-streaming", false, false},
		{"malformed", "text/event-stream; broken=\"", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if IsSSE(tt.ct) != tt.sse || IsEventStream(tt.ct) != tt.evs || IsStream(tt.ct) != (tt.sse || tt.evs) {
				t.Fatalf("unexpected media type classification for %q", tt.ct)
			}
		})
	}
}
