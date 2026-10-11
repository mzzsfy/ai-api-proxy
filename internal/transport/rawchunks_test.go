package transport

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type readCloser struct{ io.Reader }

func (readCloser) Close() error { return nil }

func TestIsStreamCT(t *testing.T) {
	tests := []struct {
		name string
		ct   string
		want bool
	}{
		{"SSE", "text/event-stream", true},
		{"SSE parameters", "TEXT/EVENT-STREAM; charset=utf-8", true},
		{"EventStream", "application/vnd.amazon.eventstream", true},
		{"EventStream parameter", "application/vnd.amazon.eventstream; x=y", true},
		{"prefix only", "text/event-streaming", false},
		{"malformed", "text/event-stream; broken=\"", false},
		{"other", "application/json", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsStreamCT(tt.ct); got != tt.want {
				t.Fatalf("IsStreamCT(%q) = %v, want %v", tt.ct, got, tt.want)
			}
		})
	}
	if IsEventStreamCT("text/event-stream") || IsSSECT("application/vnd.amazon.eventstream") {
		t.Fatal("media type recognizers overlap")
	}
}

func TestRawChunks_PreservesChunksAndFIFOError(t *testing.T) {
	readErr := errors.New("read failed")
	reader := &resultReader{results: []readResult{{data: []byte{0, 0x80}}, {data: []byte{0xff}, err: readErr}}}
	items := RawChunks(reader, make(chan struct{}))
	first := <-items
	second := <-items
	third := <-items
	if !bytes.Equal(first.Data, []byte{0, 0x80}) || first.Err != nil {
		t.Fatalf("first = %+v", first)
	}
	if !bytes.Equal(second.Data, []byte{0xff}) || second.Err != nil {
		t.Fatalf("second = %+v", second)
	}
	if !errors.Is(third.Err, readErr) || len(third.Data) != 0 {
		t.Fatalf("third = %+v", third)
	}
	if _, ok := <-items; ok {
		t.Fatal("channel must close after read error")
	}
}

type readResult struct {
	data []byte
	err  error
}

type resultReader struct{ results []readResult }

func (r *resultReader) Read(p []byte) (int, error) {
	result := r.results[0]
	r.results = r.results[1:]
	return copy(p, result.data), result.err
}
func (*resultReader) Close() error { return nil }

func TestRawChunks_StopReleasesBackpressure(t *testing.T) {
	stop := make(chan struct{})
	items := RawChunks(readCloser{Reader: bytes.NewReader(bytes.Repeat([]byte{1}, rawChunkSize*(rawChunkQueue+2)))}, stop)
	for range rawChunkQueue {
		<-items
	}
	close(stop)
	select {
	case _, ok := <-items:
		if ok {
			for range items {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not stop after blocked send")
	}
}

func TestHTTPResponseBody_CloseUnblocksRead(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	readDone := make(chan error, 1)
	go func() {
		_, err := resp.Body.Read(make([]byte, 1))
		readDone <- err
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- resp.Body.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return promptly")
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Read")
	}
}

func TestLeaseBody_CloseUnblocksAndReleasesOnce(t *testing.T) {
	started := make(chan struct{})
	body := &blockingBody{started: started, closed: make(chan struct{})}
	var releases atomic.Int32
	leased := &leaseBody{ReadCloser: body, release: func() { releases.Add(1) }}
	readDone := make(chan error, 1)
	go func() {
		_, err := leased.Read(make([]byte, 1))
		readDone <- err
	}()
	<-started
	if err := leased.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("lease Close did not unblock Read")
	}
	if releases.Load() != 1 {
		t.Fatalf("release count = %d, want 1", releases.Load())
	}
}

type blockingBody struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (b *blockingBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}
